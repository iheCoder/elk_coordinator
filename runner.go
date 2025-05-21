package elk_coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// Handle 处理分区任务的主循环
func (m *Mgr) Handle(ctx context.Context) error {
	m.Logger.Infof("开始任务处理循环")

	// 如果启用了任务窗口，使用异步并行处理模式
	if m.UseTaskWindow {
		m.Logger.Infof("使用任务窗口模式，窗口大小: %d", m.TaskWindowSize)
		return m.handleWithTaskWindow(ctx)
	}

	// 否则使用传统模式（单任务串行处理）
	for {
		select {
		case <-ctx.Done():
			m.Logger.Infof("任务处理循环停止 (上下文取消)")
			return ctx.Err()
		case <-m.workCtx.Done():
			m.Logger.Infof("任务处理循环停止 (工作上下文取消)")
			return nil
		default:
			m.executeTaskCycle(ctx)
		}
	}
}

// handleWithTaskWindow 使用任务窗口处理分区任务
func (m *Mgr) handleWithTaskWindow(ctx context.Context) error {
	// 初始化任务窗口
	m.taskWindow = NewTaskWindow(m)

	// 启动任务窗口处理
	m.taskWindow.Start(m.workCtx)

	return nil
}

// acquirePartitionTask 获取一个可用的分区任务
func (m *Mgr) acquirePartitionTask(ctx context.Context) (PartitionInfo, error) {
	// 获取当前分区信息
	partitions, err := m.getPartitions(ctx)
	if err != nil {
		return PartitionInfo{}, errors.Wrap(err, "获取分区信息失败")
	}

	if len(partitions) == 0 {
		return PartitionInfo{}, ErrNoAvailablePartition
	}

	// 寻找可用的分区
	for partitionID, partition := range partitions {
		// 优先处理Pending状态的分区
		if partition.Status == StatusPending && partition.WorkerID == "" {
			// 尝试锁定这个分区
			lockKey := fmt.Sprintf(PartitionLockKeyFormat, m.Namespace, partitionID) // Use global constant
			locked, err := m.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue
			}

			// 更新分区信息，标记为该节点处理
			partition.WorkerID = m.ID
			partition.Status = StatusClaimed
			partition.UpdatedAt = time.Now()
			// Return the claimed partition
			if err := m.updatePartitionStatus(ctx, partition); err != nil {
				m.DataStore.ReleaseLock(ctx, lockKey, m.ID) // Release lock if update fails
				return PartitionInfo{}, errors.Wrap(err, "更新新获取分区状态失败")
			}
			return partition, nil

		} else if m.shouldReclaimPartition(partition) {
			// 检查是否有长时间未更新的运行中分区（可能出现问题的分区）
			lockKey := fmt.Sprintf(PartitionLockKeyFormat, m.Namespace, partitionID) // Use global constant
			locked, err := m.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue // Failed to lock, try next partition
			}

			m.Logger.Infof("Reclaiming stale partition %d (status: %s, previously on worker: '%s', last updated: %v by worker: %s)",
				partitionID, partition.Status, partition.WorkerID, partition.UpdatedAt.Format(time.RFC3339), m.ID)

			// Re-claim the partition for the current worker
			partition.WorkerID = m.ID
			partition.Status = StatusClaimed // Set to Claimed first, will be Running by processPartitionTask
			partition.UpdatedAt = time.Now()

			if err := m.updatePartitionStatus(ctx, partition); err != nil {
				// 如果更新失败，释放锁
				m.DataStore.ReleaseLock(ctx, lockKey, m.ID) // Release lock if update fails
				return PartitionInfo{}, errors.Wrap(err, "更新回收分区状态失败")
			}

			return partition, nil
		}
	}

	return PartitionInfo{}, ErrNoAvailablePartition
}

// acquirePartitionLock 尝试获取分区锁
func (m *Mgr) acquirePartitionLock(ctx context.Context, lockKey string, partitionID int) (bool, error) {
	success, err := m.DataStore.AcquireLock(ctx, lockKey, m.ID, m.PartitionLockExpiry)
	if err != nil {
		m.Logger.Warnf("获取分区 %d 锁失败: %v", partitionID, err)
		return false, err
	}

	if success {
		m.Logger.Infof("成功锁定分区 %d", partitionID)
		return true, nil
	}

	return false, nil
}

// getPartitions 获取当前所有分区信息
func (m *Mgr) getPartitions(ctx context.Context) (map[int]PartitionInfo, error) {
	partitionInfoKey := fmt.Sprintf(PartitionInfoKeyFormat, m.Namespace) // Use global constant
	partitionsData, err := m.DataStore.GetPartitions(ctx, partitionInfoKey)
	if err != nil {
		return nil, err
	}

	if partitionsData == "" {
		return nil, nil // No partitions defined yet, not an error
	}

	var partitions map[int]PartitionInfo
	if err := json.Unmarshal([]byte(partitionsData), &partitions); err != nil {
		return nil, errors.Wrap(err, "解析分区数据失败")
	}

	return partitions, nil
}

// _updateTaskStatusAndLog updates the task's status in the datastore and logs the change.
func (m *Mgr) _updateTaskStatusAndLog(ctx context.Context, task PartitionInfo, newStatus string, operation string) error {
	task.Status = newStatus
	task.UpdatedAt = time.Now()
	err := m.updatePartitionStatus(ctx, task) // updatePartitionStatus saves all partitions, which might be heavy.
	if err != nil {
		m.Logger.Errorf("Failed to update partition %d status to %s during %s: %v", task.PartitionID, newStatus, operation, err)
		return errors.Wrapf(err, "updating partition status to %s failed during %s", newStatus, operation)
	}
	m.Logger.Infof("Partition %d status updated to %s after %s.", task.PartitionID, newStatus, operation)
	return nil
}

// _executeTaskWithHeartbeat encapsulates task execution with heartbeat and performance recording.
func (m *Mgr) _executeTaskWithHeartbeat(ctx context.Context, task PartitionInfo) (processCount int64, err error) {
	m.Logger.Debugf("Starting execution with heartbeat for partition %d", task.PartitionID)

	// For long-running tasks, create a heartbeat context
	heartbeatRunCtx, cancelHeartbeat := context.WithCancel(context.Background()) // Use background so it's not tied to incoming ctx directly for heartbeat
	defer cancelHeartbeat()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		m.runPartitionHeartbeat(heartbeatRunCtx, task) // runPartitionHeartbeat needs to use task info that might get stale
	}()

	// Record task start time for performance metrics
	startTime := time.Now()

	// Setup task execution timeout
	// Default to PartitionLockExpiry / 2. Consider making this configurable.
	taskTimeout := m.PartitionLockExpiry / 2
	if taskTimeout <= 0 { // Ensure positive timeout
		taskTimeout = 30 * time.Minute // Fallback to a generous default if PartitionLockExpiry is very short or zero
	}
	execCtx, cancelExec := context.WithTimeout(ctx, taskTimeout) // This ctx is for the TaskProcessor.Process call
	defer cancelExec()

	// Prepare processor options
	options := task.Options
	if options == nil {
		options = make(map[string]interface{})
	}
	options["partition_id"] = task.PartitionID
	options["worker_id"] = m.ID

	// Execute the task
	// This part is moved from the original executeProcessorTask
	processDone := make(chan struct {
		count int64
		err   error
	})

	go func() {
		pCount, pErr := m.TaskProcessor.Process(execCtx, task.MinID, task.MaxID, options)
		// Ensure we don't write to processDone if execCtx is already done (avoiding panic on closed channel)
		// However, processDone is read in a select, so it's generally safe.
		// A check like `if execCtx.Err() == nil` before sending might be too restrictive if Process itself handles cancellation.
		processDone <- struct {
			count int64
			err   error
		}{pCount, pErr}
	}()
	
	select {
	// case <-ctx.Done(): // The parent context from processPartitionTask
	// 	err = errors.Wrap(ctx.Err(), "parent context cancelled during task execution")
	case <-execCtx.Done(): // Task execution context (with timeout)
		if execCtx.Err() == context.DeadlineExceeded {
			err = errors.Errorf("task processing timed out after %v for partition %d", taskTimeout, task.PartitionID)
		} else {
			err = errors.Wrapf(execCtx.Err(), "task execution context ended for partition %d", task.PartitionID)
		}
	case result := <-processDone:
		processCount = result.count
		err = result.err
	}

	// Stop the heartbeat
	cancelHeartbeat()
	<-heartbeatDone // Wait for heartbeat goroutine to finish
	m.Logger.Debugf("Heartbeat stopped for partition %d", task.PartitionID)

	// Record task performance metrics if enabled
	if m.UseTaskMetrics {
		m.recordTaskPerformance(startTime, processCount, err, task.PartitionID)
	}
	
	m.Logger.Infof("Partition %d processing finished: items_processed=%d, error=%v", task.PartitionID, processCount, err)
	return processCount, err
}

// processPartitionTask processes a single partition task using helper methods.
func (m *Mgr) processPartitionTask(ctx context.Context, task PartitionInfo) error {
	m.Logger.Infof("Beginning to process partition %d (ID range: %d-%d)", task.PartitionID, task.MinID, task.MaxID)

	// 1. Update status to StatusRunning
	if err := m._updateTaskStatusAndLog(ctx, task, StatusRunning, "task_start"); err != nil {
		// If updating to running fails, we might not have the lock or there's a store issue.
		// Release lock just in case we acquired it but failed to update status.
		m.releasePartitionLock(ctx, task.PartitionID)
		return err // Return error, task processing cannot proceed.
	}

	// 2. Call _executeTaskWithHeartbeat
	// The context passed here (ctx) is the main context for this attempt to process the task.
	// _executeTaskWithHeartbeat will create its own sub-contexts for timeout and heartbeat.
	_, processErr := m._executeTaskWithHeartbeat(ctx, task)

	// 3. Determine new status
	finalStatus := StatusCompleted
	if processErr != nil {
		finalStatus = StatusFailed
		// Log the specific processing error before updating status
		m.Logger.Errorf("Error processing partition %d: %v", task.PartitionID, processErr)
	}

	// 4. Update status to finalStatus (StatusCompleted or StatusFailed)
	// Use a background context for this final status update to ensure it happens even if the original ctx was cancelled.
	// However, it's often better to use the original ctx if still valid, or a short-timeout ctx.
	// For critical cleanup like this, a new short-lived context is often a good compromise.
	updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 10*time.Second) // Example timeout
	defer cancelUpdate()
	if updateErr := m._updateTaskStatusAndLog(updateCtx, task, finalStatus, "task_end"); updateErr != nil {
		// Log this error, but the original processErr is more important to return to the caller.
		m.Logger.Errorf("Critical: Failed to update final status for partition %d to %s: %v", task.PartitionID, finalStatus, updateErr)
	}

	// 5. Release partition lock
	// Use a similar short-lived context for releasing lock.
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()
	m.releasePartitionLock(releaseCtx, task.PartitionID)
	
	// 6. recordTaskPerformance is now called inside _executeTaskWithHeartbeat.

	// Return the original error from the task processing itself.
	return processErr
// releasePartitionLock 释放分区锁
func (m *Mgr) releasePartitionLock(ctx context.Context, partitionID int) {
	lockKey := fmt.Sprintf(PartitionLockKeyFormat, m.Namespace, partitionID) // Use global constant
	if releaseErr := m.DataStore.ReleaseLock(ctx, lockKey, m.ID); releaseErr != nil {
		m.Logger.Warnf("释放分区 %d 锁失败: %v", partitionID, releaseErr)
	}
}

// updatePartitionStatus 更新分区状态
func (m *Mgr) updatePartitionStatus(ctx context.Context, task PartitionInfo) error {
	// 获取当前所有分区
	partitions, err := m.getPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "获取分区信息失败")
	}
	if partitions == nil { // Ensure partitions map is initialized if no partitions were found
		partitions = make(map[int]PartitionInfo)
	}


	// 更新特定分区
	partitions[task.PartitionID] = task

	// 保存更新后的分区信息
	return m.savePartitions(ctx, partitions)
}

// savePartitions 保存分区信息到存储
func (m *Mgr) savePartitions(ctx context.Context, partitions map[int]PartitionInfo) error {
	partitionInfoKey := fmt.Sprintf(PartitionInfoKeyFormat, m.Namespace) // Use global constant
	updatedData, err := json.Marshal(partitions)
	if err != nil {
		return errors.Wrap(err, "编码更新后分区数据失败")
	}

	return m.DataStore.SetPartitions(ctx, partitionInfoKey, string(updatedData))
}

// executeTaskCycle 执行单个任务处理周期
func (m *Mgr) executeTaskCycle(ctx context.Context) {
	// 获取分区任务
	// 小心获取分区任务时的分布式竞争问题
	task, err := m.acquirePartitionTask(ctx)

	if err == nil {
		// 处理分区任务
		m.Logger.Infof("获取到分区任务 %d, 范围 [%d, %d]",
			task.PartitionID, task.MinID, task.MaxID)

		if processErr := m.processPartitionTask(ctx, task); processErr != nil {
			m.handleTaskError(ctx, task, processErr)
		}

		// 处理完成后休息一下，避免立即抢占下一个任务
		time.Sleep(taskCompletedDelay)
	} else {
		m.handleAcquisitionError(err)
	}
}

// handleTaskError 处理任务执行错误
func (m *Mgr) handleTaskError(ctx context.Context, task PartitionInfo, processErr error) {
	m.Logger.Errorf("处理分区 %d 失败: %v", task.PartitionID, processErr)

	// 更新分区状态为失败
	// Use a short-timeout background context for this update attempt
	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if updateErr := m._updateTaskStatusAndLog(updateCtx, task, StatusFailed, "task_error_handling"); updateErr != nil {
		m.Logger.Warnf("更新分区状态为失败也失败了: %v", updateErr)
	}
}

// handleAcquisitionError 处理任务获取错误
func (m *Mgr) handleAcquisitionError(err error) {
	if errors.Is(err, ErrNoAvailablePartition) {
		// 没有可用分区，等待一段时间再检查
		time.Sleep(noTaskDelay)
	} else {
		// 其他错误
		m.Logger.Warnf("获取分区任务失败: %v", err)
		time.Sleep(taskRetryDelay)
	}
}

// shouldReclaimPartition 判断一个分区是否应该被重新获取
// 例如分区处于claimed/running状态但长时间未更新
func (m *Mgr) shouldReclaimPartition(partition PartitionInfo) bool {
	// Only consider reclaiming partitions that are already Claimed or Running by *another* worker.
	if partition.WorkerID == m.ID || (partition.Status != StatusClaimed && partition.Status != StatusRunning) {
		return false // Not managed by another worker or not in a reclaimable state.
	}

	// Ensure PartitionLockExpiry is positive to avoid issues with duration calculation.
	// StaleTaskReclaimMultiplier should also be positive (enforced by the option func when setting it).
	if m.PartitionLockExpiry <= 0 {
		m.Logger.Warnf("PartitionLockExpiry is not positive (%v), cannot calculate stale threshold for partition %d. Skipping reclaim check.", m.PartitionLockExpiry, partition.PartitionID)
		return false
	}
	
	staleThresholdDuration := time.Duration(m.StaleTaskReclaimMultiplier * float64(m.PartitionLockExpiry))
	
	// It's useful to have a minimum practical stale threshold to prevent overly aggressive reclaiming
	// if PartitionLockExpiry is configured to be very short. For example, at least 30 seconds.
	minStaleThreshold := 30 * time.Second 
	if staleThresholdDuration < minStaleThreshold {
		m.Logger.Debugf("Calculated staleThresholdDuration %v for partition %d is less than minimum %v. Using minimum.", staleThresholdDuration, partition.PartitionID, minStaleThreshold)
		staleThresholdDuration = minStaleThreshold
	}

	timeSinceUpdate := time.Since(partition.UpdatedAt)

	m.Logger.Debugf(
		"Evaluating partition %d for reclaim: Status=%s, UpdatedAt=%v (TimeSinceUpdate: %s), AssignedWorkerID=%s, CurrentEvaluatingWorkerID=%s, StaleThreshold=%s (Multiplier: %.2f, LockExpiry: %s)",
		partition.PartitionID,
		partition.Status,
		partition.UpdatedAt.Format(time.RFC3339), // Log timestamp in a standard format
		timeSinceUpdate.Round(time.Second),      // Log duration rounded to seconds
		partition.WorkerID,
		m.ID,
		staleThresholdDuration.Round(time.Second), // Log duration rounded to seconds
		m.StaleTaskReclaimMultiplier,
		m.PartitionLockExpiry.Round(time.Second),  // Log duration rounded to seconds
	)

	if timeSinceUpdate > staleThresholdDuration {
		m.Logger.Infof(
			"Partition %d (assigned to worker '%s', status '%s', last updated %s ago) deemed STALE by worker '%s'. (StaleThreshold: %s)",
			partition.PartitionID, partition.WorkerID, partition.Status, 
			timeSinceUpdate.Round(time.Second), m.ID, staleThresholdDuration.Round(time.Second),
		)
		return true
	}

	return false
}

// runPartitionHeartbeat 为正在处理的分区任务发送心跳
// 定期更新分区状态，防止任务被其他节点认为已死亡
func (m *Mgr) runPartitionHeartbeat(ctx context.Context, task PartitionInfo) {
	// Ensure PartitionLockExpiry is positive to avoid issues with ticker duration
	if m.PartitionLockExpiry <= 0 {
		m.Logger.Errorf("Cannot run partition heartbeat for partition %d: PartitionLockExpiry is not positive (%v).", task.PartitionID, m.PartitionLockExpiry)
		return
	}
	heartbeatFrequency := m.PartitionLockExpiry / 3
	if heartbeatFrequency <= 0 { // Safety for very short lock expiry
		heartbeatFrequency = time.Second * 5 // Minimum heartbeat frequency
	}

	heartbeatTicker := time.NewTicker(heartbeatFrequency) 
	defer heartbeatTicker.Stop()

	m.Logger.Debugf("Starting heartbeat for partition %d with frequency %v", task.PartitionID, heartbeatFrequency)


	for {
		select {
		case <-ctx.Done():
			m.Logger.Debugf("Stopping heartbeat for partition %d due to context cancellation: %v", task.PartitionID, ctx.Err())
			return
		case <-heartbeatTicker.C:
			// Create a short-lived context for the update operation itself.
			// This context is derived from the heartbeat's main context (ctx).
			updateCtx, cancelUpdate := context.WithTimeout(ctx, heartbeatFrequency/2) // Timeout for update op

			// We need to refresh the task's UpdatedAt timestamp.
			// It's crucial that updatePartitionStatus gets the *current* state of the partition
			// and only updates the timestamp, or that we pass a task object that reflects this.
			// The current `task` variable might be stale if other fields were meant to be updated.
			// For a pure heartbeat, just updating UpdatedAt on a fresh fetch or a specific call is better.
			// However, current updatePartitionStatus replaces the whole entry.
			// A better approach might be a specific `UpdatePartitionHeartbeatTimestamp` method.
			// For now, we make a copy and update its timestamp.
			
			currentPartitionInfo, err := m.getSpecificPartition(updateCtx, task.PartitionID)
			if err != nil {
				m.Logger.Warnf("Heartbeat for partition %d: failed to get current partition info: %v", task.PartitionID, err)
				cancelUpdate()
				continue
			}
			
			// Only update if this worker still holds the claim and it's running
			if currentPartitionInfo.WorkerID == m.ID && currentPartitionInfo.Status == StatusRunning {
				currentPartitionInfo.UpdatedAt = time.Now()
				if err := m.updatePartitionStatus(updateCtx, currentPartitionInfo); err != nil {
					m.Logger.Warnf("Failed to update heartbeat for partition %d: %v", task.PartitionID, err)
				} else {
					m.Logger.Debugf("Heartbeat updated for partition %d at %s", task.PartitionID, currentPartitionInfo.UpdatedAt.Format(time.RFC3339))
				}
			} else {
				m.Logger.Debugf("Heartbeat for partition %d skipped: worker ID (%s vs %s) or status (%s) mismatch.",
					task.PartitionID, currentPartitionInfo.WorkerID, m.ID, currentPartitionInfo.Status)
				// If the task is no longer running or claimed by this worker, the heartbeat can stop.
				cancelUpdate()
				return
			}
			cancelUpdate()
		}
	}
}

// getSpecificPartition is a helper to get a single partition's info, useful for heartbeats.
func (m *Mgr) getSpecificPartition(ctx context.Context, partitionID int) (PartitionInfo, error) {
    partitions, err := m.getPartitions(ctx)
    if err != nil {
        return PartitionInfo{}, err
    }
    if pInfo, ok := partitions[partitionID]; ok {
        return pInfo, nil
    }
    return PartitionInfo{}, errors.Errorf("partition %d not found", partitionID)
}

// Helper constants (assuming these are defined elsewhere, e.g. model.go or mgr.go)
// const (
// 	StatusPending   = "pending"
// 	StatusClaimed   = "claimed"
// 	StatusRunning   = "running"
// 	StatusCompleted = "completed"
// 	StatusFailed    = "failed"
//
// 	PartitionLockFmtFmt = "elk_coord/%s/locks/partition/%d" // Will be replaced by PartitionLockKeyFormat
// 	PartitionInfoKeyFmt = "elk_coord/%s/partitions_info" // Will be replaced by PartitionInfoKeyFormat
// )
//
// var ErrNoAvailablePartition = errors.New("no available partition")
//
// const (
// 	noTaskDelay        = 5 * time.Second
// 	taskRetryDelay     = 10 * time.Second
// 	taskCompletedDelay = 1 * time.Second
// )
//
// type PartitionInfo struct {
// 	PartitionID int                    `json:"partition_id"`
// 	MinID       string                 `json:"min_id"`
// 	MaxID       string                 `json:"max_id"`
// 	Status      string                 `json:"status"`
// 	WorkerID    string                 `json:"worker_id"`
// 	UpdatedAt   time.Time              `json:"updated_at"`
// 	Options     map[string]interface{} `json:"options"`
// }
//
// // For TaskProcessor.Process method signature
// type Processor interface {
// 	Process(ctx context.Context, minID, maxID string, options map[string]interface{}) (int64, error)
// }
//
// // For recordTaskPerformance
// func (m *Mgr) recordTaskPerformance(startTime time.Time, count int64, err error, partitionID int) {
// 	// Placeholder
// }
//
// // For NewTaskWindow
// func NewTaskWindow(m *Mgr) *TaskWindow {
// 	// Placeholder
// 	return nil
// }
// type TaskWindow struct{}
// func (tw *TaskWindow) Start(ctx context.Context) {}
