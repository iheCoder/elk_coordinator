package elk_coordinator

import (
	"context"
	"elk_coordinator/model"
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
func (m *Mgr) acquirePartitionTask(ctx context.Context) (model.PartitionInfo, error) {
	// 获取当前分区信息
	partitions, err := m.getPartitions(ctx)
	if err != nil {
		return model.PartitionInfo{}, errors.Wrap(err, "获取分区信息失败")
	}

	if len(partitions) == 0 {
		return model.PartitionInfo{}, model.ErrNoAvailablePartition
	}

	// 寻找可用的分区
	for partitionID, partition := range partitions {
		// 优先处理Pending状态的分区
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			// 尝试锁定这个分区
			lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, m.Namespace, partitionID)
			locked, err := m.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue
			}

			// 更新分区信息，标记为该节点处理
			partition.WorkerID = m.ID
			partition.Status = model.StatusClaimed
			partition.UpdatedAt = time.Now()
		} else if m.shouldReclaimPartition(partition) {
			// 检查是否有长时间未更新的运行中分区（可能出现问题的分区）
			// 注意：这里我们需要更加谨慎，只尝试获取明显过期的分区
			lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, m.Namespace, partitionID)
			locked, err := m.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue
			}

			m.Logger.Infof("重新获取可能卡住的分区 %d，上次更新时间: %v",
				partitionID, partition.UpdatedAt)

			// 重新获取该分区
			partition.WorkerID = m.ID
			partition.Status = model.StatusClaimed
			partition.UpdatedAt = time.Now()

			if err := m.updatePartitionStatus(ctx, partition); err != nil {
				// 如果更新失败，释放锁
				m.DataStore.ReleaseLock(ctx, lockKey, m.ID)
				return model.PartitionInfo{}, errors.Wrap(err, "更新分区状态失败")
			}

			return partition, nil
		}
	}

	return model.PartitionInfo{}, model.ErrNoAvailablePartition
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
func (m *Mgr) getPartitions(ctx context.Context) (map[int]model.PartitionInfo, error) {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, m.Namespace)
	partitionsData, err := m.DataStore.GetPartitions(ctx, partitionInfoKey)
	if err != nil {
		return nil, err
	}

	if partitionsData == "" {
		return nil, nil
	}

	var partitions map[int]model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionsData), &partitions); err != nil {
		return nil, errors.Wrap(err, "解析分区数据失败")
	}

	return partitions, nil
}

// processPartitionTask 处理一个分区任务
func (m *Mgr) processPartitionTask(ctx context.Context, task model.PartitionInfo) error {
	m.Logger.Infof("开始处理分区 %d (ID范围: %d-%d)", task.PartitionID, task.MinID, task.MaxID)

	// 更新分区状态为运行中
	if err := m.updateTaskStatus(ctx, task, model.StatusRunning); err != nil {
		return errors.Wrap(err, "更新分区状态为running失败")
	}

	// 为长时间运行的任务创建心跳上下文
	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	defer cancelHeartbeat()

	// 启动心跳goroutine，定期更新分区状态
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		m.runPartitionHeartbeat(heartbeatCtx, task)
	}()

	// 准备处理选项并执行任务
	processCount, err := m.executeProcessorTask(ctx, task)
	m.Logger.Infof("分区 %d 处理完成: 处理项数=%d, 错误=%v", task.PartitionID, processCount, err)

	// 停止心跳
	cancelHeartbeat()
	<-heartbeatDone

	// 根据处理结果更新状态
	newStatus := model.StatusCompleted
	if err != nil {
		newStatus = model.StatusFailed
	}

	// 更新任务状态并释放锁
	if updateErr := m.updateTaskStatus(ctx, task, newStatus); updateErr != nil {
		m.Logger.Errorf("更新分区 %d 状态为 %s 失败: %v",
			task.PartitionID, newStatus, updateErr)
	}

	m.releasePartitionLock(ctx, task.PartitionID)

	return err
}

// executeProcessorTask 执行处理器任务
func (m *Mgr) executeProcessorTask(ctx context.Context, task model.PartitionInfo) (int64, error) {
	// 记录任务开始时间，用于性能指标统计
	startTime := time.Now()

	// 设置合理的处理超时时间，避免任务运行时间过长
	// 默认使用分区锁过期时间的一半作为处理超时
	taskTimeout := m.PartitionLockExpiry / 2

	// 创建带超时的上下文
	execCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	// 准备处理选项
	options := task.Options
	if options == nil {
		options = make(map[string]interface{})
	}
	options["partition_id"] = task.PartitionID
	options["worker_id"] = m.ID

	// 启动处理，并捕获上下文超时
	processDone := make(chan struct {
		count int64
		err   error
	})

	go func() {
		count, err := m.TaskProcessor.Process(execCtx, task.MinID, task.MaxID, options)
		select {
		case <-execCtx.Done():
			// 上下文已结束，无需发送结果
		default:
			processDone <- struct {
				count int64
				err   error
			}{count, err}
		}
	}()

	// 等待处理完成或超时
	var processCount int64 = 0
	var processErr error

	select {
	case <-ctx.Done():
		processErr = ctx.Err()
	case <-execCtx.Done():
		if execCtx.Err() == context.DeadlineExceeded {
			processErr = errors.New("任务处理超时")
		} else {
			processErr = execCtx.Err()
		}
	case result := <-processDone:
		processCount = result.count
		processErr = result.err
	}

	// 记录任务性能指标
	if m.UseTaskMetrics {
		m.recordTaskPerformance(startTime, processCount, processErr, task.PartitionID)
	}

	return processCount, processErr
}

// updateTaskStatus 更新任务状态
func (m *Mgr) updateTaskStatus(ctx context.Context, task model.PartitionInfo, status string) error {
	task.Status = status
	task.UpdatedAt = time.Now()
	return m.updatePartitionStatus(ctx, task)
}

// releasePartitionLock 释放分区锁
func (m *Mgr) releasePartitionLock(ctx context.Context, partitionID int) {
	lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, m.Namespace, partitionID)
	if releaseErr := m.DataStore.ReleaseLock(ctx, lockKey, m.ID); releaseErr != nil {
		m.Logger.Warnf("释放分区 %d 锁失败: %v", partitionID, releaseErr)
	}
}

// updatePartitionStatus 更新分区状态
func (m *Mgr) updatePartitionStatus(ctx context.Context, task model.PartitionInfo) error {
	// 获取当前所有分区
	partitions, err := m.getPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "获取分区信息失败")
	}

	// 更新特定分区
	partitions[task.PartitionID] = task

	// 保存更新后的分区信息
	return m.savePartitions(ctx, partitions)
}

// savePartitions 保存分区信息到存储
func (m *Mgr) savePartitions(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, m.Namespace)
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
		time.Sleep(model.TaskCompletedDelay)
	} else {
		m.handleAcquisitionError(err)
	}
}

// handleTaskError 处理任务执行错误
func (m *Mgr) handleTaskError(ctx context.Context, task model.PartitionInfo, processErr error) {
	m.Logger.Errorf("处理分区 %d 失败: %v", task.PartitionID, processErr)

	// 更新分区状态为失败
	task.Status = model.StatusFailed
	if updateErr := m.updatePartitionStatus(ctx, task); updateErr != nil {
		m.Logger.Warnf("更新分区状态失败: %v", updateErr)
	}
}

// handleAcquisitionError 处理任务获取错误
func (m *Mgr) handleAcquisitionError(err error) {
	if errors.Is(err, model.ErrNoAvailablePartition) {
		// 没有可用分区，等待一段时间再检查
		time.Sleep(model.NoTaskDelay)
	} else {
		// 其他错误
		m.Logger.Warnf("获取分区任务失败: %v", err)
		time.Sleep(model.TaskRetryDelay)
	}
}

// shouldReclaimPartition 判断一个分区是否应该被重新获取
// 例如分区处于claimed/running状态但长时间未更新
func (m *Mgr) shouldReclaimPartition(partition model.PartitionInfo) bool {
	// 只检查Claimed或Running状态的分区
	if !(partition.Status == model.StatusClaimed || partition.Status == model.StatusRunning) {
		return false
	}

	// 检查分区是否长时间未更新 (超过分区锁过期时间的3倍)
	staleThreshold := m.PartitionLockExpiry * 3
	timeSinceUpdate := time.Since(partition.UpdatedAt)

	// 如果分区更新时间太旧，并且不是本节点持有的，可以尝试重新获取
	if timeSinceUpdate > staleThreshold && partition.WorkerID != m.ID {
		return true
	}

	return false
}

// runPartitionHeartbeat 为正在处理的分区任务发送心跳
// 定期更新分区状态，防止任务被其他节点认为已死亡
func (m *Mgr) runPartitionHeartbeat(ctx context.Context, task model.PartitionInfo) {
	heartbeatTicker := time.NewTicker(m.PartitionLockExpiry / 3) // 确保心跳频率比锁过期时间更频繁
	defer heartbeatTicker.Stop()

	// 记录上次更新时间，避免过于频繁的更新
	lastUpdate := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			// 只有处理时间超过一定阈值才需要发送心跳
			if time.Since(lastUpdate) >= m.PartitionLockExpiry/3 {
				updateCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				// 更新分区时间戳，但保持状态不变
				task.UpdatedAt = time.Now()
				if err := m.updatePartitionStatus(updateCtx, task); err != nil {
					m.Logger.Warnf("更新分区 %d 心跳失败: %v", task.PartitionID, err)
				} else {
					m.Logger.Debugf("已更新分区 %d 心跳", task.PartitionID)
					lastUpdate = time.Now()
				}

				cancel()
			}
		}
	}
}
