package task

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// Runner 分区任务执行器
// 负责获取、处理和管理分区任务的执行
type Runner struct {
	// 标识和配置
	namespace           string
	workerID            string
	partitionLockExpiry time.Duration

	// 依赖组件
	dataStore data.DataStore
	processor Processor
	logger    utils.Logger

	// 熔断器
	circuitBreaker *CircuitBreaker
}

// RunnerConfig 用于配置任务执行器
type RunnerConfig struct {
	// 标识和基本配置
	Namespace           string
	WorkerID            string
	PartitionLockExpiry time.Duration

	// 依赖组件
	DataStore data.DataStore
	Processor Processor
	Logger    utils.Logger

	// 熔断器配置（可选）
	CircuitBreaker       *CircuitBreaker       // 可以直接提供熔断器实例
	CircuitBreakerConfig *CircuitBreakerConfig // 或者提供熔断器配置，会自动创建熔断器实例
}

// NewRunner 创建新的任务执行器
func NewRunner(config RunnerConfig) *Runner {
	// 设置合理的默认值
	if config.Logger == nil {
		config.Logger = utils.NewDefaultLogger()
	}

	if config.PartitionLockExpiry <= 0 {
		config.PartitionLockExpiry = 3 * time.Minute
	}

	// 初始化熔断器
	if config.CircuitBreaker == nil {
		// 优先使用用户提供的熔断器配置
		if config.CircuitBreakerConfig != nil {
			config.CircuitBreaker = NewCircuitBreaker(*config.CircuitBreakerConfig)
		} else {
			// 使用默认熔断器配置
			config.CircuitBreaker = NewCircuitBreaker(CircuitBreakerConfig{
				ConsecutiveFailureThreshold: 3,                // 默认3次连续失败触发熔断
				TotalFailureThreshold:       5,                // 默认5个分区失败触发熔断
				OpenTimeout:                 30 * time.Second, // 熔断开启30秒后尝试恢复
				MaxHalfOpenRequests:         2,                // 半开状态下允许2个请求尝试
				FailureTimeWindow:           5 * time.Minute,  // 失败统计时间窗口
			})
		}
	}

	return &Runner{
		// 标识和配置
		namespace:           config.Namespace,
		workerID:            config.WorkerID,
		partitionLockExpiry: config.PartitionLockExpiry,

		// 依赖组件
		dataStore: config.DataStore,
		processor: config.Processor,
		logger:    config.Logger,

		// 熔断器
		circuitBreaker: config.CircuitBreaker,
	}
}

// acquirePartitionTask 获取一个可用的分区任务
func (r *Runner) acquirePartitionTask(ctx context.Context) (model.PartitionInfo, error) {
	// 获取当前分区信息
	partitions, err := r.getPartitions(ctx)
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
			lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, r.namespace, partitionID)
			locked, err := r.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue
			}

			// 更新分区信息，标记为该节点处理
			partition.WorkerID = r.workerID
			partition.Status = model.StatusClaimed
			partition.UpdatedAt = time.Now()

			if err := r.updatePartitionStatus(ctx, partition); err != nil {
				// 如果更新失败，释放锁
				r.dataStore.ReleaseLock(ctx, lockKey, r.workerID)
				return model.PartitionInfo{}, errors.Wrap(err, "更新分区状态失败")
			}

			return partition, nil
		} else if r.shouldReclaimPartition(partition) {
			// 检查是否有长时间未更新的运行中分区（可能出现问题的分区）
			// 注意：这里我们需要更加谨慎，只尝试获取明显过期的分区
			lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, r.namespace, partitionID)
			locked, err := r.acquirePartitionLock(ctx, lockKey, partitionID)
			if err != nil || !locked {
				continue
			}

			r.logger.Infof("重新获取可能卡住的分区 %d，上次更新时间: %v",
				partitionID, partition.UpdatedAt)

			// 重新获取该分区
			partition.WorkerID = r.workerID
			partition.Status = model.StatusClaimed
			partition.UpdatedAt = time.Now()

			if err := r.updatePartitionStatus(ctx, partition); err != nil {
				// 如果更新失败，释放锁
				r.dataStore.ReleaseLock(ctx, lockKey, r.workerID)
				return model.PartitionInfo{}, errors.Wrap(err, "更新分区状态失败")
			}

			return partition, nil
		}
	}

	return model.PartitionInfo{}, model.ErrNoAvailablePartition
}

// acquirePartitionLock 尝试获取分区锁
func (r *Runner) acquirePartitionLock(ctx context.Context, lockKey string, partitionID int) (bool, error) {
	success, err := r.dataStore.AcquireLock(ctx, lockKey, r.workerID, r.partitionLockExpiry)
	if err != nil {
		r.logger.Warnf("获取分区 %d 锁失败: %v", partitionID, err)
		return false, err
	}

	if success {
		r.logger.Infof("成功锁定分区 %d", partitionID)
		return true, nil
	}

	return false, nil
}

// getPartitions 获取当前所有分区信息
func (r *Runner) getPartitions(ctx context.Context) (map[int]model.PartitionInfo, error) {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, r.namespace)
	partitionsData, err := r.dataStore.GetPartitions(ctx, partitionInfoKey)
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
func (r *Runner) processPartitionTask(ctx context.Context, task model.PartitionInfo) error {
	r.logger.Infof("开始处理分区 %d (ID范围: %d-%d)", task.PartitionID, task.MinID, task.MaxID)

	// 更新分区状态为运行中
	if err := r.updateTaskStatus(ctx, task, model.StatusRunning); err != nil {
		return errors.Wrap(err, "更新分区状态为running失败")
	}

	// 为长时间运行的任务创建心跳上下文
	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	defer cancelHeartbeat()

	// 启动心跳goroutine，定期更新分区状态
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		r.runPartitionHeartbeat(heartbeatCtx, task)
	}()

	// 准备处理选项并执行任务
	processCount, err := r.executeProcessorTask(ctx, task)
	r.logger.Infof("分区 %d 处理完成: 处理项数=%d, 错误=%v", task.PartitionID, processCount, err)

	// 停止心跳
	cancelHeartbeat()
	<-heartbeatDone

	// 根据处理结果更新状态
	newStatus := model.StatusCompleted
	if err != nil {
		newStatus = model.StatusFailed
	}

	// 更新任务状态并释放锁
	if updateErr := r.updateTaskStatus(ctx, task, newStatus); updateErr != nil {
		r.logger.Errorf("更新分区 %d 状态为 %s 失败: %v",
			task.PartitionID, newStatus, updateErr)
	}

	r.releasePartitionLock(ctx, task.PartitionID)

	return err
}

// executeProcessorTask 执行处理器任务
func (r *Runner) executeProcessorTask(ctx context.Context, task model.PartitionInfo) (int64, error) {
	// 设置合理的处理超时时间，避免任务运行时间过长
	// 默认使用分区锁过期时间的一半作为处理超时
	taskTimeout := r.partitionLockExpiry / 2

	// 创建带超时的上下文
	execCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	// 准备处理选项
	options := task.Options
	if options == nil {
		options = make(map[string]interface{})
	}
	options["partition_id"] = task.PartitionID
	options["worker_id"] = r.workerID

	// 如果配置了熔断器，等待熔断器允许执行
	if r.circuitBreaker != nil {
		partitionID := task.PartitionID
		// 等待熔断器允许执行或超时
		if err := r.circuitBreaker.WaitUntilAllowed(execCtx); err != nil {
			r.logger.Warnf("等待熔断器允许执行失败: %v", err)
			return 0, err
		}
		r.logger.Debugf("熔断器允许分区 %d 处理", partitionID)
	}

	// 启动处理，并捕获上下文超时
	processDone := make(chan struct {
		count int64
		err   error
	})

	go func() {
		count, err := r.processor.Process(execCtx, task.MinID, task.MaxID, options)
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

	// 根据处理结果更新熔断器状态
	if r.circuitBreaker != nil {
		partitionID := task.PartitionID
		if processErr != nil {
			r.logger.Warnf("分区 %d 处理失败，记录到熔断器: %v", partitionID, processErr)
			r.circuitBreaker.RecordFailure(partitionID, processErr)
		} else {
			r.logger.Debugf("分区 %d 处理成功，更新熔断器状态", partitionID)
			r.circuitBreaker.RecordSuccess(partitionID)
		}
	}

	return processCount, processErr
}

// updateTaskStatus 更新任务状态
func (r *Runner) updateTaskStatus(ctx context.Context, task model.PartitionInfo, status string) error {
	task.Status = status
	task.UpdatedAt = time.Now()
	return r.updatePartitionStatus(ctx, task)
}

// releasePartitionLock 释放分区锁
func (r *Runner) releasePartitionLock(ctx context.Context, partitionID int) {
	lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, r.namespace, partitionID)
	if releaseErr := r.dataStore.ReleaseLock(ctx, lockKey, r.workerID); releaseErr != nil {
		r.logger.Warnf("释放分区 %d 锁失败: %v", partitionID, releaseErr)
	}
}

// updatePartitionStatus 更新分区状态
func (r *Runner) updatePartitionStatus(ctx context.Context, task model.PartitionInfo) error {
	// 获取当前所有分区
	partitions, err := r.getPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "获取分区信息失败")
	}

	// 更新特定分区
	partitions[task.PartitionID] = task

	// 保存更新后的分区信息
	return r.savePartitions(ctx, partitions)
}

// savePartitions 保存分区信息到存储
func (r *Runner) savePartitions(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, r.namespace)
	updatedData, err := json.Marshal(partitions)
	if err != nil {
		return errors.Wrap(err, "编码更新后分区数据失败")
	}

	return r.dataStore.SetPartitions(ctx, partitionInfoKey, string(updatedData))
}

// handleTaskError 处理任务执行错误
func (r *Runner) handleTaskError(ctx context.Context, task model.PartitionInfo, processErr error) {
	r.logger.Errorf("处理分区 %d 失败: %v", task.PartitionID, processErr)

	// 更新分区状态为失败
	// 注意：这里不直接修改task.Status，而是通过updateTaskStatus来确保一致性
	if updateErr := r.updateTaskStatus(ctx, task, model.StatusFailed); updateErr != nil {
		r.logger.Warnf("更新分区 %d 状态为失败时再次失败: %v", task.PartitionID, updateErr)
	}
	// 锁的释放由 processPartitionTask 的 defer 语句处理，或者在错误路径中显式处理
}

// shouldReclaimPartition 判断一个分区是否应该被重新获取
// 例如分区处于claimed/running状态但长时间未更新
func (r *Runner) shouldReclaimPartition(partition model.PartitionInfo) bool {
	// 只检查Claimed或Running状态的分区
	if !(partition.Status == model.StatusClaimed || partition.Status == model.StatusRunning) {
		return false
	}

	// 检查分区是否长时间未更新 (超过分区锁过期时间的3倍)
	staleThreshold := r.partitionLockExpiry * 3
	timeSinceUpdate := time.Since(partition.UpdatedAt)

	// 如果分区更新时间太旧，并且不是本节点持有的，可以尝试重新获取
	if timeSinceUpdate > staleThreshold && partition.WorkerID != r.workerID {
		return true
	}

	return false
}

// runPartitionHeartbeat 为正在处理的分区任务发送心跳
// 定期更新分区状态，防止任务被其他节点认为已死亡
func (r *Runner) runPartitionHeartbeat(ctx context.Context, task model.PartitionInfo) {
	heartbeatTicker := time.NewTicker(r.partitionLockExpiry / 3) // 确保心跳频率比锁过期时间更频繁
	defer heartbeatTicker.Stop()

	// 记录上次更新时间，避免过于频繁的更新
	lastUpdate := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			// 只有处理时间超过一定阈值才需要发送心跳
			if time.Since(lastUpdate) >= r.partitionLockExpiry/3 {
				updateCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				// 更新分区时间戳，但保持状态不变
				task.UpdatedAt = time.Now()
				if err := r.updatePartitionStatus(updateCtx, task); err != nil {
					r.logger.Warnf("更新分区 %d 心跳失败: %v", task.PartitionID, err)
				} else {
					r.logger.Debugf("已更新分区 %d 心跳", task.PartitionID)
					lastUpdate = time.Now()
				}

				cancel()
			}
		}
	}
}
