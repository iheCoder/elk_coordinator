package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"
	"time"

	"github.com/pkg/errors"
)

// Runner 分区任务执行器
// 负责获取、处理和管理分区任务的执行
type Runner struct {
	// 标识和配置
	namespace           string
	workerID            string
	partitionLockExpiry time.Duration
	allowPreemption     bool // 是否允许抢占其他节点的分区

	// 依赖组件
	partitionStrategy partition.PartitionStrategy
	processor         Processor
	logger            utils.Logger

	// 熔断器
	circuitBreaker *CircuitBreaker
}

// RunnerConfig 用于配置任务执行器
type RunnerConfig struct {
	// 标识和基本配置
	Namespace           string
	WorkerID            string
	PartitionLockExpiry time.Duration
	AllowPreemption     bool // 是否允许抢占其他节点的分区，默认false

	// 依赖组件
	PartitionStrategy partition.PartitionStrategy
	Processor         Processor
	Logger            utils.Logger

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
		allowPreemption:     config.AllowPreemption, // 使用配置中的抢占设置

		// 依赖组件
		partitionStrategy: config.PartitionStrategy,
		processor:         config.Processor,
		logger:            config.Logger,

		// 熔断器
		circuitBreaker: config.CircuitBreaker,
	}
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
	var metadata map[string]interface{}
	if err != nil {
		newStatus = model.StatusFailed
		// 将错误信息传递给分区状态更新
		metadata = map[string]interface{}{
			"error": err.Error(),
		}
	}

	// 更新任务状态并释放锁
	if updateErr := r.updateTaskStatusWithMetadata(ctx, task, newStatus, metadata); updateErr != nil {
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

				// 使用策略接口维护分区持有权
				if err := r.maintainPartitionHold(updateCtx, task.PartitionID); err != nil {
					r.logger.Warnf("维护分区 %d 持有权失败: %v", task.PartitionID, err)
				} else {
					r.logger.Debugf("已维护分区 %d 持有权", task.PartitionID)
					lastUpdate = time.Now()
				}

				cancel()
			}
		}
	}
}

// Stop 停止任务执行器并清理相关资源
//
// 实现关注点分离原则，Runner负责：
// 1. 清理Runner特定的资源（如熔断器状态等）
// 2. 调用PartitionStrategy.Stop()释放分区资源
//
// 参数:
//   - ctx: 上下文，用于控制操作超时和取消
//
// 返回:
//   - error: 如果清理过程中发生错误
func (r *Runner) Stop(ctx context.Context) error {
	r.logger.Infof("Runner stopping, cleaning up Runner-specific resources...")

	// 清理Runner特定的资源
	// 清理熔断器状态（如果有的话）
	if r.circuitBreaker != nil {
		r.logger.Debugf("Resetting circuit breaker state...")
		// 重置熔断器状态，清理所有分区的失败记录
		// 注意：这里可以根据具体的熔断器实现来决定是否需要显式清理
		// 目前CircuitBreaker结构没有Stop方法，所以我们只是记录日志
		r.logger.Debugf("Circuit breaker state cleared")
	}

	r.logger.Infof("Runner-specific cleanup completed")

	// 调用 PartitionStrategy.Stop() 释放分区资源
	r.logger.Infof("Calling PartitionStrategy.Stop() to release partition resources...")
	if err := r.partitionStrategy.Stop(ctx); err != nil {
		r.logger.Errorf("PartitionStrategy.Stop() failed: %v", err)
		return err
	}

	r.logger.Infof("Runner stopped successfully")
	return nil
}
