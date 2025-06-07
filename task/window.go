package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"
	"time"
)

// TaskWindowConfig 任务窗口配置
type TaskWindowConfig struct {
	// 标识和配置
	Namespace           string
	WorkerID            string
	WindowSize          int
	PartitionLockExpiry time.Duration
	AllowPreemption     bool // 是否允许抢占其他节点的分区

	// 任务获取重试间隔（可选，默认5秒）
	FetchRetryInterval time.Duration

	// 依赖组件
	PartitionStrategy partition.PartitionStrategy
	Processor         Processor
	Logger            utils.Logger

	// 熔断器配置（可选）
	CircuitBreaker *CircuitBreaker // 可以直接提供熔断器实例
}

// TaskWindow 处理任务窗口，用于异步获取和处理分区任务
type TaskWindow struct {
	// 标识和配置
	namespace           string
	workerID            string
	windowSize          int
	partitionLockExpiry time.Duration
	fetchRetryInterval  time.Duration // 可选配置，用于定时重试

	// 依赖组件
	processor Processor // 保留Processor用于可能的未来扩展或直接操作
	logger    utils.Logger

	// 熔断器
	circuitBreaker *CircuitBreaker

	// 内部状态
	taskQueue chan model.PartitionInfo // 任务队列
	fetchDone chan struct{}            // 通知获取新任务的信号
	runner    *Runner                  // 用于执行任务的Runner实例

	// goroutine管理
	ctx    context.Context    // 后台goroutine的context
	cancel context.CancelFunc // 用于取消后台goroutine
}

// NewTaskWindow 创建一个新的任务窗口
func NewTaskWindow(config TaskWindowConfig) *TaskWindow {
	// 设置合理的默认值
	windowSize := model.DefaultTaskWindowSize
	if config.WindowSize > 0 {
		windowSize = config.WindowSize
	}

	// 设置重试间隔默认值
	fetchRetryInterval := 5 * time.Second
	if config.FetchRetryInterval > 0 {
		fetchRetryInterval = config.FetchRetryInterval
	}

	if config.Logger == nil {
		config.Logger = utils.NewDefaultLogger()
	}

	// 创建一个Runner实例
	runner := NewRunner(RunnerConfig{
		Namespace:           config.Namespace,
		WorkerID:            config.WorkerID,
		PartitionLockExpiry: config.PartitionLockExpiry,
		AllowPreemption:     config.AllowPreemption, // 传递抢占配置
		PartitionStrategy:   config.PartitionStrategy,
		Processor:           config.Processor,
		Logger:              config.Logger,
		CircuitBreaker:      config.CircuitBreaker, // 传递熔断器
	})

	return &TaskWindow{
		// 标识和配置
		namespace:           config.Namespace,
		workerID:            config.WorkerID,
		windowSize:          windowSize,
		partitionLockExpiry: config.PartitionLockExpiry,
		fetchRetryInterval:  fetchRetryInterval,

		// 依赖组件
		processor: config.Processor,
		logger:    config.Logger,

		// 熔断器
		circuitBreaker: config.CircuitBreaker,

		// 内部状态
		taskQueue: make(chan model.PartitionInfo, windowSize), // 队列缓冲区大小等于窗口大小
		fetchDone: make(chan struct{}, 1),                     // 缓冲为1，避免阻塞处理协程
		runner:    runner,
	}
}

// Start 启动任务窗口
func (tw *TaskWindow) Start(ctx context.Context) {
	// 创建可取消的context来管理后台goroutine
	tw.ctx, tw.cancel = context.WithCancel(ctx)

	// 启动任务获取goroutine
	go tw.fetchTasks(tw.ctx)

	// 启动任务处理goroutine
	go tw.processTasks(tw.ctx)

	// 初始化时立即触发任务获取，填满窗口
	tw.fetchDone <- struct{}{}
}

// fetchTasks 根据信号或定时器获取任务填充队列
func (tw *TaskWindow) fetchTasks(ctx context.Context) {
	// 使用可配置的重试间隔创建定时器
	retryTicker := time.NewTicker(tw.fetchRetryInterval)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			tw.logger.Infof("任务获取循环停止 (上下文取消)")
			close(tw.taskQueue) // 关闭队列，通知处理协程
			return
		case <-tw.fetchDone:
			// 收到信号，尝试填充队列
			tw.fillTaskQueue(ctx)
		case <-retryTicker.C:
			// 定时器到时，尝试填充队列
			tw.fillTaskQueue(ctx)
		}
	}
}

// fillTaskQueue 尝试填充任务队列到窗口大小
func (tw *TaskWindow) fillTaskQueue(ctx context.Context) {
	// 计算需要获取的任务数量
	currentQueueSize := len(tw.taskQueue)
	tasksToFetch := tw.windowSize - currentQueueSize

	if tasksToFetch <= 0 {
		return // 队列已满，不需要获取新任务
	}

	tw.logger.Debugf("尝试获取 %d 个新任务填充窗口", tasksToFetch)

	// 尝试获取多个任务填满窗口
	for i := 0; i < tasksToFetch; i++ {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 尝试获取一个新任务
		task, err := tw.runner.acquirePartitionTask(ctx)
		if err != nil {
			if err != model.ErrNoAvailablePartition {
				tw.logger.Warnf("获取分区任务失败: %v", err)
			}
			return // 无法获取更多任务，退出方法
		}

		// 将任务加入队列
		select {
		case tw.taskQueue <- task:
			tw.logger.Debugf("成功获取分区任务 %d 并加入队列", task.PartitionID)
		default:
			// 队列已满（在极少数情况下可能发生），释放任务
			tw.logger.Warnf("任务队列已满，无法添加分区任务 %d", task.PartitionID)
			tw.runner.releasePartitionLock(ctx, task.PartitionID)
			return // 队列已满，退出方法
		}
	}
}

// processTasks 处理队列中的任务
func (tw *TaskWindow) processTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			tw.logger.Infof("任务处理循环停止 (上下文取消)")
			return
		case task, ok := <-tw.taskQueue:
			if !ok {
				tw.logger.Infof("任务处理循环停止 (队列已关闭)")
				return
			}

			// 处理分区任务
			tw.logger.Infof("开始处理分区任务 %d", task.PartitionID)
			err := tw.runner.processPartitionTask(ctx, task)
			if err != nil {
				tw.runner.handleTaskError(ctx, task, err)
			}
			tw.logger.Infof("分区任务 %d 处理完成", task.PartitionID)

			// 处理完一个任务后，立即触发获取新任务
			select {
			case tw.fetchDone <- struct{}{}:
				// 成功发送获取任务信号
			default:
				// 信号通道已满（说明已经有一个获取信号在队列中），跳过
			}
		}
	}
}

// Stop 停止任务窗口并清理相关资源
//
// 实现关注点分离原则，建立正确的调用链：TaskWindow -> Runner -> PartitionStrategy
// 1. TaskWindow负责停止自己的后台goroutines并清理内部资源
// 2. 调用Runner.Stop()进行后续清理，Runner会进一步调用PartitionStrategy.Stop()
//
// 参数:
//   - ctx: 上下文，用于控制操作超时和取消
//
// 返回:
//   - error: 如果清理过程中发生错误
func (tw *TaskWindow) Stop(ctx context.Context) error {
	tw.logger.Infof("TaskWindow stopping, cancelling background goroutines...")

	// 取消后台goroutine的context，这会停止fetchTasks和processTasks
	// fetchTasks goroutine会负责关闭taskQueue
	if tw.cancel != nil {
		tw.cancel()
	}

	tw.logger.Infof("TaskWindow background goroutines stopped, proceeding with resource cleanup...")

	// 调用 Runner.Stop() 进行后续清理和资源释放
	tw.logger.Infof("Calling Runner.Stop() for resource cleanup...")
	if err := tw.runner.Stop(ctx); err != nil {
		tw.logger.Errorf("Runner.Stop() failed: %v", err)
		return err
	}

	tw.logger.Infof("TaskWindow stopped successfully")
	return nil
}
