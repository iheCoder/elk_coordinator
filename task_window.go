package elk_coordinator

import (
	"context"
)

// 任务窗口相关常量
const (
	DefaultTaskWindowSize = 3 // 默认任务窗口大小
)

// TaskWindow 处理任务窗口，用于异步获取和处理分区任务
type TaskWindow struct {
	mgr        *Mgr
	windowSize int                // 窗口大小（队列中允许的最大任务数）
	taskQueue  chan PartitionInfo // 任务队列
	fetchDone  chan struct{}      // 通知获取新任务的信号
}

// NewTaskWindow 创建一个新的任务窗口
func NewTaskWindow(mgr *Mgr) *TaskWindow {
	windowSize := DefaultTaskWindowSize
	if mgr.TaskWindowSize > 0 {
		windowSize = mgr.TaskWindowSize
	}

	return &TaskWindow{
		mgr:        mgr,
		windowSize: windowSize,
		taskQueue:  make(chan PartitionInfo, windowSize), // 队列缓冲区大小等于窗口大小
		fetchDone:  make(chan struct{}, 1),               // 缓冲为1，避免阻塞处理协程
	}
}

// Start 启动任务窗口
func (tw *TaskWindow) Start(ctx context.Context) {
	// 启动任务获取goroutine
	go tw.fetchTasks(ctx)

	// 启动任务处理goroutine
	go tw.processTasks(ctx)

	// 初始化时立即触发任务获取，填满窗口
	tw.fetchDone <- struct{}{}
}

// fetchTasks 根据信号获取任务填充队列
func (tw *TaskWindow) fetchTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			tw.mgr.Logger.Infof("任务获取循环停止 (上下文取消)")
			close(tw.taskQueue) // 关闭队列，通知处理协程
			return
		case <-tw.fetchDone:
			// 尝试获取任务，直到填满窗口或没有可用任务
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

	tw.mgr.Logger.Debugf("尝试获取 %d 个新任务填充窗口", tasksToFetch)

	// 尝试获取多个任务填满窗口
	for i := 0; i < tasksToFetch; i++ {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 尝试获取一个新任务
		task, err := tw.mgr.acquirePartitionTask(ctx)
		if err != nil {
			if err != ErrNoAvailablePartition {
				tw.mgr.Logger.Warnf("获取分区任务失败: %v", err)
			}
			break // 无法获取更多任务，退出循环
		}

		// 将任务加入队列
		select {
		case tw.taskQueue <- task:
			tw.mgr.Logger.Debugf("成功获取分区任务 %d 并加入队列", task.PartitionID)
		default:
			// 队列已满（在极少数情况下可能发生），释放任务
			tw.mgr.Logger.Warnf("任务队列已满，无法添加分区任务 %d", task.PartitionID)
			tw.mgr.releasePartitionLock(ctx, task.PartitionID)
			break
		}
	}
}

// processTasks 处理队列中的任务
func (tw *TaskWindow) processTasks(ctx context.Context) {
	for task := range tw.taskQueue {
		// 处理分区任务
		tw.mgr.Logger.Infof("开始处理分区任务 %d", task.PartitionID)
		err := tw.mgr.processPartitionTask(ctx, task)
		if err != nil {
			tw.mgr.handleTaskError(ctx, task, err)
		}
		tw.mgr.Logger.Infof("分区任务 %d 处理完成", task.PartitionID)

		// 处理完一个任务后，立即触发获取新任务
		select {
		case tw.fetchDone <- struct{}{}:
			// 成功发送获取任务信号
		default:
			// 信号通道已满（说明已经有一个获取信号在队列中），跳过
		}
	}
}
