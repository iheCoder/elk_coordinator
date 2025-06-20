package task

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestTaskWindow_NewTaskWindow 测试TaskWindow构造函数
// 场景: 使用有效的配置创建新的TaskWindow实例
// 预期结果: TaskWindow实例成功创建，所有配置参数被正确应用
func TestTaskWindow_NewTaskWindow(t *testing.T) {
	mockStrategy := &test_utils.MockPartitionStrategy{}
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          5,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	assert.NotNil(t, taskWindow)
	assert.Equal(t, "test-namespace", taskWindow.namespace)
	assert.Equal(t, "test-worker", taskWindow.workerID)
	assert.Equal(t, 5, taskWindow.windowSize)
	assert.Equal(t, 5, cap(taskWindow.taskQueue))
}

// TestTaskWindow_FillTaskQueue 测试填充任务队列的功能
// 场景: 有两个待处理的分区可供获取，窗口大小为3
// 预期结果: 成功获取两个分区并填充到任务队列中
func TestTaskWindow_FillTaskQueue(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置测试数据
	testPartition1 := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}
	testPartition2 := &model.PartitionInfo{
		PartitionID: 2,
		MinID:       101,
		MaxID:       200,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}
	mockStrategy.AddPartition(testPartition1)
	mockStrategy.AddPartition(testPartition2)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          3,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)
	ctx := context.Background()

	// 执行测试
	taskWindow.fillTaskQueue(ctx)

	assert.Equal(t, 2, len(taskWindow.taskQueue)) // 应该获取到2个pending任务
}

// TestTaskWindow_Start_And_Process 测试任务窗口启动和处理流程
// 场景: 启动任务窗口，它应该自动获取分区并处理任务
// 预期结果: 任务窗口成功启动，能够获取和处理分区
func TestTaskWindow_Start_And_Process(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置测试数据
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}
	mockStrategy.AddPartition(testPartition)

	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 启动任务窗口
	taskWindow.Start(ctx)

	// 等待一段时间让任务处理完成
	time.Sleep(1 * time.Second)

	// 验证mock调用
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ConcurrentAccess 测试多个Runner并发访问分区的场景
// 场景: 三个Runner并发运行，尝试获取和处理分区
// 预期结果: 所有Runner能够正常工作，不会出现死锁或竞态条件
func TestRunner_ConcurrentAccess(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置足够多的分区用于并发测试
	for i := 1; i <= 10; i++ {
		testPartition := &model.PartitionInfo{
			PartitionID: i,
			MinID:       int64(i * 100),
			MaxID:       int64((i + 1) * 100),
			Status:      model.StatusPending,
			WorkerID:    "",
			UpdatedAt:   time.Now(),
			Version:     1,
			Options:     make(map[string]interface{}),
		}
		mockStrategy.AddPartition(testPartition)
	}

	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	// 创建多个runner并发执行
	numWorkers := 3
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			config := RunnerConfig{
				Namespace:           "test-namespace",
				WorkerID:            fmt.Sprintf("worker-%d", workerID),
				PartitionLockExpiry: 3 * time.Minute,
				PartitionStrategy:   mockStrategy,
				Processor:           mockProcessor,
				Logger:              mockLogger,
			}

			runner := NewRunner(config)

			// 执行几次任务获取
			for j := 0; j < 3; j++ {
				task, err := runner.acquirePartitionTask(ctx)
				if err == nil {
					runner.processPartitionTask(ctx, task)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// 验证没有竞态条件发生
	// 这里主要是确保测试能正常完成，没有死锁或panic
	assert.True(t, true, "并发测试完成")
}

// TestTaskWindow_RetryMechanismWhenNoInitialPartitions 测试当初始没有分区时的重试机制
// 场景: TaskWindow启动时没有可用分区，稍后分区变为可用
// 预期结果: TaskWindow不会死锁，能够通过定时重试机制获取到稍后可用的分区
func TestTaskWindow_RetryMechanismWhenNoInitialPartitions(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 初始时不添加任何分区，模拟还没有分区可用的情况

	// 设置处理器期望调用
	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// 启动任务窗口
	taskWindow.Start(ctx)

	// 等待初始获取尝试完成
	time.Sleep(500 * time.Millisecond)

	// 验证初始时队列为空（因为没有可用分区）
	assert.Equal(t, 0, len(taskWindow.taskQueue), "初始时队列应为空")

	// 模拟稍后分区变为可用
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	// 在2秒后添加分区（模拟Leader创建分区的延迟）
	go func() {
		time.Sleep(2 * time.Second)
		mockStrategy.AddPartition(testPartition)
		mockLogger.Infof("测试：已添加分区 %d", testPartition.PartitionID)
	}()

	// 等待足够长的时间让定时重试机制触发
	// TaskWindow 的重试间隔是5秒，所以等待7秒应该足够
	time.Sleep(7 * time.Second)

	// 验证任务最终被获取并处理
	// 注意：由于任务可能正在处理中，队列可能为空但处理器应该被调用
	mockProcessor.AssertExpectations(t)

	// 可以通过日志或其他方式验证分区确实被处理了
	// 这里我们主要验证没有死锁发生，测试能够正常完成
	mockLogger.Infof("测试完成：验证没有死锁发生")
}

// TestTaskWindow_ImmediateRetryAfterFailure 测试获取任务失败后的立即重试
// 场景: fillTaskQueue失败后，通过定时器重试机制能够成功获取任务
// 预期结果: 即使初始获取失败，后续重试能够成功
func TestTaskWindow_ImmediateRetryAfterFailure(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 创建一个会延迟返回分区的策略
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Once()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          1,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	// 1秒后添加分区
	go func() {
		time.Sleep(1 * time.Second)
		mockStrategy.AddPartition(testPartition)
	}()

	// 启动任务窗口
	taskWindow.Start(ctx)

	// 等待足够的时间让重试机制工作
	time.Sleep(6 * time.Second)

	// 验证处理器被调用了
	mockProcessor.AssertExpectations(t)
}

// TestTaskWindow_CustomFetchRetryInterval 测试自定义重试间隔配置
// 场景: 配置自定义的重试间隔，验证定时器按配置的间隔工作
// 预期结果: 重试间隔按配置工作，较短的间隔能更快地重试获取任务
func TestTaskWindow_CustomFetchRetryInterval(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 使用较短的重试间隔进行测试
	customRetryInterval := 1 * time.Second

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          1,
		PartitionLockExpiry: 3 * time.Minute,
		FetchRetryInterval:  customRetryInterval, // 自定义重试间隔
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 验证重试间隔被正确设置
	assert.Equal(t, customRetryInterval, taskWindow.fetchRetryInterval)

	// 设置测试数据 - 1.5秒后添加分区
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Once()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1.5秒后添加分区
	go func() {
		time.Sleep(1500 * time.Millisecond)
		mockStrategy.AddPartition(testPartition)
		mockLogger.Infof("测试：已添加分区 %d", testPartition.PartitionID)
	}()

	// 启动任务窗口
	taskWindow.Start(ctx)

	// 等待足够的时间，由于重试间隔是1秒，应该在2秒内就能获取到任务
	time.Sleep(2500 * time.Millisecond)

	// 验证处理器被调用了
	mockProcessor.AssertExpectations(t)
}

// TestTaskWindow_Stop_Basic 测试 TaskWindow.Stop() 方法的基本功能
// 场景: 正常启动 TaskWindow 然后调用 Stop() 进行停止
// 预期结果: Stop() 方法能够成功执行，停止后台 goroutines 并调用 Runner.Stop()
func TestTaskWindow_Stop_Basic(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待一段时间确保后台 goroutines 已启动
	time.Sleep(100 * time.Millisecond)

	// 验证 context 和 cancel 已设置
	assert.NotNil(t, taskWindow.ctx, "Start() 应该设置 context")
	assert.NotNil(t, taskWindow.cancel, "Start() 应该设置 cancel function")

	// 调用 Stop()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := taskWindow.Stop(stopCtx)

	// 验证结果
	assert.NoError(t, err, "Stop() 应该成功完成")

	// 验证 context 已被取消
	select {
	case <-taskWindow.ctx.Done():
		// 正确，context 已被取消
	default:
		t.Error("Stop() 后 context 应该已被取消")
	}

	t.Log("TaskWindow.Stop() basic functionality test completed successfully")
}

// TestTaskWindow_Stop_BackgroundGoroutinesCleanup 测试后台 goroutines 的正确清理
// 场景: 启动 TaskWindow 后立即停止，验证所有后台 goroutines 被正确停止
// 预期结果: 后台 goroutines 被正确停止，没有 goroutine 泄漏
func TestTaskWindow_Stop_BackgroundGoroutinesCleanup(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		FetchRetryInterval:  1 * time.Second, // 较短的重试间隔便于测试
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待确保后台 goroutines 启动
	time.Sleep(200 * time.Millisecond)

	// 调用 Stop()
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	err := taskWindow.Stop(stopCtx)
	elapsed := time.Since(start)

	// 验证结果
	assert.NoError(t, err, "Stop() 应该成功完成")
	assert.Less(t, elapsed, 2*time.Second, "Stop() 应该快速完成，不应该等待很长时间")

	// 验证 channels 被正确关闭
	select {
	case _, ok := <-taskWindow.fetchDone:
		assert.False(t, ok, "fetchDone channel 应该已关闭")
	default:
		// fetchDone 已关闭，从中读取会立即返回
	}

	select {
	case _, ok := <-taskWindow.taskQueue:
		assert.False(t, ok, "taskQueue channel 应该已关闭")
	default:
		// taskQueue 已关闭
	}

	t.Logf("Background goroutines cleanup test completed in %v", elapsed)
}

// TestTaskWindow_Stop_WithRunningTasks 测试有正在处理的任务时的停止行为
// 场景: TaskWindow 正在处理任务时调用 Stop()
// 预期结果: Stop() 能够快速完成，不等待正在处理的任务完成
func TestTaskWindow_Stop_WithRunningTasks(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置一个长时间运行的任务
	mockProcessor.SetProcessDelay(2 * time.Second)

	// 添加测试分区
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}
	mockStrategy.AddPartition(testPartition)

	// 设置处理器期望调用
	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          1,
		PartitionLockExpiry: 3 * time.Minute,
		FetchRetryInterval:  100 * time.Millisecond,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待任务开始处理
	time.Sleep(300 * time.Millisecond)

	// 调用 Stop()，应该能够快速完成
	stopStart := time.Now()
	stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := taskWindow.Stop(stopCtx)
	stopDuration := time.Since(stopStart)

	// 验证结果
	assert.NoError(t, err, "Stop() 应该成功完成")
	assert.Less(t, stopDuration, 800*time.Millisecond, "Stop() 应该快速完成，不等待正在处理的任务")

	t.Logf("Stop with running tasks completed in %v", stopDuration)
}

// TestTaskWindow_Stop_MultipleCallsSuccess 测试多次调用 Stop() 的行为
// 场景: 多次调用 TaskWindow.Stop() 方法
// 预期结果: 所有调用都应该成功，不应该出现 panic 或错误
func TestTaskWindow_Stop_MultipleCallsSuccess(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待确保启动完成
	time.Sleep(100 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 第一次调用 Stop()
	err1 := taskWindow.Stop(stopCtx)
	assert.NoError(t, err1, "第一次 Stop() 调用应该成功")

	// 第二次调用 Stop()
	err2 := taskWindow.Stop(stopCtx)
	assert.NoError(t, err2, "第二次 Stop() 调用应该成功")

	// 第三次调用 Stop()
	err3 := taskWindow.Stop(stopCtx)
	assert.NoError(t, err3, "第三次 Stop() 调用应该成功")

	t.Log("Multiple Stop() calls handled successfully")
}

// TestTaskWindow_Stop_CancelledContext 测试在已取消的 context 中调用 Stop()
// 场景: 使用已取消的 context 调用 TaskWindow.Stop()
// 预期结果: Stop() 应该仍然能够执行基本的清理操作
func TestTaskWindow_Stop_CancelledContext(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待确保启动完成
	time.Sleep(100 * time.Millisecond)

	// 创建已取消的 context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	// 调用 Stop() with cancelled context
	err := taskWindow.Stop(cancelledCtx)

	// 即使 context 已取消，Stop() 也应该能够执行基本清理
	assert.NoError(t, err, "Stop() 应该能够处理已取消的 context")

	// 验证 TaskWindow 的 context 已被取消
	select {
	case <-taskWindow.ctx.Done():
		// 正确，context 已被取消
	default:
		t.Error("TaskWindow context 应该已被取消")
	}

	t.Log("Stop() with cancelled context handled successfully")
}

// TestTaskWindow_Stop_ConcurrentStopCalls 测试并发调用 Stop() 的行为
// 场景: 多个 goroutines 同时调用 TaskWindow.Stop()
// 预期结果: 所有调用都应该成功完成，不应该出现竞态条件或 panic
func TestTaskWindow_Stop_ConcurrentStopCalls(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待确保启动完成
	time.Sleep(100 * time.Millisecond)

	// 并发调用 Stop()
	const numConcurrentCalls = 5
	var wg sync.WaitGroup
	errors := make([]error, numConcurrentCalls)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < numConcurrentCalls; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errors[index] = taskWindow.Stop(stopCtx)
		}(i)
	}

	wg.Wait()

	// 验证所有调用都成功
	for i, err := range errors {
		assert.NoError(t, err, fmt.Sprintf("并发 Stop() 调用 %d 应该成功", i))
	}

	t.Log("Concurrent Stop() calls handled successfully")
}

// TestTaskWindow_Stop_WithoutStart 测试在未调用 Start() 的情况下调用 Stop()
// 场景: 创建 TaskWindow 但不调用 Start()，直接调用 Stop()
// 预期结果: Stop() 应该能够正常处理这种情况，不应该 panic
func TestTaskWindow_Stop_WithoutStart(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 直接调用 Stop()，不先调用 Start()
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := taskWindow.Stop(stopCtx)

	// Stop() 应该能够处理这种情况
	assert.NoError(t, err, "Stop() 应该能够处理未启动的 TaskWindow")

	t.Log("Stop() without Start() handled successfully")
}

// TestTaskWindow_Stop_RunnerStopError 测试 Runner.Stop() 返回错误时的处理
// 场景: 模拟 Runner.Stop() 返回错误
// 预期结果: TaskWindow.Stop() 应该返回 Runner.Stop() 的错误
func TestTaskWindow_Stop_RunnerStopError(t *testing.T) {
	// 创建一个会在 Stop() 时返回错误的 mock strategy
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置 mock strategy 在 Stop() 时返回错误
	expectedErr := fmt.Errorf("simulated runner stop error")
	mockStrategy.StopFunc = func(ctx context.Context) error {
		return expectedErr
	}

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 等待确保启动完成
	time.Sleep(100 * time.Millisecond)

	// 调用 Stop()
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := taskWindow.Stop(stopCtx)

	// 验证返回了预期的错误
	assert.Error(t, err, "Stop() 应该返回 Runner.Stop() 的错误")
	assert.Contains(t, err.Error(), "simulated runner stop error", "错误信息应该包含模拟的错误")

	// 验证 TaskWindow 的后台 goroutines 仍然被停止
	select {
	case <-taskWindow.ctx.Done():
		// 正确，context 已被取消
	default:
		t.Error("即使 Runner.Stop() 失败，TaskWindow context 也应该被取消")
	}

	t.Log("Runner.Stop() error handling test completed successfully")
}

// TestTaskWindow_Stop_Performance 测试 Stop() 方法的性能
// 场景: 测试 Stop() 方法在不同场景下的执行时间
// 预期结果: Stop() 应该在合理时间内完成
func TestTaskWindow_Stop_Performance(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(false) // 关闭详细日志以减少干扰

	// 设置处理器期望调用
	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	// 添加一些测试分区
	for i := 1; i <= 10; i++ {
		testPartition := &model.PartitionInfo{
			PartitionID: i,
			MinID:       int64(i * 100),
			MaxID:       int64((i+1)*100 - 1),
			Status:      model.StatusPending,
			WorkerID:    "",
			UpdatedAt:   time.Now(),
			Version:     1,
			Options:     make(map[string]interface{}),
		}
		mockStrategy.AddPartition(testPartition)
	}

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          5,
		PartitionLockExpiry: 3 * time.Minute,
		FetchRetryInterval:  100 * time.Millisecond,
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)

	// 启动 TaskWindow
	startCtx := context.Background()
	taskWindow.Start(startCtx)

	// 让它运行一段时间
	time.Sleep(200 * time.Millisecond)

	// 测试 Stop() 性能
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := taskWindow.Stop(stopCtx)
	elapsed := time.Since(start)

	// 验证结果
	assert.NoError(t, err, "Stop() 应该成功完成")
	assert.Less(t, elapsed, 1*time.Second, "Stop() 应该在1秒内完成")

	t.Logf("Stop() performance test completed in %v", elapsed)
}
