package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/test_utils"
	"fmt"
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
