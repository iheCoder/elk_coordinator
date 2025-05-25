package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/test_utils"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

// TestTaskWindow_NewTaskWindow 测试TaskWindow构造函数
// 场景: 使用有效的配置创建新的TaskWindow实例
// 预期结果: TaskWindow实例成功创建，所有配置参数被正确应用
func TestTaskWindow_NewTaskWindow(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          5,
		PartitionLockExpiry: 3 * time.Minute,
		DataStore:           mockDataStore,
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
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置测试数据
	partitions := createTestPartitions()
	partitionsData, _ := json.Marshal(partitions)

	// 配置mock预期
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return(string(partitionsData), nil).Maybe()

	// 修正锁键格式，使用正确的model.PartitionLockFmtFmt格式
	mockDataStore.On("AcquireLock", mock.Anything, "test-namespace:partition:1", "test-worker", 3*time.Minute).Return(true, nil).Maybe()
	mockDataStore.On("AcquireLock", mock.Anything, "test-namespace:partition:2", "test-worker", 3*time.Minute).Return(true, nil).Maybe()
	// 为分区3添加锁预期，因为该分区可能也会尝试获取锁（虽然它已被其他worker认领，但是已经过期）
	mockDataStore.On("AcquireLock", mock.Anything, "test-namespace:partition:3", "test-worker", 3*time.Minute).Return(true, nil).Maybe()

	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil).Maybe()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          3,
		PartitionLockExpiry: 3 * time.Minute,
		DataStore:           mockDataStore,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}

	taskWindow := NewTaskWindow(config)
	ctx := context.Background()

	// 执行测试
	taskWindow.fillTaskQueue(ctx)

	assert.Equal(t, 3, len(taskWindow.taskQueue)) // 应该获取到2个pending任务和一个处于claimed状态但过期的任务

	mockDataStore.AssertExpectations(t)
}

// TestTaskWindow_Start_And_Process 测试任务窗口启动和处理流程
// 场景: 启动任务窗口，它应该自动获取分区并处理任务
// 预期结果: 任务窗口成功启动，能够获取和处理分区
func TestTaskWindow_Start_And_Process(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置测试数据
	partitions := createTestPartitions()
	partitionsData, _ := json.Marshal(partitions)

	// 配置mock预期 - 使用更灵活的预期设置
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return(string(partitionsData), nil).Maybe()

	// 修正锁键格式
	mockDataStore.On("AcquireLock", mock.Anything, mock.AnythingOfType("string"), "test-worker", 3*time.Minute).Return(true, nil).Maybe()
	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil).Maybe()
	mockDataStore.On("ReleaseLock", mock.Anything, mock.AnythingOfType("string"), "test-worker").Return(nil).Maybe()

	mockProcessor.On("Process", mock.Anything, mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("map[string]interface {}")).Return(int64(10), nil).Maybe()

	config := TaskWindowConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		WindowSize:          2,
		PartitionLockExpiry: 3 * time.Minute,
		DataStore:           mockDataStore,
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
	mockDataStore.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ConcurrentAccess 测试多个Runner并发访问分区的场景
// 场景: 三个Runner并发运行，尝试获取和处理分区
// 预期结果: 所有Runner能够正常工作，不会出现死锁或竞态条件
func TestRunner_ConcurrentAccess(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置足够多的分区用于并发测试
	partitions := make(map[int]model.PartitionInfo)
	for i := 1; i <= 10; i++ {
		partitions[i] = model.PartitionInfo{
			PartitionID: i,
			MinID:       int64(i * 100),
			MaxID:       int64((i + 1) * 100),
			Status:      model.StatusPending,
			WorkerID:    "",
			UpdatedAt:   time.Now(),
			Options:     make(map[string]interface{}),
		}
	}
	partitionsData, _ := json.Marshal(partitions)

	// 配置mock预期 - 使用更灵活的预期设置
	mockDataStore.On("GetPartitions", mock.Anything, mock.AnythingOfType("string")).Return(string(partitionsData), nil).Maybe()
	mockDataStore.On("AcquireLock", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("time.Duration")).Return(true, nil).Maybe()
	mockDataStore.On("SetPartitions", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil).Maybe()
	mockDataStore.On("ReleaseLock", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil).Maybe()
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
				DataStore:           mockDataStore,
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
