package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/test_utils" // 保留导入test_utils包
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

// MockProcessor 用于测试的处理器实现
type MockProcessor struct {
	mock.Mock
	processDelay time.Duration
}

func NewMockProcessor() *MockProcessor {
	return &MockProcessor{}
}

func (m *MockProcessor) Process(ctx context.Context, minID, maxID int64, options map[string]interface{}) (int64, error) {
	// 模拟处理时间
	if m.processDelay > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(m.processDelay):
		}
	}

	args := m.Called(ctx, minID, maxID, options)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockProcessor) SetProcessDelay(delay time.Duration) {
	m.processDelay = delay
}

func setupTestRunner(strategy interface{}, processor Processor, logger *test_utils.MockLogger) *Runner {
	config := RunnerConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		PartitionLockExpiry: 3 * time.Minute,
		PartitionStrategy:   strategy.(partition.PartitionStrategy),
		Processor:           processor,
		Logger:              logger,
	}
	return NewRunner(config)
}

// TestRunner_NewRunner 测试Runner构造函数
// 场景: 使用有效的配置创建新的Runner实例
// 预期结果: Runner实例成功创建，所有配置参数被正确应用
func TestRunner_NewRunner(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)

	assert.NotNil(t, runner)
	assert.Equal(t, "test-namespace", runner.namespace)
	assert.Equal(t, "test-worker", runner.workerID)
	assert.Equal(t, 3*time.Minute, runner.partitionLockExpiry)
	assert.NotNil(t, runner.circuitBreaker, "应该初始化默认熔断器")
}

// TestRunner_AcquirePartitionTask_Success 测试成功获取可用分区任务的场景
// 场景: 当有可用的待处理分区时，Runner应该能成功获取并声明该分区
// 预期结果: 成功获取ID为1的分区，并将其状态更新为Claimed，分区属主设置为当前worker
func TestRunner_AcquirePartitionTask_Success(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

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

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)
	ctx := context.Background()

	// 执行测试
	task, err := runner.acquirePartitionTask(ctx)

	// 验证结果
	assert.NoError(t, err)
	assert.Equal(t, 1, task.PartitionID)
	assert.Equal(t, int64(1), task.MinID)
	assert.Equal(t, int64(100), task.MaxID)
	assert.Equal(t, model.StatusClaimed, task.Status)
	assert.Equal(t, "test-worker", task.WorkerID)
}

// TestRunner_AcquirePartitionTask_NoAvailablePartition 测试当没有可用分区时的行为
// 场景: 所有分区都被占用或处于不可获取状态
// 预期结果: 返回ErrNoAvailablePartition错误，返回空的PartitionInfo
func TestRunner_AcquirePartitionTask_NoAvailablePartition(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	// 设置测试数据 - 所有分区都被占用
	testPartition := &model.PartitionInfo{
		PartitionID: 1,
		Status:      model.StatusRunning,
		WorkerID:    "other-worker",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}
	mockStrategy.AddPartition(testPartition)

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)
	ctx := context.Background()

	// 执行测试
	task, err := runner.acquirePartitionTask(ctx)

	// 验证结果
	assert.Error(t, err)
	assert.Equal(t, model.ErrNoAvailablePartition, err)
	assert.Equal(t, model.PartitionInfo{}, task)
}

// TestRunner_ProcessPartitionTask_Success 测试分区任务处理成功的场景
// 场景: 处理一个已声明的分区任务，处理器成功完成处理
// 预期结果: 无错误返回，任务状态从Claimed变为Running，最终变为Completed，并释放锁
func TestRunner_ProcessPartitionTask_Success(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	// 配置mock预期
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(50), nil)

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)
	ctx := context.Background()

	task := model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusClaimed,
		WorkerID:    "test-worker",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	// 将分区添加到mock策略中，这样updateTaskStatus才能找到它
	mockStrategy.AddPartition(&task)

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.NoError(t, err)
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_ProcessorError 测试处理器执行失败的场景
// 场景: 处理一个已声明的分区任务，处理器返回错误
// 预期结果: 返回处理器的错误，分区状态从Running变为Failed，并释放锁
func TestRunner_ProcessPartitionTask_ProcessorError(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 配置mock预期
	processorError := errors.New("processor failed")
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(0), processorError)

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)
	ctx := context.Background()

	task := model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusClaimed,
		WorkerID:    "test-worker",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	// 将分区添加到mock策略中，这样updateTaskStatus才能找到它
	mockStrategy.AddPartition(&task)

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.Error(t, err)
	assert.Equal(t, processorError, err)
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_Timeout 测试任务处理超时的场景
// 场景: 处理器执行时间超过设定的超时时间
// 预期结果: 返回任务处理超时错误，分区状态从Running变为Failed，并释放锁
func TestRunner_ProcessPartitionTask_Timeout(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置处理器延迟超过超时时间
	mockProcessor.SetProcessDelay(2 * time.Second)
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(0), nil)

	// 创建一个短超时的runner
	config := RunnerConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		PartitionLockExpiry: 2 * time.Second, // 短超时用于测试
		PartitionStrategy:   mockStrategy,
		Processor:           mockProcessor,
		Logger:              mockLogger,
	}
	runner := NewRunner(config)

	ctx := context.Background()
	task := model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusClaimed,
		WorkerID:    "test-worker",
		UpdatedAt:   time.Now(),
		Version:     1,
		Options:     make(map[string]interface{}),
	}

	// 将分区添加到mock策略中，这样updateTaskStatus才能找到它
	mockStrategy.AddPartition(&task)

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "任务处理超时")
}

// TestRunner_ShouldReclaimPartition 测试是否应该重新声明分区的逻辑
func TestRunner_ShouldReclaimPartition(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	runner := setupTestRunner(mockStrategy, mockProcessor, mockLogger)

	tests := []struct {
		name      string
		partition model.PartitionInfo
		expected  bool
	}{
		{
			name: "should reclaim old claimed partition",
			partition: model.PartitionInfo{
				Status:    model.StatusClaimed,
				WorkerID:  "other-worker",
				UpdatedAt: time.Now().Add(-10 * time.Minute),
			},
			expected: true,
		},
		{
			name: "should reclaim old running partition",
			partition: model.PartitionInfo{
				Status:    model.StatusRunning,
				WorkerID:  "other-worker",
				UpdatedAt: time.Now().Add(-20 * time.Minute), // 超过阈值 (3 * 5 = 15 分钟)
			},
			expected: true,
		},
		{
			name: "should not reclaim recent partition",
			partition: model.PartitionInfo{
				Status:    model.StatusClaimed,
				WorkerID:  "other-worker",
				UpdatedAt: time.Now().Add(-1 * time.Minute),
			},
			expected: false,
		},
		{
			name: "should not reclaim own partition",
			partition: model.PartitionInfo{
				Status:    model.StatusClaimed,
				WorkerID:  "test-worker",
				UpdatedAt: time.Now().Add(-10 * time.Minute),
			},
			expected: false,
		},
		{
			name: "should not reclaim completed partition",
			partition: model.PartitionInfo{
				Status:    model.StatusCompleted,
				WorkerID:  "other-worker",
				UpdatedAt: time.Now().Add(-10 * time.Minute),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runner.shouldReclaimPartition(tt.partition)
			assert.Equal(t, tt.expected, result)
		})
	}
}
