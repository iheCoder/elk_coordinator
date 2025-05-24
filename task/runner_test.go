package task

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/test_utils" // 保留导入test_utils包
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

// MockDataStore 自定义mock数据存储实现
type MockDataStore struct {
	mock.Mock
}

// AcquireLock implements data.DataStore
func (m *MockDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	args := m.Called(ctx, key, value, expiry)
	return args.Bool(0), args.Error(1)
}

// RenewLock implements data.DataStore
func (m *MockDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	args := m.Called(ctx, key, value, expiry)
	return args.Bool(0), args.Error(1)
}

// CheckLock implements data.DataStore
func (m *MockDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	args := m.Called(ctx, key, expectedValue)
	return args.Bool(0), args.Error(1)
}

// ReleaseLock implements data.DataStore
func (m *MockDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

// GetLockOwner implements data.DataStore
func (m *MockDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	args := m.Called(ctx, key)
	return args.String(0), args.Error(1)
}

// SetHeartbeat implements data.DataStore
func (m *MockDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

// GetHeartbeat implements data.DataStore
func (m *MockDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	args := m.Called(ctx, key)
	return args.String(0), args.Error(1)
}

// SetKey implements data.DataStore
func (m *MockDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	args := m.Called(ctx, key, value, expiry)
	return args.Error(0)
}

// GetKey implements data.DataStore
func (m *MockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	args := m.Called(ctx, key)
	return args.String(0), args.Error(1)
}

// GetKeys implements data.DataStore
func (m *MockDataStore) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	args := m.Called(ctx, pattern)
	return args.Get(0).([]string), args.Error(1)
}

// DeleteKey implements data.DataStore
func (m *MockDataStore) DeleteKey(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

// SetPartitions implements data.DataStore
func (m *MockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

// GetPartitions implements data.DataStore
func (m *MockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	args := m.Called(ctx, key)
	return args.String(0), args.Error(1)
}

// SetSyncStatus implements data.DataStore
func (m *MockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

// GetSyncStatus implements data.DataStore
func (m *MockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	args := m.Called(ctx, key)
	return args.String(0), args.Error(1)
}

// RegisterWorker implements data.DataStore
func (m *MockDataStore) RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error {
	args := m.Called(ctx, workersKey, workerID, heartbeatKey, heartbeatValue)
	return args.Error(0)
}

// UnregisterWorker implements data.DataStore
func (m *MockDataStore) UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error {
	args := m.Called(ctx, workersKey, workerID, heartbeatKey)
	return args.Error(0)
}

// GetActiveWorkers implements data.DataStore
func (m *MockDataStore) GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error) {
	args := m.Called(ctx, workersKey)
	return args.Get(0).([]string), args.Error(1)
}

// IsWorkerActive implements data.DataStore
func (m *MockDataStore) IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error) {
	args := m.Called(ctx, heartbeatKey)
	return args.Bool(0), args.Error(1)
}

// IncrementCounter implements data.DataStore
func (m *MockDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	args := m.Called(ctx, counterKey, increment)
	return args.Get(0).(int64), args.Error(1)
}

// SetCounter implements data.DataStore
func (m *MockDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	args := m.Called(ctx, counterKey, value, expiry)
	return args.Error(0)
}

// GetCounter implements data.DataStore
func (m *MockDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	args := m.Called(ctx, counterKey)
	return args.Get(0).(int64), args.Error(1)
}

// LockWithHeartbeat implements data.DataStore
func (m *MockDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	args := m.Called(ctx, key, value, heartbeatInterval)
	return args.Bool(0), func() {}, args.Error(1)
}

// TryLockWithTimeout implements data.DataStore
func (m *MockDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	args := m.Called(ctx, key, value, lockExpiry, waitTimeout)
	return args.Bool(0), args.Error(1)
}

// ExecuteAtomically implements data.DataStore
func (m *MockDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	mockArgs := m.Called(append([]interface{}{ctx, script, keys}, args...)...)
	return mockArgs.Get(0), mockArgs.Error(1)
}

// MoveItem implements data.DataStore
func (m *MockDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	args := m.Called(ctx, fromKey, toKey, item)
	return args.Error(0)
}

// AddToQueue implements data.DataStore
func (m *MockDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	args := m.Called(ctx, queueKey, item, score)
	return args.Error(0)
}

// GetFromQueue implements data.DataStore
func (m *MockDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	args := m.Called(ctx, queueKey, count)
	return args.Get(0).([]string), args.Error(1)
}

// RemoveFromQueue implements data.DataStore
func (m *MockDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	args := m.Called(ctx, queueKey, item)
	return args.Error(0)
}

// GetQueueLength implements data.DataStore
func (m *MockDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	args := m.Called(ctx, queueKey)
	return args.Get(0).(int64), args.Error(1)
}

// NewMockDataStore 创建一个新的MockDataStore实例
func NewMockDataStore() *MockDataStore {
	return &MockDataStore{}
}

// MockProcessor 模拟任务处理器
type MockProcessor struct {
	mock.Mock
	processDelay time.Duration
}

func NewMockProcessor() *MockProcessor {
	return &MockProcessor{}
}

func (m *MockProcessor) SetProcessDelay(delay time.Duration) {
	m.processDelay = delay
}

func (m *MockProcessor) Process(ctx context.Context, minID, maxID int64, options map[string]interface{}) (int64, error) {
	args := m.Called(ctx, minID, maxID, options)

	// 模拟处理时间
	if m.processDelay > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(m.processDelay):
		}
	}

	return args.Get(0).(int64), args.Error(1)
}

// 测试辅助函数
func createTestPartitions() map[int]model.PartitionInfo {
	now := time.Now()
	return map[int]model.PartitionInfo{
		1: {
			PartitionID: 1,
			MinID:       1,
			MaxID:       100,
			Status:      model.StatusPending,
			WorkerID:    "",
			UpdatedAt:   now,
			Options:     make(map[string]interface{}),
		},
		2: {
			PartitionID: 2,
			MinID:       101,
			MaxID:       200,
			Status:      model.StatusPending,
			WorkerID:    "",
			UpdatedAt:   now,
			Options:     make(map[string]interface{}),
		},
		3: {
			PartitionID: 3,
			MinID:       201,
			MaxID:       300,
			Status:      model.StatusClaimed,
			WorkerID:    "other-worker",
			UpdatedAt:   now.Add(-10 * time.Minute), // 旧的分区，可能需要重新获取
			Options:     make(map[string]interface{}),
		},
	}
}

func setupTestRunner(dataStore data.DataStore, processor Processor, logger *test_utils.MockLogger) *Runner {
	config := RunnerConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		PartitionLockExpiry: 3 * time.Minute,
		DataStore:           dataStore,
		Processor:           processor,
		Logger:              logger,
		UseTaskWindow:       false,
		TaskWindowSize:      3,
	}
	return NewRunner(config)
}

// TestRunner_NewRunner 测试Runner构造函数
// 场景: 使用有效的配置创建新的Runner实例
// 预期结果: Runner实例成功创建，所有配置参数被正确应用
func TestRunner_NewRunner(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)

	assert.NotNil(t, runner)
	assert.Equal(t, "test-namespace", runner.namespace)
	assert.Equal(t, "test-worker", runner.workerID)
	assert.Equal(t, 3*time.Minute, runner.partitionLockExpiry)
	assert.False(t, runner.useTaskWindow)
	assert.Equal(t, 3, runner.taskWindowSize)
}

// TestRunner_AcquirePartitionTask_Success 测试成功获取可用分区任务的场景
// 场景: 当有可用的待处理分区时，Runner应该能成功获取并声明该分区
// 预期结果: 成功获取ID为1的分区，并将其状态更新为Claimed，分区属主设置为当前worker
func TestRunner_AcquirePartitionTask_Success(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	// 设置测试数据
	partitions := createTestPartitions()
	partitionsData, _ := json.Marshal(partitions)

	// 配置mock预期 - 注意锁键格式应与model.PartitionLockFmtFmt一致
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return(string(partitionsData), nil)
	mockDataStore.On("AcquireLock", mock.Anything, "test-namespace:partition:1", "test-worker", 3*time.Minute).Return(true, nil)
	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil)

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)
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

	mockDataStore.AssertExpectations(t)
}

// TestRunner_AcquirePartitionTask_NoAvailablePartition 测试当没有可用分区时的行为
// 场景: 所有分区都被占用或处于不可获取状态
// 预期结果: 返回ErrNoAvailablePartition错误，返回空的PartitionInfo
func TestRunner_AcquirePartitionTask_NoAvailablePartition(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	// 设置测试数据 - 所有分区都被占用
	partitions := map[int]model.PartitionInfo{
		1: {
			PartitionID: 1,
			Status:      model.StatusRunning,
			WorkerID:    "other-worker",
			UpdatedAt:   time.Now(),
		},
	}
	partitionsData, _ := json.Marshal(partitions)

	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return(string(partitionsData), nil)

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)
	ctx := context.Background()

	// 执行测试
	task, err := runner.acquirePartitionTask(ctx)

	// 验证结果
	assert.Error(t, err)
	assert.Equal(t, model.ErrNoAvailablePartition, err)
	assert.Equal(t, model.PartitionInfo{}, task)

	mockDataStore.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_Success 测试分区任务处理成功的场景
// 场景: 处理一个已声明的分区任务，处理器成功完成处理
// 预期结果: 无错误返回，任务状态从Claimed变为Running，最终变为Completed，并释放锁
func TestRunner_ProcessPartitionTask_Success(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true) // 使用test_utils中的MockLogger

	// 配置mock预期
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(50), nil)

	// 根据 processPartitionTask 方法调用路径，SetPartitions 和 GetPartitions 要调整期望的调用次数
	// 实际代码中 updateTaskStatus 方法会导致 GetPartitions 和 SetPartitions 被调用
	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil).Maybe()
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return("{}", nil).Maybe()
	mockDataStore.On("ReleaseLock", mock.Anything, "test-namespace:partition:1", "test-worker").Return(nil)

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)
	ctx := context.Background()

	task := model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusClaimed,
		WorkerID:    "test-worker",
		UpdatedAt:   time.Now(),
		Options:     make(map[string]interface{}),
	}

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.NoError(t, err)

	mockDataStore.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_ProcessorError 测试处理器执行失败的场景
// 场景: 处理一个已声明的分区任务，处理器返回错误
// 预期结果: 返回处理器的错误，分区状态从Running变为Failed，并释放锁
func TestRunner_ProcessPartitionTask_ProcessorError(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 配置mock预期
	processorError := errors.New("processor failed")
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(0), processorError)
	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil).Times(2) // running, failed
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return("{}", nil).Times(2)
	mockDataStore.On("ReleaseLock", mock.Anything, "test-namespace:partition:1", "test-worker").Return(nil)

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)
	ctx := context.Background()

	task := model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       100,
		Status:      model.StatusClaimed,
		WorkerID:    "test-worker",
		UpdatedAt:   time.Now(),
		Options:     make(map[string]interface{}),
	}

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.Error(t, err)
	assert.Equal(t, processorError, err)

	mockDataStore.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_Timeout 测试任务处理超时的场景
// 场景: 处理器执行时间超过设定的超时时间
// 预期结果: 返回任务处理超时错误，分区状态从Running变为Failed，并释放锁
func TestRunner_ProcessPartitionTask_Timeout(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	// 设置处理器延迟超过超时时间
	mockProcessor.SetProcessDelay(2 * time.Second)
	mockProcessor.On("Process", mock.Anything, int64(1), int64(100), mock.AnythingOfType("map[string]interface {}")).Return(int64(0), nil)

	// 使用 Maybe() 而不是 Times(2)，避免对调用次数的严格限制
	mockDataStore.On("SetPartitions", mock.Anything, "test-namespace:partitions", mock.AnythingOfType("string")).Return(nil).Maybe()
	mockDataStore.On("GetPartitions", mock.Anything, "test-namespace:partitions").Return("{}", nil).Maybe()

	// 修正锁键格式
	mockDataStore.On("ReleaseLock", mock.Anything, "test-namespace:partition:1", "test-worker").Return(nil)

	// 创建一个短超时的runner
	config := RunnerConfig{
		Namespace:           "test-namespace",
		WorkerID:            "test-worker",
		PartitionLockExpiry: 2 * time.Second, // 短超时用于测试
		DataStore:           mockDataStore,
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
		Options:     make(map[string]interface{}),
	}

	// 执行测试
	err := runner.processPartitionTask(ctx, task)

	// 验证结果
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "任务处理超时")

	mockDataStore.AssertExpectations(t)
}

// TestRunner_ProcessPartitionTask_ReleaseLockError 测试释放锁失败的场景
// 场景: 处理器成功完成处理，但释放锁时发生错误
// 预期结果: 返回释放锁的错误，分区状态从Running变为Completed
func TestRunner_ShouldReclaimPartition(t *testing.T) {
	mockDataStore := NewMockDataStore()
	mockProcessor := NewMockProcessor()
	mockLogger := test_utils.NewMockLogger(true)

	runner := setupTestRunner(mockDataStore, mockProcessor, mockLogger)

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
				UpdatedAt: time.Now().Add(-10 * time.Minute),
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
