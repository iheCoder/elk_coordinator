package test_utils

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockDataStore 实现data.DataStore接口，用于测试
// 注意：与leader/mock_test.go中的mockDataStore不同，这个可以被其他包使用
type MockDataStore struct {
	Locks         map[string]string
	Heartbeats    map[string]string
	PartitionData map[string]string
	LockMutex     sync.RWMutex
}

// NewMockDataStore 创建一个新的模拟数据存储实例
func NewMockDataStore() *MockDataStore {
	return &MockDataStore{
		Locks:         make(map[string]string),
		Heartbeats:    make(map[string]string),
		PartitionData: make(map[string]string),
	}
}

// 实现AcquireLock方法
func (m *MockDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.LockMutex.Lock()
	defer m.LockMutex.Unlock()

	if _, exists := m.Locks[key]; exists {
		return false, nil
	}

	m.Locks[key] = value
	return true, nil
}

// 实现RenewLock方法
func (m *MockDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	currentValue, exists := m.Locks[key]
	if !exists {
		return false, nil
	}

	return currentValue == value, nil
}

// 实现GetLockOwner方法
func (m *MockDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	value, exists := m.Locks[key]
	if !exists {
		return "", fmt.Errorf("lock not found")
	}
	return value, nil
}

// 实现ReleaseLock方法
func (m *MockDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
	m.LockMutex.Lock()
	defer m.LockMutex.Unlock()

	currentValue, exists := m.Locks[key]
	if !exists {
		return nil
	}

	if currentValue == value {
		delete(m.Locks, key)
	}
	return nil
}

// 实现SetHeartbeat方法
func (m *MockDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	m.Heartbeats[key] = value
	return nil
}

// 实现GetHeartbeat方法
func (m *MockDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	value, exists := m.Heartbeats[key]
	if !exists {
		return "", fmt.Errorf("heartbeat not found")
	}
	return value, nil
}

// 实现GetKeys方法
func (m *MockDataStore) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	// 简单实现，模拟模式匹配
	var keys []string
	prefix := pattern[:len(pattern)-1] // 去掉末尾的 '*'

	for key := range m.Heartbeats {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// 实现SetPartitions方法
func (m *MockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	m.PartitionData[key] = value
	return nil
}

// 实现GetPartitions方法
func (m *MockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	value, exists := m.PartitionData[key]
	if !exists {
		return "", nil
	}
	return value, nil
}

// 实现DeleteKey方法
func (m *MockDataStore) DeleteKey(ctx context.Context, key string) error {
	delete(m.Heartbeats, key)
	return nil
}

// 实现SetKey方法
func (m *MockDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	return nil
}

// 实现GetKey方法
func (m *MockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	return "", nil
}

// 其他DataStore接口方法的存根实现
func (m *MockDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	return false, nil
}

func (m *MockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (m *MockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockDataStore) RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error {
	return nil
}

func (m *MockDataStore) UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error {
	return nil
}

func (m *MockDataStore) GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error) {
	return nil, nil
}

func (m *MockDataStore) IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error) {
	return false, nil
}

func (m *MockDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	return 0, nil
}

func (m *MockDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	return nil
}

func (m *MockDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	return 0, nil
}

func (m *MockDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	return false, nil, nil
}

func (m *MockDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	return false, nil
}

func (m *MockDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

func (m *MockDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	return nil
}

func (m *MockDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	return nil
}

func (m *MockDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	return nil, nil
}

func (m *MockDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	return nil
}

func (m *MockDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	return 0, nil
}
