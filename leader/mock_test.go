package leader

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// mockDataStore 实现data.DataStore接口，用于测试
type mockDataStore struct {
	Locks         map[string]string
	Heartbeats    map[string]string
	PartitionData map[string]string
	LockMutex     sync.RWMutex
}

// newMockDataStore 创建一个新的模拟数据存储实例
func newMockDataStore() *mockDataStore {
	return &mockDataStore{
		Locks:         make(map[string]string),
		Heartbeats:    make(map[string]string),
		PartitionData: make(map[string]string),
	}
}

// 实现AcquireLock方法
func (m *mockDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.LockMutex.Lock()
	defer m.LockMutex.Unlock()

	if _, exists := m.Locks[key]; exists {
		return false, nil
	}

	m.Locks[key] = value
	return true, nil
}

// 实现RenewLock方法
func (m *mockDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	currentValue, exists := m.Locks[key]
	if !exists {
		return false, nil
	}

	return currentValue == value, nil
}

// 实现GetLockOwner方法
func (m *mockDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	value, exists := m.Locks[key]
	if !exists {
		return "", fmt.Errorf("lock not found")
	}
	return value, nil
}

// 实现ReleaseLock方法
func (m *mockDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
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
func (m *mockDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	m.Heartbeats[key] = value
	return nil
}

// 实现GetHeartbeat方法
func (m *mockDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	value, exists := m.Heartbeats[key]
	if !exists {
		return "", fmt.Errorf("heartbeat not found")
	}
	return value, nil
}

// 实现GetKeys方法
func (m *mockDataStore) GetKeys(ctx context.Context, pattern string) ([]string, error) {
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
func (m *mockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	m.PartitionData[key] = value
	return nil
}

// 实现GetPartitions方法
func (m *mockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	value, exists := m.PartitionData[key]
	if !exists {
		return "", nil
	}
	return value, nil
}

// 实现DeleteKey方法
func (m *mockDataStore) DeleteKey(ctx context.Context, key string) error {
	delete(m.Heartbeats, key)
	return nil
}

// 实现SetKey方法
func (m *mockDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	return nil
}

// 实现GetKey方法
func (m *mockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	return "", nil
}

// 其他DataStore接口方法的存根实现
func (m *mockDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	return false, nil
}

func (m *mockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (m *mockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	return nil
}

func (m *mockDataStore) RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error {
	return nil
}

func (m *mockDataStore) UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error {
	return nil
}

func (m *mockDataStore) GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error) {
	return nil, nil
}

func (m *mockDataStore) IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error) {
	return false, nil
}

func (m *mockDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	return 0, nil
}

func (m *mockDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	return nil
}

func (m *mockDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	return 0, nil
}

func (m *mockDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	return false, nil, nil
}

func (m *mockDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	return false, nil
}

func (m *mockDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

func (m *mockDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	return nil
}

func (m *mockDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	return nil
}

func (m *mockDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	return nil, nil
}

func (m *mockDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	return nil
}

func (m *mockDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	return 0, nil
}

// mockLogger 简单实现，不发送日志到输出
type mockLogger struct{}

func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}
