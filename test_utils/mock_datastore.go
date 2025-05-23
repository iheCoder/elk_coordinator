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
	m.LockMutex.Lock()
	defer m.LockMutex.Unlock()

	m.Locks[key] = value
	return nil
}

// 实现GetKey方法
func (m *MockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	value, exists := m.Locks[key]
	if !exists {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

// 实现CheckLock方法
func (m *MockDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	m.LockMutex.RLock()
	defer m.LockMutex.RUnlock()

	value, exists := m.Locks[key]
	if !exists {
		return false, nil
	}

	return value == expectedValue, nil
}

// 同步状态数据
var syncStatusData = make(map[string]string)

// 实现GetSyncStatus方法
func (m *MockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	value, exists := syncStatusData[key]
	if !exists {
		return "", fmt.Errorf("sync status not found for key: %s", key)
	}

	return value, nil
}

// 实现SetSyncStatus方法
func (m *MockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	syncStatusData[key] = value
	return nil
}

// 工作节点数据
var workersData = make(map[string]map[string]bool) // 存储 workersKey -> {workerID: true} 的映射

// 实现RegisterWorker方法
func (m *MockDataStore) RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error {
	// 确保map已初始化
	if _, exists := workersData[workersKey]; !exists {
		workersData[workersKey] = make(map[string]bool)
	}

	// 注册工作节点
	workersData[workersKey][workerID] = true

	// 设置心跳
	m.Heartbeats[heartbeatKey] = heartbeatValue

	return nil
}

// 实现UnregisterWorker方法
func (m *MockDataStore) UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error {
	// 从工作节点列表中删除
	if workers, exists := workersData[workersKey]; exists {
		delete(workers, workerID)
	}

	// 删除心跳
	delete(m.Heartbeats, heartbeatKey)

	return nil
}

// 实现GetActiveWorkers方法
func (m *MockDataStore) GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error) {
	workers, exists := workersData[workersKey]
	if !exists {
		return []string{}, nil
	}

	// 将map的keys转为slice
	activeWorkers := make([]string, 0, len(workers))
	for workerID := range workers {
		activeWorkers = append(activeWorkers, workerID)
	}

	return activeWorkers, nil
}

// 实现IsWorkerActive方法
func (m *MockDataStore) IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error) {
	_, exists := m.Heartbeats[heartbeatKey]
	return exists, nil
}

// 计数器数据
var countersData = make(map[string]int64)

// 实现IncrementCounter方法
func (m *MockDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	currentValue, exists := countersData[counterKey]
	if !exists {
		currentValue = 0
	}

	newValue := currentValue + increment
	countersData[counterKey] = newValue

	return newValue, nil
}

// 实现SetCounter方法
func (m *MockDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	countersData[counterKey] = value
	return nil
}

// 实现GetCounter方法
func (m *MockDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	value, exists := countersData[counterKey]
	if !exists {
		return 0, fmt.Errorf("counter not found: %s", counterKey)
	}
	return value, nil
}

// 实现LockWithHeartbeat方法
func (m *MockDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	m.LockMutex.Lock()
	defer m.LockMutex.Unlock()

	// 检查锁是否已被占用
	if _, exists := m.Locks[key]; exists {
		return false, nil, nil
	}

	// 获取锁
	m.Locks[key] = value

	// 创建一个取消函数，用于停止心跳并释放锁
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	// 启动后台goroutine更新心跳
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 更新锁的过期时间，模拟心跳
				m.LockMutex.Lock()
				if currentValue, exists := m.Locks[key]; exists && currentValue == value {
					// 在实际应用中这里会更新过期时间，但在mock中我们只需保持锁存在即可
				}
				m.LockMutex.Unlock()
			case <-cancelCtx.Done():
				// 清理锁
				m.LockMutex.Lock()
				if currentValue, exists := m.Locks[key]; exists && currentValue == value {
					delete(m.Locks, key)
				}
				m.LockMutex.Unlock()
				return
			}
		}
	}()

	return true, cancelFunc, nil
}

// 实现TryLockWithTimeout方法
func (m *MockDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	// 创建一个定时器，在waitTimeout后超时
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	// 每隔一小段时间尝试获取锁
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// 上下文被取消
			return false, ctx.Err()
		case <-timer.C:
			// 等待超时
			return false, nil
		case <-ticker.C:
			// 尝试获取锁
			m.LockMutex.Lock()
			if _, exists := m.Locks[key]; !exists {
				// 锁不存在，可以获取
				m.Locks[key] = value
				m.LockMutex.Unlock()
				return true, nil
			}
			m.LockMutex.Unlock()
		}
	}
}

// 实现ExecuteAtomically方法
func (m *MockDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	// 这是一个模拟原子执行脚本的简化实现
	// 在实际应用中，这通常是由Redis的EVAL命令实现的

	// 为了测试目的，我们简单返回一个固定值
	// 具体的行为应根据测试需要进行定制
	return "ok", nil
}

// 队列数据 - 使用map存储队列项，键为queueKey，值为带有优先级(score)的项集合
type queueItem struct {
	Value interface{}
	Score float64
}

var queuesData = make(map[string][]queueItem)

// 实现MoveItem方法
func (m *MockDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	// 从源队列中查找并移除项
	fromQueue, exists := queuesData[fromKey]
	if !exists {
		return fmt.Errorf("source queue not found: %s", fromKey)
	}

	found := false
	var newFromQueue []queueItem
	var movedItem queueItem

	for _, qItem := range fromQueue {
		if fmt.Sprintf("%v", qItem.Value) == fmt.Sprintf("%v", item) {
			found = true
			movedItem = qItem
		} else {
			newFromQueue = append(newFromQueue, qItem)
		}
	}

	if !found {
		return fmt.Errorf("item not found in source queue")
	}

	// 更新源队列
	queuesData[fromKey] = newFromQueue

	// 将项添加到目标队列
	toQueue, exists := queuesData[toKey]
	if !exists {
		toQueue = []queueItem{}
	}
	toQueue = append(toQueue, movedItem)
	queuesData[toKey] = toQueue

	return nil
}

// 实现AddToQueue方法
func (m *MockDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	queue, exists := queuesData[queueKey]
	if !exists {
		queue = []queueItem{}
	}

	// 添加新项目到队列
	queue = append(queue, queueItem{
		Value: item,
		Score: score,
	})

	// 按score排序（升序）
	for i := 0; i < len(queue)-1; i++ {
		for j := i + 1; j < len(queue); j++ {
			if queue[i].Score > queue[j].Score {
				queue[i], queue[j] = queue[j], queue[i]
			}
		}
	}

	queuesData[queueKey] = queue
	return nil
}

// 实现GetFromQueue方法
func (m *MockDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	queue, exists := queuesData[queueKey]
	if !exists || len(queue) == 0 {
		return []string{}, nil
	}

	// 确定要返回的元素数量
	returnCount := int64(len(queue))
	if count < returnCount {
		returnCount = count
	}

	// 获取队列中按优先级排序的前N个元素
	result := make([]string, 0, returnCount)
	for i := int64(0); i < returnCount; i++ {
		result = append(result, fmt.Sprintf("%v", queue[i].Value))
	}

	return result, nil
}

// 实现RemoveFromQueue方法
func (m *MockDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	queue, exists := queuesData[queueKey]
	if !exists {
		return nil // 队列不存在视为成功
	}

	// 查找并移除匹配项
	itemStr := fmt.Sprintf("%v", item)
	var newQueue []queueItem

	for _, qItem := range queue {
		if fmt.Sprintf("%v", qItem.Value) != itemStr {
			newQueue = append(newQueue, qItem)
		}
	}

	queuesData[queueKey] = newQueue
	return nil
}

// 实现GetQueueLength方法
func (m *MockDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	queue, exists := queuesData[queueKey]
	if !exists {
		return 0, nil
	}

	return int64(len(queue)), nil
}
