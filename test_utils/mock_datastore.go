package test_utils

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iheCoder/elk_coordinator/model"

	"github.com/iheCoder/elk_coordinator/data"
)

// MockDataStore 实现data.DataStore接口，用于测试
// 注意：与leader/mock_test.go中的mockDataStore不同，这个可以被其他包使用
type MockDataStore struct {
	Locks          map[string]string
	Heartbeats     map[string]string
	PartitionData  map[string]string
	Commands       map[string]string // 命令存储，独立于分区数据
	LockMutex      sync.RWMutex
	HeartbeatMutex sync.RWMutex // 保护心跳数据的并发访问
	PartitionMutex sync.RWMutex // 保护分区数据的并发访问
	CommandMutex   sync.RWMutex // 保护命令数据的并发访问
}

// NewMockDataStore 创建一个新的模拟数据存储实例
func NewMockDataStore() *MockDataStore {
	return &MockDataStore{
		Locks:         make(map[string]string),
		Heartbeats:    make(map[string]string),
		PartitionData: make(map[string]string),
		Commands:      make(map[string]string),
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

// 实现SetWorkerHeartbeat方法
func (m *MockDataStore) SetWorkerHeartbeat(ctx context.Context, workerID string, value string) error {
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()
	m.Heartbeats[workerID] = value
	return nil
}

// 实现RefreshWorkerHeartbeat方法
func (m *MockDataStore) RefreshWorkerHeartbeat(ctx context.Context, workerID string) error {
	// 在mock中，只是简单返回nil，实际实现中会刷新过期时间
	return nil
}

// 实现GetWorkerHeartbeat方法
func (m *MockDataStore) GetWorkerHeartbeat(ctx context.Context, workerID string) (string, error) {
	m.HeartbeatMutex.RLock()
	defer m.HeartbeatMutex.RUnlock()
	value, exists := m.Heartbeats[workerID]
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

	m.HeartbeatMutex.RLock()
	defer m.HeartbeatMutex.RUnlock()

	for key := range m.Heartbeats {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// 实现SetPartitions方法
func (m *MockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	m.PartitionMutex.Lock()
	defer m.PartitionMutex.Unlock()
	m.PartitionData[key] = value
	return nil
}

// 实现GetPartitions方法
func (m *MockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	m.PartitionMutex.RLock()
	defer m.PartitionMutex.RUnlock()
	value, exists := m.PartitionData[key]
	if !exists {
		return "", nil
	}
	return value, nil
}

// 实现DeleteKey方法
func (m *MockDataStore) DeleteKey(ctx context.Context, key string) error {
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()
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

// 同步状态数据 - 使用线程安全的访问
var syncStatusData = make(map[string]string)
var syncStatusMutex sync.RWMutex

// 实现GetSyncStatus方法
func (m *MockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	syncStatusMutex.RLock()
	defer syncStatusMutex.RUnlock()

	value, exists := syncStatusData[key]
	if !exists {
		return "", fmt.Errorf("sync status not found for key: %s", key)
	}

	return value, nil
}

// 实现SetSyncStatus方法
func (m *MockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	syncStatusMutex.Lock()
	defer syncStatusMutex.Unlock()
	syncStatusData[key] = value
	return nil
}

// 工作节点数据 - 使用线程安全的访问
var workersData = make(map[string]map[string]bool) // 存储 workersKey -> {workerID: true} 的映射
var workersDataMutex sync.RWMutex

// 实现RegisterWorker方法
func (m *MockDataStore) RegisterWorker(ctx context.Context, workerID string) error {
	workersKey := "workers"
	heartbeatKey := fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, workerID)

	// 确保map已初始化
	if _, exists := workersData[workersKey]; !exists {
		workersData[workersKey] = make(map[string]bool)
	}

	// 注册工作节点
	workersData[workersKey][workerID] = true

	// 设置心跳（使用线程安全方式）
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()
	m.Heartbeats[heartbeatKey] = time.Now().Format(time.RFC3339)

	return nil
}

// 实现UnregisterWorker方法
func (m *MockDataStore) UnregisterWorker(ctx context.Context, workerID string) error {
	heartbeatKey := fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, workerID)

	// 删除心跳（使用线程安全方式）- 保留workers集合中的记录
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()
	delete(m.Heartbeats, heartbeatKey)

	return nil
}

// 实现GetActiveWorkers方法
func (m *MockDataStore) GetActiveWorkers(ctx context.Context) ([]string, error) {
	// 通过心跳存在性判断活跃工作节点
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()

	var activeWorkers []string
	heartbeatPrefix := model.HeartbeatKeyPrefix + ":"
	now := time.Now()

	// 遍历所有心跳，检查是否过期
	keysToDelete := make([]string, 0)

	for heartbeatKey, heartbeatValue := range m.Heartbeats {
		if strings.HasPrefix(heartbeatKey, heartbeatPrefix) {
			workerID := strings.TrimPrefix(heartbeatKey, heartbeatPrefix)
			if workerID != "" {
				// 解析心跳时间
				heartbeatTime, err := time.Parse(time.RFC3339, heartbeatValue)
				if err != nil {
					// 如果解析失败，认为心跳无效，删除它
					keysToDelete = append(keysToDelete, heartbeatKey)
					continue
				}

				// 检查是否过期（使用30秒作为默认过期时间，符合测试预期）
				if now.Sub(heartbeatTime) > 30*time.Second {
					// 过期，标记删除
					keysToDelete = append(keysToDelete, heartbeatKey)
				} else {
					// 未过期，添加到活跃列表
					activeWorkers = append(activeWorkers, workerID)
				}
			}
		}
	}

	// 删除过期的心跳
	for _, key := range keysToDelete {
		delete(m.Heartbeats, key)
	}

	return activeWorkers, nil
}

// 实现GetAllWorkers方法
func (m *MockDataStore) GetAllWorkers(ctx context.Context) ([]*model.WorkerInfo, error) {
	workersKey := "workers"
	workers, exists := workersData[workersKey]
	if !exists {
		return []*model.WorkerInfo{}, nil
	}

	// 将map的keys转为WorkerInfo slice
	allWorkerInfos := make([]*model.WorkerInfo, 0, len(workers))
	for workerID := range workers {
		// 创建WorkerInfo，使用当前时间作为注册时间
		workerInfo := &model.WorkerInfo{
			WorkerID:     workerID,
			RegisterTime: time.Now(),
			StopTime:     nil, // Mock中假设所有worker都是活跃的
		}
		allWorkerInfos = append(allWorkerInfos, workerInfo)
	}

	return allWorkerInfos, nil
}

// 实现IsWorkerActive方法
func (m *MockDataStore) IsWorkerActive(ctx context.Context, workerID string) (bool, error) {
	heartbeatKey := fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, workerID)
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()

	heartbeatValue, exists := m.Heartbeats[heartbeatKey]
	if !exists {
		return false, nil
	}

	// 解析心跳时间并检查是否过期
	heartbeatTime, err := time.Parse(time.RFC3339, heartbeatValue)
	if err != nil {
		// 如果解析失败，认为心跳无效，删除它
		delete(m.Heartbeats, heartbeatKey)
		return false, nil
	}

	// 检查是否过期（使用30秒作为默认过期时间，符合测试预期）
	now := time.Now()
	if now.Sub(heartbeatTime) > 30*time.Second {
		// 过期，删除心跳
		delete(m.Heartbeats, heartbeatKey)
		return false, nil
	}

	// 未过期，worker是活跃的
	return true, nil
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

// Hash分区数据存储
var hashPartitionsData = make(map[string]map[string]string) // key -> field -> value
var hashPartitionVersions = make(map[string]int64)          // field -> version
var hashPartitionMutex sync.RWMutex

// 实现HSetPartition方法
func (m *MockDataStore) HSetPartition(ctx context.Context, key, field, value string) error {
	hashPartitionMutex.Lock()
	defer hashPartitionMutex.Unlock()

	if hashPartitionsData[key] == nil {
		hashPartitionsData[key] = make(map[string]string)
	}
	hashPartitionsData[key][field] = value
	return nil
}

// 实现HGetPartition方法
func (m *MockDataStore) HGetPartition(ctx context.Context, key, field string) (string, error) {
	hashPartitionMutex.RLock()
	defer hashPartitionMutex.RUnlock()

	if hashPartitionsData[key] == nil {
		return "", data.ErrNotFound
	}

	value, exists := hashPartitionsData[key][field]
	if !exists {
		return "", data.ErrNotFound
	}

	return value, nil
}

// 实现HGetAllPartitions方法
func (m *MockDataStore) HGetAllPartitions(ctx context.Context, key string) (map[string]string, error) {
	hashPartitionMutex.RLock()
	defer hashPartitionMutex.RUnlock()

	result := make(map[string]string)
	if hashPartitionsData[key] != nil {
		for field, value := range hashPartitionsData[key] {
			result[field] = value
		}
	}

	return result, nil
}

// 实现HDeletePartition方法
func (m *MockDataStore) HDeletePartition(ctx context.Context, key, field string) error {
	hashPartitionMutex.Lock()
	defer hashPartitionMutex.Unlock()

	if hashPartitionsData[key] != nil {
		delete(hashPartitionsData[key], field)
		delete(hashPartitionVersions, field)
		if len(hashPartitionsData[key]) == 0 {
			delete(hashPartitionsData, key)
		}
	}

	return nil
}

// 实现HUpdatePartitionWithVersion方法
func (m *MockDataStore) HUpdatePartitionWithVersion(ctx context.Context, key, field, value string, expectedVersion int64) (bool, error) {
	hashPartitionMutex.Lock()
	defer hashPartitionMutex.Unlock()

	currentVersion := hashPartitionVersions[field]

	// 创建新分区（expectedVersion 为 0）
	if expectedVersion == 0 {
		if currentVersion != 0 {
			return false, fmt.Errorf("partition already exists")
		}
		// 创建新分区
		if hashPartitionsData[key] == nil {
			hashPartitionsData[key] = make(map[string]string)
		}
		hashPartitionsData[key][field] = value
		hashPartitionVersions[field] = 1
		return true, nil
	}

	// 更新现有分区
	if currentVersion != expectedVersion {
		return false, fmt.Errorf("optimistic lock failed: version mismatch")
	}

	if hashPartitionsData[key] == nil {
		hashPartitionsData[key] = make(map[string]string)
	}
	hashPartitionsData[key][field] = value
	hashPartitionVersions[field] = expectedVersion + 1
	return true, nil
}

// 实现HSetPartitionsInTx方法
func (m *MockDataStore) HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error {
	hashPartitionMutex.Lock()
	defer hashPartitionMutex.Unlock()

	if hashPartitionsData[key] == nil {
		hashPartitionsData[key] = make(map[string]string)
	}

	// 模拟事务批量设置
	for field, value := range partitions {
		hashPartitionsData[key][field] = value
		// 如果没有版本，设置为1，否则保持当前版本
		if hashPartitionVersions[field] == 0 {
			hashPartitionVersions[field] = 1
		}
	}

	return nil
}

// 实现HSetPartitionsWithStatsInTx方法
func (m *MockDataStore) HSetPartitionsWithStatsInTx(ctx context.Context, partitionKey string, statsKey string, partitions map[string]string, stats *model.PartitionStats) error {
	hashPartitionMutex.Lock()
	defer hashPartitionMutex.Unlock()

	// 参数验证
	if stats == nil {
		return fmt.Errorf("stats参数不能为nil")
	}

	// 初始化分区数据
	if hashPartitionsData[partitionKey] == nil {
		hashPartitionsData[partitionKey] = make(map[string]string)
	}

	// 初始化统计数据
	if partitionStatsData[statsKey] == nil {
		partitionStatsData[statsKey] = make(map[string]string)
		// 初始化默认统计值
		partitionStatsData[statsKey]["total"] = "0"
		partitionStatsData[statsKey]["pending"] = "0"
		partitionStatsData[statsKey]["processing"] = "0"
		partitionStatsData[statsKey]["completed"] = "0"
		partitionStatsData[statsKey]["failed"] = "0"
		partitionStatsData[statsKey]["max_partition_id"] = "0"
		partitionStatsData[statsKey]["last_allocated_id"] = "0"
	}

	// 模拟原子性操作：同时设置分区和更新统计
	for field, value := range partitions {
		// 设置分区数据
		hashPartitionsData[partitionKey][field] = value

		// 如果没有版本，设置为1，否则保持当前版本
		if hashPartitionVersions[field] == 0 {
			hashPartitionVersions[field] = 1
		}
	}

	// 使用预计算的统计数据更新max_partition_id
	if stats.MaxPartitionID > 0 {
		currentMaxPartitionID, _ := strconv.ParseInt(partitionStatsData[statsKey]["max_partition_id"], 10, 64)
		if int64(stats.MaxPartitionID) > currentMaxPartitionID {
			partitionStatsData[statsKey]["max_partition_id"] = strconv.Itoa(stats.MaxPartitionID)
		}
	}

	// 使用预计算的统计数据更新last_allocated_id
	if stats.LastAllocatedID > 0 {
		currentLastAllocatedID, _ := strconv.ParseInt(partitionStatsData[statsKey]["last_allocated_id"], 10, 64)
		if stats.LastAllocatedID > currentLastAllocatedID {
			partitionStatsData[statsKey]["last_allocated_id"] = strconv.FormatInt(stats.LastAllocatedID, 10)
		}
	}

	return nil
}

// CommandOperations implementation for MockDataStore

// SubmitCommand 提交命令到模拟存储
func (m *MockDataStore) SubmitCommand(ctx context.Context, namespace string, command interface{}) error {
	m.CommandMutex.Lock()
	defer m.CommandMutex.Unlock()

	// 使用独立的Commands存储，每个namespace对应一个命令列表（用JSON数组存储）
	commandKey := fmt.Sprintf("commands:%s", namespace)

	// 将命令序列化为JSON字符串
	bytes, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}
	commandStr := string(bytes)

	if m.Commands[commandKey] == "" {
		// 第一个命令，创建JSON数组
		m.Commands[commandKey] = fmt.Sprintf("[%s]", commandStr)
	} else {
		// 添加到现有数组中
		existing := m.Commands[commandKey]
		// 移除末尾的 ]，添加新命令，再加上 ]
		m.Commands[commandKey] = existing[:len(existing)-1] + "," + commandStr + "]"
	}
	return nil
}

// GetPendingCommands 获取待处理的命令列表
func (m *MockDataStore) GetPendingCommands(ctx context.Context, namespace string, limit int) ([]string, error) {
	m.CommandMutex.RLock()
	defer m.CommandMutex.RUnlock()

	commandKey := fmt.Sprintf("commands:%s", namespace)
	commandsStr := m.Commands[commandKey]
	if commandsStr == "" {
		return []string{}, nil
	}

	// 解析JSON数组，命令可能是JSON对象，需要使用json.RawMessage
	var rawCommands []json.RawMessage
	if err := json.Unmarshal([]byte(commandsStr), &rawCommands); err != nil {
		return []string{}, fmt.Errorf("failed to unmarshal commands JSON: %w", err)
	}

	// 将每个原始JSON消息转换为字符串
	commands := make([]string, len(rawCommands))
	for i, raw := range rawCommands {
		commands[i] = string(raw)
	}

	// 应用limit
	if limit > 0 && len(commands) > limit {
		commands = commands[:limit]
	}

	return commands, nil
}

// DeleteCommand 删除命令
func (m *MockDataStore) DeleteCommand(ctx context.Context, namespace, commandID string) error {
	m.CommandMutex.Lock()
	defer m.CommandMutex.Unlock()

	commandKey := fmt.Sprintf("commands:%s", namespace)
	commandsStr := m.Commands[commandKey]
	if commandsStr == "" {
		return nil // 没有命令可删除
	}

	// 解析JSON数组
	var rawCommands []json.RawMessage
	if err := json.Unmarshal([]byte(commandsStr), &rawCommands); err != nil {
		return err
	}

	// 查找并删除匹配的命令
	var newCommands []json.RawMessage
	for _, raw := range rawCommands {
		cmdStr := string(raw)
		if cmdStr != commandID {
			newCommands = append(newCommands, raw)
		}
	}

	// 更新存储
	if len(newCommands) == 0 {
		m.Commands[commandKey] = ""
	} else {
		newCommandsBytes, err := json.Marshal(newCommands)
		if err != nil {
			return err
		}
		m.Commands[commandKey] = string(newCommandsBytes)
	}

	return nil
}

// 分区统计数据存储
var partitionStatsData = make(map[string]map[string]string) // statsKey -> stats
var partitionStatsMutex sync.RWMutex

// PartitionStatsOperations implementation for MockDataStore

// InitPartitionStats 初始化分区统计数据
func (m *MockDataStore) InitPartitionStats(ctx context.Context, statsKey string) error {
	partitionStatsMutex.Lock()
	defer partitionStatsMutex.Unlock()

	// 初始化统计数据
	partitionStatsData[statsKey] = map[string]string{
		"total":        "0",
		"pending":      "0",
		"running":      "0",
		"completed":    "0",
		"failed":       "0",
		"archived":     "0",
		"last_updated": fmt.Sprintf("%d", time.Now().Unix()),
	}
	return nil
}

// GetPartitionStatsData 获取统计数据（原子操作）
func (m *MockDataStore) GetPartitionStatsData(ctx context.Context, statsKey string) (map[string]string, error) {
	partitionStatsMutex.RLock()
	defer partitionStatsMutex.RUnlock()

	stats, exists := partitionStatsData[statsKey]
	if !exists {
		return nil, fmt.Errorf("partition stats not found for key: %s", statsKey)
	}

	// 返回统计数据的副本
	result := make(map[string]string)
	for k, v := range stats {
		result[k] = v
	}
	return result, nil
}

// UpdatePartitionStatsOnCreate 创建分区时更新统计
func (m *MockDataStore) UpdatePartitionStatsOnCreate(ctx context.Context, statsKey string, partitionID int, dataID int64) error {
	partitionStatsMutex.Lock()
	defer partitionStatsMutex.Unlock()

	stats, exists := partitionStatsData[statsKey]
	if !exists {
		// 如果统计数据不存在，先初始化
		stats = map[string]string{
			"total":        "0",
			"pending":      "0",
			"running":      "0",
			"completed":    "0",
			"failed":       "0",
			"archived":     "0",
			"last_updated": fmt.Sprintf("%d", time.Now().Unix()),
		}
		partitionStatsData[statsKey] = stats
	}

	// 增加总数和待处理数
	total, _ := strconv.Atoi(stats["total"])
	pending, _ := strconv.Atoi(stats["pending"])

	stats["total"] = strconv.Itoa(total + 1)
	stats["pending"] = strconv.Itoa(pending + 1)
	stats["last_updated"] = fmt.Sprintf("%d", time.Now().Unix())

	return nil
}

// UpdatePartitionStatsOnStatusChange 状态变更时更新统计
func (m *MockDataStore) UpdatePartitionStatsOnStatusChange(ctx context.Context, statsKey string, oldStatus, newStatus string) error {
	partitionStatsMutex.Lock()
	defer partitionStatsMutex.Unlock()

	stats, exists := partitionStatsData[statsKey]
	if !exists {
		return fmt.Errorf("partition stats not found for key: %s", statsKey)
	}

	// 减少旧状态计数
	if oldCount, err := strconv.Atoi(stats[oldStatus]); err == nil && oldCount > 0 {
		stats[oldStatus] = strconv.Itoa(oldCount - 1)
	}

	// 增加新状态计数
	if newCount, err := strconv.Atoi(stats[newStatus]); err == nil {
		stats[newStatus] = strconv.Itoa(newCount + 1)
	}

	stats["last_updated"] = fmt.Sprintf("%d", time.Now().Unix())
	return nil
}

// UpdatePartitionStatsOnDelete 删除分区时更新统计
func (m *MockDataStore) UpdatePartitionStatsOnDelete(ctx context.Context, statsKey string, status string) error {
	partitionStatsMutex.Lock()
	defer partitionStatsMutex.Unlock()

	stats, exists := partitionStatsData[statsKey]
	if !exists {
		return fmt.Errorf("partition stats not found for key: %s", statsKey)
	}

	// 减少总数和相应状态的计数
	if total, err := strconv.Atoi(stats["total"]); err == nil && total > 0 {
		stats["total"] = strconv.Itoa(total - 1)
	}

	if statusCount, err := strconv.Atoi(stats[status]); err == nil && statusCount > 0 {
		stats[status] = strconv.Itoa(statusCount - 1)
	}

	stats["last_updated"] = fmt.Sprintf("%d", time.Now().Unix())
	return nil
}

// RebuildPartitionStats 重建统计数据（从现有分区数据）
func (m *MockDataStore) RebuildPartitionStats(ctx context.Context, statsKey string, activePartitionsKey, archivedPartitionsKey string) error {
	partitionStatsMutex.Lock()
	defer partitionStatsMutex.Unlock()

	// 初始化统计数据
	stats := map[string]string{
		"total":        "0",
		"pending":      "0",
		"running":      "0",
		"completed":    "0",
		"failed":       "0",
		"archived":     "0",
		"last_updated": fmt.Sprintf("%d", time.Now().Unix()),
	}

	// 统计活跃分区
	hashPartitionMutex.RLock()
	if activePartitions, exists := hashPartitionsData[activePartitionsKey]; exists {
		for _, partitionData := range activePartitions {
			// 解析分区数据获取状态
			var partitionInfo map[string]interface{}
			if err := json.Unmarshal([]byte(partitionData), &partitionInfo); err == nil {
				if status, ok := partitionInfo["status"].(string); ok {
					if count, err := strconv.Atoi(stats[status]); err == nil {
						stats[status] = strconv.Itoa(count + 1)
					}
					if total, err := strconv.Atoi(stats["total"]); err == nil {
						stats["total"] = strconv.Itoa(total + 1)
					}
				}
			}
		}
	}

	// 统计归档分区
	if archivedPartitions, exists := hashPartitionsData[archivedPartitionsKey]; exists {
		archivedCount := len(archivedPartitions)
		if archived, err := strconv.Atoi(stats["archived"]); err == nil {
			stats["archived"] = strconv.Itoa(archived + archivedCount)
		}
		if total, err := strconv.Atoi(stats["total"]); err == nil {
			stats["total"] = strconv.Itoa(total + archivedCount)
		}
	}
	hashPartitionMutex.RUnlock()

	partitionStatsData[statsKey] = stats
	return nil
}

// 实现RefreshHeartbeat方法
func (m *MockDataStore) RefreshHeartbeat(ctx context.Context, key string) error {
	m.HeartbeatMutex.Lock()
	defer m.HeartbeatMutex.Unlock()
	// 如果心跳存在，就刷新它（不改变值）
	if _, exists := m.Heartbeats[key]; exists {
		// 在mock中，我们只需要确保key还存在即可
		return nil
	}
	return fmt.Errorf("heartbeat not found")
}
