package leader

import (
	"context"
	"elk_coordinator/model"
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockDataStore 实现data.DataStore接口，用于测试
type mockDataStore struct {
	locks         map[string]string
	heartbeats    map[string]string
	partitionData map[string]string
	lockMutex     sync.RWMutex
}

func newMockDataStore() *mockDataStore {
	return &mockDataStore{
		locks:         make(map[string]string),
		heartbeats:    make(map[string]string),
		partitionData: make(map[string]string),
	}
}

// 实现AcquireLock方法
func (m *mockDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.lockMutex.Lock()
	defer m.lockMutex.Unlock()

	if _, exists := m.locks[key]; exists {
		return false, nil
	}

	m.locks[key] = value
	return true, nil
}

// 实现RenewLock方法
func (m *mockDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	m.lockMutex.RLock()
	defer m.lockMutex.RUnlock()

	currentValue, exists := m.locks[key]
	if !exists {
		return false, nil
	}

	return currentValue == value, nil
}

// 实现GetLockOwner方法
func (m *mockDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	m.lockMutex.RLock()
	defer m.lockMutex.RUnlock()

	value, exists := m.locks[key]
	if !exists {
		return "", fmt.Errorf("lock not found")
	}
	return value, nil
}

// 实现ReleaseLock方法
func (m *mockDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
	m.lockMutex.Lock()
	defer m.lockMutex.Unlock()

	currentValue, exists := m.locks[key]
	if !exists {
		return nil
	}

	if currentValue == value {
		delete(m.locks, key)
	}
	return nil
}

// 实现SetHeartbeat方法
func (m *mockDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	m.heartbeats[key] = value
	return nil
}

// 实现GetHeartbeat方法
func (m *mockDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	value, exists := m.heartbeats[key]
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

	for key := range m.heartbeats {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// 实现SetPartitions方法
func (m *mockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	m.partitionData[key] = value
	return nil
}

// 实现GetPartitions方法
func (m *mockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	value, exists := m.partitionData[key]
	if !exists {
		return "", nil
	}
	return value, nil
}

// 实现DeleteKey方法
func (m *mockDataStore) DeleteKey(ctx context.Context, key string) error {
	delete(m.heartbeats, key)
	return nil
}

// 模拟logger - 简单实现，不发送日志到输出
type mockLogger struct{}

func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}

// 其他DataStore接口方法的存根实现...
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
func (m *mockDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	return nil
}
func (m *mockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	return "", nil
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

// TestTryElect 测试选举方法
func TestTryElect(t *testing.T) {
	mockStore := newMockDataStore()

	election := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       5 * time.Second,
	})

	ctx := context.Background()

	// 第一次选举应该成功
	if success := election.TryElect(ctx); !success {
		t.Error("第一次选举失败，期望成功")
	}

	// 检查锁是否存在
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	owner, err := mockStore.GetLockOwner(ctx, leaderLockKey)
	if err != nil {
		t.Errorf("获取锁所有者失败: %v", err)
	}
	if owner != "node1" {
		t.Errorf("锁所有者不正确，期望 'node1'，得到 '%s'", owner)
	}

	// 创建第二个选举实例
	election2 := NewElection(ElectionConfig{
		NodeID:           "node2",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       5 * time.Second,
	})

	// 第二次选举应该失败（锁已被第一个节点获取）
	if success := election2.TryElect(ctx); success {
		t.Error("第二次选举成功，期望失败")
	}
}

// TestRenewLeaderLock 测试更新Leader锁
func TestRenewLeaderLock(t *testing.T) {
	mockStore := newMockDataStore()

	election := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       5 * time.Second,
	})

	ctx := context.Background()

	// 先获取锁
	if success := election.TryElect(ctx); !success {
		t.Error("选举失败，期望成功")
	}

	// 测试更新锁
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	if success := election.renewLeaderLock(leaderLockKey); !success {
		t.Error("更新锁失败，期望成功")
	}

	// 测试另一个节点尝试更新锁
	election2 := NewElection(ElectionConfig{
		NodeID:           "node2",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       5 * time.Second,
	})

	if success := election2.renewLeaderLock(leaderLockKey); success {
		t.Error("其他节点更新锁成功，期望失败")
	}
}

// TestGetLeaderInfo 测试获取Leader信息
func TestGetLeaderInfo(t *testing.T) {
	mockStore := newMockDataStore()

	election := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       5 * time.Second,
	})

	ctx := context.Background()

	// 先获取锁
	if success := election.TryElect(ctx); !success {
		t.Error("选举失败，期望成功")
	}

	// 获取Leader信息
	leaderID, err := election.GetLeaderInfo(ctx)
	if err != nil {
		t.Errorf("获取Leader信息失败: %v", err)
	}

	if leaderID != "node1" {
		t.Errorf("Leader ID不正确，期望 'node1'，得到 '%s'", leaderID)
	}
}

// TestStartRenewing 测试周期性更新锁
func TestStartRenewing(t *testing.T) {
	mockStore := newMockDataStore()

	election := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       1 * time.Second, // 使用短的过期时间便于测试
	})

	// 创建一个短暂的上下文，这会导致StartRenewing很快结束
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// 先获取锁
	if success := election.TryElect(ctx); !success {
		t.Error("选举失败，期望成功")
	}

	callbackCalled := false
	var wg sync.WaitGroup
	wg.Add(1)

	// 启动更新锁的goroutine
	go func() {
		defer wg.Done()
		election.StartRenewing(ctx, func() {
			callbackCalled = true
		})
	}()

	// 等待goroutine结束
	wg.Wait()

	// 在新的实现中，当上下文取消时回调函数会被调用
	if !callbackCalled {
		t.Error("回调函数未被调用，但应该被调用")
	}

	// 在新的实现中，锁会被主动释放，所以应该不存在或被其他人获取
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	_, err := mockStore.GetLockOwner(ctx, leaderLockKey)

	// 期望返回错误，表明锁已被释放
	if err == nil {
		t.Error("锁仍然存在，但应该已被释放")
	}
}

// TestConcurrentElection 测试多节点同时竞选的竞态场景
func TestConcurrentElection(t *testing.T) {
	mockStore := newMockDataStore()

	// 创建多个节点的选举实例
	nodeCount := 5
	elections := make([]*Election, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		elections[i] = NewElection(ElectionConfig{
			NodeID:           nodeID,
			Namespace:        "test",
			DataStore:        mockStore,
			Logger:           &mockLogger{},
			ElectionInterval: 1 * time.Second,
			LockExpiry:       5 * time.Second,
		})
	}

	ctx := context.Background()

	// 使用WaitGroup同步多个goroutine
	var wg sync.WaitGroup

	// 记录成功选举的节点ID
	var successfulNodeIDs []string
	var mu sync.Mutex // 保护successfulNodeIDs

	// 同时启动多个选举
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if elected := elections[idx].TryElect(ctx); elected {
				mu.Lock()
				successfulNodeIDs = append(successfulNodeIDs, elections[idx].config.NodeID)
				mu.Unlock()
			}
		}(i)
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 应该只有一个节点成功当选
	if len(successfulNodeIDs) != 1 {
		t.Errorf("竞态选举失败：期望只有1个节点成功，但有%d个节点成功: %v", len(successfulNodeIDs), successfulNodeIDs)
	}

	// 检查锁的状态
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	owner, err := mockStore.GetLockOwner(ctx, leaderLockKey)
	if err != nil {
		t.Errorf("获取锁所有者失败: %v", err)
	}

	if owner != successfulNodeIDs[0] {
		t.Errorf("锁所有者与成功选举的节点不匹配，锁所有者是 '%s'，成功节点是 '%s'", owner, successfulNodeIDs[0])
	}
}

// TestReElectionAfterExpiry 测试锁到期后重新选举
func TestReElectionAfterExpiry(t *testing.T) {
	mockStore := newMockDataStore()

	// 创建两个节点的选举实例
	election1 := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       2 * time.Second, // 使用短的锁过期时间来测试
	})

	election2 := NewElection(ElectionConfig{
		NodeID:           "node2",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 1 * time.Second,
		LockExpiry:       2 * time.Second,
	})

	ctx := context.Background()

	// 第一次选举 - node1应该成功
	if !election1.TryElect(ctx) {
		t.Error("node1初次选举失败")
	}

	// 验证node1是leader
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	owner, _ := mockStore.GetLockOwner(ctx, leaderLockKey)
	if owner != "node1" {
		t.Errorf("初次选举后锁所有者错误，期望'node1'，得到'%s'", owner)
	}

	// node2此时应该无法成功选举
	if election2.TryElect(ctx) {
		t.Error("node1持有锁期间，node2不应该能成功选举")
	}

	// 模拟锁过期：删除锁
	mockStore.lockMutex.Lock()
	delete(mockStore.locks, leaderLockKey)
	mockStore.lockMutex.Unlock()

	// 现在node2应该能够成功选举
	if !election2.TryElect(ctx) {
		t.Error("锁过期后node2选举失败")
	}

	// 验证node2现在是leader
	owner, _ = mockStore.GetLockOwner(ctx, leaderLockKey)
	if owner != "node2" {
		t.Errorf("锁过期重新选举后锁所有者错误，期望'node2'，得到'%s'", owner)
	}
}

// TestLeaderFailureAndRecovery 测试Leader故障恢复场景
func TestLeaderFailureAndRecovery(t *testing.T) {
	mockStore := newMockDataStore()

	// 创建两个节点
	node1 := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 100 * time.Millisecond,
		LockExpiry:       500 * time.Millisecond,
	})

	node2 := NewElection(ElectionConfig{
		NodeID:           "node2",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 100 * time.Millisecond,
		LockExpiry:       500 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// node1先成为leader
	if !node1.TryElect(ctx) {
		t.Error("node1初次选举失败")
	}

	// 启动node1的锁续期协程
	node1CallbackCalled := false
	var node1Done sync.WaitGroup
	node1Done.Add(1)

	go func() {
		defer node1Done.Done()
		node1Ctx, node1Cancel := context.WithCancel(context.Background())
		defer node1Cancel()

		// 启动续期任务，但很快停止
		time.AfterFunc(200*time.Millisecond, func() {
			node1Cancel() // 模拟node1故障
		})

		node1.StartRenewing(node1Ctx, func() {
			node1CallbackCalled = true
		})
	}()

	// 等待node1故障（续期停止）
	time.Sleep(300 * time.Millisecond)

	// 等待goroutine完成确保回调已经被调用
	node1Done.Wait()

	// 检验回调是否被调用（应该在上下文取消时调用）
	if !node1CallbackCalled {
		t.Error("node1故障后，回调函数未被调用")
	}

	// 等待锁过期
	time.Sleep(600 * time.Millisecond)

	// node2应该能成为新的leader
	electionResult := node2.TryElect(ctx)
	if !electionResult {
		t.Error("node1故障后，node2未能成为新leader")
	}

	// 验证node2确实是leader
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")
	owner, _ := mockStore.GetLockOwner(ctx, leaderLockKey)
	if owner != "node2" {
		t.Errorf("leader故障恢复后锁所有者错误，期望'node2'，得到'%s'", owner)
	}
}

// TestLockRenewalPrevention 测试锁被抢占后无法续期的场景
func TestLockRenewalPrevention(t *testing.T) {
	mockStore := newMockDataStore()
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")

	// 创建两个节点
	node1 := NewElection(ElectionConfig{
		NodeID:           "node1",
		Namespace:        "test",
		DataStore:        mockStore,
		Logger:           &mockLogger{},
		ElectionInterval: 100 * time.Millisecond,
		LockExpiry:       500 * time.Millisecond,
	})

	ctx := context.Background()

	// node1成为leader
	if !node1.TryElect(ctx) {
		t.Error("node1初次选举失败")
	}

	// 模拟锁被另一个节点获取（直接修改锁值）
	mockStore.lockMutex.Lock()
	mockStore.locks[leaderLockKey] = "node2" // 模拟锁被node2抢占
	mockStore.lockMutex.Unlock()

	// 尝试续期，应该失败
	renewalSuccess := node1.renewLeaderLock(leaderLockKey)
	if renewalSuccess {
		t.Error("锁被抢占后，node1仍然能续期，但预期应该失败")
	}
}

// TestMultipleElectionCycles 测试多次选举循环
func TestMultipleElectionCycles(t *testing.T) {
	mockStore := newMockDataStore()
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test")

	// 创建三个节点
	elections := make([]*Election, 3)
	nodeIDs := []string{"node1", "node2", "node3"}

	for i, nodeID := range nodeIDs {
		elections[i] = NewElection(ElectionConfig{
			NodeID:           nodeID,
			Namespace:        "test",
			DataStore:        mockStore,
			Logger:           &mockLogger{},
			ElectionInterval: 100 * time.Millisecond,
			LockExpiry:       500 * time.Millisecond,
		})
	}

	ctx := context.Background()

	// 进行多轮选举，每轮释放锁后再次选举
	for cycle := 0; cycle < 3; cycle++ {
		successfulNodeIdx := -1

		// 所有节点尝试选举
		for i, election := range elections {
			if election.TryElect(ctx) {
				successfulNodeIdx = i
				break
			}
		}

		if successfulNodeIdx == -1 {
			t.Errorf("周期%d: 没有节点选举成功", cycle)
			continue
		}

		// 验证选举结果
		owner, _ := mockStore.GetLockOwner(ctx, leaderLockKey)
		expectedOwner := nodeIDs[successfulNodeIdx]
		if owner != expectedOwner {
			t.Errorf("周期%d: 锁所有者错误，期望'%s'，得到'%s'", cycle, expectedOwner, owner)
		}

		// 释放锁，准备下一轮
		mockStore.lockMutex.Lock()
		delete(mockStore.locks, leaderLockKey)
		mockStore.lockMutex.Unlock()
	}
}
