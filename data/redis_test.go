package data

import (
	"context"
	"encoding/json"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// setupRedisTest 创建一个测试用的 RedisDataStore 实例
func setupRedisTest(t *testing.T) (*RedisDataStore, *miniredis.Miniredis, func()) {
	// 创建一个 miniredis 实例
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("无法启动 miniredis: %v", err)
	}

	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// 测试连接
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("无法连接到 Redis: %v", err)
	}

	// 自定义选项，使用较短的过期时间以加快测试
	opts := &Options{
		KeyPrefix:     "test:",
		DefaultExpiry: 5 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}

	// 创建 RedisDataStore 实例
	store := NewRedisDataStore(client, opts)

	// 返回清理函数
	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return store, mr, cleanup
}

func TestRedisDataStore_prefixKey(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	key := "test_key"
	prefixedKey := store.prefixKey(key)
	expected := "test:" + key

	assert.Equal(t, expected, prefixedKey)
}

func TestRedisDataStore_AcquireLock(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "lock_key"
	value := "lock_value"
	expiry := 1 * time.Second

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value, expiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 再次尝试获取同一个锁应该失败
	acquired, err = store.AcquireLock(ctx, key, "another_value", expiry)
	assert.NoError(t, err)
	assert.False(t, acquired)
}

func TestRedisDataStore_RenewLock(t *testing.T) {
	store, mr, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "lock_key"
	value := "lock_value"
	expiry := 1 * time.Second

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value, expiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 更新锁
	renewed, err := store.RenewLock(ctx, key, value, 2*time.Second)
	assert.NoError(t, err)
	assert.True(t, renewed)

	// 使用错误的值更新锁
	renewed, err = store.RenewLock(ctx, key, "wrong_value", expiry)
	assert.NoError(t, err)
	assert.False(t, renewed)

	// 验证过期时间已延长
	// miniredis 允许我们检查 TTL
	ttl := mr.TTL(store.prefixKey(key))
	assert.Greater(t, ttl, 500*time.Millisecond) // 不精确检查，只确认延长了
}

func TestRedisDataStore_CheckLock(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "lock_key"
	value := "lock_value"
	expiry := 1 * time.Second

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value, expiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 检查锁
	owned, err := store.CheckLock(ctx, key, value)
	assert.NoError(t, err)
	assert.True(t, owned)

	// 检查锁（错误的值）
	owned, err = store.CheckLock(ctx, key, "wrong_value")
	assert.NoError(t, err)
	assert.False(t, owned)

	// 检查不存在的锁
	owned, err = store.CheckLock(ctx, "non_existent_key", value)
	assert.NoError(t, err)
	assert.False(t, owned)
}

func TestRedisDataStore_ReleaseLock(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "lock_key"
	value := "lock_value"
	expiry := 1 * time.Second

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value, expiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 释放锁
	err = store.ReleaseLock(ctx, key, value)
	assert.NoError(t, err)

	// 确认锁已释放
	exists, err := store.CheckLock(ctx, key, value)
	assert.NoError(t, err)
	assert.False(t, exists)

	// 尝试释放一个不存在的锁
	err = store.ReleaseLock(ctx, "non_existent_key", value)
	assert.NoError(t, err) // 应该不报错，只是没有实际操作
}

func TestRedisDataStore_GetLockOwner(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "lock_key"
	value := "lock_value"
	expiry := 1 * time.Second

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value, expiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 获取锁的拥有者
	owner, err := store.GetLockOwner(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, owner)

	// 尝试获取不存在的锁的拥有者
	owner, err = store.GetLockOwner(ctx, "non_existent_key")
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

func TestRedisDataStore_Heartbeat(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "heartbeat_key"
	value := "heartbeat_value"

	// 设置心跳
	err := store.SetHeartbeat(ctx, key, value)
	assert.NoError(t, err)

	// 获取心跳
	gotValue, err := store.GetHeartbeat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)

	// 尝试获取不存在的心跳
	_, err = store.GetHeartbeat(ctx, "non_existent_heartbeat")
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

func TestRedisDataStore_GetKeys(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()

	// 设置多个键
	keys := []string{"key1", "key2", "other_key", "key3"}
	for _, key := range keys {
		err := store.SetHeartbeat(ctx, key, "value")
		assert.NoError(t, err)
	}

	// 获取匹配的键
	matchingKeys, err := store.GetKeys(ctx, "key*")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"key1", "key2", "key3"}, matchingKeys)
}

func TestRedisDataStore_DeleteKey(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "key_to_delete"
	value := "value"

	// 设置一个键
	err := store.SetHeartbeat(ctx, key, value)
	assert.NoError(t, err)

	// 确认键存在
	gotValue, err := store.GetHeartbeat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)

	// 删除键
	err = store.DeleteKey(ctx, key)
	assert.NoError(t, err)

	// 确认键已删除
	_, err = store.GetHeartbeat(ctx, key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

func TestRedisDataStore_Partitions(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "partitions"
	value := `{"partition1": {"data": "value1"}, "partition2": {"data": "value2"}}`

	// 设置分区
	err := store.SetPartitions(ctx, key, value)
	assert.NoError(t, err)

	// 获取分区
	gotValue, err := store.GetPartitions(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestRedisDataStore_SyncStatus(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "sync_status"
	value := `{"status": "running", "progress": 50}`

	// 设置同步状态
	err := store.SetSyncStatus(ctx, key, value)
	assert.NoError(t, err)

	// 获取同步状态
	gotValue, err := store.GetSyncStatus(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestRedisDataStore_Worker(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	workersKey := "workers"
	workerID := "worker1"
	heartbeatKey := "worker_heartbeat"
	heartbeatValue := time.Now().Format(time.RFC3339)

	// 注册工作节点
	err := store.RegisterWorker(ctx, workersKey, workerID, heartbeatKey, heartbeatValue)
	assert.NoError(t, err)

	// 确认工作节点已注册
	workers, err := store.GetActiveWorkers(ctx, workersKey)
	assert.NoError(t, err)
	assert.Contains(t, workers, workerID)

	// 确认心跳存在
	active, err := store.IsWorkerActive(ctx, heartbeatKey)
	assert.NoError(t, err)
	assert.True(t, active)

	// 注销工作节点
	err = store.UnregisterWorker(ctx, workersKey, workerID, heartbeatKey)
	assert.NoError(t, err)

	// 确认工作节点已注销
	workers, err = store.GetActiveWorkers(ctx, workersKey)
	assert.NoError(t, err)
	assert.NotContains(t, workers, workerID)

	// 确认心跳已删除
	active, err = store.IsWorkerActive(ctx, heartbeatKey)
	assert.NoError(t, err)
	assert.False(t, active)
}

func TestRedisDataStore_Counter(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	counterKey := "counter"

	// 设置计数器
	err := store.SetCounter(ctx, counterKey, 10, 1*time.Minute)
	assert.NoError(t, err)

	// 获取计数器
	value, err := store.GetCounter(ctx, counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	// 增加计数器
	newValue, err := store.IncrementCounter(ctx, counterKey, 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), newValue)

	// 再次获取计数器确认
	value, err = store.GetCounter(ctx, counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), value)
}

func TestRedisDataStore_LockWithHeartbeat(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "heartbeat_lock"
	value := "lock_value"
	heartbeatInterval := 100 * time.Millisecond

	// 获取带心跳的锁
	acquired, cancel, err := store.LockWithHeartbeat(ctx, key, value, heartbeatInterval)
	assert.NoError(t, err)
	assert.True(t, acquired)
	assert.NotNil(t, cancel)

	// 等待一会儿，确认锁仍然持有
	time.Sleep(heartbeatInterval * 2)
	owned, err := store.CheckLock(ctx, key, value)
	assert.NoError(t, err)
	assert.True(t, owned)

	// 取消心跳
	cancel()

	// 等待一会儿，确认锁最终释放
	time.Sleep(heartbeatInterval * 4)
	owned, err = store.CheckLock(ctx, key, value)
	assert.NoError(t, err)
	assert.False(t, owned)
}

func TestRedisDataStore_TryLockWithTimeout(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "timeout_lock"
	value1 := "lock_value1"
	value2 := "lock_value2"
	lockExpiry := 300 * time.Millisecond
	waitTimeout := 200 * time.Millisecond

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value1, lockExpiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 尝试用超时获取已被持有的锁
	acquired, err = store.TryLockWithTimeout(ctx, key, value2, lockExpiry, waitTimeout)
	assert.Error(t, err) // 应该超时
	assert.False(t, acquired)
	assert.Contains(t, err.Error(), "timeout")

	// 等待锁过期
	time.Sleep(lockExpiry + 100*time.Millisecond)

	// 现在应该可以获取锁
	acquired, err = store.TryLockWithTimeout(ctx, key, value2, lockExpiry, waitTimeout)
	assert.NoError(t, err)
	assert.True(t, acquired)
}

func TestRedisDataStore_ExecuteAtomically(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()

	// 设置初始值
	err := store.SetCounter(ctx, "atomic_counter", 10, 1*time.Minute)
	assert.NoError(t, err)

	// 执行原子脚本
	script := `
		local current = redis.call("GET", KEYS[1])
		current = tonumber(current)
		local newValue = current + tonumber(ARGV[1])
		redis.call("SET", KEYS[1], newValue)
		return newValue
	`
	result, err := store.ExecuteAtomically(ctx, script, []string{"atomic_counter"}, 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)

	// 确认值已更新
	value, err := store.GetCounter(ctx, "atomic_counter")
	assert.NoError(t, err)
	assert.Equal(t, int64(15), value)
}

func TestRedisDataStore_MoveItem(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	fromKey := "from_queue"
	toKey := "to_queue"
	item := "test_item"

	// 添加到源队列
	err := store.AddToQueue(ctx, fromKey, item, 1.0)
	assert.NoError(t, err)

	// 移动项目
	err = store.MoveItem(ctx, fromKey, toKey, item)
	assert.NoError(t, err)

	// 确认项目已移动
	fromItems, err := store.GetFromQueue(ctx, fromKey, 10)
	assert.NoError(t, err)
	assert.NotContains(t, fromItems, item)

	toItems, err := store.GetFromQueue(ctx, toKey, 10)
	assert.NoError(t, err)
	assert.Contains(t, toItems, item)
}

func TestRedisDataStore_Queue(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	queueKey := "test_queue"

	// 添加项目到队列
	items := []string{"item1", "item2", "item3"}
	for i, item := range items {
		err := store.AddToQueue(ctx, queueKey, item, float64(i))
		assert.NoError(t, err)
	}

	// 获取队列长度
	length, err := store.GetQueueLength(ctx, queueKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// 获取队列项目
	queueItems, err := store.GetFromQueue(ctx, queueKey, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2"}, queueItems)

	// 移除队列项目
	err = store.RemoveFromQueue(ctx, queueKey, "item2")
	assert.NoError(t, err)

	// 确认项目已移除
	length, err = store.GetQueueLength(ctx, queueKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	queueItems, err = store.GetFromQueue(ctx, queueKey, 10)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"item1", "item3"}, queueItems)
}

// 测试模拟实际场景的综合测试案例
func TestRedisDataStore_IntegrationScenario(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()

	// 模拟分布式工作节点场景
	type PartitionStatus struct {
		ID     int    `json:"id"`
		Status string `json:"status"`
		Worker string `json:"worker"`
	}

	// 1. 两个工作节点注册
	err := store.RegisterWorker(ctx, "workers", "worker1", "heartbeat:worker1", time.Now().Format(time.RFC3339))
	assert.NoError(t, err)

	err = store.RegisterWorker(ctx, "workers", "worker2", "heartbeat:worker2", time.Now().Format(time.RFC3339))
	assert.NoError(t, err)

	// 2. 检查工作节点已注册
	workers, err := store.GetActiveWorkers(ctx, "workers")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"worker1", "worker2"}, workers)

	// 3. worker1 获取分区1的锁
	acquired, err := store.AcquireLock(ctx, "partition:1", "worker1", 5*time.Second)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 4. worker2 尝试获取同一分区的锁，应该失败
	acquired, err = store.AcquireLock(ctx, "partition:1", "worker2", 5*time.Second)
	assert.NoError(t, err)
	assert.False(t, acquired)

	// 5. worker2 获取分区2的锁
	acquired, err = store.AcquireLock(ctx, "partition:2", "worker2", 5*time.Second)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 6. 更新分区状态
	partitions := map[int]PartitionStatus{
		1: {ID: 1, Status: "running", Worker: "worker1"},
		2: {ID: 2, Status: "running", Worker: "worker2"},
	}

	partitionsData, err := json.Marshal(partitions)
	assert.NoError(t, err)

	err = store.SetPartitions(ctx, "partitions", string(partitionsData))
	assert.NoError(t, err)

	// 7. 读取分区状态
	partitionsStr, err := store.GetPartitions(ctx, "partitions")
	assert.NoError(t, err)

	var retrievedPartitions map[int]PartitionStatus
	err = json.Unmarshal([]byte(partitionsStr), &retrievedPartitions)
	assert.NoError(t, err)

	assert.Equal(t, "worker1", retrievedPartitions[1].Worker)
	assert.Equal(t, "worker2", retrievedPartitions[2].Worker)

	// 8. worker1 完成任务，更新状态并释放锁
	// 修复：先获取结构体，修改后再放回 map
	partition1 := retrievedPartitions[1]
	partition1.Status = "completed"
	retrievedPartitions[1] = partition1

	partitionsData, err = json.Marshal(retrievedPartitions)
	assert.NoError(t, err)

	err = store.SetPartitions(ctx, "partitions", string(partitionsData))
	assert.NoError(t, err)

	err = store.ReleaseLock(ctx, "partition:1", "worker1")
	assert.NoError(t, err)

	// 9. worker1 注销
	err = store.UnregisterWorker(ctx, "workers", "worker1", "heartbeat:worker1")
	assert.NoError(t, err)

	// 10. 检查最终状态
	workers, err = store.GetActiveWorkers(ctx, "workers")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"worker2"}, workers)

	partitionsStr, err = store.GetPartitions(ctx, "partitions")
	assert.NoError(t, err)

	err = json.Unmarshal([]byte(partitionsStr), &retrievedPartitions)
	assert.NoError(t, err)
	assert.Equal(t, "completed", retrievedPartitions[1].Status)
	assert.Equal(t, "running", retrievedPartitions[2].Status)
}
