package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	store, mr, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "timeout_lock"
	value1 := "lock_value1"
	value2 := "lock_value2"
	lockExpiry := 500 * time.Millisecond
	waitTimeout := 300 * time.Millisecond

	// 获取锁
	acquired, err := store.AcquireLock(ctx, key, value1, lockExpiry)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// 尝试用超时获取已被持有的锁
	acquired, err = store.TryLockWithTimeout(ctx, key, value2, lockExpiry, waitTimeout)
	assert.Error(t, err) // 应该超时
	assert.False(t, acquired)
	assert.Contains(t, err.Error(), "timeout")

	// 等待锁过期，添加更多缓冲时间
	time.Sleep(lockExpiry + 200*time.Millisecond)

	// 手动触发miniredis的过期清理
	mr.FastForward(time.Second)

	// 验证锁确实已经过期
	exists := mr.Exists(store.prefixKey(key))
	assert.False(t, exists, "锁应该已经过期并被删除")

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

// TestHashPartitionOperations 测试Hash分区操作
func TestHashPartitionOperations(t *testing.T) {
	ds, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:partitions"

	// 测试 HSetPartition
	err := ds.HSetPartition(ctx, key, "1", `{"id":1,"status":"pending"}`)
	assert.NoError(t, err)

	// 测试 HGetPartition
	val, err := ds.HGetPartition(ctx, key, "1")
	assert.NoError(t, err)
	assert.Equal(t, `{"id":1,"status":"pending"}`, val)

	// 测试 HSetPartitionsInTx
	partitions := map[string]string{
		"2": `{"id":2,"status":"running"}`,
		"3": `{"id":3,"status":"completed"}`,
	}
	err = ds.HSetPartitionsInTx(ctx, key, partitions)
	assert.NoError(t, err)

	// 测试 HGetAllPartitions
	allPartitions, err := ds.HGetAllPartitions(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(allPartitions))
	assert.Equal(t, `{"id":1,"status":"pending"}`, allPartitions["1"])
	assert.Equal(t, `{"id":2,"status":"running"}`, allPartitions["2"])
	assert.Equal(t, `{"id":3,"status":"completed"}`, allPartitions["3"])

	// 测试 HDeletePartition
	err = ds.HDeletePartition(ctx, key, "3")
	assert.NoError(t, err)

	// 验证删除结果
	allPartitions, err = ds.HGetAllPartitions(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allPartitions))
	assert.Equal(t, `{"id":1,"status":"pending"}`, allPartitions["1"])
	assert.Equal(t, `{"id":2,"status":"running"}`, allPartitions["2"])

	// 测试 HUpdatePartitionWithVersion
	// 使用一个新的字段来测试版本控制
	newFieldPartition := `{"id":4,"status":"new","version":1}`

	// 首先创建新字段（期望版本0，表示字段不存在）
	success, err := ds.HUpdatePartitionWithVersion(ctx, key, "4", newFieldPartition, 0)
	assert.NoError(t, err)
	assert.True(t, success)

	// 现在测试版本控制更新
	updatedPartition := `{"id":4,"status":"updated","version":2}`
	success, err = ds.HUpdatePartitionWithVersion(ctx, key, "4", updatedPartition, 1)
	assert.NoError(t, err)
	assert.True(t, success)

	// 尝试使用旧版本更新（应该失败）
	partitionWithOldVersion := `{"id":4,"status":"failed","version":3}`
	success, err = ds.HUpdatePartitionWithVersion(ctx, key, "4", partitionWithOldVersion, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrOptimisticLockFailed, err)
	assert.False(t, success)

	// 验证版本控制有效
	val, err = ds.HGetPartition(ctx, key, "4")
	assert.NoError(t, err)
	assert.Equal(t, updatedPartition, val) // 应该仍然是版本2的数据
}

// TestRedisDataStore_SetKey 测试SetKey方法
func TestRedisDataStore_SetKey(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test_set_key"
	value := "test_value"
	expiry := 1 * time.Minute

	// 设置键值
	err := store.SetKey(ctx, key, value, expiry)
	assert.NoError(t, err)

	// 获取键值验证
	val, err := store.GetKey(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)
}

// TestRedisDataStore_HUpdatePartitionWithVersionErrors 测试乐观锁更新的各种错误情况
func TestRedisDataStore_HUpdatePartitionWithVersionErrors(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:partition:errors"

	// 测试场景1: 创建新分区
	partitionNew := `{"id":1,"status":"new","version":1}`
	success, err := store.HUpdatePartitionWithVersion(ctx, key, "1", partitionNew, 0) // 0表示预期字段不存在
	assert.NoError(t, err)
	assert.True(t, success)

	// 测试场景2: 尝试创建一个已存在的分区（应该失败）
	partitionDuplicate := `{"id":1,"status":"duplicate","version":1}`
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "1", partitionDuplicate, 0)
	assert.Error(t, err)
	assert.Equal(t, ErrPartitionAlreadyExists, err)
	assert.False(t, success)

	// 测试场景3: 尝试更新不存在的分区
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "nonexistent", `{"id":99,"status":"missing","version":1}`, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrOptimisticLockFailed, err)
	assert.False(t, success)

	// 测试场景4: 正常更新，版本号递增
	partitionUpdated := `{"id":1,"status":"updated","version":2}`
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "1", partitionUpdated, 1)
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证更新后的值
	val, err := store.HGetPartition(ctx, key, "1")
	assert.NoError(t, err)
	assert.Equal(t, partitionUpdated, val)
}

// TestHashPartitionOperations_ConcurrentAccess 测试Hash分区操作的并发访问
func TestHashPartitionOperations_ConcurrentAccess(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:concurrent:partitions"
	numWorkers := 5
	numPartitions := 10

	// 初始化分区
	for i := 1; i <= numPartitions; i++ {
		partition := fmt.Sprintf(`{"id":%d,"status":"pending","version":1,"worker_id":""}`, i)
		err := store.HSetPartition(ctx, key, strconv.Itoa(i), partition)
		assert.NoError(t, err)
	}

	// 并发读取测试
	t.Run("ConcurrentReads", func(t *testing.T) {
		var wg sync.WaitGroup
		errChan := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 1; j <= numPartitions; j++ {
					_, err := store.HGetPartition(ctx, key, strconv.Itoa(j))
					if err != nil {
						errChan <- fmt.Errorf("worker %d 读取分区 %d 失败: %v", workerID, j, err)
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// 检查是否有错误
		for err := range errChan {
			t.Error(err)
		}
	})

	// 并发获取所有分区测试
	t.Run("ConcurrentGetAll", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				allPartitions, err := store.HGetAllPartitions(ctx, key)
				if err != nil {
					errors <- fmt.Errorf("worker %d 获取所有分区失败: %v", workerID, err)
					return
				}
				if len(allPartitions) != numPartitions {
					errors <- fmt.Errorf("worker %d 期望 %d 个分区，实际获得 %d 个", workerID, numPartitions, len(allPartitions))
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Error(err)
		}
	})
}

// TestHashPartitionOperations_OptimisticLockingConflicts 测试乐观锁并发冲突
func TestHashPartitionOperations_OptimisticLockingConflicts(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:optimistic:partitions"
	partitionID := "1"

	// 初始化分区
	initialPartition := `{"id":1,"status":"pending","version":1,"worker_id":""}`
	err := store.HSetPartition(ctx, key, partitionID, initialPartition)
	assert.NoError(t, err)

	// 测试多个工作节点同时尝试更新同一分区
	t.Run("ConcurrentOptimisticUpdates", func(t *testing.T) {
		numWorkers := 5
		var wg sync.WaitGroup
		successCount := int32(0)
		failureCount := int32(0)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// 尝试更新分区状态
				updatedPartition := fmt.Sprintf(`{"id":1,"status":"running","version":2,"worker_id":"worker_%d"}`, workerID)
				success, err := store.HUpdatePartitionWithVersion(ctx, key, partitionID, updatedPartition, 1)

				if err != nil {
					// 如果是乐观锁失败，这是预期的
					if errors.Is(err, ErrOptimisticLockFailed) {
						atomic.AddInt32(&failureCount, 1)
					} else {
						t.Errorf("worker %d 遇到意外错误: %v", workerID, err)
					}
				} else if success {
					atomic.AddInt32(&successCount, 1)
				} else {
					atomic.AddInt32(&failureCount, 1)
				}
			}(i)
		}

		wg.Wait()

		// 只有一个工作节点应该成功
		assert.Equal(t, int32(1), successCount, "应该只有一个工作节点成功更新")
		assert.Equal(t, int32(numWorkers-1), failureCount, "其他工作节点应该失败")

		// 验证最终状态
		finalPartition, err := store.HGetPartition(ctx, key, partitionID)
		assert.NoError(t, err)
		assert.Contains(t, finalPartition, `"version":2`)
		assert.Contains(t, finalPartition, `"status":"running"`)
	})
}

// TestHashPartitionOperations_ConcurrentTransactions 测试并发事务操作
func TestHashPartitionOperations_ConcurrentTransactions(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:tx:partitions"
	numWorkers := 3

	// 测试并发批量设置分区
	t.Run("ConcurrentBatchSet", func(t *testing.T) {
		var wg sync.WaitGroup
		errChan := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// 每个工作节点设置不同的分区集合
				partitions := make(map[string]string)
				start := workerID * 10
				for j := 0; j < 5; j++ {
					partitionID := strconv.Itoa(start + j)
					partition := fmt.Sprintf(`{"id":%d,"status":"pending","version":1,"worker_id":"worker_%d"}`, start+j, workerID)
					partitions[partitionID] = partition
				}

				err := store.HSetPartitionsInTx(ctx, key, partitions)
				if err != nil {
					errChan <- fmt.Errorf("worker %d 批量设置失败: %v", workerID, err)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			t.Error(err)
		}

		// 验证所有分区都被正确设置
		allPartitions, err := store.HGetAllPartitions(ctx, key)
		assert.NoError(t, err)
		expectedCount := numWorkers * 5
		assert.Equal(t, expectedCount, len(allPartitions))
	})
}

// TestHashPartitionOperations_PartitionContention 测试分区竞争场景
func TestHashPartitionOperations_PartitionContention(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:contention:partitions"
	numWorkers := 10
	numPartitions := 5

	// 初始化分区
	for i := 1; i <= numPartitions; i++ {
		partition := fmt.Sprintf(`{"id":%d,"status":"pending","version":1,"worker_id":""}`, i)
		err := store.HSetPartition(ctx, key, strconv.Itoa(i), partition)
		assert.NoError(t, err)
	}

	// 模拟多个工作节点竞争获取分区
	t.Run("WorkerPartitionContention", func(t *testing.T) {
		var wg sync.WaitGroup
		claimedPartitions := sync.Map{}

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// 每个工作节点尝试获取一个分区
				for partitionID := 1; partitionID <= numPartitions; partitionID++ {
					pid := strconv.Itoa(partitionID)

					// 读取当前分区状态
					currentPartition, err := store.HGetPartition(ctx, key, pid)
					if err != nil {
						continue
					}

					// 解析当前版本
					var partitionData map[string]interface{}
					if err := json.Unmarshal([]byte(currentPartition), &partitionData); err != nil {
						continue
					}

					// 检查分区是否可用
					status, _ := partitionData["status"].(string)
					if status != "pending" {
						continue
					}

					version, _ := partitionData["version"].(float64)
					currentVersion := int64(version)

					// 尝试声明分区
					claimedPartition := fmt.Sprintf(`{"id":%d,"status":"claimed","version":%d,"worker_id":"worker_%d"}`,
						partitionID, currentVersion+1, workerID)

					success, err := store.HUpdatePartitionWithVersion(ctx, key, pid, claimedPartition, currentVersion)
					if err == nil && success {
						claimedPartitions.Store(fmt.Sprintf("worker_%d", workerID), partitionID)
						break // 成功获取一个分区后退出
					}
				}
			}(i)
		}

		wg.Wait()

		// 统计成功获取分区的工作节点数量
		claimedCount := 0
		claimedPartitions.Range(func(key, value interface{}) bool {
			claimedCount++
			t.Logf("工作节点 %s 成功获取分区 %v", key, value)
			return true
		})

		// 最多只能有 numPartitions 个工作节点成功获取分区
		assert.LessOrEqual(t, claimedCount, numPartitions, "成功获取分区的工作节点数量不应超过分区总数")

		// 验证没有分区被重复分配
		partitionAssignments := make(map[int]string)
		claimedPartitions.Range(func(workerKey, partitionValue interface{}) bool {
			worker := workerKey.(string)
			partition := partitionValue.(int)

			if existingWorker, exists := partitionAssignments[partition]; exists {
				t.Errorf("分区 %d 被重复分配给工作节点 %s 和 %s", partition, existingWorker, worker)
			}
			partitionAssignments[partition] = worker
			return true
		})
	})
}

// TestHashPartitionOperations_VersionProgression 测试版本号递进的并发安全性
func TestHashPartitionOperations_VersionProgression(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:version:partitions"
	partitionID := "1"
	numUpdates := 20

	// 初始化分区
	initialPartition := `{"id":1,"status":"pending","version":1,"worker_id":"initial"}`
	err := store.HSetPartition(ctx, key, partitionID, initialPartition)
	assert.NoError(t, err)

	// 串行更新测试版本递进
	t.Run("SerialVersionProgression", func(t *testing.T) {
		currentVersion := int64(1)

		for i := 0; i < numUpdates; i++ {
			updatedPartition := fmt.Sprintf(`{"id":1,"status":"updating","version":%d,"worker_id":"updater_%d"}`,
				currentVersion+1, i)

			success, err := store.HUpdatePartitionWithVersion(ctx, key, partitionID, updatedPartition, currentVersion)
			assert.NoError(t, err)
			assert.True(t, success, "更新 %d 应该成功", i)

			currentVersion++
		}

		// 验证最终版本
		finalPartition, err := store.HGetPartition(ctx, key, partitionID)
		assert.NoError(t, err)
		assert.Contains(t, finalPartition, fmt.Sprintf(`"version":%d`, currentVersion))
	})

	// 并发更新测试 - 模拟版本冲突处理
	t.Run("ConcurrentVersionConflicts", func(t *testing.T) {
		// 重置分区状态
		resetPartition := `{"id":1,"status":"pending","version":1,"worker_id":""}`
		err := store.HSetPartition(ctx, key, partitionID, resetPartition)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		numConcurrentWorkers := 10
		successfulUpdates := int32(0)

		for i := 0; i < numConcurrentWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				maxRetries := 5
				for retry := 0; retry < maxRetries; retry++ {
					// 读取当前分区状态
					currentPartition, err := store.HGetPartition(ctx, key, partitionID)
					if err != nil {
						continue
					}

					// 解析版本
					var partitionData map[string]interface{}
					if err := json.Unmarshal([]byte(currentPartition), &partitionData); err != nil {
						continue
					}

					version, _ := partitionData["version"].(float64)
					currentVersion := int64(version)

					// 尝试更新
					updatedPartition := fmt.Sprintf(`{"id":1,"status":"updated","version":%d,"worker_id":"worker_%d"}`,
						currentVersion+1, workerID)

					success, err := store.HUpdatePartitionWithVersion(ctx, key, partitionID, updatedPartition, currentVersion)
					if err == nil && success {
						atomic.AddInt32(&successfulUpdates, 1)
						break
					}

					// 如果失败，短暂等待后重试
					time.Sleep(time.Millisecond * time.Duration(retry+1))
				}
			}(i)
		}

		wg.Wait()

		// 至少应该有一些成功的更新
		assert.Greater(t, successfulUpdates, int32(0), "应该至少有一个成功的更新")
		t.Logf("成功更新次数: %d / %d", successfulUpdates, numConcurrentWorkers)
	})
}

// TestHashPartitionOperations_ConcurrentDeleteAndUpdate 测试并发删除和更新操作
func TestHashPartitionOperations_ConcurrentDeleteAndUpdate(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:delete:partitions"
	numPartitions := 10

	// 初始化分区
	for i := 1; i <= numPartitions; i++ {
		partition := fmt.Sprintf(`{"id":%d,"status":"pending","version":1,"worker_id":""}`, i)
		err := store.HSetPartition(ctx, key, strconv.Itoa(i), partition)
		assert.NoError(t, err)
	}

	t.Run("ConcurrentDeleteAndUpdate", func(t *testing.T) {
		var wg sync.WaitGroup
		errChan := make(chan error, numPartitions*2)

		// 启动删除操作的协程
		for i := 1; i <= numPartitions/2; i++ {
			wg.Add(1)
			go func(partitionID int) {
				defer wg.Done()

				// 随机延迟以增加并发冲突概率
				time.Sleep(time.Millisecond * time.Duration(partitionID))

				err := store.HDeletePartition(ctx, key, strconv.Itoa(partitionID))
				if err != nil {
					errChan <- fmt.Errorf("删除分区 %d 失败: %v", partitionID, err)
				}
			}(i)
		}

		// 启动更新操作的协程
		for i := 1; i <= numPartitions; i++ {
			wg.Add(1)
			go func(partitionID int) {
				defer wg.Done()

				// 随机延迟
				time.Sleep(time.Millisecond * time.Duration(partitionID))

				updatedPartition := fmt.Sprintf(`{"id":%d,"status":"updated","version":2,"worker_id":"updater"}`, partitionID)
				success, err := store.HUpdatePartitionWithVersion(ctx, key, strconv.Itoa(partitionID), updatedPartition, 1)

				// 对于已删除的分区，更新应该失败
				if partitionID <= numPartitions/2 {
					// 这些分区可能已被删除，更新失败是正常的
					if err != nil && !errors.Is(err, ErrOptimisticLockFailed) {
						errChan <- fmt.Errorf("分区 %d 更新遇到意外错误: %v", partitionID, err)
					}
				} else {
					// 这些分区不应该被删除，更新应该成功
					if err != nil {
						errChan <- fmt.Errorf("分区 %d 更新失败: %v", partitionID, err)
					} else if !success {
						errChan <- fmt.Errorf("分区 %d 更新未成功", partitionID)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			t.Error(err)
		}

		// 验证剩余分区的状态
		remainingPartitions, err := store.HGetAllPartitions(ctx, key)
		assert.NoError(t, err)

		// 应该还有一些分区存在
		assert.Greater(t, len(remainingPartitions), 0, "应该还有一些分区存在")
		assert.LessOrEqual(t, len(remainingPartitions), numPartitions, "剩余分区数量不应超过总数")

		t.Logf("剩余分区数量: %d / %d", len(remainingPartitions), numPartitions)
	})
}

// TestHashPartitionOperations_HighConcurrencyStress 高并发压力测试
func TestHashPartitionOperations_HighConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过高并发压力测试（短模式）")
	}

	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:stress:partitions"
	numWorkers := 50
	numOperationsPerWorker := 100
	numPartitions := 20

	// 初始化分区
	for i := 1; i <= numPartitions; i++ {
		partition := fmt.Sprintf(`{"id":%d,"status":"pending","version":1,"worker_id":""}`, i)
		err := store.HSetPartition(ctx, key, strconv.Itoa(i), partition)
		assert.NoError(t, err)
	}

	t.Run("HighConcurrencyMixedOperations", func(t *testing.T) {
		var wg sync.WaitGroup
		operationStats := struct {
			reads   int64
			writes  int64
			deletes int64
			errors  int64
		}{}

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for j := 0; j < numOperationsPerWorker; j++ {
					partitionID := rand.Intn(numPartitions) + 1
					pid := strconv.Itoa(partitionID)

					switch rand.Intn(4) {
					case 0: // 读操作
						_, err := store.HGetPartition(ctx, key, pid)
						if err != nil && !errors.Is(err, ErrNotFound) {
							atomic.AddInt64(&operationStats.errors, 1)
						} else {
							atomic.AddInt64(&operationStats.reads, 1)
						}

					case 1: // 写操作
						partition := fmt.Sprintf(`{"id":%d,"status":"stress_test","version":1,"worker_id":"worker_%d"}`, partitionID, workerID)
						err := store.HSetPartition(ctx, key, pid, partition)
						if err != nil {
							atomic.AddInt64(&operationStats.errors, 1)
						} else {
							atomic.AddInt64(&operationStats.writes, 1)
						}

					case 2: // 版本化更新操作
						currentPartition, err := store.HGetPartition(ctx, key, pid)
						if err != nil {
							continue
						}

						var partitionData map[string]interface{}
						if err := json.Unmarshal([]byte(currentPartition), &partitionData); err != nil {
							continue
						}

						version, _ := partitionData["version"].(float64)
						currentVersion := int64(version)

						updatedPartition := fmt.Sprintf(`{"id":%d,"status":"updated","version":%d,"worker_id":"worker_%d"}`,
							partitionID, currentVersion+1, workerID)

						success, err := store.HUpdatePartitionWithVersion(ctx, key, pid, updatedPartition, currentVersion)
						if err != nil || !success {
							atomic.AddInt64(&operationStats.errors, 1)
						} else {
							atomic.AddInt64(&operationStats.writes, 1)
						}

					case 3: // 获取所有分区
						_, err := store.HGetAllPartitions(ctx, key)
						if err != nil {
							atomic.AddInt64(&operationStats.errors, 1)
						} else {
							atomic.AddInt64(&operationStats.reads, 1)
						}
					}

					// 短暂随机延迟
					if rand.Intn(10) == 0 {
						time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
					}
				}
			}(i)
		}

		wg.Wait()

		t.Logf("操作统计 - 读取: %d, 写入: %d, 删除: %d, 错误: %d",
			operationStats.reads, operationStats.writes, operationStats.deletes, operationStats.errors)

		// 验证最终状态
		finalPartitions, err := store.HGetAllPartitions(ctx, key)
		assert.NoError(t, err)
		assert.Greater(t, len(finalPartitions), 0, "应该还有分区存在")

		// 错误率不应该太高
		totalOperations := operationStats.reads + operationStats.writes + operationStats.deletes + operationStats.errors
		errorRate := float64(operationStats.errors) / float64(totalOperations)
		assert.Less(t, errorRate, 0.1, "错误率不应超过10%%")
	})
}

// TestHUpdatePartitionWithVersion_ConcurrentRaceCondition tests the race condition in optimistic locking
func TestHUpdatePartitionWithVersion_ConcurrentRaceCondition(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:race_partitions"

	// Create initial partition with version 1
	initialPartition := `{"id":1,"status":"pending","version":1}`
	success, err := store.HUpdatePartitionWithVersion(ctx, key, "1", initialPartition, 0)
	require.NoError(t, err)
	require.True(t, success, "Initial partition creation should succeed")

	// Test concurrent updates - all workers try to claim the partition from version 1
	const numWorkers = 10
	var wg sync.WaitGroup
	results := make([]bool, numWorkers)
	errors := make([]error, numWorkers)

	// All workers simultaneously try to update from version 1 to version 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			claimedPartition := fmt.Sprintf(`{"id":1,"status":"claimed","worker":"worker-%d","version":2}`, workerIndex)

			// All workers expect version 1 and try to update to version 2
			success, err := store.HUpdatePartitionWithVersion(ctx, key, "1", claimedPartition, 1)
			results[workerIndex] = success
			errors[workerIndex] = err
		}(i)
	}

	wg.Wait()

	// Analyze results
	successCount := 0
	optimisticLockFailures := 0
	otherErrors := 0

	for i := 0; i < numWorkers; i++ {
		if errors[i] != nil {
			if errors[i] == ErrOptimisticLockFailed {
				optimisticLockFailures++
			} else {
				otherErrors++
				t.Logf("Worker %d unexpected error: %v", i, errors[i])
			}
		} else if results[i] {
			successCount++
			t.Logf("Worker %d succeeded", i)
		} else {
			// success=false without error means optimistic lock failure
			optimisticLockFailures++
		}
	}

	t.Logf("Results: %d successes, %d optimistic lock failures, %d other errors",
		successCount, optimisticLockFailures, otherErrors)

	// CRITICAL: Only ONE worker should succeed
	assert.Equal(t, 1, successCount, "Exactly one worker should succeed in claiming the partition")
	assert.Equal(t, numWorkers-1, optimisticLockFailures, "All other workers should get optimistic lock failures")
	assert.Equal(t, 0, otherErrors, "Should not have any other errors")

	// Verify final state
	finalPartition, err := store.HGetPartition(ctx, key, "1")
	require.NoError(t, err)
	t.Logf("Final partition state: %s", finalPartition)

	// Verify that the final partition has version 2 and is claimed
	assert.Contains(t, finalPartition, `"version":2`, "Final partition should have version 2")
	assert.Contains(t, finalPartition, `"status":"claimed"`, "Final partition should be claimed")
}

// TestHUpdatePartitionWithVersion_VersionSequence tests version sequencing
func TestHUpdatePartitionWithVersion_VersionSequence(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:sequence_partitions"

	// 1. Create partition (expectedVersion=0, newVersion=1)
	partition1 := `{"id":1,"status":"pending","version":1}`
	success, err := store.HUpdatePartitionWithVersion(ctx, key, "1", partition1, 0)
	require.NoError(t, err)
	assert.True(t, success, "Creation should succeed")

	// 2. Update from version 1 to 2
	partition2 := `{"id":1,"status":"claimed","worker":"worker-1","version":2}`
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "1", partition2, 1)
	require.NoError(t, err)
	assert.True(t, success, "First update should succeed")

	// 3. Try to update with wrong expected version (expecting 1, but actual is 2)
	partition3 := `{"id":1,"status":"stolen","worker":"worker-2","version":3}`
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "1", partition3, 1)

	// This should fail - either return false with ErrOptimisticLockFailed or false with no error
	if err != nil {
		assert.Equal(t, ErrOptimisticLockFailed, err, "Should return optimistic lock failed error")
		assert.False(t, success, "Should not succeed when error is returned")
	} else {
		assert.False(t, success, "Should return false for version mismatch")
	}

	// 4. Update with correct expected version (expecting 2, updating to 3)
	partition4 := `{"id":1,"status":"transferred","worker":"worker-3","version":3}`
	success, err = store.HUpdatePartitionWithVersion(ctx, key, "1", partition4, 2)
	require.NoError(t, err)
	assert.True(t, success, "Update with correct expected version should succeed")

	// Verify final state
	finalPartition, err := store.HGetPartition(ctx, key, "1")
	require.NoError(t, err)
	assert.Contains(t, finalPartition, `"version":3`, "Final partition should have version 3")
	assert.Contains(t, finalPartition, `"worker":"worker-3"`, "Final partition should be owned by worker-3")
}
