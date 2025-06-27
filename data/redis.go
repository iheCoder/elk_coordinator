package data

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/iheCoder/elk_coordinator/utils"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// RedisDataStore implements the DataStore interface with Redis
type RedisDataStore struct {
	rds  *redis.Client
	opts *Options
}

// Options configures the Redis data store
type Options struct {
	KeyPrefix     string
	DefaultExpiry time.Duration
	MaxRetries    int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
}

// DefaultOptions returns default Redis data store options
func DefaultOptions() *Options {
	nodeID := utils.GenerateNodeID()
	if len(nodeID) > 8 {
		nodeID = nodeID[len(nodeID)-8:]
	}

	return &Options{
		KeyPrefix:     nodeID + ":",
		DefaultExpiry: 24 * time.Hour,
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 1 * time.Second,
	}
}

// NewRedisDataStore creates a new Redis-based data store
func NewRedisDataStore(redisClient *redis.Client, opts *Options) *RedisDataStore {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &RedisDataStore{
		rds:  redisClient,
		opts: opts,
	}
}

// prefixKey adds the configured prefix to keys
func (d *RedisDataStore) prefixKey(key string) string {
	return elkKeyPrefix + d.opts.KeyPrefix + key
}

// AcquireLock attempts to acquire a distributed lock
func (d *RedisDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	return d.rds.SetNX(ctx, d.prefixKey(key), value, expiry).Result()
}

// RenewLock renews a distributed lock if it's still owned by the caller
func (d *RedisDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	result, err := d.rds.Eval(ctx, script, []string{d.prefixKey(key)}, value, expiry.Milliseconds()).Result()
	if err != nil {
		return false, err
	}

	return result.(int64) == 1, nil
}

// CheckLock checks if a lock exists and is owned by the caller
func (d *RedisDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	val, err := d.rds.Get(ctx, d.prefixKey(key)).Result()
	if err == redis.Nil {
		return false, nil // Key doesn't exist
	}
	if err != nil {
		return false, err // Redis error
	}

	return val == expectedValue, nil
}

// ReleaseLock releases a distributed lock if it's owned by the caller
func (d *RedisDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	_, err := d.rds.Eval(ctx, script, []string{d.prefixKey(key)}, value).Result()
	return err
}

// GetLockOwner gets the current owner of a lock
func (d *RedisDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	return d.rds.Get(ctx, d.prefixKey(key)).Result()
}

// SetHeartbeat sets a heartbeat with default expiry
func (d *RedisDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	// Set heartbeat with 3 minute expiry
	return d.rds.Set(ctx, d.prefixKey(key), value, 3*time.Minute).Err()
}

// GetHeartbeat gets a heartbeat value
func (d *RedisDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	return d.rds.Get(ctx, d.prefixKey(key)).Result()
}

// GetKeys gets keys matching a pattern
func (d *RedisDataStore) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := d.rds.Keys(ctx, d.prefixKey(pattern)).Result()
	if err != nil {
		return nil, err
	}

	// Remove full prefix (elkKeyPrefix + opts.KeyPrefix) from keys before returning
	fullPrefixLen := len(elkKeyPrefix + d.opts.KeyPrefix)
	for i := range keys {
		if len(keys[i]) > fullPrefixLen {
			keys[i] = keys[i][fullPrefixLen:]
		}
	}

	return keys, nil
}

// GetKey gets a key's value
func (d *RedisDataStore) GetKey(ctx context.Context, key string) (string, error) {
	return d.rds.Get(ctx, d.prefixKey(key)).Result()
}

// DeleteKey deletes a key
func (d *RedisDataStore) DeleteKey(ctx context.Context, key string) error {
	return d.rds.Del(ctx, d.prefixKey(key)).Err()
}

// SetPartitions saves partition information
func (d *RedisDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	return d.rds.Set(ctx, d.prefixKey(key), value, d.opts.DefaultExpiry).Err()
}

// GetPartitions gets partition information
func (d *RedisDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	val, err := d.rds.Get(ctx, d.prefixKey(key)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrNotFound // 将 redis.Nil 映射到 data.ErrNotFound
		}
		return "", err
	}
	return val, nil
}

// SetSyncStatus sets the synchronization status
func (d *RedisDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	return d.rds.Set(ctx, d.prefixKey(key), value, d.opts.DefaultExpiry).Err()
}

// GetSyncStatus gets the synchronization status
func (d *RedisDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	return d.rds.Get(ctx, d.prefixKey(key)).Result()
}

// RegisterWorker registers a worker and its heartbeat
func (d *RedisDataStore) RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error {
	pipe := d.rds.Pipeline()

	// Add to workers set
	pipe.SAdd(ctx, d.prefixKey(workersKey), workerID)
	pipe.Expire(ctx, d.prefixKey(workersKey), d.opts.DefaultExpiry)

	// Set heartbeat
	pipe.Set(ctx, d.prefixKey(heartbeatKey), heartbeatValue, 3*time.Minute)

	_, err := pipe.Exec(ctx)
	return err
}

// UnregisterWorker unregisters a worker and removes its heartbeat
func (d *RedisDataStore) UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error {
	pipe := d.rds.Pipeline()

	// Remove from workers set
	pipe.SRem(ctx, d.prefixKey(workersKey), workerID)

	// Delete heartbeat
	pipe.Del(ctx, d.prefixKey(heartbeatKey))

	_, err := pipe.Exec(ctx)
	return err
}

// GetActiveWorkers gets all workers in the workers set
func (d *RedisDataStore) GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error) {
	return d.rds.SMembers(ctx, d.prefixKey(workersKey)).Result()
}

// IsWorkerActive checks if a worker's heartbeat exists
func (d *RedisDataStore) IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error) {
	exists, err := d.rds.Exists(ctx, d.prefixKey(heartbeatKey)).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// IncrementCounter increments a counter by the specified amount
func (d *RedisDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	return d.rds.IncrBy(ctx, d.prefixKey(counterKey), increment).Result()
}

// SetCounter sets a counter value with expiry
func (d *RedisDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	return d.rds.Set(ctx, d.prefixKey(counterKey), value, expiry).Err()
}

// GetCounter gets a counter value
func (d *RedisDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	val, err := d.rds.Get(ctx, d.prefixKey(counterKey)).Result()
	if err == redis.Nil {
		return 0, nil // Return 0 if key doesn't exist
	}
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(val, 10, 64)
}

// LockWithHeartbeat acquires a lock and maintains it with a heartbeat
func (d *RedisDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	// First try to acquire the lock
	acquired, err := d.AcquireLock(ctx, key, value, heartbeatInterval*3)
	if err != nil || !acquired {
		return acquired, nil, err
	}

	// Create context for heartbeat
	heartbeatCtx, cancel := context.WithCancel(context.Background())

	// Start heartbeat goroutine
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				// Context canceled, try to release lock and exit
				_ = d.ReleaseLock(context.Background(), key, value)
				return
			case <-ticker.C:
				// Renew lock
				renewed, err := d.RenewLock(context.Background(), key, value, heartbeatInterval*3)
				if err != nil || !renewed {
					// Failed to renew, exit heartbeat loop
					return
				}
			}
		}
	}()

	return true, cancel, nil
}

// TryLockWithTimeout attempts to acquire a lock with a timeout
func (d *RedisDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	deadline := time.Now().Add(waitTimeout)
	retryDelay := d.opts.RetryDelay

	// Try until timeout
	for time.Now().Before(deadline) {
		acquired, err := d.AcquireLock(ctx, key, value, lockExpiry)
		if err != nil {
			return false, err
		}
		if acquired {
			return true, nil
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(retryDelay):
			// Continue
		}

		// Exponential backoff up to max delay
		if retryDelay < d.opts.MaxRetryDelay {
			retryDelay = retryDelay * 2
			if retryDelay > d.opts.MaxRetryDelay {
				retryDelay = d.opts.MaxRetryDelay
			}
		}
	}

	return false, errors.New("timeout waiting for lock")
}

// ExecuteAtomically executes a Lua script atomically
func (d *RedisDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	// Prefix all keys
	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = d.prefixKey(key)
	}

	return d.rds.Eval(ctx, script, prefixedKeys, args...).Result()
}

// MoveItem moves an item from one key to another
func (d *RedisDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	script := `
		if redis.call("ZREM", KEYS[1], ARGV[1]) == 1 then
			return redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
		else
			return 0
		end
	`
	// Current timestamp as score
	score := float64(time.Now().Unix())

	_, err := d.rds.Eval(ctx, script, []string{d.prefixKey(fromKey), d.prefixKey(toKey)}, item, score).Result()
	return err
}

// AddToQueue adds an item to a queue (sorted set)
func (d *RedisDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	return d.rds.ZAdd(ctx, d.prefixKey(queueKey), redis.Z{
		Score:  score,
		Member: item,
	}).Err()
}

// GetFromQueue gets items from a queue
func (d *RedisDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	// Use ZRANGE to get lowest scoring elements
	return d.rds.ZRange(ctx, d.prefixKey(queueKey), 0, count-1).Result()
}

// RemoveFromQueue removes an item from a queue
func (d *RedisDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	return d.rds.ZRem(ctx, d.prefixKey(queueKey), item).Err()
}

// GetQueueLength gets the length of a queue
func (d *RedisDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	return d.rds.ZCard(ctx, d.prefixKey(queueKey)).Result()
}

// SetKey sets a key with an expiry time
func (d *RedisDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	return d.rds.Set(ctx, d.prefixKey(key), value, expiry).Err()
}

// HSetPartition 在Redis Hash中设置特定分区字段
func (d *RedisDataStore) HSetPartition(ctx context.Context, key string, field string, value string) error {
	return d.rds.HSet(ctx, d.prefixKey(key), field, value).Err()
}

// HGetPartition 从Redis Hash中获取特定分区字段
func (d *RedisDataStore) HGetPartition(ctx context.Context, key string, field string) (string, error) {
	val, err := d.rds.HGet(ctx, d.prefixKey(key), field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrNotFound // 将 redis.Nil 映射到 data.ErrNotFound
		}
		return "", err
	}
	return val, nil
}

// HGetAllPartitions 从Redis Hash中获取所有分区字段
func (d *RedisDataStore) HGetAllPartitions(ctx context.Context, key string) (map[string]string, error) {
	return d.rds.HGetAll(ctx, d.prefixKey(key)).Result()
}

// HUpdatePartitionWithVersion 使用版本号实现乐观锁更新分区
// value 参数现在是 *model.PartitionInfo 类型
// expectedVersion 是期望的当前存储中的版本号，如果为0，则表示期望字段不存在（用于创建）
func (d *RedisDataStore) HUpdatePartitionWithVersion(ctx context.Context, key string, field string, partitionInfoJSON string, expectedVersion int64) (bool, error) {
	// Redis Lua脚本，实现版本检查和条件更新
	// ARGV[1] is the new partition data (JSON string)
	// ARGV[2] is the expected current version in the store for an update, or 0 for creation.
	script := `
		local currentData = redis.call('HGET', KEYS[1], KEYS[2])
		local newPartitionData = ARGV[1]
		local expectedCurrentVersion = tonumber(ARGV[2])

		if not currentData then
			-- Field does not exist. 
			-- If expectedCurrentVersion is 0, this is a create operation.
			if expectedCurrentVersion == 0 then
				redis.call('HSET', KEYS[1], KEYS[2], newPartitionData)
				return 1 -- Success (created)
			else
				-- Field does not exist, but we expected a version > 0 (i.e., an update on existing).
				return 0 -- Failure (optimistic lock failed - record gone)
			end
		else
			-- Field exists. This is an update operation.
			local decodedData = cjson.decode(currentData)
			local actualCurrentVersion = tonumber(decodedData['version'] or 0)

			if actualCurrentVersion == expectedCurrentVersion then
				-- Versions match, proceed with update.
				redis.call('HSET', KEYS[1], KEYS[2], newPartitionData)
				return 1 -- Success (updated)
			else
				-- Version conflict.
				return 0 -- Failure (optimistic lock failed - version mismatch)
			end
		end
	`

	result, err := d.rds.Eval(ctx, script, []string{d.prefixKey(key), field}, partitionInfoJSON, expectedVersion).Result()
	if err != nil {
		// Check for specific Redis errors if necessary, e.g., script execution error
		// For now, wrap the error for clarity.
		return false, errors.Wrapf(err, "failed to execute HUpdatePartitionWithVersion script for key %s, field %s", key, field)
	}

	// Check the result from the Lua script
	if result == nil {
		return false, errors.New("HUpdatePartitionWithVersion script returned nil result")
	}

	// The script returns 1 for success, 0 for optimistic lock failure.
	// Other return values or types would indicate an issue with the script itself.
	responseCode, ok := result.(int64)
	if !ok {
		return false, errors.Errorf("HUpdatePartitionWithVersion script returned unexpected type: %T, value: %v", result, result)
	}

	if responseCode == 1 {
		return true, nil // Success
	}

	// responseCode == 0, means optimistic lock failure (either version mismatch or record gone when expected)
	// Distinguish between creation failure (already exists) and update failure (version mismatch or gone)
	if expectedVersion == 0 { // Attempted to create
		return false, ErrPartitionAlreadyExists // Or a more generic creation conflict error
	} else { // Attempted to update
		return false, ErrOptimisticLockFailed
	}
}

// HSetPartitionsInTx 使用事务批量设置多个分区，保证原子性
func (d *RedisDataStore) HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error {
	prefixedKey := d.prefixKey(key)

	// 最大重试次数
	maxRetries := d.opts.MaxRetries

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 使用WATCH监视key变化
		txf := func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()

			// 批量设置Hash字段
			for field, value := range partitions {
				pipe.HSet(ctx, prefixedKey, field, value)
			}

			// 设置过期时间
			pipe.Expire(ctx, prefixedKey, d.opts.DefaultExpiry)

			// 执行事务
			_, err := pipe.Exec(ctx)
			return err
		}

		// 执行带WATCH的事务
		err := d.rds.Watch(ctx, txf, prefixedKey)

		if err == nil {
			// 事务成功
			return nil
		}

		if err != redis.TxFailedErr {
			// 如果错误不是事务冲突错误，直接返回
			return err
		}

		// 事务冲突，使用指数退避策略
		backoff := d.opts.RetryDelay << uint(attempt)
		if backoff > d.opts.MaxRetryDelay {
			backoff = d.opts.MaxRetryDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// 等待后继续重试
		}
	}

	return errors.New("达到最大重试次数，仍未能成功更新分区")
}

// HDeletePartition 删除Redis Hash中的特定分区字段
func (d *RedisDataStore) HDeletePartition(ctx context.Context, key string, field string) error {
	return d.rds.HDel(ctx, d.prefixKey(key), field).Err()
}

// CommandOperations implementation

// SubmitCommand 提交命令到Redis
func (d *RedisDataStore) SubmitCommand(ctx context.Context, namespace string, command interface{}) error {
	// 使用JSON序列化命令
	cmdBytes, err := json.Marshal(command)
	if err != nil {
		return errors.Wrap(err, "命令序列化失败")
	}

	// 命令存储在有序集合中，按提交时间排序
	key := d.prefixKey("commands:" + namespace)
	score := float64(time.Now().UnixNano())

	return d.rds.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: string(cmdBytes),
	}).Err()
}

// GetPendingCommands 获取待处理的命令列表
func (d *RedisDataStore) GetPendingCommands(ctx context.Context, namespace string, limit int) ([]string, error) {
	key := d.prefixKey("commands:" + namespace)

	// 如果limit <= 0，直接返回空结果
	if limit <= 0 {
		return []string{}, nil
	}

	// 获取最早的命令（按时间排序）
	results, err := d.rds.ZRange(ctx, key, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "获取待处理命令失败")
	}

	return results, nil
}

// DeleteCommand 删除命令
func (d *RedisDataStore) DeleteCommand(ctx context.Context, namespace, commandID string) error {
	key := d.prefixKey("commands:" + namespace)
	return d.rds.ZRem(ctx, key, commandID).Err()
}

// ==================== 分区统计管理实现 ====================

// InitPartitionStats 初始化分区统计数据（幂等操作）
// 只有在统计数据不存在时才会初始化，避免覆盖现有统计数据
func (d *RedisDataStore) InitPartitionStats(ctx context.Context, statsKey string) error {
	key := d.prefixKey(statsKey)

	// 检查统计数据是否已存在
	exists, err := d.rds.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	// 如果统计数据已存在，跳过初始化
	if exists > 0 {
		return nil
	}

	// 初始化所有统计字段为0
	initialStats := map[string]interface{}{
		"total":             "0",
		"pending":           "0",
		"claimed":           "0",
		"running":           "0",
		"completed":         "0",
		"failed":            "0",
		"max_partition_id":  "0",
		"last_allocated_id": "0",
	}

	return d.rds.HMSet(ctx, key, initialStats).Err()
}

// GetPartitionStatsData 获取统计数据（原子操作）
func (d *RedisDataStore) GetPartitionStatsData(ctx context.Context, statsKey string) (map[string]string, error) {
	key := d.prefixKey(statsKey)
	return d.rds.HGetAll(ctx, key).Result()
}

// UpdatePartitionStatsOnCreate 创建分区时更新统计
func (d *RedisDataStore) UpdatePartitionStatsOnCreate(ctx context.Context, statsKey string, partitionID int, dataID int64) error {
	key := d.prefixKey(statsKey)

	// 使用Lua脚本保证原子性
	script := `
		redis.call('HINCRBY', KEYS[1], 'total', 1)
		redis.call('HINCRBY', KEYS[1], 'pending', 1)

		local currentMaxPartitionID = tonumber(redis.call('HGET', KEYS[1], 'max_partition_id') or 0)
		if tonumber(ARGV[1]) > currentMaxPartitionID then
			redis.call('HSET', KEYS[1], 'max_partition_id', ARGV[1])
		end

		local currentLastAllocatedID = tonumber(redis.call('HGET', KEYS[1], 'last_allocated_id') or 0)
		if tonumber(ARGV[2]) > currentLastAllocatedID then
			redis.call('HSET', KEYS[1], 'last_allocated_id', ARGV[2])
		end

		return 'OK'
	`

	_, err := d.rds.Eval(ctx, script, []string{key}, partitionID, dataID).Result()
	return err
}

// UpdatePartitionStatsOnStatusChange 状态变更时更新统计
func (d *RedisDataStore) UpdatePartitionStatsOnStatusChange(ctx context.Context, statsKey string, oldStatus, newStatus string) error {
	key := d.prefixKey(statsKey)

	// 使用Lua脚本保证原子性
	script := `
		if ARGV[1] ~= '' then
			redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
		end
		if ARGV[2] ~= '' then
			redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
		end
		return 'OK'
	`

	_, err := d.rds.Eval(ctx, script, []string{key}, oldStatus, newStatus).Result()
	return err
}

// UpdatePartitionStatsOnDelete 删除分区时更新统计
func (d *RedisDataStore) UpdatePartitionStatsOnDelete(ctx context.Context, statsKey string, status string) error {
	key := d.prefixKey(statsKey)

	// 使用Lua脚本保证原子性
	script := `
		redis.call('HINCRBY', KEYS[1], 'total', -1)
		if ARGV[1] ~= '' then
			redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
		end
		return 'OK'
	`

	_, err := d.rds.Eval(ctx, script, []string{key}, status).Result()
	return err
}

// RebuildPartitionStats 重建统计数据（从现有分区数据）
func (d *RedisDataStore) RebuildPartitionStats(ctx context.Context, statsKey string, activePartitionsKey, archivedPartitionsKey string) error {
	key := d.prefixKey(statsKey)
	activeKey := d.prefixKey(activePartitionsKey)
	archivedKey := d.prefixKey(archivedPartitionsKey)

	// 使用Lua脚本重建统计
	script := `
		-- 重置所有统计
		redis.call('HSET', KEYS[1], 'total', 0)
		redis.call('HSET', KEYS[1], 'pending', 0)
		redis.call('HSET', KEYS[1], 'claimed', 0)
		redis.call('HSET', KEYS[1], 'running', 0)
		redis.call('HSET', KEYS[1], 'completed', 0)
		redis.call('HSET', KEYS[1], 'failed', 0)
		redis.call('HSET', KEYS[1], 'max_partition_id', 0)
		redis.call('HSET', KEYS[1], 'last_allocated_id', 0)

		local function processPartitions(partitionKey)
			local partitions = redis.call('HGETALL', partitionKey)
			for i = 2, #partitions, 2 do
				local partitionData = partitions[i]
				local success, decoded = pcall(cjson.decode, partitionData)
				if success then
					-- 更新总数
					redis.call('HINCRBY', KEYS[1], 'total', 1)

					-- 更新状态统计
					local status = decoded.status
					if status then
						redis.call('HINCRBY', KEYS[1], status, 1)
					end

					-- 更新最大分区ID
					local partitionID = tonumber(decoded.partition_id or 0)
					local currentMaxPartitionID = tonumber(redis.call('HGET', KEYS[1], 'max_partition_id'))
					if partitionID > currentMaxPartitionID then
						redis.call('HSET', KEYS[1], 'max_partition_id', partitionID)
					end

					-- 更新最大数据ID
					local maxID = tonumber(decoded.max_id or 0)
					local currentLastAllocatedID = tonumber(redis.call('HGET', KEYS[1], 'last_allocated_id'))
					if maxID > currentLastAllocatedID then
						redis.call('HSET', KEYS[1], 'last_allocated_id', maxID)
					end
				end
			end
		end

		-- 处理活跃分区
		if redis.call('EXISTS', KEYS[2]) == 1 then
			processPartitions(KEYS[2])
		end

		-- 处理归档分区
		if redis.call('EXISTS', KEYS[3]) == 1 then
			processPartitions(KEYS[3])
		end

		return 'OK'
	`

	_, err := d.rds.Eval(ctx, script, []string{key, activeKey, archivedKey}).Result()
	return err
}
