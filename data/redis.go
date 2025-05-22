package data

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
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
	return &Options{
		KeyPrefix:     "dist:",
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
	return d.opts.KeyPrefix + key
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

	// Remove prefix from keys before returning
	prefixLen := len(d.opts.KeyPrefix)
	for i := range keys {
		if len(keys[i]) > prefixLen {
			keys[i] = keys[i][prefixLen:]
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
	return d.rds.Get(ctx, d.prefixKey(key)).Result()
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
