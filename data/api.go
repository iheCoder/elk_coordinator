package data

import (
	"context"
	"time"
)

// DataStore defines the interface for distributed coordination storage
type DataStore interface {
	// Locking operations
	AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	CheckLock(ctx context.Context, key string, expectedValue string) (bool, error)
	ReleaseLock(ctx context.Context, key string, value string) error
	GetLockOwner(ctx context.Context, key string) (string, error)

	// Heartbeat operations
	SetHeartbeat(ctx context.Context, key string, value string) error
	GetHeartbeat(ctx context.Context, key string) (string, error)

	// Key operations
	SetKey(ctx context.Context, key string, value string, expiry time.Duration) error
	GetKey(ctx context.Context, key string) (string, error)
	GetKeys(ctx context.Context, pattern string) ([]string, error)
	DeleteKey(ctx context.Context, key string) error

	// Partition operations
	SetPartitions(ctx context.Context, key string, value string) error
	GetPartitions(ctx context.Context, key string) (string, error)

	// Hash-based partition operations (新增，更高效的分区存储)
	HSetPartition(ctx context.Context, key string, field string, value string) error
	HGetPartition(ctx context.Context, key string, field string) (string, error)
	HGetAllPartitions(ctx context.Context, key string) (map[string]string, error)
	HUpdatePartitionWithVersion(ctx context.Context, key string, field string, value string, version int64) (bool, error)
	HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error
	HDeletePartition(ctx context.Context, key string, field string) error

	// Status operations
	SetSyncStatus(ctx context.Context, key string, value string) error
	GetSyncStatus(ctx context.Context, key string) (string, error)

	// Worker registration
	RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error
	UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error
	GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error)
	IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error)

	// Counter operations
	IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error)
	SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error
	GetCounter(ctx context.Context, counterKey string) (int64, error)

	// Advanced operations
	LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error)
	TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error)
	ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
	MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error

	// Queue operations
	AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error
	GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error)
	RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error
	GetQueueLength(ctx context.Context, queueKey string) (int64, error)
}
