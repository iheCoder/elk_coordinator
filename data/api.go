package data

import (
	"context"
	"time"
)

const (
	elkKeyPrefix = "elk:"
)

// LockOperations 定义分布式锁操作的接口
type LockOperations interface {
	// 获取锁
	AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	// 续期锁
	RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	// 检查锁
	CheckLock(ctx context.Context, key string, expectedValue string) (bool, error)
	// 释放锁
	ReleaseLock(ctx context.Context, key string, value string) error
	// 获取锁的拥有者
	GetLockOwner(ctx context.Context, key string) (string, error)
}

// KeyOperations 定义键值操作的接口
type KeyOperations interface {
	// 设置键值
	SetKey(ctx context.Context, key string, value string, expiry time.Duration) error
	// 获取键值
	GetKey(ctx context.Context, key string) (string, error)
	// 获取匹配模式的键列表
	GetKeys(ctx context.Context, pattern string) ([]string, error)
	// 删除键
	DeleteKey(ctx context.Context, key string) error
}

// HeartbeatOperations 定义心跳操作的接口
type HeartbeatOperations interface {
	// 设置心跳
	SetHeartbeat(ctx context.Context, key string, value string) error
	// 获取心跳
	GetHeartbeat(ctx context.Context, key string) (string, error)
}

// SimplePartitionOperations 定义简单分区操作的接口
type SimplePartitionOperations interface {
	// 设置分区信息
	SetPartitions(ctx context.Context, key string, value string) error
	// 获取分区信息
	GetPartitions(ctx context.Context, key string) (string, error)
}

// HashPartitionOperations 定义基于哈希的分区操作接口
type HashPartitionOperations interface {
	// HSetPartition 设置哈希分区字段
	HSetPartition(ctx context.Context, key string, field string, value string) error
	// HGetPartition 获取哈希分区字段
	HGetPartition(ctx context.Context, key string, field string) (string, error)
	// HGetAllPartitions 获取所有哈希分区字段
	HGetAllPartitions(ctx context.Context, key string) (map[string]string, error)
	// HUpdatePartitionWithVersion 版本化更新哈希分区字段
	HUpdatePartitionWithVersion(ctx context.Context, key string, field string, value string, version int64) (bool, error)
	// HSetPartitionsInTx 事务中批量设置哈希分区
	HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error
	// HDeletePartition 删除哈希分区字段
	HDeletePartition(ctx context.Context, key string, field string) error
}

// StatusOperations 定义状态操作的接口
type StatusOperations interface {
	// 设置同步状态
	SetSyncStatus(ctx context.Context, key string, value string) error
	// 获取同步状态
	GetSyncStatus(ctx context.Context, key string) (string, error)
}

// WorkerRegistry 定义工作节点注册的接口
type WorkerRegistry interface {
	// 注册工作节点
	RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error
	// 注销工作节点
	UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error
	// 获取活跃工作节点
	GetActiveWorkers(ctx context.Context, workersKey string) ([]string, error)
	// 检查工作节点是否活跃
	IsWorkerActive(ctx context.Context, heartbeatKey string) (bool, error)
}

// CounterOperations 定义计数器操作的接口
type CounterOperations interface {
	// 增加计数器
	IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error)
	// 设置计数器
	SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error
	// 获取计数器
	GetCounter(ctx context.Context, counterKey string) (int64, error)
}

// AdvancedOperations 定义高级操作的接口
type AdvancedOperations interface {
	// 使用心跳维持的锁
	LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error)
	// 带超时的尝试获取锁
	TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error)
	// 原子执行脚本
	ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
	// 移动项目
	MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error
}

// QueueOperations 定义队列操作的接口
type QueueOperations interface {
	// 添加到队列
	AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error
	// 从队列获取
	GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error)
	// 从队列移除
	RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error
	// 获取队列长度
	GetQueueLength(ctx context.Context, queueKey string) (int64, error)
}

// CommandOperations 定义命令操作的接口
type CommandOperations interface {
	// 提交命令
	SubmitCommand(ctx context.Context, namespace string, command interface{}) error
	// 获取待处理的命令列表
	GetPendingCommands(ctx context.Context, namespace string, limit int) ([]string, error)
	// 删除命令
	DeleteCommand(ctx context.Context, namespace, commandID string) error
}

// DataStore 通过组合各个功能接口定义完整的分布式协调存储接口
type DataStore interface {
	LockOperations
	KeyOperations
	HeartbeatOperations
	SimplePartitionOperations
	HashPartitionOperations
	StatusOperations
	WorkerRegistry
	CounterOperations
	AdvancedOperations
	QueueOperations
	CommandOperations
}
