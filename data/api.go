package data

import (
	"context"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
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
	// 设置工作节点心跳
	SetWorkerHeartbeat(ctx context.Context, workerID string, value string) error
	// 刷新工作节点心跳（只重置过期时间，更高效）
	RefreshWorkerHeartbeat(ctx context.Context, workerID string) error
	// 获取工作节点心跳
	GetWorkerHeartbeat(ctx context.Context, workerID string) (string, error)
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
	// HLen 获取哈希字段数量
	HLen(ctx context.Context, key string) (int64, error)
	// HUpdatePartitionWithVersion 版本化更新哈希分区字段
	HUpdatePartitionWithVersion(ctx context.Context, key string, field string, value string, version int64) (bool, error)
	// HSetPartitionsInTx 事务中批量设置哈希分区
	HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error
	// HSetPartitionsWithStatsInTx 原子性批量创建分区并更新统计数据
	HSetPartitionsWithStatsInTx(ctx context.Context, partitionKey string, statsKey string, partitions map[string]string, stats *model.PartitionStats) error
	// HDeletePartition 删除哈希分区字段
	HDeletePartition(ctx context.Context, key string, field string) error
}

// PartitionStatsOperations 定义分区统计管理的接口
type PartitionStatsOperations interface {
	// InitPartitionStats 初始化分区统计数据
	InitPartitionStats(ctx context.Context, statsKey string) error
	// GetPartitionStatsData 获取统计数据（原子操作）
	GetPartitionStatsData(ctx context.Context, statsKey string) (map[string]string, error)
	// UpdatePartitionStatsOnCreate 创建分区时更新统计
	UpdatePartitionStatsOnCreate(ctx context.Context, statsKey string, partitionID int, dataID int64) error
	// UpdatePartitionStatsOnStatusChange 状态变更时更新统计
	UpdatePartitionStatsOnStatusChange(ctx context.Context, statsKey string, oldStatus, newStatus string) error
	// UpdatePartitionStatsOnDelete 删除分区时更新统计
	UpdatePartitionStatsOnDelete(ctx context.Context, statsKey string, status string) error
	// RebuildPartitionStats 重建统计数据（从现有分区数据）
	RebuildPartitionStats(ctx context.Context, statsKey string, activePartitionsKey, archivedPartitionsKey string) error
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
	// 注册工作节点到workers集合
	RegisterWorker(ctx context.Context, workerID string) error
	// 从workers集合注销工作节点
	UnregisterWorker(ctx context.Context, workerID string) error
	// 获取活跃工作节点（通过heartbeat存在性发现）
	GetActiveWorkers(ctx context.Context) ([]string, error)
	// 获取所有工作节点（包括已下线的），返回WorkerInfo结构
	GetAllWorkers(ctx context.Context) ([]*model.WorkerInfo, error)
	// 检查工作节点是否活跃
	IsWorkerActive(ctx context.Context, workerID string) (bool, error)
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
	PartitionStatsOperations
}
