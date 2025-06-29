package model

import (
	"errors"
	"time"
)

// 任务处理相关的常量
const (
	TaskRetryDelay     = 1 * time.Second        // 任务获取失败后的重试延迟
	NoTaskDelay        = 2 * time.Second        // 无可用任务时的等待时间
	TaskCompletedDelay = 500 * time.Millisecond // 任务完成后的延迟，避免立即抢占下一个任务
)

// 默认系统配置常量
const (
	// 时间相关默认设置
	DefaultLeaderLockExpiry        = 30 * time.Second
	DefaultPartitionLockExpiry     = 3 * time.Minute
	DefaultHeartbeatInterval       = 10 * time.Second
	DefaultLeaderElectionInterval  = 180 * time.Second
	DefaultAllocationInterval      = 30 * time.Second // 默认分区分配检查间隔
	DefaultPartitionCount          = 8
	DefaultMaxRetries              = 3
	DefaultConsolidationInterval   = 30 * time.Second
	DefaultPartitionSize           = 3000 // 默认分区大小，每个分区包含的ID数量
	DefaultWorkerPartitionMultiple = 5    // 默认每个工作节点分配的分区倍数
	DefaultMinWorkerCount          = 3    // 最小工作节点数量，避免早期分配时节点数量不足的问题
)

// Redis键格式常量
const (
	// 键格式字符串
	LeaderLockKeyFmt    = "%s:leader_lock"
	PartitionLockFmtFmt = "%s:partition:%d"
	PartitionInfoKeyFmt = "%s:partitions"
	HeartbeatFmtFmt     = "%s:heartbeat:%s"
	StatusKeyFmt        = "%s:status"
	WorkersKeyFmt       = "%s:workers"
	ExitingNodeFmt      = "%s:exiting:%s" // 标记节点正在退出的键格式
)

// Redis键名常量（用于prefixKey场景下的简化key）
const (
	HeartbeatKeyPrefix = "heartbeat"
	WorkersKey         = "workers"
)

// 系统错误定义
var (
	ErrMaxRetriesExceeded     = errors.New("maximum retry attempts exceeded")
	ErrOptimisticLockFailed   = errors.New("optimistic lock failed: version mismatch")
	ErrPartitionAlreadyExists = errors.New("partition already exists")
	ErrPartitionNotFound      = errors.New("partition not found")
)

// SyncStatus stores global synchronization status
type SyncStatus struct {
	LastCompletedSync time.Time              `json:"last_completed_sync"`
	CurrentLeader     string                 `json:"current_leader"`
	PartitionCount    int                    `json:"partition_count"`
	ActiveWorkers     int                    `json:"active_workers"`
	GlobalMaxID       int64                  `json:"global_max_id"`
	PartitionStatus   map[int]PartitionInfo  `json:"partition_status"`
	AdditionalInfo    map[string]interface{} `json:"additional_info,omitempty"`
}

// 任务窗口相关常量
const (
	DefaultTaskWindowSize = 3 // 默认任务窗口大小
)

// WorkerInfo 表示worker信息，用于ZSET存储
type WorkerInfo struct {
	WorkerID     string     `json:"worker_id"`
	RegisterTime time.Time  `json:"register_time"`
	StopTime     *time.Time `json:"stop_time,omitempty"` // nil表示worker仍然活跃
}

// StrategyType 定义分区策略类型的枚举
type StrategyType int32

const (
	// StrategyTypeSimple 简单策略，基于分布式锁实现
	StrategyTypeSimple StrategyType = iota + 1
	// StrategyTypeHash 哈希策略，基于Redis Hash实现
	StrategyTypeHash
	// 未来可以添加更多的策略类型
)

// String 返回策略类型的字符串表示
func (t StrategyType) String() string {
	switch t {
	case StrategyTypeSimple:
		return "Simple"
	case StrategyTypeHash:
		return "Hash"
	default:
		return "Unknown"
	}
}
