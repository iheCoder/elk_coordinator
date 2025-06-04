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
	DefaultLeaderElectionInterval  = 5 * time.Second
	DefaultPartitionCount          = 8
	DefaultMaxRetries              = 3
	DefaultConsolidationInterval   = 30 * time.Second
	DefaultPartitionSize           = 3000 // 默认分区大小，每个分区包含的ID数量
	DefaultWorkerPartitionMultiple = 3    // 默认每个工作节点分配的分区倍数
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

// 系统错误定义
var (
	ErrNoAvailablePartition   = errors.New("no available partition to claim")
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

// ==================== 辅助函数 ====================

// Now 返回当前时间，用于统一时间获取
func Now() time.Time {
	return time.Now()
}
