package model

import (
	"errors"
	"log"
	"time"
)

// 任务处理相关的常量
const (
	taskRetryDelay     = 1 * time.Second        // 任务获取失败后的重试延迟
	noTaskDelay        = 2 * time.Second        // 无可用任务时的等待时间
	taskCompletedDelay = 500 * time.Millisecond // 任务完成后的延迟，避免立即抢占下一个任务
)

// 分区状态常量
const (
	StatusPending   = "pending"
	StatusClaimed   = "claimed"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
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
	ErrNoAvailablePartition = errors.New("no available partition to claim")
	ErrMaxRetriesExceeded   = errors.New("maximum retry attempts exceeded")
)

// PartitionInfo stores partition information
type PartitionInfo struct {
	PartitionID   int                    `json:"partition_id"`
	MinID         int64                  `json:"min_id"`
	MaxID         int64                  `json:"max_id"`
	WorkerID      string                 `json:"worker_id"`
	LastHeartbeat time.Time              `json:"last_heartbeat"`
	Status        string                 `json:"status"` // pending, running, completed, failed
	UpdatedAt     time.Time              `json:"updated_at"`
	Options       map[string]interface{} `json:"options,omitempty"` // Optional parameters for the partition
}

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

// Logger 定义日志接口
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// defaultLogger 提供简单的默认日志实现
type defaultLogger struct{}

func (l *defaultLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func (l *defaultLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func (l *defaultLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func (l *defaultLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}
