package model

import "time"

type PartitionStatus string

// 分区状态常量
const (
	StatusPending   PartitionStatus = "pending"
	StatusClaimed   PartitionStatus = "claimed"
	StatusRunning   PartitionStatus = "running"
	StatusCompleted PartitionStatus = "completed"
	StatusFailed    PartitionStatus = "failed"
)

// PartitionInfo stores partition information
type PartitionInfo struct {
	PartitionID   int                    `json:"partition_id"`
	MinID         int64                  `json:"min_id"`
	MaxID         int64                  `json:"max_id"`
	WorkerID      string                 `json:"worker_id"`
	LastHeartbeat time.Time              `json:"last_heartbeat"`    // Consider if this is still needed if UpdatedAt is used for staleness
	Status        PartitionStatus        `json:"status"`            // pending, running, completed, failed
	UpdatedAt     time.Time              `json:"updated_at"`        // Timestamp of last update
	Options       map[string]interface{} `json:"options,omitempty"` // Optional parameters for the partition
	Version       int64                  `json:"version"`           // Version for optimistic locking
	Error         string                 `json:"error,omitempty"`   // Stores error message if status is failed
	CreatedAt     time.Time              `json:"created_at"`        // Timestamp of creation
}

// PartitionStats 分区状态统计信息
type PartitionStats struct {
	Total          int     `json:"total"`           // 总分区数
	Pending        int     `json:"pending"`         // 等待处理的分区数
	Claimed        int     `json:"claimed"`         // 已声明的分区数
	Running        int     `json:"running"`         // 正在处理的分区数
	Completed      int     `json:"completed"`       // 已完成的分区数
	Failed         int     `json:"failed"`          // 失败的分区数
	CompletionRate float64 `json:"completion_rate"` // 完成率 (completed / total)
	FailureRate    float64 `json:"failure_rate"`    // 失败率 (failed / total)
}
