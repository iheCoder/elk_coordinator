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
