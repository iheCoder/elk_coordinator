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

// PartitionHeartbeatStatus 分区心跳状态信息
type PartitionHeartbeatStatus struct {
	PartitionID            int           `json:"partition_id"`              // 分区ID
	WorkerID               string        `json:"worker_id"`                 // 持有者工作节点ID
	LastHeartbeat          time.Time     `json:"last_heartbeat"`            // 最后心跳时间
	TimeSinceLastHeartbeat time.Duration `json:"time_since_last_heartbeat"` // 距离最后心跳的时间
	IsStale                bool          `json:"is_stale"`                  // 是否已过时
	StaleThreshold         time.Duration `json:"stale_threshold"`           // 过时阈值
}

// PartitionStats 分区状态统计信息
type PartitionStats struct {
	Total           int     `json:"total"`             // 总分区数
	Pending         int     `json:"pending"`           // 等待处理的分区数
	Claimed         int     `json:"claimed"`           // 已声明的分区数
	Running         int     `json:"running"`           // 正在处理的分区数
	Completed       int     `json:"completed"`         // 已完成的分区数
	Failed          int     `json:"failed"`            // 失败的分区数
	CompletionRate  float64 `json:"completion_rate"`   // 完成率 (completed / total)
	FailureRate     float64 `json:"failure_rate"`      // 失败率 (failed / total)
	MaxPartitionID  int     `json:"max_partition_id"`  // 当前最大分区ID（用于生成新的不冲突分区ID）
	LastAllocatedID int64   `json:"last_allocated_id"` // 当前已分配的最大数据ID（用于确定新分区的数据范围）
}

// ==================== 分区策略相关类型 ====================

// UpdateOptions 更新操作的选项
type UpdateOptions struct {
	// ExpectedVersion 期望的版本号（用于支持乐观锁的策略）
	// 如果策略不支持版本控制，可以忽略此字段
	ExpectedVersion *int64 `json:"expected_version,omitempty"`

	// Upsert 如果为true，当分区不存在时会创建新分区
	Upsert bool `json:"upsert,omitempty"`

	// Force 强制更新，忽略并发控制检查（谨慎使用）
	Force bool `json:"force,omitempty"`
}

// CreatePartitionRequest 创建分区的请求
type CreatePartitionRequest struct {
	PartitionID int                    `json:"partition_id"`
	MinID       int64                  `json:"min_id"`
	MaxID       int64                  `json:"max_id"`
	Options     map[string]interface{} `json:"options,omitempty"`
}

// ToPartitionInfo 将创建请求转换为分区信息（便于测试和调试）
func (req CreatePartitionRequest) ToPartitionInfo() *PartitionInfo {
	return &PartitionInfo{
		PartitionID: req.PartitionID,
		MinID:       req.MinID,
		MaxID:       req.MaxID,
		Options:     req.Options,
		Status:      StatusPending,
		// 其他字段由系统自动设置
	}
}

// CreatePartitionsRequest 批量创建分区的请求
type CreatePartitionsRequest struct {
	Partitions []CreatePartitionRequest `json:"partitions"`
}

// ToPartitionInfos 将批量创建请求转换为分区信息列表（便于测试和调试）
func (req CreatePartitionsRequest) ToPartitionInfos() []*PartitionInfo {
	result := make([]*PartitionInfo, len(req.Partitions))
	for i, partition := range req.Partitions {
		result[i] = partition.ToPartitionInfo()
	}
	return result
}

// AcquirePartitionOptions 获取分区的选项
type AcquirePartitionOptions struct {
	// AllowPreemption 是否允许抢占其他工作节点持有的分区
	// 当为true时，可以尝试抢占过时的、非活跃的分区
	AllowPreemption bool `json:"allow_preemption,omitempty"`

	// ForcePreemption 是否强制抢占（忽略活跃性检查）
	// 仅在紧急情况下使用，可能导致重复处理
	ForcePreemption bool `json:"force_preemption,omitempty"`

	// PreemptionTimeout 抢占操作的超时时间
	// 如果为0，使用策略默认超时时间
	PreemptionTimeout time.Duration `json:"preemption_timeout,omitempty"`
}

// GetPartitionsFilters 定义了检索分区时的过滤条件
type GetPartitionsFilters struct {
	// TargetStatuses 指定了要匹配的分区状态列表
	// 如果为空，则不按状态过滤，所有状态的分区都可能成为候选
	TargetStatuses []PartitionStatus

	// StaleDuration 指定分区被视为"过时"的最小持续时间（自 UpdatedAt 以来）
	// 如果分区的 UpdatedAt 时间戳与当前时间的差值超过此持续时间，则被视为过时
	// 如果为 nil 或非正数，则此过时过滤器无效
	StaleDuration *time.Duration

	// ExcludeWorkerIDOnStale 如果 StaleDuration 过滤器激活，
	// 具有此 WorkerID 的分区不会被视为过时（即使它们满足时间条件）
	// 如果为空字符串，则不排除任何 WorkerID
	ExcludeWorkerIDOnStale string

	// Limit 限制返回的分区数量，0表示无限制
	Limit int

	// ExcludePartitionIDs 排除指定的分区ID列表
	ExcludePartitionIDs []int

	// MinID 最小分区ID过滤器，只返回ID大于等于此值的分区
	MinID *int

	// MaxID 最大分区ID过滤器，只返回ID小于等于此值的分区
	MaxID *int
}
