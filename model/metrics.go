package model

import (
	"encoding/json"
	"time"
)

// 工作节点性能指标相关常量
const (
	// 指标采集相关
	MetricsWindowSize       = 30               // 性能指标窗口大小，保留最近多少个任务的指标
	MinMetricsForAdjustment = 5                // 调整并发度所需的最小指标数量
	RecentTasksToConsider   = 10               // 调整并发度时考虑的最近任务数量
	FastTaskThreshold       = 5 * time.Second  // 认为任务处理很快的阈值
	SlowTaskThreshold       = 30 * time.Second // 认为任务处理很慢的阈值
	DefaultRecentPartitions = 3                // 默认记录最近几个分区的处理速度

	// 并发控制相关
	DefaultMaxConcurrentTasks = 10               // 默认最大并发任务数
	DefaultMinConcurrentTasks = 1                // 默认最小并发任务数
	ConcurrencyAdjustInterval = 15 * time.Second // 并发度调整间隔

	// 容量更新相关
	DefaultCapacityUpdateInterval = 10 * time.Second // 默认容量更新间隔
)

// WorkerMetrics 工作节点性能指标
type WorkerMetrics struct {
	ProcessingSpeed       float64          `json:"processing_speed"`                  // 处理速度 (项/秒)
	SuccessRate           float64          `json:"success_rate"`                      // 成功率 (0.0-1.0)
	AvgProcessingTime     time.Duration    `json:"avg_processing_time"`               // 平均处理时间
	TotalTasksCompleted   int64            `json:"total_tasks_completed"`             // 总完成任务数
	SuccessfulTasks       int64            `json:"successful_tasks"`                  // 成功任务数
	TotalItemsProcessed   int64            `json:"total_items_processed"`             // 总处理项数
	LastUpdateTime        time.Time        `json:"last_update_time"`                  // 最后更新时间
	RecentPartitionSpeeds []PartitionSpeed `json:"recent_partition_speeds,omitempty"` // 最近n个分区的处理速度
}

// PartitionSpeed 分区处理速度信息
type PartitionSpeed struct {
	PartitionID   int           `json:"partition_id"`   // 分区ID
	Speed         float64       `json:"speed"`          // 处理速度 (项/秒)
	ItemCount     int64         `json:"item_count"`     // 处理项数
	ProcessedTime time.Duration `json:"processed_time"` // 处理用时
	ProcessedAt   time.Time     `json:"processed_at"`   // 处理时间点
}

// WorkerCapacityInfo 工作节点容量信息，用于节点间通信
type WorkerCapacityInfo struct {
	WorkerID        string    `json:"worker_id"`        // 工作节点ID
	CapacityScore   float64   `json:"capacity_score"`   // 容量评分，用于任务分配
	CurrentTasks    int       `json:"current_tasks"`    // 当前并发任务数
	MaxTasks        int       `json:"max_tasks"`        // 最大并发任务数
	ProcessingSpeed float64   `json:"processing_speed"` // 处理速度
	SuccessRate     float64   `json:"success_rate"`     // 成功率
	UpdateTime      time.Time `json:"update_time"`      // 更新时间
}

// GetMetricsJSON 获取工作节点性能指标的JSON字符串
func (w *WorkerMetrics) GetMetricsJSON() (string, error) {
	data, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
