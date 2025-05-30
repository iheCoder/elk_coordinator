package partition

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"time"
)

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

// GetPartitionsFilters 定义了检索分区时的过滤条件
type GetPartitionsFilters struct {
	// TargetStatuses 指定了要匹配的分区状态列表
	// 如果为空，则不按状态过滤，所有状态的分区都可能成为候选
	TargetStatuses []model.PartitionStatus

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

// PartitionStrategy 定义分区管理策略接口
// 使用策略模式来支持不同的分区管理方式
type PartitionStrategy interface {
	// ==================== 基础CRUD操作 ====================

	// GetPartition 获取单个分区
	GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error)

	// GetAllPartitions 获取所有分区
	GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error)

	// SavePartition 保存分区（创建或更新）
	// 注意：这是直接保存，建议并发环境下使用 UpdatePartitionOptimistically
	SavePartition(ctx context.Context, partitionInfo *model.PartitionInfo) error

	// DeletePartition 删除分区
	DeletePartition(ctx context.Context, partitionID int) error

	// GetFilteredPartitions 根据过滤器获取分区
	GetFilteredPartitions(ctx context.Context, filters GetPartitionsFilters) ([]*model.PartitionInfo, error)

	// ==================== 批量操作 ====================

	// SavePartitions 批量保存分区
	SavePartitions(ctx context.Context, partitions []*model.PartitionInfo) error

	// DeletePartitions 批量删除分区
	DeletePartitions(ctx context.Context, partitionIDs []int) error

	// ==================== 并发安全操作 ====================

	// UpdatePartition 安全更新分区信息
	// 具体的并发控制机制由策略实现决定（可以是乐观锁、悲观锁或其他机制）
	// options: 更新选项，包含版本控制、upsert等可选参数
	// 返回更新后的分区信息，如果更新失败则返回错误
	UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *UpdateOptions) (*model.PartitionInfo, error)

	// CreatePartitionIfNotExists 条件创建分区
	// 只有在分区不存在时才会创建，避免重复创建
	CreatePartitionIfNotExists(ctx context.Context, partitionID int, minID, maxID int64, options map[string]interface{}) (*model.PartitionInfo, error)

	// ==================== 策略信息 ====================

	// StrategyType 获取策略类型标识
	StrategyType() string

	// ==================== 高级协调方法 ====================
	// 这些方法用于分布式环境下的分区协调和任务分配

	// AcquirePartition 尝试获取一个分区任务
	// 具体的锁机制和选择策略由实现决定
	// workerID: 工作节点标识
	// preferences: 获取偏好设置（如超时时间、优先级等）
	// 返回获取到的分区信息，如果没有可用分区则返回 ErrNoAvailablePartition
	AcquirePartition(ctx context.Context, workerID string, preferences map[string]interface{}) (*model.PartitionInfo, error)

	// UpdatePartitionStatus 更新分区状态
	// 实现应验证调用者是否有权限更新该分区
	// partitionID: 分区ID
	// workerID: 工作节点标识
	// status: 新的分区状态
	// metadata: 额外的元数据信息
	UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error

	// ReleasePartition 释放分区
	// 释放指定分区的控制权，具体行为由实现决定
	// partitionID: 分区ID
	// workerID: 工作节点标识
	// reason: 释放原因（完成、失败、取消等）
	ReleasePartition(ctx context.Context, partitionID int, workerID string, reason string) error

	// ==================== 监控和维护 ====================

	// GetPartitionStats 获取分区状态统计信息
	GetPartitionStats(ctx context.Context) (*PartitionStats, error)
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

// PartitionManager 分区管理器
// 使用策略模式来管理不同的分区存储策略
type PartitionManager struct {
	strategy PartitionStrategy
	logger   utils.Logger
}

// NewPartitionManager 创建新的分区管理器
func NewPartitionManager(strategy PartitionStrategy, logger utils.Logger) *PartitionManager {
	return &PartitionManager{
		strategy: strategy,
		logger:   logger,
	}
}

// SetStrategy 设置分区管理策略
func (pm *PartitionManager) SetStrategy(strategy PartitionStrategy) {
	if pm.strategy != nil {
		pm.logger.Infof("切换分区管理策略: %s -> %s",
			pm.strategy.StrategyType(), strategy.StrategyType())
	} else {
		pm.logger.Infof("设置分区管理策略: %s", strategy.StrategyType())
	}
	pm.strategy = strategy
}

// GetStrategy 获取当前分区管理策略
func (pm *PartitionManager) GetStrategy() PartitionStrategy {
	return pm.strategy
}

// ==================== 委托方法到当前策略 ====================

// 基础CRUD操作委托
func (pm *PartitionManager) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	return pm.strategy.GetPartition(ctx, partitionID)
}

func (pm *PartitionManager) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	return pm.strategy.GetAllPartitions(ctx)
}

func (pm *PartitionManager) SavePartition(ctx context.Context, partitionInfo *model.PartitionInfo) error {
	return pm.strategy.SavePartition(ctx, partitionInfo)
}

func (pm *PartitionManager) DeletePartition(ctx context.Context, partitionID int) error {
	return pm.strategy.DeletePartition(ctx, partitionID)
}

func (pm *PartitionManager) GetFilteredPartitions(ctx context.Context, filters GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	return pm.strategy.GetFilteredPartitions(ctx, filters)
}

// 批量操作委托
func (pm *PartitionManager) SavePartitions(ctx context.Context, partitions []*model.PartitionInfo) error {
	return pm.strategy.SavePartitions(ctx, partitions)
}

func (pm *PartitionManager) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	return pm.strategy.DeletePartitions(ctx, partitionIDs)
}

// 并发安全操作委托
func (pm *PartitionManager) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *UpdateOptions) (*model.PartitionInfo, error) {
	return pm.strategy.UpdatePartition(ctx, partitionInfo, options)
}

func (pm *PartitionManager) CreatePartitionIfNotExists(ctx context.Context, partitionID int, minID, maxID int64, options map[string]interface{}) (*model.PartitionInfo, error) {
	return pm.strategy.CreatePartitionIfNotExists(ctx, partitionID, minID, maxID, options)
}

// 高级协调方法委托
func (pm *PartitionManager) AcquirePartition(ctx context.Context, workerID string, preferences map[string]interface{}) (*model.PartitionInfo, error) {
	return pm.strategy.AcquirePartition(ctx, workerID, preferences)
}

func (pm *PartitionManager) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	return pm.strategy.UpdatePartitionStatus(ctx, partitionID, workerID, status, metadata)
}

func (pm *PartitionManager) ReleasePartition(ctx context.Context, partitionID int, workerID string, reason string) error {
	return pm.strategy.ReleasePartition(ctx, partitionID, workerID, reason)
}

// 监控和维护委托
func (pm *PartitionManager) GetPartitionStats(ctx context.Context) (*PartitionStats, error) {
	return pm.strategy.GetPartitionStats(ctx)
}
