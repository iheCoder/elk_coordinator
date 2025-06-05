package partition

import (
	"context"
	"elk_coordinator/model"
)

// PartitionStrategy 定义分区管理策略接口
// 使用策略模式来支持不同的分区管理方式
type PartitionStrategy interface {
	// ==================== 基础CRUD操作 ====================

	// GetPartition 获取单个分区
	GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error)

	// GetAllPartitions 获取所有分区
	GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error)

	// DeletePartition 删除分区
	DeletePartition(ctx context.Context, partitionID int) error

	// GetFilteredPartitions 根据过滤器获取分区
	GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error)

	// ==================== 批量操作 ====================

	// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
	// 提供明确的批量创建语义，避免单个分区创建的歧义
	CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error)

	// DeletePartitions 批量删除分区
	DeletePartitions(ctx context.Context, partitionIDs []int) error

	// ==================== 并发安全操作 ====================

	// UpdatePartition 安全更新分区信息
	// 具体的并发控制机制由策略实现决定（可以是乐观锁、悲观锁或其他机制）
	// options: 更新选项，包含版本控制、upsert等可选参数
	// 返回更新后的分区信息，如果更新失败则返回错误
	UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error)

	// ==================== 策略信息 ====================

	// StrategyType 获取策略类型标识
	StrategyType() model.StrategyType

	// ==================== 高级协调方法 ====================
	// 这些方法用于分布式环境下的分区协调和任务分配

	// AcquirePartition 声明对指定分区的持有权
	// 尝试获取指定分区的锁并声明持有权
	// partitionID: 要声明持有权的分区ID
	// workerID: 工作节点标识
	// options: 获取选项，包括抢占设置（可选，nil表示使用默认选项）
	// 返回: (分区信息, 是否获取成功, 错误)
	// - 如果获取成功，返回 (分区信息, true, nil)
	// - 如果因为分区被占用等正常原因无法获取，返回 (nil, false, nil)
	// - 如果因为系统错误（如网络、数据库等）无法处理，返回 (nil, false, error)
	AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error)

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
	ReleasePartition(ctx context.Context, partitionID int, workerID string) error

	// MaintainPartitionHold 维护对分区的持有权
	// 对于SimpleStrategy：续期分布式锁
	// 对于HashPartitionStrategy：更新心跳时间
	// partitionID: 分区ID
	// workerID: 工作节点标识
	// 返回: 是否成功维护持有权
	MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error

	// ==================== 监控和维护 ====================

	// GetPartitionStats 获取分区状态统计信息
	GetPartitionStats(ctx context.Context) (*model.PartitionStats, error)
}
