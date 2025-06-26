package partition

import (
	"context"
	"time"

	"github.com/iheCoder/elk_coordinator/model"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/utils"
)

const (
	partitionHashKey = "elk_partitions"      // 分区数据在 Redis Hash 中的键名
	statsKey         = "elk_partition_stats" // 统计数据的键名
)

// HashPartitionStoreInterface 定义Hash分区策略需要的完整存储接口
// 组合了分区操作和统计操作接口
type HashPartitionStoreInterface interface {
	data.HashPartitionOperations
	data.PartitionStatsOperations
}

// HashPartitionStrategy 处理分区数据的访问和生命周期管理。
// 实现 PartitionStrategy 接口，提供基于 Redis Hash 的分区存储策略
//
// 该策略采用模块化设计，将功能按职责分离：
// - CRUD 操作：基础的创建、读取、更新、删除操作 (hash_partition_crud.go)
// - 版本控制：乐观锁和版本管理 (hash_partition_version.go)
// - 心跳管理：分区心跳和抢占逻辑 (hash_partition_heartbeat.go)
// - 批量操作：批量创建和删除分区 (hash_partition_batch.go)
// - 状态管理：分区状态更新和释放 (hash_partition_status.go)
// - 统计功能：分区状态统计 (hash_partition_stats.go)
// - 归档策略：分离式归档和已完成分区管理 (hash_partition_archive.go)
type HashPartitionStrategy struct {
	store          HashPartitionStoreInterface // 分区存储接口，支持分区操作和统计操作
	logger         utils.Logger                // 日志记录器
	staleThreshold time.Duration               // 分区被认为过时的心跳阈值，用于抢占判断
}

// NewHashPartitionStrategy 创建一个新的 Hash 分区策略实例。
//
// 参数:
//   - store: 分区存储接口，必须实现 HashPartitionStoreInterface
//   - logger: 日志记录器，不能为空
//
// 返回:
//   - *HashPartitionStrategy: 配置完成的策略实例
//
// 注意: 如果 logger 为空会 panic
func NewHashPartitionStrategy(store HashPartitionStoreInterface, logger utils.Logger) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	strategy := &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: 5 * time.Minute, // 默认5分钟心跳超时阈值
	}

	// 初始化统计数据
	ctx := context.Background()
	if err := store.InitPartitionStats(ctx, statsKey); err != nil {
		logger.Warnf("初始化分区统计数据失败: %v", err)
	}

	return strategy
}

// NewHashPartitionStrategyWithConfig 创建带配置的 Hash 分区策略实例
//
// 参数:
//   - store: 分区存储接口，必须实现 HashPartitionStoreInterface
//   - logger: 日志记录器，不能为空
//   - staleThreshold: 心跳过期阈值，小于等于0时使用默认值
//
// 返回:
//   - *HashPartitionStrategy: 配置完成的策略实例
func NewHashPartitionStrategyWithConfig(store HashPartitionStoreInterface, logger utils.Logger, staleThreshold time.Duration) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	if staleThreshold <= 0 {
		staleThreshold = 5 * time.Minute // 默认值
	}
	strategy := &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: staleThreshold,
	}

	// 初始化统计数据
	ctx := context.Background()
	if err := store.InitPartitionStats(ctx, statsKey); err != nil {
		logger.Warnf("初始化分区统计数据失败: %v", err)
	}

	return strategy
}

// StrategyType 获取策略类型标识
//
// 返回:
//   - StrategyType: 策略类型为 StrategyTypeHash
func (s *HashPartitionStrategy) StrategyType() model.StrategyType {
	return model.StrategyTypeHash
}

// Stop 停止策略并清理相关资源
//
// 实现关注点分离原则，由 HashPartitionStrategy 负责清理自己管理的资源：
// - Hash 策略使用心跳超时机制，不管理分布式锁，因此不需要主动释放资源
// - 只需要停止内部状态和清理轻量级资源
//
// 参数:
//   - ctx: 上下文，用于控制操作超时和取消
//
// 返回:
//   - error: 如果清理过程中发生错误
func (s *HashPartitionStrategy) Stop(ctx context.Context) error {
	s.logger.Infof("HashPartitionStrategy stopping...")

	// Hash 策略使用心跳超时机制，不需要释放分布式锁
	// 分区会通过心跳超时自然释放，无需手动清理
	// 这是一个快速、轻量级的停止操作

	s.logger.Infof("HashPartitionStrategy stopped successfully")
	return nil
}
