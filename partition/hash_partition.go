package partition

import (
	"time"

	"elk_coordinator/data"
	"elk_coordinator/utils"
)

const (
	partitionHashKey = "elk_partitions" // 分区数据在 Redis Hash 中的键名
)

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
type HashPartitionStrategy struct {
	store          data.HashPartitionOperations // 分区存储接口，使用最小接口而不是完整DataStore
	logger         utils.Logger                 // 日志记录器
	staleThreshold time.Duration                // 分区被认为过时的心跳阈值，用于抢占判断
}

// NewHashPartitionStrategy 创建一个新的 Hash 分区策略实例。
//
// 参数:
//   - store: 分区存储接口，必须实现 HashPartitionOperations
//   - logger: 日志记录器，不能为空
//
// 返回:
//   - *HashPartitionStrategy: 配置完成的策略实例
//
// 注意: 如果 logger 为空会 panic
func NewHashPartitionStrategy(store data.HashPartitionOperations, logger utils.Logger) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	return &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: 5 * time.Minute, // 默认5分钟心跳超时阈值
	}
}

// NewHashPartitionStrategyWithConfig 创建带配置的 Hash 分区策略实例
//
// 参数:
//   - store: 分区存储接口
//   - logger: 日志记录器，不能为空
//   - staleThreshold: 心跳过期阈值，小于等于0时使用默认值
//
// 返回:
//   - *HashPartitionStrategy: 配置完成的策略实例
func NewHashPartitionStrategyWithConfig(store data.HashPartitionOperations, logger utils.Logger, staleThreshold time.Duration) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	if staleThreshold <= 0 {
		staleThreshold = 5 * time.Minute // 默认值
	}
	return &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: staleThreshold,
	}
}

// StrategyType 获取策略类型标识
//
// 返回:
//   - StrategyType: 策略类型为 StrategyTypeHash
func (s *HashPartitionStrategy) StrategyType() StrategyType {
	return StrategyTypeHash
}

// ==================== PartitionStrategy 接口实现 ====================

// 所有的PartitionStrategy接口方法实现都已移动到对应的专门文件中：
// - CRUD 操作在 hash_partition_crud.go
// - 版本控制在 hash_partition_version.go
// - 心跳管理在 hash_partition_heartbeat.go
// - 批量操作在 hash_partition_batch.go
// - 状态管理在 hash_partition_status.go
// - 统计功能在 hash_partition_stats.go
