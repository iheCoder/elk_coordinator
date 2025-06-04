package partition

import (
	"elk_coordinator/data"
	"elk_coordinator/utils"
	"time"
)

// SimpleStrategy 简单分区策略实现
//
// 该策略专注于提供分区的基础封装操作，包括：
// - 分布式锁管理和并发安全
// - 数据持久化和一致性保证
// - 分区生命周期管理
// - 工作节点权限验证
//
// 设计原则：
// - 模块化：功能按职责分离到不同文件
// - 可扩展：支持自定义配置和选项
// - 线程安全：所有操作都是并发安全的
// - 容错性：提供完善的错误处理和恢复机制
//
// 不包含具体的分配和获取业务逻辑，这些由leader和runner负责
type SimpleStrategy struct {
	// 基础配置
	namespace string
	dataStore interface {
		data.LockOperations
		data.SimplePartitionOperations
		data.KeyOperations
		data.HeartbeatOperations
	}
	logger utils.Logger

	// 锁和心跳相关配置
	partitionLockExpiry time.Duration
}

// SimpleStrategyConfig 简单策略配置
type SimpleStrategyConfig struct {
	// 命名空间，用于隔离不同应用的分区数据
	Namespace string

	// 数据存储接口，必须实现所需的操作接口
	DataStore interface {
		data.LockOperations
		data.SimplePartitionOperations
		data.KeyOperations
		data.HeartbeatOperations
	}

	// 日志器，用于记录操作日志和调试信息
	Logger utils.Logger

	// 分区锁过期时间，控制分布式锁的生命周期
	PartitionLockExpiry time.Duration
}

// NewSimpleStrategy 创建新的简单分区策略
//
// 该构造函数会设置合理的默认值：
// - 默认日志器：如果未提供则使用默认日志器
// - 默认锁过期时间：3分钟，平衡性能和安全性
//
// 参数:
//   - config: 策略配置，包含所有必要的依赖和设置
//
// 返回:
//   - *SimpleStrategy: 配置完成的策略实例
func NewSimpleStrategy(config SimpleStrategyConfig) *SimpleStrategy {
	// 设置默认值
	if config.Logger == nil {
		config.Logger = utils.NewDefaultLogger()
	}
	if config.PartitionLockExpiry <= 0 {
		config.PartitionLockExpiry = 3 * time.Minute
	}

	return &SimpleStrategy{
		namespace:           config.Namespace,
		dataStore:           config.DataStore,
		logger:              config.Logger,
		partitionLockExpiry: config.PartitionLockExpiry,
	}
}

// StrategyType 返回策略类型标识
//
// 返回:
//   - string: 策略类型为 "simple"
func (s *SimpleStrategy) StrategyType() string {
	return "simple"
}
