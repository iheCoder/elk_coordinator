package partition

import (
	"context"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/utils"
	"sync"
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
//   - StrategyType: 策略类型为 StrategyTypeSimple
func (s *SimpleStrategy) StrategyType() model.StrategyType {
	return model.StrategyTypeSimple
}

// Stop 停止策略并清理相关资源
//
// 实现关注点分离原则，由 SimpleStrategy 负责清理自己管理的资源：
// 1. 获取所有分区信息
// 2. 并行释放分区的分布式锁（相比顺序释放更快）
// 3. 清理相关的内部状态
//
// 参数:
//   - ctx: 上下文，用于控制操作超时和取消
//
// 返回:
//   - error: 如果清理过程中发生错误
func (s *SimpleStrategy) Stop(ctx context.Context) error {
	s.logger.Infof("SimpleStrategy stopping, cleaning up partition locks...")

	// 获取所有分区信息，用于清理
	allPartitions, err := s.GetAllActivePartitions(ctx)
	if err != nil {
		s.logger.Errorf("Failed to get partitions during stop: %v", err)
		// 即使获取分区失败，也要继续尝试清理
		return nil // 不阻塞其他组件的停止
	}

	if len(allPartitions) == 0 {
		s.logger.Infof("SimpleStrategy stopped, no partitions to clean up")
		return nil
	}

	// 并行释放所有分区锁以提高性能
	return s.releasePartitionLocksParallel(ctx, allPartitions)
}

// releasePartitionLocksParallel 并行释放分区锁
// 相比顺序释放，这个方法能显著提高大量分区的停止性能
func (s *SimpleStrategy) releasePartitionLocksParallel(ctx context.Context, partitions []*model.PartitionInfo) error {
	// 创建带缓冲的通道来控制并发数，避免过多的 goroutines
	maxConcurrency := 20 // 限制并发数以避免资源耗尽
	if len(partitions) < maxConcurrency {
		maxConcurrency = len(partitions)
	}

	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	releasedCount := 0

	s.logger.Debugf("Starting parallel lock release for %d partitions with max concurrency %d",
		len(partitions), maxConcurrency)

	for _, partition := range partitions {
		if partition == nil {
			continue
		}

		wg.Add(1)
		go func(p *model.PartitionInfo) {
			defer wg.Done()

			// 获取信号量
			sem <- struct{}{}
			defer func() { <-sem }()

			// 释放分区锁
			lockKey := s.getPartitionLockKey(p.PartitionID)
			err := s.dataStore.ReleaseLock(ctx, lockKey, p.WorkerID)

			// 安全地更新共享状态
			mu.Lock()
			if err != nil {
				s.logger.Warnf("Failed to release partition lock during stop, partitionID: %d, workerID: %s, error: %v",
					p.PartitionID, p.WorkerID, err)
				errors = append(errors, err)
			} else {
				releasedCount++
				s.logger.Debugf("Released partition lock during stop, partitionID: %d, workerID: %s",
					p.PartitionID, p.WorkerID)
			}
			mu.Unlock()
		}(partition)
	}

	// 等待所有 goroutines 完成
	wg.Wait()

	s.logger.Infof("SimpleStrategy stopped, totalPartitions: %d, releasedLocks: %d, errors: %d",
		len(partitions), releasedCount, len(errors))

	// 如果有错误，返回第一个错误，但不阻塞停止过程
	if len(errors) > 0 {
		return errors[0] // 返回第一个错误作为示例
	}

	return nil
}
