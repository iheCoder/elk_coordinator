package partition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 心跳和抢占相关操作 ====================

// isPartitionStaleByHeartbeat 检查分区是否因心跳超时而过时
//
// 参数:
//   - partition: 要检查的分区信息
//
// 返回:
//   - bool: 如果分区心跳过时则返回 true
func (s *HashPartitionStrategy) isPartitionStaleByHeartbeat(partition *model.PartitionInfo) bool {
	if partition == nil {
		return false
	}

	// 只有 claimed 或 running 状态的分区才需要心跳检查
	if partition.Status != model.StatusClaimed && partition.Status != model.StatusRunning {
		return false
	}

	// 检查心跳是否超时
	return time.Since(partition.LastHeartbeat) > s.staleThreshold
}

// updatePartitionHeartbeat 更新分区心跳时间
//
// 参数:
//   - ctx: 上下文
//   - partition: 要更新心跳的分区信息
//   - workerID: 工作节点ID
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - error: 错误信息
func (s *HashPartitionStrategy) updatePartitionHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string) (*model.PartitionInfo, error) {
	// 验证权限：只有分区的持有者才能更新心跳
	if partition.WorkerID != workerID {
		s.logger.Warnf("工作节点 %s 尝试更新分区 %d 的心跳，但该分区属于 %s", workerID, partition.PartitionID, partition.WorkerID)
		return nil, fmt.Errorf("工作节点 %s 无权更新分区 %d 的心跳", workerID, partition.PartitionID)
	}

	// 更新心跳时间
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	return s.updatePartitionWithVersionControl(ctx, partition)
}

// MaintainPartitionHold 维护对分区的持有权（更新心跳时间）
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//   - workerID: 工作节点ID
//
// 返回:
//   - error: 错误信息
func (s *HashPartitionStrategy) MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("工作节点ID不能为空")
	}

	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		s.logger.Errorf("维护分区 %d 持有权时获取分区信息失败: %v", partitionID, err)
		return fmt.Errorf("获取分区 %d 信息失败: %w", partitionID, err)
	}

	// 检查分区是否被该工作节点持有
	if partition.WorkerID != workerID {
		s.logger.Warnf("工作节点 %s 尝试维护分区 %d 的持有权，但该分区属于 %s", workerID, partitionID, partition.WorkerID)
		return fmt.Errorf("工作节点 %s 没有分区 %d 的持有权", workerID, partitionID)
	}

	// 只有活跃状态的分区才需要心跳维护
	if partition.Status != model.StatusClaimed && partition.Status != model.StatusRunning {
		s.logger.Debugf("分区 %d 状态为 %s，无需心跳维护", partitionID, partition.Status)
		return nil
	}

	// 更新心跳时间
	_, err = s.updatePartitionHeartbeat(ctx, partition, workerID)
	if err != nil {
		s.logger.Errorf("更新分区 %d 心跳失败: %v", partitionID, err)
		return fmt.Errorf("更新分区 %d 心跳失败: %w", partitionID, err)
	}

	s.logger.Debugf("成功维护分区 %d 的持有权（工作节点: %s）", partitionID, workerID)
	return nil
}

// acquirePartitionDirect 直接获取pending状态的分区
//
// 参数:
//   - ctx: 上下文
//   - partition: 要获取的分区信息
//   - workerID: 工作节点ID
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - error: 错误信息
func (s *HashPartitionStrategy) acquirePartitionDirect(ctx context.Context, partition *model.PartitionInfo, workerID string) (*model.PartitionInfo, error) {
	// 更新分区状态
	partition.Status = model.StatusClaimed
	partition.WorkerID = workerID
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	return s.updatePartitionWithVersionControl(ctx, partition)
}

// preemptPartitionByHeartbeat 基于心跳时间抢占分区
//
// 参数:
//   - ctx: 上下文
//   - partition: 要抢占的分区信息
//   - workerID: 新的工作节点ID
//   - options: 抢占选项
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - error: 错误信息
func (s *HashPartitionStrategy) preemptPartitionByHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, error) {
	// 检查是否可以抢占（基于心跳过时）
	if !s.isPartitionStaleByHeartbeat(partition) {
		s.logger.Debugf("分区 %d 的心跳未过时，无法抢占（最后心跳: %v，阈值: %v）",
			partition.PartitionID, partition.LastHeartbeat, s.staleThreshold)
		return nil, fmt.Errorf("分区 %d 心跳未过时，无法抢占", partition.PartitionID)
	}

	// 记录抢占操作
	s.logger.Infof("工作节点 %s 基于心跳超时抢占分区 %d（原持有者: %s，最后心跳: %v）",
		workerID, partition.PartitionID, partition.WorkerID, partition.LastHeartbeat)

	// 更新分区状态
	partition.Status = model.StatusClaimed
	partition.WorkerID = workerID
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	return s.updatePartitionWithVersionControl(ctx, partition)
}

// tryPreemptPartitionByHeartbeat 尝试基于心跳时间抢占分区，返回新的接口签名
//
// 参数:
//   - ctx: 上下文
//   - partition: 要抢占的分区信息
//   - workerID: 新的工作节点ID
//   - options: 抢占选项
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - bool: 是否抢占成功
//   - error: 错误信息
func (s *HashPartitionStrategy) tryPreemptPartitionByHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	// 检查是否强制抢占
	if options.ForcePreemption {
		s.logger.Infof("工作节点 %s 强制抢占分区 %d（原持有者: %s）", workerID, partition.PartitionID, partition.WorkerID)
		// 直接执行抢占，忽略心跳检查
		partition.Status = model.StatusClaimed
		partition.WorkerID = workerID
		partition.LastHeartbeat = time.Now()

		// 使用版本控制更新
		updatedPartition, err := s.updatePartitionWithVersionControl(ctx, partition)
		if err != nil {
			return nil, false, err
		}
		return updatedPartition, true, nil
	}

	// 检查是否可以抢占（基于心跳过时）
	if !s.isPartitionStaleByHeartbeat(partition) {
		s.logger.Debugf("分区 %d 的心跳未过时，无法抢占", partition.PartitionID)
		return nil, false, nil
	}

	// 调用原有的抢占方法
	updatedPartition, err := s.preemptPartitionByHeartbeat(ctx, partition, workerID, options)
	if err != nil {
		return nil, false, err
	}

	// 抢占成功
	return updatedPartition, true, nil
}

// AcquirePartition 声明对指定分区的持有权
//
// 该方法实现了分区获取的完整流程：
// 1. 获取分区当前状态
// 2. 检查分区是否可直接获取（pending状态）
// 3. 如果允许抢占，尝试基于心跳超时进行抢占
// 4. 支持重试机制处理乐观锁冲突
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//   - workerID: 工作节点ID
//   - options: 获取选项
//
// 返回:
//   - *model.PartitionInfo: 获取的分区信息
//   - bool: 是否获取成功
//   - error: 错误信息
func (s *HashPartitionStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	if workerID == "" {
		return nil, false, fmt.Errorf("工作节点ID不能为空")
	}

	// 设置默认选项
	if options == nil {
		options = &model.AcquirePartitionOptions{
			AllowPreemption:   true,
			PreemptionTimeout: s.staleThreshold,
		}
	}

	// 在并发环境下，对于pending分区的获取可能需要重试几次以处理乐观锁冲突
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 获取当前分区状态
		partition, err := s.GetPartition(ctx, partitionID)
		if err != nil {
			s.logger.Errorf("获取分区 %d 状态失败: %v", partitionID, err)
			return nil, false, fmt.Errorf("获取分区 %d 状态失败: %w", partitionID, err)
		}

		// 检查工作节点是否已经持有该分区
		if partition.WorkerID == workerID {
			// 工作节点已经持有该分区，更新心跳并返回
			if partition.Status == model.StatusClaimed || partition.Status == model.StatusRunning {
				updatedPartition, err := s.updatePartitionHeartbeat(ctx, partition, workerID)
				if err != nil {
					if errors.Is(err, data.ErrOptimisticLockFailed) && attempt < maxRetries-1 {
						s.logger.Debugf("[%s] 心跳更新乐观锁失败，重试 #%d", workerID, attempt+2)
						continue // 重试
					}
					return nil, false, err
				}
				return updatedPartition, true, nil
			}
		}

		// 检查分区是否可以直接声明（pending状态且无持有者）
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			s.logger.Debugf("[%s] 尝试直接获取pending分区 %d", workerID, partitionID)
			updatedPartition, err := s.acquirePartitionDirect(ctx, partition, workerID)
			if err != nil {
				if errors.Is(err, data.ErrOptimisticLockFailed) && attempt < maxRetries-1 {
					// 乐观锁失败，重试。但只对pending状态的分区重试
					s.logger.Debugf("[%s] 直接获取乐观锁失败，重试 #%d", workerID, attempt+2)
					continue
				}
				s.logger.Debugf("[%s] 直接获取失败: %v", workerID, err)
				return nil, false, err
			}
			return updatedPartition, true, nil
		}

		// 分区已被其他worker持有，检查是否允许抢占
		if !options.AllowPreemption {
			s.logger.Debugf("[%s] 分区 %d 已被 %s 持有，且不允许抢占", workerID, partitionID, partition.WorkerID)
			return nil, false, nil
		}

		// 尝试抢占分区
		s.logger.Debugf("[%s] 尝试抢占分区 %d（当前持有者: %s）", workerID, partitionID, partition.WorkerID)
		updatedPartition, success, err := s.tryPreemptPartitionByHeartbeat(ctx, partition, workerID, options)
		if err != nil {
			if errors.Is(err, data.ErrOptimisticLockFailed) && attempt < maxRetries-1 {
				s.logger.Debugf("[%s] 抢占乐观锁失败，重试 #%d", workerID, attempt+2)
				continue // 重试
			}
			return nil, false, err
		}

		// 抢占的结果是确定的，不需要重试
		return updatedPartition, success, nil
	}

	// 如果所有重试都失败了，返回最后的错误
	return nil, false, fmt.Errorf("获取分区 %d 失败：超过最大重试次数", partitionID)
}
