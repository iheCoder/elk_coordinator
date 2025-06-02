// partition.go 包含所有与分区管理相关的函数
package task

import (
	"context"
	"elk_coordinator/model"
	"github.com/pkg/errors"
	"time"
)

// ------------- 分区获取相关函数 -------------

// acquirePartitionTask 获取一个可用的分区任务
// 使用策略接口按照优先级顺序获取: pending -> claimed -> running
func (r *Runner) acquirePartitionTask(ctx context.Context) (model.PartitionInfo, error) {
	// 获取所有分区信息
	allPartitions, err := r.partitionStrategy.GetAllPartitions(ctx)
	if err != nil {
		return model.PartitionInfo{}, errors.Wrap(err, "获取分区信息失败")
	}

	if len(allPartitions) == 0 {
		return model.PartitionInfo{}, model.ErrNoAvailablePartition
	}

	// 按优先级顺序尝试获取分区
	// 1. 先尝试获取Pending状态的分区
	for _, partition := range allPartitions {
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
				return *acquired, nil
			}
		}
	}

	// 2. 然后尝试获取可重新获取的Claimed状态分区
	for _, partition := range allPartitions {
		if partition.Status == model.StatusClaimed && r.shouldReclaimPartition(*partition) {
			if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
				r.logger.Infof("重新获取超时的已认领分区 %d，上次更新时间: %v",
					partition.PartitionID, partition.UpdatedAt)
				return *acquired, nil
			}
		}
	}

	// 3. 最后才尝试获取可能卡住的Running状态分区
	for _, partition := range allPartitions {
		if partition.Status == model.StatusRunning && r.shouldReclaimPartition(*partition) {
			if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
				r.logger.Warnf("重新获取可能卡住的运行分区 %d，上次更新时间: %v，这可能导致重复处理",
					partition.PartitionID, partition.UpdatedAt)
				return *acquired, nil
			}
		}
	}

	return model.PartitionInfo{}, model.ErrNoAvailablePartition
}

// tryAcquirePartition 尝试获取指定的分区
// 使用策略接口的 AcquirePartition 方法
func (r *Runner) tryAcquirePartition(ctx context.Context, partitionID int) (*model.PartitionInfo, bool) {
	// 使用策略接口获取分区
	options := &model.AcquirePartitionOptions{
		AllowPreemption:   false, // 默认不允许抢占
		PreemptionTimeout: r.partitionLockExpiry,
	}

	partition, success, err := r.partitionStrategy.AcquirePartition(ctx, partitionID, r.workerID, options)
	if err != nil {
		r.logger.Warnf("获取分区 %d 失败: %v", partitionID, err)
		return nil, false
	}

	if success {
		r.logger.Infof("成功获取分区 %d", partitionID)
		return partition, true
	}

	return nil, false
}

// ------------- 分区状态判断相关函数 -------------

// shouldReclaimPartition 判断一个分区是否应该被重新获取
// 例如分区处于claimed/running状态但长时间未更新
func (r *Runner) shouldReclaimPartition(partition model.PartitionInfo) bool {
	// 检查分区状态
	switch partition.Status {
	case model.StatusClaimed:
		// 对于claimed状态，如果长时间未转为running，可能是之前的worker获取后异常退出
		staleThreshold := r.partitionLockExpiry * 2 // 对claimed状态使用较短的阈值
		timeSinceUpdate := time.Since(partition.UpdatedAt)

		// 如果分区更新时间太旧，并且不是本节点持有的，可以尝试重新获取
		if timeSinceUpdate > staleThreshold && partition.WorkerID != r.workerID {
			return true
		}

	case model.StatusRunning:
		// 对于running状态，使用更长的阈值，因为任务可能本身就需要较长处理时间
		staleThreshold := r.partitionLockExpiry * 5 // 对running状态使用更长的阈值
		timeSinceUpdate := time.Since(partition.UpdatedAt)

		// 只有在以下条件满足时才重新获取:
		// 1. 超过了更长的超时阈值
		// 2. 不是本节点持有的任务
		// 3. 没有心跳更新（心跳机制会定期更新UpdatedAt）
		if timeSinceUpdate > staleThreshold && partition.WorkerID != r.workerID {
			r.logger.Warnf("检测到可能卡住的运行分区 %d，上次更新时间: %v，超过了阈值 %v",
				partition.PartitionID, partition.UpdatedAt, staleThreshold)
			return true
		}
	}

	return false
}

// ------------- 分区状态更新相关函数 -------------

// updateTaskStatus 更新任务状态
// 使用策略接口的 UpdatePartitionStatus 方法
func (r *Runner) updateTaskStatus(ctx context.Context, task model.PartitionInfo, status model.PartitionStatus) error {
	return r.partitionStrategy.UpdatePartitionStatus(ctx, task.PartitionID, r.workerID, status, nil)
}

// releasePartitionLock 释放分区锁
// 使用策略接口的 ReleasePartition 方法
func (r *Runner) releasePartitionLock(ctx context.Context, partitionID int) {
	if err := r.partitionStrategy.ReleasePartition(ctx, partitionID, r.workerID); err != nil {
		r.logger.Warnf("释放分区 %d 失败: %v", partitionID, err)
	}
}

// maintainPartitionHold 维护分区持有权
// 使用策略接口的 MaintainPartitionHold 方法
func (r *Runner) maintainPartitionHold(ctx context.Context, partitionID int) error {
	return r.partitionStrategy.MaintainPartitionHold(ctx, partitionID, r.workerID)
}

// ------------- 分区信息查询相关函数 -------------

// getPartitionInfo 获取单个分区信息
func (r *Runner) getPartitionInfo(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	return r.partitionStrategy.GetPartition(ctx, partitionID)
}

// getAllPartitions 获取所有分区信息
func (r *Runner) getAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	return r.partitionStrategy.GetAllPartitions(ctx)
}

// getAvailablePartitions 获取可用的分区
func (r *Runner) getAvailablePartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	filters := model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending},
	}
	return r.partitionStrategy.GetFilteredPartitions(ctx, filters)
}

// getPartitionStats 获取分区状态统计信息
func (r *Runner) getPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	return r.partitionStrategy.GetPartitionStats(ctx)
}
