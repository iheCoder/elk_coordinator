// partition.go 包含所有与分区管理相关的函数
package task

import (
	"context"
	"elk_coordinator/model"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// ------------- 分区获取相关函数 -------------

// acquirePartitionTask 获取一个可用的分区任务
// 按照优先级顺序获取: pending -> claimed -> running
func (r *Runner) acquirePartitionTask(ctx context.Context) (model.PartitionInfo, error) {
	// 获取当前分区信息
	partitions, err := r.getPartitions(ctx)
	if err != nil {
		return model.PartitionInfo{}, errors.Wrap(err, "获取分区信息失败")
	}

	if len(partitions) == 0 {
		return model.PartitionInfo{}, model.ErrNoAvailablePartition
	}

	// 按优先级顺序获取分区
	// 1. 先尝试获取Pending状态的分区
	if partition, found := r.acquirePendingPartition(ctx, partitions); found {
		return partition, nil
	}

	// 2. 然后尝试获取可重新获取的Claimed状态分区
	if partition, found := r.acquireClaimedPartition(ctx, partitions); found {
		return partition, nil
	}

	// 3. 最后才尝试获取可重新获取的Running状态分区
	if partition, found := r.acquireRunningPartition(ctx, partitions); found {
		return partition, nil
	}

	return model.PartitionInfo{}, model.ErrNoAvailablePartition
}

// acquirePendingPartition 尝试获取pending状态的分区
func (r *Runner) acquirePendingPartition(
	ctx context.Context,
	partitions map[int]model.PartitionInfo,
) (model.PartitionInfo, bool) {
	for partitionID, partition := range partitions {
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			acquired, err := r.tryAcquirePartition(ctx, partitionID, partition, "", "")
			if err == nil {
				return acquired, true
			}
		}
	}
	return model.PartitionInfo{}, false
}

// acquireClaimedPartition 尝试获取可重新获取的claimed状态分区
func (r *Runner) acquireClaimedPartition(
	ctx context.Context,
	partitions map[int]model.PartitionInfo,
) (model.PartitionInfo, bool) {
	for partitionID, partition := range partitions {
		if partition.Status == model.StatusClaimed && r.shouldReclaimPartition(partition) {
			acquired, err := r.tryAcquirePartition(ctx, partitionID, partition,
				"重新获取超时的已认领分区 %d，上次更新时间: %v", "")
			if err == nil {
				return acquired, true
			}
		}
	}
	return model.PartitionInfo{}, false
}

// acquireRunningPartition 尝试获取可能卡住的running状态分区
func (r *Runner) acquireRunningPartition(
	ctx context.Context,
	partitions map[int]model.PartitionInfo,
) (model.PartitionInfo, bool) {
	for partitionID, partition := range partitions {
		if partition.Status == model.StatusRunning && r.shouldReclaimPartition(partition) {
			acquired, err := r.tryAcquirePartition(ctx, partitionID, partition,
				"", "重新获取可能卡住的运行分区 %d，上次更新时间: %v，这可能导致重复处理")
			if err == nil {
				return acquired, true
			}
		}
	}
	return model.PartitionInfo{}, false
}

// tryAcquirePartition 尝试获取指定的分区，处理锁获取和更新状态的逻辑
// infoFormat 和 warnFormat 是可选的日志格式，如果不为空则记录对应级别的日志
func (r *Runner) tryAcquirePartition(
	ctx context.Context,
	partitionID int,
	partition model.PartitionInfo,
	infoFormat string,
	warnFormat string,
) (model.PartitionInfo, error) {
	lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, r.namespace, partitionID)
	locked, err := r.acquirePartitionLock(ctx, lockKey, partitionID)
	if err != nil || !locked {
		return model.PartitionInfo{}, ErrPartitionLockFailed
	}

	// 根据提供的格式记录日志
	if infoFormat != "" {
		r.logger.Infof(infoFormat, partitionID, partition.UpdatedAt)
	}
	if warnFormat != "" {
		r.logger.Warnf(warnFormat, partitionID, partition.UpdatedAt)
	}

	// 重新获取该分区
	partition.WorkerID = r.workerID
	partition.Status = model.StatusClaimed // 总是重置为claimed状态
	partition.UpdatedAt = time.Now()

	if err := r.updatePartitionStatus(ctx, partition); err != nil {
		// 如果更新失败，释放锁
		r.dataStore.ReleaseLock(ctx, lockKey, r.workerID)
		return model.PartitionInfo{}, errors.Wrap(err, "更新分区状态失败")
	}

	return partition, nil
}

// ------------- 分区锁管理相关函数 -------------

// acquirePartitionLock 尝试获取分区锁
func (r *Runner) acquirePartitionLock(ctx context.Context, lockKey string, partitionID int) (bool, error) {
	success, err := r.dataStore.AcquireLock(ctx, lockKey, r.workerID, r.partitionLockExpiry)
	if err != nil {
		r.logger.Warnf("获取分区 %d 锁失败: %v", partitionID, err)
		return false, err
	}

	if success {
		r.logger.Infof("成功锁定分区 %d", partitionID)
		return true, nil
	}

	return false, nil
}

// releasePartitionLock 释放分区锁
func (r *Runner) releasePartitionLock(ctx context.Context, partitionID int) {
	lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, r.namespace, partitionID)
	if releaseErr := r.dataStore.ReleaseLock(ctx, lockKey, r.workerID); releaseErr != nil {
		r.logger.Warnf("释放分区 %d 锁失败: %v", partitionID, releaseErr)
	}
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

// ------------- 分区数据管理相关函数 -------------

// getPartitions 获取当前所有分区信息
func (r *Runner) getPartitions(ctx context.Context) (map[int]model.PartitionInfo, error) {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, r.namespace)
	partitionsData, err := r.dataStore.GetPartitions(ctx, partitionInfoKey)
	if err != nil {
		return nil, err
	}

	if partitionsData == "" {
		return nil, nil
	}

	var partitions map[int]model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionsData), &partitions); err != nil {
		return nil, errors.Wrap(err, "解析分区数据失败")
	}

	return partitions, nil
}

// updateTaskStatus 更新任务状态
func (r *Runner) updateTaskStatus(ctx context.Context, task model.PartitionInfo, status model.PartitionStatus) error {
	task.Status = status
	task.UpdatedAt = time.Now()
	return r.updatePartitionStatus(ctx, task)
}

// updatePartitionStatus 更新分区状态
func (r *Runner) updatePartitionStatus(ctx context.Context, task model.PartitionInfo) error {
	// 获取当前所有分区
	partitions, err := r.getPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "获取分区信息失败")
	}

	// 更新特定分区
	partitions[task.PartitionID] = task

	// 保存更新后的分区信息
	return r.savePartitions(ctx, partitions)
}

// savePartitions 保存分区信息到存储
func (r *Runner) savePartitions(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, r.namespace)
	updatedData, err := json.Marshal(partitions)
	if err != nil {
		return errors.Wrap(err, "编码更新后分区数据失败")
	}

	return r.dataStore.SetPartitions(ctx, partitionInfoKey, string(updatedData))
}
