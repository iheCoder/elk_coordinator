package partition

import (
	"context"
	"errors"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/utils"

	pkgerrors "github.com/pkg/errors"
)

// ==================== 协调操作 ====================

// AcquirePartition 声明对指定分区的持有权
// 尝试获取指定分区的锁并声明持有权
// 这是一个针对特定分区的声明式操作
func (s *SimpleStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	// 调用内部实现
	partition, err := s.acquirePartitionInternal(ctx, partitionID, workerID, options)
	if err != nil {
		// 检查是否是正常的"无法获取"情况
		if s.isExpectedAcquisitionError(err) {
			// 这些是正常的"无法获取"情况，不返回错误
			return nil, false, nil
		}
		// 系统错误
		return nil, false, err
	}
	return partition, true, nil
}

// UpdatePartitionStatus 更新分区状态
// 注意：此方法假设调用者已经获得了分区的所有权（通常是通过 AcquirePartition）
func (s *SimpleStrategy) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	if err := s.validateWorkerID(workerID); err != nil {
		return err
	}

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	partition, err := s.ensurePartitionExists(partitions, partitionID)
	if err != nil {
		return err
	}

	// 验证工作节点权限
	if err := s.validateWorkerPermission(*partition, workerID, "更新"); err != nil {
		return err
	}

	// 执行状态更新
	updatedPartition := s.updatePartitionStatusAndMetadata(*partition, status, metadata)
	partitions[partitionID] = updatedPartition

	return s.savePartitionsInternal(ctx, partitions)
}

// ReleasePartition 释放分区
func (s *SimpleStrategy) ReleasePartition(ctx context.Context, partitionID int, workerID string) error {
	if err := s.validateWorkerID(workerID); err != nil {
		return err
	}

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	partition, err := s.ensurePartitionExists(partitions, partitionID)
	if err != nil {
		return err
	}

	// 验证工作节点权限
	if err := s.validateWorkerPermission(*partition, workerID, "释放"); err != nil {
		return err
	}

	// 释放持有锁
	s.releasePartitionLock(ctx, partitionID, workerID)

	// 重置分区状态
	releasedPartition := s.resetPartitionAfterRelease(*partition)
	partitions[partitionID] = releasedPartition

	return s.savePartitionsInternal(ctx, partitions)
}

// MaintainPartitionHold 维护对分区的持有权（续期分布式锁）
func (s *SimpleStrategy) MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error {
	if err := s.validateWorkerID(workerID); err != nil {
		return err
	}

	// 验证分区所有权
	if err := s.validatePartitionOwnership(ctx, partitionID, workerID); err != nil {
		return err
	}

	// 续期分布式锁
	return s.renewPartitionLock(ctx, partitionID, workerID)
}

// ==================== 内部协调方法 ====================

// acquirePartitionInternal 内部分区获取方法，支持抢占功能
func (s *SimpleStrategy) acquirePartitionInternal(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, error) {
	if err := s.validateWorkerID(workerID); err != nil {
		return nil, err
	}

	// 设置默认选项
	if options == nil {
		options = &model.AcquirePartitionOptions{}
	}

	// 获取指定分区
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return nil, ErrPartitionNotExists
	}

	// 检查是否可以获取分区
	acquisitionResult := s.checkPartitionAcquisition(partition, workerID, options)
	if acquisitionResult.ShouldSkip {
		return acquisitionResult.Partition, acquisitionResult.Error
	}

	// 处理重复声明的情况
	if acquisitionResult.IsReAcquisition {
		return s.handleReAcquisition(ctx, partition, workerID)
	}

	// 尝试获取分布式锁并更新分区
	return s.acquirePartitionLock(ctx, partitions, partition, workerID, options)
}

// checkPartitionAcquisition 检查分区获取条件
func (s *SimpleStrategy) checkPartitionAcquisition(partition model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) *acquisitionCheckResult {
	result := &acquisitionCheckResult{}

	// 检查分区是否可以被声明或抢占
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		if !options.AllowPreemption {
			result.ShouldSkip = true
			result.Error = ErrPartitionAlreadyHeld
			return result
		}
		// 如果允许抢占，继续处理
	}

	// 如果是同一个工作节点，标记为重新获取
	if partition.WorkerID == workerID {
		result.IsReAcquisition = true
		result.Partition = &partition
	}

	return result
}

// handleReAcquisition 处理重新获取分区的情况
func (s *SimpleStrategy) handleReAcquisition(ctx context.Context, partition model.PartitionInfo, workerID string) (*model.PartitionInfo, error) {
	// 检查锁是否仍然有效
	lockKey := s.getPartitionLockKey(partition.PartitionID)
	owned, err := s.dataStore.CheckLock(ctx, lockKey, workerID)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "检查分区锁失败")
	}

	if owned {
		// 锁仍然有效，直接返回当前分区信息
		s.logger.Infof("工作节点 %s 重复声明对分区 %d 的持有权（锁仍然有效）", workerID, partition.PartitionID)
		return &partition, nil
	}

	// 锁已失效，返回错误，让外层重新获取
	return nil, ErrPartitionLockFailed
}

// acquirePartitionLock 获取分区锁并更新分区
func (s *SimpleStrategy) acquirePartitionLock(ctx context.Context, partitions map[int]model.PartitionInfo, partition model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, error) {
	lockKey := s.getPartitionLockKey(partition.PartitionID)

	// 尝试获取分布式锁
	acquired, err := s.dataStore.AcquireLock(ctx, lockKey, workerID, s.partitionLockExpiry)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "获取分区锁失败")
	}

	if !acquired {
		return nil, ErrPartitionLockFailed
	}

	// 记录获取日志
	s.logPartitionAcquisition(partition, workerID, options)

	// 更新分区状态
	updatedPartition := s.updatePartitionAfterAcquisition(partition, workerID)
	partitions[partition.PartitionID] = updatedPartition

	// 保存更新后的分区信息
	if err := s.savePartitionsInternal(ctx, partitions); err != nil {
		// 如果保存失败，释放锁
		s.dataStore.ReleaseLock(ctx, lockKey, workerID)
		return nil, err
	}

	return &updatedPartition, nil
}

// validatePartitionOwnership 验证分区所有权
func (s *SimpleStrategy) validatePartitionOwnership(ctx context.Context, partitionID int, workerID string) error {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %w", err)
	}

	partition, err := s.ensurePartitionExists(partitions, partitionID)
	if err != nil {
		return err
	}

	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 没有持有分区 %d（当前持有者: %s）", workerID, partitionID, partition.WorkerID)
	}

	return nil
}

// renewPartitionLock 续期分区锁
func (s *SimpleStrategy) renewPartitionLock(ctx context.Context, partitionID int, workerID string) error {
	lockKey := s.getPartitionLockKey(partitionID)
	renewed, err := s.dataStore.RenewLock(ctx, lockKey, workerID, s.partitionLockExpiry)
	if err != nil {
		s.logger.Errorf("续期分区 %d 锁失败: %v", partitionID, err)
		return fmt.Errorf("续期分区锁失败: %w", err)
	}

	if !renewed {
		s.logger.Warnf("工作节点 %s 无法续期分区 %d 的锁（锁可能已过期或被其他节点持有）", workerID, partitionID)
		return fmt.Errorf("无法续期分区 %d 的锁", partitionID)
	}

	s.logger.Debugf("工作节点 %s 成功续期分区 %d 的锁", workerID, partitionID)
	return nil
}

// releasePartitionLock 释放分区锁
func (s *SimpleStrategy) releasePartitionLock(ctx context.Context, partitionID int, workerID string) {
	holdLockKey := s.getPartitionLockKey(partitionID)
	if err := s.dataStore.ReleaseLock(ctx, holdLockKey, workerID); err != nil {
		s.logger.Warnf("释放分区持有锁失败", "partitionID", partitionID, "workerID", workerID, "error", err)
	}
}

// ==================== 状态更新辅助方法 ====================

// updatePartitionStatusAndMetadata 更新分区状态和元数据
func (s *SimpleStrategy) updatePartitionStatusAndMetadata(partition model.PartitionInfo, status model.PartitionStatus, metadata map[string]interface{}) model.PartitionInfo {
	partition.Status = status
	partition.UpdatedAt = utils.Now()
	partition.Version++

	// 更新元数据
	if metadata != nil {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for key, value := range metadata {
			partition.Options[key] = value
		}
	}

	return partition
}

// resetPartitionAfterRelease 释放后重置分区状态
func (s *SimpleStrategy) resetPartitionAfterRelease(partition model.PartitionInfo) model.PartitionInfo {
	partition.WorkerID = ""
	// 重置分区状态：已完成的分区保持completed状态，其他状态重置为pending
	if partition.Status != model.StatusCompleted && partition.Status != model.StatusFailed {
		partition.Status = model.StatusPending
	}
	partition.UpdatedAt = utils.Now()
	partition.Version++

	return partition
}

// updatePartitionAfterAcquisition 获取后更新分区状态
func (s *SimpleStrategy) updatePartitionAfterAcquisition(partition model.PartitionInfo, workerID string) model.PartitionInfo {
	partition.WorkerID = workerID
	partition.Status = model.StatusClaimed
	partition.UpdatedAt = utils.Now()
	partition.Version++

	return partition
}

// logPartitionAcquisition 记录分区获取日志
func (s *SimpleStrategy) logPartitionAcquisition(partition model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) {
	if options.AllowPreemption && partition.WorkerID != "" && partition.WorkerID != workerID {
		s.logger.Infof("工作节点 %s 抢占分区 %d（原持有者: %s）", workerID, partition.PartitionID, partition.WorkerID)
	} else {
		s.logger.Infof("工作节点 %s 声明对分区 %d 的持有权", workerID, partition.PartitionID)
	}
}

// isExpectedAcquisitionError 检查是否是预期的获取错误
func (s *SimpleStrategy) isExpectedAcquisitionError(err error) bool {
	return errors.Is(err, ErrPartitionNotExists) ||
		errors.Is(err, ErrPartitionAlreadyHeld) ||
		errors.Is(err, ErrPartitionLockFailed)
}

// ==================== 内部数据结构 ====================

// acquisitionCheckResult 获取检查结果
type acquisitionCheckResult struct {
	ShouldSkip      bool                 // 是否应该跳过获取
	IsReAcquisition bool                 // 是否是重新获取
	Partition       *model.PartitionInfo // 分区信息
	Error           error                // 错误信息
}
