package partition

import (
	"context"
	"fmt"
	"time"

	"elk_coordinator/model"
)

// ==================== 状态管理操作 ====================

// UpdatePartitionStatus 更新分区状态
//
// 该方法允许分区的持有者更新分区状态和元数据。只有分区的当前持有者
// 才能更新状态，这确保了状态更新的安全性。
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//   - workerID: 工作节点ID，必须是分区的当前持有者
//   - status: 新的分区状态
//   - metadata: 要合并到分区选项中的元数据
//
// 返回:
//   - error: 错误信息
func (s *HashPartitionStrategy) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	if workerID == "" {
		return fmt.Errorf("workerID 不能为空")
	}

	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		s.logger.Errorf("更新分区 %d 状态时获取分区信息失败: %v", partitionID, err)
		return fmt.Errorf("获取分区 %d 信息失败: %w", partitionID, err)
	}

	// 验证权限：只有分区的持有者才能更新状态
	if partition.WorkerID != workerID {
		s.logger.Warnf("工作节点 %s 尝试更新分区 %d 状态，但该分区属于 %s", workerID, partitionID, partition.WorkerID)
		return fmt.Errorf("工作节点 %s 无权更新分区 %d 状态", workerID, partitionID)
	}

	// 更新分区状态
	partition.Status = status

	// 如果状态是活跃状态，更新心跳时间
	if status == model.StatusClaimed || status == model.StatusRunning {
		partition.LastHeartbeat = time.Now()
	}

	// 如果有元数据，合并到分区选项中
	if len(metadata) > 0 {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for key, value := range metadata {
			partition.Options[key] = value
		}
	}

	// 使用版本控制更新
	_, err = s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		s.logger.Errorf("更新分区 %d 状态失败: %v", partitionID, err)
		return fmt.Errorf("更新分区 %d 状态失败: %w", partitionID, err)
	}

	s.logger.Infof("成功更新分区 %d 状态为 %s（工作节点: %s）", partitionID, status, workerID)
	return nil
}

// ReleasePartition 释放分区
//
// 该方法允许分区的持有者释放分区的持有权。释放后的分区状态会根据
// 当前状态进行相应调整：completed状态保持不变，其他状态重置为pending。
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//   - workerID: 工作节点ID，必须是分区的当前持有者
//
// 返回:
//   - error: 错误信息
func (s *HashPartitionStrategy) ReleasePartition(ctx context.Context, partitionID int, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("workerID 不能为空")
	}

	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		s.logger.Errorf("释放分区 %d 时获取分区信息失败: %v", partitionID, err)
		return fmt.Errorf("获取分区 %d 信息失败: %w", partitionID, err)
	}

	// 验证权限：只有分区的持有者才能释放
	if partition.WorkerID != workerID {
		s.logger.Warnf("工作节点 %s 尝试释放分区 %d，但该分区属于 %s", workerID, partitionID, partition.WorkerID)
		return fmt.Errorf("工作节点 %s 无权释放分区 %d", workerID, partitionID)
	}

	// 重置分区状态：已完成的分区保持completed状态，其他状态重置为pending
	if partition.Status != model.StatusCompleted {
		partition.Status = model.StatusPending
	}

	// 清空工作节点信息
	partition.WorkerID = ""

	// 清空心跳时间
	partition.LastHeartbeat = time.Time{}

	// 使用版本控制更新
	_, err = s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		s.logger.Errorf("释放分区 %d 失败: %v", partitionID, err)
		return fmt.Errorf("释放分区 %d 失败: %w", partitionID, err)
	}

	s.logger.Infof("成功释放分区 %d（原持有者: %s）", partitionID, workerID)
	return nil
}
