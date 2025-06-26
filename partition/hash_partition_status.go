package partition

import (
	"context"
	"fmt"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 状态管理操作 ====================

// UpdatePartitionStatus 更新分区状态
//
// 该方法允许分区的持有者更新分区状态和元数据。只有分区的当前持有者
// 才能更新状态，这确保了状态更新的安全性。
//
// 特别处理：当状态更新为 completed 时，会触发分离式归档策略：
// 1. 立即从 Active Layer 移除分区
// 2. 异步归档到 Archive Layer
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

	// 分离式归档策略：处理 completed 状态
	if status == model.StatusCompleted {
		return s.handleCompletedPartition(ctx, partition, workerID, metadata)
	}

	// 非 completed 状态的正常处理
	return s.updatePartitionNormalStatus(ctx, partition, status, metadata)
}

// handleCompletedPartition 处理 completed 状态分区的分离式归档
// 实现"先归档再删除"策略，确保数据安全
func (s *HashPartitionStrategy) handleCompletedPartition(ctx context.Context, partition *model.PartitionInfo, workerID string, metadata map[string]interface{}) error {
	// 1. 更新分区状态为 completed
	partition.Status = model.StatusCompleted

	// 2. 合并元数据到分区选项中
	if len(metadata) > 0 {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for key, value := range metadata {
			partition.Options[key] = value
		}
	}

	// 3. 更新分区的最后修改时间
	partition.UpdatedAt = time.Now()

	// 4. 先归档到 Archive Layer（同步操作，确保归档成功）
	if err := s.archiveCompletedPartition(ctx, partition); err != nil {
		s.logger.Errorf("归档分区 %d 失败: %v", partition.PartitionID, err)
		return fmt.Errorf("归档分区 %d 失败: %w", partition.PartitionID, err)
	}

	// 5. 归档成功后，才从 Active Layer 移除分区
	if err := s.DeletePartition(ctx, partition.PartitionID); err != nil {
		s.logger.Errorf("从活跃层移除分区 %d 失败: %v", partition.PartitionID, err)
		// 注意：此时归档已成功，即使删除失败，数据也是安全的
		// 可以考虑添加补偿机制，但不应该返回错误影响业务流程
		s.logger.Warnf("分区 %d 已成功归档，但从活跃层删除失败，存在数据重复", partition.PartitionID)
	}

	s.logger.Infof("分区 %d 已完成分离式归档处理 (Worker: %s, 状态: %s)",
		partition.PartitionID, workerID, partition.Status)

	return nil
}

// updatePartitionNormalStatus 更新非 completed 状态的分区
func (s *HashPartitionStrategy) updatePartitionNormalStatus(ctx context.Context, partition *model.PartitionInfo, status model.PartitionStatus, metadata map[string]interface{}) error {
	// 更新分区状态
	partition.Status = status

	// 如果状态是活跃状态，更新心跳时间
	if status == model.StatusClaimed || status == model.StatusRunning {
		partition.LastHeartbeat = time.Now()
	}

	// 特殊处理：如果状态为失败，检查metadata中是否有错误信息并设置到Error字段
	if status == model.StatusFailed && len(metadata) > 0 {
		if errorMsg, exists := metadata["error"]; exists {
			if errorStr, ok := errorMsg.(string); ok {
				partition.Error = errorStr
			}
		}
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
	_, err := s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		s.logger.Errorf("更新分区 %d 状态失败: %v", partition.PartitionID, err)
		return fmt.Errorf("更新分区 %d 状态失败: %w", partition.PartitionID, err)
	}

	s.logger.Infof("成功更新分区 %d 状态为 %s（工作节点: %s）", partition.PartitionID, status, partition.WorkerID)
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

	// 特殊处理：如果分区已经是 completed 状态，说明已经被归档，不需要释放操作
	if partition.Status == model.StatusCompleted {
		s.logger.Infof("分区 %d 已完成并归档，跳过释放操作（原持有者: %s）", partitionID, workerID)
		return nil
	}

	// 重置分区状态：失败的分区保持状态和WorkerID，其他状态重置为pending
	if partition.Status != model.StatusFailed {
		partition.Status = model.StatusPending
		// 对于非终态分区（pending, claimed, running），清空工作节点信息以便重新分配
		partition.WorkerID = ""
	}
	// 注意：对于失败的分区，保留 WorkerID 以记录哪个工作节点处理了该分区

	// 清空心跳时间（无论什么状态都清空，因为分区已不再活跃处理）
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
