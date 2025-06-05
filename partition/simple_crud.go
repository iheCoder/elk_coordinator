package partition

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/utils"
)

// ==================== 基础CRUD操作 ====================

// GetPartition 获取单个分区
func (s *SimpleStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	return s.ensurePartitionExists(partitions, partitionID)
}

// GetAllPartitions 获取所有分区
func (s *SimpleStrategy) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	return s.convertToPartitionSlice(partitions), nil
}

// DeletePartition 删除单个分区
func (s *SimpleStrategy) DeletePartition(ctx context.Context, partitionID int) error {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	// 确保分区存在
	if _, err := s.ensurePartitionExists(partitions, partitionID); err != nil {
		return err
	}

	delete(partitions, partitionID)
	return s.savePartitionsInternal(ctx, partitions)
}

// GetFilteredPartitions 根据过滤器获取分区
func (s *SimpleStrategy) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	return s.applyPartitionFilters(partitions, filters), nil
}

// UpdatePartition 更新分区信息
// 注意：此方法假设调用者已经获得了适当的锁（通常是通过 AcquirePartition）
func (s *SimpleStrategy) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	if err := s.validatePartitionInfo(partitionInfo); err != nil {
		return nil, err
	}

	partitionID := partitionInfo.PartitionID

	// 获取最新的分区信息
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	if partitions == nil {
		partitions = make(map[int]model.PartitionInfo)
	}

	// 更新或创建分区
	updatedPartition := s.preparePartitionUpdate(partitions, partitionID, partitionInfo)

	partitions[updatedPartition.PartitionID] = updatedPartition

	if err := s.savePartitionsInternal(ctx, partitions); err != nil {
		return nil, err
	}

	return &updatedPartition, nil
}

// ==================== 辅助方法 ====================

// convertToPartitionSlice 将分区映射转换为分区切片
func (s *SimpleStrategy) convertToPartitionSlice(partitions map[int]model.PartitionInfo) []*model.PartitionInfo {
	result := make([]*model.PartitionInfo, 0, len(partitions))
	for _, partition := range partitions {
		p := partition // 避免循环变量问题
		result = append(result, &p)
	}
	return result
}

// preparePartitionUpdate 准备分区更新
func (s *SimpleStrategy) preparePartitionUpdate(partitions map[int]model.PartitionInfo, partitionID int, newPartitionInfo *model.PartitionInfo) model.PartitionInfo {
	existingPartition, exists := partitions[partitionID]

	if !exists {
		return s.createNewPartition(newPartitionInfo)
	}

	return s.updateExistingPartition(existingPartition, newPartitionInfo)
}

// createNewPartition 创建新分区
func (s *SimpleStrategy) createNewPartition(partitionInfo *model.PartitionInfo) model.PartitionInfo {
	now := utils.Now()

	newPartition := *partitionInfo
	if newPartition.CreatedAt.IsZero() {
		newPartition.CreatedAt = now
	}
	if newPartition.UpdatedAt.IsZero() {
		newPartition.UpdatedAt = now
	}
	newPartition.Version = 1

	return newPartition
}

// updateExistingPartition 更新现有分区
func (s *SimpleStrategy) updateExistingPartition(existing model.PartitionInfo, newInfo *model.PartitionInfo) model.PartitionInfo {
	updated := existing

	// 更新需要变化的字段
	s.updatePartitionFields(&updated, newInfo)

	// 更新时间戳和版本
	if !newInfo.UpdatedAt.IsZero() {
		updated.UpdatedAt = newInfo.UpdatedAt
	} else {
		updated.UpdatedAt = utils.Now()
	}

	// 保留原始创建时间
	updated.CreatedAt = existing.CreatedAt
	// 版本号递增
	updated.Version = existing.Version + 1

	return updated
}

// updatePartitionFields 更新分区字段
func (s *SimpleStrategy) updatePartitionFields(updated *model.PartitionInfo, newInfo *model.PartitionInfo) {
	// WorkerID
	if newInfo.WorkerID != updated.WorkerID {
		updated.WorkerID = newInfo.WorkerID
	}

	// Status
	if newInfo.Status != updated.Status {
		updated.Status = newInfo.Status
	}

	// MinID 和 MaxID
	if newInfo.MinID != updated.MinID {
		updated.MinID = newInfo.MinID
	}
	if newInfo.MaxID != updated.MaxID {
		updated.MaxID = newInfo.MaxID
	}

	// 更新Options
	s.updatePartitionOptions(updated, newInfo)
}

// updatePartitionOptions 更新分区选项
func (s *SimpleStrategy) updatePartitionOptions(updated *model.PartitionInfo, newInfo *model.PartitionInfo) {
	if newInfo.Options != nil {
		if updated.Options == nil {
			updated.Options = make(map[string]interface{})
		}
		for key, value := range newInfo.Options {
			updated.Options[key] = value
		}
	}
}
