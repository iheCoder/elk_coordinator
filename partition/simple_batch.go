package partition

import (
	"context"
	"elk_coordinator/model"
	"time"
)

// ==================== 批量操作 ====================

// DeletePartitions 批量删除分区
func (s *SimpleStrategy) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	if partitions == nil {
		return nil // 没有分区需要删除
	}

	// 批量删除分区
	for _, partitionID := range partitionIDs {
		delete(partitions, partitionID)
	}

	return s.savePartitionsInternal(ctx, partitions)
}

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
func (s *SimpleStrategy) CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error) {
	existingPartitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	if existingPartitions == nil {
		existingPartitions = make(map[int]model.PartitionInfo)
	}

	result := s.processBatchCreateRequest(existingPartitions, request)

	if err := s.savePartitionsInternal(ctx, existingPartitions); err != nil {
		return nil, err
	}

	return result, nil
}

// ==================== 辅助方法 ====================

// processBatchCreateRequest 处理批量创建请求
func (s *SimpleStrategy) processBatchCreateRequest(existingPartitions map[int]model.PartitionInfo, request model.CreatePartitionsRequest) []*model.PartitionInfo {
	var result []*model.PartitionInfo
	now := time.Now()

	for _, req := range request.Partitions {
		partition := s.processCreatePartitionRequest(existingPartitions, req, now)
		result = append(result, &partition)
	}

	return result
}

// processCreatePartitionRequest 处理单个创建分区请求
func (s *SimpleStrategy) processCreatePartitionRequest(existingPartitions map[int]model.PartitionInfo, req model.CreatePartitionRequest, now time.Time) model.PartitionInfo {
	// 检查是否已存在
	if existingPartition, exists := existingPartitions[req.PartitionID]; exists {
		return existingPartition
	}

	// 创建新分区
	newPartition := s.buildNewPartition(req, now)
	existingPartitions[req.PartitionID] = newPartition

	return newPartition
}

// buildNewPartition 构建新分区
func (s *SimpleStrategy) buildNewPartition(req model.CreatePartitionRequest, now time.Time) model.PartitionInfo {
	newPartition := model.PartitionInfo{
		PartitionID: req.PartitionID,
		MinID:       req.MinID,
		MaxID:       req.MaxID,
		WorkerID:    "",
		Status:      model.StatusPending,
		UpdatedAt:   now,
		CreatedAt:   now,
		Options:     req.Options,
		Version:     1,
	}

	if newPartition.Options == nil {
		newPartition.Options = make(map[string]interface{})
	}

	return newPartition
}
