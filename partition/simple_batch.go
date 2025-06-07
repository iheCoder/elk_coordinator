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
		// 检查数据范围是否一致
		if existingPartition.MinID == req.MinID && existingPartition.MaxID == req.MaxID {
			// 数据范围一致，直接返回现有分区
			return existingPartition
		}

		// 数据范围不一致，需要生成新的不重复ID并创建新分区
		s.logger.Warnf("分区 %d 已存在但数据范围不一致: 现有[%d-%d] vs 请求[%d-%d]，将重新分配新ID",
			req.PartitionID, existingPartition.MinID, existingPartition.MaxID, req.MinID, req.MaxID)

		newPartitionID := s.generateNextAvailablePartitionID(existingPartitions)
		s.logger.Infof("为数据范围[%d-%d]重新分配分区ID: %d -> %d",
			req.MinID, req.MaxID, req.PartitionID, newPartitionID)

		// 使用新的分区ID创建分区
		newReq := req
		newReq.PartitionID = newPartitionID
		newPartition := s.buildNewPartition(newReq, now)
		existingPartitions[newPartitionID] = newPartition

		return newPartition
	}

	// 创建新分区
	newPartition := s.buildNewPartition(req, now)
	existingPartitions[req.PartitionID] = newPartition

	return newPartition
}

// generateNextAvailablePartitionID 生成下一个可用的分区ID
func (s *SimpleStrategy) generateNextAvailablePartitionID(existingPartitions map[int]model.PartitionInfo) int {
	// 查找当前最大的分区ID
	maxPartitionID := 0
	for id := range existingPartitions {
		if id > maxPartitionID {
			maxPartitionID = id
		}
	}

	// 返回最大ID + 1
	return maxPartitionID + 1
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
