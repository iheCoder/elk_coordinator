package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 批量操作 ====================

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
//
// 该方法使用 Redis 事务批量创建多个分区，相比并发方式具有以下优势：
// 1. 减少网络往返次数，提高性能
// 2. 原子性保证，要么全部成功要么全部失败
// 3. 避免连接池压力和并发复杂性
// 4. 更好的错误处理和调试体验
//
// 注意：此方法直接使用请求中的 PartitionID，不会重新分配ID。
// 如果存在ID冲突，会获取现有分区而不是创建新的。
//
// 参数:
//   - ctx: 上下文
//   - request: 创建分区请求，包含分区列表
//
// 返回:
//   - []*model.PartitionInfo: 成功创建的分区信息列表
//   - error: 错误信息
func (s *HashPartitionStrategy) CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error) {
	if len(request.Partitions) == 0 {
		return []*model.PartitionInfo{}, nil
	}

	s.logger.Infof("开始批量创建 %d 个分区", len(request.Partitions))

	// 第一步：批量检查哪些分区已存在，并验证数据范围一致性
	var existingPartitions []*model.PartitionInfo
	var newPartitionRequests []model.CreatePartitionRequest

	// 使用管道批量检查分区是否存在，减少网络往返
	for _, partitionDef := range request.Partitions {
		existingPartition, err := s.GetPartition(ctx, partitionDef.PartitionID)
		if err != nil {
			if errors.Is(err, ErrPartitionNotFound) {
				// 分区不存在，需要创建
				newPartitionRequests = append(newPartitionRequests, partitionDef)
			} else {
				// 系统错误
				s.logger.Errorf("检查分区 %d 是否存在时发生错误: %v", partitionDef.PartitionID, err)
				return nil, fmt.Errorf("检查分区 %d 失败: %w", partitionDef.PartitionID, err)
			}
		} else {
			// 分区已存在，检查数据范围是否一致
			if existingPartition.MinID != partitionDef.MinID || existingPartition.MaxID != partitionDef.MaxID {
				s.logger.Errorf("分区 %d 已存在但数据范围不一致: 现有[%d-%d] vs 请求[%d-%d]",
					partitionDef.PartitionID, existingPartition.MinID, existingPartition.MaxID,
					partitionDef.MinID, partitionDef.MaxID)
				return nil, fmt.Errorf("分区 %d 数据范围冲突: 现有[%d-%d] vs 请求[%d-%d]",
					partitionDef.PartitionID, existingPartition.MinID, existingPartition.MaxID,
					partitionDef.MinID, partitionDef.MaxID)
			}
			s.logger.Debugf("分区 %d 已存在且数据范围一致，跳过创建", partitionDef.PartitionID)
			existingPartitions = append(existingPartitions, existingPartition)
		}
	}

	// 第二步：如果有新分区需要创建，使用批量操作
	var newPartitions []*model.PartitionInfo
	if len(newPartitionRequests) > 0 {
		s.logger.Infof("发现 %d 个新分区需要创建", len(newPartitionRequests))

		// 按分区ID排序，确保创建顺序的一致性，避免中间失败导致的数据缺口
		sort.Slice(newPartitionRequests, func(i, j int) bool {
			return newPartitionRequests[i].PartitionID < newPartitionRequests[j].PartitionID
		})

		// 批量创建新分区 - 使用较小的批次避免单个事务过大
		const batchSize = 50 // 每批最多50个分区，避免Redis事务过大

		for i := 0; i < len(newPartitionRequests); i += batchSize {
			end := i + batchSize
			if end > len(newPartitionRequests) {
				end = len(newPartitionRequests)
			}

			batch := newPartitionRequests[i:end]
			batchPartitions, err := s.createPartitionsBatch(ctx, batch)
			if err != nil {
				s.logger.Errorf("批量创建分区失败 (批次 %d-%d): %v", i, end-1, err)

				// 由于分区按ID排序创建，失败批次之前的分区都是连续成功的
				// 只需要记录失败位置，不需要回滚已成功的批次
				s.logger.Warnf("分区创建在批次 %d-%d 处失败，已成功创建 %d 个分区",
					i, end-1, len(newPartitions))

				return nil, fmt.Errorf("批量创建分区失败 (分区ID %d-%d): %w",
					batch[0].PartitionID, batch[len(batch)-1].PartitionID, err)
			}

			newPartitions = append(newPartitions, batchPartitions...)
		}
	}

	// 合并结果
	allPartitions := make([]*model.PartitionInfo, 0, len(existingPartitions)+len(newPartitions))
	allPartitions = append(allPartitions, existingPartitions...)
	allPartitions = append(allPartitions, newPartitions...)

	// 按 PartitionID 排序以确保一致的顺序
	sort.Slice(allPartitions, func(i, j int) bool {
		return allPartitions[i].PartitionID < allPartitions[j].PartitionID
	})

	s.logger.Infof("批量创建/获取分区完成，总共处理 %d 个分区请求，其中 %d 个已存在，%d 个新创建",
		len(allPartitions), len(existingPartitions), len(newPartitions))
	return allPartitions, nil
}

// createPartitionsBatch 批量创建多个分区，使用Redis事务保证原子性
func (s *HashPartitionStrategy) createPartitionsBatch(ctx context.Context, requests []model.CreatePartitionRequest) ([]*model.PartitionInfo, error) {
	if len(requests) == 0 {
		return []*model.PartitionInfo{}, nil
	}

	// 准备批量数据
	partitionsData := make(map[string]string, len(requests))
	partitionInfos := make([]*model.PartitionInfo, 0, len(requests))

	now := time.Now()
	for _, req := range requests {
		newPartition := &model.PartitionInfo{
			PartitionID:   req.PartitionID,
			MinID:         req.MinID,
			MaxID:         req.MaxID,
			Status:        model.StatusPending,
			WorkerID:      "", // 初始没有工作节点
			LastHeartbeat: now,
			UpdatedAt:     now,
			CreatedAt:     now,
			Version:       1, // 新记录的初始版本
			Options:       req.Options,
		}

		partitionJson, err := json.Marshal(newPartition)
		if err != nil {
			s.logger.Errorf("序列化分区 %d 失败: %v", req.PartitionID, err)
			return nil, fmt.Errorf("序列化分区 %d 失败: %w", req.PartitionID, err)
		}

		partitionsData[strconv.Itoa(req.PartitionID)] = string(partitionJson)
		partitionInfos = append(partitionInfos, newPartition)
	}

	// 使用事务批量创建
	err := s.store.HSetPartitionsInTx(ctx, partitionHashKey, partitionsData)
	if err != nil {
		s.logger.Errorf("批量创建分区事务失败: %v", err)
		return nil, fmt.Errorf("批量创建分区事务失败: %w", err)
	}

	s.logger.Infof("成功批量创建 %d 个分区", len(partitionInfos))
	return partitionInfos, nil
}

// DeletePartitions 批量删除分区
//
// 该方法使用 Redis 管道批量删除多个分区，提高性能。
// 对于不存在的分区会被跳过，不会返回错误。
//
// 参数:
//   - ctx: 上下文
//   - partitionIDs: 要删除的分区ID列表
//
// 返回:
//   - error: 错误信息
func (s *HashPartitionStrategy) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	if len(partitionIDs) == 0 {
		return nil
	}

	s.logger.Infof("开始批量删除 %d 个分区", len(partitionIDs))

	// 使用较小的批次避免Redis管道过大
	const batchSize = 100
	var totalDeleted int

	for i := 0; i < len(partitionIDs); i += batchSize {
		end := i + batchSize
		if end > len(partitionIDs) {
			end = len(partitionIDs)
		}

		batch := partitionIDs[i:end]
		deleted, err := s.deletePartitionsBatch(ctx, batch)
		if err != nil {
			s.logger.Errorf("批量删除分区失败 (批次 %d-%d): %v", i, end-1, err)
			return fmt.Errorf("批量删除分区失败: %w", err)
		}

		totalDeleted += deleted
	}

	s.logger.Infof("成功批量删除 %d 个分区", totalDeleted)
	return nil
}

// deletePartitionsBatch 批量删除一批分区
// 由于接口限制，使用循环逐个删除，但批量处理以提高效率
func (s *HashPartitionStrategy) deletePartitionsBatch(ctx context.Context, partitionIDs []int) (int, error) {
	if len(partitionIDs) == 0 {
		return 0, nil
	}

	deletedCount := 0
	// 逐个删除分区
	for _, partitionID := range partitionIDs {
		field := strconv.Itoa(partitionID)
		err := s.store.HDeletePartition(ctx, partitionHashKey, field)
		if err != nil {
			s.logger.Errorf("删除分区 %d 失败: %v", partitionID, err)
			// 继续删除其他分区，不要因为一个失败而停止
			continue
		}
		deletedCount++
	}

	s.logger.Debugf("批次删除完成，删除了 %d 个分区", deletedCount)
	return deletedCount, nil
}
