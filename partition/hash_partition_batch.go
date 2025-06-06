package partition

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"elk_coordinator/model"
)

// ==================== 批量操作 ====================

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
//
// 该方法会并发创建多个分区，提高性能。对于已存在的分区会被跳过，
// 不会返回错误。只有系统错误才会导致整个操作失败。
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

	// 并发创建分区以提高性能
	var wg sync.WaitGroup
	resultChan := make(chan *model.PartitionInfo, len(request.Partitions))
	errorChan := make(chan error, len(request.Partitions))

	for _, partitionDef := range request.Partitions {
		wg.Add(1)
		go func(def model.CreatePartitionRequest) {
			defer wg.Done()

			partition, err := s.CreatePartitionAtomically(ctx, def.PartitionID, def.MinID, def.MaxID, def.Options)
			if err != nil {
				if errors.Is(err, ErrPartitionAlreadyExists) {
					// 分区已存在，获取现有分区
					s.logger.Debugf("分区 %d 已存在，获取现有分区", def.PartitionID)
					existingPartition, getErr := s.GetPartition(ctx, def.PartitionID)
					if getErr != nil {
						errorChan <- fmt.Errorf("获取现有分区 %d 失败: %w", def.PartitionID, getErr)
						return
					}
					resultChan <- existingPartition
					return
				}
				// 其他错误是真正的系统错误
				errorChan <- fmt.Errorf("创建分区 %d 失败: %w", def.PartitionID, err)
				return
			}

			resultChan <- partition
		}(partitionDef)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// 收集结果
	var createdPartitions []*model.PartitionInfo
	var errors []error

	// 收集所有结果
	for partition := range resultChan {
		createdPartitions = append(createdPartitions, partition)
	}

	// 收集所有错误
	for err := range errorChan {
		errors = append(errors, err)
	}

	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		s.logger.Errorf("批量创建分区时遇到 %d 个错误，第一个错误: %v", len(errors), errors[0])
		return createdPartitions, errors[0]
	}

	// 按 PartitionID 排序以确保一致的顺序
	sort.Slice(createdPartitions, func(i, j int) bool {
		return createdPartitions[i].PartitionID < createdPartitions[j].PartitionID
	})

	s.logger.Infof("批量创建/获取分区完成，总共处理 %d 个分区请求，其中可能包含已存在的分区", len(createdPartitions))
	return createdPartitions, nil
}

// DeletePartitions 批量删除分区
//
// 该方法会并发删除多个分区，提高性能。对于不存在的分区会被跳过，
// 不会返回错误。只有系统错误才会导致操作失败。
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

	// 并发删除分区以提高性能
	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitionIDs))

	for _, partitionID := range partitionIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			err := s.DeletePartition(ctx, id)
			if err != nil {
				if errors.Is(err, ErrPartitionNotFound) {
					// 分区不存在，这是预期的情况，不是错误
					s.logger.Debugf("分区 %d 不存在，跳过删除", id)
					return
				}
				// 其他错误是真正的系统错误
				errorChan <- fmt.Errorf("删除分区 %d 失败: %w", id, err)
			}
		}(partitionID)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// 收集错误
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		s.logger.Errorf("批量删除分区时遇到 %d 个错误，第一个错误: %v", len(errors), errors[0])
		return errors[0]
	}

	s.logger.Infof("成功批量删除 %d 个分区", len(partitionIDs))
	return nil
}
