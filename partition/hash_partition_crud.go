package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 基础 CRUD 操作 ====================

// GetPartition 通过其 ID 检索特定的分区。
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//
// 返回:
//   - *model.PartitionInfo: 分区信息
//   - error: 错误信息
func (s *HashPartitionStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	partitionJson, err := s.store.HGetPartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		if err == data.ErrNotFound {
			return nil, ErrPartitionNotFound
		}
		s.logger.Errorf("从存储中获取分区 %d 失败: %v", partitionID, err)
		return nil, fmt.Errorf("获取分区 %d 失败: %w", partitionID, err)
	}

	var partition model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionJson), &partition); err != nil {
		s.logger.Errorf("分区 %d 的 JSON 反序列化失败: %v", partitionID, err)
		return nil, fmt.Errorf("反序列化分区 %d 失败: %w", partitionID, err)
	}

	return &partition, nil
}

// GetAllPartitions 从存储中检索所有分区数据。
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - []*model.PartitionInfo: 所有分区信息的切片，按 PartitionID 升序排序
//   - error: 错误信息
func (s *HashPartitionStrategy) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, partitionHashKey)
	if err != nil {
		s.logger.Errorf("从存储中获取所有分区失败: %v", err)
		return nil, fmt.Errorf("获取所有分区失败: %w", err)
	}

	// 如果 dataMap 为空（键不存在或键存在但没有字段），这将正确地返回一个空切片。
	partitions := make([]*model.PartitionInfo, 0, len(dataMap))
	for field, value := range dataMap {
		var partition model.PartitionInfo
		if err := json.Unmarshal([]byte(value), &partition); err != nil {
			s.logger.Warnf("分区字段 %s 的 JSON 反序列化失败: %v", field, err)
			continue // 跳过损坏的数据
		}
		partitions = append(partitions, &partition)
	}

	// 按 PartitionID 排序以确保一致的顺序
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].PartitionID < partitions[j].PartitionID
	})

	return partitions, nil
}

// GetFilteredPartitions 根据提供的过滤器从存储中检索分区数据。
// 返回的分区将按 PartitionID 升序排序。
//
// 参数:
//   - ctx: 上下文
//   - filters: 过滤条件
//
// 返回:
//   - []*model.PartitionInfo: 符合条件的分区信息切片
//   - error: 错误信息
func (s *HashPartitionStrategy) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	// 首先获取所有分区
	allPartitions, err := s.GetAllPartitions(ctx)
	if err != nil {
		return nil, err
	}

	// 在循环外获取当前时间，以保持一致性
	now := time.Now()

	// 构建状态查找映射以提高效率
	var statusFilter map[model.PartitionStatus]bool
	if len(filters.TargetStatuses) > 0 {
		statusFilter = make(map[model.PartitionStatus]bool)
		for _, status := range filters.TargetStatuses {
			statusFilter[status] = true
		}
	}

	// 过滤分区
	var filteredPartitions []*model.PartitionInfo
	for _, partition := range allPartitions {
		// 状态过滤
		if statusFilter != nil && !statusFilter[partition.Status] {
			continue
		}

		// 心跳过期过滤 - 使用 StaleDuration 参数
		if filters.StaleDuration != nil {
			// 对于零或负持续时间，没有分区会被认为是过时的
			if *filters.StaleDuration <= 0 {
				continue // 跳过这个分区，因为没有分区可以满足零或负持续时间条件
			}

			isStale := now.Sub(partition.UpdatedAt) > *filters.StaleDuration
			isExcluded := filters.ExcludeWorkerIDOnStale != "" && partition.WorkerID == filters.ExcludeWorkerIDOnStale

			// 如果不过时，跳过
			if !isStale {
				continue
			}

			// 如果过时但被排除，也跳过
			if isExcluded {
				continue
			}
		}

		// ID 范围过滤
		if filters.MinID != nil && partition.PartitionID < *filters.MinID {
			continue
		}
		if filters.MaxID != nil && partition.PartitionID > *filters.MaxID {
			continue
		}

		// 排除指定分区
		excluded := false
		for _, excludeID := range filters.ExcludePartitionIDs {
			if partition.PartitionID == excludeID {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		filteredPartitions = append(filteredPartitions, partition)
	}

	// 应用限制
	if filters.Limit > 0 && len(filteredPartitions) > filters.Limit {
		filteredPartitions = filteredPartitions[:filters.Limit]
	}

	return filteredPartitions, nil
}

// DeletePartition 从存储中删除一个分区。
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 要删除的分区ID
//
// 返回:
//   - error: 错误信息
func (s *HashPartitionStrategy) DeletePartition(ctx context.Context, partitionID int) error {
	err := s.store.HDeletePartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		s.logger.Errorf("删除分区 %d 失败: %v", partitionID, err)
		return fmt.Errorf("删除分区 %d 失败: %w", partitionID, err)
	}

	s.logger.Infof("成功删除分区 %d", partitionID)
	return nil
}

// SavePartition 保存（创建或更新）一个分区。这是一个直接保存，没有乐观锁。
// 对于并发更新，请优先使用 UpdatePartition 或 CreatePartitionAtomically。
//
// 参数:
//   - ctx: 上下文
//   - partitionInfo: 要保存的分区信息
//
// 返回:
//   - error: 错误信息
//
// 注意: 此方法没有版本控制，在并发环境下可能导致数据丢失
func (s *HashPartitionStrategy) SavePartition(ctx context.Context, partitionInfo *model.PartitionInfo) error {
	if partitionInfo == nil {
		return fmt.Errorf("SavePartition 的 partitionInfo 不能为空")
	}

	// 对于直接保存，如果版本为 0，设置为 1 避免潜在问题
	if partitionInfo.Version == 0 {
		s.logger.Warnf("分区 %d 的版本为 0，自动设置为 1", partitionInfo.PartitionID)
		partitionInfo.Version = 1
	}

	// 更新时间戳 - 但保留已存在的 UpdatedAt（用于测试场景）
	if partitionInfo.UpdatedAt.IsZero() {
		partitionInfo.UpdatedAt = time.Now()
	}
	if partitionInfo.CreatedAt.IsZero() {
		partitionInfo.CreatedAt = partitionInfo.UpdatedAt
	}

	partitionJson, err := json.Marshal(partitionInfo)
	if err != nil {
		s.logger.Errorf("序列化分区 %d 失败: %v", partitionInfo.PartitionID, err)
		return fmt.Errorf("序列化分区 %d 失败: %w", partitionInfo.PartitionID, err)
	}

	err = s.store.HSetPartition(ctx, partitionHashKey, strconv.Itoa(partitionInfo.PartitionID), string(partitionJson))
	if err != nil {
		s.logger.Errorf("保存分区 %d 失败: %v", partitionInfo.PartitionID, err)
		return fmt.Errorf("保存分区 %d 失败: %w", partitionInfo.PartitionID, err)
	}

	s.logger.Infof("成功保存分区 %d (状态: %s, WorkerID: %s, 版本: %d)",
		partitionInfo.PartitionID, partitionInfo.Status, partitionInfo.WorkerID, partitionInfo.Version)
	return nil
}
