package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 分离式归档策略常量 ====================

const (
	// 已完成分区的存储键
	completedPartitionsKey = "elk_completed" // 已完成分区独立存储
)

// ==================== 分离式归档策略实现 ====================

// 注意：使用同步归档策略，确保"先归档再删除"的数据安全
// 不使用异步通道，避免归档失败导致的数据丢失风险

// archiveCompletedPartition 归档已完成的分区
// 直接使用 model.PartitionInfo，保持数据结构一致性
func (s *HashPartitionStrategy) archiveCompletedPartition(ctx context.Context, partition *model.PartitionInfo) error {
	// 序列化为 JSON（直接使用完整的 PartitionInfo 结构）
	partitionJson, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化已完成分区 %d 失败: %w", partition.PartitionID, err)
	}

	// 存储到独立的已完成分区 Hash 中
	err = s.store.HSetPartition(ctx, completedPartitionsKey, strconv.Itoa(partition.PartitionID), string(partitionJson))
	if err != nil {
		return fmt.Errorf("存储已完成分区 %d 失败: %w", partition.PartitionID, err)
	}

	s.logger.Infof("成功归档分区 %d 到已完成分区存储 (Worker: %s, 状态: %s)",
		partition.PartitionID, partition.WorkerID, partition.Status)

	return nil
}

// GetCompletedPartitionsCount 获取已完成分区的数量（公开函数，供统计使用）
func (s *HashPartitionStrategy) GetCompletedPartitionsCount(ctx context.Context) (int, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, completedPartitionsKey)
	if err != nil {
		return 0, fmt.Errorf("获取已完成分区数量失败: %w", err)
	}
	return len(dataMap), nil
}

// GetCompletedPartition 从已完成分区存储中获取分区信息
func (s *HashPartitionStrategy) GetCompletedPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	partitionJson, err := s.store.HGetPartition(ctx, completedPartitionsKey, strconv.Itoa(partitionID))
	if err != nil {
		return nil, fmt.Errorf("获取已完成分区 %d 失败: %w", partitionID, err)
	}

	var partition model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionJson), &partition); err != nil {
		return nil, fmt.Errorf("反序列化已完成分区 %d 失败: %w", partitionID, err)
	}

	return &partition, nil
}

// GetAllCompletedPartitions 获取所有已完成分区
func (s *HashPartitionStrategy) GetAllCompletedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, completedPartitionsKey)
	if err != nil {
		return nil, fmt.Errorf("获取所有已完成分区失败: %w", err)
	}

	partitions := make([]*model.PartitionInfo, 0, len(dataMap))
	for field, value := range dataMap {
		var partition model.PartitionInfo
		if err := json.Unmarshal([]byte(value), &partition); err != nil {
			s.logger.Warnf("已完成分区字段 %s 的 JSON 反序列化失败: %v", field, err)
			continue
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}
