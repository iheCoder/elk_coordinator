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
	// 归档分区的存储键
	archivedPartitionsKey = "elk_archived" // 归档分区独立存储
)

// ==================== 分离式归档策略实现 ====================

// 注意：使用同步归档策略，确保"先归档再删除"的数据安全
// 不使用异步通道，避免归档失败导致的数据丢失风险

// archiveCompletedPartition 归档已完成的分区
// 直接使用 model.PartitionInfo，保持数据结构一致性
// 注意：这是完成状态分区的特定归档逻辑，未来可扩展支持其他归档触发条件
func (s *HashPartitionStrategy) archiveCompletedPartition(ctx context.Context, partition *model.PartitionInfo) error {
	return s.archivePartition(ctx, partition, "completed")
}

// archivePartition 通用的分区归档方法
// reason: 归档原因，用于日志记录和未来的归档策略扩展
func (s *HashPartitionStrategy) archivePartition(ctx context.Context, partition *model.PartitionInfo, reason string) error {
	// 序列化为 JSON（直接使用完整的 PartitionInfo 结构）
	partitionJson, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化归档分区 %d 失败: %w", partition.PartitionID, err)
	}

	// 存储到独立的归档分区 Hash 中
	err = s.store.HSetPartition(ctx, archivedPartitionsKey, strconv.Itoa(partition.PartitionID), string(partitionJson))
	if err != nil {
		return fmt.Errorf("存储归档分区 %d 失败: %w", partition.PartitionID, err)
	}

	s.logger.Infof("成功归档分区 %d (Worker: %s, 状态: %s, 原因: %s)",
		partition.PartitionID, partition.WorkerID, partition.Status, reason)

	return nil
}

// GetArchivedPartitionsCount 获取归档分区的数量
func (s *HashPartitionStrategy) GetArchivedPartitionsCount(ctx context.Context) (int, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, archivedPartitionsKey)
	if err != nil {
		return 0, fmt.Errorf("获取归档分区数量失败: %w", err)
	}
	return len(dataMap), nil
}

// GetArchivedPartition 从归档存储中获取分区信息
func (s *HashPartitionStrategy) GetArchivedPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	partitionJson, err := s.store.HGetPartition(ctx, archivedPartitionsKey, strconv.Itoa(partitionID))
	if err != nil {
		return nil, fmt.Errorf("获取归档分区 %d 失败: %w", partitionID, err)
	}

	var partition model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionJson), &partition); err != nil {
		return nil, fmt.Errorf("反序列化归档分区 %d 失败: %w", partitionID, err)
	}

	return &partition, nil
}

// GetAllCompletedPartitions 获取所有已完成分区
// 注意：这里仍然使用"completed"概念，因为从业务角度它查询的是已完成的分区
// 但底层使用通用的归档存储
func (s *HashPartitionStrategy) GetAllCompletedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	return s.GetAllArchivedPartitions(ctx)
}

// GetAllArchivedPartitions 获取所有归档分区
func (s *HashPartitionStrategy) GetAllArchivedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, archivedPartitionsKey)
	if err != nil {
		return nil, fmt.Errorf("获取所有归档分区失败: %w", err)
	}

	partitions := make([]*model.PartitionInfo, 0, len(dataMap))
	for field, value := range dataMap {
		var partition model.PartitionInfo
		if err := json.Unmarshal([]byte(value), &partition); err != nil {
			s.logger.Warnf("归档分区字段 %s 的 JSON 反序列化失败: %v", field, err)
			continue
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}
