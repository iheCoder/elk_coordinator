package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// ==================== 内部辅助方法 ====================

// getAllPartitionsInternal 内部方法：获取所有分区
func (s *SimpleStrategy) getAllPartitionsInternal(ctx context.Context) (map[int]model.PartitionInfo, error) {
	key := s.getPartitionsKey()

	partitionData, err := s.dataStore.GetPartitions(ctx, key)
	if err != nil {
		// 检查是否是键不存在的错误
		if errors.Is(err, data.ErrNotFound) {
			// 键不存在，返回空的分区映射
			return make(map[int]model.PartitionInfo), nil
		}
		return nil, pkgerrors.Wrap(err, "获取分区数据失败")
	}

	var partitions map[int]model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionData), &partitions); err != nil {
		return nil, pkgerrors.Wrap(err, "解析分区数据失败")
	}

	if partitions == nil {
		partitions = make(map[int]model.PartitionInfo)
	}

	return partitions, nil
}

// savePartitionsInternal 内部方法：保存所有分区
func (s *SimpleStrategy) savePartitionsInternal(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	key := s.getPartitionsKey()

	partitionData, err := json.Marshal(partitions)
	if err != nil {
		return pkgerrors.Wrap(err, "序列化分区数据失败")
	}

	if err := s.dataStore.SetPartitions(ctx, key, string(partitionData)); err != nil {
		return pkgerrors.Wrap(err, "保存分区数据失败")
	}

	return nil
}

// applyPartitionFilters 应用分区过滤器
func (s *SimpleStrategy) applyPartitionFilters(partitions map[int]model.PartitionInfo, filters model.GetPartitionsFilters) []*model.PartitionInfo {
	var result []*model.PartitionInfo
	now := time.Now()

	for _, partition := range partitions {
		if s.shouldIncludePartition(partition, filters, now) {
			p := partition // 避免循环变量问题
			result = append(result, &p)

			// 限制数量
			if filters.Limit > 0 && len(result) >= filters.Limit {
				break
			}
		}
	}

	return result
}

// shouldIncludePartition 检查分区是否应该包含在过滤结果中
func (s *SimpleStrategy) shouldIncludePartition(partition model.PartitionInfo, filters model.GetPartitionsFilters, now time.Time) bool {
	// 状态过滤
	if !s.matchesStatusFilter(partition, filters.TargetStatuses) {
		return false
	}

	// 过时过滤
	if !s.matchesStaleFilter(partition, filters, now) {
		return false
	}

	// 分区ID范围过滤
	if !s.matchesIDRangeFilter(partition, filters) {
		return false
	}

	// 排除特定分区ID
	if s.isExcludedPartition(partition, filters.ExcludePartitionIDs) {
		return false
	}

	return true
}

// matchesStatusFilter 检查状态过滤器
func (s *SimpleStrategy) matchesStatusFilter(partition model.PartitionInfo, targetStatuses []model.PartitionStatus) bool {
	if len(targetStatuses) == 0 {
		return true
	}

	for _, status := range targetStatuses {
		if partition.Status == status {
			return true
		}
	}
	return false
}

// matchesStaleFilter 检查过时过滤器
func (s *SimpleStrategy) matchesStaleFilter(partition model.PartitionInfo, filters model.GetPartitionsFilters, now time.Time) bool {
	if filters.StaleDuration == nil || *filters.StaleDuration <= 0 {
		return true
	}

	timeSinceUpdate := now.Sub(partition.UpdatedAt)
	// 只包含过时的分区
	if timeSinceUpdate <= *filters.StaleDuration {
		return false // 不过时，跳过
	}

	// 如果设置了排除工作节点，跳过该节点的分区
	if filters.ExcludeWorkerIDOnStale != "" && partition.WorkerID == filters.ExcludeWorkerIDOnStale {
		return false
	}

	return true
}

// matchesIDRangeFilter 检查ID范围过滤器
func (s *SimpleStrategy) matchesIDRangeFilter(partition model.PartitionInfo, filters model.GetPartitionsFilters) bool {
	if filters.MinID != nil && partition.PartitionID < *filters.MinID {
		return false
	}
	if filters.MaxID != nil && partition.PartitionID > *filters.MaxID {
		return false
	}
	return true
}

// isExcludedPartition 检查是否为排除的分区
func (s *SimpleStrategy) isExcludedPartition(partition model.PartitionInfo, excludeIDs []int) bool {
	for _, excludeID := range excludeIDs {
		if partition.PartitionID == excludeID {
			return true
		}
	}
	return false
}

// validateWorkerID 验证工作节点ID
func (s *SimpleStrategy) validateWorkerID(workerID string) error {
	if workerID == "" {
		return fmt.Errorf("工作节点ID不能为空")
	}
	return nil
}

// validatePartitionInfo 验证分区信息
func (s *SimpleStrategy) validatePartitionInfo(partitionInfo *model.PartitionInfo) error {
	if partitionInfo == nil {
		return fmt.Errorf("分区信息不能为空")
	}
	return nil
}

// ensurePartitionExists 确保分区存在
func (s *SimpleStrategy) ensurePartitionExists(partitions map[int]model.PartitionInfo, partitionID int) (*model.PartitionInfo, error) {
	partition, exists := partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("分区 %d 不存在", partitionID)
	}
	return &partition, nil
}

// validateWorkerPermission 验证工作节点权限
func (s *SimpleStrategy) validateWorkerPermission(partition model.PartitionInfo, workerID string, operation string) error {
	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 无权%s分区 %d，当前所有者：%s",
			workerID, operation, partition.PartitionID, partition.WorkerID)
	}
	return nil
}

// ==================== 键生成方法 ====================

// getPartitionsKey 获取分区存储键
func (s *SimpleStrategy) getPartitionsKey() string {
	return fmt.Sprintf("%s:partitions", s.namespace)
}

// getPartitionLockKey 获取分区锁键
func (s *SimpleStrategy) getPartitionLockKey(partitionID int) string {
	return fmt.Sprintf("%s:partition_lock:%d", s.namespace, partitionID)
}
