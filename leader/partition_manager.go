package leader

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"
	"github.com/pkg/errors"
	"time"
)

// PartitionManager 分区管理器，处理分区的创建、更新和状态管理
type PartitionManager struct {
	config PartitionManagerConfig
}

// PartitionManagerConfig 分区管理器配置
type PartitionManagerConfig struct {
	Namespace string
	Strategy  partition.PartitionStrategy
	Logger    utils.Logger
	Planer    PartitionPlaner
}

// 分区统计信息
type PartitionStats struct {
	Total          int     // 总分区数
	Pending        int     // 等待处理的分区数
	Running        int     // 正在处理的分区数
	Completed      int     // 已完成的分区数
	Failed         int     // 失败的分区数
	CompletionRate float64 // 完成率 (completed / total)
}

// NewPartitionManager 创建新的分区管理器
func NewPartitionManager(config PartitionManagerConfig) *PartitionManager {
	return &PartitionManager{
		config: config,
	}
}

// AllocatePartitions 分配工作分区
func (pm *PartitionManager) AllocatePartitions(ctx context.Context, activeWorkers []string, workerPartitionMultiple int64) error {
	// 首先检查现有分区状态
	existingPartitions, stats, err := pm.GetExistingPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "检查现有分区失败")
	}

	// 获取ID范围
	lastAllocatedID, nextMaxID, err := pm.GetProcessingRange(ctx, activeWorkers, workerPartitionMultiple)
	if err != nil {
		return err
	}

	// 如果没有新的数据要分配，直接返回
	if nextMaxID <= lastAllocatedID {
		pm.config.Logger.Debugf("没有新的数据需要分配，当前已分配的最大ID: %d", lastAllocatedID)
		return nil
	}

	// 检查是否需要分配新的分区
	if !pm.ShouldAllocateNewPartitions(stats) {
		pm.config.Logger.Debugf("现有分区未达到完成率阈值，暂不分配新分区。完成率: %.2f%%", stats.CompletionRate*100)
		return nil
	}

	// 创建新的分区，基于ID范围和建议的分区大小
	newPartitions, err := pm.CreatePartitions(ctx, lastAllocatedID, nextMaxID)
	if err != nil {
		return err
	}

	// 合并现有分区和新分区
	mergedPartitions := pm.MergePartitions(existingPartitions, newPartitions)

	// 保存合并后的分区到存储
	if err := pm.SavePartitionsToStorage(ctx, mergedPartitions); err != nil {
		return err
	}

	pm.config.Logger.Infof("成功创建 %d 个新分区，ID范围 [%d, %d]", len(newPartitions), lastAllocatedID+1, nextMaxID)

	return nil
}

// GetExistingPartitions 检查现有的分区状态
func (pm *PartitionManager) GetExistingPartitions(ctx context.Context) (map[int]model.PartitionInfo, PartitionStats, error) {
	allPartitions, err := pm.config.Strategy.GetAllPartitions(ctx)

	stats := PartitionStats{}
	partitions := make(map[int]model.PartitionInfo)

	if err != nil {
		return nil, stats, errors.Wrap(err, "获取分区信息失败")
	}

	// 将切片转换为映射，并统计状态
	stats.Total = len(allPartitions)
	for _, partition := range allPartitions {
		partitions[partition.PartitionID] = *partition

		switch partition.Status {
		case model.StatusPending:
			stats.Pending++
		case model.StatusClaimed, model.StatusRunning:
			stats.Running++
		case model.StatusCompleted:
			stats.Completed++
		case model.StatusFailed:
			stats.Failed++
		}
	}

	// 计算完成率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
	}

	return partitions, stats, nil
}

// GetProcessingRange 获取需要处理的ID范围
func (pm *PartitionManager) GetProcessingRange(ctx context.Context, activeWorkers []string, workerPartitionMultiple int64) (int64, int64, error) {
	// 获取当前已分配分区的最大ID边界
	lastAllocatedID, err := pm.GetLastAllocatedID(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取最后分配的ID边界失败")
	}

	// 计算合适的ID探测范围大小
	rangeSize, err := pm.CalculateLookAheadRange(ctx, activeWorkers, workerPartitionMultiple)
	if err != nil {
		pm.config.Logger.Warnf("计算ID探测范围失败: %v，使用默认值", err)
		rangeSize = 10000 // 使用一个默认值作为备选
	}

	pm.config.Logger.Debugf("使用动态计算的ID探测范围: %d", rangeSize)

	// 获取下一批次的最大ID
	nextMaxID, err := pm.config.Planer.GetNextMaxID(ctx, lastAllocatedID, rangeSize)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取下一个最大ID失败")
	}

	return lastAllocatedID, nextMaxID, nil
}

// ShouldAllocateNewPartitions 判断是否应该分配新的分区
func (pm *PartitionManager) ShouldAllocateNewPartitions(stats PartitionStats) bool {
	// 如果没有分区，应该分配
	if stats.Total == 0 {
		return true
	}

	// 如果有太多失败的分区，暂停分配新分区
	if stats.Failed > stats.Total/3 { // 失败率超过1/3
		return false
	}

	// 如果有足够的等待处理或正在处理的分区，不需要分配新分区
	if (stats.Pending + stats.Running) >= stats.Total/2 {
		return false
	}

	// 如果完成率达到70%，可以分配新分区
	return stats.CompletionRate >= 0.7
}

// CreatePartitions 创建分区信息
func (pm *PartitionManager) CreatePartitions(ctx context.Context, lastAllocatedID, nextMaxID int64) (map[int]model.PartitionInfo, error) {
	// 获取有效的分区大小（从处理器建议或默认值）
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		pm.config.Logger.Warnf("获取有效分区大小失败: %v，使用默认值", err)
		partitionSize = model.DefaultPartitionSize
	}

	// 记录日志
	if partitionSize == model.DefaultPartitionSize {
		pm.config.Logger.Debugf("使用默认分区大小: %d", partitionSize)
	} else {
		pm.config.Logger.Infof("使用处理器建议的分区大小: %d", partitionSize)
	}

	// 根据分区大小计算分区数量
	totalIds := nextMaxID - lastAllocatedID
	partitionCount := int(totalIds / partitionSize)
	if partitionCount == 0 {
		partitionCount = 1 // 至少创建一个分区
	}

	pm.config.Logger.Infof("ID范围 [%d, %d]，分区大小 %d，将创建 %d 个分区",
		lastAllocatedID+1, nextMaxID, partitionSize, partitionCount)

	partitions := make(map[int]model.PartitionInfo)
	for i := 0; i < partitionCount; i++ {
		minID := lastAllocatedID + int64(i)*partitionSize + 1
		maxID := lastAllocatedID + int64(i+1)*partitionSize

		// 最后一个分区处理到nextMaxID
		if i == partitionCount-1 {
			maxID = nextMaxID
		}

		partitions[i] = model.PartitionInfo{
			PartitionID: i,
			MinID:       minID,
			MaxID:       maxID,
			WorkerID:    "", // 空，等待被认领
			Status:      model.StatusPending,
			UpdatedAt:   time.Now(),
			Options:     make(map[string]interface{}),
		}
	}

	return partitions, nil
}

// MergePartitions 合并现有分区与新分区
func (pm *PartitionManager) MergePartitions(existingPartitions, newPartitions map[int]model.PartitionInfo) map[int]model.PartitionInfo {
	// 如果没有现有分区，直接返回新分区
	if len(existingPartitions) == 0 {
		return newPartitions
	}

	// 合并分区，找到最大的分区ID
	maxID := 0
	for id := range existingPartitions {
		if id > maxID {
			maxID = id
		}
	}

	// 为新分区分配新的ID，避免冲突
	mergedPartitions := make(map[int]model.PartitionInfo)
	for id, partition := range existingPartitions {
		mergedPartitions[id] = partition
	}

	// 添加新分区，确保ID不冲突
	for _, partition := range newPartitions {
		maxID++
		partition.PartitionID = maxID
		mergedPartitions[maxID] = partition
	}

	return mergedPartitions
}

// SavePartitionsToStorage 保存分区信息到存储
func (pm *PartitionManager) SavePartitionsToStorage(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	// 将分区映射转换为创建请求
	requests := make([]partition.CreatePartitionRequest, 0, len(partitions))
	for _, p := range partitions {
		requests = append(requests, partition.CreatePartitionRequest{
			PartitionID: p.PartitionID,
			MinID:       p.MinID,
			MaxID:       p.MaxID,
			Options:     p.Options,
		})
	}

	createRequest := partition.CreatePartitionsRequest{
		Partitions: requests,
	}

	// 使用策略的批量创建方法
	_, err := pm.config.Strategy.CreatePartitionsIfNotExist(ctx, createRequest)
	if err != nil {
		return errors.Wrap(err, "批量创建分区失败")
	}

	return nil
}

// GetLastAllocatedID 获取当前已分配分区的最大ID边界
func (pm *PartitionManager) GetLastAllocatedID(ctx context.Context) (int64, error) {
	partitions, _, err := pm.GetExistingPartitions(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "获取分区信息失败")
	}

	// 如果没有分区，表示还没有开始分配
	if len(partitions) == 0 {
		return 0, nil
	}

	// 查找所有分区的最大ID
	var maxID int64 = 0
	for _, p := range partitions {
		if p.MaxID > maxID {
			maxID = p.MaxID
		}
	}

	return maxID, nil
}

// CalculateLookAheadRange 计算合适的ID探测范围大小
func (pm *PartitionManager) CalculateLookAheadRange(ctx context.Context, activeWorkers []string, workerPartitionMultiple int64) (int64, error) {
	// 获取当前建议的分区大小
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		// 如果获取失败，使用默认值
		partitionSize = model.DefaultPartitionSize
	}

	// 计算活跃节点数量
	workerCount := len(activeWorkers)
	if workerCount == 0 {
		workerCount = 1 // 避免除以零
	}

	// 基于分区大小、节点数量和配置的分区倍数计算合理的探测范围
	// 为每个节点准备指定倍数的分区工作量
	rangeSize := partitionSize * int64(workerCount) * workerPartitionMultiple

	return rangeSize, nil
}

// GetEffectivePartitionSize 获取有效的分区大小（从处理器建议或默认值）
func (pm *PartitionManager) GetEffectivePartitionSize(ctx context.Context) (int64, error) {
	// 尝试从处理器获取建议的分区大小
	suggestedSize, err := pm.config.Planer.PartitionSize(ctx)

	if err != nil || suggestedSize <= 0 {
		// 如果处理器没有建议分区大小，使用默认分区大小
		return model.DefaultPartitionSize, nil
	}

	return suggestedSize, nil
}
