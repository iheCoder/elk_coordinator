package leader

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"

	"github.com/pkg/errors"
)

// PartitionAssignerConfig 分区分配器的配置参数（只包含配置，不包含依赖）
type PartitionAssignerConfig struct {
	Namespace               string
	WorkerPartitionMultiple int64
	// 可以添加其他配置参数，如超时时间、重试次数等
	// RetryCount       int
	// AllocationTimeout time.Duration
}

// PartitionAssigner 分区分配器。负责分配和管理分区任务的执行
type PartitionAssigner struct {
	config   PartitionAssignerConfig
	strategy partition.PartitionStrategy
	logger   utils.Logger
	planer   PartitionPlaner
}

// NewPartitionManager 创建新的分区管理器
// 将依赖作为构造函数参数，配置作为单独的结构体
func NewPartitionManager(
	config PartitionAssignerConfig,
	strategy partition.PartitionStrategy,
	logger utils.Logger,
	planer PartitionPlaner,
) *PartitionAssigner {
	// 设置默认值
	if config.WorkerPartitionMultiple <= 0 {
		config.WorkerPartitionMultiple = model.DefaultWorkerPartitionMultiple
	}

	return &PartitionAssigner{
		config:   config,
		strategy: strategy,
		logger:   logger,
		planer:   planer,
	}
}

// 或者使用选项模式的变体（推荐用于复杂情况）
type PartitionAssignerOption func(*PartitionAssigner)

func WithLogger(logger utils.Logger) PartitionAssignerOption {
	return func(pa *PartitionAssigner) {
		pa.logger = logger
	}
}

func WithPlaner(planer PartitionPlaner) PartitionAssignerOption {
	return func(pa *PartitionAssigner) {
		pa.planer = planer
	}
}

// NewPartitionManagerWithOptions 使用选项模式创建分区管理器
func NewPartitionManagerWithOptions(
	config PartitionAssignerConfig,
	strategy partition.PartitionStrategy, // 必须依赖
	opts ...PartitionAssignerOption,
) *PartitionAssigner {
	pa := &PartitionAssigner{
		config:   config,
		strategy: strategy,
		logger:   utils.NewDefaultLogger(),
	}

	for _, opt := range opts {
		opt(pa)
	}

	return pa
}

// AllocatePartitions 分配工作分区
func (pm *PartitionAssigner) AllocatePartitions(ctx context.Context, activeWorkers []string) error {
	// 获取分区统计信息，判断是否需要分配新分区
	stats, err := pm.strategy.GetPartitionStats(ctx)
	if err != nil {
		return errors.Wrap(err, "获取分区统计信息失败")
	}

	// 检查 stats 是否为 nil
	if stats == nil {
		return errors.New("分区统计信息为空")
	}

	// 检查是否需要分配新的分区
	if !pm.ShouldAllocateNewPartitions(*stats) {
		pm.logger.Debugf("现有分区依然存在很多未处理，暂不分配新分区。完成率: %.2f%%", stats.CompletionRate*100)
		return nil
	}

	// 获取ID范围
	lastAllocatedID, nextMaxID, err := pm.GetProcessingRange(ctx, activeWorkers, pm.config.WorkerPartitionMultiple)
	if err != nil {
		return err
	}

	// 如果没有新的数据要分配，直接返回
	if nextMaxID <= lastAllocatedID {
		pm.logger.Debugf("没有新的数据需要分配，当前已分配的最大ID: %d", lastAllocatedID)
		return nil
	}

	// 创建新的分区请求
	createRequest, err := pm.CreatePartitionsRequest(ctx, lastAllocatedID, nextMaxID)
	if err != nil {
		return err
	}

	// 直接使用策略的批量创建方法
	createdPartitions, err := pm.strategy.CreatePartitionsIfNotExist(ctx, createRequest)
	if err != nil {
		return errors.Wrap(err, "创建分区失败")
	}

	pm.logger.Infof("成功创建 %d 个新分区，ID范围 [%d, %d]", len(createdPartitions), lastAllocatedID+1, nextMaxID)

	return nil
}

// GetProcessingRange 获取需要处理的ID范围
func (pm *PartitionAssigner) GetProcessingRange(ctx context.Context, activeWorkers []string, workerPartitionMultiple int64) (int64, int64, error) {
	// 获取当前已分配分区的最大ID边界
	lastAllocatedID, err := pm.GetLastAllocatedID(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取最后分配的ID边界失败")
	}

	// 计算合适的ID探测范围大小
	rangeSize, err := pm.CalculateLookAheadRange(ctx, activeWorkers, workerPartitionMultiple)
	if err != nil {
		pm.logger.Warnf("计算ID探测范围失败: %v，使用默认值", err)
		rangeSize = 10000 // 使用一个默认值作为备选
	}

	pm.logger.Debugf("使用动态计算的ID探测范围: %d", rangeSize)

	// 获取下一批次的最大ID
	nextMaxID, err := pm.planer.GetNextMaxID(ctx, lastAllocatedID, rangeSize)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取下一个最大ID失败")
	}

	return lastAllocatedID, nextMaxID, nil
}

// ShouldAllocateNewPartitions 判断是否应该分配新的分区
// 简化版本：主要确保不会一次性分配过多分区，不过度依赖完成率
func (pm *PartitionAssigner) ShouldAllocateNewPartitions(stats model.PartitionStats) bool {
	// 如果没有分区，应该分配
	if stats.Total == 0 {
		return true
	}

	// 如果有太多失败的分区，暂停分配新分区
	if stats.Failed > stats.Total/3 { // 失败率超过1/3
		pm.logger.Debugf("失败分区过多 (%d/%d)，暂停分配新分区", stats.Failed, stats.Total)
		return false
	}

	// 如果等待处理或正在运行的分区太多，不需要分配新分区
	// 简化逻辑：只要等待处理和正在运行的分区不超过总数的一半，就可以分配新分区
	if (stats.Pending + stats.Running) >= stats.Total/2 {
		pm.logger.Debugf("等待处理和正在运行的分区太多 (%d+%d=%d/%d)，暂停分配新分区",
			stats.Pending, stats.Running, stats.Pending+stats.Running, stats.Total)
		return false
	}

	// 简化判断：只要不是上述两种情况，就可以分配新分区
	pm.logger.Debugf("分区状态正常，可以分配新分区。总数=%d, 等待=%d, 运行中=%d, 完成=%d, 失败=%d",
		stats.Total, stats.Pending, stats.Running, stats.Completed, stats.Failed)
	return true
}

// CreatePartitionsRequest 创建分区请求
func (pm *PartitionAssigner) CreatePartitionsRequest(ctx context.Context, lastAllocatedID, nextMaxID int64) (model.CreatePartitionsRequest, error) {
	// 获取有效的分区大小（从处理器建议或默认值）
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		pm.logger.Warnf("获取有效分区大小失败: %v，使用默认值", err)
		partitionSize = model.DefaultPartitionSize
	}

	// 记录日志
	if partitionSize == model.DefaultPartitionSize {
		pm.logger.Debugf("使用默认分区大小: %d", partitionSize)
	} else {
		pm.logger.Infof("使用处理器建议的分区大小: %d", partitionSize)
	}

	// 根据分区大小计算分区数量
	totalIds := nextMaxID - lastAllocatedID
	partitionCount := int(totalIds / partitionSize)
	if partitionCount == 0 {
		partitionCount = 1 // 至少创建一个分区
	}

	pm.logger.Infof("ID范围 [%d, %d]，分区大小 %d，将创建 %d 个分区",
		lastAllocatedID+1, nextMaxID, partitionSize, partitionCount)

	// 创建分区请求
	partitions := make([]model.CreatePartitionRequest, partitionCount)
	for i := 0; i < partitionCount; i++ {
		minID := lastAllocatedID + int64(i)*partitionSize + 1
		maxID := lastAllocatedID + int64(i+1)*partitionSize

		// 最后一个分区处理到nextMaxID
		if i == partitionCount-1 {
			maxID = nextMaxID
		}

		partitions[i] = model.CreatePartitionRequest{
			PartitionID: i, // 这个ID会在策略层重新分配，避免冲突
			MinID:       minID,
			MaxID:       maxID,
			Options:     make(map[string]interface{}),
		}
	}

	return model.CreatePartitionsRequest{
		Partitions: partitions,
	}, nil
}

// GetLastAllocatedID 获取当前已分配分区的最大ID边界
func (pm *PartitionAssigner) GetLastAllocatedID(ctx context.Context) (int64, error) {
	allPartitions, err := pm.strategy.GetAllPartitions(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "获取分区信息失败")
	}

	// 如果没有分区，表示还没有开始分配
	if len(allPartitions) == 0 {
		return 0, nil
	}

	// 查找所有分区的最大ID
	var maxID int64 = 0
	for _, p := range allPartitions {
		if p.MaxID > maxID {
			maxID = p.MaxID
		}
	}

	return maxID, nil
}

// CalculateLookAheadRange 计算合适的ID探测范围大小
// 改进版本：使用活跃worker数量，但确保最小值，避免时序问题
func (pm *PartitionAssigner) CalculateLookAheadRange(ctx context.Context, activeWorkers []string, workerPartitionMultiple int64) (int64, error) {
	// 获取当前建议的分区大小
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		// 如果获取失败，使用默认值
		partitionSize = model.DefaultPartitionSize
	}

	// 计算活跃节点数量，但确保最小值，避免节点注册时序问题
	workerCount := len(activeWorkers)
	if workerCount < model.DefaultMinWorkerCount {
		workerCount = model.DefaultMinWorkerCount
	}

	// 基于分区大小、节点数量和配置的分区倍数计算合理的探测范围
	rangeSize := partitionSize * int64(workerCount) * workerPartitionMultiple

	pm.logger.Debugf("ID探测范围计算: partitionSize=%d, activeWorkers=%d, effectiveWorkerCount=%d, workerPartitionMultiple=%d, rangeSize=%d",
		partitionSize, len(activeWorkers), workerCount, workerPartitionMultiple, rangeSize)

	return rangeSize, nil
}

// GetEffectivePartitionSize 获取有效的分区大小（从处理器建议或默认值）
func (pm *PartitionAssigner) GetEffectivePartitionSize(ctx context.Context) (int64, error) {
	// 尝试从处理器获取建议的分区大小
	suggestedSize, err := pm.planer.PartitionSize(ctx)

	if err != nil || suggestedSize <= 0 {
		// 如果处理器没有建议分区大小，使用默认分区大小
		return model.DefaultPartitionSize, nil
	}

	return suggestedSize, nil
}
