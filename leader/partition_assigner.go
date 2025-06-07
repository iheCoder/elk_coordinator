package leader

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"
	"sort"

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
	// 统计信息现在包含了 MaxPartitionID 和 LastAllocatedID，避免了额外的查询
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

	// 从统计信息中直接获取已计算好的边界值
	lastAllocatedID := stats.LastAllocatedID
	maxPartitionID := stats.MaxPartitionID

	// 获取下一批次的处理范围
	nextMaxID, err := pm.GetNextProcessingRange(ctx, activeWorkers, lastAllocatedID)
	if err != nil {
		return err
	}

	// 如果没有新的数据要分配，尝试检测并填补现有分区的缺口
	if nextMaxID <= lastAllocatedID {
		pm.logger.Debugf("没有新的数据需要分配，当前已分配的最大ID: %d", lastAllocatedID)

		// 尝试检测现有分区中的缺口
		gapPartitions, err := pm.DetectAndCreateGapPartitions(ctx, activeWorkers, maxPartitionID)
		if err != nil {
			return errors.Wrap(err, "检测分区缺口失败")
		}

		if len(gapPartitions) > 0 {
			pm.logger.Infof("检测到 %d 个分区缺口，已创建对应分区", len(gapPartitions))
		} else {
			pm.logger.Debugf("未检测到分区缺口")
		}

		return nil
	}

	// 创建新的分区请求
	createRequest, err := pm.CreatePartitionsRequestWithBounds(ctx, lastAllocatedID, nextMaxID, maxPartitionID)
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

// GetNextProcessingRange 获取下一批次的处理范围
func (pm *PartitionAssigner) GetNextProcessingRange(ctx context.Context, activeWorkers []string, lastAllocatedID int64) (int64, error) {
	// 计算合适的ID探测范围大小
	rangeSize, err := pm.CalculateLookAheadRange(ctx, activeWorkers, pm.config.WorkerPartitionMultiple)
	if err != nil {
		pm.logger.Warnf("计算ID探测范围失败: %v，使用默认值", err)
		rangeSize = 10000 // 使用一个默认值作为备选
	}

	pm.logger.Debugf("使用动态计算的ID探测范围: %d", rangeSize)

	// 获取下一批次的最大ID
	nextMaxID, err := pm.planer.GetNextMaxID(ctx, lastAllocatedID, rangeSize)
	if err != nil {
		return 0, errors.Wrap(err, "获取下一个最大ID失败")
	}

	return nextMaxID, nil
}

// CreatePartitionsRequestWithBounds 使用预计算的边界值创建分区请求
func (pm *PartitionAssigner) CreatePartitionsRequestWithBounds(ctx context.Context, lastAllocatedID, nextMaxID int64, maxPartitionID int) (model.CreatePartitionsRequest, error) {
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

	// 创建分区请求，使用真正不冲突的分区ID
	partitions := make([]model.CreatePartitionRequest, partitionCount)
	for i := 0; i < partitionCount; i++ {
		minID := lastAllocatedID + int64(i)*partitionSize + 1
		maxID := lastAllocatedID + int64(i+1)*partitionSize

		// 最后一个分区处理到nextMaxID
		if i == partitionCount-1 {
			maxID = nextMaxID
		}

		partitions[i] = model.CreatePartitionRequest{
			PartitionID: maxPartitionID + 1 + i, // 使用真正不冲突的分区ID
			MinID:       minID,
			MaxID:       maxID,
			Options:     make(map[string]interface{}),
		}
	}

	return model.CreatePartitionsRequest{
		Partitions: partitions,
	}, nil
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

// DataGap 表示数据范围中的缺口
type DataGap struct {
	StartID int64 // 缺口开始ID（包含）
	EndID   int64 // 缺口结束ID（包含）
}

// createPartitionsForGap 为指定的缺口创建分区请求（简化版）
func (pm *PartitionAssigner) createPartitionsForGap(ctx context.Context, gap DataGap, partitionSize int64, currentPartitionID *int) []model.CreatePartitionRequest {
	var partitions []model.CreatePartitionRequest

	gapSize := gap.EndID - gap.StartID + 1

	// 如果缺口小于等于分区大小，验证后创建一个分区
	if gapSize <= partitionSize {
		// 验证这个范围内是否真的有数据
		if pm.hasDataInRange(ctx, gap.StartID, gap.EndID) {
			p := model.CreatePartitionRequest{
				PartitionID: *currentPartitionID,
				MinID:       gap.StartID,
				MaxID:       gap.EndID,
				Options:     make(map[string]interface{}),
			}
			partitions = append(partitions, p)
			*currentPartitionID++
		}
		return partitions
	}

	// 将大缺口拆分成多个分区，每个分区都验证是否有数据
	currentID := gap.StartID
	for currentID <= gap.EndID {
		maxID := min(currentID+partitionSize-1, gap.EndID)

		// 只为有数据的范围创建分区
		if pm.hasDataInRange(ctx, currentID, maxID) {
			p := model.CreatePartitionRequest{
				PartitionID: *currentPartitionID,
				MinID:       currentID,
				MaxID:       maxID,
				Options:     make(map[string]interface{}),
			}
			partitions = append(partitions, p)
			*currentPartitionID++
		}

		currentID = maxID + 1
	}

	return partitions
}

// DetectAndCreateGapPartitions 检测并创建分区缺口
// 专注于检测现有分区之间是否有遗漏的数据范围
func (pm *PartitionAssigner) DetectAndCreateGapPartitions(ctx context.Context, activeWorkers []string, maxPartitionID int) ([]model.CreatePartitionRequest, error) {
	// 获取当前所有分区
	allPartitions, err := pm.strategy.GetAllPartitions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "获取所有分区失败")
	}

	if len(allPartitions) == 0 {
		pm.logger.Debugf("没有现有分区，无法检测缺口")
		return nil, nil
	}

	// 检测分区之间的数据缺口
	realGaps := pm.detectGapsBetweenPartitions(ctx, allPartitions)
	if len(realGaps) == 0 {
		pm.logger.Debugf("未检测到数据缺口")
		return nil, nil
	}

	// 获取分区大小，只调用一次
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		pm.logger.Warnf("获取分区大小失败: %v，使用默认值", err)
		partitionSize = model.DefaultPartitionSize
	}

	// 为检测到的每个缺口创建分区请求
	var gapPartitions []model.CreatePartitionRequest
	currentPartitionID := maxPartitionID + 1

	for _, gap := range realGaps {
		partitionsForGap := pm.createPartitionsForGap(ctx, gap, partitionSize, &currentPartitionID)
		gapPartitions = append(gapPartitions, partitionsForGap...)

		pm.logger.Infof("检测到数据缺口 [%d, %d]，创建 %d 个分区来填补",
			gap.StartID, gap.EndID, len(partitionsForGap))
	}

	// 批量创建缺口分区
	if len(gapPartitions) > 0 {
		_, err := pm.strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{Partitions: gapPartitions})
		if err != nil {
			return nil, errors.Wrap(err, "创建分区缺口失败")
		}

		pm.logger.Infof("成功创建 %d 个缺口分区", len(gapPartitions))
	}

	return gapPartitions, nil
}

// detectGapsBetweenPartitions 检测现有分区之间的数据缺口
func (pm *PartitionAssigner) detectGapsBetweenPartitions(ctx context.Context, allPartitions []*model.PartitionInfo) []DataGap {
	if len(allPartitions) < 2 {
		return nil
	}

	// 简单排序分区（按MinID）
	sortedPartitions := make([]*model.PartitionInfo, len(allPartitions))
	copy(sortedPartitions, allPartitions)

	// 使用标准库的sort进行排序，效率更高
	sort.Slice(sortedPartitions, func(i, j int) bool {
		return sortedPartitions[i].MinID < sortedPartitions[j].MinID
	})

	var gaps []DataGap

	// 检查相邻分区之间的缺口
	for i := 0; i < len(sortedPartitions)-1; i++ {
		current := sortedPartitions[i]
		next := sortedPartitions[i+1]

		// 如果两个分区之间有空隙，验证是否有真实数据
		if current.MaxID+1 < next.MinID {
			gapStart := current.MaxID + 1
			gapEnd := next.MinID - 1

			// 验证这个缺口中是否真的有数据
			if pm.hasDataInRange(ctx, gapStart, gapEnd) {
				gap := DataGap{
					StartID: gapStart,
					EndID:   gapEnd,
				}
				gaps = append(gaps, gap)
				pm.logger.Debugf("发现分区间数据缺口: [%d, %d]", gap.StartID, gap.EndID)
			}
		}
	}

	return gaps
}

// hasDataInRange 检查指定范围内是否存在真实数据
func (pm *PartitionAssigner) hasDataInRange(ctx context.Context, startID, endID int64) bool {
	if startID > endID {
		return false
	}

	// 使用GetNextMaxID检查从startID开始是否有数据
	// GetNextMaxID(startID, 1)会查询 id > startID limit 1，返回第一个 > startID 的数据ID
	firstDataID, err := pm.planer.GetNextMaxID(ctx, startID, 1)
	if err != nil {
		pm.logger.Warnf("检查数据范围 [%d, %d] 失败: %v", startID, endID, err)
		return false
	}

	// 如果返回的ID等于原始的startID，表示没有找到任何id > startID的数据
	if firstDataID <= startID {
		return false
	}

	// 如果找到的第一个数据ID在指定范围内，则存在数据
	return firstDataID <= endID
}

// min 返回两个int64的最小值
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
