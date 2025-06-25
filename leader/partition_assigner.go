package leader

import (
	"context"
	"github.com/iheCoder/elk_coordinator/metrics"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/partition"
	"github.com/iheCoder/elk_coordinator/utils"
	"sort"
	"time"

	"github.com/pkg/errors"
)

const (
	DefaultRangeSize = 1000_000
)

// PartitionRange 表示分区的数据范围
type PartitionRange struct {
	PartitionID int   // 分区ID
	MinID       int64 // 最小数据ID
	MaxID       int64 // 最大数据ID
}

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
	config               PartitionAssignerConfig
	strategy             partition.PartitionStrategy
	logger               utils.Logger
	planer               PartitionPlaner
	lastGapDetectionID   int              // 上次缺口检测时的最大分区ID
	knownPartitionRanges []PartitionRange // 已知的分区范围缓存
}

// NewPartitionAssigner 创建新的分区管理器
// 将依赖作为构造函数参数，配置作为单独的结构体
func NewPartitionAssigner(
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

// NewPartitionAssignerWithOptions 使用选项模式创建分区管理器
func NewPartitionAssignerWithOptions(
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
	start := time.Now() // 记录开始时间

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

	// 更新分区总数指标
	metrics.SetPartitionsTotal(float64(stats.Total))

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
			// 记录分区分配次数和耗时（只有在实际创建分区时）
			metrics.IncPartitionsAssigned()
			metrics.ObservePartitionAssignmentDuration(time.Since(start))
		} else {
			pm.logger.Debugf("未检测到分区缺口")
			// 不记录分配耗时，因为没有实际分配
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

	// 记录分区分配次数和耗时
	if len(createdPartitions) > 0 {
		metrics.IncPartitionsAssigned()
	}
	metrics.ObservePartitionAssignmentDuration(time.Since(start))

	return nil
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
	// 修复：计算实际需要处理的ID数量，并使用向上取整避免余数丢失
	totalIds := nextMaxID - lastAllocatedID
	// 使用向上取整，确保所有ID都被分区覆盖
	partitionCount := int((totalIds + partitionSize - 1) / partitionSize)
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

	// 安全的乘法计算，防止溢出
	rangeSize, err := pm.safeMultiply(partitionSize, int64(workerCount), workerPartitionMultiple)
	if err != nil {
		pm.logger.Warnf("计算ID探测范围时检测到溢出风险: %v，使用安全的备选计算", err)
		rangeSize = DefaultRangeSize
	}

	pm.logger.Debugf("ID探测范围计算: partitionSize=%d, activeWorkers=%d, effectiveWorkerCount=%d, workerPartitionMultiple=%d, rangeSize=%d",
		partitionSize, len(activeWorkers), workerCount, workerPartitionMultiple, rangeSize)

	return rangeSize, nil
}

// safeMultiply 安全的三个int64乘法，检查溢出
func (pm *PartitionAssigner) safeMultiply(a, b, c int64) (int64, error) {
	if a <= 0 || b <= 0 || c <= 0 {
		return 0, errors.New("乘法参数必须为正数")
	}

	result := a * b * c
	if result < 0 {
		return 0, errors.New("乘法溢出")
	}

	return result, nil
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

// DetectAndCreateGapPartitions 增量缺口检测
func (pm *PartitionAssigner) DetectAndCreateGapPartitions(ctx context.Context, activeWorkers []string, maxPartitionID int) ([]model.CreatePartitionRequest, error) {
	pm.logger.Debugf("执行增量缺口检测，当前最大分区ID: %d，上次检测ID: %d", maxPartitionID, pm.lastGapDetectionID)

	// 如果是第一次检测或者已知范围为空，进行完整检测
	if pm.lastGapDetectionID == 0 || len(pm.knownPartitionRanges) == 0 {
		return pm.fullGapDetection(ctx, maxPartitionID)
	}

	// 如果没有新分区，跳过检测
	if maxPartitionID <= pm.lastGapDetectionID {
		pm.logger.Debugf("没有新分区，跳过缺口检测")
		return nil, nil
	}

	// 增量检测：只获取新分区
	return pm.incrementalGapDetection(ctx, maxPartitionID)
}

// fullGapDetection 完整的缺口检测（首次检测或缓存重置时使用）
func (pm *PartitionAssigner) fullGapDetection(ctx context.Context, maxPartitionID int) ([]model.CreatePartitionRequest, error) {
	pm.logger.Debugf("执行完整缺口检测")

	// 获取所有分区
	allPartitions, err := pm.strategy.GetAllPartitions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "获取所有分区失败")
	}

	if len(allPartitions) == 0 {
		pm.logger.Debugf("没有现有分区，无法检测缺口")
		pm.lastGapDetectionID = maxPartitionID
		return nil, nil
	}

	// 重建分区范围缓存
	pm.rebuildPartitionRangeCache(allPartitions)

	// 检测缺口
	realGaps := pm.detectGapsBetweenPartitions(ctx, allPartitions)
	if len(realGaps) == 0 {
		pm.logger.Debugf("未检测到数据缺口")
		pm.lastGapDetectionID = maxPartitionID
		return nil, nil
	}

	// 创建缺口分区
	gapPartitions, err := pm.createGapPartitions(ctx, realGaps, maxPartitionID)
	if err != nil {
		return nil, err
	}

	pm.lastGapDetectionID = maxPartitionID
	return gapPartitions, nil
}

// incrementalGapDetection 增量缺口检测：只检查新分区
func (pm *PartitionAssigner) incrementalGapDetection(ctx context.Context, maxPartitionID int) ([]model.CreatePartitionRequest, error) {
	pm.logger.Debugf("执行增量缺口检测，检查分区ID %d 到 %d", pm.lastGapDetectionID+1, maxPartitionID)

	// 获取新分区
	minID := pm.lastGapDetectionID + 1
	filters := model.GetPartitionsFilters{
		MinID: &minID,
	}
	newPartitions, err := pm.strategy.GetFilteredPartitions(ctx, filters)
	if err != nil {
		pm.logger.Warnf("获取新分区失败，降级为完整检测: %v", err)
		return pm.fullGapDetection(ctx, maxPartitionID)
	}

	if len(newPartitions) == 0 {
		pm.logger.Debugf("没有新分区，跳过缺口检测")
		pm.lastGapDetectionID = maxPartitionID
		return nil, nil
	}

	pm.logger.Debugf("发现 %d 个新分区，检查缺口", len(newPartitions))

	// 更新分区范围缓存
	pm.updatePartitionRangeCache(newPartitions)

	// 检测新分区附近的缺口
	gaps := pm.detectGapsAroundNewPartitions(ctx, newPartitions)
	if len(gaps) == 0 {
		pm.logger.Debugf("新分区附近未检测到缺口")
		pm.lastGapDetectionID = maxPartitionID
		return nil, nil
	}

	// 创建缺口分区
	gapPartitions, err := pm.createGapPartitions(ctx, gaps, maxPartitionID)
	if err != nil {
		return nil, err
	}

	pm.lastGapDetectionID = maxPartitionID
	return gapPartitions, nil
}

// rebuildPartitionRangeCache 重建分区范围缓存
func (pm *PartitionAssigner) rebuildPartitionRangeCache(allPartitions []*model.PartitionInfo) {
	pm.knownPartitionRanges = make([]PartitionRange, 0, len(allPartitions))
	for _, p := range allPartitions {
		pm.knownPartitionRanges = append(pm.knownPartitionRanges, PartitionRange{
			PartitionID: p.PartitionID,
			MinID:       p.MinID,
			MaxID:       p.MaxID,
		})
	}

	// 按MinID排序，便于后续缺口检测
	sort.Slice(pm.knownPartitionRanges, func(i, j int) bool {
		return pm.knownPartitionRanges[i].MinID < pm.knownPartitionRanges[j].MinID
	})

	pm.logger.Debugf("重建分区范围缓存，共 %d 个分区", len(pm.knownPartitionRanges))
}

// updatePartitionRangeCache 更新分区范围缓存（增量添加新分区）
func (pm *PartitionAssigner) updatePartitionRangeCache(newPartitions []*model.PartitionInfo) {
	for _, p := range newPartitions {
		newRange := PartitionRange{
			PartitionID: p.PartitionID,
			MinID:       p.MinID,
			MaxID:       p.MaxID,
		}
		pm.knownPartitionRanges = append(pm.knownPartitionRanges, newRange)
	}

	// 重新排序
	sort.Slice(pm.knownPartitionRanges, func(i, j int) bool {
		return pm.knownPartitionRanges[i].MinID < pm.knownPartitionRanges[j].MinID
	})

	pm.logger.Debugf("更新分区范围缓存，新增 %d 个分区，总共 %d 个分区",
		len(newPartitions), len(pm.knownPartitionRanges))
}

// detectGapsAroundNewPartitions 检测新分区附近的缺口
func (pm *PartitionAssigner) detectGapsAroundNewPartitions(ctx context.Context, newPartitions []*model.PartitionInfo) []DataGap {
	var gaps []DataGap

	for _, newPartition := range newPartitions {
		// 查找新分区前后的相邻分区
		prevRange, nextRange := pm.findAdjacentRanges(newPartition.MinID, newPartition.MaxID, newPartition.PartitionID)

		// 检查前面的缺口
		if prevRange != nil && prevRange.MaxID+1 < newPartition.MinID {
			gapStart := prevRange.MaxID + 1
			gapEnd := newPartition.MinID - 1
			gap := DataGap{StartID: gapStart, EndID: gapEnd}
			gaps = append(gaps, gap)
			pm.logger.Debugf("发现新分区 %d 前面的缺口: [%d, %d]",
				newPartition.PartitionID, gapStart, gapEnd)

		}

		// 检查后面的缺口
		if nextRange != nil && newPartition.MaxID+1 < nextRange.MinID {
			gapStart := newPartition.MaxID + 1
			gapEnd := nextRange.MinID - 1
			gap := DataGap{StartID: gapStart, EndID: gapEnd}
			gaps = append(gaps, gap)
			pm.logger.Debugf("发现新分区 %d 后面的缺口: [%d, %d]",
				newPartition.PartitionID, gapStart, gapEnd)
		}
	}

	// 去重缺口
	return pm.deduplicateGaps(gaps)
}

// findAdjacentRanges 在缓存中查找指定范围的前后相邻分区
func (pm *PartitionAssigner) findAdjacentRanges(minID, maxID int64, currentPartitionID int) (*PartitionRange, *PartitionRange) {
	var prevRange, nextRange *PartitionRange

	for i := range pm.knownPartitionRanges {
		r := &pm.knownPartitionRanges[i]

		// 跳过当前分区自己
		if r.PartitionID == currentPartitionID {
			continue
		}

		// 查找前面最近的分区
		if r.MaxID < minID {
			if prevRange == nil || r.MaxID > prevRange.MaxID {
				prevRange = r
			}
		}

		// 查找后面最近的分区
		if r.MinID > maxID {
			if nextRange == nil || r.MinID < nextRange.MinID {
				nextRange = r
			}
		}
	}

	return prevRange, nextRange
}

// deduplicateGaps 去重相同的缺口
func (pm *PartitionAssigner) deduplicateGaps(gaps []DataGap) []DataGap {
	if len(gaps) <= 1 {
		return gaps
	}

	seen := make(map[DataGap]bool)
	var unique []DataGap

	for _, gap := range gaps {
		if !seen[gap] {
			seen[gap] = true
			unique = append(unique, gap)
		}
	}

	return unique
}

// createGapPartitions 为缺口创建分区（提取的公共方法）
func (pm *PartitionAssigner) createGapPartitions(ctx context.Context, gaps []DataGap, maxPartitionID int) ([]model.CreatePartitionRequest, error) {
	// 获取分区大小，只调用一次
	partitionSize, err := pm.GetEffectivePartitionSize(ctx)
	if err != nil {
		pm.logger.Warnf("获取分区大小失败: %v，使用默认值", err)
		partitionSize = model.DefaultPartitionSize
	}

	// 为检测到的每个缺口创建分区请求
	var gapPartitions []model.CreatePartitionRequest
	currentPartitionID := maxPartitionID + 1

	for _, gap := range gaps {
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

		// 如果两个分区之间有空隙，构建缺口
		if current.MaxID+1 < next.MinID {
			gapStart := current.MaxID + 1
			gapEnd := next.MinID - 1

			gap := DataGap{
				StartID: gapStart,
				EndID:   gapEnd,
			}
			gaps = append(gaps, gap)
			pm.logger.Debugf("发现分区间数据缺口: [%d, %d]", gap.StartID, gap.EndID)
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
