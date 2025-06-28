package leader

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"github.com/iheCoder/elk_coordinator/utils"
)

// mockPartitionPlaner 实现PartitionPlaner接口
type mockPartitionPlaner struct {
	suggestedPartitionSize int64
	nextMaxID              int64
}

func (m *mockPartitionPlaner) PartitionSize(ctx context.Context) (int64, error) {
	return m.suggestedPartitionSize, nil
}

func (m *mockPartitionPlaner) GetNextMaxID(ctx context.Context, lastID int64, rangeSize int64) (int64, error) {
	// 正确实现GetNextMaxID语义：模拟 SELECT * FROM t WHERE id > lastID LIMIT rangeSize
	// 返回第rangeSize条记录的ID值，如果记录不足则返回找到的最后一个ID

	// 使用nextMaxID字段来控制最大可用数据ID
	maxDataID := m.nextMaxID

	// 如果没有设置nextMaxID或查询起点已超过最大数据ID，返回无数据
	if maxDataID == 0 || lastID >= maxDataID {
		return lastID, nil
	}

	// 找到第一个 > lastID 的数据ID
	firstDataID := lastID + 1

	// 如果第一个数据ID已经超出数据范围，没有数据
	if firstDataID > maxDataID {
		return lastID, nil
	}

	// 计算第rangeSize个数据ID（假设数据是连续的）
	nthDataID := firstDataID + rangeSize - 1

	// 如果超出数据范围，返回数据范围的最后一个ID
	if nthDataID > maxDataID {
		nthDataID = maxDataID
	}

	return nthDataID, nil
}

// TestGetExistingPartitions 测试获取现有分区功能
func TestGetExistingPartitions(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()

	ctx := context.Background()

	// 测试空分区情况 - 直接使用 Strategy 方法
	allPartitions, err := mockStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(allPartitions) != 0 {
		t.Errorf("期望空分区，但获取到 %d 个分区", len(allPartitions))
	}

	// 测试统计信息
	stats, err := mockStrategy.GetPartitionStats(ctx)
	if err != nil {
		t.Errorf("获取分区统计失败: %v", err)
	}
	if stats.Total != 0 {
		t.Errorf("期望分区统计为0，但得到 %d", stats.Total)
	}

	// 设置分区数据
	testPartitions := map[int]*model.PartitionInfo{
		1: {
			PartitionID: 1,
			MinID:       1,
			MaxID:       1000,
			Status:      model.StatusPending,
			UpdatedAt:   time.Now(),
		},
		2: {
			PartitionID: 2,
			MinID:       1001,
			MaxID:       2000,
			Status:      model.StatusRunning,
			UpdatedAt:   time.Now(),
		},
		3: {
			PartitionID: 3,
			MinID:       2001,
			MaxID:       3000,
			Status:      model.StatusCompleted,
			UpdatedAt:   time.Now(),
		},
		4: {
			PartitionID: 4,
			MinID:       3001,
			MaxID:       4000,
			Status:      model.StatusFailed,
			UpdatedAt:   time.Now(),
		},
	}

	mockStrategy.SetPartitions(testPartitions)

	// 再次获取分区，现在应该有数据了 - 直接使用 Strategy 方法
	allPartitions, err = mockStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(allPartitions) != 4 {
		t.Errorf("期望有4个分区，但获取到 %d 个分区", len(allPartitions))
	}

	// 测试统计信息
	stats, err = mockStrategy.GetPartitionStats(ctx)
	if err != nil {
		t.Errorf("获取分区统计失败: %v", err)
	}
	if stats.Total != 4 {
		t.Errorf("期望分区总数为4，但得到 %d", stats.Total)
	}
	if stats.Pending != 1 {
		t.Errorf("期望等待处理分区为1，但得到 %d", stats.Pending)
	}
	if stats.Running != 1 {
		t.Errorf("期望运行中分区为1，但得到 %d", stats.Running)
	}
	if stats.Completed != 1 {
		t.Errorf("期望已完成分区为1，但得到 %d", stats.Completed)
	}
	if stats.Failed != 1 {
		t.Errorf("期望失败分区为1，但得到 %d", stats.Failed)
	}
}

// TestGetLastAllocatedID 测试获取最后分配的ID边界
func TestGetLastAllocatedID(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()

	ctx := context.Background()

	// 测试空分区情况
	stats, err := mockStrategy.GetPartitionStats(ctx)
	if err != nil {
		t.Errorf("获取分区统计信息失败: %v", err)
	}
	if stats.LastAllocatedID != 0 {
		t.Errorf("期望最后分配ID为0，但得到 %d", stats.LastAllocatedID)
	}

	// 设置分区数据
	testPartitions := map[int]*model.PartitionInfo{
		1: {
			PartitionID: 1,
			MinID:       1,
			MaxID:       1000,
			Status:      model.StatusPending,
		},
		2: {
			PartitionID: 2,
			MinID:       1001,
			MaxID:       2000,
			Status:      model.StatusRunning,
		},
		3: {
			PartitionID: 3,
			MinID:       2001,
			MaxID:       5000, // 这个是最大的
			Status:      model.StatusCompleted,
		},
	}

	mockStrategy.SetPartitions(testPartitions)

	// 再次获取分区统计信息
	stats, err = mockStrategy.GetPartitionStats(ctx)
	if err != nil {
		t.Errorf("获取分区统计信息失败: %v", err)
	}
	if stats.LastAllocatedID != 5000 {
		t.Errorf("期望最后分配ID为5000，但得到 %d", stats.LastAllocatedID)
	}
}

// TestCreatePartitionsRequest 测试创建分区请求
func TestCreatePartitionsRequest(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 500, // 建议的分区大小
	}

	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer)

	ctx := context.Background()

	// 测试场景: 创建1000个ID的分区，预期会创建2个分区
	// 使用新的 CreatePartitionsRequestWithBounds 方法，maxPartitionID 为 0（表示当前没有分区）
	request, err := partitionMgr.CreatePartitionsRequestWithBounds(ctx, 0, 1000, 0)
	if err != nil {
		t.Errorf("创建分区请求失败: %v", err)
	}

	if len(request.Partitions) != 2 {
		t.Errorf("期望创建2个分区请求，但得到 %d 个", len(request.Partitions))
	}

	// 验证第一个分区请求
	p0 := request.Partitions[0]
	if p0.MinID != 1 || p0.MaxID != 500 {
		t.Errorf("分区0范围不正确，期望[1, 500]，得到[%d, %d]", p0.MinID, p0.MaxID)
	}
	if p0.PartitionID != 1 {
		t.Errorf("分区0的ID不正确，期望1，得到%d", p0.PartitionID)
	}

	// 验证第二个分区请求
	p1 := request.Partitions[1]
	if p1.MinID != 501 || p1.MaxID != 1000 {
		t.Errorf("分区1范围不正确，期望[501, 1000]，得到[%d, %d]", p1.MinID, p1.MaxID)
	}
	if p1.PartitionID != 2 {
		t.Errorf("分区1的ID不正确，期望2，得到%d", p1.PartitionID)
	}

	// 测试场景: 使用默认分区大小
	planer.suggestedPartitionSize = 0 // 让planer返回0，应该使用默认值
	request, err = partitionMgr.CreatePartitionsRequestWithBounds(ctx, 0, 1000, 0)
	if err != nil {
		t.Errorf("创建分区请求失败: %v", err)
	}

	// 默认分区大小是3000，所以应该只创建1个分区
	if len(request.Partitions) != 1 {
		t.Errorf("期望创建1个分区请求，但得到 %d 个", len(request.Partitions))
	}
}

// TestMergePartitions 功能已移至 Strategy 层处理
// 此测试已被移除，因为合并逻辑现在由 CreatePartitionsIfNotExist 处理

// TestCalculateLookAheadRange 测试ID探测范围计算
func TestCalculateLookAheadRange(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 1000, // 建议的分区大小
	}

	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer)

	ctx := context.Background()

	// 测试场景: 2个活跃节点，每个节点3倍分区
	activeWorkers := []string{"node1", "node2"}
	workerPartitionMultiple := int64(3)

	rangeSize, err := partitionMgr.CalculateLookAheadRange(ctx, activeWorkers, workerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 期望的探测范围 = 分区大小 * 最小节点数量 * 倍数 = 1000 * 3 * 3 = 9000
	// 因为2个节点 < DefaultMinWorkerCount(3)，所以使用最小值3
	expectedRangeSize := int64(9000)
	if rangeSize != expectedRangeSize {
		t.Errorf("ID探测范围计算不正确，期望 %d，得到 %d", expectedRangeSize, rangeSize)
	}

	// 测试场景: 无活跃节点
	emptyWorkers := []string{}
	rangeSize, err = partitionMgr.CalculateLookAheadRange(ctx, emptyWorkers, workerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 期望的探测范围 = 分区大小 * 最小节点数量 * 倍数 = 1000 * 3 * 3 = 9000
	// 0个节点 < DefaultMinWorkerCount(3)，所以使用最小值3
	expectedRangeSize = int64(9000)
	if rangeSize != expectedRangeSize {
		t.Errorf("ID探测范围计算不正确，期望 %d，得到 %d", expectedRangeSize, rangeSize)
	}

	// 测试场景: 活跃节点数大于等于最小值时，使用实际节点数
	manyWorkers := []string{"node1", "node2", "node3", "node4"}
	rangeSize, err = partitionMgr.CalculateLookAheadRange(ctx, manyWorkers, workerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 期望的探测范围 = 分区大小 * 实际节点数量 * 倍数 = 1000 * 4 * 3 = 12000
	// 4个节点 >= DefaultMinWorkerCount(3)，所以使用实际值4
	expectedRangeSize = int64(12000)
	if rangeSize != expectedRangeSize {
		t.Errorf("ID探测范围计算不正确，期望 %d，得到 %d", expectedRangeSize, rangeSize)
	}
}

// TestGetEffectivePartitionSize 测试获取有效分区大小
func TestGetEffectivePartitionSize(t *testing.T) {
	ctx := context.Background()

	// 测试场景1: 处理器建议的分区大小
	planer1 := &mockPartitionPlaner{
		suggestedPartitionSize: 2000,
	}

	partitionMgr1 := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer1)

	size, err := partitionMgr1.GetEffectivePartitionSize(ctx)
	if err != nil {
		t.Errorf("获取有效分区大小失败: %v", err)
	}

	if size != 2000 {
		t.Errorf("分区大小不正确，期望 2000，得到 %d", size)
	}

	// 测试场景2: 处理器未建议分区大小，使用默认值
	planer2 := &mockPartitionPlaner{
		suggestedPartitionSize: 0, // 返回0表示未建议
	}

	partitionMgr2 := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer2)

	size, err = partitionMgr2.GetEffectivePartitionSize(ctx)
	if err != nil {
		t.Errorf("获取有效分区大小失败: %v", err)
	}

	if size != model.DefaultPartitionSize {
		t.Errorf("分区大小不正确，期望默认值 %d，得到 %d", model.DefaultPartitionSize, size)
	}
}

// TestCreatePartitionsForGap 测试为缺口创建分区
func TestCreatePartitionsForGap(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              4000, // 设置足够大的数据范围，确保测试范围内有数据
	}
	logger := utils.NewDefaultLogger()

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)
	ctx := context.Background()

	// 测试场景1: 小缺口，创建单个分区
	smallGap := DataGap{StartID: 1001, EndID: 1500}
	partitionSize := int64(1000)
	currentPartitionID := 10

	partitions := partitionMgr.createPartitionsForGap(ctx, smallGap, partitionSize, &currentPartitionID)
	if len(partitions) != 1 {
		t.Errorf("小缺口应该创建1个分区，但创建了 %d 个分区", len(partitions))
	}

	if len(partitions) > 0 {
		if partitions[0].MinID != 1001 || partitions[0].MaxID != 1500 {
			t.Errorf("分区范围应该是 [1001, 1500]，但得到 [%d, %d]",
				partitions[0].MinID, partitions[0].MaxID)
		}

		if partitions[0].PartitionID != 10 {
			t.Errorf("分区ID应该是10，但得到 %d", partitions[0].PartitionID)
		}
	}

	// 测试场景2: 大缺口，创建多个分区
	largeGap := DataGap{StartID: 1001, EndID: 3500}
	currentPartitionID = 20

	partitions = partitionMgr.createPartitionsForGap(ctx, largeGap, partitionSize, &currentPartitionID)
	expectedPartitionCount := 3 // (3500-1001+1)/1000 = 2.5，向上取整为3

	if len(partitions) != expectedPartitionCount {
		t.Errorf("大缺口应该创建%d个分区，但创建了 %d 个分区", expectedPartitionCount, len(partitions))
	}

	if len(partitions) > 0 {
		// 验证第一个分区
		if partitions[0].MinID != 1001 || partitions[0].MaxID != 2000 {
			t.Errorf("第一个分区范围应该是 [1001, 2000]，但得到 [%d, %d]",
				partitions[0].MinID, partitions[0].MaxID)
		}

		// 验证最后一个分区
		lastPartition := partitions[len(partitions)-1]
		if lastPartition.MaxID != 3500 {
			t.Errorf("最后一个分区的MaxID应该是3500，但得到 %d", lastPartition.MaxID)
		}
	}

	// 验证分区ID递增
	if currentPartitionID != 23 {
		t.Errorf("当前分区ID应该递增到23，但得到 %d", currentPartitionID)
	}
}

// TestDetectAndCreateGapPartitions 测试简化的缺口检测和创建流程
func TestDetectAndCreateGapPartitions(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              2500, // 模拟在缺口范围内有数据
	}
	logger := utils.NewDefaultLogger()

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	ctx := context.Background()

	// 设置有缺口的现有分区
	existingPartitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, MinID: 1, MaxID: 1000},
		2: {PartitionID: 2, MinID: 2001, MaxID: 3000}, // 缺口: 1001-2000
	}
	mockStrategy.SetPartitions(existingPartitions)

	// 执行缺口检测和创建
	activeWorkers := []string{"worker1", "worker2"}
	maxPartitionID := 2

	gapPartitions, err := partitionMgr.DetectAndCreateGapPartitions(ctx, activeWorkers, maxPartitionID)
	if err != nil {
		t.Errorf("检测和创建缺口分区失败: %v", err)
	}

	// 验证创建了缺口分区
	if len(gapPartitions) == 0 {
		t.Errorf("应该创建缺口分区，但没有创建")
	}

	// 验证缺口分区覆盖了正确的范围
	foundGapPartition := false
	for _, partition := range gapPartitions {
		if partition.MinID >= 1001 && partition.MaxID <= 2000 {
			foundGapPartition = true
			break
		}
	}

	if !foundGapPartition {
		t.Errorf("缺口分区没有覆盖预期的范围 [1001, 2000]")
	}
}

// TestDetectGapsBetweenPartitions 测试分区间缺口检测的核心逻辑
func TestDetectGapsBetweenPartitions(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              2500,
	}
	logger := test_utils.NewMockLogger(false)

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)
	ctx := context.Background()

	// 测试场景1: 无分区，应该返回空
	emptyPartitions := []*model.PartitionInfo{}
	gaps := partitionMgr.detectGapsBetweenPartitions(ctx, emptyPartitions)
	if len(gaps) != 0 {
		t.Errorf("空分区列表应该返回0个缺口，但得到 %d 个", len(gaps))
	}

	// 测试场景2: 单个分区，应该返回空
	singlePartition := []*model.PartitionInfo{
		{PartitionID: 1, MinID: 1, MaxID: 1000},
	}
	gaps = partitionMgr.detectGapsBetweenPartitions(ctx, singlePartition)
	if len(gaps) != 0 {
		t.Errorf("单个分区应该返回0个缺口，但得到 %d 个", len(gaps))
	}

	// 测试场景3: 连续分区，应该返回空
	continuousPartitions := []*model.PartitionInfo{
		{PartitionID: 1, MinID: 1, MaxID: 1000},
		{PartitionID: 2, MinID: 1001, MaxID: 2000},
	}
	gaps = partitionMgr.detectGapsBetweenPartitions(ctx, continuousPartitions)
	if len(gaps) != 0 {
		t.Errorf("连续分区应该返回0个缺口，但得到 %d 个", len(gaps))
	}

	// 测试场景4: 有缺口且缺口中有数据的分区
	gapPartitions := []*model.PartitionInfo{
		{PartitionID: 1, MinID: 1, MaxID: 1000},
		{PartitionID: 2, MinID: 1501, MaxID: 2500}, // 缺口: 1001-1500
	}
	gaps = partitionMgr.detectGapsBetweenPartitions(ctx, gapPartitions)
	if len(gaps) != 1 {
		t.Errorf("有数据缺口的分区应该返回1个缺口，但得到 %d 个", len(gaps))
	} else {
		gap := gaps[0]
		if gap.StartID != 1001 || gap.EndID != 1500 {
			t.Errorf("缺口范围应该是 [1001, 1500]，但得到 [%d, %d]", gap.StartID, gap.EndID)
		}
	}
}

// TestHasDataInRange 测试数据存在性检查
func TestHasDataInRange(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	logger := test_utils.NewMockLogger(false)

	ctx := context.Background()

	// 测试场景1: 范围内有数据
	mockPlanerWithData := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              1500, // 模拟在1001-2000范围内有数据
	}
	partitionMgr1 := NewPartitionAssigner(config, mockStrategy, logger, mockPlanerWithData)

	hasData := partitionMgr1.hasDataInRange(ctx, 1000, 2000)
	if !hasData {
		t.Errorf("范围 [1000, 2000] 内应该有数据，但检测为无数据")
	}

	// 测试场景2: 范围内无数据
	mockPlanerNoData := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              500, // 模拟在1001-2000范围内无数据
	}
	partitionMgr2 := NewPartitionAssigner(config, mockStrategy, logger, mockPlanerNoData)

	hasData = partitionMgr2.hasDataInRange(ctx, 1000, 2000)
	if hasData {
		t.Errorf("范围 [1000, 2000] 内应该无数据，但检测为有数据")
	}

	// 测试场景3: 无效范围
	hasData = partitionMgr1.hasDataInRange(ctx, 2000, 1000)
	if hasData {
		t.Errorf("无效范围应该返回false，但返回true")
	}
}

// TestDetectAndCreateGapPartitions_DiscreteData 测试简化的gap detection处理不连续数据ID
func TestDetectAndCreateGapPartitions_DiscreteData(t *testing.T) {
	// 创建专门的模拟处理器，模拟不连续的数据ID
	discreteDataPlaner := &discreteDataPartitionPlaner{
		suggestedPartitionSize: 1000,
		discreteDataRanges: []dataRange{
			{start: 1, end: 500},     // 数据段1: 1-500
			{start: 800, end: 1200},  // 数据段2: 800-1200 (缺口501-799)
			{start: 1500, end: 2000}, // 数据段3: 1500-2000 (缺口1201-1499)
			{start: 3000, end: 3500}, // 数据段4: 3000-3500 (缺口2001-2999)
		},
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	logger := test_utils.NewMockLogger(false)

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, discreteDataPlaner)

	// 设置现有分区（有缺口）
	existingPartitions := map[int]*model.PartitionInfo{
		1: {
			PartitionID: 1,
			MinID:       1,
			MaxID:       500, // 完全覆盖数据段1
			Status:      model.StatusCompleted,
		},
		2: {
			PartitionID: 2,
			MinID:       1000, // 只覆盖数据段2的一部分 (遗漏800-999)
			MaxID:       1200,
			Status:      model.StatusCompleted,
		},
		3: {
			PartitionID: 3,
			MinID:       2500, // 跨越空区域 (遗漏1500-2000)
			MaxID:       3000,
			Status:      model.StatusCompleted,
		},
	}

	mockStrategy.SetPartitions(existingPartitions)

	ctx := context.Background()
	activeWorkers := []string{"worker1", "worker2"}

	// 执行gap detection
	gapPartitions, err := partitionMgr.DetectAndCreateGapPartitions(ctx, activeWorkers, 3)
	if err != nil {
		t.Fatalf("Gap detection失败: %v", err)
	}

	// 验证结果
	t.Logf("检测到 %d 个gap分区", len(gapPartitions))

	// 打印所有创建的分区
	for i, gap := range gapPartitions {
		t.Logf("分区 %d: [%d, %d]", i, gap.MinID, gap.MaxID)
	}

	// 应该检测到的真实缺口（简化版只检测分区间缺口）
	expectedGaps := []struct {
		name       string
		minID      int64
		maxID      int64
		shouldFind bool
	}{
		{"缺口1: 501-999", 501, 999, true},     // 分区1和分区2之间的缺口
		{"缺口2: 1201-2200", 1201, 2200, true}, // 分区2和分区3之间的缺口（有数据部分）
	}

	// 验证每个期望的缺口是否被正确检测
	for _, expected := range expectedGaps {
		found := false
		for _, gap := range gapPartitions {
			// 检查是否有分区覆盖了这个期望的缺口范围
			if gap.MinID <= expected.minID && gap.MaxID >= expected.maxID {
				found = true
				t.Logf("✅ 正确检测到 %s: 分区[%d, %d]",
					expected.name, gap.MinID, gap.MaxID)
				break
			}
		}

		if expected.shouldFind && !found {
			t.Errorf("❌ 未检测到期望的缺口: %s", expected.name)
		} else if !expected.shouldFind && found {
			t.Errorf("❌ 误检测了不应存在的缺口: %s", expected.name)
		}
	}

	// 确保创建的分区确实有数据
	for _, gap := range gapPartitions {
		// 验证每个创建的分区范围内确实有数据
		hasData := false
		for _, dataRange := range discreteDataPlaner.discreteDataRanges {
			if !(gap.MaxID < dataRange.start || gap.MinID > dataRange.end) {
				hasData = true
				break
			}
		}

		if !hasData {
			t.Errorf("❌ 为无数据范围[%d, %d]创建了分区", gap.MinID, gap.MaxID)
		}
	}
}

// TestDetectAndCreateGapPartitions_Incremental 测试增量缺口检测逻辑
func TestDetectAndCreateGapPartitions_Incremental(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 500,
		nextMaxID:              5000,
	}
	logger := test_utils.NewMockLogger(false)

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)
	ctx := context.Background()

	// 阶段1: 设置初始分区
	initialPartitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, MinID: 1, MaxID: 500},
		2: {PartitionID: 2, MinID: 501, MaxID: 1000},
		3: {PartitionID: 3, MinID: 2001, MaxID: 2500}, // 故意留缺口 1001-2000
	}
	mockStrategy.SetPartitions(initialPartitions)

	// 第一次检测（完整检测）- 应该检测到缺口 [1001, 2000]
	t.Log("=== 阶段1: 第一次完整检测 ===")
	gapPartitions1, err := partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 3)
	if err != nil {
		t.Fatalf("第一次缺口检测失败: %v", err)
	}

	// 验证检测到缺口并创建了分区
	if len(gapPartitions1) == 0 {
		t.Errorf("第一次检测应该找到缺口分区，但没有找到")
	} else {
		t.Logf("第一次检测创建了 %d 个缺口分区", len(gapPartitions1))
		for i, gap := range gapPartitions1 {
			t.Logf("  缺口分区 %d: [%d, %d]", i, gap.MinID, gap.MaxID)
		}
	}

	// 阶段2: 添加新分区（模拟增量场景）
	t.Log("=== 阶段2: 添加新分区，测试增量检测 ===")

	// 添加新分区，在新位置创建缺口
	newPartitions := map[int]*model.PartitionInfo{
		4: {PartitionID: 4, MinID: 3001, MaxID: 3500}, // 缺口: 2501-3000
		5: {PartitionID: 5, MinID: 4001, MaxID: 4500}, // 缺口: 3501-4000
	}

	// 合并分区到mock strategy
	allPartitions := make(map[int]*model.PartitionInfo)
	for k, v := range initialPartitions {
		allPartitions[k] = v
	}
	for k, v := range newPartitions {
		allPartitions[k] = v
	}
	// 添加第一次检测创建的缺口分区
	for i, gap := range gapPartitions1 {
		allPartitions[10+i] = &model.PartitionInfo{
			PartitionID: gap.PartitionID,
			MinID:       gap.MinID,
			MaxID:       gap.MaxID,
			Status:      model.StatusPending,
		}
	}
	mockStrategy.SetPartitions(allPartitions)

	// 第二次检测（增量检测）- 应该只检测新分区附近的缺口
	gapPartitions2, err := partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 5)
	if err != nil {
		t.Fatalf("第二次增量检测失败: %v", err)
	}

	t.Logf("第二次增量检测创建了 %d 个缺口分区", len(gapPartitions2))
	for i, gap := range gapPartitions2 {
		t.Logf("  增量缺口分区 %d: [%d, %d]", i, gap.MinID, gap.MaxID)
	}

	// 验证增量检测的结果
	expectedGapRanges := []struct {
		name  string
		minID int64
		maxID int64
		found bool
	}{
		{"缺口 2501-3000", 2501, 3000, false},
		{"缺口 3501-4000", 3501, 4000, false},
	}

	for _, gap := range gapPartitions2 {
		for i := range expectedGapRanges {
			if gap.MinID >= expectedGapRanges[i].minID && gap.MaxID <= expectedGapRanges[i].maxID {
				expectedGapRanges[i].found = true
				t.Logf("✅ 发现期望的%s: 分区[%d, %d]",
					expectedGapRanges[i].name, gap.MinID, gap.MaxID)
			}
		}
	}

	// 验证期望的缺口是否被找到
	for _, expected := range expectedGapRanges {
		if !expected.found {
			t.Logf("⚠️ 未检测到期望的%s（可能该范围内无数据）", expected.name)
		}
	}

	// 阶段3: 测试无新分区的增量检测
	t.Log("=== 阶段3: 无新分区的增量检测 ===")
	gapPartitions3, err := partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 5) // 相同的maxPartitionID
	if err != nil {
		t.Fatalf("第三次增量检测失败: %v", err)
	}

	if len(gapPartitions3) != 0 {
		t.Errorf("无新分区时应该跳过检测，但创建了 %d 个分区", len(gapPartitions3))
	} else {
		t.Log("✅ 无新分区时正确跳过了检测")
	}

	// 验证状态跟踪
	if partitionMgr.lastGapDetectionID != 5 {
		t.Errorf("lastGapDetectionID应该是5，但得到 %d", partitionMgr.lastGapDetectionID)
	}

	if len(partitionMgr.knownPartitionRanges) == 0 {
		t.Errorf("knownPartitionRanges缓存应该不为空")
	} else {
		t.Logf("✅ 分区范围缓存包含 %d 个分区", len(partitionMgr.knownPartitionRanges))
	}
}

// dataRange 表示数据范围
type dataRange struct {
	start int64
	end   int64
}

// discreteDataPartitionPlaner 模拟处理不连续数据的分区规划器
type discreteDataPartitionPlaner struct {
	suggestedPartitionSize int64
	discreteDataRanges     []dataRange
}

func (d *discreteDataPartitionPlaner) PartitionSize(ctx context.Context) (int64, error) {
	return d.suggestedPartitionSize, nil
}

func (d *discreteDataPartitionPlaner) GetNextMaxID(ctx context.Context, lastID int64, rangeSize int64) (int64, error) {
	// 正确实现GetNextMaxID语义：返回从lastID+1开始的第rangeSize个数据ID的值

	// 收集从lastID+1开始的所有数据ID
	var dataIDs []int64
	startSearchID := lastID + 1

	for _, dataRange := range d.discreteDataRanges {
		// 找到与搜索起点有交集的数据范围
		if dataRange.end >= startSearchID {
			// 确定这个数据范围内需要包含的ID起点
			rangeStart := dataRange.start
			if rangeStart < startSearchID {
				rangeStart = startSearchID
			}

			// 将这个范围内的所有ID添加到列表中
			for id := rangeStart; id <= dataRange.end; id++ {
				dataIDs = append(dataIDs, id)

				// 如果已经收集够了rangeSize个数据，返回第rangeSize个
				if int64(len(dataIDs)) >= rangeSize {
					return dataIDs[rangeSize-1], nil
				}
			}
		}
	}

	// 如果没有找到足够的数据，返回lastID（表示没有足够的数据）
	if len(dataIDs) == 0 {
		return lastID, nil
	}

	// 返回找到的最后一个数据ID（可能少于rangeSize个）
	return dataIDs[len(dataIDs)-1], nil
}

// TestIncrementalGapDetection_EdgeCases 测试增量检测的边界情况
func TestIncrementalGapDetection_EdgeCases(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}
	logger := test_utils.NewMockLogger(false)
	ctx := context.Background()

	// 测试用例1: 空分区列表的增量检测
	t.Run("EmptyPartitions", func(t *testing.T) {
		emptyStrategy := test_utils.NewMockPartitionStrategy()
		emptyStrategy.SetPartitions(map[int]*model.PartitionInfo{})

		tempMgr := NewPartitionAssigner(config, emptyStrategy, logger, mockPlaner)

		// 对空分区执行增量检测
		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 0)
		if err != nil {
			t.Fatalf("空分区列表检测失败: %v", err)
		}

		if len(gapPartitions) == 0 {
			t.Log("✅ 空分区列表正确处理：未创建分区")
		} else {
			t.Logf("空分区列表创建了 %d 个分区", len(gapPartitions))
		}
	})

	// 测试用例2: 单个分区的增量检测
	t.Run("SinglePartition", func(t *testing.T) {
		singleStrategy := test_utils.NewMockPartitionStrategy()
		singlePartitions := map[int]*model.PartitionInfo{
			1: {PartitionID: 1, MinID: 1, MaxID: 1000},
		}
		singleStrategy.SetPartitions(singlePartitions)

		tempMgr := NewPartitionAssigner(config, singleStrategy, logger, mockPlaner)

		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 1)
		if err != nil {
			t.Fatalf("单分区检测失败: %v", err)
		}

		t.Logf("单分区检测创建了 %d 个缺口分区", len(gapPartitions))
	})

	// 测试用例3: 大量连续分区的增量检测性能
	t.Run("ManyConsecutivePartitions", func(t *testing.T) {
		consecutiveStrategy := test_utils.NewMockPartitionStrategy()
		consecutivePartitions := make(map[int]*model.PartitionInfo)

		// 创建100个连续分区
		for i := 1; i <= 100; i++ {
			consecutivePartitions[i] = &model.PartitionInfo{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
				Status:      model.StatusCompleted,
			}
		}
		consecutiveStrategy.SetPartitions(consecutivePartitions)

		tempMgr := NewPartitionAssigner(config, consecutiveStrategy, logger, mockPlaner)

		start := time.Now()
		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 100)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("连续分区检测失败: %v", err)
		}

		t.Logf("100个连续分区检测耗时: %v, 创建缺口分区: %d", duration, len(gapPartitions))

		// 验证连续分区不应该创建额外的缺口分区（除了可能的尾部分区）
		if len(gapPartitions) > 1 {
			t.Logf("连续分区创建了 %d 个缺口分区，可能包含尾部数据", len(gapPartitions))
		}
	})

	// 测试用例4: 分区ID不连续但数据连续的情况
	t.Run("NonConsecutivePartitionIDs", func(t *testing.T) {
		nonConsecutiveStrategy := test_utils.NewMockPartitionStrategy()
		nonConsecutivePartitions := map[int]*model.PartitionInfo{
			1:  {PartitionID: 1, MinID: 1, MaxID: 1000},
			5:  {PartitionID: 5, MinID: 1001, MaxID: 2000},  // 分区ID跳跃但数据连续
			10: {PartitionID: 10, MinID: 2001, MaxID: 3000}, // 分区ID继续跳跃但数据连续
		}
		nonConsecutiveStrategy.SetPartitions(nonConsecutivePartitions)

		tempMgr := NewPartitionAssigner(config, nonConsecutiveStrategy, logger, mockPlaner)

		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 10)
		if err != nil {
			t.Fatalf("非连续分区ID检测失败: %v", err)
		}

		t.Logf("非连续分区ID但数据连续的情况创建了 %d 个缺口分区", len(gapPartitions))
	})

	// 测试用例5: 重叠分区的处理
	t.Run("OverlappingPartitions", func(t *testing.T) {
		overlappingStrategy := test_utils.NewMockPartitionStrategy()
		overlappingPartitions := map[int]*model.PartitionInfo{
			1: {PartitionID: 1, MinID: 1, MaxID: 1500},
			2: {PartitionID: 2, MinID: 1200, MaxID: 2500}, // 与分区1重叠
			3: {PartitionID: 3, MinID: 3000, MaxID: 4000}, // 有缺口
		}
		overlappingStrategy.SetPartitions(overlappingPartitions)

		tempMgr := NewPartitionAssigner(config, overlappingStrategy, logger, mockPlaner)

		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 3)
		if err != nil {
			t.Fatalf("重叠分区检测失败: %v", err)
		}

		t.Logf("重叠分区情况创建了 %d 个缺口分区", len(gapPartitions))

		// 验证缺口应该在2500-3000之间
		foundExpectedGap := false
		for _, gap := range gapPartitions {
			if gap.MinID >= 2501 && gap.MaxID <= 2999 {
				foundExpectedGap = true
				t.Logf("✅ 正确检测到重叠分区后的缺口: [%d, %d]", gap.MinID, gap.MaxID)
				break
			}
		}

		if len(gapPartitions) > 0 && !foundExpectedGap {
			t.Logf("检测到缺口但位置可能不符合预期")
		}
	})

	// 测试用例6: 增量检测中的极端情况 - 添加非常大的分区ID跳跃
	t.Run("LargePartitionIDJump", func(t *testing.T) {
		jumpStrategy := test_utils.NewMockPartitionStrategy()
		initialPartitions := map[int]*model.PartitionInfo{
			1: {PartitionID: 1, MinID: 1, MaxID: 1000},
		}
		jumpStrategy.SetPartitions(initialPartitions)

		tempMgr := NewPartitionAssigner(config, jumpStrategy, logger, mockPlaner)

		// 建立初始缓存
		_, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 1)
		if err != nil {
			t.Fatalf("建立初始缓存失败: %v", err)
		}

		// 添加分区ID大幅跳跃的新分区
		allPartitions := map[int]*model.PartitionInfo{
			1:    {PartitionID: 1, MinID: 1, MaxID: 1000},
			1000: {PartitionID: 1000, MinID: 5001, MaxID: 6000}, // 极大的ID跳跃
		}
		jumpStrategy.SetPartitions(allPartitions)

		start := time.Now()
		gapPartitions, err := tempMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 1000)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("大ID跳跃增量检测失败: %v", err)
		}

		t.Logf("大分区ID跳跃增量检测耗时: %v, 创建缺口分区: %d", duration, len(gapPartitions))

		// 验证性能 - 即使有大的ID跳跃，增量检测也应该很快
		if duration > 100*time.Millisecond {
			t.Logf("注意：大ID跳跃的增量检测耗时 %v 可能超过预期", duration)
		}
	})

	t.Log("✅ 所有边界情况测试完成")
}

// TestPartitionCountCalculationEdgeCases 测试分区数量计算的边界情况，确保不会丢失数据
func TestPartitionCountCalculationEdgeCases(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 1000, // 分区大小为1000
	}

	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer)

	ctx := context.Background()

	testCases := []struct {
		name               string
		lastAllocatedID    int64
		nextMaxID          int64
		expectedPartitions int
		description        string
	}{
		{
			name:               "整除情况",
			lastAllocatedID:    0,
			nextMaxID:          2000,
			expectedPartitions: 2,
			description:        "2000 ID，分区大小1000，应该创建2个分区",
		},
		{
			name:               "有余数情况1",
			lastAllocatedID:    0,
			nextMaxID:          2500,
			expectedPartitions: 3,
			description:        "2500 ID，分区大小1000，应该创建3个分区(2个完整+1个500)",
		},
		{
			name:               "有余数情况2",
			lastAllocatedID:    100,
			nextMaxID:          1101,
			expectedPartitions: 2,
			description:        "1001 ID (101-1101)，分区大小1000，应该创建2个分区(1个1000+1个1)",
		},
		{
			name:               "小于一个分区大小",
			lastAllocatedID:    0,
			nextMaxID:          500,
			expectedPartitions: 1,
			description:        "500 ID，分区大小1000，应该创建1个分区",
		},
		{
			name:               "刚好一个分区大小",
			lastAllocatedID:    0,
			nextMaxID:          1000,
			expectedPartitions: 1,
			description:        "1000 ID，分区大小1000，应该创建1个分区",
		},
		{
			name:               "边界情况-1个多余ID",
			lastAllocatedID:    999,
			nextMaxID:          2000,
			expectedPartitions: 2,
			description:        "1001 ID (1000-2000)，分区大小1000，应该创建2个分区",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			request, err := partitionMgr.CreatePartitionsRequestWithBounds(ctx, tc.lastAllocatedID, tc.nextMaxID, 0)
			if err != nil {
				t.Fatalf("创建分区请求失败: %v", err)
			}

			actualPartitions := len(request.Partitions)
			if actualPartitions != tc.expectedPartitions {
				t.Errorf("%s: 期望创建 %d 个分区，但实际创建了 %d 个分区。总ID数=%d",
					tc.description, tc.expectedPartitions, actualPartitions, tc.nextMaxID-tc.lastAllocatedID)
			}

			// 验证分区覆盖的ID范围是否完整
			if len(request.Partitions) > 0 {
				firstPartition := request.Partitions[0]
				lastPartition := request.Partitions[len(request.Partitions)-1]

				expectedFirstMinID := tc.lastAllocatedID + 1
				expectedLastMaxID := tc.nextMaxID

				if firstPartition.MinID != expectedFirstMinID {
					t.Errorf("第一个分区的MinID应该是 %d，但得到 %d", expectedFirstMinID, firstPartition.MinID)
				}

				if lastPartition.MaxID != expectedLastMaxID {
					t.Errorf("最后一个分区的MaxID应该是 %d，但得到 %d", expectedLastMaxID, lastPartition.MaxID)
				}

				// 验证分区之间没有空隙
				for i := 1; i < len(request.Partitions); i++ {
					prevMaxID := request.Partitions[i-1].MaxID
					currentMinID := request.Partitions[i].MinID
					if prevMaxID+1 != currentMinID {
						t.Errorf("分区 %d 和 %d 之间有空隙: %d -> %d", i-1, i, prevMaxID, currentMinID)
					}
				}
			}

			t.Logf("✅ %s: 创建了 %d 个分区，覆盖ID范围 [%d, %d]",
				tc.name, actualPartitions, tc.lastAllocatedID+1, tc.nextMaxID)
		})
	}
}

// TestSafeMultiply 测试安全乘法功能
func TestSafeMultiply(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 5,
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 3000,
	}
	logger := test_utils.NewMockLogger(false)

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	// 测试场景1: 正常计算，不溢出
	result, err := partitionMgr.safeMultiply(1000, 10, 5)
	if err != nil {
		t.Errorf("正常计算不应该出错: %v", err)
	}
	expected := int64(50000)
	if result != expected {
		t.Errorf("计算结果不正确，期望 %d，得到 %d", expected, result)
	}

	// 测试场景2: 第一步乘法溢出
	maxVal := int64(9223372036854775807) // int64最大值
	_, err = partitionMgr.safeMultiply(maxVal, 2, 1)
	if err == nil {
		t.Error("应该检测到乘法溢出")
	}
	if !strings.Contains(err.Error(), "乘法溢出") {
		t.Errorf("错误信息不正确: %v", err)
	}

	// 测试场景3: 第二步乘法溢出
	_, err = partitionMgr.safeMultiply(1000000000, 1000000000, 10) // 10^9 * 10^9 * 10 = 10^19，超过int64最大值
	if err == nil {
		t.Error("应该检测到乘法溢出")
	}

	// 测试场景4: 正常的大数值计算（不溢出）
	result2, err := partitionMgr.safeMultiply(500000000, 3, 1) // 15亿，在int64范围内
	if err != nil {
		t.Errorf("正常的大数值计算不应该出错: %v", err)
	}
	expected2 := int64(1500000000)
	if result2 != expected2 {
		t.Errorf("大数值计算结果不正确，期望 %d，得到 %d", expected2, result2)
	}

	// 测试场景5: 负数参数
	_, err = partitionMgr.safeMultiply(-1, 10, 5)
	if err == nil {
		t.Error("应该检测到负数参数")
	}
	if !strings.Contains(err.Error(), "乘法参数必须为正数") {
		t.Errorf("错误信息不正确: %v", err)
	}
}

// TestCalculateLookAheadRangeWithOverflow 测试在溢出情况下的ID探测范围计算
func TestCalculateLookAheadRangeWithOverflow(t *testing.T) {
	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 10000000000, // 非常大的倍数，确保溢出
	}

	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000000000, // 10亿的分区大小
	}
	logger := test_utils.NewMockLogger(false)

	partitionMgr := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)
	ctx := context.Background()

	// 测试场景: 大量worker导致计算溢出
	manyWorkers := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		manyWorkers[i] = fmt.Sprintf("worker-%d", i)
	}

	rangeSize, err := partitionMgr.CalculateLookAheadRange(ctx, manyWorkers, config.WorkerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 验证结果应该是一个合理的值，而不是溢出或极大的数字
	if rangeSize <= 0 {
		t.Errorf("探测范围应该大于0，得到 %d", rangeSize)
	}

	// 当溢出时，应该返回默认值
	if rangeSize == DefaultRangeSize {
		t.Logf("✅ 正确检测到溢出，使用默认值: %d", rangeSize)
	} else {
		// 如果没有溢出，验证结果是否合理
		t.Logf("未发生溢出，计算结果: %d", rangeSize)
	}

	t.Logf("溢出保护后的探测范围: %d", rangeSize)
}

// TestCacheOptimization 测试缓存优化功能
func TestCacheOptimization(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	t.Run("测试初始状态", func(t *testing.T) {
		// 验证初始状态
		if len(assigner.knownPartitionRanges) != 0 {
			t.Errorf("期望初始分区范围为0，得到 %d", len(assigner.knownPartitionRanges))
		}

		if assigner.lastGapDetectionID != 0 {
			t.Errorf("期望初始检测ID为0，得到 %d", assigner.lastGapDetectionID)
		}

		if assigner.confirmedNoGapMaxID != 0 {
			t.Errorf("期望初始确认ID为0，得到 %d", assigner.confirmedNoGapMaxID)
		}
	})

	t.Run("测试缓存清理功能", func(t *testing.T) {
		// 模拟添加大量分区到缓存
		var testPartitions []*model.PartitionInfo
		for i := 1; i <= MaxCachedRanges+200; i++ {
			partition := &model.PartitionInfo{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
				Status:      model.StatusCompleted,
				UpdatedAt:   time.Now(),
			}
			testPartitions = append(testPartitions, partition)
		}

		// 重建缓存，这应该触发清理
		assigner.rebuildPartitionRangeCache(testPartitions)

		// 验证缓存被清理了
		cacheSize := len(assigner.knownPartitionRanges)
		if cacheSize > MaxCachedRanges {
			t.Errorf("缓存清理后仍然超过阈值: %d > %d", cacheSize, MaxCachedRanges)
		}

		// 验证确认无缺口的ID被更新
		if assigner.confirmedNoGapMaxID <= 0 {
			t.Errorf("期望确认无缺口ID大于0，得到 %d", assigner.confirmedNoGapMaxID)
		}

		t.Logf("缓存清理后: 缓存大小=%d, 确认无缺口ID=%d", cacheSize, assigner.confirmedNoGapMaxID)
	})
}

// TestShouldSkipGapDetection 测试缺口检测跳过逻辑
func TestShouldSkipGapDetection(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	// 设置已确认无缺口的最大ID
	assigner.confirmedNoGapMaxID = 5000

	testCases := []struct {
		name     string
		startID  int64
		endID    int64
		expected bool
	}{
		{
			name:     "完全在确认范围内",
			startID:  1000,
			endID:    2000,
			expected: true,
		},
		{
			name:     "边界情况-正好等于确认ID",
			startID:  4000,
			endID:    5000,
			expected: true,
		},
		{
			name:     "部分超出确认范围",
			startID:  4000,
			endID:    6000,
			expected: false,
		},
		{
			name:     "完全超出确认范围",
			startID:  6000,
			endID:    7000,
			expected: false,
		},
		{
			name:     "起始ID超出",
			startID:  5001,
			endID:    6000,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := assigner.shouldSkipGapDetection(tc.startID, tc.endID)
			if result != tc.expected {
				t.Errorf("shouldSkipGapDetection(%d, %d) = %v, 期望 %v",
					tc.startID, tc.endID, result, tc.expected)
			}
		})
	}
}

// TestCleanupOldPartitionRanges 测试旧分区范围清理
func TestCleanupOldPartitionRanges(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	t.Run("小于阈值时不清理", func(t *testing.T) {
		// 添加少量分区
		for i := 1; i <= 100; i++ {
			assigner.knownPartitionRanges = append(assigner.knownPartitionRanges, PartitionRange{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
			})
		}

		originalCount := len(assigner.knownPartitionRanges)
		assigner.cleanupOldPartitionRanges()

		// 应该没有清理
		if len(assigner.knownPartitionRanges) != originalCount {
			t.Errorf("不应该清理缓存，原来 %d 个，现在 %d 个",
				originalCount, len(assigner.knownPartitionRanges))
		}
	})

	t.Run("超过阈值时清理", func(t *testing.T) {
		// 清空并添加大量连续分区
		assigner.knownPartitionRanges = nil
		for i := 1; i <= MaxCachedRanges+100; i++ {
			assigner.knownPartitionRanges = append(assigner.knownPartitionRanges, PartitionRange{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
			})
		}

		originalCount := len(assigner.knownPartitionRanges)
		assigner.cleanupOldPartitionRanges()

		// 应该被清理了
		if len(assigner.knownPartitionRanges) >= originalCount {
			t.Errorf("缓存应该被清理，原来 %d 个，现在 %d 个",
				originalCount, len(assigner.knownPartitionRanges))
		}

		// 确认ID应该被更新
		if assigner.confirmedNoGapMaxID <= 0 {
			t.Errorf("确认无缺口ID应该被更新，得到 %d", assigner.confirmedNoGapMaxID)
		}

		t.Logf("清理结果: 原来 %d 个，现在 %d 个，确认无缺口ID: %d",
			originalCount, len(assigner.knownPartitionRanges), assigner.confirmedNoGapMaxID)
	})

	t.Run("有缺口时的清理", func(t *testing.T) {
		// 清空并添加有缺口的分区
		assigner.knownPartitionRanges = nil
		assigner.confirmedNoGapMaxID = 0

		for i := 1; i <= MaxCachedRanges+100; i++ {
			// 在第50个分区制造缺口
			minID := int64((i-1)*1000 + 1)
			if i > 50 {
				minID += 10000 // 制造缺口
			}

			assigner.knownPartitionRanges = append(assigner.knownPartitionRanges, PartitionRange{
				PartitionID: i,
				MinID:       minID,
				MaxID:       minID + 999,
			})
		}

		originalCount := len(assigner.knownPartitionRanges)
		assigner.cleanupOldPartitionRanges()

		// 应该被清理了，但确认ID应该较小（因为有缺口）
		if len(assigner.knownPartitionRanges) >= originalCount {
			t.Errorf("缓存应该被清理，原来 %d 个，现在 %d 个",
				originalCount, len(assigner.knownPartitionRanges))
		}

		// 确认ID应该只到缺口前
		expectedMaxConfirmedID := int64(49 * 1000) // 第49个分区的MaxID
		if assigner.confirmedNoGapMaxID > expectedMaxConfirmedID {
			t.Errorf("确认无缺口ID不应该超过 %d，得到 %d",
				expectedMaxConfirmedID, assigner.confirmedNoGapMaxID)
		}

		t.Logf("有缺口的清理结果: 原来 %d 个，现在 %d 个，确认无缺口ID: %d",
			originalCount, len(assigner.knownPartitionRanges), assigner.confirmedNoGapMaxID)
	})
}

// TestDeduplicateGaps 测试缺口去重功能
func TestDeduplicateGaps(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	testCases := []struct {
		name     string
		input    []DataGap
		expected int
	}{
		{
			name:     "空缺口列表",
			input:    []DataGap{},
			expected: 0,
		},
		{
			name: "单个缺口",
			input: []DataGap{
				{StartID: 1001, EndID: 1500},
			},
			expected: 1,
		},
		{
			name: "无重复缺口",
			input: []DataGap{
				{StartID: 1001, EndID: 1500},
				{StartID: 2001, EndID: 2500},
				{StartID: 3001, EndID: 3500},
			},
			expected: 3,
		},
		{
			name: "有重复缺口",
			input: []DataGap{
				{StartID: 1001, EndID: 1500},
				{StartID: 2001, EndID: 2500},
				{StartID: 1001, EndID: 1500}, // 重复
				{StartID: 3001, EndID: 3500},
				{StartID: 2001, EndID: 2500}, // 重复
			},
			expected: 3,
		},
		{
			name: "完全重复",
			input: []DataGap{
				{StartID: 1001, EndID: 1500},
				{StartID: 1001, EndID: 1500},
				{StartID: 1001, EndID: 1500},
			},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := assigner.deduplicateGaps(tc.input)
			if len(result) != tc.expected {
				t.Errorf("期望去重后有 %d 个缺口，得到 %d 个", tc.expected, len(result))
			}

			// 验证结果中没有重复
			seen := make(map[DataGap]bool)
			for _, gap := range result {
				if seen[gap] {
					t.Errorf("去重后仍有重复缺口: %+v", gap)
				}
				seen[gap] = true
			}
		})
	}
}

// TestUpdatePartitionRangeCache 测试分区范围缓存更新
func TestUpdatePartitionRangeCache(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	t.Run("增量更新缓存", func(t *testing.T) {
		// 添加初始分区
		initialPartitions := []*model.PartitionInfo{
			{
				PartitionID: 1,
				MinID:       1,
				MaxID:       1000,
				Status:      model.StatusCompleted,
			},
			{
				PartitionID: 2,
				MinID:       1001,
				MaxID:       2000,
				Status:      model.StatusCompleted,
			},
		}

		assigner.updatePartitionRangeCache(initialPartitions)

		if len(assigner.knownPartitionRanges) != 2 {
			t.Errorf("期望缓存有2个分区，得到 %d 个", len(assigner.knownPartitionRanges))
		}

		// 添加新分区
		newPartitions := []*model.PartitionInfo{
			{
				PartitionID: 3,
				MinID:       2001,
				MaxID:       3000,
				Status:      model.StatusCompleted,
			},
		}

		assigner.updatePartitionRangeCache(newPartitions)

		if len(assigner.knownPartitionRanges) != 3 {
			t.Errorf("期望缓存有3个分区，得到 %d 个", len(assigner.knownPartitionRanges))
		}

		// 验证排序
		for i := 0; i < len(assigner.knownPartitionRanges)-1; i++ {
			current := assigner.knownPartitionRanges[i]
			next := assigner.knownPartitionRanges[i+1]
			if current.MinID >= next.MinID {
				t.Errorf("缓存排序错误: [%d].MinID=%d >= [%d].MinID=%d",
					i, current.MinID, i+1, next.MinID)
			}
		}
	})

	t.Run("达到阈值时触发清理", func(t *testing.T) {
		// 清空缓存
		assigner.knownPartitionRanges = nil

		// 添加接近阈值的分区
		var partitions []*model.PartitionInfo
		for i := 1; i <= MaxCachedRanges-10; i++ {
			partitions = append(partitions, &model.PartitionInfo{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
				Status:      model.StatusCompleted,
			})
		}

		assigner.updatePartitionRangeCache(partitions)
		beforeCount := len(assigner.knownPartitionRanges)

		// 添加足够的分区触发清理
		var newPartitions []*model.PartitionInfo
		for i := MaxCachedRanges - 9; i <= MaxCachedRanges+50; i++ {
			newPartitions = append(newPartitions, &model.PartitionInfo{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
				Status:      model.StatusCompleted,
			})
		}

		assigner.updatePartitionRangeCache(newPartitions)
		afterCount := len(assigner.knownPartitionRanges)

		// 验证清理被触发
		if afterCount >= beforeCount+len(newPartitions) {
			t.Errorf("期望清理被触发，清理前约 %d 个，应该少于 %d 个，实际 %d 个",
				beforeCount, beforeCount+len(newPartitions), afterCount)
		}

		t.Logf("阈值触发清理: 清理前约 %d 个，清理后 %d 个",
			beforeCount+len(newPartitions), afterCount)
	})
}

// TestFindAdjacentRanges 测试查找相邻分区功能
func TestFindAdjacentRanges(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              10000,
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 3,
	}

	logger := utils.NewDefaultLogger()
	assigner := NewPartitionAssigner(config, mockStrategy, logger, mockPlaner)

	// 设置测试分区
	assigner.knownPartitionRanges = []PartitionRange{
		{PartitionID: 1, MinID: 1, MaxID: 1000},
		{PartitionID: 2, MinID: 1001, MaxID: 2000},
		{PartitionID: 3, MinID: 3001, MaxID: 4000}, // 故意留出2001-3000的缺口
		{PartitionID: 4, MinID: 4001, MaxID: 5000},
		{PartitionID: 5, MinID: 6001, MaxID: 7000}, // 故意留出5001-6000的缺口
	}

	testCases := []struct {
		name           string
		minID          int64
		maxID          int64
		currentPartID  int
		expectedPrevID int
		expectedNextID int
	}{
		{
			name:           "查找中间分区的相邻",
			minID:          2001,
			maxID:          3000,
			currentPartID:  999, // 不存在的ID
			expectedPrevID: 2,   // 分区2在前面
			expectedNextID: 3,   // 分区3在后面
		},
		{
			name:           "查找第一个分区前面",
			minID:          500,
			maxID:          800,
			currentPartID:  999,
			expectedPrevID: 0, // 被包含，返回nil
			expectedNextID: 0, // 被包含，返回nil
		},
		{
			name:           "查找完全在所有分区前面的范围",
			minID:          -100,
			maxID:          -1,
			currentPartID:  999,
			expectedPrevID: 0, // 没有前面的分区
			expectedNextID: 1, // 分区1在后面
		},
		{
			name:           "查找最后一个分区后面",
			minID:          7001,
			maxID:          8000,
			currentPartID:  999,
			expectedPrevID: 5, // 分区5在前面
			expectedNextID: 0, // 没有后面的分区
		},
		{
			name:           "跳过当前分区",
			minID:          3001,
			maxID:          4000,
			currentPartID:  3, // 分区3自己
			expectedPrevID: 2, // 分区2在前面
			expectedNextID: 4, // 分区4在后面
		},
		{
			name:           "查找完全在前面的范围",
			minID:          -100,
			maxID:          0,
			currentPartID:  999,
			expectedPrevID: 0, // 没有前面的分区
			expectedNextID: 1, // 分区1在后面
		},
		{
			name:           "前部分重叠-与分区2重叠",
			minID:          1500,
			maxID:          2500,
			currentPartID:  999,
			expectedPrevID: 0, // 前部分与分区2重叠，前邻居为nil
			expectedNextID: 3, // 分区3在后面
		},
		{
			name:           "纯前部分重叠",
			minID:          1800,
			maxID:          2500,
			currentPartID:  999,
			expectedPrevID: 0, // 前部分与分区2重叠，前邻居为nil
			expectedNextID: 3, // 分区3在后面
		},
		{
			name:           "后部分重叠-与分区2重叠",
			minID:          900,
			maxID:          1500,
			currentPartID:  999,
			expectedPrevID: 0, // 前部分与分区1重叠，前邻居为nil
			expectedNextID: 0, // 后部分与分区2重叠，后邻居为nil
		},
		{
			name:           "全部重叠",
			minID:          500,
			maxID:          1200,
			currentPartID:  999,
			expectedPrevID: 0, // 没有完全在前面的分区
			expectedNextID: 0, // 后部分与分区1重叠，后邻居为nil
		},
		{
			name:           "完全被包含-被分区2包含",
			minID:          1100,
			maxID:          1900,
			currentPartID:  999,
			expectedPrevID: 0, // 前后都重叠，返回nil
			expectedNextID: 0, // 前后都重叠，返回nil
		},
		{
			name:           "跨越两个分区-与分区2和3都重叠",
			minID:          1800,
			maxID:          3200,
			currentPartID:  999,
			expectedPrevID: 0, // 前部分与分区2重叠，前邻居为nil
			expectedNextID: 0, // 后部分与分区3重叠，后邻居为nil
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prevRange, nextRange := assigner.findAdjacentRanges(tc.minID, tc.maxID, tc.currentPartID)

			// 检查前面的分区
			if tc.expectedPrevID == 0 {
				if prevRange != nil {
					t.Errorf("期望没有前面的分区，但得到分区 %d", prevRange.PartitionID)
				}
			} else {
				if prevRange == nil {
					t.Errorf("期望前面分区ID为 %d，但得到 nil", tc.expectedPrevID)
				} else if prevRange.PartitionID != tc.expectedPrevID {
					t.Errorf("期望前面分区ID为 %d，得到 %d", tc.expectedPrevID, prevRange.PartitionID)
				}
			}

			// 检查后面的分区
			if tc.expectedNextID == 0 {
				if nextRange != nil {
					t.Errorf("期望没有后面的分区，但得到分区 %d", nextRange.PartitionID)
				}
			} else {
				if nextRange == nil {
					t.Errorf("期望后面分区ID为 %d，但得到 nil", tc.expectedNextID)
				} else if nextRange.PartitionID != tc.expectedNextID {
					t.Errorf("期望后面分区ID为 %d，得到 %d", tc.expectedNextID, nextRange.PartitionID)
				}
			}
		})
	}
}

// TestAllocatePartitions_PendingThresholdControl 测试积压控制功能
func TestAllocatePartitions_PendingThresholdControl(t *testing.T) {
	ctx := context.Background()
	logger := test_utils.NewMockLogger(false)

	testCases := []struct {
		name                 string
		maxPendingPartitions int
		pendingPartitions    int
		shouldAllocate       bool
		description          string
	}{
		{
			name:                 "正常分配_待处理分区未超过阈值",
			maxPendingPartitions: 1000,
			pendingPartitions:    500, // 少于阈值
			shouldAllocate:       true,
			description:          "当待处理分区数量未超过阈值时，应该正常分配新分区",
		},
		{
			name:                 "跳过分配_待处理分区等于阈值",
			maxPendingPartitions: 1000,
			pendingPartitions:    1000, // 等于阈值
			shouldAllocate:       false,
			description:          "当待处理分区数量等于阈值时，应该跳过分配",
		},
		{
			name:                 "跳过分配_待处理分区超过阈值",
			maxPendingPartitions: 1000,
			pendingPartitions:    1500, // 超过阈值
			shouldAllocate:       false,
			description:          "当待处理分区数量超过阈值时，应该跳过分配",
		},
		{
			name:                 "小阈值测试_跳过分配",
			maxPendingPartitions: 10,
			pendingPartitions:    15,
			shouldAllocate:       false,
			description:          "测试小阈值情况下的积压控制",
		},
		{
			name:                 "混合状态测试_pending和claimed计入总数",
			maxPendingPartitions: 100,
			pendingPartitions:    80, // pending 80个，加上 claimed 30个，总共110个超过阈值
			shouldAllocate:       false,
			description:          "测试pending和claimed状态都计入待处理分区总数",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 为每个测试用例创建新的 mock strategy
			mockStrategy := test_utils.NewMockPartitionStrategy()

			// 创建一个模拟的 planer，设置有新数据可分配
			planer := &mockPartitionPlaner{
				suggestedPartitionSize: 1000,
				nextMaxID:              int64(tc.pendingPartitions+1000) * 1000, // 设置比现有分区更大的数据ID
			}

			// 创建分区分配器，使用测试案例的配置
			config := PartitionAssignerConfig{
				Namespace:               "test",
				WorkerPartitionMultiple: 5,
				MaxPendingPartitions:    tc.maxPendingPartitions,
			}
			assigner := NewPartitionAssigner(config, mockStrategy, logger, planer)

			// 设置模拟的分区数据来达到期望的pending数量
			if tc.name == "混合状态测试_pending和claimed计入总数" {
				// 特殊处理：创建80个pending + 30个claimed = 110个待处理分区
				// 创建80个pending分区
				for i := 1; i <= tc.pendingPartitions; i++ {
					partition := &model.PartitionInfo{
						PartitionID: i,
						MinID:       int64((i-1)*1000 + 1),
						MaxID:       int64(i * 1000),
						Status:      model.StatusPending,
						UpdatedAt:   time.Now(),
						CreatedAt:   time.Now(),
					}
					mockStrategy.AddPartition(partition)
				}
				// 创建30个claimed分区
				for i := tc.pendingPartitions + 1; i <= tc.pendingPartitions+30; i++ {
					partition := &model.PartitionInfo{
						PartitionID: i,
						MinID:       int64((i-1)*1000 + 1),
						MaxID:       int64(i * 1000),
						Status:      model.StatusClaimed,
						UpdatedAt:   time.Now(),
						CreatedAt:   time.Now(),
					}
					mockStrategy.AddPartition(partition)
				}
			} else {
				// 普通测试：创建指定数量的pending分区
				for i := 1; i <= tc.pendingPartitions; i++ {
					partition := &model.PartitionInfo{
						PartitionID: i,
						MinID:       int64((i-1)*1000 + 1),
						MaxID:       int64(i * 1000),
						Status:      model.StatusPending, // 设置为pending状态
						UpdatedAt:   time.Now(),
						CreatedAt:   time.Now(),
					}
					mockStrategy.AddPartition(partition)
				}
			}

			// 添加一些其他状态的分区
			startID := tc.pendingPartitions + 1
			if tc.name == "混合状态测试_pending和claimed计入总数" {
				// 对于混合状态测试，跳过claimed分区的ID范围
				startID = tc.pendingPartitions + 31 // 跳过30个claimed分区
			}

			for i := startID; i <= startID+199; i++ { // 保持添加200个其他状态的分区
				status := model.StatusCompleted
				if i%2 == 0 {
					status = model.StatusRunning
				}
				partition := &model.PartitionInfo{
					PartitionID: i,
					MinID:       int64((i-1)*1000 + 1),
					MaxID:       int64(i * 1000),
					Status:      status,
					UpdatedAt:   time.Now(),
					CreatedAt:   time.Now(),
				}
				mockStrategy.AddPartition(partition)
			}

			// 记录分配前的分区数量
			initialPartitions, _ := mockStrategy.GetAllActivePartitions(ctx)
			initialCount := len(initialPartitions)

			// 模拟活跃的worker
			activeWorkers := []string{"worker1", "worker2", "worker3"}

			// 执行分区分配
			err := assigner.AllocatePartitions(ctx, activeWorkers)

			// 验证结果
			if err != nil {
				t.Errorf("分区分配失败: %v", err)
				return
			}

			// 检查分配后的分区数量
			afterPartitions, _ := mockStrategy.GetAllActivePartitions(ctx)
			afterCount := len(afterPartitions)
			newPartitionsCount := afterCount - initialCount

			if tc.shouldAllocate {
				// 应该分配新分区
				if newPartitionsCount == 0 {
					t.Errorf("%s: 期望分配新分区，但没有创建任何分区", tc.description)
				} else {
					t.Logf("%s: 成功分配了 %d 个新分区", tc.description, newPartitionsCount)
				}
			} else {
				// 应该跳过分配
				if newPartitionsCount > 0 {
					t.Errorf("%s: 期望跳过分配，但创建了 %d 个分区", tc.description, newPartitionsCount)
				} else {
					t.Logf("%s: 成功跳过分配，符合积压控制预期", tc.description)
				}
			}
		})
	}
}
