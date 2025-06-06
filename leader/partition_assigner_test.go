package leader

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/test_utils"
	"testing"
	"time"
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
	if m.nextMaxID > 0 {
		return m.nextMaxID, nil
	}
	return lastID + rangeSize, nil
}

// TestGetExistingPartitions 测试获取现有分区功能
func TestGetExistingPartitions(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()

	ctx := context.Background()

	// 测试空分区情况 - 直接使用 Strategy 方法
	allPartitions, err := mockStrategy.GetAllPartitions(ctx)
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
	allPartitions, err = mockStrategy.GetAllPartitions(ctx)
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
	partitionMgr := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, mockStrategy, test_utils.NewMockLogger(false), &mockPartitionPlaner{})

	ctx := context.Background()

	// 测试空分区情况
	lastID, err := partitionMgr.GetLastAllocatedID(ctx)
	if err != nil {
		t.Errorf("获取最后分配ID失败: %v", err)
	}
	if lastID != 0 {
		t.Errorf("期望最后分配ID为0，但得到 %d", lastID)
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

	// 再次获取最后分配的ID
	lastID, err = partitionMgr.GetLastAllocatedID(ctx)
	if err != nil {
		t.Errorf("获取最后分配ID失败: %v", err)
	}
	if lastID != 5000 {
		t.Errorf("期望最后分配ID为5000，但得到 %d", lastID)
	}
}

// TestShouldAllocateNewPartitions 测试分区分配决策逻辑
func TestShouldAllocateNewPartitions(t *testing.T) {
	partitionMgr := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), &mockPartitionPlaner{})

	// 测试场景1: 空分区应该分配
	stats1 := model.PartitionStats{
		Total: 0,
	}
	if !partitionMgr.ShouldAllocateNewPartitions(stats1) {
		t.Error("对于空分区，应该分配新分区，但结果是不分配")
	}

	// 测试场景2: 大量失败分区应该暂停分配
	stats2 := model.PartitionStats{
		Total:  10,
		Failed: 4, // 40%失败率，超过1/3
	}
	if partitionMgr.ShouldAllocateNewPartitions(stats2) {
		t.Error("对于大量失败分区，应该暂停分配，但结果是分配")
	}

	// 测试场景3: 足够的等待/运行中分区应该暂停分配
	stats3 := model.PartitionStats{
		Total:   10,
		Pending: 3,
		Running: 3, // 总共6个，超过了一半
	}
	if partitionMgr.ShouldAllocateNewPartitions(stats3) {
		t.Error("对于足够的等待/运行中分区，应该暂停分配，但结果是分配")
	}

	// 测试场景4: 高完成率应该分配新分区
	stats4 := model.PartitionStats{
		Total:          10,
		Completed:      8,
		CompletionRate: 0.8, // 80%完成率，超过70%
	}
	if !partitionMgr.ShouldAllocateNewPartitions(stats4) {
		t.Error("对于高完成率，应该分配新分区，但结果是不分配")
	}
}

// TestCreatePartitionsRequest 测试创建分区请求
func TestCreatePartitionsRequest(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 500, // 建议的分区大小
	}

	partitionMgr := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer)

	ctx := context.Background()

	// 测试场景: 创建1000个ID的分区，预期会创建2个分区
	request, err := partitionMgr.CreatePartitionsRequest(ctx, 0, 1000)
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

	// 验证第二个分区请求
	p1 := request.Partitions[1]
	if p1.MinID != 501 || p1.MaxID != 1000 {
		t.Errorf("分区1范围不正确，期望[501, 1000]，得到[%d, %d]", p1.MinID, p1.MaxID)
	}

	// 测试场景: 使用默认分区大小
	planer.suggestedPartitionSize = 0 // 让planer返回0，应该使用默认值
	request, err = partitionMgr.CreatePartitionsRequest(ctx, 0, 1000)
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

	partitionMgr := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer)

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

	partitionMgr1 := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer1)

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

	partitionMgr2 := NewPartitionManager(PartitionAssignerConfig{Namespace: "test"}, test_utils.NewMockPartitionStrategy(), test_utils.NewMockLogger(false), planer2)

	size, err = partitionMgr2.GetEffectivePartitionSize(ctx)
	if err != nil {
		t.Errorf("获取有效分区大小失败: %v", err)
	}

	if size != model.DefaultPartitionSize {
		t.Errorf("分区大小不正确，期望默认值 %d，得到 %d", model.DefaultPartitionSize, size)
	}
}
