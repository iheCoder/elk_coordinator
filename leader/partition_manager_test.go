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
	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  mockStrategy,
		Logger:    test_utils.NewMockLogger(false),
		Planer:    &mockPartitionPlaner{},
	})

	ctx := context.Background()

	// 测试空分区情况
	partitions, stats, err := partitionMgr.GetExistingPartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("期望空分区，但获取到 %d 个分区", len(partitions))
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

	// 再次获取分区，现在应该有数据了
	partitions, stats, err = partitionMgr.GetExistingPartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(partitions) != 4 {
		t.Errorf("期望有4个分区，但获取到 %d 个分区", len(partitions))
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
	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  mockStrategy,
		Logger:    test_utils.NewMockLogger(false),
		Planer:    &mockPartitionPlaner{},
	})

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
	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    &mockPartitionPlaner{},
	})

	// 测试场景1: 空分区应该分配
	stats1 := PartitionStats{
		Total: 0,
	}
	if !partitionMgr.ShouldAllocateNewPartitions(stats1) {
		t.Error("对于空分区，应该分配新分区，但结果是不分配")
	}

	// 测试场景2: 大量失败分区应该暂停分配
	stats2 := PartitionStats{
		Total:  10,
		Failed: 4, // 40%失败率，超过1/3
	}
	if partitionMgr.ShouldAllocateNewPartitions(stats2) {
		t.Error("对于大量失败分区，应该暂停分配，但结果是分配")
	}

	// 测试场景3: 足够的等待/运行中分区应该暂停分配
	stats3 := PartitionStats{
		Total:   10,
		Pending: 3,
		Running: 3, // 总共6个，超过了一半
	}
	if partitionMgr.ShouldAllocateNewPartitions(stats3) {
		t.Error("对于足够的等待/运行中分区，应该暂停分配，但结果是分配")
	}

	// 测试场景4: 高完成率应该分配新分区
	stats4 := PartitionStats{
		Total:          10,
		Completed:      8,
		CompletionRate: 0.8, // 80%完成率，超过70%
	}
	if !partitionMgr.ShouldAllocateNewPartitions(stats4) {
		t.Error("对于高完成率，应该分配新分区，但结果是不分配")
	}
}

// TestCreatePartitions 测试创建分区
func TestCreatePartitions(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 500, // 建议的分区大小
	}

	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    planer,
	})

	ctx := context.Background()

	// 测试场景: 创建1000个ID的分区，预期会创建2个分区
	partitions, err := partitionMgr.CreatePartitions(ctx, 0, 1000)
	if err != nil {
		t.Errorf("创建分区失败: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("期望创建2个分区，但得到 %d 个", len(partitions))
	}

	// 验证第一个分区
	if p, exists := partitions[0]; !exists {
		t.Error("分区0不存在")
	} else {
		if p.MinID != 1 || p.MaxID != 500 {
			t.Errorf("分区0范围不正确，期望[1, 500]，得到[%d, %d]", p.MinID, p.MaxID)
		}
		if p.Status != model.StatusPending {
			t.Errorf("分区0状态不正确，期望Pending，得到 %s", p.Status)
		}
	}

	// 验证第二个分区
	if p, exists := partitions[1]; !exists {
		t.Error("分区1不存在")
	} else {
		if p.MinID != 501 || p.MaxID != 1000 {
			t.Errorf("分区1范围不正确，期望[501, 1000]，得到[%d, %d]", p.MinID, p.MaxID)
		}
	}

	// 测试场景: 使用默认分区大小
	planer.suggestedPartitionSize = 0 // 让planer返回0，应该使用默认值
	partitions, err = partitionMgr.CreatePartitions(ctx, 0, 1000)
	if err != nil {
		t.Errorf("创建分区失败: %v", err)
	}

	// 默认分区大小是1000，所以应该只创建1个分区
	if len(partitions) != 1 {
		t.Errorf("期望创建1个分区，但得到 %d 个", len(partitions))
	}
}

// TestMergePartitions 测试合并分区
func TestMergePartitions(t *testing.T) {
	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    &mockPartitionPlaner{},
	})

	// 现有分区
	existingPartitions := map[int]model.PartitionInfo{
		1: {
			PartitionID: 1,
			MinID:       1,
			MaxID:       1000,
			Status:      model.StatusCompleted,
		},
		2: {
			PartitionID: 2,
			MinID:       1001,
			MaxID:       2000,
			Status:      model.StatusRunning,
		},
	}

	// 新分区
	newPartitions := map[int]model.PartitionInfo{
		0: {
			PartitionID: 0,
			MinID:       2001,
			MaxID:       3000,
			Status:      model.StatusPending,
		},
		1: {
			PartitionID: 1,
			MinID:       3001,
			MaxID:       4000,
			Status:      model.StatusPending,
		},
	}

	// 合并分区
	mergedPartitions := partitionMgr.MergePartitions(existingPartitions, newPartitions)

	// 验证合并结果
	if len(mergedPartitions) != 4 {
		t.Errorf("期望合并后有4个分区，但得到 %d 个", len(mergedPartitions))
	}

	// 检查原有分区是否保留
	if p, exists := mergedPartitions[1]; !exists || p.Status != model.StatusCompleted {
		t.Error("现有分区1未正确保留")
	}
	if p, exists := mergedPartitions[2]; !exists || p.Status != model.StatusRunning {
		t.Error("现有分区2未正确保留")
	}

	// 检查新分区是否添加且ID不冲突
	foundNew1 := false
	foundNew2 := false
	for id, p := range mergedPartitions {
		if id > 2 && p.MinID == 2001 && p.MaxID == 3000 {
			foundNew1 = true
		}
		if id > 2 && p.MinID == 3001 && p.MaxID == 4000 {
			foundNew2 = true
		}
	}

	if !foundNew1 {
		t.Error("未找到新分区1")
	}
	if !foundNew2 {
		t.Error("未找到新分区2")
	}
}

// TestCalculateLookAheadRange 测试ID探测范围计算
func TestCalculateLookAheadRange(t *testing.T) {
	planer := &mockPartitionPlaner{
		suggestedPartitionSize: 1000, // 建议的分区大小
	}

	partitionMgr := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    planer,
	})

	ctx := context.Background()

	// 测试场景: 2个活跃节点，每个节点3倍分区
	activeWorkers := []string{"node1", "node2"}
	workerPartitionMultiple := int64(3)

	rangeSize, err := partitionMgr.CalculateLookAheadRange(ctx, activeWorkers, workerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 期望的探测范围 = 分区大小 * 节点数量 * 倍数 = 1000 * 2 * 3 = 6000
	expectedRangeSize := int64(6000)
	if rangeSize != expectedRangeSize {
		t.Errorf("ID探测范围计算不正确，期望 %d，得到 %d", expectedRangeSize, rangeSize)
	}

	// 测试场景: 无活跃节点（避免除零）
	emptyWorkers := []string{}
	rangeSize, err = partitionMgr.CalculateLookAheadRange(ctx, emptyWorkers, workerPartitionMultiple)
	if err != nil {
		t.Errorf("计算ID探测范围失败: %v", err)
	}

	// 期望的探测范围 = 分区大小 * 1 * 倍数 = 1000 * 1 * 3 = 3000
	expectedRangeSize = int64(3000)
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

	partitionMgr1 := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    planer1,
	})

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

	partitionMgr2 := NewPartitionManager(PartitionManagerConfig{
		Namespace: "test",
		Strategy:  test_utils.NewMockPartitionStrategy(),
		Logger:    test_utils.NewMockLogger(false),
		Planer:    planer2,
	})

	size, err = partitionMgr2.GetEffectivePartitionSize(ctx)
	if err != nil {
		t.Errorf("获取有效分区大小失败: %v", err)
	}

	if size != model.DefaultPartitionSize {
		t.Errorf("分区大小不正确，期望默认值 %d，得到 %d", model.DefaultPartitionSize, size)
	}
}
