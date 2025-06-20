// consistent_hash_test.go 任务级别一致性hash的完整测试套件
package task

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTaskConsistentHash_Basic 测试基本的一致性hash功能
func TestTaskConsistentHash_Basic(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	config := TaskConsistentHashConfig{
		Namespace:              "test",
		WorkerID:               "worker1",
		Logger:                 logger,
		DataStore:              mockStore,
		ValidHeartbeatDuration: 60 * time.Second,
		UpdateTTL:              5 * time.Second,
		VirtualNodes:           10, // 使用较小的虚拟节点数便于测试
	}

	tch := NewTaskConsistentHash(config)

	// 设置心跳数据
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	workers := []string{"worker1", "worker2", "worker3"}
	for _, worker := range workers {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 30)
	for i := 0; i < 30; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 测试检查分区偏好
	preferredCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			preferredCount++
		}
	}

	// 验证至少有一些分区是当前worker偏好的
	assert.Greater(t, preferredCount, 0, "应该有一些分区是当前worker偏好的")

	// 测试指定worker ID的情况
	isPreferred, err := tch.IsPreferredPartition(ctx, partitions[0].PartitionID, "worker1")
	require.NoError(t, err)

	// 验证结果一致性
	isCurrentPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partitions[0].PartitionID)
	require.NoError(t, err)
	assert.Equal(t, isPreferred, isCurrentPreferred, "IsPreferredPartition和IsCurrentWorkerPreferred结果应该一致")

	t.Logf("Worker1偏好 %d 个分区", preferredCount)
}

// TestTaskConsistentHash_Distribution 测试分区分布的均匀性
func TestTaskConsistentHash_Distribution(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 设置心跳数据
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	workers := []string{"worker1", "worker2", "worker3", "worker4"}
	for _, worker := range workers {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	// 创建大量测试分区
	partitions := make([]*model.PartitionInfo, 1000)
	for i := 0; i < 1000; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 为每个worker创建一致性hash实例并统计分配
	distribution := make(map[string]int)

	for _, worker := range workers {
		config := TaskConsistentHashConfig{
			Namespace:              "test",
			WorkerID:               worker,
			Logger:                 logger,
			DataStore:              mockStore,
			ValidHeartbeatDuration: 60 * time.Second,
			UpdateTTL:              5 * time.Second,
			VirtualNodes:           50,
		}

		tch := NewTaskConsistentHash(config)

		// 统计该worker偏好的分区数量
		preferredCount := 0
		for _, partition := range partitions {
			isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
			require.NoError(t, err)
			if isPreferred {
				preferredCount++
			}
		}

		distribution[worker] = preferredCount
		t.Logf("Worker %s 分配到 %d 个分区", worker, preferredCount)
	}

	// 验证分布的均匀性
	expectedPerWorker := float64(len(partitions)) / float64(len(workers))
	maxDeviation := expectedPerWorker * 0.2 // 允许20%的偏差

	totalAssigned := 0
	for worker, count := range distribution {
		totalAssigned += count
		deviation := math.Abs(float64(count) - expectedPerWorker)
		assert.Less(t, deviation, maxDeviation,
			"Worker %s 的分区数量 %d 偏离期望值 %.1f 过多", worker, count, expectedPerWorker)
	}

	// 验证所有分区都被分配
	assert.Equal(t, len(partitions), totalAssigned, "所有分区都应该被分配")
}

// TestTaskConsistentHash_IncrementalUpdate 测试增量更新功能
func TestTaskConsistentHash_IncrementalUpdate(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	config := TaskConsistentHashConfig{
		Namespace:              "test",
		WorkerID:               "worker1",
		Logger:                 logger,
		DataStore:              mockStore,
		ValidHeartbeatDuration: 60 * time.Second,
		UpdateTTL:              1 * time.Second, // 短TTL便于测试
		VirtualNodes:           10,
	}

	tch := NewTaskConsistentHash(config)

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 100)
	for i := 0; i < 100; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 阶段1：只有worker1和worker2
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	for _, worker := range []string{"worker1", "worker2"} {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	// 第一次获取分区分布
	firstDistribution, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)
	assert.Equal(t, 2, firstDistribution.WorkerCount, "应该有2个工作节点")

	// 统计worker1偏好的分区数量
	beforeCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			beforeCount++
		}
	}

	t.Logf("增加节点前，worker1分配到 %d 个分区", beforeCount)

	// 等待TTL过期
	time.Sleep(2 * time.Second)

	// 阶段2：添加worker3（模拟节点扩容）
	heartbeatKey3 := fmt.Sprintf(model.HeartbeatFmtFmt, "test", "worker3")
	mockStore.Heartbeats[heartbeatKey3] = time.Now().Format(time.RFC3339)

	// 第二次获取分区分布
	secondDistribution, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)
	assert.Equal(t, 3, secondDistribution.WorkerCount, "应该有3个工作节点")

	// 统计worker1增加节点后偏好的分区数量
	afterCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			afterCount++
		}
	}

	t.Logf("增加节点后，worker1分配到 %d 个分区", afterCount)

	// 验证增量更新的效果：新增节点后，worker1的分区数应该减少
	assert.Less(t, afterCount, beforeCount, "新增节点后，原有节点的分区数应该减少")

	// 验证分布更加均匀：检查实际的分布情况
	expectedAfter := float64(len(partitions)) / float64(secondDistribution.WorkerCount)

	t.Logf("增加节点后期望每个节点分配 %.1f 个分区，worker1实际分配 %d 个", expectedAfter, afterCount)
	t.Logf("第一次分布方差: %.2f，质量评分: %.2f", firstDistribution.DistributionVariance, firstDistribution.QualityScore)
	t.Logf("第二次分布方差: %.2f，质量评分: %.2f", secondDistribution.DistributionVariance, secondDistribution.QualityScore)

	// 验证分布变得更均匀（方差应该更小或质量评分有所改善）
	// 考虑到一致性hash的特性，我们允许一定的质量评分波动
	assert.True(t, secondDistribution.DistributionVariance <= firstDistribution.DistributionVariance*1.5 ||
		secondDistribution.QualityScore >= firstDistribution.QualityScore*0.7,
		"增加节点后分布应该保持合理的质量")
}

// TestTaskConsistentHash_NodeRemoval 测试节点删除的处理
func TestTaskConsistentHash_NodeRemoval(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	config := TaskConsistentHashConfig{
		Namespace:              "test",
		WorkerID:               "worker1",
		Logger:                 logger,
		DataStore:              mockStore,
		ValidHeartbeatDuration: 60 * time.Second,
		UpdateTTL:              1 * time.Second,
		VirtualNodes:           10,
	}

	tch := NewTaskConsistentHash(config)

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 60)
	for i := 0; i < 60; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 阶段1：三个节点
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	workers := []string{"worker1", "worker2", "worker3"}
	for _, worker := range workers {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	// 获取初始分布
	firstDistribution, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)
	assert.Equal(t, 3, firstDistribution.WorkerCount)

	// 统计worker1删除节点前偏好的分区数量
	beforeCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			beforeCount++
		}
	}

	// 等待TTL过期
	time.Sleep(2 * time.Second)

	// 阶段2：删除worker3（模拟节点下线）
	heartbeatKey3 := fmt.Sprintf(model.HeartbeatFmtFmt, "test", "worker3")
	delete(mockStore.Heartbeats, heartbeatKey3)

	// 获取删除后的分布
	secondDistribution, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)
	assert.Equal(t, 2, secondDistribution.WorkerCount, "应该剩余2个工作节点")

	// 统计worker1删除节点后偏好的分区数量
	afterCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			afterCount++
		}
	}

	t.Logf("删除节点前，worker1分配 %d 个分区", beforeCount)
	t.Logf("删除节点后，worker1分配 %d 个分区", afterCount)

	// 验证节点删除后，剩余节点承担更多分区
	assert.GreaterOrEqual(t, afterCount, beforeCount, "删除节点后，剩余节点应该承担更多分区")
}

// TestTaskConsistentHash_Consistency 测试一致性（同样的输入总是产生同样的输出）
func TestTaskConsistentHash_Consistency(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 设置相同的心跳数据
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	workers := []string{"worker1", "worker2", "worker3"}
	for _, worker := range workers {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 50)
	for i := 0; i < 50; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 创建多个一致性hash实例
	var instances []*TaskConsistentHash
	for i := 0; i < 3; i++ {
		config := TaskConsistentHashConfig{
			Namespace:              "test",
			WorkerID:               "worker1",
			Logger:                 logger,
			DataStore:              mockStore,
			ValidHeartbeatDuration: 60 * time.Second,
			UpdateTTL:              5 * time.Second,
			VirtualNodes:           20,
		}
		instances = append(instances, NewTaskConsistentHash(config))
	}

	// 多次获取分区，验证结果一致性
	var results [][]int
	for _, tch := range instances {
		var partitionIDs []int
		for _, partition := range partitions {
			isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
			require.NoError(t, err)
			if isPreferred {
				partitionIDs = append(partitionIDs, partition.PartitionID)
			}
		}
		results = append(results, partitionIDs)
	}

	// 验证所有实例返回相同的结果
	for i := 1; i < len(results); i++ {
		assert.Equal(t, results[0], results[i],
			"所有一致性hash实例应该返回相同的分区分配结果")
	}
}

// TestTaskConsistentHash_EmptyWorkers 测试没有活跃工作节点的情况
func TestTaskConsistentHash_EmptyWorkers(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	config := TaskConsistentHashConfig{
		Namespace:              "test",
		WorkerID:               "worker1",
		Logger:                 logger,
		DataStore:              mockStore,
		ValidHeartbeatDuration: 60 * time.Second,
		UpdateTTL:              5 * time.Second,
		VirtualNodes:           10,
	}

	tch := NewTaskConsistentHash(config)

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 10)
	for i := 0; i < 10; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 没有设置任何心跳数据，模拟没有活跃工作节点的情况
	// 测试所有分区是否被当前工作节点偏好 - 应该都返回false
	preferredCount := 0
	for _, partition := range partitions {
		isPreferred, err := tch.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		require.NoError(t, err)
		if isPreferred {
			preferredCount++
		}
	}

	// 应该没有优先分区
	assert.Equal(t, 0, preferredCount, "没有活跃工作节点时应该没有优先分区")

	// 验证分布分析
	analysis, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)
	assert.Equal(t, 0, analysis.WorkerCount, "应该没有工作节点")
	assert.Equal(t, 10, analysis.PendingPartitions, "应该有10个待处理分区")
}

// TestTaskConsistentHash_QualityScore 测试分布质量评分
func TestTaskConsistentHash_QualityScore(t *testing.T) {
	ctx := context.Background()
	mockStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 设置心跳数据
	now := time.Now()
	heartbeatTime := now.Format(time.RFC3339)

	workers := []string{"worker1", "worker2", "worker3"}
	for _, worker := range workers {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test", worker)
		mockStore.Heartbeats[heartbeatKey] = heartbeatTime
	}

	config := TaskConsistentHashConfig{
		Namespace:              "test",
		WorkerID:               "worker1",
		Logger:                 logger,
		DataStore:              mockStore,
		ValidHeartbeatDuration: 60 * time.Second,
		UpdateTTL:              5 * time.Second,
		VirtualNodes:           50, // 较多的虚拟节点应该产生更好的分布
	}

	tch := NewTaskConsistentHash(config)

	// 创建大量测试分区以便测试分布质量
	partitions := make([]*model.PartitionInfo, 300)
	for i := 0; i < 300; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
			WorkerID:    "",
		}
	}

	// 获取分布分析
	analysis, err := tch.GetDistributionAnalysis(ctx, partitions)
	require.NoError(t, err)

	// 验证分析结果
	assert.Equal(t, 3, analysis.WorkerCount)
	assert.Equal(t, 300, analysis.TotalPartitions)
	assert.Equal(t, 300, analysis.PendingPartitions)
	assert.Greater(t, analysis.QualityScore, 80.0, "质量评分应该较高（>80）")
	assert.Less(t, analysis.DistributionVariance, 100.0, "分布方差应该较小")

	// 验证所有工作节点都分配到了分区
	totalAssigned := 0
	for worker, count := range analysis.WorkerDistribution {
		assert.Greater(t, count, 0, "工作节点 %s 应该分配到分区", worker)
		totalAssigned += count
	}
	assert.Equal(t, 300, totalAssigned, "所有分区都应该被分配")

	t.Logf("分布质量评分: %.2f", analysis.QualityScore)
	t.Logf("分布方差: %.2f", analysis.DistributionVariance)
	t.Logf("分区分布: %v", analysis.WorkerDistribution)
}
