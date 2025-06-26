package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iheCoder/elk_coordinator/model"
)

// TestHashPartitionStrategy_ArchiveCompletedPartition 测试分离式归档策略
func TestHashPartitionStrategy_ArchiveCompletedPartition(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 步骤1: 创建并获取分区
	partitions, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	})
	require.NoError(t, err)
	require.Len(t, partitions, 1)

	// 步骤2: Worker获取分区
	partition, success, err := strategy.AcquirePartition(ctx, 1, "test-worker", nil)
	require.NoError(t, err)
	require.True(t, success)
	require.Equal(t, "test-worker", partition.WorkerID)
	require.Equal(t, model.StatusClaimed, partition.Status)

	// 步骤3: 更新状态为运行中
	err = strategy.UpdatePartitionStatus(ctx, 1, "test-worker", model.StatusRunning, map[string]interface{}{
		"started_at": time.Now().Unix(),
	})
	require.NoError(t, err)

	// 验证分区仍在Active Layer中
	activePartition, err := strategy.GetPartition(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, model.StatusRunning, activePartition.Status)

	// 步骤4: 更新状态为已完成 - 触发分离式归档
	err = strategy.UpdatePartitionStatus(ctx, 1, "test-worker", model.StatusCompleted, map[string]interface{}{
		"completed_at":      time.Now().Unix(),
		"records_processed": 1000,
	})
	require.NoError(t, err)

	// 验证：分区现在应该从Archive Layer中获取（而不是Active Layer）
	partitionAfterArchive, err := strategy.GetPartition(ctx, 1)
	require.NoError(t, err, "GetPartition现在应该能从Archive Layer获取分区")
	require.Equal(t, model.StatusCompleted, partitionAfterArchive.Status)
	require.Equal(t, "test-worker", partitionAfterArchive.WorkerID)

	// 验证：同样的分区应该已归档到Archive Layer（使用专门的归档查询确认）
	archivedPartition, err := strategy.GetCompletedPartition(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, archivedPartition)

	// 验证归档分区的数据完整性
	assert.Equal(t, 1, archivedPartition.PartitionID)
	assert.Equal(t, int64(1), archivedPartition.MinID)
	assert.Equal(t, int64(1000), archivedPartition.MaxID)
	assert.Equal(t, "test-worker", archivedPartition.WorkerID)
	assert.Equal(t, model.StatusCompleted, archivedPartition.Status)

	// 验证元数据是否正确保存
	require.NotNil(t, archivedPartition.Options)
	assert.Contains(t, archivedPartition.Options, "completed_at")
	assert.Contains(t, archivedPartition.Options, "records_processed")
	// JSON 反序列化会将数字转为 float64，需要正确处理类型
	if recordsProcessed, ok := archivedPartition.Options["records_processed"].(float64); ok {
		assert.Equal(t, float64(1000), recordsProcessed)
	} else if recordsProcessed, ok := archivedPartition.Options["records_processed"].(int); ok {
		assert.Equal(t, 1000, recordsProcessed)
	} else {
		t.Errorf("records_processed 类型不正确: %T", archivedPartition.Options["records_processed"])
	}

	// 步骤5: 验证统计信息
	completedCount, err := strategy.GetCompletedPartitionsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, completedCount)

	// 步骤6: 验证获取所有已完成分区
	allCompleted, err := strategy.GetAllCompletedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allCompleted, 1)
	assert.Equal(t, 1, allCompleted[0].PartitionID)
}

// TestHashPartitionStrategy_ArchiveMultiplePartitions 测试多分区归档
func TestHashPartitionStrategy_ArchiveMultiplePartitions(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	const numPartitions = 5

	// 创建多个分区
	var requests []model.CreatePartitionRequest
	for i := 1; i <= numPartitions; i++ {
		requests = append(requests, model.CreatePartitionRequest{
			PartitionID: i,
			MinID:       int64((i-1)*1000 + 1),
			MaxID:       int64(i * 1000),
		})
	}

	partitions, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
		Partitions: requests,
	})
	require.NoError(t, err)
	require.Len(t, partitions, numPartitions)

	// 模拟处理每个分区
	for i := 1; i <= numPartitions; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		// 获取分区
		_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
		require.NoError(t, err)
		require.True(t, success)

		// 开始处理
		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusRunning, nil)
		require.NoError(t, err)

		// 完成处理 - 触发归档
		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
			"processed_records": i * 1000,
		})
		require.NoError(t, err)
	}

	// 验证所有分区都可以通过GetPartition获取（无论是从Active Layer还是Archive Layer）
	for i := 1; i <= numPartitions; i++ {
		partition, err := strategy.GetPartition(ctx, i)
		require.NoError(t, err, "分区 %d 应该可以通过GetPartition获取", i)
		assert.Equal(t, model.StatusCompleted, partition.Status, "分区 %d 状态应该是completed", i)
	}

	// 验证所有分区都已归档
	completedCount, err := strategy.GetCompletedPartitionsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, numPartitions, completedCount)

	// 验证每个归档分区的数据完整性
	for i := 1; i <= numPartitions; i++ {
		archived, err := strategy.GetCompletedPartition(ctx, i)
		require.NoError(t, err)
		assert.Equal(t, i, archived.PartitionID)
		assert.Equal(t, model.StatusCompleted, archived.Status)
		assert.Equal(t, fmt.Sprintf("worker-%d", i), archived.WorkerID)
	}
}

// TestHashPartitionStrategy_ArchiveIntegrity 测试归档数据完整性
func TestHashPartitionStrategy_ArchiveIntegrity(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	partitions, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 100, MinID: 100001, MaxID: 101000},
		},
	})
	require.NoError(t, err)
	require.Len(t, partitions, 1)

	// 获取并处理分区
	originalPartition, success, err := strategy.AcquirePartition(ctx, 100, "integrity-worker", nil)
	require.NoError(t, err)
	require.True(t, success)

	// 记录原始创建时间用于验证
	originalCreatedAt := originalPartition.CreatedAt

	// 运行状态
	err = strategy.UpdatePartitionStatus(ctx, 100, "integrity-worker", model.StatusRunning, map[string]interface{}{
		"test_metadata": "original_value", // 在运行状态时添加测试元数据
		"progress":      50,
		"runtime_info":  "processing...",
	})
	require.NoError(t, err)

	// 完成状态并归档
	completionTime := time.Now()
	err = strategy.UpdatePartitionStatus(ctx, 100, "integrity-worker", model.StatusCompleted, map[string]interface{}{
		"completion_time": completionTime.Unix(),
		"final_count":     1000,
		"success":         true,
	})
	require.NoError(t, err)

	// 验证归档分区包含所有原始数据
	archived, err := strategy.GetCompletedPartition(ctx, 100)
	require.NoError(t, err)

	// 基本分区信息验证
	assert.Equal(t, 100, archived.PartitionID)
	assert.Equal(t, int64(100001), archived.MinID)
	assert.Equal(t, int64(101000), archived.MaxID)
	assert.Equal(t, "integrity-worker", archived.WorkerID)
	assert.Equal(t, model.StatusCompleted, archived.Status)

	// 时间戳验证
	assert.Equal(t, originalCreatedAt.Unix(), archived.CreatedAt.Unix())
	assert.True(t, archived.UpdatedAt.After(completionTime.Add(-time.Second)))

	// 元数据完整性验证
	require.NotNil(t, archived.Options)

	// 验证所有阶段的元数据都被保留
	assert.Equal(t, "original_value", archived.Options["test_metadata"])

	// JSON 反序列化会将数字转换为 float64，需要正确处理
	if progress, ok := archived.Options["progress"].(float64); ok {
		assert.Equal(t, float64(50), progress)
	} else if progress, ok := archived.Options["progress"].(int); ok {
		assert.Equal(t, 50, progress)
	} else {
		t.Errorf("progress 类型不正确: %T", archived.Options["progress"])
	}

	assert.Equal(t, "processing...", archived.Options["runtime_info"])

	if completionTime, ok := archived.Options["completion_time"].(float64); ok {
		assert.True(t, completionTime > 0, "completion_time 应该大于0")
	} else if completionTime, ok := archived.Options["completion_time"].(int64); ok {
		assert.True(t, completionTime > 0, "completion_time 应该大于0")
	} else {
		t.Errorf("completion_time 类型不正确: %T", archived.Options["completion_time"])
	}

	if finalCount, ok := archived.Options["final_count"].(float64); ok {
		assert.Equal(t, float64(1000), finalCount)
	} else if finalCount, ok := archived.Options["final_count"].(int); ok {
		assert.Equal(t, 1000, finalCount)
	} else {
		t.Errorf("final_count 类型不正确: %T", archived.Options["final_count"])
	}

	assert.Equal(t, true, archived.Options["success"])
}

// TestHashPartitionStrategy_StatsWithArchive 测试带归档的统计功能
func TestHashPartitionStrategy_StatsWithArchive(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建多个分区
	partitions, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
			{PartitionID: 4, MinID: 3001, MaxID: 4000},
			{PartitionID: 5, MinID: 4001, MaxID: 5000},
		},
	})
	require.NoError(t, err)
	require.Len(t, partitions, 5)

	// 模拟不同状态的分区
	// 分区1和2：完成并归档
	_, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	require.NoError(t, err)
	require.True(t, success)
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusRunning, nil)
	require.NoError(t, err)
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusCompleted, nil)
	require.NoError(t, err)

	_, success, err = strategy.AcquirePartition(ctx, 2, "worker-2", nil)
	require.NoError(t, err)
	require.True(t, success)
	err = strategy.UpdatePartitionStatus(ctx, 2, "worker-2", model.StatusRunning, nil)
	require.NoError(t, err)
	err = strategy.UpdatePartitionStatus(ctx, 2, "worker-2", model.StatusCompleted, nil)
	require.NoError(t, err)

	// 分区3：运行中
	_, success, err = strategy.AcquirePartition(ctx, 3, "worker-3", nil)
	require.NoError(t, err)
	require.True(t, success)
	err = strategy.UpdatePartitionStatus(ctx, 3, "worker-3", model.StatusRunning, nil)
	require.NoError(t, err)

	// 分区4：失败
	_, success, err = strategy.AcquirePartition(ctx, 4, "worker-4", nil)
	require.NoError(t, err)
	require.True(t, success)
	err = strategy.UpdatePartitionStatus(ctx, 4, "worker-4", model.StatusRunning, nil)
	require.NoError(t, err)
	err = strategy.UpdatePartitionStatus(ctx, 4, "worker-4", model.StatusFailed, map[string]interface{}{
		"error": "processing failed",
	})
	require.NoError(t, err)

	// 分区5：保持待处理状态

	// 验证统计信息
	stats, err := strategy.GetPartitionStats(ctx)
	require.NoError(t, err)

	// 验证总数和各状态统计
	assert.Equal(t, 5, stats.Total, "总分区数应该是5")
	assert.Equal(t, 2, stats.Completed, "已完成分区数应该是2")
	assert.Equal(t, 1, stats.Running, "运行中分区数应该是1")
	assert.Equal(t, 1, stats.Failed, "失败分区数应该是1")
	assert.Equal(t, 1, stats.Pending, "待处理分区数应该是1")

	// 验证比率计算
	expectedCompletionRate := float64(2) / float64(5) // 2/5 = 0.4
	expectedFailureRate := float64(1) / float64(5)    // 1/5 = 0.2
	assert.InDelta(t, expectedCompletionRate, stats.CompletionRate, 0.001)
	assert.InDelta(t, expectedFailureRate, stats.FailureRate, 0.001)

	// 验证跨层查询功能
	// 查询已完成分区（应该从Archive Layer找到）
	completedPartition1, err := strategy.GetPartition(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, completedPartition1.Status)
	assert.Equal(t, "worker-1", completedPartition1.WorkerID)

	// 查询活跃分区（应该从Active Layer找到）
	runningPartition3, err := strategy.GetPartition(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, runningPartition3.Status)
	assert.Equal(t, "worker-3", runningPartition3.WorkerID)

	// 验证过滤查询
	// 只查询活跃层
	activeOnly, err := strategy.GetFilteredPartitions(ctx, model.GetPartitionsFilters{
		IncludeCompleted: false,
	})
	require.NoError(t, err)
	assert.Len(t, activeOnly, 3, "活跃层应该有3个分区") // 分区3,4,5

	// 查询包含归档层
	allIncluded, err := strategy.GetFilteredPartitions(ctx, model.GetPartitionsFilters{
		IncludeCompleted: true,
	})
	require.NoError(t, err)
	assert.Len(t, allIncluded, 5, "包含归档层应该有5个分区")

	// 验证状态过滤功能
	completedOnly, err := strategy.GetFilteredPartitions(ctx, model.GetPartitionsFilters{
		TargetStatuses:   []model.PartitionStatus{model.StatusCompleted},
		IncludeCompleted: true,
	})
	require.NoError(t, err)
	assert.Len(t, completedOnly, 2, "应该找到2个已完成分区")
	for _, p := range completedOnly {
		assert.Equal(t, model.StatusCompleted, p.Status)
	}
}
