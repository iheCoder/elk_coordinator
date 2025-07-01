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
	archivedPartition, err := strategy.GetArchivedPartition(ctx, 1)
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
	completedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
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
	completedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	assert.Equal(t, numPartitions, completedCount)

	// 验证每个归档分区的数据完整性
	for i := 1; i <= numPartitions; i++ {
		archived, err := strategy.GetArchivedPartition(ctx, i)
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
	archived, err := strategy.GetArchivedPartition(ctx, 100)
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
		IncludeArchived: false,
	})
	require.NoError(t, err)
	assert.Len(t, activeOnly, 3, "活跃层应该有3个分区") // 分区3,4,5

	// 查询包含归档层
	allIncluded, err := strategy.GetFilteredPartitions(ctx, model.GetPartitionsFilters{
		IncludeArchived: true,
	})
	require.NoError(t, err)
	assert.Len(t, allIncluded, 5, "包含归档层应该有5个分区")

	// 验证状态过滤功能
	completedOnly, err := strategy.GetFilteredPartitions(ctx, model.GetPartitionsFilters{
		TargetStatuses:  []model.PartitionStatus{model.StatusCompleted},
		IncludeArchived: true,
	})
	require.NoError(t, err)
	assert.Len(t, completedOnly, 2, "应该找到2个已完成分区")
	for _, p := range completedOnly {
		assert.Equal(t, model.StatusCompleted, p.Status)
	}
}

// TestHashPartitionStrategy_ArchiveCompressionThreshold 测试压缩阈值触发
func TestHashPartitionStrategy_ArchiveCompressionThreshold(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建接近压缩阈值的分区数量
	const testPartitionCount = 120000 // 超过10万压缩阈值
	t.Logf("开始创建 %d 个分区用于压缩测试", testPartitionCount)

	// 分批创建分区，避免一次性创建过多分区导致超时
	batchSize := 10000
	for batchStart := 1; batchStart <= testPartitionCount; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > testPartitionCount {
			batchEnd = testPartitionCount
		}

		var requests []model.CreatePartitionRequest
		for i := batchStart; i <= batchEnd; i++ {
			requests = append(requests, model.CreatePartitionRequest{
				PartitionID: i,
				MinID:       int64((i-1)*1000 + 1),
				MaxID:       int64(i * 1000),
			})
		}

		_, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
			Partitions: requests,
		})
		require.NoError(t, err)

		t.Logf("已创建分区批次 %d-%d", batchStart, batchEnd)
	}

	t.Log("开始处理和归档分区，触发压缩")

	// 分批处理分区，模拟真实的worker处理过程
	processedCount := 0
	for i := 1; i <= testPartitionCount; i++ {
		workerID := fmt.Sprintf("worker-%d", (i%100)+1) // 使用100个worker模拟

		// 获取分区
		_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
		require.NoError(t, err)
		require.True(t, success)

		// 直接设置为完成状态，触发归档
		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
			"test_batch":        fmt.Sprintf("batch_%d", i/1000),
			"processed_records": i * 10,
		})
		require.NoError(t, err)

		processedCount++

		// 每处理1000个分区检查一次压缩状态
		if processedCount%1000 == 0 {
			uncompressedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
			require.NoError(t, err)
			t.Logf("处理了 %d 个分区，当前未压缩归档数：%d", processedCount, uncompressedCount)

			// 当超过阈值时，应该开始压缩
			if processedCount > 100000 {
				// 验证压缩是否生效（未压缩归档数应该控制在合理范围内）
				assert.LessOrEqual(t, uncompressedCount, 120000, "压缩应该已经生效，未压缩分区数应该被控制")
			}
		}
	}

	// 最终验证压缩效果
	t.Log("验证最终压缩效果")

	uncompressedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	t.Logf("最终未压缩归档分区数：%d", uncompressedCount)

	// 验证压缩确实发生了（未压缩分区数应该远小于总数）
	assert.LessOrEqual(t, uncompressedCount, 100000, "应该有大量分区被压缩")

	// 验证查询功能：随机查询一些分区，确保能够正确获取（包括被压缩的）
	t.Log("验证压缩数据查询功能")
	testPartitionIDs := []int{1, 5000, 50000, 100000, 115000} // 跨越压缩边界的分区

	for _, partitionID := range testPartitionIDs {
		partition, err := strategy.GetArchivedPartition(ctx, partitionID)
		require.NoError(t, err, "应该能够查询到分区 %d（无论是否被压缩）", partitionID)
		require.NotNil(t, partition)
		assert.Equal(t, partitionID, partition.PartitionID)
		assert.Equal(t, model.StatusCompleted, partition.Status)
	}

	// 验证GetAllArchivedPartitions能返回所有分区（包括压缩的）
	t.Log("验证获取所有归档分区功能")
	allPartitions, err := strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, testPartitionCount, "应该能获取到所有归档分区")
}

// TestHashPartitionStrategy_CompressedDataQuery 测试压缩数据查询功能
func TestHashPartitionStrategy_CompressedDataQuery(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建足够多的分区来触发压缩
	const partitionCount = 55000 // 刚好超过一个压缩批次
	t.Logf("创建 %d 个分区用于压缩查询测试", partitionCount)

	// 分批创建和处理分区
	batchSize := 5000
	var allPartitionIDs []int

	for batchStart := 1; batchStart <= partitionCount; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > partitionCount {
			batchEnd = partitionCount
		}

		var requests []model.CreatePartitionRequest
		for i := batchStart; i <= batchEnd; i++ {
			requests = append(requests, model.CreatePartitionRequest{
				PartitionID: i,
				MinID:       int64((i-1)*100 + 1),
				MaxID:       int64(i * 100),
			})
			allPartitionIDs = append(allPartitionIDs, i)
		}

		_, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
			Partitions: requests,
		})
		require.NoError(t, err)

		// 处理这批分区
		for i := batchStart; i <= batchEnd; i++ {
			workerID := fmt.Sprintf("worker-%d", i%10+1)

			_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
			require.NoError(t, err)
			require.True(t, success)

			err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
				"batch_info":  fmt.Sprintf("batch_%d", batchStart/batchSize),
				"final_count": i * 10,
				"test_data":   fmt.Sprintf("test_value_%d", i),
			})
			require.NoError(t, err)
		}

		t.Logf("完成批次 %d-%d 的处理", batchStart, batchEnd)
	}

	// 等待所有归档和压缩操作完成
	time.Sleep(100 * time.Millisecond)

	// 验证单个分区查询（混合压缩和未压缩数据）
	t.Log("测试单个分区查询")
	testCases := []struct {
		partitionID int
		expectFound bool
		description string
	}{
		{1, true, "第一个分区（可能被压缩）"},
		{25000, true, "中间分区（可能被压缩）"},
		{50000, true, "接近阈值分区（可能被压缩）"},
		{55000, true, "最后一个分区（可能未压缩）"},
		{999999, false, "不存在的分区"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			partition, err := strategy.GetArchivedPartition(ctx, tc.partitionID)
			if tc.expectFound {
				require.NoError(t, err)
				require.NotNil(t, partition)
				assert.Equal(t, tc.partitionID, partition.PartitionID)
				assert.Equal(t, model.StatusCompleted, partition.Status)

				// 验证元数据完整性
				require.NotNil(t, partition.Options)
				assert.Contains(t, partition.Options, "final_count")
				assert.Contains(t, partition.Options, "test_data")

				expectedTestData := fmt.Sprintf("test_value_%d", tc.partitionID)
				assert.Equal(t, expectedTestData, partition.Options["test_data"])
			} else {
				assert.ErrorIs(t, err, ErrArchivedPartitionNotFound)
				assert.Nil(t, partition)
			}
		})
	}

	// 验证批量查询所有归档分区
	t.Log("测试批量查询所有归档分区")
	allPartitions, err := strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, partitionCount, "应该获取到所有归档分区")

	// 验证返回分区的数据完整性
	partitionMap := make(map[int]*model.PartitionInfo)
	for _, p := range allPartitions {
		partitionMap[p.PartitionID] = p
	}

	// 随机检查一些分区的数据完整性
	checkPartitionIDs := []int{1, 1000, 25000, 40000, 55000}
	for _, partitionID := range checkPartitionIDs {
		partition, exists := partitionMap[partitionID]
		require.True(t, exists, "分区 %d 应该存在于返回结果中", partitionID)
		assert.Equal(t, partitionID, partition.PartitionID)
		assert.Equal(t, model.StatusCompleted, partition.Status)

		// 验证范围正确性
		expectedMinID := int64((partitionID-1)*100 + 1)
		expectedMaxID := int64(partitionID * 100)
		assert.Equal(t, expectedMinID, partition.MinID)
		assert.Equal(t, expectedMaxID, partition.MaxID)
	}

	// 验证压缩统计信息
	t.Log("验证压缩统计")
	uncompressedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	t.Logf("未压缩归档分区数：%d", uncompressedCount)

	// 由于总数超过了50000，应该有至少50000个分区被压缩
	expectedCompressed := partitionCount - uncompressedCount
	t.Logf("推测被压缩的分区数：%d", expectedCompressed)
	assert.Greater(t, expectedCompressed, 0, "应该有分区被压缩")
}

// TestHashPartitionStrategy_MixedArchiveQueries 测试混合归档查询（压缩+未压缩）
func TestHashPartitionStrategy_MixedArchiveQueries(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 第一阶段：创建足够多的分区触发第一次压缩
	t.Log("第一阶段：创建并处理足够多分区触发压缩")
	const firstBatchSize = 52000

	for i := 1; i <= firstBatchSize; i++ {
		// 创建分区
		_, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
			Partitions: []model.CreatePartitionRequest{
				{PartitionID: i, MinID: int64((i-1)*100 + 1), MaxID: int64(i * 100)},
			},
		})
		require.NoError(t, err)

		// 处理分区
		workerID := fmt.Sprintf("worker-%d", i%20+1)
		_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
		require.NoError(t, err)
		require.True(t, success)

		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
			"phase":       "first_batch",
			"batch_index": i / 1000,
			"final_count": i * 5,
		})
		require.NoError(t, err)

		if i%10000 == 0 {
			t.Logf("第一阶段已处理 %d 个分区", i)
		}
	}

	// 验证第一次压缩效果
	uncompressedCount1, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	t.Logf("第一阶段完成，未压缩归档分区数：%d", uncompressedCount1)

	// 第二阶段：再添加一些分区，创建混合状态（部分压缩，部分未压缩）
	t.Log("第二阶段：添加更多分区创建混合状态")
	const secondBatchStart = firstBatchSize + 1
	const secondBatchEnd = firstBatchSize + 30000

	for i := secondBatchStart; i <= secondBatchEnd; i++ {
		// 创建分区
		_, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
			Partitions: []model.CreatePartitionRequest{
				{PartitionID: i, MinID: int64((i-1)*100 + 1), MaxID: int64(i * 100)},
			},
		})
		require.NoError(t, err)

		// 处理分区
		workerID := fmt.Sprintf("worker-%d", i%15+1)
		_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
		require.NoError(t, err)
		require.True(t, success)

		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
			"phase":        "second_batch",
			"newer_data":   true,
			"process_time": time.Now().Unix(),
		})
		require.NoError(t, err)

		if (i-secondBatchStart)%5000 == 0 {
			t.Logf("第二阶段已处理 %d 个分区", i-secondBatchStart+1)
		}
	}

	// 验证混合状态
	uncompressedCount2, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	t.Logf("第二阶段完成，未压缩归档分区数：%d", uncompressedCount2)

	totalPartitions := secondBatchEnd
	t.Logf("总分区数：%d", totalPartitions)

	// 测试混合查询功能
	t.Log("测试混合查询功能")

	// 1. 测试GetAllArchivedPartitions能正确合并压缩和未压缩数据
	allPartitions, err := strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, totalPartitions, "应该获取到所有分区（压缩+未压缩）")

	// 2. 测试随机单个分区查询（跨越压缩边界）
	testPartitionIDs := []int{
		1,     // 很早的分区，应该被压缩
		25000, // 第一批中间的分区，应该被压缩
		50000, // 第一批末尾的分区，可能被压缩
		52001, // 第二批开始的分区，可能未压缩
		70000, // 第二批中间的分区，可能未压缩
		82000, // 最后的分区，很可能未压缩
	}

	for _, partitionID := range testPartitionIDs {
		t.Run(fmt.Sprintf("查询分区%d", partitionID), func(t *testing.T) {
			partition, err := strategy.GetArchivedPartition(ctx, partitionID)
			require.NoError(t, err)
			require.NotNil(t, partition)

			assert.Equal(t, partitionID, partition.PartitionID)
			assert.Equal(t, model.StatusCompleted, partition.Status)

			// 验证分区所属的阶段信息
			require.NotNil(t, partition.Options)
			if partitionID <= firstBatchSize {
				assert.Equal(t, "first_batch", partition.Options["phase"])
				assert.Contains(t, partition.Options, "batch_index")
			} else {
				assert.Equal(t, "second_batch", partition.Options["phase"])
				assert.Equal(t, true, partition.Options["newer_data"])
			}
		})
	}

	// 3. 验证数据一致性：对比单个查询和批量查询的结果
	t.Log("验证数据一致性")
	partitionMap := make(map[int]*model.PartitionInfo)
	for _, p := range allPartitions {
		partitionMap[p.PartitionID] = p
	}

	for _, partitionID := range testPartitionIDs {
		// 单个查询
		singleResult, err := strategy.GetArchivedPartition(ctx, partitionID)
		require.NoError(t, err)

		// 批量查询结果
		batchResult, exists := partitionMap[partitionID]
		require.True(t, exists)

		// 对比两种查询方式的结果
		assert.Equal(t, singleResult.PartitionID, batchResult.PartitionID)
		assert.Equal(t, singleResult.Status, batchResult.Status)
		assert.Equal(t, singleResult.WorkerID, batchResult.WorkerID)
		assert.Equal(t, singleResult.MinID, batchResult.MinID)
		assert.Equal(t, singleResult.MaxID, batchResult.MaxID)
		// 注意：时间戳可能因为序列化精度有轻微差异，使用Unix时间戳比较
		assert.Equal(t, singleResult.CreatedAt.Unix(), batchResult.CreatedAt.Unix())
	}

	// 4. 性能测试：批量查询应该比多次单个查询快
	t.Log("性能对比测试")

	// 单个查询的时间
	singleQueryStart := time.Now()
	for _, partitionID := range testPartitionIDs {
		_, err := strategy.GetArchivedPartition(ctx, partitionID)
		require.NoError(t, err)
	}
	singleQueryDuration := time.Since(singleQueryStart)

	// 批量查询的时间
	batchQueryStart := time.Now()
	_, err = strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	batchQueryDuration := time.Since(batchQueryStart)

	t.Logf("单个查询总耗时：%v", singleQueryDuration)
	t.Logf("批量查询耗时：%v", batchQueryDuration)

	// 在大量数据情况下，批量查询通常更高效（但这不是强制性断言，因为测试环境可能差异很大）
	if totalPartitions > 50000 {
		t.Logf("大数据量测试：单个查询平均耗时 %v，批量查询平均每项 %v",
			singleQueryDuration/time.Duration(len(testPartitionIDs)),
			batchQueryDuration/time.Duration(totalPartitions))
	}
}

// TestHashPartitionStrategy_CompressionErrorHandling 测试压缩错误处理
func TestHashPartitionStrategy_CompressionErrorHandling(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试查询不存在的归档分区
	t.Log("测试查询不存在的归档分区")

	nonExistentIDs := []int{999999, -1, 0}
	for _, partitionID := range nonExistentIDs {
		partition, err := strategy.GetArchivedPartition(ctx, partitionID)
		assert.ErrorIs(t, err, ErrArchivedPartitionNotFound, "查询不存在分区 %d 应该返回特定错误", partitionID)
		assert.Nil(t, partition)
	}

	// 测试空归档状态下的各种操作
	t.Log("测试空归档状态下的操作")

	// 空状态下获取归档分区数量
	count, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// 空状态下获取所有归档分区
	allPartitions, err := strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, 0)

	// 创建少量分区（不足以触发压缩）测试边界情况
	t.Log("测试压缩阈值边界情况")

	const smallBatchSize = 1000 // 远小于压缩阈值
	for i := 1; i <= smallBatchSize; i++ {
		_, err := strategy.CreatePartitionsIfNotExist(ctx, model.CreatePartitionsRequest{
			Partitions: []model.CreatePartitionRequest{
				{PartitionID: i, MinID: int64(i), MaxID: int64(i)},
			},
		})
		require.NoError(t, err)

		workerID := fmt.Sprintf("worker-%d", i%5+1)
		_, success, err := strategy.AcquirePartition(ctx, i, workerID, nil)
		require.NoError(t, err)
		require.True(t, success)

		err = strategy.UpdatePartitionStatus(ctx, i, workerID, model.StatusCompleted, map[string]interface{}{
			"small_batch": true,
		})
		require.NoError(t, err)
	}

	// 验证未达到压缩阈值时的状态
	uncompressedCount, err := strategy.GetArchivedPartitionsCountWithoutCompress(ctx)
	require.NoError(t, err)
	assert.Equal(t, smallBatchSize, uncompressedCount, "未达到压缩阈值时，所有分区都应该未压缩")

	// 验证这些分区都可以正常查询
	for i := 1; i <= smallBatchSize; i++ {
		partition, err := strategy.GetArchivedPartition(ctx, i)
		require.NoError(t, err)
		assert.Equal(t, i, partition.PartitionID)
		assert.Equal(t, true, partition.Options["small_batch"])
	}

	allPartitions, err = strategy.GetAllArchivedPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, smallBatchSize)
}
