package data

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/iheCoder/elk_coordinator/model"
	"github.com/stretchr/testify/assert"
)

// calculateStatsFromPartitions 从分区数据中预计算统计信息
func calculateStatsFromPartitions(partitions map[string]string) (*model.PartitionStats, error) {
	stats := &model.PartitionStats{
		MaxPartitionID:  0,
		LastAllocatedID: 0,
		// 其他字段使用默认值（批量创建时我们只关心这两个字段）
	}

	maxPartitionIDFromData := int64(0) // 用于跟踪最大的partition_id
	hasExplicitMaxID := false          // 标记是否有显式的max_id字段

	for field, value := range partitions {
		var partitionData map[string]interface{}
		if err := json.Unmarshal([]byte(value), &partitionData); err != nil {
			return nil, fmt.Errorf("parse partition %s failed: %v", field, err)
		}

		// 获取partition_id
		if partitionIDValue, exists := partitionData["partition_id"]; exists {
			if partitionID, ok := partitionIDValue.(float64); ok {
				partitionIDInt := int(partitionID) // 转换为int类型以匹配PartitionStats
				if partitionIDInt > 0 && partitionIDInt > stats.MaxPartitionID {
					stats.MaxPartitionID = partitionIDInt
				}
				// 跟踪最大的partition_id用于后续逻辑
				if int64(partitionIDInt) > maxPartitionIDFromData {
					maxPartitionIDFromData = int64(partitionIDInt)
				}
			}
		}

		// 获取max_id（如果存在）
		if maxIDValue, exists := partitionData["max_id"]; exists {
			hasExplicitMaxID = true
			if maxID, ok := maxIDValue.(float64); ok {
				maxIDInt := int64(maxID)
				if maxIDInt > stats.LastAllocatedID {
					stats.LastAllocatedID = maxIDInt
				}
			}
		}
	}

	// 如果没有显式的max_id字段，则使用最大的partition_id作为LastAllocatedID
	// 这适用于简化场景，其中partition_id本身就代表了最后分配的ID
	if !hasExplicitMaxID && maxPartitionIDFromData > stats.LastAllocatedID {
		stats.LastAllocatedID = maxPartitionIDFromData
	}

	return stats, nil
}

// TestHSetPartitionsWithStatsInTx 测试原子性批量创建分区并更新统计数据
func TestHSetPartitionsWithStatsInTx(t *testing.T) {
	store, _, cleanup := setupRedisTest(t)
	defer cleanup()

	ctx := context.Background()
	partitionKey := "test:partitions"
	statsKey := "test:stats"

	t.Run("NormalBatchCreation", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据（模拟正常业务场景）
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 准备测试分区数据
		partitions := map[string]string{
			"1": `{"partition_id":1,"status":"pending","worker_id":"worker1","created_at":"2024-01-01T00:00:00Z"}`,
			"3": `{"partition_id":3,"status":"running","worker_id":"worker2","created_at":"2024-01-01T00:01:00Z"}`,
			"5": `{"partition_id":5,"status":"completed","worker_id":"worker3","created_at":"2024-01-01T00:02:00Z"}`,
		}

		// 预计算统计数据
		stats, err := calculateStatsFromPartitions(partitions)
		assert.NoError(t, err)
		fmt.Printf("DEBUG: calculated stats: MaxPartitionID=%d, LastAllocatedID=%d\n", stats.MaxPartitionID, stats.LastAllocatedID)

		// 调用原子性批量创建方法
		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, partitions, stats)
		assert.NoError(t, err)

		// 验证分区数据正确写入
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(allPartitions))
		assert.Equal(t, partitions["1"], allPartitions["1"])
		assert.Equal(t, partitions["3"], allPartitions["3"])
		assert.Equal(t, partitions["5"], allPartitions["5"])

		// 验证统计数据正确更新
		maxPartitionIDResult, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "5", maxPartitionIDResult) // 最大partition_id应该是5

		lastAllocatedIDResult, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "last_allocated_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "5", lastAllocatedIDResult) // last_allocated_id应该是5

		// 验证 total 和 pending 计数
		totalResult, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "total").Result()
		assert.NoError(t, err)
		assert.Equal(t, "3", totalResult) // 总数应该是3

		pendingResult, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "pending").Result()
		assert.NoError(t, err)
		assert.Equal(t, "3", pendingResult) // pending数应该是3（新创建的分区默认都是pending状态）
	})

	t.Run("EmptyPartitions", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 测试空分区列表
		emptyPartitions := map[string]string{}
		emptyStats := &model.PartitionStats{
			MaxPartitionID:  0,
			LastAllocatedID: 0,
		}
		err := store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, emptyPartitions, emptyStats)
		assert.NoError(t, err)

		// 验证没有分区被创建
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(allPartitions))

		// 验证统计数据不存在
		exists, err := store.rds.Exists(ctx, store.prefixKey(statsKey)).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("IdempotentOperation", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 准备测试分区数据
		partitions := map[string]string{
			"2": `{"partition_id":2,"status":"pending","worker_id":"worker1"}`,
			"4": `{"partition_id":4,"status":"running","worker_id":"worker2"}`,
		}

		// 预计算统计数据
		stats, err := calculateStatsFromPartitions(partitions)
		assert.NoError(t, err)

		// 第一次调用
		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, partitions, stats)
		assert.NoError(t, err)

		// 验证第一次调用结果
		maxPartitionID1, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "4", maxPartitionID1)

		totalResult1, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "total").Result()
		assert.NoError(t, err)
		assert.Equal(t, "2", totalResult1)

		// 第二次调用相同的分区（幂等性测试）
		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, partitions, stats)
		assert.NoError(t, err)

		// 验证分区数据一致
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(allPartitions))
		assert.Equal(t, partitions["2"], allPartitions["2"])
		assert.Equal(t, partitions["4"], allPartitions["4"])

		// 验证统计数据增加了（因为重复创建会累加计数）
		maxPartitionID2, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "4", maxPartitionID2)

		totalResult2, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "total").Result()
		assert.NoError(t, err)
		assert.Equal(t, "4", totalResult2) // 第二次调用会再次增加计数
	})

	t.Run("IncrementalUpdate", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 第一批分区
		firstBatch := map[string]string{
			"1": `{"partition_id":1,"status":"pending"}`,
			"3": `{"partition_id":3,"status":"running"}`,
		}

		firstBatchStats, err := calculateStatsFromPartitions(firstBatch)
		assert.NoError(t, err)

		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, firstBatch, firstBatchStats)
		assert.NoError(t, err)

		// 验证第一批结果
		maxPartitionIDResult1, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "3", maxPartitionIDResult1)

		totalResult1, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "total").Result()
		assert.NoError(t, err)
		assert.Equal(t, "2", totalResult1)

		// 第二批分区（包含更大的partition_id）
		secondBatch := map[string]string{
			"2": `{"partition_id":2,"status":"completed"}`,
			"7": `{"partition_id":7,"status":"pending"}`,
		}

		secondBatchStats, err := calculateStatsFromPartitions(secondBatch)
		assert.NoError(t, err)

		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, secondBatch, secondBatchStats)
		assert.NoError(t, err)

		// 验证合并后的结果
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(allPartitions)) // 总共4个分区

		// 验证统计数据正确更新为最大值
		maxPartitionIDResult2, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "7", maxPartitionIDResult2) // 最大partition_id应该更新为7

		lastAllocatedIDResult, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "last_allocated_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "7", lastAllocatedIDResult)

		totalResult2, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "total").Result()
		assert.NoError(t, err)
		assert.Equal(t, "4", totalResult2) // 总数应该是4

		pendingResult2, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "pending").Result()
		assert.NoError(t, err)
		assert.Equal(t, "4", pendingResult2) // pending数应该是4
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 包含无效JSON的分区数据
		invalidPartitions := map[string]string{
			"1": `{"partition_id":1,"status":"pending"}`,
			"2": `{invalid json}`, // 无效JSON
			"3": `{"partition_id":3,"status":"running"}`,
		}

		// 计算统计数据时应该返回错误
		_, err := calculateStatsFromPartitions(invalidPartitions)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "parse partition")

		// 验证没有分区被创建
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(allPartitions))

		// 验证统计数据也没有被创建
		exists, err := store.rds.Exists(ctx, store.prefixKey(statsKey)).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("MissingPartitionID", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 包含缺少partition_id的分区数据
		invalidPartitions := map[string]string{
			"1": `{"partition_id":1,"status":"pending"}`,
			"2": `{"status":"running"}`, // 缺少partition_id
		}

		// 预计算时会正常处理（因为缺少partition_id只是不会被统计）
		stats, err := calculateStatsFromPartitions(invalidPartitions)
		assert.NoError(t, err)

		// 调用应该成功，但统计数据只反映有效的分区
		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, invalidPartitions, stats)
		assert.NoError(t, err)

		// 验证分区被设置（包括无效的那个）
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(allPartitions))
	})

	t.Run("ConcurrentUpdate", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		numWorkers := 5
		partitionsPerWorker := 10
		var wg sync.WaitGroup
		errChan := make(chan error, numWorkers)

		// 并发执行批量创建
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// 每个worker创建不同范围的分区
				partitions := make(map[string]string)
				start := workerID * partitionsPerWorker
				for j := 0; j < partitionsPerWorker; j++ {
					partitionID := start + j + 1 // partition_id从1开始
					field := strconv.Itoa(partitionID)
					partition := fmt.Sprintf(`{"partition_id":%d,"status":"pending","worker_id":"worker_%d"}`, partitionID, workerID)
					partitions[field] = partition
				} // 预计算统计数据
				stats, err := calculateStatsFromPartitions(partitions)
				if err != nil {
					errChan <- fmt.Errorf("worker %d failed to calculate stats: %v", workerID, err)
					return
				}

				err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, partitions, stats)
				if err != nil {
					errChan <- fmt.Errorf("worker %d failed: %v", workerID, err)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// 检查是否有错误
		for err := range errChan {
			t.Error(err)
		}

		// 验证所有分区都被正确创建
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		expectedCount := numWorkers * partitionsPerWorker
		assert.Equal(t, expectedCount, len(allPartitions))

		// 验证统计数据正确（最大partition_id应该是最大的）
		maxPartitionID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		expectedMaxID := strconv.Itoa(numWorkers * partitionsPerWorker)
		assert.Equal(t, expectedMaxID, maxPartitionID)

		lastAllocatedID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "last_allocated_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, expectedMaxID, lastAllocatedID)
	})

	t.Run("SinglePartition", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 测试单个分区
		singlePartition := map[string]string{
			"42": `{"partition_id":42,"status":"pending","worker_id":"worker1","metadata":{"key":"value"}}`,
		}

		singleStats, err := calculateStatsFromPartitions(singlePartition)
		assert.NoError(t, err)

		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, singlePartition, singleStats)
		assert.NoError(t, err)

		// 验证分区数据
		partition, err := store.HGetPartition(ctx, partitionKey, "42")
		assert.NoError(t, err)
		assert.Equal(t, singlePartition["42"], partition)

		// 验证统计数据
		maxPartitionID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "42", maxPartitionID)

		lastAllocatedID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "last_allocated_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "42", lastAllocatedID)
	})

	t.Run("ZeroPartitionID", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 初始化统计数据
		err := store.InitPartitionStats(ctx, statsKey)
		assert.NoError(t, err)

		// 测试partition_id为0的情况
		zeroPartition := map[string]string{
			"0": `{"partition_id":0,"status":"pending"}`,
			"1": `{"partition_id":1,"status":"running"}`,
		}

		zeroStats, err := calculateStatsFromPartitions(zeroPartition)
		assert.NoError(t, err)

		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, zeroPartition, zeroStats)
		assert.NoError(t, err)

		// 验证统计数据（应该是最大的非零值）
		maxPartitionID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "max_partition_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "1", maxPartitionID) // 应该是1而不是0

		lastAllocatedID, err := store.rds.HGet(ctx, store.prefixKey(statsKey), "last_allocated_id").Result()
		assert.NoError(t, err)
		assert.Equal(t, "1", lastAllocatedID)
	})

	t.Run("StatsKeyNotExists", func(t *testing.T) {
		// 清理之前的测试数据
		store.rds.FlushAll(ctx)

		// 准备测试分区数据
		partitions := map[string]string{
			"1": `{"partition_id":1,"status":"pending"}`,
		}

		stats, err := calculateStatsFromPartitions(partitions)
		assert.NoError(t, err)

		// 不初始化统计键，直接调用批量创建方法应该报错
		err = store.HSetPartitionsWithStatsInTx(ctx, partitionKey, statsKey, partitions, stats)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "stats key does not exist")

		// 验证没有分区被创建
		allPartitions, err := store.HGetAllPartitions(ctx, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(allPartitions))
	})
}
