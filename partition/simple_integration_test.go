package partition

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/test_utils"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// setupSimpleStrategyIntegrationTest 创建集成测试环境
func setupSimpleStrategyIntegrationTest(t *testing.T) (*SimpleStrategy, *miniredis.Miniredis, *data.RedisDataStore, func()) {
	// 创建 miniredis 实例
	mr, err := miniredis.Run()
	require.NoError(t, err, "启动 miniredis 失败")

	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// 测试连接
	err = client.Ping(context.Background()).Err()
	require.NoError(t, err, "连接 Redis 失败")

	// 创建 RedisDataStore
	opts := &data.Options{
		KeyPrefix:     "test:simple:",
		DefaultExpiry: 5 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	// 创建 SimpleStrategy
	config := SimpleStrategyConfig{
		Namespace:           "test",
		DataStore:           dataStore,
		Logger:              test_utils.NewMockLogger(true),
		PartitionLockExpiry: 30 * time.Second,
	}
	strategy := NewSimpleStrategy(config)

	// 清理函数
	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return strategy, mr, dataStore, cleanup
}

// TestSimpleStrategy_BasicCRUD 测试基本的CRUD操作
func TestSimpleStrategy_BasicCRUD(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试创建分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
		},
	}

	// 创建分区
	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	assert.NoError(t, err)
	assert.Len(t, createdPartitions, 3)

	// 验证创建的分区
	for i, partition := range createdPartitions {
		assert.Equal(t, request.Partitions[i].PartitionID, partition.PartitionID)
		assert.Equal(t, request.Partitions[i].MinID, partition.MinID)
		assert.Equal(t, request.Partitions[i].MaxID, partition.MaxID)
		assert.Equal(t, model.StatusPending, partition.Status)
		assert.NotZero(t, partition.CreatedAt)
		assert.NotZero(t, partition.UpdatedAt)
		assert.Equal(t, int64(1), partition.Version)
	}

	// 测试获取单个分区
	partition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, partition.PartitionID)
	assert.Equal(t, int64(1), partition.MinID)
	assert.Equal(t, int64(1000), partition.MaxID)

	// 测试获取所有分区
	allPartitions, err := strategy.GetAllPartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, 3)

	// 测试更新分区
	partition.Status = model.StatusRunning
	partition.WorkerID = "worker-1"
	updatedPartition, err := strategy.UpdatePartition(ctx, partition, nil)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, updatedPartition.Status)
	assert.Equal(t, "worker-1", updatedPartition.WorkerID)
	assert.Equal(t, int64(2), updatedPartition.Version)

	// 测试删除分区
	err = strategy.DeletePartition(ctx, 3)
	assert.NoError(t, err)

	// 验证删除
	_, err = strategy.GetPartition(ctx, 3)
	assert.Error(t, err)

	allPartitions, err = strategy.GetAllPartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, 2)
}

// TestSimpleStrategy_FilteredOperations 测试过滤操作
func TestSimpleStrategy_FilteredOperations(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建测试分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
			{PartitionID: 4, MinID: 3001, MaxID: 4000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 更新一些分区状态
	createdPartitions[0].Status = model.StatusRunning
	createdPartitions[0].WorkerID = "worker-1"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[0], nil)
	require.NoError(t, err)

	createdPartitions[1].Status = model.StatusCompleted
	createdPartitions[1].WorkerID = "worker-2"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[1], nil)
	require.NoError(t, err)

	// 测试按状态过滤
	filters := model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending},
	}
	partitions, err := strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 2) // 分区3和4应该还是pending状态

	// 测试按状态过滤 - 多个状态
	filters.TargetStatuses = []model.PartitionStatus{model.StatusRunning, model.StatusCompleted}
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 2) // 分区1和2

	// 测试限制数量
	filters.TargetStatuses = []model.PartitionStatus{model.StatusPending}
	filters.Limit = 1
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 1)

	// 测试排除分区ID
	filters.Limit = 0 // 取消限制
	filters.ExcludePartitionIDs = []int{3}
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 1) // 只有分区4
	assert.Equal(t, 4, partitions[0].PartitionID)

	// 测试ID范围过滤
	filters = model.GetPartitionsFilters{
		MinID: func(v int) *int { return &v }(2),
		MaxID: func(v int) *int { return &v }(3),
	}
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 2) // 分区2和3

	// 测试过时过滤 - 手动设置更新时间
	staleDuration := 1 * time.Second

	// 获取所有分区并设置为过时
	allPartitions, err := strategy.GetAllPartitions(ctx)
	require.NoError(t, err)

	staleTime := time.Now().Add(-2 * time.Second)
	for _, partition := range allPartitions {
		partition.UpdatedAt = staleTime
		_, err = strategy.UpdatePartition(ctx, partition, nil)
		require.NoError(t, err)
	}

	filters = model.GetPartitionsFilters{
		StaleDuration: &staleDuration,
	}
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 4) // 所有分区都应该被认为是过时的

	// 测试过时过滤 - 排除特定worker
	filters.ExcludeWorkerIDOnStale = "worker-1"
	partitions, err = strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, partitions, 3) // 应该排除worker-1持有的分区
}

// TestSimpleStrategy_BatchOperations 测试批量操作
func TestSimpleStrategy_BatchOperations(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建多个分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
			{PartitionID: 4, MinID: 3001, MaxID: 4000},
			{PartitionID: 5, MinID: 4001, MaxID: 5000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	assert.NoError(t, err)
	assert.Len(t, createdPartitions, 5)

	// 测试重复创建（应该返回现有分区）
	createdPartitions2, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	assert.NoError(t, err)
	assert.Len(t, createdPartitions2, 5)

	// 验证返回的是相同的分区（通过版本号）
	for i := range createdPartitions {
		assert.Equal(t, createdPartitions[i].Version, createdPartitions2[i].Version)
	}

	// 测试批量删除
	partitionIDsToDelete := []int{2, 4}
	err = strategy.DeletePartitions(ctx, partitionIDsToDelete)
	assert.NoError(t, err)

	// 验证删除结果
	allPartitions, err := strategy.GetAllPartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, 3)

	// 验证剩余的分区ID
	remainingIDs := make(map[int]bool)
	for _, partition := range allPartitions {
		remainingIDs[partition.PartitionID] = true
	}
	assert.True(t, remainingIDs[1])
	assert.False(t, remainingIDs[2])
	assert.True(t, remainingIDs[3])
	assert.False(t, remainingIDs[4])
	assert.True(t, remainingIDs[5])
}

// TestSimpleStrategy_ConcurrentUpdates 测试并发操作
func TestSimpleStrategy_ConcurrentUpdates(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建初始分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}

	_, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 并发更新同一个分区
	const numWorkers = 10
	doneCh := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			partition, err := strategy.GetPartition(ctx, 1)
			if err != nil {
				doneCh <- err
				return
			}

			partition.WorkerID = fmt.Sprintf("worker-%d", workerID)
			partition.Status = model.StatusRunning

			_, err = strategy.UpdatePartition(ctx, partition, nil)
			doneCh <- err
		}(i)
	}

	// 等待所有goroutine完成
	var errors []error
	for i := 0; i < numWorkers; i++ {
		if err := <-doneCh; err != nil {
			errors = append(errors, err)
		}
	}

	// 验证最终状态
	finalPartition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, finalPartition.Status)
	assert.NotEmpty(t, finalPartition.WorkerID)

	// 版本应该被正确递增
	assert.True(t, finalPartition.Version > 1)
}

// TestSimpleStrategy_ErrorCases 测试错误情况
func TestSimpleStrategy_ErrorCases(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试获取不存在的分区
	_, err := strategy.GetPartition(ctx, 999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "分区 999 不存在")

	// 测试删除不存在的分区
	err = strategy.DeletePartition(ctx, 999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "分区 999 不存在")

	// 测试更新nil分区
	_, err = strategy.UpdatePartition(ctx, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "分区信息不能为空")
}

// TestSimpleStrategy_VersionControl 测试版本控制
func TestSimpleStrategy_VersionControl(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)
	originalPartition := createdPartitions[0]
	assert.Equal(t, int64(1), originalPartition.Version)

	// 第一次更新
	originalPartition.Status = model.StatusRunning
	updatedPartition1, err := strategy.UpdatePartition(ctx, originalPartition, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), updatedPartition1.Version)

	// 第二次更新
	updatedPartition1.WorkerID = "worker-1"
	updatedPartition2, err := strategy.UpdatePartition(ctx, updatedPartition1, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), updatedPartition2.Version)

	// 验证创建时间保持不变，更新时间发生变化
	assert.Equal(t, originalPartition.CreatedAt, updatedPartition2.CreatedAt)
	assert.True(t, updatedPartition2.UpdatedAt.After(originalPartition.UpdatedAt) ||
		updatedPartition2.UpdatedAt.Equal(updatedPartition1.UpdatedAt))
}

// TestSimpleStrategy_LockOperations 测试分区锁操作
func TestSimpleStrategy_LockOperations(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建测试分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}

	_, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 测试获取分区（AcquirePartition功能）
	options := &model.AcquirePartitionOptions{
		AllowPreemption: false,
	}

	partition, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", options)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.NotNil(t, partition)
	assert.Equal(t, 1, partition.PartitionID)
	assert.Equal(t, "worker-1", partition.WorkerID)
	assert.Equal(t, model.StatusClaimed, partition.Status)

	// 测试另一个worker不能获取同一个分区
	partition2, success2, err := strategy.AcquirePartition(ctx, 1, "worker-2", options)
	assert.NoError(t, err)
	assert.False(t, success2)
	assert.Nil(t, partition2) // 应该没有可用分区

	// 测试维持分区持有
	err = strategy.MaintainPartitionHold(ctx, 1, "worker-1")
	assert.NoError(t, err)

	// 测试更新分区状态
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusRunning, nil)
	assert.NoError(t, err)

	// 验证状态更新
	updatedPartition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, updatedPartition.Status)

	// 测试释放分区
	err = strategy.ReleasePartition(ctx, 1, "worker-1")
	assert.NoError(t, err)

	// 验证分区可以被其他worker获取
	partition3, success3, err := strategy.AcquirePartition(ctx, 1, "worker-2", options)
	assert.NoError(t, err)
	assert.True(t, success3)
	assert.NotNil(t, partition3)
	assert.Equal(t, "worker-2", partition3.WorkerID)
}

// TestSimpleStrategy_PartitionStats 测试分区统计功能
func TestSimpleStrategy_PartitionStats(t *testing.T) {
	strategy, _, _, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建多个不同状态的分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
			{PartitionID: 4, MinID: 3001, MaxID: 4000},
			{PartitionID: 5, MinID: 4001, MaxID: 5000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 设置不同的状态
	createdPartitions[0].Status = model.StatusRunning
	createdPartitions[0].WorkerID = "worker-1"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[0], nil)
	require.NoError(t, err)

	createdPartitions[1].Status = model.StatusCompleted
	createdPartitions[1].WorkerID = "worker-2"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[1], nil)
	require.NoError(t, err)

	createdPartitions[2].Status = model.StatusFailed
	createdPartitions[2].WorkerID = "worker-3"
	createdPartitions[2].Error = "处理失败"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[2], nil)
	require.NoError(t, err)

	// 获取分区统计
	stats, err := strategy.GetPartitionStats(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5, stats.Total)
	assert.Equal(t, 2, stats.Pending)          // 分区4和5
	assert.Equal(t, 1, stats.Running)          // 分区1
	assert.Equal(t, 1, stats.Completed)        // 分区2
	assert.Equal(t, 1, stats.Failed)           // 分区3
	assert.Equal(t, 0.2, stats.CompletionRate) // 1/5
	assert.Equal(t, 0.2, stats.FailureRate)    // 1/5
}

// TestSimpleStrategy_PreemptionOperations 测试抢占操作
func TestSimpleStrategy_PreemptionOperations(t *testing.T) {
	strategy, _, dataStore, cleanup := setupSimpleStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建测试分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}

	_, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// worker-1获取分区
	options := &model.AcquirePartitionOptions{AllowPreemption: false}
	partition, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", options)
	require.NoError(t, err)
	require.True(t, success)
	require.NotNil(t, partition)

	// worker-2尝试获取分区（不允许抢占）
	partition2, success2, err := strategy.AcquirePartition(ctx, 1, "worker-2", options)
	assert.NoError(t, err)
	assert.False(t, success2)
	assert.Nil(t, partition2)

	// 模拟worker-1释放锁，允许worker-2抢占
	// 在实际的SimpleStrategy中，抢占是基于分布式锁的，而不是基于时间
	// 使用与SimpleStrategy相同的锁键格式: "{namespace}:partition_lock:{partitionID}"
	lockKey := "test:partition_lock:1"
	err = dataStore.ReleaseLock(ctx, lockKey, "worker-1")
	require.NoError(t, err)

	// worker-2现在可以抢占分区（因为锁已释放）
	preemptOptions := &model.AcquirePartitionOptions{AllowPreemption: true}
	preemptedPartition, success3, err := strategy.AcquirePartition(ctx, 1, "worker-2", preemptOptions)
	assert.NoError(t, err)
	assert.True(t, success3)
	assert.NotNil(t, preemptedPartition)
	assert.Equal(t, "worker-2", preemptedPartition.WorkerID)

	// 验证原始worker不能再维持分区持有
	err = strategy.MaintainPartitionHold(ctx, 1, "worker-1")
	assert.Error(t, err) // 应该失败，因为分区已被抢占
}
