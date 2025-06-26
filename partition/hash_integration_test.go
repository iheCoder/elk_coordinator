package partition

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupHashPartitionStrategyIntegrationTest 创建集成测试环境
func setupHashPartitionStrategyIntegrationTest(t *testing.T) (*HashPartitionStrategy, *miniredis.Miniredis, *data.RedisDataStore, func()) {
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
		KeyPrefix:     "test:hash:",
		DefaultExpiry: 5 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	// 创建 HashPartitionStrategy
	logger := test_utils.NewMockLogger(true)
	strategy := NewHashPartitionStrategy(dataStore, logger)

	// 清理函数
	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return strategy, mr, dataStore, cleanup
}

// setupHashPartitionStrategyWithCustomConfig 创建带自定义配置的集成测试环境
func setupHashPartitionStrategyWithCustomConfig(t *testing.T, staleThreshold time.Duration) (*HashPartitionStrategy, *miniredis.Miniredis, *data.RedisDataStore, func()) {
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
		KeyPrefix:     "test:hash:",
		DefaultExpiry: 5 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	// 创建 HashPartitionStrategy with custom config
	logger := test_utils.NewMockLogger(true)
	strategy := NewHashPartitionStrategyWithConfig(dataStore, logger, staleThreshold)

	// 清理函数
	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return strategy, mr, dataStore, cleanup
}

// TestHashPartitionStrategy_BasicCRUD 测试基本的CRUD操作
func TestHashPartitionStrategy_BasicCRUD(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
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
	allPartitions, err := strategy.GetAllActivePartitions(ctx)
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
	assert.ErrorIs(t, err, ErrPartitionNotFound)

	allPartitions, err = strategy.GetAllActivePartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, 2)
}

// TestHashPartitionStrategy_FilteredOperations 测试过滤操作
func TestHashPartitionStrategy_FilteredOperations(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
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

	createdPartitions[1].Status = model.StatusClaimed
	createdPartitions[1].WorkerID = "worker-2"
	_, err = strategy.UpdatePartition(ctx, createdPartitions[1], nil)
	require.NoError(t, err)

	// 测试状态过滤
	filters := model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending},
	}
	pendingPartitions, err := strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, pendingPartitions, 2) // partitions 3 and 4 should be pending

	// 验证返回的分区确实是pending状态
	for _, partition := range pendingPartitions {
		assert.Equal(t, model.StatusPending, partition.Status)
		assert.Empty(t, partition.WorkerID)
	}

	// 测试多状态过滤
	filters = model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusRunning, model.StatusClaimed},
	}
	activePartitions, err := strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)
	assert.Len(t, activePartitions, 2) // partitions 1 and 2

	// 验证返回的分区状态
	statusMap := make(map[int]model.PartitionStatus)
	for _, partition := range activePartitions {
		statusMap[partition.PartitionID] = partition.Status
	}
	assert.Equal(t, model.StatusRunning, statusMap[1])
	assert.Equal(t, model.StatusClaimed, statusMap[2])

	// 测试工作节点过滤 - 通过状态和手动验证WorkerID
	filters = model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusRunning, model.StatusClaimed},
	}
	workerPartitions, err := strategy.GetFilteredPartitions(ctx, filters)
	assert.NoError(t, err)

	// 手动过滤worker-1的分区
	worker1Partitions := make([]*model.PartitionInfo, 0)
	for _, p := range workerPartitions {
		if p.WorkerID == "worker-1" {
			worker1Partitions = append(worker1Partitions, p)
		}
	}
	assert.Len(t, worker1Partitions, 1)
	assert.Equal(t, "worker-1", worker1Partitions[0].WorkerID)
	assert.Equal(t, 1, worker1Partitions[0].PartitionID)
}

// TestHashPartitionStrategy_BatchOperations 测试批量操作
func TestHashPartitionStrategy_BatchOperations(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试批量创建
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

	// 验证所有分区都被创建
	for i, partition := range createdPartitions {
		assert.Equal(t, request.Partitions[i].PartitionID, partition.PartitionID)
		assert.Equal(t, model.StatusPending, partition.Status)
		assert.Equal(t, int64(1), partition.Version)
	}

	// 测试重复创建应该返回现有分区
	duplicatePartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	assert.NoError(t, err)
	assert.Len(t, duplicatePartitions, 5)

	// 验证返回的是现有分区（版本号应该相同）
	for i, partition := range duplicatePartitions {
		assert.Equal(t, createdPartitions[i].PartitionID, partition.PartitionID)
		assert.Equal(t, createdPartitions[i].Version, partition.Version)
	}

	// 测试批量删除
	partitionIDs := []int{1, 3, 5}
	err = strategy.DeletePartitions(ctx, partitionIDs)
	assert.NoError(t, err)

	// 验证删除结果
	remainingPartitions, err := strategy.GetAllActivePartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, remainingPartitions, 2)

	// 验证剩余的分区
	remainingIDs := make(map[int]bool)
	for _, partition := range remainingPartitions {
		remainingIDs[partition.PartitionID] = true
	}
	assert.True(t, remainingIDs[2])
	assert.True(t, remainingIDs[4])
	assert.False(t, remainingIDs[1])
	assert.False(t, remainingIDs[3])
	assert.False(t, remainingIDs[5])
}

// TestHashPartitionStrategy_ConcurrentOperations 测试并发操作
func TestHashPartitionStrategy_ConcurrentOperations(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建测试分区
	_, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	const numWorkers = 10
	type result struct {
		success bool
		err     error
	}
	results := make(chan result, numWorkers)

	var wg sync.WaitGroup

	// 启动多个goroutine尝试获取同一个分区
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerName := fmt.Sprintf("worker-%d", workerID)
			_, success, err := strategy.AcquirePartition(ctx, 1, workerName, nil)
			results <- result{success, err}
		}(i)
	}

	wg.Wait()
	close(results)

	// 统计结果
	var successCount, failureCount, errorCount int
	for result := range results {
		if result.err != nil {
			errorCount++
			fmt.Printf("Worker error: %v\n", result.err)
		} else if result.success {
			successCount++
		} else {
			failureCount++
		}
	}

	// 验证结果：应该只有一个成功，其他都是正常失败
	assert.Equal(t, 1, successCount, "应该只有一个worker成功获取分区")
	assert.Equal(t, numWorkers-1, failureCount, "其他worker应该正常失败")
	assert.Equal(t, 0, errorCount, "不应该有系统错误")

	// 验证分区确实被获取
	partition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusClaimed, partition.Status)
	assert.NotEmpty(t, partition.WorkerID)
}

// TestHashPartitionStrategy_ErrorHandling 测试错误处理
func TestHashPartitionStrategy_ErrorHandling(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试获取不存在的分区
	_, err := strategy.GetPartition(ctx, 999)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrPartitionNotFound)

	// 测试删除不存在的分区
	err = strategy.DeletePartition(ctx, 999)
	assert.NoError(t, err) // Redis HDEL 对不存在的字段不报错

	// 测试使用空工作节点ID获取分区
	_, success, err := strategy.AcquirePartition(ctx, 1, "", nil)
	assert.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "工作节点ID不能为空")

	// 测试创建重复分区
	_, err = strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	_, err = strategy.CreatePartitionAtomically(ctx, 1, 1001, 2000, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrPartitionAlreadyExists)

	// 测试无效的版本控制更新
	partition, err := strategy.GetPartition(ctx, 1)
	require.NoError(t, err)

	// 尝试使用过期版本更新
	_, err = strategy.UpdatePartitionOptimistically(ctx, partition, partition.Version-1)
	assert.Error(t, err)
}

// TestHashPartitionStrategy_VersionControl 测试版本控制
func TestHashPartitionStrategy_VersionControl(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	partition, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), partition.Version)

	// 测试乐观锁更新成功
	partition.Status = model.StatusRunning
	updatedPartition, err := strategy.UpdatePartitionOptimistically(ctx, partition, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), updatedPartition.Version)
	assert.Equal(t, model.StatusRunning, updatedPartition.Status)

	// 测试乐观锁更新失败（版本不匹配）
	partition.Status = model.StatusCompleted
	_, err = strategy.UpdatePartitionOptimistically(ctx, partition, 1) // 使用过期版本
	assert.Error(t, err)

	// 验证分区状态没有改变
	currentPartition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, currentPartition.Status)
	assert.Equal(t, int64(2), currentPartition.Version)

	// 测试使用正确版本更新
	currentPartition.Status = model.StatusCompleted
	finalPartition, err := strategy.UpdatePartitionOptimistically(ctx, currentPartition, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), finalPartition.Version)
	assert.Equal(t, model.StatusCompleted, finalPartition.Status)
}

// TestHashPartitionStrategy_LockOperations 测试锁操作
func TestHashPartitionStrategy_LockOperations(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	_, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	// 测试获取分区
	acquiredPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-1", acquiredPartition.WorkerID)
	assert.Equal(t, model.StatusClaimed, acquiredPartition.Status)

	// 测试同一工作节点重复获取
	reacquiredPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-1", reacquiredPartition.WorkerID)

	// 测试其他工作节点获取被占用的分区（不允许抢占）
	_, success, err = strategy.AcquirePartition(ctx, 1, "worker-2", nil)
	assert.NoError(t, err)
	assert.False(t, success)

	// 测试释放分区
	err = strategy.ReleasePartition(ctx, 1, "worker-1")
	assert.NoError(t, err)

	// 验证分区已释放
	partition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusPending, partition.Status)
	assert.Empty(t, partition.WorkerID)

	// 测试其他工作节点现在可以获取分区
	newAcquiredPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-2", nil)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-2", newAcquiredPartition.WorkerID)

	// 测试无权限释放分区
	err = strategy.ReleasePartition(ctx, 1, "worker-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "无权释放")
}

// TestHashPartitionStrategy_HeartbeatAndPreemption 测试心跳和抢占
func TestHashPartitionStrategy_HeartbeatAndPreemption(t *testing.T) {
	// 使用短超时时间进行测试
	strategy, _, _, cleanup := setupHashPartitionStrategyWithCustomConfig(t, 100*time.Millisecond)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	_, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	// 工作节点1获取分区
	acquiredPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-1", acquiredPartition.WorkerID)

	// 等待心跳超时
	time.Sleep(150 * time.Millisecond)

	// 工作节点2尝试抢占（不允许抢占）
	options := &model.AcquirePartitionOptions{AllowPreemption: false}
	_, success, err = strategy.AcquirePartition(ctx, 1, "worker-2", nil)
	assert.NoError(t, err)
	assert.False(t, success)

	// 工作节点2尝试抢占（允许抢占）
	options = &model.AcquirePartitionOptions{AllowPreemption: true}
	preemptedPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-2", options)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-2", preemptedPartition.WorkerID)
	assert.Equal(t, model.StatusClaimed, preemptedPartition.Status)

	// 验证分区确实被抢占
	currentPartition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, "worker-2", currentPartition.WorkerID)
}

// TestHashPartitionStrategy_StatusUpdates 测试状态更新
func TestHashPartitionStrategy_StatusUpdates(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建并获取分区
	_, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	_, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	require.NoError(t, err)
	require.True(t, success)

	// 测试状态更新
	metadata := map[string]interface{}{
		"progress": 50,
		"message":  "Processing data",
	}

	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusRunning, metadata)
	assert.NoError(t, err)

	// 验证状态更新
	partition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, partition.Status)
	assert.Equal(t, "worker-1", partition.WorkerID)

	// 测试无权限的状态更新
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-2", model.StatusCompleted, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "无权更新")

	// 验证状态没有改变
	partition, err = strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusRunning, partition.Status)
	assert.Equal(t, "worker-1", partition.WorkerID)
}

// TestHashPartitionStrategy_ForcePreemption 测试强制抢占
func TestHashPartitionStrategy_ForcePreemption(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 创建分区
	_, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	require.NoError(t, err)

	// 工作节点1获取分区
	_, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	require.NoError(t, err)
	require.True(t, success)

	// 工作节点2尝试强制抢占
	options := &model.AcquirePartitionOptions{
		AllowPreemption: true,
		ForcePreemption: true,
	}
	preemptedPartition, success, err := strategy.AcquirePartition(ctx, 1, "worker-2", options)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, "worker-2", preemptedPartition.WorkerID)

	// 验证分区确实被强制抢占
	currentPartition, err := strategy.GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, "worker-2", currentPartition.WorkerID)
	assert.Equal(t, model.StatusClaimed, currentPartition.Status)
}

// TestHashPartitionStrategy_AtomicOperations 测试原子操作
func TestHashPartitionStrategy_AtomicOperations(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试原子创建
	partition1, err := strategy.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, partition1.PartitionID)
	assert.Equal(t, int64(1), partition1.Version)

	// 测试重复原子创建失败
	_, err = strategy.CreatePartitionAtomically(ctx, 1, 1001, 2000, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrPartitionAlreadyExists)

	// 测试并发原子创建
	const numWorkers = 5
	var wg sync.WaitGroup
	results := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(partitionID int) {
			defer wg.Done()
			_, err := strategy.CreatePartitionAtomically(ctx, partitionID+10, int64(partitionID*1000), int64((partitionID+1)*1000), nil)
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// 验证所有创建都成功
	errorCount := 0
	for err := range results {
		if err != nil {
			errorCount++
		}
	}
	assert.Equal(t, 0, errorCount, "所有原子创建应该都成功")

	// 验证创建的分区数量
	allPartitions, err := strategy.GetAllActivePartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, numWorkers+1) // +1 for the first partition
}

// TestHashPartitionStrategy_ComplexWorkflow 测试复杂工作流程
func TestHashPartitionStrategy_ComplexWorkflow(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 步骤1: 创建多个分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
		},
	}
	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)
	assert.Len(t, createdPartitions, 3)

	// 步骤2: 多个工作节点获取分区
	_, success, err := strategy.AcquirePartition(ctx, 1, "worker-1", nil)
	require.NoError(t, err)
	require.True(t, success)

	_, success, err = strategy.AcquirePartition(ctx, 2, "worker-2", nil)
	require.NoError(t, err)
	require.True(t, success)

	// 步骤3: 更新分区状态到运行中
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusRunning, map[string]interface{}{
		"started_at": time.Now().Unix(),
		"progress":   0,
	})
	require.NoError(t, err)

	err = strategy.UpdatePartitionStatus(ctx, 2, "worker-2", model.StatusRunning, map[string]interface{}{
		"started_at": time.Now().Unix(),
		"progress":   0,
	})
	require.NoError(t, err)

	// 步骤4: 模拟处理进度更新
	for progress := 25; progress <= 100; progress += 25 {
		err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusRunning, map[string]interface{}{
			"progress": progress,
		})
		require.NoError(t, err)
	}

	// 步骤5: 完成第一个分区
	err = strategy.UpdatePartitionStatus(ctx, 1, "worker-1", model.StatusCompleted, map[string]interface{}{
		"completed_at": time.Now().Unix(),
		"records":      1000,
	})
	require.NoError(t, err)

	// 步骤6: 释放已完成的分区
	err = strategy.ReleasePartition(ctx, 1, "worker-1")
	require.NoError(t, err)

	// 步骤7: worker-1获取第3个分区
	_, success, err = strategy.AcquirePartition(ctx, 3, "worker-1", nil)
	require.NoError(t, err)
	require.True(t, success)

	// 步骤8: 验证最终状态
	allPartitions, err := strategy.GetAllPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, allPartitions, 3)

	statusMap := make(map[int]model.PartitionStatus)
	workerMap := make(map[int]string)
	for _, partition := range allPartitions {
		statusMap[partition.PartitionID] = partition.Status
		workerMap[partition.PartitionID] = partition.WorkerID
	}

	// 验证状态
	assert.Equal(t, model.StatusCompleted, statusMap[1])
	assert.Equal(t, model.StatusRunning, statusMap[2])
	assert.Equal(t, model.StatusClaimed, statusMap[3])

	// 验证工作节点分配
	// 注意：已完成的分区保留 WorkerID 以记录处理历史
	assert.Equal(t, "worker-1", workerMap[1]) // 已完成，保留 WorkerID
	assert.Equal(t, "worker-2", workerMap[2])
	assert.Equal(t, "worker-1", workerMap[3])

	// 步骤9: 获取统计信息
	stats, err := strategy.GetPartitionStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 1, stats.Completed)
	assert.Equal(t, 1, stats.Running)
	assert.Equal(t, 1, stats.Claimed)
	assert.Equal(t, 0, stats.Pending)
	assert.Equal(t, 0, stats.Failed)
}
