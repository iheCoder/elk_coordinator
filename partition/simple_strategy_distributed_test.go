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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// setupDistributedSimulation 创建多个独立的SimpleStrategy实例来模拟分布式环境
func setupDistributedSimulation(t *testing.T, workerCount int) ([]*SimpleStrategy, *miniredis.Miniredis, func()) {
	// 创建共享的 miniredis 实例
	mr, err := miniredis.Run()
	require.NoError(t, err, "启动 miniredis 失败")

	var strategies []*SimpleStrategy
	var clients []*redis.Client

	for i := 0; i < workerCount; i++ {
		// 每个worker都有自己的Redis客户端和SimpleStrategy实例
		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		// 测试连接
		err = client.Ping(context.Background()).Err()
		require.NoError(t, err, "连接 Redis 失败")

		// 创建独立的 RedisDataStore
		opts := &data.Options{
			KeyPrefix:     "test:simple:",
			DefaultExpiry: 5 * time.Second,
			MaxRetries:    3,
			RetryDelay:    10 * time.Millisecond,
			MaxRetryDelay: 50 * time.Millisecond,
		}
		dataStore := data.NewRedisDataStore(client, opts)

		// 创建独立的 SimpleStrategy（模拟不同进程中的实例）
		config := SimpleStrategyConfig{
			Namespace:           "test",
			DataStore:           dataStore,
			Logger:              test_utils.NewMockLogger(true),
			PartitionLockExpiry: 30 * time.Second,
		}
		strategy := NewSimpleStrategy(config)

		strategies = append(strategies, strategy)
		clients = append(clients, client)
	}

	// 清理函数
	cleanup := func() {
		for _, client := range clients {
			client.Close()
		}
		mr.Close()
	}

	return strategies, mr, cleanup
}

// TestSimpleStrategy_DistributedConcurrentUpdates 测试分布式并发更新
// 这个测试模拟了真实分布式环境中的并发问题
func TestSimpleStrategy_DistributedConcurrentUpdates(t *testing.T) {
	const workerCount = 5
	strategies, _, cleanup := setupDistributedSimulation(t, workerCount)
	defer cleanup()

	ctx := context.Background()

	// 使用第一个策略创建初始分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}
	_, err := strategies[0].CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 获取初始分区信息
	initialPartition, err := strategies[0].GetPartition(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), initialPartition.Version)

	// 并发更新：每个worker都尝试更新同一个分区
	const numWorkers = workerCount
	var wg sync.WaitGroup
	results := make(chan error, numWorkers)
	successCount := new(int32) // 用于计数成功更新的worker数量

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			// 获取分区信息（每个worker从自己的策略实例获取）
			partition, err := strategies[workerIndex].GetPartition(ctx, 1)
			if err != nil {
				results <- fmt.Errorf("worker %d 获取分区失败: %v", workerIndex, err)
				return
			}

			// 模拟处理时间
			time.Sleep(10 * time.Millisecond)

			// 更新分区
			partition.WorkerID = fmt.Sprintf("worker-%d", workerIndex)
			partition.Status = model.StatusRunning

			_, err = strategies[workerIndex].UpdatePartition(ctx, partition, nil)
			if err != nil {
				// 在分布式环境中，并发更新失败是正常的
				results <- fmt.Errorf("worker %d 更新分区失败: %v", workerIndex, err)
				return
			}

			// 更新成功计数器
			atomic.AddInt32(successCount, 1)
			results <- nil // 成功
		}(i)
	}

	wg.Wait()
	close(results)

	// 分析结果
	var updateErrors []error
	var successfulUpdates int
	for result := range results {
		if result != nil {
			updateErrors = append(updateErrors, result)
		} else {
			successfulUpdates++
		}
	}

	// 打印所有错误（如果有的话）
	for _, err := range updateErrors {
		t.Logf("并发更新错误: %v", err)
	}

	// 检查最终状态
	finalPartition, err := strategies[0].GetPartition(ctx, 1)
	assert.NoError(t, err)

	t.Logf("最终分区状态: PartitionID=%d, WorkerID=%s, Status=%s, Version=%d",
		finalPartition.PartitionID, finalPartition.WorkerID, finalPartition.Status, finalPartition.Version)

	// 记录成功的更新数量
	t.Logf("成功更新的worker数量: %d", successfulUpdates)

	// 在分布式锁的保护下，我们应该期望：
	// 1. 至少有一个worker成功更新
	// 2. 最终版本应该等于初始版本 + 成功更新次数
	assert.Greater(t, successfulUpdates, 0, "应该至少有一个worker成功更新")

	expectedVersion := int64(1 + successfulUpdates) // 初始版本 + 成功更新次数
	assert.Equal(t, expectedVersion, finalPartition.Version,
		"分区版本应该准确反映成功更新的次数")
}

// TestSimpleStrategy_DistributedLockContention 测试分布式锁竞争
func TestSimpleStrategy_DistributedLockContention(t *testing.T) {
	const workerCount = 10
	strategies, _, cleanup := setupDistributedSimulation(t, workerCount)
	defer cleanup()

	ctx := context.Background()

	// 创建初始分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}
	_, err := strategies[0].CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 并发获取同一个分区
	var wg sync.WaitGroup
	successfulAcquisitions := make(chan string, workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			workerID := fmt.Sprintf("worker-%d", workerIndex)
			options := &model.AcquirePartitionOptions{
				AllowPreemption: false,
			}

			partition, success, err := strategies[workerIndex].AcquirePartition(ctx, 1, workerID, options)
			if err != nil {
				t.Logf("Worker %s 获取分区时出错: %v", workerID, err)
				return
			}

			if success && partition != nil {
				successfulAcquisitions <- workerID
				t.Logf("Worker %s 成功获取分区", workerID)
			} else {
				t.Logf("Worker %s 未能获取分区", workerID)
			}
		}(i)
	}

	wg.Wait()
	close(successfulAcquisitions)

	// 统计成功获取分区的worker数量
	var successfulWorkers []string
	for worker := range successfulAcquisitions {
		successfulWorkers = append(successfulWorkers, worker)
	}

	// 在正确的分布式锁实现下，应该只有一个worker能成功获取分区
	if len(successfulWorkers) != 1 {
		t.Errorf("期望只有1个worker成功获取分区，但实际有 %d 个: %v",
			len(successfulWorkers), successfulWorkers)
	}

	// 验证最终分区状态
	finalPartition, err := strategies[0].GetPartition(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, model.StatusClaimed, finalPartition.Status)
	assert.NotEmpty(t, finalPartition.WorkerID)
	t.Logf("最终分区持有者: %s", finalPartition.WorkerID)
}

// TestSimpleStrategy_DistributedDataConsistency 测试分布式数据一致性
func TestSimpleStrategy_DistributedDataConsistency(t *testing.T) {
	const workerCount = 3
	strategies, _, cleanup := setupDistributedSimulation(t, workerCount)
	defer cleanup()

	ctx := context.Background()

	// 并发创建相同的分区
	var wg sync.WaitGroup
	createErrors := make(chan error, workerCount)

	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
		},
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			_, err := strategies[workerIndex].CreatePartitionsIfNotExist(ctx, request)
			createErrors <- err
		}(i)
	}

	wg.Wait()
	close(createErrors)

	// 检查创建操作的结果
	var errors []error
	for err := range createErrors {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// 应该都能成功（因为是"IfNotExist"操作）
	assert.Empty(t, errors, "并发创建操作不应该产生错误")

	// 验证所有策略实例看到的数据是一致的
	for i, strategy := range strategies {
		partitions, err := strategy.GetAllPartitions(ctx)
		assert.NoError(t, err, "策略实例 %d 获取分区失败", i)
		assert.Len(t, partitions, 2, "策略实例 %d 看到的分区数量不正确", i)

		// 验证分区详情
		for _, partition := range partitions {
			assert.Equal(t, int64(1), partition.Version, "分区版本应该为1")
		}
	}
}

// TestSimpleStrategy_SimulateRealWorldScenario 模拟真实世界场景
func TestSimpleStrategy_SimulateRealWorldScenario(t *testing.T) {
	const workerCount = 5
	strategies, _, cleanup := setupDistributedSimulation(t, workerCount)
	defer cleanup()

	ctx := context.Background()

	// 场景：多个worker同时工作，进行各种操作
	// 1. Leader创建分区
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
		},
	}
	_, err := strategies[0].CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)

	// 2. 多个worker同时尝试获取和处理分区
	var wg sync.WaitGroup
	operationResults := make(chan string, workerCount*10)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			workerID := fmt.Sprintf("worker-%d", workerIndex)

			// 尝试获取分区
			for partitionID := 1; partitionID <= 3; partitionID++ {
				options := &model.AcquirePartitionOptions{AllowPreemption: false}
				partition, success, err := strategies[workerIndex].AcquirePartition(ctx, partitionID, workerID, options)

				if err != nil {
					operationResults <- fmt.Sprintf("%s: 获取分区%d失败: %v", workerID, partitionID, err)
					continue
				}

				if success && partition != nil {
					operationResults <- fmt.Sprintf("%s: 成功获取分区%d", workerID, partitionID)

					// 模拟处理时间
					time.Sleep(50 * time.Millisecond)

					// 更新状态为运行中
					err = strategies[workerIndex].UpdatePartitionStatus(ctx, partitionID, workerID, model.StatusRunning, nil)
					if err != nil {
						operationResults <- fmt.Sprintf("%s: 更新分区%d状态失败: %v", workerID, partitionID, err)
					} else {
						operationResults <- fmt.Sprintf("%s: 成功更新分区%d状态为运行中", workerID, partitionID)
					}

					// 维护心跳
					err = strategies[workerIndex].MaintainPartitionHold(ctx, partitionID, workerID)
					if err != nil {
						operationResults <- fmt.Sprintf("%s: 维护分区%d心跳失败: %v", workerID, partitionID, err)
					}

					break // 获取到一个分区就退出循环
				}
			}
		}(i)
	}

	wg.Wait()
	close(operationResults)

	// 收集并显示操作结果
	for result := range operationResults {
		t.Log(result)
	}

	// 验证最终状态
	allPartitions, err := strategies[0].GetAllPartitions(ctx)
	assert.NoError(t, err)
	assert.Len(t, allPartitions, 3)

	// 统计各种状态的分区
	statusCount := make(map[model.PartitionStatus]int)
	for _, partition := range allPartitions {
		statusCount[partition.Status]++
		t.Logf("分区 %d: 状态=%s, 持有者=%s, 版本=%d",
			partition.PartitionID, partition.Status, partition.WorkerID, partition.Version)
	}

	t.Logf("分区状态统计: %v", statusCount)
}
