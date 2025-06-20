package partition

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
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
// 修改后的测试：测试正确的工作流程 - 先 AcquirePartition 然后 UpdatePartition
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

	// 并发获取：每个worker都尝试获取同一个分区
	const numWorkers = workerCount
	var wg sync.WaitGroup
	results := make(chan struct {
		workerID string
		acquired bool
		err      error
	}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			workerID := fmt.Sprintf("worker-%d", workerIndex)

			// 模拟一些随机延迟
			time.Sleep(time.Duration(workerIndex) * time.Millisecond)

			// 尝试获取分区
			partition, acquired, err := strategies[workerIndex].AcquirePartition(ctx, 1, workerID, nil)
			if err != nil {
				results <- struct {
					workerID string
					acquired bool
					err      error
				}{workerID, false, err}
				return
			}

			if !acquired {
				results <- struct {
					workerID string
					acquired bool
					err      error
				}{workerID, false, nil}
				return
			}

			// 获取成功，更新状态
			err = strategies[workerIndex].UpdatePartitionStatus(ctx, 1, workerID, model.StatusRunning, nil)
			if err != nil {
				t.Logf("Worker %s 更新状态失败: %v", workerID, err)
			}

			results <- struct {
				workerID string
				acquired bool
				err      error
			}{workerID, true, nil}

			t.Logf("Worker %s 成功获取并更新分区 %d", workerID, partition.PartitionID)
		}(i)
	}

	wg.Wait()
	close(results)

	// 分析结果
	var acquireErrors []error
	var successfulAcquisitions []string
	for result := range results {
		if result.err != nil {
			acquireErrors = append(acquireErrors, result.err)
		} else if result.acquired {
			successfulAcquisitions = append(successfulAcquisitions, result.workerID)
		}
	}

	// 打印所有错误（如果有的话）
	for _, err := range acquireErrors {
		t.Logf("并发获取错误: %v", err)
	}

	// 检查最终状态
	finalPartition, err := strategies[0].GetPartition(ctx, 1)
	assert.NoError(t, err)

	t.Logf("最终分区状态: PartitionID=%d, WorkerID=%s, Status=%s, Version=%d",
		finalPartition.PartitionID, finalPartition.WorkerID, finalPartition.Status, finalPartition.Version)

	t.Logf("成功获取分区的worker: %v", successfulAcquisitions)

	// 在分布式锁的保护下，我们应该期望：
	// 1. 有且仅有一个worker成功获取分区
	// 2. 不应该有系统错误
	assert.Len(t, acquireErrors, 0, "不应该有系统错误")
	assert.Len(t, successfulAcquisitions, 1, "应该有且仅有一个worker成功获取分区")

	// 最终分区应该被某个worker持有
	assert.NotEmpty(t, finalPartition.WorkerID, "分区应该被某个worker持有")
	assert.Contains(t, successfulAcquisitions, finalPartition.WorkerID, "最终持有者应该在成功获取列表中")
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
