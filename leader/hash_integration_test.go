package leader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/partition"
	"github.com/iheCoder/elk_coordinator/test_utils"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPartitionPlaner 用于测试的分区规划器
type testPartitionPlaner struct {
	partitionSize int64
	nextMaxID     int64
	mu            sync.Mutex
}

func (p *testPartitionPlaner) PartitionSize(ctx context.Context) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.partitionSize <= 0 {
		return model.DefaultPartitionSize, nil
	}
	return p.partitionSize, nil
}

func (p *testPartitionPlaner) GetNextMaxID(ctx context.Context, lastID int64, rangeSize int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 模拟数据库中有新数据
	if p.nextMaxID <= 0 {
		p.nextMaxID = lastID + rangeSize
	}

	// 模拟增长的数据
	result := p.nextMaxID
	p.nextMaxID += rangeSize / 2 // 下次调用时会有更多数据
	return result, nil
}

func (p *testPartitionPlaner) setPartitionSize(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitionSize = size
}

func (p *testPartitionPlaner) setNextMaxID(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextMaxID = id
}

// setupLeaderIntegrationTest 创建leader集成测试环境
func setupLeaderIntegrationTest(t *testing.T, nodeID string) (*LeaderManager, *miniredis.Miniredis, *data.RedisDataStore, *testPartitionPlaner, func()) {
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
		KeyPrefix:     "test:leader:",
		DefaultExpiry: 30 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	// 创建 hash分区策略
	logger := test_utils.NewMockLogger(true)
	strategy := partition.NewHashPartitionStrategy(dataStore, logger)

	// 创建测试分区规划器
	planer := &testPartitionPlaner{
		partitionSize: 1000,
		nextMaxID:     5000,
	}

	// 创建 LeaderManager
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  nodeID,
		Namespace:               "test",
		DataStore:               dataStore,
		Logger:                  logger,
		Planer:                  planer,
		Strategy:                strategy,
		ElectionInterval:        1 * time.Second,
		LockExpiry:              5 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  10 * time.Second,
	})

	// 清理函数
	cleanup := func() {
		lm.Stop()
		client.Close()
		mr.Close()
	}

	return lm, mr, dataStore, planer, cleanup
}

// TestLeaderManager_BasicFunctionality 测试LeaderManager基本功能
func TestLeaderManager_BasicFunctionality(t *testing.T) {
	lm, _, _, _, cleanup := setupLeaderIntegrationTest(t, "node1")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 初始状态不是leader
	assert.False(t, lm.IsLeader(), "初始状态不应该是leader")

	// 启动leader管理器（在goroutine中）
	done := make(chan error, 1)
	go func() {
		done <- lm.Start(ctx)
	}()

	// 等待一段时间让选举完成
	time.Sleep(500 * time.Millisecond)

	// 验证已成为leader
	assert.True(t, lm.IsLeader(), "应该成为leader")

	// 停止leader管理器
	lm.Stop()

	// 等待goroutine完成，增加超时时间
	select {
	case err := <-done:
		// 期望收到context超时错误或nil
		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("意外的错误: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("等待leader停止超时")
	}
}

// TestLeaderManager_ConcurrentElection 测试多节点并发选举
func TestLeaderManager_ConcurrentElection(t *testing.T) {
	// 创建共享的 miniredis 实例
	mr, err := miniredis.Run()
	require.NoError(t, err, "启动 miniredis 失败")
	defer mr.Close()

	nodeCount := 5
	leaders := make([]*LeaderManager, nodeCount)
	cleanups := make([]func(), nodeCount)

	// 创建多个leader实例
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)

		// 创建 Redis 客户端
		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		// 创建 RedisDataStore
		opts := &data.Options{
			KeyPrefix:     "test:leader:",
			DefaultExpiry: 30 * time.Second,
			MaxRetries:    3,
			RetryDelay:    10 * time.Millisecond,
			MaxRetryDelay: 50 * time.Millisecond,
		}
		dataStore := data.NewRedisDataStore(client, opts)

		// 创建策略和规划器
		logger := test_utils.NewMockLogger(false)
		strategy := partition.NewHashPartitionStrategy(dataStore, logger)
		planer := &testPartitionPlaner{partitionSize: 1000, nextMaxID: 5000}

		// 创建 LeaderManager
		leaders[i] = NewLeaderManager(LeaderConfig{
			NodeID:                  nodeID,
			Namespace:               "test",
			DataStore:               dataStore,
			Logger:                  logger,
			Planer:                  planer,
			Strategy:                strategy,
			ElectionInterval:        500 * time.Millisecond,
			LockExpiry:              3 * time.Second,
			WorkerPartitionMultiple: 2,
			ValidHeartbeatDuration:  10 * time.Second,
		})

		cleanups[i] = func(c *redis.Client, lm *LeaderManager) func() {
			return func() {
				lm.Stop()
				c.Close()
			}
		}(client, leaders[i])
	}

	// 清理所有资源
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 并发启动所有leader
	var wg sync.WaitGroup
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			leaders[idx].Start(ctx)
		}(i)
	}

	// 等待选举完成
	time.Sleep(1 * time.Second)

	// 统计成为leader的节点数量
	leaderCount := 0
	var currentLeader *LeaderManager
	for i := 0; i < nodeCount; i++ {
		if leaders[i].IsLeader() {
			leaderCount++
			currentLeader = leaders[i]
		}
	}

	// 验证只有一个leader
	assert.Equal(t, 1, leaderCount, "应该只有一个节点成为leader")
	assert.NotNil(t, currentLeader, "应该有一个leader")

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
	}()
}

// TestLeaderManager_PartitionAllocation 测试分区分配功能
func TestLeaderManager_PartitionAllocation(t *testing.T) {
	lm, _, dataStore, planer, cleanup := setupLeaderIntegrationTest(t, "leader-node")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 32*time.Second)
	defer cancel()

	// 模拟工作节点心跳
	workerNodes := []string{"worker1", "worker2", "worker3"}
	for _, nodeID := range workerNodes {
		err := dataStore.RegisterWorker(ctx, nodeID)
		require.NoError(t, err)
	}

	// 设置分区规划器参数
	planer.setPartitionSize(500)
	planer.setNextMaxID(3000)

	// 启动leader管理器
	done := make(chan error, 1)
	go func() {
		done <- lm.Start(ctx)
	}()

	// 等待一段时间让分区分配完成
	time.Sleep(3 * time.Second)

	// 验证已成为leader
	assert.True(t, lm.IsLeader(), "应该成为leader")

	// 验证分区已创建
	strategy := partition.NewHashPartitionStrategy(dataStore, test_utils.NewMockLogger(false))
	allPartitions, err := strategy.GetAllPartitions(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(allPartitions), 0, "应该创建了分区")

	// 验证分区统计
	stats, err := strategy.GetPartitionStats(ctx)
	assert.NoError(t, err)
	assert.Greater(t, stats.Total, 0, "分区总数应该大于0")

	// 停止并等待完成
	lm.Stop()

	// 等待goroutine完成，增加超时时间
	select {
	case err := <-done:
		// 期望收到context超时错误或nil
		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("意外的错误: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("等待leader停止超时")
	}
}

// TestLeaderManager_LeaderFailover 测试leader故障转移
func TestLeaderManager_LeaderFailover(t *testing.T) {
	// 创建共享的 miniredis 实例
	mr, err := miniredis.Run()
	require.NoError(t, err, "启动 miniredis 失败")
	defer mr.Close()

	// 创建两个leader实例
	var leaders []*LeaderManager
	var cleanups []func()

	for i := 0; i < 2; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)

		// 创建 Redis 客户端
		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		opts := &data.Options{
			KeyPrefix:     "test:leader:",
			DefaultExpiry: 30 * time.Second,
			MaxRetries:    3,
			RetryDelay:    10 * time.Millisecond,
			MaxRetryDelay: 50 * time.Millisecond,
		}
		dataStore := data.NewRedisDataStore(client, opts)

		logger := test_utils.NewMockLogger(false)
		strategy := partition.NewHashPartitionStrategy(dataStore, logger)
		planer := &testPartitionPlaner{partitionSize: 1000, nextMaxID: 5000}

		lm := NewLeaderManager(LeaderConfig{
			NodeID:                  nodeID,
			Namespace:               "test",
			DataStore:               dataStore,
			Logger:                  logger,
			Planer:                  planer,
			Strategy:                strategy,
			ElectionInterval:        200 * time.Millisecond,
			LockExpiry:              1 * time.Second, // 短过期时间用于快速故障转移
			WorkerPartitionMultiple: 2,
			ValidHeartbeatDuration:  10 * time.Second,
		})

		leaders = append(leaders, lm)
		cleanups = append(cleanups, func(c *redis.Client, lm *LeaderManager) func() {
			return func() {
				lm.Stop()
				c.Close()
			}
		}(client, lm))
	}

	// 清理所有资源
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 启动第一个leader
	done1 := make(chan error, 1)
	go func() {
		done1 <- leaders[0].Start(ctx)
	}()

	// 等待第一个节点成为leader
	time.Sleep(500 * time.Millisecond)
	assert.True(t, leaders[0].IsLeader(), "node1应该成为leader")
	assert.False(t, leaders[1].IsLeader(), "node2不应该是leader")

	// 启动第二个leader（应该作为follower）
	done2 := make(chan error, 1)
	go func() {
		done2 <- leaders[1].Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	assert.True(t, leaders[0].IsLeader(), "node1应该仍然是leader")
	assert.False(t, leaders[1].IsLeader(), "node2应该仍然不是leader")

	// 模拟第一个leader故障（停止）
	leaders[0].Stop()

	// 等待故障转移
	time.Sleep(2 * time.Second)

	// 验证第二个节点成为新leader
	assert.False(t, leaders[0].IsLeader(), "node1应该不再是leader")
	assert.True(t, leaders[1].IsLeader(), "node2应该成为新leader")

	// 停止第二个leader
	leaders[1].Stop()

	// 等待完成
	select {
	case <-done1:
	case <-time.After(1 * time.Second):
	}
	select {
	case <-done2:
	case <-time.After(1 * time.Second):
	}
}

// TestLeaderManager_ConcurrentWorkerHeartbeats 测试并发工作节点心跳管理
func TestLeaderManager_ConcurrentWorkerHeartbeats(t *testing.T) {
	lm, _, dataStore, planer, cleanup := setupLeaderIntegrationTest(t, "leader-node")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 设置分区规划器
	planer.setPartitionSize(200)
	planer.setNextMaxID(2000)

	// 并发模拟多个工作节点发送心跳
	workerCount := 10
	var wg sync.WaitGroup
	stopHeartbeat := make(chan struct{})
	workersReady := make(chan struct{})
	var readyWorkers int64

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("worker%d", workerID)

			// 注册工作节点（新API会自动设置心跳）
			err := dataStore.RegisterWorker(ctx, nodeID)
			if err != nil {
				t.Logf("Worker %s 注册失败: %v", nodeID, err)
				return
			}

			// 标记工作节点准备就绪
			if atomic.AddInt64(&readyWorkers, 1) == int64(workerCount) {
				close(workersReady)
			}

			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-stopHeartbeat:
					return
				case <-ticker.C:
					// 使用RefreshWorkerHeartbeat刷新心跳（更高效）
					err := dataStore.RefreshWorkerHeartbeat(ctx, nodeID)
					if err != nil {
						t.Logf("Worker %s 心跳失败: %v", nodeID, err)
					}
				}
			}
		}(i)
	}

	// 等待所有工作节点准备就绪
	select {
	case <-workersReady:
		t.Log("所有工作节点已准备就绪")
	case <-time.After(3 * time.Second):
		t.Fatal("等待工作节点准备就绪超时")
	}

	// 启动leader管理器
	done := make(chan error, 1)
	go func() {
		done <- lm.Start(ctx)
	}()

	// 等待leader启动和分区分配，需要等待足够时间让leader检测到工作节点
	time.Sleep(5 * time.Second)

	// 验证leader状态
	assert.True(t, lm.IsLeader(), "应该成为leader")

	// 验证分区已创建 - 使用轮询方式等待分区创建
	strategy := partition.NewHashPartitionStrategy(dataStore, test_utils.NewMockLogger(false))
	var allPartitions []*model.PartitionInfo
	var err error
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		allPartitions, err = strategy.GetAllPartitions(ctx)
		if err == nil && len(allPartitions) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	assert.NoError(t, err)
	assert.Greater(t, len(allPartitions), 0, "应该创建了分区")

	// 停止心跳
	close(stopHeartbeat)
	wg.Wait()

	// 停止leader
	lm.Stop()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Leader停止时发生错误: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("等待leader停止超时")
	}
}

// TestLeaderManager_PartitionCreationWithDifferentStrategies 测试不同分区策略下的分区创建
func TestLeaderManager_PartitionCreationWithDifferentStrategies(t *testing.T) {
	testCases := []struct {
		name          string
		partitionSize int64
		nextMaxID     int64
		workerCount   int
		expectedMin   int // 期望的最小分区数
	}{
		{
			name:          "小分区大数据量",
			partitionSize: 100,
			nextMaxID:     1000,
			workerCount:   2,
			expectedMin:   5,
		},
		{
			name:          "大分区小数据量",
			partitionSize: 2000,
			nextMaxID:     1500,
			workerCount:   3,
			expectedMin:   1,
		},
		{
			name:          "中等分区中等数据量",
			partitionSize: 500,
			nextMaxID:     2500,
			workerCount:   4,
			expectedMin:   2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lm, _, dataStore, planer, cleanup := setupLeaderIntegrationTest(t, "leader-node")
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			// 设置测试参数
			planer.setPartitionSize(tc.partitionSize)
			planer.setNextMaxID(tc.nextMaxID)

			// 模拟工作节点心跳和注册
			for i := 0; i < tc.workerCount; i++ {
				nodeID := fmt.Sprintf("worker%d", i)
				// 注册工作节点（新API会自动设置心跳）
				err := dataStore.RegisterWorker(ctx, nodeID)
				require.NoError(t, err, "注册工作节点失败")
			}

			// 启动leader
			done := make(chan error, 1)
			go func() {
				done <- lm.Start(ctx)
			}()

			// 等待分区创建
			time.Sleep(3 * time.Second)

			// 验证分区创建 - 使用轮询方式等待分区创建
			strategy := partition.NewHashPartitionStrategy(dataStore, test_utils.NewMockLogger(false))
			var allPartitions []*model.PartitionInfo
			var err error
			maxRetries := 3
			for i := 0; i < maxRetries; i++ {
				allPartitions, err = strategy.GetAllPartitions(ctx)
				if err == nil && len(allPartitions) >= tc.expectedMin {
					break
				}
				time.Sleep(1 * time.Second)
			}

			assert.NoError(t, err)
			assert.GreaterOrEqual(t, len(allPartitions), tc.expectedMin,
				"分区数量应该满足最小期望: 分区大小=%d, 数据量=%d, 工作节点=%d",
				tc.partitionSize, tc.nextMaxID, tc.workerCount)

			// 验证分区ID范围合理性
			for _, p := range allPartitions {
				assert.Greater(t, p.MaxID, p.MinID, "分区MaxID应该大于MinID")
				assert.LessOrEqual(t, p.MaxID-p.MinID, tc.partitionSize,
					"分区大小不应超过配置的分区大小")
			}

			// 停止leader
			lm.Stop()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Leader停止时发生错误: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("等待leader停止超时")
			}
		})
	}
}

// TestLeaderManager_HighConcurrencyElection 测试高并发选举场景
func TestLeaderManager_HighConcurrencyElection(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过高并发测试（短模式）")
	}

	// 创建共享的 miniredis 实例
	mr, err := miniredis.Run()
	require.NoError(t, err, "启动 miniredis 失败")
	defer mr.Close()

	nodeCount := 20
	leaders := make([]*LeaderManager, nodeCount)
	cleanups := make([]func(), nodeCount)
	var leaderCounts int64

	// 创建多个leader实例
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)

		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		opts := &data.Options{
			KeyPrefix:     "test:leader:",
			DefaultExpiry: 30 * time.Second,
			MaxRetries:    3,
			RetryDelay:    10 * time.Millisecond,
			MaxRetryDelay: 50 * time.Millisecond,
		}
		dataStore := data.NewRedisDataStore(client, opts)

		logger := test_utils.NewMockLogger(false)
		strategy := partition.NewHashPartitionStrategy(dataStore, logger)
		planer := &testPartitionPlaner{partitionSize: 1000, nextMaxID: 5000}

		leaders[i] = NewLeaderManager(LeaderConfig{
			NodeID:                  nodeID,
			Namespace:               "test",
			DataStore:               dataStore,
			Logger:                  logger,
			Planer:                  planer,
			Strategy:                strategy,
			ElectionInterval:        100 * time.Millisecond,
			LockExpiry:              2 * time.Second,
			WorkerPartitionMultiple: 2,
			ValidHeartbeatDuration:  10 * time.Second,
		})

		cleanups[i] = func(c *redis.Client, lm *LeaderManager) func() {
			return func() {
				lm.Stop()
				c.Close()
			}
		}(client, leaders[i])
	}

	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 并发启动所有leader
	var wg sync.WaitGroup
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			leaders[idx].Start(ctx)
		}(i)
	}

	// 等待选举稳定
	time.Sleep(2 * time.Second)

	// 统计leader数量
	for i := 0; i < nodeCount; i++ {
		if leaders[i].IsLeader() {
			atomic.AddInt64(&leaderCounts, 1)
		}
	}

	// 验证只有一个leader
	assert.Equal(t, int64(1), atomic.LoadInt64(&leaderCounts),
		"在高并发场景下，应该只有一个节点成为leader")

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
	}()
}

// TestLeaderManager_CommandProcessing 测试Leader的命令处理功能
func TestLeaderManager_CommandProcessing(t *testing.T) {
	lm, _, dataStore, planer, cleanup := setupLeaderIntegrationTest(t, "leader-node")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 设置分区规划器
	planer.setPartitionSize(500)
	planer.setNextMaxID(2000)

	// 模拟工作节点心跳
	workerNodes := []string{"worker1", "worker2"}
	for _, nodeID := range workerNodes {
		err := dataStore.RegisterWorker(ctx, nodeID)
		require.NoError(t, err)
	}

	// 启动leader管理器
	done := make(chan error, 1)
	go func() {
		done <- lm.Start(ctx)
	}()
	// 等待leader启动并创建一些分区
	time.Sleep(2 * time.Second)
	assert.True(t, lm.IsLeader(), "应该成为leader")

	// 获取当前已创建的分区，然后在此基础上创建测试分区
	strategy := partition.NewHashPartitionStrategy(dataStore, test_utils.NewMockLogger(false))
	existingPartitions, err := strategy.GetAllPartitions(ctx)
	require.NoError(t, err)

	// 计算下一个可用的分区ID
	maxExistingID := 0
	for _, p := range existingPartitions {
		if p.PartitionID > maxExistingID {
			maxExistingID = p.PartitionID
		}
	}

	nextPartitionID := maxExistingID + 1

	// 创建一些测试用的失败分区（使用不会冲突的ID和范围）
	baseID := int64(10000) // 使用一个远离自动创建分区的范围

	failedPartition1 := &model.PartitionInfo{
		PartitionID: nextPartitionID,
		MinID:       baseID,
		MaxID:       baseID + 500,
		Status:      model.StatusFailed,
		WorkerID:    "worker1",
		Error:       "模拟处理失败",
	}
	_, err = strategy.CreatePartitionAtomically(ctx, failedPartition1.PartitionID, failedPartition1.MinID, failedPartition1.MaxID, map[string]interface{}{
		"status":    string(failedPartition1.Status),
		"worker_id": failedPartition1.WorkerID,
		"error":     failedPartition1.Error,
	})
	require.NoError(t, err)

	failedPartition2 := &model.PartitionInfo{
		PartitionID: nextPartitionID + 1,
		MinID:       baseID + 501,
		MaxID:       baseID + 1000,
		Status:      model.StatusFailed,
		WorkerID:    "worker2",
		Error:       "网络连接错误",
	}
	_, err = strategy.CreatePartitionAtomically(ctx, failedPartition2.PartitionID, failedPartition2.MinID, failedPartition2.MaxID, map[string]interface{}{
		"status":    string(failedPartition2.Status),
		"worker_id": failedPartition2.WorkerID,
		"error":     failedPartition2.Error,
	})
	require.NoError(t, err)

	// 等待一下确保分区创建完成
	time.Sleep(500 * time.Millisecond)

	// 创建重试失败分区的命令
	retryCommand := model.NewRetryFailedPartitionsCommand([]int{nextPartitionID, nextPartitionID + 1})

	// 提交命令到命令队列 - 直接传递命令对象，让DataStore进行序列化
	err = dataStore.SubmitCommand(ctx, "test", retryCommand)
	require.NoError(t, err, "提交命令失败")

	// 等待命令被处理（LeaderManager会每5秒检查一次命令）
	// 由于测试环境，我们等待足够长的时间确保命令被处理
	time.Sleep(6 * time.Second)

	// 验证命令已被处理
	// 检查待处理命令是否为空（命令应该已被删除）
	pendingCommands, err := dataStore.GetPendingCommands(ctx, "test", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pendingCommands), "命令应该已被处理并删除")

	// 注意：由于分区可能被自动重新分配或状态改变，我们主要验证命令处理机制而不是具体的分区状态变化
	// 重要的是命令被正确识别、解析和处理了

	// 测试重试所有失败分区的命令
	// 先创建更多失败分区
	failedPartition3 := &model.PartitionInfo{
		PartitionID: nextPartitionID + 2,
		MinID:       baseID + 1001,
		MaxID:       baseID + 1500,
		Status:      model.StatusFailed,
		WorkerID:    "worker1",
		Error:       "另一个错误",
	}
	_, err = strategy.CreatePartitionAtomically(ctx, failedPartition3.PartitionID, failedPartition3.MinID, failedPartition3.MaxID, map[string]interface{}{
		"status":    string(failedPartition3.Status),
		"worker_id": failedPartition3.WorkerID,
		"error":     failedPartition3.Error,
	})
	require.NoError(t, err)

	// 创建重试所有失败分区的命令（不指定具体分区ID）
	retryAllCommand := model.NewRetryFailedPartitionsCommand(nil)

	err = dataStore.SubmitCommand(ctx, "test", retryAllCommand)
	require.NoError(t, err, "提交重试所有失败分区命令失败")

	// 等待命令处理
	time.Sleep(6 * time.Second)

	// 验证所有命令都被处理 - 这是主要的验证点
	// 使用新的 context 来避免原始 context 过期
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()

	pendingCommands, err = dataStore.GetPendingCommands(verifyCtx, "test", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pendingCommands), "所有命令应该已被处理")

	// 停止leader
	lm.Stop()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Leader停止时发生错误: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("等待leader停止超时")
	}
}

// TestLeaderManager_CommandProcessingWithInvalidCommand 测试无效命令的处理
func TestLeaderManager_CommandProcessingWithInvalidCommand(t *testing.T) {
	lm, _, dataStore, planer, cleanup := setupLeaderIntegrationTest(t, "leader-node")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// 设置基本参数
	planer.setPartitionSize(1000)
	planer.setNextMaxID(2000)

	// 模拟一个工作节点
	err := dataStore.RegisterWorker(ctx, "worker1")
	require.NoError(t, err)

	// 启动leader管理器
	done := make(chan error, 1)
	go func() {
		done <- lm.Start(ctx)
	}()

	// 等待leader启动
	time.Sleep(1 * time.Second)
	assert.True(t, lm.IsLeader(), "应该成为leader")

	// 提交一个无效的JSON命令（直接提交字符串而不是对象）
	invalidJSON := `{"id": "invalid", "type": "unknown_command", invalid_json}`
	err = dataStore.SubmitCommand(ctx, "test", invalidJSON)
	require.NoError(t, err, "提交无效命令失败")

	// 提交一个有效JSON但无效类型的命令
	invalidCommand := &model.Command{
		ID:         "test-invalid-type",
		Type:       "unknown_type",
		Timestamp:  time.Now().Unix(),
		Status:     "pending",
		Parameters: map[string]interface{}{},
	}

	err = dataStore.SubmitCommand(ctx, "test", invalidCommand)
	require.NoError(t, err, "提交未知类型命令失败")

	// 等待命令处理
	time.Sleep(6 * time.Second)

	// 验证无效命令已被清理
	pendingCommands, err := dataStore.GetPendingCommands(ctx, "test", 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pendingCommands), "无效命令应该已被清理")

	// 停止leader
	lm.Stop()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Leader停止时发生错误: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("等待leader停止超时")
	}
}
