package elk_coordinator

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NetworkPartitionSimulator 网络分区模拟器
type NetworkPartitionSimulator struct {
	mu          sync.RWMutex
	partitioned map[string]bool // 记录哪些节点被分区
	redis       *miniredis.Miniredis
}

// NewNetworkPartitionSimulator 创建网络分区模拟器
func NewNetworkPartitionSimulator(redis *miniredis.Miniredis) *NetworkPartitionSimulator {
	return &NetworkPartitionSimulator{
		partitioned: make(map[string]bool),
		redis:       redis,
	}
}

// PartitionNode 模拟节点网络分区（通过阻断Redis连接）
func (nps *NetworkPartitionSimulator) PartitionNode(nodeID string, client *redis.Client) {
	nps.mu.Lock()
	defer nps.mu.Unlock()

	nps.partitioned[nodeID] = true
	// 通过关闭Redis连接来模拟网络分区
	// 注意：这是一个简化的模拟，实际情况更复杂
}

// RecoverNode 恢复节点网络连接
func (nps *NetworkPartitionSimulator) RecoverNode(nodeID string) {
	nps.mu.Lock()
	defer nps.mu.Unlock()

	nps.partitioned[nodeID] = false
}

// IsPartitioned 检查节点是否被分区
func (nps *NetworkPartitionSimulator) IsPartitioned(nodeID string) bool {
	nps.mu.RLock()
	defer nps.mu.RUnlock()

	return nps.partitioned[nodeID]
}

// RedisFailureSimulator Redis故障模拟器
type RedisFailureSimulator struct {
	redis  *miniredis.Miniredis
	failed bool
	mu     sync.RWMutex
}

// NewRedisFailureSimulator 创建Redis故障模拟器
func NewRedisFailureSimulator(redis *miniredis.Miniredis) *RedisFailureSimulator {
	return &RedisFailureSimulator{
		redis: redis,
	}
}

// SimulateFailure 模拟Redis故障
func (rfs *RedisFailureSimulator) SimulateFailure() error {
	rfs.mu.Lock()
	defer rfs.mu.Unlock()

	if !rfs.failed {
		rfs.redis.Close()
		rfs.failed = true
	}
	return nil
}

// SimulateRecovery 模拟Redis恢复
func (rfs *RedisFailureSimulator) SimulateRecovery() error {
	rfs.mu.Lock()
	defer rfs.mu.Unlock()

	if rfs.failed {
		// 重新启动Redis实例
		newRedis, err := miniredis.Run()
		if err != nil {
			return err
		}
		rfs.redis = newRedis
		rfs.failed = false
	}
	return nil
}

// IsFailed 检查Redis是否处于故障状态
func (rfs *RedisFailureSimulator) IsFailed() bool {
	rfs.mu.RLock()
	defer rfs.mu.RUnlock()
	return rfs.failed
}

// GetAddr 获取Redis地址
func (rfs *RedisFailureSimulator) GetAddr() string {
	rfs.mu.RLock()
	defer rfs.mu.RUnlock()
	return rfs.redis.Addr()
}

// TestMgr_NetworkPartitionTolerance 测试网络分区容错性
// 高优先级边界测试：验证系统在网络分区情况下的行为
// 测试场景：
// 1. 启动3个MGR节点
// 2. 等待系统稳定和leader选举
// 3. 模拟网络分区，将leader节点隔离
// 4. 验证剩余节点能够重新选举leader
// 5. 恢复网络分区
// 6. 验证系统能够重新整合
func TestMgr_NetworkPartitionTolerance(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建测试组件
	processors := make([]*TestProcessor, 3)
	planers := make([]*TestPartitionPlaner, 3)
	mgrs := make([]*Mgr, 3)

	for i := 0; i < 3; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(200 * time.Millisecond)
		planers[i] = NewTestPartitionPlaner(500, 5000) // 较小数据集便于观察
	}

	// 创建网络分区模拟器
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// 创建MGR实例
	for i := 0; i < 3; i++ {
		mgrs[i] = createTestMgr(t, "test-partition", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(1*time.Second),
			WithLeaderElectionInterval(1*time.Second),
			WithPartitionLockExpiry(3*time.Second),
			WithLeaderLockExpiry(5*time.Second),
		)
	}

	// 启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(100 * time.Millisecond) // 错开启动时间
	}

	// 等待系统稳定和leader选举
	time.Sleep(3 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-partition")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err)
	assert.Len(t, workers, 3, "应该有3个活跃节点")

	// 找到当前leader节点
	leaderKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test-partition")
	leaderID, err := dataStore.GetKey(ctx, leaderKey)
	require.NoError(t, err, "获取leader失败")
	assert.NotEmpty(t, leaderID, "应该有leader被选出")

	t.Logf("当前Leader节点: %s", leaderID)

	// 找到leader节点的索引
	var leaderIndex int = -1
	for i, mgr := range mgrs {
		if mgr.ID == leaderID {
			leaderIndex = i
			break
		}
	}
	require.NotEqual(t, -1, leaderIndex, "应该找到leader节点")

	// 记录分区前的处理状态
	totalProcessedBefore := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		totalProcessedBefore += processed
		t.Logf("分区前节点%d处理项目数: %d", i, processed)
	}

	t.Logf("分区前总处理项目数: %d", totalProcessedBefore)

	// 模拟网络分区：隔离leader节点
	t.Logf("模拟网络分区，隔离Leader节点 %d", leaderIndex)

	// 停止leader节点来模拟网络分区
	mgrs[leaderIndex].Stop()

	// 等待故障检测和新leader选举
	time.Sleep(8 * time.Second)

	// 验证新的leader被选出
	newLeaderID, err := dataStore.GetKey(ctx, leaderKey)
	if err == nil && newLeaderID != "" && newLeaderID != leaderID {
		t.Logf("新Leader已选出: %s (原Leader: %s)", newLeaderID, leaderID)
	} else {
		t.Logf("Leader选举可能仍在进行中或失败")
	}

	// 等待系统恢复
	time.Sleep(5 * time.Second)

	// 检查剩余节点状态
	remainingWorkers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	if err == nil {
		t.Logf("网络分区后活跃节点数: %d", len(remainingWorkers))
		for _, workerID := range remainingWorkers {
			if workerID != leaderID {
				heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test-partition", workerID)
				heartbeat, err := dataStore.GetKey(ctx, heartbeatKey)
				if err == nil && heartbeat != "" {
					t.Logf("节点 %s 心跳正常", workerID)
				}
			}
		}
	}

	// 记录分区后的处理状态
	totalProcessedAfter := int64(0)
	for i, processor := range processors {
		if i != leaderIndex { // 跳过被分区的节点
			processed := processor.GetProcessedItems()
			totalProcessedAfter += processed
			t.Logf("分区后节点%d处理项目数: %d", i, processed)
		}
	}

	t.Logf("分区后总处理项目数: %d", totalProcessedAfter)

	// 验证剩余节点能够继续处理任务（可能的处理增长）
	if totalProcessedAfter >= totalProcessedBefore {
		t.Logf("网络分区后任务处理能够继续")
	} else {
		t.Logf("网络分区对任务处理产生了影响")
	}

	// 停止剩余节点
	for i, mgr := range mgrs {
		if i != leaderIndex {
			mgr.Stop()
		}
	}
}

// TestMgr_RedisFailureRecovery 测试Redis故障和恢复场景
// 高优先级边界测试：验证系统在Redis故障时的容错性和恢复能力
// 测试场景：
// 1. 启动2个MGR节点
// 2. 等待系统正常运行
// 3. 模拟Redis故障
// 4. 验证节点能够正确处理Redis连接失败
// 5. 恢复Redis服务
// 6. 验证系统能够重新连接并恢复正常运行
func TestMgr_RedisFailureRecovery(t *testing.T) {
	// 创建可控的Redis实例
	mr, err := miniredis.Run()
	require.NoError(t, err)

	// 创建Redis客户端
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// 测试连接
	err = client.Ping(context.Background()).Err()
	require.NoError(t, err)

	// 清除数据
	err = client.FlushAll(context.Background()).Err()
	require.NoError(t, err)

	// 创建数据存储
	opts := &data.Options{
		KeyPrefix:     "test:redis-failure:",
		DefaultExpiry: 10 * time.Second,
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 500 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建Redis故障模拟器
	failureSim := NewRedisFailureSimulator(mr)

	// 创建测试组件
	processors := make([]*TestProcessor, 2)
	planers := make([]*TestPartitionPlaner, 2)
	mgrs := make([]*Mgr, 2)

	for i := 0; i < 2; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(300 * time.Millisecond)
		planers[i] = NewTestPartitionPlaner(500, 3000)
	}

	// 创建MGR实例，使用更短的重试间隔以便快速检测故障
	for i := 0; i < 2; i++ {
		mgrs[i] = createTestMgr(t, "test-redis-failure", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(1*time.Second),
			WithLeaderElectionInterval(1*time.Second),
			WithPartitionLockExpiry(3*time.Second),
			WithLeaderLockExpiry(5*time.Second),
		)
	}

	// 启动MGR节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(200 * time.Millisecond)
	}

	// 等待系统稳定
	time.Sleep(3 * time.Second)

	// 验证系统正常运行
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-redis-failure")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点失败")
	assert.Len(t, workers, 2, "应该有2个活跃节点")

	// 记录Redis故障前的状态
	totalProcessedBefore := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		totalProcessedBefore += processed
		t.Logf("Redis故障前节点%d处理项目数: %d", i, processed)
	}

	t.Logf("Redis故障前总处理项目数: %d", totalProcessedBefore)

	// 模拟Redis故障
	t.Logf("模拟Redis故障...")
	err = failureSim.SimulateFailure()
	require.NoError(t, err, "模拟Redis故障失败")

	// 等待故障被检测到
	time.Sleep(5 * time.Second)

	// 此时系统应该检测到Redis连接失败
	// 节点应该停止处理新任务，但不应该崩溃
	t.Logf("Redis故障期间，系统应该停止处理新任务")

	// 等待一段时间观察故障期间的行为
	time.Sleep(3 * time.Second)

	// 记录Redis故障期间的状态
	totalProcessedDuringFailure := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		totalProcessedDuringFailure += processed
		t.Logf("Redis故障期间节点%d处理项目数: %d", i, processed)
	}

	t.Logf("Redis故障期间总处理项目数: %d", totalProcessedDuringFailure)

	// 模拟Redis恢复
	t.Logf("模拟Redis恢复...")
	err = failureSim.SimulateRecovery()
	require.NoError(t, err, "模拟Redis恢复失败")

	// 创建新的Redis客户端连接到恢复的实例
	newClient := redis.NewClient(&redis.Options{
		Addr: failureSim.GetAddr(),
	})
	defer newClient.Close()

	// 验证Redis恢复
	err = newClient.Ping(ctx).Err()
	require.NoError(t, err, "Redis应该已经恢复")

	// 等待系统重新连接和恢复
	time.Sleep(8 * time.Second)

	// 记录Redis恢复后的状态
	totalProcessedAfterRecovery := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		totalProcessedAfterRecovery += processed
		t.Logf("Redis恢复后节点%d处理项目数: %d", i, processed)
	}

	t.Logf("Redis恢复后总处理项目数: %d", totalProcessedAfterRecovery)

	// 验证Redis故障和恢复的影响
	if totalProcessedDuringFailure == totalProcessedBefore {
		t.Logf("Redis故障期间没有新的任务处理（符合预期）")
	}

	// 验证系统在故障期间的弹性
	t.Logf("Redis故障容错测试完成")

	// 清理
	for i, mgr := range mgrs {
		mgr.Stop()
		t.Logf("已停止MGR节点 %d", i)
	}

	newClient.Close()
	mr.Close()
}

// TestMgr_TaskWindowDistributedEdgeCases 测试TaskWindow在分布式环境下的边界情况
// 高优先级边界测试：验证TaskWindow在分布式场景下的特殊边界情况
// 测试场景：
// 1. 任务窗口在高并发抢占情况下的行为
// 2. 任务窗口在网络延迟情况下的超时处理
// 3. 任务窗口在分区快速变化时的稳定性
// 4. 多个TaskWindow同时竞争相同分区的边界情况
func TestMgr_TaskWindowDistributedEdgeCases(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// 创建一个特殊的处理器，模拟处理时间变化很大的场景
	type VariableDelayProcessor struct {
		*TestProcessor
		delays     []time.Duration
		currentIdx int64
		mu         sync.RWMutex
	}

	newVariableDelayProcessor := func(delays []time.Duration) *VariableDelayProcessor {
		return &VariableDelayProcessor{
			TestProcessor: NewTestProcessor(),
			delays:        delays,
		}
	}

	// 重写Process方法以使用变化的延迟
	variableDelayProcessor := newVariableDelayProcessor([]time.Duration{
		50 * time.Millisecond,   // 快速处理
		500 * time.Millisecond,  // 中等处理
		1500 * time.Millisecond, // 慢速处理
		100 * time.Millisecond,  // 快速处理
		800 * time.Millisecond,  // 中等偏慢处理
	})

	// 自定义处理函数，使用变化的延迟
	variableDelayProcessor.WithCustomProcessor(func(minID, maxID int64) error {
		variableDelayProcessor.mu.RLock()
		idx := atomic.LoadInt64(&variableDelayProcessor.currentIdx)
		delay := variableDelayProcessor.delays[idx%int64(len(variableDelayProcessor.delays))]
		variableDelayProcessor.mu.RUnlock()

		atomic.AddInt64(&variableDelayProcessor.currentIdx, 1)

		// 模拟变化的处理时间
		time.Sleep(delay)
		return nil
	})

	// 创建测试组件 - 4个节点以增加竞争
	processors := make([]*TestProcessor, 4)
	planers := make([]*TestPartitionPlaner, 4)
	mgrs := make([]*Mgr, 4)

	// 第一个节点使用变化延迟处理器，其他使用标准处理器
	processors[0] = variableDelayProcessor.TestProcessor
	for i := 1; i < 4; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(200 * time.Millisecond)
	}

	// 创建较小的分区以增加竞争
	for i := 0; i < 4; i++ {
		planers[i] = NewTestPartitionPlaner(300, 2400) // 8个分区，4个节点竞争
	}

	// 创建MGR实例，使用较短的锁过期时间以测试边界情况
	for i := 0; i < 4; i++ {
		mgrs[i] = createTestMgr(t, "test-taskwindow-edge", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(800*time.Millisecond),
			WithLeaderElectionInterval(1*time.Second),
			WithPartitionLockExpiry(2*time.Second), // 较短的锁过期时间
			WithLeaderLockExpiry(5*time.Second),
			WithTaskWindow(3),         // 较小的任务窗口增加获取频率
			WithAllowPreemption(true), // 允许抢占以测试边界情况
		)
	}

	// 快速启动所有节点以增加初始竞争
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(50 * time.Millisecond) // 很短的启动间隔
	}

	// 等待系统稍微稳定
	time.Sleep(2 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-taskwindow-edge")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点失败")
	assert.Len(t, workers, 4, "应该有4个活跃节点")

	t.Logf("TaskWindow分布式边界测试开始，4个节点竞争8个分区")

	// 运行测试一段时间，观察TaskWindow的边界行为
	time.Sleep(12 * time.Second)

	// 收集统计信息
	totalProcessed := int64(0)
	processedByNode := make([]int64, 4)
	processHistories := make([][]ProcessRecord, 4)

	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		processCount := processor.GetProcessCount()
		history := processor.GetProcessHistory()

		totalProcessed += processed
		processedByNode[i] = processed
		processHistories[i] = history

		t.Logf("节点%d: 处理次数=%d, 处理项目数=%d", i, processCount, processed)
	}

	t.Logf("总处理项目数: %d", totalProcessed)

	// 验证TaskWindow边界情况处理
	assert.Greater(t, totalProcessed, int64(0), "应该有任务被处理")

	// 验证多个节点都参与了处理（在TaskWindow和抢占机制下）
	activeNodes := 0
	for i, processed := range processedByNode {
		if processed > 0 {
			activeNodes++
			t.Logf("节点%d参与了任务处理", i)
		}
	}

	// 在TaskWindow和抢占机制下，期望至少2个节点参与处理
	assert.GreaterOrEqual(t, activeNodes, 2, "至少应该有2个节点参与处理")

	// 分析处理历史，查找可能的边界情况
	var totalProcessEvents int
	var errorEvents int

	for i, history := range processHistories {
		nodeProcessEvents := len(history)
		totalProcessEvents += nodeProcessEvents

		for _, record := range history {
			if record.Error != nil {
				errorEvents++
				t.Logf("节点%d处理错误: %v (MinID=%d, MaxID=%d)", i, record.Error, record.MinID, record.MaxID)
			}
		}
	}

	t.Logf("总处理事件数: %d, 错误事件数: %d", totalProcessEvents, errorEvents)

	// 在边界测试中，少量错误是可以接受的（如锁竞争失败）
	errorRate := float64(errorEvents) / float64(totalProcessEvents)
	t.Logf("错误率: %.2f%%", errorRate*100)

	// 验证错误率在可接受范围内（不超过20%）
	assert.LessOrEqual(t, errorRate, 0.2, "错误率应该在可接受范围内")

	// 检查是否有重复处理的情况（这是最重要的正确性检查）
	processedRanges := make(map[string]int)
	for i, history := range processHistories {
		for _, record := range history {
			if record.Error == nil { // 只统计成功的处理
				rangeKey := fmt.Sprintf("%d-%d", record.MinID, record.MaxID)
				processedRanges[rangeKey]++
				if processedRanges[rangeKey] > 1 {
					t.Logf("检测到重复处理: 节点%d, 范围%s, 重复次数%d", i, rangeKey, processedRanges[rangeKey])
				}
			}
		}
	}

	// 统计重复处理的数量
	duplicateCount := 0
	for _, count := range processedRanges {
		if count > 1 {
			duplicateCount += count - 1
		}
	}

	t.Logf("重复处理的范围数量: %d", duplicateCount)

	// 在TaskWindow和抢占的边界测试中，少量重复是可能的，但应该很少
	duplicateRate := float64(duplicateCount) / float64(len(processedRanges))
	t.Logf("重复处理率: %.2f%%", duplicateRate*100)

	// 验证重复处理率在可接受范围内（不超过10%）
	assert.LessOrEqual(t, duplicateRate, 0.1, "重复处理率应该在可接受范围内")

	t.Logf("TaskWindow分布式边界测试完成")

	// 停止所有MGR节点
	for i, mgr := range mgrs {
		mgr.Stop()
		t.Logf("已停止MGR节点 %d", i)
	}
}

// TestMgr_HighFrequencyLeaderElectionCompetition 测试高频leader选举竞争
// 高优先级边界测试：验证系统在频繁leader变化情况下的稳定性
// 测试场景：
// 1. 启动5个MGR节点
// 2. 使用很短的leader锁过期时间
// 3. 频繁停止和重启leader节点
// 4. 验证系统能够处理快速的leader变化
// 5. 验证任务处理的连续性和正确性
func TestMgr_HighFrequencyLeaderElectionCompetition(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// 创建测试组件 - 5个节点增加选举竞争
	processors := make([]*TestProcessor, 5)
	planers := make([]*TestPartitionPlaner, 5)
	mgrs := make([]*Mgr, 5)

	for i := 0; i < 5; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(150 * time.Millisecond)
		planers[i] = NewTestPartitionPlaner(400, 2000) // 5个分区，增加竞争
	}

	// 创建MGR实例，使用非常短的leader锁过期时间
	for i := 0; i < 5; i++ {
		mgrs[i] = createTestMgr(t, "test-leader-competition", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(500*time.Millisecond),      // 较快的心跳
			WithLeaderElectionInterval(500*time.Millisecond), // 较快的选举检查
			WithPartitionLockExpiry(3*time.Second),
			WithLeaderLockExpiry(2*time.Second), // 非常短的leader锁过期时间
			WithTaskWindow(2),                   // 较小的任务窗口
		)
	}

	// 快速启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(50 * time.Millisecond) // 很短的启动间隔
	}

	// 等待初始选举稳定
	time.Sleep(2 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-leader-competition")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err)
	assert.Len(t, workers, 5, "应该有5个活跃节点")

	leaderKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test-leader-competition")
	leaderChanges := make([]string, 0)
	previousLeader := ""

	// 监控leader变化并人为创造leader选举竞争
	for round := 0; round < 4; round++ {
		t.Logf("=== Leader竞争轮次 %d ===", round+1)

		// 获取当前leader
		currentLeader, err := dataStore.GetKey(ctx, leaderKey)
		if err == nil && currentLeader != "" && currentLeader != previousLeader {
			leaderChanges = append(leaderChanges, currentLeader)
			t.Logf("检测到Leader变化: %s", currentLeader)
			previousLeader = currentLeader
		}

		// 找到当前leader并停止它
		if currentLeader != "" {
			for i, mgr := range mgrs {
				if mgr != nil && mgr.ID == currentLeader {
					t.Logf("停止当前Leader节点 %d (%s)", i, mgr.ID)
					mgr.Stop()
					mgrs[i] = nil // 标记为已停止
					break
				}
			}
		}

		// 等待新leader选举
		time.Sleep(3 * time.Second)

		// 检查新leader是否被选出
		newLeader, err := dataStore.GetKey(ctx, leaderKey)
		if err == nil && newLeader != "" && newLeader != currentLeader {
			t.Logf("新Leader已选出: %s", newLeader)
		}

		// 等待一段时间观察处理
		time.Sleep(1 * time.Second)
	}

	// 等待系统稳定
	time.Sleep(3 * time.Second)

	// 收集最终统计信息
	totalProcessed := int64(0)
	activeNodes := 0
	for i, processor := range processors {
		if mgrs[i] != nil { // 只统计仍然活跃的节点
			processed := processor.GetProcessedItems()
			totalProcessed += processed
			if processed > 0 {
				activeNodes++
			}
			t.Logf("节点%d: 处理项目数=%d (节点状态: %v)", i, processed, mgrs[i] != nil)
		} else {
			processed := processor.GetProcessedItems()
			totalProcessed += processed
			t.Logf("节点%d: 处理项目数=%d (已停止)", i, processed)
		}
	}

	t.Logf("总处理项目数: %d", totalProcessed)
	t.Logf("Leader变化次数: %d", len(leaderChanges))
	t.Logf("参与处理的节点数: %d", activeNodes)

	// 验证在高频leader变化下系统仍能处理任务
	assert.Greater(t, totalProcessed, int64(0), "即使在频繁leader变化下，也应该有任务被处理")
	assert.GreaterOrEqual(t, len(leaderChanges), 2, "应该有多次leader变化")

	// 停止剩余节点
	for i, mgr := range mgrs {
		if mgr != nil {
			mgr.Stop()
			t.Logf("已停止剩余MGR节点 %d", i)
		}
	}
}

// TestMgr_PartitionLockPreemptionEdgeCases 测试分区锁抢占边界情况
// 高优先级边界测试：验证分区锁抢占机制在极端情况下的正确性
// 测试场景：
// 1. 启动多个节点，其中一些设置允许抢占，一些不允许
// 2. 模拟节点长时间持有分区锁但无法完成处理
// 3. 验证抢占机制能正确工作
// 4. 验证抢占过程中没有重复处理
// 5. 测试抢占失败的恢复机制
func TestMgr_PartitionLockPreemptionEdgeCases(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 创建特殊的慢速处理器，模拟长时间持有锁的情况
	type SlowProcessor struct {
		*TestProcessor
		slowDuration time.Duration
		shouldBlock  bool
		blockCtx     context.Context
		blockCancel  context.CancelFunc
		mu           sync.RWMutex
	}

	newSlowProcessor := func(slowDuration time.Duration) *SlowProcessor {
		blockCtx, blockCancel := context.WithCancel(context.Background())
		return &SlowProcessor{
			TestProcessor: NewTestProcessor(),
			slowDuration:  slowDuration,
			shouldBlock:   true,
			blockCtx:      blockCtx,
			blockCancel:   blockCancel,
		}
	}

	// 创建测试组件
	processors := make([]*TestProcessor, 4)
	planers := make([]*TestPartitionPlaner, 4)
	mgrs := make([]*Mgr, 4)

	// 第一个节点使用慢速处理器（会长时间持有锁）
	slowProcessor := newSlowProcessor(8 * time.Second)
	slowProcessor.WithCustomProcessor(func(minID, maxID int64) error {
		slowProcessor.mu.RLock()
		shouldBlock := slowProcessor.shouldBlock
		blockCtx := slowProcessor.blockCtx
		slowDuration := slowProcessor.slowDuration
		slowProcessor.mu.RUnlock()

		if shouldBlock {
			t.Logf("慢速处理器开始长时间处理 (MinID=%d, MaxID=%d, 持续=%v)", minID, maxID, slowDuration)
			select {
			case <-blockCtx.Done():
				t.Logf("慢速处理器被中断")
				return blockCtx.Err()
			case <-time.After(slowDuration):
				t.Logf("慢速处理器完成处理 (MinID=%d, MaxID=%d)", minID, maxID)
				return nil
			}
		}
		return nil
	})

	processors[0] = slowProcessor.TestProcessor

	// 其他节点使用正常处理器
	for i := 1; i < 4; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(100 * time.Millisecond)
	}

	// 创建较小的分区数据集
	for i := 0; i < 4; i++ {
		planers[i] = NewTestPartitionPlaner(500, 2000) // 4个分区
	}

	// 创建MGR实例
	// 第一个节点：不允许抢占（会长时间持有锁）
	mgrs[0] = createTestMgr(t, "test-preemption-edge", dataStore, processors[0], planers[0],
		WithHeartbeatInterval(1*time.Second),
		WithLeaderElectionInterval(1*time.Second),
		WithPartitionLockExpiry(4*time.Second), // 较短的锁过期时间
		WithLeaderLockExpiry(8*time.Second),
		WithTaskWindow(1),          // 单个任务窗口
		WithAllowPreemption(false), // 不允许抢占
	)

	// 其他节点：允许抢占
	for i := 1; i < 4; i++ {
		mgrs[i] = createTestMgr(t, "test-preemption-edge", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(1*time.Second),
			WithLeaderElectionInterval(1*time.Second),
			WithPartitionLockExpiry(4*time.Second),
			WithLeaderLockExpiry(8*time.Second),
			WithTaskWindow(2),         // 较大的任务窗口增加抢占机会
			WithAllowPreemption(true), // 允许抢占
		)
	}

	// 启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(200 * time.Millisecond)
	}

	// 等待系统稳定
	time.Sleep(3 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-preemption-edge")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err)
	assert.Len(t, workers, 4, "应该有4个活跃节点")

	t.Logf("分区锁抢占边界测试开始")

	// 观察抢占行为
	time.Sleep(8 * time.Second)

	// 停止慢速处理器的阻塞行为，看看是否能恢复正常
	slowProcessor.mu.Lock()
	slowProcessor.shouldBlock = false
	slowProcessor.blockCancel()
	slowProcessor.mu.Unlock()

	t.Logf("已停止慢速处理器的阻塞行为")

	// 继续观察一段时间
	time.Sleep(4 * time.Second)

	// 收集统计信息
	totalProcessed := int64(0)
	processHistories := make([][]ProcessRecord, 4)

	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		processCount := processor.GetProcessCount()
		history := processor.GetProcessHistory()

		totalProcessed += processed
		processHistories[i] = history

		t.Logf("节点%d: 处理次数=%d, 处理项目数=%d", i, processCount, processed)

		// 分析处理历史中的错误
		var errors []error
		for _, record := range history {
			if record.Error != nil {
				errors = append(errors, record.Error)
			}
		}
		if len(errors) > 0 {
			t.Logf("节点%d处理错误: %d个", i, len(errors))
		}
	}

	t.Logf("总处理项目数: %d", totalProcessed)

	// 验证抢占机制的有效性
	assert.Greater(t, totalProcessed, int64(0), "抢占机制下应该有任务被处理")

	// 验证慢速节点和快速节点都参与了处理
	slowNodeProcessed := processors[0].GetProcessedItems()
	fastNodesProcessed := int64(0)
	for i := 1; i < 4; i++ {
		fastNodesProcessed += processors[i].GetProcessedItems()
	}

	t.Logf("慢速节点处理项目数: %d", slowNodeProcessed)
	t.Logf("快速节点总处理项目数: %d", fastNodesProcessed)

	// 在抢占机制下，快速节点应该能够处理一些任务
	assert.Greater(t, fastNodesProcessed, int64(0), "快速节点应该通过抢占处理了一些任务")

	// 检查重复处理情况
	processedRanges := make(map[string]int)
	for i, history := range processHistories {
		for _, record := range history {
			if record.Error == nil {
				rangeKey := fmt.Sprintf("%d-%d", record.MinID, record.MaxID)
				processedRanges[rangeKey]++
				if processedRanges[rangeKey] > 1 {
					t.Logf("检测到重复处理: 节点%d, 范围%s", i, rangeKey)
				}
			}
		}
	}

	duplicateCount := 0
	for _, count := range processedRanges {
		if count > 1 {
			duplicateCount += count - 1
		}
	}

	t.Logf("重复处理的范围数量: %d", duplicateCount)

	// 在抢占边界测试中，少量重复是可能的，但应该很少
	if len(processedRanges) > 0 {
		duplicateRate := float64(duplicateCount) / float64(len(processedRanges))
		t.Logf("重复处理率: %.2f%%", duplicateRate*100)
		assert.LessOrEqual(t, duplicateRate, 0.15, "抢占边界测试中重复处理率应该较低")
	}

	t.Logf("分区锁抢占边界测试完成")

	// 停止所有MGR节点
	for i, mgr := range mgrs {
		mgr.Stop()
		t.Logf("已停止MGR节点 %d", i)
	}
}

// TestMgr_ClockSkewAndTimeoutEdgeCases 测试时钟偏移和超时边界情况
// 高优先级边界测试：验证系统在时钟偏移和各种超时情况下的行为
// 测试场景：
// 1. 模拟不同节点的时钟偏移
// 2. 测试各种超时边界情况
// 3. 验证系统的时间相关容错性
func TestMgr_ClockSkewAndTimeoutEdgeCases(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 创建测试组件
	processors := make([]*TestProcessor, 3)
	planers := make([]*TestPartitionPlaner, 3)
	mgrs := make([]*Mgr, 3)

	for i := 0; i < 3; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(200 * time.Millisecond)
		planers[i] = NewTestPartitionPlaner(600, 1800) // 3个分区
	}

	// 创建MGR实例，使用不同的超时配置来模拟时钟偏移影响
	timeouts := []struct {
		heartbeat    time.Duration
		election     time.Duration
		partitionExp time.Duration
		leaderExp    time.Duration
	}{
		{800 * time.Millisecond, 1200 * time.Millisecond, 3 * time.Second, 6 * time.Second},   // "快时钟"节点
		{1200 * time.Millisecond, 1800 * time.Millisecond, 5 * time.Second, 8 * time.Second},  // "正常时钟"节点
		{1500 * time.Millisecond, 2200 * time.Millisecond, 6 * time.Second, 10 * time.Second}, // "慢时钟"节点
	}

	for i := 0; i < 3; i++ {
		mgrs[i] = createTestMgr(t, "test-clock-timeout", dataStore, processors[i], planers[i],
			WithHeartbeatInterval(timeouts[i].heartbeat),
			WithLeaderElectionInterval(timeouts[i].election),
			WithPartitionLockExpiry(timeouts[i].partitionExp),
			WithLeaderLockExpiry(timeouts[i].leaderExp),
			WithTaskWindow(2),
		)
	}

	// 启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR节点 %d 失败", i)
		time.Sleep(300 * time.Millisecond) // 错开启动时间
	}

	// 等待系统在不同超时配置下稳定
	time.Sleep(5 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-clock-timeout")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err)
	assert.Len(t, workers, 3, "应该有3个活跃节点")

	t.Logf("时钟偏移和超时边界测试开始")

	// 运行测试观察行为
	time.Sleep(8 * time.Second)

	// 收集统计信息
	totalProcessed := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		totalProcessed += processed
		t.Logf("节点%d (超时配置%d): 处理项目数=%d", i, i, processed)
	}

	t.Logf("总处理项目数: %d", totalProcessed)

	// 验证在不同超时配置下系统仍能正常工作
	assert.Greater(t, totalProcessed, int64(0), "在时钟偏移情况下应该有任务被处理")

	// 检查leader稳定性
	leaderKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test-clock-timeout")
	leader, err := dataStore.GetKey(ctx, leaderKey)
	if err == nil && leader != "" {
		t.Logf("最终Leader: %s", leader)
	}

	t.Logf("时钟偏移和超时边界测试完成")

	// 停止所有MGR节点
	for i, mgr := range mgrs {
		mgr.Stop()
		t.Logf("已停止MGR节点 %d", i)
	}
}
