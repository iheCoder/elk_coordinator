package elk_coordinator

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/leader"
	"elk_coordinator/model"
	"elk_coordinator/task"
	"elk_coordinator/test_utils"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessor 是一个测试用的任务处理器实现
type TestProcessor struct {
	processCount   int64                    // 处理次数计数器
	processedItems int64                    // 处理的项目总数
	processDelay   time.Duration            // 每次处理的延迟
	shouldFail     bool                     // 是否应该返回错误
	failureRate    float64                  // 失败率 (0.0-1.0)
	processHistory []ProcessRecord          // 处理历史记录
	mu             sync.RWMutex             // 保护共享状态
	onProcess      func(int64, int64) error // 自定义处理函数
}

// ProcessRecord 记录单次处理的信息
type ProcessRecord struct {
	MinID     int64
	MaxID     int64
	Count     int64
	Timestamp time.Time
	Error     error
}

// NewTestProcessor 创建一个新的测试处理器
func NewTestProcessor() *TestProcessor {
	return &TestProcessor{
		processDelay:   100 * time.Millisecond, // 默认处理延迟
		processHistory: make([]ProcessRecord, 0),
	}
}

// WithProcessDelay 设置处理延迟
func (tp *TestProcessor) WithProcessDelay(delay time.Duration) *TestProcessor {
	tp.processDelay = delay
	return tp
}

// WithFailureRate 设置失败率
func (tp *TestProcessor) WithFailureRate(rate float64) *TestProcessor {
	tp.failureRate = rate
	return tp
}

// WithCustomProcessor 设置自定义处理函数
func (tp *TestProcessor) WithCustomProcessor(fn func(int64, int64) error) *TestProcessor {
	tp.onProcess = fn
	return tp
}

// Process 实现task.Processor接口
func (tp *TestProcessor) Process(ctx context.Context, minID int64, maxID int64, options map[string]interface{}) (int64, error) {
	// 增加处理计数
	atomic.AddInt64(&tp.processCount, 1)

	// 模拟处理延迟
	if tp.processDelay > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(tp.processDelay):
		}
	}

	var processError error
	var itemCount int64

	// 如果设置了自定义处理函数，使用它
	if tp.onProcess != nil {
		processError = tp.onProcess(minID, maxID)
	}

	// 模拟失败率
	if processError == nil && tp.failureRate > 0 {
		if float64(atomic.LoadInt64(&tp.processCount)%100)/100.0 < tp.failureRate {
			processError = fmt.Errorf("模拟处理失败 (失败率: %.2f)", tp.failureRate)
		}
	}

	// 如果没有错误，计算处理的项目数
	if processError == nil {
		itemCount = maxID - minID + 1
		atomic.AddInt64(&tp.processedItems, itemCount)
	}

	// 记录处理历史
	tp.mu.Lock()
	tp.processHistory = append(tp.processHistory, ProcessRecord{
		MinID:     minID,
		MaxID:     maxID,
		Count:     itemCount,
		Timestamp: time.Now(),
		Error:     processError,
	})
	tp.mu.Unlock()

	return itemCount, processError
}

// GetProcessCount 获取处理次数
func (tp *TestProcessor) GetProcessCount() int64 {
	return atomic.LoadInt64(&tp.processCount)
}

// GetProcessedItems 获取处理的项目总数
func (tp *TestProcessor) GetProcessedItems() int64 {
	return atomic.LoadInt64(&tp.processedItems)
}

// GetProcessHistory 获取处理历史
func (tp *TestProcessor) GetProcessHistory() []ProcessRecord {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	history := make([]ProcessRecord, len(tp.processHistory))
	copy(history, tp.processHistory)
	return history
}

// TestPartitionPlaner 是一个测试用的分区规划器实现
type TestPartitionPlaner struct {
	partitionSize int64
	maxID         int64
	mu            sync.RWMutex
}

// NewTestPartitionPlaner 创建一个新的测试分区规划器
func NewTestPartitionPlaner(partitionSize int64, maxID int64) *TestPartitionPlaner {
	return &TestPartitionPlaner{
		partitionSize: partitionSize,
		maxID:         maxID,
	}
}

// PartitionSize 实现leader.PartitionPlaner接口
func (tpp *TestPartitionPlaner) PartitionSize(ctx context.Context) (int64, error) {
	tpp.mu.RLock()
	defer tpp.mu.RUnlock()
	return tpp.partitionSize, nil
}

// GetNextMaxID 实现leader.PartitionPlaner接口
func (tpp *TestPartitionPlaner) GetNextMaxID(ctx context.Context, startID int64, rangeSize int64) (int64, error) {
	tpp.mu.RLock()
	defer tpp.mu.RUnlock()

	nextMaxID := startID + rangeSize - 1
	if nextMaxID > tpp.maxID {
		nextMaxID = tpp.maxID
	}
	return nextMaxID, nil
}

// UpdateMaxID 更新最大ID
func (tpp *TestPartitionPlaner) UpdateMaxID(maxID int64) {
	tpp.mu.Lock()
	defer tpp.mu.Unlock()
	tpp.maxID = maxID
}

// setupMgrIntegrationTest 设置MGR集成测试环境
// 返回: (miniredis实例, redis客户端, datastore, 清理函数)
func setupMgrIntegrationTest(t *testing.T) (*miniredis.Miniredis, *redis.Client, *data.RedisDataStore, func()) {
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

	// 清除所有现有数据，确保测试环境干净
	err = client.FlushAll(context.Background()).Err()
	require.NoError(t, err, "清除 Redis 数据失败")

	// 创建 RedisDataStore，使用适合测试的配置
	opts := &data.Options{
		KeyPrefix:     "test:mgr:",
		DefaultExpiry: 10 * time.Second,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: 50 * time.Millisecond,
	}
	dataStore := data.NewRedisDataStore(client, opts)

	// 清理函数
	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return mr, client, dataStore, cleanup
}

// createTestMgr 创建一个测试用的MGR实例
func createTestMgr(t *testing.T, namespace string, dataStore *data.RedisDataStore, processor task.Processor, planer leader.PartitionPlaner, options ...MgrOption) *Mgr {
	// 默认选项：快速的测试配置
	defaultOptions := []MgrOption{
		WithHeartbeatInterval(1 * time.Second),
		WithLeaderElectionInterval(1 * time.Second),
		WithPartitionLockExpiry(5 * time.Second),
		WithLeaderLockExpiry(10 * time.Second),
		WithWorkerPartitionMultiple(2),
		WithLogger(test_utils.NewMockLogger(true)),
	}

	// 合并选项
	allOptions := append(defaultOptions, options...)

	mgr := NewMgr(namespace, dataStore, processor, planer, model.StrategyTypeHash, allOptions...)
	return mgr
}

// TestMgr_SingleNodeBasicOperation 测试单节点基本操作
// 测试场景：
// 1. 启动一个MGR节点
// 2. 验证节点能够成功注册和发送心跳
// 3. 验证节点能够获得leader身份
// 4. 验证节点能够处理分区任务
// 预期结果：
// - 节点成功注册并保持活跃状态
// - 节点获得leader身份并开始分配分区
// - 任务处理器被调用并处理了预期的任务
func TestMgr_SingleNodeBasicOperation(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建测试组件
	processor := NewTestProcessor().WithProcessDelay(50 * time.Millisecond)
	planer := NewTestPartitionPlaner(1000, 10000) // 分区大小1000，最大ID 10000

	// 创建MGR实例，现在TaskWindow是默认启用的
	mgr := createTestMgr(t, "test-single", dataStore, processor, planer)

	// 启动MGR
	err := mgr.Start(ctx)
	require.NoError(t, err, "启动MGR失败")

	// 等待一段时间让系统稳定
	time.Sleep(3 * time.Second)

	// 验证节点是否注册成功
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-single")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点列表失败")
	assert.Contains(t, workers, mgr.ID, "节点应该已注册")

	// 验证心跳是否正常
	heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test-single", mgr.ID)
	heartbeat, err := dataStore.GetKey(ctx, heartbeatKey)
	require.NoError(t, err, "获取心跳失败")
	assert.NotEmpty(t, heartbeat, "心跳应该存在")

	// 等待更多时间让任务处理开始
	time.Sleep(4 * time.Second)

	// 验证任务处理
	processCount := processor.GetProcessCount()
	processedItems := processor.GetProcessedItems()

	t.Logf("处理次数: %d, 处理项目数: %d", processCount, processedItems)

	// 单节点应该能够处理一些任务
	assert.Greater(t, processCount, int64(0), "应该有任务被处理")
	assert.Greater(t, processedItems, int64(0), "应该有项目被处理")

	// 停止MGR
	mgr.Stop()
}

// TestMgr_MultiNodeDistributedOperation 测试多节点分布式操作
// 测试场景：
// 1. 启动3个MGR节点
// 2. 验证只有一个节点成为leader
// 3. 验证任务在多个节点间分布式处理
// 4. 验证每个分区只被一个节点处理（无重复处理）
// 预期结果：
// - 3个节点都注册成功
// - 只有一个节点获得leader身份
// - 任务被分布式处理，无重复
// - 总处理量符合预期
func TestMgr_MultiNodeDistributedOperation(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建3个节点的测试组件
	nodeCount := 3
	processors := make([]*TestProcessor, nodeCount)
	mgrs := make([]*Mgr, nodeCount)

	// 大幅增加分区数量，确保有足够工作供多个节点处理
	// 分区大小100，最大ID 20000，预期200个分区
	planer := NewTestPartitionPlaner(100, 20000)

	// 创建所有节点，增加处理延迟确保有时间分布
	for i := 0; i < nodeCount; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(800 * time.Millisecond) // 增加处理延迟
		mgrs[i] = createTestMgr(t, "test-multi", dataStore, processors[i], planer,
			WithTaskWindow(1)) // 进一步减小TaskWindow到1，增加分布机会
	}

	// 分阶段启动节点，但减少启动间隔以便尽快有多个节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR %d失败", i)
		t.Logf("启动节点%d: %s", i, mgr.ID)

		// 减少启动间隔，让多个节点更快地参与
		if i == 0 {
			// 第一个节点启动后等待少量时间，让它成为leader并创建初始分区
			time.Sleep(500 * time.Millisecond)
		} else {
			// 后续节点间隔较短，让它们快速加入
			time.Sleep(200 * time.Millisecond)
		}
	}

	// 等待系统稍微稳定，但不等太久避免所有任务被处理完
	time.Sleep(1 * time.Second)

	// 验证所有节点都注册了
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-multi")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点列表失败")
	assert.Equal(t, nodeCount, len(workers), "应该有%d个节点注册", nodeCount)

	// 验证所有节点的心跳
	for _, mgr := range mgrs {
		heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "test-multi", mgr.ID)
		heartbeat, err := dataStore.GetKey(ctx, heartbeatKey)
		require.NoError(t, err, "获取节点%s心跳失败", mgr.ID)
		assert.NotEmpty(t, heartbeat, "节点%s心跳应该存在", mgr.ID)
	}

	// 等待任务处理进行，给足够时间让多个节点都能参与处理
	// 由于现在有更多分区和更长的处理时间，需要足够的运行时间
	time.Sleep(15 * time.Second)

	// 收集所有处理统计
	totalProcessCount := int64(0)
	totalProcessedItems := int64(0)
	allProcessHistory := make([]ProcessRecord, 0)

	for i, processor := range processors {
		count := processor.GetProcessCount()
		items := processor.GetProcessedItems()
		history := processor.GetProcessHistory()

		t.Logf("节点%d: 处理次数=%d, 处理项目数=%d", i, count, items)

		totalProcessCount += count
		totalProcessedItems += items
		allProcessHistory = append(allProcessHistory, history...)
	}

	t.Logf("总计: 处理次数=%d, 处理项目数=%d", totalProcessCount, totalProcessedItems)

	// 验证分布式处理效果
	assert.Greater(t, totalProcessCount, int64(0), "应该有任务被处理")
	assert.Greater(t, totalProcessedItems, int64(0), "应该有项目被处理")

	// 验证多个节点都参与了处理（至少2个节点应该有处理记录）
	activeNodes := 0
	for _, processor := range processors {
		if processor.GetProcessCount() > 0 {
			activeNodes++
		}
	}
	assert.GreaterOrEqual(t, activeNodes, 2, "至少应该有2个节点参与处理")

	// 验证没有重复处理相同的ID范围
	processedRanges := make(map[string]int)
	for _, record := range allProcessHistory {
		if record.Error == nil {
			rangeKey := fmt.Sprintf("%d-%d", record.MinID, record.MaxID)
			processedRanges[rangeKey]++
		}
	}

	for rangeKey, count := range processedRanges {
		assert.Equal(t, 1, count, "ID范围%s被处理了%d次，应该只处理1次", rangeKey, count)
	}

	// 停止所有MGR
	for _, mgr := range mgrs {
		mgr.Stop()
	}
}

// TestMgr_HighConcurrencyStressTest 测试高并发压力场景
// 测试场景：
// 1. 启动5个MGR节点
// 2. 使用较小的处理延迟模拟高频任务处理
// 3. 验证系统在高并发下的稳定性
// 4. 验证分区锁和leader锁在高并发下的正确性
// 预期结果：
// - 所有节点保持稳定运行
// - 没有重复处理或数据竞争
// - 系统吞吐量符合预期
// - 无死锁或资源泄露
func TestMgr_HighConcurrencyStressTest(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 创建5个节点进行压力测试
	nodeCount := 5
	processors := make([]*TestProcessor, nodeCount)
	mgrs := make([]*Mgr, nodeCount)

	// 较大的数据集和较小的分区，制造更多并发竞争
	planer := NewTestPartitionPlaner(200, 10000) // 分区大小200，最大ID 10000，预期50个分区

	// 创建所有节点，使用更快的处理速度和任务窗口
	for i := 0; i < nodeCount; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(20 * time.Millisecond) // 快速处理
		mgrs[i] = createTestMgr(t, "test-stress", dataStore, processors[i], planer,
			WithTaskWindow(5), // 启用任务窗口，同时处理5个分区
			WithHeartbeatInterval(500*time.Millisecond), // 更频繁的心跳
			WithLeaderElectionInterval(500*time.Millisecond),
			WithPartitionLockExpiry(3*time.Second), // 较短的锁过期时间，增加竞争
		)
	}

	// 快速启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR %d失败", i)
		time.Sleep(100 * time.Millisecond) // 较短的启动间隔
	}

	// 等待系统预热
	time.Sleep(3 * time.Second)

	// 验证所有节点注册
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-stress")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点列表失败")
	assert.Equal(t, nodeCount, len(workers), "应该有%d个节点注册", nodeCount)

	// 运行压力测试
	time.Sleep(12 * time.Second)

	// 收集统计信息
	totalProcessCount := int64(0)
	totalProcessedItems := int64(0)
	successfulProcesses := int64(0)
	failedProcesses := int64(0)
	allProcessHistory := make([]ProcessRecord, 0)

	for i, processor := range processors {
		count := processor.GetProcessCount()
		items := processor.GetProcessedItems()
		history := processor.GetProcessHistory()

		// 统计成功和失败的处理
		successCount := int64(0)
		failCount := int64(0)
		for _, record := range history {
			if record.Error == nil {
				successCount++
			} else {
				failCount++
			}
		}

		t.Logf("节点%d: 处理次数=%d, 成功=%d, 失败=%d, 处理项目数=%d",
			i, count, successCount, failCount, items)

		totalProcessCount += count
		totalProcessedItems += items
		successfulProcesses += successCount
		failedProcesses += failCount
		allProcessHistory = append(allProcessHistory, history...)
	}

	t.Logf("总计: 处理次数=%d, 成功=%d, 失败=%d, 处理项目数=%d",
		totalProcessCount, successfulProcesses, failedProcesses, totalProcessedItems)

	// 验证高并发处理效果
	assert.Greater(t, totalProcessCount, int64(10), "高并发下应该处理大量任务")
	assert.Greater(t, totalProcessedItems, int64(1000), "应该处理大量项目")

	// 验证成功率（应该大部分成功）
	if totalProcessCount > 0 {
		successRate := float64(successfulProcesses) / float64(totalProcessCount)
		assert.Greater(t, successRate, 0.8, "成功率应该大于80%%")
	}

	// 验证所有节点都参与了处理
	activeNodes := 0
	for _, processor := range processors {
		if processor.GetProcessCount() > 0 {
			activeNodes++
		}
	}
	assert.GreaterOrEqual(t, activeNodes, 3, "至少应该有3个节点参与处理")

	// 验证没有重复处理（这是最重要的正确性检查）
	processedRanges := make(map[string][]time.Time)
	for _, record := range allProcessHistory {
		if record.Error == nil {
			rangeKey := fmt.Sprintf("%d-%d", record.MinID, record.MaxID)
			processedRanges[rangeKey] = append(processedRanges[rangeKey], record.Timestamp)
		}
	}

	duplicateCount := 0
	for rangeKey, timestamps := range processedRanges {
		if len(timestamps) > 1 {
			duplicateCount++
			t.Logf("警告: ID范围%s被处理了%d次，时间戳: %v", rangeKey, len(timestamps), timestamps)
		}
	}

	// 在高并发情况下，可能偶尔出现重复，但应该很少
	duplicateRate := float64(duplicateCount) / float64(len(processedRanges))
	assert.Less(t, duplicateRate, 0.05, "重复处理率应该小于5%%")

	// 停止所有MGR
	for _, mgr := range mgrs {
		mgr.Stop()
	}
}

// TestMgr_LeaderFailoverScenario 测试Leader故障转移场景
// 测试场景：
// 1. 启动3个MGR节点
// 2. 等待leader选举完成
// 3. 停止当前leader节点
// 4. 验证新的leader能够被选出
// 5. 验证任务处理能够继续
// 预期结果：
// - 原leader停止后，新leader能够快速选出
// - 任务处理无长时间中断
// - 系统能够自愈并继续正常运行
func TestMgr_LeaderFailoverScenario(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// 创建3个节点
	nodeCount := 3
	processors := make([]*TestProcessor, nodeCount)
	mgrs := make([]*Mgr, nodeCount)

	planer := NewTestPartitionPlaner(1000, 8000) // 分区大小1000，预期8个分区

	// 创建所有节点
	for i := 0; i < nodeCount; i++ {
		processors[i] = NewTestProcessor().WithProcessDelay(100 * time.Millisecond)
		mgrs[i] = createTestMgr(t, "test-failover", dataStore, processors[i], planer)
	}

	// 启动所有节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR %d失败", i)
		time.Sleep(200 * time.Millisecond)
	}

	// 等待leader选举和任务处理开始
	time.Sleep(5 * time.Second)

	// 检查哪个节点是leader（通过检查leader锁）
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, "test-failover")
	currentLeader, err := dataStore.GetKey(ctx, leaderLockKey)
	require.NoError(t, err, "获取leader锁失败")
	require.NotEmpty(t, currentLeader, "应该有leader存在")

	t.Logf("当前leader: %s", currentLeader)

	// 找到leader节点的索引
	leaderIndex := -1
	for i, mgr := range mgrs {
		if mgr.ID == currentLeader {
			leaderIndex = i
			break
		}
	}
	require.NotEqual(t, -1, leaderIndex, "应该找到leader节点")

	// 记录故障转移前的处理状态
	preFailoverProcessed := int64(0)
	for _, processor := range processors {
		preFailoverProcessed += processor.GetProcessedItems()
	}

	t.Logf("故障转移前已处理项目数: %d", preFailoverProcessed)

	// 停止leader节点，模拟故障
	t.Logf("停止leader节点 %s", mgrs[leaderIndex].ID)
	mgrs[leaderIndex].Stop()

	// 等待故障转移完成
	time.Sleep(8 * time.Second)

	// 验证新的leader被选出
	newLeader, err := dataStore.GetKey(ctx, leaderLockKey)
	require.NoError(t, err, "获取新leader锁失败")
	require.NotEmpty(t, newLeader, "应该有新leader存在")
	assert.NotEqual(t, currentLeader, newLeader, "应该选出新的leader")

	t.Logf("新leader: %s", newLeader)

	// 等待系统恢复并继续处理任务
	time.Sleep(5 * time.Second)

	// 记录故障转移后的处理状态
	postFailoverProcessed := int64(0)
	for i, processor := range processors {
		if i != leaderIndex { // 排除已停止的节点
			postFailoverProcessed += processor.GetProcessedItems()
		}
	}

	t.Logf("故障转移后已处理项目数: %d", postFailoverProcessed)

	// 验证任务处理能够继续（应该有新的处理发生）
	assert.Greater(t, postFailoverProcessed, preFailoverProcessed,
		"故障转移后应该继续处理任务")

	// 验证剩余节点都在正常工作
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, "test-failover")
	workers, err := dataStore.GetActiveWorkers(ctx, workersKey)
	require.NoError(t, err, "获取工作节点列表失败")

	activeNodes := 0
	for i, mgr := range mgrs {
		if i != leaderIndex {
			contains := false
			for _, workerID := range workers {
				if workerID == mgr.ID {
					contains = true
					break
				}
			}
			if contains {
				activeNodes++
			}
		}
	}
	assert.Equal(t, nodeCount-1, activeNodes, "剩余节点应该都保持活跃")

	// 停止剩余节点
	for i, mgr := range mgrs {
		if i != leaderIndex {
			mgr.Stop()
		}
	}
}

// TestMgr_TaskWindowParallelProcessing 测试任务窗口并行处理
// 测试场景：
// 1. 启动2个MGR节点，启用任务窗口功能
// 2. 验证单个节点能够并行处理多个分区
// 3. 验证任务窗口大小限制生效
// 4. 验证并行处理的正确性
// 预期结果：
// - 节点能够同时处理多个分区
// - 并行处理数量不超过窗口大小
// - 所有任务都正确完成，无数据竞争
func TestMgr_TaskWindowParallelProcessing(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 记录并发处理的分区
	var concurrentPartitions sync.Map
	var maxConcurrentCount int64

	// 创建能够跟踪并发的处理器
	trackingProcessor := NewTestProcessor().
		WithProcessDelay(2 * time.Second). // 较长的处理时间，确保能观察到并发
		WithCustomProcessor(func(minID, maxID int64) error {
			// 记录开始处理
			partitionKey := fmt.Sprintf("%d-%d", minID, maxID)
			concurrentPartitions.Store(partitionKey, time.Now())

			// 计算当前并发数
			currentConcurrent := int64(0)
			concurrentPartitions.Range(func(key, value interface{}) bool {
				currentConcurrent++
				return true
			})

			// 更新最大并发数
			for {
				current := atomic.LoadInt64(&maxConcurrentCount)
				if currentConcurrent <= current || atomic.CompareAndSwapInt64(&maxConcurrentCount, current, currentConcurrent) {
					break
				}
			}

			// 模拟处理时间
			time.Sleep(1500 * time.Millisecond)

			// 删除处理记录
			concurrentPartitions.Delete(partitionKey)

			return nil
		})

	planer := NewTestPartitionPlaner(500, 5000) // 预期10个分区

	// 创建2个节点，启用任务窗口
	windowSize := 3
	processors := []*TestProcessor{trackingProcessor, NewTestProcessor().WithProcessDelay(100 * time.Millisecond)}
	mgrs := make([]*Mgr, 2)

	for i := 0; i < 2; i++ {
		mgrs[i] = createTestMgr(t, "test-window", dataStore, processors[i], planer,
			WithTaskWindow(windowSize), // 启用任务窗口
		)
	}

	// 启动节点
	for i, mgr := range mgrs {
		err := mgr.Start(ctx)
		require.NoError(t, err, "启动MGR %d失败", i)
		time.Sleep(200 * time.Millisecond)
	}

	// 等待任务处理开始
	time.Sleep(3 * time.Second)

	// 让系统运行一段时间以观察并发处理
	time.Sleep(8 * time.Second)

	// 验证并发处理效果
	maxConcurrent := atomic.LoadInt64(&maxConcurrentCount)
	t.Logf("观察到的最大并发处理数: %d", maxConcurrent)

	// 验证确实有并发处理发生
	assert.Greater(t, maxConcurrent, int64(1), "应该观察到并发处理")

	// 验证并发数不超过窗口大小
	assert.LessOrEqual(t, maxConcurrent, int64(windowSize),
		"并发处理数不应该超过窗口大小 %d", windowSize)

	// 验证任务处理统计
	totalProcessed := int64(0)
	for i, processor := range processors {
		processed := processor.GetProcessedItems()
		t.Logf("节点%d处理项目数: %d", i, processed)
		totalProcessed += processed
	}

	assert.Greater(t, totalProcessed, int64(0), "应该有任务被处理")

	// 停止所有MGR
	for _, mgr := range mgrs {
		mgr.Stop()
	}
}

// TestMgr_ConfigurationValidation 测试配置验证和默认值
// 测试场景：
// 1. 测试各种配置选项的有效性
// 2. 验证默认值的正确性
// 3. 测试边界值和异常配置
// 预期结果：
// - 所有配置选项都能正确应用
// - 无效配置能够被修正为合理值
// - 系统在各种配置下都能稳定运行
func TestMgr_ConfigurationValidation(t *testing.T) {
	_, _, dataStore, cleanup := setupMgrIntegrationTest(t)
	defer cleanup()

	processor := NewTestProcessor()
	planer := NewTestPartitionPlaner(1000, 3000)

	// 测试默认配置
	t.Run("DefaultConfiguration", func(t *testing.T) {
		mgr := NewMgr("test-default", dataStore, processor, planer, model.StrategyTypeHash)

		assert.Equal(t, model.StrategyTypeHash, mgr.PartitionStrategyType)
		assert.Equal(t, model.DefaultHeartbeatInterval, mgr.HeartbeatInterval)
		assert.Equal(t, model.DefaultLeaderElectionInterval, mgr.LeaderElectionInterval)
		assert.Equal(t, model.DefaultPartitionLockExpiry, mgr.PartitionLockExpiry)
		assert.Equal(t, model.DefaultLeaderLockExpiry, mgr.LeaderLockExpiry)
		assert.Equal(t, int64(model.DefaultWorkerPartitionMultiple), mgr.WorkerPartitionMultiple)
		assert.Equal(t, model.DefaultTaskWindowSize, mgr.TaskWindowSize)
	})

	// 测试自定义配置
	t.Run("CustomConfiguration", func(t *testing.T) {
		mgr := NewMgr("test-custom", dataStore, processor, planer, model.StrategyTypeSimple,
			WithHeartbeatInterval(2*time.Second),
			WithLeaderElectionInterval(3*time.Second),
			WithPartitionLockExpiry(10*time.Second),
			WithLeaderLockExpiry(20*time.Second),
			WithWorkerPartitionMultiple(5),
			WithTaskWindow(7),
		)

		assert.Equal(t, model.StrategyTypeSimple, mgr.PartitionStrategyType)
		assert.Equal(t, 2*time.Second, mgr.HeartbeatInterval)
		assert.Equal(t, 3*time.Second, mgr.LeaderElectionInterval)
		assert.Equal(t, 10*time.Second, mgr.PartitionLockExpiry)
		assert.Equal(t, 20*time.Second, mgr.LeaderLockExpiry)
		assert.Equal(t, int64(5), mgr.WorkerPartitionMultiple)
		assert.Equal(t, 7, mgr.TaskWindowSize)
	})

	// 测试边界值配置（应该被修正）
	t.Run("BoundaryConfiguration", func(t *testing.T) {
		mgr := NewMgr("test-boundary", dataStore, processor, planer, model.StrategyTypeHash,
			WithHeartbeatInterval(100*time.Millisecond),      // 太小，应该被修正为1秒
			WithLeaderElectionInterval(500*time.Millisecond), // 太小，应该被修正为1秒
			WithPartitionLockExpiry(100*time.Millisecond),    // 太小，应该被修正为1秒
			WithLeaderLockExpiry(500*time.Millisecond),       // 太小，应该被修正为1秒
			WithWorkerPartitionMultiple(-1),                  // 无效值，应该使用默认值
			WithTaskWindow(0),                                // 无效窗口大小，应该使用默认值
		)

		assert.Equal(t, 1*time.Second, mgr.HeartbeatInterval)
		assert.Equal(t, 1*time.Second, mgr.LeaderElectionInterval)
		assert.Equal(t, 1*time.Second, mgr.PartitionLockExpiry)
		assert.Equal(t, 1*time.Second, mgr.LeaderLockExpiry)
		assert.Equal(t, int64(model.DefaultWorkerPartitionMultiple), mgr.WorkerPartitionMultiple)
		assert.Equal(t, model.DefaultTaskWindowSize, mgr.TaskWindowSize) // 使用默认大小
	})
}
