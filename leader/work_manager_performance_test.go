package leader

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"runtime"
	"sync"
	"testing"
	"time"
)

// slowPartitionStrategy 模拟真实Redis操作的网络和序列化延迟
// 用于准确测试增量检测相对于完整扫描的性能优势
type slowPartitionStrategy struct {
	*test_utils.MockPartitionStrategy
	networkLatency       time.Duration // 网络往返延迟
	serializationBase    time.Duration // 序列化基础开销
	serializationPerItem time.Duration // 每个分区的序列化开销
}

// newSlowPartitionStrategy 创建模拟真实Redis延迟的分区策略
func newSlowPartitionStrategy(networkLatency, serializationBase, serializationPerItem time.Duration) *slowPartitionStrategy {
	return &slowPartitionStrategy{
		MockPartitionStrategy: test_utils.NewMockPartitionStrategy(),
		networkLatency:        networkLatency,
		serializationBase:     serializationBase,
		serializationPerItem:  serializationPerItem,
	}
}

// GetAllPartitions 模拟Redis的SCAN/KEYS操作 - 延迟随分区数量线性增长
func (s *slowPartitionStrategy) GetAllActivePartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	// 网络往返延迟（TCP连接 + Redis命令传输）
	time.Sleep(s.networkLatency)

	// 获取分区数据以计算序列化延迟
	partitions, err := s.MockPartitionStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		return nil, err
	}

	partitionCount := len(partitions)
	if partitionCount > 0 {
		// 序列化基础开销（Redis内部处理）
		time.Sleep(s.serializationBase)

		// 每个分区的序列化和网络传输延迟
		// 这反映了Redis需要序列化每个分区数据并通过网络发送的真实成本
		totalSerializationDelay := time.Duration(partitionCount) * s.serializationPerItem
		time.Sleep(totalSerializationDelay)
	}

	return partitions, nil
}

// GetPartition 模拟Redis的GET操作 - 固定延迟，不随总分区数增长
func (s *slowPartitionStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	// 单个分区查询：只有网络往返延迟 + 最小序列化开销
	// 这是增量检测的核心优势：不需要传输大量无关数据
	time.Sleep(s.networkLatency + s.serializationBase)
	return s.MockPartitionStrategy.GetPartition(ctx, partitionID)
}

// AcquirePartition 模拟分区获取操作
func (s *slowPartitionStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	// 分区获取操作：网络延迟 + 基础序列化开销
	time.Sleep(s.networkLatency + s.serializationBase)
	return s.MockPartitionStrategy.AcquirePartition(ctx, partitionID, workerID, options)
}

// SetPartitions 设置分区数据（用于测试）
func (s *slowPartitionStrategy) SetPartitions(partitions map[int]*model.PartitionInfo) {
	s.MockPartitionStrategy.SetPartitions(partitions)
}

// TestIncrementalGapDetection_PerformanceComparison 测试增量检测的性能优势
func TestIncrementalGapDetection_PerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试（使用 -short 标志）")
	}

	config := PartitionAssignerConfig{
		Namespace:               "test",
		WorkerPartitionMultiple: 2,
	}

	// 使用更真实的Redis延迟模拟
	// 网络延迟: 1ms (局域网内Redis的典型往返时间)
	// 序列化基础开销: 100µs (Redis内部处理和基础序列化)
	// 每分区序列化开销: 5µs (每个分区对象的序列化和传输成本)
	slowStrategy := newSlowPartitionStrategy(
		1*time.Millisecond,   // networkLatency
		100*time.Microsecond, // serializationBase
		5*time.Microsecond,   // serializationPerItem
	)

	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 1000,
		nextMaxID:              50000,
	}
	logger := test_utils.NewMockLogger(false)

	partitionMgr := NewPartitionAssigner(config, slowStrategy, logger, mockPlaner)
	ctx := context.Background()

	// 创建大量分区以测试性能差异 - 增加到5000个让效果更明显
	const numPartitions = 5000
	partitions := make(map[int]*model.PartitionInfo)

	for i := 1; i <= numPartitions; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i,
			MinID:       int64((i-1)*1000 + 1),
			MaxID:       int64(i * 1000),
			Status:      model.StatusCompleted,
		}
	}
	slowStrategy.SetPartitions(partitions)

	// 第一次检测：完整检测，建立缓存
	t.Log("=== 建立初始缓存（完整检测） ===")
	start := time.Now()
	_, err := partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, numPartitions)
	fullDetectionTime := time.Since(start)
	if err != nil {
		t.Fatalf("初始完整检测失败: %v", err)
	}
	t.Logf("完整检测（%d个分区）耗时: %v", numPartitions, fullDetectionTime)

	// 添加一个新分区，触发增量检测
	newPartitionID := numPartitions + 1
	partitions[newPartitionID] = &model.PartitionInfo{
		PartitionID: newPartitionID,
		MinID:       int64(newPartitionID-1)*1000 + 1,
		MaxID:       int64(newPartitionID * 1000),
		Status:      model.StatusCompleted,
	}
	slowStrategy.SetPartitions(partitions)

	// 增量检测
	t.Log("=== 增量检测（添加1个新分区） ===")
	start = time.Now()
	_, err = partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, newPartitionID)
	incrementalDetectionTime := time.Since(start)
	if err != nil {
		t.Fatalf("增量检测失败: %v", err)
	}
	t.Logf("增量检测（添加1个新分区）耗时: %v", incrementalDetectionTime)

	// 性能对比验证
	performanceImprovement := float64(fullDetectionTime) / float64(incrementalDetectionTime)
	t.Logf("性能提升: %.2fx", performanceImprovement)

	// 验证增量检测应该比完整检测快很多（至少2倍）
	// 在有延迟的模拟环境中，5000个分区的完整检测应该明显比单个分区的增量检测慢
	minExpectedImprovement := 2.0
	if performanceImprovement < minExpectedImprovement {
		t.Errorf("增量检测性能提升不足: 实际%.2fx < 期望%.2fx (完整检测=%v, 增量检测=%v)",
			performanceImprovement, minExpectedImprovement, fullDetectionTime, incrementalDetectionTime)
	}

	// 清除缓存，强制完整检测进行对比
	t.Log("=== 清除缓存后的完整检测 ===")
	partitionMgr.knownPartitionRanges = nil // 清除缓存
	start = time.Now()
	_, err = partitionMgr.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, newPartitionID)
	fullDetectionTime2 := time.Since(start)
	if err != nil {
		t.Fatalf("清除缓存后的完整检测失败: %v", err)
	}
	t.Logf("清除缓存后完整检测（%d个分区）耗时: %v", newPartitionID, fullDetectionTime2)

	// 验证缓存失效后性能回到完整检测水平
	// 第二次完整检测应该和第一次时间相近（因为分区数量只多了1个）
	timeDifference := float64(fullDetectionTime2) / float64(fullDetectionTime)
	t.Logf("两次完整检测时间比较: %.2fx", timeDifference)

	// 第二次完整检测应该明显比增量检测慢
	improvement2 := float64(fullDetectionTime2) / float64(incrementalDetectionTime)
	if improvement2 < minExpectedImprovement {
		t.Logf("警告：清除缓存后的完整检测时间 %v 与增量检测时间 %v 差异较小（%.2fx）",
			fullDetectionTime2, incrementalDetectionTime, improvement2)
	}

	// 验证结果正确性：增量检测和完整检测应该产生相同的结果
	t.Log("=== 验证结果一致性 ===")

	// 重新设置相同的测试环境
	partitionMgr1 := NewPartitionAssigner(config, test_utils.NewMockPartitionStrategy(), logger, mockPlaner)
	partitionMgr2 := NewPartitionAssigner(config, test_utils.NewMockPartitionStrategy(), logger, mockPlaner)

	// 为两个管理器设置相同的分区状态
	testPartitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, MinID: 1, MaxID: 1000},
		2: {PartitionID: 2, MinID: 2001, MaxID: 3000}, // 故意留缺口 1001-2000
	}

	mockStrategy1 := test_utils.NewMockPartitionStrategy()
	mockStrategy2 := test_utils.NewMockPartitionStrategy()
	mockStrategy1.SetPartitions(testPartitions)
	mockStrategy2.SetPartitions(testPartitions)

	partitionMgr1.strategy = mockStrategy1
	partitionMgr2.strategy = mockStrategy2

	// 使用完整检测
	fullResult, err := partitionMgr1.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 2)
	if err != nil {
		t.Fatalf("完整检测失败: %v", err)
	}

	// 建立增量检测的缓存状态
	_, err = partitionMgr2.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 1)
	if err != nil {
		t.Fatalf("建立增量缓存失败: %v", err)
	}

	// 添加新分区并使用增量检测
	testPartitions[2] = &model.PartitionInfo{PartitionID: 2, MinID: 2001, MaxID: 3000}
	mockStrategy2.SetPartitions(testPartitions)

	incrementalResult, err := partitionMgr2.DetectAndCreateGapPartitions(ctx, []string{"worker1"}, 2)
	if err != nil {
		t.Fatalf("增量检测失败: %v", err)
	}

	// 比较结果
	if len(fullResult) != len(incrementalResult) {
		t.Errorf("完整检测和增量检测结果数量不同：完整=%d，增量=%d",
			len(fullResult), len(incrementalResult))
	}

	t.Logf("✅ 性能测试完成：增量检测比完整检测快 %.2fx", performanceImprovement)
}

// TestPartitionAssignerScalingPerformance 测试分区算法在大规模扩缩容时的性能
func TestPartitionAssignerScalingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试（使用 -short 标志）")
	}

	mockStore := test_utils.NewMockDataStore()
	mockPlaner := &mockPartitionPlaner{nextMaxID: 10000}
	mockStrategy := test_utils.NewMockPartitionStrategy()

	// 创建工作管理器和分区分配器
	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 "leader",
		Namespace:              "perf-test",
		DataStore:              mockStore,
		Logger:                 test_utils.NewMockLogger(true), // 启用详细日志
		ValidHeartbeatDuration: 30 * time.Second,
	})

	partitionMgr := NewPartitionAssigner(
		PartitionAssignerConfig{
			Namespace:               "perf-test",
			WorkerPartitionMultiple: 2,
		},
		mockStrategy,
		test_utils.NewMockLogger(true),
		mockPlaner,
	)

	ctx := context.Background()
	now := time.Now()

	// 性能测试场景：模拟K8s集群扩缩容
	testScenarios := []struct {
		name        string
		workerCount int
		description string
	}{
		{"小规模", 10, "模拟小型K8s集群"},
		{"中规模", 50, "模拟中型K8s集群"},
		{"大规模", 100, "模拟大型K8s集群"},
		{"超大规模", 500, "模拟超大型K8s集群"},
	}

	var results []string

	for _, scenario := range testScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// 清理之前的测试数据
			mockStore.Heartbeats = make(map[string]string)
			mockStrategy.Partitions = make(map[int]*model.PartitionInfo)

			// 模拟worker节点注册（扩容）
			setupStart := time.Now()
			for i := 0; i < scenario.workerCount; i++ {
				workerID := fmt.Sprintf("worker-%d", i)
				heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "perf-test", workerID)
				mockStore.Heartbeats[heartbeatKey] = now.Format(time.RFC3339)
			}
			setupDuration := time.Since(setupStart)

			// 预创建一些分区来模拟已有工作负载
			for i := 0; i < scenario.workerCount*10; i++ {
				partition := &model.PartitionInfo{
					PartitionID: i,
					MinID:       int64(i * 1000),
					MaxID:       int64((i + 1) * 1000),
					Status:      model.StatusCompleted,
					WorkerID:    fmt.Sprintf("worker-%d", i%scenario.workerCount),
				}
				mockStrategy.Partitions[i] = partition
			}

			// 测试分区分配性能
			allocateStart := time.Now()

			// 执行多次分配来模拟实际运行情况
			for round := 0; round < 5; round++ {
				workManager.tryAllocatePartitions(ctx, partitionMgr)
				// tryAllocatePartitions 方法内部处理错误，这里不需要检查返回值
			}

			allocateDuration := time.Since(allocateStart)

			// 测试缺口检测性能（模拟重新平衡）
			gapDetectionStart := time.Now()

			// 获取活跃worker
			activeWorkers, err := workManager.getActiveWorkers(ctx)
			if err != nil {
				t.Fatalf("获取活跃worker失败: %v", err)
			}

			// 执行缺口检测
			_, err = partitionMgr.DetectAndCreateGapPartitions(ctx, activeWorkers, len(mockStrategy.Partitions))
			if err != nil {
				t.Errorf("缺口检测失败: %v", err)
			}

			gapDetectionDuration := time.Since(gapDetectionStart)

			// 模拟缩容：移除一半worker
			scaleDownStart := time.Now()
			removedWorkers := 0
			for key := range mockStore.Heartbeats {
				if removedWorkers >= scenario.workerCount/2 {
					break
				}
				delete(mockStore.Heartbeats, key)
				removedWorkers++
			}

			// 再次执行分区分配（处理缩容）
			workManager.tryAllocatePartitions(ctx, partitionMgr)
			// tryAllocatePartitions 方法内部处理错误

			scaleDownDuration := time.Since(scaleDownStart)

			// 记录性能数据
			result := fmt.Sprintf("%s: Workers=%d, Setup=%v, Allocate=%v, GapDetection=%v, ScaleDown=%v",
				scenario.description, scenario.workerCount,
				setupDuration, allocateDuration, gapDetectionDuration, scaleDownDuration)

			results = append(results, result)
			t.Log(result)

			// 性能断言：确保在合理时间内完成
			maxAllowedDuration := time.Duration(scenario.workerCount) * time.Millisecond * 10 // 每个worker最多10ms

			if allocateDuration > maxAllowedDuration {
				t.Errorf("分区分配耗时过长: %v > %v (workers: %d)",
					allocateDuration, maxAllowedDuration, scenario.workerCount)
			}

			if gapDetectionDuration > maxAllowedDuration*2 { // 缺口检测允许更长时间
				t.Errorf("缺口检测耗时过长: %v > %v (workers: %d)",
					gapDetectionDuration, maxAllowedDuration*2, scenario.workerCount)
			}

			// 验证分区数据正确性
			finalPartitions, err := mockStrategy.GetAllActivePartitions(ctx)
			if err != nil {
				t.Errorf("获取最终分区失败: %v", err)
			}

			if len(finalPartitions) == 0 {
				t.Error("扩缩容后没有创建任何分区")
			}

			t.Logf("扩缩容完成: 最终分区数=%d, 剩余worker数=%d",
				len(finalPartitions), len(mockStore.Heartbeats))
		})
	}

	// 输出性能对比总结
	t.Log("\n=== 分区算法扩缩容性能测试总结 ===")
	for _, result := range results {
		t.Log(result)
	}
}

// TestWorkManagerMemoryLeak 测试WorkManager的内存泄漏风险
func TestWorkManagerMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过内存泄漏测试（使用 -short 标志）")
	}

	// 创建测试环境
	mockStore := test_utils.NewMockDataStore()
	mockPlaner := &mockPartitionPlaner{nextMaxID: 1000}
	mockStrategy := test_utils.NewMockPartitionStrategy()

	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 "leak-test",
		Namespace:              "memory-test",
		DataStore:              mockStore,
		Logger:                 test_utils.NewMockLogger(false), // 关闭详细日志减少噪音
		ValidHeartbeatDuration: 1 * time.Second,                 // 短超时时间加速测试
		AllocationInterval:     50 * time.Millisecond,           // 快速循环
	})

	partitionMgr := NewPartitionAssigner(
		PartitionAssignerConfig{
			Namespace:               "memory-test",
			WorkerPartitionMultiple: 1,
		},
		mockStrategy,
		test_utils.NewMockLogger(false),
		mockPlaner,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 子测试1: Goroutine泄漏测试
	t.Run("GoroutineLeakTest", func(t *testing.T) {
		initialGoroutines := runtime.NumGoroutine()
		t.Logf("初始goroutine数量: %d", initialGoroutines)

		// 启动多个allocation循环来模拟goroutine泄漏
		var wg sync.WaitGroup
		leaderContexts := make([]context.CancelFunc, 0)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			leaderCtx, leaderCancel := context.WithCancel(context.Background())
			leaderContexts = append(leaderContexts, leaderCancel)

			go func(iteration int) {
				defer wg.Done()

				// 添加心跳数据
				workerID := fmt.Sprintf("worker-%d", iteration)
				heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "memory-test", workerID)
				mockStore.Heartbeats[heartbeatKey] = time.Now().Format(time.RFC3339)

				// 运行分区分配循环
				err := workManager.RunPartitionAllocationLoop(ctx, leaderCtx, partitionMgr)
				if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
					t.Errorf("分区分配循环 %d 异常退出: %v", iteration, err)
				}
			}(i)

			// 模拟快速的leader切换
			time.Sleep(20 * time.Millisecond)
			if i%3 == 0 { // 每3个就取消一个，模拟leader切换
				leaderCancel()
			}
		}

		// 等待一段时间让goroutine运行
		time.Sleep(500 * time.Millisecond)

		// 取消剩余的leader contexts
		for _, cancel := range leaderContexts {
			cancel()
		}

		// 等待所有goroutine完成
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			t.Log("所有goroutine正常完成")
		case <-time.After(1 * time.Second):
			t.Error("等待goroutine完成超时，可能存在goroutine泄漏")
		}

		// 强制GC并检查goroutine数量
		runtime.GC()
		runtime.GC() // 执行两次确保清理完成
		time.Sleep(100 * time.Millisecond)

		finalGoroutines := runtime.NumGoroutine()
		t.Logf("最终goroutine数量: %d", finalGoroutines)

		// 允许一些合理的goroutine增长，但不应该有显著泄漏
		if finalGoroutines > initialGoroutines+5 {
			t.Errorf("可能存在goroutine泄漏: 初始=%d, 最终=%d, 增长=%d",
				initialGoroutines, finalGoroutines, finalGoroutines-initialGoroutines)
		}
	})

	// 子测试2: 内存占用增长测试
	t.Run("MemoryGrowthTest", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		initialAlloc := m1.Alloc
		t.Logf("初始内存分配: %d KB", initialAlloc/1024)

		// 模拟长期运行：大量worker注册/注销和分区操作
		for cycle := 0; cycle < 100; cycle++ {
			// 模拟worker节点动态变化
			for i := 0; i < 20; i++ {
				workerID := fmt.Sprintf("temp-worker-%d-%d", cycle, i)
				heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, "memory-test", workerID)

				// 添加心跳
				mockStore.Heartbeats[heartbeatKey] = time.Now().Format(time.RFC3339)

				// 立即获取活跃worker（触发内部缓存更新）
				_, err := workManager.getActiveWorkers(context.Background())
				if err != nil {
					t.Errorf("Cycle %d: 获取活跃worker失败: %v", cycle, err)
				}

				// 创建分区（模拟分区范围缓存增长）
				partition := &model.PartitionInfo{
					PartitionID: cycle*20 + i,
					MinID:       int64((cycle*20 + i) * 1000),
					MaxID:       int64((cycle*20 + i + 1) * 1000),
					Status:      model.StatusPending,
					WorkerID:    workerID,
				}
				mockStrategy.Partitions[cycle*20+i] = partition
			}

			// 执行分区分配（触发knownPartitionRanges缓存增长）
			workManager.tryAllocatePartitions(context.Background(), partitionMgr)

			// 模拟一些worker下线（但心跳数据可能残留）
			count := 0
			for key := range mockStore.Heartbeats {
				if count >= 10 { // 只删除一半
					break
				}
				delete(mockStore.Heartbeats, key)
				count++
			}

			// 定期检查内存增长
			if cycle%20 == 0 {
				runtime.GC()
				runtime.ReadMemStats(&m2)
				currentAlloc := m2.Alloc
				growth := currentAlloc - initialAlloc

				t.Logf("Cycle %d: 当前内存=%d KB, 增长=%d KB",
					cycle, currentAlloc/1024, growth/1024)

				// 如果内存增长超过合理范围，报告潜在泄漏
				if growth > 10*1024*1024 { // 超过10MB增长
					t.Errorf("Cycle %d: 内存增长过大: %d KB，可能存在内存泄漏", cycle, growth/1024)
				}
			}
		}

		// 最终内存检查
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m2)

		finalAlloc := m2.Alloc
		totalGrowth := finalAlloc - initialAlloc

		t.Logf("内存测试完成: 初始=%d KB, 最终=%d KB, 总增长=%d KB",
			initialAlloc/1024, finalAlloc/1024, totalGrowth/1024)

		// 检查是否存在显著内存泄漏
		if totalGrowth > 20*1024*1024 { // 超过20MB
			t.Errorf("检测到可能的内存泄漏: 总增长 %d KB", totalGrowth/1024)
		}
	})

	// 子测试3: 分区缓存无限增长测试
	t.Run("PartitionCacheGrowthTest", func(t *testing.T) {
		// 清理现有数据
		mockStrategy.Partitions = make(map[int]*model.PartitionInfo)

		// 检查分区范围缓存的初始状态
		initialCacheSize := len(partitionMgr.knownPartitionRanges)
		t.Logf("初始分区范围缓存大小: %d", initialCacheSize)

		// 模拟大量分区创建
		for i := 0; i < 1000; i++ {
			partition := &model.PartitionInfo{
				PartitionID: i,
				MinID:       int64(i * 1000),
				MaxID:       int64((i + 1) * 1000),
				Status:      model.StatusCompleted,
				WorkerID:    fmt.Sprintf("worker-%d", i%10),
			}
			mockStrategy.Partitions[i] = partition
		}

		// 执行多次缺口检测，每次都会更新缓存
		activeWorkers := []string{"worker-1", "worker-2"}
		for round := 0; round < 10; round++ {
			_, err := partitionMgr.DetectAndCreateGapPartitions(context.Background(), activeWorkers, len(mockStrategy.Partitions))
			if err != nil {
				t.Errorf("Round %d: 缺口检测失败: %v", round, err)
			}

			currentCacheSize := len(partitionMgr.knownPartitionRanges)
			t.Logf("Round %d: 分区范围缓存大小: %d", round, currentCacheSize)

			// 检查缓存是否在无控制地增长
			if currentCacheSize > 1500 { // 超过合理大小
				t.Errorf("Round %d: 分区范围缓存过大: %d，可能存在无限增长问题", round, currentCacheSize)
			}
		}

		finalCacheSize := len(partitionMgr.knownPartitionRanges)
		t.Logf("最终分区范围缓存大小: %d", finalCacheSize)

		// 验证缓存大小是否合理
		if finalCacheSize > len(mockStrategy.Partitions)*2 {
			t.Errorf("分区范围缓存异常增长: 实际分区=%d, 缓存大小=%d",
				len(mockStrategy.Partitions), finalCacheSize)
		}
	})
}
