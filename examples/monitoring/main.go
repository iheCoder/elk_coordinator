package main

import (
	"context"
	"elk_coordinator"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// MockProcessor 增强的模拟处理器，用于演示监控功能，包含动态负载变化
type MockProcessor struct {
	processDelay    time.Duration
	baseFailureRate float64
	processedTasks  int64
	dynamicDelay    bool
	rand            *rand.Rand
	startTime       time.Time
	cyclePhase      int // 用于模拟不同的负载周期
}

func NewMockProcessor(delay time.Duration) *MockProcessor {
	return &MockProcessor{
		processDelay:    delay,
		baseFailureRate: 0.08, // 基础8% 失败率，稍微降低
		dynamicDelay:    true,
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		startTime:       time.Now(),
		cyclePhase:      0,
	}
}

func (p *MockProcessor) Process(ctx context.Context, minID, maxID int64, options map[string]interface{}) (int64, error) {
	p.processedTasks++

	// 计算运行时间，用于模拟不同的负载周期
	runningTime := time.Since(p.startTime)
	cycleSeconds := int(runningTime.Seconds()) % 300 // 5分钟一个周期

	// 动态失败率：根据时间周期变化
	currentFailureRate := p.baseFailureRate
	switch {
	case cycleSeconds < 60: // 第一分钟：低错误率
		currentFailureRate = 0.05
	case cycleSeconds < 120: // 第二分钟：正常错误率
		currentFailureRate = 0.1
	case cycleSeconds < 180: // 第三分钟：高错误率时期
		currentFailureRate = 0.25
	case cycleSeconds < 240: // 第四分钟：恢复期
		currentFailureRate = 0.15
	default: // 第五分钟：稳定期
		currentFailureRate = 0.08
	}

	// 动态调整处理时间，模拟真实的负载变化
	baseDelay := p.processDelay
	var actualDelay time.Duration

	// 根据周期和任务数量决定处理时间
	switch {
	case cycleSeconds < 60: // 第一分钟：快速处理期
		actualDelay = time.Duration(float64(baseDelay) * (0.8 + p.rand.Float64()*0.6)) // 0.8-1.4倍
		if p.processedTasks%5 == 0 {
			actualDelay = baseDelay + time.Duration(p.rand.Intn(800))*time.Millisecond
		}
	case cycleSeconds < 120: // 第二分钟：正常负载
		actualDelay = baseDelay + time.Duration(p.rand.Intn(1000))*time.Millisecond // 1-2秒
		if p.processedTasks%10 == 0 {
			actualDelay = baseDelay + time.Duration(p.rand.Intn(2000))*time.Millisecond
		}
	case cycleSeconds < 180: // 第三分钟：高负载期
		actualDelay = time.Duration(float64(baseDelay) * (2.0 + p.rand.Float64()*1.5)) // 2-3.5倍
		if p.processedTasks%3 == 0 {
			actualDelay = baseDelay + time.Duration(p.rand.Intn(4000))*time.Millisecond
			fmt.Printf("🐌 高负载任务 ID 范围 [%d, %d] (预计耗时: %v)\n", minID, maxID, actualDelay)
		}
	case cycleSeconds < 240: // 第四分钟：波动期
		if p.processedTasks%2 == 0 {
			actualDelay = time.Duration(float64(baseDelay) * (0.5 + p.rand.Float64()*0.5)) // 0.5-1倍
		} else {
			actualDelay = baseDelay + time.Duration(p.rand.Intn(3000))*time.Millisecond
		}
	default: // 第五分钟：稳定期
		actualDelay = baseDelay + time.Duration(p.rand.Intn(1500))*time.Millisecond // 1-2.5秒
	}

	// 增加一些随机的突发延迟
	if p.rand.Float64() < 0.05 { // 5%概率出现突发延迟
		actualDelay += time.Duration(p.rand.Intn(5000)) * time.Millisecond
		fmt.Printf("💥 突发延迟任务 ID 范围 [%d, %d] (耗时: %v)\n", minID, maxID, actualDelay)
	}

	// 模拟失败
	if p.rand.Float64() < currentFailureRate {
		failureType := ""
		switch {
		case currentFailureRate > 0.2:
			failureType = "高负载期失败"
		case p.rand.Float64() < 0.3:
			failureType = "网络超时"
		case p.rand.Float64() < 0.6:
			failureType = "资源不足"
		default:
			failureType = "处理异常"
		}
		fmt.Printf("❌ %s ID 范围 [%d, %d] (当前失败率: %.1f%%)\n",
			failureType, minID, maxID, currentFailureRate*100)
		return 0, fmt.Errorf("%s - 周期: %ds", failureType, cycleSeconds)
	}

	// 模拟处理延迟
	select {
	case <-time.After(actualDelay):
		processedCount := maxID - minID + 1

		// 根据处理时间和周期显示不同的状态
		status := "✅"
		phaseDesc := ""
		switch {
		case cycleSeconds < 60:
			phaseDesc = "快速期"
			status = "⚡"
		case cycleSeconds < 120:
			phaseDesc = "正常期"
			status = "✅"
		case cycleSeconds < 180:
			phaseDesc = "高负载期"
			if actualDelay > p.processDelay*2 {
				status = "🐌"
			} else {
				status = "🔥"
			}
		case cycleSeconds < 240:
			phaseDesc = "波动期"
			status = "📊"
		default:
			phaseDesc = "稳定期"
			status = "🔄"
		}

		fmt.Printf("%s [%s] 处理完成 ID 范围 [%d, %d], 处理了 %d 个项目 (耗时: %v, 累计: %d)\n",
			status, phaseDesc, minID, maxID, processedCount, actualDelay, p.processedTasks)
		return processedCount, nil
	case <-ctx.Done():
		fmt.Printf("⏸️  处理被取消 ID 范围 [%d, %d]\n", minID, maxID)
		return 0, ctx.Err()
	}
}

// MockPlaner 动态分区规划器，模拟分区大小的变化
type MockPlaner struct {
	callCount int64
	rand      *rand.Rand
	startTime time.Time
}

func NewMockPlaner() *MockPlaner {
	return &MockPlaner{
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		startTime: time.Now(),
	}
}

func (p *MockPlaner) PartitionSize(ctx context.Context) (int64, error) {
	p.callCount++

	// 根据运行时间模拟不同的分区策略
	runningTime := time.Since(p.startTime)
	cycleSeconds := int(runningTime.Seconds()) % 300 // 5分钟一个周期

	var size int64
	var mode string

	switch {
	case cycleSeconds < 60: // 第一分钟：小分区，快速处理
		size = int64(15 + p.rand.Intn(25)) // 15-40
		mode = "小分区快速模式"
	case cycleSeconds < 120: // 第二分钟：正常分区
		size = int64(50 + p.rand.Intn(50)) // 50-100
		mode = "正常分区模式"
	case cycleSeconds < 180: // 第三分钟：大分区，批量处理
		size = int64(150 + p.rand.Intn(200)) // 150-350
		mode = "大分区批量模式"
	case cycleSeconds < 240: // 第四分钟：混合模式
		if p.callCount%3 == 0 {
			size = int64(200 + p.rand.Intn(100)) // 大分区
			mode = "混合模式-大分区"
		} else {
			size = int64(20 + p.rand.Intn(30)) // 小分区
			mode = "混合模式-小分区"
		}
	default: // 第五分钟：稳定模式
		size = int64(80 + p.rand.Intn(40)) // 80-120
		mode = "稳定模式"
	}

	// 添加一些随机的特殊情况
	switch {
	case p.callCount%20 == 0: // 每20次有一次超大分区
		size = int64(400 + p.rand.Intn(200))
		mode = "超大分区模式"
		fmt.Printf("📊 %s: %d 项目/分区 (特殊批处理)\n", mode, size)
	case p.callCount%15 == 0: // 每15次有一次微分区
		size = int64(5 + p.rand.Intn(10))
		mode = "微分区模式"
		fmt.Printf("📊 %s: %d 项目/分区 (精细处理)\n", mode, size)
	case cycleSeconds%60 == 0: // 每分钟开始时输出模式
		fmt.Printf("📊 %s: %d 项目/分区 (周期: %ds)\n", mode, size, cycleSeconds)
	}

	return size, nil
}

func (p *MockPlaner) GetNextMaxID(ctx context.Context, startID int64, rangeSize int64) (int64, error) {
	// 总是返回一个比当前更大的ID，确保有持续的任务要处理
	// 这样系统就会持续创建新分区，产生监控数据
	nextMaxID := startID + rangeSize

	// 每次调用都产生新的数据，模拟真实的数据流
	// 这确保了系统始终有新任务要处理
	//
	// 为了模拟真实场景，我们让每次调用都返回更大的ID
	// 这样coordinator就会不断创建新分区来处理
	p.callCount++

	// 添加一些随机增量，模拟数据流的不均匀性
	randomIncrement := int64(p.rand.Intn(100) + 50) // 50-150的随机增量
	nextMaxID += randomIncrement

	// 每10次调用产生一个较大的跳跃，模拟批量数据到达
	if p.callCount%10 == 0 {
		nextMaxID += int64(p.rand.Intn(500) + 200) // 额外200-700的增量
	}

	return nextMaxID, nil
}

func main() {
	fmt.Println("=== ELK Coordinator 监控示例 ===")
	fmt.Println("启动分布式任务管理器，包含完整的监控功能")
	fmt.Println()

	// 创建 Redis 数据存储
	fmt.Println("1. 初始化 Redis 数据存储...")
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// 创建 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	// 创建数据存储选项
	options := &data.Options{
		KeyPrefix:     "elk:",
		DefaultExpiry: 2 * time.Hour,
		MaxRetries:    3,                     // 增加重试次数
		RetryDelay:    50 * time.Millisecond, // 较短的初始重试延迟
		MaxRetryDelay: 3 * time.Second,       // 最大重试延迟
	}

	dataStore := data.NewRedisDataStore(rdb, options)

	// 测试 Redis 连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testKey := "elk:test:connection"
	if err := dataStore.SetHeartbeat(ctx, testKey, "test"); err != nil {
		log.Fatalf("Redis 连接失败: %v\n请确保 Redis 服务器在 %s 上运行", err, redisAddr)
	}
	fmt.Printf("✓ Redis 连接成功 (%s)\n", redisAddr)

	// 创建处理器和规划器
	fmt.Println("2. 创建任务处理器和分区规划器...")
	processor := NewMockProcessor(1200 * time.Millisecond) // 基础处理时间1.2秒，便于观察指标变化
	planer := NewMockPlaner()                              // 使用动态分区规划器
	fmt.Println("✓ 增强型模拟组件创建完成 - 包含5分钟周期性负载变化")

	// 创建管理器
	fmt.Println("3. 创建 ELK Coordinator 管理器...")
	namespace := "monitoring-demo"
	mgr := elk_coordinator.NewMgr(
		namespace,
		dataStore,
		processor,
		planer,
		model.StrategyTypeHash, // 使用Hash分区策略，展示Hash策略的监控指标
		// 配置监控选项
		elk_coordinator.WithMetricsEnabled(true),
		elk_coordinator.WithMetricsAddr(":8080"), // 监控服务端口
		// 为了演示效果，缩短分配间隔到20秒
		elk_coordinator.WithAllocationInterval(20*time.Second),
		// 增加锁过期时间，避免心跳超时
		elk_coordinator.WithPartitionLockExpiry(5*time.Minute),
		elk_coordinator.WithLeaderLockExpiry(2*time.Minute),
		elk_coordinator.WithHeartbeatInterval(30*time.Second), // 增加心跳间隔
	)
	fmt.Printf("✓ 管理器创建完成 (命名空间: %s)\n", namespace)

	// 启动管理器
	fmt.Println("4. 启动管理器...")
	if err := mgr.Start(context.Background()); err != nil {
		log.Fatalf("启动管理器失败: %v", err)
	}
	fmt.Println("✓ 管理器启动成功")

	// 输出监控信息
	fmt.Println()
	fmt.Println("=== 监控信息 ===")
	fmt.Println("监控服务已启动在: http://localhost:8080")
	fmt.Println("访问以下端点查看监控数据:")
	fmt.Println("  • Prometheus 指标: http://localhost:8080/metrics")
	fmt.Println("  • 健康检查: http://localhost:8080/health")
	fmt.Println()
	fmt.Println("=== 动态负载模拟 ===")
	fmt.Println("系统将按5分钟周期运行不同的负载模式:")
	fmt.Println("  • 0-1分钟：快速处理期 (低延迟, 5%失败率, 小分区)")
	fmt.Println("  • 1-2分钟：正常负载期 (正常延迟, 10%失败率, 正常分区)")
	fmt.Println("  • 2-3分钟：高负载期 (高延迟, 25%失败率, 大分区)")
	fmt.Println("  • 3-4分钟：波动期 (交替延迟, 15%失败率, 混合分区)")
	fmt.Println("  • 4-5分钟：稳定期 (稳定延迟, 8%失败率, 中等分区)")
	fmt.Println()
	fmt.Println("可用的监控指标包括:")
	fmt.Println("  • elk_coordinator_is_leader - Leader 状态")
	fmt.Println("  • elk_coordinator_partitions_assignment_duration_seconds - 分区分配耗时")
	fmt.Println("  • elk_coordinator_active_workers_count - 活跃工作节点数")
	fmt.Println("  • elk_coordinator_node_tasks_processed_total - 节点已处理任务数")
	fmt.Println("  • elk_coordinator_node_tasks_errors_total - 节点任务错误数")
	fmt.Println("  • elk_coordinator_node_task_processing_duration_seconds - 节点任务处理耗时")
	fmt.Println("  • elk_coordinator_partitions_total - 分区总数")
	fmt.Println("  • elk_coordinator_node_heartbeat_timestamp_seconds - 节点心跳时间戳")
	fmt.Println()
	fmt.Println("使用 curl 查看指标:")
	fmt.Println("  curl http://localhost:8080/metrics")
	fmt.Println()
	fmt.Println("按 Ctrl+C 停止服务...")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动一个 goroutine 来定期输出状态
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		startTime := time.Now()

		for {
			select {
			case <-ticker.C:
				runningTime := time.Since(startTime)
				cycleSeconds := int(runningTime.Seconds()) % 300

				var phase string
				switch {
				case cycleSeconds < 60:
					phase = "快速处理期"
				case cycleSeconds < 120:
					phase = "正常负载期"
				case cycleSeconds < 180:
					phase = "高负载期"
				case cycleSeconds < 240:
					phase = "波动期"
				default:
					phase = "稳定期"
				}

				fmt.Printf("[%s] 管理器运行中... 当前阶段: %s (%ds) | 监控: http://localhost:8080/metrics\n",
					time.Now().Format("15:04:05"), phase, cycleSeconds)
			case <-sigChan:
				return
			}
		}
	}()

	// 等待停止信号
	<-sigChan
	fmt.Println("\n收到停止信号，正在优雅关闭...")

	// 优雅停止
	mgr.Stop()
	fmt.Println("✓ 管理器已优雅停止")

	fmt.Println("程序已退出")
}
