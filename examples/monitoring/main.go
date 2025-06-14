package main

import (
	"context"
	"elk_coordinator"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// MockProcessor 简单的模拟处理器，用于演示监控功能
type MockProcessor struct {
	processDelay time.Duration
}

func NewMockProcessor(delay time.Duration) *MockProcessor {
	return &MockProcessor{
		processDelay: delay,
	}
}

func (p *MockProcessor) Process(ctx context.Context, minID, maxID int64, options map[string]interface{}) (int64, error) {
	// 模拟处理延迟
	select {
	case <-time.After(p.processDelay):
		// 正常处理完成
		processedCount := maxID - minID + 1
		fmt.Printf("模拟处理 ID 范围 [%d, %d], 处理了 %d 个项目\n", minID, maxID, processedCount)
		return processedCount, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// MockPlaner 简单的模拟分区规划器
type MockPlaner struct{}

func (p *MockPlaner) PartitionSize(ctx context.Context) (int64, error) {
	return 1000, nil
}

func (p *MockPlaner) GetNextMaxID(ctx context.Context, startID int64, rangeSize int64) (int64, error) {
	return startID + rangeSize, nil
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
	processor := NewMockProcessor(2 * time.Second) // 每个任务处理2秒
	planer := &MockPlaner{}
	fmt.Println("✓ 模拟组件创建完成")

	// 创建管理器
	fmt.Println("3. 创建 ELK Coordinator 管理器...")
	namespace := "monitoring-demo"
	mgr := elk_coordinator.NewMgr(
		namespace,
		dataStore,
		processor,
		planer,
		model.StrategyTypeHash, // 使用 Hash 分区策略
		// 配置监控选项
		elk_coordinator.WithMetricsEnabled(true),
		elk_coordinator.WithMetricsAddr(":8080"), // 监控服务端口
	)
	fmt.Printf("✓ 管理器创建完成 (命名空间: %s)\n", namespace)

	// 启动管理器
	fmt.Println("4. 启动管理器...")
	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()

	if err := mgr.Start(startCtx); err != nil {
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
	fmt.Println("可用的监控指标包括:")
	fmt.Println("  • elk_coordinator_leader_is_leader - Leader 状态")
	fmt.Println("  • elk_coordinator_partition_allocation_duration_seconds - 分区分配耗时")
	fmt.Println("  • elk_coordinator_active_workers_total - 活跃工作节点数")
	fmt.Println("  • elk_coordinator_tasks_processed_total - 已处理任务数")
	fmt.Println("  • elk_coordinator_tasks_errors_total - 任务错误数")
	fmt.Println("  • elk_coordinator_task_processing_duration_seconds - 任务处理耗时")
	fmt.Println("  • elk_coordinator_heartbeat_timestamp - 节点心跳时间戳")
	fmt.Println("  • elk_coordinator_circuit_breaker_state - 熔断器状态")
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

		for {
			select {
			case <-ticker.C:
				fmt.Printf("[%s] 管理器运行中... 请访问 http://localhost:8080/metrics 查看监控数据\n",
					time.Now().Format("15:04:05"))
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
