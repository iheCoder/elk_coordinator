# ELK Coordinator

ELK Coordinator 是一个高性能、可扩展的分布式任务处理框架，专为需要大规模并行处理的系统设计。它采用Leader-Worker架构，通过分区机制将工作负载分配给多个工作节点，支持动态扩缩容和故障自动恢复。

## 特性

- **自动Leader选举**: 无需手动指定主节点，系统自动选举和维护Leader
- **工作节点自动发现**: 通过心跳机制自动发现和管理工作节点
- **分区任务处理**: 将工作负载分割为可管理的分区，支持并行处理
- **任务窗口**: 单节点可并行处理多个分区，提高资源利用率
- **熔断保护**: 内置熔断器机制，防止系统过载并提高稳定性
- **性能指标收集**: 自动收集处理速度、成功率等指标
- **优雅启停**: 支持平滑的启动和关闭流程，避免任务丢失
- **可扩展接口**: 提供灵活的接口允许自定义处理逻辑和分区规划

## 安装

```bash
go get github.com/yourusername/elk_coordinator
```

## 快速开始

以下是一个简单的示例，展示如何使用ELK Coordinator处理任务：

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/redis/go-redis/v9"
    "github.com/yourusername/elk_coordinator"
    "github.com/yourusername/elk_coordinator/data"
    "github.com/yourusername/elk_coordinator/leader"
)

// 自定义处理器实现
type MyProcessor struct{}

// 处理指定ID范围内的任务
func (p *MyProcessor) Process(ctx context.Context, minID int64, maxID int64, options map[string]interface{}) (int64, error) {
    fmt.Printf("处理ID范围: %d - %d\n", minID, maxID)
    
    // 这里实现你的任务处理逻辑
    // 例如，处理数据库中的一批记录、处理队列中的消息等
    
    processedCount := maxID - minID + 1
    return processedCount, nil
}

// 自定义分区规划器
type MyPartitionPlaner struct{}

// 返回建议的分区大小
func (p *MyPartitionPlaner) PartitionSize(ctx context.Context) (int64, error) {
    return 1000, nil // 每个分区处理1000条记录
}

// 获取下一批次的最大ID
func (p *MyPartitionPlaner) GetNextMaxID(ctx context.Context, lastID int64, rangeSize int64) (int64, error) {
    // 这里可以从数据库或其他数据源获取实际的ID上限
    // 简单示例：每次增加rangeSize
    return lastID + rangeSize, nil
}

func main() {
    // 创建Redis客户端
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    
    // 创建数据存储
    dataStore := data.NewRedisDataStore(redisClient, data.DefaultOptions())
    
    // 创建处理器和分区规划器
    processor := &MyProcessor{}
    planer := &MyPartitionPlaner{}
    
    // 创建管理器
    // "my-app"是应用命名空间，用于隔离不同应用的数据
    mgr := elk_coordinator.NewMgr(
        "my-app",
        dataStore,
        processor,
        planer,
        model.StrategyTypeHash,                              // 指定分区策略类型
        elk_coordinator.WithTaskWindow(5),                   // 启用任务窗口并设置大小（并行处理5个分区）
        elk_coordinator.WithHeartbeatInterval(5*time.Second), // 自定义心跳间隔
    )
    
    // 创建上下文（可以通过取消此上下文来停止服务）
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // 启动协调器
    if err := mgr.Start(ctx); err != nil {
        fmt.Printf("启动失败: %v\n", err)
        return
    }
    
    fmt.Println("服务已启动，节点ID:", mgr.ID)
    fmt.Println("按Ctrl+C停止...")
    
    // 等待终止信号
    <-ctx.Done()
    
    // 停止服务
    mgr.Stop()
    fmt.Println("服务已停止")
}
```

## 核心概念

### 分区 (Partition)

分区是任务处理的基本单位，由ID范围（最小ID和最大ID）定义。系统将工作负载分割为多个分区，并分配给不同的工作节点处理。

分区状态包括：
- `pending`: 等待被处理
- `claimed`: 已被工作节点认领
- `running`: 正在处理中
- `completed`: 处理完成
- `failed`: 处理失败

### Leader选举

系统使用分布式锁实现自动Leader选举。Leader节点负责：
- 监控活跃工作节点
- 创建和分配分区
- 处理节点故障和任务重新分配

### 任务窗口

任务窗口允许单个工作节点同时处理多个分区，提高资源利用率。窗口大小决定了并发处理的分区数量。

### 熔断器

内置的熔断器机制可以检测连续失败，并在系统压力过大时暂时阻止新任务的处理，防止系统崩溃。

## 配置选项

ELK Coordinator提供了多种配置选项，可以通过选项模式灵活配置：

```go
mgr := elk_coordinator.NewMgr(
    "my-namespace",
    dataStore,
    processor,
    planer,
    model.StrategyTypeHash,                                   // 分区策略类型
    elk_coordinator.WithLogger(customLogger),                 // 自定义日志记录器
    elk_coordinator.WithHeartbeatInterval(10*time.Second),    // 心跳间隔
    elk_coordinator.WithLeaderElectionInterval(5*time.Second), // Leader选举间隔
    elk_coordinator.WithPartitionLockExpiry(3*time.Minute),   // 分区锁过期时间
    elk_coordinator.WithLeaderLockExpiry(30*time.Second),     // Leader锁过期时间
    elk_coordinator.WithWorkerPartitionMultiple(3),           // 工作节点分区倍数
    elk_coordinator.WithTaskWindow(5),                        // 启用任务窗口并设置大小
)
```

## 核心接口

### Processor 接口

用户需要实现的处理器接口，定义如何处理分区内的任务：

```go
type Processor interface {
    Process(ctx context.Context, minID int64, maxID int64, options map[string]interface{}) (int64, error)
}
```

### PartitionPlaner 接口

定义如何规划分区大小和ID范围：

```go
type PartitionPlaner interface {
    PartitionSize(ctx context.Context) (int64, error)
    GetNextMaxID(ctx context.Context, lastID int64, rangeSize int64) (int64, error)
}
```

### DataStore 接口

定义数据存储操作，默认由RedisDataStore实现：

```go
type DataStore interface {
    // 分布式锁操作
    AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
    RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
    ReleaseLock(ctx context.Context, key string, value string) error
    
    // 心跳和工作节点管理
    SetHeartbeat(ctx context.Context, key string, value string) error
    RegisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string, heartbeatValue string) error
    UnregisterWorker(ctx context.Context, workersKey, workerID string, heartbeatKey string) error
    
    // 分区和同步状态操作
    SetPartitions(ctx context.Context, key string, value string) error
    GetPartitions(ctx context.Context, key string) (string, error)
    
    // ... 其他方法
}
```

## 高级功能

### 指标收集

系统自动收集处理速度、成功率等指标，可用于监控和负载均衡。

### 熔断器配置

可以自定义熔断器行为：

```go
circuitBreakerConfig := &task.CircuitBreakerConfig{
    ConsecutiveFailureThreshold: 5,              // 连续失败触发熔断阈值
    TotalFailureThreshold:       10,             // 失败分区数阈值
    OpenTimeout:                 30*time.Second, // 熔断开启后等待恢复的时间
    MaxHalfOpenRequests:         2,              // Half-Open状态下最大探测请求数
    FailureTimeWindow:           5*time.Minute,  // 失败统计时间窗口
}

mgr := elk_coordinator.NewMgr(
    "my-namespace",
    dataStore,
    processor,
    planer,
    model.StrategyTypeHash,                                      // 分区策略类型
    elk_coordinator.WithCircuitBreakerConfig(circuitBreakerConfig),
)
```

## 贡献

欢迎提交Pull Request或Issue！

## 许可证

[MIT](LICENSE)
