# ELK Coordinator 监控系统设计方案

## 1. 引言

本文档旨在为 `elk_coordinator` 项目设计一个轻量级、低侵入且具有可行性的观测监控系统。该系统将专注于监控 Leader 状态、分区分配、节点健康度以及任务处理的关键指标，以便及时发现问题、评估系统性能和保障系统稳定性。

**设计原则：**

*   **低侵入性：** 监控组件的集成不应对核心业务逻辑和性能产生显著影响。
*   **简单实用：** 避免引入过于复杂或重量级的监控系统，优先选择成熟、易于部署和维护的方案。
*   **核心指标：** 关注最能反映系统状态和性能的关键指标。
*   **可扩展性：** 设计应具备一定的扩展性，以便未来按需增加更多监控维度。

## 2. 技术选型

*   **指标采集与存储：** **Prometheus**
    *   Prometheus 是一个开源的监控和告警工具包，非常适合记录时序数据。
    *   它采用 Pull 模型从目标服务暴露的 HTTP 端点（通常是 `/metrics`）拉取指标。
    *   Go 应用可以通过官方的 `prometheus/client_golang` 库轻松集成。
*   **指标可视化：** **Grafana**
    *   Grafana 是一个开源的分析和可视化平台，可以与 Prometheus 无缝集成，创建丰富的仪表盘。
*   **指标暴露：**
    *   在 `elk_coordinator` 应用中，通过 `prometheus/client_golang` 库定义和更新指标。
    *   应用将通过一个 HTTP `/metrics` 端点暴露这些指标给 Prometheus。

## 3. 核心监控指标设计

以下是建议监控的核心指标，分为几个关键维度。所有指标都应包含 `namespace` 标签，以支持多租户或多环境部署。

### 3.1. Leader 相关指标

| 指标名称                                       | 类型      | 标签 (除全局标签外) | 描述                                                                 | 采集位置/逻辑                                                                 |
| :--------------------------------------------- | :-------- | :------------------ | :------------------------------------------------------------------- | :---------------------------------------------------------------------------- |
| `elk_coordinator_is_leader`                    | Gauge     | `node_id`           | 标记当前节点是否为 Leader (1 表示是，0 表示否)。                               | `LeaderManager` 在成为/放弃 Leader 时更新。                                     |
| `elk_coordinator_leader_election_attempts_total` | Counter   | `node_id`           | 节点尝试参与 Leader 选举的总次数。                                       | `LeaderManager` / `Election` 每次尝试选举时递增。                               |
| `elk_coordinator_leader_election_failures_total` | Counter   | `node_id`, `reason` | 节点参与 Leader 选举失败的总次数，可带失败原因标签。                         | `LeaderManager` / `Election` 选举失败时递增。                                   |
| `elk_coordinator_partitions_total`             | Gauge     |                     | Leader 视角下，当前需要管理的总分区数。                                  | `PartitionAssigner` 在规划分区后更新。                                        |
| `elk_coordinator_partitions_assigned_total`    | Counter   |                     | Leader 成功分配的分区总数（非去重，指分配动作次数）。                        | `PartitionAssigner` 每次成功分配一批分区后递增。                                |
| `elk_coordinator_partitions_assignment_duration_seconds` | Histogram |                   | Leader 分配一批分区的耗时分布。                                          | `PartitionAssigner` 记录分配操作的耗时。                                        |
| `elk_coordinator_active_workers_count`         | Gauge     |                     | Leader 视角下，当前活跃（心跳正常）的工作节点数量。                          | `WorkManager` 定期检查并更新。                                                |

### 3.2. 节点心跳与状态指标

| 指标名称                                    | 类型  | 标签 (除全局标签外) | 描述                                                       | 采集位置/逻辑                                                              |
| :------------------------------------------ | :---- | :------------------ | :--------------------------------------------------------- | :------------------------------------------------------------------------- |
| `elk_coordinator_node_heartbeat_timestamp_seconds` | Gauge | `worker_id`         | 节点上次成功发送心跳的时间戳（Unix timestamp）。               | `Mgr` 的 `heartbeatKeeper` 每次成功注册/更新心跳时记录 `time.Now().Unix()`。 |
| `elk_coordinator_node_heartbeat_errors_total` | Counter | `worker_id`         | 节点发送心跳失败的总次数。                                   | `Mgr` 的 `heartbeatKeeper` 心跳失败时递增。                                  |
| `elk_coordinator_node_info`                 | Gauge | `worker_id`, `version`, `go_version`, `os_type` | 节点基本信息，Gauge 值恒为1，通过标签携带信息。 | `Mgr` 启动时注册。                                                         |

*   **心跳告警：** 可以通过 Prometheus 的查询 `time() - elk_coordinator_node_heartbeat_timestamp_seconds{job="elk_coordinator", worker_id="xxx"} > STALE_THRESHOLD_SECONDS` 来判断节点心跳是否超时，并在 Grafana 中展示或通过 Alertmanager 告警。`STALE_THRESHOLD_SECONDS` 通常是心跳间隔的2-3倍。

### 3.3. 节点任务处理指标 (Worker 级别)

这些指标由每个 Worker 节点自身上报，并带有 `worker_id` 标签。

| 指标名称                                              | 类型      | 标签 (除全局标签外) | 描述                                                              | 采集位置/逻辑                                                                                                |
| :---------------------------------------------------- | :-------- | :------------------ | :---------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------- |
| `elk_coordinator_node_task_window_size`               | Gauge     | `worker_id`         | 当前节点的任务窗口大小配置。                                          | `TaskWindow` 初始化时设置。                                                                                    |
| `elk_coordinator_node_task_queue_active_tasks`        | Gauge     | `worker_id`         | 当前节点任务队列中待处理的任务数量。                                  | `TaskWindow` 的 `taskQueue` 长度。                                                                           |
| `elk_coordinator_node_tasks_active_processing`        | Gauge     | `worker_id`         | 当前节点正在通过 `Runner` 处理的分区任务数量。                        | `Runner` 开始/结束 `processPartitionTask` 时调整。                                                               |
| `elk_coordinator_node_tasks_processed_total`          | Counter   | `worker_id`         | 当前节点已成功处理的分区任务总数。                                    | `Runner` 的 `processPartitionTask` 成功完成时递增。                                                            |
| `elk_coordinator_node_tasks_processed_items_total`    | Counter   | `worker_id`         | 当前节点已成功处理的任务项总数（如果任务有“项”的概念）。                  | `Runner` 的 `executeProcessorTask` 返回处理项数时累加。                                                        |
| `elk_coordinator_node_tasks_errors_total`             | Counter   | `worker_id`, `error_type` | 当前节点处理分区任务失败的总次数，可带错误类型标签。                    | `Runner` 的 `processPartitionTask` 或 `executeProcessorTask` 失败时递增。                                      |
| `elk_coordinator_node_task_processing_duration_seconds` | Histogram | `worker_id`         | 当前节点处理单个分区任务的耗时分布（从获取到处理完成）。                  | `Runner` 的 `processPartitionTask` 记录从开始到结束（成功或失败）的耗时。                                        |
| `elk_coordinator_node_task_processor_duration_seconds`| Histogram | `worker_id`         | 当前节点 `Processor.Process` 方法执行的耗时分布。                   | `Runner` 的 `executeProcessorTask` 记录 `processor.ProcessTask` 的耗时。                                       |
| `elk_coordinator_node_circuit_breaker_state`          | Gauge     | `worker_id`, `breaker_name` | 节点上熔断器的状态 (0: Closed, 1: Open, 2: HalfOpen)。         | `CircuitBreaker` 状态变更时更新。                                                                            |
| `elk_coordinator_node_circuit_breaker_tripped_total`  | Counter   | `worker_id`, `breaker_name` | 节点上熔断器被触发（进入 Open 状态）的总次数。                      | `CircuitBreaker` 触发时递增。                                                                                |

### 3.4. 全局任务汇总指标

这些指标主要通过 Prometheus 对上述节点级别指标进行聚合查询得到，并在 Grafana 中展示。

*   **总活跃任务数：** `sum(elk_coordinator_node_tasks_active_processing)`
*   **总任务处理速率：** `sum(rate(elk_coordinator_node_tasks_processed_total[5m]))`
*   **总任务项处理速率：** `sum(rate(elk_coordinator_node_tasks_processed_items_total[5m]))`
*   **总任务错误率：** `sum(rate(elk_coordinator_node_tasks_errors_total[5m])) / sum(rate(elk_coordinator_node_tasks_processed_total[5m]))` (如果处理数不为0)
*   **平均任务处理耗时：** `sum(rate(elk_coordinator_node_task_processing_duration_seconds_sum[5m])) / sum(rate(elk_coordinator_node_task_processing_duration_seconds_count[5m]))` (对于 Histogram 类型指标)

## 4. 实现方案

### 4.1. `metrics` 包设计

在 `/metrics` 目录下创建 `metrics_manager.go` (如果尚不存在)。该文件将负责：

*   定义所有 Prometheus 指标对象 (CounterVec, GaugeVec, HistogramVec)。
*   提供注册这些指标到 Prometheus 全局注册表或自定义注册表的函数。
*   提供更新这些指标的辅助函数，供项目其他组件调用。

```go
// metrics/metrics_manager.go (示例片段)
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	namespace = "elk_coordinator" // Or read from config

	IsLeader = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "is_leader",
			Help:      "Marks if the current node is the leader (1 for leader, 0 for follower).",
		},
		[]string{"node_id"},
	)

	NodeTasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_tasks_processed_total",
			Help:      "Total number of partition tasks successfully processed by the node.",
		},
		[]string{"worker_id"},
	)

	// ... 其他指标定义
)

func InitMetrics() {
	// 可以进行一些初始化注册，promauto 会自动注册到 DefaultRegisterer
}

// 可选：提供一些更新指标的便捷函数，或者让调用方直接使用导出的指标对象
// func RecordTaskProcessed(workerID string) {
// 	NodeTasksProcessedTotal.WithLabelValues(workerID).Inc()
// }
```

### 4.2. 代码埋点位置

*   **`mgr.go` (`Mgr` 类型):**
    *   在 `NewMgr` 或 `Start` 方法中初始化 `metrics_manager`。
    *   启动一个 HTTP 服务（或集成到现有服务）来暴露 `/metrics` 端点 (e.g., 使用 `promhttp.Handler()`)。
    *   `heartbeatKeeper`: 更新 `elk_coordinator_node_heartbeat_timestamp_seconds` 和 `elk_coordinator_node_heartbeat_errors_total`。
    *   `registerNode`: 记录 `elk_coordinator_node_info`。
*   **`leader/leader_manager.go` (`LeaderManager` 类型):**
    *   `becomeLeader` / `relinquishLeadership`: 更新 `elk_coordinator_is_leader`。
    *   `TryElect`: 更新 `elk_coordinator_leader_election_attempts_total` 和 `elk_coordinator_leader_election_failures_total`。
*   **`leader/partition_assigner.go` (`PartitionAssigner` 类型):**
    *   分区分配逻辑中：更新 `elk_coordinator_partitions_total`, `elk_coordinator_partitions_assigned_total`, `elk_coordinator_partitions_assignment_duration_seconds`。
*   **`leader/work_manager.go` (`WorkManager` 类型):**
    *   定期检查活跃节点：更新 `elk_coordinator_active_workers_count`。
*   **`task/window.go` (`TaskWindow` 类型):**
    *   `NewTaskWindow`: 设置 `elk_coordinator_node_task_window_size`。
    *   `fetchTasks` / `fillTaskQueue` / `processTasks`: 定期或在任务入队/出队时更新 `elk_coordinator_node_task_queue_active_tasks`。
*   **`task/runner.go` (`Runner` 类型):**
    *   `processPartitionTask`：
        *   开始时：递增 `elk_coordinator_node_tasks_active_processing`。
        *   结束时（成功或失败）：递减 `elk_coordinator_node_tasks_active_processing`，记录 `elk_coordinator_node_task_processing_duration_seconds`。
        *   成功时：递增 `elk_coordinator_node_tasks_processed_total`。
        *   失败时：递增 `elk_coordinator_node_tasks_errors_total`。
    *   `executeProcessorTask`：
        *   记录 `elk_coordinator_node_task_processor_duration_seconds`。
        *   成功时：累加 `elk_coordinator_node_tasks_processed_items_total` (如果适用)。
        *   失败时：递增 `elk_coordinator_node_tasks_errors_total` (如果错误在此处捕获并归类)。
*   **`task/circuit_breaker.go` (`CircuitBreaker` 类型):**
    *   状态转换时：更新 `elk_coordinator_node_circuit_breaker_state`。
    *   跳闸时：递增 `elk_coordinator_node_circuit_breaker_tripped_total`。

### 4.3. HTTP 服务暴露 `/metrics`

在 `Mgr` 的启动逻辑中，添加一个 HTTP 服务器来暴露 Prometheus 指标端点。

```go
// mgr.go (示例片段)
import (
	// ...
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	// "elk_coordinator/metrics" // 假设的 metrics 包
)

func (m *Mgr) Start(ctx context.Context) error {
	// ... 其他启动逻辑 ...

	// metrics.InitMetrics() // 初始化指标（如果需要）

	// 启动 HTTP 服务暴露 /metrics
	go func() {
		httpMux := http.NewServeMux()
		httpMux.Handle("/metrics", promhttp.Handler())
		// 选择一个合适的端口，例如 :9091
		// 确保端口不与应用其他服务冲突，并可被 Prometheus 访问
		metricsAddr := ":9091" // 可以从配置读取
		m.Logger.Infof("Starting metrics server on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, httpMux); err != nil && err != http.ErrServerClosed {
			m.Logger.Errorf("Metrics server failed: %v", err)
		}
	}()

	// ...
	return nil
}
```

## 5. 部署简述

1.  **Prometheus 配置：**
    在 Prometheus 的配置文件 (`prometheus.yml`) 中添加 scrape job 来发现和拉取 `elk_coordinator` 实例的指标。
    ```yaml
    scrape_configs:
      - job_name: 'elk_coordinator'
        static_configs:
          - targets: ['<node1_ip>:9091', '<node2_ip>:9091', ...] # 列出所有 elk_coordinator 实例
        # 或者使用服务发现机制 (e.g., Consul, Kubernetes SD)
    ```
2.  **Grafana 配置：**
    *   添加 Prometheus 作为数据源。
    *   创建仪表盘，使用上面定义的指标和 PromQL 查询来可视化系统状态。例如：
        *   当前 Leader (使用 `elk_coordinator_is_leader == 1`)。
        *   分区分配进度。
        *   各节点心跳状态 (使用 `time() - elk_coordinator_node_heartbeat_timestamp_seconds > threshold`)。
        *   各节点任务处理速率、错误率、队列长度。
        *   全局任务处理速率、错误总数等。

## 6. 性能和复杂度评估

*   **性能影响：**
    *   `prometheus/client_golang` 库本身对性能的影响非常小，主要涉及原子操作和 map 访问。
    *   HTTP `/metrics` 端点的开销也较低，Prometheus 拉取频率通常在秒级（如 15s, 30s），不会对应用造成频繁中断。
    *   应避免在高频代码路径中进行过于复杂的指标计算或标签生成。
*   **复杂度：**
    *   引入 Prometheus 和 Grafana 增加了运维的组件，但它们是监控领域的标准工具，社区支持良好，部署和维护相对成熟。
    *   代码层面的修改主要是添加指标定义和在关键位置更新指标，复杂度可控。

## 7. 未来扩展

*   **告警集成：** 配置 Prometheus Alertmanager，根据关键指标（如 Leader 丢失、节点心跳超时、错误率过高、任务队列持续满等）设置告警规则。
*   **更细粒度的指标：** 根据实际需求，可以添加更细致的指标，例如特定类型任务的处理情况、DataStore 操作的延迟和错误等。
*   **分布式追踪：** 对于复杂的请求链路，可以考虑集成分布式追踪系统 (如 Jaeger, Zipkin)，但这通常比指标监控更复杂。
