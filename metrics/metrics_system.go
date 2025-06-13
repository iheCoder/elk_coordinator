package metrics

import (
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// 默认命名空间
	DefaultNamespace = "elk_coordinator"
)

var (
	// 全局命名空间，可以通过配置修改
	namespace = DefaultNamespace

	// === Leader 相关指标 ===

	// 标记当前节点是否为 Leader (1 表示是，0 表示否)
	IsLeader = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "is_leader",
			Help:      "Marks if the current node is the leader (1 for leader, 0 for follower).",
		},
		[]string{"node_id"},
	)

	// Leader 视角下，当前需要管理的总分区数
	PartitionsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "partitions_total",
			Help:      "Total number of partitions to be managed from leader perspective.",
		},
	)

	// Leader 成功分配的分区总数（分配动作次数）
	PartitionsAssignedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "partitions_assigned_total",
			Help:      "Total number of partition assignment operations successfully completed.",
		},
	)

	// Leader 分配一批分区的耗时分布
	PartitionsAssignmentDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "partitions_assignment_duration_seconds",
			Help:      "Duration distribution of partition assignment operations.",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
	)

	// Leader 视角下，当前活跃的工作节点数量
	ActiveWorkersCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_workers_count",
			Help:      "Number of currently active worker nodes from leader perspective.",
		},
	)

	// === 节点心跳与状态指标 ===

	// 节点上次成功发送心跳的时间戳
	NodeHeartbeatTimestampSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "node_heartbeat_timestamp_seconds",
			Help:      "Timestamp of the last successful heartbeat sent by the node.",
		},
		[]string{"worker_id"},
	)

	// 节点发送心跳失败的总次数
	NodeHeartbeatErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_heartbeat_errors_total",
			Help:      "Total number of heartbeat failures by the node.",
		},
		[]string{"worker_id"},
	)

	// 节点基本信息，Gauge 值恒为1，通过标签携带信息
	NodeInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "node_info",
			Help:      "Node basic information with constant value 1, info carried by labels.",
		},
		[]string{"worker_id", "version", "go_version", "os_type"},
	)

	// === 节点任务处理指标 ===

	// 当前节点任务队列中待处理的任务数量
	NodeTaskQueueActiveTasks = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "node_task_queue_active_tasks",
			Help:      "Number of pending tasks in the node's task queue.",
		},
		[]string{"worker_id"},
	)

	// 当前节点已成功处理的分区任务总数
	NodeTasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_tasks_processed_total",
			Help:      "Total number of partition tasks successfully processed by the node.",
		},
		[]string{"worker_id"},
	)

	// 当前节点已成功处理的任务项总数
	NodeTasksProcessedItemsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_tasks_processed_items_total",
			Help:      "Total number of task items successfully processed by the node.",
		},
		[]string{"worker_id"},
	)

	// 当前节点处理分区任务失败的总次数
	NodeTasksErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_tasks_errors_total",
			Help:      "Total number of partition task processing failures by the node.",
		},
		[]string{"worker_id", "error_type"},
	)

	// 当前节点处理单个分区任务的耗时分布
	NodeTaskProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "node_task_processing_duration_seconds",
			Help:      "Duration distribution of partition task processing by the node.",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{"worker_id"},
	)

	// 当前节点 Processor.Process 方法执行的耗时分布
	NodeTaskProcessorDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "node_task_processor_duration_seconds",
			Help:      "Duration distribution of Processor.Process method execution by the node.",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{"worker_id"},
	)

	// 节点上熔断器被触发的总次数
	NodeCircuitBreakerTrippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_circuit_breaker_tripped_total",
			Help:      "Total number of times the circuit breaker was tripped on the node.",
		},
		[]string{"worker_id", "breaker_name"},
	)
)

// MetricsManager 管理监控指标的结构体
type MetricsManager struct {
	namespace string
	enabled   bool
}

// NewMetricsManager 创建新的指标管理器
func NewMetricsManager(customNamespace ...string) *MetricsManager {
	ns := namespace
	if len(customNamespace) > 0 && customNamespace[0] != "" {
		ns = customNamespace[0]
	}

	return &MetricsManager{
		namespace: ns,
		enabled:   true,
	}
}

// InitMetrics 初始化指标系统
func (m *MetricsManager) InitMetrics() {
	// 如果使用了自定义命名空间，需要重新注册指标
	if m.namespace != DefaultNamespace {
		namespace = m.namespace
		// 这里可以重新创建指标，但 promauto 已经注册了，所以保持当前实现
	}
}

// SetEnabled 设置监控系统开关
func (m *MetricsManager) SetEnabled(enabled bool) {
	m.enabled = enabled
}

// IsEnabled 检查监控系统是否启用
func (m *MetricsManager) IsEnabled() bool {
	return m.enabled
}

// === Leader 相关指标操作方法 ===

// SetLeaderStatus 设置当前节点的 Leader 状态
func (m *MetricsManager) SetLeaderStatus(nodeID string, isLeader bool) {
	if !m.enabled {
		return
	}
	value := float64(0)
	if isLeader {
		value = 1
	}
	IsLeader.WithLabelValues(nodeID).Set(value)
}

// SetPartitionsTotal 设置分区总数
func (m *MetricsManager) SetPartitionsTotal(count float64) {
	if !m.enabled {
		return
	}
	PartitionsTotal.Set(count)
}

// IncPartitionsAssigned 增加分区分配次数
func (m *MetricsManager) IncPartitionsAssigned() {
	if !m.enabled {
		return
	}
	PartitionsAssignedTotal.Inc()
}

// ObservePartitionAssignmentDuration 记录分区分配耗时
func (m *MetricsManager) ObservePartitionAssignmentDuration(duration time.Duration) {
	if !m.enabled {
		return
	}
	PartitionsAssignmentDurationSeconds.Observe(duration.Seconds())
}

// SetActiveWorkersCount 设置活跃工作节点数
func (m *MetricsManager) SetActiveWorkersCount(count float64) {
	if !m.enabled {
		return
	}
	ActiveWorkersCount.Set(count)
}

// === 节点心跳与状态指标操作方法 ===

// UpdateHeartbeatTimestamp 更新心跳时间戳
func (m *MetricsManager) UpdateHeartbeatTimestamp(workerID string) {
	if !m.enabled {
		return
	}
	NodeHeartbeatTimestampSeconds.WithLabelValues(workerID).SetToCurrentTime()
}

// IncHeartbeatErrors 增加心跳错误计数
func (m *MetricsManager) IncHeartbeatErrors(workerID string) {
	if !m.enabled {
		return
	}
	NodeHeartbeatErrorsTotal.WithLabelValues(workerID).Inc()
}

// SetNodeInfo 设置节点信息
func (m *MetricsManager) SetNodeInfo(workerID, version string) {
	if !m.enabled {
		return
	}
	NodeInfo.WithLabelValues(workerID, version, runtime.Version(), runtime.GOOS).Set(1)
}

// === 节点任务处理指标操作方法 ===

// SetTaskQueueActiveTasks 设置任务队列中的活跃任务数
func (m *MetricsManager) SetTaskQueueActiveTasks(workerID string, count float64) {
	if !m.enabled {
		return
	}
	NodeTaskQueueActiveTasks.WithLabelValues(workerID).Set(count)
}

// IncTasksProcessed 增加已处理任务计数
func (m *MetricsManager) IncTasksProcessed(workerID string) {
	if !m.enabled {
		return
	}
	NodeTasksProcessedTotal.WithLabelValues(workerID).Inc()
}

// AddTasksProcessedItems 增加已处理任务项计数
func (m *MetricsManager) AddTasksProcessedItems(workerID string, count float64) {
	if !m.enabled {
		return
	}
	NodeTasksProcessedItemsTotal.WithLabelValues(workerID).Add(count)
}

// IncTasksErrors 增加任务错误计数
func (m *MetricsManager) IncTasksErrors(workerID string, errorType string) {
	if !m.enabled {
		return
	}
	NodeTasksErrorsTotal.WithLabelValues(workerID, errorType).Inc()
}

// ObserveTaskProcessingDuration 记录任务处理耗时
func (m *MetricsManager) ObserveTaskProcessingDuration(workerID string, duration time.Duration) {
	if !m.enabled {
		return
	}
	NodeTaskProcessingDurationSeconds.WithLabelValues(workerID).Observe(duration.Seconds())
}

// ObserveTaskProcessorDuration 记录任务处理器执行耗时
func (m *MetricsManager) ObserveTaskProcessorDuration(workerID string, duration time.Duration) {
	if !m.enabled {
		return
	}
	NodeTaskProcessorDurationSeconds.WithLabelValues(workerID).Observe(duration.Seconds())
}

// === 熔断器相关指标操作方法 ===

// CircuitBreakerState 熔断器状态枚举
type CircuitBreakerState int

const (
	CircuitBreakerClosed   CircuitBreakerState = 0
	CircuitBreakerOpen     CircuitBreakerState = 1
	CircuitBreakerHalfOpen CircuitBreakerState = 2
)

// IncCircuitBreakerTripped 增加熔断器触发计数
func (m *MetricsManager) IncCircuitBreakerTripped(workerID, breakerName string) {
	if !m.enabled {
		return
	}
	NodeCircuitBreakerTrippedTotal.WithLabelValues(workerID, breakerName).Inc()
}

// === HTTP 服务相关方法 ===

// StartMetricsServer 启动指标 HTTP 服务器
func (m *MetricsManager) StartMetricsServer(addr string) error {
	if !m.enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// 添加健康检查端点
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return http.ListenAndServe(addr, mux)
}

// GetMetricsHandler 获取指标处理器，用于集成到现有的 HTTP 服务中
func (m *MetricsManager) GetMetricsHandler() http.Handler {
	if !m.enabled {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Metrics disabled"))
		})
	}
	return promhttp.Handler()
}

// === 便捷的全局实例 ===

var (
	// DefaultMetricsManager 默认的指标管理器实例
	DefaultMetricsManager = NewMetricsManager()
)

// 全局便捷方法，使用默认指标管理器

// SetLeaderStatus 设置 Leader 状态（全局方法）
func SetLeaderStatus(nodeID string, isLeader bool) {
	DefaultMetricsManager.SetLeaderStatus(nodeID, isLeader)
}

// UpdateHeartbeatTimestamp 更新心跳时间戳（全局方法）
func UpdateHeartbeatTimestamp(workerID string) {
	DefaultMetricsManager.UpdateHeartbeatTimestamp(workerID)
}

// IncHeartbeatErrors 增加心跳错误（全局方法）
func IncHeartbeatErrors(workerID string) {
	DefaultMetricsManager.IncHeartbeatErrors(workerID)
}

// SetNodeInfo 设置节点信息（全局方法）
func SetNodeInfo(workerID, version string) {
	DefaultMetricsManager.SetNodeInfo(workerID, version)
}

// IncTasksProcessed 增加已处理任务计数（全局方法）
func IncTasksProcessed(workerID string) {
	DefaultMetricsManager.IncTasksProcessed(workerID)
}

// IncTasksErrors 增加任务错误计数（全局方法）
func IncTasksErrors(workerID string, errorType string) {
	DefaultMetricsManager.IncTasksErrors(workerID, errorType)
}

// ObserveTaskProcessingDuration 记录任务处理耗时（全局方法）
func ObserveTaskProcessingDuration(workerID string, duration time.Duration) {
	DefaultMetricsManager.ObserveTaskProcessingDuration(workerID, duration)
}
