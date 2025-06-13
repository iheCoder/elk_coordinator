package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetricsManager(t *testing.T) {
	tests := []struct {
		name              string
		customNamespace   []string
		expectedNamespace string
		expectedEnabled   bool
	}{
		{
			name:              "default namespace",
			customNamespace:   nil,
			expectedNamespace: DefaultNamespace,
			expectedEnabled:   true,
		},
		{
			name:              "custom namespace",
			customNamespace:   []string{"custom_test"},
			expectedNamespace: "custom_test",
			expectedEnabled:   true,
		},
		{
			name:              "empty custom namespace",
			customNamespace:   []string{""},
			expectedNamespace: DefaultNamespace,
			expectedEnabled:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mm *MetricsManager
			if tt.customNamespace != nil {
				mm = NewMetricsManager(tt.customNamespace...)
			} else {
				mm = NewMetricsManager()
			}

			assert.Equal(t, tt.expectedNamespace, mm.namespace)
			assert.Equal(t, tt.expectedEnabled, mm.enabled)
			assert.True(t, mm.IsEnabled())
		})
	}
}

func TestMetricsManager_SetEnabled(t *testing.T) {
	mm := NewMetricsManager()

	// 初始状态应该是启用的
	assert.True(t, mm.IsEnabled())

	// 禁用监控
	mm.SetEnabled(false)
	assert.False(t, mm.IsEnabled())

	// 重新启用监控
	mm.SetEnabled(true)
	assert.True(t, mm.IsEnabled())
}

func TestMetricsManager_LeaderMetrics(t *testing.T) {
	mm := NewMetricsManager()
	nodeID := "test-node-1"

	// 测试设置 Leader 状态
	mm.SetLeaderStatus(nodeID, true)

	// 验证指标值
	assert.Equal(t, float64(1), testutil.ToFloat64(IsLeader.WithLabelValues(nodeID)))

	mm.SetLeaderStatus(nodeID, false)
	assert.Equal(t, float64(0), testutil.ToFloat64(IsLeader.WithLabelValues(nodeID)))

	// 测试分区总数
	mm.SetPartitionsTotal(100)
	assert.Equal(t, float64(100), testutil.ToFloat64(PartitionsTotal))

	// 测试分区分配计数
	initialCount := testutil.ToFloat64(PartitionsAssignedTotal)
	mm.IncPartitionsAssigned()
	assert.Equal(t, initialCount+1, testutil.ToFloat64(PartitionsAssignedTotal))

	// 测试分区分配耗时
	duration := 500 * time.Millisecond
	mm.ObservePartitionAssignmentDuration(duration)

	// 验证 histogram 有数据（检查采样总数）
	histogramCount := testutil.CollectAndCount(PartitionsAssignmentDurationSeconds)
	assert.Greater(t, histogramCount, 0)

	// 测试活跃工作节点数
	mm.SetActiveWorkersCount(5)
	assert.Equal(t, float64(5), testutil.ToFloat64(ActiveWorkersCount))
}

func TestMetricsManager_HeartbeatMetrics(t *testing.T) {
	mm := NewMetricsManager()
	workerID := "worker-1"

	// 测试心跳时间戳更新
	mm.UpdateHeartbeatTimestamp(workerID)

	// 验证时间戳被设置（应该接近当前时间）
	timestamp := testutil.ToFloat64(NodeHeartbeatTimestampSeconds.WithLabelValues(workerID))
	now := float64(time.Now().Unix())
	assert.InDelta(t, now, timestamp, 2) // 允许2秒误差

	// 测试心跳错误计数
	initialErrors := testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID))
	mm.IncHeartbeatErrors(workerID)
	assert.Equal(t, initialErrors+1, testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID)))

	// 测试节点信息
	version := "v1.0.0"
	mm.SetNodeInfo(workerID, version)
	assert.Equal(t, float64(1), testutil.ToFloat64(NodeInfo.WithLabelValues(workerID, version, runtime.Version(), runtime.GOOS)))
}

func TestMetricsManager_TaskMetrics(t *testing.T) {
	mm := NewMetricsManager()
	workerID := "worker-1"

	// 测试任务队列活跃任务数
	mm.SetTaskQueueActiveTasks(workerID, 10)
	assert.Equal(t, float64(10), testutil.ToFloat64(NodeTaskQueueActiveTasks.WithLabelValues(workerID)))

	// 测试已处理任务计数
	initialProcessed := testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID))
	mm.IncTasksProcessed(workerID)
	assert.Equal(t, initialProcessed+1, testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID)))

	// 测试已处理任务项计数
	initialItems := testutil.ToFloat64(NodeTasksProcessedItemsTotal.WithLabelValues(workerID))
	mm.AddTasksProcessedItems(workerID, 5)
	assert.Equal(t, initialItems+5, testutil.ToFloat64(NodeTasksProcessedItemsTotal.WithLabelValues(workerID)))

	// 测试任务错误计数
	errorType := "timeout"
	initialErrors := testutil.ToFloat64(NodeTasksErrorsTotal.WithLabelValues(workerID, errorType))
	mm.IncTasksErrors(workerID, errorType)
	assert.Equal(t, initialErrors+1, testutil.ToFloat64(NodeTasksErrorsTotal.WithLabelValues(workerID, errorType)))

	// 测试任务处理耗时
	duration := 2 * time.Second
	mm.ObserveTaskProcessingDuration(workerID, duration)
	// 验证 histogram 有数据（检查总样本数）
	assert.Greater(t, testutil.CollectAndCount(NodeTaskProcessingDurationSeconds), 0)

	// 测试任务处理器执行耗时
	mm.ObserveTaskProcessorDuration(workerID, duration)
	// 验证 histogram 有数据（检查总样本数）
	assert.Greater(t, testutil.CollectAndCount(NodeTaskProcessorDurationSeconds), 0)
}

func TestMetricsManager_CircuitBreakerMetrics(t *testing.T) {
	mm := NewMetricsManager()
	workerID := "worker-1"
	breakerName := "db-breaker"

	// 测试熔断器触发计数
	initialTripped := testutil.ToFloat64(NodeCircuitBreakerTrippedTotal.WithLabelValues(workerID, breakerName))
	mm.IncCircuitBreakerTripped(workerID, breakerName)
	assert.Equal(t, initialTripped+1, testutil.ToFloat64(NodeCircuitBreakerTrippedTotal.WithLabelValues(workerID, breakerName)))
}

func TestMetricsManager_DisabledBehavior(t *testing.T) {
	mm := NewMetricsManager()
	mm.SetEnabled(false)

	nodeID := "test-node"
	workerID := "worker-1"

	// 当监控禁用时，所有方法都应该安全返回，不更新指标
	initialLeaderValue := testutil.ToFloat64(IsLeader.WithLabelValues(nodeID))
	mm.SetLeaderStatus(nodeID, true)
	assert.Equal(t, initialLeaderValue, testutil.ToFloat64(IsLeader.WithLabelValues(nodeID)))

	initialPartitionsTotal := testutil.ToFloat64(PartitionsTotal)
	mm.SetPartitionsTotal(100)
	assert.Equal(t, initialPartitionsTotal, testutil.ToFloat64(PartitionsTotal))

	initialAssigned := testutil.ToFloat64(PartitionsAssignedTotal)
	mm.IncPartitionsAssigned()
	assert.Equal(t, initialAssigned, testutil.ToFloat64(PartitionsAssignedTotal))

	initialHeartbeatErrors := testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID))
	mm.IncHeartbeatErrors(workerID)
	assert.Equal(t, initialHeartbeatErrors, testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID)))

	initialTasksProcessed := testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID))
	mm.IncTasksProcessed(workerID)
	assert.Equal(t, initialTasksProcessed, testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID)))
}

func TestMetricsManager_GetMetricsHandler(t *testing.T) {
	tests := []struct {
		name           string
		enabled        bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "enabled metrics handler",
			enabled:        true,
			expectedStatus: http.StatusOK,
			expectedBody:   "", // Prometheus metrics will be present
		},
		{
			name:           "disabled metrics handler",
			enabled:        false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "Metrics disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := NewMetricsManager()
			mm.SetEnabled(tt.enabled)

			handler := mm.GetMetricsHandler()
			req := httptest.NewRequest("GET", "/metrics", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			assert.Equal(t, tt.expectedStatus, rr.Code)
			if tt.expectedBody != "" {
				assert.Equal(t, tt.expectedBody, rr.Body.String())
			} else if tt.enabled {
				// 验证返回的是 Prometheus 格式的指标
				body := rr.Body.String()
				assert.Contains(t, body, "# HELP")
				assert.Contains(t, body, "# TYPE")
			}
		})
	}
}

func TestMetricsManager_StartMetricsServer(t *testing.T) {
	t.Run("disabled metrics server", func(t *testing.T) {
		mm := NewMetricsManager()
		mm.SetEnabled(false)

		// 当监控禁用时，启动服务器应该直接返回 nil
		err := mm.StartMetricsServer(":0")
		assert.NoError(t, err)
	})

	t.Run("metrics server endpoints", func(t *testing.T) {
		mm := NewMetricsManager()

		// 创建测试服务器
		mux := http.NewServeMux()
		mux.Handle("/metrics", mm.GetMetricsHandler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		server := httptest.NewServer(mux)
		defer server.Close()

		// 测试 /metrics 端点
		resp, err := http.Get(server.URL + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		bodyStr := string(body)
		assert.Contains(t, bodyStr, "# HELP")
		assert.Contains(t, bodyStr, "elk_coordinator")

		// 测试 /health 端点
		resp, err = http.Get(server.URL + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))
	})
}

func TestCircuitBreakerState(t *testing.T) {
	tests := []struct {
		name     string
		state    CircuitBreakerState
		expected int
	}{
		{"closed", CircuitBreakerClosed, 0},
		{"open", CircuitBreakerOpen, 1},
		{"half-open", CircuitBreakerHalfOpen, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, int(tt.state))
		})
	}
}

func TestGlobalMethods(t *testing.T) {
	// 测试全局便捷方法是否正常工作
	nodeID := "global-test-node"
	workerID := "global-test-worker"

	// 这些方法应该使用默认的 MetricsManager
	SetLeaderStatus(nodeID, true)
	assert.Equal(t, float64(1), testutil.ToFloat64(IsLeader.WithLabelValues(nodeID)))

	UpdateHeartbeatTimestamp(workerID)
	timestamp := testutil.ToFloat64(NodeHeartbeatTimestampSeconds.WithLabelValues(workerID))
	now := float64(time.Now().Unix())
	assert.InDelta(t, now, timestamp, 2)

	initialErrors := testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID))
	IncHeartbeatErrors(workerID)
	assert.Equal(t, initialErrors+1, testutil.ToFloat64(NodeHeartbeatErrorsTotal.WithLabelValues(workerID)))

	SetNodeInfo(workerID, "v1.0.0")
	// 这里我们只验证方法不会 panic，具体值验证在其他测试中已经覆盖

	initialProcessed := testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID))
	IncTasksProcessed(workerID)
	assert.Equal(t, initialProcessed+1, testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID)))

	initialTaskErrors := testutil.ToFloat64(NodeTasksErrorsTotal.WithLabelValues(workerID, "test-error"))
	IncTasksErrors(workerID, "test-error")
	assert.Equal(t, initialTaskErrors+1, testutil.ToFloat64(NodeTasksErrorsTotal.WithLabelValues(workerID, "test-error")))

	ObserveTaskProcessingDuration(workerID, time.Second)
	assert.Greater(t, testutil.CollectAndCount(NodeTaskProcessingDurationSeconds), 0)
}

func TestMetricsIntegration(t *testing.T) {
	// 集成测试：模拟完整的监控场景
	mm := NewMetricsManager("integration_test")

	nodeID := "integration-node"
	workerID := "integration-worker"

	// 模拟节点启动
	mm.SetNodeInfo(workerID, "v1.0.0")
	mm.SetLeaderStatus(nodeID, true)
	mm.SetPartitionsTotal(50)
	mm.SetActiveWorkersCount(3)

	// 模拟心跳
	mm.UpdateHeartbeatTimestamp(workerID)

	// 模拟任务处理
	mm.SetTaskQueueActiveTasks(workerID, 5)

	// 模拟处理任务
	start := time.Now()
	mm.IncTasksProcessed(workerID)
	mm.AddTasksProcessedItems(workerID, 10)
	mm.ObserveTaskProcessingDuration(workerID, time.Since(start))
	mm.ObserveTaskProcessorDuration(workerID, 100*time.Millisecond)

	// 模拟分区分配
	assignmentStart := time.Now()
	mm.IncPartitionsAssigned()
	mm.ObservePartitionAssignmentDuration(time.Since(assignmentStart))

	// 模拟错误和熔断器
	mm.IncTasksErrors(workerID, "connection_timeout")
	mm.IncCircuitBreakerTripped(workerID, "db-connection")

	// 验证指标是否正确设置
	assert.Equal(t, float64(1), testutil.ToFloat64(IsLeader.WithLabelValues(nodeID)))
	assert.Equal(t, float64(50), testutil.ToFloat64(PartitionsTotal))
	assert.Equal(t, float64(3), testutil.ToFloat64(ActiveWorkersCount))
	assert.Equal(t, float64(5), testutil.ToFloat64(NodeTaskQueueActiveTasks.WithLabelValues(workerID)))

	// 验证计数器递增
	assert.Greater(t, testutil.ToFloat64(NodeTasksProcessedTotal.WithLabelValues(workerID)), float64(0))
	assert.Greater(t, testutil.ToFloat64(NodeTasksProcessedItemsTotal.WithLabelValues(workerID)), float64(9))
	assert.Greater(t, testutil.ToFloat64(PartitionsAssignedTotal), float64(0))

	// 验证错误计数
	assert.Greater(t, testutil.ToFloat64(NodeTasksErrorsTotal.WithLabelValues(workerID, "connection_timeout")), float64(0))
	assert.Greater(t, testutil.ToFloat64(NodeCircuitBreakerTrippedTotal.WithLabelValues(workerID, "db-connection")), float64(0))

	// 验证直方图有数据
	assert.Greater(t, testutil.CollectAndCount(NodeTaskProcessingDurationSeconds), 0)
	assert.Greater(t, testutil.CollectAndCount(NodeTaskProcessorDurationSeconds), 0)
	assert.Greater(t, testutil.CollectAndCount(PartitionsAssignmentDurationSeconds), 0)
}

// 基准测试
func BenchmarkMetricsManager_SetLeaderStatus(b *testing.B) {
	mm := NewMetricsManager()
	nodeID := "bench-node"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mm.SetLeaderStatus(nodeID, i%2 == 0)
	}
}

func BenchmarkMetricsManager_IncTasksProcessed(b *testing.B) {
	mm := NewMetricsManager()
	workerID := "bench-worker"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mm.IncTasksProcessed(workerID)
	}
}

func BenchmarkMetricsManager_ObserveTaskProcessingDuration(b *testing.B) {
	mm := NewMetricsManager()
	workerID := "bench-worker"
	duration := time.Millisecond * 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mm.ObserveTaskProcessingDuration(workerID, duration)
	}
}

func BenchmarkMetricsManager_Disabled(b *testing.B) {
	mm := NewMetricsManager()
	mm.SetEnabled(false)
	workerID := "bench-worker"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mm.IncTasksProcessed(workerID)
	}
}
