package elk_coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// MetricsManager 管理工作节点性能指标的收集和发布
type MetricsManager struct {
	mgr            *Mgr
	taskTimes      []time.Duration // 任务处理时间窗口
	taskSuccesses  []bool          // 任务成功状态窗口
	taskItemsCount []int64         // 每个任务处理的项目数窗口
	metricsMutex   sync.RWMutex
	metricsKey     string // 存储节点指标的键
}

// NewMetricsManager 创建一个新的指标管理器
func NewMetricsManager(mgr *Mgr) *MetricsManager {
	metricsKey := fmt.Sprintf("%s:metrics:%s", mgr.Namespace, mgr.ID)
	return &MetricsManager{
		mgr:            mgr,
		taskTimes:      make([]time.Duration, 0, MetricsWindowSize),
		taskSuccesses:  make([]bool, 0, MetricsWindowSize),
		taskItemsCount: make([]int64, 0, MetricsWindowSize),
		metricsKey:     metricsKey,
	}
}

// RecordTaskMetrics 记录单个任务的性能指标
func (mm *MetricsManager) RecordTaskMetrics(processTime time.Duration, itemCount int64, isSuccess bool) {
	mm.metricsMutex.Lock()
	defer mm.metricsMutex.Unlock()

	// 添加到时间窗口
	mm.taskTimes = append(mm.taskTimes, processTime)
	if len(mm.taskTimes) > MetricsWindowSize {
		mm.taskTimes = mm.taskTimes[1:]
	}

	// 添加到成功窗口
	mm.taskSuccesses = append(mm.taskSuccesses, isSuccess)
	if len(mm.taskSuccesses) > MetricsWindowSize {
		mm.taskSuccesses = mm.taskSuccesses[1:]
	}

	// 添加到项目数窗口
	mm.taskItemsCount = append(mm.taskItemsCount, itemCount)
	if len(mm.taskItemsCount) > MetricsWindowSize {
		mm.taskItemsCount = mm.taskItemsCount[1:]
	}

	// 更新全局指标
	mm.updateGlobalMetrics()
}

// updateGlobalMetrics 根据窗口数据更新全局指标
func (mm *MetricsManager) updateGlobalMetrics() {
	if len(mm.taskTimes) == 0 {
		return
	}

	// 计算平均处理时间
	var totalTime time.Duration
	for _, t := range mm.taskTimes {
		totalTime += t
	}
	avgTime := totalTime / time.Duration(len(mm.taskTimes))

	// 计算成功率
	successCount := 0
	for _, success := range mm.taskSuccesses {
		if success {
			successCount++
		}
	}
	successRate := float64(successCount) / float64(len(mm.taskSuccesses))

	// 计算处理速度 (items/sec)
	var totalItems int64
	for _, count := range mm.taskItemsCount {
		totalItems += count
	}

	var processingSpeed float64
	if totalTime > 0 {
		processingSpeed = float64(totalItems) / totalTime.Seconds()
	}

	// 更新管理器全局指标
	mm.mgr.metricsMutex.Lock()
	defer mm.mgr.metricsMutex.Unlock()

	// 获取现有的总计数
	totalTasksCompleted := mm.mgr.Metrics.TotalTasksCompleted + 1
	successfulTasks := mm.mgr.Metrics.SuccessfulTasks
	if isSuccess := successCount > 0; isSuccess {
		successfulTasks++
	}
	totalItemsProcessed := mm.mgr.Metrics.TotalItemsProcessed + totalItems

	// 更新新的指标
	mm.mgr.Metrics = &WorkerMetrics{
		ProcessingSpeed:     processingSpeed,
		SuccessRate:         successRate,
		AvgProcessingTime:   avgTime,
		TotalTasksCompleted: totalTasksCompleted,
		SuccessfulTasks:     successfulTasks,
		TotalItemsProcessed: totalItemsProcessed,
		LastUpdateTime:      time.Now(),
	}
}

// PublishMetrics 发布最新的性能指标到共享存储
func (mm *MetricsManager) PublishMetrics(ctx context.Context) error {
	mm.mgr.metricsMutex.RLock()
	metrics := *mm.mgr.Metrics
	mm.mgr.metricsMutex.RUnlock()

	// 发布前更新总体指标
	workerStatus := map[string]interface{}{
		"worker_id":   mm.mgr.ID,
		"metrics":     metrics,
		"hostname":    getHostname(),
		"update_time": time.Now(),
	}

	data, err := json.Marshal(workerStatus)
	if err != nil {
		return fmt.Errorf("序列化指标数据失败: %v", err)
	}

	// 使用较长的过期时间，确保指标在节点重启期间仍然可见
	expiry := mm.mgr.MetricsUpdateInterval * 3
	if err := mm.mgr.DataStore.SetKey(ctx, mm.metricsKey, string(data), expiry); err != nil {
		return fmt.Errorf("发布指标数据失败: %v", err)
	}

	mm.mgr.Logger.Debugf("已发布工作节点指标: 处理速度=%.2f项/秒, 成功率=%.1f%%, 平均处理时间=%v",
		metrics.ProcessingSpeed, metrics.SuccessRate*100, metrics.AvgProcessingTime)

	return nil
}

// getHostname 获取当前主机名，用于标识节点
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return hostname
}
