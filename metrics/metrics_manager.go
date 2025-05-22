package metrics

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// MetricsManager 管理工作节点性能指标的收集和发布
type MetricsManager struct {
	// 基本信息
	nodeID     string
	namespace  string
	metricsKey string

	// 依赖项
	dataStore data.DataStore
	logger    utils.Logger

	// 配置参数
	updateInterval          time.Duration
	recentPartitionsToTrack int

	// 指标数据
	metrics      *model.WorkerMetrics
	metricsMutex sync.RWMutex

	// 指标计算窗口
	taskTimes      []time.Duration
	taskSuccesses  []bool
	taskItemsCount []int64

	// 发布控制
	publishCtx    context.Context
	cancelPublish context.CancelFunc
	isPublishing  bool
	publishMutex  sync.RWMutex
}

// NewMetricsManager 创建一个新的指标管理器
func NewMetricsManager(config MetricsConfig) *MetricsManager {
	metricsKey := fmt.Sprintf("%s:metrics:%s", config.Namespace, config.NodeID)

	// 确保合理的默认值
	if config.UpdateInterval <= 0 {
		config.UpdateInterval = model.DefaultCapacityUpdateInterval
	}

	if config.RecentPartitionsToTrack <= 0 {
		config.RecentPartitionsToTrack = model.DefaultRecentPartitions
	}

	if config.Logger == nil {
		config.Logger = utils.NewDefaultLogger()
	}

	return &MetricsManager{
		// 基本信息
		nodeID:     config.NodeID,
		namespace:  config.Namespace,
		metricsKey: metricsKey,

		// 依赖项
		dataStore: config.DataStore,
		logger:    config.Logger,

		// 配置参数
		updateInterval:          config.UpdateInterval,
		recentPartitionsToTrack: config.RecentPartitionsToTrack,

		// 指标数据
		metrics: &model.WorkerMetrics{
			ProcessingSpeed:       0.0,
			SuccessRate:           1.0,
			AvgProcessingTime:     0,
			TotalTasksCompleted:   0,
			SuccessfulTasks:       0,
			TotalItemsProcessed:   0,
			LastUpdateTime:        time.Now(),
			RecentPartitionSpeeds: nil,
		},

		// 指标计算窗口
		taskTimes:      make([]time.Duration, 0, model.MetricsWindowSize),
		taskSuccesses:  make([]bool, 0, model.MetricsWindowSize),
		taskItemsCount: make([]int64, 0, model.MetricsWindowSize),
	}
}

// MetricsConfig 用于配置MetricsManager的参数
type MetricsConfig struct {
	// 必需参数
	NodeID    string
	Namespace string
	DataStore data.DataStore

	// 可选参数（有默认值）
	Logger                  utils.Logger
	UpdateInterval          time.Duration
	RecentPartitionsToTrack int
}

// Start 启动指标管理器的定期发布功能
func (mm *MetricsManager) Start(ctx context.Context) {
	mm.publishMutex.Lock()
	defer mm.publishMutex.Unlock()

	if mm.isPublishing {
		return
	}

	mm.publishCtx, mm.cancelPublish = context.WithCancel(ctx)
	mm.isPublishing = true

	go mm.publishMetricsPeriodically()
}

// Stop 停止指标管理器的定期发布
func (mm *MetricsManager) Stop() {
	mm.publishMutex.Lock()
	defer mm.publishMutex.Unlock()

	if !mm.isPublishing {
		return
	}

	mm.cancelPublish()
	mm.isPublishing = false
}

// RecordTaskResult 记录单个任务的执行结果（简化接口，仅需调用一次）
func (mm *MetricsManager) RecordTaskResult(partitionID int, startTime time.Time, itemCount int64, err error) {
	// 计算处理时间
	duration := time.Since(startTime)
	isSuccess := err == nil

	// 更新指标窗口
	mm.updateMetricsWindow(duration, itemCount, isSuccess)

	// 更新全局指标
	mm.updateGlobalMetrics()

	// 如果成功且有处理项，记录分区处理速度
	if isSuccess && itemCount > 0 {
		mm.updatePartitionSpeed(partitionID, duration, itemCount)
	}

	// 记录详细的性能日志
	if isSuccess {
		mm.logger.Debugf("任务处理性能: 分区=%d, 处理项=%d, 耗时=%v, 处理速度=%.2f项/秒",
			partitionID, itemCount, duration, float64(itemCount)/duration.Seconds())
	} else {
		mm.logger.Debugf("任务处理失败: 分区=%d, 处理项=%d, 耗时=%v, 错误=%v",
			partitionID, itemCount, duration, err)
	}
}

// 更新指标窗口数据（内部方法）
func (mm *MetricsManager) updateMetricsWindow(processTime time.Duration, itemCount int64, isSuccess bool) {
	mm.metricsMutex.Lock()
	defer mm.metricsMutex.Unlock()

	// 添加到时间窗口
	mm.taskTimes = append(mm.taskTimes, processTime)
	if len(mm.taskTimes) > model.MetricsWindowSize {
		mm.taskTimes = mm.taskTimes[1:]
	}

	// 添加到成功窗口
	mm.taskSuccesses = append(mm.taskSuccesses, isSuccess)
	if len(mm.taskSuccesses) > model.MetricsWindowSize {
		mm.taskSuccesses = mm.taskSuccesses[1:]
	}

	// 添加到项目数窗口
	mm.taskItemsCount = append(mm.taskItemsCount, itemCount)
	if len(mm.taskItemsCount) > model.MetricsWindowSize {
		mm.taskItemsCount = mm.taskItemsCount[1:]
	}
}

// 更新全局指标（内部方法）
func (mm *MetricsManager) updateGlobalMetrics() {
	mm.metricsMutex.Lock()
	defer mm.metricsMutex.Unlock()

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

	// 获取现有的总计数
	totalTasksCompleted := mm.metrics.TotalTasksCompleted + 1
	successfulTasks := mm.metrics.SuccessfulTasks
	if successCount > 0 {
		successfulTasks++
	}
	totalItemsProcessed := mm.metrics.TotalItemsProcessed + totalItems

	// 保留之前的分区速度记录
	recentPartitionSpeeds := mm.metrics.RecentPartitionSpeeds

	// 更新新的指标
	mm.metrics = &model.WorkerMetrics{
		ProcessingSpeed:       processingSpeed,
		SuccessRate:           successRate,
		AvgProcessingTime:     avgTime,
		TotalTasksCompleted:   totalTasksCompleted,
		SuccessfulTasks:       successfulTasks,
		TotalItemsProcessed:   totalItemsProcessed,
		LastUpdateTime:        time.Now(),
		RecentPartitionSpeeds: recentPartitionSpeeds,
	}
}

// 更新分区处理速度（内部方法）
func (mm *MetricsManager) updatePartitionSpeed(partitionID int, duration time.Duration, itemCount int64) {
	mm.metricsMutex.Lock()
	defer mm.metricsMutex.Unlock()

	// 计算处理速度 (items/sec)
	speed := float64(itemCount) / duration.Seconds()

	// 创建分区处理速度记录
	partitionSpeed := model.PartitionSpeed{
		PartitionID:   partitionID,
		Speed:         speed,
		ItemCount:     itemCount,
		ProcessedTime: duration,
		ProcessedAt:   time.Now(),
	}

	// 添加到最近处理的分区列表
	recentSpeeds := mm.metrics.RecentPartitionSpeeds
	if recentSpeeds == nil {
		recentSpeeds = make([]model.PartitionSpeed, 0, mm.recentPartitionsToTrack)
	}

	// 添加到列表头部，保持最新的记录在前面
	recentSpeeds = append([]model.PartitionSpeed{partitionSpeed}, recentSpeeds...)

	// 限制列表长度，只保留最近的N个
	if len(recentSpeeds) > mm.recentPartitionsToTrack {
		recentSpeeds = recentSpeeds[:mm.recentPartitionsToTrack]
	}

	// 更新指标中的分区速度列表
	mm.metrics.RecentPartitionSpeeds = recentSpeeds
}

// publishMetricsPeriodically 定期发布工作节点性能指标
func (mm *MetricsManager) publishMetricsPeriodically() {
	ticker := time.NewTicker(mm.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.publishCtx.Done():
			return
		case <-ticker.C:
			if err := mm.PublishMetrics(); err != nil {
				mm.logger.Warnf("发布性能指标失败: %v", err)
			}
		}
	}
}

// PublishMetrics 发布最新的性能指标到共享存储
func (mm *MetricsManager) PublishMetrics() error {
	mm.metricsMutex.RLock()
	metrics := *mm.metrics
	mm.metricsMutex.RUnlock()

	// 发布前更新总体指标
	workerStatus := map[string]interface{}{
		"worker_id":   mm.nodeID,
		"metrics":     metrics,
		"hostname":    getHostname(),
		"update_time": time.Now(),
	}

	data, err := json.Marshal(workerStatus)
	if err != nil {
		return fmt.Errorf("序列化指标数据失败: %v", err)
	}

	// 使用父上下文的超时控制
	ctx, cancel := context.WithTimeout(context.Background(), mm.updateInterval/2)
	defer cancel()

	// 使用较长的过期时间，确保指标在节点重启期间仍然可见
	expiry := mm.updateInterval * 3
	if err := mm.dataStore.SetKey(ctx, mm.metricsKey, string(data), expiry); err != nil {
		return fmt.Errorf("发布指标数据失败: %v", err)
	}

	mm.logger.Debugf("已发布工作节点指标: 处理速度=%.2f项/秒, 成功率=%.1f%%, 平均处理时间=%v",
		metrics.ProcessingSpeed, metrics.SuccessRate*100, metrics.AvgProcessingTime)

	return nil
}

// GetMetrics 获取当前工作节点的最新性能指标
func (mm *MetricsManager) GetMetrics() model.WorkerMetrics {
	mm.metricsMutex.RLock()
	defer mm.metricsMutex.RUnlock()
	return *mm.metrics
}

// CalculateCapacityScore 计算工作节点容量评分
func (mm *MetricsManager) CalculateCapacityScore() float64 {
	mm.metricsMutex.RLock()
	defer mm.metricsMutex.RUnlock()

	if mm.metrics.ProcessingSpeed <= 0 {
		// 如果没有足够的指标数据，返回默认值
		return 1.0
	}

	// 计算基于处理速度和成功率的综合评分
	processingWeight := 0.7
	successRateWeight := 0.3

	// 处理速度得分
	speedScore := mm.metrics.ProcessingSpeed

	// 成功率得分
	successScore := 0.0
	if mm.metrics.SuccessRate >= 0.95 {
		successScore = mm.metrics.SuccessRate * 2
	} else {
		successScore = mm.metrics.SuccessRate
	}

	// 综合评分
	return (speedScore * processingWeight) + (successScore * successRateWeight)
}

// GetCapacityInfo 获取工作节点当前的容量信息
func (mm *MetricsManager) GetCapacityInfo() model.WorkerCapacityInfo {
	mm.metricsMutex.RLock()
	metrics := *mm.metrics
	mm.metricsMutex.RUnlock()

	return model.WorkerCapacityInfo{
		WorkerID:        mm.nodeID,
		CapacityScore:   mm.CalculateCapacityScore(),
		ProcessingSpeed: metrics.ProcessingSpeed,
		SuccessRate:     metrics.SuccessRate,
		UpdateTime:      time.Now(),
	}
}

// ResetMetrics 重置节点性能指标
func (mm *MetricsManager) ResetMetrics() {
	mm.metricsMutex.Lock()
	defer mm.metricsMutex.Unlock()

	mm.metrics = &model.WorkerMetrics{
		ProcessingSpeed:       0.0,
		SuccessRate:           1.0,
		AvgProcessingTime:     0,
		TotalTasksCompleted:   0,
		SuccessfulTasks:       0,
		TotalItemsProcessed:   0,
		LastUpdateTime:        time.Now(),
		RecentPartitionSpeeds: nil,
	}

	mm.taskTimes = make([]time.Duration, 0, model.MetricsWindowSize)
	mm.taskSuccesses = make([]bool, 0, model.MetricsWindowSize)
	mm.taskItemsCount = make([]int64, 0, model.MetricsWindowSize)

	mm.logger.Infof("工作节点性能指标已重置")
}

// GetAllWorkersMetrics 获取所有工作节点的性能指标
func (mm *MetricsManager) GetAllWorkersMetrics(ctx context.Context) (map[string]model.WorkerMetrics, error) {
	// 获取所有工作节点的指标键
	metricsPattern := fmt.Sprintf("%s:metrics:*", mm.namespace)
	metricsKeys, err := mm.dataStore.GetKeys(ctx, metricsPattern)
	if err != nil {
		return nil, fmt.Errorf("获取指标键失败: %v", err)
	}

	result := make(map[string]model.WorkerMetrics)

	for _, key := range metricsKeys {
		// 获取每个节点的指标数据
		data, err := mm.dataStore.GetKey(ctx, key)
		if err != nil {
			mm.logger.Warnf("获取指标数据失败 (键=%s): %v", key, err)
			continue
		}

		var workerStatus struct {
			WorkerID string              `json:"worker_id"`
			Metrics  model.WorkerMetrics `json:"metrics"`
		}

		if err := json.Unmarshal([]byte(data), &workerStatus); err != nil {
			mm.logger.Warnf("解析指标数据失败: %v", err)
			continue
		}

		// 只保存最近更新的指标
		if time.Since(workerStatus.Metrics.LastUpdateTime) < mm.updateInterval*3 {
			result[workerStatus.WorkerID] = workerStatus.Metrics
		}
	}

	return result, nil
}

// getHostname 获取当前主机名，用于标识节点
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return hostname
}
