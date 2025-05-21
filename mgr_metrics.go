package elk_coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// 指标相关方法

// publishMetricsPeriodically 定期发布工作节点性能指标
func (m *Mgr) publishMetricsPeriodically(ctx context.Context) {
	// 创建指标管理器
	metricsManager := NewMetricsManager(m)

	ticker := time.NewTicker(m.MetricsUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 发布最新指标
			if err := metricsManager.PublishMetrics(ctx); err != nil {
				m.Logger.Warnf("发布性能指标失败: %v", err)
			}
		}
	}
}

// recordTaskPerformance 记录任务处理性能
func (m *Mgr) recordTaskPerformance(startTime time.Time, itemCount int64, err error, partitionID int) {
	if !m.UseTaskMetrics {
		return
	}

	// 任务处理时间
	duration := time.Since(startTime)

	// 创建临时指标管理器
	metricsManager := NewMetricsManager(m)

	// 记录任务指标
	isSuccess := err == nil
	metricsManager.RecordTaskMetrics(duration, itemCount, isSuccess)

	// 更新总任务计数
	m.metricsMutex.Lock()
	m.Metrics.TotalTasksCompleted++
	if isSuccess {
		m.Metrics.SuccessfulTasks++
	}
	m.Metrics.TotalItemsProcessed += itemCount

	// 记录分区处理速度
	if isSuccess && itemCount > 0 {
		// 计算处理速度 (items/sec)
		speed := float64(itemCount) / duration.Seconds()

		// 创建分区处理速度记录
		partitionSpeed := PartitionSpeed{
			PartitionID:   partitionID,
			Speed:         speed,
			ItemCount:     itemCount,
			ProcessedTime: duration,
			ProcessedAt:   time.Now(),
		}

		// 添加到最近处理的分区列表
		recentSpeeds := m.Metrics.RecentPartitionSpeeds
		if recentSpeeds == nil {
			recentSpeeds = make([]PartitionSpeed, 0, m.RecentPartitionsToTrack)
		}

		// 添加到列表头部，保持最新的记录在前面
		recentSpeeds = append([]PartitionSpeed{partitionSpeed}, recentSpeeds...)

		// 限制列表长度，只保留最近的N个
		maxToKeep := m.RecentPartitionsToTrack
		if maxToKeep <= 0 {
			maxToKeep = DefaultRecentPartitions
		}

		if len(recentSpeeds) > maxToKeep {
			recentSpeeds = recentSpeeds[:maxToKeep]
		}

		// 更新指标中的分区速度列表
		m.Metrics.RecentPartitionSpeeds = recentSpeeds
	}

	m.metricsMutex.Unlock()

	// 记录详细的性能日志
	if isSuccess {
		m.Logger.Debugf("任务处理性能: 分区=%d, 处理项=%d, 耗时=%v, 处理速度=%.2f项/秒",
			partitionID, itemCount, duration, float64(itemCount)/duration.Seconds())
	} else {
		m.Logger.Debugf("任务处理失败: 分区=%d, 处理项=%d, 耗时=%v, 错误=%v",
			partitionID, itemCount, duration, err)
	}
}

// GetCurrentMetrics 获取当前工作节点的最新性能指标
func (m *Mgr) GetCurrentMetrics() WorkerMetrics {
	m.metricsMutex.RLock()
	defer m.metricsMutex.RUnlock()
	return *m.Metrics
}

// CalculateCapacityScore 计算工作节点容量评分
// 评分越高表示节点处理能力越强
func (m *Mgr) CalculateCapacityScore() float64 {
	m.metricsMutex.RLock()
	defer m.metricsMutex.RUnlock()

	if m.Metrics.ProcessingSpeed <= 0 {
		// 如果没有足够的指标数据，返回默认值
		return 1.0
	}

	// 计算基于处理速度和成功率的综合评分
	// 评分根据处理速度和成功率的加权平均计算
	processingWeight := 0.7
	successRateWeight := 0.3

	// 处理速度得分 - 线性相关，速度越快得分越高
	speedScore := m.Metrics.ProcessingSpeed

	// 成功率得分 - 成功率越高得分越高
	// 为了放大影响，对低成功率进行惩罚
	successScore := 0.0
	if m.Metrics.SuccessRate >= 0.95 { // 95%以上成功率认为是优秀的
		successScore = m.Metrics.SuccessRate * 2
	} else {
		successScore = m.Metrics.SuccessRate
	}

	// 综合评分
	capacityScore := (speedScore * processingWeight) + (successScore * successRateWeight)
	return capacityScore
}

// GetCapacityInfo 获取工作节点当前的容量信息
func (m *Mgr) GetCapacityInfo() WorkerCapacityInfo {
	m.metricsMutex.RLock()
	metrics := *m.Metrics
	m.metricsMutex.RUnlock()

	return WorkerCapacityInfo{
		WorkerID:        m.ID,
		CapacityScore:   m.CalculateCapacityScore(),
		ProcessingSpeed: metrics.ProcessingSpeed,
		SuccessRate:     metrics.SuccessRate,
		UpdateTime:      time.Now(),
	}
}

// SetMetricsUpdateInterval 设置指标更新间隔
func (m *Mgr) SetMetricsUpdateInterval(interval time.Duration) {
	if interval < time.Second {
		m.Logger.Warnf("指标更新间隔过短: %v，使用默认值 30s", interval)
		interval = 30 * time.Second
	}
	m.MetricsUpdateInterval = interval
}

// ResetMetrics 重置节点性能指标
func (m *Mgr) ResetMetrics() {
	m.metricsMutex.Lock()
	defer m.metricsMutex.Unlock()

	m.Metrics = &WorkerMetrics{
		ProcessingSpeed:     0.0,
		SuccessRate:         1.0, // 初始默认100%成功率
		AvgProcessingTime:   0,
		TotalTasksCompleted: 0,
		SuccessfulTasks:     0,
		TotalItemsProcessed: 0,
		LastUpdateTime:      time.Now(),
	}

	m.Logger.Infof("工作节点性能指标已重置")
}

// GetAllWorkersMetrics 获取所有工作节点的性能指标
func (m *Mgr) GetAllWorkersMetrics(ctx context.Context) (map[string]WorkerMetrics, error) {
	// 获取所有工作节点的指标键
	metricsPattern := fmt.Sprintf("%s:metrics:*", m.Namespace)
	metricsKeys, err := m.DataStore.GetKeys(ctx, metricsPattern)
	if err != nil {
		return nil, fmt.Errorf("获取指标键失败: %v", err)
	}

	result := make(map[string]WorkerMetrics)

	for _, key := range metricsKeys {
		// 获取每个节点的指标数据
		data, err := m.DataStore.GetSyncStatus(ctx, key)
		if err != nil {
			m.Logger.Warnf("获取指标数据失败 (键=%s): %v", key, err)
			continue
		}

		var workerStatus struct {
			WorkerID string        `json:"worker_id"`
			Metrics  WorkerMetrics `json:"metrics"`
		}

		if err := json.Unmarshal([]byte(data), &workerStatus); err != nil {
			m.Logger.Warnf("解析指标数据失败: %v", err)
			continue
		}

		// 只保存最近更新的指标
		if time.Since(workerStatus.Metrics.LastUpdateTime) < m.MetricsUpdateInterval*3 {
			result[workerStatus.WorkerID] = workerStatus.Metrics
		}
	}

	return result, nil
}
