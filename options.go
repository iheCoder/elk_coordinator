package elk_coordinator

import (
	"time"
)

// MgrOption 定义管理器的配置选项
type MgrOption func(*Mgr)

// WithLogger 设置自定义日志记录器
func WithLogger(logger Logger) MgrOption {
	return func(m *Mgr) {
		m.Logger = logger
	}
}

// WithHeartbeatInterval 设置心跳间隔
func WithHeartbeatInterval(interval time.Duration) MgrOption {
	return func(m *Mgr) {
		if interval < time.Second {
			// 设置最小心跳间隔为1秒
			interval = time.Second
		}
		m.HeartbeatInterval = interval
	}
}

// WithLeaderElectionInterval 设置Leader选举间隔
func WithLeaderElectionInterval(interval time.Duration) MgrOption {
	return func(m *Mgr) {
		if interval < time.Second {
			interval = time.Second
		}
		m.LeaderElectionInterval = interval
	}
}

// WithPartitionLockExpiry 设置分区锁过期时间
func WithPartitionLockExpiry(expiry time.Duration) MgrOption {
	return func(m *Mgr) {
		if expiry < time.Second {
			expiry = time.Second
		}
		m.PartitionLockExpiry = expiry
	}
}

// WithLeaderLockExpiry 设置Leader锁过期时间
func WithLeaderLockExpiry(expiry time.Duration) MgrOption {
	return func(m *Mgr) {
		if expiry < time.Second {
			expiry = time.Second
		}
		m.LeaderLockExpiry = expiry
	}
}

// WithWorkerPartitionMultiple 设置工作节点分区倍数
func WithWorkerPartitionMultiple(multiple int64) MgrOption {
	return func(m *Mgr) {
		if multiple <= 0 {
			multiple = DefaultWorkerPartitionMultiple
		}
		m.WorkerPartitionMultiple = multiple
	}
}

// WithTaskWindow 启用任务窗口及其配置
func WithTaskWindow(windowSize int) MgrOption {
	return func(m *Mgr) {
		m.UseTaskWindow = true
		if windowSize <= 0 {
			windowSize = DefaultTaskWindowSize
		}
		m.TaskWindowSize = windowSize
	}
}

// WithTaskMetrics 启用任务指标收集及其配置
func WithTaskMetrics(updateInterval time.Duration, recentPartitionsToTrack int) MgrOption {
	return func(m *Mgr) {
		m.UseTaskMetrics = true

		if updateInterval < time.Second {
			updateInterval = DefaultCapacityUpdateInterval
		}
		m.MetricsUpdateInterval = updateInterval

		if recentPartitionsToTrack <= 0 {
			recentPartitionsToTrack = DefaultRecentPartitions
		}
		m.RecentPartitionsToTrack = recentPartitionsToTrack
	}
}

// DisableTaskMetrics 禁用任务指标收集
func DisableTaskMetrics() MgrOption {
	return func(m *Mgr) {
		m.UseTaskMetrics = false
	}
}

// WithMetricsUpdateInterval 设置指标更新间隔
func WithMetricsUpdateInterval(interval time.Duration) MgrOption {
	return func(m *Mgr) {
		if interval < time.Second {
			interval = 30 * time.Second
		}
		m.MetricsUpdateInterval = interval
	}
}

// WithRecentPartitionsTracking 设置要记录的最近分区数量
func WithRecentPartitionsTracking(count int) MgrOption {
	return func(m *Mgr) {
		if count <= 0 {
			count = DefaultRecentPartitions
		}
		m.RecentPartitionsToTrack = count
	}
}

// WithStaleTaskReclaimMultiplier sets the multiplier for PartitionLockExpiry to determine when a task is considered stale.
// A stale task (Claimed or Running but not updated for StaleTaskReclaimMultiplier * PartitionLockExpiry)
// can be reclaimed by another worker. The default multiplier is DefaultStaleTaskReclaimMultiplier (e.g., 3.0).
func WithStaleTaskReclaimMultiplier(multiplier float64) MgrOption {
	return func(m *Mgr) {
		if multiplier <= 0 {
			// Logger might not be initialized yet if this option is set before WithLogger.
			// So, we can't use m.Logger here reliably without checking for nil.
			// For simplicity, we'll just set to default. A more robust solution might queue warnings.
			m.StaleTaskReclaimMultiplier = DefaultStaleTaskReclaimMultiplier
		} else {
			m.StaleTaskReclaimMultiplier = multiplier
		}
	}
}
