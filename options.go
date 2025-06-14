package elk_coordinator

import (
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"time"
)

// MgrOption 定义管理器的配置选项
type MgrOption func(*Mgr)

// WithLogger 设置自定义日志记录器
func WithLogger(logger utils.Logger) MgrOption {
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
			multiple = model.DefaultWorkerPartitionMultiple
		}
		m.WorkerPartitionMultiple = multiple
	}
}

// WithTaskWindow 设置任务窗口大小
func WithTaskWindow(windowSize int) MgrOption {
	return func(m *Mgr) {
		if windowSize <= 0 {
			windowSize = model.DefaultTaskWindowSize
		}
		m.TaskWindowSize = windowSize
	}
}

// WithAllocationInterval 设置分区分配检查间隔
func WithAllocationInterval(interval time.Duration) MgrOption {
	return func(m *Mgr) {
		if interval <= 0 {
			interval = model.DefaultAllocationInterval
		}
		m.AllocationInterval = interval
	}
}

// WithAllowPreemption 设置是否允许抢占其他节点的分区
func WithAllowPreemption(allowPreemption bool) MgrOption {
	return func(m *Mgr) {
		m.AllowPreemption = allowPreemption
	}
}

// WithMetricsEnabled 设置是否启用监控系统
func WithMetricsEnabled(enabled bool) MgrOption {
	return func(m *Mgr) {
		m.MetricsEnabled = enabled
		if m.MetricsManager != nil {
			m.MetricsManager.SetEnabled(enabled)
		}
	}
}

// WithMetricsAddr 设置监控服务地址
func WithMetricsAddr(addr string) MgrOption {
	return func(m *Mgr) {
		m.MetricsAddr = addr
	}
}
