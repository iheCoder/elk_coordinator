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
