package leader

import (
	"context"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/metrics"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/utils"
)

// WorkManager 负责管理和协调分区分配相关的工作
type WorkManager struct {
	config WorkManagerConfig
}

// WorkManagerConfig 工作管理器配置
type WorkManagerConfig struct {
	NodeID    string
	Namespace string
	DataStore interface {
		data.KeyOperations
		data.HeartbeatOperations
		data.WorkerRegistry
	}
	Logger                 utils.Logger
	ValidHeartbeatDuration time.Duration // 有效心跳持续时间
	AllocationInterval     time.Duration // 分区分配检查间隔，默认30秒
}

// NewWorkManager 创建新的工作管理器
func NewWorkManager(config WorkManagerConfig) *WorkManager {
	// 如果未设置分配间隔，使用默认值
	if config.AllocationInterval <= 0 {
		config.AllocationInterval = model.DefaultAllocationInterval
	}

	return &WorkManager{
		config: config,
	}
}

// RunPartitionAllocationLoop 运行分区分配循环
func (wm *WorkManager) RunPartitionAllocationLoop(ctx context.Context, leaderCtx context.Context, pm *PartitionAssigner) error {
	// 初始运行一次分区分配，确保刚启动时有任务可执行
	go wm.tryAllocatePartitions(ctx, pm)

	// 使用配置的分配间隔
	ticker := time.NewTicker(wm.config.AllocationInterval)
	defer ticker.Stop()

	// 持续分配分区任务
	for {
		select {
		case <-ctx.Done():
			wm.config.Logger.Infof("Leader工作停止 (上下文取消)")
			return ctx.Err()
		case <-leaderCtx.Done():
			wm.config.Logger.Infof("Leader工作停止 (不再是Leader)")
			return nil
		case <-ticker.C:
			// 检查分区状态并按需分配新分区
			wm.tryAllocatePartitions(ctx, pm)
		}
	}
}

// tryAllocatePartitions 尝试分配分区并处理错误
func (wm *WorkManager) tryAllocatePartitions(ctx context.Context, pm *PartitionAssigner) {
	// 检查是否有活跃节点，如果没有，则不分配分区
	activeWorkers, err := wm.getActiveWorkers(ctx)
	if err != nil {
		wm.config.Logger.Warnf("获取活跃节点失败: %v", err)
		return
	}

	wm.config.Logger.Debugf("获取到的活跃工作节点: %v (数量: %d)", activeWorkers, len(activeWorkers))

	// 记录活跃工作节点数量指标
	metrics.SetActiveWorkersCount(float64(len(activeWorkers)))

	if len(activeWorkers) == 0 {
		wm.config.Logger.Debugf("没有活跃工作节点，跳过分区分配")
		return
	}

	// 尝试分配分区
	if err = pm.AllocatePartitions(ctx, activeWorkers); err != nil {
		wm.config.Logger.Warnf("分配分区失败: %v", err)
	}
}

// getActiveWorkers 获取活跃节点列表
func (wm *WorkManager) getActiveWorkers(ctx context.Context) ([]string, error) {
	return wm.config.DataStore.GetActiveWorkers(ctx)
}
