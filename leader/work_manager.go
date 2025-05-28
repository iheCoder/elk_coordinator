package leader

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"fmt"
	"github.com/pkg/errors"
	"time"
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
	}
	Logger                  utils.Logger
	WorkerPartitionMultiple int64         // 每个工作节点分配的分区倍数
	ValidHeartbeatDuration  time.Duration // 有效心跳持续时间
}

// NewWorkManager 创建新的工作管理器
func NewWorkManager(config WorkManagerConfig) *WorkManager {
	return &WorkManager{
		config: config,
	}
}

// RunPartitionAllocationLoop 运行分区分配循环
func (wm *WorkManager) RunPartitionAllocationLoop(ctx context.Context, leaderCtx context.Context, pm *PartitionManager) error {
	// 初始运行一次分区分配，确保刚启动时有任务可执行
	go wm.tryAllocatePartitions(ctx, pm)

	// 使用较长时间间隔进行常规分区检查和分配
	allocationInterval := 2 * time.Minute // 使用适中的间隔，可以根据需求调整
	ticker := time.NewTicker(allocationInterval)
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
func (wm *WorkManager) tryAllocatePartitions(ctx context.Context, pm *PartitionManager) {
	// 检查是否有活跃节点，如果没有，则不分配分区
	activeWorkers, err := wm.getActiveWorkers(ctx)
	if err != nil {
		wm.config.Logger.Warnf("获取活跃节点失败: %v", err)
		return
	}

	if len(activeWorkers) == 0 {
		wm.config.Logger.Debugf("没有活跃工作节点，跳过分区分配")
		return
	}

	// 尝试分配分区
	if err = pm.AllocatePartitions(ctx, activeWorkers, wm.config.WorkerPartitionMultiple); err != nil {
		wm.config.Logger.Warnf("分配分区失败: %v", err)
	}
}

// getActiveWorkers 获取活跃节点列表
func (wm *WorkManager) getActiveWorkers(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf(model.HeartbeatFmtFmt, wm.config.Namespace, "*")
	keys, err := wm.config.DataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳失败")
	}

	var activeWorkers []string
	now := time.Now()
	validHeartbeatDuration := wm.config.ValidHeartbeatDuration

	for _, key := range keys {
		// 从key中提取节点ID
		prefix := fmt.Sprintf(model.HeartbeatFmtFmt, wm.config.Namespace, "")
		nodeID := key[len(prefix):]

		// 获取最后心跳时间
		lastHeartbeatStr, err := wm.config.DataStore.GetHeartbeat(ctx, key)
		if err != nil {
			continue // 跳过错误的心跳
		}

		lastHeartbeat, err := time.Parse(time.RFC3339, lastHeartbeatStr)
		if err != nil {
			continue // 跳过无效的时间格式
		}

		// 检查心跳是否有效
		if now.Sub(lastHeartbeat) <= validHeartbeatDuration {
			activeWorkers = append(activeWorkers, nodeID)
		} else {
			// 删除过期心跳
			wm.config.DataStore.DeleteKey(ctx, key)
		}
	}

	return activeWorkers, nil
}
