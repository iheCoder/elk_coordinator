package leader

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/metrics"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/partition"
	"github.com/iheCoder/elk_coordinator/utils"
	"sync"
	"time"
)

// LeaderManager 管理选举过程和领导者行为
type LeaderManager struct {
	// 核心依赖
	election         *Election          // 选举管理组件
	workManager      *WorkManager       // 工作管理组件
	partitionMgr     *PartitionAssigner // 分区管理组件
	commandProcessor *CommandProcessor  // 命令处理组件
	isLeader         bool
	leaderCtx        context.Context
	cancelLeader     context.CancelFunc
	mu               sync.RWMutex

	// 用于整个 LeaderManager 生命周期控制
	stopOnce sync.Once
	stopped  chan struct{}
}

// NewLeaderManager 创建一个新的领导者管理器
func NewLeaderManager(config LeaderConfig) *LeaderManager {
	// 创建并配置选举管理器
	election := NewElection(ElectionConfig{
		NodeID:           config.NodeID,
		Namespace:        config.Namespace,
		DataStore:        config.DataStore,
		Logger:           config.Logger,
		ElectionInterval: config.ElectionInterval,
		LockExpiry:       config.LockExpiry,
	})

	// 创建并配置工作管理器
	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 config.NodeID,
		Namespace:              config.Namespace,
		DataStore:              config.DataStore,
		Logger:                 config.Logger,
		ValidHeartbeatDuration: config.ValidHeartbeatDuration,
		AllocationInterval:     config.AllocationInterval,
	})

	// 创建并配置分区管理器
	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{
		Namespace:               config.Namespace,
		WorkerPartitionMultiple: config.WorkerPartitionMultiple,
	}, config.Strategy, config.Logger, config.Planer)

	// 创建并配置命令处理器
	commandProcessor := NewCommandProcessor(config.Namespace, config.DataStore, config.Strategy, config.Logger)

	return &LeaderManager{
		election:         election,
		workManager:      workManager,
		partitionMgr:     partitionMgr,
		commandProcessor: commandProcessor,
		stopped:          make(chan struct{}),
	}
}

// LeaderConfig 配置领导者管理器
type LeaderConfig struct {
	NodeID    string
	Namespace string
	DataStore interface {
		data.LockOperations
		data.KeyOperations
		data.HeartbeatOperations
		data.SimplePartitionOperations
		data.CommandOperations
	}
	Logger                  utils.Logger
	Planer                  PartitionPlaner
	ElectionInterval        time.Duration
	LockExpiry              time.Duration
	WorkerPartitionMultiple int64
	ValidHeartbeatDuration  time.Duration
	Strategy                partition.PartitionStrategy
	AllocationInterval      time.Duration // 分区分配检查间隔，默认30秒
}

// Start 启动领导者管理
func (lm *LeaderManager) Start(ctx context.Context) error {
	// 初始化领导者工作的上下文
	lm.leaderCtx, lm.cancelLeader = context.WithCancel(context.Background())

	// 运行选举循环
	return lm.runElectionLoop(ctx)
}

// Stop 停止领导者管理
func (lm *LeaderManager) Stop() {
	lm.stopOnce.Do(func() {
		// 先停止领导者工作
		lm.relinquishLeadership()
		// 然后关闭 stopped channel，这会优雅地停止选举循环
		close(lm.stopped)
	})
}

// IsLeader 返回当前节点是否是领导者
func (lm *LeaderManager) IsLeader() bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.isLeader
}

// runElectionLoop 运行选举循环
func (lm *LeaderManager) runElectionLoop(ctx context.Context) error {
	ticker := time.NewTicker(lm.election.config.ElectionInterval)
	defer ticker.Stop()

	// 初次尝试竞选leader
	if elected := lm.election.TryElect(ctx); elected {
		lm.becomeLeader()
		lm.startLeaderWork(ctx)
	}

	// 竞选失败，则周期性判断是否需要重新竞选
	for {
		select {
		case <-ctx.Done():
			lm.election.config.Logger.Infof("Leader选举任务停止 (上下文取消)")
			return ctx.Err()
		case <-lm.stopped:
			lm.election.config.Logger.Infof("Leader选举任务停止 (停止信号)")
			return nil
		case <-ticker.C:
			lm.periodicElection(ctx)
		}
	}
}

// periodicElection 周期性检查是否需要重新竞选
func (lm *LeaderManager) periodicElection(ctx context.Context) {
	// 如果当前不是Leader，尝试重新竞选
	lm.mu.RLock()
	isLeader := lm.isLeader
	lm.mu.RUnlock()

	if !isLeader {
		if elected := lm.election.TryElect(ctx); elected {
			lm.becomeLeader()
			lm.startLeaderWork(ctx)
		}
	}
}

// becomeLeader 设置当前节点为Leader
func (lm *LeaderManager) becomeLeader() {
	lm.election.config.Logger.Infof("成功获取Leader锁，节点 %s 成为Leader", lm.election.config.NodeID)

	lm.mu.Lock()
	lm.isLeader = true
	lm.mu.Unlock()

	// 记录Leader状态指标
	metrics.SetLeaderStatus(lm.election.config.NodeID, true)
}

// startLeaderWork 启动Leader工作
func (lm *LeaderManager) startLeaderWork(ctx context.Context) {
	lm.leaderCtx, lm.cancelLeader = context.WithCancel(context.Background())
	lm.election.config.Logger.Infof("节点 %s 成为Leader，开始Leader工作", lm.election.config.NodeID)

	// 开始领导者工作
	go lm.doLeaderWork(ctx)
}

// doLeaderWork 执行Leader的工作
func (lm *LeaderManager) doLeaderWork(ctx context.Context) error {
	// 1. 异步周期性续leader锁
	go lm.election.StartRenewing(lm.leaderCtx, func() {
		lm.relinquishLeadership()
	})

	// 2. 启动命令处理循环
	go lm.runCommandProcessingLoop(ctx)

	// 3. 持续性分配分区
	return lm.workManager.RunPartitionAllocationLoop(ctx, lm.leaderCtx, lm.partitionMgr)
}

// runCommandProcessingLoop 运行命令处理循环
func (lm *LeaderManager) runCommandProcessingLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // 每5秒检查一次命令
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-lm.leaderCtx.Done():
			return
		case <-ticker.C:
			if err := lm.commandProcessor.ProcessCommands(ctx); err != nil {
				// 命令处理错误不应该中断领导者工作，只记录错误
				// 可以在这里添加适当的错误处理和重试逻辑
			}
		}
	}
}

// relinquishLeadership 放弃领导权
func (lm *LeaderManager) relinquishLeadership() {
	lm.mu.Lock()
	wasLeader := lm.isLeader
	lm.isLeader = false
	lm.mu.Unlock()

	// 只有当前确实是Leader时才需要释放锁
	if wasLeader {
		// 记录Leader状态指标
		metrics.SetLeaderStatus(lm.election.config.NodeID, false)

		// 显式释放Leader锁，确保其他节点能快速获取
		leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, lm.election.config.Namespace)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := lm.election.config.DataStore.ReleaseLock(ctx, leaderLockKey, lm.election.config.NodeID); err != nil {
			lm.election.config.Logger.Warnf("放弃领导权时释放Leader锁失败: %v", err)
		} else {
			lm.election.config.Logger.Infof("放弃领导权，成功释放Leader锁")
		}
	}

	if lm.cancelLeader != nil {
		lm.cancelLeader()
	}
}

// Lead 处理Leader相关的工作 (对外接口，兼容旧API)
func (lm *LeaderManager) Lead(ctx context.Context) error {
	return lm.Start(ctx)
}
