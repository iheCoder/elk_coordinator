package elk_coordinator

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/leader"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/task"
	"elk_coordinator/utils"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Mgr 是一个分布式管理器，管理分区任务的执行
type Mgr struct {
	// 核心字段
	ID              string
	Namespace       string
	DataStore       data.DataStore
	TaskProcessor   task.Processor
	PartitionPlaner leader.PartitionPlaner
	Logger          utils.Logger

	// 分区策略配置
	PartitionStrategyType model.StrategyType          // 分区策略类型
	PartitionStrategy     partition.PartitionStrategy // 分区策略实例

	// 配置选项
	HeartbeatInterval       time.Duration // 心跳间隔
	LeaderElectionInterval  time.Duration // Leader选举间隔
	PartitionLockExpiry     time.Duration // 分区锁过期时间
	LeaderLockExpiry        time.Duration // Leader锁过期时间
	WorkerPartitionMultiple int64         // 每个工作节点分配的分区倍数，用于计算ID探测范围
	AllocationInterval      time.Duration // 分区分配检查间隔，默认2分钟
	AllowPreemption         bool          // 是否允许抢占其他节点的分区，默认false

	// 任务窗口配置
	TaskWindowSize int // 任务窗口大小（同时处理的最大分区数）

	// 任务处理器和执行器
	taskRunner *task.Runner

	// 组件实例 - 用于协调停止过程
	leaderManager *leader.LeaderManager
	taskWindow    *task.TaskWindow

	// 内部状态
	isLeader        bool
	heartbeatCtx    context.Context
	cancelHeartbeat context.CancelFunc
	leaderCtx       context.Context
	cancelLeader    context.CancelFunc
	workCtx         context.Context
	cancelWork      context.CancelFunc
	mu              sync.RWMutex
}

// NewMgr 创建一个新的管理器实例
func NewMgr(namespace string, dataStore data.DataStore, processor task.Processor, planer leader.PartitionPlaner, strategyType model.StrategyType, options ...MgrOption) *Mgr {
	nodeID := generateNodeID()

	// 创建带默认值的管理器
	mgr := &Mgr{
		ID:              nodeID,
		Namespace:       namespace,
		DataStore:       dataStore,
		TaskProcessor:   processor,
		PartitionPlaner: planer,
		Logger:          utils.NewDefaultLogger(),

		// 默认配置
		HeartbeatInterval:       model.DefaultHeartbeatInterval,
		LeaderElectionInterval:  model.DefaultLeaderElectionInterval,
		PartitionLockExpiry:     model.DefaultPartitionLockExpiry,
		LeaderLockExpiry:        model.DefaultLeaderLockExpiry,
		WorkerPartitionMultiple: model.DefaultWorkerPartitionMultiple,
		AllocationInterval:      model.DefaultAllocationInterval,
		AllowPreemption:         true, // 默认启用抢占功能

		// 分区策略配置
		PartitionStrategyType: strategyType,

		// 任务窗口默认配置
		TaskWindowSize: model.DefaultTaskWindowSize,
	}

	// 应用所有选项
	for _, option := range options {
		option(mgr)
	}

	// 初始化分区策略
	mgr.PartitionStrategy = mgr.createPartitionStrategy()

	return mgr
}

// generateNodeID 生成唯一的节点ID
func generateNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String()[:8])
}

// Start 启动管理器
func (m *Mgr) Start(ctx context.Context) error {
	m.Logger.Infof("启动管理器 %s, 命名空间: %s", m.ID, m.Namespace)

	// 创建上下文
	m.heartbeatCtx, m.cancelHeartbeat = context.WithCancel(context.Background())
	m.workCtx, m.cancelWork = context.WithCancel(context.Background())

	// 首先同步注册本节点，确保心跳已设置
	err := m.registerNode(ctx)
	if err != nil {
		return err
	}

	// 启动心跳维护goroutine
	go m.heartbeatKeeper(ctx)

	// 做leader相关的工作
	go m.Lead(ctx)

	// 处理分配任务
	go m.Handle(ctx)

	// 设置信号处理，捕获终止信号
	go m.setupSignalHandler(ctx)

	return nil
}

// heartbeatKeeper 维护节点心跳
func (m *Mgr) heartbeatKeeper(ctx context.Context) {
	m.Logger.Infof("开始心跳维护任务")

	// 周期性发送心跳
	ticker := time.NewTicker(m.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.Logger.Infof("心跳维护任务停止 (上下文取消)")
			return
		case <-m.heartbeatCtx.Done():
			m.Logger.Infof("心跳维护任务停止 (心跳上下文取消)")
			return
		case <-ticker.C:
			heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, m.Namespace, m.ID)
			err := m.DataStore.SetHeartbeat(ctx, heartbeatKey, time.Now().Format(time.RFC3339))
			if err != nil {
				m.Logger.Warnf("发送心跳失败: %v", err)
			}
		}
	}
}

// registerNode 注册节点到系统
func (m *Mgr) registerNode(ctx context.Context) error {
	heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, m.Namespace, m.ID)
	workersKey := fmt.Sprintf(model.WorkersKeyFmt, m.Namespace)

	err := m.DataStore.RegisterWorker(
		ctx,
		workersKey,
		m.ID,
		heartbeatKey,
		time.Now().Format(time.RFC3339),
	)

	if err != nil {
		return errors.Wrap(err, "注册节点失败")
	}

	m.Logger.Infof("节点 %s 已注册", m.ID)
	return nil
}

// Lead 处理Leader相关的工作
func (m *Mgr) Lead(ctx context.Context) error {
	m.Logger.Infof("启动Leader选举与工作任务")

	// 创建LeaderManager配置
	leaderConfig := leader.LeaderConfig{
		NodeID:                  m.ID,
		Namespace:               m.Namespace,
		DataStore:               m.DataStore,
		Logger:                  m.Logger,
		ElectionInterval:        m.LeaderElectionInterval,
		LockExpiry:              m.LeaderLockExpiry,
		WorkerPartitionMultiple: m.WorkerPartitionMultiple,
		ValidHeartbeatDuration:  m.HeartbeatInterval * 3,
		AllocationInterval:      m.AllocationInterval,
		Planer:                  m.PartitionPlaner, // 直接使用Mgr中的PartitionPlaner
		Strategy:                m.PartitionStrategy,
	}

	// 创建并存储LeaderManager实例
	m.mu.Lock()
	m.leaderManager = leader.NewLeaderManager(leaderConfig)
	m.mu.Unlock()

	// 启动LeaderManager
	return m.leaderManager.Start(ctx)
}

// Handle 处理分区任务的主循环
func (m *Mgr) Handle(ctx context.Context) error {
	m.Logger.Infof("开始任务处理循环")

	// 创建任务窗口配置
	windowConfig := task.TaskWindowConfig{
		Namespace:           m.Namespace,
		WorkerID:            m.ID,
		WindowSize:          m.TaskWindowSize,
		PartitionLockExpiry: m.PartitionLockExpiry,
		AllowPreemption:     m.AllowPreemption, // 传递抢占配置
		PartitionStrategy:   m.PartitionStrategy,
		Processor:           m.TaskProcessor,
		Logger:              m.Logger,
		// TaskWindow会内部创建Runner并具备熔断器功能
	}

	// 创建并存储TaskWindow实例
	m.mu.Lock()
	m.taskWindow = task.NewTaskWindow(windowConfig)
	m.mu.Unlock()

	// 启动任务窗口处理
	m.taskWindow.Start(m.workCtx)

	// Handle不返回错误，除非上下文取消
	<-m.workCtx.Done()
	return m.workCtx.Err()
}

// createPartitionStrategy 创建分区策略实例
// 这是一个内部辅助函数，在 NewMgr 中调用一次来初始化策略
func (m *Mgr) createPartitionStrategy() partition.PartitionStrategy {
	// 根据配置的策略类型创建相应的实例
	switch m.PartitionStrategyType {
	case model.StrategyTypeSimple:
		m.Logger.Infof("使用Simple分区策略")
		return partition.NewSimpleStrategy(partition.SimpleStrategyConfig{
			DataStore: m.DataStore,
			Namespace: m.Namespace,
			Logger:    m.Logger,
		})
	case model.StrategyTypeHash:
		m.Logger.Infof("使用Hash分区策略")
		return partition.NewHashPartitionStrategy(m.DataStore, m.Logger)
	default:
		// 如果未设置或无效，默认使用Hash策略
		m.Logger.Warnf("未知的分区策略类型: %v，使用默认的Hash策略", m.PartitionStrategyType)
		return partition.NewHashPartitionStrategy(m.DataStore, m.Logger)
	}
}
