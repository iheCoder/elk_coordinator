package elk_coordinator

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/leader"
	"elk_coordinator/model"
	"elk_coordinator/task"
	"elk_coordinator/utils"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"os"
	"sync"
	"time"
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

	// 配置选项
	HeartbeatInterval       time.Duration // 心跳间隔
	LeaderElectionInterval  time.Duration // Leader选举间隔
	PartitionLockExpiry     time.Duration // 分区锁过期时间
	LeaderLockExpiry        time.Duration // Leader锁过期时间
	WorkerPartitionMultiple int64         // 每个工作节点分配的分区倍数，用于计算ID探测范围

	// 任务窗口相关配置
	UseTaskWindow  bool // 是否使用任务窗口（并行处理多个分区）
	TaskWindowSize int  // 任务窗口大小（同时处理的最大分区数）

	// 任务处理器和执行器
	taskRunner *task.Runner

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
func NewMgr(namespace string, dataStore data.DataStore, processor task.Processor, planer leader.PartitionPlaner, options ...MgrOption) *Mgr {
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

		// 任务窗口默认配置
		UseTaskWindow:  false,
		TaskWindowSize: model.DefaultTaskWindowSize,
	}

	// 应用所有选项
	for _, option := range options {
		option(mgr)
	}

	return mgr
}

// SetLogger 已被选项模式替代，保留用于向后兼容
//
// 推荐使用 WithLogger 选项代替
func (m *Mgr) SetLogger(logger utils.Logger) {
	m.Logger.Warnf("SetLogger 方法已过时，请使用 WithLogger 选项")
	m.Logger = logger
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

	// 注册本节点，并周期发送心跳
	go m.nodeKeeper(ctx)

	// 做leader相关的工作
	go m.Lead(ctx)

	// 处理分配任务
	go m.Handle(ctx)

	// 设置信号处理，捕获终止信号
	go m.setupSignalHandler(ctx)

	return nil
}

// IsLeader 返回当前节点是否是Leader
func (m *Mgr) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isLeader
}

// nodeKeeper 管理节点注册和心跳
func (m *Mgr) nodeKeeper(ctx context.Context) {
	m.Logger.Infof("开始节点维护任务")

	// 注册本节点
	err := m.registerNode(ctx)
	if err != nil {
		m.Logger.Errorf("注册节点失败: %v", err)
		return
	}

	// 周期性发送心跳
	ticker := time.NewTicker(m.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.Logger.Infof("节点维护任务停止 (上下文取消)")
			return
		case <-m.heartbeatCtx.Done():
			m.Logger.Infof("节点维护任务停止 (心跳上下文取消)")
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

// getActiveWorkers 获取活跃节点列表
func (m *Mgr) getActiveWorkers(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf(model.HeartbeatFmtFmt, m.Namespace, "*")
	keys, err := m.DataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳失败")
	}

	var activeWorkers []string
	now := time.Now()
	validHeartbeatDuration := m.HeartbeatInterval * 3

	for _, key := range keys {
		// 从key中提取节点ID
		prefix := fmt.Sprintf(model.HeartbeatFmtFmt, m.Namespace, "")
		nodeID := key[len(prefix):]

		// 获取最后心跳时间
		lastHeartbeatStr, err := m.DataStore.GetHeartbeat(ctx, key)
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
			m.DataStore.DeleteKey(ctx, key)
		}
	}

	return activeWorkers, nil
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
		Planer:                  m.PartitionPlaner, // 直接使用Mgr中的PartitionPlaner
	}

	// 创建并启动LeaderManager
	leaderManager := leader.NewLeaderManager(leaderConfig)
	return leaderManager.Start(ctx)
}

// Handle 处理分区任务的主循环
func (m *Mgr) Handle(ctx context.Context) error {
	m.Logger.Infof("开始任务处理循环")

	// 初始化任务执行器
	m.taskRunner = task.NewRunner(task.RunnerConfig{
		Namespace:           m.Namespace,
		WorkerID:            m.ID,
		PartitionLockExpiry: m.PartitionLockExpiry,
		DataStore:           m.DataStore,
		Processor:           m.TaskProcessor,
		Logger:              m.Logger,
		UseTaskWindow:       m.UseTaskWindow,
		TaskWindowSize:      m.TaskWindowSize,
	})

	// 启动任务执行器并等待完成
	return m.taskRunner.Start(m.workCtx)
}

// SetWorkerPartitionMultiple 已被选项模式替代，保留用于向后兼容
//
// 推荐使用 WithWorkerPartitionMultiple 选项代替
func (m *Mgr) SetWorkerPartitionMultiple(multiple int64) {
	m.Logger.Warnf("SetWorkerPartitionMultiple 方法已过时，请使用 WithWorkerPartitionMultiple 选项")
	if multiple <= 0 {
		m.Logger.Warnf("无效的工作节点分区倍数: %d，使用默认值 %d", multiple, model.DefaultWorkerPartitionMultiple)
		m.WorkerPartitionMultiple = model.DefaultWorkerPartitionMultiple
		return
	}
	m.WorkerPartitionMultiple = multiple
}
