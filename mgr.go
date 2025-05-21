package elk_coordinator

import (
	"context"
	"elk_coordinator/data"
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
	ID            string
	Namespace     string
	DataStore     data.DataStore
	TaskProcessor Processor
	Logger        Logger

	// 配置选项
	HeartbeatInterval       time.Duration // 心跳间隔
	LeaderElectionInterval  time.Duration // Leader选举间隔
	PartitionLockExpiry     time.Duration // 分区锁过期时间
	LeaderLockExpiry        time.Duration // Leader锁过期时间
	WorkerPartitionMultiple int64         // 每个工作节点分配的分区倍数，用于计算ID探测范围

	// 任务窗口相关配置
	UseTaskWindow  bool        // 是否使用任务窗口（并行处理多个分区）
	TaskWindowSize int         // 任务窗口大小（同时处理的最大分区数）
	taskWindow     *TaskWindow // 任务窗口实例

	// 性能指标和自适应处理参数
	UseTaskMetrics          bool           // 是否使用任务指标
	MetricsUpdateInterval   time.Duration  // 指标更新间隔
	RecentPartitionsToTrack int            // 记录最近多少个分区的处理速度
	Metrics                 *WorkerMetrics // 节点性能指标
	metricsMutex            sync.RWMutex   // 指标访问互斥锁

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

// NewMgr 创建一个新的管理器实例，使用选项模式配置可选参数
func NewMgr(namespace string, dataStore data.DataStore, processor Processor, options ...MgrOption) *Mgr {
	nodeID := generateNodeID()

	// 创建带默认值的管理器
	mgr := &Mgr{
		ID:            nodeID,
		Namespace:     namespace,
		DataStore:     dataStore,
		TaskProcessor: processor,
		Logger:        &defaultLogger{},

		// 默认配置
		HeartbeatInterval:       DefaultHeartbeatInterval,
		LeaderElectionInterval:  DefaultLeaderElectionInterval,
		PartitionLockExpiry:     DefaultPartitionLockExpiry,
		LeaderLockExpiry:        DefaultLeaderLockExpiry,
		WorkerPartitionMultiple: DefaultWorkerPartitionMultiple,

		// 任务窗口默认配置
		UseTaskWindow:  false,
		TaskWindowSize: DefaultTaskWindowSize,

		// 指标收集默认配置
		UseTaskMetrics:          true,
		MetricsUpdateInterval:   DefaultCapacityUpdateInterval,
		RecentPartitionsToTrack: DefaultRecentPartitions,
		Metrics: &WorkerMetrics{
			ProcessingSpeed:     0.0,
			SuccessRate:         1.0,
			AvgProcessingTime:   0,
			TotalTasksCompleted: 0,
			SuccessfulTasks:     0,
			TotalItemsProcessed: 0,
			LastUpdateTime:      time.Now(),
		},
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
func (m *Mgr) SetLogger(logger Logger) {
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

	// 如果启用了指标收集，定期发布指标
	if m.UseTaskMetrics {
		go m.publishMetricsPeriodically(ctx)
	}

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
			heartbeatKey := fmt.Sprintf(HeartbeatFmtFmt, m.Namespace, m.ID)
			err := m.DataStore.SetHeartbeat(ctx, heartbeatKey, time.Now().Format(time.RFC3339))
			if err != nil {
				m.Logger.Warnf("发送心跳失败: %v", err)
			}
		}
	}
}

// registerNode 注册节点到系统
func (m *Mgr) registerNode(ctx context.Context) error {
	heartbeatKey := fmt.Sprintf(HeartbeatFmtFmt, m.Namespace, m.ID)
	workersKey := fmt.Sprintf(WorkersKeyFmt, m.Namespace)

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
	pattern := fmt.Sprintf(HeartbeatFmtFmt, m.Namespace, "*")
	keys, err := m.DataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳失败")
	}

	var activeWorkers []string
	now := time.Now()
	validHeartbeatDuration := m.HeartbeatInterval * 3

	for _, key := range keys {
		// 从key中提取节点ID
		prefix := fmt.Sprintf(HeartbeatFmtFmt, m.Namespace, "")
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

// SetWorkerPartitionMultiple 已被选项模式替代，保留用于向后兼容
//
// 推荐使用 WithWorkerPartitionMultiple 选项代替
func (m *Mgr) SetWorkerPartitionMultiple(multiple int64) {
	m.Logger.Warnf("SetWorkerPartitionMultiple 方法已过时，请使用 WithWorkerPartitionMultiple 选项")
	if multiple <= 0 {
		m.Logger.Warnf("无效的工作节点分区倍数: %d，使用默认值 %d", multiple, DefaultWorkerPartitionMultiple)
		m.WorkerPartitionMultiple = DefaultWorkerPartitionMultiple
		return
	}
	m.WorkerPartitionMultiple = multiple
}
