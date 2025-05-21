package elk_coordinator

import (
	"context"
	"context"
	"elk_coordinator/data"
	"fmt"
	"os"
	"os/signal" // For signal handling
	"strings"  // Added for strings.TrimPrefix
	"sync"
	"syscall" // For signal handling
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus" // For logger interaction with LeaderElector
	"golang.org/x/sync/errgroup"
)

// Mgr 是一个分布式管理器，管理分区任务的执行
type Mgr struct {
	// 核心字段
	ID            string
	Namespace     string
	DataStore     data.DataStore // Must implement leader_election.Store
	TaskProcessor Processor
	Logger        Logger

	// Leader Elector
	leaderElector *LeaderElector // Instance of the new LeaderElector
	// Node Manager
	nodeManager *NodeManager // Instance of the new NodeManager

	// 配置选项 (LeaderElectionInterval and LeaderLockExpiry are used to configure LeaderElector)
	// HeartbeatInterval is used to configure NodeManager
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

	// Stale Task Reclaiming
	StaleTaskReclaimMultiplier float64 // Multiplier for PartitionLockExpiry to determine stale task threshold

	// 内部状态
	// heartbeatCtx and cancelHeartbeat are now in NodeManager
	workCtx         context.Context
	cancelWork      context.CancelFunc
	mu              sync.RWMutex // Protects Mgr's mutable state

	stopOnce sync.Once     // Ensures Stop() logic runs only once
	stopCh   chan struct{} // Signals shutdown completion
}

// NewMgr 创建一个新的管理器实例，使用选项模式配置可选参数
func NewMgr(namespace string, dataStore data.DataStore, processor Processor, options ...MgrOption) *Mgr {
	nodeID := generateNodeID()

	// Create Mgr with default values
	mgr := &Mgr{
		ID:            nodeID,
		Namespace:     namespace,
		DataStore:     dataStore,
		TaskProcessor: processor,
		Logger:        NewDefaultLogger(), // Use constructor for defaultLogger

		// Default configurations
		HeartbeatInterval:       DefaultHeartbeatInterval,
		LeaderElectionInterval:  DefaultLeaderElectionInterval,
		PartitionLockExpiry:     DefaultPartitionLockExpiry,
		LeaderLockExpiry:        DefaultLeaderLockExpiry,
		WorkerPartitionMultiple: DefaultWorkerPartitionMultiple,
		StaleTaskReclaimMultiplier: DefaultStaleTaskReclaimMultiplier, // Initialize with default
		UseTaskWindow:           false,
		TaskWindowSize:          DefaultTaskWindowSize,
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
		stopCh: make(chan struct{}),
	}

	// Apply all options. Options might override Logger, LeaderElectionInterval, LeaderLockExpiry, etc.
	for _, option := range options {
		option(mgr)
	}

	// Extract logrus.Entry from mgr.Logger for LeaderElector
	// This part needs to be robust. Assuming Logger has a way to get a logrus.Entry
	// or NewLeaderElector can handle a generic logger or has a fallback.
	var leLogEntry *logrus.Entry
	// Attempt to get a logrus entry from the logger.
	// This is a placeholder for actual mechanism.
	// If your Logger is always a wrapper around logrus, you can cast it.
	// For example: if loggerWrapper, ok := mgr.Logger.(interface{ GetLogrusEntry() *logrus.Entry }); ok {
	// 	leLogEntry = loggerWrapper.GetLogrusEntry()
	// } else {
	// Fallback if the logger doesn't provide a logrus.Entry
	// Create a new one, but this means LeaderElector logs might be separate.
	// mgr.Logger.Warnf("Could not extract logrus.Entry for LeaderElector, using a default one.")
	// tempLogger := logrus.New()
	// tempLogger.SetFormatter(mgr.Logger.(*defaultLogger).entry.Logger.Formatter) // try to use similar formatter
	// leLogEntry = logrus.NewEntry(tempLogger)
	// }
	// For now, let's assume defaultLogger can provide its entry or NewLeaderElector handles it.
	// Consolidated logic for deriving *logrus.Entry for sub-components (LeaderElector, NodeManager)
	var subsystemLogEntry *logrus.Entry
	if defaultLog, ok := mgr.Logger.(*defaultLogger); ok {
		// If mgr.Logger is the defaultLogger, use its internal logrus entry.
		// NewLeaderElector and NewNodeManager will further customize this with WithField.
		subsystemLogEntry = defaultLog.entry
	} else {
		// If mgr.Logger is a custom type (set via WithLogger), create a new default logrus entry for sub-components.
		mgr.Logger.Warnf("Mgr.Logger is a custom implementation and not a *defaultLogger. LeaderElector and NodeManager will use a separate, default logrus logger.")
		fallbackLog := logrus.New()
		// Attempt to match the level of the custom logger if possible, otherwise default to Info.
		// This requires the custom Logger interface to expose a GetLevel() or similar,
		// or we just default it. For now, defaulting to InfoLevel.
		// if customLoggerWithLevel, ok := mgr.Logger.(interface{ GetLevel() logrus.Level }); ok {
		// 	fallbackLog.SetLevel(customLoggerWithLevel.GetLevel())
		// } else {
		fallbackLog.SetLevel(logrus.InfoLevel)
		// }
		fallbackLog.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02T15:04:05.000Z07:00"}) // Consistent formatting
		subsystemLogEntry = logrus.NewEntry(fallbackLog)
	}

	// Initialize LeaderElector
	mgr.leaderElector = NewLeaderElector(
		mgr.ID,
		mgr.Namespace,
		mgr.DataStore,
		subsystemLogEntry, // Pass the derived logrus.Entry
		mgr.LeaderLockExpiry,
		mgr.LeaderElectionInterval,
		// No longer pass keyFmt here, LeaderElector will use the global constant
	)

	// Initialize NodeManager
	mgr.nodeManager = NewNodeManager(
		mgr.ID,
		mgr.Namespace,
		mgr.DataStore,     // Pass the full DataStore
		subsystemLogEntry, // Pass the same derived logrus.Entry
		mgr.HeartbeatInterval,
		// No longer pass keyFmt here, NodeManager will use the global constants
	)

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

	m.Logger.Infof("启动管理器 %s, 命名空间: %s", m.ID, m.Namespace)
	var g *errgroup.Group
	gCtx, gCancel := context.WithCancel(ctx) 
	g, gCtx = errgroup.WithContext(gCtx)
	m.mu.Lock()
	m.workCtx, m.cancelWork = context.WithCancel(gCtx) 
	// m.heartbeatCtx and m.cancelHeartbeat are now managed by NodeManager
	m.mu.Unlock()


	// Goroutine for leader election
	m.leaderElector.Lead(gCtx, g)

	// Goroutine for node registration and heartbeats (managed by NodeManager)
	g.Go(func() error {
		// NodeManager.Start will run its own loop in a goroutine if needed by its design.
		// Or, if NodeManager.Start is blocking, this g.Go call is appropriate.
		// Based on the NodeManager skeleton, its Start calls registerNode then go nm.nodeKeeperLoop.
		// So, NodeManager.Start itself is not blocking in the long run.
		err := m.nodeManager.Start(gCtx)
		if err != nil {
			m.Logger.Errorf("NodeManager failed to start: %v", err)
			// To ensure the error group cancels other goroutines if NodeManager fails critically on start:
			// return err (this depends on how critical an initial NodeManager start failure is)
		}
		// If NodeManager.Start is non-blocking and its internal goroutine (nodeKeeperLoop) needs to be waited for,
		// NodeManager.Start should return a channel or an error to signal its goroutine's termination.
		// For now, assume NodeManager.Start correctly uses gCtx for its long-running tasks.
		// We need to ensure nodeManager.nodeKeeperLoop respects gCtx.
		// The current node_manager.go does this.
		<-gCtx.Done() // Wait for group context to be done to keep this goroutine alive if Start is non-blocking
		m.Logger.Infof("NodeManager's context (gCtx) is done in Mgr.Start: %v", gCtx.Err())
		return nil 
	})

	// Goroutine for task handling
	g.Go(func() error {
		m.Handle(m.workCtx) // Use workCtx
		return nil
	})

	// Goroutine for signal handling
	g.Go(func() error {
		m.setupSignalHandler(gCtx, gCancel) // Pass group context and its cancel func
		return nil
	})

	// Goroutine for metrics publishing
	if m.UseTaskMetrics {
		g.Go(func() error {
			m.publishMetricsPeriodically(gCtx) // Use group context
			return nil
		})
	}

	m.Logger.Infof("Manager %s started all components.", m.ID)

	// Wait for all goroutines in the group to finish or for an error.
	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		m.Logger.Errorf("Manager %s encountered an error from a component: %v", m.ID, err)
	}

	m.Logger.Infof("Manager %s is shutting down.", m.ID)
	m.Stop() // Ensure Stop logic is called to clean up other resources
	close(m.stopCh) // Signal that shutdown is complete
	return err
}

// IsLeader returns true if this instance is currently the leader.
func (m *Mgr) IsLeader() bool {
	if m.leaderElector == nil {
		m.Logger.Warnf("IsLeader called before leaderElector is initialized.")
		return false
	}
	return m.leaderElector.IsLeader()
}

// getActiveWorkers 获取活跃节点列表
func (m *Mgr) getActiveWorkers(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf(HeartbeatKeyFormat, m.Namespace, "*")
	keys, err := m.DataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳失败")
	}

	var activeWorkers []string
	now := time.Now()
	validHeartbeatDuration := m.HeartbeatInterval * 3

	// Construct the prefix to trim based on HeartbeatKeyFormat
	// Example: HeartbeatKeyFormat = "elk_coord/%s/heartbeats/%s"
	// Prefix for namespace "my_ns" would be "elk_coord/my_ns/heartbeats/"
	// This is achieved by formatting with an empty string for the nodeID part and then removing the trailing %s,
	// or more simply by finding the last element after splitting.
	// A more robust way is to replace the last '%s' with an empty string for prefix construction.
	
	// Assuming HeartbeatKeyFormat is "elk_coord/%s/heartbeats/%s"
	// The part before the nodeID is "elk_coord/NAMESPACE/heartbeats/"
	keyPrefixToTrim := fmt.Sprintf(HeartbeatKeyFormat, m.Namespace, "") 
	// This results in "elk_coord/NAMESPACE/heartbeats/%s". We need to remove the final '%s'.
	// A safer way if the format is fixed:
	// keyPrefixToTrim = "elk_coord/" + m.Namespace + "/heartbeats/"
	// Let's use strings.LastIndex to find the last part as nodeID for robustness with the format string.
	// However, given the task asks for TrimPrefix, we'll construct the prefix carefully.
	// If HeartbeatKeyFormat = "elk_coord/%s/heartbeats/%s",
	// then fmt.Sprintf(HeartbeatKeyFormat, m.Namespace, "PLACEHOLDER_NODE_ID") gives "elk_coord/ns/heartbeats/PLACEHOLDER_NODE_ID"
	// The prefix part is "elk_coord/ns/heartbeats/"
	tempFormattedKeyForPrefix := fmt.Sprintf(HeartbeatKeyFormat, m.Namespace, "X") // X is a placeholder
	prefixToTrim := tempFormattedKeyForPrefix[:strings.LastIndex(tempFormattedKeyForPrefix, "X")]


	for _, key := range keys {
		var nodeID string
		if strings.HasPrefix(key, prefixToTrim) {
			nodeID = strings.TrimPrefix(key, prefixToTrim)
		} else {
			// Fallback or log warning if key doesn't match expected prefix structure
			// This might happen if GetKeys returns keys not matching the exact format
			m.Logger.Warnf("getActiveWorkers: key '%s' does not match expected prefix '%s'. Extracting last segment as nodeID.", key, prefixToTrim)
			parts := strings.Split(key, "/")
			if len(parts) > 0 {
				nodeID = parts[len(parts)-1]
			} else {
				m.Logger.Errorf("getActiveWorkers: Could not parse nodeID from key '%s'", key)
				continue
			}
		}
		
		if nodeID == "" {
			m.Logger.Warnf("getActiveWorkers: extracted empty nodeID from key '%s' with prefix '%s'", key, prefixToTrim)
			continue
		}

		lastHeartbeatStr, err := m.DataStore.GetHeartbeat(ctx, key) // Use full key to get heartbeat
		if err != nil {
			m.Logger.Warnf("getActiveWorkers: failed to get heartbeat for key %s: %v", key, err)
			continue 
		}

		lastHeartbeat, err := time.Parse(time.RFC3339, lastHeartbeatStr)
		if err != nil {
			m.Logger.Warnf("getActiveWorkers: failed to parse heartbeat time for key %s ('%s'): %v", key, lastHeartbeatStr, err)
			continue 
		}

		if now.Sub(lastHeartbeat) <= validHeartbeatDuration {
			activeWorkers = append(activeWorkers, nodeID)
		} else {
			m.Logger.Infof("getActiveWorkers: Stale heartbeat for nodeID %s (key %s). Last updated: %v. Deleting key.", nodeID, key, lastHeartbeat)
			if delErr := m.DataStore.DeleteKey(ctx, key); delErr != nil { // Use full key to delete
				m.Logger.Warnf("getActiveWorkers: Failed to delete stale heartbeat key %s: %v", key, delErr)
			}
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

// Stop gracefully shuts down the manager and its components.
// It's idempotent and can be called multiple times.
func (m *Mgr) Stop() {
	m.stopOnce.Do(func() {
		m.Logger.Infof("Manager %s received stop signal.", m.ID)

		// Stop LeaderElector first
		if m.leaderElector != nil {
			m.Logger.Debugf("Stopping leader elector for %s.", m.ID)
			m.leaderElector.Stop() // LeaderElector.Stop should be idempotent and handle its own context/cleanup.
			m.Logger.Debugf("Leader elector for %s signaled to stop.", m.ID)
		}

		// Stop NodeManager
		if m.nodeManager != nil {
			m.Logger.Debugf("Stopping node manager for %s.", m.ID)
			// NodeManager.Stop should also be idempotent and handle its cleanup.
			// It might need its own context for cleanup operations.
			stopCtx, cancelStopCtx := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStopCtx()
			if err := m.nodeManager.Stop(stopCtx); err != nil {
				m.Logger.Warnf("Error stopping node manager for %s: %v", m.ID, err)
			}
			m.Logger.Debugf("Node manager for %s signaled to stop.", m.ID)
		}

		// Cancel main work context controlled by Mgr (if not already cancelled by errgroup)
		m.mu.Lock()
		// cancelHeartbeat is now managed by NodeManager
		if m.cancelWork != nil {
			m.Logger.Debugf("Cancelling work context for %s.", m.ID)
			m.cancelWork()
			m.cancelWork = nil
		}
		m.mu.Unlock()
		
		// Note: The unregisterNode logic is now part of NodeManager.Stop().
		// If there was a global errgroup context cancel function (gCancel in Start),
		// it should ideally be called here or by setupSignalHandler to ensure all errgroup goroutines are stopped.
		// However, calling it here might be redundant if Stop is called from Start's defer/error handling.

		m.Logger.Infof("Manager %s shutdown process initiated/completed.", m.ID)
	})
}

// WaitUntilStopped blocks until the manager has fully shut down.
func (m *Mgr) WaitUntilStopped() {
	<-m.stopCh
	m.Logger.Infof("Manager %s has confirmed full stop.", m.ID)
}


// setupSignalHandler listens for termination signals and initiates a graceful shutdown.
// It now also accepts the errgroup's main context cancel function.
func (m *Mgr) setupSignalHandler(ctx context.Context, cancelGroup context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	m.Logger.Debug("Signal handler started.")

	select {
	case sig := <-sigCh:
		m.Logger.Infof("Received signal: %s. Initiating graceful shutdown...", sig)
		cancelGroup() // Cancel the main errgroup context, which will propagate to components
		m.Stop()      // Call Mgr's Stop method for further cleanup
	case <-ctx.Done(): // Listen for context cancellation from other sources (e.g. errgroup itself)
		m.Logger.Infof("Signal handler context done: %v. Shutting down...", ctx.Err())
		// If ctx.Done() is triggered, it means the errgroup is already stopping.
		// Stop() might have already been called or will be by Start()'s defer.
		// Calling it again is fine due to stopOnce.
		m.Stop()
	}
	m.Logger.Debug("Signal handler finished.")
}

// DefaultLogger returns a new default logger instance.
// This needs to be defined if it's not in a separate logger.go file.
// For now, to make this file self-contained for the diff.
type defaultLogger struct {
	entry *logrus.Entry
}

func NewDefaultLogger() Logger {
	l := logrus.New()
	l.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02T15:04:05.000Z07:00"})
	l.SetLevel(logrus.InfoLevel)
	return &defaultLogger{entry: logrus.NewEntry(l)}
}

func (l *defaultLogger) WithField(key string, value interface{}) Logger {
	return &defaultLogger{entry: l.entry.WithField(key, value)}
}

func (l *defaultLogger) WithFields(fields map[string]interface{}) Logger {
	//logrus.Fields is map[string]interface{}
	return &defaultLogger{entry: l.entry.WithFields(logrus.Fields(fields))}
}
func (l *defaultLogger) Debugf(format string, args ...interface{}) { l.entry.Debugf(format, args...) }
func (l *defaultLogger) Infof(format string, args ...interface{})  { l.entry.Infof(format, args...) }
func (l *defaultLogger) Warnf(format string, args ...interface{})  { l.entry.Warnf(format, args...) }
func (l *defaultLogger) Errorf(format string, args ...interface{}) { l.entry.Errorf(format, args...) }
func (l *defaultLogger) Fatalf(format string, args ...interface{}) { l.entry.Fatalf(format, args...) }
func (l *defaultLogger) Panicf(format string, args ...interface{}) { l.entry.Panicf(format, args...) }

// Ensure defaultLogger implements Logger
var _ Logger = (*defaultLogger)(nil)


// TODO: Remove this if it exists in mgr.go already and was just not in the initial snippet.
// These constants are assumed to be defined elsewhere, adding them here for compilation if not.
// (HeartbeatFmtFmt and WorkersKeyFmt are removed as they are now centralized)
const (
	DefaultHeartbeatInterval      = 10 * time.Second
	DefaultLeaderElectionInterval = 15 * time.Second
	DefaultPartitionLockExpiry    = 60 * time.Second
	DefaultLeaderLockExpiry       = 30 * time.Second
	DefaultWorkerPartitionMultiple = 3
	DefaultTaskWindowSize         = 10
	DefaultCapacityUpdateInterval = 30 * time.Second
	DefaultRecentPartitions       = 100
	DefaultStaleTaskReclaimMultiplier = 3.0 // Default value for stale task reclaim multiplier
	// HeartbeatFmtFmt               = "elk_coord/%s/heartbeats/%s" // Moved to constants/keys.go
	// WorkersKeyFmt                 = "elk_coord/%s/workers"       // Moved to constants/keys.go
)

// TODO: Remove this if Processor interface is defined elsewhere.
type Processor interface {
	Process(ctx context.Context, partitionID string) error
	// other methods...
}

// TODO: Remove this if TaskWindow is defined elsewhere.
type TaskWindow struct {
	// fields...
}

// TODO: Remove this if WorkerMetrics is defined elsewhere.
type WorkerMetrics struct {
	ProcessingSpeed     float64
	SuccessRate         float64
	AvgProcessingTime   time.Duration
	TotalTasksCompleted int64
	SuccessfulTasks     int64
	TotalItemsProcessed int64
	LastUpdateTime      time.Time
}

// TODO: Remove this if Handle, publishMetricsPeriodically are complex and defined elsewhere.
// Adding stubs for compilation.
func (m *Mgr) Handle(ctx context.Context) {
	m.Logger.Infof("Task handling loop started for %s. Waiting for assignments or context cancellation.", m.ID)
	<-ctx.Done() // Simulate work until context is cancelled
	m.Logger.Infof("Task handling loop for %s stopped: %v", m.ID, ctx.Err())
}

func (m *Mgr) publishMetricsPeriodically(ctx context.Context) {
	m.Logger.Infof("Metrics publishing loop started for %s.", m.ID)
	ticker := time.NewTicker(m.MetricsUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.Logger.Infof("Metrics publishing loop for %s stopped: %v", m.ID, ctx.Err())
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				m.Logger.Infof("Metrics publishing loop for %s stopping due to context error: %v", m.ID, ctx.Err())
				return
			}
			m.Logger.Debugf("Publishing metrics for %s.", m.ID)
			// Actual metrics publishing logic here
		}
	}
}

// Removed splitKey helper as it was a placeholder and not used.
// func splitKey(key, sep string) []string {
//     return nil 
// }
