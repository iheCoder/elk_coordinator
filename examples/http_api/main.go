package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	elkhttp "github.com/iheCoder/elk_coordinator/http"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/utils"
)

func main() {
	// 创建模拟的数据存储
	dataStore := &MockDataStore{}

	// 创建日志器
	logger := utils.NewDefaultLogger()

	// 创建HTTP路由器
	router := elkhttp.NewRouter("example", dataStore, logger)

	// 设置路由
	engine := router.SetupRoutes()

	// 添加一些额外的示例路由
	setupExampleRoutes(engine, logger)

	// 创建HTTP服务器
	srv := &http.Server{
		Addr:    ":8080",
		Handler: engine,
	}

	// 启动服务器
	go func() {
		logger.Infof("HTTP服务器启动在端口 :8080")
		logger.Infof("访问 http://localhost:8080/health 进行健康检查")
		logger.Infof("访问 http://localhost:8080/api/v1/retry-failed-partitions (POST) 重试失败分区")
		logger.Infof("访问 http://localhost:8080/example/info (GET) 查看示例信息")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("启动服务器失败: %v", err)
		}
	}()

	// 等待中断信号以优雅地关闭服务器
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Infof("正在关闭服务器...")

	// 5秒的超时时间来完成现有请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("服务器强制关闭:", err)
	}

	logger.Infof("服务器已退出")
}

// setupExampleRoutes 设置一些示例路由
func setupExampleRoutes(engine *gin.Engine, logger utils.Logger) {
	example := engine.Group("/example")
	{
		example.GET("/info", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"service":   "elk-coordinator",
				"version":   "1.0.0",
				"framework": "gin",
				"endpoints": []string{
					"GET /health - 健康检查",
					"POST /api/v1/retry-failed-partitions - 重试失败分区",
					"GET /example/info - 服务信息",
					"GET /example/status - 服务状态",
				},
			})
		})

		example.GET("/status", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"status":    "running",
				"timestamp": time.Now().Unix(),
				"uptime":    "运行中",
			})
		})
	}
}

// MockDataStore 模拟数据存储实现 - 仅用于演示
type MockDataStore struct{}

// 实现CommandOperations
func (m *MockDataStore) SubmitCommand(ctx context.Context, namespace string, command interface{}) error {
	fmt.Printf("模拟提交命令: namespace=%s, command=%+v\n", namespace, command)
	return nil
}

func (m *MockDataStore) GetPendingCommands(ctx context.Context, namespace string, limit int) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataStore) DeleteCommand(ctx context.Context, namespace, commandID string) error {
	return nil
}

// 实现LockOperations
func (m *MockDataStore) AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	return true, nil
}

func (m *MockDataStore) RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error) {
	return true, nil
}

func (m *MockDataStore) CheckLock(ctx context.Context, key string, expectedValue string) (bool, error) {
	return true, nil
}

func (m *MockDataStore) ReleaseLock(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockDataStore) GetLockOwner(ctx context.Context, key string) (string, error) {
	return "mock-owner", nil
}

// 实现KeyOperations
func (m *MockDataStore) SetKey(ctx context.Context, key string, value string, expiry time.Duration) error {
	return nil
}

func (m *MockDataStore) GetKey(ctx context.Context, key string) (string, error) {
	return "mock-value", nil
}

func (m *MockDataStore) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataStore) DeleteKey(ctx context.Context, key string) error {
	return nil
}

// 实现HeartbeatOperations
func (m *MockDataStore) SetHeartbeat(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockDataStore) GetHeartbeat(ctx context.Context, key string) (string, error) {
	return "alive", nil
}

// 实现SimplePartitionOperations
func (m *MockDataStore) SetPartitions(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockDataStore) GetPartitions(ctx context.Context, key string) (string, error) {
	return "{}", nil
}

// 实现HashPartitionOperations
func (m *MockDataStore) HSetPartition(ctx context.Context, key string, field string, value string) error {
	return nil
}

func (m *MockDataStore) HGetPartition(ctx context.Context, key string, field string) (string, error) {
	return "", nil
}

func (m *MockDataStore) HGetAllPartitions(ctx context.Context, key string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (m *MockDataStore) HUpdatePartitionWithVersion(ctx context.Context, key string, field string, value string, version int64) (bool, error) {
	return true, nil
}

func (m *MockDataStore) HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error {
	return nil
}

func (m *MockDataStore) HDeletePartition(ctx context.Context, key string, field string) error {
	return nil
}

// 实现StatusOperations
func (m *MockDataStore) SetSyncStatus(ctx context.Context, key string, value string) error {
	return nil
}

func (m *MockDataStore) GetSyncStatus(ctx context.Context, key string) (string, error) {
	return "synced", nil
}

// 实现WorkerRegistry
func (m *MockDataStore) RegisterWorker(ctx context.Context, workerID string) error {
	return nil
}

func (m *MockDataStore) UnregisterWorker(ctx context.Context, workerID string) error {
	return nil
}

func (m *MockDataStore) GetActiveWorkers(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataStore) GetAllWorkers(ctx context.Context) ([]*model.WorkerInfo, error) {
	return []*model.WorkerInfo{}, nil
}

func (m *MockDataStore) IsWorkerActive(ctx context.Context, workerID string) (bool, error) {
	return true, nil
}

// 实现CounterOperations
func (m *MockDataStore) IncrementCounter(ctx context.Context, counterKey string, increment int64) (int64, error) {
	return 1, nil
}

func (m *MockDataStore) SetCounter(ctx context.Context, counterKey string, value int64, expiry time.Duration) error {
	return nil
}

func (m *MockDataStore) GetCounter(ctx context.Context, counterKey string) (int64, error) {
	return 0, nil
}

// 实现PartitionStatsOperations
func (m *MockDataStore) InitPartitionStats(ctx context.Context, statsKey string) error {
	return nil
}

func (m *MockDataStore) GetPartitionStatsData(ctx context.Context, statsKey string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (m *MockDataStore) UpdatePartitionStatsOnCreate(ctx context.Context, statsKey string, partitionID int, dataID int64) error {
	return nil
}

func (m *MockDataStore) UpdatePartitionStatsOnStatusChange(ctx context.Context, statsKey string, oldStatus, newStatus string) error {
	return nil
}

func (m *MockDataStore) UpdatePartitionStatsOnDelete(ctx context.Context, statsKey string, status string) error {
	return nil
}

func (m *MockDataStore) RebuildPartitionStats(ctx context.Context, statsKey string, activePartitionsKey, archivedPartitionsKey string) error {
	return nil
}

// 实现AdvancedOperations
func (m *MockDataStore) LockWithHeartbeat(ctx context.Context, key, value string, heartbeatInterval time.Duration) (bool, context.CancelFunc, error) {
	return true, func() {}, nil
}

func (m *MockDataStore) TryLockWithTimeout(ctx context.Context, key string, value string, lockExpiry, waitTimeout time.Duration) (bool, error) {
	return true, nil
}

func (m *MockDataStore) ExecuteAtomically(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

func (m *MockDataStore) MoveItem(ctx context.Context, fromKey, toKey string, item interface{}) error {
	return nil
}

// 实现QueueOperations
func (m *MockDataStore) AddToQueue(ctx context.Context, queueKey string, item interface{}, score float64) error {
	return nil
}

func (m *MockDataStore) GetFromQueue(ctx context.Context, queueKey string, count int64) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataStore) RemoveFromQueue(ctx context.Context, queueKey string, item interface{}) error {
	return nil
}

func (m *MockDataStore) GetQueueLength(ctx context.Context, queueKey string) (int64, error) {
	return 0, nil
}

func (m *MockDataStore) RefreshHeartbeat(ctx context.Context, key string) error {
	return nil
}

// 实现WorkerHeartbeatOperations
func (m *MockDataStore) SetWorkerHeartbeat(ctx context.Context, workerID string, value string) error {
	return nil
}

func (m *MockDataStore) RefreshWorkerHeartbeat(ctx context.Context, workerID string) error {
	return nil
}

func (m *MockDataStore) GetWorkerHeartbeat(ctx context.Context, workerID string) (string, error) {
	return "alive", nil
}
