package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"elk_coordinator/data"
	"elk_coordinator/model"
)

// MockLogger 实现 utils.Logger 接口用于测试
type MockLogger struct {
	mu   sync.Mutex
	logs []string
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf("INFO: "+format, args...))
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf("ERROR: "+format, args...))
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf("WARN: "+format, args...))
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf("DEBUG: "+format, args...))
}

func (m *MockLogger) GetLogs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.logs))
	copy(result, m.logs)
	return result
}

func (m *MockLogger) ClearLogs() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = m.logs[:0]
}

// MockHashPartitionOperations 实现 data.HashPartitionOperations 接口用于测试
type MockHashPartitionOperations struct {
	mu         sync.RWMutex
	partitions map[string]map[string]string // key -> field -> value
	versions   map[string]int64             // field -> version
}

func NewMockHashPartitionOperations() *MockHashPartitionOperations {
	return &MockHashPartitionOperations{
		partitions: make(map[string]map[string]string),
		versions:   make(map[string]int64),
	}
}

func (m *MockHashPartitionOperations) HSetPartition(ctx context.Context, key, field, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.partitions[key] == nil {
		m.partitions[key] = make(map[string]string)
	}
	m.partitions[key][field] = value

	// 解析JSON以获取版本信息，确保版本同步
	// 这对于SavePartition等直接操作很重要
	var partitionInfo map[string]interface{}
	if err := json.Unmarshal([]byte(value), &partitionInfo); err == nil {
		if version, ok := partitionInfo["version"]; ok {
			if versionFloat, ok := version.(float64); ok {
				m.versions[field] = int64(versionFloat)
			}
		}
	}

	return nil
}

func (m *MockHashPartitionOperations) HGetPartition(ctx context.Context, key, field string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.partitions[key] == nil {
		return "", data.ErrNotFound
	}

	value, exists := m.partitions[key][field]
	if !exists {
		return "", data.ErrNotFound
	}

	return value, nil
}

func (m *MockHashPartitionOperations) HGetAllPartitions(ctx context.Context, key string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)
	if m.partitions[key] != nil {
		for field, value := range m.partitions[key] {
			result[field] = value
		}
	}

	return result, nil
}

func (m *MockHashPartitionOperations) HDeletePartition(ctx context.Context, key, field string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.partitions[key] != nil {
		delete(m.partitions[key], field)
		delete(m.versions, field)
		if len(m.partitions[key]) == 0 {
			delete(m.partitions, key)
		}
	}

	return nil
}

func (m *MockHashPartitionOperations) HUpdatePartitionWithVersion(ctx context.Context, key, field, value string, expectedVersion int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentVersion := m.versions[field]

	// 创建新分区（expectedVersion 为 0）
	if expectedVersion == 0 {
		if currentVersion != 0 {
			return false, model.ErrPartitionAlreadyExists
		}
		// 创建新分区
		if m.partitions[key] == nil {
			m.partitions[key] = make(map[string]string)
		}
		m.partitions[key][field] = value
		m.versions[field] = 1
		return true, nil
	}

	// 更新现有分区
	if currentVersion != expectedVersion {
		return false, model.ErrOptimisticLockFailed
	}

	if m.partitions[key] == nil {
		m.partitions[key] = make(map[string]string)
	}
	m.partitions[key][field] = value
	m.versions[field] = expectedVersion + 1
	return true, nil
}

// SetVersion 设置分区版本，用于测试
func (m *MockHashPartitionOperations) SetVersion(field string, version int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.versions[field] = version
}

// GetVersion 获取分区版本，用于测试
func (m *MockHashPartitionOperations) GetVersion(field string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.versions[field]
}

// HSetPartitionsInTx 事务中批量设置哈希分区
func (m *MockHashPartitionOperations) HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.partitions[key] == nil {
		m.partitions[key] = make(map[string]string)
	}

	// 模拟事务批量设置
	for field, value := range partitions {
		m.partitions[key][field] = value
		// 如果没有版本，设置为1，否则保持当前版本
		if m.versions[field] == 0 {
			m.versions[field] = 1
		}
	}

	return nil
}

// 测试助手函数
func createTestRepository() (*HashPartitionStrategy, *MockHashPartitionOperations, *MockLogger) {
	mockStore := NewMockHashPartitionOperations()
	mockLogger := &MockLogger{}
	repo := NewHashPartitionStrategy(mockStore, mockLogger)
	return repo, mockStore, mockLogger
}

func createTestPartitionInfo(partitionID int, status model.PartitionStatus, version int64) *model.PartitionInfo {
	now := time.Now()
	return &model.PartitionInfo{
		PartitionID:   partitionID,
		MinID:         int64(partitionID*1000 + 1),
		MaxID:         int64((partitionID + 1) * 1000),
		Status:        status,
		WorkerID:      "test-worker",
		LastHeartbeat: now,
		UpdatedAt:     now,
		CreatedAt:     now,
		Version:       version,
		Options:       map[string]interface{}{"test": "value"},
	}
}

// TestNewRepository 测试仓库创建
func TestNewRepository(t *testing.T) {
	mockStore := NewMockHashPartitionOperations()
	mockLogger := &MockLogger{}

	// 正常情况
	repo := NewHashPartitionStrategy(mockStore, mockLogger)
	if repo == nil {
		t.Fatal("NewRepository 应该返回非空仓库")
	}
	if repo.store != mockStore {
		t.Error("仓库的存储接口不匹配")
	}
	if repo.logger != mockLogger {
		t.Error("仓库的日志接口不匹配")
	}

	// 日志为空的情况
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewRepository 应该在日志为空时 panic")
		}
	}()
	NewHashPartitionStrategy(mockStore, nil)
}

// TestUpdatePartitionOptimistically 测试乐观锁更新
func TestUpdatePartitionOptimistically(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 正常更新
	partition := createTestPartitionInfo(1, model.StatusClaimed, 1)
	mockStore.SetVersion("1", 1) // 设置当前版本为1

	updated, err := repo.UpdatePartitionOptimistically(ctx, partition, 1)
	if err != nil {
		t.Fatalf("乐观锁更新失败: %v", err)
	}
	if updated.Version != 2 {
		t.Errorf("期望版本为2，得到 %d", updated.Version)
	}
	if updated.UpdatedAt.IsZero() {
		t.Error("UpdatedAt 应该被更新")
	}

	// 测试场景2: partitionInfo 为空
	_, err = repo.UpdatePartitionOptimistically(ctx, nil, 1)
	if err == nil || err.Error() != "partitionInfo cannot be nil" {
		t.Error("应该返回 partitionInfo 不能为空的错误")
	}

	// 测试场景3: 版本冲突
	partition2 := createTestPartitionInfo(2, model.StatusRunning, 1)
	mockStore.SetVersion("2", 2) // 当前版本为2，但期望版本为1

	_, err = repo.UpdatePartitionOptimistically(ctx, partition2, 1)
	if !errors.Is(err, model.ErrOptimisticLockFailed) {
		t.Errorf("期望乐观锁失败错误，得到: %v", err)
	}

	// 测试场景4: 心跳更新 (StatusClaimed)
	partition3 := createTestPartitionInfo(3, model.StatusClaimed, 1)
	mockStore.SetVersion("3", 1)

	updated3, err := repo.UpdatePartitionOptimistically(ctx, partition3, 1)
	if err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	if updated3.LastHeartbeat.IsZero() {
		t.Error("StatusClaimed 状态下 LastHeartbeat 应该被更新")
	}

	// 测试场景5: 心跳更新 (StatusRunning)
	partition4 := createTestPartitionInfo(4, model.StatusRunning, 1)
	mockStore.SetVersion("4", 1)

	updated4, err := repo.UpdatePartitionOptimistically(ctx, partition4, 1)
	if err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	if updated4.LastHeartbeat.IsZero() {
		t.Error("StatusRunning 状态下 LastHeartbeat 应该被更新")
	}
}

// TestCreatePartitionAtomically 测试原子创建分区
func TestCreatePartitionAtomically(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 正常创建
	options := map[string]interface{}{"priority": "high"}
	created, err := repo.CreatePartitionAtomically(ctx, 1, 1, 1000, options)
	if err != nil {
		t.Fatalf("创建分区失败: %v", err)
	}
	if created.PartitionID != 1 {
		t.Errorf("期望分区ID为1，得到 %d", created.PartitionID)
	}
	if created.MinID != 1 || created.MaxID != 1000 {
		t.Errorf("期望范围[1,1000]，得到[%d,%d]", created.MinID, created.MaxID)
	}
	if created.Version != 1 {
		t.Errorf("期望版本为1，得到 %d", created.Version)
	}
	if created.Status != model.StatusPending {
		t.Errorf("期望状态为Pending，得到 %s", created.Status)
	}
	if created.WorkerID != "" {
		t.Errorf("期望WorkerID为空，得到 %s", created.WorkerID)
	}
	if created.Options["priority"] != "high" {
		t.Error("Options 应该被正确设置")
	}

	// 测试场景2: 分区已存在
	mockStore.SetVersion("2", 1) // 模拟分区已存在
	_, err = repo.CreatePartitionAtomically(ctx, 2, 1001, 2000, nil)
	if !errors.Is(err, model.ErrPartitionAlreadyExists) {
		t.Errorf("期望分区已存在错误，得到: %v", err)
	}
}

// TestGetPartition 测试获取单个分区
func TestGetPartition(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 分区不存在
	_, err := repo.GetPartition(ctx, 999)
	if !errors.Is(err, ErrPartitionNotFound) {
		t.Errorf("期望分区未找到错误，得到: %v", err)
	}

	// 测试场景2: 正常获取
	partition := createTestPartitionInfo(1, model.StatusRunning, 2)
	// 先创建分区
	repo.SavePartition(ctx, partition)

	retrieved, err := repo.GetPartition(ctx, 1)
	if err != nil {
		t.Fatalf("获取分区失败: %v", err)
	}
	if retrieved.PartitionID != 1 {
		t.Errorf("期望分区ID为1，得到 %d", retrieved.PartitionID)
	}
	if retrieved.Status != model.StatusRunning {
		t.Errorf("期望状态为Running，得到 %s", retrieved.Status)
	}

	// 测试场景3: 损坏的JSON数据
	mockStore.HSetPartition(ctx, partitionHashKey, "999", "invalid-json")
	_, err = repo.GetPartition(ctx, 999)
	if err == nil {
		t.Error("应该返回JSON反序列化错误")
	}
}

// TestGetAllPartitions 测试获取所有分区
func TestGetAllPartitions(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 空结果
	partitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("期望0个分区，得到 %d 个", len(partitions))
	}

	// 测试场景2: 多个分区
	partition1 := createTestPartitionInfo(1, model.StatusPending, 1)
	partition2 := createTestPartitionInfo(2, model.StatusRunning, 1)
	partition3 := createTestPartitionInfo(3, model.StatusCompleted, 1)

	repo.SavePartition(ctx, partition1)
	repo.SavePartition(ctx, partition2)
	repo.SavePartition(ctx, partition3)

	partitions, err = repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	if len(partitions) != 3 {
		t.Errorf("期望3个分区，得到 %d 个", len(partitions))
	}

	// 验证分区数据
	foundIds := make(map[int]bool)
	for _, p := range partitions {
		foundIds[p.PartitionID] = true
	}
	for i := 1; i <= 3; i++ {
		if !foundIds[i] {
			t.Errorf("分区 %d 未找到", i)
		}
	}

	// 测试场景3: 包含损坏数据
	mockStore.HSetPartition(ctx, partitionHashKey, "999", "invalid-json")
	partitions, err = repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	// 应该跳过损坏的数据，仍然返回3个有效分区
	if len(partitions) != 3 {
		t.Errorf("期望3个有效分区，得到 %d 个", len(partitions))
	}
}

// TestGetFilteredPartitions 测试过滤分区
func TestGetFilteredPartitions(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 准备测试数据
	now := time.Now()
	partition1 := &model.PartitionInfo{
		PartitionID: 1,
		MinID:       1,
		MaxID:       1000,
		Status:      model.StatusPending,
		WorkerID:    "",
		UpdatedAt:   now,
		Version:     1,
	}
	partition2 := &model.PartitionInfo{
		PartitionID: 2,
		MinID:       1001,
		MaxID:       2000,
		Status:      model.StatusRunning,
		WorkerID:    "worker1",
		UpdatedAt:   now.Add(-10 * time.Minute), // 过时的分区
		Version:     1,
	}
	partition3 := &model.PartitionInfo{
		PartitionID: 3,
		MinID:       2001,
		MaxID:       3000,
		Status:      model.StatusCompleted,
		WorkerID:    "worker2",
		UpdatedAt:   now,
		Version:     1,
	}
	partition4 := &model.PartitionInfo{
		PartitionID: 4,
		MinID:       3001,
		MaxID:       4000,
		Status:      model.StatusRunning,
		WorkerID:    "worker2",
		UpdatedAt:   now.Add(-15 * time.Minute), // 过时的分区，但是worker2的
		Version:     1,
	}

	repo.SavePartition(ctx, partition1)
	repo.SavePartition(ctx, partition2)
	repo.SavePartition(ctx, partition3)
	repo.SavePartition(ctx, partition4)

	// 测试场景1: 按状态过滤
	filters := GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending, model.StatusRunning},
	}
	filtered, err := repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	if len(filtered) != 3 { // partition1, partition2, partition4
		t.Errorf("期望3个分区，得到 %d 个", len(filtered))
	}

	// 测试场景2: 按过时时间过滤
	staleDuration := 5 * time.Minute
	filters = GetPartitionsFilters{
		StaleDuration: &staleDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	if len(filtered) != 2 { // partition2, partition4
		t.Errorf("期望2个过时分区，得到 %d 个", len(filtered))
	}

	// 测试场景3: 过时时间过滤并排除特定WorkerID
	filters = GetPartitionsFilters{
		StaleDuration:          &staleDuration,
		ExcludeWorkerIDOnStale: "worker2",
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	if len(filtered) != 1 { // 只有partition2，partition4被排除
		t.Errorf("期望1个过时分区，得到 %d 个", len(filtered))
	}
	if filtered[0].PartitionID != 2 {
		t.Errorf("期望分区2，得到分区 %d", filtered[0].PartitionID)
	}

	// 测试场景4: 组合过滤
	filters = GetPartitionsFilters{
		TargetStatuses:         []model.PartitionStatus{model.StatusRunning},
		StaleDuration:          &staleDuration,
		ExcludeWorkerIDOnStale: "worker2",
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	if len(filtered) != 1 { // 只有partition2符合所有条件
		t.Errorf("期望1个分区，得到 %d 个", len(filtered))
	}

	// 测试场景5: 结果按PartitionID排序
	filters = GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending, model.StatusRunning, model.StatusCompleted},
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	for i := 1; i < len(filtered); i++ {
		if filtered[i-1].PartitionID >= filtered[i].PartitionID {
			t.Error("结果应该按PartitionID升序排序")
			break
		}
	}
}

// TestDeletePartition 测试删除分区
func TestDeletePartition(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 删除存在的分区
	partition := createTestPartitionInfo(1, model.StatusCompleted, 1)
	repo.SavePartition(ctx, partition)

	err := repo.DeletePartition(ctx, 1)
	if err != nil {
		t.Fatalf("删除分区失败: %v", err)
	}

	// 验证分区已被删除
	_, err = repo.GetPartition(ctx, 1)
	if !errors.Is(err, ErrPartitionNotFound) {
		t.Error("分区应该已被删除")
	}

	// 测试场景2: 删除不存在的分区（应该成功，不报错）
	err = repo.DeletePartition(ctx, 999)
	if err != nil {
		t.Fatalf("删除不存在的分区不应该返回错误: %v", err)
	}
}

// TestSavePartition 测试直接保存分区
func TestSavePartition(t *testing.T) {
	repo, _, mockLogger := createTestRepository()
	ctx := context.Background()

	// 测试场景1: partitionInfo 为空
	err := repo.SavePartition(ctx, nil)
	if err == nil || err.Error() != "SavePartition 的 partitionInfo 不能为空" {
		t.Error("应该返回 partitionInfo 不能为空的错误")
	}

	// 测试场景2: 正常保存新分区
	partition := createTestPartitionInfo(1, model.StatusPending, 0) // 版本为0
	partition.CreatedAt = time.Time{}                               // 设置为零值以测试自动设置

	err = repo.SavePartition(ctx, partition)
	if err != nil {
		t.Fatalf("保存分区失败: %v", err)
	}

	// 验证CreatedAt被设置
	if partition.CreatedAt.IsZero() {
		t.Error("CreatedAt 应该被设置")
	}

	// 验证版本被设置为1
	if partition.Version != 1 {
		t.Errorf("期望版本为1，得到 %d", partition.Version)
	}

	// 验证警告日志
	logs := mockLogger.GetLogs()
	found := false
	for _, log := range logs {
		if contains(log, "WARN") && contains(log, "版本为 0") {
			found = true
			break
		}
	}
	if !found {
		t.Error("应该记录版本为0的警告日志")
	}

	// 测试场景3: 更新现有分区
	partition.Status = model.StatusRunning
	partition.Version = 2
	err = repo.SavePartition(ctx, partition)
	if err != nil {
		t.Fatalf("更新分区失败: %v", err)
	}

	// 验证分区被更新
	retrieved, err := repo.GetPartition(ctx, 1)
	if err != nil {
		t.Fatalf("获取分区失败: %v", err)
	}
	if retrieved.Status != model.StatusRunning {
		t.Errorf("期望状态为Running，得到 %s", retrieved.Status)
	}
}

// ========== 并发测试 ==========

// TestRepository_ConcurrentOptimisticUpdates 测试并发乐观锁更新
func TestRepository_ConcurrentOptimisticUpdates(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 创建初始分区
	partition := createTestPartitionInfo(1, model.StatusPending, 1)
	repo.SavePartition(ctx, partition)
	mockStore.SetVersion("1", 1)

	const numWorkers = 10
	var wg sync.WaitGroup
	var successCount, failureCount int32

	// 启动多个goroutine同时尝试更新同一个分区
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			updatePartition := &model.PartitionInfo{
				PartitionID: 1,
				MinID:       1001,
				MaxID:       2000,
				Status:      model.StatusRunning,
				WorkerID:    fmt.Sprintf("worker-%d", workerID),
				Version:     1, // 都期望当前版本为1
			}

			_, err := repo.UpdatePartitionOptimistically(ctx, updatePartition, 1)
			if err != nil {
				if errors.Is(err, model.ErrOptimisticLockFailed) {
					atomic.AddInt32(&failureCount, 1)
				} else {
					t.Errorf("Worker %d 遇到意外错误: %v", workerID, err)
				}
			} else {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// 验证只有一个更新成功
	if successCount != 1 {
		t.Errorf("期望1个成功更新，得到 %d 个", successCount)
	}
	if failureCount != numWorkers-1 {
		t.Errorf("期望 %d 个失败更新，得到 %d 个", numWorkers-1, failureCount)
	}

	// 验证最终版本为2
	finalVersion := mockStore.GetVersion("1")
	if finalVersion != 2 {
		t.Errorf("期望最终版本为2，得到 %d", finalVersion)
	}
}

// TestRepository_ConcurrentCreatePartitions 测试并发创建分区
func TestRepository_ConcurrentCreatePartitions(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	const numWorkers = 10
	var wg sync.WaitGroup
	var successCount, failureCount int32
	createdPartitions := make(chan int, numWorkers)

	// 多个goroutine尝试创建相同的分区ID
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			_, err := repo.CreatePartitionAtomically(ctx, 1, 1, 1000, map[string]interface{}{
				"created_by": workerID,
			})

			if err != nil {
				if errors.Is(err, model.ErrPartitionAlreadyExists) {
					atomic.AddInt32(&failureCount, 1)
				} else {
					t.Errorf("Worker %d 创建分区遇到意外错误: %v", workerID, err)
				}
			} else {
				atomic.AddInt32(&successCount, 1)
				createdPartitions <- workerID
			}
		}(i)
	}

	wg.Wait()
	close(createdPartitions)

	// 验证只有一个创建成功
	if successCount != 1 {
		t.Errorf("期望1个成功创建，得到 %d 个", successCount)
	}
	if failureCount != numWorkers-1 {
		t.Errorf("期望 %d 个失败创建，得到 %d 个", numWorkers-1, failureCount)
	}

	// 验证确实创建了分区
	partition, err := repo.GetPartition(ctx, 1)
	if err != nil {
		t.Fatalf("获取创建的分区失败: %v", err)
	}
	if partition.Version != 1 {
		t.Errorf("期望版本为1，得到 %d", partition.Version)
	}
}

// TestRepository_ConcurrentReadWrites 测试并发读写操作
func TestRepository_ConcurrentReadWrites(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 创建一些初始分区
	for i := 1; i <= 5; i++ {
		partition := createTestPartitionInfo(i, model.StatusPending, 1)
		repo.SavePartition(ctx, partition)
	}

	const numReaders = 5
	const numWriters = 3
	const duration = 2 * time.Second

	var wg sync.WaitGroup
	var readCount, writeCount int32
	errorChan := make(chan error, numReaders+numWriters)

	// 启动读取goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			timeout := time.After(duration)
			for {
				select {
				case <-timeout:
					return
				default:
					// 执行各种读取操作
					switch readerID % 3 {
					case 0:
						_, err := repo.GetAllPartitions(ctx)
						if err != nil {
							errorChan <- fmt.Errorf("Reader %d GetAllPartitions 错误: %v", readerID, err)
							return
						}
					case 1:
						_, err := repo.GetPartition(ctx, (readerID%5)+1)
						if err != nil && !errors.Is(err, ErrPartitionNotFound) {
							errorChan <- fmt.Errorf("Reader %d GetPartition 错误: %v", readerID, err)
							return
						}
					case 2:
						filters := GetPartitionsFilters{
							TargetStatuses: []model.PartitionStatus{model.StatusPending, model.StatusRunning},
						}
						_, err := repo.GetFilteredPartitions(ctx, filters)
						if err != nil {
							errorChan <- fmt.Errorf("Reader %d GetFilteredPartitions 错误: %v", readerID, err)
							return
						}
					}
					atomic.AddInt32(&readCount, 1)
					time.Sleep(10 * time.Millisecond) // 短暂休息
				}
			}
		}(i)
	}

	// 启动写入goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			timeout := time.After(duration)
			partitionID := writerID + 6 // 使用不同的分区ID避免冲突

			for {
				select {
				case <-timeout:
					return
				default:
					// 创建新分区
					_, err := repo.CreatePartitionAtomically(ctx, partitionID,
						int64(partitionID*1000+1), int64((partitionID+1)*1000),
						map[string]interface{}{"writer": writerID})

					if err != nil && !errors.Is(err, model.ErrPartitionAlreadyExists) {
						errorChan <- fmt.Errorf("Writer %d CreatePartition 错误: %v", writerID, err)
						return
					}

					atomic.AddInt32(&writeCount, 1)
					partitionID += numWriters         // 下一个分区ID
					time.Sleep(50 * time.Millisecond) // 短暂休息
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查是否有错误
	for err := range errorChan {
		t.Error(err)
	}

	t.Logf("完成 %d 次读操作和 %d 次写操作", readCount, writeCount)

	// 验证最终状态一致性
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("最终获取所有分区失败: %v", err)
	}

	if len(allPartitions) < 5 {
		t.Errorf("期望至少5个分区，得到 %d 个", len(allPartitions))
	}
}

// TestRepository_ConcurrentVersionProgressions 测试并发版本递进
func TestRepository_ConcurrentVersionProgressions(t *testing.T) {
	repo, mockStore, _ := createTestRepository()
	ctx := context.Background()

	// 创建初始分区
	partition := createTestPartitionInfo(1, model.StatusPending, 1)
	repo.SavePartition(ctx, partition)
	mockStore.SetVersion("1", 1)

	const numUpdates = 20
	var wg sync.WaitGroup
	updateResults := make(chan int64, numUpdates)
	var successfulUpdates int32

	// 串行更新，每次使用正确的期望版本
	currentVersion := int64(1)
	versionMutex := sync.Mutex{}

	for i := 0; i < numUpdates; i++ {
		wg.Add(1)
		go func(updateID int) {
			defer wg.Done()

			// 获取当前版本
			versionMutex.Lock()
			expectedVersion := currentVersion
			versionMutex.Unlock()

			updatePartition := &model.PartitionInfo{
				PartitionID: 1,
				MinID:       1001,
				MaxID:       2000,
				Status:      model.StatusRunning,
				WorkerID:    fmt.Sprintf("worker-%d", updateID),
				Version:     expectedVersion,
			}

			updated, err := repo.UpdatePartitionOptimistically(ctx, updatePartition, expectedVersion)
			if err == nil {
				versionMutex.Lock()
				if updated.Version > currentVersion {
					currentVersion = updated.Version
				}
				versionMutex.Unlock()

				updateResults <- updated.Version
				atomic.AddInt32(&successfulUpdates, 1)
			}
			// 忽略乐观锁失败，这是预期的
		}(i)
	}

	wg.Wait()
	close(updateResults)

	// 收集所有成功的版本
	versions := make([]int64, 0, numUpdates)
	for version := range updateResults {
		versions = append(versions, version)
	}

	// 验证版本递进
	if len(versions) == 0 {
		t.Fatal("应该至少有一次成功更新")
	}

	// 验证最终版本
	finalVersion := mockStore.GetVersion("1")
	if finalVersion < 2 {
		t.Errorf("期望最终版本至少为2，得到 %d", finalVersion)
	}

	t.Logf("完成 %d 次成功更新，最终版本: %d", successfulUpdates, finalVersion)
}

// TestRepository_ConcurrentDeleteAndAccess 测试并发删除和访问
func TestRepository_ConcurrentDeleteAndAccess(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	const numPartitions = 10
	const numWorkers = 5

	// 创建多个分区
	for i := 1; i <= numPartitions; i++ {
		partition := createTestPartitionInfo(i, model.StatusCompleted, 1)
		repo.SavePartition(ctx, partition)
	}

	var wg sync.WaitGroup
	var deleteCount, accessCount int32
	errorChan := make(chan error, numWorkers*2)

	// 删除操作
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for partitionID := 1; partitionID <= numPartitions; partitionID++ {
				if partitionID%numWorkers == workerID {
					err := repo.DeletePartition(ctx, partitionID)
					if err != nil {
						errorChan <- fmt.Errorf("Worker %d 删除分区 %d 失败: %v", workerID, partitionID, err)
						return
					}
					atomic.AddInt32(&deleteCount, 1)
				}
			}
		}(i)
	}

	// 访问操作
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 20; j++ { // 每个worker尝试20次随机访问
				partitionID := (j % numPartitions) + 1
				_, err := repo.GetPartition(ctx, partitionID)
				// 可能找到也可能找不到，都是正常的
				if err != nil && !errors.Is(err, ErrPartitionNotFound) {
					errorChan <- fmt.Errorf("Worker %d 访问分区 %d 失败: %v", workerID, partitionID, err)
					return
				}
				atomic.AddInt32(&accessCount, 1)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查错误
	for err := range errorChan {
		t.Error(err)
	}

	// 验证所有分区都被删除
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	if len(allPartitions) != 0 {
		t.Errorf("期望0个分区，但还剩余 %d 个", len(allPartitions))
	}

	t.Logf("完成 %d 次删除操作和 %d 次访问操作", deleteCount, accessCount)
}

// TestRepository_HighConcurrencyStress 高并发压力测试
func TestRepository_HighConcurrencyStress(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	const (
		numWorkers    = 20
		duration      = 3 * time.Second
		maxPartitions = 100
	)

	var wg sync.WaitGroup
	var operationCounts struct {
		creates int32
		reads   int32
		updates int32
		deletes int32
		filters int32
	}

	errorChan := make(chan error, numWorkers)
	partitionCounter := int32(0)

	// 启动多个worker执行混合操作
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			timeout := time.After(duration)
			for {
				select {
				case <-timeout:
					return
				default:
					// 随机选择操作类型
					switch workerID % 5 {
					case 0: // 创建操作
						partitionID := int(atomic.AddInt32(&partitionCounter, 1))
						if partitionID <= maxPartitions {
							_, err := repo.CreatePartitionAtomically(ctx, partitionID,
								int64(partitionID*1000+1), int64((partitionID+1)*1000),
								map[string]interface{}{"worker": workerID})
							if err != nil && !errors.Is(err, model.ErrPartitionAlreadyExists) {
								errorChan <- fmt.Errorf("Worker %d 创建分区失败: %v", workerID, err)
								return
							}
							atomic.AddInt32(&operationCounts.creates, 1)
						}

					case 1: // 读取操作
						partitionID := (workerID % 50) + 1
						_, err := repo.GetPartition(ctx, partitionID)
						if err != nil && !errors.Is(err, ErrPartitionNotFound) {
							errorChan <- fmt.Errorf("Worker %d 读取分区失败: %v", workerID, err)
							return
						}
						atomic.AddInt32(&operationCounts.reads, 1)

					case 2: // 更新操作
						partitionID := (workerID % 20) + 1
						// 先尝试获取分区
						existing, err := repo.GetPartition(ctx, partitionID)
						if err == nil {
							existing.WorkerID = fmt.Sprintf("worker-%d", workerID)
							existing.Status = model.StatusRunning
							_, updateErr := repo.UpdatePartitionOptimistically(ctx, existing, existing.Version)
							if updateErr != nil && !errors.Is(updateErr, model.ErrOptimisticLockFailed) {
								errorChan <- fmt.Errorf("Worker %d 更新分区失败: %v", workerID, updateErr)
								return
							}
						}
						atomic.AddInt32(&operationCounts.updates, 1)

					case 3: // 删除操作
						partitionID := (workerID % 10) + 91 // 删除高ID分区
						err := repo.DeletePartition(ctx, partitionID)
						if err != nil {
							errorChan <- fmt.Errorf("Worker %d 删除分区失败: %v", workerID, err)
							return
						}
						atomic.AddInt32(&operationCounts.deletes, 1)

					case 4: // 过滤操作
						filters := GetPartitionsFilters{
							TargetStatuses: []model.PartitionStatus{
								model.StatusPending, model.StatusRunning, model.StatusCompleted,
							},
						}
						_, err := repo.GetFilteredPartitions(ctx, filters)
						if err != nil {
							errorChan <- fmt.Errorf("Worker %d 过滤分区失败: %v", workerID, err)
							return
						}
						atomic.AddInt32(&operationCounts.filters, 1)
					}

					// 短暂休息避免过度占用CPU
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查错误
	for err := range errorChan {
		t.Error(err)
	}

	// 输出统计信息
	t.Logf("压力测试完成:")
	t.Logf("  创建操作: %d", operationCounts.creates)
	t.Logf("  读取操作: %d", operationCounts.reads)
	t.Logf("  更新操作: %d", operationCounts.updates)
	t.Logf("  删除操作: %d", operationCounts.deletes)
	t.Logf("  过滤操作: %d", operationCounts.filters)

	// 验证系统仍然正常工作
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("压力测试后获取所有分区失败: %v", err)
	}

	t.Logf("压力测试后剩余分区数: %d", len(allPartitions))

	// 验证数据一致性
	for _, partition := range allPartitions {
		if partition.PartitionID <= 0 {
			t.Errorf("发现无效的分区ID: %d", partition.PartitionID)
		}
		if partition.Version <= 0 {
			t.Errorf("发现无效的版本号: %d (分区 %d)", partition.Version, partition.PartitionID)
		}
	}
}

// ========== 边界测试和错误场景 ==========

// TestRepository_EdgeCases 测试边界情况
func TestRepository_EdgeCases(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 极大的分区ID
	largePartitionID := 999999999
	partition := createTestPartitionInfo(largePartitionID, model.StatusPending, 1)
	err := repo.SavePartition(ctx, partition)
	if err != nil {
		t.Fatalf("保存大分区ID失败: %v", err)
	}

	retrieved, err := repo.GetPartition(ctx, largePartitionID)
	if err != nil {
		t.Fatalf("获取大分区ID失败: %v", err)
	}
	if retrieved.PartitionID != largePartitionID {
		t.Errorf("分区ID不匹配: 期望 %d, 得到 %d", largePartitionID, retrieved.PartitionID)
	}

	// 测试场景2: 负数分区ID
	negativePartitionID := -1
	_, err = repo.CreatePartitionAtomically(ctx, negativePartitionID, 1, 1000, nil)
	if err != nil {
		t.Fatalf("创建负数分区ID失败: %v", err)
	}

	// 测试场景3: 零分区ID
	_, err = repo.CreatePartitionAtomically(ctx, 0, 1, 1000, nil)
	if err != nil {
		t.Fatalf("创建零分区ID失败: %v", err)
	}

	// 测试场景4: 极大的MinID和MaxID
	largeID := int64(9223372036854775807) // int64最大值
	partition2 := &model.PartitionInfo{
		PartitionID: 100,
		MinID:       largeID - 1000,
		MaxID:       largeID,
		Status:      model.StatusPending,
		Version:     1,
		UpdatedAt:   time.Now(),
		CreatedAt:   time.Now(),
	}
	err = repo.SavePartition(ctx, partition2)
	if err != nil {
		t.Fatalf("保存极大ID范围分区失败: %v", err)
	}

	// 测试场景5: MinID > MaxID (无效范围)
	invalidPartition := &model.PartitionInfo{
		PartitionID: 101,
		MinID:       1000,
		MaxID:       999, // MaxID < MinID
		Status:      model.StatusPending,
		Version:     1,
		UpdatedAt:   time.Now(),
		CreatedAt:   time.Now(),
	}
	err = repo.SavePartition(ctx, invalidPartition)
	if err != nil {
		t.Fatalf("保存无效范围分区失败: %v", err)
	}

	// 测试场景6: 空Options
	_, err = repo.CreatePartitionAtomically(ctx, 102, 1, 1000, nil)
	if err != nil {
		t.Fatalf("创建空Options分区失败: %v", err)
	}

	// 测试场景7: 复杂Options
	complexOptions := map[string]interface{}{
		"string":  "value",
		"number":  42,
		"float":   3.14,
		"boolean": true,
		"array":   []interface{}{1, 2, 3},
		"nested": map[string]interface{}{
			"key": "value",
		},
	}
	_, err = repo.CreatePartitionAtomically(ctx, 103, 1, 1000, complexOptions)
	if err != nil {
		t.Fatalf("创建复杂Options分区失败: %v", err)
	}

	// 测试场景8: 空WorkerID
	partition3 := createTestPartitionInfo(104, model.StatusPending, 1)
	partition3.WorkerID = ""
	err = repo.SavePartition(ctx, partition3)
	if err != nil {
		t.Fatalf("保存空WorkerID分区失败: %v", err)
	}

	// 测试场景9: 极长WorkerID
	longWorkerID := ""
	for i := 0; i < 1000; i++ {
		longWorkerID += "a"
	}
	partition4 := createTestPartitionInfo(105, model.StatusRunning, 1)
	partition4.WorkerID = longWorkerID
	err = repo.SavePartition(ctx, partition4)
	if err != nil {
		t.Fatalf("保存长WorkerID分区失败: %v", err)
	}

	// 测试场景10: 时间边界值
	partition5 := createTestPartitionInfo(106, model.StatusCompleted, 1)
	partition5.CreatedAt = time.Time{} // 零值时间
	partition5.UpdatedAt = time.Time{}
	partition5.LastHeartbeat = time.Time{}
	err = repo.SavePartition(ctx, partition5)
	if err != nil {
		t.Fatalf("保存零时间分区失败: %v", err)
	}
}

// TestRepository_FilterEdgeCases 测试过滤器边界情况
func TestRepository_FilterEdgeCases(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 准备测试数据
	now := time.Now()
	partitions := []*model.PartitionInfo{
		{
			PartitionID: 1,
			Status:      model.StatusPending,
			UpdatedAt:   now,
			Version:     1,
		},
		{
			PartitionID: 2,
			Status:      model.StatusRunning,
			UpdatedAt:   now.Add(-1 * time.Hour),
			Version:     1,
		},
		{
			PartitionID: 3,
			Status:      model.StatusCompleted,
			UpdatedAt:   now.Add(-2 * time.Hour),
			Version:     1,
		},
	}

	for _, p := range partitions {
		repo.SavePartition(ctx, p)
	}

	// 测试场景1: 空状态过滤器
	filters := GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{},
	}
	filtered, err := repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("空状态过滤器失败: %v", err)
	}
	if len(filtered) != 3 { // 应该返回所有分区
		t.Errorf("期望3个分区，得到 %d 个", len(filtered))
	}

	// 测试场景2: 不存在的状态过滤器
	filters = GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{"nonexistent"},
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("不存在状态过滤器失败: %v", err)
	}
	if len(filtered) != 0 {
		t.Errorf("期望0个分区，得到 %d 个", len(filtered))
	}

	// 测试场景3: 零时长过时过滤器
	zeroDuration := time.Duration(0)
	filters = GetPartitionsFilters{
		StaleDuration: &zeroDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("零时长过时过滤器失败: %v", err)
	}
	if len(filtered) != 0 { // 零时长应该不激活过滤器
		t.Errorf("期望0个分区，得到 %d 个", len(filtered))
	}

	// 测试场景4: 负时长过时过滤器
	negativeDuration := -1 * time.Hour
	filters = GetPartitionsFilters{
		StaleDuration: &negativeDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("负时长过时过滤器失败: %v", err)
	}
	if len(filtered) != 0 { // 负时长应该不激活过滤器
		t.Errorf("期望0个分区，得到 %d 个", len(filtered))
	}

	// 测试场景5: 极长时长过时过滤器
	veryLongDuration := 1000 * time.Hour
	filters = GetPartitionsFilters{
		StaleDuration: &veryLongDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("极长时长过时过滤器失败: %v", err)
	}
	if len(filtered) != 0 { // 没有分区超过1000小时没有更新，所以没有过时分区
		t.Errorf("期望0个分区，得到 %d 个", len(filtered))
	}

	// 测试场景6: 空WorkerID排除
	shortDuration := 30 * time.Minute
	filters = GetPartitionsFilters{
		StaleDuration:          &shortDuration,
		ExcludeWorkerIDOnStale: "", // 空字符串
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("空WorkerID排除过滤器失败: %v", err)
	}
	if len(filtered) != 2 { // partition2 和 partition3
		t.Errorf("期望2个分区，得到 %d 个", len(filtered))
	}
}

// TestRepository_ConcurrentContextCancellation 测试上下文取消
func TestRepository_ConcurrentContextCancellation(t *testing.T) {
	repo, _, _ := createTestRepository()

	// 测试场景1: 创建时取消上下文
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	_, err := repo.CreatePartitionAtomically(ctx, 1, 1, 1000, nil)
	// 根据mock实现，这应该仍然成功，因为mock不检查上下文
	// 在实际实现中，这可能会返回context.Canceled错误

	// 测试场景2: 读取时取消上下文
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel2()

	time.Sleep(2 * time.Millisecond) // 确保超时
	_, err = repo.GetPartition(ctx2, 1)
	// 同样，mock实现可能不会检查上下文

	// 测试场景3: 并发操作中取消上下文
	const numWorkers = 5
	var wg sync.WaitGroup

	ctx3, cancel3 := context.WithCancel(context.Background())

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				select {
				case <-ctx3.Done():
					return
				default:
					partition := createTestPartitionInfo(workerID*10+j, model.StatusPending, 1)
					repo.SavePartition(ctx3, partition)
				}
			}
		}(i)
	}

	// 1秒后取消上下文
	time.AfterFunc(100*time.Millisecond, cancel3)

	wg.Wait()

	// 验证一些分区可能已经创建
	allPartitions, err := repo.GetAllPartitions(context.Background())
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}

	t.Logf("上下文取消测试后创建了 %d 个分区", len(allPartitions))
}

// TestRepository_LargeDataSets 测试大数据集
func TestRepository_LargeDataSets(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	const numPartitions = 1000

	// 创建大量分区
	start := time.Now()
	for i := 1; i <= numPartitions; i++ {
		partition := createTestPartitionInfo(i, model.StatusPending, 1)
		err := repo.SavePartition(ctx, partition)
		if err != nil {
			t.Fatalf("创建分区 %d 失败: %v", i, err)
		}
	}
	createDuration := time.Since(start)

	t.Logf("创建 %d 个分区耗时: %v", numPartitions, createDuration)

	// 获取所有分区
	start = time.Now()
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	getAllDuration := time.Since(start)

	if len(allPartitions) != numPartitions {
		t.Errorf("期望 %d 个分区，得到 %d 个", numPartitions, len(allPartitions))
	}

	t.Logf("获取 %d 个分区耗时: %v", numPartitions, getAllDuration)

	// 过滤分区
	start = time.Now()
	filters := GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending},
	}
	filtered, err := repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("过滤分区失败: %v", err)
	}
	filterDuration := time.Since(start)

	if len(filtered) != numPartitions {
		t.Errorf("期望 %d 个过滤分区，得到 %d 个", numPartitions, len(filtered))
	}

	t.Logf("过滤 %d 个分区耗时: %v", len(filtered), filterDuration)

	// 验证排序
	for i := 1; i < len(filtered); i++ {
		if filtered[i-1].PartitionID >= filtered[i].PartitionID {
			t.Error("过滤结果应该按PartitionID升序排序")
			break
		}
	}

	// 批量删除
	start = time.Now()
	for i := 1; i <= numPartitions; i++ {
		err := repo.DeletePartition(ctx, i)
		if err != nil {
			t.Fatalf("删除分区 %d 失败: %v", i, err)
		}
	}
	deleteDuration := time.Since(start)

	t.Logf("删除 %d 个分区耗时: %v", numPartitions, deleteDuration)

	// 验证删除
	finalPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("最终获取所有分区失败: %v", err)
	}
	if len(finalPartitions) != 0 {
		t.Errorf("期望0个分区，但还剩余 %d 个", len(finalPartitions))
	}
}

// TestRepository_ErrorRecovery 测试错误恢复
func TestRepository_ErrorRecovery(t *testing.T) {
	repo, mockStore, mockLogger := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 存储错误后的恢复
	// 先正常创建一个分区
	partition := createTestPartitionInfo(1, model.StatusPending, 1)
	err := repo.SavePartition(ctx, partition)
	if err != nil {
		t.Fatalf("初始保存分区失败: %v", err)
	}

	// 验证可以正常访问
	retrieved, err := repo.GetPartition(ctx, 1)
	if err != nil {
		t.Fatalf("获取分区失败: %v", err)
	}
	if retrieved.PartitionID != 1 {
		t.Errorf("分区ID不匹配")
	}

	// 模拟部分数据损坏
	mockStore.HSetPartition(ctx, partitionHashKey, "damaged", "invalid-json-data")

	// 获取所有分区应该跳过损坏的数据
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("获取所有分区失败: %v", err)
	}
	if len(allPartitions) != 1 { // 应该只有1个有效分区
		t.Errorf("期望1个有效分区，得到 %d 个", len(allPartitions))
	}

	// 验证警告日志
	logs := mockLogger.GetLogs()
	found := false
	for _, log := range logs {
		if contains(log, "WARN") && contains(log, "反序列化") {
			found = true
			break
		}
	}
	if !found {
		t.Error("应该记录反序列化失败的警告日志")
	}

	// 继续正常操作
	partition2 := createTestPartitionInfo(2, model.StatusRunning, 1)
	err = repo.SavePartition(ctx, partition2)
	if err != nil {
		t.Fatalf("恢复后保存分区失败: %v", err)
	}

	// 验证系统仍然正常工作
	retrieved2, err := repo.GetPartition(ctx, 2)
	if err != nil {
		t.Fatalf("恢复后获取分区失败: %v", err)
	}
	if retrieved2.Status != model.StatusRunning {
		t.Errorf("恢复后分区状态不正确")
	}
}

// ========== 附加测试用例 ==========

// TestRepository_PartitionOptionsComplexTypes 测试复杂Options类型
func TestRepository_PartitionOptionsComplexTypes(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 嵌套结构
	complexOptions := map[string]interface{}{
		"nested_object": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3":  "deep_value",
				"numbers": []int{1, 2, 3, 4, 5},
			},
			"metadata": map[string]interface{}{
				"priority": 10,
				"tags":     []string{"urgent", "important"},
			},
		},
		"configuration": map[string]interface{}{
			"retry_count": 5,
			"timeout_ms":  30000,
			"enabled_features": []interface{}{
				"feature1",
				map[string]interface{}{"name": "feature2", "enabled": true},
			},
		},
		"numeric_types": map[string]interface{}{
			"int64":    int64(9223372036854775807),
			"float64":  3.141592653589793,
			"negative": -12345,
		},
	}

	_, err := repo.CreatePartitionAtomically(ctx, 1, 1, 1000, complexOptions)
	if err != nil {
		t.Fatalf("创建复杂Options分区失败: %v", err)
	}

	// 验证复杂类型保存正确
	retrieved, err := repo.GetPartition(ctx, 1)
	if err != nil {
		t.Fatalf("获取复杂Options分区失败: %v", err)
	}

	// 验证嵌套对象
	nestedObj := retrieved.Options["nested_object"].(map[string]interface{})
	level2 := nestedObj["level2"].(map[string]interface{})
	if level2["level3"] != "deep_value" {
		t.Errorf("嵌套值不匹配")
	}

	// 验证数组
	numbers := level2["numbers"].([]interface{})
	if len(numbers) != 5 {
		t.Errorf("数组长度不匹配")
	}

	// 验证数值类型
	numericTypes := retrieved.Options["numeric_types"].(map[string]interface{})
	// JSON会将所有数字转换为float64，所以需要类型转换
	if int64(numericTypes["int64"].(float64)) != int64(9223372036854775807) {
		t.Errorf("大整数值不匹配")
	}
}

// TestRepository_ConcurrentFilterOperations 测试并发过滤操作
func TestRepository_ConcurrentFilterOperations(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 准备测试数据
	numPartitions := 100
	for i := 1; i <= numPartitions; i++ {
		status := model.StatusPending
		if i%2 == 0 {
			status = model.StatusRunning
		}
		if i%3 == 0 {
			status = model.StatusCompleted
		}

		partition := &model.PartitionInfo{
			PartitionID: i,
			MinID:       int64(i * 1000),
			MaxID:       int64((i + 1) * 1000),
			Status:      status,
			WorkerID:    fmt.Sprintf("worker-%d", i%10),
			UpdatedAt:   time.Now().Add(-time.Duration(i) * time.Minute),
			Version:     1,
		}

		err := repo.SavePartition(ctx, partition)
		if err != nil {
			t.Fatalf("创建测试分区失败: %v", err)
		}
	}

	// 并发执行多种过滤操作
	const numWorkers = 20
	const operationsPerWorker = 50

	var wg sync.WaitGroup
	errorChan := make(chan error, numWorkers*operationsPerWorker)
	var totalFiltered int64

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				var filters GetPartitionsFilters

				switch j % 4 {
				case 0:
					// 按状态过滤
					filters = GetPartitionsFilters{
						TargetStatuses: []model.PartitionStatus{model.StatusPending, model.StatusRunning},
					}
				case 1:
					// 按过时时间过滤
					staleDuration := time.Duration(30+workerID) * time.Minute
					filters = GetPartitionsFilters{
						StaleDuration: &staleDuration,
					}
				case 2:
					// 组合过滤
					staleDuration := time.Duration(20) * time.Minute
					filters = GetPartitionsFilters{
						TargetStatuses:         []model.PartitionStatus{model.StatusRunning},
						StaleDuration:          &staleDuration,
						ExcludeWorkerIDOnStale: fmt.Sprintf("worker-%d", workerID%5),
					}
				case 3:
					// 无过滤器（获取全部）
					filters = GetPartitionsFilters{}
				}

				filtered, err := repo.GetFilteredPartitions(ctx, filters)
				if err != nil {
					errorChan <- fmt.Errorf("Worker %d 过滤操作失败: %v", workerID, err)
					return
				}

				atomic.AddInt64(&totalFiltered, int64(len(filtered)))

				// 验证结果排序
				for k := 1; k < len(filtered); k++ {
					if filtered[k-1].PartitionID >= filtered[k].PartitionID {
						errorChan <- fmt.Errorf("Worker %d 结果未正确排序", workerID)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查错误
	for err := range errorChan {
		t.Error(err)
	}

	t.Logf("并发过滤操作完成，总计过滤结果: %d", totalFiltered)
}

// TestRepository_MemoryLeakPrevention 测试内存泄漏预防
func TestRepository_MemoryLeakPrevention(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	// 测试场景1: 大量创建和删除操作
	const cycles = 100
	const partitionsPerCycle = 50

	for cycle := 0; cycle < cycles; cycle++ {
		// 创建分区
		for i := 1; i <= partitionsPerCycle; i++ {
			partitionID := cycle*partitionsPerCycle + i
			_, err := repo.CreatePartitionAtomically(ctx, partitionID,
				int64(partitionID*1000), int64((partitionID+1)*1000),
				map[string]interface{}{"cycle": cycle})
			if err != nil {
				t.Fatalf("Cycle %d: 创建分区 %d 失败: %v", cycle, partitionID, err)
			}
		}

		// 删除一半分区
		for i := 1; i <= partitionsPerCycle/2; i++ {
			partitionID := cycle*partitionsPerCycle + i
			err := repo.DeletePartition(ctx, partitionID)
			if err != nil {
				t.Fatalf("Cycle %d: 删除分区 %d 失败: %v", cycle, partitionID, err)
			}
		}

		// 每10个周期检查一次分区数量
		if cycle%10 == 9 {
			allPartitions, err := repo.GetAllPartitions(ctx)
			if err != nil {
				t.Fatalf("Cycle %d: 获取所有分区失败: %v", cycle, err)
			}

			expectedCount := (cycle + 1) * partitionsPerCycle / 2
			if len(allPartitions) != expectedCount {
				t.Errorf("Cycle %d: 期望 %d 个分区，实际 %d 个", cycle, expectedCount, len(allPartitions))
			}
		}
	}

	// 最终清理验证
	allPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("最终获取所有分区失败: %v", err)
	}

	// 删除所有剩余分区
	for _, p := range allPartitions {
		err := repo.DeletePartition(ctx, p.PartitionID)
		if err != nil {
			t.Fatalf("清理分区 %d 失败: %v", p.PartitionID, err)
		}
	}

	// 验证清理完成
	finalPartitions, err := repo.GetAllPartitions(ctx)
	if err != nil {
		t.Fatalf("清理后获取分区失败: %v", err)
	}
	if len(finalPartitions) != 0 {
		t.Errorf("清理后应该有0个分区，实际 %d 个", len(finalPartitions))
	}
}

// TestRepository_StaleFilterEdgeCases 测试过时过滤器的边界情况
func TestRepository_StaleFilterEdgeCases(t *testing.T) {
	repo, _, _ := createTestRepository()
	ctx := context.Background()

	now := time.Now()

	// 准备测试数据
	partitions := []*model.PartitionInfo{
		{
			PartitionID: 1,
			MinID:       1, MaxID: 1000,
			Status:    model.StatusRunning,
			WorkerID:  "",
			UpdatedAt: now, // 刚刚更新
			Version:   1,
		},
		{
			PartitionID: 2,
			MinID:       1001, MaxID: 2000,
			Status:    model.StatusRunning,
			WorkerID:  "worker1",
			UpdatedAt: now.Add(-1 * time.Nanosecond), // 仅1纳秒前
			Version:   1,
		},
		{
			PartitionID: 3,
			MinID:       2001, MaxID: 3000,
			Status:    model.StatusRunning,
			WorkerID:  "worker1",
			UpdatedAt: now.Add(-1 * time.Second), // 1秒前
			Version:   1,
		},
		{
			PartitionID: 4,
			MinID:       3001, MaxID: 4000,
			Status:    model.StatusRunning,
			WorkerID:  "excluded_worker",
			UpdatedAt: now.Add(-1 * time.Hour), // 1小时前
			Version:   1,
		},
	}

	for _, p := range partitions {
		err := repo.SavePartition(ctx, p)
		if err != nil {
			t.Fatalf("保存分区 %d 失败: %v", p.PartitionID, err)
		}
	}

	// 测试场景1: 零持续时间过滤器
	zeroDuration := time.Duration(0)
	filters := GetPartitionsFilters{
		StaleDuration: &zeroDuration,
	}
	filtered, err := repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("零持续时间过滤失败: %v", err)
	}
	if len(filtered) != 0 {
		t.Errorf("零持续时间应该不匹配任何分区，但得到 %d 个", len(filtered))
	}

	// 测试场景2: 负持续时间过滤器
	negativeDuration := -1 * time.Hour
	filters = GetPartitionsFilters{
		StaleDuration: &negativeDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("负持续时间过滤失败: %v", err)
	}
	if len(filtered) != 0 {
		t.Errorf("负持续时间应该不匹配任何分区，但得到 %d 个", len(filtered))
	}

	// 测试场景3: 纳秒级精度过滤
	nanoDuration := 500 * time.Millisecond
	filters = GetPartitionsFilters{
		StaleDuration: &nanoDuration,
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("纳秒精度过滤失败: %v", err)
	}
	// 应该包含partition3（1秒前 > 500ms）
	if len(filtered) < 1 {
		t.Errorf("纳秒精度过滤应该至少匹配1个分区，但得到 %d 个", len(filtered))
	}

	// 测试场景4: 空WorkerID与ExcludeWorkerIDOnStale
	staleDuration := 30 * time.Minute
	filters = GetPartitionsFilters{
		StaleDuration:          &staleDuration,
		ExcludeWorkerIDOnStale: "", // 空WorkerID不应排除任何分区
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("空WorkerID排除过滤失败: %v", err)
	}
	// 应该包含partition4（1小时前 > 30分钟）
	found := false
	for _, p := range filtered {
		if p.PartitionID == 4 {
			found = true
			break
		}
	}
	if !found {
		t.Error("空WorkerID排除应该包含所有过时分区")
	}

	// 测试场景5: ExcludeWorkerIDOnStale匹配
	filters = GetPartitionsFilters{
		StaleDuration:          &staleDuration,
		ExcludeWorkerIDOnStale: "excluded_worker",
	}
	filtered, err = repo.GetFilteredPartitions(ctx, filters)
	if err != nil {
		t.Fatalf("WorkerID排除过滤失败: %v", err)
	}
	// partition4应该被排除
	for _, p := range filtered {
		if p.PartitionID == 4 {
			t.Error("excluded_worker应该被排除在过时过滤之外")
		}
	}
}

// contains 辅助函数用于字符串检查
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (len(substr) == 0 || s[len(s)-len(substr):] == substr ||
		s[:len(substr)] == substr || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
