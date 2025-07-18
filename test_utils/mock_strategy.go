package test_utils

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"sync"
	"time"
)

// MockPartitionStrategy 实现 partition.PartitionStrategy 接口，用于测试
type MockPartitionStrategy struct {
	Partitions map[int]*model.PartitionInfo
	mutex      sync.RWMutex

	// 可配置的行为函数，用于测试特殊场景
	GetFilteredPartitionsFunc func(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error)
	StopFunc                  func(ctx context.Context) error // 用于控制Stop方法的行为
}

// NewMockPartitionStrategy 创建一个新的模拟分区策略实例
func NewMockPartitionStrategy() *MockPartitionStrategy {
	return &MockPartitionStrategy{
		Partitions: make(map[int]*model.PartitionInfo),
	}
}

// AddPartition 添加分区（测试辅助方法）
func (m *MockPartitionStrategy) AddPartition(partition *model.PartitionInfo) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 创建副本避免外部修改影响内部状态
	cp := *partition
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now()
	}
	if cp.UpdatedAt.IsZero() {
		cp.UpdatedAt = time.Now()
	}

	m.Partitions[partition.PartitionID] = &cp
}

// GetPartition 获取单个分区
func (m *MockPartitionStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	partition, exists := m.Partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// 返回副本避免并发修改
	result := *partition
	return &result, nil
}

// GetAllPartitions 获取所有分区
func (m *MockPartitionStrategy) GetAllActivePartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make([]*model.PartitionInfo, 0, len(m.Partitions))
	for _, partition := range m.Partitions {
		// 返回副本避免并发修改
		copy := *partition
		result = append(result, &copy)
	}

	return result, nil
}

// DeletePartition 删除分区
func (m *MockPartitionStrategy) DeletePartition(ctx context.Context, partitionID int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.Partitions, partitionID)
	return nil
}

// GetFilteredPartitions 根据过滤器获取分区
func (m *MockPartitionStrategy) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	// 如果设置了自定义函数，使用自定义函数
	if m.GetFilteredPartitionsFunc != nil {
		return m.GetFilteredPartitionsFunc(ctx, filters)
	}

	// 默认实现
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make([]*model.PartitionInfo, 0)

	for _, p := range m.Partitions {
		// 状态过滤
		if len(filters.TargetStatuses) > 0 {
			found := false
			for _, status := range filters.TargetStatuses {
				if p.Status == status {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// 排除分区ID过滤
		excluded := false
		for _, excludeID := range filters.ExcludePartitionIDs {
			if p.PartitionID == excludeID {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		// ID范围过滤
		if filters.MinID != nil && p.PartitionID < *filters.MinID {
			continue
		}
		if filters.MaxID != nil && p.PartitionID > *filters.MaxID {
			continue
		}

		// 过时检查
		if filters.StaleDuration != nil && *filters.StaleDuration > 0 {
			if time.Since(p.UpdatedAt) > *filters.StaleDuration {
				// 如果指定了排除WorkerID，且分区的WorkerID匹配，则不视为过时
				if filters.ExcludeWorkerIDOnStale != "" && p.WorkerID == filters.ExcludeWorkerIDOnStale {
					continue
				}
			} else {
				// 不满足过时条件
				continue
			}
		}

		// 返回副本
		copy := *p
		result = append(result, &copy)

		// 限制数量
		if filters.Limit > 0 && len(result) >= filters.Limit {
			break
		}
	}

	return result, nil
}

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
func (m *MockPartitionStrategy) CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	result := make([]*model.PartitionInfo, 0, len(request.Partitions))
	now := time.Now()

	for _, req := range request.Partitions {
		// 检查分区是否已存在
		if existing, exists := m.Partitions[req.PartitionID]; exists {
			// 返回现有分区的副本
			copy := *existing
			result = append(result, &copy)
		} else {
			// 创建新分区
			newPartition := &model.PartitionInfo{
				PartitionID: req.PartitionID,
				MinID:       req.MinID,
				MaxID:       req.MaxID,
				Status:      model.StatusPending,
				Options:     req.Options,
				CreatedAt:   now,
				UpdatedAt:   now,
			}

			m.Partitions[req.PartitionID] = newPartition

			// 返回副本
			copy := *newPartition
			result = append(result, &copy)
		}
	}

	return result, nil
}

// DeletePartitions 批量删除分区
func (m *MockPartitionStrategy) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, id := range partitionIDs {
		delete(m.Partitions, id)
	}

	return nil
}

// UpdatePartition 安全更新分区信息
func (m *MockPartitionStrategy) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	existing, exists := m.Partitions[partitionInfo.PartitionID]

	if !exists {
		if options != nil && options.Upsert {
			// 创建新分区
			newPartition := *partitionInfo
			newPartition.CreatedAt = time.Now()
			newPartition.UpdatedAt = time.Now()
			m.Partitions[partitionInfo.PartitionID] = &newPartition

			copy := newPartition
			return &copy, nil
		}
		return nil, fmt.Errorf("partition %d not found", partitionInfo.PartitionID)
	}

	// 版本检查（乐观锁）
	if options != nil && options.ExpectedVersion != nil {
		if existing.Version != *options.ExpectedVersion {
			return nil, fmt.Errorf("version mismatch: expected %d, got %d", *options.ExpectedVersion, existing.Version)
		}
	}

	// 更新分区
	updated := *partitionInfo
	updated.CreatedAt = existing.CreatedAt // 保持创建时间
	updated.UpdatedAt = time.Now()
	updated.Version = existing.Version + 1 // 增加版本号

	m.Partitions[partitionInfo.PartitionID] = &updated

	copy := updated
	return &copy, nil
}

// StrategyType 获取策略类型标识
func (m *MockPartitionStrategy) StrategyType() model.StrategyType {
	return model.StrategyTypeSimple // 返回简单策略类型作为mock策略
}

// AcquirePartition 声明对指定分区的持有权
func (m *MockPartitionStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	p, exists := m.Partitions[partitionID]
	if !exists {
		return nil, false, fmt.Errorf("partition %d not found", partitionID)
	}

	// 设置默认选项
	if options == nil {
		options = &model.AcquirePartitionOptions{}
	}

	// 检查分区是否可获取
	if p.WorkerID != "" && p.WorkerID != workerID {
		if !options.AllowPreemption {
			// 正常的"无法获取"情况，不返回错误
			return nil, false, nil
		}
	}

	// 获取分区
	p.WorkerID = workerID
	p.Status = model.StatusClaimed
	p.UpdatedAt = time.Now()
	p.Version++

	partitionInfo := *p
	return &partitionInfo, true, nil
}

// UpdatePartitionStatus 更新分区状态
func (m *MockPartitionStrategy) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	partition, exists := m.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// 验证权限
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		return fmt.Errorf("worker %s is not authorized to update partition %d", workerID, partitionID)
	}

	// 更新状态
	partition.Status = status
	partition.UpdatedAt = time.Now()
	partition.Version++

	if metadata != nil {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for k, v := range metadata {
			partition.Options[k] = v
		}
	}

	return nil
}

// ReleasePartition 释放分区
func (m *MockPartitionStrategy) ReleasePartition(ctx context.Context, partitionID int, workerID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	partition, exists := m.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// 验证权限
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		return fmt.Errorf("worker %s is not authorized to release partition %d", workerID, partitionID)
	}

	// 释放分区
	partition.WorkerID = ""
	partition.UpdatedAt = time.Now()
	partition.Version++

	return nil
}

// MaintainPartitionHold 维护对分区的持有权
func (m *MockPartitionStrategy) MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if workerID == "" {
		return fmt.Errorf("工作节点ID不能为空")
	}

	partition, exists := m.Partitions[partitionID]
	if !exists {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	// 检查分区是否被该工作节点持有
	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 没有持有分区 %d（当前持有者: %s）", workerID, partitionID, partition.WorkerID)
	}

	// 模拟维护持有权（更新时间戳）
	partition.UpdatedAt = time.Now()

	// 对于HashPartitionStrategy，还会更新LastHeartbeat
	if !partition.LastHeartbeat.IsZero() {
		now := time.Now()
		partition.LastHeartbeat = now
	}

	return nil
}

// GetPartitionStats 获取分区状态统计信息
func (m *MockPartitionStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	stats := &model.PartitionStats{
		Total:           len(m.Partitions),
		MaxPartitionID:  0,
		LastAllocatedID: 0,
	}

	for _, partition := range m.Partitions {
		switch partition.Status {
		case model.StatusPending:
			stats.Pending++
		case model.StatusClaimed:
			stats.Claimed++
		case model.StatusRunning:
			stats.Running++
		case model.StatusCompleted:
			stats.Completed++
		case model.StatusFailed:
			stats.Failed++
		}

		// 更新最大分区ID
		if partition.PartitionID > stats.MaxPartitionID {
			stats.MaxPartitionID = partition.PartitionID
		}

		// 更新最大数据ID
		if partition.MaxID > stats.LastAllocatedID {
			stats.LastAllocatedID = partition.MaxID
		}
	}

	// 计算比率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	}

	return stats, nil
}

// Stop 停止分区策略（实现PartitionStrategy接口）
func (m *MockPartitionStrategy) Stop(ctx context.Context) error {
	// 如果设置了自定义的Stop行为，使用它
	if m.StopFunc != nil {
		return m.StopFunc(ctx)
	}

	// 默认行为：简单清空分区数据
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 可以在测试中通过设置特定的行为来模拟错误
	m.Partitions = make(map[int]*model.PartitionInfo)
	return nil
}

// SetPartitions 设置分区数据（用于测试设置）
func (m *MockPartitionStrategy) SetPartitions(partitions map[int]*model.PartitionInfo) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Partitions = make(map[int]*model.PartitionInfo)
	for k, v := range partitions {
		copy := *v
		m.Partitions[k] = &copy
	}
}

// UpdatePartitionStatusCall 记录UpdatePartitionStatus方法调用
type UpdatePartitionStatusCall struct {
	PartitionID int
	WorkerID    string
	Status      model.PartitionStatus
	Metadata    map[string]interface{}
}

// 为测试添加额外的字段和方法
type MockPartitionStrategyTestHelper struct {
	*MockPartitionStrategy
	updatePartitionStatusCalls  []UpdatePartitionStatusCall
	updatePartitionStatusErrors map[int]string // partitionID -> error message
	filteredPartitions          []*model.PartitionInfo
	testMutex                   sync.RWMutex
}

// NewMockPartitionStrategyWithTestHelper 创建带测试助手的MockPartitionStrategy
func NewMockPartitionStrategyWithTestHelper() *MockPartitionStrategyTestHelper {
	return &MockPartitionStrategyTestHelper{
		MockPartitionStrategy:       NewMockPartitionStrategy(),
		updatePartitionStatusCalls:  make([]UpdatePartitionStatusCall, 0),
		updatePartitionStatusErrors: make(map[int]string),
		filteredPartitions:          make([]*model.PartitionInfo, 0),
	}
}

// UpdatePartitionStatus 重写UpdatePartitionStatus以记录调用
func (m *MockPartitionStrategyTestHelper) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	m.testMutex.Lock()
	m.updatePartitionStatusCalls = append(m.updatePartitionStatusCalls, UpdatePartitionStatusCall{
		PartitionID: partitionID,
		WorkerID:    workerID,
		Status:      status,
		Metadata:    metadata,
	})
	m.testMutex.Unlock()

	// 检查是否设置了错误
	if errMsg, exists := m.updatePartitionStatusErrors[partitionID]; exists {
		return fmt.Errorf(errMsg)
	}

	// 调用原始方法
	return m.MockPartitionStrategy.UpdatePartitionStatus(ctx, partitionID, workerID, status, metadata)
}

// GetFilteredPartitions 重写GetFilteredPartitions以支持测试数据
func (m *MockPartitionStrategyTestHelper) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	m.testMutex.RLock()
	defer m.testMutex.RUnlock()

	if len(m.filteredPartitions) > 0 {
		// 返回测试设置的数据
		result := make([]*model.PartitionInfo, len(m.filteredPartitions))
		for i, p := range m.filteredPartitions {
			copy := *p
			result[i] = &copy
		}
		return result, nil
	}

	// 调用原始方法
	return m.MockPartitionStrategy.GetFilteredPartitions(ctx, filters)
}

// SetFilteredPartitions 设置GetFilteredPartitions返回的数据
func (m *MockPartitionStrategyTestHelper) SetFilteredPartitions(partitions []*model.PartitionInfo) {
	m.testMutex.Lock()
	defer m.testMutex.Unlock()

	m.filteredPartitions = make([]*model.PartitionInfo, len(partitions))
	for i, p := range partitions {
		copy := *p
		m.filteredPartitions[i] = &copy
	}
}

// SetUpdatePartitionStatusError 设置特定分区的UpdatePartitionStatus错误
func (m *MockPartitionStrategyTestHelper) SetUpdatePartitionStatusError(partitionID int, errorMessage string) {
	m.testMutex.Lock()
	defer m.testMutex.Unlock()
	m.updatePartitionStatusErrors[partitionID] = errorMessage
}

// GetUpdatePartitionStatusCalls 获取UpdatePartitionStatus的调用记录
func (m *MockPartitionStrategyTestHelper) GetUpdatePartitionStatusCalls() []UpdatePartitionStatusCall {
	m.testMutex.RLock()
	defer m.testMutex.RUnlock()

	result := make([]UpdatePartitionStatusCall, len(m.updatePartitionStatusCalls))
	copy(result, m.updatePartitionStatusCalls)
	return result
}

// ClearUpdatePartitionStatusCalls 清空UpdatePartitionStatus的调用记录
func (m *MockPartitionStrategyTestHelper) ClearUpdatePartitionStatusCalls() {
	m.testMutex.Lock()
	defer m.testMutex.Unlock()
	m.updatePartitionStatusCalls = make([]UpdatePartitionStatusCall, 0)
}
