package partition

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"encoding/json"
	"errors"
	"fmt"
	pkgerrors "github.com/pkg/errors"
	"sync"
	"time"
)

// SimpleStrategy 简单分区策略实现
// 专注于提供分区的基础封装操作，包括分布式锁、并发安全、数据持久化等
// 不包含具体的分配和获取业务逻辑，这些由leader和runner负责
type SimpleStrategy struct {
	// 基础配置
	namespace string
	dataStore interface {
		data.LockOperations
		data.SimplePartitionOperations
		data.KeyOperations
		data.HeartbeatOperations
	}
	logger utils.Logger

	// 锁和心跳相关配置
	partitionLockExpiry time.Duration

	// 同步控制
	mu sync.RWMutex
}

// SimpleStrategyConfig 简单策略配置
type SimpleStrategyConfig struct {
	Namespace string
	DataStore interface {
		data.LockOperations
		data.SimplePartitionOperations
		data.KeyOperations
		data.HeartbeatOperations
	}
	Logger              utils.Logger
	PartitionLockExpiry time.Duration
}

// NewSimpleStrategy 创建新的简单分区策略
func NewSimpleStrategy(config SimpleStrategyConfig) *SimpleStrategy {
	// 设置默认值
	if config.Logger == nil {
		config.Logger = utils.NewDefaultLogger()
	}
	if config.PartitionLockExpiry <= 0 {
		config.PartitionLockExpiry = 3 * time.Minute
	}

	return &SimpleStrategy{
		namespace:           config.Namespace,
		dataStore:           config.DataStore,
		logger:              config.Logger,
		partitionLockExpiry: config.PartitionLockExpiry,
	}
}

// StrategyType 返回策略类型
func (s *SimpleStrategy) StrategyType() string {
	return "simple"
}

// ==================== 基础CRUD操作 ====================

// GetPartition 获取单个分区
func (s *SimpleStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	if partition, exists := partitions[partitionID]; exists {
		return &partition, nil
	}

	return nil, fmt.Errorf("分区 %d 不存在", partitionID)
}

// GetAllPartitions 获取所有分区
func (s *SimpleStrategy) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*model.PartitionInfo, 0, len(partitions))
	for _, partition := range partitions {
		p := partition // 避免循环变量问题
		result = append(result, &p)
	}

	return result, nil
}

// DeletePartition 删除分区
func (s *SimpleStrategy) DeletePartition(ctx context.Context, partitionID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	if partitions == nil {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	if _, exists := partitions[partitionID]; !exists {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	delete(partitions, partitionID)
	return s.savePartitionsInternal(ctx, partitions)
}

// GetFilteredPartitions 根据过滤器获取分区
func (s *SimpleStrategy) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	var result []*model.PartitionInfo
	now := time.Now()

	for _, partition := range partitions {
		// 状态过滤
		if len(filters.TargetStatuses) > 0 {
			found := false
			for _, status := range filters.TargetStatuses {
				if partition.Status == status {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// 过时过滤
		if filters.StaleDuration != nil && *filters.StaleDuration > 0 {
			timeSinceUpdate := now.Sub(partition.UpdatedAt)
			// 只包含过时的分区
			if timeSinceUpdate <= *filters.StaleDuration {
				continue // 不过时，跳过
			}
			// 如果设置了排除工作节点，跳过该节点的分区
			if filters.ExcludeWorkerIDOnStale != "" && partition.WorkerID == filters.ExcludeWorkerIDOnStale {
				continue
			}
		}

		// 分区ID范围过滤
		if filters.MinID != nil && partition.PartitionID < *filters.MinID {
			continue
		}
		if filters.MaxID != nil && partition.PartitionID > *filters.MaxID {
			continue
		}

		// 排除特定分区ID
		excluded := false
		for _, excludeID := range filters.ExcludePartitionIDs {
			if partition.PartitionID == excludeID {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		p := partition // 避免循环变量问题
		result = append(result, &p)

		// 限制数量
		if filters.Limit > 0 && len(result) >= filters.Limit {
			break
		}
	}

	return result, nil
}

// ==================== 批量操作 ====================

// DeletePartitions 批量删除分区
func (s *SimpleStrategy) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	if partitions == nil {
		return nil // 没有分区需要删除
	}

	for _, partitionID := range partitionIDs {
		delete(partitions, partitionID)
	}

	return s.savePartitionsInternal(ctx, partitions)
}

// ==================== 并发安全操作 ====================

// UpdatePartition 安全更新分区信息
func (s *SimpleStrategy) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	if partitionInfo == nil {
		return nil, fmt.Errorf("分区信息不能为空")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	if partitions == nil {
		partitions = make(map[int]model.PartitionInfo)
	}

	// 检查是否存在
	existingPartition, exists := partitions[partitionInfo.PartitionID]

	// 更新分区信息
	updatedPartition := *partitionInfo
	// 只在 UpdatedAt 为零值时才自动更新为当前时间
	if updatedPartition.UpdatedAt.IsZero() {
		updatedPartition.UpdatedAt = time.Now()
	}

	if !exists {
		if updatedPartition.CreatedAt.IsZero() {
			updatedPartition.CreatedAt = time.Now()
		}
		updatedPartition.Version = 1
	} else {
		if updatedPartition.CreatedAt.IsZero() {
			updatedPartition.CreatedAt = existingPartition.CreatedAt
		}
		updatedPartition.Version = existingPartition.Version + 1
	}

	partitions[updatedPartition.PartitionID] = updatedPartition

	if err := s.savePartitionsInternal(ctx, partitions); err != nil {
		return nil, err
	}

	return &updatedPartition, nil
}

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
func (s *SimpleStrategy) CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingPartitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	if existingPartitions == nil {
		existingPartitions = make(map[int]model.PartitionInfo)
	}

	var result []*model.PartitionInfo
	now := time.Now()

	for _, req := range request.Partitions {
		// 检查是否已存在
		if existingPartition, exists := existingPartitions[req.PartitionID]; exists {
			result = append(result, &existingPartition)
			continue
		}

		// 创建新分区
		newPartition := model.PartitionInfo{
			PartitionID: req.PartitionID,
			MinID:       req.MinID,
			MaxID:       req.MaxID,
			WorkerID:    "",
			Status:      model.StatusPending,
			UpdatedAt:   now,
			CreatedAt:   now,
			Options:     req.Options,
			Version:     1,
		}

		if newPartition.Options == nil {
			newPartition.Options = make(map[string]interface{})
		}

		existingPartitions[req.PartitionID] = newPartition
		result = append(result, &newPartition)
	}

	if err := s.savePartitionsInternal(ctx, existingPartitions); err != nil {
		return nil, err
	}

	return result, nil
}

// ==================== 协调操作 ====================

// AcquirePartition 声明对指定分区的持有权
// 尝试获取指定分区的锁并声明持有权
// 这是一个针对特定分区的声明式操作
func (s *SimpleStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	// 调用内部实现
	partition, err := s.acquirePartitionInternal(ctx, partitionID, workerID, options)
	if err != nil {
		// 检查是否是正常的"无法获取"情况
		if errors.Is(err, ErrPartitionNotExists) ||
			errors.Is(err, ErrPartitionAlreadyHeld) ||
			errors.Is(err, ErrPartitionLockFailed) {
			// 这些是正常的"无法获取"情况，不返回错误
			return nil, false, nil
		}
		// 系统错误
		return nil, false, err
	}
	return partition, true, nil
}

// acquirePartitionInternal 内部分区获取方法，支持抢占功能
func (s *SimpleStrategy) acquirePartitionInternal(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, error) {
	if workerID == "" {
		return nil, fmt.Errorf("工作节点ID不能为空")
	}

	// 设置默认选项
	if options == nil {
		options = &model.AcquirePartitionOptions{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取指定分区
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return nil, ErrPartitionNotExists
	}

	// 检查分区是否可以被声明或抢占
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		if !options.AllowPreemption {
			return nil, ErrPartitionAlreadyHeld
		}
		// 如果允许抢占，继续处理
	}

	// 如果是同一个工作节点，检查是否已经持有锁
	lockKey := s.getPartitionLockKey(partitionID)
	if partition.WorkerID == workerID {
		// 同一个工作节点重复声明，检查锁是否仍然有效
		owned, err := s.dataStore.CheckLock(ctx, lockKey, workerID)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "检查分区锁失败")
		}
		if owned {
			// 锁仍然有效，直接返回当前分区信息
			s.logger.Infof("工作节点 %s 重复声明对分区 %d 的持有权（锁仍然有效）", workerID, partitionID)
			return &partition, nil
		}
		// 锁已失效，需要重新获取
	}

	// 尝试获取分布式锁
	acquired, err := s.dataStore.AcquireLock(ctx, lockKey, workerID, s.partitionLockExpiry)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "获取分区锁失败")
	}

	if !acquired {
		return nil, ErrPartitionLockFailed
	}

	// 声明对分区的持有权
	if options.AllowPreemption && partition.WorkerID != "" && partition.WorkerID != workerID {
		s.logger.Infof("工作节点 %s 抢占分区 %d（原持有者: %s）", workerID, partitionID, partition.WorkerID)
	} else {
		s.logger.Infof("工作节点 %s 声明对分区 %d 的持有权", workerID, partitionID)
	}

	partition.WorkerID = workerID
	partition.Status = model.StatusClaimed
	partition.UpdatedAt = time.Now()
	partition.Version++

	// 保存更新后的分区信息
	partitions[partitionID] = partition
	if err := s.savePartitionsInternal(ctx, partitions); err != nil {
		// 如果保存失败，释放锁
		s.dataStore.ReleaseLock(ctx, lockKey, workerID)
		return nil, err
	}

	return &partition, nil
}

// UpdatePartitionStatus 更新分区状态
func (s *SimpleStrategy) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	// 验证工作节点权限
	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 无权更新分区 %d，当前所有者：%s", workerID, partitionID, partition.WorkerID)
	}

	// 更新状态
	partition.Status = status
	partition.UpdatedAt = time.Now()
	partition.Version++

	// 更新元数据
	if metadata != nil {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for key, value := range metadata {
			partition.Options[key] = value
		}
	}

	partitions[partitionID] = partition
	return s.savePartitionsInternal(ctx, partitions)
}

// ReleasePartition 释放分区
func (s *SimpleStrategy) ReleasePartition(ctx context.Context, partitionID int, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return err
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	// 验证工作节点权限
	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 无权释放分区 %d，当前所有者：%s", workerID, partitionID, partition.WorkerID)
	}

	// 释放分布式锁
	lockKey := s.getPartitionLockKey(partitionID)
	if err := s.dataStore.ReleaseLock(ctx, lockKey, workerID); err != nil {
		s.logger.Warnf("释放分区锁失败", "partitionID", partitionID, "workerID", workerID, "error", err)
	}

	// 重置分区状态
	partition.WorkerID = ""
	partition.Status = model.StatusPending
	partition.UpdatedAt = time.Now()
	partition.Version++

	partitions[partitionID] = partition
	return s.savePartitionsInternal(ctx, partitions)
}

// MaintainPartitionHold 维护对分区的持有权（续期分布式锁）
func (s *SimpleStrategy) MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("工作节点ID不能为空")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查分区是否存在且被该工作节点持有
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %w", err)
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return fmt.Errorf("分区 %d 不存在", partitionID)
	}

	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 没有持有分区 %d（当前持有者: %s）", workerID, partitionID, partition.WorkerID)
	}

	// 续期分布式锁
	lockKey := s.getPartitionLockKey(partitionID)
	renewed, err := s.dataStore.RenewLock(ctx, lockKey, workerID, s.partitionLockExpiry)
	if err != nil {
		s.logger.Errorf("续期分区 %d 锁失败: %v", partitionID, err)
		return fmt.Errorf("续期分区锁失败: %w", err)
	}

	if !renewed {
		s.logger.Warnf("工作节点 %s 无法续期分区 %d 的锁（锁可能已过期或被其他节点持有）", workerID, partitionID)
		return fmt.Errorf("无法续期分区 %d 的锁", partitionID)
	}

	s.logger.Debugf("工作节点 %s 成功续期分区 %d 的锁", workerID, partitionID)
	return nil
}

// GetPartitionStats 获取分区统计信息
func (s *SimpleStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	stats := &model.PartitionStats{
		Total:     0,
		Pending:   0,
		Claimed:   0,
		Running:   0,
		Failed:    0,
		Completed: 0,
	}

	for _, partition := range partitions {
		stats.Total++
		switch partition.Status {
		case model.StatusPending:
			stats.Pending++
		case model.StatusClaimed:
			stats.Claimed++
		case model.StatusRunning:
			stats.Running++
		case model.StatusFailed:
			stats.Failed++
		case model.StatusCompleted:
			stats.Completed++
		}
	}

	// 计算比率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	}

	return stats, nil
}

// ==================== 内部辅助方法 ====================

// getAllPartitionsInternal 内部方法：获取所有分区
func (s *SimpleStrategy) getAllPartitionsInternal(ctx context.Context) (map[int]model.PartitionInfo, error) {
	key := s.getPartitionsKey()

	partitionData, err := s.dataStore.GetPartitions(ctx, key)
	if err != nil {
		// 检查是否是键不存在的错误
		if errors.Is(err, data.ErrNotFound) {
			// 键不存在，返回空的分区映射
			return make(map[int]model.PartitionInfo), nil
		}
		return nil, pkgerrors.Wrap(err, "获取分区数据失败")
	}

	var partitions map[int]model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionData), &partitions); err != nil {
		return nil, pkgerrors.Wrap(err, "解析分区数据失败")
	}

	if partitions == nil {
		partitions = make(map[int]model.PartitionInfo)
	}

	return partitions, nil
}

// savePartitionsInternal 内部方法：保存所有分区
func (s *SimpleStrategy) savePartitionsInternal(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	key := s.getPartitionsKey()

	partitionData, err := json.Marshal(partitions)
	if err != nil {
		return pkgerrors.Wrap(err, "序列化分区数据失败")
	}

	if err := s.dataStore.SetPartitions(ctx, key, string(partitionData)); err != nil {
		return pkgerrors.Wrap(err, "保存分区数据失败")
	}

	return nil
}

// getFilteredPartitionsInternal 内部方法：获取过滤后的分区列表
func (s *SimpleStrategy) getFilteredPartitionsInternal(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	var result []*model.PartitionInfo
	now := time.Now()

	for _, partition := range partitions {
		// 状态过滤
		if len(filters.TargetStatuses) > 0 {
			found := false
			for _, status := range filters.TargetStatuses {
				if partition.Status == status {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// 过时过滤
		if filters.StaleDuration != nil && *filters.StaleDuration > 0 {
			timeSinceUpdate := now.Sub(partition.UpdatedAt)
			// 只包含过时的分区
			if timeSinceUpdate <= *filters.StaleDuration {
				continue // 不过时，跳过
			}
			// 如果设置了排除工作节点，跳过该节点的分区
			if filters.ExcludeWorkerIDOnStale != "" && partition.WorkerID == filters.ExcludeWorkerIDOnStale {
				continue
			}
		}

		// 分区ID范围过滤
		if filters.MinID != nil && partition.PartitionID < *filters.MinID {
			continue
		}
		if filters.MaxID != nil && partition.PartitionID > *filters.MaxID {
			continue
		}

		// 排除特定分区ID
		excluded := false
		for _, excludeID := range filters.ExcludePartitionIDs {
			if partition.PartitionID == excludeID {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		p := partition // 避免循环变量问题
		result = append(result, &p)

		// 限制数量
		if filters.Limit > 0 && len(result) >= filters.Limit {
			break
		}
	}

	return result, nil
}

// getPartitionsKey 获取分区存储键
func (s *SimpleStrategy) getPartitionsKey() string {
	return fmt.Sprintf("%s:partitions", s.namespace)
}

// getPartitionLockKey 获取分区锁键
func (s *SimpleStrategy) getPartitionLockKey(partitionID int) string {
	return fmt.Sprintf("%s:partition_lock:%d", s.namespace, partitionID)
}
