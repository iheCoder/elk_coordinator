package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 版本控制相关操作 ====================

// updatePartitionWithVersionControl 内部方法，处理带版本控制的分区更新
//
// 使用乐观锁机制进行分区更新：
// 1. 使用传入分区信息中的版本号作为期望版本
// 2. 只有当存储中的分区版本等于期望版本时才执行更新
// 3. 更新成功后版本号自动递增
// 4. 如果版本不匹配，返回 ErrOptimisticLockFailed 错误
//
// 这种机制避免了并发修改导致的数据不一致问题。
//
// 参数:
//   - ctx: 上下文
//   - partitionInfo: 要更新的分区信息，必须包含当前版本号
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息（版本号已递增）
//   - error: 错误信息，如果乐观锁失败返回 data.ErrOptimisticLockFailed
//
// 注意: 调用方应该已经获取了分区的当前版本，避免重新获取造成竞争条件
func (s *HashPartitionStrategy) updatePartitionWithVersionControl(ctx context.Context, partitionInfo *model.PartitionInfo) (*model.PartitionInfo, error) {
	if partitionInfo == nil {
		return nil, errors.New("partitionInfo cannot be nil")
	}

	// 使用传入分区信息中的版本，避免重新获取造成竞争条件
	// 调用方已经获取了分区信息，应该使用调用方读取到的版本
	expectedVersion := partitionInfo.Version
	newVersion := expectedVersion + 1

	// 创建更新后的分区副本
	updatedPartition := *partitionInfo
	updatedPartition.Version = newVersion
	updatedPartition.UpdatedAt = time.Now()

	// 如果是声明操作，LastHeartbeat 也应该更新
	if partitionInfo.Status == model.StatusClaimed || partitionInfo.Status == model.StatusRunning {
		updatedPartition.LastHeartbeat = updatedPartition.UpdatedAt
	}

	partitionJson, err := json.Marshal(updatedPartition)
	if err != nil {
		s.logger.Errorf("为分区 %d 序列化更新后的 PartitionInfo 失败: %v", updatedPartition.PartitionID, err)
		return nil, fmt.Errorf("序列化分区 %d 以进行更新失败: %w", updatedPartition.PartitionID, err)
	}

	// 使用乐观锁进行更新：只有当存储中的分区版本等于expectedVersion时才执行更新
	// 这是一个原子操作，防止并发修改导致的数据不一致
	success, err := s.store.HUpdatePartitionWithVersion(ctx, partitionHashKey, strconv.Itoa(updatedPartition.PartitionID), string(partitionJson), expectedVersion)
	if err != nil {
		if errors.Is(err, data.ErrOptimisticLockFailed) {
			// 乐观锁失败：存储中的分区版本已经不是expectedVersion了
			// 这通常意味着另一个并发操作已经修改了这个分区
			s.logger.Warnf("分区 %d 的乐观锁失败：期望版本 %d，但存储中的分区版本已被其他操作修改", updatedPartition.PartitionID, expectedVersion)
			return nil, data.ErrOptimisticLockFailed // 返回特定的错误
		}
		s.logger.Errorf("为分区 %d 执行 HUpdatePartitionWithVersion 失败: %v", updatedPartition.PartitionID, err)
		return nil, fmt.Errorf("乐观更新分区 %d 失败: %w", updatedPartition.PartitionID, err)
	}
	if !success {
		// success为false表示条件更新失败：存储中的分区版本与expectedVersion不匹配
		// 这是另一种形式的乐观锁失败，通常发生在并发环境中
		s.logger.Warnf("分区 %d 的乐观锁失败：期望版本 %d，但存储中的分区版本不匹配（可能被其他worker修改）", updatedPartition.PartitionID, expectedVersion)
		return nil, data.ErrOptimisticLockFailed
	}

	s.logger.Infof("成功将分区 %d 更新到版本 %d (状态: %s, WorkerID: %s)", updatedPartition.PartitionID, newVersion, updatedPartition.Status, updatedPartition.WorkerID)
	return &updatedPartition, nil
}

// UpdatePartitionOptimistically 尝试在分区版本与 expectedVersion 匹配时更新分区。
// 成功更新后，版本号会递增。
//
// 已弃用：请使用 UpdatePartition 方法，它会自动处理版本控制
//
// 参数:
//   - ctx: 上下文
//   - partitionInfo: 要更新的分区信息
//   - expectedVersion: 期望的当前版本号
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - error: 错误信息，可能是 data.ErrOptimisticLockFailed
func (s *HashPartitionStrategy) UpdatePartitionOptimistically(ctx context.Context, partitionInfo *model.PartitionInfo, expectedVersion int64) (*model.PartitionInfo, error) {
	if partitionInfo == nil {
		return nil, errors.New("partitionInfo cannot be nil") // partitionInfo 不能为空
	}

	newVersion := expectedVersion + 1
	updatedPartition := *partitionInfo // 创建一个副本进行修改
	updatedPartition.Version = newVersion
	updatedPartition.UpdatedAt = time.Now()
	// 如果是声明操作，LastHeartbeat 也应该更新
	if partitionInfo.Status == model.StatusClaimed || partitionInfo.Status == model.StatusRunning {
		updatedPartition.LastHeartbeat = updatedPartition.UpdatedAt
	}

	partitionJson, err := json.Marshal(updatedPartition)
	if err != nil {
		s.logger.Errorf("为分区 %d 序列化更新后的 PartitionInfo 失败: %v", updatedPartition.PartitionID, err)
		return nil, fmt.Errorf("序列化分区 %d 以进行更新失败: %w", updatedPartition.PartitionID, err)
	}

	// 将序列化后的 JSON 字符串和 *现有* 记录的 expectedVersion 传递给数据存储层。
	// 数据存储层中的 HUpdatePartitionWithVersion 方法应处理乐观锁逻辑。
	success, err := s.store.HUpdatePartitionWithVersion(ctx, partitionHashKey, strconv.Itoa(updatedPartition.PartitionID), string(partitionJson), expectedVersion)
	if err != nil {
		// 像 ErrOptimisticLockFailed 这样的特定错误应该由存储的 HUpdatePartitionWithVersion 实现返回。
		s.logger.Errorf("为分区 %d 执行 HUpdatePartitionWithVersion 失败: %v", updatedPartition.PartitionID, err)
		return nil, fmt.Errorf("乐观更新分区 %d 失败: %w", updatedPartition.PartitionID, err)
	}
	if !success {
		s.logger.Warnf("分区 %d 的乐观锁失败：期望版本 %d，但存储中的条件未满足", updatedPartition.PartitionID, expectedVersion)
		return nil, data.ErrOptimisticLockFailed // 返回特定的错误
	}

	s.logger.Infof("成功将分区 %d 更新到版本 %d (状态: %s, WorkerID: %s)", updatedPartition.PartitionID, newVersion, updatedPartition.Status, updatedPartition.WorkerID)
	return &updatedPartition, nil
}

// CreatePartitionAtomically 尝试在存储中创建一个新的分区定义，
// 仅当它尚不存在时（即 expectedVersion 为 0）。它将初始版本设置为 1。
//
// 参数:
//   - ctx: 上下文
//   - partitionID: 分区ID
//   - minID: 分区最小ID范围
//   - maxID: 分区最大ID范围
//   - options: 分区配置选项
//
// 返回:
//   - *model.PartitionInfo: 创建的分区信息
//   - error: 错误信息，可能是 data.ErrPartitionAlreadyExists
func (s *HashPartitionStrategy) CreatePartitionAtomically(ctx context.Context, partitionID int, minID, maxID int64, options map[string]interface{}) (*model.PartitionInfo, error) {
	now := time.Now()
	newPartition := &model.PartitionInfo{
		PartitionID:   partitionID,
		MinID:         minID,
		MaxID:         maxID,
		Status:        model.StatusPending,
		WorkerID:      "", // 初始没有工作节点
		LastHeartbeat: now,
		UpdatedAt:     now,
		CreatedAt:     now,
		Version:       1, // 新记录的初始版本
		Options:       options,
	}

	partitionJson, err := json.Marshal(newPartition)
	if err != nil {
		s.logger.Errorf("序列化新分区 %d 失败: %v", partitionID, err)
		return nil, fmt.Errorf("序列化分区 %d 失败: %w", partitionID, err)
	}

	// 创建时期望版本为 0（即字段不应存在，或其版本实际上为 0）。
	// 数据存储层中的 HUpdatePartitionWithVersion 方法应处理此逻辑。
	success, err := s.store.HUpdatePartitionWithVersion(ctx, partitionHashKey, strconv.Itoa(partitionID), string(partitionJson), 0)
	if err != nil {
		// 将数据层错误转换为分区层错误
		if errors.Is(err, data.ErrPartitionAlreadyExists) {
			s.logger.Warnf("分区 %d 已存在，无法创建", partitionID)
			return nil, ErrPartitionAlreadyExists
		}
		s.logger.Errorf("创建分区 %d 失败: %v", partitionID, err)
		return nil, fmt.Errorf("创建分区 %d 失败: %w", partitionID, err)
	}

	if !success {
		s.logger.Warnf("分区 %d 创建失败：分区可能已存在", partitionID)
		return nil, ErrPartitionAlreadyExists
	}

	s.logger.Infof("成功创建分区 %d (范围: %d-%d)", partitionID, minID, maxID)
	return newPartition, nil
}

// UpdatePartition 安全更新分区信息，自动处理版本控制
//
// 这是推荐的分区更新方法，支持多种更新模式：
// - 自动版本控制：使用分区中的版本号进行乐观锁更新
// - 强制更新：绕过版本控制直接更新（不推荐在并发环境中使用）
// - 期望版本：使用指定的期望版本进行更新
// - Upsert：如果分区不存在则创建，存在则更新
//
// 参数:
//   - ctx: 上下文
//   - partitionInfo: 要更新的分区信息
//   - options: 更新选项
//
// 返回:
//   - *model.PartitionInfo: 更新后的分区信息
//   - error: 错误信息
func (s *HashPartitionStrategy) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	if partitionInfo == nil {
		return nil, errors.New("partitionInfo cannot be nil")
	}

	// 设置默认选项
	if options == nil {
		options = &model.UpdateOptions{}
	}

	// 如果指定了强制更新选项，使用直接保存
	if options.Force {
		s.logger.Warnf("强制更新分区 %d，绕过版本控制", partitionInfo.PartitionID)
		err := s.SavePartition(ctx, partitionInfo)
		if err != nil {
			return nil, err
		}
		return partitionInfo, nil
	}

	// 如果指定了期望版本，使用传统的乐观锁方法
	if options.ExpectedVersion != nil {
		return s.UpdatePartitionOptimistically(ctx, partitionInfo, *options.ExpectedVersion)
	}

	// 如果指定了 Upsert 选项，先尝试获取，如果不存在则创建
	if options.Upsert {
		currentPartition, err := s.GetPartition(ctx, partitionInfo.PartitionID)
		if err != nil {
			if errors.Is(err, ErrPartitionNotFound) {
				// 分区不存在，创建新分区
				return s.CreatePartitionAtomically(ctx, partitionInfo.PartitionID,
					partitionInfo.MinID, partitionInfo.MaxID, partitionInfo.Options)
			}
			return nil, err
		}
		// 分区存在，使用其版本进行更新
		partitionInfo.Version = currentPartition.Version
	}

	// 默认使用自动版本控制更新
	return s.updatePartitionWithVersionControl(ctx, partitionInfo)
}
