package partition

import (
	"context"
	"elk_coordinator/utils"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9" // 确保导入了 redis
	"sort"
	"strconv"
	"time"

	"elk_coordinator/data"
	"elk_coordinator/model"
)

const (
	partitionHashKey = "elk_partitions" // 分区数据在 Redis Hash 中的键名
)

// HashPartitionStrategy 处理分区数据的访问和生命周期管理。
// 实现 PartitionStrategy 接口，提供基于 Redis Hash 的分区存储策略
type HashPartitionStrategy struct {
	store          data.HashPartitionOperations // 分区存储接口，使用最小接口而不是完整DataStore
	logger         utils.Logger                 // 日志记录器
	staleThreshold time.Duration                // 分区被认为过时的心跳阈值，用于抢占判断
}

// NewHashPartitionStrategy 创建一个新的 Hash 分区策略实例。
func NewHashPartitionStrategy(store data.HashPartitionOperations, logger utils.Logger) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	return &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: 5 * time.Minute, // 默认5分钟心跳超时阈值
	}
}

// NewHashPartitionStrategyWithConfig 创建带配置的 Hash 分区策略实例
func NewHashPartitionStrategyWithConfig(store data.HashPartitionOperations, logger utils.Logger, staleThreshold time.Duration) *HashPartitionStrategy {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	if staleThreshold <= 0 {
		staleThreshold = 5 * time.Minute // 默认值
	}
	return &HashPartitionStrategy{
		store:          store,
		logger:         logger,
		staleThreshold: staleThreshold,
	}
}

// updatePartitionWithVersionControl 内部方法，处理带版本控制的分区更新
// 使用传入分区信息中的版本进行乐观锁更新，避免竞争条件
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

	// 使用乐观锁进行更新
	success, err := s.store.HUpdatePartitionWithVersion(ctx, partitionHashKey, strconv.Itoa(updatedPartition.PartitionID), string(partitionJson), expectedVersion)
	if err != nil {
		if errors.Is(err, data.ErrOptimisticLockFailed) {
			s.logger.Warnf("分区 %d 的乐观锁失败：期望版本 %d，但存储中的条件未满足", updatedPartition.PartitionID, expectedVersion)
			return nil, data.ErrOptimisticLockFailed // 返回特定的错误
		}
		s.logger.Errorf("为分区 %d 执行 HUpdatePartitionWithVersion 失败: %v", updatedPartition.PartitionID, err)
		return nil, fmt.Errorf("乐观更新分区 %d 失败: %w", updatedPartition.PartitionID, err)
	}
	if !success {
		s.logger.Warnf("分区 %d 的乐观锁失败：期望版本 %d，但存储中的条件未满足", updatedPartition.PartitionID, expectedVersion)
		return nil, ErrOptimisticLockFailed
	}

	s.logger.Infof("成功将分区 %d 更新到版本 %d (状态: %s, WorkerID: %s)", updatedPartition.PartitionID, newVersion, updatedPartition.Status, updatedPartition.WorkerID)
	return &updatedPartition, nil
}

// UpdatePartitionOptimistically 尝试在分区版本与 expectedVersion 匹配时更新分区。
// 成功更新后，版本号会递增。
// 已弃用：请使用 UpdatePartition 方法，它会自动处理版本控制
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
		return nil, ErrOptimisticLockFailed // 返回特定的错误
	}

	s.logger.Infof("成功将分区 %d 更新到版本 %d (状态: %s, WorkerID: %s)", updatedPartition.PartitionID, newVersion, updatedPartition.Status, updatedPartition.WorkerID)
	return &updatedPartition, nil
}

// CreatePartitionAtomically 尝试在存储中创建一个新的分区定义，
// 仅当它尚不存在时（即 expectedVersion 为 0）。它将初始版本设置为 1。
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
		s.logger.Errorf("为分区 %d 序列化 newPartition 失败: %v", partitionID, err)
		return nil, fmt.Errorf("序列化分区 %d 以进行创建失败: %w", partitionID, err)
	}

	// 创建时期望版本为 0（即字段不应存在，或其版本实际上为 0）。
	// 数据存储层中的 HUpdatePartitionWithVersion 方法应处理此逻辑。
	success, err := s.store.HUpdatePartitionWithVersion(ctx, partitionHashKey, strconv.Itoa(partitionID), string(partitionJson), 0)
	if err != nil {
		// 像 ErrPartitionAlreadyExists 这样的特定错误应该由存储的 HUpdatePartitionWithVersion 返回。
		s.logger.Errorf("通过 HUpdatePartitionWithVersion 原子创建分区 %d 失败: %v", partitionID, err)
		return nil, fmt.Errorf("创建分区 %d 失败: %w", partitionID, err)
	}
	if !success {
		// 这意味着创建失败，可能是因为它已存在或其他冲突。
		// 如果是特定原因，存储的 HUpdatePartitionWithVersion 理想情况下应返回 model.ErrPartitionAlreadyExists。
		// 目前，我们假设一般性失败意味着它可能已存在或创建时发生其他乐观锁类型的问题。
		s.logger.Warnf("原子创建分区 %d 失败，HUpdatePartitionWithVersion 返回 false (期望版本 0)。它可能已存在。", partitionID)
		// 如果底层的存储方法过于通用，可以尝试获取分区以确认其是否存在，从而返回更具体的错误。
		// 但目前，我们依赖 HUpdatePartitionWithVersion 的错误契约。
		// 如果 HUpdatePartitionWithVersion 行为良好，它应该在 `err` 中返回 ErrPartitionAlreadyExists。
		// 如果 `err` 为 nil 但 `success` 为 false，则表示存在冲突。
		return nil, ErrPartitionAlreadyExists // 或更通用的创建冲突错误
	}

	s.logger.Infof("成功创建分区 %d，版本为 1", partitionID)
	return newPartition, nil
}

// GetFilteredPartitions 根据提供的过滤器从存储中检索分区数据。
// 返回的分区将按 PartitionID 升序排序。
func (s *HashPartitionStrategy) GetFilteredPartitions(ctx context.Context, filters model.GetPartitionsFilters) ([]*model.PartitionInfo, error) {
	allPartitions, err := s.GetAllPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetFilteredPartitions: 获取所有分区失败: %w", err)
	}

	var resultPartitions []*model.PartitionInfo
	now := time.Now() // 在循环外获取当前时间，以保持一致性

	// 构建状态查找映射以提高效率
	statusFilterActive := len(filters.TargetStatuses) > 0
	statusMap := make(map[model.PartitionStatus]bool)
	if statusFilterActive {
		for _, status := range filters.TargetStatuses {
			statusMap[status] = true
		}
	}

	staleFilterActive := filters.StaleDuration != nil && *filters.StaleDuration > 0
	staleFilterSet := filters.StaleDuration != nil

	for _, p := range allPartitions {
		passStatusFilter := !statusFilterActive // 如果状态过滤器未激活，则默认通过
		if statusFilterActive {
			if _, ok := statusMap[p.Status]; ok {
				passStatusFilter = true
			}
		}

		if !passStatusFilter {
			continue // 未通过状态过滤
		}

		passStaleFilter := !staleFilterSet // 如果过时过滤器未设置，则默认通过
		if staleFilterSet {
			if !staleFilterActive {
				// 设置了过时过滤器但时长无效（<=0），返回空结果
				passStaleFilter = false
			} else {
				// 过时过滤器激活且有效
				isPotentiallyStaleByTime := now.Sub(p.UpdatedAt) > *filters.StaleDuration
				isExcludedWorker := filters.ExcludeWorkerIDOnStale != "" && p.WorkerID == filters.ExcludeWorkerIDOnStale

				if isPotentiallyStaleByTime && !isExcludedWorker {
					passStaleFilter = true
				} else {
					passStaleFilter = false
				}
			}
		}

		if !passStaleFilter {
			continue // 未通过过时过滤
		}

		// 如果所有激活的过滤器都通过了，则添加分区
		resultPartitions = append(resultPartitions, p)
	}

	// 按 PartitionID 排序以确保一致的顺序
	sort.Slice(resultPartitions, func(i, j int) bool {
		return resultPartitions[i].PartitionID < resultPartitions[j].PartitionID
	})

	s.logger.Debugf("GetFilteredPartitions: 使用过滤器 %+v 找到 %d 个分区", filters, len(resultPartitions))
	return resultPartitions, nil
}

// DeletePartition 从存储中删除一个分区。
func (s *HashPartitionStrategy) DeletePartition(ctx context.Context, partitionID int) error {
	err := s.store.HDeletePartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		s.logger.Errorf("删除分区 %d 失败: %v", partitionID, err)
		return fmt.Errorf("删除分区 %d 失败: %w", partitionID, err)
	}
	s.logger.Infof("成功删除分区 %d", partitionID)
	return nil
}

// GetAllPartitions 从存储中检索所有分区数据。
func (s *HashPartitionStrategy) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	dataMap, err := s.store.HGetAllPartitions(ctx, partitionHashKey)
	if err != nil {
		// HGetAllPartitions 在键不存在时通常返回空 map 和 nil 错误。
		// 如果它确实返回一个错误（例如 data.ErrNotFound，尽管对于 HGetAll 来说不典型，
		// 或者其他如连接错误），我们在这里处理它。
		if errors.Is(err, data.ErrNotFound) { // 理论上 HGetAll 不会返回这个，但为了保险
			s.logger.Warnf("HGetAllPartitions 对于键 %s 返回 data.ErrNotFound，返回空列表", partitionHashKey)
			return []*model.PartitionInfo{}, nil
		}
		s.logger.Errorf("从存储中获取所有分区失败: %v", err)
		return nil, fmt.Errorf("从存储中获取所有分区失败: %w", err)
	}

	// 如果 dataMap 为空（键不存在或键存在但没有字段），这将正确地返回一个空切片。
	partitions := make([]*model.PartitionInfo, 0, len(dataMap))
	for field, jsonStr := range dataMap {
		var p model.PartitionInfo
		if errUnmarshal := json.Unmarshal([]byte(jsonStr), &p); errUnmarshal != nil {
			s.logger.Warnf("反序列化字段 %s 的分区数据失败: %v。数据: %s", field, errUnmarshal, jsonStr)
			continue // 跳过损坏的数据
		}
		partitions = append(partitions, &p)
	}
	return partitions, nil
}

// GetPartition 通过其 ID 检索特定的分区。
func (s *HashPartitionStrategy) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	jsonStr, err := s.store.HGetPartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		if errors.Is(err, data.ErrNotFound) { // 检查 data.ErrNotFound
			s.logger.Infof("分区 %d 未在存储中找到 (data.ErrNotFound)", partitionID)
			return nil, ErrPartitionNotFound // 使用 partition.ErrPartitionNotFound
		}
		// 对于其他类型的错误，例如 redis.Nil 直接从 HGetPartition 泄漏（不应该发生如果 HGetPartition 正确包装）
		// 或者其他连接/IO错误
		if errors.Is(err, redis.Nil) { // 以防万一 HGetPartition 没有包装 redis.Nil
			s.logger.Infof("分区 %d 未在存储中找到 (redis.Nil)", partitionID)
			return nil, ErrPartitionNotFound // 使用 partition.ErrPartitionNotFound
		}
		s.logger.Errorf("从存储中获取分区 %d 失败: %v", partitionID, err)
		return nil, fmt.Errorf("从存储中获取分区 %d 失败: %w", partitionID, err)
	}

	var p model.PartitionInfo
	if errUnmarshal := json.Unmarshal([]byte(jsonStr), &p); errUnmarshal != nil {
		s.logger.Errorf("反序列化分区 ID %d 的数据失败: %v。数据: %s", partitionID, errUnmarshal, jsonStr)
		// 这里返回一个更具体的反序列化错误，而不是 ErrPartitionNotFound
		return nil, fmt.Errorf("反序列化分区 ID %d 的数据失败: %w。数据: %s", partitionID, errUnmarshal, jsonStr)
	}
	return &p, nil
}

// SavePartition 保存（创建或更新）一个分区。这是一个直接保存，没有乐观锁。
// 对于并发更新，请优先使用 UpdatePartitionOptimistically 或 CreatePartitionAtomically。
func (s *HashPartitionStrategy) SavePartition(ctx context.Context, partitionInfo *model.PartitionInfo) error {
	if partitionInfo == nil {
		return errors.New("SavePartition 的 partitionInfo 不能为空")
	}
	if partitionInfo.UpdatedAt.IsZero() {
		partitionInfo.UpdatedAt = time.Now()
	}

	if partitionInfo.CreatedAt.IsZero() {
		partitionInfo.CreatedAt = partitionInfo.UpdatedAt
	}

	// 对于直接保存，如果版本为 0，如果通常期望乐观锁，则可能存在问题。
	// 然而，此方法明确指出它没有乐观锁。
	// 如果此方法被滥用，在此处设置默认版本可能会隐藏问题。
	// 目前，保留逻辑但强调其直接性。
	if partitionInfo.Version == 0 {
		partitionInfo.Version = 1 // 如果版本为0，默认为1
		s.logger.Warnf("分区 %d 被直接保存且版本为 0，已默认设置为 1。这将绕过乐观锁。", partitionInfo.PartitionID)
	}

	jsonData, err := json.Marshal(partitionInfo)
	if err != nil {
		s.logger.Errorf("序列化用于保存的 partition info 失败: %v", err)
		return fmt.Errorf("序列化用于保存的 partition info 失败: %w", err)
	}

	err = s.store.HSetPartition(ctx, partitionHashKey, strconv.Itoa(partitionInfo.PartitionID), string(jsonData))
	if err != nil {
		s.logger.Errorf("直接保存分区 %d 失败: %v", partitionInfo.PartitionID, err)
		return fmt.Errorf("直接保存分区 %d 失败: %w", partitionInfo.PartitionID, err)
	}
	s.logger.Infof("成功直接保存分区 %d (版本 %d)", partitionInfo.PartitionID, partitionInfo.Version)
	return nil
}

// ==================== PartitionStrategy 接口实现 ====================

// UpdatePartition 安全更新分区信息，自动处理版本控制
func (s *HashPartitionStrategy) UpdatePartition(ctx context.Context, partitionInfo *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	if partitionInfo == nil {
		return nil, errors.New("partitionInfo cannot be nil")
	}

	// 如果指定了强制更新选项，使用直接保存
	if options != nil && options.Force {
		err := s.SavePartition(ctx, partitionInfo)
		if err != nil {
			return nil, err
		}
		return partitionInfo, nil
	}

	// 如果指定了期望版本，使用传统的乐观锁方法
	if options != nil && options.ExpectedVersion != nil {
		return s.UpdatePartitionOptimistically(ctx, partitionInfo, *options.ExpectedVersion)
	}

	// 如果指定了 Upsert 选项，先尝试获取，如果不存在则创建
	if options != nil && options.Upsert {
		_, err := s.GetPartition(ctx, partitionInfo.PartitionID)
		if err != nil {
			if errors.Is(err, ErrPartitionNotFound) {
				// 分区不存在，创建新分区
				return s.CreatePartitionAtomically(ctx, partitionInfo.PartitionID, partitionInfo.MinID, partitionInfo.MaxID, partitionInfo.Options)
			}
			return nil, err
		}
		// 分区存在，使用版本控制更新
		return s.updatePartitionWithVersionControl(ctx, partitionInfo)
	}

	// 默认使用自动版本控制更新
	return s.updatePartitionWithVersionControl(ctx, partitionInfo)
}

// CreatePartitionsIfNotExist 批量创建分区（如果不存在）
func (s *HashPartitionStrategy) CreatePartitionsIfNotExist(ctx context.Context, request model.CreatePartitionsRequest) ([]*model.PartitionInfo, error) {
	var result []*model.PartitionInfo
	var errs []error

	for _, partition := range request.Partitions {
		// 检查分区是否已存在
		existing, err := s.GetPartition(ctx, partition.PartitionID)
		if err != nil && !errors.Is(err, ErrPartitionNotFound) {
			errs = append(errs, fmt.Errorf("检查分区 %d 存在性失败: %w", partition.PartitionID, err))
			continue
		}

		if existing != nil {
			// 分区已存在，添加到结果中
			result = append(result, existing)
			s.logger.Infof("分区 %d 已存在，跳过创建", partition.PartitionID)
			continue
		}

		// 分区不存在，创建新分区
		newPartition, err := s.CreatePartitionAtomically(ctx, partition.PartitionID, partition.MinID, partition.MaxID, partition.Options)
		if err != nil {
			if errors.Is(err, ErrPartitionAlreadyExists) {
				// 并发创建导致的冲突，重新获取分区
				existing, getErr := s.GetPartition(ctx, partition.PartitionID)
				if getErr != nil {
					errs = append(errs, fmt.Errorf("创建分区 %d 冲突后重新获取失败: %w", partition.PartitionID, getErr))
					continue
				}
				result = append(result, existing)
				s.logger.Infof("分区 %d 由于并发创建而获取现有分区", partition.PartitionID)
			} else {
				errs = append(errs, fmt.Errorf("创建分区 %d 失败: %w", partition.PartitionID, err))
				continue
			}
		} else {
			result = append(result, newPartition)
		}
	}

	if len(errs) > 0 {
		return result, fmt.Errorf("批量创建分区时发生错误: %v", errs)
	}

	s.logger.Infof("成功批量创建/获取 %d 个分区", len(result))
	return result, nil
}

// DeletePartitions 批量删除分区
func (s *HashPartitionStrategy) DeletePartitions(ctx context.Context, partitionIDs []int) error {
	var errs []error

	for _, partitionID := range partitionIDs {
		err := s.DeletePartition(ctx, partitionID)
		if err != nil {
			errs = append(errs, fmt.Errorf("删除分区 %d 失败: %w", partitionID, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("批量删除分区时发生错误: %v", errs)
	}

	s.logger.Infof("成功批量删除 %d 个分区", len(partitionIDs))
	return nil
}

// StrategyType 获取策略类型标识
func (s *HashPartitionStrategy) StrategyType() string {
	return "hash"
}

// AcquirePartition 声明对指定分区的持有权
func (s *HashPartitionStrategy) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	if workerID == "" {
		return nil, false, fmt.Errorf("工作节点ID不能为空")
	}

	// 设置默认选项
	if options == nil {
		options = &model.AcquirePartitionOptions{}
	}

	// 在并发环境下，对于pending分区的获取可能需要重试几次以处理乐观锁冲突
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// 获取当前分区信息
		partition, err := s.GetPartition(ctx, partitionID)
		if err != nil {
			return nil, false, fmt.Errorf("获取分区 %d 失败: %w", partitionID, err)
		}

		s.logger.Debugf("[%s] 尝试 #%d: 分区 %d 状态=%s, 持有者=%s", workerID, attempt+1, partitionID, partition.Status, partition.WorkerID)

		// 检查是否是同一个工作节点重复声明
		if partition.WorkerID == workerID {
			// 同一工作节点重复声明，更新心跳时间
			s.logger.Infof("工作节点 %s 重新声明分区 %d，更新心跳", workerID, partitionID)
			updatedPartition, err := s.updatePartitionHeartbeat(ctx, partition, workerID)
			if err != nil {
				if errors.Is(err, ErrOptimisticLockFailed) && attempt < maxRetries-1 {
					s.logger.Debugf("[%s] 心跳更新乐观锁失败，重试 #%d", workerID, attempt+2)
					continue // 重试
				}
				return nil, false, err
			}
			return updatedPartition, true, nil
		}

		// 检查分区是否可以直接声明（pending状态且无持有者）
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			s.logger.Debugf("[%s] 尝试直接获取pending分区 %d", workerID, partitionID)
			updatedPartition, err := s.acquirePartitionDirect(ctx, partition, workerID)
			if err != nil {
				if errors.Is(err, data.ErrOptimisticLockFailed) && attempt < maxRetries-1 {
					// 乐观锁失败，重试。但只对pending状态的分区重试
					s.logger.Debugf("[%s] 直接获取乐观锁失败，重试 #%d", workerID, attempt+2)
					continue
				}
				s.logger.Debugf("[%s] 直接获取失败: %v", workerID, err)
				return nil, false, err
			}
			return updatedPartition, true, nil
		}

		// 分区已被其他worker持有，检查是否允许抢占
		if !options.AllowPreemption {
			// 分区被占用且不允许抢占，这是正常情况，不返回错误，也不重试
			s.logger.Debugf("分区 %d 已被工作节点 %s 持有，且不允许抢占", partitionID, partition.WorkerID)
			return nil, false, nil
		}

		// 尝试基于心跳时间抢占分区
		updatedPartition, success, err := s.tryPreemptPartitionByHeartbeat(ctx, partition, workerID, options)
		if err != nil {
			if errors.Is(err, ErrOptimisticLockFailed) && attempt < maxRetries-1 {
				s.logger.Debugf("[%s] 抢占乐观锁失败，重试 #%d", workerID, attempt+2)
				continue // 重试
			}
			return nil, false, err
		}

		// 抢占的结果是确定的，不需要重试
		return updatedPartition, success, nil
	}

	// 如果所有重试都失败了，返回最后的错误
	return nil, false, fmt.Errorf("获取分区 %d 失败：超过最大重试次数", partitionID)
}

// UpdatePartitionStatus 更新分区状态
func (s *HashPartitionStrategy) UpdatePartitionStatus(ctx context.Context, partitionID int, workerID string, status model.PartitionStatus, metadata map[string]interface{}) error {
	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		return fmt.Errorf("获取分区 %d 失败: %w", partitionID, err)
	}

	// 验证权限：只有分区的持有者才能更新状态
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 无权更新分区 %d (当前持有者: %s)", workerID, partitionID, partition.WorkerID)
	}

	// 更新分区状态
	partition.Status = status
	if status == model.StatusRunning || status == model.StatusClaimed {
		partition.LastHeartbeat = time.Now()
	}

	// 如果有元数据，合并到分区选项中
	if metadata != nil {
		if partition.Options == nil {
			partition.Options = make(map[string]interface{})
		}
		for k, v := range metadata {
			partition.Options[k] = v
		}
	}

	// 使用版本控制更新
	_, err = s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		return fmt.Errorf("更新分区 %d 状态失败: %w", partitionID, err)
	}

	s.logger.Infof("工作节点 %s 成功更新分区 %d 状态为 %s", workerID, partitionID, status)
	return nil
}

// ReleasePartition 释放分区
func (s *HashPartitionStrategy) ReleasePartition(ctx context.Context, partitionID int, workerID string) error {
	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		return fmt.Errorf("获取分区 %d 失败: %w", partitionID, err)
	}

	// 验证权限：只有分区的持有者才能释放
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 无权释放分区 %d (当前持有者: %s)", workerID, partitionID, partition.WorkerID)
	}

	// 重置分区状态：已完成的分区保持completed状态，其他状态重置为pending
	if partition.Status != model.StatusCompleted && partition.Status != model.StatusFailed {
		partition.Status = model.StatusPending
	}
	partition.WorkerID = ""
	partition.LastHeartbeat = time.Time{} // 清空心跳时间

	// 使用版本控制更新
	_, err = s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		return fmt.Errorf("释放分区 %d 失败: %w", partitionID, err)
	}

	s.logger.Infof("工作节点 %s 成功释放分区 %d", workerID, partitionID)
	return nil
}

// GetPartitionStats 获取分区状态统计信息
func (s *HashPartitionStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	allPartitions, err := s.GetAllPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取所有分区失败: %w", err)
	}

	stats := &model.PartitionStats{
		Total: len(allPartitions),
	}

	// 统计各种状态的分区数量
	for _, partition := range allPartitions {
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
	}

	// 计算比率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	}

	s.logger.Debugf("分区统计: 总计=%d, 等待=%d, 已声明=%d, 运行中=%d, 已完成=%d, 失败=%d",
		stats.Total, stats.Pending, stats.Claimed, stats.Running, stats.Completed, stats.Failed)

	return stats, nil
}

// ==================== 心跳和抢占辅助方法 ====================

// acquirePartitionDirect 直接获取pending状态的分区
func (s *HashPartitionStrategy) acquirePartitionDirect(ctx context.Context, partition *model.PartitionInfo, workerID string) (*model.PartitionInfo, error) {
	// 更新分区状态
	partition.Status = model.StatusClaimed
	partition.WorkerID = workerID
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	updatedPartition, err := s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		return nil, err
	}

	s.logger.Infof("工作节点 %s 成功声明分区 %d", workerID, partition.PartitionID)
	return updatedPartition, nil
}

// preemptPartitionByHeartbeat 基于心跳时间抢占分区
func (s *HashPartitionStrategy) preemptPartitionByHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, error) {
	// 检查是否可以抢占（基于心跳过时）
	if !options.ForcePreemption {
		if !s.isPartitionStaleByHeartbeat(partition) {
			return nil, fmt.Errorf("分区 %d 持有者 %s 心跳仍然活跃（最后心跳: %s），无法抢占",
				partition.PartitionID, partition.WorkerID, partition.LastHeartbeat.Format(time.RFC3339))
		}
	}

	// 记录抢占操作
	originalWorker := partition.WorkerID
	s.logger.Infof("工作节点 %s 基于心跳超时抢占分区 %d（原持有者: %s, 最后心跳: %s）",
		workerID, partition.PartitionID, originalWorker, partition.LastHeartbeat.Format(time.RFC3339))

	// 更新分区状态
	partition.Status = model.StatusClaimed
	partition.WorkerID = workerID
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	updatedPartition, err := s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		return nil, fmt.Errorf("抢占分区 %d 失败: %w", partition.PartitionID, err)
	}

	s.logger.Infof("工作节点 %s 成功抢占分区 %d（原持有者: %s）", workerID, partition.PartitionID, originalWorker)
	return updatedPartition, nil
}

// tryPreemptPartitionByHeartbeat 尝试基于心跳时间抢占分区，返回新的接口签名
func (s *HashPartitionStrategy) tryPreemptPartitionByHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	// 检查是否可以抢占（基于心跳过时）
	if !options.ForcePreemption {
		if !s.isPartitionStaleByHeartbeat(partition) {
			// 分区心跳仍然活跃，无法抢占，这是正常情况，不返回错误
			s.logger.Debugf("分区 %d 持有者 %s 心跳仍然活跃（最后心跳: %s），无法抢占",
				partition.PartitionID, partition.WorkerID, partition.LastHeartbeat.Format(time.RFC3339))
			return nil, false, nil
		}
	}

	// 调用原有的抢占方法
	updatedPartition, err := s.preemptPartitionByHeartbeat(ctx, partition, workerID, options)
	if err != nil {
		// 系统错误（如网络、数据库错误）
		return nil, false, err
	}

	// 抢占成功
	return updatedPartition, true, nil
}

// isPartitionStaleByHeartbeat 检查分区是否因心跳超时而过时
func (s *HashPartitionStrategy) isPartitionStaleByHeartbeat(partition *model.PartitionInfo) bool {
	if partition.LastHeartbeat.IsZero() {
		// 如果从未有心跳，认为是过时的
		return true
	}
	return time.Since(partition.LastHeartbeat) > s.staleThreshold
}

// updatePartitionHeartbeat 更新分区心跳时间
func (s *HashPartitionStrategy) updatePartitionHeartbeat(ctx context.Context, partition *model.PartitionInfo, workerID string) (*model.PartitionInfo, error) {
	// 验证权限：只有分区的持有者才能更新心跳
	if partition.WorkerID != "" && partition.WorkerID != workerID {
		return nil, fmt.Errorf("工作节点 %s 无权更新分区 %d 心跳 (当前持有者: %s)", workerID, partition.PartitionID, partition.WorkerID)
	}

	// 更新心跳时间
	partition.LastHeartbeat = time.Now()

	// 使用版本控制更新
	updatedPartition, err := s.updatePartitionWithVersionControl(ctx, partition)
	if err != nil {
		return nil, fmt.Errorf("更新分区 %d 心跳失败: %w", partition.PartitionID, err)
	}

	s.logger.Debugf("工作节点 %s 成功更新分区 %d 心跳", workerID, partition.PartitionID)
	return updatedPartition, nil
}

// MaintainPartitionHold 维护对分区的持有权（更新心跳时间）
func (s *HashPartitionStrategy) MaintainPartitionHold(ctx context.Context, partitionID int, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("工作节点ID不能为空")
	}

	// 获取当前分区信息
	partition, err := s.GetPartition(ctx, partitionID)
	if err != nil {
		if errors.Is(err, ErrPartitionNotFound) {
			return fmt.Errorf("分区 %d 不存在", partitionID)
		}
		return fmt.Errorf("获取分区 %d 失败: %w", partitionID, err)
	}

	// 检查分区是否被该工作节点持有
	if partition.WorkerID != workerID {
		return fmt.Errorf("工作节点 %s 没有持有分区 %d（当前持有者: %s）", workerID, partitionID, partition.WorkerID)
	}

	// 更新心跳时间
	_, err = s.updatePartitionHeartbeat(ctx, partition, workerID)
	if err != nil {
		s.logger.Errorf("工作节点 %s 维护分区 %d 心跳失败: %v", workerID, partitionID, err)
		return fmt.Errorf("维护分区 %d 心跳失败: %w", partitionID, err)
	}

	s.logger.Debugf("工作节点 %s 成功维护分区 %d 的持有权", workerID, partitionID)
	return nil
}
