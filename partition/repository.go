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

// Repository 处理分区数据的访问和生命周期管理。
type Repository struct {
	store  data.HashPartitionOperations // 分区存储接口，使用最小接口而不是完整DataStore
	logger utils.Logger                 // 日志记录器
}

// NewRepository 创建一个新的分区仓库实例。
// 现在需要一个 utils.Logger 实例。
func NewRepository(store data.HashPartitionOperations, logger utils.Logger) *Repository {
	if logger == nil {
		panic("logger cannot be nil") // 日志记录器不能为空
	}
	return &Repository{
		store:  store,
		logger: logger,
	}
}

// UpdatePartitionOptimistically 尝试在分区版本与 expectedVersion 匹配时更新分区。
// 成功更新后，版本号会递增。
func (s *Repository) UpdatePartitionOptimistically(ctx context.Context, partitionInfo *model.PartitionInfo, expectedVersion int64) (*model.PartitionInfo, error) {
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
		return nil, model.ErrOptimisticLockFailed // 返回特定的错误
	}

	s.logger.Infof("成功将分区 %d 更新到版本 %d (状态: %s, WorkerID: %s)", updatedPartition.PartitionID, newVersion, updatedPartition.Status, updatedPartition.WorkerID)
	return &updatedPartition, nil
}

// CreatePartitionAtomically 尝试在存储中创建一个新的分区定义，
// 仅当它尚不存在时（即 expectedVersion 为 0）。它将初始版本设置为 1。
func (s *Repository) CreatePartitionAtomically(ctx context.Context, partitionID int, minID, maxID int64, options map[string]interface{}) (*model.PartitionInfo, error) {
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
		// 如果 HUpdatePartitionWithVersion 行为良好，它应该在 `err` 中返回 model.ErrPartitionAlreadyExists。
		// 如果 `err` 为 nil 但 `success` 为 false，则表示存在冲突。
		return nil, model.ErrPartitionAlreadyExists // 或更通用的创建冲突错误
	}

	s.logger.Infof("成功创建分区 %d，版本为 1", partitionID)
	return newPartition, nil
}

// GetPartitionsFilters 定义了检索分区时的过滤条件。
type GetPartitionsFilters struct {
	// TargetStatuses 指定了要匹配的分区状态列表。
	// 如果为空，则不按状态过滤，所有状态的分区都可能成为候选，具体取决于其他过滤器。
	TargetStatuses []model.PartitionStatus

	// StaleDuration 指定分区被视为“过时”的最小持续时间（自 UpdatedAt 以来）。
	// 如果分区的 UpdatedAt 时间戳与当前时间的差值超过此持续时间，则被视为过时，应该被返回。
	// 如果为 nil 或非正数（例如 time.Duration(0)），则此过时过滤器无效。
	StaleDuration *time.Duration

	// ExcludeWorkerIDOnStale 如果 StaleDuration 过滤器激活，
	// 则具有此 WorkerID 的分区不会被视为过时（即使它们满足时间条件）。
	// 如果为空字符串，则不排除任何 WorkerID。
	ExcludeWorkerIDOnStale string
}

// GetFilteredPartitions 根据提供的过滤器从存储中检索分区数据。
// 返回的分区将按 PartitionID 升序排序。
func (s *Repository) GetFilteredPartitions(ctx context.Context, filters GetPartitionsFilters) ([]*model.PartitionInfo, error) {
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
func (s *Repository) DeletePartition(ctx context.Context, partitionID int) error {
	err := s.store.HDeletePartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		s.logger.Errorf("删除分区 %d 失败: %v", partitionID, err)
		return fmt.Errorf("删除分区 %d 失败: %w", partitionID, err)
	}
	s.logger.Infof("成功删除分区 %d", partitionID)
	return nil
}

// GetAllPartitions 从存储中检索所有分区数据。
func (s *Repository) GetAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
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
func (s *Repository) GetPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	jsonStr, err := s.store.HGetPartition(ctx, partitionHashKey, strconv.Itoa(partitionID))
	if err != nil {
		if errors.Is(err, data.ErrNotFound) { // 检查 data.ErrNotFound
			s.logger.Infof("分区 %d 未在存储中找到 (data.ErrNotFound)", partitionID)
			return nil, ErrPartitionNotFound // 更正：使用 model.ErrPartitionNotFound
		}
		// 对于其他类型的错误，例如 redis.Nil 直接从 HGetPartition 泄漏（不应该发生如果 HGetPartition 正确包装）
		// 或者其他连接/IO错误
		if errors.Is(err, redis.Nil) { // 以防万一 HGetPartition 没有包装 redis.Nil
			s.logger.Infof("分区 %d 未在存储中找到 (redis.Nil)", partitionID)
			return nil, ErrPartitionNotFound // 更正：使用 model.ErrPartitionNotFound
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
func (s *Repository) SavePartition(ctx context.Context, partitionInfo *model.PartitionInfo) error {
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
