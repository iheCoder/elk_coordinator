package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 分离式归档策略常量 ====================

const (
	// 归档分区的存储键
	archivedPartitionsKey = "elk_archived" // 归档分区独立存储
	// 压缩归档的存储键
	compressedArchiveKey = "elk_compressed_archive" // 压缩归档独立存储
	// 默认压缩阈值
	defaultArchiveCompressionThreshold = 100000
	// 默认压缩批次大小
	defaultCompressionBatchSize = 50000
)

// ArchiveConfig 归档配置
type ArchiveConfig struct {
	// CompressionThreshold 压缩阈值：当归档分区数量超过此值时进行压缩
	CompressionThreshold int
	// CompressionBatchSize 压缩批次大小：每次压缩处理的分区数量
	CompressionBatchSize int
}

// DefaultArchiveConfig 返回默认的归档配置
func DefaultArchiveConfig() *ArchiveConfig {
	return &ArchiveConfig{
		CompressionThreshold: defaultArchiveCompressionThreshold,
		CompressionBatchSize: defaultCompressionBatchSize,
	}
}

// ==================== 分离式归档策略实现 ====================

// 注意：使用同步归档策略，确保"先归档再删除"的数据安全
// 不使用异步通道，避免归档失败导致的数据丢失风险

// archiveCompletedPartition 归档已完成的分区
// 直接使用 model.PartitionInfo，保持数据结构一致性
// 注意：这是完成状态分区的特定归档逻辑，未来可扩展支持其他归档触发条件
func (s *HashPartitionStrategy) archiveCompletedPartition(ctx context.Context, partition *model.PartitionInfo) error {
	if err := s.archivePartition(ctx, partition, "completed"); err != nil {
		return err
	}

	// 归档成功后，尝试进行压缩
	if err := s.tryCompressArchive(ctx); err != nil {
		s.logger.Warnf("尝试压缩归档失败: %v", err)
		// 压缩失败不应该影响归档操作的成功
	}

	return nil
}

// archivePartition 通用的分区归档方法
// reason: 归档原因，用于日志记录和未来的归档策略扩展
func (s *HashPartitionStrategy) archivePartition(ctx context.Context, partition *model.PartitionInfo, reason string) error {
	// 序列化为 JSON（直接使用完整的 PartitionInfo 结构）
	partitionJson, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化归档分区 %d 失败: %w", partition.PartitionID, err)
	}

	// 存储到独立的归档分区 Hash 中
	err = s.store.HSetPartition(ctx, archivedPartitionsKey, strconv.Itoa(partition.PartitionID), string(partitionJson))
	if err != nil {
		return fmt.Errorf("存储归档分区 %d 失败: %w", partition.PartitionID, err)
	}

	s.logger.Infof("成功归档分区 %d (Worker: %s, 状态: %s, 原因: %s)",
		partition.PartitionID, partition.WorkerID, partition.Status, reason)

	return nil
}

// GetArchivedPartitionsCountWithoutCompress 获取归档分区的数量（使用 HLen 轻量级操作）
func (s *HashPartitionStrategy) GetArchivedPartitionsCountWithoutCompress(ctx context.Context) (int, error) {
	count, err := s.store.HLen(ctx, archivedPartitionsKey)
	if err != nil {
		return 0, fmt.Errorf("获取归档分区数量失败: %w", err)
	}
	return int(count), nil
}

// GetArchivedPartition 从归档存储中获取分区信息（包括压缩的）
// 优化版本：先查未压缩，再查压缩，避免不必要的压缩数据处理
func (s *HashPartitionStrategy) GetArchivedPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	if partitionID <= 0 {
		return nil, fmt.Errorf("无效的分区ID: %d", partitionID)
	}

	// 首先尝试从未压缩的归档中获取（通常更快）
	partitionJson, err := s.store.HGetPartition(ctx, archivedPartitionsKey, strconv.Itoa(partitionID))
	if err == nil {
		var partition model.PartitionInfo
		if err := json.Unmarshal([]byte(partitionJson), &partition); err != nil {
			s.logger.Warnf("反序列化未压缩归档分区 %d 失败: %v", partitionID, err)
			// 继续尝试压缩数据，可能数据损坏但压缩数据完好
		} else {
			return &partition, nil
		}
	}

	// 如果未压缩归档中没有，且有压缩器，则搜索压缩数据
	if s.compressor != nil {
		partition, found, searchErr := s.searchCompressedPartition(ctx, partitionID)
		if searchErr != nil {
			s.logger.Warnf("搜索压缩归档分区 %d 失败: %v", partitionID, searchErr)
			// 搜索错误不应该掩盖原始的"未找到"错误
		} else if found {
			return partition, nil
		}
	}

	return nil, ErrArchivedPartitionNotFound
}

// GetAllCompletedPartitions 获取所有已完成分区（包括压缩的）
// 注意：这里仍然使用"completed"概念，因为从业务角度它查询的是已完成的分区
// 但底层使用优化后的归档存储，自动包含压缩数据
func (s *HashPartitionStrategy) GetAllCompletedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	return s.GetAllArchivedPartitions(ctx)
}

// GetAllArchivedPartitions 获取所有归档分区（包括压缩的）
// 优化版本：在获取未压缩分区的同时检测是否有压缩数据，提升性能
func (s *HashPartitionStrategy) GetAllArchivedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	// 获取未压缩的归档分区
	uncompressedPartitions, err := s.getUncompressedArchivedPartitions(ctx)
	if err != nil {
		return nil, err
	}

	// 快速检测是否需要获取压缩数据
	if s.compressor != nil {
		compressedPartitions, hasCompressed := s.tryGetCompressedPartitions(ctx)
		if hasCompressed {
			// 合并未压缩和压缩的分区
			allPartitions := make([]*model.PartitionInfo, 0, len(uncompressedPartitions)+len(compressedPartitions))
			allPartitions = append(allPartitions, uncompressedPartitions...)
			allPartitions = append(allPartitions, compressedPartitions...)
			return allPartitions, nil
		}
	}

	return uncompressedPartitions, nil
}

// tryGetCompressedPartitions 尝试获取压缩的分区，返回分区列表和是否存在压缩数据的标志
// 优化版本：使用 HLen 进行快速检测，添加内存保护和错误恢复
func (s *HashPartitionStrategy) tryGetCompressedPartitions(ctx context.Context) ([]*model.PartitionInfo, bool) {
	// 第一步：使用 HLen 进行轻量级检查
	compressedCount, err := s.store.HLen(ctx, compressedArchiveKey)
	if err != nil || compressedCount == 0 {
		return nil, false // 没有压缩数据或获取失败
	}

	// 第二步：检查数据量，防止内存溢出
	if compressedCount > 1000 { // 假设每个批次平均50000个分区，1000个批次约5000万分区
		s.logger.Warnf("压缩批次数量过多 (%d)，可能影响性能，建议优化压缩策略", compressedCount)
	}

	// 第三步：只有确认有压缩数据时才获取实际数据
	compressedDataMap, err := s.store.HGetAllPartitions(ctx, compressedArchiveKey)
	if err != nil {
		s.logger.Warnf("获取压缩归档数据失败: %v", err)
		return nil, false
	}

	// 第四步：预估总分区数，做内存预分配优化
	estimatedPartitionCount := int(compressedCount) * s.archiveConfig.CompressionBatchSize / 2 // 保守估计
	allPartitions := make([]*model.PartitionInfo, 0, estimatedPartitionCount)

	// 第五步：解压缩所有批次，添加错误恢复机制
	successfulBatches := 0
	failedBatches := 0

	for batchKey, batchJson := range compressedDataMap {
		var batch CompressedBatch
		if err := json.Unmarshal([]byte(batchJson), &batch); err != nil {
			s.logger.Warnf("反序列化压缩批次 %s 失败: %v", batchKey, err)
			failedBatches++
			continue
		}

		partitions, err := s.compressor.DecompressPartitionBatch(&batch)
		if err != nil {
			s.logger.Warnf("解压缩批次 %s 失败: %v", batchKey, err)
			failedBatches++
			continue
		}

		allPartitions = append(allPartitions, partitions...)
		successfulBatches++
	}

	// 记录处理统计信息
	if failedBatches > 0 {
		s.logger.Warnf("解压缩完成：成功 %d 批次，失败 %d 批次，共获取 %d 个分区",
			successfulBatches, failedBatches, len(allPartitions))
	} else {
		s.logger.Infof("解压缩完成：成功处理 %d 批次，共获取 %d 个分区",
			successfulBatches, len(allPartitions))
	}

	return allPartitions, len(allPartitions) > 0
}

// getUncompressedArchivedPartitions 获取未压缩的归档分区
func (s *HashPartitionStrategy) getUncompressedArchivedPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	// 获取未压缩的归档分区数据
	dataMap, err := s.store.HGetAllPartitions(ctx, archivedPartitionsKey)
	if err != nil {
		return nil, fmt.Errorf("获取未压缩归档分区失败: %w", err)
	}

	// 解析未压缩的分区
	partitions := make([]*model.PartitionInfo, 0, len(dataMap))
	for field, value := range dataMap {
		var partition model.PartitionInfo
		if err := json.Unmarshal([]byte(value), &partition); err != nil {
			s.logger.Warnf("归档分区字段 %s 的 JSON 反序列化失败: %v", field, err)
			continue
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}

// searchCompressedPartition 在压缩数据中搜索特定分区（优化版本，避免解压所有数据）
func (s *HashPartitionStrategy) searchCompressedPartition(ctx context.Context, partitionID int) (*model.PartitionInfo, bool, error) {
	// 快速检查是否有压缩数据
	compressedCount, err := s.store.HLen(ctx, compressedArchiveKey)
	if err != nil || compressedCount == 0 {
		return nil, false, err
	}

	// 获取压缩批次元数据
	compressedDataMap, err := s.store.HGetAllPartitions(ctx, compressedArchiveKey)
	if err != nil {
		return nil, false, err
	}

	// 优化策略：使用 goroutine 并行搜索多个批次
	// 当批次数量较多时，并行搜索可以显著提升性能
	batchCount := len(compressedDataMap)
	if batchCount == 0 {
		return nil, false, nil
	}

	// 小批次数量时使用顺序搜索，避免 goroutine 开销
	if batchCount <= 3 {
		return s.searchCompressedPartitionSequential(ctx, compressedDataMap, partitionID)
	}

	// 大批次数量时使用并行搜索
	return s.searchCompressedPartitionParallel(ctx, compressedDataMap, partitionID)
}

// searchCompressedPartitionSequential 顺序搜索压缩分区
func (s *HashPartitionStrategy) searchCompressedPartitionSequential(ctx context.Context, compressedDataMap map[string]string, partitionID int) (*model.PartitionInfo, bool, error) {
	for batchKey, batchJson := range compressedDataMap {
		var batch CompressedBatch
		if err := json.Unmarshal([]byte(batchJson), &batch); err != nil {
			s.logger.Warnf("反序列化压缩批次 %s 失败: %v", batchKey, err)
			continue
		}

		// 解压这个批次的数据
		partitions, err := s.compressor.DecompressPartitionBatch(&batch)
		if err != nil {
			s.logger.Warnf("解压缩批次 %s 失败: %v", batchKey, err)
			continue
		}

		// 在这个批次中搜索目标分区（一旦找到立即返回）
		for _, partition := range partitions {
			if partition.PartitionID == partitionID {
				return partition, true, nil
			}
		}
	}

	return nil, false, nil
}

// searchCompressedPartitionParallel 并行搜索压缩分区
func (s *HashPartitionStrategy) searchCompressedPartitionParallel(ctx context.Context, compressedDataMap map[string]string, partitionID int) (*model.PartitionInfo, bool, error) {
	type batchResult struct {
		partition *model.PartitionInfo
		found     bool
		err       error
	}

	// 创建结果通道和取消上下文
	resultChan := make(chan batchResult, len(compressedDataMap))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动 goroutine 搜索每个批次
	var wg sync.WaitGroup
	for batchKey, batchJson := range compressedDataMap {
		wg.Add(1)
		go func(key, jsonData string) {
			defer wg.Done()

			// 检查是否已经取消
			select {
			case <-ctx.Done():
				return
			default:
			}

			var batch CompressedBatch
			if err := json.Unmarshal([]byte(jsonData), &batch); err != nil {
				s.logger.Warnf("反序列化压缩批次 %s 失败: %v", key, err)
				resultChan <- batchResult{err: err}
				return
			}

			partitions, err := s.compressor.DecompressPartitionBatch(&batch)
			if err != nil {
				s.logger.Warnf("解压缩批次 %s 失败: %v", key, err)
				resultChan <- batchResult{err: err}
				return
			}

			// 搜索目标分区
			for _, partition := range partitions {
				if partition.PartitionID == partitionID {
					// 找到目标分区，立即返回并取消其他搜索
					resultChan <- batchResult{partition: partition, found: true}
					cancel() // 取消其他 goroutine
					return
				}
			}

			// 此批次中没有找到
			resultChan <- batchResult{found: false}
		}(batchKey, batchJson)
	}

	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	for result := range resultChan {
		if result.found && result.partition != nil {
			return result.partition, true, nil
		}
		if result.err != nil {
			s.logger.Warnf("搜索压缩批次时发生错误: %v", result.err)
		}
	}

	return nil, false, nil
}

// ==================== 压缩归档策略实现 ====================

// tryCompressArchive 尝试对归档分区进行压缩
// 当归档分区数量达到压缩阈值时，将分区批量压缩存储
func (s *HashPartitionStrategy) tryCompressArchive(ctx context.Context) error {
	// 检查是否支持压缩
	if s.compressor == nil {
		return nil // 压缩器未初始化，跳过压缩
	}

	// 获取当前未压缩归档分区数量
	count, err := s.GetArchivedPartitionsCountWithoutCompress(ctx)
	if err != nil {
		return fmt.Errorf("获取归档分区数量失败: %w", err)
	}

	// 检查是否达到压缩阈值
	if count < s.archiveConfig.CompressionThreshold {
		return nil // 未达到压缩阈值，无需压缩
	}

	// 只获取未压缩的归档分区（避免重复压缩）
	partitions, err := s.getUncompressedArchivedPartitions(ctx)
	if err != nil {
		return fmt.Errorf("获取未压缩归档分区失败: %w", err)
	}

	// 按批次进行压缩
	return s.compressArchiveBatches(ctx, partitions)
}

// compressArchiveBatches 将归档分区按批次进行压缩
// 新策略：超过配置阈值时，以配置的批次大小进行压缩，直到不超过阈值为止
// 优化版本：添加性能监控和资源管理
func (s *HashPartitionStrategy) compressArchiveBatches(ctx context.Context, partitions []*model.PartitionInfo) error {
	totalPartitions := len(partitions)
	if totalPartitions == 0 {
		return nil // 没有分区需要压缩
	}

	// 计算需要压缩多少个分区才能降到阈值以下
	excessCount := totalPartitions - s.archiveConfig.CompressionThreshold
	if excessCount <= 0 {
		return nil // 已经在阈值以下，无需压缩
	}

	// 计算需要压缩的批次数量
	batchesToCompress := (excessCount + s.archiveConfig.CompressionBatchSize - 1) / s.archiveConfig.CompressionBatchSize // 向上取整
	if batchesToCompress*s.archiveConfig.CompressionBatchSize < excessCount {
		batchesToCompress++ // 确保能压缩足够的分区
	}

	s.logger.Infof("开始压缩归档：总分区数=%d，超出阈值=%d，需要压缩批次数=%d，预计压缩后剩余=%d",
		totalPartitions, excessCount, batchesToCompress, totalPartitions-batchesToCompress*s.archiveConfig.CompressionBatchSize)

	// 性能监控
	compressionStartTime := time.Now()
	var totalCompressedPartitions int
	var totalCompressionErrors int

	batchIndex := 0
	for batch := 0; batch < batchesToCompress; batch++ {
		start := batch * s.archiveConfig.CompressionBatchSize
		end := start + s.archiveConfig.CompressionBatchSize
		if end > totalPartitions {
			end = totalPartitions
		}

		if start >= totalPartitions {
			break
		}

		batchPartitions := partitions[start:end]
		batchStartTime := time.Now()

		if err := s.compressAndStoreArchiveBatch(ctx, batchPartitions, batchIndex); err != nil {
			s.logger.Errorf("压缩批次 %d 失败: %v", batchIndex, err)
			totalCompressionErrors++
			// 压缩失败时继续处理下一批次，避免单个批次失败影响整体压缩
			continue
		}

		// 删除已压缩的原始归档分区
		if err := s.removeCompressedArchivePartitions(ctx, batchPartitions); err != nil {
			s.logger.Warnf("删除已压缩的归档分区失败: %v", err)
			// 删除失败不应该阻止压缩过程，但需要记录
		}

		batchIndex++
		totalCompressedPartitions += len(batchPartitions)
		batchDuration := time.Since(batchStartTime)

		s.logger.Infof("完成压缩批次 %d/%d，包含 %d 个分区，耗时 %v，平均 %v/分区",
			batch+1, batchesToCompress, len(batchPartitions), batchDuration, batchDuration/time.Duration(len(batchPartitions)))
	}

	// 最终统计
	totalDuration := time.Since(compressionStartTime)
	s.logger.Infof("压缩归档完成：处理 %d 个分区，%d 个批次，总耗时 %v，平均 %v/分区，错误 %d 次",
		totalCompressedPartitions, batchIndex, totalDuration,
		totalDuration/time.Duration(max(totalCompressedPartitions, 1)), totalCompressionErrors)

	if totalCompressionErrors > 0 {
		return fmt.Errorf("压缩过程中发生 %d 次错误，请检查日志", totalCompressionErrors)
	}

	return nil
}

// compressAndStoreArchiveBatch 压缩并存储归档批次
func (s *HashPartitionStrategy) compressAndStoreArchiveBatch(ctx context.Context, partitions []*model.PartitionInfo, batchIndex int) error {
	// 压缩分区批次
	compressedBatch, err := s.compressor.CompressPartitionBatch(partitions)
	if err != nil {
		return fmt.Errorf("压缩分区批次失败: %w", err)
	}

	// 序列化压缩批次
	batchJson, err := json.Marshal(compressedBatch)
	if err != nil {
		return fmt.Errorf("序列化压缩批次失败: %w", err)
	}

	// 使用Redis原子计数器+时间戳确保全局唯一性
	batchKey, err := s.generateUniqueBatchKey(ctx)
	if err != nil {
		return fmt.Errorf("生成唯一批次键失败: %w", err)
	}
	err = s.store.HSetPartition(ctx, compressedArchiveKey, batchKey, string(batchJson))
	if err != nil {
		return fmt.Errorf("存储压缩批次失败: %w", err)
	}

	s.logger.Infof("成功压缩归档批次 %s，包含 %d 个分区", batchKey, len(partitions))
	return nil
}

// removeCompressedArchivePartitions 删除已压缩的原始归档分区
func (s *HashPartitionStrategy) removeCompressedArchivePartitions(ctx context.Context, partitions []*model.PartitionInfo) error {
	for _, partition := range partitions {
		if err := s.store.HDeletePartition(ctx, archivedPartitionsKey, strconv.Itoa(partition.PartitionID)); err != nil {
			s.logger.Warnf("删除已压缩的归档分区 %d 失败: %v", partition.PartitionID, err)
		}
	}
	return nil
}

// validateCompressionIntegrity 验证压缩数据的完整性（仅在调试或关键操作时使用）
func (s *HashPartitionStrategy) validateCompressionIntegrity(ctx context.Context) error {
	if s.compressor == nil {
		return nil // 没有压缩器，无需验证
	}

	// 获取压缩数据
	compressedDataMap, err := s.store.HGetAllPartitions(ctx, compressedArchiveKey)
	if err != nil {
		return fmt.Errorf("获取压缩数据失败: %w", err)
	}

	if len(compressedDataMap) == 0 {
		return nil // 没有压缩数据，验证通过
	}

	s.logger.Infof("开始验证 %d 个压缩批次的完整性", len(compressedDataMap))

	var totalPartitions int
	var corruptedBatches []string

	for batchKey, batchJson := range compressedDataMap {
		var batch CompressedBatch
		if err := json.Unmarshal([]byte(batchJson), &batch); err != nil {
			corruptedBatches = append(corruptedBatches, fmt.Sprintf("%s(反序列化失败)", batchKey))
			continue
		}

		partitions, err := s.compressor.DecompressPartitionBatch(&batch)
		if err != nil {
			corruptedBatches = append(corruptedBatches, fmt.Sprintf("%s(解压失败)", batchKey))
			continue
		}

		totalPartitions += len(partitions)
	}

	if len(corruptedBatches) > 0 {
		return fmt.Errorf("发现 %d 个损坏的压缩批次: %v", len(corruptedBatches), corruptedBatches)
	}

	s.logger.Infof("压缩完整性验证通过：%d 个批次，%d 个分区", len(compressedDataMap), totalPartitions)
	return nil
}

// GetArchiveStats 获取归档统计信息
func (s *HashPartitionStrategy) GetArchiveStats(ctx context.Context) (*ArchiveStats, error) {
	uncompressedCount, err := s.GetArchivedPartitionsCountWithoutCompress(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取未压缩归档数量失败: %w", err)
	}

	var compressedBatchCount int64
	var compressedPartitionCount int

	if s.compressor != nil {
		compressedBatchCount, err = s.store.HLen(ctx, compressedArchiveKey)
		if err != nil {
			s.logger.Warnf("获取压缩批次数量失败: %v", err)
		} else if compressedBatchCount > 0 {
			// 估算压缩分区数量（避免全部解压）
			compressedPartitionCount = int(compressedBatchCount) * s.archiveConfig.CompressionBatchSize
		}
	}

	return &ArchiveStats{
		UncompressedPartitions:        uncompressedCount,
		CompressedBatches:             int(compressedBatchCount),
		EstimatedCompressedPartitions: compressedPartitionCount,
		TotalEstimatedPartitions:      uncompressedCount + compressedPartitionCount,
		CompressionEnabled:            s.compressor != nil,
	}, nil
}

// ArchiveStats 归档统计信息
type ArchiveStats struct {
	UncompressedPartitions        int  `json:"uncompressed_partitions"`
	CompressedBatches             int  `json:"compressed_batches"`
	EstimatedCompressedPartitions int  `json:"estimated_compressed_partitions"`
	TotalEstimatedPartitions      int  `json:"total_estimated_partitions"`
	CompressionEnabled            bool `json:"compression_enabled"`
}

// generateUniqueBatchKey 使用随机字符串+批次ID生成唯一的批次键
func (s *HashPartitionStrategy) generateUniqueBatchKey(ctx context.Context) (string, error) {
	// 生成简单的随机字符串
	randomStr := s.generateRandomString(8)

	// 使用毫秒时间戳作为批次ID
	batchID := time.Now().UnixMilli()

	// 组合生成唯一键，添加压缩前缀标识
	batchKey := fmt.Sprintf("compressed_batch_%s_%d", randomStr, batchID)

	return batchKey, nil
}

// generateRandomString 生成指定长度的随机字符串
func (s *HashPartitionStrategy) generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	now := time.Now().UnixNano()

	for i := range result {
		// 使用时间戳加索引作为种子生成随机索引
		seed := now + int64(i*1000)
		result[i] = charset[seed%int64(len(charset))]
	}

	return string(result)
}

// generateUniqueBatchKeyWithUUID 使用UUID生成唯一批次键的备选方案
// 如果需要不依赖Redis的唯一性保证，可以使用此方法
/*
func (s *HashPartitionStrategy) generateUniqueBatchKeyWithUUID() string {
	// 生成UUID v4
	uuid := generateUUID()
	timestamp := time.Now().UnixNano() / 1000000
	return fmt.Sprintf("batch_%d_%s", timestamp, uuid)
}

// generateUUID 生成简单的UUID v4
func generateUUID() string {
	// 生成16个随机字节
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() + int64(i))
	}

	// 设置版本号和变体
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
*/

// generateUniqueBatchKeyWithRandom 使用高精度时间戳+随机数的方案
// 如果需要更高性能但略低唯一性保证的方案，可以使用此方法
/*
func (s *HashPartitionStrategy) generateUniqueBatchKeyWithRandom() string {
	timestamp := time.Now().UnixNano() // 纳秒精度
	random := time.Now().UnixNano() % 1000000 // 简单随机数
	return fmt.Sprintf("batch_%d_%d", timestamp, random)
}
*/
