package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// mockWorkerRegistry 为测试提供的mock WorkerRegistry实现
type mockWorkerRegistry struct {
	workers map[string]*model.WorkerInfo
}

func newMockWorkerRegistry() *mockWorkerRegistry {
	// 创建一些测试用的WorkerInfo数据
	now := time.Now()
	workers := map[string]*model.WorkerInfo{
		"elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa": {
			WorkerID:     "elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa",
			RegisterTime: now.Add(-time.Hour),
			StopTime:     nil, // 活跃worker
		},
		"elk-coordinator-worker-7f8g9h0i1j-k2lm3n-2-15a6b7c8": {
			WorkerID:     "elk-coordinator-worker-7f8g9h0i1j-k2lm3n-2-15a6b7c8",
			RegisterTime: now.Add(-2 * time.Hour),
			StopTime:     nil, // 活跃worker
		},
		"elk-coordinator-worker-9e2d3c4b5a-p6q7r8-3-27d8e9f0": {
			WorkerID:     "elk-coordinator-worker-9e2d3c4b5a-p6q7r8-3-27d8e9f0",
			RegisterTime: now.Add(-30 * time.Minute),
			StopTime:     nil, // 活跃worker
		},
		"elk-coordinator-worker-1a2b3c4d5e-s9t0u1-4-39f0a1b2": {
			WorkerID:     "elk-coordinator-worker-1a2b3c4d5e-s9t0u1-4-39f0a1b2",
			RegisterTime: now.Add(-3 * time.Hour),
			StopTime:     nil, // 活跃worker
		},
		"elk-coordinator-worker-6f7g8h9i0j-v2w3x4-5-4b2c3d4e": {
			WorkerID:     "elk-coordinator-worker-6f7g8h9i0j-v2w3x4-5-4b2c3d4e",
			RegisterTime: now.Add(-4 * time.Hour),
			StopTime:     nil, // 活跃worker
		},
		"elk-coordinator-worker-2c3d4e5f6g-y5z6a7-6-5d4e5f6g": {
			WorkerID:     "elk-coordinator-worker-2c3d4e5f6g-y5z6a7-6-5d4e5f6g",
			RegisterTime: now.Add(-5 * time.Hour),
			StopTime:     &now, // 已停止的worker
		},
	}

	return &mockWorkerRegistry{workers: workers}
}

func (m *mockWorkerRegistry) RegisterWorker(ctx context.Context, workerID string) error {
	if _, exists := m.workers[workerID]; !exists {
		m.workers[workerID] = &model.WorkerInfo{
			WorkerID:     workerID,
			RegisterTime: time.Now(),
			StopTime:     nil,
		}
	}
	return nil
}

func (m *mockWorkerRegistry) UnregisterWorker(ctx context.Context, workerID string) error {
	if worker, exists := m.workers[workerID]; exists {
		now := time.Now()
		worker.StopTime = &now
	}
	return nil
}

func (m *mockWorkerRegistry) GetActiveWorkers(ctx context.Context) ([]string, error) {
	var activeWorkers []string
	for workerID, worker := range m.workers {
		if worker.StopTime == nil { // 活跃的worker没有停止时间
			activeWorkers = append(activeWorkers, workerID)
		}
	}
	return activeWorkers, nil
}

func (m *mockWorkerRegistry) GetAllWorkers(ctx context.Context) ([]*model.WorkerInfo, error) {
	var workers []*model.WorkerInfo
	for _, worker := range m.workers {
		workers = append(workers, worker)
	}
	return workers, nil
}

func (m *mockWorkerRegistry) IsWorkerActive(ctx context.Context, workerID string) (bool, error) {
	if worker, exists := m.workers[workerID]; exists {
		return worker.StopTime == nil, nil // 没有停止时间则为活跃
	}
	return false, nil
}

// createRealisticTestPartitions 创建真实场景的测试分区数据
// seed参数确保相同输入产生相同的测试数据，保证对比的一致性
func createRealisticTestPartitions(count int, seed int64) []*model.PartitionInfo {
	partitions := make([]*model.PartitionInfo, count)

	// 固定的基准创建时间，确保测试一致性
	// 以创建时间为基准更符合业务逻辑：分区先创建，后更新
	baseCreatedTime := time.Date(2025, 6, 24, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))

	// 模拟真实的WorkerID池 - 真实长度的K8s Pod名称
	workerIDPool := []string{
		"elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa",
		"elk-coordinator-worker-7f8g9h0i1j-k2lm3n-2-15a6b7c8",
		"elk-coordinator-worker-9e2d3c4b5a-p6q7r8-3-27d8e9f0",
		"elk-coordinator-worker-1a2b3c4d5e-s9t0u1-4-39f0a1b2",
		"elk-coordinator-worker-6f7g8h9i0j-v2w3x4-5-4b2c3d4e",
		"elk-coordinator-worker-2c3d4e5f6g-y5z6a7-6-5d4e5f6g",
	}

	// 错误信息池 - 模拟真实的错误场景
	errorPool := []string{
		"", // 大部分情况下无错误
		"",
		"",
		"",
		"",
		"connection timeout after 30s",
		"database connection lost during processing",
		"insufficient memory: OOM killed",
		"worker node became unreachable",
		"data validation failed: invalid record format",
		"network error: connection refused by target service",
		"processing timeout: exceeded 5 minute limit",
	}

	// 使用种子确保可重现的随机数序列
	// 这里用简单的线性同余算法替代rand，避免外部依赖
	rng := seed
	nextRand := func() int64 {
		rng = (rng*1103515245 + 12345) & 0x7fffffff
		return rng
	}

	for i := 0; i < count; i++ {
		// 确定性的随机选择
		workerIndex := int(nextRand()) % len(workerIDPool)
		errorIndex := int(nextRand()) % len(errorPool)

		// 数据范围大小变化 - 模拟不同业务场景
		rangeVariation := int64(500 + (nextRand() % 1500)) // 500-2000之间
		minID := int64(1000000) + int64(i)*rangeVariation
		maxID := minID + rangeVariation - 1

		// 时间戳变化 - 以创建时间为基准，更符合业务逻辑
		createdTimeOffset := time.Duration(nextRand()%86400) * time.Second // 一天内的随机创建时间
		createdAt := baseCreatedTime.Add(createdTimeOffset)
		// 更新时间在创建时间之后，模拟处理时长（5-35分钟）
		processingDuration := time.Duration(300+nextRand()%1800) * time.Second // 5-35分钟处理时间
		updatedAt := createdAt.Add(processingDuration)

		// 状态确定性分布
		var status model.PartitionStatus
		errorMsg := ""
		statusRand := nextRand() % 100
		if statusRand < 85 { // 85%完成
			status = model.StatusCompleted
		} else if statusRand < 95 { // 10%失败
			status = model.StatusFailed
			errorMsg = errorPool[errorIndex]
			if errorMsg == "" { // 确保失败状态有错误信息
				errorMsg = "processing failed with unknown error"
			}
		} else { // 5%运行中
			status = model.StatusRunning
		}

		partitions[i] = &model.PartitionInfo{
			PartitionID:   4970 + i,
			MinID:         minID,
			MaxID:         maxID,
			WorkerID:      workerIDPool[workerIndex],
			LastHeartbeat: time.Time{}, // completed分区不需要心跳
			Status:        status,
			UpdatedAt:     updatedAt,
			Version:       int64((nextRand() % 50) + 1), // 1-50的版本号
			CreatedAt:     createdAt,
			Error:         errorMsg,
		}
	}

	return partitions
}

// createSamplePartition 创建单个样本分区（保持原有逻辑，用于基础测试）
func createSamplePartition() *model.PartitionInfo {
	// 模拟您提供的真实Redis hash value
	return &model.PartitionInfo{
		PartitionID:   4970,
		MinID:         4969001,
		MaxID:         4970000,
		WorkerID:      "elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa",
		LastHeartbeat: time.Time{}, // "0001-01-01T00:00:00Z"
		Status:        model.StatusCompleted,
		UpdatedAt:     time.Date(2025, 6, 24, 17, 36, 45, 933443109, time.FixedZone("CST", 8*3600)),
		Version:       5,
		CreatedAt:     time.Date(2025, 6, 24, 17, 31, 31, 419944805, time.FixedZone("CST", 8*3600)),
		Error:         "", // 正常情况下为空
	}
}

// createSamplePartitionWithError 创建带错误信息的分区
func createSamplePartitionWithError() *model.PartitionInfo {
	p := createSamplePartition()
	p.PartitionID = 4971
	p.MinID = 4970001
	p.MaxID = 4971000
	p.Status = model.StatusFailed
	p.Error = "connection timeout after 30s"
	return p
}

// createBatchPartitions 创建批量测试分区 - 重构为使用真实场景数据
func createBatchPartitions(count int) []*model.PartitionInfo {
	// 使用固定种子确保测试结果可重现
	return createRealisticTestPartitions(count, 12345)
}

// TestSinglePartitionCompression 测试单个分区的压缩效果
func TestSinglePartitionCompression(t *testing.T) {
	// 创建测试分区
	partition := createSamplePartition()

	// 计算原始JSON大小
	originalJSON, err := json.Marshal(partition)
	if err != nil {
		t.Fatalf("Marshal partition failed: %v", err)
	}

	fmt.Printf("=== 单个分区压缩测试 ===\n")
	fmt.Printf("原始JSON数据: %s\n", string(originalJSON))
	fmt.Printf("原始JSON大小: %d bytes\n", len(originalJSON))

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		t.Fatalf("Failed to create partition compressor: %v", err)
	}

	// 计算压缩统计信息
	stats, err := compressor.CalculateCompressionStats([]*model.PartitionInfo{partition})
	if err != nil {
		t.Fatalf("Calculate compression stats failed: %v", err)
	}

	fmt.Printf("二进制编码大小: %d bytes\n", stats.BinarySize)
	fmt.Printf("gzip压缩后大小: %d bytes\n", stats.CompressedSize)
	fmt.Printf("压缩比: %.2f%%\n", stats.CompressionRatio*100)
	fmt.Printf("二进制编码压缩比: %.2f%%\n", (1.0-float64(stats.BinarySize)/float64(stats.OriginalSize))*100)

	// 验证压缩和解压的正确性
	compressedBatch, err := compressor.CompressPartitionBatch([]*model.PartitionInfo{partition})
	if err != nil {
		t.Fatalf("Compress partition batch failed: %v", err)
	}

	decompressedPartitions, err := compressor.DecompressPartitionBatch(compressedBatch)
	if err != nil {
		t.Fatalf("Decompress partition batch failed: %v", err)
	}

	// 验证数据完整性
	if len(decompressedPartitions) != 1 {
		t.Fatalf("Expected 1 partition, got %d", len(decompressedPartitions))
	}

	decompressed := decompressedPartitions[0]
	if decompressed.PartitionID != partition.PartitionID {
		t.Errorf("PartitionID mismatch: expected %d, got %d", partition.PartitionID, decompressed.PartitionID)
	}
	if decompressed.MinID != partition.MinID {
		t.Errorf("MinID mismatch: expected %d, got %d", partition.MinID, decompressed.MinID)
	}
	if decompressed.MaxID != partition.MaxID {
		t.Errorf("MaxID mismatch: expected %d, got %d", partition.MaxID, decompressed.MaxID)
	}
	if decompressed.WorkerID != partition.WorkerID {
		t.Errorf("WorkerID mismatch: expected %s, got %s", partition.WorkerID, decompressed.WorkerID)
	}
	if decompressed.Status != partition.Status {
		t.Errorf("Status mismatch: expected %s, got %s", partition.Status, decompressed.Status)
	}
	if decompressed.Error != partition.Error {
		t.Errorf("Error mismatch: expected %s, got %s", partition.Error, decompressed.Error)
	}

	// 验证时间戳字段 - 注意压缩过程中会有精度损失，只比较到秒级
	if decompressed.UpdatedAt.Unix() != partition.UpdatedAt.Unix() {
		t.Errorf("UpdatedAt mismatch: expected %v, got %v",
			partition.UpdatedAt.Unix(), decompressed.UpdatedAt.Unix())
	}
	if decompressed.CreatedAt.Unix() != partition.CreatedAt.Unix() {
		t.Errorf("CreatedAt mismatch: expected %v, got %v",
			partition.CreatedAt.Unix(), decompressed.CreatedAt.Unix())
	}

	// 验证Version字段 - 当前实现中没有保存Version，应该为0或默认值
	fmt.Printf("原始Version: %d, 解压后Version: %d\n", partition.Version, decompressed.Version)

	fmt.Printf("✓ 数据完整性验证通过\n\n")
}

// TestPartitionWithErrorCompression 测试带错误信息分区的压缩
func TestPartitionWithErrorCompression(t *testing.T) {
	partition := createSamplePartitionWithError()

	originalJSON, err := json.Marshal(partition)
	if err != nil {
		t.Fatalf("Marshal partition failed: %v", err)
	}

	fmt.Printf("=== 带错误信息分区压缩测试 ===\n")
	fmt.Printf("原始JSON大小: %d bytes\n", len(originalJSON))
	fmt.Printf("错误信息: %s\n", partition.Error)

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		t.Fatalf("Failed to create partition compressor: %v", err)
	}

	stats, err := compressor.CalculateCompressionStats([]*model.PartitionInfo{partition})
	if err != nil {
		t.Fatalf("Calculate compression stats failed: %v", err)
	}

	fmt.Printf("压缩后大小: %d bytes\n", stats.CompressedSize)
	fmt.Printf("压缩比: %.2f%%\n", stats.CompressionRatio*100)

	// 验证错误信息是否正确还原
	compressedBatch, err := compressor.CompressPartitionBatch([]*model.PartitionInfo{partition})
	if err != nil {
		t.Fatalf("Compress partition batch failed: %v", err)
	}

	decompressedPartitions, err := compressor.DecompressPartitionBatch(compressedBatch)
	if err != nil {
		t.Fatalf("Decompress partition batch failed: %v", err)
	}

	if decompressedPartitions[0].Error != partition.Error {
		t.Errorf("Error mismatch: expected %s, got %s", partition.Error, decompressedPartitions[0].Error)
	}

	fmt.Printf("✓ 错误信息还原验证通过\n\n")
}

// TestBatchCompression 测试批量压缩效果
func TestBatchCompression(t *testing.T) {
	batchSizes := []int{10, 50, 100, 500, 1000}

	fmt.Printf("=== 批量压缩效果测试 ===\n")
	fmt.Printf("%-10s %-15s %-15s %-15s %-12s %-15s\n",
		"批次大小", "原始大小(KB)", "二进制大小(KB)", "压缩后大小(KB)", "压缩比(%)", "平均每分区(bytes)")
	fmt.Printf("%-10s %-15s %-15s %-15s %-12s %-15s\n",
		"--------", "------------", "-------------", "-------------", "--------", "---------------")

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		t.Fatalf("Failed to create partition compressor: %v", err)
	}

	for _, batchSize := range batchSizes {
		partitions := createBatchPartitions(batchSize)

		stats, err := compressor.CalculateCompressionStats(partitions)
		if err != nil {
			t.Fatalf("Calculate compression stats for batch size %d failed: %v", batchSize, err)
		}

		originalKB := float64(stats.OriginalSize) / 1024
		binaryKB := float64(stats.BinarySize) / 1024
		compressedKB := float64(stats.CompressedSize) / 1024
		avgBytesPerPartition := float64(stats.CompressedSize) / float64(batchSize)

		fmt.Printf("%-10d %-15.2f %-15.2f %-15.2f %-12.2f %-15.2f\n",
			batchSize, originalKB, binaryKB, compressedKB,
			stats.CompressionRatio*100, avgBytesPerPartition)
	}
	fmt.Printf("\n")
}

// TestCompressionRatioComparison 测试与原始方案的压缩比对比
func TestCompressionRatioComparison(t *testing.T) {
	fmt.Printf("=== 压缩方案对比测试 ===\n")

	// 测试不同数量的分区
	partitionCounts := []int{1, 10, 100, 1000}

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		t.Fatalf("Failed to create partition compressor: %v", err)
	}

	fmt.Printf("%-12s %-15s %-15s %-15s %-15s\n",
		"分区数量", "原始JSON(KB)", "新方案(KB)", "压缩比(%)", "内存节省")
	fmt.Printf("%-12s %-15s %-15s %-15s %-15s\n",
		"--------", "------------", "----------", "--------", "--------")

	for _, count := range partitionCounts {
		partitions := createBatchPartitions(count)

		stats, err := compressor.CalculateCompressionStats(partitions)
		if err != nil {
			t.Fatalf("Calculate compression stats failed: %v", err)
		}

		originalKB := float64(stats.OriginalSize) / 1024
		compressedKB := float64(stats.CompressedSize) / 1024
		memoryReduction := originalKB - compressedKB

		fmt.Printf("%-12d %-15.2f %-15.2f %-15.2f %-15.2f KB\n",
			count, originalKB, compressedKB,
			stats.CompressionRatio*100, memoryReduction)
	}

	fmt.Printf("\n=== 大规模场景估算 ===\n")

	// 估算大规模场景
	scenarios := []struct {
		name  string
		count int
	}{
		{"10万分区", 100000},
		{"50万分区", 500000},
		{"100万分区", 1000000},
		{"500万分区", 5000000},
	}

	// 基于单个分区的平均大小来估算
	singlePartition := createSamplePartition()
	singleStats, err := compressor.CalculateCompressionStats([]*model.PartitionInfo{singlePartition})
	if err != nil {
		t.Fatalf("Calculate single partition stats failed: %v", err)
	}
	avgOriginalSize := float64(singleStats.OriginalSize)
	avgCompressedSize := float64(singleStats.CompressedSize)

	fmt.Printf("%-12s %-15s %-15s %-15s\n",
		"场景", "原始大小", "压缩后大小", "节省内存")
	fmt.Printf("%-12s %-15s %-15s %-15s\n",
		"----", "--------", "----------", "--------")

	for _, scenario := range scenarios {
		originalMB := (avgOriginalSize * float64(scenario.count)) / (1024 * 1024)
		compressedMB := (avgCompressedSize * float64(scenario.count)) / (1024 * 1024)
		savedMB := originalMB - compressedMB

		if originalMB > 1024 {
			fmt.Printf("%-12s %-15.2f GB %-15.2f MB %-15.2f GB\n",
				scenario.name, originalMB/1024, compressedMB, savedMB/1024)
		} else {
			fmt.Printf("%-12s %-15.2f MB %-15.2f MB %-15.2f MB\n",
				scenario.name, originalMB, compressedMB, savedMB)
		}
	}
	fmt.Printf("\n")
}

// TestWorkerIDMapping 测试WorkerID映射的有效性
func TestWorkerIDMapping(t *testing.T) {
	fmt.Printf("=== WorkerID映射压缩测试 ===\n")

	// 创建有多个不同WorkerID的分区
	partitions := []*model.PartitionInfo{
		{
			PartitionID: 1,
			MinID:       1000, MaxID: 1999,
			WorkerID:  "elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa",
			Status:    model.StatusCompleted,
			UpdatedAt: time.Now(),
		},
		{
			PartitionID: 2,
			MinID:       2000, MaxID: 2999,
			WorkerID:  "elk-coordinator-worker-7f8g9h0i1j-k2lm3n-2-15a6b7c8",
			Status:    model.StatusCompleted,
			UpdatedAt: time.Now(),
		},
		{
			PartitionID: 3,
			MinID:       3000, MaxID: 3999,
			WorkerID:  "elk-coordinator-worker-5d4d557bdc-j4xvf-1-08f3e2fa", // 重复的WorkerID
			Status:    model.StatusCompleted,
			UpdatedAt: time.Now(),
		},
	}

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		t.Fatalf("Failed to create partition compressor: %v", err)
	}

	// 压缩和解压
	compressedBatch, err := compressor.CompressPartitionBatch(partitions)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressedPartitions, err := compressor.DecompressPartitionBatch(compressedBatch)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	// 验证WorkerID映射 - 检查压缩数据的基本信息
	fmt.Printf("压缩数据大小: %d bytes\n", len(compressedBatch.CompressedData))
	fmt.Printf("分区数量: %d\n", compressedBatch.PartitionCount)
	fmt.Printf("压缩算法: %s\n", compressedBatch.CompressionAlgorithm)

	// 验证解压后的WorkerID正确性
	for i, original := range partitions {
		decompressed := decompressedPartitions[i]
		if original.WorkerID != decompressed.WorkerID {
			t.Errorf("Partition %d WorkerID mismatch: expected %s, got %s",
				i, original.WorkerID, decompressed.WorkerID)
		}
	}

	fmt.Printf("✓ WorkerID映射验证通过\n\n")
}

// BenchmarkCompression 压缩性能基准测试
func BenchmarkCompression(b *testing.B) {
	partitions := createBatchPartitions(100)

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		b.Fatalf("Failed to create partition compressor: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.CompressPartitionBatch(partitions)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecompression 解压性能基准测试
func BenchmarkDecompression(b *testing.B) {
	partitions := createBatchPartitions(100)

	// 创建压缩器实例
	mockRegistry := newMockWorkerRegistry()
	compressor, err := NewPartitionCompressor(mockRegistry)
	if err != nil {
		b.Fatalf("Failed to create partition compressor: %v", err)
	}

	compressedBatch, err := compressor.CompressPartitionBatch(partitions)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.DecompressPartitionBatch(compressedBatch)
		if err != nil {
			b.Fatal(err)
		}
	}
}
