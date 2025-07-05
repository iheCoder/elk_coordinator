package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/partition"
	"github.com/redis/go-redis/v9"
)

const (
	PARTITION_COUNT = 50000
)

func main() {
	// 连接到本地Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// 测试连接
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("无法连接到Redis: %v\n请确保Redis服务正在运行 (localhost:6379)", err)
	}

	fmt.Printf("=== Redis内存使用量比较测试 ===\n")
	fmt.Printf("分区数量: %d\n", PARTITION_COUNT)
	fmt.Printf("开始时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	// 清理所有测试键
	cleanupTestKeys(ctx, redisClient)

	// 预生成统一的分区数据集，确保两个测试使用完全相同的数据
	fmt.Println("🔄 预生成分区数据集...")
	partitions := generatePartitionDataset()
	fmt.Printf("✓ 已生成 %d 个分区数据\n\n", len(partitions))

	// 测试1: 未压缩存储
	fmt.Println("==== 测试1: 未压缩JSON存储 ====")
	uncompressedSize := testUncompressedStorage(ctx, redisClient, partitions)
	time.Sleep(1 * time.Second)
	uncompressedMemory := getTestKeysMemoryUsage(ctx, redisClient, "test_uncompressed:")
	cleanupTestKeys(ctx, redisClient)
	runtime.GC()
	time.Sleep(1 * time.Second)

	// 测试2: 压缩存储
	fmt.Println("\n==== 测试2: 压缩存储 ====")
	// 先注册所有WorkerID以支持压缩
	registerWorkersForCompression(ctx, redisClient, partitions)
	compressedSize := testCompressedStorage(ctx, redisClient, partitions)
	time.Sleep(1 * time.Second)
	compressedMemory := getTestKeysMemoryUsage(ctx, redisClient, "test_compressed:")
	cleanupTestKeys(ctx, redisClient)

	// 输出结果
	printResults(uncompressedSize, compressedSize, uncompressedMemory, compressedMemory)
}

func testUncompressedStorage(ctx context.Context, redisClient *redis.Client, partitions []*model.PartitionInfo) int64 {
	opts := data.DefaultOptions()
	opts.KeyPrefix = "test_uncompressed:"
	store := data.NewRedisDataStore(redisClient, opts)

	fmt.Printf("存储 %d 个分区（未压缩JSON格式）...\n", len(partitions))

	var totalSize int64
	partitionsKey := "elk_active_partitions"

	for i, partition := range partitions {
		jsonData, err := json.Marshal(partition)
		if err != nil {
			log.Fatalf("序列化分区失败: %v", err)
		}

		err = store.HSetPartition(ctx, partitionsKey, strconv.Itoa(partition.PartitionID), string(jsonData))
		if err != nil {
			log.Fatalf("存储分区失败: %v", err)
		}

		totalSize += int64(len(jsonData))

		if (i+1)%10000 == 0 {
			fmt.Printf("已存储 %d 个分区\n", i+1)
		}
	}

	fmt.Printf("✓ 未压缩存储完成，总数据大小: %.2f MB\n", float64(totalSize)/1024/1024)
	return totalSize
}

func testCompressedStorage(ctx context.Context, redisClient *redis.Client, partitions []*model.PartitionInfo) int64 {
	opts := data.DefaultOptions()
	opts.KeyPrefix = "test_compressed:"
	store := data.NewRedisDataStore(redisClient, opts)

	compressor, err := partition.NewPartitionCompressor(store)
	if err != nil {
		log.Fatalf("创建压缩器失败: %v", err)
	}

	fmt.Printf("存储 %d 个分区（压缩格式）...\n", len(partitions))

	var totalSize int64
	compressedKey := "elk_compressed_archive"

	// 一次性压缩所有分区
	fmt.Printf("正在压缩所有 %d 个分区...\n", len(partitions))
	compressedBatch, err := compressor.CompressPartitionBatch(partitions)
	if err != nil {
		log.Fatalf("压缩所有分区失败: %v", err)
	}

	// 序列化压缩批次
	batchJson, err := json.Marshal(compressedBatch)
	if err != nil {
		log.Fatalf("序列化压缩批次失败: %v", err)
	}

	// 存储到Redis
	batchKey := fmt.Sprintf("batch_all_%d", len(partitions))
	err = store.HSetPartition(ctx, compressedKey, batchKey, string(batchJson))
	if err != nil {
		log.Fatalf("存储压缩批次失败: %v", err)
	}

	totalSize = int64(len(batchJson))

	fmt.Printf("✓ 压缩存储完成，总数据大小: %.2f MB (1个批次)\n", float64(totalSize)/1024/1024)
	return totalSize
}

// generatePartitionDataset 预生成统一的分区数据集，确保两次测试使用相同数据
func generatePartitionDataset() []*model.PartitionInfo {
	partitions := make([]*model.PartitionInfo, PARTITION_COUNT)

	// 生成50种不同的WorkerID模式，更符合实际情况
	workerIDTemplates := make([]string, 50)
	for i := 0; i < 50; i++ {
		switch i % 5 {
		case 0:
			workerIDTemplates[i] = fmt.Sprintf("elk-coordinator-worker-5d4d557bdc-j4xvf-%d-08f3e2fa", i+1)
		case 1:
			workerIDTemplates[i] = fmt.Sprintf("elk-worker-deployment-%d-abc123def", i+1)
		case 2:
			workerIDTemplates[i] = fmt.Sprintf("coordinator-pod-%d-xyz789", i+1)
		case 3:
			workerIDTemplates[i] = fmt.Sprintf("worker-node-%d-mno456pqr", i+1)
		case 4:
			workerIDTemplates[i] = fmt.Sprintf("elk-processor-%d-stu123vwx", i+1)
		}
	}

	baseTime := time.Now()

	for i := 0; i < PARTITION_COUNT; i++ {
		partitionID := i + 1

		// 99.5%的分区为StatusCompleted，0.5%为失败状态（更符合实际情况）
		var status model.PartitionStatus
		var errorMsg string

		if partitionID%200 == 0 { // 0.5%失败率
			status = model.StatusFailed
			errorMsg = fmt.Sprintf("网络超时：分区 %d 处理失败", partitionID)
		} else {
			status = model.StatusCompleted
			errorMsg = ""
		}

		partitions[i] = &model.PartitionInfo{
			PartitionID:   partitionID,
			MinID:         int64((partitionID-1)*1000 + 1),
			MaxID:         int64(partitionID * 1000),
			WorkerID:      workerIDTemplates[partitionID%50], // 使用50种不同的WorkerID
			LastHeartbeat: time.Time{},                       // 完成的分区通常没有心跳
			Status:        status,
			UpdatedAt:     baseTime.Add(-time.Duration(partitionID%86400) * time.Second),
			Version:       int64(partitionID%5 + 1), // 版本1-5
			Error:         errorMsg,
			CreatedAt:     baseTime.Add(-time.Duration(partitionID) * time.Minute),
		}
	}

	return partitions
}

// getTestKeysMemoryUsage 获取测试相关key的内存使用量（字节）
func getTestKeysMemoryUsage(ctx context.Context, redisClient *redis.Client, keyPrefix string) int64 {
	// 获取所有测试相关的key
	pattern := keyPrefix + "*"
	keys, err := redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("获取key列表失败: %v", err)
		return 0
	}

	var totalMemory int64
	for _, key := range keys {
		// 使用MEMORY USAGE命令获取每个key的内存使用量
		memUsage, err := redisClient.MemoryUsage(ctx, key).Result()
		if err != nil {
			log.Printf("获取key %s 内存使用失败: %v", key, err)
			continue
		}
		totalMemory += memUsage
	}

	return totalMemory
}

func cleanupTestKeys(ctx context.Context, redisClient *redis.Client) {
	patterns := []string{
		"test_uncompressed:*",
		"test_compressed:*",
	}

	for _, pattern := range patterns {
		keys, err := redisClient.Keys(ctx, pattern).Result()
		if err != nil {
			continue
		}

		if len(keys) > 0 {
			redisClient.Del(ctx, keys...)
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
}

func printResults(uncompressedSize, compressedSize, uncompressedMemory, compressedMemory int64) {
	fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Printf("           详细性能比较结果\n")
	fmt.Printf(strings.Repeat("=", 60) + "\n")

	compressionRatio := float64(uncompressedSize) / float64(compressedSize)
	spaceSavingPercent := (1.0 - float64(compressedSize)/float64(uncompressedSize)) * 100

	uncompressedMB := float64(uncompressedSize) / 1024 / 1024
	compressedMB := float64(compressedSize) / 1024 / 1024

	fmt.Printf("\n📊 数据大小对比:\n")
	fmt.Printf("• 未压缩JSON存储: %.2f MB (平均 %.1f bytes/分区)\n",
		uncompressedMB, float64(uncompressedSize)/PARTITION_COUNT)
	fmt.Printf("• 压缩存储:       %.2f MB (平均 %.1f bytes/分区)\n",
		compressedMB, float64(compressedSize)/PARTITION_COUNT)
	fmt.Printf("• 压缩比:         %.1f:1\n", compressionRatio)

	fmt.Printf("\n💾 空间节省:\n")
	fmt.Printf("• 节省空间: %.2f MB\n", uncompressedMB-compressedMB)
	fmt.Printf("• 节省比例: %.1f%%\n", spaceSavingPercent)

	fmt.Printf("\n🗄️  Redis实际内存使用:\n")
	uncompressedMemoryMB := float64(uncompressedMemory) / 1024 / 1024
	compressedMemoryMB := float64(compressedMemory) / 1024 / 1024
	memorySpaceSavingPercent := (1.0 - float64(compressedMemory)/float64(uncompressedMemory)) * 100

	fmt.Printf("• 未压缩方案: %.2f MB\n", uncompressedMemoryMB)
	fmt.Printf("• 压缩方案:   %.2f MB\n", compressedMemoryMB)
	fmt.Printf("• 内存节省:   %.2f MB (%.1f%%)\n", uncompressedMemoryMB-compressedMemoryMB, memorySpaceSavingPercent)

	fmt.Printf("\n📈 扩展分析 - 不同规模的预估内存使用:\n")
	fmt.Printf("┌─────────────┬─────────────┬─────────────┬─────────────┐\n")
	fmt.Printf("│   分区数量  │  未压缩内存 │  压缩内存   │   节省内存  │\n")
	fmt.Printf("├─────────────┼─────────────┼─────────────┼─────────────┤\n")

	avgUncompressed := float64(uncompressedSize) / PARTITION_COUNT
	avgCompressed := float64(compressedSize) / PARTITION_COUNT

	partitionCounts := []int{100000, 500000, 1000000, 5000000, 10000000}
	for _, count := range partitionCounts {
		uncompressedProj := float64(count) * avgUncompressed / 1024 / 1024
		compressedProj := float64(count) * avgCompressed / 1024 / 1024
		saved := uncompressedProj - compressedProj

		fmt.Printf("│ %7s     │ %8.1f MB │ %8.1f MB │ %8.1f MB │\n",
			formatNumber(count), uncompressedProj, compressedProj, saved)
	}
	fmt.Printf("└─────────────┴─────────────┴─────────────┴─────────────┘\n")

	fmt.Printf("\n🎯 总结:\n")
	fmt.Printf("✓ 压缩方案节省 %.1f%% 存储空间，压缩比 %.1f:1\n", spaceSavingPercent, compressionRatio)
	fmt.Printf("✓ Redis内存实际节省 %.1f%% (%.2f MB)\n", memorySpaceSavingPercent, uncompressedMemoryMB-compressedMemoryMB)
	fmt.Printf("✓ 对于大规模分区场景，内存节省效果显著\n")
	fmt.Printf("✓ 建议在完成分区超过10万时使用压缩存储\n")
}

func formatNumber(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

// registerWorkersForCompression 注册所有唯一的WorkerID以支持压缩映射
func registerWorkersForCompression(ctx context.Context, redisClient *redis.Client, partitions []*model.PartitionInfo) {
	opts := data.DefaultOptions()
	opts.KeyPrefix = "test_compressed:"
	store := data.NewRedisDataStore(redisClient, opts)

	// 收集所有唯一的WorkerID
	workerIDSet := make(map[string]bool)
	for _, partition := range partitions {
		workerIDSet[partition.WorkerID] = true
	}

	fmt.Printf("🔧 注册 %d 个唯一WorkerID以支持压缩映射...\n", len(workerIDSet))

	// 注册所有WorkerID
	for workerID := range workerIDSet {
		err := store.RegisterWorker(ctx, workerID)
		if err != nil {
			log.Printf("注册WorkerID %s 失败: %v", workerID, err)
		}
	}

	fmt.Printf("✓ WorkerID注册完成\n")
}
