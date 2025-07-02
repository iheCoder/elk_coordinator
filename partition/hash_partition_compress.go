package partition

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/iheCoder/elk_coordinator/data"
	"github.com/iheCoder/elk_coordinator/model"
)

// WorkerIDManager 管理WorkerID到数字ID的映射
type WorkerIDManager struct {
	mapping map[string]uint16 // workerID -> 压缩ID
	reverse map[uint16]string // 压缩ID -> workerID
	rds     data.WorkerRegistry
	mutex   sync.RWMutex // 并发安全
}

// NewWorkerIDManager 创建新的WorkerID管理器，立即从Redis构建缓存
func NewWorkerIDManager(rds data.WorkerRegistry) (*WorkerIDManager, error) {
	wm := &WorkerIDManager{
		mapping: make(map[string]uint16),
		reverse: make(map[uint16]string),
		rds:     rds,
	}

	// 立即构建缓存
	if err := wm.rebuildCacheFromRedis(); err != nil {
		return nil, err
	}

	return wm, nil
}

// GetMapping 获取WorkerID映射，找不到则重建缓存后再查找，仍找不到返回错误
func (wm *WorkerIDManager) GetMapping(workerID string) (uint16, error) {
	wm.mutex.RLock()
	// 如果已存在映射，直接返回
	if id, exists := wm.mapping[workerID]; exists {
		wm.mutex.RUnlock()
		return id, nil
	}
	wm.mutex.RUnlock()

	// 如果不存在，重建缓存后再查找
	if wm.rds != nil {
		if err := wm.rebuildCacheFromRedis(); err != nil {
			return 0, fmt.Errorf("failed to rebuild cache for worker %s: %w", workerID, err)
		}
		wm.mutex.RLock()
		if id, exists := wm.mapping[workerID]; exists {
			wm.mutex.RUnlock()
			return id, nil
		}
		wm.mutex.RUnlock()
	}

	// 找不到返回错误
	return 0, fmt.Errorf("%w: worker ID %s", ErrWorkerMappingNotFound, workerID)
}

// rebuildCacheFromRedis 从Redis重建缓存映射
func (wm *WorkerIDManager) rebuildCacheFromRedis() error {
	ctx := context.Background()
	allWorkers, err := wm.rds.GetAllWorkers(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerCacheRebuildFailed, err)
	}

	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	// 清空现有映射
	wm.mapping = make(map[string]uint16)
	wm.reverse = make(map[uint16]string)

	// 按score顺序构建映射
	for i, worker := range allWorkers {
		compressedID := uint16(i + 1) // 从1开始编号
		wm.mapping[worker.WorkerID] = compressedID
		wm.reverse[compressedID] = worker.WorkerID
	}

	return nil
}

// GetWorkerID 通过压缩ID获取原始WorkerID
func (wm *WorkerIDManager) GetWorkerID(compressedID uint16) string {
	wm.mutex.RLock()
	defer wm.mutex.RUnlock()

	if workerID, exists := wm.reverse[compressedID]; exists {
		return workerID
	}
	return fmt.Sprintf("unknown_worker_%d", compressedID)
}

// 状态编码映射
var statusCodeMapping = map[model.PartitionStatus]uint8{
	model.StatusPending:   0,
	model.StatusClaimed:   1,
	model.StatusRunning:   2,
	model.StatusCompleted: 3,
	model.StatusFailed:    4,
}

var codeStatusMapping = map[uint8]model.PartitionStatus{
	0: model.StatusPending,
	1: model.StatusClaimed,
	2: model.StatusRunning,
	3: model.StatusCompleted,
	4: model.StatusFailed,
}

func statusToCode(status model.PartitionStatus) uint8 {
	if code, exists := statusCodeMapping[status]; exists {
		return code
	}
	return 0 // 默认为pending
}

func codeToStatus(code uint8) model.PartitionStatus {
	if status, exists := codeStatusMapping[code]; exists {
		return status
	}
	return model.StatusPending // 默认为pending
}

// CompressedBatch 压缩批次结构
type CompressedBatch struct {
	BaseTimestamp        int64  `json:"base_timestamp"`        // 基准时间戳
	CompressedData       []byte `json:"compressed_data"`       // gzip压缩的批量分区数据
	PartitionCount       int    `json:"partition_count"`       // 批次中的分区数量
	CompressionAlgorithm string `json:"compression_algorithm"` // 压缩算法
	Version              int    `json:"version"`               // 压缩格式版本号
}

// PartitionEncoder 分区编码器
type PartitionEncoder struct {
	buffer    *bytes.Buffer
	baseTime  int64
	workerMgr *WorkerIDManager
}

// PartitionCompressor 分区压缩器
type PartitionCompressor struct {
	workerMgr *WorkerIDManager
	encoder   *PartitionEncoder // 复用编码器，提升性能
}

// NewPartitionCompressor 创建新的分区压缩器，传入redis以构建WorkerID缓存
func NewPartitionCompressor(rds data.WorkerRegistry) (*PartitionCompressor, error) {
	workerMgr, err := NewWorkerIDManager(rds)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker ID manager: %w", err)
	}

	return &PartitionCompressor{
		workerMgr: workerMgr,
		encoder:   NewPartitionEncoder(0, workerMgr), // 初始化时baseTime为0，使用时会reset
	}, nil
}

// NewPartitionEncoder 创建新的分区编码器
func NewPartitionEncoder(baseTime int64, workerMgr *WorkerIDManager) *PartitionEncoder {
	return &PartitionEncoder{
		buffer:    &bytes.Buffer{},
		baseTime:  baseTime,
		workerMgr: workerMgr,
	}
}

// Reset 重置编码器状态以复用
func (pe *PartitionEncoder) Reset(baseTime int64) {
	pe.buffer.Reset()
	pe.baseTime = baseTime
}

// Bytes 返回编码后的字节数据
func (pe *PartitionEncoder) Bytes() []byte {
	return pe.buffer.Bytes()
}

// EncodePartition 编码单个分区
func (pe *PartitionEncoder) EncodePartition(p *model.PartitionInfo) error {
	// 固定长度字段编码
	if err := binary.Write(pe.buffer, binary.LittleEndian, uint32(p.PartitionID)); err != nil {
		return err
	}
	if err := binary.Write(pe.buffer, binary.LittleEndian, uint64(p.MinID)); err != nil {
		return err
	}

	// 计算并编码数据范围大小
	dataRange := uint32(p.MaxID - p.MinID + 1)
	if err := binary.Write(pe.buffer, binary.LittleEndian, dataRange); err != nil {
		return err
	}

	// WorkerID压缩编码
	workerCompressed, err := pe.workerMgr.GetMapping(p.WorkerID)
	if err != nil {
		return fmt.Errorf("failed to get worker mapping for %s: %w", p.WorkerID, err)
	}
	if err := binary.Write(pe.buffer, binary.LittleEndian, workerCompressed); err != nil {
		return err
	}

	// 状态编码
	statusCode := statusToCode(p.Status)
	if err := binary.Write(pe.buffer, binary.LittleEndian, statusCode); err != nil {
		return err
	}

	// 编码UpdatedAt和CreatedAt时间戳
	updatedAt := encodeRelativeTimestamp(p.UpdatedAt, pe.baseTime)
	if err := binary.Write(pe.buffer, binary.LittleEndian, updatedAt); err != nil {
		return err
	}

	createdAt := encodeRelativeTimestamp(p.CreatedAt, pe.baseTime)
	if err := binary.Write(pe.buffer, binary.LittleEndian, createdAt); err != nil {
		return err
	}

	// 错误信息变长编码
	errorBytes := []byte(p.Error)
	if err := binary.Write(pe.buffer, binary.LittleEndian, uint16(len(errorBytes))); err != nil {
		return err
	}
	if len(errorBytes) > 0 {
		if _, err := pe.buffer.Write(errorBytes); err != nil {
			return err
		}
	}

	return nil
}

// encodeRelativeTimestamp 编码相对时间戳
func encodeRelativeTimestamp(timestamp time.Time, baseTime int64) uint32 {
	relative := timestamp.Unix() - baseTime
	if relative < 0 || relative > math.MaxUint32 {
		// 处理边界情况，记录警告日志
		return 0
	}
	return uint32(relative)
}

// calculateBaseTimestamp 计算基准时间戳
// 使用批次中最早的CreatedAt作为基准，这样可以获得更好的压缩效果
// 因为CreatedAt通常比UpdatedAt更早且更集中
func calculateBaseTimestamp(partitions []*model.PartitionInfo) int64 {
	if len(partitions) == 0 {
		return time.Now().Unix()
	}

	// 选择批次中最早的CreatedAt作为基准
	minTime := partitions[0].CreatedAt.Unix()
	for _, p := range partitions {
		if p.CreatedAt.Unix() < minTime {
			minTime = p.CreatedAt.Unix()
		}
	}
	return minTime
}

// CompressPartitionBatch 压缩分区批次
func (c *PartitionCompressor) CompressPartitionBatch(partitions []*model.PartitionInfo) (*CompressedBatch, error) {
	if len(partitions) == 0 {
		return nil, fmt.Errorf("empty partition list")
	}

	// 1. 计算基准时间戳
	baseTime := calculateBaseTimestamp(partitions)

	// 2. 重置复用编码器
	c.encoder.Reset(baseTime)

	// 3. 逐个编码分区
	for _, partition := range partitions {
		if err := c.encoder.EncodePartition(partition); err != nil {
			return nil, fmt.Errorf("encode partition %d failed: %w",
				partition.PartitionID, err)
		}
	}

	// 5. gzip压缩
	var compressedBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuf)
	if _, err := gzipWriter.Write(c.encoder.Bytes()); err != nil {
		return nil, fmt.Errorf("gzip compression failed: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("gzip close failed: %w", err)
	}

	// 6. 构建压缩批次
	batch := &CompressedBatch{
		BaseTimestamp:        baseTime,
		CompressedData:       compressedBuf.Bytes(),
		PartitionCount:       len(partitions),
		CompressionAlgorithm: "gzip",
		Version:              1,
	}

	return batch, nil
}

// DecompressPartitionBatch 解压分区批次
func (c *PartitionCompressor) DecompressPartitionBatch(batch *CompressedBatch) ([]*model.PartitionInfo, error) {
	// 1. gzip解压
	gzipReader, err := gzip.NewReader(bytes.NewReader(batch.CompressedData))
	if err != nil {
		return nil, fmt.Errorf("gzip decompression failed: %w", err)
	}
	defer gzipReader.Close()

	decompressedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("read decompressed data failed: %w", err)
	}

	// 2. 二进制解码
	partitions := make([]*model.PartitionInfo, 0, batch.PartitionCount)
	reader := bytes.NewReader(decompressedData)

	for i := 0; i < batch.PartitionCount; i++ {
		partition, err := decodePartition(reader, batch.BaseTimestamp, c.workerMgr)
		if err != nil {
			return nil, fmt.Errorf("decode partition %d failed: %w", i, err)
		}
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// decodePartition 解码单个分区
func decodePartition(reader *bytes.Reader, baseTime int64, workerMgr *WorkerIDManager) (*model.PartitionInfo, error) {
	partition := &model.PartitionInfo{}

	// 解码固定长度字段
	var partitionID uint32
	var minID uint64
	var dataRange uint32
	var workerCompressed uint16
	var statusCode uint8
	var updatedAt uint32
	var createdAt uint32

	if err := binary.Read(reader, binary.LittleEndian, &partitionID); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &minID); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &dataRange); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &workerCompressed); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &statusCode); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &updatedAt); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &createdAt); err != nil {
		return nil, err
	}

	// 设置基本字段
	partition.PartitionID = int(partitionID)
	partition.MinID = int64(minID)
	partition.MaxID = int64(minID) + int64(dataRange) - 1 // 从MinID和范围计算MaxID
	partition.WorkerID = workerMgr.GetWorkerID(workerCompressed)
	partition.Status = codeToStatus(statusCode)

	// 还原时间戳
	partition.UpdatedAt = time.Unix(baseTime+int64(updatedAt), 0)
	partition.CreatedAt = time.Unix(baseTime+int64(createdAt), 0)
	// LastHeartbeat对completed分区无意义，设为零值

	// 解码错误信息
	var errorLength uint16
	if err := binary.Read(reader, binary.LittleEndian, &errorLength); err != nil {
		return nil, err
	}
	if errorLength > 0 {
		errorBytes := make([]byte, errorLength)
		if _, err := reader.Read(errorBytes); err != nil {
			return nil, err
		}
		partition.Error = string(errorBytes)
	}

	return partition, nil
}

// CalculateCompressionStats 计算压缩统计信息
type CompressionStats struct {
	OriginalSize     int64   `json:"original_size"`     // 原始JSON大小
	BinarySize       int64   `json:"binary_size"`       // 二进制编码大小
	CompressedSize   int64   `json:"compressed_size"`   // 压缩后大小
	CompressionRatio float64 `json:"compression_ratio"` // 压缩比
	PartitionCount   int     `json:"partition_count"`   // 分区数量
}

// CalculateCompressionStats 计算压缩统计信息
func (c *PartitionCompressor) CalculateCompressionStats(partitions []*model.PartitionInfo) (*CompressionStats, error) {
	if len(partitions) == 0 {
		return nil, fmt.Errorf("empty partition list")
	}

	// 1. 计算原始JSON大小
	originalSize := int64(0)
	for _, p := range partitions {
		jsonData, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("marshal partition %d failed: %w", p.PartitionID, err)
		}
		originalSize += int64(len(jsonData))
	}

	// 2. 压缩并计算大小 (重用已有方法，避免重复逻辑)
	compressedBatch, err := c.CompressPartitionBatch(partitions)
	if err != nil {
		return nil, fmt.Errorf("compress batch failed: %w", err)
	}

	// 3. 计算二进制编码大小（复用编码器状态）
	binarySize := int64(c.encoder.buffer.Len())
	compressedSize := int64(len(compressedBatch.CompressedData))
	compressionRatio := 1.0 - float64(compressedSize)/float64(originalSize)

	return &CompressionStats{
		OriginalSize:     originalSize,
		BinarySize:       binarySize,
		CompressedSize:   compressedSize,
		CompressionRatio: compressionRatio,
		PartitionCount:   len(partitions),
	}, nil
}

// 使用示例：
// 1. 创建压缩器（会自动从Redis构建WorkerID缓存）
// compresser, err := NewPartitionCompresser(redisDataStore)
// if err != nil {
//     return fmt.Errorf("failed to create compresser: %w", err)
// }
//
// 2. 压缩分区 (所有WorkerID映射错误都会返回)
// batch, err := compresser.CompressPartitionBatch(partitions)
// if err != nil {
//     return fmt.Errorf("failed to compress partitions: %w", err)
// }
//
// 3. 解压分区 (使用同一个压缩器，映射已存在)
// decompressedPartitions, err := compresser.DecompressPartitionBatch(batch)
// if err != nil {
//     return fmt.Errorf("failed to decompress partitions: %w", err)
// }
//
// 4. 计算压缩统计
// stats, err := compresser.CalculateCompressionStats(partitions)
// if err != nil {
//     return fmt.Errorf("failed to calculate compression stats: %w", err)
// }
