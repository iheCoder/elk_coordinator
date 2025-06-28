package partition

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// WorkerIDManager 管理WorkerID到数字ID的映射
type WorkerIDManager struct {
	mapping map[string]uint16 // workerID -> 压缩ID
	reverse map[uint16]string // 压缩ID -> workerID
	nextID  uint16            // 下一个可用ID
	mutex   sync.RWMutex      // 并发安全
}

// NewWorkerIDManager 创建新的WorkerID管理器
func NewWorkerIDManager() *WorkerIDManager {
	return &WorkerIDManager{
		mapping: make(map[string]uint16),
		reverse: make(map[uint16]string),
		nextID:  1, // 从1开始，0保留给空值
	}
}

// GetOrCreateMapping 获取或创建WorkerID映射
func (wm *WorkerIDManager) GetOrCreateMapping(workerID string) uint16 {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	// 如果已存在映射，直接返回
	if id, exists := wm.mapping[workerID]; exists {
		return id
	}

	// 创建新映射
	newID := wm.nextID
	wm.nextID++
	wm.mapping[workerID] = newID
	wm.reverse[newID] = workerID

	return newID
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
	BaseTimestamp        int64           `json:"base_timestamp"`        // 基准时间戳
	CompressedData       []byte          `json:"compressed_data"`       // gzip压缩的批量分区数据
	PartitionCount       int             `json:"partition_count"`       // 批次中的分区数量
	CompressionAlgorithm string          `json:"compression_algorithm"` // 压缩算法
	Version              int             `json:"version"`               // 压缩格式版本号
	WorkerMappings       []WorkerMapping `json:"worker_mappings"`       // 批次使用的WorkerID映射
}

// WorkerMapping WorkerID映射项
type WorkerMapping struct {
	CompressedID uint16 `json:"compressed_id"`
	WorkerID     string `json:"worker_id"`
}

// PartitionEncoder 分区编码器
type PartitionEncoder struct {
	buffer    *bytes.Buffer
	baseTime  int64
	workerMgr *WorkerIDManager
}

// NewPartitionEncoder 创建新的分区编码器
func NewPartitionEncoder(baseTime int64, workerMgr *WorkerIDManager) *PartitionEncoder {
	return &PartitionEncoder{
		buffer:    &bytes.Buffer{},
		baseTime:  baseTime,
		workerMgr: workerMgr,
	}
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
	workerCompressed := pe.workerMgr.GetOrCreateMapping(p.WorkerID)
	if err := binary.Write(pe.buffer, binary.LittleEndian, workerCompressed); err != nil {
		return err
	}

	// 状态编码
	statusCode := statusToCode(p.Status)
	if err := binary.Write(pe.buffer, binary.LittleEndian, statusCode); err != nil {
		return err
	}

	// 只保留UpdatedAt时间戳
	updatedAt := encodeRelativeTimestamp(p.UpdatedAt, pe.baseTime)
	if err := binary.Write(pe.buffer, binary.LittleEndian, updatedAt); err != nil {
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
func calculateBaseTimestamp(partitions []*model.PartitionInfo) int64 {
	if len(partitions) == 0 {
		return time.Now().Unix()
	}

	// 选择批次中最早的UpdatedAt作为基准
	minTime := partitions[0].UpdatedAt.Unix()
	for _, p := range partitions {
		if p.UpdatedAt.Unix() < minTime {
			minTime = p.UpdatedAt.Unix()
		}
	}
	return minTime
}

// CompressPartitionBatch 压缩分区批次
func CompressPartitionBatch(partitions []*model.PartitionInfo, workerMgr *WorkerIDManager) (*CompressedBatch, error) {
	if len(partitions) == 0 {
		return nil, fmt.Errorf("empty partition list")
	}

	// 1. 计算基准时间戳
	baseTime := calculateBaseTimestamp(partitions)

	// 2. 初始化编码器
	encoder := NewPartitionEncoder(baseTime, workerMgr)

	// 3. 逐个编码分区
	for _, partition := range partitions {
		if err := encoder.EncodePartition(partition); err != nil {
			return nil, fmt.Errorf("encode partition %d failed: %w",
				partition.PartitionID, err)
		}
	}

	// 4. gzip压缩
	var compressedBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuf)
	if _, err := gzipWriter.Write(encoder.buffer.Bytes()); err != nil {
		return nil, fmt.Errorf("gzip compression failed: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("gzip close failed: %w", err)
	}

	// 5. 收集WorkerID映射
	workerMappings := extractWorkerMappings(workerMgr, partitions)

	// 6. 构建压缩批次
	batch := &CompressedBatch{
		BaseTimestamp:        baseTime,
		CompressedData:       compressedBuf.Bytes(),
		PartitionCount:       len(partitions),
		CompressionAlgorithm: "gzip",
		Version:              1,
		WorkerMappings:       workerMappings,
	}

	return batch, nil
}

// extractWorkerMappings 提取批次中使用的WorkerID映射
func extractWorkerMappings(workerMgr *WorkerIDManager, partitions []*model.PartitionInfo) []WorkerMapping {
	usedWorkers := make(map[string]bool)
	for _, p := range partitions {
		usedWorkers[p.WorkerID] = true
	}

	var mappings []WorkerMapping
	for workerID := range usedWorkers {
		compressedID := workerMgr.GetOrCreateMapping(workerID)
		mappings = append(mappings, WorkerMapping{
			CompressedID: compressedID,
			WorkerID:     workerID,
		})
	}

	return mappings
}

// DecompressPartitionBatch 解压分区批次
func DecompressPartitionBatch(batch *CompressedBatch) ([]*model.PartitionInfo, error) {
	// 1. 重建WorkerID映射
	workerMgr := NewWorkerIDManager()
	for _, mapping := range batch.WorkerMappings {
		workerMgr.mapping[mapping.WorkerID] = mapping.CompressedID
		workerMgr.reverse[mapping.CompressedID] = mapping.WorkerID
		if mapping.CompressedID >= workerMgr.nextID {
			workerMgr.nextID = mapping.CompressedID + 1
		}
	}

	// 2. gzip解压
	gzipReader, err := gzip.NewReader(bytes.NewReader(batch.CompressedData))
	if err != nil {
		return nil, fmt.Errorf("gzip decompression failed: %w", err)
	}
	defer gzipReader.Close()

	decompressedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("read decompressed data failed: %w", err)
	}

	// 3. 二进制解码
	partitions := make([]*model.PartitionInfo, 0, batch.PartitionCount)
	reader := bytes.NewReader(decompressedData)

	for i := 0; i < batch.PartitionCount; i++ {
		partition, err := decodePartition(reader, batch.BaseTimestamp, workerMgr)
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

	// 设置基本字段
	partition.PartitionID = int(partitionID)
	partition.MinID = int64(minID)
	partition.MaxID = int64(minID) + int64(dataRange) - 1 // 从MinID和范围计算MaxID
	partition.WorkerID = workerMgr.GetWorkerID(workerCompressed)
	partition.Status = codeToStatus(statusCode)

	// 还原时间戳
	partition.UpdatedAt = time.Unix(baseTime+int64(updatedAt), 0)
	partition.CreatedAt = partition.UpdatedAt // 使用UpdatedAt作为CreatedAt的近似值
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
func CalculateCompressionStats(partitions []*model.PartitionInfo) (*CompressionStats, error) {
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

	// 2. 压缩并计算大小
	workerMgr := NewWorkerIDManager()
	compressedBatch, err := CompressPartitionBatch(partitions, workerMgr)
	if err != nil {
		return nil, fmt.Errorf("compress batch failed: %w", err)
	}

	// 3. 计算二进制编码大小（未压缩前）
	baseTime := calculateBaseTimestamp(partitions)
	encoder := NewPartitionEncoder(baseTime, workerMgr)
	for _, partition := range partitions {
		if err := encoder.EncodePartition(partition); err != nil {
			return nil, fmt.Errorf("encode partition for stats failed: %w", err)
		}
	}
	binarySize := int64(encoder.buffer.Len())

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
