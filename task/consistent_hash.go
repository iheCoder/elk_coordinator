// consistent_hash.go 任务级别的一致性hash实现
package task

import (
	"context"
	"crypto/md5"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// TaskConsistentHash 任务级别的一致性hash实现
// 用于在获取pending分区时减少工作节点之间的冲突
type TaskConsistentHash struct {
	// 配置
	namespace string
	workerID  string
	logger    utils.Logger

	// 数据存储接口，用于获取活跃工作节点
	dataStore interface {
		data.HeartbeatOperations
		data.KeyOperations
	}

	// 心跳配置
	validHeartbeatDuration time.Duration

	// 一致性hash环
	mu           sync.RWMutex
	ring         map[uint32]string // hash值 -> 工作节点ID的映射
	sortedHashes []uint32          // 排序后的hash值列表
	workers      []string          // 当前活跃的工作节点列表
	lastUpdate   time.Time         // 上次更新时间
	updateTTL    time.Duration     // 更新间隔，避免频繁重建hash环
	virtualNodes int               // 每个节点的虚拟节点数量
}

// TaskConsistentHashConfig 任务一致性hash配置
type TaskConsistentHashConfig struct {
	Namespace string
	WorkerID  string
	Logger    utils.Logger
	DataStore interface {
		data.HeartbeatOperations
		data.KeyOperations
	}
	ValidHeartbeatDuration time.Duration // 有效心跳持续时间
	UpdateTTL              time.Duration // hash环更新间隔，默认30秒
	VirtualNodes           int           // 每个节点的虚拟节点数量，默认50
}

// NewTaskConsistentHash 创建新的任务一致性hash实例
func NewTaskConsistentHash(config TaskConsistentHashConfig) *TaskConsistentHash {
	// 设置默认值
	if config.UpdateTTL <= 0 {
		config.UpdateTTL = 30 * time.Second
	}
	if config.VirtualNodes <= 0 {
		config.VirtualNodes = 50
	}
	if config.ValidHeartbeatDuration <= 0 {
		config.ValidHeartbeatDuration = 60 * time.Second
	}

	return &TaskConsistentHash{
		namespace:              config.Namespace,
		workerID:               config.WorkerID,
		logger:                 config.Logger,
		dataStore:              config.DataStore,
		validHeartbeatDuration: config.ValidHeartbeatDuration,
		ring:                   make(map[uint32]string),
		updateTTL:              config.UpdateTTL,
		virtualNodes:           config.VirtualNodes,
	}
}

// IsPreferredPartition 检查指定分区是否是指定工作节点所偏好的
// 使用一致性hash算法确定分区是否应该优先由指定节点处理
func (tch *TaskConsistentHash) IsPreferredPartition(ctx context.Context, partitionID int, workerID string) (bool, error) {
	// 更新hash环（如果需要）
	if err := tch.updateHashRingIfNeeded(ctx); err != nil {
		tch.logger.Warnf("更新hash环失败: %v", err)
		// 更新失败时返回false，让调用方使用其他策略
		return false, nil
	}

	tch.mu.RLock()
	defer tch.mu.RUnlock()

	// 如果hash环为空，返回false
	if len(tch.sortedHashes) == 0 {
		tch.logger.Debugf("hash环为空，无法确定分区偏好")
		return false, nil
	}

	// 获取分区的偏好工作节点
	preferredWorker := tch.getPreferredWorkerForPartition(partitionID)
	return preferredWorker == workerID, nil
}

// IsCurrentWorkerPreferred 检查指定分区是否是当前工作节点所偏好的
// 这是IsPreferredPartition的便捷方法，使用当前工作节点ID
func (tch *TaskConsistentHash) IsCurrentWorkerPreferred(ctx context.Context, partitionID int) (bool, error) {
	return tch.IsPreferredPartition(ctx, partitionID, tch.workerID)
}

// getPreferredWorkerForPartition 获取指定分区的偏好工作节点
// 内部方法，调用时已持有读锁
func (tch *TaskConsistentHash) getPreferredWorkerForPartition(partitionID int) string {
	if len(tch.sortedHashes) == 0 {
		return ""
	}

	// 计算分区的hash值
	partitionHash := tch.hashPartition(partitionID)

	// 在hash环上找到第一个大于等于分区hash的节点
	index := sort.Search(len(tch.sortedHashes), func(i int) bool {
		return tch.sortedHashes[i] >= partitionHash
	})

	// 如果没有找到，则使用第一个节点（环形结构）
	if index == len(tch.sortedHashes) {
		index = 0
	}

	hash := tch.sortedHashes[index]
	return tch.ring[hash]
}

// updateHashRingIfNeeded 如果需要则更新hash环
func (tch *TaskConsistentHash) updateHashRingIfNeeded(ctx context.Context) error {
	tch.mu.RLock()
	needUpdate := time.Since(tch.lastUpdate) > tch.updateTTL
	tch.mu.RUnlock()

	if needUpdate {
		return tch.updateHashRing(ctx)
	}
	return nil
}

// updateHashRing 增量更新hash环，只对变化的节点进行操作
func (tch *TaskConsistentHash) updateHashRing(ctx context.Context) error {
	// 获取活跃工作节点列表
	activeWorkers, err := tch.getActiveWorkers(ctx)
	if err != nil {
		return errors.Wrap(err, "获取活跃工作节点失败")
	}

	tch.mu.Lock()
	defer tch.mu.Unlock()

	// 检查是否需要更新（双重检查）
	if time.Since(tch.lastUpdate) <= tch.updateTTL {
		return nil
	}

	// 如果工作节点列表没有变化，则不需要更新hash环
	if tch.workersEqual(activeWorkers) {
		tch.lastUpdate = time.Now()
		return nil
	}

	// 计算新增和删除的节点
	addedWorkers, removedWorkers := tch.calculateWorkerChanges(activeWorkers)

	tch.logger.Debugf("hash环增量更新，新增节点: %v，删除节点: %v", addedWorkers, removedWorkers)

	// 如果是首次初始化，直接重建
	if len(tch.workers) == 0 {
		tch.rebuildHashRing(activeWorkers)
	} else {
		// 增量更新
		tch.incrementalUpdateHashRing(addedWorkers, removedWorkers, activeWorkers)
	}

	tch.lastUpdate = time.Now()

	tch.logger.Infof("hash环增量更新完成，工作节点数: %d，虚拟节点数: %d",
		len(activeWorkers), len(tch.sortedHashes))

	return nil
}

// getActiveWorkers 获取活跃工作节点列表
// 基于leader/work_manager.go中的getActiveWorkers逻辑实现
func (tch *TaskConsistentHash) getActiveWorkers(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf(model.HeartbeatFmtFmt, tch.namespace, "*")
	keys, err := tch.dataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳键失败")
	}

	var activeWorkers []string
	now := time.Now()

	for _, key := range keys {
		// 从key中提取节点ID
		prefix := fmt.Sprintf(model.HeartbeatFmtFmt, tch.namespace, "")
		if len(key) <= len(prefix) {
			continue
		}
		nodeID := key[len(prefix):]

		// 获取最后心跳时间
		lastHeartbeatStr, err := tch.dataStore.GetHeartbeat(ctx, key)
		if err != nil {
			continue // 跳过错误的心跳
		}

		lastHeartbeat, err := time.Parse(time.RFC3339, lastHeartbeatStr)
		if err != nil {
			continue // 跳过无效的时间格式
		}

		// 检查心跳是否有效
		if now.Sub(lastHeartbeat) <= tch.validHeartbeatDuration {
			activeWorkers = append(activeWorkers, nodeID)
		}
	}

	return activeWorkers, nil
}

// workersEqual 检查两个工作节点列表是否相等
func (tch *TaskConsistentHash) workersEqual(newWorkers []string) bool {
	if len(tch.workers) != len(newWorkers) {
		return false
	}

	// 创建map进行快速比较
	existing := make(map[string]bool)
	for _, worker := range tch.workers {
		existing[worker] = true
	}

	for _, worker := range newWorkers {
		if !existing[worker] {
			return false
		}
	}

	return true
}

// hashString 计算字符串的hash值
// 使用MD5哈希算法确保更好的分布
func (tch *TaskConsistentHash) hashString(s string) uint32 {
	h := md5.New()
	h.Write([]byte(s))
	hash := h.Sum(nil)

	// 使用MD5的前4字节
	return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
}

// hashPartition 计算分区的hash值
func (tch *TaskConsistentHash) hashPartition(partitionID int) uint32 {
	return tch.hashString(strconv.Itoa(partitionID))
}

// GetDistributionAnalysis 获取分区分布分析（用于调试和监控）
func (tch *TaskConsistentHash) GetDistributionAnalysis(ctx context.Context, allPartitions []*model.PartitionInfo) (*ConsistentHashDistributionAnalysis, error) {
	if err := tch.updateHashRingIfNeeded(ctx); err != nil {
		return nil, err
	}

	tch.mu.RLock()
	defer tch.mu.RUnlock()

	analysis := &ConsistentHashDistributionAnalysis{
		WorkerCount:        len(tch.workers),
		VirtualNodes:       tch.virtualNodes,
		TotalPartitions:    len(allPartitions),
		WorkerDistribution: make(map[string]int),
		PendingPartitions:  0,
	}

	// 统计每个工作节点应该处理的分区数量
	for _, partition := range allPartitions {
		if partition.Status == model.StatusPending {
			analysis.PendingPartitions++
			preferredWorker := tch.getPreferredWorkerForPartition(partition.PartitionID)
			if preferredWorker != "" {
				analysis.WorkerDistribution[preferredWorker]++
			}
		}
	}

	// 计算分布质量
	if len(tch.workers) > 0 && analysis.PendingPartitions > 0 {
		expectedPerWorker := float64(analysis.PendingPartitions) / float64(len(tch.workers))
		var variance float64
		var totalDeviation float64

		// 确保所有工作节点都在分布中（即使分配数为0）
		for _, worker := range tch.workers {
			count := analysis.WorkerDistribution[worker]
			diff := float64(count) - expectedPerWorker
			variance += diff * diff
			totalDeviation += math.Abs(diff)
		}

		variance /= float64(len(tch.workers))
		analysis.DistributionVariance = variance

		// 改进的质量评分计算
		if variance == 0 {
			analysis.QualityScore = 100
		} else {
			// 使用变异系数（CV）来计算质量评分，这样可以更好地比较不同规模的分布
			cv := math.Sqrt(variance) / expectedPerWorker
			// 质量评分：CV越小质量越高，使用负指数函数
			analysis.QualityScore = 100 * math.Exp(-cv)

			// 确保评分在合理范围内
			if analysis.QualityScore > 100 {
				analysis.QualityScore = 100
			}
			if analysis.QualityScore < 0 {
				analysis.QualityScore = 0
			}
		}
	}

	return analysis, nil
}

// ConsistentHashDistributionAnalysis 一致性hash分布分析结果
type ConsistentHashDistributionAnalysis struct {
	WorkerCount          int            `json:"worker_count"`          // 工作节点数量
	VirtualNodes         int            `json:"virtual_nodes"`         // 每个节点的虚拟节点数
	TotalPartitions      int            `json:"total_partitions"`      // 总分区数
	PendingPartitions    int            `json:"pending_partitions"`    // 待处理分区数
	WorkerDistribution   map[string]int `json:"worker_distribution"`   // 每个工作节点分配的分区数
	DistributionVariance float64        `json:"distribution_variance"` // 分布方差
	QualityScore         float64        `json:"quality_score"`         // 分布质量评分 (0-100)
}

// rebuildHashRing 完全重建hash环（仅用于首次初始化）
func (tch *TaskConsistentHash) rebuildHashRing(activeWorkers []string) {
	tch.workers = make([]string, len(activeWorkers))
	copy(tch.workers, activeWorkers)
	tch.ring = make(map[uint32]string)
	tch.sortedHashes = nil

	// 为每个工作节点创建虚拟节点
	for _, worker := range activeWorkers {
		tch.addWorkerToRing(worker)
	}

	// 重新排序
	tch.sortHashedValues()
}

// incrementalUpdateHashRing 增量更新hash环
func (tch *TaskConsistentHash) incrementalUpdateHashRing(addedWorkers, removedWorkers, activeWorkers []string) {
	// 从hash环中删除节点
	for _, worker := range removedWorkers {
		tch.removeWorkerFromRing(worker)
	}

	// 向hash环中添加新节点
	for _, worker := range addedWorkers {
		tch.addWorkerToRing(worker)
	}

	// 更新工作节点列表
	tch.workers = make([]string, len(activeWorkers))
	copy(tch.workers, activeWorkers)

	// 重新排序（只有在有变化时才需要）
	if len(addedWorkers) > 0 || len(removedWorkers) > 0 {
		tch.sortHashedValues()
	}
}

// addWorkerToRing 向hash环添加工作节点
func (tch *TaskConsistentHash) addWorkerToRing(worker string) {
	for i := 0; i < tch.virtualNodes; i++ {
		// 使用多种不同的哈希种子来确保更好的分布
		virtualKey1 := fmt.Sprintf("%s:%d", worker, i)
		virtualKey2 := fmt.Sprintf("%d:%s", i, worker)

		hash1 := tch.hashString(virtualKey1)
		hash2 := tch.hashString(virtualKey2)

		// 添加两个不同的虚拟节点以增加分布均匀性
		tch.ring[hash1] = worker
		tch.ring[hash2] = worker
		tch.sortedHashes = append(tch.sortedHashes, hash1, hash2)
	}
}

// removeWorkerFromRing 从hash环删除工作节点
func (tch *TaskConsistentHash) removeWorkerFromRing(worker string) {
	// 收集要删除的hash值
	hashesToRemove := make([]uint32, 0, tch.virtualNodes*2) // 每个虚拟节点有2个哈希
	for i := 0; i < tch.virtualNodes; i++ {
		virtualKey1 := fmt.Sprintf("%s:%d", worker, i)
		virtualKey2 := fmt.Sprintf("%d:%s", i, worker)

		hash1 := tch.hashString(virtualKey1)
		hash2 := tch.hashString(virtualKey2)

		hashesToRemove = append(hashesToRemove, hash1, hash2)
	}

	// 从ring中删除
	for _, hash := range hashesToRemove {
		delete(tch.ring, hash)
	}

	// 从sortedHashes中删除 - 重建而不是逐个删除以避免复杂度
	newSortedHashes := make([]uint32, 0, len(tch.sortedHashes)-len(hashesToRemove))
	hashToRemoveMap := make(map[uint32]bool)
	for _, hash := range hashesToRemove {
		hashToRemoveMap[hash] = true
	}

	for _, hash := range tch.sortedHashes {
		if !hashToRemoveMap[hash] {
			newSortedHashes = append(newSortedHashes, hash)
		}
	}

	tch.sortedHashes = newSortedHashes
}

// sortHashedValues 对hash值进行排序
func (tch *TaskConsistentHash) sortHashedValues() {
	sort.Slice(tch.sortedHashes, func(i, j int) bool {
		return tch.sortedHashes[i] < tch.sortedHashes[j]
	})
}

// calculateWorkerChanges 计算工作节点的变化
func (tch *TaskConsistentHash) calculateWorkerChanges(newWorkers []string) (added, removed []string) {
	// 创建当前和新的工作节点集合
	currentSet := make(map[string]bool)
	newSet := make(map[string]bool)

	for _, worker := range tch.workers {
		currentSet[worker] = true
	}

	for _, worker := range newWorkers {
		newSet[worker] = true
	}

	// 找出新增的节点
	for _, worker := range newWorkers {
		if !currentSet[worker] {
			added = append(added, worker)
		}
	}

	// 找出删除的节点
	for _, worker := range tch.workers {
		if !newSet[worker] {
			removed = append(removed, worker)
		}
	}

	return added, removed
}
