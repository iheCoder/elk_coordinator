package leader

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type Leader struct {
	// 核心依赖
	NodeID    string
	Namespace string
	DataStore data.DataStore
	Logger    utils.Logger
	Planer    PartitionPlaner

	// 配置选项
	ElectionInterval        time.Duration // Leader选举间隔
	LockExpiry              time.Duration // Leader锁过期时间
	WorkerPartitionMultiple int64         // 每个工作节点分配的分区倍数，用于计算ID探测范围
	ValidHeartbeatDuration  time.Duration // 有效心跳持续时间

	// 状态管理
	isLeader     bool
	leaderCtx    context.Context
	cancelLeader context.CancelFunc
	mu           sync.RWMutex
}

// Lead 执行Leader相关的工作
func (l *Leader) Lead(ctx context.Context) error {
	ticker := time.NewTicker(l.ElectionInterval)
	defer ticker.Stop()

	// 初次尝试竞选leader
	if elected := l.initialElection(ctx); elected {
		l.startLeaderWork(ctx)
	}

	// 竞选失败，则周期性判断是否需要重新竞选
	for {
		select {
		case <-ctx.Done():
			l.Logger.Infof("Leader选举任务停止 (上下文取消)")
			return ctx.Err()
		case <-ticker.C:
			l.periodicElection(ctx)
		}
	}
}

// initialElection 进行初始Leader选举
func (l *Leader) initialElection(ctx context.Context) bool {
	elected, err := l.tryElect(ctx)
	if err != nil {
		l.Logger.Warnf("竞选Leader失败: %v", err)
		return false
	}
	return elected
}

// periodicElection 周期性检查是否需要重新竞选
func (l *Leader) periodicElection(ctx context.Context) {
	// 如果当前不是Leader，尝试重新竞选
	l.mu.RLock()
	isLeader := l.isLeader
	l.mu.RUnlock()

	if !isLeader {
		elected, err := l.tryElect(ctx)
		if err != nil {
			l.Logger.Warnf("重新竞选Leader失败: %v", err)
			return
		}

		if elected {
			l.startLeaderWork(ctx)
		}
	}
}

// startLeaderWork 启动Leader工作
func (l *Leader) startLeaderWork(ctx context.Context) {
	l.leaderCtx, l.cancelLeader = context.WithCancel(context.Background())
	l.Logger.Infof("节点 %s 成为Leader，开始Leader工作", l.NodeID)
	go l.doLeaderWork(ctx)
}

// tryElect 尝试竞选Leader
func (l *Leader) tryElect(ctx context.Context) (bool, error) {
	// 获取leader锁
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, l.Namespace)
	success, err := l.DataStore.AcquireLock(ctx, leaderLockKey, l.NodeID, l.LockExpiry)
	if err != nil {
		return false, errors.Wrap(err, "获取Leader锁失败")
	}

	if success {
		l.becomeLeader()
		return true, nil
	}

	// 竞选失败
	l.Logger.Debugf("获取Leader锁失败，将作为Worker运行")
	return false, nil
}

// becomeLeader 设置当前节点为Leader
func (l *Leader) becomeLeader() {
	l.Logger.Infof("成功获取Leader锁，节点 %s 成为Leader", l.NodeID)

	l.mu.Lock()
	l.isLeader = true
	l.mu.Unlock()
}

// doLeaderWork 执行Leader的工作
func (l *Leader) doLeaderWork(ctx context.Context) error {
	// 1. 异步周期性续leader锁
	go l.renewLeaderLock()

	// 2. 持续性分配分区
	return l.runPartitionAllocationLoop(ctx)
}

// runPartitionAllocationLoop 运行分区分配循环
func (l *Leader) runPartitionAllocationLoop(ctx context.Context) error {
	// 初始运行一次分区分配，确保刚启动时有任务可执行
	go l.tryAllocatePartitions(ctx)

	// 使用较长时间间隔进行常规分区检查和分配
	allocationInterval := l.ElectionInterval * 2 // 使用更长的间隔，减少不必要的分配尝试
	ticker := time.NewTicker(allocationInterval)
	defer ticker.Stop()

	// 从lastProcessedID开始，持续分配分区任务，直到GetNextMaxID没有更多任务为止
	for {
		select {
		case <-ctx.Done():
			l.Logger.Infof("Leader工作停止 (上下文取消)")
			return ctx.Err()
		case <-l.leaderCtx.Done():
			l.Logger.Infof("Leader工作停止 (不再是Leader)")
			return nil
		case <-ticker.C:
			// 检查分区状态并按需分配新分区
			l.tryAllocatePartitions(ctx)
		}
	}
}

// tryAllocatePartitions 尝试分配分区并处理错误
func (l *Leader) tryAllocatePartitions(ctx context.Context) {
	// 检查是否有活跃节点，如果没有，则不分配分区
	activeWorkers, err := l.getActiveWorkers(ctx)
	if err != nil {
		l.Logger.Warnf("获取活跃节点失败: %v", err)
		return
	}

	if len(activeWorkers) == 0 {
		l.Logger.Debugf("没有活跃工作节点，跳过分区分配")
		return
	}

	// 尝试分配分区
	if err = l.allocatePartitions(ctx); err != nil {
		l.Logger.Warnf("分配分区失败: %v", err)
	}
}

// renewLeaderLock 定期更新Leader锁
func (l *Leader) renewLeaderLock() {
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, l.Namespace)
	// 使锁更新频率是锁超时时间的1/3，确保有足够时间进行更新
	ticker := time.NewTicker(l.LockExpiry / 3)
	defer ticker.Stop()

	for {
		select {
		case <-l.leaderCtx.Done():
			l.Logger.Infof("停止更新Leader锁")
			return
		case <-ticker.C:
			if !l.tryRenewLeaderLock(leaderLockKey) {
				l.relinquishLeadership()
				return
			}
		}
	}
}

// tryRenewLeaderLock 尝试更新Leader锁
func (l *Leader) tryRenewLeaderLock(leaderLockKey string) bool {
	ctx := context.Background()
	success, err := l.DataStore.RenewLock(ctx, leaderLockKey, l.NodeID, l.LockExpiry)

	if err != nil {
		l.Logger.Warnf("更新Leader锁失败: %v", err)
		return true // 遇到错误时继续尝试，不放弃领导权
	}

	if !success {
		l.Logger.Warnf("无法更新Leader锁，可能锁已被其他节点获取")
		return false // 更新失败，需要放弃领导权
	}

	return true // 成功更新，继续保持领导权
}

// relinquishLeadership 放弃领导权
func (l *Leader) relinquishLeadership() {
	l.mu.Lock()
	l.isLeader = false
	l.mu.Unlock()

	if l.cancelLeader != nil {
		l.cancelLeader()
	}
}

// allocatePartitions 分配工作分区
func (l *Leader) allocatePartitions(ctx context.Context) error {
	// 首先检查现有分区状态
	existingPartitions, currentPartitionStats, err := l.checkExistingPartitions(ctx)
	if err != nil {
		return errors.Wrap(err, "检查现有分区失败")
	}

	// 获取ID范围
	lastAllocatedID, nextMaxID, err := l.getProcessingRange(ctx)
	if err != nil {
		return err
	}

	// 如果没有新的数据要分配，直接返回
	if nextMaxID <= lastAllocatedID {
		l.Logger.Debugf("没有新的数据需要分配，当前已分配的最大ID: %d", lastAllocatedID)
		return nil
	}

	// 检查是否需要分配新的分区
	if !l.shouldAllocateNewPartitions(currentPartitionStats) {
		l.Logger.Debugf("现有分区未达到完成率阈值，暂不分配新分区。完成率: %.2f%%", currentPartitionStats.completionRate*100)
		return nil
	}

	// 创建新的分区，基于ID范围和建议的分区大小
	newPartitions := l.createPartitions(lastAllocatedID, nextMaxID)

	// 合并现有分区和新分区
	mergedPartitions := l.mergePartitions(existingPartitions, newPartitions)

	// 保存合并后的分区到存储
	if err := l.savePartitionsToStorage(ctx, mergedPartitions); err != nil {
		return err
	}

	l.Logger.Infof("成功创建 %d 个新分区，ID范围 [%d, %d]", len(newPartitions), lastAllocatedID+1, nextMaxID)

	return nil
}

// getProcessingRange 获取需要处理的ID范围
func (l *Leader) getProcessingRange(ctx context.Context) (int64, int64, error) {
	// 获取当前已分配分区的最大ID边界
	lastAllocatedID, err := l.getLastAllocatedID(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取最后分配的ID边界失败")
	}

	// 计算合适的ID探测范围大小
	rangeSize, err := l.calculateLookAheadRange(ctx)
	if err != nil {
		l.Logger.Warnf("计算ID探测范围失败: %v，使用默认值", err)
		rangeSize = 10000 // 使用一个默认值作为备选
	}

	l.Logger.Debugf("使用动态计算的ID探测范围: %d", rangeSize)

	// 获取下一批次的最大ID
	nextMaxID, err := l.Planer.GetNextMaxID(ctx, lastAllocatedID, rangeSize)
	if err != nil {
		return 0, 0, errors.Wrap(err, "获取下一个最大ID失败")
	}

	return lastAllocatedID, nextMaxID, nil
}

// calculatePartitionCount 根据活跃工作节点和现有分区状态计算新分区数量
// 注意: 此方法已不是主要的分区数量计算方式，现主要通过分区大小和ID范围计算分区数量
func (l *Leader) calculatePartitionCount(ctx context.Context) (int, error) {
	// 获取活跃节点
	activeWorkers, err := l.getActiveWorkers(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "获取活跃节点失败")
	}

	// 获取现有分区状态
	_, stats, err := l.checkExistingPartitions(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "获取现有分区状态失败")
	}

	// 计算每个节点正在处理的平均分区数
	workerCount := len(activeWorkers)
	if workerCount == 0 {
		workerCount = 1 // 避免除以零
	}

	avgPartitionsPerWorker := float64(stats.running) / float64(workerCount)

	// 基于当前负载计算新的分区数量
	var partitionCount int

	// 如果平均每个节点处理的分区数小于2，可以分配更多分区
	if avgPartitionsPerWorker < 2.0 {
		// 分配节点数量*2的分区，为每个节点提供足够的工作
		partitionCount = workerCount * 2
	} else if avgPartitionsPerWorker < 3.0 {
		// 适中分配
		partitionCount = workerCount
	} else {
		// 负载较高，少分配一些
		partitionCount = workerCount / 2
		if partitionCount == 0 {
			partitionCount = 1
		}
	}

	// 确保至少有一个分区，且不超过最大限制
	if partitionCount == 0 {
		partitionCount = 1 // 至少有一个分区
	} else if partitionCount > model.DefaultPartitionCount {
		partitionCount = model.DefaultPartitionCount // 限制最大分区数
	}

	return partitionCount, nil
}

// createPartitions 创建分区信息
func (l *Leader) createPartitions(lastAllocatedID, nextMaxID int64) map[int]model.PartitionInfo {
	// 获取有效的分区大小（从处理器建议或默认值）
	partitionSize, err := l.getEffectivePartitionSize(context.Background())
	if err != nil {
		l.Logger.Warnf("获取有效分区大小失败: %v，使用默认值", err)
		partitionSize = model.DefaultPartitionSize
	}

	// 记录日志
	if partitionSize == model.DefaultPartitionSize {
		l.Logger.Debugf("使用默认分区大小: %d", partitionSize)
	} else {
		l.Logger.Infof("使用处理器建议的分区大小: %d", partitionSize)
	}

	// 根据分区大小计算分区数量
	totalIds := nextMaxID - lastAllocatedID
	partitionCount := int(totalIds / partitionSize)
	if partitionCount == 0 {
		partitionCount = 1 // 至少创建一个分区
	}

	l.Logger.Infof("ID范围 [%d, %d]，分区大小 %d，将创建 %d 个分区",
		lastAllocatedID+1, nextMaxID, partitionSize, partitionCount)

	partitions := make(map[int]model.PartitionInfo)
	for i := 0; i < partitionCount; i++ {
		minID := lastAllocatedID + int64(i)*partitionSize + 1
		maxID := lastAllocatedID + int64(i+1)*partitionSize

		// 最后一个分区处理到nextMaxID
		if i == partitionCount-1 {
			maxID = nextMaxID
		}

		partitions[i] = model.PartitionInfo{
			PartitionID: i,
			MinID:       minID,
			MaxID:       maxID,
			WorkerID:    "", // 空，等待被认领
			Status:      model.StatusPending,
			UpdatedAt:   time.Now(),
			Options:     make(map[string]interface{}),
		}
	}

	return partitions
}

// savePartitionsToStorage 保存分区信息到存储
func (l *Leader) savePartitionsToStorage(ctx context.Context, partitions map[int]model.PartitionInfo) error {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, l.Namespace)
	partitionsData, err := json.Marshal(partitions)
	if err != nil {
		return errors.Wrap(err, "序列化分区数据失败")
	}

	err = l.DataStore.SetPartitions(ctx, partitionInfoKey, string(partitionsData))
	if err != nil {
		return errors.Wrap(err, "保存分区数据失败")
	}

	return nil
}

// 分区统计信息
type partitionStats struct {
	total          int     // 总分区数
	pending        int     // 等待处理的分区数
	running        int     // 正在处理的分区数
	completed      int     // 已完成的分区数
	failed         int     // 失败的分区数
	completionRate float64 // 完成率 (completed / total)
}

// checkExistingPartitions 检查现有的分区状态
func (l *Leader) checkExistingPartitions(ctx context.Context) (map[int]model.PartitionInfo, partitionStats, error) {
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, l.Namespace)
	partitionsData, err := l.DataStore.GetPartitions(ctx, partitionInfoKey)

	stats := partitionStats{}
	var partitions map[int]model.PartitionInfo

	if err != nil {
		return nil, stats, errors.Wrap(err, "获取分区信息失败")
	}

	// 如果没有分区信息，返回空映射
	if partitionsData == "" {
		return make(map[int]model.PartitionInfo), stats, nil
	}

	if err := json.Unmarshal([]byte(partitionsData), &partitions); err != nil {
		return nil, stats, errors.Wrap(err, "解析分区数据失败")
	}

	// 统计分区状态
	stats.total = len(partitions)
	for _, partition := range partitions {
		switch partition.Status {
		case model.StatusPending:
			stats.pending++
		case model.StatusClaimed, model.StatusRunning:
			stats.running++
		case model.StatusCompleted:
			stats.completed++
		case model.StatusFailed:
			stats.failed++
		}
	}

	// 计算完成率
	if stats.total > 0 {
		stats.completionRate = float64(stats.completed) / float64(stats.total)
	}

	return partitions, stats, nil
}

// shouldAllocateNewPartitions 判断是否应该分配新的分区
func (l *Leader) shouldAllocateNewPartitions(stats partitionStats) bool {
	// 如果没有分区，应该分配
	if stats.total == 0 {
		return true
	}

	// 如果有太多失败的分区，暂停分配新分区
	if stats.failed > stats.total/3 { // 失败率超过1/3
		return false
	}

	// 如果有足够的等待处理或正在处理的分区，不需要分配新分区
	if (stats.pending + stats.running) >= stats.total/2 {
		return false
	}

	// 如果完成率达到70%，可以分配新分区
	return stats.completionRate >= 0.7
}

// mergePartitions 合并现有分区与新分区
func (l *Leader) mergePartitions(existingPartitions, newPartitions map[int]model.PartitionInfo) map[int]model.PartitionInfo {
	// 如果没有现有分区，直接返回新分区
	if len(existingPartitions) == 0 {
		return newPartitions
	}

	// 合并分区，找到最大的分区ID
	maxID := 0
	for id := range existingPartitions {
		if id > maxID {
			maxID = id
		}
	}

	// 为新分区分配新的ID，避免冲突
	mergedPartitions := make(map[int]model.PartitionInfo)
	for id, partition := range existingPartitions {
		mergedPartitions[id] = partition
	}

	// 添加新分区，确保ID不冲突
	for _, partition := range newPartitions {
		maxID++
		partition.PartitionID = maxID
		mergedPartitions[maxID] = partition
	}

	return mergedPartitions
}

// getLastAllocatedID 获取当前已分配分区的最大ID边界
// 注意：这个ID表示的是已经分配的分区范围，而不是已处理完成的ID
func (l *Leader) getLastAllocatedID(ctx context.Context) (int64, error) {
	partitions, _, err := l.checkExistingPartitions(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "获取分区信息失败")
	}

	// 如果没有分区，表示还没有开始分配
	if len(partitions) == 0 {
		return 0, nil
	}

	// 查找所有分区的最大ID
	var maxID int64 = 0
	for _, p := range partitions {
		if p.MaxID > maxID {
			maxID = p.MaxID
		}
	}

	return maxID, nil
}

// calculateLookAheadRange 计算合适的ID探测范围大小
// 基于当前分区大小、活跃节点数量和配置的分区倍数，动态计算合适的范围大小
func (l *Leader) calculateLookAheadRange(ctx context.Context) (int64, error) {
	// 获取当前建议的分区大小
	partitionSize, err := l.getEffectivePartitionSize(ctx)
	if err != nil {
		// 如果获取失败，使用默认值
		partitionSize = model.DefaultPartitionSize
	}

	// 获取活跃节点数量
	activeWorkers, err := l.getActiveWorkers(ctx)
	if err != nil {
		// 如果获取失败，假设至少有一个节点
		return partitionSize * l.WorkerPartitionMultiple, nil
	}

	workerCount := len(activeWorkers)
	if workerCount == 0 {
		workerCount = 1 // 避免除以零
	}

	// 基于分区大小、节点数量和配置的分区倍数计算合理的探测范围
	// 为每个节点准备指定倍数的分区工作量
	rangeSize := partitionSize * int64(workerCount) * l.WorkerPartitionMultiple

	return rangeSize, nil
}

// getEffectivePartitionSize 获取有效的分区大小（从处理器建议或默认值）
func (l *Leader) getEffectivePartitionSize(ctx context.Context) (int64, error) {
	// 尝试从处理器获取建议的分区大小
	suggestedSize, err := l.Planer.PartitionSize(ctx)

	if err != nil || suggestedSize <= 0 {
		// 如果处理器没有建议分区大小，使用默认分区大小
		return model.DefaultPartitionSize, nil
	}

	return suggestedSize, nil
}

// getActiveWorkers 获取活跃节点列表
func (l *Leader) getActiveWorkers(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf(model.HeartbeatFmtFmt, l.Namespace, "*")
	keys, err := l.DataStore.GetKeys(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "获取心跳失败")
	}

	var activeWorkers []string
	now := time.Now()
	validHeartbeatDuration := l.ValidHeartbeatDuration * 3

	for _, key := range keys {
		// 从key中提取节点ID
		prefix := fmt.Sprintf(model.HeartbeatFmtFmt, l.Namespace, "")
		nodeID := key[len(prefix):]

		// 获取最后心跳时间
		lastHeartbeatStr, err := l.DataStore.GetHeartbeat(ctx, key)
		if err != nil {
			continue // 跳过错误的心跳
		}

		lastHeartbeat, err := time.Parse(time.RFC3339, lastHeartbeatStr)
		if err != nil {
			continue // 跳过无效的时间格式
		}

		// 检查心跳是否有效
		if now.Sub(lastHeartbeat) <= validHeartbeatDuration {
			activeWorkers = append(activeWorkers, nodeID)
		} else {
			// 删除过期心跳
			l.DataStore.DeleteKey(ctx, key)
		}
	}

	return activeWorkers, nil
}
