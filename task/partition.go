// partition.go 包含所有与分区管理相关的函数
package task

import (
	"context"
	"github.com/iheCoder/elk_coordinator/model"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// ------------- 分区获取相关函数 -------------

// acquirePartitionTask 获取一个可用的分区任务
// 使用智能混合策略减少获取分区冲突，策略优先级：
// 1. 一致性哈希策略：pending分区较多时使用，减少worker间冲突
// 2. 随机选择策略：pending分区较少时使用，避免一致性哈希开销
// 3. 直接遍历策略：pending分区极少时，简单高效
// 4. 时间优先策略：对于claimed/running分区，优先获取更新时间较久远的
func (r *Runner) acquirePartitionTask(ctx context.Context) (model.PartitionInfo, error) {
	// 获取所有分区信息
	allPartitions, err := r.partitionStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		return model.PartitionInfo{}, errors.Wrap(err, "获取分区信息失败")
	}

	if len(allPartitions) == 0 {
		return model.PartitionInfo{}, ErrNoAvailablePartition
	}

	// 统计pending分区数量以选择合适的策略
	pendingPartitions := r.filterPendingPartitions(allPartitions)
	pendingCount := len(pendingPartitions)

	r.logger.Debugf("发现 %d 个pending分区，总分区数 %d", pendingCount, len(allPartitions))

	// 策略1：尝试获取pending分区（根据数量选择策略）
	if pendingCount > 0 {
		if acquired, err := r.tryAcquirePendingPartitions(ctx, pendingPartitions, pendingCount); err == nil {
			return acquired, nil
		}
	}

	// 策略2：尝试获取可重新获取的claimed分区（按时间排序）
	if acquired, err := r.tryAcquireReclaimablePartitions(ctx, allPartitions, model.StatusClaimed); err == nil {
		return acquired, nil
	}

	// 策略3：最后尝试获取可能卡住的running分区（按时间排序）
	if acquired, err := r.tryAcquireReclaimablePartitions(ctx, allPartitions, model.StatusRunning); err == nil {
		return acquired, nil
	}

	return model.PartitionInfo{}, ErrNoAvailablePartition
}

// tryAcquirePartition 尝试获取指定的分区
// 使用策略接口的 AcquirePartition 方法
func (r *Runner) tryAcquirePartition(ctx context.Context, partitionID int) (*model.PartitionInfo, bool) {
	// 使用策略接口获取分区，根据配置决定是否启用抢占机制
	options := &model.AcquirePartitionOptions{
		AllowPreemption:   r.allowPreemption, // 使用配置中的抢占设置
		PreemptionTimeout: r.partitionLockExpiry,
	}

	partition, success, err := r.partitionStrategy.AcquirePartition(ctx, partitionID, r.workerID, options)
	if err != nil {
		r.logger.Warnf("获取分区 %d 失败: %v", partitionID, err)
		return nil, false
	}

	if success {
		if r.allowPreemption {
			r.logger.Infof("成功获取分区 %d (抢占模式: 开启)", partitionID)
		} else {
			r.logger.Infof("成功获取分区 %d (抢占模式: 关闭)", partitionID)
		}
		return partition, true
	}

	return nil, false
}

// ------------- 分区状态判断相关函数 -------------

// shouldReclaimPartition 判断一个分区是否应该被重新获取
// 例如分区处于claimed/running状态但长时间未更新
func (r *Runner) shouldReclaimPartition(partition model.PartitionInfo) bool {
	// 检查分区状态
	switch partition.Status {
	case model.StatusClaimed:
		// 对于claimed状态，如果长时间未转为running，可能是之前的worker获取后异常退出
		staleThreshold := r.partitionLockExpiry * 2 // 对claimed状态使用较短的阈值
		timeSinceUpdate := time.Since(partition.UpdatedAt)

		// 如果分区更新时间太旧，并且不是本节点持有的，可以尝试重新获取
		if timeSinceUpdate > staleThreshold && partition.WorkerID != r.workerID {
			return true
		}

	case model.StatusRunning:
		// 对于running状态，使用心跳时间而不是更新时间来判断，因为心跳更能反映worker的真实状态
		staleThreshold := r.partitionLockExpiry * 5 // 对running状态使用更长的阈值
		timeSinceHeartbeat := time.Since(partition.LastHeartbeat)

		// 只有在以下条件满足时才重新获取:
		// 1. 超过了心跳超时阈值
		// 2. 不是本节点持有的任务
		// 3. 心跳时间过久，表明worker可能已经停止工作
		if timeSinceHeartbeat > staleThreshold && partition.WorkerID != r.workerID {
			r.logger.Warnf("检测到可能卡住的运行分区 %d，上次心跳时间: %v，超过了阈值 %v",
				partition.PartitionID, partition.LastHeartbeat, staleThreshold)
			return true
		}
	}

	return false
}

// ------------- 分区状态更新相关函数 -------------

// updateTaskStatus 更新任务状态
// 使用策略接口的 UpdatePartitionStatus 方法
func (r *Runner) updateTaskStatus(ctx context.Context, task model.PartitionInfo, status model.PartitionStatus) error {
	return r.partitionStrategy.UpdatePartitionStatus(ctx, task.PartitionID, r.workerID, status, nil)
}

// updateTaskStatusWithMetadata 更新任务状态并传递元数据
// 使用策略接口的 UpdatePartitionStatus 方法
func (r *Runner) updateTaskStatusWithMetadata(ctx context.Context, task model.PartitionInfo, status model.PartitionStatus, metadata map[string]interface{}) error {
	return r.partitionStrategy.UpdatePartitionStatus(ctx, task.PartitionID, r.workerID, status, metadata)
}

// releasePartitionLock 释放分区锁
// 使用策略接口的 ReleasePartition 方法
func (r *Runner) releasePartitionLock(ctx context.Context, partitionID int) {
	if err := r.partitionStrategy.ReleasePartition(ctx, partitionID, r.workerID); err != nil {
		r.logger.Warnf("释放分区 %d 失败: %v", partitionID, err)
	}
}

// maintainPartitionHold 维护分区持有权
// 使用策略接口的 MaintainPartitionHold 方法
func (r *Runner) maintainPartitionHold(ctx context.Context, partitionID int) error {
	return r.partitionStrategy.MaintainPartitionHold(ctx, partitionID, r.workerID)
}

// ------------- 分区信息查询相关函数 -------------

// getPartitionInfo 获取单个分区信息
func (r *Runner) getPartitionInfo(ctx context.Context, partitionID int) (*model.PartitionInfo, error) {
	return r.partitionStrategy.GetPartition(ctx, partitionID)
}

// getAllPartitions 获取所有分区信息
func (r *Runner) getAllPartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	return r.partitionStrategy.GetAllActivePartitions(ctx)
}

// getAvailablePartitions 获取可用的分区
func (r *Runner) getAvailablePartitions(ctx context.Context) ([]*model.PartitionInfo, error) {
	filters := model.GetPartitionsFilters{
		TargetStatuses: []model.PartitionStatus{model.StatusPending},
	}
	return r.partitionStrategy.GetFilteredPartitions(ctx, filters)
}

// getPartitionStats 获取分区状态统计信息
func (r *Runner) getPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	return r.partitionStrategy.GetPartitionStats(ctx)
}

// ------------- 混合分区获取策略的辅助方法 -------------

// filterPendingPartitions 过滤出所有pending状态的分区
func (r *Runner) filterPendingPartitions(allPartitions []*model.PartitionInfo) []*model.PartitionInfo {
	var pendingPartitions []*model.PartitionInfo
	for _, partition := range allPartitions {
		if partition.Status == model.StatusPending && partition.WorkerID == "" {
			pendingPartitions = append(pendingPartitions, partition)
		}
	}
	return pendingPartitions
}

// tryAcquirePendingPartitions 尝试获取pending分区，根据数量选择合适的策略
func (r *Runner) tryAcquirePendingPartitions(ctx context.Context, pendingPartitions []*model.PartitionInfo, pendingCount int) (model.PartitionInfo, error) {
	const (
		consistentHashThreshold = 20 // pending分区数>=20时使用一致性哈希
		randomSelectThreshold   = 5  // pending分区数>=5但<20时使用随机选择
		// 分区数<5时使用直接遍历
	)

	if pendingCount >= consistentHashThreshold && r.consistentHash != nil {
		// 策略1: pending分区较多，使用一致性哈希减少冲突
		return r.tryAcquireWithConsistentHash(ctx, pendingPartitions)
	} else if pendingCount >= randomSelectThreshold {
		// 策略2: pending分区较少，使用随机选择避免一致性哈希开销
		return r.tryAcquireWithRandomSelection(ctx, pendingPartitions)
	} else {
		// 策略3: pending分区极少，直接遍历获取
		return r.tryAcquireWithDirectTraversal(ctx, pendingPartitions)
	}
}

// tryAcquireWithConsistentHash 使用一致性哈希策略获取分区
func (r *Runner) tryAcquireWithConsistentHash(ctx context.Context, pendingPartitions []*model.PartitionInfo) (model.PartitionInfo, error) {
	// 如果没有一致性哈希，回退到随机选择
	if r.consistentHash == nil {
		r.logger.Debugf("一致性哈希不可用，回退到随机选择策略")
		return r.tryAcquireWithRandomSelection(ctx, pendingPartitions)
	}

	r.logger.Debugf("使用一致性哈希策略，检查 %d 个pending分区中推荐给当前工作节点的分区", len(pendingPartitions))

	// 首先尝试获取一致性哈希推荐的分区
	preferredCount := 0
	for _, partition := range pendingPartitions {
		isPreferred, err := r.consistentHash.IsCurrentWorkerPreferred(ctx, partition.PartitionID)
		if err != nil {
			r.logger.Warnf("检查分区 %d 一致性哈希偏好时出错: %v", partition.PartitionID, err)
			continue
		}

		if isPreferred {
			preferredCount++
			if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
				r.logger.Infof("成功获取一致性哈希推荐的分区 %d", partition.PartitionID)
				return *acquired, nil
			}
		}
	}

	if preferredCount > 0 {
		r.logger.Debugf("一致性哈希推荐了 %d 个分区，但都已被获取，回退到随机选择", preferredCount)
	}

	// 如果推荐的分区都被占用，回退到随机选择
	return r.tryAcquireWithRandomSelection(ctx, pendingPartitions)
}

// tryAcquireWithRandomSelection 使用随机选择策略获取分区
func (r *Runner) tryAcquireWithRandomSelection(ctx context.Context, pendingPartitions []*model.PartitionInfo) (model.PartitionInfo, error) {
	if len(pendingPartitions) == 0 {
		return model.PartitionInfo{}, ErrNoAvailablePartition
	}

	r.logger.Debugf("使用随机选择策略，从 %d 个pending分区中选择", len(pendingPartitions))

	// 随机选择策略：为了减少多个worker同时竞争相同分区的冲突，
	// 我们使用基于workerID的确定性随机种子，使不同worker倾向于选择不同的分区
	// 同时限制尝试次数以避免无效的重复尝试

	// 使用workerID生成种子，确保不同worker有不同的随机序列
	hashValue := r.hashWorkerID(r.workerID)

	// 限制尝试次数：当分区较少时尝试全部，分区较多时尝试一个合理的子集
	// 这样可以减少冲突同时保证获取效率
	maxTryCount := r.calculateOptimalTryCount(len(pendingPartitions))

	randomPartitions := r.getRandomPartitionsWithSeed(pendingPartitions, maxTryCount, hashValue)

	// 尝试获取随机选择的分区
	for _, partition := range randomPartitions {
		if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
			r.logger.Infof("成功获取随机选择的分区 %d", partition.PartitionID)
			return *acquired, nil
		}
	}

	return model.PartitionInfo{}, ErrNoAvailablePartition
}

// tryAcquireWithDirectTraversal 使用直接遍历策略获取分区
func (r *Runner) tryAcquireWithDirectTraversal(ctx context.Context, pendingPartitions []*model.PartitionInfo) (model.PartitionInfo, error) {
	r.logger.Debugf("使用直接遍历策略，逐个尝试 %d 个pending分区", len(pendingPartitions))

	// 直接遍历所有pending分区
	for _, partition := range pendingPartitions {
		if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
			r.logger.Infof("成功获取pending分区 %d", partition.PartitionID)
			return *acquired, nil
		}
	}

	return model.PartitionInfo{}, ErrNoAvailablePartition
}

// tryAcquireReclaimablePartitions 尝试获取可重新获取的分区（claimed或running状态）
// 对于claimed分区按更新时间排序，对于running分区按心跳时间排序
func (r *Runner) tryAcquireReclaimablePartitions(ctx context.Context, allPartitions []*model.PartitionInfo, status model.PartitionStatus) (model.PartitionInfo, error) {
	// 过滤出指定状态的可重新获取分区
	var reclaimablePartitions []*model.PartitionInfo
	for _, partition := range allPartitions {
		if partition.Status == status && r.shouldReclaimPartition(*partition) {
			reclaimablePartitions = append(reclaimablePartitions, partition)
		}
	}

	if len(reclaimablePartitions) == 0 {
		return model.PartitionInfo{}, ErrNoAvailablePartition
	}

	// 根据分区状态选择不同的排序策略
	if status == model.StatusClaimed {
		// 对于claimed状态，按更新时间排序，最旧的在前面
		sort.Slice(reclaimablePartitions, func(i, j int) bool {
			return reclaimablePartitions[i].UpdatedAt.Before(reclaimablePartitions[j].UpdatedAt)
		})
		r.logger.Debugf("尝试重新获取 %d 个claimed状态的分区（按更新时间排序）", len(reclaimablePartitions))
	} else if status == model.StatusRunning {
		// 对于running状态，按心跳时间排序，最旧的在前面
		sort.Slice(reclaimablePartitions, func(i, j int) bool {
			return reclaimablePartitions[i].LastHeartbeat.Before(reclaimablePartitions[j].LastHeartbeat)
		})
		r.logger.Debugf("尝试重新获取 %d 个running状态的分区（按心跳时间排序）", len(reclaimablePartitions))
	}

	// 按排序顺序尝试获取分区
	for _, partition := range reclaimablePartitions {
		if acquired, success := r.tryAcquirePartition(ctx, partition.PartitionID); success {
			if status == model.StatusClaimed {
				r.logger.Infof("成功重新获取超时的已认领分区 %d，上次更新时间: %v",
					partition.PartitionID, partition.UpdatedAt)
			} else {
				r.logger.Warnf("成功重新获取可能卡住的运行分区 %d，上次心跳时间: %v，这可能导致重复处理",
					partition.PartitionID, partition.LastHeartbeat)
			}
			return *acquired, nil
		}
	}

	return model.PartitionInfo{}, ErrNoAvailablePartition
}

// getRandomPartitionsSimple 简单的随机分区选择实现（当一致性哈希不可用时使用）
func (r *Runner) getRandomPartitionsSimple(partitions []*model.PartitionInfo, maxCount int) []*model.PartitionInfo {
	if len(partitions) <= maxCount {
		return partitions
	}

	// 创建分区副本，避免修改原始列表
	shuffled := make([]*model.PartitionInfo, len(partitions))
	copy(shuffled, partitions)

	// 简单洗牌算法
	for i := len(shuffled) - 1; i > 0; i-- {
		j := int(time.Now().UnixNano()) % (i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:maxCount]
}

// ------------- 混合策略的辅助方法 -------------

// hashWorkerID 为workerID生成一个确定性的hash值，用作随机种子
// 这确保了不同worker使用不同的随机序列，减少冲突
func (r *Runner) hashWorkerID(workerID string) uint32 {
	var hash uint32 = 2166136261 // FNV-1a 32-bit offset basis
	for _, b := range []byte(workerID) {
		hash ^= uint32(b)
		hash *= 16777619 // FNV-1a 32-bit prime
	}
	return hash
}

// calculateOptimalTryCount 计算最优的尝试次数
// 随机策略限制尝试次数的原因：
// 1. 减少并发冲突：当多个worker同时获取分区时，如果都尝试全部分区，冲突概率很高
// 2. 提高获取效率：限制尝试次数避免无效的重复尝试
// 3. 负载均衡：通过确定性的随机种子，不同worker倾向于选择不同的分区子集
func (r *Runner) calculateOptimalTryCount(totalPartitions int) int {
	switch {
	case totalPartitions <= 3:
		// 分区极少时，全部尝试
		return totalPartitions
	case totalPartitions <= 10:
		// 分区较少时，尝试大部分
		return (totalPartitions*3 + 3) / 4 // 约75%
	case totalPartitions <= 50:
		// 中等数量分区，尝试一半多一点
		return (totalPartitions + 1) / 2
	default:
		// 大量分区时，限制在合理范围内
		// 最多尝试一半，但不超过20个，至少尝试5个
		maxTry := totalPartitions / 2
		if maxTry > 20 {
			maxTry = 20
		}
		if maxTry < 5 {
			maxTry = 5
		}
		return maxTry
	}
}

// getRandomPartitionsWithSeed 使用指定种子的确定性随机选择
// 这样相同workerID在相同分区集合上总是产生相同的选择序列，
// 但不同workerID会产生不同的序列，从而减少冲突
func (r *Runner) getRandomPartitionsWithSeed(partitions []*model.PartitionInfo, maxCount int, seed uint32) []*model.PartitionInfo {
	if len(partitions) <= maxCount {
		return partitions
	}

	// 创建分区副本
	shuffled := make([]*model.PartitionInfo, len(partitions))
	copy(shuffled, partitions)

	// 使用Linear Congruential Generator (LCG) 进行确定性洗牌
	// 这样相同的种子总是产生相同的序列
	rng := seed
	for i := len(shuffled) - 1; i > 0; i-- {
		// LCG公式: next = (a * current + c) mod m
		rng = rng*1103515245 + 12345 // 标准的LCG参数
		j := int(rng) % (i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:maxCount]
}
