package partition

import (
	"context"

	"elk_coordinator/model"
)

// ==================== 统计功能 ====================

// GetPartitionStats 获取分区状态统计信息
//
// 该方法提供分区的全面统计信息，包括各种状态的分区数量、
// 活跃分区数量以及相关比率。这些统计信息对于监控和管理
// 分区系统的健康状态非常有用。
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - *model.PartitionStats: 分区统计信息
//   - error: 错误信息
func (s *HashPartitionStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	// 获取所有分区
	partitions, err := s.GetAllPartitions(ctx)
	if err != nil {
		s.logger.Errorf("获取分区统计信息时失败: %v", err)
		return nil, err
	}

	// 初始化统计信息
	stats := &model.PartitionStats{
		Total:           len(partitions),
		Pending:         0,
		Claimed:         0,
		Running:         0,
		Completed:       0,
		Failed:          0,
		CompletionRate:  0.0,
		FailureRate:     0.0,
		MaxPartitionID:  0,
		LastAllocatedID: 0,
	}

	// 统计各种状态的分区数量，同时计算最大分区ID和最大数据ID
	for _, partition := range partitions {
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

		// 更新最大分区ID
		if partition.PartitionID > stats.MaxPartitionID {
			stats.MaxPartitionID = partition.PartitionID
		}

		// 更新最大数据ID
		if partition.MaxID > stats.LastAllocatedID {
			stats.LastAllocatedID = partition.MaxID
		}
	}

	// 计算比率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	}

	s.logger.Debugf("分区统计信息: 总数=%d, 待处理=%d, 已声明=%d, 运行中=%d, 已完成=%d, 失败=%d, 最大分区ID=%d, 最大数据ID=%d",
		stats.Total, stats.Pending, stats.Claimed, stats.Running, stats.Completed, stats.Failed, stats.MaxPartitionID, stats.LastAllocatedID)

	return stats, nil
}
