package partition

import (
	"context"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 统计功能 ====================

// GetPartitionStats 获取分区状态统计信息
//
// 该方法提供分区的全面统计信息，包括各种状态的分区数量、
// 活跃分区数量以及相关比率。由于实现了分离式归档策略，
// 该方法会同时统计 Active Layer 和 Archive Layer 的分区。
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - *model.PartitionStats: 分区统计信息
//   - error: 错误信息
func (s *HashPartitionStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	// 获取活跃分区（Active Layer）
	activePartitions, err := s.GetAllActivePartitions(ctx)
	if err != nil {
		s.logger.Errorf("获取活跃分区统计信息时失败: %v", err)
		return nil, err
	}

	// 获取已完成分区（Archive Layer）
	completedPartitions, err := s.GetAllCompletedPartitions(ctx)
	if err != nil {
		s.logger.Errorf("获取已完成分区统计信息时失败: %v", err)
		// 如果获取已完成分区失败，记录警告但继续处理，只返回活跃分区统计
		s.logger.Warnf("无法获取已完成分区统计，将只统计活跃分区")
		completedPartitions = []*model.PartitionInfo{}
	}

	// 合并所有分区进行统计
	allPartitions := make([]*model.PartitionInfo, 0, len(activePartitions)+len(completedPartitions))
	allPartitions = append(allPartitions, activePartitions...)
	allPartitions = append(allPartitions, completedPartitions...)

	// 初始化统计信息
	stats := &model.PartitionStats{
		Total:           len(allPartitions),
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
	for _, partition := range allPartitions {
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

	s.logger.Debugf("分区统计信息: 总数=%d(活跃:%d+已完成:%d), 待处理=%d, 已声明=%d, 运行中=%d, 已完成=%d, 失败=%d, 最大分区ID=%d, 最大数据ID=%d",
		stats.Total, len(activePartitions), len(completedPartitions), stats.Pending, stats.Claimed, stats.Running, stats.Completed, stats.Failed, stats.MaxPartitionID, stats.LastAllocatedID)

	return stats, nil
}
