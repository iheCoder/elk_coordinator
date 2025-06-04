package partition

import (
	"context"
	"elk_coordinator/model"
)

// ==================== 统计操作 ====================

// GetPartitionStats 获取分区统计信息
func (s *SimpleStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	partitions, err := s.getAllPartitionsInternal(ctx)
	if err != nil {
		return nil, err
	}

	return s.calculatePartitionStats(partitions), nil
}

// ==================== 统计辅助方法 ====================

// calculatePartitionStats 计算分区统计信息
func (s *SimpleStrategy) calculatePartitionStats(partitions map[int]model.PartitionInfo) *model.PartitionStats {
	stats := &model.PartitionStats{
		Total:     0,
		Pending:   0,
		Claimed:   0,
		Running:   0,
		Failed:    0,
		Completed: 0,
	}

	// 统计各状态分区数量
	for _, partition := range partitions {
		stats.Total++
		s.updateStatsForStatus(stats, partition.Status)
	}

	// 计算比率
	s.calculateStatsRatios(stats)

	return stats
}

// updateStatsForStatus 根据状态更新统计信息
func (s *SimpleStrategy) updateStatsForStatus(stats *model.PartitionStats, status model.PartitionStatus) {
	switch status {
	case model.StatusPending:
		stats.Pending++
	case model.StatusClaimed:
		stats.Claimed++
	case model.StatusRunning:
		stats.Running++
	case model.StatusFailed:
		stats.Failed++
	case model.StatusCompleted:
		stats.Completed++
	}
}

// calculateStatsRatios 计算统计比率
func (s *SimpleStrategy) calculateStatsRatios(stats *model.PartitionStats) {
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	}
}
