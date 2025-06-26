package partition

import (
	"context"
	"strconv"

	"github.com/iheCoder/elk_coordinator/model"
)

// ==================== 统计功能 ====================

// GetPartitionStats 获取分区状态统计信息（轻量级版本）
//
// 该方法使用缓存的统计数据，避免扫描所有分区。统计数据在分区状态变更时
// 进行增量更新，查询时直接返回预计算的结果，大幅提升性能。
//
// 当缓存统计数据为空或不一致时，会自动重建统计数据以确保准确性。
//
// 性能特点:
//   - O(1) 时间复杂度，直接读取统计摘要
//   - 无需传输大量分区数据，只读取统计计数器
//   - 实时性好，统计数据随分区状态变更同步更新
//   - 自动处理缓存失效和数据不一致问题
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - *model.PartitionStats: 分区统计信息
//   - error: 错误信息
func (s *HashPartitionStrategy) GetPartitionStats(ctx context.Context) (*model.PartitionStats, error) {
	// 从Redis获取预计算的统计数据
	statsData, err := s.store.GetPartitionStatsData(ctx, statsKey)
	if err != nil {
		s.logger.Errorf("获取分区统计数据失败: %v", err)
		return nil, err
	}

	// 如果统计数据为空，说明还未初始化，尝试重建
	if len(statsData) == 0 {
		s.logger.Warnf("统计数据为空，尝试重建统计数据")
		if err := s.RebuildStats(ctx); err != nil {
			s.logger.Errorf("重建统计数据失败: %v", err)
			return nil, err
		}
		// 重新获取统计数据
		statsData, err = s.store.GetPartitionStatsData(ctx, statsKey)
		if err != nil {
			s.logger.Errorf("重建后获取统计数据仍失败: %v", err)
			return nil, err
		}
	}

	// 解析统计数据
	stats := &model.PartitionStats{}

	// 辅助函数：安全地将字符串转换为整数
	parseInt := func(s string, defaultVal int) int {
		if s == "" {
			return defaultVal
		}
		if val, err := strconv.Atoi(s); err == nil {
			return val
		}
		return defaultVal
	}

	// 辅助函数：安全地将字符串转换为int64
	parseInt64 := func(s string, defaultVal int64) int64 {
		if s == "" {
			return defaultVal
		}
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		return defaultVal
	}

	// 填充统计数据
	stats.Total = parseInt(statsData["total"], 0)
	stats.Pending = parseInt(statsData["pending"], 0)
	stats.Claimed = parseInt(statsData["claimed"], 0)
	stats.Running = parseInt(statsData["running"], 0)
	stats.Completed = parseInt(statsData["completed"], 0)
	stats.Failed = parseInt(statsData["failed"], 0)
	stats.MaxPartitionID = parseInt(statsData["max_partition_id"], 0)
	stats.LastAllocatedID = parseInt64(statsData["last_allocated_id"], 0)

	// 计算比率
	if stats.Total > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Total)
		stats.FailureRate = float64(stats.Failed) / float64(stats.Total)
	} else {
		stats.CompletionRate = 0.0
		stats.FailureRate = 0.0
	}

	s.logger.Debugf("分区统计信息(缓存): 总数=%d, 待处理=%d, 已声明=%d, 运行中=%d, 已完成=%d, 失败=%d, 完成率=%.2f%%, 失败率=%.2f%%, 最大分区ID=%d, 最大数据ID=%d",
		stats.Total, stats.Pending, stats.Claimed, stats.Running, stats.Completed, stats.Failed,
		stats.CompletionRate*100, stats.FailureRate*100, stats.MaxPartitionID, stats.LastAllocatedID)

	return stats, nil
}

// RebuildStats 重建统计数据
// 当统计数据丢失或不一致时，从现有分区数据重新构建统计信息
func (s *HashPartitionStrategy) RebuildStats(ctx context.Context) error {
	s.logger.Infof("开始重建分区统计数据")

	err := s.store.RebuildPartitionStats(ctx, statsKey, partitionHashKey, archivedPartitionsKey)
	if err != nil {
		s.logger.Errorf("重建分区统计数据失败: %v", err)
		return err
	}

	s.logger.Infof("分区统计数据重建完成")
	return nil
}
