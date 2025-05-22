package task

import (
	"context"
)

// Processor 定义了任务处理器的接口
// 所有实现分区任务处理逻辑的组件都应该实现这个接口
type Processor interface {
	// Process 处理指定ID范围内的任务
	// 参数:
	// - ctx: 处理上下文，可用于取消或设置超时
	// - minID: 处理范围的最小ID（包含）
	// - maxID: 处理范围的最大ID（包含）
	// - options: 处理选项，可以包含任何特定于任务的参数
	// 返回:
	// - 处理的项目数量
	// - 处理过程中遇到的错误
	Process(ctx context.Context, minID int64, maxID int64, options map[string]interface{}) (int64, error)
}
