package model

import "time"

// CommandType 命令类型
type CommandType string

const (
	// CommandRetryFailedPartitions 重试失败分区命令
	CommandRetryFailedPartitions CommandType = "retry_failed_partitions"
	// 未来可以添加其他命令类型，如：
	// CommandResetPartitions CommandType = "reset_partitions"
	// CommandScalePartitions CommandType = "scale_partitions"
)

// Command 通用命令结构
type Command struct {
	// 命令标识
	ID        string      `json:"id"`
	Type      CommandType `json:"type"`
	Timestamp int64       `json:"timestamp"`
	Status    string      `json:"status"` // pending, processing, completed, failed

	// 命令参数（不同命令类型有不同参数）
	Parameters map[string]interface{} `json:"parameters"`

	// 执行结果
	Result map[string]interface{} `json:"result,omitempty"`

	// 错误信息
	Error string `json:"error,omitempty"`
}

// RetryFailedPartitionsParams 重试失败分区命令的参数
type RetryFailedPartitionsParams struct {
	// PartitionIDs 要重试的分区ID列表，如果为空则重试所有失败分区
	PartitionIDs []int `json:"partition_ids,omitempty"`
}

// CommandResult 命令执行结果
type CommandResult struct {
	Success            bool   `json:"success"`
	ProcessedCount     int    `json:"processed_count"`
	Message            string `json:"message"`
	AffectedPartitions []int  `json:"affected_partitions,omitempty"`
}

// NewRetryFailedPartitionsCommand 创建重试失败分区命令
func NewRetryFailedPartitionsCommand(partitionIDs []int) *Command {
	paramMap := make(map[string]interface{})
	paramMap["partition_ids"] = partitionIDs

	return &Command{
		ID:         generateCommandID(),
		Type:       CommandRetryFailedPartitions,
		Timestamp:  time.Now().Unix(),
		Status:     "pending",
		Parameters: paramMap,
	}
}

// generateCommandID 生成唯一的命令ID
func generateCommandID() string {
	return "cmd-" + time.Now().Format("20060102150405") + "-" + randomString(6)
}

// randomString 生成随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}
