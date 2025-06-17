package leader

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/partition"
	"elk_coordinator/utils"
	"encoding/json"
	"fmt"
	"time"
)

// CommandProcessor 命令处理器
type CommandProcessor struct {
	namespace  string
	commandOps data.CommandOperations
	strategy   partition.PartitionStrategy
	logger     utils.Logger
}

// NewCommandProcessor 创建命令处理器
func NewCommandProcessor(namespace string, commandOps data.CommandOperations, strategy partition.PartitionStrategy, logger utils.Logger) *CommandProcessor {
	return &CommandProcessor{
		namespace:  namespace,
		commandOps: commandOps,
		strategy:   strategy,
		logger:     logger,
	}
}

// ProcessCommands 处理待执行的命令
func (cp *CommandProcessor) ProcessCommands(ctx context.Context) error {
	// 获取待处理的命令列表
	commandStrings, err := cp.commandOps.GetPendingCommands(ctx, cp.namespace, 10) // 每次处理最多10个命令
	if err != nil {
		cp.logger.Errorf("获取待处理命令失败: %v", err)
		return err
	}

	if len(commandStrings) == 0 {
		// 没有待处理的命令
		return nil
	}

	// 逐个处理命令
	for _, commandStr := range commandStrings {
		if err := cp.processCommand(ctx, commandStr); err != nil {
			cp.logger.Errorf("处理命令失败: %v", err)
		}
	}

	return nil
}

// processCommand 处理单个命令
func (cp *CommandProcessor) processCommand(ctx context.Context, commandStr string) error {
	// 解析命令
	var command model.Command
	if err := json.Unmarshal([]byte(commandStr), &command); err != nil {
		cp.logger.Errorf("解析命令失败: %v", err)
		// 删除无法解析的命令
		cp.commandOps.DeleteCommand(ctx, cp.namespace, commandStr)
		return err
	}

	cp.logger.Infof("开始处理命令: %s, 类型: %s", command.ID, command.Type)

	// 更新命令状态为处理中
	command.Status = "processing"
	if err := cp.updateCommandInDataStore(ctx, commandStr, &command); err != nil {
		cp.logger.Errorf("更新命令状态失败: %v", err)
		return err
	}

	// 根据命令类型执行相应操作
	var result *model.CommandResult
	var processErr error

	switch command.Type {
	case model.CommandRetryFailedPartitions:
		result, processErr = cp.processRetryFailedPartitions(ctx, &command)
	default:
		processErr = fmt.Errorf("未知的命令类型: %s", command.Type)
	}

	// 更新命令结果
	if processErr != nil {
		command.Status = "failed"
		command.Error = processErr.Error()
		cp.logger.Errorf("命令 %s 执行失败: %v", command.ID, processErr)
	} else {
		command.Status = "completed"
		if result != nil {
			resultMap := map[string]interface{}{
				"success":             result.Success,
				"processed_count":     result.ProcessedCount,
				"message":             result.Message,
				"affected_partitions": result.AffectedPartitions,
			}
			command.Result = resultMap
		}
		cp.logger.Infof("命令 %s 执行成功", command.ID)
	}

	// 保存最终状态
	if err := cp.updateCommandInDataStore(ctx, commandStr, &command); err != nil {
		cp.logger.Errorf("保存命令最终状态失败: %v", err)
		return err
	}

	// 最后删除已处理的命令
	cp.commandOps.DeleteCommand(ctx, cp.namespace, commandStr)
	return nil
}

// processRetryFailedPartitions 处理重试失败分区命令
func (cp *CommandProcessor) processRetryFailedPartitions(ctx context.Context, command *model.Command) (*model.CommandResult, error) {
	// 解析参数
	partitionIDsInterface, exists := command.Parameters["partition_ids"]
	if !exists {
		return nil, fmt.Errorf("缺少partition_ids参数")
	}

	var partitionIDs []int
	if partitionIDsInterface != nil {
		// 处理interface{}到[]int的转换
		switch v := partitionIDsInterface.(type) {
		case []interface{}:
			for _, id := range v {
				if intID, ok := id.(float64); ok { // JSON unmarshaling会将数字解析为float64
					partitionIDs = append(partitionIDs, int(intID))
				}
			}
		case []int:
			partitionIDs = v
		}
	}

	cp.logger.Infof("处理重试失败分区命令，分区IDs: %v", partitionIDs)

	// 获取要重试的分区
	var targetPartitions []*model.PartitionInfo
	var err error

	if len(partitionIDs) == 0 {
		// 如果未指定分区ID，获取所有失败分区
		filters := model.GetPartitionsFilters{
			TargetStatuses: []model.PartitionStatus{model.StatusFailed},
		}
		targetPartitions, err = cp.strategy.GetFilteredPartitions(ctx, filters)
		if err != nil {
			return nil, fmt.Errorf("获取失败分区列表失败: %w", err)
		}
		cp.logger.Infof("未指定分区ID，将重试所有 %d 个失败分区", len(targetPartitions))
	} else {
		// 获取指定的分区并验证它们是否处于失败状态
		for _, partitionID := range partitionIDs {
			partition, err := cp.strategy.GetPartition(ctx, partitionID)
			if err != nil {
				cp.logger.Warnf("获取分区 %d 失败: %v", partitionID, err)
				continue
			}
			if partition.Status == model.StatusFailed {
				targetPartitions = append(targetPartitions, partition)
			} else {
				cp.logger.Warnf("分区 %d 状态为 %s，不是失败状态，跳过", partitionID, partition.Status)
			}
		}
		cp.logger.Infof("指定 %d 个分区ID，其中 %d 个处于失败状态", len(partitionIDs), len(targetPartitions))
	}

	if len(targetPartitions) == 0 {
		return &model.CommandResult{
			Success:        true,
			ProcessedCount: 0,
			Message:        "没有找到需要重试的失败分区",
		}, nil
	}

	// 将失败分区状态更新为pending，让Runner重新处理
	successCount := 0
	var affectedPartitions []int

	for _, partition := range targetPartitions {
		// 重置分区状态为pending
		partition.Status = model.StatusPending
		partition.WorkerID = ""
		partition.UpdatedAt = time.Now()

		// 更新分区状态
		if err := cp.strategy.UpdatePartitionStatus(ctx, partition.PartitionID, "", model.StatusPending, nil); err != nil {
			cp.logger.Errorf("更新分区 %d 状态为pending失败: %v", partition.PartitionID, err)
			continue
		}

		successCount++
		affectedPartitions = append(affectedPartitions, partition.PartitionID)
		cp.logger.Infof("已将失败分区 %d 重置为pending状态", partition.PartitionID)
	}

	result := &model.CommandResult{
		Success:            true,
		ProcessedCount:     successCount,
		Message:            fmt.Sprintf("成功重试 %d 个失败分区", successCount),
		AffectedPartitions: affectedPartitions,
	}

	return result, nil
}

// updateCommandInDataStore 更新命令状态到数据存储
func (cp *CommandProcessor) updateCommandInDataStore(ctx context.Context, originalCommandStr string, command *model.Command) error {
	return cp.commandOps.UpdateCommandStatus(ctx, cp.namespace, originalCommandStr, command)
}
