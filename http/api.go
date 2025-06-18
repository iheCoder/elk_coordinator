package http

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RetryFailedPartitionsRequest 重试失败分区的请求结构
type RetryFailedPartitionsRequest struct {
	// PartitionIDs 指定要重试的失败分区ID列表，如果为空则重试所有失败分区
	PartitionIDs []int `json:"partition_ids,omitempty"`
}

// CommandResponse 通用命令响应结构
type CommandResponse struct {
	// Success 操作是否成功
	Success bool `json:"success"`
	// Message 响应消息
	Message string `json:"message"`
	// CommandID 命令ID，用于跟踪
	CommandID string `json:"command_id,omitempty"`
}

// SubmitCommand 提交命令到系统
func SubmitCommand(ctx context.Context, namespace string, dataStore data.DataStore, command *model.Command) error {
	return dataStore.SubmitCommand(ctx, namespace, command)
}

// RetryFailedPartitionsGinHandler Gin版本的重试失败分区处理器
func RetryFailedPartitionsGinHandler(namespace string, dataStore data.DataStore, logger utils.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析请求体
		var req RetryFailedPartitionsRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			response := CommandResponse{
				Success: false,
				Message: "无效的请求格式: " + err.Error(),
			}
			c.JSON(http.StatusBadRequest, response)
			return
		}

		// 验证分区ID
		for _, partitionID := range req.PartitionIDs {
			if partitionID <= 0 {
				response := CommandResponse{
					Success: false,
					Message: fmt.Sprintf("无效的分区ID: %d, 分区ID必须大于0", partitionID),
				}
				c.JSON(http.StatusBadRequest, response)
				return
			}
		}

		// 创建重试命令
		command := model.NewRetryFailedPartitionsCommand(req.PartitionIDs)

		// 提交命令
		ctx := c.Request.Context()
		err := SubmitCommand(ctx, namespace, dataStore, command)

		if err != nil {
			logger.Errorf("提交重试命令失败: %v", err)
			response := CommandResponse{
				Success: false,
				Message: "提交命令失败: " + err.Error(),
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		// 返回成功响应
		message := fmt.Sprintf("重试命令已提交")
		if len(req.PartitionIDs) == 0 {
			message = "重试所有失败分区的命令已提交"
		} else {
			message = fmt.Sprintf("重试 %d 个指定分区的命令已提交", len(req.PartitionIDs))
		}

		logger.Infof("重试命令已提交: CommandID=%s, PartitionIDs=%v", command.ID, req.PartitionIDs)

		response := CommandResponse{
			Success:   true,
			Message:   message,
			CommandID: command.ID,
		}
		c.JSON(http.StatusOK, response)
	}
}
