package http

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"encoding/json"
	"fmt"
	"net/http"
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

// RetryFailedPartitionsHandler HTTP处理器，用于处理重试失败分区的请求
func RetryFailedPartitionsHandler(namespace string, dataStore data.DataStore, logger interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 设置响应头
		w.Header().Set("Content-Type", "application/json")

		// 只接受POST请求
		if r.Method != http.MethodPost {
			response := CommandResponse{
				Success: false,
				Message: "只支持POST方法",
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(response)
			return
		}

		// 解析请求体
		var req RetryFailedPartitionsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response := CommandResponse{
				Success: false,
				Message: "无效的请求格式: " + err.Error(),
			}
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(response)
			return
		}

		// 验证分区ID
		for _, partitionID := range req.PartitionIDs {
			if partitionID <= 0 {
				response := CommandResponse{
					Success: false,
					Message: fmt.Sprintf("无效的分区ID: %d, 分区ID必须大于0", partitionID),
				}
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(response)
				return
			}
		}

		// 创建重试命令
		command := model.NewRetryFailedPartitionsCommand(req.PartitionIDs)

		// 提交命令
		ctx := r.Context()
		err := SubmitCommand(ctx, namespace, dataStore, command)

		if err != nil {
			response := CommandResponse{
				Success: false,
				Message: "提交命令失败: " + err.Error(),
			}
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(response)
			return
		}

		// 返回成功响应
		message := fmt.Sprintf("重试命令已提交")
		if len(req.PartitionIDs) == 0 {
			message = "重试所有失败分区的命令已提交"
		} else {
			message = fmt.Sprintf("重试 %d 个指定分区的命令已提交", len(req.PartitionIDs))
		}

		response := CommandResponse{
			Success:   true,
			Message:   message,
			CommandID: command.ID,
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}
