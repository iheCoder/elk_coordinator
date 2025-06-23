package http

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func init() {
	// 设置Gin为测试模式
	gin.SetMode(gin.TestMode)
}

func TestRetryFailedPartitionsGinHandler_Success(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建测试请求
	req := RetryFailedPartitionsRequest{
		PartitionIDs: []int{1, 2, 3},
	}
	reqBody, _ := json.Marshal(req)

	// 创建HTTP请求
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")

	// 创建响应记录器
	w := httptest.NewRecorder()

	// 创建Gin上下文
	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	// 调用处理器
	handler := RetryFailedPartitionsGinHandler("test-namespace", dataStore, logger)
	handler(c)

	// 验证响应
	assert.Equal(t, http.StatusOK, w.Code)

	var response CommandResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.True(t, response.Success)
	assert.Contains(t, response.Message, "重试 3 个指定分区的命令已提交")
	assert.NotEmpty(t, response.CommandID)

	t.Logf("测试成功，响应: %+v", response)
}

func TestRetryFailedPartitionsGinHandler_AllPartitions(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建测试请求（空的分区ID列表，表示重试所有分区）
	req := RetryFailedPartitionsRequest{
		PartitionIDs: []int{},
	}
	reqBody, _ := json.Marshal(req)

	// 创建HTTP请求
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")

	// 创建响应记录器
	w := httptest.NewRecorder()

	// 创建Gin上下文
	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	// 调用处理器
	handler := RetryFailedPartitionsGinHandler("test-namespace", dataStore, logger)
	handler(c)

	// 验证响应
	assert.Equal(t, http.StatusOK, w.Code)

	var response CommandResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.True(t, response.Success)
	assert.Contains(t, response.Message, "重试所有失败分区的命令已提交")
	assert.NotEmpty(t, response.CommandID)

	t.Logf("测试成功，响应: %+v", response)
}

func TestRetryFailedPartitionsGinHandler_InvalidJSON(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建无效的JSON请求
	invalidJSON := []byte(`{"partition_ids": [1, 2, "invalid"]}`)

	// 创建HTTP请求
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(invalidJSON))
	httpReq.Header.Set("Content-Type", "application/json")

	// 创建响应记录器
	w := httptest.NewRecorder()

	// 创建Gin上下文
	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	// 调用处理器
	handler := RetryFailedPartitionsGinHandler("test-namespace", dataStore, logger)
	handler(c)

	// 验证响应
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response CommandResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.False(t, response.Success)
	assert.Contains(t, response.Message, "无效的请求格式")

	t.Logf("测试成功，错误响应: %+v", response)
}

func TestRetryFailedPartitionsGinHandler_InvalidPartitionID(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建包含无效分区ID的请求
	req := RetryFailedPartitionsRequest{
		PartitionIDs: []int{1, 0, 3}, // 0是无效的分区ID
	}
	reqBody, _ := json.Marshal(req)

	// 创建HTTP请求
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")

	// 创建响应记录器
	w := httptest.NewRecorder()

	// 创建Gin上下文
	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	// 调用处理器
	handler := RetryFailedPartitionsGinHandler("test-namespace", dataStore, logger)
	handler(c)

	// 验证响应
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response CommandResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.False(t, response.Success)
	assert.Contains(t, response.Message, "无效的分区ID: 0")

	t.Logf("测试成功，错误响应: %+v", response)
}

func TestRouter_SetupRoutes(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建路由器
	router := NewRouter("test-namespace", dataStore, logger)
	engine := router.SetupRoutes()

	// 测试健康检查端点
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "elk-coordinator")
	assert.Contains(t, w.Body.String(), "ok")

	t.Logf("健康检查响应: %s", w.Body.String())
}

func TestHealthCheckHandler(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建路由器
	router := NewRouter("test-namespace", dataStore, logger)

	// 创建测试上下文
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// 调用健康检查处理器
	handler := router.healthCheckHandler()
	handler(c)

	// 验证响应
	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "ok", response["status"])
	assert.Equal(t, "elk-coordinator", response["service"])

	t.Logf("健康检查响应: %+v", response)
}

// 基准测试
func BenchmarkRetryFailedPartitionsGinHandler(b *testing.B) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(false)

	// 创建测试请求
	req := RetryFailedPartitionsRequest{
		PartitionIDs: []int{1, 2, 3},
	}
	reqBody, _ := json.Marshal(req)

	// 创建处理器
	handler := RetryFailedPartitionsGinHandler("test-namespace", dataStore, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 创建HTTP请求
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(reqBody))
		httpReq.Header.Set("Content-Type", "application/json")

		// 创建响应记录器
		w := httptest.NewRecorder()

		// 创建Gin上下文
		c, _ := gin.CreateTestContext(w)
		c.Request = httpReq

		// 调用处理器
		handler(c)
	}
}

// 测试SubmitCommand功能
func TestSubmitCommand(t *testing.T) {
	// 创建测试用的数据存储
	dataStore := test_utils.NewMockDataStore()

	// 创建测试命令
	command := model.NewRetryFailedPartitionsCommand([]int{1, 2, 3})

	// 调用SubmitCommand
	ctx := context.Background()
	err := SubmitCommand(ctx, "test-namespace", dataStore, command)

	// 验证没有错误
	assert.NoError(t, err)

	// 验证命令被正确存储
	// 检查命令是否被添加到Commands映射中
	dataStore.CommandMutex.RLock()
	commandCount := len(dataStore.Commands)
	dataStore.CommandMutex.RUnlock()

	assert.Equal(t, 1, commandCount, "应该有一个命令被存储")

	t.Logf("命令提交成功，命令ID: %s", command.ID)
}

// 集成测试：测试完整的API流程
func TestIntegrationAPIFlow(t *testing.T) {
	// 创建测试用的数据存储和日志器
	dataStore := test_utils.NewMockDataStore()
	logger := test_utils.NewMockLogger(true) // 启用debug日志

	// 创建路由器和引擎
	router := NewRouter("integration-test", dataStore, logger)
	engine := router.SetupRoutes()

	// 测试场景1：成功重试指定分区
	t.Run("SuccessRetrySpecificPartitions", func(t *testing.T) {
		req := RetryFailedPartitionsRequest{PartitionIDs: []int{1, 2, 3}}
		reqBody, _ := json.Marshal(req)

		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/retry-failed-partitions", bytes.NewBuffer(reqBody))
		httpReq.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusOK, w.Code)

		var response CommandResponse
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.True(t, response.Success)
		assert.NotEmpty(t, response.CommandID)

		t.Logf("集成测试成功，响应: %+v", response)
	})

	// 测试场景2：健康检查
	t.Run("HealthCheck", func(t *testing.T) {
		httpReq := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httpReq)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "elk-coordinator")

		t.Logf("健康检查成功，响应: %s", w.Body.String())
	})

	// 验证命令存储
	dataStore.CommandMutex.RLock()
	commandCount := len(dataStore.Commands)
	dataStore.CommandMutex.RUnlock()

	assert.GreaterOrEqual(t, commandCount, 1, "应该至少有一个命令被存储")
	t.Logf("总共存储了 %d 个命令", commandCount)
}
