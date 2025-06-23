// Package clientdemo demonstrates how to call ELK Coordinator HTTP API
// Run this as: go run client_demo.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RetryRequest 重试失败分区的请求结构
type RetryRequest struct {
	PartitionIDs []int64 `json:"partition_ids,omitempty"`
	Mode         string  `json:"mode"`
	Force        bool    `json:"force,omitempty"`
}

// RetryResponse 重试失败分区的响应结构
type RetryResponse struct {
	Success               bool    `json:"success"`
	Message               string  `json:"message"`
	TotalFailedPartitions int     `json:"total_failed_partitions"`
	RetriedPartitions     []int64 `json:"retried_partitions"`
	SkippedPartitions     []int64 `json:"skipped_partitions"`
	RequestID             string  `json:"request_id"`
	Timestamp             string  `json:"timestamp"`
}

// HealthResponse 健康检查响应结构
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message,omitempty"`
}

// ExampleInfoResponse 示例信息响应结构
type ExampleInfoResponse struct {
	Service   string   `json:"service"`
	Version   string   `json:"version"`
	Framework string   `json:"framework"`
	Endpoints []string `json:"endpoints"`
}

// HTTPClient ELK Coordinator HTTP API 客户端
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPClient 创建新的HTTP客户端
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// HealthCheck 健康检查
func (c *HTTPClient) HealthCheck() (*HealthResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/health")
	if err != nil {
		return nil, fmt.Errorf("健康检查请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("健康检查失败，状态码: %d", resp.StatusCode)
	}

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, fmt.Errorf("解析健康检查响应失败: %w", err)
	}

	return &health, nil
}

// GetExampleInfo 获取示例服务信息
func (c *HTTPClient) GetExampleInfo() (*ExampleInfoResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/example/info")
	if err != nil {
		return nil, fmt.Errorf("获取示例信息请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("获取示例信息失败，状态码: %d", resp.StatusCode)
	}

	var info ExampleInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("解析示例信息响应失败: %w", err)
	}

	return &info, nil
}

// RetryFailedPartitions 重试失败的分区
func (c *HTTPClient) RetryFailedPartitions(req *RetryRequest) (*RetryResponse, error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/api/v1/retry-failed-partitions",
		"application/json",
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("重试失败分区请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("重试失败分区请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
	}

	var retryResp RetryResponse
	if err := json.Unmarshal(body, &retryResp); err != nil {
		return nil, fmt.Errorf("解析重试响应失败: %w", err)
	}

	return &retryResp, nil
}

// demonstrateCurlCommands 演示curl命令调用
func demonstrateCurlCommands() {
	fmt.Println("=== CURL 命令示例 ===")
	fmt.Println()

	fmt.Println("1. 健康检查:")
	fmt.Println("curl -X GET http://localhost:8080/health")
	fmt.Println()

	fmt.Println("2. 获取示例服务信息:")
	fmt.Println("curl -X GET http://localhost:8080/example/info")
	fmt.Println()

	fmt.Println("3. 获取服务状态:")
	fmt.Println("curl -X GET http://localhost:8080/example/status")
	fmt.Println()

	fmt.Println("4. 重试所有失败分区 (立即模式):")
	fmt.Println(`curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "immediate"
  }'`)
	fmt.Println()

	fmt.Println("5. 重试指定的失败分区:")
	fmt.Println(`curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "partition_ids": [1, 2, 3],
    "mode": "immediate"
  }'`)
	fmt.Println()

	fmt.Println("6. 强制重试分区 (忽略版本检查):")
	fmt.Println(`curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "partition_ids": [1, 2],
    "mode": "immediate",
    "force": true
  }'`)
	fmt.Println()

	fmt.Println("7. 查看Prometheus指标 (如果启用了监控):")
	fmt.Println("curl -X GET http://localhost:8080/metrics")
	fmt.Println()
}

// demonstrateGoClient 演示Go客户端调用
func demonstrateGoClient() {
	fmt.Println("=== Go 客户端调用示例 ===")
	fmt.Println()

	// 创建客户端
	client := NewHTTPClient("http://localhost:8080")

	// 1. 健康检查
	fmt.Println("1. 执行健康检查...")
	health, err := client.HealthCheck()
	if err != nil {
		fmt.Printf("❌ 健康检查失败: %v\n", err)
	} else {
		fmt.Printf("✅ 健康检查成功: %+v\n", health)
	}
	fmt.Println()

	// 2. 获取示例信息
	fmt.Println("2. 获取示例服务信息...")
	info, err := client.GetExampleInfo()
	if err != nil {
		fmt.Printf("❌ 获取示例信息失败: %v\n", err)
	} else {
		fmt.Printf("✅ 示例信息获取成功:\n")
		fmt.Printf("   服务: %s\n", info.Service)
		fmt.Printf("   版本: %s\n", info.Version)
		fmt.Printf("   框架: %s\n", info.Framework)
		fmt.Printf("   端点: %v\n", info.Endpoints)
	}
	fmt.Println()

	// 3. 重试所有失败分区
	fmt.Println("3. 重试所有失败分区...")
	retryReq := &RetryRequest{
		Mode: "immediate",
	}
	retryResp, err := client.RetryFailedPartitions(retryReq)
	if err != nil {
		fmt.Printf("❌ 重试失败分区失败: %v\n", err)
	} else {
		fmt.Printf("✅ 重试操作完成:\n")
		fmt.Printf("   成功: %v\n", retryResp.Success)
		fmt.Printf("   消息: %s\n", retryResp.Message)
		fmt.Printf("   请求ID: %s\n", retryResp.RequestID)
		fmt.Printf("   时间戳: %s\n", retryResp.Timestamp)
	}
	fmt.Println()

	// 4. 重试指定分区
	fmt.Println("4. 重试指定分区...")
	retrySpecificReq := &RetryRequest{
		PartitionIDs: []int64{1, 2, 3},
		Mode:         "immediate",
	}
	retrySpecificResp, err := client.RetryFailedPartitions(retrySpecificReq)
	if err != nil {
		fmt.Printf("❌ 重试指定分区失败: %v\n", err)
	} else {
		fmt.Printf("✅ 重试指定分区完成:\n")
		fmt.Printf("   成功: %v\n", retrySpecificResp.Success)
		fmt.Printf("   消息: %s\n", retrySpecificResp.Message)
		fmt.Printf("   重试的分区: %v\n", retrySpecificResp.RetriedPartitions)
		fmt.Printf("   跳过的分区: %v\n", retrySpecificResp.SkippedPartitions)
	}
	fmt.Println()

	// 5. 强制重试
	fmt.Println("5. 强制重试分区...")
	forceRetryReq := &RetryRequest{
		PartitionIDs: []int64{4, 5},
		Mode:         "immediate",
		Force:        true,
	}
	forceRetryResp, err := client.RetryFailedPartitions(forceRetryReq)
	if err != nil {
		fmt.Printf("❌ 强制重试分区失败: %v\n", err)
	} else {
		fmt.Printf("✅ 强制重试完成:\n")
		fmt.Printf("   成功: %v\n", forceRetryResp.Success)
		fmt.Printf("   消息: %s\n", forceRetryResp.Message)
		fmt.Printf("   总失败分区数: %d\n", forceRetryResp.TotalFailedPartitions)
	}
	fmt.Println()
}

func main() {
	fmt.Println("=== ELK Coordinator HTTP API 客户端调用演示 ===")
	fmt.Println("此程序演示如何使用curl和Go客户端调用ELK Coordinator的HTTP API")
	fmt.Println()

	fmt.Println("⚠️  注意：请确保ELK Coordinator HTTP API服务已经在 http://localhost:8080 上运行")
	fmt.Println("   运行服务器: go run main.go (在 examples/http_api 目录)")
	fmt.Println()

	// 演示curl命令
	demonstrateCurlCommands()

	// 等待用户确认
	fmt.Println("按回车键继续执行Go客户端调用演示...")
	fmt.Scanln()

	// 演示Go客户端调用
	demonstrateGoClient()

	fmt.Println("=== 基础演示完成 ===")
	fmt.Println()
	fmt.Println("💡 提示:")
	fmt.Println("• 如果服务器未运行，Go客户端调用会失败")
	fmt.Println("• 可以复制上面的curl命令到终端中执行")
	fmt.Println("• 建议在服务器运行时运行此客户端程序")
	fmt.Println("• 可以修改客户端代码来测试不同的API参数")
	fmt.Println()
}
