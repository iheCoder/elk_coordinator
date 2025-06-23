# ELK Coordinator HTTP API 示例

这个示例展示了如何使用ELK Coordinator的HTTP API功能，包含完整的服务器和客户端调用演示。

## 功能特性

### 1. HTTP API 服务器
- **健康检查**: GET /health
- **重试失败分区**: POST /api/v1/retry-failed-partitions  
- **示例端点**: GET /example/info, GET /example/status
- **使用实际Redis**: 不使用mock，真实的Redis数据存储

### 2. 客户端调用演示
- **curl命令示例**: 展示如何使用curl调用API
- **Go客户端**: 完整的Go HTTP客户端实现
- **错误处理**: 完善的错误处理和响应解析

## 目录结构

```
examples/http_api/
├── main.go              # HTTP API 服务器
├── client/
│   └── main.go          # 客户端调用演示
└── README.md            # 本文档
```

## 快速开始

### 1. 启动Redis服务器
```bash
# 使用Docker启动Redis
docker run -d -p 6379:6379 redis:alpine

# 或者使用本地Redis
redis-server
```

### 2. 启动HTTP API服务器
```bash
cd examples/http_api
go run main.go
```

服务器将在以下端口启动：
- HTTP API: http://localhost:8080
- 监控指标: http://localhost:8080/metrics (如果启用)

### 3. 运行客户端演示
```bash
cd examples/http_api/client
go run main.go
```

## API 端点详解

### 健康检查
**端点**: `GET /health`
**描述**: 检查服务健康状态

**curl示例**:
```bash
curl -X GET http://localhost:8080/health
```

**响应示例**:
```json
{
  "status": "healthy",
  "timestamp": 1703475600
}
```
### 重试失败分区
**端点**: `POST /api/v1/retry-failed-partitions`
**描述**: 重试处理失败的分区

**请求体结构**:
```json
{
  "partition_ids": [1, 2, 3],  // 可选：指定要重试的分区ID
  "mode": "immediate",         // 重试模式：immediate 或 scheduled
  "force": false               // 可选：是否强制重试（忽略版本检查）
}
```

**curl示例**:
```bash
# 重试所有失败分区
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "immediate"
  }'

# 重试指定分区
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "partition_ids": [1, 2, 3],
    "mode": "immediate"
  }'

# 强制重试
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{
    "partition_ids": [1, 2],
    "mode": "immediate",
    "force": true
  }'
```

**响应示例**:
```json
{
  "success": true,
  "message": "重试操作成功完成",
  "total_failed_partitions": 3,
  "retried_partitions": [1, 2, 3],
  "skipped_partitions": [],
  "request_id": "retry-1671234567890-abc123",
  "timestamp": "2023-12-15T10:30:00Z"
}
```

### 服务信息
**端点**: `GET /example/info`
**描述**: 获取服务基本信息

**curl示例**:
```bash
curl -X GET http://localhost:8080/example/info
```

**响应示例**:
```json
{
  "service": "elk-coordinator",
  "version": "1.0.0",
  "framework": "gin",
  "endpoints": [
    "GET /health - 健康检查",
    "POST /api/v1/retry-failed-partitions - 重试失败分区",
    "GET /example/info - 服务信息",
    "GET /example/status - 服务状态"
  ]
}
```

### 服务状态
**端点**: `GET /example/status`  
**描述**: 获取服务运行状态

**curl示例**:
```bash
curl -X GET http://localhost:8080/example/status
```

**响应示例**:
```json
{
  "status": "running",
  "timestamp": 1703475600,
  "uptime": "运行中"
}
```

## Go 客户端使用示例

客户端演示代码在 `client/main.go` 中，包含以下功能：

### 1. 创建HTTP客户端
```go
client := NewHTTPClient("http://localhost:8080")
```

### 2. 健康检查
```go
health, err := client.HealthCheck()
if err != nil {
    log.Printf("健康检查失败: %v", err)
} else {
    log.Printf("健康状态: %+v", health)
}
```

### 3. 重试失败分区
```go
// 重试所有失败分区
retryReq := &RetryRequest{
    Mode: "immediate",
}
retryResp, err := client.RetryFailedPartitions(retryReq)

// 重试指定分区
retryReq := &RetryRequest{
    PartitionIDs: []int64{1, 2, 3},
    Mode:         "immediate",
}
retryResp, err := client.RetryFailedPartitions(retryReq)

// 强制重试
retryReq := &RetryRequest{
    PartitionIDs: []int64{1, 2},
    Mode:         "immediate", 
    Force:        true,
}
retryResp, err := client.RetryFailedPartitions(retryReq)
```

## 完整的curl命令参考

```bash
# 1. 健康检查
curl -X GET http://localhost:8080/health

# 2. 获取服务信息
curl -X GET http://localhost:8080/example/info

# 3. 获取服务状态
curl -X GET http://localhost:8080/example/status

# 4. 重试所有失败分区
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{"mode": "immediate"}'

# 5. 重试指定分区
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{"partition_ids": [1, 2, 3], "mode": "immediate"}'

# 6. 强制重试分区
curl -X POST http://localhost:8080/api/v1/retry-failed-partitions \
  -H "Content-Type: application/json" \
  -d '{"partition_ids": [1, 2], "mode": "immediate", "force": true}'

# 7. 查看Prometheus指标 (如果启用监控)
curl -X GET http://localhost:8080/metrics
```

## API端点

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/health` | 健康检查 |
| POST | `/api/v1/retry-failed-partitions` | 重试失败分区 |
| GET | `/example/info` | 服务信息 |
| GET | `/example/status` | 服务状态 |

## 请求示例

### 重试失败分区

**请求**:
```json
{
    "partition_ids": [1, 2, 3]
}
```

**响应**:
```json
{
    "success": true,
    "message": "重试 3 个指定分区的命令已提交",
    "command_id": "cmd-12345"
}
```

### 重试所有失败分区

**请求**:
```json
{
    "partition_ids": []
}
```

**响应**:
```json
{
    "success": true,
    "message": "重试所有失败分区的命令已提交",
    "command_id": "cmd-67890"
}
```

## 配置选项

### 环境变量
- `REDIS_ADDR`: Redis服务器地址 (默认: localhost:6379)

### 服务器配置
```go
mgr := elk_coordinator.NewMgr(
    namespace,
    dataStore,
    processor,
    planer,
    model.StrategyTypeSimple,
    // HTTP API配置
    elk_coordinator.WithHTTPServer(true, ":8081"),
    elk_coordinator.WithMetricsEnabled(true),
    elk_coordinator.WithMetricsAddr(":8080"),
    // 时间配置
    elk_coordinator.WithAllocationInterval(30*time.Second),
    elk_coordinator.WithPartitionLockExpiry(5*time.Minute),
    elk_coordinator.WithLeaderLockExpiry(2*time.Minute),
    elk_coordinator.WithHeartbeatInterval(30*time.Second),
)
```

## 故障排除

### 1. Redis连接失败
```
ERROR: Redis 连接失败: dial tcp 127.0.0.1:6379: connect: connection refused
```
**解决方案**:
- 确保Redis服务器正在运行
- 检查Redis地址配置
- 使用环境变量设置Redis地址: `export REDIS_ADDR=localhost:6379`

### 2. 端口被占用
```
ERROR: listen tcp :8080: bind: address already in use
```
**解决方案**:
- 更改端口配置
- 或者停止占用端口的进程: `lsof -ti:8080 | xargs kill -9`

### 3. HTTP客户端超时
```
ERROR: 健康检查请求失败: context deadline exceeded
```
**解决方案**:
- 确保服务器正在运行
- 检查网络连接
- 增加客户端超时时间

### 4. JSON解析错误
```
ERROR: 解析重试响应失败: invalid character 'H' looking for beginning of value
```
**解决方案**:
- 检查服务器是否返回正确的JSON格式
- 验证请求的Content-Type头部

## 最佳实践

### 1. 错误处理
```go
resp, err := client.RetryFailedPartitions(req)
if err != nil {
    // 区分不同类型的错误
    if strings.Contains(err.Error(), "connection refused") {
        log.Printf("服务器未运行: %v", err)
    } else if strings.Contains(err.Error(), "timeout") {
        log.Printf("请求超时: %v", err)  
    } else {
        log.Printf("其他错误: %v", err)
    }
    return
}

if !resp.Success {
    log.Printf("API调用失败: %s", resp.Message)
    return
}
```

### 2. 重试机制
```go
func retryWithBackoff(client *HTTPClient, req *RetryRequest, maxRetries int) (*RetryResponse, error) {
    var lastErr error
    for i := 0; i < maxRetries; i++ {
        resp, err := client.RetryFailedPartitions(req)
        if err == nil {
            return resp, nil
        }
        
        lastErr = err
        backoff := time.Duration(i+1) * time.Second
        log.Printf("重试 %d/%d 失败，%v后重试: %v", i+1, maxRetries, backoff, err)
        time.Sleep(backoff)
    }
    return nil, lastErr
}
```

### 3. 超时控制
```go
client := &HTTPClient{
    baseURL: "http://localhost:8080",
    httpClient: &http.Client{
        Timeout: 30 * time.Second,
        Transport: &http.Transport{
            MaxIdleConns:       10,
            IdleConnTimeout:    30 * time.Second,
            DisableCompression: true,
        },
    },
}
```

## 扩展功能

### 1. 添加认证
```go
// 在客户端请求中添加认证头
func (c *HTTPClient) addAuth(req *http.Request) {
    req.Header.Set("Authorization", "Bearer "+c.token)
}
```

### 2. 请求日志
```go
// 添加请求响应日志
func (c *HTTPClient) logRequest(req *http.Request, resp *http.Response, duration time.Duration) {
    log.Printf("%s %s -> %d (%v)", req.Method, req.URL.Path, resp.StatusCode, duration)
}
```

### 3. 指标收集
```go
// 收集客户端指标
type ClientMetrics struct {
    requestCount    int64
    errorCount      int64
    avgResponseTime time.Duration
}
```

## 部署建议

### 1. 生产环境配置
```bash
# 设置环境变量
export REDIS_ADDR=redis-cluster:6379
export HTTP_PORT=8080
export METRICS_PORT=9090

# 使用Docker运行
docker run -d \
  -e REDIS_ADDR=$REDIS_ADDR \
  -p $HTTP_PORT:8080 \
  -p $METRICS_PORT:9090 \
  elk-coordinator-http-api
```

### 2. 负载均衡
```nginx
upstream elk_coordinator {
    server 127.0.0.1:8080;
    server 127.0.0.1:8081; 
    server 127.0.0.1:8082;
}

server {
    listen 80;
    location /api/ {
        proxy_pass http://elk_coordinator;
    }
}
```

### 3. 监控告警
```yaml
# Prometheus告警规则
groups:
- name: elk-coordinator
  rules:
  - alert: ELKCoordinatorDown
    expr: up{job="elk-coordinator"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "ELK Coordinator服务下线"
```

## 相关文档

- [ELK Coordinator 核心文档](../../README.md)
- [监控示例](../monitoring/README.md) 
- [API设计文档](../../docs/http_api.md)
- [性能测试报告](../../docs/performance_summary.md)
