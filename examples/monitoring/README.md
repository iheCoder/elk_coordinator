# ELK Coordinator 监控配置指南

## 方法1：直接查看 Prometheus 指标（最简单）

1. 确保你的 main 程序正在运行（应该在 `:8080` 端口提供监控服务）

2. 在浏览器中直接访问：
   - **Prometheus 指标**: http://localhost:8080/metrics
   - **健康检查**: http://localhost:8080/health

3. 你可以看到所有的监控指标，包括：
   ```
   # Leader 状态
   elk_coordinator_leader_is_leader{worker_id="hostname-12345-abcd1234"} 1
   
   # 活跃工作节点数
   elk_coordinator_active_workers_total 1
   
   # 已处理任务数
   elk_coordinator_tasks_processed_total{worker_id="hostname-12345-abcd1234"} 5
   
   # 任务处理耗时
   elk_coordinator_task_processing_duration_seconds_bucket{worker_id="hostname-12345-abcd1234",le="0.1"} 0
   elk_coordinator_task_processing_duration_seconds_bucket{worker_id="hostname-12345-abcd1234",le="1"} 0
   elk_coordinator_task_processing_duration_seconds_bucket{worker_id="hostname-12345-abcd1234",le="2.5"} 5
   
   # 熔断器状态
   elk_coordinator_circuit_breaker_state{worker_id="hostname-12345-abcd1234",breaker_name="process_breaker"} 0
   ```

## 方法2：使用 Grafana Dashboard（推荐）

### 前提条件
确保 Docker Desktop 已安装并运行：
- 下载安装 Docker Desktop for Mac: https://docs.docker.com/desktop/install/mac-install/
- 启动 Docker Desktop

### 启动步骤

1. **启动 ELK Coordinator 监控程序**：
   ```bash
   cd /Users/ihewe/GolandProjects/elk_coordinator/examples/monitoring
   go run main.go
   ```

2. **启动 Prometheus 和 Grafana**：
   ```bash
   cd /Users/ihewe/GolandProjects/elk_coordinator/examples/monitoring
   docker-compose up -d
   ```

3. **访问 Grafana Dashboard**：
   - 打开浏览器访问：http://localhost:3000
   - 默认用户名/密码：`admin/admin`
   - 自动加载 "ELK Coordinator Monitoring" 仪表板

### Grafana 仪表板功能

仪表板包含以下监控面板：

1. **Leader Status** - 显示当前 Leader 状态
2. **Active Workers** - 活跃工作节点数量
3. **Task Processing Rate** - 任务处理速率（每秒）
4. **Task Processing Duration** - 任务处理耗时分布
5. **Circuit Breaker State** - 熔断器状态监控
6. **Task Error Rate** - 任务错误率

### 监控指标说明

| 指标名称 | 说明 | 值含义 |
|---------|------|--------|
| `elk_coordinator_leader_is_leader` | Leader 状态 | 1=是Leader, 0=不是Leader |
| `elk_coordinator_active_workers_total` | 活跃工作节点数 | 当前集群中活跃的工作节点数量 |
| `elk_coordinator_tasks_processed_total` | 已处理任务数 | 累计处理的任务数量 |
| `elk_coordinator_tasks_errors_total` | 任务错误数 | 累计发生的任务错误数 |
| `elk_coordinator_task_processing_duration_seconds` | 任务处理耗时 | 任务处理时间分布（直方图） |
| `elk_coordinator_circuit_breaker_state` | 熔断器状态 | 0=关闭, 1=开启, 2=半开 |
| `elk_coordinator_heartbeat_timestamp` | 心跳时间戳 | 最后一次心跳的时间戳 |

## 方法3：使用 curl 命令查看指标

如果你只想快速查看指标，可以使用 curl：

```bash
# 查看所有指标
curl http://localhost:8080/metrics

# 查看特定指标（Leader 状态）
curl http://localhost:8080/metrics | grep elk_coordinator_leader_is_leader

# 查看任务处理指标
curl http://localhost:8080/metrics | grep elk_coordinator_tasks_processed

# 查看熔断器状态
curl http://localhost:8080/metrics | grep elk_coordinator_circuit_breaker_state
```

## 故障排除

### 1. 无法访问 http://localhost:8080/metrics
- 检查 main 程序是否正在运行
- 检查是否有其他程序占用 8080 端口：`lsof -i :8080`

### 2. Docker 相关问题
- 确保 Docker Desktop 已启动
- 如果端口冲突，修改 `docker-compose.yml` 中的端口映射

### 3. Grafana 无法连接 Prometheus
- 检查 Prometheus 是否在 9090 端口运行：http://localhost:9090
- 检查 Docker 容器网络连接：`docker-compose logs`

## 清理资源

停止所有服务：
```bash
# 停止 Docker 服务
docker-compose down

# 停止 main 程序（Ctrl+C）
```
