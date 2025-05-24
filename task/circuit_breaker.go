package task

import (
	"sync"
	"time"
)

// CircuitBreakerConfig 熔断器配置
type CircuitBreakerConfig struct {
	ConsecutiveFailureThreshold int           // 连续失败触发熔断阈值
	TotalFailureThreshold       int           // 总失败分区数阈值
	OpenTimeout                 time.Duration // 熔断开启后等待恢复的时间
}

// CircuitBreakerState 熔断器状态
const (
	CBStateClosed   = "closed"   // 正常
	CBStateOpen     = "open"     // 熔断
	CBStateHalfOpen = "halfopen" // 半开
)

// CircuitBreaker 熔断器结构体
// 仅实现核心功能，暂不集成到Runner
// 支持并发安全
type CircuitBreaker struct {
	config              CircuitBreakerConfig
	state               string
	consecutiveFailures int
	totalFailures       int
	failedPartitions    map[int]error // 记录失败分区及错误
	lastStateChange     time.Time
	mu                  sync.Mutex
}

// NewCircuitBreaker 创建熔断器实例
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config:           cfg,
		state:            CBStateClosed,
		failedPartitions: make(map[int]error),
		lastStateChange:  time.Now(),
	}
}

// RecordFailure 记录分区处理失败
func (cb *CircuitBreaker) RecordFailure(partitionID int, err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures++
	cb.totalFailures++
	cb.failedPartitions[partitionID] = err

	if cb.state == CBStateClosed && cb.consecutiveFailures >= cb.config.ConsecutiveFailureThreshold {
		cb.state = CBStateOpen
		cb.lastStateChange = time.Now()
	}

	if cb.totalFailures >= cb.config.TotalFailureThreshold {
		cb.state = CBStateOpen
		cb.lastStateChange = time.Now()
	}
}

// RecordSuccess 记录成功处理，重置连续失败计数
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFailures = 0
	if cb.state == CBStateHalfOpen {
		cb.state = CBStateClosed
		cb.lastStateChange = time.Now()
	}
}

// AllowRequest 判断当前是否允许处理新任务
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CBStateClosed:
		return true
	case CBStateOpen:
		if time.Since(cb.lastStateChange) > cb.config.OpenTimeout {
			cb.state = CBStateHalfOpen
			cb.lastStateChange = time.Now()
			return true
		}
		return false
	case CBStateHalfOpen:
		return true
	default:
		return true
	}
}

// State 获取当前熔断器状态
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// FailedPartitions 获取所有失败分区
func (cb *CircuitBreaker) FailedPartitions() map[int]error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	result := make(map[int]error, len(cb.failedPartitions))
	for k, v := range cb.failedPartitions {
		result[k] = v
	}
	return result
}

// Reset 重置熔断器状态
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CBStateClosed
	cb.consecutiveFailures = 0
	cb.totalFailures = 0
	cb.failedPartitions = make(map[int]error)
	cb.lastStateChange = time.Now()
}
