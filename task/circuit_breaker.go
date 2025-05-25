package task

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerConfig 熔断器配置
type CircuitBreakerConfig struct {
	ConsecutiveFailureThreshold int           // 连续失败触发熔断阈值
	TotalFailureThreshold       int           // 失败分区数阈值
	OpenTimeout                 time.Duration // 熔断开启后等待恢复的时间
	MaxHalfOpenRequests         int32         // Half-Open状态下最大探测请求数
	FailureTimeWindow           time.Duration // 失败统计时间窗口
}

// CircuitBreakerState 熔断器状态
const (
	CBStateClosed   = "closed"   // 正常
	CBStateOpen     = "open"     // 熔断
	CBStateHalfOpen = "halfopen" // 半开
)

// PartitionFailure 分区失败记录
type PartitionFailure struct {
	Error     error
	Timestamp time.Time
}

// CircuitBreakerStats 熔断器统计信息
type CircuitBreakerStats struct {
	State               string    `json:"state"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	TotalFailures       int       `json:"total_failures"`
	FailedPartitions    int       `json:"failed_partitions"`
	HalfOpenRequests    int32     `json:"half_open_requests"`
	LastStateChange     time.Time `json:"last_state_change"`
}

// CircuitBreaker 熔断器结构体
// 仅实现核心功能，暂不集成到Runner
// 支持并发安全
type CircuitBreaker struct {
	config              CircuitBreakerConfig
	state               string
	consecutiveFailures int
	totalFailures       int                      // 当前失败分区数量
	failedPartitions    map[int]PartitionFailure // 记录失败分区及错误和时间
	lastStateChange     time.Time
	halfOpenRequests    int32 // 正在进行的探测请求数
	mu                  sync.Mutex
}

// NewCircuitBreaker 创建熔断器实例
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	// 设置默认值
	if cfg.MaxHalfOpenRequests <= 0 {
		cfg.MaxHalfOpenRequests = 1
	}
	if cfg.FailureTimeWindow <= 0 {
		cfg.FailureTimeWindow = 5 * time.Minute
	}

	return &CircuitBreaker{
		config:           cfg,
		state:            CBStateClosed,
		failedPartitions: make(map[int]PartitionFailure),
		lastStateChange:  time.Now(),
	}
}

// RecordFailure 记录分区处理失败
func (cb *CircuitBreaker) RecordFailure(partitionID int, err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// 清理过期的失败记录
	cb.cleanExpiredFailures()

	// 记录失败
	wasNew := false
	if _, exists := cb.failedPartitions[partitionID]; !exists {
		cb.totalFailures++
		wasNew = true
	}
	cb.failedPartitions[partitionID] = PartitionFailure{
		Error:     err,
		Timestamp: time.Now(),
	}

	// 只有新的失败才增加连续失败计数
	if wasNew {
		cb.consecutiveFailures++
	}

	// 如果在Half-Open状态下失败，直接转为Open
	if cb.state == CBStateHalfOpen {
		cb.state = CBStateOpen
		cb.lastStateChange = time.Now()
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
		return
	}

	// 检查是否需要触发熔断
	if cb.state == CBStateClosed {
		if cb.consecutiveFailures >= cb.config.ConsecutiveFailureThreshold ||
			cb.totalFailures >= cb.config.TotalFailureThreshold {
			cb.state = CBStateOpen
			cb.lastStateChange = time.Now()
		}
	}
}

// RecordSuccess 记录成功处理，重置连续失败计数
func (cb *CircuitBreaker) RecordSuccess(partitionID int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// 清理该分区的失败记录
	if _, exists := cb.failedPartitions[partitionID]; exists {
		delete(cb.failedPartitions, partitionID)
		cb.totalFailures--
	}

	// 重置连续失败计数
	cb.consecutiveFailures = 0

	// 如果在Half-Open状态下成功，转为Closed
	if cb.state == CBStateHalfOpen {
		atomic.AddInt32(&cb.halfOpenRequests, -1)
		cb.state = CBStateClosed
		cb.lastStateChange = time.Now()
		// 成功恢复时，清理所有历史失败记录
		cb.failedPartitions = make(map[int]PartitionFailure)
		cb.totalFailures = 0
	}
}

// AllowProcess 判断当前是否允许处理新任务
func (cb *CircuitBreaker) AllowProcess() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// 清理过期的失败记录
	cb.cleanExpiredFailures()

	switch cb.state {
	case CBStateClosed:
		return true
	case CBStateOpen:
		if time.Since(cb.lastStateChange) > cb.config.OpenTimeout {
			cb.state = CBStateHalfOpen
			cb.lastStateChange = time.Now()
			atomic.StoreInt32(&cb.halfOpenRequests, 1)
			return true
		}
		return false
	case CBStateHalfOpen:
		if atomic.LoadInt32(&cb.halfOpenRequests) < cb.config.MaxHalfOpenRequests {
			atomic.AddInt32(&cb.halfOpenRequests, 1)
			return true
		}
		return false
	default:
		return true
	}
}

// cleanExpiredFailures 清理过期的失败记录（需要在持有锁时调用）
func (cb *CircuitBreaker) cleanExpiredFailures() {
	if cb.config.FailureTimeWindow <= 0 {
		return
	}

	now := time.Now()
	expiredPartitions := make([]int, 0)

	for partitionID, failure := range cb.failedPartitions {
		if now.Sub(failure.Timestamp) > cb.config.FailureTimeWindow {
			expiredPartitions = append(expiredPartitions, partitionID)
		}
	}

	for _, partitionID := range expiredPartitions {
		delete(cb.failedPartitions, partitionID)
		cb.totalFailures--
		if cb.consecutiveFailures > 0 {
			cb.consecutiveFailures--
		}
	}

	// 如果在清理后，熔断器处于Open状态，但连续失败数和总失败数都低于阈值，则重置为Closed状态
	if cb.state == CBStateOpen &&
		cb.consecutiveFailures < cb.config.ConsecutiveFailureThreshold &&
		cb.totalFailures < cb.config.TotalFailureThreshold {
		cb.state = CBStateClosed
		cb.lastStateChange = time.Now()
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

	// 清理过期的失败记录
	cb.cleanExpiredFailures()

	result := make(map[int]error, len(cb.failedPartitions))
	for k, v := range cb.failedPartitions {
		result[k] = v.Error
	}
	return result
}

// GetStats 获取熔断器统计信息
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// 清理过期的失败记录
	cb.cleanExpiredFailures()

	return CircuitBreakerStats{
		State:               cb.state,
		ConsecutiveFailures: cb.consecutiveFailures,
		TotalFailures:       cb.totalFailures,
		FailedPartitions:    len(cb.failedPartitions),
		HalfOpenRequests:    atomic.LoadInt32(&cb.halfOpenRequests),
		LastStateChange:     cb.lastStateChange,
	}
}

// Reset 重置熔断器状态
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CBStateClosed
	cb.consecutiveFailures = 0
	cb.totalFailures = 0
	cb.failedPartitions = make(map[int]PartitionFailure)
	cb.lastStateChange = time.Now()
	atomic.StoreInt32(&cb.halfOpenRequests, 0)
}

// WaitUntilAllowed 等待直到熔断器允许执行，或者上下文取消
// 如果熔断器已经允许执行，立即返回nil
// 如果熔断器处于开启状态，阻塞等待直到变为半开或关闭状态
// 如果上下文被取消，返回上下文的错误
func (cb *CircuitBreaker) WaitUntilAllowed(ctx context.Context) error {
	// 立即检查是否允许执行
	if cb.AllowProcess() {
		return nil
	}

	// 设置默认的等待间隔时间
	retryInterval := 500 * time.Millisecond
	if cb.config.OpenTimeout > 0 {
		// 使用OpenTimeout的1/10作为重试间隔，最少100ms，最多1秒
		calculatedInterval := cb.config.OpenTimeout / 10
		if calculatedInterval < 100*time.Millisecond {
			calculatedInterval = 100 * time.Millisecond
		} else if calculatedInterval > time.Second {
			calculatedInterval = time.Second
		}
		retryInterval = calculatedInterval
	}

	// 定期检查熔断器状态直到允许或上下文取消
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cb.AllowProcess() {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
