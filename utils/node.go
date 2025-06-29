package utils

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"time"
)

// GenerateNodeID 生成唯一的节点ID
func GenerateNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String()[:8])
}

// CalculateWorkerScore 计算worker在ZSET中的score
// score = registerTime的Unix纳秒时间戳
// 这个算法确保按注册时间排序（精确到纳秒）
func CalculateWorkerScore(workerID string, registerTime time.Time) float64 {
	// 直接使用注册时间的Unix纳秒时间戳作为分数
	return float64(registerTime.UnixNano())
}
