package task

import (
	"elk_coordinator/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestOptimalTryCountCalculation 测试最优尝试次数计算算法
func TestOptimalTryCountCalculation(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	tests := []struct {
		name             string
		totalPartitions  int
		expectedTryCount int
	}{
		{"极少分区全部尝试", 3, 3},
		{"较少分区尝试75%", 8, 6},      // (8*3+3)/4 = 6
		{"中等分区尝试一半多", 20, 10},    // (20+1)/2 = 10
		{"大量分区限制尝试次数", 100, 20},  // min(100/2, 20) = 20
		{"超大量分区限制尝试次数", 200, 20}, // min(200/2, 20) = 20
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := runner.calculateOptimalTryCount(tt.totalPartitions)
			assert.Equal(t, tt.expectedTryCount, actual)
		})
	}
}

// TestWorkerIDHashingConsistency 测试workerID哈希的一致性
func TestWorkerIDHashingConsistency(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 测试同一workerID总是产生相同hash
	workerID := "test-worker-123"
	hash1 := runner.hashWorkerID(workerID)
	hash2 := runner.hashWorkerID(workerID)
	assert.Equal(t, hash1, hash2, "同一workerID应该产生相同的hash值")

	// 测试不同workerID产生不同hash
	workerID2 := "test-worker-456"
	hash3 := runner.hashWorkerID(workerID2)
	assert.NotEqual(t, hash1, hash3, "不同workerID应该产生不同的hash值")
}
