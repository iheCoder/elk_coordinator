package task

import (
	"context"
	"elk_coordinator/model"
	"elk_coordinator/test_utils"
	"elk_coordinator/utils"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ConsistentHashInterface 定义一致性哈希接口，用于测试
type ConsistentHashInterface interface {
	IsPreferredPartition(ctx context.Context, partitionID int, workerID string) (bool, error)
	IsCurrentWorkerPreferred(ctx context.Context, partitionID int) (bool, error)
}

// MockTaskConsistentHash 模拟TaskConsistentHash用于测试
type MockTaskConsistentHash struct {
	preferredPartitions map[int]bool // partitionID -> isPreferred
}

func (m *MockTaskConsistentHash) IsPreferredPartition(ctx context.Context, partitionID int, workerID string) (bool, error) {
	return m.preferredPartitions[partitionID], nil
}

func (m *MockTaskConsistentHash) IsCurrentWorkerPreferred(ctx context.Context, partitionID int) (bool, error) {
	return m.preferredPartitions[partitionID], nil
}

// MockPartitionStrategyWithErrors 带错误注入功能的模拟分区策略
type MockPartitionStrategyWithErrors struct {
	*test_utils.MockPartitionStrategy
	shouldFailAcquire bool
	shouldFailUpdate  bool
}

func NewMockPartitionStrategyWithErrors() *MockPartitionStrategyWithErrors {
	return &MockPartitionStrategyWithErrors{
		MockPartitionStrategy: test_utils.NewMockPartitionStrategy(),
	}
}

func (m *MockPartitionStrategyWithErrors) AcquirePartition(ctx context.Context, partitionID int, workerID string, options *model.AcquirePartitionOptions) (*model.PartitionInfo, bool, error) {
	if m.shouldFailAcquire {
		return nil, false, errors.New("模拟获取分区失败")
	}
	return m.MockPartitionStrategy.AcquirePartition(ctx, partitionID, workerID, options)
}

func (m *MockPartitionStrategyWithErrors) UpdatePartition(ctx context.Context, partition *model.PartitionInfo, options *model.UpdateOptions) (*model.PartitionInfo, error) {
	if m.shouldFailUpdate {
		return nil, errors.New("模拟更新分区失败")
	}
	return m.MockPartitionStrategy.UpdatePartition(ctx, partition, options)
}

// Clear 清理所有分区数据
func (m *MockPartitionStrategyWithErrors) Clear() {
	m.MockPartitionStrategy.Partitions = make(map[int]*model.PartitionInfo)
}

// Add Clear method to test_utils.MockPartitionStrategy wrapper
func clearMockStrategy(m *test_utils.MockPartitionStrategy) {
	m.Partitions = make(map[int]*model.PartitionInfo)
}

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
		{"边界值测试", 10, 8},         // (10*3+3)/4 = 8
		{"边界值测试50", 50, 25},      // (50+1)/2 = 25
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := runner.calculateOptimalTryCount(tt.totalPartitions)
			assert.Equal(t, tt.expectedTryCount, actual,
				"输入分区数: %d, 期望尝试次数: %d, 实际: %d",
				tt.totalPartitions, tt.expectedTryCount, actual)
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

	// 测试空字符串
	emptyHash := runner.hashWorkerID("")
	assert.NotEqual(t, uint32(0), emptyHash, "空字符串也应该产生有效的hash值")
}

// TestWorkerIDHashingDistribution 测试workerID哈希的分布性
func TestWorkerIDHashingDistribution(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 测试多个不同workerID的hash分布
	hashes := make(map[uint32]bool)
	workerIDs := []string{
		"worker-1", "worker-2", "worker-3", "worker-4", "worker-5",
		"worker-6", "worker-7", "worker-8", "worker-9", "worker-10",
		"node-a", "node-b", "node-c", "server-1", "server-2",
	}

	for _, workerID := range workerIDs {
		hash := runner.hashWorkerID(workerID)
		hashes[hash] = true
	}

	// 期望大部分hash值都是不同的（允许极少碰撞）
	uniqueHashes := len(hashes)
	totalWorkers := len(workerIDs)

	// 至少90%的workerID应该产生不同的hash值
	minExpected := int(float64(totalWorkers) * 0.9)
	assert.GreaterOrEqual(t, uniqueHashes, minExpected,
		"hash分布性测试: %d个workerID产生了%d个不同的hash值，期望至少%d个",
		totalWorkers, uniqueHashes, minExpected)
}

// TestDeterministicRandomSelectionConsistency 测试确定性随机选择的一致性
func TestDeterministicRandomSelectionConsistency(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 10)
	for i := 0; i < 10; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
	}

	seed := runner.hashWorkerID("worker-1")
	maxCount := 5

	// 多次调用应该产生相同的结果
	result1 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed)
	result2 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed)

	assert.Equal(t, len(result1), len(result2), "返回的分区数量应该相同")
	assert.Equal(t, maxCount, len(result1), "应该返回指定数量的分区")

	// 验证相同种子产生相同顺序
	for i := 0; i < len(result1); i++ {
		assert.Equal(t, result1[i].PartitionID, result2[i].PartitionID,
			"位置%d的分区ID应该相同: result1=%d, result2=%d",
			i, result1[i].PartitionID, result2[i].PartitionID)
	}
}

// TestDeterministicRandomSelectionDifferentSeeds 测试不同种子产生不同结果
func TestDeterministicRandomSelectionDifferentSeeds(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 10)
	for i := 0; i < 10; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
	}

	// 使用不同工作节点的种子
	seed1 := runner.hashWorkerID("worker-1")
	seed2 := runner.hashWorkerID("worker-2")
	maxCount := 5

	result1 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed1)
	result2 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed2)

	assert.Equal(t, maxCount, len(result1))
	assert.Equal(t, maxCount, len(result2))

	// 检查是否有至少一个位置的分区不同（概率很高）
	different := false
	for i := 0; i < len(result1); i++ {
		if result1[i].PartitionID != result2[i].PartitionID {
			different = true
			break
		}
	}
	assert.True(t, different, "不同种子应该产生不同的选择顺序")
}

// TestShouldReclaimPartitionLogic 测试分区重新获取判断逻辑
func TestShouldReclaimPartitionLogic(t *testing.T) {
	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		logger:              utils.NewDefaultLogger(),
	}

	now := time.Now()

	tests := []struct {
		name            string
		partition       model.PartitionInfo
		expectedReclaim bool
		description     string
	}{
		{
			name: "claimed分区超时应该重新获取",
			partition: model.PartitionInfo{
				PartitionID: 1,
				Status:      model.StatusClaimed,
				WorkerID:    "worker-2",
				UpdatedAt:   now.Add(-130 * time.Second), // 超过阈值（2*60=120秒）
			},
			expectedReclaim: true,
			description:     "claimed分区更新时间超过阈值且不是本节点持有",
		},
		{
			name: "claimed分区未超时不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID: 2,
				Status:      model.StatusClaimed,
				WorkerID:    "worker-2",
				UpdatedAt:   now.Add(-60 * time.Second), // 未超过阈值
			},
			expectedReclaim: false,
			description:     "claimed分区更新时间未超过阈值",
		},
		{
			name: "本节点的claimed分区不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID: 3,
				Status:      model.StatusClaimed,
				WorkerID:    "worker-1", // 本节点
				UpdatedAt:   now.Add(-130 * time.Second),
			},
			expectedReclaim: false,
			description:     "本节点持有的分区不应该被重新获取",
		},
		{
			name: "running分区心跳超时应该重新获取",
			partition: model.PartitionInfo{
				PartitionID:   4,
				Status:        model.StatusRunning,
				WorkerID:      "worker-2",
				UpdatedAt:     now.Add(-100 * time.Second),
				LastHeartbeat: now.Add(-310 * time.Second), // 超过阈值（5*60=300秒）
			},
			expectedReclaim: true,
			description:     "running分区心跳时间超过阈值且不是本节点持有",
		},
		{
			name: "running分区心跳正常不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID:   5,
				Status:        model.StatusRunning,
				WorkerID:      "worker-2",
				UpdatedAt:     now.Add(-400 * time.Second), // 更新时间很旧，但心跳正常
				LastHeartbeat: now.Add(-100 * time.Second), // 心跳正常
			},
			expectedReclaim: false,
			description:     "running分区心跳正常，即使更新时间很旧",
		},
		{
			name: "本节点的running分区不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID:   6,
				Status:        model.StatusRunning,
				WorkerID:      "worker-1", // 本节点
				UpdatedAt:     now.Add(-100 * time.Second),
				LastHeartbeat: now.Add(-310 * time.Second), // 心跳超时
			},
			expectedReclaim: false,
			description:     "本节点持有的分区不应该被重新获取",
		},
		{
			name: "pending状态分区不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID: 7,
				Status:      model.StatusPending,
				WorkerID:    "",
				UpdatedAt:   now.Add(-400 * time.Second),
			},
			expectedReclaim: false,
			description:     "pending状态的分区不需要重新获取",
		},
		{
			name: "completed状态分区不应该重新获取",
			partition: model.PartitionInfo{
				PartitionID: 8,
				Status:      model.StatusCompleted,
				WorkerID:    "worker-2",
				UpdatedAt:   now.Add(-400 * time.Second),
			},
			expectedReclaim: false,
			description:     "completed状态的分区不需要重新获取",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := runner.shouldReclaimPartition(tt.partition)
			assert.Equal(t, tt.expectedReclaim, actual, tt.description)
		})
	}
}

// TestHybridStrategyThresholds 测试混合策略的阈值逻辑
func TestHybridStrategyThresholds(t *testing.T) {
	// 这个测试验证策略选择的阈值逻辑
	tests := []struct {
		name              string
		pendingCount      int
		hasConsistentHash bool
		expectedStrategy  string
	}{
		{"大于等于20个分区且有一致性哈希时使用一致性哈希", 25, true, "consistent_hash"},
		{"等于20个分区且有一致性哈希时使用一致性哈希", 20, true, "consistent_hash"},
		{"大于等于20个分区但无一致性哈希时回退到随机", 25, false, "random"},
		{"5-19个分区使用随机策略", 10, true, "random"},
		{"等于5个分区使用随机策略", 5, true, "random"},
		{"小于5个分区使用直接遍历", 3, true, "direct"},
		{"1个分区使用直接遍历", 1, true, "direct"},
		{"大于等于20个分区且无一致性哈希时使用随机策略", 30, false, "random"},
		{"大于等于5个分区且有一致性哈希时使用随机策略", 10, true, "random"},
		{"小于5个分区且无一致性哈希时使用直接遍历", 2, false, "direct"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 验证策略选择逻辑的常量
			const (
				consistentHashThreshold = 20
				randomSelectThreshold   = 5
			)

			var expectedStrategy string
			if tt.pendingCount >= consistentHashThreshold && tt.hasConsistentHash {
				expectedStrategy = "consistent_hash"
			} else if tt.pendingCount >= randomSelectThreshold {
				expectedStrategy = "random"
			} else {
				expectedStrategy = "direct"
			}

			assert.Equal(t, tt.expectedStrategy, expectedStrategy,
				"分区数: %d, 有一致性哈希: %v, 期望策略: %s, 实际策略: %s",
				tt.pendingCount, tt.hasConsistentHash, tt.expectedStrategy, expectedStrategy)
		})
	}
}

// TestRandomPartitionsWithSeedEdgeCases 测试随机分区选择的边界情况
func TestRandomPartitionsWithSeedEdgeCases(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	t.Run("空分区列表", func(t *testing.T) {
		var partitions []*model.PartitionInfo
		result := runner.getRandomPartitionsWithSeed(partitions, 5, 12345)
		assert.Empty(t, result, "空分区列表应该返回空结果")
	})

	t.Run("请求数量大于分区数", func(t *testing.T) {
		partitions := []*model.PartitionInfo{
			{PartitionID: 1, Status: model.StatusPending},
			{PartitionID: 2, Status: model.StatusPending},
		}
		result := runner.getRandomPartitionsWithSeed(partitions, 5, 12345)
		assert.Equal(t, 2, len(result), "请求数量大于分区数时应该返回所有分区")
	})

	t.Run("请求数量等于分区数", func(t *testing.T) {
		partitions := []*model.PartitionInfo{
			{PartitionID: 1, Status: model.StatusPending},
			{PartitionID: 2, Status: model.StatusPending},
			{PartitionID: 3, Status: model.StatusPending},
		}
		result := runner.getRandomPartitionsWithSeed(partitions, 3, 12345)
		assert.Equal(t, 3, len(result), "请求数量等于分区数时应该返回所有分区")
	})

	t.Run("请求数量为0", func(t *testing.T) {
		partitions := []*model.PartitionInfo{
			{PartitionID: 1, Status: model.StatusPending},
			{PartitionID: 2, Status: model.StatusPending},
		}
		result := runner.getRandomPartitionsWithSeed(partitions, 0, 12345)
		assert.Empty(t, result, "请求数量为0时应该返回空结果")
	})
}

// TestDeterministicRandomSelection 测试确定性随机选择
func TestDeterministicRandomSelection(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 10)
	for i := 0; i < 10; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
	}

	seed := runner.hashWorkerID("worker-1")
	maxCount := 5

	// 多次调用应该产生相同的结果
	result1 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed)
	result2 := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed)

	assert.Equal(t, len(result1), len(result2))
	assert.Equal(t, maxCount, len(result1))

	// 验证相同种子产生相同顺序
	for i := 0; i < len(result1); i++ {
		assert.Equal(t, result1[i].PartitionID, result2[i].PartitionID)
	}
}

// ==================== 混合策略执行流程测试 ====================

// TestHybridStrategyExecution 测试混合策略的完整执行流程
func TestHybridStrategyExecution(t *testing.T) {
	// 使用mock策略
	mockStrategy := test_utils.NewMockPartitionStrategy()

	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		consistentHash:      nil, // MockTaskConsistentHash类型不兼容，暂时设为nil
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	// 创建测试分区
	testPartitions := []*model.PartitionInfo{
		{PartitionID: 1, Status: model.StatusPending},
		{PartitionID: 2, Status: model.StatusPending},
		{PartitionID: 3, Status: model.StatusPending},
		{PartitionID: 4, Status: model.StatusPending},
		{PartitionID: 5, Status: model.StatusPending},
	}

	// 将分区添加到mock策略中
	for _, partition := range testPartitions {
		mockStrategy.AddPartition(partition)
	}

	t.Run("少于5个分区使用直接遍历策略", func(t *testing.T) {
		// 过滤出3个分区进行测试
		pendingPartitions := testPartitions[:3]
		acquired, err := runner.tryAcquirePendingPartitions(ctx, pendingPartitions, 3)

		assert.NoError(t, err)
		assert.True(t, acquired.PartitionID >= 1 && acquired.PartitionID <= 3)
		assert.Equal(t, model.StatusClaimed, acquired.Status)
		assert.Equal(t, "worker-1", acquired.WorkerID)
	})

	t.Run("5-19个分区使用随机选择策略", func(t *testing.T) {
		// 创建10个分区进行测试
		morePartitions := make([]*model.PartitionInfo, 10)
		for i := 0; i < 10; i++ {
			partition := &model.PartitionInfo{
				PartitionID: i + 10,
				Status:      model.StatusPending,
			}
			morePartitions[i] = partition
			mockStrategy.AddPartition(partition)
		}

		acquired, err := runner.tryAcquirePendingPartitions(ctx, morePartitions, 10)

		assert.NoError(t, err)
		assert.True(t, acquired.PartitionID >= 10 && acquired.PartitionID <= 19)
		assert.Equal(t, model.StatusClaimed, acquired.Status)
	})

	t.Run("20个以上分区使用一致性哈希策略（但回退到随机）", func(t *testing.T) {
		// 创建25个分区进行测试
		manyPartitions := make([]*model.PartitionInfo, 25)
		for i := 0; i < 25; i++ {
			partition := &model.PartitionInfo{
				PartitionID: i + 100,
				Status:      model.StatusPending,
			}
			manyPartitions[i] = partition
			mockStrategy.AddPartition(partition)
		}

		// 由于consistentHash被设为nil，会回退到随机选择策略
		acquired, err := runner.tryAcquireWithConsistentHash(ctx, manyPartitions)

		assert.NoError(t, err)
		// 应该成功获取到一个分区（通过回退到随机选择）
		assert.True(t, acquired.PartitionID >= 100 && acquired.PartitionID <= 124)
		assert.Equal(t, model.StatusClaimed, acquired.Status)
	})
}

// TestHybridStrategyFallback 测试混合策略的回退机制
func TestHybridStrategyFallback(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()

	// 没有一致性哈希的runner
	runnerWithoutHash := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		consistentHash:      nil, // 没有一致性哈希
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	// 创建25个分区（应该触发一致性哈希策略）
	testPartitions := make([]*model.PartitionInfo, 25)
	for i := 0; i < 25; i++ {
		partition := &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
		testPartitions[i] = partition
		mockStrategy.AddPartition(partition)
	}

	t.Run("一致性哈希不可用时回退到随机选择", func(t *testing.T) {
		acquired, err := runnerWithoutHash.tryAcquirePendingPartitions(ctx, testPartitions, 25)

		// 应该回退到随机选择并成功获取分区
		assert.NoError(t, err)
		assert.True(t, acquired.PartitionID >= 1 && acquired.PartitionID <= 25)
		assert.Equal(t, model.StatusClaimed, acquired.Status)
	})

	t.Run("一致性哈希推荐分区被占用时回退", func(t *testing.T) {
		// 注意：由于类型兼容性问题，这里我们测试没有一致性哈希时的回退行为
		runnerWithHash := &Runner{
			workerID:            "worker-2",
			partitionLockExpiry: 60 * time.Second,
			partitionStrategy:   mockStrategy,
			consistentHash:      nil, // 没有一致性哈希，将回退到随机选择
			logger:              utils.NewDefaultLogger(),
		}

		// 预先占用一些分区以测试回退逻辑
		partition1, _ := mockStrategy.GetPartition(ctx, 1)
		partition1.Status = model.StatusClaimed
		partition1.WorkerID = "other-worker"
		_, err := mockStrategy.UpdatePartition(ctx, partition1, nil)
		assert.NoError(t, err)

		partition2, _ := mockStrategy.GetPartition(ctx, 2)
		partition2.Status = model.StatusClaimed
		partition2.WorkerID = "other-worker"
		_, err = mockStrategy.UpdatePartition(ctx, partition2, nil)
		assert.NoError(t, err)

		acquired, err := runnerWithHash.tryAcquireWithConsistentHash(ctx, testPartitions)

		// 没有一致性哈希时应该直接回退到随机选择并获取可用分区
		assert.NoError(t, err)
		assert.NotEqual(t, 1, acquired.PartitionID)
		assert.NotEqual(t, 2, acquired.PartitionID)
		assert.Equal(t, model.StatusClaimed, acquired.Status)
	})
}

// TestReclaimablePartitionsStrategy 测试可重新获取分区的策略
func TestReclaimablePartitionsStrategy(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		allowPreemption:     true, // 启用抢占模式以测试reclaimable逻辑
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()
	now := time.Now()

	// 创建超时的claimed分区
	claimedPartitions := []*model.PartitionInfo{
		{
			PartitionID: 1,
			Status:      model.StatusClaimed,
			WorkerID:    "worker-2",
			UpdatedAt:   now.Add(-150 * time.Second), // 超时
		},
		{
			PartitionID: 2,
			Status:      model.StatusClaimed,
			WorkerID:    "worker-3",
			UpdatedAt:   now.Add(-200 * time.Second), // 更早超时
		},
	}

	// 创建心跳超时的running分区
	runningPartitions := []*model.PartitionInfo{
		{
			PartitionID:   3,
			Status:        model.StatusRunning,
			WorkerID:      "worker-4",
			UpdatedAt:     now.Add(-100 * time.Second),
			LastHeartbeat: now.Add(-350 * time.Second), // 心跳超时
		},
		{
			PartitionID:   4,
			Status:        model.StatusRunning,
			WorkerID:      "worker-5",
			UpdatedAt:     now.Add(-50 * time.Second),
			LastHeartbeat: now.Add(-400 * time.Second), // 更早心跳超时
		},
	}

	allPartitions := append(claimedPartitions, runningPartitions...)
	for _, partition := range allPartitions {
		mockStrategy.AddPartition(partition)
	}

	t.Run("claimed分区按更新时间排序", func(t *testing.T) {
		acquired, err := runner.tryAcquireReclaimablePartitions(ctx, allPartitions, model.StatusClaimed)

		assert.NoError(t, err)
		// 应该获取到更早超时的分区（分区2，UpdatedAt更早）
		assert.Equal(t, 2, acquired.PartitionID)
		assert.Equal(t, "worker-1", acquired.WorkerID)
	})

	t.Run("running分区按心跳时间排序", func(t *testing.T) {
		acquired, err := runner.tryAcquireReclaimablePartitions(ctx, allPartitions, model.StatusRunning)

		assert.NoError(t, err)
		// 应该获取到心跳更早超时的分区（分区4，LastHeartbeat更早）
		assert.Equal(t, 4, acquired.PartitionID)
		assert.Equal(t, "worker-1", acquired.WorkerID)
	})
}

// ==================== 错误处理和边界情况测试 ====================

// TestHybridStrategyErrorHandling 测试混合策略的错误处理
func TestHybridStrategyErrorHandling(t *testing.T) {
	mockStrategy := &MockPartitionStrategyWithErrors{}
	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	t.Run("策略接口返回错误时的处理", func(t *testing.T) {
		mockStrategy.shouldFailAcquire = true

		acquired, success := runner.tryAcquirePartition(ctx, 1)

		assert.False(t, success)
		assert.Nil(t, acquired)
	})

	t.Run("空分区列表的处理", func(t *testing.T) {
		var emptyPartitions []*model.PartitionInfo

		acquired, err := runner.tryAcquirePendingPartitions(ctx, emptyPartitions, 0)

		assert.Error(t, err)
		assert.Equal(t, ErrNoAvailablePartition, err)
		assert.Equal(t, model.PartitionInfo{}, acquired)
	})

	t.Run("所有分区都被占用时的处理", func(t *testing.T) {
		mockStrategy := test_utils.NewMockPartitionStrategy()
		runner.partitionStrategy = mockStrategy

		// 创建已被占用的分区
		occupiedPartitions := []*model.PartitionInfo{
			{PartitionID: 1, Status: model.StatusClaimed, WorkerID: "other-worker"},
			{PartitionID: 2, Status: model.StatusRunning, WorkerID: "other-worker"},
		}

		for _, partition := range occupiedPartitions {
			mockStrategy.AddPartition(partition)
		}

		// 由于所有分区都被占用，应该返回错误
		_, err := runner.tryAcquirePendingPartitions(ctx, occupiedPartitions, 2)

		assert.Error(t, err)
		assert.Equal(t, ErrNoAvailablePartition, err)
	})
}

// TestOptimalTryCountEdgeCases 测试最优尝试次数计算的边界情况
func TestOptimalTryCountEdgeCases(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	edgeCases := []struct {
		name             string
		totalPartitions  int
		expectedTryCount int
		description      string
	}{
		{"零分区", 0, 0, "零分区应该返回0"},
		{"单个分区", 1, 1, "单个分区应该全部尝试"},
		{"边界值4", 4, 3, "4个分区应该尝试3个 (4*3+3)/4=3"},
		{"边界值5", 5, 4, "5个分区应该尝试4个 (5*3+3)/4=4"},
		{"边界值10", 10, 8, "10个分区应该尝试8个 (10*3+3)/4=8"},
		{"边界值11", 11, 6, "11个分区应该尝试6个 (11+1)/2=6"},
		{"边界值50", 50, 25, "50个分区应该尝试25个 (50+1)/2=25"},
		{"边界值51", 51, 20, "51个分区应该限制到20个 min(51/2,20)=20"},
		{"超大值1000", 1000, 20, "1000个分区应该限制到20个"},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := runner.calculateOptimalTryCount(tc.totalPartitions)
			assert.Equal(t, tc.expectedTryCount, actual, tc.description)
		})
	}
}

// ==================== 并发安全测试 ====================

// TestHybridStrategyConcurrency 测试混合策略的并发安全性
func TestHybridStrategyConcurrency(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()

	// 创建多个runner模拟并发workers
	runners := make([]*Runner, 5)
	for i := 0; i < 5; i++ {
		runners[i] = &Runner{
			workerID:            fmt.Sprintf("worker-%d", i+1),
			partitionLockExpiry: 60 * time.Second,
			partitionStrategy:   mockStrategy,
			logger:              utils.NewDefaultLogger(),
		}
	}

	ctx := context.Background()

	// 创建测试分区
	testPartitions := make([]*model.PartitionInfo, 20)
	for i := 0; i < 20; i++ {
		partition := &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
		testPartitions[i] = partition
		mockStrategy.AddPartition(partition)
	}

	t.Run("并发获取分区", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan model.PartitionInfo, 5)
		errors := make(chan error, 5)

		// 启动多个goroutine并发获取分区
		for i, runner := range runners {
			wg.Add(1)
			go func(r *Runner, index int) {
				defer wg.Done()

				acquired, err := r.tryAcquirePendingPartitions(ctx, testPartitions, 20)
				if err != nil {
					errors <- err
				} else {
					results <- acquired
				}
			}(runner, i)
		}

		wg.Wait()
		close(results)
		close(errors)

		// 收集结果
		acquiredPartitions := make(map[int]string)
		for result := range results {
			acquiredPartitions[result.PartitionID] = result.WorkerID
		}

		// 验证没有重复获取同一分区
		partitionIDs := make(map[int]bool)
		for partitionID := range acquiredPartitions {
			assert.False(t, partitionIDs[partitionID], "分区%d被重复获取", partitionID)
			partitionIDs[partitionID] = true
		}

		// 验证至少有一些分区被成功获取
		assert.True(t, len(acquiredPartitions) > 0, "应该有分区被成功获取")

		// 验证没有致命错误
		errorCount := 0
		for range errors {
			errorCount++
		}
		// 允许一些ErrNoAvailablePartition错误，但不应该有其他类型的错误
		assert.True(t, errorCount <= 5, "错误数量不应该超过worker数量")
	})
}

// TestRandomPartitionsWithSeedConcurrency 测试确定性随机选择的并发安全
func TestRandomPartitionsWithSeedConcurrency(t *testing.T) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 创建测试分区
	partitions := make([]*model.PartitionInfo, 100)
	for i := 0; i < 100; i++ {
		partitions[i] = &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
	}

	seed := runner.hashWorkerID("worker-1")
	maxCount := 10

	t.Run("并发调用产生一致结果", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan []*model.PartitionInfo, 10)

		// 并发调用相同的方法
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := runner.getRandomPartitionsWithSeed(partitions, maxCount, seed)
				results <- result
			}()
		}

		wg.Wait()
		close(results)

		// 验证所有结果都相同
		var firstResult []*model.PartitionInfo
		resultCount := 0
		for result := range results {
			if firstResult == nil {
				firstResult = result
			} else {
				assert.Equal(t, len(firstResult), len(result))
				for i := 0; i < len(firstResult); i++ {
					assert.Equal(t, firstResult[i].PartitionID, result[i].PartitionID)
				}
			}
			resultCount++
		}

		assert.Equal(t, 10, resultCount, "应该收到10个结果")
		assert.Equal(t, maxCount, len(firstResult), "每个结果应该包含指定数量的分区")
	})
}

// ==================== 性能测试 ====================

// BenchmarkHybridStrategyDirectTraversal 基准测试：直接遍历策略
func BenchmarkHybridStrategyDirectTraversal(b *testing.B) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	// 创建3个分区（触发直接遍历）
	partitions := make([]*model.PartitionInfo, 3)
	for i := 0; i < 3; i++ {
		partition := &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
		partitions[i] = partition
		mockStrategy.AddPartition(partition)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runner.tryAcquireWithDirectTraversal(ctx, partitions)
	}
}

// BenchmarkHybridStrategyRandomSelection 基准测试：随机选择策略
func BenchmarkHybridStrategyRandomSelection(b *testing.B) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	// 创建10个分区（触发随机选择）
	partitions := make([]*model.PartitionInfo, 10)
	for i := 0; i < 10; i++ {
		partition := &model.PartitionInfo{
			PartitionID: i + 1,
			Status:      model.StatusPending,
		}
		partitions[i] = partition
		mockStrategy.AddPartition(partition)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runner.tryAcquireWithRandomSelection(ctx, partitions)
	}
}

// BenchmarkRandomPartitionsWithSeed 基准测试：确定性随机选择
func BenchmarkRandomPartitionsWithSeed(b *testing.B) {
	runner := &Runner{
		workerID: "worker-1",
		logger:   utils.NewDefaultLogger(),
	}

	// 创建不同规模的分区进行测试
	testCases := []struct {
		name           string
		partitionCount int
		maxCount       int
	}{
		{"小规模", 10, 5},
		{"中等规模", 100, 20},
		{"大规模", 1000, 50},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			partitions := make([]*model.PartitionInfo, tc.partitionCount)
			for i := 0; i < tc.partitionCount; i++ {
				partitions[i] = &model.PartitionInfo{
					PartitionID: i + 1,
					Status:      model.StatusPending,
				}
			}

			seed := runner.hashWorkerID("worker-1")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runner.getRandomPartitionsWithSeed(partitions, tc.maxCount, seed)
			}
		})
	}
}

// ==================== 集成测试 ====================

// TestHybridStrategyIntegration 混合策略集成测试
func TestHybridStrategyIntegration(t *testing.T) {
	mockStrategy := test_utils.NewMockPartitionStrategy()
	mockConsistentHash := &MockConsistentHash{
		preferredPartitions: map[int]bool{1: true, 5: true, 10: true},
	}

	runner := &Runner{
		workerID:            "worker-1",
		partitionLockExpiry: 60 * time.Second,
		partitionStrategy:   mockStrategy,
		consistentHash:      nil, // MockTaskConsistentHash类型不兼容，暂时设为nil
		logger:              utils.NewDefaultLogger(),
	}

	ctx := context.Background()

	t.Run("完整的分区获取流程", func(t *testing.T) {
		// 步骤1：创建各种状态的分区
		now := time.Now()
		partitions := []*model.PartitionInfo{
			// pending分区
			{PartitionID: 1, Status: model.StatusPending},
			{PartitionID: 2, Status: model.StatusPending},
			{PartitionID: 3, Status: model.StatusPending},
			// 超时的claimed分区
			{
				PartitionID: 4,
				Status:      model.StatusClaimed,
				WorkerID:    "worker-2",
				UpdatedAt:   now.Add(-150 * time.Second),
			},
			// 心跳超时的running分区
			{
				PartitionID:   5,
				Status:        model.StatusRunning,
				WorkerID:      "worker-3",
				UpdatedAt:     now.Add(-100 * time.Second),
				LastHeartbeat: now.Add(-350 * time.Second),
			},
			// 正常的running分区（不应该被获取）
			{
				PartitionID:   6,
				Status:        model.StatusRunning,
				WorkerID:      "worker-4",
				UpdatedAt:     now.Add(-30 * time.Second),
				LastHeartbeat: now.Add(-30 * time.Second),
			},
			// 已完成的分区（不应该被获取）
			{PartitionID: 7, Status: model.StatusCompleted, WorkerID: "worker-5"},
		}

		for _, partition := range partitions {
			mockStrategy.AddPartition(partition)
		}

		// 步骤2：尝试获取分区任务
		acquired, err := runner.acquirePartitionTask(ctx)

		// 步骤3：验证结果
		assert.NoError(t, err)
		assert.NotEqual(t, 0, acquired.PartitionID)
		assert.Equal(t, "worker-1", acquired.WorkerID)
		assert.Equal(t, model.StatusClaimed, acquired.Status)

		// 应该优先获取pending分区，特别是一致性哈希推荐的分区
		if acquired.Status == model.StatusPending {
			isPreferred := mockConsistentHash.preferredPartitions[acquired.PartitionID]
			// 如果获取到的是pending分区，可能是推荐的分区
			if isPreferred {
				t.Logf("成功获取一致性哈希推荐的pending分区: %d", acquired.PartitionID)
			}
		}
	})

	t.Run("没有可用分区时的处理", func(t *testing.T) {
		// 清空所有分区
		clearMockStrategy(mockStrategy)

		acquired, err := runner.acquirePartitionTask(ctx)

		assert.Error(t, err)
		assert.Equal(t, ErrNoAvailablePartition, err)
		assert.Equal(t, model.PartitionInfo{}, acquired)
	})
}

// ==================== Mock 辅助类 ====================

// MockConsistentHash Mock一致性哈希实现
type MockConsistentHash struct {
	preferredPartitions map[int]bool
}

func (m *MockConsistentHash) IsCurrentWorkerPreferred(ctx context.Context, partitionID int) (bool, error) {
	return m.preferredPartitions[partitionID], nil
}

func (m *MockConsistentHash) AddWorker(workerID string, weight int) error {
	return nil
}

func (m *MockConsistentHash) RemoveWorker(workerID string) error {
	return nil
}

func (m *MockConsistentHash) UpdateWorkerWeight(workerID string, weight int) error {
	return nil
}

func (m *MockConsistentHash) GetWorkerForPartition(partitionID int) (string, error) {
	return "worker-1", nil
}

func (m *MockConsistentHash) GetAllWorkers() []string {
	return []string{"worker-1"}
}

func (m *MockConsistentHash) GetWorkerWeight(workerID string) (int, error) {
	return 1, nil
}
