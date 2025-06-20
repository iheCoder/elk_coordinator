package partition

import (
	"context"
	"fmt"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreatePartitionsIfNotExist_DataRangeConsistency 测试数据范围一致性检查
func TestCreatePartitionsIfNotExist_DataRangeConsistency(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 先创建一个分区
	firstRequest := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, firstRequest)
	require.NoError(t, err)
	assert.Len(t, createdPartitions, 1)

	// 尝试创建相同ID但不同数据范围的分区，应该失败
	conflictRequest := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1001, MaxID: 2000}, // 不同的数据范围
		},
	}

	_, err = strategy.CreatePartitionsIfNotExist(ctx, conflictRequest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "数据范围冲突")
	assert.Contains(t, err.Error(), "现有[1-1000] vs 请求[1001-2000]")

	// 尝试创建相同ID和相同数据范围的分区，应该成功（幂等）
	identicalRequest := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000}, // 相同的数据范围
		},
	}

	existingPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, identicalRequest)
	require.NoError(t, err)
	assert.Len(t, existingPartitions, 1)
	assert.Equal(t, int64(1), existingPartitions[0].MinID)
	assert.Equal(t, int64(1000), existingPartitions[0].MaxID)
}

// TestCreatePartitionsIfNotExist_OrderedCreation 测试有序创建和连续性保证
func TestCreatePartitionsIfNotExist_OrderedCreation(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 测试批量创建时的排序行为
	request := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 5, MinID: 4001, MaxID: 5000}, // 故意乱序
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
			{PartitionID: 2, MinID: 1001, MaxID: 2000},
			{PartitionID: 4, MinID: 3001, MaxID: 4000},
		},
	}

	createdPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	require.NoError(t, err)
	assert.Len(t, createdPartitions, 5)

	// 验证返回的分区按ID排序
	for i := 0; i < len(createdPartitions)-1; i++ {
		assert.True(t, createdPartitions[i].PartitionID < createdPartitions[i+1].PartitionID,
			"分区应该按ID排序")
	}

	// 验证所有分区都被正确创建
	for i, partition := range createdPartitions {
		expectedID := i + 1
		assert.Equal(t, expectedID, partition.PartitionID)
		assert.Equal(t, model.StatusPending, partition.Status)
		assert.Equal(t, int64(1), partition.Version)
	}
}

// MockHashPartitionOperationsWithFailure 支持部分失败的mock store
type MockHashPartitionOperationsWithFailure struct {
	*MockHashPartitionOperations
	batchCount     int  // 当前批次计数
	failAfterBatch int  // 在第几个批次之后失败
	shouldFail     bool // 是否启用失败模式
}

func NewMockHashPartitionOperationsWithFailure(failAfterBatch int) *MockHashPartitionOperationsWithFailure {
	return &MockHashPartitionOperationsWithFailure{
		MockHashPartitionOperations: &MockHashPartitionOperations{
			partitions: make(map[string]map[string]string),
			versions:   make(map[string]int64),
		},
		failAfterBatch: failAfterBatch,
		shouldFail:     true,
	}
}

func (m *MockHashPartitionOperationsWithFailure) HSetPartitionsInTx(ctx context.Context, key string, partitions map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.batchCount++

	// 如果启用失败模式且达到失败批次，返回错误
	if m.shouldFail && m.batchCount > m.failAfterBatch {
		return fmt.Errorf("模拟批量操作失败在批次 %d", m.batchCount)
	}

	// 否则正常执行批量设置
	if m.partitions[key] == nil {
		m.partitions[key] = make(map[string]string)
	}

	for field, value := range partitions {
		m.partitions[key][field] = value
		if m.versions[field] == 0 {
			m.versions[field] = 1
		}
	}

	return nil
}

// TestCreatePartitionsIfNotExist_PartialFailureScenario 测试部分失败场景
func TestCreatePartitionsIfNotExist_PartialFailureScenario(t *testing.T) {
	// 创建一个mock存储，可以模拟部分失败
	mockStore := NewMockHashPartitionOperationsWithFailure(1) // 第二个批次失败

	logger := test_utils.NewMockLogger(true)
	strategy := NewHashPartitionStrategy(mockStore, logger)

	ctx := context.Background()

	// 创建足够多的分区以触发多个批次（batchSize=50）
	var partitions []model.CreatePartitionRequest
	for i := 1; i <= 75; i++ { // 会产生2个批次：1-50, 51-75
		partitions = append(partitions, model.CreatePartitionRequest{
			PartitionID: i,
			MinID:       int64((i-1)*1000 + 1),
			MaxID:       int64(i * 1000),
		})
	}

	request := model.CreatePartitionsRequest{Partitions: partitions}

	// 期望第二个批次失败
	_, err := strategy.CreatePartitionsIfNotExist(ctx, request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "批量创建分区失败")

	// 验证第一个批次的分区被创建（由于不回滚策略）
	createdCount := 0
	for i := 1; i <= 50; i++ {
		_, err := strategy.GetPartition(ctx, i)
		if err == nil {
			createdCount++
		}
	}
	assert.Equal(t, 50, createdCount, "第一个批次的分区应该被成功创建")

	// 验证第二个批次的分区没有被创建
	for i := 51; i <= 75; i++ {
		_, err := strategy.GetPartition(ctx, i)
		assert.Error(t, err, "第二个批次的分区不应该被创建")
	}
}

// TestCreatePartitionsIfNotExist_MixedExistingAndNew 测试混合已存在和新分区的场景
func TestCreatePartitionsIfNotExist_MixedExistingAndNew(t *testing.T) {
	strategy, _, _, cleanup := setupHashPartitionStrategyIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()

	// 先创建一些分区
	existingRequest := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},
			{PartitionID: 3, MinID: 2001, MaxID: 3000},
		},
	}

	_, err := strategy.CreatePartitionsIfNotExist(ctx, existingRequest)
	require.NoError(t, err)

	// 创建混合请求：包含已存在的和新的分区
	mixedRequest := model.CreatePartitionsRequest{
		Partitions: []model.CreatePartitionRequest{
			{PartitionID: 1, MinID: 1, MaxID: 1000},    // 已存在，数据范围一致
			{PartitionID: 2, MinID: 1001, MaxID: 2000}, // 新分区
			{PartitionID: 3, MinID: 2001, MaxID: 3000}, // 已存在，数据范围一致
			{PartitionID: 4, MinID: 3001, MaxID: 4000}, // 新分区
		},
	}

	allPartitions, err := strategy.CreatePartitionsIfNotExist(ctx, mixedRequest)
	require.NoError(t, err)
	assert.Len(t, allPartitions, 4)

	// 验证所有分区都存在
	for i := 1; i <= 4; i++ {
		partition, err := strategy.GetPartition(ctx, i)
		require.NoError(t, err)
		assert.Equal(t, i, partition.PartitionID)
	}

	// 验证返回的分区按ID排序
	for i := 0; i < len(allPartitions)-1; i++ {
		assert.True(t, allPartitions[i].PartitionID < allPartitions[i+1].PartitionID)
	}
}
