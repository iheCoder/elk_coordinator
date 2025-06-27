package leader

import (
	"context"
	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewCommandProcessor 测试CommandProcessor创建
func TestNewCommandProcessor(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)

	assert.NotNil(t, processor)
	assert.Equal(t, namespace, processor.namespace)
	assert.Equal(t, dataStore, processor.commandOps)
	assert.Equal(t, strategy, processor.strategy)
	assert.Equal(t, logger, processor.logger)
}

// TestCommandProcessor_ProcessCommands_NoCommands 测试没有待处理命令的情况
func TestCommandProcessor_ProcessCommands_NoCommands(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	err := processor.ProcessCommands(ctx)
	assert.NoError(t, err)
}

// TestCommandProcessor_ProcessCommands_InvalidCommand 测试处理无效命令
func TestCommandProcessor_ProcessCommands_InvalidCommand(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 提交一个无效的JSON命令
	invalidCommand := "invalid-json-command"
	err := dataStore.SubmitCommand(ctx, namespace, invalidCommand)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err) // 应该不报错，只是删除无效命令

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)
}

// TestCommandProcessor_ProcessCommands_UnknownCommandType 测试处理未知命令类型
func TestCommandProcessor_ProcessCommands_UnknownCommandType(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 创建一个未知类型的命令
	unknownCommand := &model.Command{
		ID:         "unknown-cmd-1",
		Type:       "unknown_command_type",
		Timestamp:  time.Now().Unix(),
		Status:     "pending",
		Parameters: make(map[string]interface{}),
	}

	err := dataStore.SubmitCommand(ctx, namespace, unknownCommand)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err) // 应该不报错，只是删除未知命令

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)
}

// TestCommandProcessor_ProcessRetryFailedPartitions_NoPartitionIDs 测试重试所有失败分区
func TestCommandProcessor_ProcessRetryFailedPartitions_NoPartitionIDs(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回失败的分区
	failedPartitions := []*model.PartitionInfo{
		{PartitionID: 1, Status: model.StatusFailed},
		{PartitionID: 2, Status: model.StatusFailed},
		{PartitionID: 3, Status: model.StatusFailed},
	}
	strategy.SetFilteredPartitions(failedPartitions)

	// 创建重试失败分区命令（不指定分区ID）
	command := model.NewRetryFailedPartitionsCommand(nil)

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err)

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证所有失败分区状态已更新为pending
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Len(t, updateCalls, 3)
	for i, call := range updateCalls {
		assert.Equal(t, failedPartitions[i].PartitionID, call.PartitionID)
		assert.Equal(t, "", call.WorkerID)
		assert.Equal(t, model.StatusPending, call.Status)
	}
}

// TestCommandProcessor_ProcessRetryFailedPartitions_SpecificPartitionIDs 测试重试指定分区
func TestCommandProcessor_ProcessRetryFailedPartitions_SpecificPartitionIDs(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回分区信息
	partitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, Status: model.StatusFailed},
		2: {PartitionID: 2, Status: model.StatusRunning}, // 非失败状态，应该跳过
		3: {PartitionID: 3, Status: model.StatusFailed},
	}
	strategy.SetPartitions(partitions)

	// 创建重试失败分区命令（指定分区ID）
	partitionIDs := []int{1, 2, 3}
	command := model.NewRetryFailedPartitionsCommand(partitionIDs)

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err)

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证只有失败状态的分区被更新（分区1和3）
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Len(t, updateCalls, 2)

	// 验证更新调用
	expectedPartitionIDs := []int{1, 3}
	actualPartitionIDs := make([]int, len(updateCalls))
	for i, call := range updateCalls {
		actualPartitionIDs[i] = call.PartitionID
		assert.Equal(t, "", call.WorkerID)
		assert.Equal(t, model.StatusPending, call.Status)
	}
	assert.ElementsMatch(t, expectedPartitionIDs, actualPartitionIDs)
}

// TestCommandProcessor_ProcessRetryFailedPartitions_NoFailedPartitions 测试没有失败分区的情况
func TestCommandProcessor_ProcessRetryFailedPartitions_NoFailedPartitions(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回空的失败分区列表
	strategy.SetFilteredPartitions([]*model.PartitionInfo{})

	// 创建重试失败分区命令
	command := model.NewRetryFailedPartitionsCommand(nil)

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err)

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证没有分区状态更新调用
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Empty(t, updateCalls)
}

// TestCommandProcessor_ProcessRetryFailedPartitions_MissingParameters 测试缺少参数的命令
func TestCommandProcessor_ProcessRetryFailedPartitions_MissingParameters(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 创建缺少参数的命令
	command := &model.Command{
		ID:        "missing-params-cmd",
		Type:      model.CommandRetryFailedPartitions,
		Timestamp: time.Now().Unix(),
		Status:    "pending",
		// Parameters: 故意留空
	}

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err) // 应该不报错，只是删除无效命令

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证没有分区状态更新调用
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Empty(t, updateCalls)
}

// TestCommandProcessor_ProcessRetryFailedPartitions_PartialFailure 测试部分分区更新失败
func TestCommandProcessor_ProcessRetryFailedPartitions_PartialFailure(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回失败的分区
	failedPartitions := []*model.PartitionInfo{
		{PartitionID: 1, Status: model.StatusFailed},
		{PartitionID: 2, Status: model.StatusFailed},
		{PartitionID: 3, Status: model.StatusFailed},
	}
	strategy.SetFilteredPartitions(failedPartitions)

	// 设置某个分区更新失败
	strategy.SetUpdatePartitionStatusError(2, "update failed")

	// 创建重试失败分区命令
	command := model.NewRetryFailedPartitionsCommand(nil)

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err) // 应该不报错，只是部分成功

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证所有分区都尝试更新了（包括失败的）
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Len(t, updateCalls, 3)
}

// TestCommandProcessor_ProcessRetryFailedPartitions_InterfaceSliceParameters 测试接口切片参数解析
func TestCommandProcessor_ProcessRetryFailedPartitions_InterfaceSliceParameters(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回分区信息
	partitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, Status: model.StatusFailed},
		2: {PartitionID: 2, Status: model.StatusFailed},
	}
	strategy.SetPartitions(partitions)

	// 手动创建命令，模拟JSON解析后的interface{}切片
	command := &model.Command{
		ID:        "interface-slice-cmd",
		Type:      model.CommandRetryFailedPartitions,
		Timestamp: time.Now().Unix(),
		Status:    "pending",
		Parameters: map[string]interface{}{
			"partition_ids": []interface{}{1.0, 2.0}, // JSON解析后数字变成float64
		},
	}

	err := dataStore.SubmitCommand(ctx, namespace, command)
	require.NoError(t, err)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err)

	// 验证命令已被删除
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证分区状态已更新
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Len(t, updateCalls, 2)

	expectedPartitionIDs := []int{1, 2}
	actualPartitionIDs := make([]int, len(updateCalls))
	for i, call := range updateCalls {
		actualPartitionIDs[i] = call.PartitionID
	}
	assert.ElementsMatch(t, expectedPartitionIDs, actualPartitionIDs)
}

// TestCommandProcessor_ProcessCommands_MultipleCommands 测试处理多个命令
func TestCommandProcessor_ProcessCommands_MultipleCommands(t *testing.T) {
	dataStore := test_utils.NewMockDataStore()
	strategy := test_utils.NewMockPartitionStrategyWithTestHelper()
	logger := test_utils.NewMockLogger(false)
	namespace := "test-namespace"

	processor := NewCommandProcessor(namespace, dataStore, strategy, logger)
	ctx := context.Background()

	// 设置mock策略返回失败的分区
	failedPartitions := []*model.PartitionInfo{
		{PartitionID: 1, Status: model.StatusFailed},
		{PartitionID: 2, Status: model.StatusFailed},
	}
	strategy.SetFilteredPartitions(failedPartitions)

	// 将分区添加到strategy的内部存储中，以便UpdatePartitionStatus能找到它们
	partitions := map[int]*model.PartitionInfo{
		1: {PartitionID: 1, Status: model.StatusFailed},
		2: {PartitionID: 2, Status: model.StatusFailed},
	}
	strategy.SetPartitions(partitions)

	// 提交多个命令
	for i := 0; i < 3; i++ {
		command := model.NewRetryFailedPartitionsCommand(nil)
		err := dataStore.SubmitCommand(ctx, namespace, command)
		require.NoError(t, err)
	}

	// 验证有3个待处理命令
	commands, err := dataStore.GetPendingCommands(ctx, namespace, 10)
	require.NoError(t, err)
	assert.Len(t, commands, 3)

	// 处理命令
	err = processor.ProcessCommands(ctx)
	assert.NoError(t, err)

	// 验证所有命令已被删除
	commands, err = dataStore.GetPendingCommands(ctx, namespace, 10)
	assert.NoError(t, err)
	assert.Empty(t, commands)

	// 验证分区状态更新被调用了3次，每次2个分区
	updateCalls := strategy.GetUpdatePartitionStatusCalls()
	assert.Len(t, updateCalls, 6) // 3个命令 × 2个分区
}
