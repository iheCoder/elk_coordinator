package leader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
	"github.com/iheCoder/elk_coordinator/test_utils"
)

// TestGetActiveWorkers 测试获取活跃节点列表
func TestGetActiveWorkers(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 "node1",
		Namespace:              "test",
		DataStore:              mockStore,
		Logger:                 test_utils.NewMockLogger(false),
		ValidHeartbeatDuration: 30 * time.Second,
	})

	ctx := context.Background()

	// 设置一些心跳数据
	now := time.Now()
	validHeartbeat := now.Format(time.RFC3339)
	expiredHeartbeat := now.Add(-time.Minute).Format(time.RFC3339) // 过期的心跳

	// 添加测试数据
	mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node1")] = validHeartbeat
	mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node2")] = validHeartbeat
	mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node3")] = expiredHeartbeat // 过期的

	// 测试获取活跃节点
	activeWorkers, err := workManager.getActiveWorkers(ctx)
	if err != nil {
		t.Errorf("获取活跃节点失败: %v", err)
	}

	// 检查结果
	if len(activeWorkers) != 2 {
		t.Errorf("期望活跃节点数为2，得到 %d", len(activeWorkers))
	}

	// 检查是否包含正确的节点
	foundNode1 := false
	foundNode2 := false
	for _, nodeID := range activeWorkers {
		if nodeID == "node1" {
			foundNode1 = true
		}
		if nodeID == "node2" {
			foundNode2 = true
		}
		if nodeID == "node3" {
			t.Error("返回了过期节点")
		}
	}

	if !foundNode1 {
		t.Error("未找到活跃节点node1")
	}
	if !foundNode2 {
		t.Error("未找到活跃节点node2")
	}

	// 检查过期节点是否被清理
	if _, exists := mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node3")]; exists {
		t.Error("过期节点心跳未被清理")
	}
}

// TestTryAllocatePartitions 测试尝试分配分区
func TestTryAllocatePartitions(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	mockPlaner := &mockPartitionPlaner{
		nextMaxID: 1000,
	}

	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 "node1",
		Namespace:              "test",
		DataStore:              mockStore,
		Logger:                 test_utils.NewMockLogger(false),
		ValidHeartbeatDuration: 30 * time.Second,
	})

	mockStrategy := test_utils.NewMockPartitionStrategy()
	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, mockStrategy, test_utils.NewMockLogger(false), mockPlaner)

	ctx := context.Background()

	// 设置一个有效的心跳
	now := time.Now()
	validHeartbeat := now.Format(time.RFC3339)
	mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node1")] = validHeartbeat

	// 测试 tryAllocatePartitions
	workManager.tryAllocatePartitions(ctx, partitionMgr)

	// 验证分区是否已创建 - 使用strategy检查而不是datastore
	allPartitions, err := mockStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(allPartitions) == 0 {
		t.Error("分区数据未创建")
	}

	// 测试没有活跃节点的情况
	delete(mockStore.Heartbeats, fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node1"))

	// 先清除已有分区数据
	mockStrategy.Partitions = make(map[int]*model.PartitionInfo)

	// 测试没有活跃节点时的分配
	workManager.tryAllocatePartitions(ctx, partitionMgr)

	// 验证没有分区被创建
	allPartitions, err = mockStrategy.GetAllActivePartitions(ctx)
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(allPartitions) != 0 {
		t.Error("没有活跃节点时不应该创建分区，但仍然创建了分区")
	}
}

// TestRunPartitionAllocationLoop 测试分区分配��环
func TestRunPartitionAllocationLoop(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	mockPlaner := &mockPartitionPlaner{
		nextMaxID: 1000,
	}

	workManager := NewWorkManager(WorkManagerConfig{
		NodeID:                 "node1",
		Namespace:              "test",
		DataStore:              mockStore,
		Logger:                 test_utils.NewMockLogger(false),
		ValidHeartbeatDuration: 30 * time.Second,
	})

	mockStrategy := test_utils.NewMockPartitionStrategy()
	partitionMgr := NewPartitionAssigner(PartitionAssignerConfig{Namespace: "test"}, mockStrategy, test_utils.NewMockLogger(false), mockPlaner)

	// 设置一个有效的心跳
	now := time.Now()
	validHeartbeat := now.Format(time.RFC3339)
	mockStore.Heartbeats[fmt.Sprintf("%s:%s", model.HeartbeatKeyPrefix, "node1")] = validHeartbeat

	// 创建一个短时间的上下文和主动取消的leader上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	leaderCtx, leaderCancel := context.WithCancel(context.Background())

	// 用于同步的WaitGroup
	var wg sync.WaitGroup
	wg.Add(1)

	// 启动分区分配循环
	go func() {
		defer wg.Done()
		err := workManager.RunPartitionAllocationLoop(ctx, leaderCtx, partitionMgr)
		if err != nil && err != context.DeadlineExceeded {
			t.Errorf("分区分配循环错误: %v", err)
		}
	}()

	// 等待一段时间后手动取消leader上下文
	time.Sleep(50 * time.Millisecond)
	leaderCancel()

	// 等待goroutine完成
	wg.Wait()

	// 验证分区是否已创建 - 使用strategy检查而不是datastore
	allPartitions, err := mockStrategy.GetAllActivePartitions(context.Background())
	if err != nil {
		t.Errorf("获取分区失败: %v", err)
	}
	if len(allPartitions) == 0 {
		t.Error("分区数据未创建")
	}
}
