package leader

import (
	"context"
	"elk_coordinator/test_utils"
	"sync"
	"testing"
	"time"
)

// TestNewLeaderManager 测试创建新的领导者管理器
func TestNewLeaderManager(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	mockPlaner := &mockPartitionPlaner{
		suggestedPartitionSize: 2000,
		nextMaxID:              5000,
	}

	// 创建LeaderManager
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  mockPlaner,
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 验证组件是否正确初始化
	if lm.election == nil {
		t.Error("Election组件未初始化")
	}
	if lm.workManager == nil {
		t.Error("WorkManager组件未初始化")
	}
	if lm.partitionMgr == nil {
		t.Error("PartitionManager组件未初始化")
	}
}

// TestIsLeader 测试领导者状态查询
func TestIsLeader(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 初始状态应该不是领导者
	if lm.IsLeader() {
		t.Error("初始状态下不应该是领导者")
	}

	// 设置为领导者
	lm.mu.Lock()
	lm.isLeader = true
	lm.mu.Unlock()

	// 现在应该是领导者
	if !lm.IsLeader() {
		t.Error("设置后应该是领导者，但返回false")
	}
}

// TestBecomeLeader 测试成为领导者
func TestBecomeLeader(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 执行成为领导者的方法
	lm.becomeLeader()

	// 验证状态
	if !lm.IsLeader() {
		t.Error("becomeLeader后应该是领导者，但返回false")
	}
}

// TestRelinquishLeadership 测试放弃领导权
func TestRelinquishLeadership(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 先设置为领导者
	lm.mu.Lock()
	lm.isLeader = true
	lm.mu.Unlock()

	// 初始化上下文
	lm.leaderCtx, lm.cancelLeader = context.WithCancel(context.Background())

	// 放弃领导权
	lm.relinquishLeadership()

	// 验证状态
	if lm.IsLeader() {
		t.Error("放弃领导权后不应该是领导者，但返回true")
	}

	// 验证上下文是否已取消
	select {
	case <-lm.leaderCtx.Done():
		// 正确，上下文已取消
	default:
		t.Error("放弃领导权后上下文应该已取消，但仍然有效")
	}
}

// TestStartLeaderWork 测试启动领导者工作
func TestStartLeaderWork(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 创建简单的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 执行启动领导者工作
	lm.startLeaderWork(ctx)

	// 验证上下文是否已创建
	if lm.leaderCtx == nil {
		t.Error("启动领导者工作后上下文应该已创建，但为nil")
	}
	if lm.cancelLeader == nil {
		t.Error("启动领导者工作后取消函数应该已创建，但为nil")
	}

	// 等待一段时间，让goroutine有时间执行
	time.Sleep(150 * time.Millisecond)
}

// TestPeriodicElection 测试周期性选举
func TestPeriodicElection(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	ctx := context.Background()

	// 确保初始状态不是领导者
	lm.mu.Lock()
	lm.isLeader = false
	lm.mu.Unlock()

	// 测试成功选举的情况
	lm.periodicElection(ctx)

	// 验证状态，应该已成为领导者
	if !lm.IsLeader() {
		t.Error("周期性选举后应该成为领导者，但没有")
	}

	// 测试已经是领导者的情况
	lm.mu.Lock()
	lm.isLeader = true
	lm.mu.Unlock()

	// 修改mock以使下次选举失败
	mockStore.LockMutex.Lock()
	mockStore.Locks[lm.election.config.Namespace+":leader"] = "node2" // 模拟另一个节点已经获取了锁
	mockStore.LockMutex.Unlock()

	// 执行周期性选举，但已经是领导者，不应该尝试选举
	lm.periodicElection(ctx)

	// 状态应该保持不变
	if !lm.IsLeader() {
		t.Error("已是领导者的情况下，周期性选举不应改变状态")
	}
}

// TestRunElectionLoopSimple 测试选举循环的简单场景
func TestRunElectionLoopSimple(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 创建短期上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 用于同步的WaitGroup
	var wg sync.WaitGroup
	wg.Add(1)

	// 启动选举循环
	go func() {
		defer wg.Done()
		err := lm.runElectionLoop(ctx)
		if err != context.DeadlineExceeded {
			t.Errorf("选举循环错误: %v", err)
		}
	}()

	// 等待goroutine完成
	wg.Wait()

	// 验证状态，应该已成为领导者
	if !lm.IsLeader() {
		t.Error("选举循环后应该成为领导者，但没有")
	}
}

// TestStart 测试启动方法
func TestStart(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 创建短期上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 用于同步的WaitGroup
	var wg sync.WaitGroup
	wg.Add(1)

	// 启动
	go func() {
		defer wg.Done()
		err := lm.Start(ctx)
		if err != context.DeadlineExceeded {
			t.Errorf("启动错误: %v", err)
		}
	}()

	// 等待goroutine完成
	wg.Wait()

	// 验证上下文是否已创建
	if lm.leaderCtx == nil {
		t.Error("启动后上下文应该已创建，但为nil")
	}
}

// TestStop 测试停止方法
func TestStop(t *testing.T) {
	mockStore := test_utils.NewMockDataStore()
	lm := NewLeaderManager(LeaderConfig{
		NodeID:                  "node1",
		Namespace:               "test",
		DataStore:               mockStore,
		Logger:                  test_utils.NewMockLogger(false),
		Planer:                  &mockPartitionPlaner{},
		ElectionInterval:        5 * time.Second,
		LockExpiry:              30 * time.Second,
		WorkerPartitionMultiple: 2,
		ValidHeartbeatDuration:  60 * time.Second,
	})

	// 初始化上下文
	lm.leaderCtx, lm.cancelLeader = context.WithCancel(context.Background())

	// 执行停止
	lm.Stop()

	// 验证上下文是否已取消
	select {
	case <-lm.leaderCtx.Done():
		// 正确，上下文已取消
	default:
		t.Error("停止后上下文应该已取消，但仍然有效")
	}
}
