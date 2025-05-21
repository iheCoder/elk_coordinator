package elk_coordinator

import (
	"context"
	"elk_coordinator/model"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Stop 停止管理器，快速释放资源
func (m *Mgr) Stop() {
	m.Logger.Infof("正在停止管理器 %s", m.ID)

	// 1. 立即标记节点为退出状态，便于其他节点感知
	exitingKey := fmt.Sprintf(model.ExitingNodeFmt, m.Namespace, m.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = m.DataStore.SetKey(ctx, exitingKey, time.Now().Format(time.RFC3339), 30*time.Second)

	// 2. 取消所有上下文，通知协程停止工作
	if m.cancelHeartbeat != nil {
		m.cancelHeartbeat()
	}
	if m.cancelLeader != nil {
		m.cancelLeader()
	}
	if m.cancelWork != nil {
		m.cancelWork()
	}

	// 3. 快速释放所有持有的锁（Leader锁和分区锁）
	go m.asyncReleaseAllLocks()

	// 4. 删除心跳和节点注册（允许超时后被其他节点清理）
	heartbeatKey := fmt.Sprintf(model.HeartbeatFmtFmt, m.Namespace, m.ID)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		m.DataStore.DeleteKey(ctx, heartbeatKey)

		// 注销节点
		workersKey := fmt.Sprintf(model.WorkersKeyFmt, m.Namespace)
		m.DataStore.UnregisterWorker(ctx, workersKey, m.ID, heartbeatKey)
	}()

	m.Logger.Infof("管理器 %s 已通知停止", m.ID)
}

// asyncReleaseAllLocks 异步快速释放所有锁
func (m *Mgr) asyncReleaseAllLocks() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 释放Leader锁
	m.mu.RLock()
	isLeader := m.isLeader
	m.mu.RUnlock()

	if isLeader {
		leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, m.Namespace)
		if err := m.DataStore.ReleaseLock(ctx, leaderLockKey, m.ID); err != nil {
			m.Logger.Warnf("释放Leader锁失败: %v", err)
		} else {
			m.Logger.Infof("已释放Leader锁")
		}
	}

	// 快速获取并释放所有分区锁
	partitionInfoKey := fmt.Sprintf(model.PartitionInfoKeyFmt, m.Namespace)
	partitionsData, err := m.DataStore.GetPartitions(ctx, partitionInfoKey)
	if err != nil {
		m.Logger.Warnf("获取分区信息失败: %v", err)
		return
	}

	if partitionsData == "" {
		m.Logger.Infof("无分区信息，无需释放分区锁")
		return
	}

	var partitionMap map[int]model.PartitionInfo
	if err := json.Unmarshal([]byte(partitionsData), &partitionMap); err != nil {
		m.Logger.Warnf("解析分区信息失败: %v", err)
		return
	}

	// 找出当前节点持有的分区
	var myPartitionsCount int
	for _, p := range partitionMap {
		if p.WorkerID == m.ID {
			myPartitionsCount++
		}
	}

	if myPartitionsCount == 0 {
		m.Logger.Infof("没有持有的分区锁需要释放")
		return
	}

	m.Logger.Infof("开始释放 %d 个分区锁", myPartitionsCount)

	// 并行释放所有分区锁
	var wg sync.WaitGroup
	for id, p := range partitionMap {
		if p.WorkerID == m.ID {
			wg.Add(1)
			go func(partitionID int) {
				defer wg.Done()
				lockKey := fmt.Sprintf(model.PartitionLockFmtFmt, m.Namespace, partitionID)
				if err := m.DataStore.ReleaseLock(ctx, lockKey, m.ID); err != nil {
					m.Logger.Warnf("释放分区 %d 锁失败: %v", partitionID, err)
				}
			}(id)
		}
	}

	// 等待所有锁释放完成，但设置一个较短的超时时间
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.Logger.Infof("所有分区锁释放完成")
	case <-time.After(3 * time.Second):
		m.Logger.Warnf("部分分区锁释放超时")
	}
}

// setupSignalHandler 设置信号处理器来捕获操作系统信号
func (m *Mgr) setupSignalHandler(ctx context.Context) {
	// 创建信号通道
	sigCh := make(chan os.Signal, 1)

	// 注册要捕获的信号
	// SIGTERM: Kubernetes 终止 Pod 时发送的信号
	// SIGINT: 通常是用户按 Ctrl+C 发送的信号
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// 等待信号
	select {
	case <-ctx.Done():
		// 上下文取消，无需处理信号
		return
	case sig := <-sigCh:
		m.Logger.Infof("收到操作系统信号: %v，开始优雅退出", sig)

		// 调用 Stop 方法进行优雅退出
		m.Stop()

		// 如果是在 Kubernetes 环境中，可以在此处添加额外的清理工作
		if sig == syscall.SIGTERM {
			m.Logger.Infof("正在响应 SIGTERM 信号，这可能是 Kubernetes 终止 Pod")
		}
	}
}
