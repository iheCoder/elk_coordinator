package elk_coordinator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/iheCoder/elk_coordinator/model"
)

// Stop 停止管理器，协调各组件的优雅关闭
func (m *Mgr) Stop() {
	m.Logger.Infof("正在停止管理器 %s", m.ID)

	// 1. 立即标记节点为退出状态，便于其他节点感知
	exitingKey := fmt.Sprintf(model.ExitingNodeFmt, m.Namespace, m.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_ = m.DataStore.SetKey(ctx, exitingKey, time.Now().Format(time.RFC3339), 30*time.Second)

	// 2. 协调各组件进行资源清理（使用关注点分离原则）
	m.stopComponents(ctx)

	// 3. 然后取消所有上下文，通知协程停止工作
	if m.cancelHeartbeat != nil {
		m.cancelHeartbeat()
	}
	if m.cancelLeader != nil {
		m.cancelLeader()
	}
	if m.cancelWork != nil {
		m.cancelWork()
	}

	// 4. 最后删除心跳和节点注册（允许其他节点快速感知到节点离线）
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// 注销节点（只删除心跳）
		m.DataStore.UnregisterWorker(ctx, m.ID)
	}()

	m.Logger.Infof("管理器 %s 已通知停止", m.ID)
}

// stopComponents 协调各组件进行资源清理
func (m *Mgr) stopComponents(ctx context.Context) {
	m.Logger.Infof("开始协调组件停止...")

	// 使用WaitGroup来并行停止组件，提高停止效率
	var wg sync.WaitGroup

	// 停止TaskWindow组件（如果存在）
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.mu.Lock()
		taskWindow := m.taskWindow
		m.mu.Unlock()

		if taskWindow != nil {
			if err := taskWindow.Stop(ctx); err != nil {
				m.Logger.Warnf("TaskWindow停止失败: %v", err)
			} else {
				m.Logger.Infof("TaskWindow已停止")
			}
		}
	}()

	// 停止LeaderManager组件（如果存在）
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.mu.Lock()
		leaderManager := m.leaderManager
		m.mu.Unlock()

		if leaderManager != nil {
			leaderManager.Stop()
			m.Logger.Infof("LeaderManager已停止")
		}
	}()

	// 注意：不直接停止PartitionStrategy
	// TaskWindow会在其Stop()方法中通过Runner调用PartitionStrategy.Stop()
	// 这确保了正在处理的任务完成后再停止策略

	// 等待所有组件停止完成，但设置超时时间避免无限等待
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.Logger.Infof("所有组件停止完成")
	case <-ctx.Done():
		m.Logger.Warnf("组件停止超时，但继续关闭流程")
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
