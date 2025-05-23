package leader

import (
	"context"
	"elk_coordinator/data"
	"elk_coordinator/model"
	"elk_coordinator/utils"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// Election 管理领导者选举过程
type Election struct {
	config ElectionConfig
}

// ElectionConfig 选举配置
type ElectionConfig struct {
	NodeID           string
	Namespace        string
	DataStore        data.DataStore
	Logger           utils.Logger
	ElectionInterval time.Duration
	LockExpiry       time.Duration
}

// NewElection 创建新的选举管理器
func NewElection(config ElectionConfig) *Election {
	return &Election{
		config: config,
	}
}

// TryElect 尝试竞选领导者
func (e *Election) TryElect(ctx context.Context) bool {
	// 获取leader锁
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, e.config.Namespace)
	success, err := e.config.DataStore.AcquireLock(ctx, leaderLockKey, e.config.NodeID, e.config.LockExpiry)
	if err != nil {
		e.config.Logger.Warnf("获取Leader锁失败: %v", err)
		return false
	}

	if success {
		return true
	}

	// 竞选失败
	e.config.Logger.Debugf("获取Leader锁失败，将作为Worker运行")
	return false
}

// StartRenewing 开始定期更新Leader锁
func (e *Election) StartRenewing(ctx context.Context, onLossCallback func()) {
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, e.config.Namespace)
	// 使锁更新频率是锁超时时间的1/3，确保有足够时间进行更新
	ticker := time.NewTicker(e.config.LockExpiry / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.config.Logger.Infof("停止更新Leader锁")
			return
		case <-ticker.C:
			if !e.renewLeaderLock(leaderLockKey) {
				e.config.Logger.Warnf("更新Leader锁失败，放弃领导权")
				onLossCallback()
				return
			}
		}
	}
}

// renewLeaderLock 尝试更新Leader锁
func (e *Election) renewLeaderLock(leaderLockKey string) bool {
	ctx := context.Background()
	success, err := e.config.DataStore.RenewLock(ctx, leaderLockKey, e.config.NodeID, e.config.LockExpiry)

	if err != nil {
		e.config.Logger.Warnf("更新Leader锁失败: %v", err)
		return true // 遇到错误时继续尝试，不放弃领导权
	}

	if !success {
		e.config.Logger.Warnf("无法更新Leader锁，可能锁已被其他节点获取")
		return false // 更新失败，需要放弃领导权
	}

	return true // 成功更新，继续保持领导权
}

// GetLeaderInfo 获取当前领导者信息
func (e *Election) GetLeaderInfo(ctx context.Context) (string, error) {
	leaderLockKey := fmt.Sprintf(model.LeaderLockKeyFmt, e.config.Namespace)
	leaderID, err := e.config.DataStore.GetLockOwner(ctx, leaderLockKey)
	if err != nil {
		return "", errors.Wrap(err, "获取Leader信息失败")
	}
	return leaderID, nil
}
