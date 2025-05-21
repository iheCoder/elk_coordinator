package elk_coordinator

import (
	"context"
	"elk_coordinator/data" // Import data package
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// NodeManager handles node registration and heartbeats.
type NodeManager struct {
	instanceID string
	namespace  string
	store      data.DataStore // Changed from Store to data.DataStore
	log        *logrus.Entry

	heartbeatInterval time.Duration
	heartbeatCtx      context.Context
	cancelHeartbeat   context.CancelFunc
}

// NewNodeManager creates a new NodeManager.
// The store parameter is now data.DataStore.
func NewNodeManager(
	instanceID string,
	namespace string,
	store data.DataStore, // Changed from Store to data.DataStore
	log *logrus.Entry,
	heartbeatInterval time.Duration,
) *NodeManager {
	actualLog := log
	if actualLog == nil {
		actualLog = logrus.NewEntry(logrus.New())
		actualLog.Warn("NewNodeManager received a nil logger, using a default one.")
	}

	return &NodeManager{
		instanceID:        instanceID,
		namespace:         namespace,
		store:             store,
		log:               actualLog.WithField("component", "node-manager").WithField("instance_id", instanceID),
		heartbeatInterval: heartbeatInterval,
	}
}

// Start begins the node keeping process (registration and periodic heartbeats).
func (nm *NodeManager) Start(ctx context.Context) error {
	nm.log.Infof("Node manager starting for instance %s in namespace '%s'", nm.instanceID, nm.namespace)
	nm.heartbeatCtx, nm.cancelHeartbeat = context.WithCancel(ctx)

	// Register the node first.
	err := nm.registerNode(nm.heartbeatCtx)
	if err != nil {
		nm.log.Errorf("Failed to register node %s: %v", nm.instanceID, err)
		return errors.Wrap(err, "node registration failed during start")
	}

	// Start the heartbeat loop.
	go nm.nodeKeeperLoop(nm.heartbeatCtx)

	return nil
}

// Stop terminates the node keeping process.
func (nm *NodeManager) Stop(ctx context.Context) error {
	nm.log.Infof("Node manager stopping for instance %s...", nm.instanceID)
	if nm.cancelHeartbeat != nil {
		nm.cancelHeartbeat()
	}

	err := nm.unregisterNode(ctx)
	if err != nil {
		nm.log.Warnf("Failed to unregister node %s during stop: %v", nm.instanceID, err)
	} else {
		nm.log.Infof("Node %s unregistered successfully.", nm.instanceID)
	}

	nm.log.Infof("Node manager for instance %s stopped.", nm.instanceID)
	return nil // Return nil or the error from unregisterNode as appropriate
}

// nodeKeeperLoop manages periodic heartbeats.
func (nm *NodeManager) nodeKeeperLoop(ctx context.Context) {
	defer nm.log.Infof("Node keeper loop stopped for instance %s.", nm.instanceID)
	ticker := time.NewTicker(nm.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			nm.log.Infof("Node keeper loop for instance %s received stop signal: %v", nm.instanceID, ctx.Err())
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				nm.log.Infof("Context cancelled before sending heartbeat for %s. Exiting loop.", nm.instanceID)
				return
			}
			heartbeatKey := fmt.Sprintf(HeartbeatKeyFormat, nm.namespace, nm.instanceID)
			hbCtx, hbCancel := context.WithTimeout(ctx, nm.heartbeatInterval/2)
			
			// Removed type assertion, calling SetHeartbeat directly on nm.store
			err := nm.store.SetHeartbeat(hbCtx, heartbeatKey, time.Now().Format(time.RFC3339))
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					nm.log.Warnf("Sending heartbeat timed out for key %s: %v", heartbeatKey, err)
				} else if errors.Is(err, context.Canceled) {
					nm.log.Infof("Context cancelled, stopping heartbeat for key %s: %v", heartbeatKey, err)
					hbCancel()
					return
				} else {
					nm.log.Warnf("Failed to send heartbeat for key %s: %v", heartbeatKey, err)
				}
			} else {
				nm.log.Debugf("Heartbeat sent for %s", nm.instanceID)
			}
			hbCancel()
		}
	}
}

// registerNode registers this instance with the data store.
func (nm *NodeManager) registerNode(ctx context.Context) error {
	nm.log.Infof("Registering node %s...", nm.instanceID)
	heartbeatKey := fmt.Sprintf(HeartbeatKeyFormat, nm.namespace, nm.instanceID)
	workersKey := fmt.Sprintf(WorkersKeyFormat, nm.namespace)

	regCtx, regCancel := context.WithTimeout(ctx, nm.heartbeatInterval)
	defer regCancel()

	// Removed type assertion, calling RegisterWorker directly on nm.store
	err := nm.store.RegisterWorker(
		regCtx,
		workersKey,
		nm.instanceID,
		heartbeatKey,
		time.Now().Format(time.RFC3339),
	)
	if err != nil {
		return errors.Wrap(err, "failed to register worker with store")
	}

	nm.log.Infof("Node %s registered successfully.", nm.instanceID)
	return nil
}

// unregisterNode removes this instance's registration from the data store.
func (nm *NodeManager) unregisterNode(ctx context.Context) error {
	nm.log.Infof("Unregistering node %s...", nm.instanceID)
	heartbeatKey := fmt.Sprintf(HeartbeatKeyFormat, nm.namespace, nm.instanceID)
	// The UnregisterWorker method from data.DataStore might be more appropriate if it exists and handles all necessary cleanup.
	// For now, using DeleteKey for the heartbeatKey as per previous logic.
	// If workersKey also needs cleanup, that would be an additional step or part of a dedicated UnregisterWorker.
	// The current data.DataStore interface has UnregisterWorker(ctx, workersKey, workerID, heartbeatKey).
	// Let's use that for a more complete unregistration.

	workersKey := fmt.Sprintf(WorkersKeyFormat, nm.namespace)

	unregCtx, unregCancel := context.WithTimeout(ctx, nm.heartbeatInterval)
	defer unregCancel()

	// Using the UnregisterWorker method from data.DataStore
	err := nm.store.UnregisterWorker(unregCtx, workersKey, nm.instanceID, heartbeatKey)
	if err != nil {
		// If UnregisterWorker fails, log it. Depending on the error, one might still want to attempt
		// to delete the heartbeat key directly as a fallback, but that could lead to partial states.
		// For now, we just return the error from UnregisterWorker.
		return errors.Wrapf(err, "failed to unregister worker %s (heartbeat key: %s, workers key: %s)", nm.instanceID, heartbeatKey, workersKey)
	}
	
	// Previously, it was just deleting the heartbeatKey.
	// err := nm.store.DeleteKey(unregCtx, heartbeatKey)
	// if err != nil {
	// 	return errors.Wrapf(err, "failed to delete heartbeat key %s during unregistration", heartbeatKey)
	// }
	return nil
}
