package elk_coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Store defines the interface for a data store that can be used for leader election.
// It's a subset of methods from data.DataStore, tailored for leader election needs.
type Store interface {
	// AcquireLock attempts to acquire a lock (SetNX behavior).
	// Returns true if the lock was acquired.
	AcquireLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	// RenewLock attempts to renew an existing lock.
	// Returns true if the lock was renewed.
	RenewLock(ctx context.Context, key string, value string, expiry time.Duration) (bool, error)
	// GetLockOwner retrieves the current owner of the lock.
	// If the lock does not exist or is expired, it might return an empty string or an error.
	GetLockOwner(ctx context.Context, key string) (string, error)
	// ReleaseLock releases a lock.
	ReleaseLock(ctx context.Context, key string, value string) error
}

// LeaderElector handles the leader election process for an instance.
type LeaderElector struct {
	instanceID string
	namespace  string
	store      Store // The data store for lock operations

	// Leader election configuration
	// leaderKeyFmt is now the global LeaderKeyFormat constant
	leaderLockExpiry     time.Duration // How long the leader lock is valid
	leaderElectionInterval time.Duration // How often to check/try for leadership

	log *logrus.Entry // Structured logger

	mu           sync.Mutex
	isLeader     bool
	leaderCtx    context.Context    // Context for the current leadership term
	cancelLeader context.CancelFunc // Cancels the leaderCtx
}

// NewLeaderElector creates and returns a new LeaderElector.
// keyFmt argument is removed, will use global LeaderKeyFormat.
func NewLeaderElector(
	instanceID string,
	namespace string,
	store Store,
	log *logrus.Entry,
	lockExpiry time.Duration,
	electionInterval time.Duration,
) *LeaderElector {
	// Ensure logger is not nil
	actualLog := log
	if actualLog == nil {
		actualLog = logrus.NewEntry(logrus.New()) // Fallback to a default logger
		actualLog.Warn("NewLeaderElector received a nil logger, using a default one.")
	}

	return &LeaderElector{
		instanceID:             instanceID,
		namespace:              namespace,
		store:                  store,
		leaderLockExpiry:       lockExpiry,
		leaderElectionInterval: electionInterval,
		// leaderKeyFmt:        is now global LeaderKeyFormat
		log:                    actualLog.WithField("component", "leader-elector").WithField("instance_id", instanceID),
	}
}

// getLeaderKey returns the specific leader key for the configured namespace.
func (l *LeaderElector) getLeaderKey() string {
	return fmt.Sprintf(LeaderKeyFormat, l.namespace) // Uses global LeaderKeyFormat
}

// Lead starts the leader election loop.
// The provided errgroup.Group can be used to manage the lifecycle of this goroutine.
func (l *LeaderElector) Lead(ctx context.Context, g *errgroup.Group) {
	l.log.Infof("Starting leader election process for namespace '%s'", l.namespace)

	l.mu.Lock()
	// If leaderCtx is already active from a previous call for this instance, cancel it.
	if l.cancelLeader != nil {
		l.log.Warn("LeaderElector.Lead called while previous leader loop might be active. Cancelling previous loop.")
		l.cancelLeader()
	}
	l.leaderCtx, l.cancelLeader = context.WithCancel(ctx)
	l.mu.Unlock()

	g.Go(func() error {
		// Defer the cleanup actions.
		defer func() {
			l.log.Infof("Leader election goroutine for namespace '%s' is shutting down.", l.namespace)
			// Ensure leadership is released if this instance was the leader.
			// Use a background context for releasing leadership as leaderCtx might be cancelled.
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), l.leaderLockExpiry/2+time.Millisecond*100)
			defer cancelRelease()
			l.releaseLeadership(releaseCtx, "leader loop ended")

			l.mu.Lock()
			if l.cancelLeader != nil {
				// l.cancelLeader() // This would cancel the context we are operating in, which is fine if error group handles it.
				l.cancelLeader = nil // Mark as no longer active
			}
			l.mu.Unlock()
		}()

		l.leaderLoop(l.leaderCtx)
		return nil // Errors are handled by logging and attempting to recover or step down.
	})
}

// leaderLoop is the main loop for leader election.
func (l *LeaderElector) leaderLoop(ctx context.Context) {
	l.log.Debug("Leader loop started.")
	// Attempt to become leader immediately on start.
	// Use a short timeout for the initial attempt.
	initialAttemptCtx, initialCancel := context.WithTimeout(ctx, l.leaderElectionInterval/2)
	l.attemptElectionOrRenewal(initialAttemptCtx)
	initialCancel() // Release resources of initialAttemptCtx

	ticker := time.NewTicker(l.leaderElectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.log.Infof("Leader election context cancelled. Reason: %v. Exiting leader loop.", ctx.Err())
			return
		case <-ticker.C:
			if ctx.Err() != nil { // Check if context was cancelled during tick wait
				l.log.Infof("Context cancelled while waiting for ticker. Reason: %v. Exiting leader loop.", ctx.Err())
				return
			}
			l.log.Debug("Ticker event: attempting election or renewal.")
			attemptCtx, attemptCancel := context.WithTimeout(ctx, l.leaderElectionInterval-time.Millisecond*100) // Give some buffer
			l.attemptElectionOrRenewal(attemptCtx)
			attemptCancel()
		}
	}
}

// attemptElectionOrRenewal tries to become leader or renews the leadership if already leader.
func (l *LeaderElector) attemptElectionOrRenewal(ctx context.Context) {
	leaderKey := l.getLeaderKey()
	l.log.Debugf("Attempting election/renewal for leader key: %s", leaderKey)

	currentLeaderID, err := l.store.GetLockOwner(ctx, leaderKey)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			l.log.Warnf("Context error while getting lock owner for key '%s': %v", leaderKey, ctx.Err())
			return // Context error, don't proceed
		}
		// Assume "key not found" if error is not context related.
		// Specific error handling for "not found" should be preferred if Store provides it.
		l.log.Warnf("Failed to get current leader for key '%s': %v. Assuming no leader or key expired.", leaderKey, err)
		currentLeaderID = "" // Treat error as no current leader for safety, attempt to acquire.
	}
	l.log.Debugf("Current lock owner for key '%s' is '%s'. This instance is '%s'.", leaderKey, currentLeaderID, l.instanceID)


	l.mu.Lock()
	isCurrentlyLeader := l.isLeader
	l.mu.Unlock()

	if isCurrentlyLeader {
		if currentLeaderID == l.instanceID {
			l.log.Debugf("Instance is leader, renewing lock for key '%s'", leaderKey)
			renewed, renewErr := l.store.RenewLock(ctx, leaderKey, l.instanceID, l.leaderLockExpiry)
			if renewErr != nil {
				if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					l.log.Warnf("Context error during lock renewal for key '%s': %v", leaderKey, ctx.Err())
					// Don't change leader status based on context error during renewal, might recover.
					return
				}
				l.log.Errorf("Failed to renew leadership lock for key '%s': %v. Stepping down.", leaderKey, renewErr)
				l.setLeader(false, "renewal_failed_error")
			} else if !renewed {
				l.log.Warnf("Leadership renewal for key '%s' was not successful (e.g. lock lost or not granted by store). Stepping down.", leaderKey)
				l.setLeader(false, "renewal_not_granted_by_store")
			} else {
				l.log.Debugf("Leadership renewed for instance on key '%s'", leaderKey)
			}
		} else {
			l.log.Warnf("Instance thought it was leader, but current lock owner for key '%s' is '%s'. Stepping down.", leaderKey, currentLeaderID)
			l.setLeader(false, "lock_usurped_or_expired_unexpectedly")
		}
	} else { // Not currently leader
		if currentLeaderID == "" {
			l.log.Infof("No active leader detected for key '%s'. Instance attempting to become leader.", leaderKey)
			l.tryToBecomeLeader(ctx, leaderKey)
		} else if currentLeaderID == l.instanceID {
			// We are listed as leader in store, but don't know it locally (e.g. post-restart).
			// Try to take ownership definitively.
			l.log.Warnf("Instance is listed as leader in store for key '%s', but was not aware. Attempting to confirm and take leadership.", leaderKey)
			l.tryToBecomeLeader(ctx, leaderKey) 
		} else {
			l.log.Debugf("Instance '%s' is current leader for key '%s'. This instance ('%s') is a follower.", currentLeaderID, leaderKey, l.instanceID)
		}
	}
}

// tryToBecomeLeader attempts to acquire the leader lock.
func (l *LeaderElector) tryToBecomeLeader(ctx context.Context, leaderKey string) {
	l.log.Infof("Instance attempting to acquire leadership for key '%s'...", leaderKey)
	acquired, err := l.store.AcquireLock(ctx, leaderKey, l.instanceID, l.leaderLockExpiry)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			l.log.Warnf("Context cancelled or deadline exceeded while trying to acquire leadership for key '%s': %v", leaderKey, ctx.Err())
		} else {
			l.log.Errorf("Error trying to acquire leadership for key '%s': %v", leaderKey, err)
		}
		return
	}

	if acquired {
		l.log.Infof("Instance successfully acquired leadership for key '%s'.", leaderKey)
		l.setLeader(true, "acquired_new_lock_successfully")
	} else {
		l.log.Debugf("Instance failed to acquire leadership for key '%s' (already locked or SetNX failed).", leaderKey)
		if l.IsLeader() { 
			l.log.Warnf("Attempted to acquire lock for %s but failed, yet instance thought it was leader. Correcting.", leaderKey)
		}
		l.setLeader(false, "acquire_lock_denied_by_store")
	}
}

// releaseLeadership releases the lock if this instance is the leader.
func (l *LeaderElector) releaseLeadership(ctx context.Context, reason string) {
	l.mu.Lock()
	if !l.isLeader {
		l.mu.Unlock()
		l.log.Debugf("Instance is not leader, no need to release leadership (reason: %s).", reason)
		return
	}
	l.isLeader = false
	l.mu.Unlock() 

	leaderKey := l.getLeaderKey()
	l.log.Infof("Instance, which believed it was leader, attempting to release leadership for key '%s' (reason: %s).", leaderKey, reason)

	checkCtx, checkCancel := context.WithTimeout(ctx, l.leaderElectionInterval/2)
	defer checkCancel()
	currentOwner, err := l.store.GetLockOwner(checkCtx, leaderKey)
	if err != nil {
		l.log.Warnf("Failed to get lock owner for key '%s' during release: %v. Proceeding with release attempt cautiously.", leaderKey, err)
	} else if currentOwner != "" && currentOwner != l.instanceID {
		l.log.Warnf("Instance attempted to release lock for key '%s', but owner is '%s'. Not releasing.", leaderKey, currentOwner)
		return
	}

	releaseOpCtx, releaseOpCancel := context.WithTimeout(ctx, l.leaderElectionInterval/2)
	defer releaseOpCancel()
	err = l.store.ReleaseLock(releaseOpCtx, leaderKey, l.instanceID)
	if err != nil {
		l.log.Errorf("Failed to release leadership lock for key '%s': %v", leaderKey, err)
	} else {
		l.log.Infof("Leadership lock for key '%s' released by instance.", leaderKey)
	}
}

// IsLeader returns true if this instance is currently the leader.
func (l *LeaderElector) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLeader
}

// setLeader updates the leadership status and logs changes.
func (l *LeaderElector) setLeader(isLeader bool, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isLeader == isLeader {
		return // No change
	}
	l.isLeader = isLeader

	if isLeader {
		l.log.Infof("IS NOW LEADER. Reason: %s", reason)
	} else {
		l.log.Infof("IS NO LONGER LEADER. Reason: %s", reason)
	}
}

// Stop gracefully stops the leader election loop and releases leadership if held.
func (l *LeaderElector) Stop() {
	l.log.Infof("Stopping leader elector for namespace %s...", l.namespace)
	l.mu.Lock()
	if l.cancelLeader != nil {
		l.cancelLeader()
		l.cancelLeader = nil 
	}
	wasLeader := l.isLeader 
	l.mu.Unlock()


	if wasLeader {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), l.leaderLockExpiry/2+time.Millisecond*200) 
		defer cancelRelease()
		l.log.Debugf("Calling releaseLeadership from Stop() because wasLeader=true.")
		l.releaseLeadership(releaseCtx, "elector_stopped")
	} else {
		l.log.Debugf("Stop() called, wasLeader=false, no proactive release needed.")
	}
	l.log.Infof("Leader elector for namespace %s has been signaled to stop.", l.namespace)
}
