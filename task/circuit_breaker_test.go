package task

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errTest = errors.New("test error")

// Helper function to assert states and counts
func assertStats(t *testing.T, cb *CircuitBreaker, expectedState string, expectedConsecutive, expectedTotal, expectedFailedPartitionsLen int, expectedHalfOpen int32) {
	t.Helper()
	stats := cb.GetStats()
	if stats.State != expectedState {
		t.Errorf("Expected state %s, got %s", expectedState, stats.State)
	}
	if stats.ConsecutiveFailures != expectedConsecutive {
		t.Errorf("Expected consecutive failures %d, got %d", expectedConsecutive, stats.ConsecutiveFailures)
	}
	if stats.TotalFailures != expectedTotal {
		t.Errorf("Expected total failures %d, got %d", expectedTotal, stats.TotalFailures)
	}
	if stats.FailedPartitions != expectedFailedPartitionsLen {
		t.Errorf("Expected failed partitions length %d, got %d", expectedFailedPartitionsLen, stats.FailedPartitions)
	}
	if stats.HalfOpenRequests != expectedHalfOpen {
		t.Errorf("Expected half-open requests %d, got %d", expectedHalfOpen, stats.HalfOpenRequests)
	}
}

// TestNewCircuitBreaker tests the creation of a new circuit breaker.
// Scenario: Create a circuit breaker with default and custom configurations.
// Expected Result: Correct initial state and configuration values.
func TestNewCircuitBreaker(t *testing.T) {
	// Default config
	cbDefault := NewCircuitBreaker(CircuitBreakerConfig{}, "test-worker")
	if cbDefault.state != CBStateClosed {
		t.Errorf("Default CB: Expected state %s, got %s", CBStateClosed, cbDefault.state)
	}
	if cbDefault.config.MaxHalfOpenRequests != 1 {
		t.Errorf("Default CB: Expected MaxHalfOpenRequests 1, got %d", cbDefault.config.MaxHalfOpenRequests)
	}
	if cbDefault.config.FailureTimeWindow != 5*time.Minute {
		t.Errorf("Default CB: Expected FailureTimeWindow 5m, got %v", cbDefault.config.FailureTimeWindow)
	}

	// Custom config
	cfg := CircuitBreakerConfig{
		ConsecutiveFailureThreshold: 5,
		TotalFailureThreshold:       10,
		OpenTimeout:                 30 * time.Second,
		MaxHalfOpenRequests:         3,
		FailureTimeWindow:           1 * time.Minute,
	}
	cbCustom := NewCircuitBreaker(cfg, "test-worker")
	if cbCustom.state != CBStateClosed {
		t.Errorf("Custom CB: Expected state %s, got %s", CBStateClosed, cbCustom.state)
	}
	if cbCustom.config.ConsecutiveFailureThreshold != 5 {
		t.Errorf("Custom CB: Expected ConsecutiveFailureThreshold 5, got %d", cbCustom.config.ConsecutiveFailureThreshold)
	}
	assertStats(t, cbCustom, CBStateClosed, 0, 0, 0, 0)
}

// TestRecordFailure_TransitionsToOpen_ConsecutiveThreshold tests transition to Open state via consecutive failures.
// Scenario: Record distinct partition failures until ConsecutiveFailureThreshold is met.
// Expected Result: State becomes CBStateOpen.
func TestRecordFailure_TransitionsToOpen_ConsecutiveThreshold(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 2, TotalFailureThreshold: 5, OpenTimeout: 1 * time.Minute}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)
	assertStats(t, cb, CBStateClosed, 1, 1, 1, 0)
	if !cb.AllowProcess() {
		t.Error("AllowProcess should return true in Closed state")
	}

	cb.RecordFailure(2, errTest) // Meets ConsecutiveFailureThreshold
	assertStats(t, cb, CBStateOpen, 2, 2, 2, 0)
	if cb.AllowProcess() {
		t.Error("AllowProcess should return false in Open state")
	}
}

// TestRecordFailure_TransitionsToOpen_TotalThreshold tests transition to Open state via total failures.
// Scenario: Record distinct partition failures until TotalFailureThreshold is met.
// Expected Result: State becomes CBStateOpen.
func TestRecordFailure_TransitionsToOpen_TotalThreshold(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 5, TotalFailureThreshold: 2, OpenTimeout: 1 * time.Minute}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)
	// Reset consecutive to ensure total threshold is the trigger
	cb.mu.Lock()
	cb.consecutiveFailures = 0
	cb.mu.Unlock()
	assertStats(t, cb, CBStateClosed, 0, 1, 1, 0)

	cb.RecordFailure(2, errTest)                // Meets TotalFailureThreshold
	assertStats(t, cb, CBStateOpen, 1, 2, 2, 0) // consecutiveFailures is 1 because the second failure was new
}

// TestRecordSuccess_ResetsCounters_And_PartitionFailures_InClosedState tests success handling in Closed state.
// Scenario: Record failures, then a success for a failed partition while Closed.
// Expected Result: consecutiveFailures resets, specific partition failure removed, totalFailures decremented.
func TestRecordSuccess_ResetsCounters_And_PartitionFailures_InClosedState(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 3, TotalFailureThreshold: 5}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)
	cb.RecordFailure(2, errTest)
	assertStats(t, cb, CBStateClosed, 2, 2, 2, 0)

	cb.RecordSuccess(1)
	assertStats(t, cb, CBStateClosed, 0, 1, 1, 0)
	if _, exists := cb.failedPartitions[1]; exists {
		t.Error("Partition 1 failure should have been cleared")
	}
	if _, exists := cb.failedPartitions[2]; !exists {
		t.Error("Partition 2 failure should still exist")
	}
}

// TestAllowRequest_OpenToHalfOpen_AfterTimeout tests transition from Open to HalfOpen.
// Scenario: Trip to Open, wait for OpenTimeout, call AllowProcess.
// Expected Result: State becomes CBStateHalfOpen, AllowProcess returns true, halfOpenRequests is 1.
func TestAllowRequest_OpenToHalfOpen_AfterTimeout(t *testing.T) {
	openTimeout := 50 * time.Millisecond
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: openTimeout, MaxHalfOpenRequests: 1}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Trip to Open
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0)
	if cb.AllowProcess() {
		t.Error("AllowProcess should be false immediately after tripping to Open")
	}

	time.Sleep(openTimeout + 10*time.Millisecond) // Wait for OpenTimeout to expire

	if !cb.AllowProcess() { // This should transition to HalfOpen and allow the request
		t.Error("AllowProcess should return true to transition to HalfOpen")
	}
	assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, 1)
}

// TestHalfOpen_Success_TransitionsToClosed tests success in HalfOpen state.
// Scenario: Breaker in HalfOpen, record a success.
// Expected Result: State becomes CBStateClosed, counters reset, failedPartitions cleared.
func TestHalfOpen_Success_TransitionsToClosed(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond, MaxHalfOpenRequests: 1}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)                      // Open
	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond) // Wait for timeout
	cb.AllowProcess()                                 // HalfOpen, request allowed
	assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, 1)

	cb.RecordSuccess(1)                           // Success in HalfOpen
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0) // halfOpenRequests should be 0 after AddInt32(-1)
}

// TestHalfOpen_Failure_TransitionsToOpen tests failure in HalfOpen state.
// Scenario: Breaker in HalfOpen, record a failure.
// Expected Result: State becomes CBStateOpen, halfOpenRequests becomes 0.
func TestHalfOpen_Failure_TransitionsToOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond, MaxHalfOpenRequests: 1}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)                      // Open
	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond) // Wait for timeout
	cb.AllowProcess()                                 // HalfOpen, request allowed
	assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, 1)

	cb.RecordFailure(2, errTest)                // Failure in HalfOpen (can be same or different partition)
	assertStats(t, cb, CBStateOpen, 2, 2, 2, 0) // consecutiveFailures increments as it's a new partition
}

// TestHalfOpen_RespectsMaxHalfOpenRequests tests MaxHalfOpenRequests limit.
// Scenario: Breaker in HalfOpen, attempt more requests than MaxHalfOpenRequests.
// Expected Result: Only MaxHalfOpenRequests are allowed.
func TestHalfOpen_RespectsMaxHalfOpenRequests(t *testing.T) {
	maxHalfOpen := int32(2)
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond, MaxHalfOpenRequests: maxHalfOpen}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest)                      // Open
	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond) // Wait for timeout

	for i := int32(0); i < maxHalfOpen; i++ {
		if !cb.AllowProcess() {
			t.Errorf("AllowProcess should return true for half-open request #%d", i+1)
		}
		if atomic.LoadInt32(&cb.halfOpenRequests) != i+1 {
			t.Errorf("Expected halfOpenRequests %d, got %d", i+1, atomic.LoadInt32(&cb.halfOpenRequests))
		}
		assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, i+1)
	}

	if cb.AllowProcess() { // This one should be denied
		t.Error("AllowProcess should return false when MaxHalfOpenRequests is reached")
	}
	assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, maxHalfOpen)
}

// TestReset_ResetsToClosedState tests the Reset method.
// Scenario: Trip breaker, then call Reset.
// Expected Result: State becomes CBStateClosed, counters and failures cleared.
func TestReset_ResetsToClosedState(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Open
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0)

	cb.Reset()
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0)
	if !cb.AllowProcess() {
		t.Error("AllowProcess should return true after Reset")
	}
}

// TestCleanExpiredFailures tests cleanup of expired failures.
// Scenario: Record failures, wait for FailureTimeWindow, trigger cleanup.
// Expected Result: Expired failures removed, totalFailures updated.
func TestCleanExpiredFailures(t *testing.T) {
	failureWindow := 50 * time.Millisecond
	cfg := CircuitBreakerConfig{
		FailureTimeWindow:           failureWindow,
		TotalFailureThreshold:       10, // High threshold to avoid tripping
		ConsecutiveFailureThreshold: 2,
	}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Timestamp A
	cb.RecordFailure(2, errTest) // Timestamp B
	assertStats(t, cb, CBStateOpen, 2, 2, 2, 0)

	time.Sleep(failureWindow + time.Millisecond)
	cb.RecordFailure(3, errTest) // Timestamp C (later)
	assertStats(t, cb, CBStateClosed, 1, 1, 1, 0)
}

// TestFailedPartitions_ReturnsCorrectFailures tests FailedPartitions method.
// Scenario: Record failures, call FailedPartitions.
// Expected Result: Correct map of non-expired failed partitions.
func TestFailedPartitions_ReturnsCorrectFailures(t *testing.T) {
	cfg := CircuitBreakerConfig{FailureTimeWindow: 1 * time.Minute}
	cb := NewCircuitBreaker(cfg, "test-worker")
	err2 := errors.New("another error")

	cb.RecordFailure(1, errTest)
	cb.RecordFailure(2, err2)

	failed := cb.FailedPartitions()
	if len(failed) != 2 {
		t.Errorf("Expected 2 failed partitions, got %d", len(failed))
	}
	if err := failed[1]; err != errTest {
		t.Errorf("Expected error for partition 1 to be '%v', got '%v'", errTest, err)
	}
	if err := failed[2]; err != err2 {
		t.Errorf("Expected error for partition 2 to be '%v', got '%v'", err2, err)
	}
}

// TestGetStats_ReturnsCorrectStats tests GetStats method.
// Scenario: Manipulate breaker, call GetStats.
// Expected Result: Accurate CircuitBreakerStats.
func TestGetStats_ReturnsCorrectStats(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond}
	cb := NewCircuitBreaker(cfg, "test-worker")
	initialTime := cb.lastStateChange

	cb.RecordFailure(1, errTest) // Open
	statsOpen := cb.GetStats()
	if statsOpen.State != CBStateOpen {
		t.Errorf("Expected state Open, got %s", statsOpen.State)
	}
	if statsOpen.ConsecutiveFailures != 1 || statsOpen.TotalFailures != 1 || statsOpen.FailedPartitions != 1 {
		t.Error("Failure counts incorrect in Open state")
	}
	if statsOpen.LastStateChange == initialTime {
		t.Error("LastStateChange should have updated on transition to Open")
	}
	openTime := statsOpen.LastStateChange

	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond)
	cb.AllowProcess() // HalfOpen
	statsHalfOpen := cb.GetStats()
	if statsHalfOpen.State != CBStateHalfOpen {
		t.Errorf("Expected state HalfOpen, got %s", statsHalfOpen.State)
	}
	if statsHalfOpen.HalfOpenRequests != 1 {
		t.Errorf("Expected 1 half-open request, got %d", statsHalfOpen.HalfOpenRequests)
	}
	if statsHalfOpen.LastStateChange == openTime {
		t.Error("LastStateChange should have updated on transition to HalfOpen")
	}
}

// TestRecordFailure_ConsecutiveFailures_SamePartition tests consecutive failure logic for the same partition.
// Scenario: Record multiple failures for the same partition.
// Expected Result: consecutiveFailures increments only for the first failure of that partition.
func TestRecordFailure_ConsecutiveFailures_SamePartition(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, TotalFailureThreshold: 5}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // First failure for partition 1
	// State should be Open because ConsecutiveFailureThreshold is 1 and this is the first distinct failure
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0)

	// Record another failure for the same partition 1
	// This should NOT increment consecutiveFailures further due to `if wasNew`
	// The breaker is already Open, so state remains Open.
	// We need to reset to Closed to observe consecutiveFailures behavior accurately for a second hit.
	cb.Reset()
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0)

	cb.RecordFailure(1, errTest)                // First failure for partition 1 again
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0) // Trips again due to threshold 1

	cb.RecordFailure(1, errTest) // Second failure for partition 1, breaker already Open
	// State remains Open. consecutiveFailures should still be 1 because it wasn't a "new" failure.
	// totalFailures remains 1.
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0)
}

// TestRecordFailure_DoesNotIncrementConsecutive_IfNotNew tests that consecutiveFailures doesn't increment if partition failure isn't new.
// Scenario: Record failure for partition A, then another for partition A.
// Expected Result: consecutiveFailures increments for the first, not for the second.
func TestRecordFailure_DoesNotIncrementConsecutive_IfNotNew(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 3, TotalFailureThreshold: 5}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Partition A, first failure
	assertStats(t, cb, CBStateClosed, 1, 1, 1, 0)

	cb.RecordFailure(1, errTest)                  // Partition A, second failure (not new)
	assertStats(t, cb, CBStateClosed, 1, 1, 1, 0) // ConsecutiveFailures should still be 1

	cb.RecordFailure(2, errTest)                  // Partition B, first failure (new)
	assertStats(t, cb, CBStateClosed, 2, 2, 2, 0) // ConsecutiveFailures becomes 2
}

// TestRecordSuccess_InHalfOpen_ClearsAllFailuresOnTransitionToClosed tests that all failures are cleared on successful transition from HalfOpen.
// Scenario: Breaker has multiple failed partitions, goes to HalfOpen, then a success is recorded.
// Expected Result: Transitions to Closed, all failedPartitions cleared, totalFailures is 0.
func TestRecordSuccess_InHalfOpen_ClearsAllFailuresOnTransitionToClosed(t *testing.T) {
	cfg := CircuitBreakerConfig{
		ConsecutiveFailureThreshold: 1, // Trip easily
		TotalFailureThreshold:       5,
		OpenTimeout:                 50 * time.Millisecond,
		MaxHalfOpenRequests:         1,
	}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Failure 1, trips to Open
	// Manually add another failure to simulate it happened before tripping, or during open state (though not typical for this CB)
	// To ensure there are multiple failures when HalfOpen success occurs.
	// This requires a bit of internal manipulation for a clean test setup for this specific scenario.
	cb.mu.Lock()
	cb.failedPartitions[2] = PartitionFailure{Error: errTest, Timestamp: time.Now()}
	cb.totalFailures++
	// cb.consecutiveFailures remains 1 as it was the trigger
	cb.mu.Unlock()

	assertStats(t, cb, CBStateOpen, 1, 2, 2, 0)

	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond) // Wait for timeout
	if !cb.AllowProcess() {                           // Transition to HalfOpen
		t.Fatal("AllowProcess should succeed for HalfOpen transition")
	}
	assertStats(t, cb, CBStateHalfOpen, 1, 2, 2, 1)

	cb.RecordSuccess(1) // Success for partition 1 in HalfOpen
	// Expect all failures to be cleared, not just partition 1
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0)
	if len(cb.failedPartitions) != 0 {
		t.Errorf("Expected all failedPartitions to be cleared, got %d", len(cb.failedPartitions))
	}
}

// TestState_ReturnsCurrentState tests the State method.
// Scenario: Change state and check if State() returns the correct one.
// Expected Result: State() returns the current internal state.
func TestState_ReturnsCurrentState(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond}
	cb := NewCircuitBreaker(cfg, "test-worker")

	if cb.State() != CBStateClosed {
		t.Errorf("Expected state %s, got %s", CBStateClosed, cb.State())
	}

	cb.RecordFailure(1, errTest) // Trip to Open
	if cb.State() != CBStateOpen {
		t.Errorf("Expected state %s, got %s", CBStateOpen, cb.State())
	}

	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond)
	cb.AllowProcess() // Transition to HalfOpen
	if cb.State() != CBStateHalfOpen {
		t.Errorf("Expected state %s, got %s", CBStateHalfOpen, cb.State())
	}

	cb.RecordSuccess(1) // Transition to Closed
	if cb.State() != CBStateClosed {
		t.Errorf("Expected state %s, got %s", CBStateClosed, cb.State())
	}
}

// TestRecordSuccess_WhenAlreadyClosed tests recording a success when the breaker is already closed.
// Scenario: Breaker is closed, no failures. Record a success.
// Expected Result: State remains closed, counts remain zero.
func TestRecordSuccess_WhenAlreadyClosed(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{}, "test-worker")
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0)

	cb.RecordSuccess(1) // Success on a partition that wasn't marked as failed
	assertStats(t, cb, CBStateClosed, 0, 0, 0, 0)
}

// TestAllowRequest_OpenState_StaysOpenBeforeTimeout tests that AllowProcess returns false
// and state remains Open if OpenTimeout has not passed.
// Scenario: Breaker is Open. Call AllowProcess before OpenTimeout.
// Expected Result: AllowProcess returns false, state remains Open.
func TestAllowRequest_OpenState_StaysOpenBeforeTimeout(t *testing.T) {
	openTimeout := 100 * time.Millisecond
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: openTimeout}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Trip to Open
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0)

	time.Sleep(openTimeout / 2) // Wait for less than OpenTimeout

	if cb.AllowProcess() {
		t.Error("AllowProcess should return false in Open state before timeout")
	}
	assertStats(t, cb, CBStateOpen, 1, 1, 1, 0) // State should remain Open
}

// TestRecordFailure_InHalfOpen_WithDifferentPartition tests that a failure on a *different* partition
// in HalfOpen state also transitions to Open.
// Scenario: Breaker in HalfOpen due to partition A. Record failure for partition B.
// Expected Result: State transitions to Open.
func TestRecordFailure_InHalfOpen_WithDifferentPartition(t *testing.T) {
	cfg := CircuitBreakerConfig{ConsecutiveFailureThreshold: 1, OpenTimeout: 50 * time.Millisecond, MaxHalfOpenRequests: 1}
	cb := NewCircuitBreaker(cfg, "test-worker")

	cb.RecordFailure(1, errTest) // Partition 1 fails, trips to Open
	time.Sleep(cfg.OpenTimeout + 10*time.Millisecond)
	cb.AllowProcess() // Allows request for partition 1 (hypothetically), transitions to HalfOpen
	assertStats(t, cb, CBStateHalfOpen, 1, 1, 1, 1)

	cb.RecordFailure(2, errors.New("failure on partition 2")) // Failure on a different partition (2)
	// consecutiveFailures should be 2 (1 from partition 1 + 1 from partition 2)
	// totalFailures should be 2
	// failedPartitions should contain 2 entries
	// halfOpenRequests should be 0
	assertStats(t, cb, CBStateOpen, 2, 2, 2, 0)
}
