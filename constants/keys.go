package elk_coordinator

// Key format string constants used across the coordinator.
// These define the structure of keys stored in the data store (e.g., Redis).
// Using '%s' for namespace substitution where applicable.
const (
	// HeartbeatKeyFormat is the format for worker heartbeat keys.
	// Usage: fmt.Sprintf(HeartbeatKeyFormat, namespace, workerID)
	HeartbeatKeyFormat = "elk_coord/%s/heartbeats/%s"

	// WorkersKeyFormat is the format for the key storing the set of active workers.
	// Usage: fmt.Sprintf(WorkersKeyFormat, namespace)
	WorkersKeyFormat = "elk_coord/%s/workers"

	// LeaderKeyFormat is the format for the leader election lock key.
	// Usage: fmt.Sprintf(LeaderKeyFormat, namespace)
	LeaderKeyFormat = "elk_coord/%s/leader"

	// PartitionLockKeyFormat is the format for partition-specific lock keys.
	// Usage: fmt.Sprintf(PartitionLockKeyFormat, namespace, partitionID)
	PartitionLockKeyFormat = "elk_coord/%s/locks/partition/%d"

	// PartitionInfoKeyFormat is the format for the key storing all partition information.
	// Usage: fmt.Sprintf(PartitionInfoKeyFormat, namespace)
	PartitionInfoKeyFormat = "elk_coord/%s/partitions_info"

	// TaskStatusKeyFormat is a generic example if task statuses were stored individually.
	// Not currently used by the provided snippets but shown as an example.
	// TaskStatusKeyFormat = "elk_coord/%s/tasks/%s/status"

	// Add other key formats here as needed.
)
