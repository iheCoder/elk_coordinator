package elk_coordinator

import "context"

// Processor is an interface that task processors must implement
type Processor interface {
	// Process processes a range of tasks identified by minID and maxID
	// Returns the number of processed items and any error
	Process(ctx context.Context, minID, maxID int64, opts map[string]interface{}) (int64, error)

	// GetNextMaxID estimates the next batch's max ID based on currentMax
	// The rangeSize parameter suggests how far to look ahead
	// startID 是当前已分配分区的最大ID，而不一定是已处理完成的ID
	GetNextMaxID(ctx context.Context, startID int64, rangeSize int64) (int64, error)

	// SuggestPartitionSize suggests an optimal partition size based on the processor's knowledge
	// If not implemented or returns 0, the system will use the default calculation method
	SuggestPartitionSize(ctx context.Context) (int64, error)
}
