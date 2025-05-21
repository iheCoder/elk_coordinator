package leader

import "context"

type PartitionPlaner interface {
	PartitionSize(ctx context.Context) (int64, error)
	GetNextMaxID(ctx context.Context, startID int64, rangeSize int64) (int64, error)
}
