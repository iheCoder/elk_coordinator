package data

import "errors"

var (
	ErrNotFound               = errors.New("data: data not found")
	ErrOptimisticLockFailed   = errors.New("data: optimistic lock failed")
	ErrPartitionAlreadyExists = errors.New("data: partition already exists")
)
