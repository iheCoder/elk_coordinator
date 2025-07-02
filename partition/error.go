package partition

import "errors"

var (
	ErrPartitionNotFound = errors.New("partition not found")

	// 错误常量定义 - 用于区分正常的"无法获取"情况和系统错误
	ErrPartitionNotExists     = errors.New("partition not exists")
	ErrPartitionAlreadyHeld   = errors.New("partition already held")
	ErrPartitionLockFailed    = errors.New("partition lock failed")
	ErrOptimisticLockFailed   = errors.New("optimistic lock failed: version mismatch")
	ErrPartitionAlreadyExists = errors.New("partition already exists")

	// 归档相关错误
	ErrArchivedPartitionNotFound = errors.New("archived partition not found")

	ErrWorkerCacheRebuildFailed = errors.New("worker ID cache rebuild failed")
	ErrWorkerMappingNotFound    = errors.New("worker ID mapping not found")
)
