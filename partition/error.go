package partition

import "errors"

var (
	ErrPartitionNotFound = errors.New("partition not found")

	// 错误常量定义 - 用于区分正常的"无法获取"情况和系统错误
	ErrPartitionNotExists   = errors.New("partition not exists")
	ErrPartitionAlreadyHeld = errors.New("partition already held")
	ErrPartitionLockFailed  = errors.New("partition lock failed")
)
