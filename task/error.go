package task

import (
	"errors"
)

var ErrNoAvailablePartition = errors.New("no available partition to claim")
