package utils

import (
	"fmt"
	"github.com/google/uuid"
	"os"
)

// GenerateNodeID 生成唯一的节点ID
func GenerateNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String()[:8])
}
