package test_utils

import (
	"fmt"
	"log"
	"os"
	"time"
)

// MockLogger 标准化输出的日志实现
type MockLogger struct {
	logger *log.Logger
	Debug  bool // 是否输出Debug级别日志
}

// NewMockLogger 创建一个新的MockLogger实例
func NewMockLogger(debug bool) *MockLogger {
	return &MockLogger{
		logger: log.New(os.Stdout, "", 0),
		Debug:  debug,
	}
}

// 获取当前时间格式化字符串
func getTimeString() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}

// Debugf 输出Debug级别日志
func (m *MockLogger) Debugf(format string, args ...interface{}) {
	if m.Debug {
		m.logger.Printf("[%s] [DEBUG] %s", getTimeString(), fmt.Sprintf(format, args...))
	}
}

// Infof 输出Info级别日志
func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.logger.Printf("[%s] [INFO] %s", getTimeString(), fmt.Sprintf(format, args...))
}

// Warnf 输出Warn级别日志
func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.logger.Printf("[%s] [WARN] %s", getTimeString(), fmt.Sprintf(format, args...))
}

// Errorf 输出Error级别日志
func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.logger.Printf("[%s] [ERROR] %s", getTimeString(), fmt.Sprintf(format, args...))
}
