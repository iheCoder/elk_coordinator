// Package utils 提供通用工具功能
package utils

import (
	"log"
)

// LogLevel 定义日志等级
type LogLevel int

const (
	// DebugLevel 调试等级
	DebugLevel LogLevel = iota
	// InfoLevel 信息等级
	InfoLevel
	// WarnLevel 警告等级
	WarnLevel
	// ErrorLevel 错误等级
	ErrorLevel
)

// String 返回日志等级的字符串表示
func (l LogLevel) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger 定义日志接口
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// DefaultLogger 提供简单的默认日志实现
type DefaultLogger struct{}

// NewDefaultLogger 创建一个默认的日志实现
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{}
}

func (l *DefaultLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func (l *DefaultLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func (l *DefaultLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func (l *DefaultLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}

// LeveledLogger 带等级控制的日志封装
type LeveledLogger struct {
	logger Logger
	level  LogLevel
}

// NewLeveledLogger 创建一个带等级控制的日志实例，默认为WarnLevel
func NewLeveledLogger(logger Logger) *LeveledLogger {
	return &LeveledLogger{
		logger: logger,
		level:  WarnLevel,
	}
}

// NewLeveledLoggerWithLevel 创建一个指定等级的日志实例
func NewLeveledLoggerWithLevel(logger Logger, level LogLevel) *LeveledLogger {
	return &LeveledLogger{
		logger: logger,
		level:  level,
	}
}

// SetLevel 设置日志等级
func (l *LeveledLogger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevel 获取当前日志等级
func (l *LeveledLogger) GetLevel() LogLevel {
	return l.level
}

// Debugf 输出调试等级日志
func (l *LeveledLogger) Debugf(format string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.logger.Debugf(format, args...)
	}
}

// Infof 输出信息等级日志
func (l *LeveledLogger) Infof(format string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.logger.Infof(format, args...)
	}
}

// Warnf 输出警告等级日志
func (l *LeveledLogger) Warnf(format string, args ...interface{}) {
	if l.level <= WarnLevel {
		l.logger.Warnf(format, args...)
	}
}

// Errorf 输出错误等级日志
func (l *LeveledLogger) Errorf(format string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.logger.Errorf(format, args...)
	}
}
