package utils

import (
	"bytes"
	"strings"
	"testing"
)

// 测试用的Logger实现，用于捕获日志输出
type TestLogger struct {
	buffer *bytes.Buffer
}

func NewTestLogger() *TestLogger {
	return &TestLogger{
		buffer: &bytes.Buffer{},
	}
}

func (t *TestLogger) Infof(format string, args ...interface{}) {
	t.buffer.WriteString("[INFO] ")
	t.buffer.WriteString(format)
	t.buffer.WriteString("\n")
}

func (t *TestLogger) Warnf(format string, args ...interface{}) {
	t.buffer.WriteString("[WARN] ")
	t.buffer.WriteString(format)
	t.buffer.WriteString("\n")
}

func (t *TestLogger) Errorf(format string, args ...interface{}) {
	t.buffer.WriteString("[ERROR] ")
	t.buffer.WriteString(format)
	t.buffer.WriteString("\n")
}

func (t *TestLogger) Debugf(format string, args ...interface{}) {
	t.buffer.WriteString("[DEBUG] ")
	t.buffer.WriteString(format)
	t.buffer.WriteString("\n")
}

func (t *TestLogger) GetOutput() string {
	return t.buffer.String()
}

func (t *TestLogger) Clear() {
	t.buffer.Reset()
}

func TestLeveledLogger_ErrorLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLoggerWithLevel(testLogger, ErrorLevel)

	// 在错误级别下，只有错误日志会输出
	leveledLogger.Debugf("This is debug")
	leveledLogger.Infof("This is info")
	leveledLogger.Warnf("This is warn")
	leveledLogger.Errorf("This is error")

	output := testLogger.GetOutput()
	if !strings.Contains(output, "[ERROR] This is error") {
		t.Errorf("Expected error log to be output, got: %s", output)
	}
	if strings.Contains(output, "[DEBUG]") || strings.Contains(output, "[INFO]") || strings.Contains(output, "[WARN]") {
		t.Errorf("Expected only error log to be output, got: %s", output)
	}
}

func TestLeveledLogger_WarnLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLoggerWithLevel(testLogger, WarnLevel)

	// 在警告级别下，警告和错误日志会输出
	leveledLogger.Debugf("This is debug")
	leveledLogger.Infof("This is info")
	leveledLogger.Warnf("This is warn")
	leveledLogger.Errorf("This is error")

	output := testLogger.GetOutput()
	if !strings.Contains(output, "[WARN] This is warn") {
		t.Errorf("Expected warn log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[ERROR] This is error") {
		t.Errorf("Expected error log to be output, got: %s", output)
	}
	if strings.Contains(output, "[DEBUG]") || strings.Contains(output, "[INFO]") {
		t.Errorf("Expected only warn and error logs to be output, got: %s", output)
	}
}

func TestLeveledLogger_InfoLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLoggerWithLevel(testLogger, InfoLevel)

	// 在信息级别下，信息、警告和错误日志会输出
	leveledLogger.Debugf("This is debug")
	leveledLogger.Infof("This is info")
	leveledLogger.Warnf("This is warn")
	leveledLogger.Errorf("This is error")

	output := testLogger.GetOutput()
	if !strings.Contains(output, "[INFO] This is info") {
		t.Errorf("Expected info log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[WARN] This is warn") {
		t.Errorf("Expected warn log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[ERROR] This is error") {
		t.Errorf("Expected error log to be output, got: %s", output)
	}
	if strings.Contains(output, "[DEBUG]") {
		t.Errorf("Expected debug log NOT to be output, got: %s", output)
	}
}

func TestLeveledLogger_DebugLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLoggerWithLevel(testLogger, DebugLevel)

	// 在调试级别下，所有日志都会输出
	leveledLogger.Debugf("This is debug")
	leveledLogger.Infof("This is info")
	leveledLogger.Warnf("This is warn")
	leveledLogger.Errorf("This is error")

	output := testLogger.GetOutput()
	if !strings.Contains(output, "[DEBUG] This is debug") {
		t.Errorf("Expected debug log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[INFO] This is info") {
		t.Errorf("Expected info log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[WARN] This is warn") {
		t.Errorf("Expected warn log to be output, got: %s", output)
	}
	if !strings.Contains(output, "[ERROR] This is error") {
		t.Errorf("Expected error log to be output, got: %s", output)
	}
}

func TestLeveledLogger_DefaultLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLogger(testLogger)

	// 默认应该是WarnLevel
	if leveledLogger.GetLevel() != WarnLevel {
		t.Errorf("Expected default level to be WarnLevel, got: %v", leveledLogger.GetLevel())
	}
}

func TestLeveledLogger_SetLevel(t *testing.T) {
	testLogger := NewTestLogger()
	leveledLogger := NewLeveledLogger(testLogger)

	// 测试设置日志级别
	leveledLogger.SetLevel(ErrorLevel)
	if leveledLogger.GetLevel() != ErrorLevel {
		t.Errorf("Expected level to be ErrorLevel, got: %v", leveledLogger.GetLevel())
	}
}

func TestLogLevel_String(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{LogLevel(999), "UNKNOWN"},
	}

	for _, test := range tests {
		if test.level.String() != test.expected {
			t.Errorf("Expected %s, got %s", test.expected, test.level.String())
		}
	}
}

// 示例：演示如何使用LeveledLogger
func ExampleLeveledLogger() {
	// 创建一个默认的Logger
	defaultLogger := NewDefaultLogger()

	// 创建一个带等级控制的Logger，默认为WarnLevel
	leveledLogger := NewLeveledLogger(defaultLogger)

	// 只有Warn和Error级别的日志会输出
	leveledLogger.Debugf("This debug message will be ignored")
	leveledLogger.Infof("This info message will be ignored")
	leveledLogger.Warnf("This warning will be shown")
	leveledLogger.Errorf("This error will be shown")

	// 修改日志级别为InfoLevel
	leveledLogger.SetLevel(InfoLevel)

	// 现在Info、Warn和Error级别的日志都会输出
	leveledLogger.Debugf("This debug message will still be ignored")
	leveledLogger.Infof("This info message will now be shown")
	leveledLogger.Warnf("This warning will be shown")
	leveledLogger.Errorf("This error will be shown")

	// 直接创建指定级别的Logger
	errorOnlyLogger := NewLeveledLoggerWithLevel(defaultLogger, ErrorLevel)
	errorOnlyLogger.Warnf("This warning will be ignored")
	errorOnlyLogger.Errorf("Only this error will be shown")
}
