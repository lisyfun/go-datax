package core

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

// LogLevel 日志级别
type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// Logger 统一的日志记录器
type Logger struct {
	level     LogLevel
	prefix    string
	logFile   *os.File
	stdLogger *log.Logger
}

// LoggerOption 日志选项
type LoggerOption struct {
	Level     LogLevel
	Prefix    string
	LogFile   string
	WithTime  bool
	WithLevel bool
}

// NewLogger 创建新的日志记录器
func NewLogger(opt *LoggerOption) *Logger {
	if opt == nil {
		opt = &LoggerOption{
			Level:     LogLevelInfo,
			WithTime:  true,
			WithLevel: true,
		}
	}

	logger := &Logger{
		level:  opt.Level,
		prefix: opt.Prefix,
	}

	// 设置日志输出
	flags := 0
	if opt.WithTime {
		flags |= log.Ldate | log.Ltime | log.Lmicroseconds
	}

	if opt.LogFile != "" {
		// 确保日志目录存在
		if err := os.MkdirAll(filepath.Dir(opt.LogFile), 0755); err != nil {
			log.Printf("创建日志目录失败: %v", err)
			logger.stdLogger = log.New(os.Stdout, "", flags)
			return logger
		}

		// 打开日志文件
		f, err := os.OpenFile(opt.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("打开日志文件失败: %v", err)
			logger.stdLogger = log.New(os.Stdout, "", flags)
			return logger
		}
		logger.logFile = f
		logger.stdLogger = log.New(f, "", flags)
	} else {
		logger.stdLogger = log.New(os.Stdout, "", flags)
	}

	return logger
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// formatMessage 格式化日志消息
func (l *Logger) formatMessage(level LogLevel, format string, v ...interface{}) string {
	// 获取调用者信息
	_, file, line, ok := runtime.Caller(2)
	caller := ""
	if ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}

	// 构建日志前缀
	prefix := ""
	switch level {
	case LogLevelError:
		prefix = "[ERROR]"
	case LogLevelWarn:
		prefix = "[WARN]"
	case LogLevelInfo:
		prefix = "[INFO]"
	case LogLevelDebug:
		prefix = "[DEBUG]"
	}

	if l.prefix != "" {
		prefix = fmt.Sprintf("%s %s", prefix, l.prefix)
	}

	// 格式化消息
	msg := fmt.Sprintf(format, v...)

	// 添加调用者信息
	if caller != "" {
		return fmt.Sprintf("%s [%s] %s", prefix, caller, msg)
	}
	return fmt.Sprintf("%s %s", prefix, msg)
}

// logf 根据日志级别打印日志
func (l *Logger) logf(level LogLevel, format string, v ...interface{}) {
	if level <= l.level {
		l.stdLogger.Print(l.formatMessage(level, format, v...))
	}
}

// Error 打印错误日志
func (l *Logger) Error(format string, v ...interface{}) {
	l.logf(LogLevelError, format, v...)
}

// Warn 打印警告日志
func (l *Logger) Warn(format string, v ...interface{}) {
	l.logf(LogLevelWarn, format, v...)
}

// Info 打印信息日志
func (l *Logger) Info(format string, v ...interface{}) {
	l.logf(LogLevelInfo, format, v...)
}

// Debug 打印调试日志
func (l *Logger) Debug(format string, v ...interface{}) {
	l.logf(LogLevelDebug, format, v...)
}

// GetLevel 获取当前日志级别
func (l *Logger) GetLevel() LogLevel {
	return l.level
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevelName 获取日志级别名称
func GetLevelName(level LogLevel) string {
	switch level {
	case LogLevelError:
		return "ERROR"
	case LogLevelWarn:
		return "WARN"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel 解析日志级别
func ParseLevel(level string) (LogLevel, error) {
	switch level {
	case "ERROR":
		return LogLevelError, nil
	case "WARN":
		return LogLevelWarn, nil
	case "INFO":
		return LogLevelInfo, nil
	case "DEBUG":
		return LogLevelDebug, nil
	default:
		return LogLevelInfo, fmt.Errorf("未知的日志级别: %s", level)
	}
}
