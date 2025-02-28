package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Level 日志级别
type Level int

const (
	LevelError Level = iota
	LevelWarn
	LevelInfo
	LevelDebug
)

// Logger 统一的日志记录器
type Logger struct {
	level     Level
	prefix    string
	logFile   *os.File
	stdLogger *log.Logger
}

// Option 日志选项
type Option struct {
	Level     Level
	Prefix    string
	LogFile   string
	WithTime  bool
	WithLevel bool
}

// New 创建新的日志记录器
func New(opt *Option) *Logger {
	if opt == nil {
		opt = &Option{
			Level:     LevelInfo,
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

// getCaller 获取调用者信息
func getCaller() (string, int) {
	// 我们从第3层开始查找
	for i := 3; i < 15; i++ { // 设置合理的最大查找深度
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		// 跳过对 logger 包自身的引用
		if !strings.Contains(file, "pkg/logger") {
			// 获取相对于工作目录的路径
			if idx := strings.Index(file, "datax/"); idx != -1 {
				return file[idx:], line
			}
			return filepath.Base(file), line
		}
	}
	return "unknown", 0
}

// formatMessage 格式化日志消息
func (l *Logger) formatMessage(level Level, format string, v ...interface{}) string {
	// 获取调用者信息
	file, line := getCaller()
	caller := fmt.Sprintf("%s:%d", file, line)

	// 构建日志前缀
	prefix := ""
	switch level {
	case LevelError:
		prefix = "[ERROR]"
	case LevelWarn:
		prefix = "[WARN]"
	case LevelInfo:
		prefix = "[INFO]"
	case LevelDebug:
		prefix = "[DEBUG]"
	}

	if l.prefix != "" {
		prefix = fmt.Sprintf("%s %s", prefix, l.prefix)
	}

	// 格式化消息
	msg := fmt.Sprintf(format, v...)

	// 添加调用者信息
	return fmt.Sprintf("%s [%s] %s", prefix, caller, msg)
}

// logf 根据日志级别打印日志
func (l *Logger) logf(level Level, format string, v ...interface{}) {
	if level <= l.level {
		l.stdLogger.Print(l.formatMessage(level, format, v...))
	}
}

// Error 打印错误日志
func (l *Logger) Error(format string, v ...interface{}) {
	l.logf(LevelError, format, v...)
}

// Warn 打印警告日志
func (l *Logger) Warn(format string, v ...interface{}) {
	l.logf(LevelWarn, format, v...)
}

// Info 打印信息日志
func (l *Logger) Info(format string, v ...interface{}) {
	l.logf(LevelInfo, format, v...)
}

// Debug 打印调试日志
func (l *Logger) Debug(format string, v ...interface{}) {
	l.logf(LevelDebug, format, v...)
}

// GetLevel 获取当前日志级别
func (l *Logger) GetLevel() Level {
	return l.level
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level Level) {
	l.level = level
}

// GetLevelName 获取日志级别名称
func GetLevelName(level Level) string {
	switch level {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel 解析日志级别
func ParseLevel(level string) (Level, error) {
	switch level {
	case "ERROR":
		return LevelError, nil
	case "WARN":
		return LevelWarn, nil
	case "INFO":
		return LevelInfo, nil
	case "DEBUG":
		return LevelDebug, nil
	default:
		return LevelInfo, fmt.Errorf("未知的日志级别: %s", level)
	}
}
