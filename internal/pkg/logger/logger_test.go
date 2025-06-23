package logger

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		opt  *Option
		want Level
	}{
		{
			name: "默认选项",
			opt:  nil,
			want: LevelInfo,
		},
		{
			name: "自定义选项",
			opt: &Option{
				Level:     LevelDebug,
				Prefix:    "TEST",
				WithTime:  true,
				WithLevel: true,
			},
			want: LevelDebug,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := New(tt.opt)
			if logger.level != tt.want {
				t.Errorf("New() level = %v, want %v", logger.level, tt.want)
			}
			if logger.stdLogger == nil {
				t.Error("New() stdLogger should not be nil")
			}
			logger.Close()
		})
	}
}

func TestLogger_LogLevels(t *testing.T) {
	// 创建一个临时文件用于测试
	tmpFile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	logger := New(&Option{
		Level:     LevelDebug,
		Prefix:    "TEST",
		LogFile:   tmpFile.Name(),
		WithTime:  false, // 关闭时间戳以便测试
		WithLevel: true,
	})
	defer logger.Close()

	// 测试各种日志级别
	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")

	// 读取日志文件内容
	tmpFile.Seek(0, 0)
	content, err := io.ReadAll(tmpFile)
	if err != nil {
		t.Fatalf("读取日志文件失败: %v", err)
	}

	logContent := string(content)

	// 验证日志内容
	if !strings.Contains(logContent, "[ERROR] TEST error message") {
		t.Error("Error log not found")
	}
	if !strings.Contains(logContent, "[WARN] TEST warn message") {
		t.Error("Warn log not found")
	}
	if !strings.Contains(logContent, "[INFO] TEST info message") {
		t.Error("Info log not found")
	}
	if !strings.Contains(logContent, "[DEBUG] TEST debug message") {
		t.Error("Debug log not found")
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// 创建一个只记录ERROR级别的logger
	logger := New(&Option{
		Level:     LevelError,
		Prefix:    "TEST",
		LogFile:   tmpFile.Name(),
		WithTime:  false,
		WithLevel: true,
	})
	defer logger.Close()

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")

	// 读取日志文件内容
	tmpFile.Seek(0, 0)
	content, err := io.ReadAll(tmpFile)
	if err != nil {
		t.Fatalf("读取日志文件失败: %v", err)
	}

	logContent := string(content)

	// 只应该有ERROR级别的日志
	if !strings.Contains(logContent, "[ERROR] TEST error message") {
		t.Error("Error log not found")
	}
	if strings.Contains(logContent, "[WARN] TEST warn message") {
		t.Error("Warn log should be filtered out")
	}
	if strings.Contains(logContent, "[INFO] TEST info message") {
		t.Error("Info log should be filtered out")
	}
	if strings.Contains(logContent, "[DEBUG] TEST debug message") {
		t.Error("Debug log should be filtered out")
	}
}

func TestLogger_SetLevel(t *testing.T) {
	logger := New(&Option{Level: LevelInfo})
	defer logger.Close()

	if logger.GetLevel() != LevelInfo {
		t.Errorf("初始级别应该是 LevelInfo, 得到 %v", logger.GetLevel())
	}

	logger.SetLevel(LevelDebug)
	if logger.GetLevel() != LevelDebug {
		t.Errorf("设置后级别应该是 LevelDebug, 得到 %v", logger.GetLevel())
	}
}

func TestGetLevelName(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{LevelError, "ERROR"},
		{LevelWarn, "WARN"},
		{LevelInfo, "INFO"},
		{LevelDebug, "DEBUG"},
		{Level(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := GetLevelName(tt.level); got != tt.want {
				t.Errorf("GetLevelName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		level   string
		want    Level
		wantErr bool
	}{
		{"ERROR", LevelError, false},
		{"WARN", LevelWarn, false},
		{"INFO", LevelInfo, false},
		{"DEBUG", LevelDebug, false},
		{"INVALID", LevelInfo, true},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			got, err := ParseLevel(tt.level)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogger_FileCreation(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "test_log_dir_*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "subdir", "test.log")

	logger := New(&Option{
		Level:   LevelInfo,
		LogFile: logFile,
	})
	defer logger.Close()

	logger.Info("test message")

	// 验证文件是否创建
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("日志文件应该被创建")
	}

	// 验证文件内容
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("读取日志文件失败: %v", err)
	}

	if !strings.Contains(string(content), "test message") {
		t.Error("日志文件应该包含测试消息")
	}
}

func TestLogger_Close(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	logger := New(&Option{
		Level:   LevelInfo,
		LogFile: tmpFile.Name(),
	})

	// 测试关闭
	err = logger.Close()
	if err != nil {
		t.Errorf("Close() 应该成功, 得到错误: %v", err)
	}

	// 测试关闭没有文件的logger
	logger2 := New(&Option{Level: LevelInfo})
	err = logger2.Close()
	if err != nil {
		t.Errorf("Close() 没有文件的logger应该成功, 得到错误: %v", err)
	}
}
