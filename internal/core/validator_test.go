package core

import (
	"strings"
	"testing"
)

func TestNewJobConfigValidator(t *testing.T) {
	config := &JobConfig{}
	validator := NewJobConfigValidator(config)

	if validator == nil {
		t.Error("NewJobConfigValidator should return a non-nil validator")
	}

	if validator.config != config {
		t.Error("Validator should store the provided config")
	}
}

func TestJobConfigValidator_Validate_NilConfig(t *testing.T) {
	validator := NewJobConfigValidator(nil)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for nil config")
	}

	expectedMsg := "配置不能为空"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestJobConfigValidator_Validate_EmptyContent(t *testing.T) {
	config := &JobConfig{}
	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for empty content")
	}

	if !strings.Contains(err.Error(), "基本结构验证失败: 任务配置中没有content") {
		t.Errorf("Expected basic structure validation error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_MultipleContent(t *testing.T) {
	config := &JobConfig{}
	config.Job.Content = make([]struct {
		Reader struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"reader"`
		Writer struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"writer"`
	}, 2)

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for multiple content")
	}

	if !strings.Contains(err.Error(), "基本结构验证失败: 当前版本只支持单个content配置") {
		t.Errorf("Expected multiple content error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_EmptyReaderName(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Reader.Name = ""

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for empty reader name")
	}

	if !strings.Contains(err.Error(), "Reader名称不能为空") {
		t.Errorf("Expected reader name error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_UnsupportedReader(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Reader.Name = "unsupportedreader"

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for unsupported reader")
	}

	if !strings.Contains(err.Error(), "不支持的Reader类型: unsupportedreader") {
		t.Errorf("Expected unsupported reader error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_EmptyWriterName(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Writer.Name = ""

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for empty writer name")
	}

	if !strings.Contains(err.Error(), "Writer名称不能为空") {
		t.Errorf("Expected writer name error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_UnsupportedWriter(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Writer.Name = "unsupportedwriter"

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for unsupported writer")
	}

	if !strings.Contains(err.Error(), "不支持的Writer类型: unsupportedwriter") {
		t.Errorf("Expected unsupported writer error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_MissingReaderParameter(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Reader.Parameter = nil

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for missing reader parameter")
	}

	if !strings.Contains(err.Error(), "Reader参数不能为空") {
		t.Errorf("Expected reader parameter error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_MissingWriterParameter(t *testing.T) {
	config := createBasicJobConfig()
	config.Job.Content[0].Writer.Parameter = nil

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for missing writer parameter")
	}

	if !strings.Contains(err.Error(), "Writer参数不能为空") {
		t.Errorf("Expected writer parameter error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_InvalidPort(t *testing.T) {
	config := createBasicJobConfig()

	// 设置无效端口
	readerParams := config.Job.Content[0].Reader.Parameter.(map[string]any)
	readerParams["port"] = float64(-1)

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for invalid port")
	}

	if !strings.Contains(err.Error(), "端口号必须在1-65535之间") {
		t.Errorf("Expected port validation error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_InvalidBatchSize(t *testing.T) {
	config := createBasicJobConfig()

	// 设置无效批次大小
	readerParams := config.Job.Content[0].Reader.Parameter.(map[string]any)
	readerParams["batchSize"] = float64(-1)

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for invalid batch size")
	}

	if !strings.Contains(err.Error(), "批次大小必须大于0") {
		t.Errorf("Expected batch size validation error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_InvalidWriteMode(t *testing.T) {
	config := createBasicJobConfig()

	// 设置无效写入模式
	writerParams := config.Job.Content[0].Writer.Parameter.(map[string]any)
	writerParams["writeMode"] = "invalid_mode"

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for invalid write mode")
	}

	if !strings.Contains(err.Error(), "不支持的写入模式: invalid_mode") {
		t.Errorf("Expected write mode validation error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_InvalidSettings(t *testing.T) {
	config := createBasicJobConfig()

	// 设置无效的错误百分比
	config.Job.Setting.ErrorLimit.Percentage = 1.5

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err == nil {
		t.Error("Validate should return error for invalid error percentage")
	}

	if !strings.Contains(err.Error(), "错误百分比必须在0-1之间") {
		t.Errorf("Expected error percentage validation error, got: %s", err.Error())
	}
}

func TestJobConfigValidator_Validate_ValidConfig(t *testing.T) {
	config := createBasicJobConfig()

	validator := NewJobConfigValidator(config)
	err := validator.Validate()

	if err != nil {
		t.Errorf("Validate should succeed for valid config, got error: %v", err)
	}
}

func TestValidateJobConfig(t *testing.T) {
	config := createBasicJobConfig()

	err := ValidateJobConfig(config)

	if err != nil {
		t.Errorf("ValidateJobConfig should succeed for valid config, got error: %v", err)
	}
}

func TestValidateJobConfig_NilConfig(t *testing.T) {
	err := ValidateJobConfig(nil)

	if err == nil {
		t.Error("ValidateJobConfig should return error for nil config")
	}
}

// createBasicJobConfig 创建一个基本的有效配置用于测试
func createBasicJobConfig() *JobConfig {
	config := &JobConfig{}
	config.Job.Content = make([]struct {
		Reader struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"reader"`
		Writer struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"writer"`
	}, 1)

	// 设置Reader
	config.Job.Content[0].Reader.Name = "mysqlreader"
	config.Job.Content[0].Reader.Parameter = map[string]any{
		"host":      "localhost",
		"port":      float64(3306),
		"database":  "test",
		"username":  "root",
		"password":  "password",
		"table":     "users",
		"batchSize": float64(1000),
	}

	// 设置Writer
	config.Job.Content[0].Writer.Name = "mysqlwriter"
	config.Job.Content[0].Writer.Parameter = map[string]any{
		"host":      "localhost",
		"port":      float64(3306),
		"database":  "test",
		"username":  "root",
		"password":  "password",
		"table":     "users",
		"batchSize": float64(1000),
		"writeMode": "insert",
	}

	// 设置Settings
	config.Job.Setting.Speed.Channel = 1
	config.Job.Setting.Speed.Bytes = 1024
	config.Job.Setting.ErrorLimit.Record = 0
	config.Job.Setting.ErrorLimit.Percentage = 0.02

	return config
}
