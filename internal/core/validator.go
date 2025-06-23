package core

import (
	"fmt"
	"strings"
)

// ConfigValidator 配置验证器接口
type ConfigValidator interface {
	Validate() error
}

// JobConfigValidator 任务配置验证器
type JobConfigValidator struct {
	config *JobConfig
}

// NewJobConfigValidator 创建新的任务配置验证器
func NewJobConfigValidator(config *JobConfig) *JobConfigValidator {
	return &JobConfigValidator{
		config: config,
	}
}

// Validate 验证任务配置
func (v *JobConfigValidator) Validate() error {
	if v.config == nil {
		return fmt.Errorf("配置不能为空")
	}

	// 验证基本结构
	if err := v.validateBasicStructure(); err != nil {
		return fmt.Errorf("基本结构验证失败: %v", err)
	}

	// 验证内容配置
	if err := v.validateContent(); err != nil {
		return fmt.Errorf("内容配置验证失败: %v", err)
	}

	// 验证设置配置
	if err := v.validateSettings(); err != nil {
		return fmt.Errorf("设置配置验证失败: %v", err)
	}

	return nil
}

// validateBasicStructure 验证基本结构
func (v *JobConfigValidator) validateBasicStructure() error {
	if len(v.config.Job.Content) == 0 {
		return fmt.Errorf("任务配置中没有content")
	}

	if len(v.config.Job.Content) > 1 {
		return fmt.Errorf("当前版本只支持单个content配置")
	}

	return nil
}

// validateContent 验证内容配置
func (v *JobConfigValidator) validateContent() error {
	content := v.config.Job.Content[0]

	// 验证Reader配置
	if err := v.validateReader(&content.Reader); err != nil {
		return fmt.Errorf("Reader配置验证失败: %v", err)
	}

	// 验证Writer配置
	if err := v.validateWriter(&content.Writer); err != nil {
		return fmt.Errorf("Writer配置验证失败: %v", err)
	}

	return nil
}

// validateReader 验证Reader配置
func (v *JobConfigValidator) validateReader(reader *struct {
	Name      string `json:"name"`
	Parameter any    `json:"parameter"`
}) error {
	if reader.Name == "" {
		return fmt.Errorf("Reader名称不能为空")
	}

	// 验证Reader名称是否支持
	supportedReaders := []string{"mysqlreader", "postgresqlreader", "oraclereader"}
	if !contains(supportedReaders, reader.Name) {
		return fmt.Errorf("不支持的Reader类型: %s，支持的类型: %s", 
			reader.Name, strings.Join(supportedReaders, ", "))
	}

	if reader.Parameter == nil {
		return fmt.Errorf("Reader参数不能为空")
	}

	// 根据Reader类型验证具体参数
	return v.validateReaderParameter(reader.Name, reader.Parameter)
}

// validateWriter 验证Writer配置
func (v *JobConfigValidator) validateWriter(writer *struct {
	Name      string `json:"name"`
	Parameter any    `json:"parameter"`
}) error {
	if writer.Name == "" {
		return fmt.Errorf("Writer名称不能为空")
	}

	// 验证Writer名称是否支持
	supportedWriters := []string{"mysqlwriter", "postgresqlwriter", "oraclewriter"}
	if !contains(supportedWriters, writer.Name) {
		return fmt.Errorf("不支持的Writer类型: %s，支持的类型: %s", 
			writer.Name, strings.Join(supportedWriters, ", "))
	}

	if writer.Parameter == nil {
		return fmt.Errorf("Writer参数不能为空")
	}

	// 根据Writer类型验证具体参数
	return v.validateWriterParameter(writer.Name, writer.Parameter)
}

// validateReaderParameter 验证Reader参数
func (v *JobConfigValidator) validateReaderParameter(readerType string, parameter any) error {
	paramMap, ok := parameter.(map[string]any)
	if !ok {
		return fmt.Errorf("Reader参数格式错误")
	}

	switch readerType {
	case "mysqlreader", "postgresqlreader":
		return v.validateDatabaseReaderParameter(paramMap)
	case "oraclereader":
		return v.validateOracleReaderParameter(paramMap)
	default:
		return fmt.Errorf("未知的Reader类型: %s", readerType)
	}
}

// validateWriterParameter 验证Writer参数
func (v *JobConfigValidator) validateWriterParameter(writerType string, parameter any) error {
	paramMap, ok := parameter.(map[string]any)
	if !ok {
		return fmt.Errorf("Writer参数格式错误")
	}

	switch writerType {
	case "mysqlwriter", "postgresqlwriter":
		return v.validateDatabaseWriterParameter(paramMap)
	case "oraclewriter":
		return v.validateOracleWriterParameter(paramMap)
	default:
		return fmt.Errorf("未知的Writer类型: %s", writerType)
	}
}

// validateDatabaseReaderParameter 验证数据库Reader参数
func (v *JobConfigValidator) validateDatabaseReaderParameter(params map[string]any) error {
	requiredFields := []string{"host", "port", "database", "username", "password", "table"}
	
	for _, field := range requiredFields {
		if _, exists := params[field]; !exists {
			return fmt.Errorf("缺少必需字段: %s", field)
		}
	}

	// 验证端口号
	if port, ok := params["port"]; ok {
		if portNum, ok := port.(float64); ok {
			if portNum <= 0 || portNum > 65535 {
				return fmt.Errorf("端口号必须在1-65535之间")
			}
		}
	}

	// 验证批次大小
	if batchSize, ok := params["batchSize"]; ok {
		if size, ok := batchSize.(float64); ok {
			if size <= 0 {
				return fmt.Errorf("批次大小必须大于0")
			}
		}
	}

	return nil
}

// validateDatabaseWriterParameter 验证数据库Writer参数
func (v *JobConfigValidator) validateDatabaseWriterParameter(params map[string]any) error {
	requiredFields := []string{"host", "port", "database", "username", "password", "table"}
	
	for _, field := range requiredFields {
		if _, exists := params[field]; !exists {
			return fmt.Errorf("缺少必需字段: %s", field)
		}
	}

	// 验证端口号
	if port, ok := params["port"]; ok {
		if portNum, ok := port.(float64); ok {
			if portNum <= 0 || portNum > 65535 {
				return fmt.Errorf("端口号必须在1-65535之间")
			}
		}
	}

	// 验证批次大小
	if batchSize, ok := params["batchSize"]; ok {
		if size, ok := batchSize.(float64); ok {
			if size <= 0 {
				return fmt.Errorf("批次大小必须大于0")
			}
		}
	}

	// 验证写入模式
	if writeMode, ok := params["writeMode"]; ok {
		if mode, ok := writeMode.(string); ok {
			validModes := []string{"insert", "replace", "update"}
			if !contains(validModes, mode) {
				return fmt.Errorf("不支持的写入模式: %s，支持的模式: %s", 
					mode, strings.Join(validModes, ", "))
			}
		}
	}

	return nil
}

// validateOracleReaderParameter 验证Oracle Reader参数
func (v *JobConfigValidator) validateOracleReaderParameter(params map[string]any) error {
	requiredFields := []string{"host", "port", "serviceName", "username", "password", "table"}
	
	for _, field := range requiredFields {
		if _, exists := params[field]; !exists {
			return fmt.Errorf("缺少必需字段: %s", field)
		}
	}

	return nil
}

// validateOracleWriterParameter 验证Oracle Writer参数
func (v *JobConfigValidator) validateOracleWriterParameter(params map[string]any) error {
	requiredFields := []string{"host", "port", "serviceName", "username", "password", "table"}
	
	for _, field := range requiredFields {
		if _, exists := params[field]; !exists {
			return fmt.Errorf("缺少必需字段: %s", field)
		}
	}

	return nil
}

// validateSettings 验证设置配置
func (v *JobConfigValidator) validateSettings() error {
	settings := &v.config.Job.Setting

	// 验证速度设置
	if settings.Speed.Channel < 0 {
		return fmt.Errorf("通道数不能为负数")
	}

	if settings.Speed.Bytes < 0 {
		return fmt.Errorf("字节数限制不能为负数")
	}

	// 验证错误限制设置
	if settings.ErrorLimit.Record < 0 {
		return fmt.Errorf("错误记录数限制不能为负数")
	}

	if settings.ErrorLimit.Percentage < 0 || settings.ErrorLimit.Percentage > 1 {
		return fmt.Errorf("错误百分比必须在0-1之间")
	}

	return nil
}

// contains 检查切片是否包含指定元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ValidateJobConfig 验证任务配置的便捷函数
func ValidateJobConfig(config *JobConfig) error {
	validator := NewJobConfigValidator(config)
	return validator.Validate()
}
