package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"datax/internal/core"
	"datax/internal/pkg/logger"
	mysqlReader "datax/internal/plugin/reader/mysql"
	oracleReader "datax/internal/plugin/reader/oracle"
	pgReader "datax/internal/plugin/reader/postgresql"
	mysqlWriter "datax/internal/plugin/writer/mysql"
	oracleWriter "datax/internal/plugin/writer/oracle"
	pgWriter "datax/internal/plugin/writer/postgresql"
)

// 版本信息，在编译时通过 -ldflags 注入
var (
	Version   = "dev"
	BuildTime = "unknown"
	CommitID  = "unknown"
)

var log *logger.Logger

func init() {
	// 初始化日志记录器
	log = logger.New(&logger.Option{
		Level:     logger.LevelInfo,
		Prefix:    "DataX",
		WithTime:  true,
		WithLevel: true,
	})
}

// registerReaderPlugin 动态注册Reader插件
func registerReaderPlugin(name string) error {
	switch name {
	case "mysqlreader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p mysqlReader.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return mysqlReader.NewMySQLReader(&p), nil
		})
	case "postgresqlreader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p pgReader.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return pgReader.NewPostgreSQLReader(&p), nil
		})
	case "oraclereader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p oracleReader.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return oracleReader.NewOracleReader(&p), nil
		})
	default:
		return fmt.Errorf("未知的Reader类型: %s", name)
	}
	return nil
}

// registerWriterPlugin 动态注册Writer插件
func registerWriterPlugin(name string) error {
	switch name {
	case "mysqlwriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p mysqlWriter.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return mysqlWriter.NewMySQLWriter(&p), nil
		})
	case "postgresqlwriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p pgWriter.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return pgWriter.NewPostgreSQLWriter(&p), nil
		})
	case "oraclewriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(parameter); err != nil {
				return nil, err
			}
			paramBytes := buf.Bytes()
			var p oracleWriter.Parameter
			decoder := json.NewDecoder(bytes.NewReader(paramBytes))
			decoder.UseNumber()
			if err := decoder.Decode(&p); err != nil {
				return nil, err
			}
			return oracleWriter.NewOracleWriter(&p), nil
		})
	default:
		return fmt.Errorf("未知的Writer类型: %s", name)
	}
	return nil
}

func main() {
	// 解析命令行参数
	var jobFile string
	var showVersion bool
	flag.StringVar(&jobFile, "job", "", "任务配置文件路径")
	flag.BoolVar(&showVersion, "version", false, "显示版本信息")
	flag.Parse()

	// 显示版本信息
	if showVersion {
		log.Info("DataX 版本: %s", Version)
		log.Info("构建时间: %s", BuildTime)
		log.Info("提交ID: %s", CommitID)
		return
	}

	if jobFile == "" {
		log.Error("请指定任务配置文件路径")
		os.Exit(1)
	}

	// 输出版本信息
	log.Info("DataX 版本: %s, 构建时间: %s, 提交ID: %s", Version, BuildTime, CommitID)

	// 读取任务配置文件
	content, err := os.ReadFile(jobFile)
	if err != nil {
		log.Error("读取任务配置文件失败: %v", err)
		os.Exit(1)
	}

	// 解析任务配置
	var jobConfig core.JobConfig
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&jobConfig); err != nil {
		log.Error("解析任务配置失败: %v", err)
		os.Exit(1)
	}

	// 验证配置
	if len(jobConfig.Job.Content) == 0 {
		log.Error("任务配置中没有content")
		os.Exit(1)
	}

	// 获取第一个任务内容
	content0 := jobConfig.Job.Content[0]

	// 动态注册Reader插件
	if err := registerReaderPlugin(content0.Reader.Name); err != nil {
		log.Error("注册Reader插件失败: %v", err)
		os.Exit(1)
	}

	// 动态注册Writer插件
	if err := registerWriterPlugin(content0.Writer.Name); err != nil {
		log.Error("注册Writer插件失败: %v", err)
		os.Exit(1)
	}

	// 创建引擎
	engine := core.NewDataXEngine(&jobConfig)

	// 开始数据同步
	log.Info("开始数据同步任务...")
	if err := engine.Start(); err != nil {
		log.Error("数据同步失败: %v", err)
		os.Exit(1)
	}
	log.Info("数据同步完成!")
}
