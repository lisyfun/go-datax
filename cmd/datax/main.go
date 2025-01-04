package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"datax/internal/core"
	mysqlReader "datax/internal/plugin/reader/mysql"
	oracleReader "datax/internal/plugin/reader/oracle"
	pgReader "datax/internal/plugin/reader/postgresql"
	mysqlWriter "datax/internal/plugin/writer/mysql"
	oracleWriter "datax/internal/plugin/writer/oracle"
	pgWriter "datax/internal/plugin/writer/postgresql"
)

// registerReaderPlugin 动态注册Reader插件
func registerReaderPlugin(name string) error {
	switch name {
	case "mysqlreader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p mysqlReader.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
				return nil, err
			}
			return mysqlReader.NewMySQLReader(&p), nil
		})
	case "postgresqlreader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p pgReader.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
				return nil, err
			}
			return pgReader.NewPostgreSQLReader(&p), nil
		})
	case "oraclereader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p oracleReader.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
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
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p mysqlWriter.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
				return nil, err
			}
			return mysqlWriter.NewMySQLWriter(&p), nil
		})
	case "postgresqlwriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p pgWriter.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
				return nil, err
			}
			return pgWriter.NewPostgreSQLWriter(&p), nil
		})
	case "oraclewriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}
			var p oracleWriter.Parameter
			if err := json.Unmarshal(paramBytes, &p); err != nil {
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
	flag.StringVar(&jobFile, "job", "", "任务配置文件路径")
	flag.Parse()

	if jobFile == "" {
		log.Fatal("请指定任务配置文件路径")
	}

	// 读取任务配置文件
	content, err := os.ReadFile(jobFile)
	if err != nil {
		log.Fatalf("读取任务配置文件失败: %v", err)
	}

	// 解析任务配置
	var jobConfig core.JobConfig
	if err := json.Unmarshal(content, &jobConfig); err != nil {
		log.Fatalf("解析任务配置失败: %v", err)
	}

	// 验证配置
	if len(jobConfig.Job.Content) == 0 {
		log.Fatal("任务配置中没有content")
	}

	// 获取第一个任务内容
	content0 := jobConfig.Job.Content[0]

	// 动态注册Reader插件
	if err := registerReaderPlugin(content0.Reader.Name); err != nil {
		log.Fatalf("注册Reader插件失败: %v", err)
	}

	// 动态注册Writer插件
	if err := registerWriterPlugin(content0.Writer.Name); err != nil {
		log.Fatalf("注册Writer插件失败: %v", err)
	}

	// 创建引擎
	engine := core.NewDataXEngine(&jobConfig)

	// 开始数据同步
	log.Println("开始数据同步任务...")
	if err := engine.Start(); err != nil {
		log.Fatalf("数据同步失败: %v", err)
	}
	log.Println("数据同步完成!")
}
