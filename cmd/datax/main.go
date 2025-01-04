package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"datax/internal/core"
	mysqlreader "datax/internal/plugin/reader/mysql"
	mysqlwriter "datax/internal/plugin/writer/mysql"
)

// registerReaderPlugin 动态注册Reader插件
func registerReaderPlugin(name string) {
	switch name {
	case "mysqlreader":
		core.RegisterReader(name, func(parameter interface{}) (core.Reader, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}

			var param mysqlreader.Parameter
			if err := json.Unmarshal(paramBytes, &param); err != nil {
				return nil, err
			}

			return mysqlreader.NewMySQLReader(&param), nil
		})
	default:
		log.Printf("未知的Reader类型: %s", name)
	}
}

// registerWriterPlugin 动态注册Writer插件
func registerWriterPlugin(name string) {
	switch name {
	case "mysqlwriter":
		core.RegisterWriter(name, func(parameter interface{}) (core.Writer, error) {
			paramBytes, err := json.Marshal(parameter)
			if err != nil {
				return nil, err
			}

			var param mysqlwriter.Parameter
			if err := json.Unmarshal(paramBytes, &param); err != nil {
				return nil, err
			}

			return mysqlwriter.NewMySQLWriter(&param), nil
		})
	default:
		log.Printf("未知的Writer类型: %s", name)
	}
}

func main() {
	// 定义命令行参数
	var jobPath string
	flag.StringVar(&jobPath, "job", "", "任务配置文件路径(json)")
	flag.Parse()

	// 检查参数
	if jobPath == "" {
		log.Fatal("请指定任务配置文件路径，使用 -job 参数")
	}

	// 读取任务配置文件
	jobData, err := ioutil.ReadFile(jobPath)
	if err != nil {
		log.Fatalf("读取任务配置文件失败: %v", err)
	}

	var jobConfig core.JobConfig
	if err := json.Unmarshal(jobData, &jobConfig); err != nil {
		log.Fatalf("解析任务配置文件失败: %v", err)
	}

	// 验证配置
	if err := validateJobConfig(&jobConfig); err != nil {
		log.Fatalf("配置验证失败: %v", err)
	}

	// 动态注册插件
	content := jobConfig.Job.Content[0]
	registerReaderPlugin(content.Reader.Name)
	registerWriterPlugin(content.Writer.Name)

	// 创建并启动引擎
	engine := core.NewDataXEngine(&jobConfig)
	if err := engine.Init(); err != nil {
		log.Fatalf("初始化引擎失败: %v", err)
	}

	if err := engine.Start(); err != nil {
		log.Fatalf("数据同步失败: %v", err)
	}
}

// validateJobConfig 验证任务配置
func validateJobConfig(config *core.JobConfig) error {
	if len(config.Job.Content) == 0 {
		return fmt.Errorf("任务配置中没有content")
	}

	content := config.Job.Content[0]
	if content.Reader.Name == "" {
		return fmt.Errorf("reader name 不能为空")
	}
	if content.Writer.Name == "" {
		return fmt.Errorf("writer name 不能为空")
	}

	return nil
}
