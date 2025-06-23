package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"datax/internal/core"
	"datax/internal/pkg/logger"
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

// registerPlugin 注册插件
func registerPlugin(pluginType, name string) error {
	// 验证插件是否支持
	if err := core.ValidateBuiltinPlugin(pluginType, name); err != nil {
		return err
	}

	// 注册插件
	return core.DefaultPluginManager.RegisterPlugin(pluginType, name, nil)
}

func main() {
	if err := run(); err != nil {
		log.Error("程序执行失败: %v", err)
		os.Exit(1)
	}
}

// run 主要的程序逻辑，返回错误而不是直接退出
func run() error {
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
		return nil
	}

	if jobFile == "" {
		return fmt.Errorf("请指定任务配置文件路径")
	}

	// 输出版本信息
	log.Info("DataX 版本: %s, 构建时间: %s, 提交ID: %s", Version, BuildTime, CommitID)

	// 读取任务配置文件
	content, err := os.ReadFile(jobFile)
	if err != nil {
		return fmt.Errorf("读取任务配置文件失败: %v", err)
	}

	// 解析任务配置
	var jobConfig core.JobConfig
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&jobConfig); err != nil {
		return fmt.Errorf("解析任务配置失败: %v", err)
	}

	// 验证配置
	if err := core.ValidateJobConfig(&jobConfig); err != nil {
		return fmt.Errorf("配置验证失败: %v", err)
	}

	// 获取第一个任务内容
	content0 := jobConfig.Job.Content[0]

	// 动态注册Reader插件
	if err := registerPlugin("reader", content0.Reader.Name); err != nil {
		return fmt.Errorf("注册Reader插件失败: %v", err)
	}

	// 动态注册Writer插件
	if err := registerPlugin("writer", content0.Writer.Name); err != nil {
		return fmt.Errorf("注册Writer插件失败: %v", err)
	}

	// 创建引擎
	engine := core.NewDataXEngine(&jobConfig)

	// 开始数据同步
	log.Info("开始数据同步任务...")
	if err := engine.Start(); err != nil {
		return fmt.Errorf("数据同步失败: %v", err)
	}
	log.Info("数据同步完成!")
	return nil
}
