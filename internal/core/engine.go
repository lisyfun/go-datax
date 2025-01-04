package core

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// JobConfig 作业配置结构
type JobConfig struct {
	Job struct {
		Content []struct {
			Reader struct {
				Name      string          `json:"name"`
				Parameter json.RawMessage `json:"parameter"`
			} `json:"reader"`
			Writer struct {
				Name      string          `json:"name"`
				Parameter json.RawMessage `json:"parameter"`
			} `json:"writer"`
		} `json:"content"`
		Setting struct {
			Speed struct {
				Channel int `json:"channel"`
				Bytes   int `json:"bytes"`
			} `json:"speed"`
			ErrorLimit struct {
				Record     int     `json:"record"`
				Percentage float64 `json:"percentage"`
			} `json:"errorLimit"`
		} `json:"setting"`
	} `json:"job"`
}

// DataXEngine 数据同步引擎
type DataXEngine struct {
	jobConfig *JobConfig
	reader    Reader
	writer    Writer
}

// NewDataXEngine 创建新的数据同步引擎实例
func NewDataXEngine(config *JobConfig) *DataXEngine {
	return &DataXEngine{
		jobConfig: config,
	}
}

// Init 初始化引擎
func (e *DataXEngine) Init() error {
	// 目前只处理第一个content
	content := e.jobConfig.Job.Content[0]

	// 初始化Reader
	reader, err := CreateReader(content.Reader.Name, content.Reader.Parameter)
	if err != nil {
		return fmt.Errorf("创建Reader失败: %v", err)
	}
	e.reader = reader

	// 初始化Writer
	writer, err := CreateWriter(content.Writer.Name, content.Writer.Parameter)
	if err != nil {
		return fmt.Errorf("创建Writer失败: %v", err)
	}
	e.writer = writer

	return nil
}

// Start 开始数据同步
func (e *DataXEngine) Start() error {
	startTime := time.Now()
	log.Printf("开始数据同步任务...")

	// 连接数据源和目标
	if err := e.reader.Connect(); err != nil {
		return fmt.Errorf("Reader连接失败: %v", err)
	}
	defer e.reader.Close()

	if err := e.writer.Connect(); err != nil {
		return fmt.Errorf("Writer连接失败: %v", err)
	}
	defer e.writer.Close()

	// 获取总记录数
	totalCount, err := e.reader.GetTotalCount()
	if err != nil {
		return fmt.Errorf("获取总记录数失败: %v", err)
	}
	log.Printf("总记录数: %d", totalCount)

	// 执行写入前处理
	if err := e.writer.PreProcess(); err != nil {
		return fmt.Errorf("写入前处理失败: %v", err)
	}

	// 开始事务
	if err := e.writer.StartTransaction(); err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}

	var processedCount int64
	var errorCount int64

	// 读取并写入数据
	for {
		records, err := e.reader.Read()
		if err != nil {
			e.writer.RollbackTransaction()
			return fmt.Errorf("读取数据失败: %v", err)
		}

		// 如果没有更多数据，退出循环
		if len(records) == 0 {
			break
		}

		// 写入数据
		if err := e.writer.Write(records); err != nil {
			e.writer.RollbackTransaction()
			return fmt.Errorf("写入数据失败: %v", err)
		}

		processedCount += int64(len(records))
		log.Printf("已处理记录数: %d/%d (%.2f%%)",
			processedCount,
			totalCount,
			float64(processedCount)/float64(totalCount)*100,
		)

		// 检查错误限制
		if e.jobConfig.Job.Setting.ErrorLimit.Record > 0 &&
			errorCount >= int64(e.jobConfig.Job.Setting.ErrorLimit.Record) {
			e.writer.RollbackTransaction()
			return fmt.Errorf("错误记录数超过限制: %d", errorCount)
		}
	}

	// 提交事务
	if err := e.writer.CommitTransaction(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	// 执行写入后处理
	if err := e.writer.PostProcess(); err != nil {
		return fmt.Errorf("写入后处理失败: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("数据同步完成! 总耗时: %v, 处理记录数: %d, 错误记录数: %d",
		duration,
		processedCount,
		errorCount,
	)

	return nil
}
