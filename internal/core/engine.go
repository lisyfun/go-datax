package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// DataXEngine 数据同步引擎
type DataXEngine struct {
	jobConfig *JobConfig
	reader    Reader
	writer    Writer
}

// NewDataXEngine 创建新的数据同步引擎
func NewDataXEngine(config *JobConfig) *DataXEngine {
	return &DataXEngine{
		jobConfig: config,
	}
}

// Start 开始数据同步
func (e *DataXEngine) Start() error {
	if len(e.jobConfig.Job.Content) == 0 {
		return fmt.Errorf("任务配置中没有content")
	}

	content := e.jobConfig.Job.Content[0]

	// 构建完整的配置信息
	configInfo := struct {
		Reader struct {
			Name      string      `json:"name"`
			Parameter interface{} `json:"parameter"`
		} `json:"reader"`
		Writer struct {
			Name      string      `json:"name"`
			Parameter interface{} `json:"parameter"`
		} `json:"writer"`
		Setting interface{} `json:"setting"`
	}{
		Reader: struct {
			Name      string      `json:"name"`
			Parameter interface{} `json:"parameter"`
		}{
			Name:      content.Reader.Name,
			Parameter: content.Reader.Parameter,
		},
		Writer: struct {
			Name      string      `json:"name"`
			Parameter interface{} `json:"parameter"`
		}{
			Name:      content.Writer.Name,
			Parameter: content.Writer.Parameter,
		},
		Setting: e.jobConfig.Job.Setting,
	}

	// 打印格式化的配置信息
	var configBuf bytes.Buffer
	configEncoder := json.NewEncoder(&configBuf)
	configEncoder.SetIndent("", "  ")
	configEncoder.SetEscapeHTML(false)
	if err := configEncoder.Encode(configInfo); err != nil {
		return fmt.Errorf("格式化配置信息失败: %v", err)
	}

	log.Printf("任务配置信息:\n%s", configBuf.String())

	// 创建Reader
	factoryMutex.RLock()
	readerFactory, ok := readerFactories[content.Reader.Name]
	factoryMutex.RUnlock()
	if !ok {
		return fmt.Errorf("未找到Reader插件: %s", content.Reader.Name)
	}

	reader, err := readerFactory(content.Reader.Parameter)
	if err != nil {
		return fmt.Errorf("创建Reader失败: %v", err)
	}
	e.reader = reader
	defer e.reader.Close()

	// 创建Writer
	factoryMutex.RLock()
	writerFactory, ok := writerFactories[content.Writer.Name]
	factoryMutex.RUnlock()
	if !ok {
		return fmt.Errorf("未找到Writer插件: %s", content.Writer.Name)
	}

	writer, err := writerFactory(content.Writer.Parameter)
	if err != nil {
		return fmt.Errorf("创建Writer失败: %v", err)
	}
	e.writer = writer
	defer e.writer.Close()

	// 连接数据源
	if err := e.reader.Connect(); err != nil {
		return fmt.Errorf("连接Reader失败: %v", err)
	}

	// 连接目标
	if err := e.writer.Connect(); err != nil {
		return fmt.Errorf("连接Writer失败: %v", err)
	}

	// 获取总记录数
	totalCount, err := e.reader.GetTotalCount()
	if err != nil {
		return fmt.Errorf("获取总记录数失败: %v", err)
	}
	log.Printf("总记录数: %d", totalCount)

	// 根据数据量动态调整批次大小
	batchSize := calculateBatchSize(totalCount)

	// 更新 Reader 和 Writer 的批次大小
	if rp, ok := content.Reader.Parameter.(map[string]interface{}); ok {
		rp["batchSize"] = batchSize
	}
	if wp, ok := content.Writer.Parameter.(map[string]interface{}); ok {
		wp["batchSize"] = batchSize
	}

	log.Printf("根据数据量(%d)自动调整批次大小为: %d", totalCount, batchSize)

	// 执行预处理
	log.Printf("开始执行预处理操作...")
	if err := e.writer.PreProcess(); err != nil {
		return fmt.Errorf("执行预处理失败: %v", err)
	}
	log.Printf("预处理操作执行完成")

	// 读取并写入数据
	startTime := time.Now()
	var processedCount int64
	var errorCount int64

	for {
		// 读取一批数据
		records, err := e.reader.Read()
		if err != nil {
			return fmt.Errorf("读取数据失败: %v", err)
		}

		// 如果没有更多数据，退出循环
		if len(records) == 0 {
			break
		}

		// 写入数据
		if err := e.writer.Write(records); err != nil {
			return fmt.Errorf("写入数据失败: %v", err)
		}

		processedCount += int64(len(records))

		// 打印进度
		elapsed := time.Since(startTime)
		speed := float64(processedCount) / elapsed.Seconds()
		progress := float64(processedCount) / float64(totalCount) * 100
		log.Printf("进度: %.2f%%, 已处理: %d/%d, 速度: %.2f 条/秒",
			progress, processedCount, totalCount, speed)

		// 检查是否已处理完所有数据
		if processedCount >= totalCount {
			log.Printf("已处理完所有数据，总记录数: %d", totalCount)
			break
		}

		// 检查错误限制
		if e.jobConfig.Job.Setting.ErrorLimit.Record > 0 &&
			errorCount >= int64(e.jobConfig.Job.Setting.ErrorLimit.Record) {
			return fmt.Errorf("错误记录数超过限制: %d", errorCount)
		}
	}

	// 验证处理记录数与总记录数
	if processedCount > totalCount {
		log.Printf("警告: 处理记录数(%d)大于总记录数(%d)，可能存在数据不一致", processedCount, totalCount)
	}

	// 执行后处理
	log.Printf("开始执行后处理操作...")
	if err := e.writer.PostProcess(); err != nil {
		return fmt.Errorf("执行后处理失败: %v", err)
	}
	log.Printf("后处理操作执行完成")

	elapsed := time.Since(startTime)
	speed := float64(processedCount) / elapsed.Seconds()
	log.Printf("数据同步完成! 总耗时: %v, 处理记录数: %d, 错误记录数: %d, 平均速度: %.2f 条/秒",
		elapsed, processedCount, errorCount, speed)

	return nil
}

// calculateBatchSize 根据数据总量计算合适的批次大小
func calculateBatchSize(totalCount int64) int {
	switch {
	case totalCount <= 10000:
		return 1000 // 小数据量使用较小批次
	case totalCount <= 100000:
		return 5000 // 中等数据量
	case totalCount <= 1000000:
		return 10000 // 较大数据量
	case totalCount <= 10000000:
		return 20000 // 大数据量
	default:
		return 50000 // 超大数据量
	}
}
