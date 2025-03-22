package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"datax/internal/pkg/logger"
	"datax/internal/plugin/common"
)

// DataXEngine 数据同步引擎
type DataXEngine struct {
	jobConfig *JobConfig
	reader    Reader
	writer    Writer
	logger    *logger.Logger
}

// NewDataXEngine 创建新的数据同步引擎
func NewDataXEngine(config *JobConfig) *DataXEngine {
	// 创建日志记录器
	l := logger.New(&logger.Option{
		Level:     logger.LevelInfo, // 默认使用 INFO 级别
		Prefix:    "DataXEngine",
		WithTime:  true,
		WithLevel: true,
	})

	return &DataXEngine{
		jobConfig: config,
		logger:    l,
	}
}

// Start 开始数据同步
func (e *DataXEngine) Start() error {
	if len(e.jobConfig.Job.Content) == 0 {
		return fmt.Errorf("任务配置中没有content")
	}

	content := e.jobConfig.Job.Content[0]

	// 打印完整的JSON配置信息
	jobBytes, err := json.Marshal(e.jobConfig)
	if err != nil {
		e.logger.Warn("任务配置序列化失败: %v", err)
	} else {
		compactJSON := new(bytes.Buffer)
		if err := json.Compact(compactJSON, jobBytes); err != nil {
			e.logger.Warn("压缩JSON失败: %v", err)
			e.logger.Info("DataXEngine 任务配置: %s", string(jobBytes))
		} else {
			e.logger.Info("DataXEngine 任务配置: %s", compactJSON.String())
		}
	}

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

	// 检查是否需要根据数据量调整批次大小
	// 注意：上面的GetTotalCount方法可能已经调整了批次大小
	// 这里我们仅检查配置中是否没有明确设置批次大小的情况
	needsAdjustBatchSize := false

	// 检查Reader配置中是否已有批次大小
	readerNeedsBatchSize := false
	if rp, ok := content.Reader.Parameter.(map[string]any); ok {
		if _, exists := rp["batchSize"]; !exists {
			readerNeedsBatchSize = true
		}
	}

	// 检查Writer配置中是否已有批次大小
	writerNeedsBatchSize := false
	if wp, ok := content.Writer.Parameter.(map[string]any); ok {
		if _, exists := wp["batchSize"]; !exists {
			writerNeedsBatchSize = true
		}
	}

	// 如果Reader或Writer中任一方需要设置批次大小
	if readerNeedsBatchSize || writerNeedsBatchSize {
		needsAdjustBatchSize = true
	}

	// 只有在需要时才调整批次大小
	if needsAdjustBatchSize {
		batchSize := calculateBatchSize(totalCount)
		e.logger.Debug("根据数据量(%d)设置批次大小为: %d", totalCount, batchSize)

		// 更新 Reader 和 Writer 的批次大小（仅当它们没有明确设置时）
		if readerNeedsBatchSize {
			if rp, ok := content.Reader.Parameter.(map[string]any); ok {
				rp["batchSize"] = batchSize
			}
		}

		if writerNeedsBatchSize {
			if wp, ok := content.Writer.Parameter.(map[string]any); ok {
				wp["batchSize"] = batchSize
			}
		}
	}

	// 尝试传递列名信息（如果支持）
	e.tryTransferColumns()

	// 执行预处理
	e.logger.Info("============开始执行预处理操作============")
	if err := e.writer.PreProcess(); err != nil {
		return fmt.Errorf("执行预处理失败: %v", err)
	}
	e.logger.Debug("============预处理操作执行完成============")

	// 读取并写入数据
	startTime := time.Now()
	var processedCount int64
	var errorCount int64

	// 上次打印进度的时间
	lastProgressTime := time.Now()
	// 最短进度更新间隔（500毫秒）
	progressInterval := 500 * time.Millisecond

	// 打印数据同步开始信息
	e.logger.Info("开始数据同步 [总记录数: %d]...", totalCount)

	for {
		// 读取一批数据
		records, err := e.reader.Read()
		if err != nil {
			// 确保在返回错误前输出换行
			fmt.Println()
			return fmt.Errorf("读取数据失败: %v", err)
		}

		// 如果没有更多数据，退出循环
		if len(records) == 0 {
			break
		}

		// 写入数据
		if err := e.writer.Write(records); err != nil {
			// 确保在返回错误前输出换行
			fmt.Println()
			return fmt.Errorf("写入数据失败: %v", err)
		}

		processedCount += int64(len(records))

		// 检查是否需要更新进度条
		currentTime := time.Now()
		if currentTime.Sub(lastProgressTime) >= progressInterval || processedCount >= totalCount {
			// 更新上次打印时间
			lastProgressTime = currentTime

			// 计算进度
			elapsed := time.Since(startTime)
			speed := float64(processedCount) / elapsed.Seconds()
			progress := float64(processedCount) / float64(totalCount) * 100

			// 生成进度条
			progressBarWidth := 40 // 进度条宽度
			completedWidth := int(float64(progressBarWidth) * float64(processedCount) / float64(totalCount))
			progressBar := "["
			for i := 0; i < progressBarWidth; i++ {
				if i < completedWidth {
					progressBar += "="
				} else if i == completedWidth {
					progressBar += ">"
				} else {
					progressBar += " "
				}
			}
			progressBar += "]"

			// 计算预计剩余时间
			var etaStr string
			if speed > 0 && processedCount < totalCount {
				remainingCount := totalCount - processedCount
				etaSec := float64(remainingCount) / speed
				eta := time.Duration(etaSec) * time.Second
				etaStr = fmt.Sprintf("预计剩余时间: %v", eta.Round(time.Second))
			} else {
				etaStr = "即将完成"
			}

			// 使用\r使进度显示在同一行，不通过logger输出
			fmt.Printf("\r同步进度: %s %.2f%%, 已处理: %d/%d, 速度: %.2f 条/秒, %s",
				progressBar, progress, processedCount, totalCount, speed, etaStr)
		}

		// 检查是否已处理完所有数据
		if processedCount >= totalCount {
			fmt.Println() // 添加换行以结束进度行
			e.logger.Debug("已处理完所有数据，总记录数: %d", totalCount)
			break
		}

		// 检查错误限制
		if e.jobConfig.Job.Setting.ErrorLimit.Record > 0 &&
			errorCount >= int64(e.jobConfig.Job.Setting.ErrorLimit.Record) {
			fmt.Println() // 添加换行以结束进度行
			return fmt.Errorf("错误记录数超过限制: %d", errorCount)
		}
	}

	// 验证处理记录数与总记录数
	if processedCount > totalCount {
		e.logger.Warn("处理记录数(%d)大于总记录数(%d)，可能存在数据不一致", processedCount, totalCount)
	}

	// 执行后处理
	e.logger.Info("============开始执行后处理操作============")
	if err := e.writer.PostProcess(); err != nil {
		return fmt.Errorf("执行后处理失败: %v", err)
	}
	e.logger.Debug("后处理操作执行完成")

	elapsed := time.Since(startTime)
	speed := float64(processedCount) / elapsed.Seconds()

	// 显示任务完成信息（精简版）
	e.logger.Info("数据同步任务已完成 | 总耗时: %v | 记录数: %d | 速度: %.2f 条/秒",
		elapsed.Round(time.Millisecond),
		processedCount,
		speed)

	return nil
}

// tryTransferColumns 尝试从Reader向Writer传递列信息
func (e *DataXEngine) tryTransferColumns() {
	// 从Reader获取列名信息
	columnProvider, isColumnProvider := e.reader.(common.ColumnProvider)
	if !isColumnProvider {
		e.logger.Debug("Reader不支持列名提供功能")
		return
	}

	// 获取Writer的列名设置能力
	columnAwareWriter, isColumnAwareWriter := e.writer.(common.ColumnAwareWriter)
	if !isColumnAwareWriter {
		e.logger.Debug("Writer不支持列名感知功能")
		return
	}

	// 获取实际列名并传递给Writer
	actualColumns := columnProvider.GetActualColumns()
	if len(actualColumns) == 0 {
		e.logger.Debug("从Reader获取的列名为空")
		return
	}

	e.logger.Debug("从Reader获取实际列名: %v", actualColumns)
	columnAwareWriter.SetColumns(actualColumns)
	e.logger.Debug("成功将列名信息从Reader传递到Writer")
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
