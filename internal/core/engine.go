package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
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
	reader, err := DefaultRegistry.CreateReader(content.Reader.Name, content.Reader.Parameter)
	if err != nil {
		return fmt.Errorf("创建Reader失败: %v", err)
	}
	e.reader = reader
	defer e.reader.Close()

	// 创建Writer
	writer, err := DefaultRegistry.CreateWriter(content.Writer.Name, content.Writer.Parameter)
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

	// 创建并发管道配置
	pipelineConfig := &PipelineConfig{
		BufferSize:       100, // 增大缓冲区大小提升性能
		MaxRetries:       3,   // 最大重试次数
		RetryInterval:    time.Second,
		ProgressInterval: 5 * time.Second, // 进一步减少进度更新频率
		ErrorLimit:       int64(e.jobConfig.Job.Setting.ErrorLimit.Record),
		ErrorPercentage:  e.jobConfig.Job.Setting.ErrorLimit.Percentage,
		WriterWorkers:    4, // 启动4个Writer工作协程
	}

	// 创建Writer工厂函数，为每个Worker创建独立的Writer实例
	writerFactory := func() (Writer, error) {
		writer, err := DefaultRegistry.CreateWriter(content.Writer.Name, content.Writer.Parameter)
		if err != nil {
			return nil, fmt.Errorf("创建Writer失败: %v", err)
		}

		// 如果Writer支持列名感知，传递列名信息
		if columnAwareWriter, ok := writer.(common.ColumnAwareWriter); ok {
			if columnProvider, isColumnProvider := e.reader.(common.ColumnProvider); isColumnProvider {
				actualColumns := columnProvider.GetActualColumns()
				if len(actualColumns) > 0 {
					columnAwareWriter.SetColumns(actualColumns)
				}
			}
		}

		return writer, nil
	}

	// 创建并发数据传输管道
	pipeline := NewPipelineWithFactory(e.reader, writerFactory, e.logger, pipelineConfig)

	// 启动并发数据传输
	startTime := time.Now()
	if err := pipeline.Start(totalCount); err != nil {
		return fmt.Errorf("数据同步失败: %v", err)
	}

	// 获取统计信息
	stats := pipeline.GetStats()
	elapsed := time.Since(startTime)

	// 计算详细性能指标
	readDuration := time.Duration(atomic.LoadInt64(&stats.currentReadTime))
	writeDuration := time.Duration(atomic.LoadInt64(&stats.currentWriteTime))

	// 验证处理记录数与总记录数
	processedCount := stats.ProcessedRecords
	if processedCount > totalCount {
		e.logger.Warn("处理记录数(%d)大于总记录数(%d)，可能存在数据不一致", processedCount, totalCount)
	}

	// 执行后处理
	e.logger.Info("============开始执行后处理操作============")
	if err := e.writer.PostProcess(); err != nil {
		return fmt.Errorf("执行后处理失败: %v", err)
	}
	e.logger.Debug("后处理操作执行完成")

	avgSpeed := float64(processedCount) / elapsed.Seconds()

	// 显示详细的任务完成信息
	e.logger.Info("数据同步完成!")
	e.logger.Info("总记录数: %d, 成功: %d, 错误: %d",
		stats.ProcessedRecords+stats.ErrorRecords, stats.ProcessedRecords, stats.ErrorRecords)
	e.logger.Info("总耗时: %v, 平均速度: %.2f 条/秒",
		elapsed.Round(time.Millisecond), avgSpeed)
	e.logger.Info("读取批次: %d, 写入批次: %d",
		stats.ReadBatches, stats.WriteBatches)
	e.logger.Info("读取耗时: %v, 写入耗时: %v",
		readDuration.Round(time.Millisecond), writeDuration.Round(time.Millisecond))

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

// calculateBatchSize 根据数据总量和系统性能计算合适的批次大小
func calculateBatchSize(totalCount int64) int {
	// 基础批次大小计算
	baseBatchSize := calculateBaseBatchSize(totalCount)

	// 根据系统性能调整批次大小
	adjustedBatchSize := adjustBatchSizeForPerformance(baseBatchSize, totalCount)

	// 确保批次大小在合理范围内
	return clampBatchSize(adjustedBatchSize)
}

// calculateBaseBatchSize 根据数据总量计算基础批次大小
func calculateBaseBatchSize(totalCount int64) int {
	switch {
	case totalCount <= 1000:
		return 100 // 极小数据量
	case totalCount <= 10000:
		return 1000 // 小数据量
	case totalCount <= 100000:
		return 10000 // 中等数据量
	case totalCount <= 1000000:
		return 25000 // 较大数据量
	case totalCount <= 10000000:
		return 50000 // 大数据量
	default:
		return 100000 // 超大数据量
	}
}

// adjustBatchSizeForPerformance 根据系统性能调整批次大小
func adjustBatchSizeForPerformance(baseBatchSize int, totalCount int64) int {
	// 根据CPU核心数调整
	cpuCount := runtime.NumCPU()
	cpuFactor := float64(cpuCount) / 4.0 // 以4核为基准
	if cpuFactor < 0.5 {
		cpuFactor = 0.5 // 最小不低于50%
	} else if cpuFactor > 2.0 {
		cpuFactor = 2.0 // 最大不超过200%
	}

	// 根据可用内存调整（简化版本）
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// 可用内存（MB）
	availableMemMB := memStats.Sys / 1024 / 1024
	memoryFactor := 1.0

	if availableMemMB < 512 { // 小于512MB
		memoryFactor = 0.5
	} else if availableMemMB < 1024 { // 小于1GB
		memoryFactor = 0.7
	} else if availableMemMB < 2048 { // 小于2GB
		memoryFactor = 0.9
	} else if availableMemMB >= 4096 { // 大于等于4GB
		memoryFactor = 1.5
	}

	// 根据数据量大小调整策略
	sizeFactor := 1.0
	if totalCount > 5000000 { // 超过500万条记录
		sizeFactor = 1.3 // 增大批次以提高效率
	} else if totalCount < 10000 { // 小于1万条记录
		sizeFactor = 0.8 // 减小批次以降低延迟
	}

	// 综合调整
	adjustedSize := float64(baseBatchSize) * cpuFactor * memoryFactor * sizeFactor

	return int(adjustedSize)
}

// clampBatchSize 确保批次大小在合理范围内
func clampBatchSize(batchSize int) int {
	const (
		minBatchSize = 100    // 最小批次大小
		maxBatchSize = 200000 // 最大批次大小
	)

	if batchSize < minBatchSize {
		return minBatchSize
	}
	if batchSize > maxBatchSize {
		return maxBatchSize
	}
	return batchSize
}
