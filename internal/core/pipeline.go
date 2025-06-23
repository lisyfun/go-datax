package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"datax/internal/pkg/logger"
)

// DataBatch 数据批次结构
type DataBatch struct {
	Records [][]any
	BatchID int64
	Error   error
}

// Pipeline 数据传输管道
type Pipeline struct {
	reader        Reader
	writerFactory func() (Writer, error) // Writer工厂函数
	logger        *logger.Logger
	config        *PipelineConfig

	// 统计信息
	stats *PipelineStats

	// 控制通道
	dataChan  chan *DataBatch
	errorChan chan error
	doneChan  chan struct{}

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 同步控制
	wg         sync.WaitGroup
	doneOnce   sync.Once // 确保doneChan只被关闭一次
	writerDone int32     // 原子计数器，记录完成的Writer数量
}

// PipelineConfig 管道配置
type PipelineConfig struct {
	BufferSize       int           // 缓冲区大小
	MaxRetries       int           // 最大重试次数
	RetryInterval    time.Duration // 重试间隔
	ProgressInterval time.Duration // 进度更新间隔
	ErrorLimit       int64         // 错误限制
	ErrorPercentage  float64       // 错误百分比限制
	WriterWorkers    int           // Writer工作协程数量
}

// PipelineStats 管道统计信息
type PipelineStats struct {
	TotalRecords     int64
	ProcessedRecords int64
	ErrorRecords     int64
	ReadBatches      int64
	WriteBatches     int64

	StartTime        time.Time
	LastProgressTime time.Time

	ReadDuration  time.Duration
	WriteDuration time.Duration

	// 原子操作字段
	currentReadTime  int64 // 纳秒
	currentWriteTime int64 // 纳秒
}

// NewPipeline 创建新的数据传输管道
func NewPipeline(reader Reader, writer Writer, logger *logger.Logger, config *PipelineConfig) *Pipeline {
	return NewPipelineWithFactory(reader, func() (Writer, error) { return writer, nil }, logger, config)
}

// NewPipelineWithFactory 使用Writer工厂函数创建新的数据传输管道
func NewPipelineWithFactory(reader Reader, writerFactory func() (Writer, error), logger *logger.Logger, config *PipelineConfig) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	if config == nil {
		config = &PipelineConfig{
			BufferSize:       100, // 增大默认缓冲区大小
			MaxRetries:       3,
			RetryInterval:    time.Second,
			ProgressInterval: 5 * time.Second, // 进一步减少进度更新频率
			ErrorLimit:       -1,              // -1 表示不限制错误数
			ErrorPercentage:  0,               // 0 表示不限制错误百分比
			WriterWorkers:    4,               // 默认4个Writer工作协程
		}
	}

	return &Pipeline{
		reader:        reader,
		writerFactory: writerFactory,
		logger:        logger,
		config:        config,
		stats:         &PipelineStats{},
		dataChan:      make(chan *DataBatch, config.BufferSize),
		errorChan:     make(chan error, 2),
		doneChan:      make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start 启动数据传输管道
func (p *Pipeline) Start(totalRecords int64) error {
	p.stats.TotalRecords = totalRecords
	p.stats.StartTime = time.Now()
	p.stats.LastProgressTime = time.Now()

	p.logger.Info("启动并发数据传输管道 [缓冲区大小: %d]", p.config.BufferSize)

	// 启动Reader goroutine
	p.wg.Add(1)
	go p.readerWorker()

	// 启动多个Writer goroutine
	writerCount := p.config.WriterWorkers
	if writerCount <= 0 {
		writerCount = 1 // 至少启动一个Writer
	}
	for i := 0; i < writerCount; i++ {
		p.wg.Add(1)
		go p.writerWorker(i + 1) // 传递Worker ID
	}

	// 启动进度监控goroutine
	p.wg.Add(1)
	go p.progressWorker()

	// 等待完成或错误
	select {
	case err := <-p.errorChan:
		p.cancel()
		p.wg.Wait()
		return err
	case <-p.doneChan:
		p.wg.Wait()
		return nil
	case <-p.ctx.Done():
		p.wg.Wait()
		return p.ctx.Err()
	}
}

// Stop 停止数据传输管道
func (p *Pipeline) Stop() {
	p.cancel()
}

// GetStats 获取统计信息
func (p *Pipeline) GetStats() *PipelineStats {
	return p.stats
}

// readerWorker Reader工作协程
func (p *Pipeline) readerWorker() {
	defer p.wg.Done()
	defer close(p.dataChan)

	var batchID int64

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		// 记录读取开始时间
		readStart := time.Now()

		// 读取数据
		records, err := p.reader.Read()

		// 记录读取耗时
		readDuration := time.Since(readStart)
		atomic.AddInt64(&p.stats.currentReadTime, readDuration.Nanoseconds())

		if err != nil {
			// 尝试重试
			if p.retryRead(&records, &err) {
				continue
			}

			select {
			case p.errorChan <- fmt.Errorf("读取数据失败: %v", err):
			case <-p.ctx.Done():
			}
			return
		}

		// 如果没有更多数据，结束读取
		if len(records) == 0 {
			p.logger.Debug("Reader: 没有更多数据，结束读取")
			return
		}

		batchID++
		batch := &DataBatch{
			Records: records,
			BatchID: batchID,
		}

		// 发送数据到管道
		select {
		case p.dataChan <- batch:
			atomic.AddInt64(&p.stats.ReadBatches, 1)
			p.logger.Debug("Reader: 发送批次 %d，记录数: %d", batchID, len(records))
		case <-p.ctx.Done():
			return
		}
	}
}

// writerWorker Writer工作协程
func (p *Pipeline) writerWorker(workerID int) {
	defer p.wg.Done()
	defer func() {
		// 当Writer协程结束时，检查是否所有Writer都已完成
		doneCount := atomic.AddInt32(&p.writerDone, 1)
		expectedWorkers := int32(p.config.WriterWorkers)
		if expectedWorkers <= 0 {
			expectedWorkers = 1
		}

		if doneCount == expectedWorkers {
			// 所有Writer都已完成，关闭doneChan
			p.doneOnce.Do(func() {
				p.logger.Debug("所有Writer协程已完成，关闭doneChan")
				// 最后更新一次进度
				p.updateProgress()
				close(p.doneChan)
			})
		}
	}()

	// 为每个Worker创建独立的Writer实例
	writer, err := p.writerFactory()
	if err != nil {
		p.logger.Error("Writer-%d: 创建Writer实例失败: %v", workerID, err)
		select {
		case p.errorChan <- fmt.Errorf("创建Writer实例失败: %v", err):
		case <-p.ctx.Done():
		}
		return
	}
	defer writer.Close()

	// 连接Writer
	if err := writer.Connect(); err != nil {
		p.logger.Error("Writer-%d: 连接Writer失败: %v", workerID, err)
		select {
		case p.errorChan <- fmt.Errorf("连接Writer失败: %v", err):
		case <-p.ctx.Done():
		}
		return
	}

	p.logger.Debug("Writer-%d: 已启动并连接", workerID)

	for {
		select {
		case batch, ok := <-p.dataChan:
			if !ok {
				// 数据通道已关闭，所有数据处理完成
				p.logger.Debug("Writer-%d: 数据通道已关闭，结束写入", workerID)
				return
			}

			// 记录写入开始时间
			writeStart := time.Now()

			// 写入数据
			err := writer.Write(batch.Records)

			// 记录写入耗时
			writeDuration := time.Since(writeStart)
			atomic.AddInt64(&p.stats.currentWriteTime, writeDuration.Nanoseconds())

			if err != nil {
				// 尝试重试
				if p.retryWriteWithWriter(writer, batch, &err) {
					continue
				}

				atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))

				// 检查错误限制
				if p.checkErrorLimit() {
					select {
					case p.errorChan <- fmt.Errorf("错误记录数超过限制"):
					case <-p.ctx.Done():
					}
					return
				}

				p.logger.Warn("Writer-%d: 批次 %d 写入失败: %v", workerID, batch.BatchID, err)
				continue
			}

			// 更新统计信息
			atomic.AddInt64(&p.stats.ProcessedRecords, int64(len(batch.Records)))
			atomic.AddInt64(&p.stats.WriteBatches, 1)

			p.logger.Debug("Writer-%d: 完成批次 %d，记录数: %d", workerID, batch.BatchID, len(batch.Records))

		case <-p.ctx.Done():
			return
		}
	}
}

// progressWorker 进度监控协程
func (p *Pipeline) progressWorker() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.ProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.updateProgress()
		case <-p.doneChan:
			// 数据传输完成，停止进度监控
			return
		case <-p.ctx.Done():
			return
		}
	}
}

// updateProgress 更新进度显示
func (p *Pipeline) updateProgress() {
	processed := atomic.LoadInt64(&p.stats.ProcessedRecords)
	total := p.stats.TotalRecords

	if total == 0 {
		return
	}

	// 避免频繁的时间计算，只在进度有显著变化时更新
	progressPercent := float64(processed) / float64(total) * 100

	// 只在进度变化超过1%或者是完成状态时才更新显示
	lastProgress := p.stats.LastProgressTime
	now := time.Now()

	// 减少进度更新频率：只在进度变化超过1%或间隔超过5秒时更新
	if progressPercent < 100 && now.Sub(lastProgress) < 5*time.Second {
		// 检查进度是否有显著变化（超过1%）
		lastProcessed := int64(float64(total) * float64(p.stats.LastProgressTime.Unix()-p.stats.StartTime.Unix()) / float64(now.Unix()-p.stats.StartTime.Unix()) * 100)
		if processed-lastProcessed < total/100 { // 变化小于1%
			return
		}
	}

	p.stats.LastProgressTime = now
	elapsed := now.Sub(p.stats.StartTime)
	speed := float64(processed) / elapsed.Seconds()

	// 生成简化的进度条（减少字符串操作）
	progressBarWidth := 40
	completedWidth := int(float64(progressBarWidth) * progressPercent / 100)

	// 使用更高效的字符串构建
	progressBar := make([]byte, progressBarWidth+2)
	progressBar[0] = '['
	for i := 1; i <= progressBarWidth; i++ {
		if i-1 < completedWidth {
			progressBar[i] = '='
		} else {
			progressBar[i] = ' '
		}
	}
	progressBar[progressBarWidth+1] = ']'

	// 计算预计剩余时间（只在需要时计算）
	var etaStr string
	if speed > 0 && processed < total {
		remainingCount := total - processed
		etaSec := float64(remainingCount) / speed
		eta := time.Duration(etaSec) * time.Second
		etaStr = fmt.Sprintf("预计剩余: %v", eta.Round(time.Second))
	} else {
		etaStr = "即将完成"
	}

	// 输出进度（使用\r在同一行更新）
	fmt.Printf("\r同步进度: %s %.2f%%, 已处理: %d/%d, 速度: %.2f 条/秒, %s",
		string(progressBar), progressPercent, processed, total, speed, etaStr)

	// 如果完成，添加换行并停止进度更新
	if processed >= total {
		fmt.Println()
		return
	}
}

// retryRead 重试读取操作
func (p *Pipeline) retryRead(records *[][]any, err *error) bool {
	for i := 0; i < p.config.MaxRetries; i++ {
		p.logger.Warn("Reader重试 %d/%d: %v", i+1, p.config.MaxRetries, *err)

		select {
		case <-time.After(p.config.RetryInterval):
		case <-p.ctx.Done():
			return false
		}

		*records, *err = p.reader.Read()
		if *err == nil {
			p.logger.Info("Reader重试成功")
			return true
		}
	}
	return false
}

// retryWrite 重试写入操作（兼容性方法，已废弃）
func (p *Pipeline) retryWrite(batch *DataBatch, err *error) bool {
	// 这个方法现在不应该被调用，因为我们使用独立的Writer实例
	p.logger.Error("retryWrite方法被调用，但应该使用retryWriteWithWriter")
	return false
}

// retryWriteWithWriter 使用指定Writer重试写入操作
func (p *Pipeline) retryWriteWithWriter(writer Writer, batch *DataBatch, err *error) bool {
	for i := 0; i < p.config.MaxRetries; i++ {
		p.logger.Warn("Writer重试 %d/%d (批次 %d): %v", i+1, p.config.MaxRetries, batch.BatchID, *err)

		select {
		case <-time.After(p.config.RetryInterval):
		case <-p.ctx.Done():
			return false
		}

		*err = writer.Write(batch.Records)
		if *err == nil {
			p.logger.Info("Writer重试成功 (批次 %d)", batch.BatchID)
			return true
		}
	}
	return false
}

// checkErrorLimit 检查错误限制
func (p *Pipeline) checkErrorLimit() bool {
	errorCount := atomic.LoadInt64(&p.stats.ErrorRecords)
	processedCount := atomic.LoadInt64(&p.stats.ProcessedRecords)

	// 检查错误记录数限制
	// ErrorLimit = 0 表示不允许任何错误
	// ErrorLimit > 0 表示允许的最大错误数
	// ErrorLimit < 0 表示不限制错误数
	if p.config.ErrorLimit >= 0 && errorCount > p.config.ErrorLimit {
		return true
	}

	// 检查错误百分比限制
	if p.config.ErrorPercentage > 0 && processedCount > 0 {
		errorPercentage := float64(errorCount) / float64(processedCount)
		if errorPercentage > p.config.ErrorPercentage {
			return true
		}
	}

	return false
}
