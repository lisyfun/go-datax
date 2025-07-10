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
	Records    [][]any
	BatchID    int64
	Error      error
	RetryCount int       // 重试次数
	Timestamp  time.Time // 创建时间
}

// FailedBatchQueue 失败批次重试队列
type FailedBatchQueue struct {
	queue   chan *DataBatch
	mu      sync.RWMutex
	batches map[int64]*DataBatch // 用于去重和状态跟踪
}

// NewFailedBatchQueue 创建失败批次队列
func NewFailedBatchQueue(size int) *FailedBatchQueue {
	return &FailedBatchQueue{
		queue:   make(chan *DataBatch, size),
		batches: make(map[int64]*DataBatch),
	}
}

// Add 添加失败的批次到重试队列
func (q *FailedBatchQueue) Add(batch *DataBatch) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// 检查是否已存在
	if _, exists := q.batches[batch.BatchID]; exists {
		return false
	}

	// 增加重试次数
	batch.RetryCount++
	q.batches[batch.BatchID] = batch

	select {
	case q.queue <- batch:
		return true
	default:
		// 队列满了，删除记录
		delete(q.batches, batch.BatchID)
		return false
	}
}

// Get 获取下一个需要重试的批次
func (q *FailedBatchQueue) Get() *DataBatch {
	select {
	case batch := <-q.queue:
		q.mu.Lock()
		delete(q.batches, batch.BatchID)
		q.mu.Unlock()
		return batch
	default:
		return nil
	}
}

// Size 获取队列大小
func (q *FailedBatchQueue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.batches)
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
	dataChan   chan *DataBatch
	errorChan  chan error
	doneChan   chan struct{}
	retryQueue *FailedBatchQueue // 失败重试队列

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
	RetryQueueSize   int           // 重试队列大小
	MaxBatchRetries  int           // 单个批次最大重试次数
	EnableDataCheck  bool          // 是否启用数据完整性检查
}

// PipelineStats 管道统计信息
type PipelineStats struct {
	TotalRecords       int64
	ProcessedRecords   int64
	ErrorRecords       int64
	ReadBatches        int64
	WriteBatches       int64
	RetriedBatches     int64 // 重试的批次数
	FinalFailedBatches int64 // 最终失败的批次数

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
			RetryQueueSize:   1000,            // 默认重试队列大小
			MaxBatchRetries:  5,               // 默认单个批次最大重试次数
			EnableDataCheck:  true,            // 默认启用数据完整性检查
		}
	}

	// 设置默认值
	if config.RetryQueueSize <= 0 {
		config.RetryQueueSize = 1000
	}
	if config.MaxBatchRetries <= 0 {
		config.MaxBatchRetries = 5
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
		retryQueue:    NewFailedBatchQueue(config.RetryQueueSize),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start 启动数据传输管道
func (p *Pipeline) Start(totalRecords int64) error {
	p.stats.TotalRecords = totalRecords
	p.stats.StartTime = time.Now()
	p.stats.LastProgressTime = time.Now()

	p.logger.Info("启动数据传输管道: 缓冲区 %d, Worker %d, 重试 %d, 队列 %d, 批次重试 %d",
		p.config.BufferSize, p.config.WriterWorkers, p.config.MaxRetries, p.config.RetryQueueSize, p.config.MaxBatchRetries)

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

	// 启动重试处理goroutine
	p.wg.Add(1)
	go p.retryWorker()

	// 等待完成或错误
	select {
	case err := <-p.errorChan:
		p.cancel()
		p.wg.Wait()
		return err
	case <-p.doneChan:
		p.wg.Wait()
		// 处理剩余的重试队列
		return p.processRemainingRetries()
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
			Records:    records,
			BatchID:    batchID,
			RetryCount: 0,
			Timestamp:  time.Now(),
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
					// 重试成功，更新统计信息
					atomic.AddInt64(&p.stats.ProcessedRecords, int64(len(batch.Records)))
					atomic.AddInt64(&p.stats.WriteBatches, 1)
					p.logger.Debug("Writer-%d: 完成批次 %d（重试成功），记录数: %d", workerID, batch.BatchID, len(batch.Records))
					continue
				}

				// 立即重试失败，检查是否可以加入重试队列
				if batch.RetryCount < p.config.MaxBatchRetries {
					if p.retryQueue.Add(batch) {
						atomic.AddInt64(&p.stats.RetriedBatches, 1)
						p.logger.Info("批次重试: Worker %d, 批次 %d, 重试次数 %d/%d, 状态 已加入重试队列",
							workerID, batch.BatchID, batch.RetryCount, p.config.MaxBatchRetries)
						continue
					} else {
						p.logger.Warn("批次重试失败: Worker %d, 批次 %d, 原因 重试队列已满", workerID, batch.BatchID)
					}
				}

				// 最终失败
				atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))
				atomic.AddInt64(&p.stats.FinalFailedBatches, 1)

				// 检查错误限制
				if p.checkErrorLimit() {
					select {
					case p.errorChan <- fmt.Errorf("错误记录数超过限制"):
					case <-p.ctx.Done():
					}
					return
				}

				p.logger.Error("批次最终失败: Worker %d, 批次 %d, 记录数 %d, 错误 %v",
					workerID, batch.BatchID, len(batch.Records), err)
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

	// 计算预计剩余时间
	var etaStr string
	if speed > 0 && processed < total {
		remainingCount := total - processed
		etaSec := float64(remainingCount) / speed
		eta := time.Duration(etaSec) * time.Second
		etaStr = eta.Round(time.Second).String()
	} else {
		etaStr = "即将完成"
	}

	// 单行输出进度信息
	p.logger.Info("数据同步进度: 已处理 %d/%d (%.2f%%), 速度 %.2f 条/秒, 已用时间 %v, 预计剩余 %s",
		processed, total, progressPercent, speed, elapsed.Round(time.Second), etaStr)
}

// retryWorker 重试工作协程
func (p *Pipeline) retryWorker() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 处理重试队列中的批次
			for {
				batch := p.retryQueue.Get()
				if batch == nil {
					break // 队列为空
				}

				// 创建新的Writer实例进行重试
				writer, err := p.writerFactory()
				if err != nil {
					p.logger.Error("重试时创建Writer失败: %v", err)
					// 重新加入队列
					if batch.RetryCount < p.config.MaxBatchRetries {
						p.retryQueue.Add(batch)
					}
					continue
				}

				// 连接Writer
				if err := writer.Connect(); err != nil {
					p.logger.Error("重试时连接Writer失败: %v", err)
					writer.Close()
					// 重新加入队列
					if batch.RetryCount < p.config.MaxBatchRetries {
						p.retryQueue.Add(batch)
					}
					continue
				}

				// 尝试写入
				err = writer.Write(batch.Records)
				writer.Close()

				if err == nil {
					// 重试成功
					atomic.AddInt64(&p.stats.ProcessedRecords, int64(len(batch.Records)))
					atomic.AddInt64(&p.stats.WriteBatches, 1)
					p.logger.Info("重试成功: 批次 %d, 记录数 %d, 重试次数 %d", batch.BatchID, len(batch.Records), batch.RetryCount)
				} else {
					// 重试失败，检查是否还能继续重试
					if batch.RetryCount < p.config.MaxBatchRetries {
						p.retryQueue.Add(batch)
						p.logger.Warn("重试失败，继续重试: 批次 %d, 重试次数 %d/%d, 错误 %v",
							batch.BatchID, batch.RetryCount, p.config.MaxBatchRetries, err)
					} else {
						// 最终失败
						atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))
						atomic.AddInt64(&p.stats.FinalFailedBatches, 1)
						p.logger.Error("重试次数已达上限，最终失败: 批次 %d, 记录数 %d, 重试次数 %d/%d, 错误 %v",
							batch.BatchID, len(batch.Records), batch.RetryCount, p.config.MaxBatchRetries, err)
					}
				}
			}

		case <-p.doneChan:
			// 主要数据传输完成，但还需要处理剩余的重试
			p.logger.Info("主要数据传输完成，开始处理剩余重试队列")
			return

		case <-p.ctx.Done():
			return
		}
	}
}

// processRemainingRetries 处理剩余的重试队列
func (p *Pipeline) processRemainingRetries() error {
	retryCount := p.retryQueue.Size()
	if retryCount == 0 {
		p.logger.Info("重试队列为空，无需处理")
		return nil
	}

	p.logger.Info("开始处理剩余重试批次: %d 个", retryCount)

	// 设置超时时间，避免无限等待
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			remaining := p.retryQueue.Size()
			if remaining > 0 {
				p.logger.Warn("重试超时，仍有 %d 个批次未处理完成", remaining)
				return fmt.Errorf("重试超时，仍有 %d 个批次未处理完成", remaining)
			}
			return nil

		case <-ticker.C:
			batch := p.retryQueue.Get()
			if batch == nil {
				// 队列为空，检查是否真的完成了
				if p.retryQueue.Size() == 0 {
					p.logger.Info("所有重试批次处理完成")
					return nil
				}
				continue
			}

			// 创建Writer并重试
			writer, err := p.writerFactory()
			if err != nil {
				p.logger.Error("最终重试时创建Writer失败: %v", err)
				atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))
				atomic.AddInt64(&p.stats.FinalFailedBatches, 1)
				continue
			}

			if err := writer.Connect(); err != nil {
				p.logger.Error("最终重试时连接Writer失败: %v", err)
				writer.Close()
				atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))
				atomic.AddInt64(&p.stats.FinalFailedBatches, 1)
				continue
			}

			err = writer.Write(batch.Records)
			writer.Close()

			if err == nil {
				atomic.AddInt64(&p.stats.ProcessedRecords, int64(len(batch.Records)))
				atomic.AddInt64(&p.stats.WriteBatches, 1)
				p.logger.Info("最终重试成功: 批次 %d, 记录数 %d", batch.BatchID, len(batch.Records))
			} else {
				atomic.AddInt64(&p.stats.ErrorRecords, int64(len(batch.Records)))
				atomic.AddInt64(&p.stats.FinalFailedBatches, 1)
				p.logger.Error("最终重试失败: 批次 %d, 记录数 %d, 错误 %v", batch.BatchID, len(batch.Records), err)
			}
		}
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
