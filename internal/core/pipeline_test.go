package core

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"datax/internal/pkg/logger"
)

// 模拟的慢速Reader
type slowReader struct {
	mockReader
	delay    time.Duration
	count    int
	maxCount int
}

func (r *slowReader) Read() ([][]any, error) {
	if r.count >= r.maxCount {
		return [][]any{}, nil // 没有更多数据
	}

	time.Sleep(r.delay)
	r.count++
	return [][]any{{"data", r.count}}, nil
}

func (r *slowReader) GetTotalCount() (int64, error) {
	return int64(r.maxCount), nil
}

// 模拟的慢速Writer
type slowWriter struct {
	mockWriter
	delay      time.Duration
	writeCount int64
}

func (w *slowWriter) Write(records [][]any) error {
	time.Sleep(w.delay)
	atomic.AddInt64(&w.writeCount, int64(len(records)))
	return nil
}

// 模拟的可重试Writer
type retryableWriter struct {
	mockWriter
	failCount *int
	maxFails  int
}

func (w *retryableWriter) Write(records [][]any) error {
	*w.failCount++
	if *w.failCount <= w.maxFails {
		return errors.New("temporary error")
	}
	return nil
}

func TestNewPipeline(t *testing.T) {
	reader := &mockReader{}
	writer := &mockWriter{}
	logger := logger.New(nil)

	pipeline := NewPipeline(reader, writer, logger, nil)

	if pipeline == nil {
		t.Error("NewPipeline should return a non-nil pipeline")
	}

	if pipeline.reader != reader {
		t.Error("Pipeline should store the provided reader")
	}

	// 注意：现在使用工厂函数，无法直接比较writer实例

	if pipeline.config == nil {
		t.Error("Pipeline should have default config when nil is provided")
	}

	// 测试默认配置
	if pipeline.config.BufferSize != 100 {
		t.Errorf("Expected default buffer size 100, got %d", pipeline.config.BufferSize)
	}

	if pipeline.config.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", pipeline.config.MaxRetries)
	}
}

func TestPipeline_Start_Success(t *testing.T) {
	reader := &slowReader{
		delay:    10 * time.Millisecond,
		maxCount: 5,
	}
	writer := &slowWriter{
		delay: 5 * time.Millisecond,
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		BufferSize:       2,
		MaxRetries:       1,
		RetryInterval:    100 * time.Millisecond,
		ProgressInterval: 1 * time.Second, // 增加进度间隔避免测试中过多输出
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	start := time.Now()
	err := pipeline.Start(5)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Pipeline.Start should succeed, got error: %v", err)
	}

	stats := pipeline.GetStats()
	if stats.ProcessedRecords != 5 {
		t.Errorf("Expected 5 processed records, got %d", stats.ProcessedRecords)
	}

	if stats.ReadBatches != 5 {
		t.Errorf("Expected 5 read batches, got %d", stats.ReadBatches)
	}

	if stats.WriteBatches != 5 {
		t.Errorf("Expected 5 write batches, got %d", stats.WriteBatches)
	}

	if writer.writeCount != 5 {
		t.Errorf("Expected writer to receive 5 records, got %d", writer.writeCount)
	}

	// 验证并发性能提升（应该比串行快）
	// 串行时间约为: 5 * (10ms + 5ms) = 75ms
	// 并发时间应该接近: max(5 * 10ms, 5 * 5ms) = 50ms
	if elapsed > 70*time.Millisecond {
		t.Errorf("Pipeline should be faster due to concurrency, took %v", elapsed)
	}
}

func TestPipeline_Start_ReaderError(t *testing.T) {
	reader := &mockReader{
		readErr: errors.New("reader error"),
	}
	writer := &mockWriter{}
	logger := logger.New(nil)

	pipeline := NewPipeline(reader, writer, logger, nil)

	err := pipeline.Start(10)

	if err == nil {
		t.Error("Pipeline.Start should return error when reader fails")
	}

	if !strings.Contains(err.Error(), "读取数据失败") {
		t.Errorf("Expected reader error, got: %v", err)
	}
}

func TestPipeline_Start_WriterError(t *testing.T) {
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 1,
	}
	writer := &mockWriter{
		writeErr: errors.New("writer error"),
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       1, // 只重试1次
		RetryInterval:    10 * time.Millisecond,
		ProgressInterval: 100 * time.Millisecond, // 添加进度间隔
		ErrorLimit:       0,                      // 不允许错误
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(1)

	if err == nil {
		t.Error("Pipeline.Start should return error when writer fails and error limit is 0")
		return
	}

	if !strings.Contains(err.Error(), "错误记录数超过限制") {
		t.Errorf("Expected error limit exceeded, got: %v", err)
	}
}

func TestPipeline_Start_ErrorLimit(t *testing.T) {
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 10,
	}
	writer := &mockWriter{
		writeErr: errors.New("writer error"),
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0,                      // 不重试
		ProgressInterval: 100 * time.Millisecond, // 添加进度间隔
		ErrorLimit:       5,                      // 允许5个错误
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(10)

	if err == nil {
		t.Error("Pipeline.Start should return error when error limit is exceeded")
	}

	stats := pipeline.GetStats()
	if stats.ErrorRecords < 5 {
		t.Errorf("Expected at least 5 error records, got %d", stats.ErrorRecords)
	}
}

func TestPipeline_Retry_Success(t *testing.T) {
	// 创建一个会失败几次然后成功的Writer
	failCount := 0
	writer := &retryableWriter{
		failCount: &failCount,
		maxFails:  2,
	}

	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 1,
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       3,
		RetryInterval:    10 * time.Millisecond,
		ProgressInterval: 100 * time.Millisecond, // 添加进度间隔
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(1)

	if err != nil {
		t.Errorf("Pipeline.Start should succeed after retries, got error: %v", err)
	}

	stats := pipeline.GetStats()
	if stats.ProcessedRecords != 1 {
		t.Errorf("Expected 1 processed record after retry, got %d", stats.ProcessedRecords)
	}
}

func TestPipeline_Stop(t *testing.T) {
	reader := &slowReader{
		delay:    100 * time.Millisecond,
		maxCount: 100,
	}
	writer := &slowWriter{
		delay: 50 * time.Millisecond,
	}
	logger := logger.New(nil)

	pipeline := NewPipeline(reader, writer, logger, nil)

	// 在另一个goroutine中启动pipeline
	errChan := make(chan error, 1)
	go func() {
		errChan <- pipeline.Start(100)
	}()

	// 等待一小段时间然后停止
	time.Sleep(50 * time.Millisecond)
	pipeline.Stop()

	// 等待pipeline结束
	select {
	case err := <-errChan:
		// 应该因为context取消而返回错误
		if err == nil {
			t.Error("Pipeline should return error when stopped")
		}
	case <-time.After(1 * time.Second):
		t.Error("Pipeline should stop within reasonable time")
	}
}

func TestPipelineConfig_Defaults(t *testing.T) {
	reader := &mockReader{}
	writer := &mockWriter{}
	logger := logger.New(nil)

	pipeline := NewPipeline(reader, writer, logger, nil)

	config := pipeline.config

	if config.BufferSize != 100 {
		t.Errorf("Expected default buffer size 100, got %d", config.BufferSize)
	}

	if config.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", config.MaxRetries)
	}

	if config.RetryInterval != time.Second {
		t.Errorf("Expected default retry interval 1s, got %v", config.RetryInterval)
	}

	if config.ProgressInterval != 5*time.Second {
		t.Errorf("Expected default progress interval 5s, got %v", config.ProgressInterval)
	}

	if config.ErrorLimit != -1 {
		t.Errorf("Expected default error limit -1, got %d", config.ErrorLimit)
	}

	if config.ErrorPercentage != 0 {
		t.Errorf("Expected default error percentage 0, got %f", config.ErrorPercentage)
	}

	if config.WriterWorkers != 4 {
		t.Errorf("Expected default writer workers 4, got %d", config.WriterWorkers)
	}
}

func TestPipelineStats(t *testing.T) {
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 3,
	}
	writer := &mockWriter{}
	logger := logger.New(nil)

	pipeline := NewPipeline(reader, writer, logger, nil)

	err := pipeline.Start(3)
	if err != nil {
		t.Errorf("Pipeline.Start should succeed, got error: %v", err)
	}

	stats := pipeline.GetStats()

	if stats.TotalRecords != 3 {
		t.Errorf("Expected total records 3, got %d", stats.TotalRecords)
	}

	if stats.ProcessedRecords != 3 {
		t.Errorf("Expected processed records 3, got %d", stats.ProcessedRecords)
	}

	if stats.ErrorRecords != 0 {
		t.Errorf("Expected error records 0, got %d", stats.ErrorRecords)
	}

	if stats.ReadBatches != 3 {
		t.Errorf("Expected read batches 3, got %d", stats.ReadBatches)
	}

	if stats.WriteBatches != 3 {
		t.Errorf("Expected write batches 3, got %d", stats.WriteBatches)
	}

	if stats.StartTime.IsZero() {
		t.Error("Start time should be set")
	}
}
