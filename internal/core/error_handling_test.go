package core

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"datax/internal/pkg/logger"
)

// 模拟的错误Reader，会产生指定数量的错误
type errorReader struct {
	mockReader
	errorAfter int // 在第几次读取后开始产生错误
	readCount  int
	maxReads   int
}

func (r *errorReader) Read() ([][]any, error) {
	r.readCount++

	if r.readCount > r.maxReads {
		return [][]any{}, nil // 没有更多数据
	}

	if r.readCount > r.errorAfter {
		return nil, errors.New("reader error")
	}

	return [][]any{{"data", r.readCount}}, nil
}

func (r *errorReader) GetTotalCount() (int64, error) {
	return int64(r.maxReads), nil
}

// 模拟的错误Writer，会产生指定比例的错误
type errorWriter struct {
	mockWriter
	errorRate    float64 // 错误率 (0.0 - 1.0)
	writeCount   int64
	successCount int64
	errorCount   int64
}

func (w *errorWriter) Write(records [][]any) error {
	count := atomic.AddInt64(&w.writeCount, 1)

	// 根据错误率决定是否产生错误
	if float64(count)*w.errorRate >= float64(atomic.LoadInt64(&w.errorCount)+1) {
		atomic.AddInt64(&w.errorCount, 1)
		return errors.New("writer error")
	}

	atomic.AddInt64(&w.successCount, int64(len(records)))
	return nil
}

func TestErrorLimit_RecordCount(t *testing.T) {
	// 创建一个总是失败的Writer
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 10,
	}
	writer := &errorWriter{
		errorRate: 1.0, // 100% 错误率
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0, // 不重试
		ProgressInterval: 100 * time.Millisecond,
		ErrorLimit:       3, // 允许3个错误
		ErrorPercentage:  0, // 不使用百分比限制
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(10)

	if err == nil {
		t.Error("Pipeline should return error when error limit is exceeded")
	}

	if !strings.Contains(err.Error(), "错误记录数超过限制") {
		t.Errorf("Expected error limit exceeded message, got: %v", err)
	}

	stats := pipeline.GetStats()
	if stats.ErrorRecords < 3 {
		t.Errorf("Expected at least 3 error records, got %d", stats.ErrorRecords)
	}
}

func TestErrorLimit_Percentage(t *testing.T) {
	// 创建一个50%错误率的Writer
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 10,
	}
	writer := &errorWriter{
		errorRate: 0.6, // 60% 错误率
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0, // 不重试
		ProgressInterval: 100 * time.Millisecond,
		ErrorLimit:       0,   // 不使用记录数限制
		ErrorPercentage:  0.5, // 50% 错误率限制
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(10)

	if err == nil {
		t.Error("Pipeline should return error when error percentage is exceeded")
	}

	if !strings.Contains(err.Error(), "错误记录数超过限制") {
		t.Errorf("Expected error limit exceeded message, got: %v", err)
	}

	stats := pipeline.GetStats()
	totalProcessed := stats.ProcessedRecords + stats.ErrorRecords
	if totalProcessed > 0 {
		actualErrorRate := float64(stats.ErrorRecords) / float64(totalProcessed)
		if actualErrorRate < 0.5 {
			t.Errorf("Error rate should be at least 50%%, got %.2f%%", actualErrorRate*100)
		}
	}
}

func TestErrorLimit_NoLimit(t *testing.T) {
	// 测试没有错误限制的情况
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 5,
	}
	writer := &errorWriter{
		errorRate: 0.4, // 40% 错误率
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0, // 不重试
		ProgressInterval: 100 * time.Millisecond,
		ErrorLimit:       0, // 不限制错误数量
		ErrorPercentage:  0, // 不限制错误百分比
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(5)

	if err != nil {
		t.Errorf("Pipeline should succeed when no error limits are set, got error: %v", err)
	}

	stats := pipeline.GetStats()
	totalProcessed := stats.ProcessedRecords + stats.ErrorRecords
	if totalProcessed != 5 {
		t.Errorf("Expected 5 total records processed, got %d", totalProcessed)
	}
}

func TestErrorLimit_BothLimits(t *testing.T) {
	// 测试同时设置记录数和百分比限制
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 20,
	}
	writer := &errorWriter{
		errorRate: 0.3, // 30% 错误率
	}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0, // 不重试
		ProgressInterval: 100 * time.Millisecond,
		ErrorLimit:       5,    // 最多5个错误
		ErrorPercentage:  0.25, // 25% 错误率限制
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(20)

	if err == nil {
		t.Error("Pipeline should return error when either error limit is exceeded")
	}

	stats := pipeline.GetStats()

	// 应该在达到任一限制时停止
	if stats.ErrorRecords > 5 {
		t.Errorf("Error records should not exceed 5, got %d", stats.ErrorRecords)
	}

	totalProcessed := stats.ProcessedRecords + stats.ErrorRecords
	if totalProcessed > 0 {
		actualErrorRate := float64(stats.ErrorRecords) / float64(totalProcessed)
		// 错误率可能超过25%，因为记录数限制可能先触发
		t.Logf("Actual error rate: %.2f%%, Error records: %d, Total processed: %d",
			actualErrorRate*100, stats.ErrorRecords, totalProcessed)
	}
}

// 可控制的Writer，用于测试错误统计
type controllableWriter struct {
	mockWriter
	writeCount int64
}

func (w *controllableWriter) Write(records [][]any) error {
	w.writeCount++
	if w.writeCount%3 == 0 { // 每3次写入失败一次
		return errors.New("predictable error")
	}
	return nil
}

func TestErrorStatistics_Accuracy(t *testing.T) {
	// 测试错误统计的准确性
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 10,
	}

	writer := &controllableWriter{}

	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       0, // 不重试
		ProgressInterval: 100 * time.Millisecond,
		ErrorLimit:       0, // 不限制错误
		ErrorPercentage:  0, // 不限制错误百分比
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(10)

	if err != nil {
		t.Errorf("Pipeline should succeed, got error: %v", err)
	}

	stats := pipeline.GetStats()

	// 验证统计数据
	expectedErrorCount := int64(3)   // 第3、6、9次写入失败
	expectedSuccessCount := int64(7) // 其余7次成功

	if stats.ErrorRecords != expectedErrorCount {
		t.Errorf("Expected %d error records, got %d", expectedErrorCount, stats.ErrorRecords)
	}

	if stats.ProcessedRecords != expectedSuccessCount {
		t.Errorf("Expected %d processed records, got %d", expectedSuccessCount, stats.ProcessedRecords)
	}

	totalRecords := stats.ProcessedRecords + stats.ErrorRecords
	if totalRecords != 10 {
		t.Errorf("Expected total 10 records, got %d", totalRecords)
	}
}

func TestReaderError_Handling(t *testing.T) {
	// 测试Reader错误的处理
	reader := &errorReader{
		errorAfter: 3, // 前3次成功，之后失败
		maxReads:   10,
	}
	writer := &mockWriter{}
	logger := logger.New(nil)

	config := &PipelineConfig{
		MaxRetries:       1, // 重试1次
		RetryInterval:    10 * time.Millisecond,
		ProgressInterval: 100 * time.Millisecond,
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	err := pipeline.Start(10)

	if err == nil {
		t.Error("Pipeline should return error when reader fails")
	}

	if !strings.Contains(err.Error(), "读取数据失败") {
		t.Errorf("Expected reader error message, got: %v", err)
	}

	stats := pipeline.GetStats()

	// 应该处理了前3条记录
	if stats.ProcessedRecords != 3 {
		t.Errorf("Expected 3 processed records before reader error, got %d", stats.ProcessedRecords)
	}
}
