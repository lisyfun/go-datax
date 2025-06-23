package core

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"datax/internal/pkg/logger"
)

// 高性能模拟Reader，用于性能测试
type performanceReader struct {
	batchSize    int
	totalRecords int64
	readCount    int64
	recordSize   int // 每条记录的字段数
}

func (r *performanceReader) Connect() error {
	return nil
}

func (r *performanceReader) Read() ([][]any, error) {
	currentCount := atomic.LoadInt64(&r.readCount)
	if currentCount >= r.totalRecords {
		return [][]any{}, nil // 没有更多数据
	}

	// 计算本批次应该读取的记录数
	remaining := r.totalRecords - currentCount
	batchSize := int64(r.batchSize)
	if remaining < batchSize {
		batchSize = remaining
	}

	// 生成模拟数据
	records := make([][]any, batchSize)
	for i := int64(0); i < batchSize; i++ {
		record := make([]any, r.recordSize)
		for j := 0; j < r.recordSize; j++ {
			record[j] = fmt.Sprintf("data_%d_%d", currentCount+i, j)
		}
		records[i] = record
	}

	atomic.AddInt64(&r.readCount, batchSize)
	return records, nil
}

func (r *performanceReader) Close() error {
	return nil
}

func (r *performanceReader) GetTotalCount() (int64, error) {
	return r.totalRecords, nil
}

// 高性能模拟Writer，用于性能测试
type performanceWriter struct {
	writeCount   int64
	totalLatency int64 // 总延迟（纳秒）
	writeDelay   time.Duration
}

func (w *performanceWriter) Connect() error {
	return nil
}

func (w *performanceWriter) Write(records [][]any) error {
	start := time.Now()
	
	// 模拟写入延迟
	if w.writeDelay > 0 {
		time.Sleep(w.writeDelay)
	}
	
	// 更新统计
	atomic.AddInt64(&w.writeCount, int64(len(records)))
	atomic.AddInt64(&w.totalLatency, time.Since(start).Nanoseconds())
	
	return nil
}

func (w *performanceWriter) Close() error {
	return nil
}

func (w *performanceWriter) PreProcess() error {
	return nil
}

func (w *performanceWriter) PostProcess() error {
	return nil
}

// BenchmarkPipeline_SmallDataset 小数据集性能测试
func BenchmarkPipeline_SmallDataset(b *testing.B) {
	benchmarkPipeline(b, 10000, 10, 0) // 1万条记录，10个字段，无延迟
}

// BenchmarkPipeline_MediumDataset 中等数据集性能测试
func BenchmarkPipeline_MediumDataset(b *testing.B) {
	benchmarkPipeline(b, 100000, 10, 0) // 10万条记录，10个字段，无延迟
}

// BenchmarkPipeline_LargeDataset 大数据集性能测试
func BenchmarkPipeline_LargeDataset(b *testing.B) {
	benchmarkPipeline(b, 1000000, 10, 0) // 100万条记录，10个字段，无延迟
}

// BenchmarkPipeline_WithLatency 带延迟的性能测试
func BenchmarkPipeline_WithLatency(b *testing.B) {
	benchmarkPipeline(b, 100000, 10, 1*time.Microsecond) // 10万条记录，1微秒延迟
}

// benchmarkPipeline 通用的性能测试函数
func benchmarkPipeline(b *testing.B, totalRecords int64, recordSize int, writeDelay time.Duration) {
	logger := logger.New(nil)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		reader := &performanceReader{
			batchSize:    50000, // 使用优化后的大批次
			totalRecords: totalRecords,
			recordSize:   recordSize,
		}
		
		writer := &performanceWriter{
			writeDelay: writeDelay,
		}
		
		config := &PipelineConfig{
			BufferSize:       100, // 使用优化后的大缓冲区
			MaxRetries:       0,   // 性能测试中不重试
			ProgressInterval: 10 * time.Second, // 减少进度输出
			WriterWorkers:    4,   // 使用多线程Writer
		}
		
		pipeline := NewPipeline(reader, writer, logger, config)
		
		start := time.Now()
		err := pipeline.Start(totalRecords)
		elapsed := time.Since(start)
		
		if err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}
		
		stats := pipeline.GetStats()
		if stats.ProcessedRecords != totalRecords {
			b.Fatalf("Expected %d processed records, got %d", totalRecords, stats.ProcessedRecords)
		}
		
		// 计算性能指标
		recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
		avgWriteLatency := time.Duration(atomic.LoadInt64(&writer.totalLatency)) / time.Duration(atomic.LoadInt64(&writer.writeCount))
		
		b.ReportMetric(recordsPerSecond, "records/sec")
		b.ReportMetric(float64(avgWriteLatency.Nanoseconds()), "avg_write_latency_ns")
		b.ReportMetric(float64(elapsed.Nanoseconds()), "total_time_ns")
	}
}

// BenchmarkPipeline_SingleVsMultiWriter 单线程vs多线程Writer性能对比
func BenchmarkPipeline_SingleVsMultiWriter(b *testing.B) {
	totalRecords := int64(500000) // 50万条记录
	recordSize := 10
	logger := logger.New(nil)
	
	b.Run("SingleWriter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := &performanceReader{
				batchSize:    50000,
				totalRecords: totalRecords,
				recordSize:   recordSize,
			}
			
			writer := &performanceWriter{}
			
			config := &PipelineConfig{
				BufferSize:       100,
				MaxRetries:       0,
				ProgressInterval: 10 * time.Second,
				WriterWorkers:    1, // 单线程
			}
			
			pipeline := NewPipeline(reader, writer, logger, config)
			
			start := time.Now()
			err := pipeline.Start(totalRecords)
			elapsed := time.Since(start)
			
			if err != nil {
				b.Fatalf("Pipeline failed: %v", err)
			}
			
			recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
			b.ReportMetric(recordsPerSecond, "records/sec")
		}
	})
	
	b.Run("MultiWriter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := &performanceReader{
				batchSize:    50000,
				totalRecords: totalRecords,
				recordSize:   recordSize,
			}
			
			writer := &performanceWriter{}
			
			config := &PipelineConfig{
				BufferSize:       100,
				MaxRetries:       0,
				ProgressInterval: 10 * time.Second,
				WriterWorkers:    4, // 多线程
			}
			
			pipeline := NewPipeline(reader, writer, logger, config)
			
			start := time.Now()
			err := pipeline.Start(totalRecords)
			elapsed := time.Since(start)
			
			if err != nil {
				b.Fatalf("Pipeline failed: %v", err)
			}
			
			recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
			b.ReportMetric(recordsPerSecond, "records/sec")
		}
	})
}

// BenchmarkPipeline_BatchSizeComparison 批次大小性能对比
func BenchmarkPipeline_BatchSizeComparison(b *testing.B) {
	totalRecords := int64(200000) // 20万条记录
	recordSize := 10
	logger := logger.New(nil)
	
	batchSizes := []int{1000, 10000, 50000, 100000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := &performanceReader{
					batchSize:    batchSize,
					totalRecords: totalRecords,
					recordSize:   recordSize,
				}
				
				writer := &performanceWriter{}
				
				config := &PipelineConfig{
					BufferSize:       100,
					MaxRetries:       0,
					ProgressInterval: 10 * time.Second,
					WriterWorkers:    4,
				}
				
				pipeline := NewPipeline(reader, writer, logger, config)
				
				start := time.Now()
				err := pipeline.Start(totalRecords)
				elapsed := time.Since(start)
				
				if err != nil {
					b.Fatalf("Pipeline failed: %v", err)
				}
				
				recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
				b.ReportMetric(recordsPerSecond, "records/sec")
			}
		})
	}
}

// TestPerformanceTarget 验证性能目标测试
func TestPerformanceTarget(t *testing.T) {
	// 目标：处理500万条记录应该在5分钟内完成（即 16,667 条/秒）
	// 优化后目标：50,000+ 条/秒
	
	totalRecords := int64(100000) // 使用10万条记录进行快速测试
	recordSize := 10
	logger := logger.New(nil)
	
	reader := &performanceReader{
		batchSize:    50000,
		totalRecords: totalRecords,
		recordSize:   recordSize,
	}
	
	writer := &performanceWriter{}
	
	config := &PipelineConfig{
		BufferSize:       100,
		MaxRetries:       0,
		ProgressInterval: 10 * time.Second,
		WriterWorkers:    4,
	}
	
	pipeline := NewPipeline(reader, writer, logger, config)
	
	start := time.Now()
	err := pipeline.Start(totalRecords)
	elapsed := time.Since(start)
	
	if err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}
	
	stats := pipeline.GetStats()
	if stats.ProcessedRecords != totalRecords {
		t.Fatalf("Expected %d processed records, got %d", totalRecords, stats.ProcessedRecords)
	}
	
	recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
	
	t.Logf("性能测试结果:")
	t.Logf("  总记录数: %d", totalRecords)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  处理速度: %.2f 条/秒", recordsPerSecond)
	t.Logf("  读取批次: %d", stats.ReadBatches)
	t.Logf("  写入批次: %d", stats.WriteBatches)
	
	// 验证性能目标：应该达到至少 30,000 条/秒
	minTargetSpeed := 30000.0
	if recordsPerSecond < minTargetSpeed {
		t.Errorf("性能未达到目标：期望至少 %.0f 条/秒，实际 %.2f 条/秒", minTargetSpeed, recordsPerSecond)
	} else {
		t.Logf("✅ 性能达到目标：%.2f 条/秒 > %.0f 条/秒", recordsPerSecond, minTargetSpeed)
	}
	
	// 估算500万条记录的处理时间
	estimatedTimeFor5M := time.Duration(float64(5000000) / recordsPerSecond * float64(time.Second))
	t.Logf("估算500万条记录处理时间: %v", estimatedTimeFor5M)
	
	if estimatedTimeFor5M > 3*time.Minute {
		t.Logf("⚠️  500万条记录预计耗时超过3分钟，建议进一步优化")
	} else {
		t.Logf("✅ 500万条记录预计耗时在3分钟内，性能良好")
	}
}
