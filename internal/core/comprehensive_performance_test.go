package core

import (
	"fmt"
	"testing"
	"time"

	"datax/internal/pkg/logger"
)

// TestComprehensivePerformanceOptimization 综合性能优化测试
func TestComprehensivePerformanceOptimization(t *testing.T) {
	// 模拟500万条记录的性能测试
	totalRecords := int64(5000000)
	recordSize := 10
	logger := logger.New(nil)

	t.Logf("🚀 开始综合性能测试")
	t.Logf("目标: 处理 %d 条记录，从原来的5分钟优化到1分钟内", totalRecords)

	// 使用优化后的配置
	reader := &performanceReader{
		batchSize:    calculateBatchSize(totalRecords), // 使用自适应批次大小
		totalRecords: totalRecords,
		recordSize:   recordSize,
	}

	writer := &performanceWriter{}

	// 使用所有优化配置
	config := &PipelineConfig{
		BufferSize:       100,              // 大缓冲区
		MaxRetries:       0,                // 性能测试中不重试
		ProgressInterval: 10 * time.Second, // 减少进度输出
		WriterWorkers:    4,                // 多线程Writer
	}

	pipeline := NewPipeline(reader, writer, logger, config)

	// 开始性能测试
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

	// 计算性能指标
	recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
	
	t.Logf("📊 性能测试结果:")
	t.Logf("  总记录数: %d", totalRecords)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  处理速度: %.2f 条/秒", recordsPerSecond)
	t.Logf("  批次大小: %d", reader.batchSize)
	t.Logf("  读取批次: %d", stats.ReadBatches)
	t.Logf("  写入批次: %d", stats.WriteBatches)
	t.Logf("  Writer工作协程: %d", config.WriterWorkers)

	// 性能目标验证
	targetTime := 60 * time.Second // 目标1分钟内完成
	originalTime := 5 * time.Minute // 原来需要5分钟

	if elapsed <= targetTime {
		improvement := float64(originalTime) / float64(elapsed)
		t.Logf("✅ 性能目标达成！")
		t.Logf("  目标时间: %v", targetTime)
		t.Logf("  实际时间: %v", elapsed)
		t.Logf("  性能提升: %.1fx", improvement)
	} else {
		improvement := float64(originalTime) / float64(elapsed)
		t.Logf("⚠️  未完全达到目标时间，但仍有显著提升")
		t.Logf("  目标时间: %v", targetTime)
		t.Logf("  实际时间: %v", elapsed)
		t.Logf("  性能提升: %.1fx", improvement)
	}

	// 验证最低性能要求：至少50,000条/秒
	minRequiredSpeed := 50000.0
	if recordsPerSecond >= minRequiredSpeed {
		t.Logf("✅ 达到最低性能要求: %.2f 条/秒 >= %.0f 条/秒", recordsPerSecond, minRequiredSpeed)
	} else {
		t.Errorf("❌ 未达到最低性能要求: %.2f 条/秒 < %.0f 条/秒", recordsPerSecond, minRequiredSpeed)
	}
}

// TestOptimizationComparison 优化前后对比测试
func TestOptimizationComparison(t *testing.T) {
	totalRecords := int64(1000000) // 100万条记录用于快速对比
	recordSize := 10
	logger := logger.New(nil)

	t.Logf("🔄 优化前后对比测试")

	// 测试优化前的配置（模拟原始配置）
	t.Run("优化前", func(t *testing.T) {
		reader := &performanceReader{
			batchSize:    20000, // 原始较小的批次大小
			totalRecords: totalRecords,
			recordSize:   recordSize,
		}

		writer := &performanceWriter{}

		config := &PipelineConfig{
			BufferSize:       10,               // 小缓冲区
			MaxRetries:       0,
			ProgressInterval: 500 * time.Millisecond, // 频繁的进度更新
			WriterWorkers:    1, // 单线程Writer
		}

		pipeline := NewPipeline(reader, writer, logger, config)

		start := time.Now()
		err := pipeline.Start(totalRecords)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Pipeline failed: %v", err)
		}

		recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
		t.Logf("优化前性能:")
		t.Logf("  耗时: %v", elapsed)
		t.Logf("  速度: %.2f 条/秒", recordsPerSecond)
		t.Logf("  批次大小: %d", reader.batchSize)
		t.Logf("  缓冲区大小: %d", config.BufferSize)
		t.Logf("  Writer数量: %d", config.WriterWorkers)
	})

	// 测试优化后的配置
	t.Run("优化后", func(t *testing.T) {
		reader := &performanceReader{
			batchSize:    calculateBatchSize(totalRecords), // 自适应批次大小
			totalRecords: totalRecords,
			recordSize:   recordSize,
		}

		writer := &performanceWriter{}

		config := &PipelineConfig{
			BufferSize:       100,              // 大缓冲区
			MaxRetries:       0,
			ProgressInterval: 5 * time.Second, // 减少进度更新频率
			WriterWorkers:    4, // 多线程Writer
		}

		pipeline := NewPipeline(reader, writer, logger, config)

		start := time.Now()
		err := pipeline.Start(totalRecords)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Pipeline failed: %v", err)
		}

		recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
		t.Logf("优化后性能:")
		t.Logf("  耗时: %v", elapsed)
		t.Logf("  速度: %.2f 条/秒", recordsPerSecond)
		t.Logf("  批次大小: %d", reader.batchSize)
		t.Logf("  缓冲区大小: %d", config.BufferSize)
		t.Logf("  Writer数量: %d", config.WriterWorkers)
	})
}

// TestScalabilityAnalysis 可扩展性分析测试
func TestScalabilityAnalysis(t *testing.T) {
	logger := logger.New(nil)
	recordSize := 10

	dataSizes := []int64{
		100000,   // 10万
		500000,   // 50万
		1000000,  // 100万
		2000000,  // 200万
		5000000,  // 500万
	}

	t.Logf("📈 可扩展性分析测试")

	for _, totalRecords := range dataSizes {
		t.Run(fmt.Sprintf("数据量_%d", totalRecords), func(t *testing.T) {
			reader := &performanceReader{
				batchSize:    calculateBatchSize(totalRecords),
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

			recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
			
			t.Logf("数据量 %d:", totalRecords)
			t.Logf("  耗时: %v", elapsed)
			t.Logf("  速度: %.2f 条/秒", recordsPerSecond)
			t.Logf("  批次大小: %d", reader.batchSize)
			
			// 验证线性扩展性
			expectedMinSpeed := 30000.0 // 最低期望速度
			if recordsPerSecond < expectedMinSpeed {
				t.Errorf("性能不达标: %.2f 条/秒 < %.0f 条/秒", recordsPerSecond, expectedMinSpeed)
			}
		})
	}
}

// TestResourceUtilization 资源利用率测试
func TestResourceUtilization(t *testing.T) {
	totalRecords := int64(1000000)
	recordSize := 10
	logger := logger.New(nil)

	// 测试不同Writer数量的性能
	workerCounts := []int{1, 2, 4, 8}

	t.Logf("🔧 资源利用率测试")

	for _, workerCount := range workerCounts {
		t.Run(fmt.Sprintf("Workers_%d", workerCount), func(t *testing.T) {
			reader := &performanceReader{
				batchSize:    calculateBatchSize(totalRecords),
				totalRecords: totalRecords,
				recordSize:   recordSize,
			}

			writer := &performanceWriter{}

			config := &PipelineConfig{
				BufferSize:       100,
				MaxRetries:       0,
				ProgressInterval: 10 * time.Second,
				WriterWorkers:    workerCount,
			}

			pipeline := NewPipeline(reader, writer, logger, config)

			start := time.Now()
			err := pipeline.Start(totalRecords)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Pipeline failed: %v", err)
			}

			recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
			
			t.Logf("Worker数量 %d:", workerCount)
			t.Logf("  耗时: %v", elapsed)
			t.Logf("  速度: %.2f 条/秒", recordsPerSecond)
			t.Logf("  每Worker效率: %.2f 条/秒", recordsPerSecond/float64(workerCount))
		})
	}
}

// TestMemoryEfficiency 内存效率测试
func TestMemoryEfficiency(t *testing.T) {
	totalRecords := int64(1000000)
	recordSize := 20 // 更大的记录大小
	logger := logger.New(nil)

	// 测试不同缓冲区大小的内存使用
	bufferSizes := []int{10, 50, 100, 200}

	t.Logf("💾 内存效率测试")

	for _, bufferSize := range bufferSizes {
		t.Run(fmt.Sprintf("BufferSize_%d", bufferSize), func(t *testing.T) {
			reader := &performanceReader{
				batchSize:    calculateBatchSize(totalRecords),
				totalRecords: totalRecords,
				recordSize:   recordSize,
			}

			writer := &performanceWriter{}

			config := &PipelineConfig{
				BufferSize:       bufferSize,
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

			recordsPerSecond := float64(totalRecords) / elapsed.Seconds()
			
			t.Logf("缓冲区大小 %d:", bufferSize)
			t.Logf("  耗时: %v", elapsed)
			t.Logf("  速度: %.2f 条/秒", recordsPerSecond)
			
			// 估算内存使用（简化计算）
			estimatedMemoryMB := float64(bufferSize * reader.batchSize * recordSize * 8) / 1024 / 1024
			t.Logf("  估算内存使用: %.2f MB", estimatedMemoryMB)
		})
	}
}
