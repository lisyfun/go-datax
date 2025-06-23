package core

import (
	"fmt"
	"runtime"
	"testing"
)

func TestCalculateBaseBatchSize(t *testing.T) {
	tests := []struct {
		name        string
		totalCount  int64
		expectedMin int
		expectedMax int
	}{
		{"极小数据量", 500, 50, 200},
		{"小数据量", 5000, 500, 2000},
		{"中等数据量", 50000, 5000, 20000},
		{"较大数据量", 500000, 15000, 40000},
		{"大数据量", 5000000, 30000, 80000},
		{"超大数据量", 50000000, 50000, 200000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateBaseBatchSize(tt.totalCount)
			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("calculateBaseBatchSize(%d) = %d, expected range [%d, %d]",
					tt.totalCount, result, tt.expectedMin, tt.expectedMax)
			}
			t.Logf("数据量: %d, 基础批次大小: %d", tt.totalCount, result)
		})
	}
}

func TestAdjustBatchSizeForPerformance(t *testing.T) {
	baseBatchSize := 10000
	totalCount := int64(1000000)

	result := adjustBatchSizeForPerformance(baseBatchSize, totalCount)

	// 结果应该在合理范围内
	if result < 1000 || result > 100000 {
		t.Errorf("adjustBatchSizeForPerformance(%d, %d) = %d, expected range [1000, 100000]",
			baseBatchSize, totalCount, result)
	}

	// 获取系统信息用于验证
	cpuCount := runtime.NumCPU()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	availableMemMB := memStats.Sys / 1024 / 1024

	t.Logf("系统信息:")
	t.Logf("  CPU核心数: %d", cpuCount)
	t.Logf("  系统内存: %d MB", availableMemMB)
	t.Logf("  基础批次大小: %d", baseBatchSize)
	t.Logf("  调整后批次大小: %d", result)
	t.Logf("  调整比例: %.2f", float64(result)/float64(baseBatchSize))
}

func TestClampBatchSize(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"小于最小值", 50, 100},
		{"正常范围内", 5000, 5000},
		{"大于最大值", 300000, 200000},
		{"边界值-最小", 100, 100},
		{"边界值-最大", 200000, 200000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clampBatchSize(tt.input)
			if result != tt.expected {
				t.Errorf("clampBatchSize(%d) = %d, expected %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCalculateBatchSize_Integration(t *testing.T) {
	// 测试不同数据量的完整批次大小计算
	testCases := []struct {
		name       string
		totalCount int64
	}{
		{"小数据集", 1000},
		{"中数据集", 100000},
		{"大数据集", 1000000},
		{"超大数据集", 10000000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batchSize := calculateBatchSize(tc.totalCount)

			// 验证批次大小在合理范围内
			if batchSize < 100 || batchSize > 200000 {
				t.Errorf("calculateBatchSize(%d) = %d, out of valid range [100, 200000]",
					tc.totalCount, batchSize)
			}

			// 计算批次数量
			batchCount := (tc.totalCount + int64(batchSize) - 1) / int64(batchSize)

			t.Logf("数据量: %d", tc.totalCount)
			t.Logf("  批次大小: %d", batchSize)
			t.Logf("  批次数量: %d", batchCount)
			t.Logf("  平均每批次: %.2f 条", float64(tc.totalCount)/float64(batchCount))

			// 验证批次数量合理性
			if batchCount < 1 {
				t.Errorf("Batch count should be at least 1, got %d", batchCount)
			}

			// 对于大数据集，批次数量不应该太多（避免过多的网络往返）
			if tc.totalCount > 1000000 && batchCount > 1000 {
				t.Errorf("Too many batches (%d) for large dataset (%d records)", batchCount, tc.totalCount)
			}
		})
	}
}

func TestAdaptiveBatchSize_PerformanceImpact(t *testing.T) {
	// 测试自适应批次大小对不同系统配置的影响
	testData := []int64{10000, 100000, 1000000, 5000000}

	for _, totalCount := range testData {
		t.Run(fmt.Sprintf("TotalCount_%d", totalCount), func(t *testing.T) {
			// 计算基础批次大小
			baseBatchSize := calculateBaseBatchSize(totalCount)

			// 计算调整后的批次大小
			adjustedBatchSize := adjustBatchSizeForPerformance(baseBatchSize, totalCount)

			// 计算最终批次大小
			finalBatchSize := clampBatchSize(adjustedBatchSize)

			// 计算调整比例
			adjustmentRatio := float64(adjustedBatchSize) / float64(baseBatchSize)
			clampingRatio := float64(finalBatchSize) / float64(adjustedBatchSize)

			t.Logf("数据量: %d", totalCount)
			t.Logf("  基础批次大小: %d", baseBatchSize)
			t.Logf("  性能调整后: %d (比例: %.2f)", adjustedBatchSize, adjustmentRatio)
			t.Logf("  最终批次大小: %d (限制比例: %.2f)", finalBatchSize, clampingRatio)

			// 验证调整是否合理
			if adjustmentRatio < 0.1 || adjustmentRatio > 10.0 {
				t.Errorf("Adjustment ratio %.2f is too extreme", adjustmentRatio)
			}

			// 验证最终批次大小的有效性
			if finalBatchSize < 100 || finalBatchSize > 200000 {
				t.Errorf("Final batch size %d is out of valid range", finalBatchSize)
			}
		})
	}
}

func BenchmarkCalculateBatchSize(b *testing.B) {
	testCases := []int64{1000, 10000, 100000, 1000000, 10000000}

	for _, totalCount := range testCases {
		b.Run(fmt.Sprintf("TotalCount_%d", totalCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = calculateBatchSize(totalCount)
			}
		})
	}
}

func TestAdaptiveBatchSize_SystemAwareness(t *testing.T) {
	// 测试系统感知能力
	cpuCount := runtime.NumCPU()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	t.Logf("当前系统配置:")
	t.Logf("  CPU核心数: %d", cpuCount)
	t.Logf("  系统内存: %d MB", memStats.Sys/1024/1024)

	// 测试不同数据量在当前系统上的批次大小
	testCounts := []int64{100000, 1000000, 5000000}

	for _, count := range testCounts {
		batchSize := calculateBatchSize(count)
		baseBatchSize := calculateBaseBatchSize(count)

		t.Logf("数据量 %d:", count)
		t.Logf("  基础批次大小: %d", baseBatchSize)
		t.Logf("  自适应批次大小: %d", batchSize)
		t.Logf("  调整比例: %.2f", float64(batchSize)/float64(baseBatchSize))

		// 验证自适应调整的合理性
		ratio := float64(batchSize) / float64(baseBatchSize)
		if ratio < 0.2 || ratio > 5.0 {
			t.Errorf("Adaptive adjustment ratio %.2f is too extreme for count %d", ratio, count)
		}
	}
}

func TestBatchSizeConsistency(t *testing.T) {
	// 测试批次大小计算的一致性
	totalCount := int64(1000000)

	// 多次计算应该得到相同的结果
	firstResult := calculateBatchSize(totalCount)
	for i := 0; i < 10; i++ {
		result := calculateBatchSize(totalCount)
		if result != firstResult {
			t.Errorf("Batch size calculation is not consistent: first=%d, iteration %d=%d",
				firstResult, i, result)
		}
	}

	t.Logf("批次大小计算一致性测试通过，结果: %d", firstResult)
}
