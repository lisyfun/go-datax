package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"datax/internal/pkg/logger"
	"datax/internal/plugin/common"
)

// Parameter MySQL写入器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Database  string   `json:"database"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	PreSQL    []string `json:"preSql"`
	PostSQL   []string `json:"postSql"`
	BatchSize int      `json:"batchSize"`
	WriteMode string   `json:"writeMode"`
	LogLevel  int      `json:"logLevel"` // 日志级别
}

// MySQLWriter MySQL写入器结构体
type MySQLWriter struct {
	Parameter *Parameter
	DB        *sql.DB
	tx        *sql.Tx // 当前事务
	logger    *logger.Logger
}

// Ensure MySQLWriter implements ColumnAwareWriter
var _ common.ColumnAwareWriter = (*MySQLWriter)(nil)

// NewMySQLWriter 创建新的MySQL写入器实例
func NewMySQLWriter(parameter *Parameter) *MySQLWriter {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 10000 // 增大默认批次大小提升性能
	}
	if parameter.WriteMode == "" {
		parameter.WriteMode = "insert"
	}

	// 如果没有设置日志级别，默认使用 INFO 级别
	if parameter.LogLevel == 0 {
		parameter.LogLevel = int(logger.LevelInfo)
	}

	// 创建日志记录器
	l := logger.New(&logger.Option{
		Level:     logger.Level(parameter.LogLevel),
		Prefix:    "MySQLWriter",
		WithTime:  true,
		WithLevel: true,
	})

	return &MySQLWriter{
		Parameter: parameter,
		logger:    l,
	}
}

// SetColumns 设置要写入的列名（实现ColumnAwareWriter接口）
func (w *MySQLWriter) SetColumns(columns []string) {
	// 检查是否有意义的列名数据
	if len(columns) == 0 {
		w.logger.Debug("收到空的列名列表，忽略")
		return
	}

	// 检查当前列名配置
	hasAsterisk := false
	for _, col := range w.Parameter.Columns {
		if col == "*" {
			hasAsterisk = true
			break
		}
	}

	// 如果当前配置包含星号或者未配置列名，则使用传入的列名
	if hasAsterisk || len(w.Parameter.Columns) == 0 {
		w.logger.Debug("使用从读取器获取的实际列名: %v", strings.Join(columns, ", "))
		w.Parameter.Columns = columns
		
		// 列名更新后，重新计算批次大小
		if w.DB != nil {
			w.logger.Info("列名已更新，重新计算批次大小")
			w.adjustBatchSize()
		}
	} else {
		w.logger.Debug("已有明确的列名配置，保留当前配置: %v", strings.Join(w.Parameter.Columns, ", "))
	}
}

// Connect 连接MySQL数据库
func (w *MySQLWriter) Connect() error {
	// 在 DSN 中设置更多的连接参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&timeout=10s&writeTimeout=30s&readTimeout=30s",
		w.Parameter.Username,
		w.Parameter.Password,
		w.Parameter.Host,
		w.Parameter.Port,
		w.Parameter.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接MySQL失败: %v", err)
	}

	// 优化连接池配置
	db.SetMaxIdleConns(10)                  // 最小空闲连接数
	db.SetMaxOpenConns(20)                  // 最大连接数
	db.SetConnMaxLifetime(time.Hour)        // 连接最大生命周期
	db.SetConnMaxIdleTime(30 * time.Minute) // 空闲连接最大生命周期

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("ping MySQL失败: %v", err)
	}

	w.DB = db
	
	// 连接成功后，计算并调整批次大小
	w.adjustBatchSize()
	
	return nil
}

// PreProcess 预处理：执行写入前的SQL语句
func (w *MySQLWriter) PreProcess() error {
	if len(w.Parameter.PreSQL) == 0 {
		w.logger.Debug("没有配置预处理SQL语句")
		return nil
	}

	// 简化日志输出
	w.logger.Info("开始执行预处理SQL语句（%d条）", len(w.Parameter.PreSQL))

	for i, sql := range w.Parameter.PreSQL {
		// SQL语句太长时截断显示
		displaySQL := sql
		if len(displaySQL) > 100 {
			displaySQL = displaySQL[:97] + "..."
		}
		// 提升为Info级别，确保显示
		w.logger.Info("执行预处理SQL[%d]: %s", i+1, displaySQL)

		if strings.Contains(strings.ToLower(sql), "select") {
			startTime := time.Now()
			rows, err := w.DB.Query(sql)
			if err != nil {
				w.logger.Error("查询预处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("查询预处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					w.logger.Error("读取预处理SQL[%d]结果失败: %v", i+1, err)
					return fmt.Errorf("读取预处理SQL结果失败: %v", err)
				}
				w.logger.Info("预处理SQL[%d]查询结果: %d, 耗时: %v", i+1, count, time.Since(startTime))
			} else {
				w.logger.Info("预处理SQL[%d]查询无结果, 耗时: %v", i+1, time.Since(startTime))
			}
		} else {
			startTime := time.Now()
			result, err := w.DB.Exec(sql)
			if err != nil {
				w.logger.Error("执行预处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("执行预处理SQL失败: %v", err)
			}

			rowsAffected, _ := result.RowsAffected()
			w.logger.Info("预处理SQL[%d]执行成功, 影响行数: %d, 耗时: %v", i+1, rowsAffected, time.Since(startTime))
		}
	}

	w.logger.Info("============预处理SQL语句执行完成============")
	return nil
}

// PostProcess 后处理：执行写入后的SQL语句
func (w *MySQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		w.logger.Debug("没有配置后处理SQL语句")
		return nil
	}

	// 简化日志输出
	w.logger.Info("开始执行后处理SQL语句（%d条）", len(w.Parameter.PostSQL))

	for i, sql := range w.Parameter.PostSQL {
		// SQL语句太长时截断显示
		displaySQL := sql
		if len(displaySQL) > 100 {
			displaySQL = displaySQL[:97] + "..."
		}
		// 提升为Info级别，确保显示
		w.logger.Info("执行后处理SQL[%d]: %s", i+1, displaySQL)

		startTime := time.Now()
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				w.logger.Error("查询后处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("查询后处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					w.logger.Error("读取后处理SQL[%d]结果失败: %v", i+1, err)
					return fmt.Errorf("读取后处理SQL结果失败: %v", err)
				}
				w.logger.Info("后处理SQL[%d]查询结果: %d, 耗时: %v", i+1, count, time.Since(startTime))
			} else {
				w.logger.Info("后处理SQL[%d]查询无结果, 耗时: %v", i+1, time.Since(startTime))
			}
		} else {
			result, err := w.DB.Exec(sql)
			if err != nil {
				w.logger.Error("执行后处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("执行后处理SQL失败: %v", err)
			}

			rowsAffected, _ := result.RowsAffected()
			w.logger.Info("后处理SQL[%d]执行成功, 影响行数: %d, 耗时: %v", i+1, rowsAffected, time.Since(startTime))
		}
	}

	w.logger.Info("============后处理SQL语句执行完成============")
	return nil
}

// StartTransaction 开始事务
func (w *MySQLWriter) StartTransaction() error {
	tx, err := w.DB.Begin()
	if err != nil {
		w.logger.Error("开始事务失败: %v", err)
		return fmt.Errorf("开始事务失败: %v", err)
	}
	w.tx = tx
	w.logger.Debug("开始新事务")
	return nil
}

// CommitTransaction 提交事务
func (w *MySQLWriter) CommitTransaction() error {
	if w.tx == nil {
		w.logger.Warn("没有活动的事务")
		return fmt.Errorf("没有活动的事务")
	}
	err := w.tx.Commit()
	w.tx = nil
	if err != nil {
		w.logger.Error("提交事务失败: %v", err)
		return fmt.Errorf("提交事务失败: %v", err)
	}
	w.logger.Debug("事务提交成功")
	return nil
}

// RollbackTransaction 回滚事务
func (w *MySQLWriter) RollbackTransaction() error {
	if w.tx == nil {
		w.logger.Debug("没有活动的事务需要回滚")
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	if err != nil {
		w.logger.Error("回滚事务失败: %v", err)
		return fmt.Errorf("回滚事务失败: %v", err)
	}
	w.logger.Debug("事务回滚成功")
	return nil
}

// calculateSafeBatchSize 计算安全的批次大小，考虑占位符限制和数据包大小
func (w *MySQLWriter) calculateSafeBatchSize() int {
	// 确保列数有效
	columnsCount := len(w.Parameter.Columns)
	if columnsCount == 0 {
		w.logger.Warn("列数为0，无法计算安全批次大小，使用默认值1000")
		return 1000
	}

	// 1. 基于占位符限制计算最大批次大小
	const maxPlaceholderLimit = 65535 // MySQL预编译语句占位符限制
	maxBatchByPlaceholder := maxPlaceholderLimit / columnsCount
	
	w.logger.Debug("基于占位符限制计算的最大批次大小: %d (列数: %d)", maxBatchByPlaceholder, columnsCount)

	// 2. 查询 max_allowed_packet 配置
	var variableName string
	var maxAllowedPacket int64
	err := w.DB.QueryRow("SHOW VARIABLES LIKE 'max_allowed_packet'").Scan(&variableName, &maxAllowedPacket)
	if err != nil {
		w.logger.Warn("查询 max_allowed_packet 失败: %v, 将使用默认配置", err)
		maxAllowedPacket = 4 * 1024 * 1024 // 默认使用 4MB
	}

	// 3. 基于数据包大小计算最大批次大小
	avgFieldSize := 100  // 每个字段平均 100 字节
	recordOverhead := 20 // 每条记录额外开销 20 字节
	estimatedRowSize := columnsCount*avgFieldSize + recordOverhead
	
	// 预留 20% 的空间作为安全边界
	maxBatchByPacket := int(float64(maxAllowedPacket) * 0.8 / float64(estimatedRowSize))
	
	w.logger.Debug("基于数据包大小计算的最大批次大小: %d (max_allowed_packet: %d bytes)",
		maxBatchByPacket, maxAllowedPacket)

	// 4. 取两者中的较小值作为安全批次大小
	safeBatchSize := maxBatchByPlaceholder
	if maxBatchByPacket < maxBatchByPlaceholder {
		safeBatchSize = maxBatchByPacket
	}

	// 5. 确保不小于最小批次大小
	const minBatchSize = 1000
	if safeBatchSize < minBatchSize {
		safeBatchSize = minBatchSize
		w.logger.Debug("批次大小(%d)小于最小值，调整为%d", safeBatchSize, minBatchSize)
	}

	w.logger.Debug("最终计算的安全批次大小: %d", safeBatchSize)
	return safeBatchSize
}

// adjustBatchSize 调整批次大小，确保不超过MySQL限制
func (w *MySQLWriter) adjustBatchSize() {
	// 如果列数为空，无法计算，跳过调整
	if len(w.Parameter.Columns) == 0 {
		w.logger.Debug("列数为空，跳过批次大小调整")
		return
	}
	
	originalBatchSize := w.Parameter.BatchSize
	safeBatchSize := w.calculateSafeBatchSize()
	
	// 只有当原始批次大小大于安全批次大小时才调整
	if originalBatchSize > safeBatchSize {
		w.Parameter.BatchSize = safeBatchSize
		w.logger.Info("批次大小已自动调整: %d -> %d (避免占位符超限)",
			originalBatchSize, safeBatchSize)
	} else {
		w.logger.Debug("当前批次大小(%d)已在安全范围内，无需调整", originalBatchSize)
	}
}

// buildValueTemplate 构建值模板
func (w *MySQLWriter) buildValueTemplate() string {
	placeholders := make([]string, len(w.Parameter.Columns))
	for i := range w.Parameter.Columns {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(placeholders, ",") + ")"
}

// buildInsertPrefix 构建插入SQL前缀
func (w *MySQLWriter) buildInsertPrefix() string {
	var mode string
	switch strings.ToLower(w.Parameter.WriteMode) {
	case "insert":
		mode = "INSERT INTO"
	case "replace":
		mode = "REPLACE INTO"
	case "update":
		mode = "INSERT INTO"
	default:
		mode = "INSERT INTO"
	}

	// 检查columns是否包含星号
	hasAsterisk := false
	for _, col := range w.Parameter.Columns {
		if col == "*" {
			hasAsterisk = true
			break
		}
	}

	// 如果包含星号，需要查询表结构获取所有列名
	if hasAsterisk {
		columns, err := w.getTableColumns()
		if err != nil {
			w.logger.Error("获取表结构失败: %v", err)
			// 发生错误时返回空的列名，上层函数会检查列数并报错
			return fmt.Sprintf("%s %s () VALUES ", mode, w.Parameter.Table)
		}

		// 更新Parameter.Columns
		w.Parameter.Columns = columns
		w.logger.Debug("检测到通配符*，已获取表%s的全部字段: %v", w.Parameter.Table, strings.Join(columns, ", "))
	}

	return fmt.Sprintf("%s %s (%s) VALUES ",
		mode,
		w.Parameter.Table,
		strings.Join(w.Parameter.Columns, ","))
}

// getTableColumns 获取表的所有列名
func (w *MySQLWriter) getTableColumns() ([]string, error) {
	// 构建查询列名的SQL
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
		w.Parameter.Database,
		w.Parameter.Table)

	// 执行查询
	rows, err := w.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询表结构失败: %v", err)
	}
	defer rows.Close()

	// 收集列名
	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("读取列名失败: %v", err)
		}
		columns = append(columns, columnName)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列名结果集失败: %v", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("表 %s.%s 未找到任何列", w.Parameter.Database, w.Parameter.Table)
	}

	return columns, nil
}

// Write 写入数据
func (w *MySQLWriter) Write(records [][]any) error {
	if len(records) == 0 {
		return nil
	}

	// 如果存在星号，先构建插入SQL前缀，这会更新Parameter.Columns
	insertPrefix := w.buildInsertPrefix()

	// 使用已在Connect方法中调整好的批次大小
	batchSize := w.Parameter.BatchSize
	w.logger.Debug("使用已调整的批次大小: %d", batchSize)

	// 只在开始写入时输出一次批次大小信息
	w.logger.Debug("开始数据写入，批次大小: %d，总记录数: %d", batchSize, len(records))

	// 开始事务
	if err := w.StartTransaction(); err != nil {
		return err
	}

	// 重用已构建的插入SQL前缀
	valueTemplate := w.buildValueTemplate()

	// 分批处理数据
	processedRecords := 0
	startTime := time.Now()

	// 不打印每个批次的进度，减少日志输出
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]

		// 验证每条记录的列数是否与配置的列数一致
		if err := w.validateColumnCount(batch); err != nil {
			// 尝试回滚事务
			if rbErr := w.RollbackTransaction(); rbErr != nil {
				w.logger.Error("回滚事务失败: %v", rbErr)
			}
			return err
		}

		// 构建完整的SQL语句
		var values []string
		var args []any

		for _, record := range batch {
			values = append(values, valueTemplate)
			args = append(args, record...)
		}

		// 验证SQL参数数量是否匹配
		if err := w.validateSQLParameters(args, len(batch)); err != nil {
			// 尝试回滚事务
			if rbErr := w.RollbackTransaction(); rbErr != nil {
				w.logger.Error("回滚事务失败: %v", rbErr)
			}
			return err
		}

		sql := insertPrefix + strings.Join(values, ",")
		w.logger.Debug("执行SQL: %s", sql)

		// 执行SQL
		result, err := w.tx.Exec(sql, args...)
		if err != nil {
			// 检查是否是参数数量不匹配的错误
			if strings.Contains(err.Error(), "expected") && strings.Contains(err.Error(), "arguments") && strings.Contains(err.Error(), "got") {
				// 解析错误信息，提取期望和实际的参数数量
				expectedArgs := w.extractExpectedArgs(err.Error())
				actualArgs := len(args)
				
				w.logger.Error("SQL参数数量不匹配: 期望 %d 个参数，实际提供了 %d 个参数", expectedArgs, actualArgs)
				w.logger.Error("可能的原因: Reader和Writer的列数不一致")
				w.logger.Error("当前Writer配置的列数: %d", len(w.Parameter.Columns))
				w.logger.Error("当前批次大小: %d", batchSize)
				w.logger.Error("计算得到的期望参数数: %d (列数 × 批次大小)", len(w.Parameter.Columns) * batchSize)
				
				// 尝试回滚事务
				if rbErr := w.RollbackTransaction(); rbErr != nil {
					w.logger.Error("回滚事务失败: %v", rbErr)
				}
				
				return fmt.Errorf("SQL参数数量不匹配: 期望 %d 个参数，实际提供了 %d 个参数. "+
					"这通常是因为Reader和Writer的列数不一致。请检查配置文件中的columns参数。",
					expectedArgs, actualArgs)
			}
			
			w.logger.Error("执行SQL失败: %v", err)
			if rbErr := w.RollbackTransaction(); rbErr != nil {
				w.logger.Error("回滚事务失败: %v", rbErr)
			}
			return fmt.Errorf("执行SQL失败: %v", err)
		}

		// 获取影响的行数
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			w.logger.Error("获取影响行数失败: %v", err)
			if rbErr := w.RollbackTransaction(); rbErr != nil {
				w.logger.Error("回滚事务失败: %v", rbErr)
			}
			return fmt.Errorf("获取影响行数失败: %v", err)
		}

		processedRecords += int(rowsAffected)
	}

	// 提交事务
	if err := w.CommitTransaction(); err != nil {
		w.logger.Error("提交事务失败: %v", err)
		if rbErr := w.RollbackTransaction(); rbErr != nil {
			w.logger.Error("回滚事务失败: %v", rbErr)
		}
		return fmt.Errorf("提交事务失败: %v", err)
	}

	// 只在完成时输出一次总结信息
	totalElapsed := time.Since(startTime)
	avgSpeed := float64(processedRecords) / totalElapsed.Seconds()
	w.logger.Info("数据写入完成: 写入记录 %d 条, 耗时 %v, 速度 %.2f 条/秒", processedRecords, totalElapsed, avgSpeed)

	return nil
}

// extractExpectedArgs 从错误信息中提取期望的参数数量
func (w *MySQLWriter) extractExpectedArgs(errMsg string) int {
	// 错误信息格式: "sql: expected 3800 arguments, got 4100"
	re := regexp.MustCompile(`expected (\d+) arguments`)
	matches := re.FindStringSubmatch(errMsg)
	if len(matches) > 1 {
		if num, err := strconv.Atoi(matches[1]); err == nil {
			return num
		}
	}
	return 0
}

// validateColumnCount 验证记录的列数是否与配置的列数一致
func (w *MySQLWriter) validateColumnCount(records [][]any) error {
	columnsCount := len(w.Parameter.Columns)
	if columnsCount == 0 {
		return fmt.Errorf("列数不能为0，请检查配置的columns参数")
	}

	for recordIndex, record := range records {
		if len(record) != columnsCount {
			w.logger.Error("数据列数不匹配: 记录 %d 有 %d 列，但配置了 %d 列",
				recordIndex+1, len(record), columnsCount)
			w.logger.Error("期望的列: %v", w.Parameter.Columns)
			
			return fmt.Errorf("数据列数不匹配: 记录 %d 有 %d 列，但配置了 %d 列. "+
				"请检查Reader和Writer的列配置是否一致",
				recordIndex+1, len(record), columnsCount)
		}
	}
	
	return nil
}

// validateSQLParameters 验证SQL参数数量是否匹配
func (w *MySQLWriter) validateSQLParameters(args []any, batchSize int) error {
	columnsCount := len(w.Parameter.Columns)
	expectedParams := columnsCount * batchSize
	actualParams := len(args)
	
	if expectedParams != actualParams {
		w.logger.Error("SQL参数数量不匹配: 期望 %d 个参数，实际提供了 %d 个参数",
			expectedParams, actualParams)
		w.logger.Error("可能的原因: Reader和Writer的列数不一致")
		w.logger.Error("当前Writer配置的列数: %d", columnsCount)
		w.logger.Error("当前批次大小: %d", batchSize)
		w.logger.Error("计算得到的期望参数数: %d (列数 × 批次大小)", expectedParams)
		
		return fmt.Errorf("SQL参数数量不匹配: 期望 %d 个参数，实际提供了 %d 个参数. "+
			"这通常是因为Reader和Writer的列数不一致。请检查配置文件中的columns参数。",
			expectedParams, actualParams)
	}
	
	return nil
}

// GetRecordCount 获取目标表的记录数，用于数据完整性检查
func (w *MySQLWriter) GetRecordCount() (int64, error) {
	if w.DB == nil {
		return 0, fmt.Errorf("数据库连接未初始化")
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", w.Parameter.Table)

	var count int64
	err := w.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("查询记录数失败: %v", err)
	}

	return count, nil
}

// Close 关闭数据库连接
func (w *MySQLWriter) Close() error {
	if w.DB != nil {
		w.logger.Debug("关闭数据库连接")
		return w.DB.Close()
	}
	return nil
}
