package mysql

import (
	"context"
	"database/sql"
	"fmt"
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

	// 检查列数是否为0
	columnsCount := len(w.Parameter.Columns)
	if columnsCount == 0 {
		return fmt.Errorf("列数不能为0，请检查配置的columns参数")
	}

	// 查询 max_allowed_packet 配置以确保安全写入
	var variableName string
	var maxAllowedPacket int64
	err := w.DB.QueryRow("SHOW VARIABLES LIKE 'max_allowed_packet'").Scan(&variableName, &maxAllowedPacket)
	if err != nil {
		w.logger.Warn("查询 max_allowed_packet 失败: %v, 将使用默认配置", err)
		maxAllowedPacket = 4 * 1024 * 1024 // 默认使用 4MB
	}

	// 只在Debug级别记录详细的参数信息
	w.logger.Debug("当前 MySQL max_allowed_packet 配置为: %d bytes (%.2f MB)",
		maxAllowedPacket,
		float64(maxAllowedPacket)/(1024*1024))

	// 计算每条记录的估计大小
	avgFieldSize := 100  // 每个字段平均 100 字节
	recordOverhead := 20 // 每条记录额外开销 20 字节
	estimatedRowSize := columnsCount*avgFieldSize + recordOverhead

	// 计算基于max_allowed_packet的安全行数限制
	// 预留 20% 的空间作为安全边界
	maxRowsPerPacket := int(float64(maxAllowedPacket) * 0.8 / float64(estimatedRowSize))

	// 检查prepared statement参数数量限制 (MySQL限制为65535个参数)
	maxParamsPerQuery := maxRowsPerPacket * columnsCount
	if maxParamsPerQuery > 65535 {
		maxParamsPerQuery = 65535
		maxRowsPerPacket = maxParamsPerQuery / columnsCount
	}

	// 只有当当前批次大小超出安全限制时才调整
	originalBatchSize := w.Parameter.BatchSize
	if originalBatchSize > maxRowsPerPacket {
		w.Parameter.BatchSize = maxRowsPerPacket
		w.logger.Debug("由于 MySQL 限制，调整批次大小从 %d 到 %d", originalBatchSize, w.Parameter.BatchSize)
	}

	// 确保批次大小不会太小
	minBatchSize := 5000 // 提高最小批次大小
	if w.Parameter.BatchSize < minBatchSize {
		w.Parameter.BatchSize = minBatchSize
		w.logger.Debug("批次大小(%d)过小，自动调整为%d", originalBatchSize, w.Parameter.BatchSize)
	}

	// 使用最终确定的批次大小
	batchSize := w.Parameter.BatchSize

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

		// 构建完整的SQL语句
		var values []string
		var args []any

		for _, record := range batch {
			values = append(values, valueTemplate)
			args = append(args, record...)
		}

		sql := insertPrefix + strings.Join(values, ",")
		w.logger.Debug("执行SQL: %s", sql)

		// 执行SQL
		result, err := w.tx.Exec(sql, args...)
		if err != nil {
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
	w.logger.Debug("数据写入完成，总共写入: %d 条记录，总耗时: %v，平均速度: %.2f 条/秒",
		processedRecords,
		totalElapsed,
		avgSpeed)

	return nil
}

// Close 关闭数据库连接
func (w *MySQLWriter) Close() error {
	if w.DB != nil {
		w.logger.Debug("关闭数据库连接")
		return w.DB.Close()
	}
	return nil
}
