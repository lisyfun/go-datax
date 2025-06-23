package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"datax/internal/pkg/logger"
	"datax/internal/plugin/common"
)

// Parameter Oracle写入器参数
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Service   string   `json:"service"`
	Table     string   `json:"table"`
	Schema    string   `json:"schema"`
	Columns   []string `json:"columns"`
	WriteMode string   `json:"writeMode"`
	BatchSize int      `json:"batchSize"`
	PreSQL    []string `json:"preSql"`   // 写入前执行的SQL
	PostSQL   []string `json:"postSql"`  // 写入后执行的SQL
	LogLevel  int      `json:"logLevel"` // 日志级别
}

// OracleWriter Oracle写入器结构体
type OracleWriter struct {
	Parameter *Parameter
	DB        *sql.DB
	tx        *sql.Tx // 当前事务
	logger    *logger.Logger
}

// Ensure OracleWriter implements ColumnAwareWriter
var _ common.ColumnAwareWriter = (*OracleWriter)(nil)

// NewOracleWriter 创建新的Oracle写入器实例
func NewOracleWriter(parameter *Parameter) *OracleWriter {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 10000 // 增大默认批次大小提升性能
	}
	if parameter.Schema == "" {
		parameter.Schema = parameter.Username // Oracle默认使用用户名作为schema
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
		Prefix:    "OracleWriter",
		WithTime:  true,
		WithLevel: true,
	})

	return &OracleWriter{
		Parameter: parameter,
		logger:    l,
	}
}

// SetColumns 设置要写入的列名（实现ColumnAwareWriter接口）
func (w *OracleWriter) SetColumns(columns []string) {
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

// Connect 连接Oracle数据库
func (w *OracleWriter) Connect() error {
	// 构建Oracle连接字符串
	connStr := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		w.Parameter.Username,
		w.Parameter.Password,
		w.Parameter.Host,
		w.Parameter.Port,
		w.Parameter.Service,
	)

	db, err := sql.Open("godror", connStr)
	if err != nil {
		return fmt.Errorf("连接Oracle失败: %v", err)
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
		return fmt.Errorf("ping Oracle失败: %v", err)
	}

	w.DB = db
	return nil
}

// PreProcess 预处理：执行写入前的SQL语句
func (w *OracleWriter) PreProcess() error {
	if len(w.Parameter.PreSQL) == 0 {
		w.logger.Debug("没有配置预处理SQL语句")
		return nil
	}

	w.logger.Info("开始执行预处理SQL语句（%d条）", len(w.Parameter.PreSQL))

	for i, sql := range w.Parameter.PreSQL {
		displaySQL := sql
		if len(displaySQL) > 100 {
			displaySQL = displaySQL[:97] + "..."
		}
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
func (w *OracleWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		w.logger.Debug("没有配置后处理SQL语句")
		return nil
	}

	w.logger.Info("开始执行后处理SQL语句（%d条）", len(w.Parameter.PostSQL))

	for i, sql := range w.Parameter.PostSQL {
		displaySQL := sql
		if len(displaySQL) > 100 {
			displaySQL = displaySQL[:97] + "..."
		}
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

// Write 写入数据
func (w *OracleWriter) Write(records [][]any) error {
	if len(records) == 0 {
		return nil
	}

	// 检查列数是否为0
	columnsCount := len(w.Parameter.Columns)
	if columnsCount == 0 {
		return fmt.Errorf("列数不能为0，请检查配置的columns参数")
	}

	// 确保批次大小不会太小
	minBatchSize := 5000 // 提高最小批次大小
	if w.Parameter.BatchSize < minBatchSize {
		w.Parameter.BatchSize = minBatchSize
		w.logger.Debug("批次大小过小，自动调整为%d", w.Parameter.BatchSize)
	}

	// 使用最终确定的批次大小
	batchSize := w.Parameter.BatchSize

	// 只在开始写入时输出一次批次大小信息
	w.logger.Debug("开始数据写入，批次大小: %d，总记录数: %d", batchSize, len(records))

	// 开始事务
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 构建插入SQL前缀
	tableName := w.Parameter.Table
	if w.Parameter.Schema != "" {
		tableName = w.Parameter.Schema + "." + w.Parameter.Table
	}

	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		tableName,
		strings.Join(w.Parameter.Columns, ","))

	// 构建值模板（Oracle使用:1, :2格式）
	placeholders := make([]string, len(w.Parameter.Columns))
	for i := range w.Parameter.Columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	valueTemplate := "(" + strings.Join(placeholders, ",") + ")"

	// 分批处理数据
	processedRecords := 0
	startTime := time.Now()

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

		// 执行SQL
		result, err := tx.Exec(sql, args...)
		if err != nil {
			w.logger.Error("执行SQL失败: %v", err)
			return fmt.Errorf("执行SQL失败: %v", err)
		}

		// 获取影响的行数
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			w.logger.Error("获取影响行数失败: %v", err)
			return fmt.Errorf("获取影响行数失败: %v", err)
		}

		processedRecords += int(rowsAffected)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		w.logger.Error("提交事务失败: %v", err)
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
func (w *OracleWriter) Close() error {
	if w.DB != nil {
		w.logger.Debug("关闭数据库连接")
		return w.DB.Close()
	}
	return nil
}
