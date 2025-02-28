package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"datax/internal/pkg/logger"
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

// NewMySQLWriter 创建新的MySQL写入器实例
func NewMySQLWriter(parameter *Parameter) *MySQLWriter {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
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

// Connect 连接MySQL数据库
func (w *MySQLWriter) Connect() error {
	// 在 DSN 中设置更多的连接参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&timeout=30s&writeTimeout=30s&readTimeout=30s",
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
		w.logger.Info("没有配置预处理SQL语句")
		return nil
	}

	w.logger.Info("========================= 预处理SQL语句 =========================")
	w.logger.Info("预处理SQL语句列表（共 %d 条）:", len(w.Parameter.PreSQL))
	// 先打印所有SQL语句
	for i, sql := range w.Parameter.PreSQL {
		w.logger.Info("[%d] %s", i+1, sql)
	}
	w.logger.Info("============================================================")

	for i, sql := range w.Parameter.PreSQL {
		w.logger.Info("正在执行预处理SQL[%d]: %s", i+1, sql)

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

	w.logger.Info("预处理SQL语句执行完成")
	w.logger.Info("============================================================")
	return nil
}

// PostProcess 后处理：执行写入后的SQL语句
func (w *MySQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		w.logger.Info("没有配置后处理SQL语句")
		return nil
	}

	w.logger.Info("========================= 后处理SQL语句 =========================")
	w.logger.Info("后处理SQL语句列表（共 %d 条）:", len(w.Parameter.PostSQL))
	// 先打印所有SQL语句
	for i, sql := range w.Parameter.PostSQL {
		w.logger.Info("[%d] %s", i+1, sql)
	}
	w.logger.Info("============================================================")

	for i, sql := range w.Parameter.PostSQL {
		w.logger.Info("正在执行后处理SQL[%d]: %s", i+1, sql)

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

	w.logger.Info("后处理SQL语句执行完成")
	w.logger.Info("============================================================")
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

	return fmt.Sprintf("%s %s (%s) VALUES ",
		mode,
		w.Parameter.Table,
		strings.Join(w.Parameter.Columns, ","))
}

// Write 写入数据
func (w *MySQLWriter) Write(records [][]any) error {
	if len(records) == 0 {
		return nil
	}

	// 查询 max_allowed_packet 配置
	var variableName string
	var maxAllowedPacket int64
	err := w.DB.QueryRow("SHOW VARIABLES LIKE 'max_allowed_packet'").Scan(&variableName, &maxAllowedPacket)
	if err != nil {
		w.logger.Warn("查询 max_allowed_packet 失败: %v, 将使用默认配置", err)
		maxAllowedPacket = 4 * 1024 * 1024 // 默认使用 4MB
	}
	w.logger.Info("当前 MySQL max_allowed_packet 配置为: %d bytes (%.2f MB)", maxAllowedPacket, float64(maxAllowedPacket)/(1024*1024))

	// 开始事务
	if err := w.StartTransaction(); err != nil {
		return err
	}

	// 构建插入SQL前缀
	insertPrefix := w.buildInsertPrefix()
	valueTemplate := w.buildValueTemplate()

	// 动态计算每个SQL语句的最大批次大小
	// 假设每个字段平均 100 字节，每条记录额外开销 20 字节
	// 预留 20% 的空间作为安全边界
	avgFieldSize := 100
	recordOverhead := 20
	columnsCount := len(w.Parameter.Columns)
	estimatedRowSize := columnsCount*avgFieldSize + recordOverhead
	maxRowsPerPacket := int(float64(maxAllowedPacket) * 0.8 / float64(estimatedRowSize))

	// 计算每个查询的最大参数数
	maxParamsPerQuery := maxRowsPerPacket * columnsCount
	if maxParamsPerQuery > 65535 { // MySQL 的 prepared statement 参数数量限制
		maxParamsPerQuery = 65535
		maxRowsPerPacket = maxParamsPerQuery / columnsCount
	}

	// 如果配置的批次大小超过了每个查询的最大行数，则使用较小的值
	batchSize := w.Parameter.BatchSize
	if batchSize > maxRowsPerPacket {
		w.logger.Info("由于 MySQL max_allowed_packet 限制，调整批次大小从 %d 到 %d", batchSize, maxRowsPerPacket)
		batchSize = maxRowsPerPacket
	}

	// 确保批次大小不会太小，影响性能
	minBatchSize := 1000
	if batchSize < minBatchSize {
		w.logger.Info("批次大小(%d)过小，自动调整为%d以提升性能", batchSize, minBatchSize)
		batchSize = minBatchSize
	}

	w.logger.Info("最终确定的批次大小为: %d, 预估每行大小: %d bytes, 每批次大约占用: %.2f MB",
		batchSize,
		estimatedRowSize,
		float64(batchSize*estimatedRowSize)/(1024*1024))

	// 分批处理数据
	totalRecords := len(records)
	processedRecords := 0
	startTime := time.Now()

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		batchStartTime := time.Now()

		// 构建完整的SQL语句
		var values []string
		var args []interface{}

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
		batchElapsed := time.Since(batchStartTime)
		totalElapsed := time.Since(startTime)

		w.logger.Info("批次进度: %d/%d (%.1f%%), 本批次写入 %d 条记录，耗时: %v，速度: %.2f 条/秒",
			processedRecords,
			totalRecords,
			float64(processedRecords)/float64(totalRecords)*100,
			rowsAffected,
			batchElapsed,
			float64(rowsAffected)/batchElapsed.Seconds())

		if processedRecords%50000 == 0 || processedRecords == totalRecords {
			w.logger.Info("总进度: 已处理 %d/%d 条记录，总耗时: %v，平均速度: %.2f 条/秒",
				processedRecords,
				totalRecords,
				totalElapsed,
				float64(processedRecords)/totalElapsed.Seconds())
		}
	}

	// 提交事务
	if err := w.CommitTransaction(); err != nil {
		w.logger.Error("提交事务失败: %v", err)
		if rbErr := w.RollbackTransaction(); rbErr != nil {
			w.logger.Error("回滚事务失败: %v", rbErr)
		}
		return fmt.Errorf("提交事务失败: %v", err)
	}

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
