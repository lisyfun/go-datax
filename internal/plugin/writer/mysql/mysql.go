package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// LogLevel 日志级别
type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
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
	LogLevel  LogLevel `json:"logLevel"` // 日志级别
}

// MySQLWriter MySQL写入器结构体
type MySQLWriter struct {
	Parameter *Parameter
	DB        *sql.DB
	tx        *sql.Tx // 当前事务
}

// logf 根据日志级别打印日志
func (w *MySQLWriter) logf(level LogLevel, format string, v ...interface{}) {
	if level <= w.Parameter.LogLevel {
		prefix := ""
		switch level {
		case LogLevelError:
			prefix = "[ERROR] "
		case LogLevelWarn:
			prefix = "[WARN] "
		case LogLevelInfo:
			prefix = "[INFO] "
		case LogLevelDebug:
			prefix = "[DEBUG] "
		}
		log.Printf(prefix+format, v...)
	}
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
	// 默认日志级别为 Info
	if parameter.LogLevel == 0 {
		parameter.LogLevel = LogLevelInfo
	}

	return &MySQLWriter{
		Parameter: parameter,
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
		w.logf(LogLevelDebug, "没有配置预处理SQL语句")
		return nil
	}

	w.logf(LogLevelInfo, "开始执行预处理SQL语句，共 %d 条", len(w.Parameter.PreSQL))

	for i, sql := range w.Parameter.PreSQL {
		w.logf(LogLevelDebug, "执行预处理SQL[%d]: %s", i+1, sql)

		if strings.Contains(strings.ToLower(sql), "select") {
			startTime := time.Now()
			rows, err := w.DB.Query(sql)
			if err != nil {
				w.logf(LogLevelError, "查询预处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("查询预处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					w.logf(LogLevelError, "读取预处理SQL[%d]结果失败: %v", i+1, err)
					return fmt.Errorf("读取预处理SQL结果失败: %v", err)
				}
				w.logf(LogLevelInfo, "预处理SQL[%d]查询结果: %d, 耗时: %v", i+1, count, time.Since(startTime))
			} else {
				w.logf(LogLevelInfo, "预处理SQL[%d]查询无结果, 耗时: %v", i+1, time.Since(startTime))
			}
		} else {
			startTime := time.Now()
			result, err := w.DB.Exec(sql)
			if err != nil {
				w.logf(LogLevelError, "执行预处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("执行预处理SQL失败: %v", err)
			}

			rowsAffected, _ := result.RowsAffected()
			w.logf(LogLevelInfo, "预处理SQL[%d]执行成功, 影响行数: %d, 耗时: %v", i+1, rowsAffected, time.Since(startTime))
		}
	}

	w.logf(LogLevelInfo, "预处理SQL语句执行完成")
	return nil
}

// PostProcess 后处理：执行写入后的SQL语句
func (w *MySQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		w.logf(LogLevelDebug, "没有配置后处理SQL语句")
		return nil
	}

	w.logf(LogLevelInfo, "开始执行后处理SQL语句，共 %d 条", len(w.Parameter.PostSQL))

	for i, sql := range w.Parameter.PostSQL {
		w.logf(LogLevelDebug, "执行后处理SQL[%d]: %s", i+1, sql)

		startTime := time.Now()
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				w.logf(LogLevelError, "查询后处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("查询后处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					w.logf(LogLevelError, "读取后处理SQL[%d]结果失败: %v", i+1, err)
					return fmt.Errorf("读取后处理SQL结果失败: %v", err)
				}
				w.logf(LogLevelInfo, "后处理SQL[%d]查询结果: %d, 耗时: %v", i+1, count, time.Since(startTime))
			}
		} else {
			result, err := w.DB.Exec(sql)
			if err != nil {
				w.logf(LogLevelError, "执行后处理SQL[%d]失败: %v", i+1, err)
				return fmt.Errorf("执行后处理SQL失败: %v", err)
			}

			rowsAffected, _ := result.RowsAffected()
			w.logf(LogLevelInfo, "后处理SQL[%d]执行成功, 影响行数: %d, 耗时: %v", i+1, rowsAffected, time.Since(startTime))
		}
	}

	w.logf(LogLevelInfo, "后处理SQL语句执行完成")
	return nil
}

// StartTransaction 开始事务
func (w *MySQLWriter) StartTransaction() error {
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	w.tx = tx
	return nil
}

// CommitTransaction 提交事务
func (w *MySQLWriter) CommitTransaction() error {
	if w.tx == nil {
		return fmt.Errorf("没有活动的事务")
	}
	err := w.tx.Commit()
	w.tx = nil
	if err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}
	return nil
}

// RollbackTransaction 回滚事务
func (w *MySQLWriter) RollbackTransaction() error {
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	if err != nil {
		return fmt.Errorf("回滚事务失败: %v", err)
	}
	return nil
}

// buildInsertPrefix 构建插入SQL前缀
func (w *MySQLWriter) buildInsertPrefix() string {
	var columns []string
	for _, col := range w.Parameter.Columns {
		columns = append(columns, "`"+col+"`")
	}

	var action string
	switch strings.ToLower(w.Parameter.WriteMode) {
	case "replace":
		action = "REPLACE"
	default:
		action = "INSERT"
	}

	return fmt.Sprintf("%s INTO `%s` (%s) VALUES ",
		action,
		w.Parameter.Table,
		strings.Join(columns, ","),
	)
}

// Write 写入数据
func (w *MySQLWriter) Write(records [][]interface{}) error {
	if w.DB == nil {
		return fmt.Errorf("数据库连接未初始化")
	}

	if len(records) == 0 {
		w.logf(LogLevelDebug, "本批次无数据需要写入")
		return nil
	}

	// 计算每批次最大记录数，考虑MySQL占位符限制
	columnCount := len(w.Parameter.Columns)
	maxPlaceholders := 65535 // MySQL最大占位符数量
	maxBatchSize := maxPlaceholders / columnCount
	if maxBatchSize > w.Parameter.BatchSize {
		maxBatchSize = w.Parameter.BatchSize
	}

	// 预先构建SQL模板
	insertPrefix := w.buildInsertPrefix()
	w.logf(LogLevelDebug, "构建写入SQL: %s", insertPrefix)

	// 开始事务
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback() // 确保在出错时回滚

	// 预分配缓冲区
	var sqlBuilder strings.Builder
	sqlBuilder.Grow(1024 * 1024) // 预分配1MB的buffer
	valueArgs := make([]interface{}, 0, maxBatchSize*columnCount)

	startTime := time.Now()
	lastLogTime := startTime
	totalRecords := 0
	batchCount := 0

	// 分批处理数据
	for i := 0; i < len(records); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		sqlBuilder.Reset()
		sqlBuilder.WriteString(insertPrefix)
		valueArgs = valueArgs[:0]

		// 构建批量插入值
		for j, record := range batch {
			if j > 0 {
				sqlBuilder.WriteByte(',')
			}
			sqlBuilder.WriteString("(")
			for k := range w.Parameter.Columns {
				if k > 0 {
					sqlBuilder.WriteByte(',')
				}
				sqlBuilder.WriteByte('?')
				valueArgs = append(valueArgs, record[k])
			}
			sqlBuilder.WriteByte(')')
		}

		// 执行批量写入
		if _, err := tx.Exec(sqlBuilder.String(), valueArgs...); err != nil {
			return fmt.Errorf("执行写入失败: %v", err)
		}

		totalRecords += len(batch)
		batchCount++

		// 每处理50个批次或累计100万条记录提交一次事务
		if batchCount >= 50 || totalRecords >= 1000000 {
			if err := tx.Commit(); err != nil {
				// 处理事务提交错误
				if mysqlErr, ok := err.(*mysql.MySQLError); ok {
					switch mysqlErr.Number {
					case 1213: // 死锁
						return fmt.Errorf("发生死锁，需要重试: %v", err)
					case 1205: // 锁等待超时
						return fmt.Errorf("锁等待超时，需要重试: %v", err)
					default:
						return fmt.Errorf("提交事务失败: %v", err)
					}
				}
				return fmt.Errorf("提交事务失败: %v", err)
			}

			// 开启新事务
			tx, err = w.DB.Begin()
			if err != nil {
				return fmt.Errorf("开始新事务失败: %v", err)
			}
			batchCount = 0
			totalRecords = 0
		}

		// 每10秒输出一次进度日志
		now := time.Now()
		if now.Sub(lastLogTime) >= 10*time.Second {
			w.logf(LogLevelInfo, "已处理 %d 条记录, 耗时: %v", totalRecords, now.Sub(startTime))
			lastLogTime = now
		}
	}

	// 提交最后的事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交最后的事务失败: %v", err)
	}

	w.logf(LogLevelInfo, "写入完成，共处理 %d 条记录, 总耗时: %v", totalRecords, time.Since(startTime))
	return nil
}

// Close 关闭数据库连接
func (w *MySQLWriter) Close() error {
	var errs []error

	// 如果存在未完成的事务，进行回滚
	if w.tx != nil {
		if err := w.RollbackTransaction(); err != nil {
			errs = append(errs, fmt.Errorf("回滚事务失败: %v", err))
		}
	}

	// 关闭数据库连接
	if w.DB != nil {
		if err := w.DB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭数据库连接失败: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("关闭过程中发生错误: %v", errs)
	}
	return nil
}
