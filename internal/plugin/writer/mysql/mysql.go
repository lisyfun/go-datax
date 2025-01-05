package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	BatchSize int      `json:"batchSize"`
	PreSQL    []string `json:"preSql"`    // 写入前执行的SQL
	PostSQL   []string `json:"postSql"`   // 写入后执行的SQL
	WriteMode string   `json:"writeMode"` // 写入模式：insert/replace
}

// MySQLWriter MySQL写入器结构体
type MySQLWriter struct {
	Parameter *Parameter
	DB        *sql.DB
	tx        *sql.Tx // 当前事务
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

	return &MySQLWriter{
		Parameter: parameter,
	}
}

// Connect 连接MySQL数据库
func (w *MySQLWriter) Connect() error {
	// 在 DSN 中设置更大的 maxAllowedPacket
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true",
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
	db.SetMaxIdleConns(24)                  // 最小空闲连接数
	db.SetMaxOpenConns(50)                  // 最大连接数
	db.SetConnMaxLifetime(time.Hour)        // 连接最大生命周期
	db.SetConnMaxIdleTime(30 * time.Minute) // 空闲连接最大生命周期

	// 测试连接
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping MySQL失败: %v", err)
	}

	// 设置会话变量以优化写入性能（只设置会话级别的变量）
	_, err = db.Exec(`
		SET SESSION
		unique_checks = 0,
		foreign_key_checks = 0,
		sql_mode = ''
	`)
	if err != nil {
		log.Printf("设置会话变量失败: %v", err)
	}

	w.DB = db
	return nil
}

// PreProcess 预处理：执行写入前的SQL语句
func (w *MySQLWriter) PreProcess() error {
	if len(w.Parameter.PreSQL) == 0 {
		return nil
	}

	for _, sql := range w.Parameter.PreSQL {
		log.Printf("执行预处理SQL: %s", sql)
		result, err := w.DB.Exec(sql)
		if err != nil {
			return fmt.Errorf("执行预处理SQL失败: %v", err)
		}

		// 获取执行结果
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询预处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					return fmt.Errorf("读取预处理SQL结果失败: %v", err)
				}
				log.Printf("预处理SQL执行结果: %d", count)
			}
		} else {
			affected, err := result.RowsAffected()
			if err != nil {
				log.Printf("获取预处理SQL影响行数失败: %v", err)
			} else {
				log.Printf("预处理SQL执行结果: 影响 %d 行", affected)
			}
		}
	}
	return nil
}

// PostProcess 后处理：执行写入后的SQL语句
func (w *MySQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		return nil
	}

	for _, sql := range w.Parameter.PostSQL {
		log.Printf("执行后处理SQL: %s", sql)
		result, err := w.DB.Exec(sql)
		if err != nil {
			return fmt.Errorf("执行后处理SQL失败: %v", err)
		}

		// 获取执行结果
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询后处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					return fmt.Errorf("读取后处理SQL结果失败: %v", err)
				}
				log.Printf("后处理SQL执行结果: %d", count)
			}
		} else {
			affected, err := result.RowsAffected()
			if err != nil {
				log.Printf("获取后处理SQL影响行数失败: %v", err)
			} else {
				log.Printf("后处理SQL执行结果: 影响 %d 行", affected)
			}
		}
	}
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

// Write 写入数据
func (w *MySQLWriter) Write(records []map[string]interface{}) error {
	if w.DB == nil {
		return fmt.Errorf("数据库连接未初始化")
	}

	if len(records) == 0 {
		return nil
	}

	// 设置会话变量以提高性能
	_, err := w.DB.Exec(`
		SET SESSION
		unique_checks = 0,
		foreign_key_checks = 0,
		sql_log_bin = 0
	`)
	if err != nil {
		return fmt.Errorf("设置会话变量失败: %v", err)
	}

	// 构建列名映射
	columnMap := make(map[string]string)
	columnMap["name"] = "username" // 特殊映射

	// 构建插入SQL前缀
	var columns []string
	for _, col := range w.Parameter.Columns {
		columns = append(columns, "`"+col+"`")
	}

	// 计算每批次最大记录数，考虑MySQL占位符限制
	columnCount := len(w.Parameter.Columns)
	maxPlaceholders := 65535 // MySQL最大占位符数量
	maxBatchSize := maxPlaceholders / columnCount
	if maxBatchSize > w.Parameter.BatchSize {
		maxBatchSize = w.Parameter.BatchSize
	}

	// 开始事务
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	startTime := time.Now()
	lastLogTime := startTime
	totalRecords := 0

	// 分批处理
	for i := 0; i < len(records); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		// 构建批量插入的VALUES部分
		var valueStrings []string
		var valueArgs []interface{}

		for _, record := range batch {
			placeholders := make([]string, len(w.Parameter.Columns))
			for i := range placeholders {
				placeholders[i] = "?"
			}
			valueStrings = append(valueStrings, "("+strings.Join(placeholders, ",")+")")

			for _, col := range w.Parameter.Columns {
				// 检查是否需要映射列名
				srcCol := col
				if mappedCol, ok := columnMap[col]; ok {
					srcCol = mappedCol
				}
				valueArgs = append(valueArgs, record[srcCol])
			}
		}

		// 构建完整的SQL语句
		query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
			w.Parameter.Table,
			strings.Join(columns, ","),
			strings.Join(valueStrings, ","),
		)

		// 执行插入
		_, err = tx.Exec(query, valueArgs...)
		if err != nil {
			return fmt.Errorf("执行写入失败: %v", err)
		}

		totalRecords += len(batch)

		// 每秒最多输出一次进度日志
		now := time.Now()
		if now.Sub(lastLogTime) >= time.Second {
			elapsed := now.Sub(startTime)
			speed := float64(totalRecords) / elapsed.Seconds()
			progress := float64(totalRecords) / float64(len(records)) * 100
			log.Printf("进度: %.2f%%, 已处理: %d/%d, 速度: %.2f 条/秒",
				progress, totalRecords, len(records), speed)
			lastLogTime = now
		}

		// 每处理50个批次或累计100万条记录提交一次事务
		if (i/maxBatchSize+1)%50 == 0 || totalRecords >= 1000000 {
			if err = tx.Commit(); err != nil {
				return fmt.Errorf("提交事务失败: %v", err)
			}
			// 开启新事务
			tx, err = w.DB.Begin()
			if err != nil {
				return fmt.Errorf("开始新事务失败: %v", err)
			}
			totalRecords = 0
		}
	}

	// 提交最后的事务
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("提交最终事务失败: %v", err)
	}

	// 恢复会话变量
	_, err = w.DB.Exec(`
		SET SESSION
		unique_checks = 1,
		foreign_key_checks = 1,
		sql_log_bin = 1
	`)
	if err != nil {
		log.Printf("恢复会话变量失败: %v", err)
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

// Close 关闭数据库连接
func (w *MySQLWriter) Close() error {
	if w.tx != nil {
		w.RollbackTransaction()
	}

	// 恢复会话变量
	if w.DB != nil {
		_, err := w.DB.Exec(`
			SET SESSION
			unique_checks = 1,
			foreign_key_checks = 1
		`)
		if err != nil {
			log.Printf("恢复会话变量失败: %v", err)
		}

		return w.DB.Close()
	}
	return nil
}
