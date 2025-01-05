package postgresql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Parameter PostgreSQL写入器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Database  string   `json:"database"`
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	BatchSize int      `json:"batchSize"`
	PreSQL    []string `json:"preSql"`    // 写入前执行的SQL
	PostSQL   []string `json:"postSql"`   // 写入后执行的SQL
	WriteMode string   `json:"writeMode"` // 写入模式：insert/copy
}

// PostgreSQLWriter PostgreSQL写入器结构体
type PostgreSQLWriter struct {
	Parameter *Parameter
	DB        *sql.DB
	tx        *sql.Tx // 当前事务
}

// NewPostgreSQLWriter 创建新的PostgreSQL写入器实例
func NewPostgreSQLWriter(parameter *Parameter) *PostgreSQLWriter {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
	}
	if parameter.Schema == "" {
		parameter.Schema = "public"
	}
	if parameter.WriteMode == "" {
		parameter.WriteMode = "insert"
	}

	return &PostgreSQLWriter{
		Parameter: parameter,
	}
}

// Connect 连接PostgreSQL数据库
func (w *PostgreSQLWriter) Connect() error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		w.Parameter.Host,
		w.Parameter.Port,
		w.Parameter.Username,
		w.Parameter.Password,
		w.Parameter.Database,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("连接PostgreSQL失败: %v", err)
	}

	// 优化连接池配置
	db.SetMaxIdleConns(24)                  // 最小空闲连接数
	db.SetMaxOpenConns(50)                  // 最大连接数
	db.SetConnMaxLifetime(time.Hour)        // 连接最大生命周期
	db.SetConnMaxIdleTime(30 * time.Minute) // 空闲连接最大生命周期

	// 测试连接
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping PostgreSQL失败: %v", err)
	}

	w.DB = db
	return nil
}

// PreProcess 预处理：执行写入前的SQL语句
func (w *PostgreSQLWriter) PreProcess() error {
	if len(w.Parameter.PreSQL) == 0 {
		return nil
	}

	for _, sql := range w.Parameter.PreSQL {
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询预处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				// 获取列信息
				columns, err := rows.Columns()
				if err != nil {
					return fmt.Errorf("获取列信息失败: %v", err)
				}

				// 创建一个切片来存储所有列的值
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("读取预处理SQL结果失败: %v", err)
				}
			}
		} else {
			_, err := w.DB.Exec(sql)
			if err != nil {
				return fmt.Errorf("执行预处理SQL失败: %v", err)
			}
		}
	}
	return nil
}

// PostProcess 后处理：执行写入后的SQL语句
func (w *PostgreSQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		return nil
	}

	for _, sql := range w.Parameter.PostSQL {
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询后处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			if rows.Next() {
				// 获取列信息
				columns, err := rows.Columns()
				if err != nil {
					return fmt.Errorf("获取列信息失败: %v", err)
				}

				// 创建一个切片来存储所有列的值
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("读取后处理SQL结果失败: %v", err)
				}
			}
		} else {
			_, err := w.DB.Exec(sql)
			if err != nil {
				return fmt.Errorf("执行后处理SQL失败: %v", err)
			}
		}
	}
	return nil
}

// Write 写入数据
func (w *PostgreSQLWriter) Write(records [][]interface{}) error {
	if w.DB == nil {
		return fmt.Errorf("数据库连接未初始化")
	}

	if len(records) == 0 {
		return nil
	}

	// 构建插入SQL
	var columns []string
	for _, col := range w.Parameter.Columns {
		columns = append(columns, fmt.Sprintf("%s", col))
	}

	// 构建占位符
	var placeholders []string
	for i := range w.Parameter.Columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	// 构建SQL语句
	var action string
	switch strings.ToLower(w.Parameter.WriteMode) {
	case "update":
		action = "UPDATE"
	default:
		action = "INSERT"
	}

	sql := fmt.Sprintf("%s INTO %s.%s (%s) VALUES (%s)",
		action,
		w.Parameter.Schema,
		w.Parameter.Table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)

	// 开始事务
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 准备语句
	stmt, err := tx.Prepare(sql)
	if err != nil {
		return fmt.Errorf("准备语句失败: %v", err)
	}
	defer stmt.Close()

	// 批量写入数据
	for _, record := range records {
		// 直接使用记录中的值
		_, err = stmt.Exec(record...)
		if err != nil {
			return fmt.Errorf("执行写入失败: %v", err)
		}
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	return nil
}

// buildInsertPrefix 构建插入SQL前缀
func (w *PostgreSQLWriter) buildInsertPrefix() string {
	var columns []string
	for _, col := range w.Parameter.Columns {
		columns = append(columns, fmt.Sprintf("\"%s\"", col))
	}

	var sql string
	switch strings.ToLower(w.Parameter.WriteMode) {
	case "copy":
		sql = fmt.Sprintf("COPY %s.%s (%s) FROM STDIN",
			w.Parameter.Schema,
			w.Parameter.Table,
			strings.Join(columns, ","),
		)
	default:
		sql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ",
			w.Parameter.Schema,
			w.Parameter.Table,
			strings.Join(columns, ","),
		)
	}

	return sql
}

// buildUpdateSet 构建 UPDATE SET 子句
func buildUpdateSet(columns []string) string {
	var updates []string
	for _, col := range columns {
		if col != "id" {
			updates = append(updates, fmt.Sprintf("\"%s\" = EXCLUDED.\"%s\"", col, col))
		}
	}
	return strings.Join(updates, ", ")
}

// Close 关闭数据库连接
func (w *PostgreSQLWriter) Close() error {
	if w.tx != nil {
		w.RollbackTransaction()
	}
	if w.DB != nil {
		return w.DB.Close()
	}
	return nil
}

// StartTransaction 开始事务
func (w *PostgreSQLWriter) StartTransaction() error {
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	w.tx = tx
	return nil
}

// CommitTransaction 提交事务
func (w *PostgreSQLWriter) CommitTransaction() error {
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
func (w *PostgreSQLWriter) RollbackTransaction() error {
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
