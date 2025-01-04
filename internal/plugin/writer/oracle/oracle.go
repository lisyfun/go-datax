package oracle

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Parameter Oracle写入器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Service   string   `json:"service"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	BatchSize int      `json:"batchSize"`
	PreSQL    string   `json:"preSql"`
	PostSQL   string   `json:"postSql"`
}

// OracleWriter Oracle写入器结构体
type OracleWriter struct {
	Parameter *Parameter
	DB        *sql.DB
}

// NewOracleWriter 创建新的Oracle写入器实例
func NewOracleWriter(parameter *Parameter) *OracleWriter {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
	}

	return &OracleWriter{
		Parameter: parameter,
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

	// 设置连接池配置
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// 测试连接
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping Oracle失败: %v", err)
	}

	w.DB = db
	return nil
}

// PreProcess 执行写入前的SQL操作
func (w *OracleWriter) PreProcess() error {
	if w.Parameter.PreSQL == "" {
		return nil
	}

	_, err := w.DB.Exec(w.Parameter.PreSQL)
	if err != nil {
		return fmt.Errorf("执行PreSQL失败: %v", err)
	}

	return nil
}

// PostProcess 执行写入后的SQL操作
func (w *OracleWriter) PostProcess() error {
	if w.Parameter.PostSQL == "" {
		return nil
	}

	_, err := w.DB.Exec(w.Parameter.PostSQL)
	if err != nil {
		return fmt.Errorf("执行PostSQL失败: %v", err)
	}

	return nil
}

// Write 写入数据
func (w *OracleWriter) Write(records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// 开启事务
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("开启事务失败: %v", err)
	}

	// 构建插入SQL
	columns := w.Parameter.Columns
	if len(columns) == 0 {
		// 如果没有指定列，使用第一条记录的所有列
		for col := range records[0] {
			columns = append(columns, col)
		}
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" ("%s") VALUES (%s)`,
		w.Parameter.Table,
		strings.Join(columns, `","`),
		strings.Join(placeholders, ","),
	)

	// 准备语句
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("准备SQL语句失败: %v", err)
	}
	defer stmt.Close()

	// 批量插入数据
	for _, record := range records {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = record[col]
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("插入数据失败: %v", err)
		}
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	return nil
}

// Close 关闭数据库连接
func (w *OracleWriter) Close() error {
	if w.DB != nil {
		return w.DB.Close()
	}
	return nil
}
