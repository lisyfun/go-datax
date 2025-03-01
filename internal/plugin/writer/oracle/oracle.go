package oracle

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

// Parameter Oracle写入器参数
type Parameter struct {
	Username   string   `json:"username"`
	Password   string   `json:"password"`
	Host       string   `json:"host"`
	Port       int      `json:"port"`
	Service    string   `json:"service"`
	Table      string   `json:"table"`
	Schema     string   `json:"schema"`
	Columns    []string `json:"columns"`
	WriteMode  string   `json:"write_mode"`
	BatchSize  int      `json:"batch_size"`
	Concurrent int      `json:"concurrent"`
	PreSQL     string   `json:"pre_sql"`
	PostSQL    string   `json:"post_sql"`
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
		log.Println("没有配置预处理SQL语句")
		return nil
	}

	log.Printf("执行预处理SQL: %s", w.Parameter.PreSQL)
	startTime := time.Now()

	result, err := w.DB.Exec(w.Parameter.PreSQL)
	if err != nil {
		log.Printf("执行预处理SQL失败: %v", err)
		return fmt.Errorf("执行PreSQL失败: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("预处理SQL执行成功, 影响行数: %d, 耗时: %v", rowsAffected, time.Since(startTime))

	return nil
}

// PostProcess 执行写入后的SQL操作
func (w *OracleWriter) PostProcess() error {
	if w.Parameter.PostSQL == "" {
		log.Println("没有配置后处理SQL语句")
		return nil
	}

	log.Printf("执行后处理SQL: %s", w.Parameter.PostSQL)
	startTime := time.Now()

	result, err := w.DB.Exec(w.Parameter.PostSQL)
	if err != nil {
		log.Printf("执行后处理SQL失败: %v", err)
		return fmt.Errorf("执行PostSQL失败: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("后处理SQL执行成功, 影响行数: %d, 耗时: %v", rowsAffected, time.Since(startTime))

	return nil
}

// Write 写入数据
func (w *OracleWriter) Write(records [][]any) error {
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
		placeholders = append(placeholders, fmt.Sprintf(":%d", i+1))
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

// Init 初始化Oracle写入器
func (w *OracleWriter) Init() error {
	if w.Parameter == nil {
		return fmt.Errorf("参数不能为空")
	}

	// 验证必填参数
	if w.Parameter.Username == "" {
		return fmt.Errorf("用户名不能为空")
	}
	if w.Parameter.Password == "" {
		return fmt.Errorf("密码不能为空")
	}
	if w.Parameter.Host == "" {
		return fmt.Errorf("主机不能为空")
	}
	if w.Parameter.Port == 0 {
		return fmt.Errorf("端口不能为空")
	}
	if w.Parameter.Service == "" {
		return fmt.Errorf("服务名不能为空")
	}
	if w.Parameter.Table == "" {
		return fmt.Errorf("表名不能为空")
	}

	// 设置默认值
	if w.Parameter.BatchSize == 0 {
		w.Parameter.BatchSize = 1000
	}
	if w.Parameter.Concurrent == 0 {
		w.Parameter.Concurrent = 1
	}

	// 构建连接字符串
	dsn := fmt.Sprintf(
		"oracle://%s:%s@%s:%d/%s",
		w.Parameter.Username,
		w.Parameter.Password,
		w.Parameter.Host,
		w.Parameter.Port,
		w.Parameter.Service,
	)

	// 连接数据库
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}
	w.DB = db

	// 执行PreSQL
	if w.Parameter.PreSQL != "" {
		if _, err := w.DB.Exec(w.Parameter.PreSQL); err != nil {
			return fmt.Errorf("执行PreSQL失败: %v", err)
		}
	}

	return nil
}

// Close 关闭Oracle写入器
func (w *OracleWriter) Close() error {
	if w.DB != nil {
		// 执行PostSQL
		if w.Parameter.PostSQL != "" {
			if _, err := w.DB.Exec(w.Parameter.PostSQL); err != nil {
				return fmt.Errorf("执行PostSQL失败: %v", err)
			}
		}

		// 关闭数据库连接
		if err := w.DB.Close(); err != nil {
			return fmt.Errorf("关闭数据库连接失败: %v", err)
		}
	}
	return nil
}
