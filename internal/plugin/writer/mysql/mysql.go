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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
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

	// 设置连接池配置
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// 测试连接
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping MySQL失败: %v", err)
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
	if len(records) == 0 {
		return nil
	}

	// 计算每批次最大记录数（考虑到MySQL占位符限制）
	maxBatchSize := 65000 / len(w.Parameter.Columns)
	if maxBatchSize > w.Parameter.BatchSize {
		maxBatchSize = w.Parameter.BatchSize
	}

	totalBatches := (len(records) + maxBatchSize - 1) / maxBatchSize
	log.Printf("总记录数: %d, 每批次记录数: %d, 总批次数: %d", len(records), maxBatchSize, totalBatches)

	// 创建错误通道和完成通道
	errChan := make(chan error, totalBatches)
	doneChan := make(chan bool, totalBatches)

	// 创建工作通道，限制并发数
	workerCount := 10 // 设置合适的并发数
	workChan := make(chan []map[string]interface{}, workerCount)

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		go func() {
			for batch := range workChan {
				if err := w.writeBatch(batch); err != nil {
					errChan <- err
				} else {
					doneChan <- true
				}
			}
		}()
	}

	// 分批发送数据到工作通道
	go func() {
		for i := 0; i < len(records); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(records) {
				end = len(records)
			}
			currentBatch := (i / maxBatchSize) + 1
			log.Printf("正在处理第 %d/%d 批, 记录数: %d", currentBatch, totalBatches, end-i)

			batch := records[i:end]
			workChan <- batch
		}
		close(workChan)
	}()

	// 等待所有批次处理完成
	var firstErr error
	completedBatches := 0
	for completedBatches < totalBatches {
		select {
		case err := <-errChan:
			if firstErr == nil {
				firstErr = err
			}
			completedBatches++
		case <-doneChan:
			completedBatches++
			log.Printf("完成批次: %d/%d", completedBatches, totalBatches)
		}
	}

	if firstErr != nil {
		return firstErr
	}

	log.Printf("所有批次执行完成，共写入 %d 条记录", len(records))
	return nil
}

// writeBatch 写入一批数据
func (w *MySQLWriter) writeBatch(records []map[string]interface{}) error {
	// 构建插入SQL
	query := w.buildInsertSQL()

	// 准备值
	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]interface{}, 0, len(records)*len(w.Parameter.Columns))

	for _, record := range records {
		placeholders := make([]string, len(w.Parameter.Columns))
		for i := range w.Parameter.Columns {
			placeholders[i] = "?"
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ",")+")")

		for _, col := range w.Parameter.Columns {
			valueArgs = append(valueArgs, record[col])
		}
	}

	// 完成SQL语句
	query = fmt.Sprintf(query, strings.Join(valueStrings, ","))

	// 执行写入
	var err error
	if w.tx != nil {
		_, err = w.tx.Exec(query, valueArgs...)
	} else {
		_, err = w.DB.Exec(query, valueArgs...)
	}

	if err != nil {
		return fmt.Errorf("执行写入失败: %v", err)
	}

	return nil
}

// buildInsertSQL 构建插入SQL语句
func (w *MySQLWriter) buildInsertSQL() string {
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

	return fmt.Sprintf("%s INTO `%s` (%s) VALUES %%s",
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
	if w.DB != nil {
		return w.DB.Close()
	}
	return nil
}
