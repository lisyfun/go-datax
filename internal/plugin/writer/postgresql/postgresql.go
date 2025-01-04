package postgresql

import (
	"database/sql"
	"fmt"
	"log"
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
		log.Printf("执行预处理SQL: %s", sql)

		// 对于SELECT语句使用Query，其他语句使用Exec
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询预处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			// 获取列信息
			columns, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("获取列信息失败: %v", err)
			}

			if rows.Next() {
				// 创建一个切片来存储所有列的值
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("读取预处理SQL结果失败: %v", err)
				}

				// 打印结果
				result := make([]string, len(columns))
				for i, val := range values {
					if val == nil {
						result[i] = "NULL"
					} else {
						result[i] = fmt.Sprintf("%v", val)
					}
				}
				log.Printf("预处理SQL执行结果: %s", strings.Join(result, ", "))
			}
		} else {
			result, err := w.DB.Exec(sql)
			if err != nil {
				return fmt.Errorf("执行预处理SQL失败: %v", err)
			}
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
func (w *PostgreSQLWriter) PostProcess() error {
	if len(w.Parameter.PostSQL) == 0 {
		return nil
	}

	for _, sql := range w.Parameter.PostSQL {
		log.Printf("执行后处理SQL: %s", sql)

		// 对于SELECT语句使用Query，其他语句使用Exec
		if strings.Contains(strings.ToLower(sql), "select") {
			rows, err := w.DB.Query(sql)
			if err != nil {
				return fmt.Errorf("查询后处理SQL结果失败: %v", err)
			}
			defer rows.Close()

			// 获取列信息
			columns, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("获取列信息失败: %v", err)
			}

			if rows.Next() {
				// 创建一个切片来存储所有列的值
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("读取后处理SQL结果失败: %v", err)
				}

				// 打印结果
				result := make([]string, len(columns))
				for i, val := range values {
					if val == nil {
						result[i] = "NULL"
					} else {
						// 检查是否是时间间隔类型
						if interval, ok := val.([]uint8); ok && strings.Contains(strings.ToLower(columns[i]), "time_span") {
							// 将字节数组转换为字符串
							result[i] = string(interval)
						} else {
							result[i] = fmt.Sprintf("%v", val)
						}
					}
				}
				log.Printf("后处理SQL执行结果: %s", strings.Join(result, ", "))
			}
		} else {
			result, err := w.DB.Exec(sql)
			if err != nil {
				return fmt.Errorf("执行后处理SQL失败: %v", err)
			}
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

// Write 写入数据
func (w *PostgreSQLWriter) Write(records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// 计算每批次最大记录数
	maxBatchSize := w.Parameter.BatchSize
	totalBatches := (len(records) + maxBatchSize - 1) / maxBatchSize
	log.Printf("总记录数: %d, 每批次记录数: %d, 总批次数: %d", len(records), maxBatchSize, totalBatches)

	// 创建错误通道和完成通道
	errChan := make(chan error, totalBatches)
	doneChan := make(chan bool, totalBatches)
	stopChan := make(chan struct{}) // 用于通知所有工作协程停止
	defer close(stopChan)

	// 使用配置的channel数作为工作协程数
	workerCount := 8                                               // 减少工作协程数，避免过多并发
	workChan := make(chan []map[string]interface{}, workerCount*2) // 增加工作通道缓冲区

	// 预先构建SQL模板
	insertPrefix := w.buildInsertPrefix()

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case <-stopChan:
					return
				case batch, ok := <-workChan:
					if !ok {
						return
					}

					// 每个批次使用独立的事务
					tx, err := w.DB.Begin()
					if err != nil {
						errChan <- fmt.Errorf("工作协程 %d 开始事务失败: %v", workerID, err)
						continue
					}

					// 构建批量插入值
					var values []string
					valueArgs := make([]interface{}, 0, len(batch)*len(w.Parameter.Columns))
					paramCount := 1

					for _, record := range batch {
						var placeholders []string
						for _, col := range w.Parameter.Columns {
							placeholders = append(placeholders, fmt.Sprintf("$%d", paramCount))
							paramCount++
							valueArgs = append(valueArgs, record[col])
						}
						values = append(values, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
					}

					// 构建完整的SQL语句
					finalSQL := insertPrefix + strings.Join(values, ",")
					if w.Parameter.WriteMode == "replace" {
						finalSQL += " ON CONFLICT (\"id\") DO UPDATE SET " + buildUpdateSet(w.Parameter.Columns)
					}

					// 执行批量写入
					if _, err := tx.Exec(finalSQL, valueArgs...); err != nil {
						tx.Rollback()
						errChan <- fmt.Errorf("工作协程 %d 执行写入失败: %v", workerID, err)
						continue
					}

					// 提交事务
					if err := tx.Commit(); err != nil {
						errChan <- fmt.Errorf("工作协程 %d 提交事务失败: %v", workerID, err)
						continue
					}

					doneChan <- true
				}
			}
		}(i)
	}

	// 分批发送数据到工作通道
	startTime := time.Now()
	lastLogTime := startTime
	processedRecords := 0

	// 使用超时控制
	timeout := time.After(30 * time.Minute) // 设置30分钟超时

	go func() {
		defer close(workChan)
		for i := 0; i < len(records); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(records) {
				end = len(records)
			}

			select {
			case <-stopChan:
				return
			case workChan <- records[i:end]:
				processedRecords = i + maxBatchSize
				// 每秒最多输出一次进度日志
				now := time.Now()
				if now.Sub(lastLogTime) >= time.Second {
					elapsed := now.Sub(startTime)
					speed := float64(processedRecords) / elapsed.Seconds()
					progress := float64(processedRecords) / float64(len(records)) * 100
					log.Printf("进度: %.2f%%, 已处理: %d/%d, 速度: %.2f 条/秒",
						progress, processedRecords, len(records), speed)
					lastLogTime = now
				}
			case <-timeout:
				log.Printf("数据同步超时")
				return
			}
		}
	}()

	// 等待所有批次处理完成
	var firstErr error
	completedBatches := 0
	errorCount := 0

	for completedBatches < totalBatches {
		select {
		case err := <-errChan:
			if firstErr == nil {
				firstErr = err
			}
			errorCount++
			completedBatches++
			log.Printf("发生错误: %v", err)
		case <-doneChan:
			completedBatches++
		case <-timeout:
			return fmt.Errorf("数据同步超时")
		}
	}

	elapsed := time.Since(startTime)
	speed := float64(len(records)) / elapsed.Seconds()
	log.Printf("数据同步完成! 总耗时: %v, 处理记录数: %d, 错误记录数: %d, 平均速度: %.2f 条/秒",
		elapsed, len(records), errorCount, speed)

	if firstErr != nil {
		return firstErr
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
