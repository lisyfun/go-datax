package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
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
	PreSQL    []string `json:"preSql"`
	PostSQL   []string `json:"postSql"`
	BatchSize int      `json:"batchSize"`
	WriteMode string   `json:"writeMode"`
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
func (w *MySQLWriter) PostProcess() error {
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
				var count int
				if err := rows.Scan(&count); err != nil {
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
	defer func() {
		// 恢复会话变量
		_, err := w.DB.Exec(`
			SET SESSION
			unique_checks = 1,
			foreign_key_checks = 1,
			sql_log_bin = 1
		`)
		if err != nil {
			log.Printf("恢复会话变量失败: %v", err)
		}
	}()

	// 计算每批次最大记录数，考虑MySQL占位符限制
	columnCount := len(w.Parameter.Columns)
	maxPlaceholders := 65535 // MySQL最大占位符数量
	maxBatchSize := maxPlaceholders / columnCount
	if maxBatchSize > w.Parameter.BatchSize {
		maxBatchSize = w.Parameter.BatchSize
	}

	// 计算总批次数
	totalBatches := (len(records) + maxBatchSize - 1) / maxBatchSize

	// 创建错误通道和完成通道
	errChan := make(chan error, totalBatches)
	doneChan := make(chan bool, totalBatches)
	stopChan := make(chan struct{}) // 用于通知所有工作协程停止
	defer close(stopChan)

	// 使用配置的channel数作为工作协程数
	workerCount := 24 // 与配置文件中的channel数保持一致
	workChan := make(chan [][]interface{}, workerCount)

	// 预先构建SQL模板
	insertPrefix := w.buildInsertPrefix()

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			// 每个工作协程创建自己的事务
			tx, err := w.DB.Begin()
			if err != nil {
				errChan <- fmt.Errorf("工作协程 %d 开始事务失败: %v", workerID, err)
				return
			}
			defer tx.Rollback() // 确保在出错时回滚

			// 预分配缓冲区
			var sqlBuilder strings.Builder
			sqlBuilder.Grow(1024 * 1024) // 预分配1MB的buffer
			valueArgs := make([]interface{}, 0, maxBatchSize*columnCount)

			batchCount := 0
			totalRecords := 0

			for {
				select {
				case <-stopChan:
					return
				case batch, ok := <-workChan:
					if !ok {
						// 提交最后的事务
						if batchCount > 0 {
							if err := tx.Commit(); err != nil {
								// 应该区分是否需要重试的错误
								if mysqlErr, ok := err.(*mysql.MySQLError); ok {
									switch mysqlErr.Number {
									case 1213: // 死锁
										errChan <- fmt.Errorf("发生死锁，需要重试: %v", err)
									case 1205: // 锁等待超时
										errChan <- fmt.Errorf("锁等待超时，需要重试: %v", err)
									}
								}
								return
							}
						}
						return
					}

					sqlBuilder.Reset()
					sqlBuilder.WriteString(insertPrefix)
					valueArgs = valueArgs[:0]

					// 构建批量插入值
					for i, record := range batch {
						if i > 0 {
							sqlBuilder.WriteByte(',')
						}
						sqlBuilder.WriteString("ROW(")
						for j := range w.Parameter.Columns {
							if j > 0 {
								sqlBuilder.WriteByte(',')
							}
							sqlBuilder.WriteByte('?')
							valueArgs = append(valueArgs, record[j])
						}
						sqlBuilder.WriteByte(')')
					}

					// 执行批量写入
					if _, err := tx.Exec(sqlBuilder.String(), valueArgs...); err != nil {
						errChan <- fmt.Errorf("工作协程 %d 执行写入失败: %v", workerID, err)
						return
					}

					totalRecords += len(batch)
					batchCount++

					// 每处理50个批次或累计100万条记录提交一次事务
					if batchCount >= 50 || totalRecords >= 1000000 {
						if err := tx.Commit(); err != nil {
							// 应该区分是否需要重试的错误
							if mysqlErr, ok := err.(*mysql.MySQLError); ok {
								switch mysqlErr.Number {
								case 1213: // 死锁
									errChan <- fmt.Errorf("发生死锁，需要重试: %v", err)
								case 1205: // 锁等待超时
									errChan <- fmt.Errorf("锁等待超时，需要重试: %v", err)
								}
							}
							return
						}
						// 开启新事务
						tx, err = w.DB.Begin()
						if err != nil {
							errChan <- fmt.Errorf("工作协程 %d 开始新事务失败: %v", workerID, err)
							return
						}
						batchCount = 0
						totalRecords = 0
					}

					doneChan <- true
				}
			}
		}(i)
	}

	// 分批发送数据到工作通道
	startTime := time.Now()
	lastLogTime := startTime

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
				// 每10秒输出一次进度日志
				now := time.Now()
				if now.Sub(lastLogTime) >= 10*time.Second {
					elapsed := now.Sub(startTime)
					speed := float64(i+maxBatchSize) / elapsed.Seconds()
					progress := float64(i+maxBatchSize) / float64(len(records)) * 100
					log.Printf("写入进度: %.2f%%, 速度: %.2f 条/秒", progress, speed)
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
	for completedBatches < totalBatches {
		select {
		case err := <-errChan:
			if firstErr == nil {
				firstErr = err
			}
			completedBatches++
		case <-doneChan:
			completedBatches++
		case <-timeout:
			return fmt.Errorf("数据同步超时")
		}
	}

	elapsed := time.Since(startTime)
	speed := float64(len(records)) / elapsed.Seconds()
	log.Printf("写入完成，总耗时: %.2f秒, 平均速度: %.2f 条/秒", elapsed.Seconds(), speed)

	if firstErr != nil {
		return firstErr
	}

	return nil
}

// Close 关闭数据库连接
func (w *MySQLWriter) Close() error {
	var errs []error

	if w.tx != nil {
		if err := w.RollbackTransaction(); err != nil {
			errs = append(errs, fmt.Errorf("回滚事务失败: %v", err))
		}
	}

	if w.DB != nil {
		if _, err := w.DB.Exec(`
			SET SESSION
			unique_checks = 1,
			foreign_key_checks = 1
		`); err != nil {
			errs = append(errs, fmt.Errorf("恢复会话变量失败: %v", err))
		}

		if err := w.DB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭数据库连接失败: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("关闭过程中发生多个错误: %v", errs)
	}
	return nil
}
