package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Parameter MySQL读取器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Database  string   `json:"database"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Where     string   `json:"where"`
	SelectSQL string   `json:"selectSql"`
	BatchSize int      `json:"batchSize"`
}

// MySQLReader MySQL读取器结构体
type MySQLReader struct {
	Parameter *Parameter
	DB        *sql.DB
	offset    int // 用于记录当前读取位置
}

// NewMySQLReader 创建新的MySQL读取器实例
func NewMySQLReader(parameter *Parameter) *MySQLReader {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
	}

	return &MySQLReader{
		Parameter: parameter,
		offset:    0,
	}
}

// Connect 连接MySQL数据库
func (r *MySQLReader) Connect() error {
	// 在DSN中设置连接超时
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&timeout=30s&writeTimeout=30s&readTimeout=30s",
		r.Parameter.Username,
		r.Parameter.Password,
		r.Parameter.Host,
		r.Parameter.Port,
		r.Parameter.Database,
	)

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接MySQL失败: %v", err)
	}

	// 设置连接池配置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(20)
	db.SetConnMaxLifetime(time.Hour) // 设置连接最大生命周期为1小时
	db.SetConnMaxIdleTime(30 * time.Minute)

	// 创建一个带10秒超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用带超时的上下文测试连接
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("ping MySQL失败: %v", err)
	}

	r.DB = db
	return nil
}

// Read 读取数据
func (r *MySQLReader) Read() ([][]interface{}, error) {
	if r.DB == nil {
		return nil, fmt.Errorf("数据库连接未初始化")
	}

	query := r.buildQuery()
	log.Printf("执行查询: %s [offset=%d]", query, r.offset)

	// 记录首次查询的SQL语句
	if r.offset == 0 {
		log.Printf("执行数据读取的SQL: %s", query)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := r.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}

	// 准备数据容器
	var result [][]interface{}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// 读取数据
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Printf("扫描行数据失败: %v", err)
			continue
		}

		// 处理特殊类型
		row := make([]interface{}, len(columns))
		for i, val := range values {
			switch v := val.(type) {
			case []byte:
				row[i] = string(v)
			case time.Time:
				row[i] = v.Format("2006-01-02 15:04:05")
			case sql.NullString:
				row[i] = v.String
			case sql.NullInt64:
				row[i] = v.Int64
			case sql.NullFloat64:
				row[i] = v.Float64
			case sql.NullBool:
				row[i] = v.Bool
			case sql.NullTime:
				if v.Valid {
					row[i] = v.Time.Format("2006-01-02 15:04:05")
				} else {
					row[i] = nil
				}
			default:
				row[i] = v
			}
		}

		result = append(result, row)

		// 打印当前批次信息
		log.Printf("当前offset: %d, 本批次读取记录数: %d", r.offset, len(result))

		// 只有在实际读取到数据时才更新 offset
		if len(result) > 0 {
			r.offset += len(result)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("读取数据过程中发生错误: %v", err)
	}

	recordCount := len(result)
	log.Printf("查询结果: offset=%d, 记录数=%d", r.offset, recordCount)

	return result, nil
}

// buildQuery 构建SQL查询语句
func (r *MySQLReader) buildQuery() string {
	if r.Parameter.SelectSQL != "" {
		baseSQL := r.Parameter.SelectSQL

		// 检查是否同时存在 where 条件
		if r.Parameter.Where != "" {
			// 检查 selectSql 中是否已经包含 WHERE 子句
			whereIndex := strings.Index(strings.ToUpper(baseSQL), " WHERE ")
			if whereIndex == -1 {
				// 如果不包含 WHERE 子句，添加 where 条件
				baseSQL += " WHERE " + r.Parameter.Where
			} else {
				// 如果已包含 WHERE 子句，使用 AND 连接条件
				baseSQL = baseSQL[:whereIndex+7] + "(" + baseSQL[whereIndex+7:] + ") AND (" + r.Parameter.Where + ")"
			}
		}

		// 添加分页
		return fmt.Sprintf("%s LIMIT %d OFFSET %d",
			baseSQL,
			r.Parameter.BatchSize,
			r.offset,
		)
	}

	// 否则根据配置构建SQL
	columnsStr := "*"
	if len(r.Parameter.Columns) > 0 {
		columnsStr = fmt.Sprintf("`%s`", r.Parameter.Columns[0])
		for _, col := range r.Parameter.Columns[1:] {
			columnsStr += fmt.Sprintf(",`%s`", col)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", columnsStr, r.Parameter.Table)

	if r.Parameter.Where != "" {
		query += " WHERE " + r.Parameter.Where
	}

	// 添加分页
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", r.Parameter.BatchSize, r.offset)

	return query
}

// Close 关闭数据库连接
func (r *MySQLReader) Close() error {
	if r.DB != nil {
		return r.DB.Close()
	}
	return nil
}

// GetTotalCount 获取总记录数
func (r *MySQLReader) GetTotalCount() (int64, error) {
	if r.DB == nil {
		return 0, fmt.Errorf("数据库连接未初始化")
	}

	var query string
	if r.Parameter.SelectSQL != "" {
		// 预处理SQL语句：移除换行符和多余的空格
		baseSQL := strings.TrimSpace(r.Parameter.SelectSQL)
		baseSQL = strings.ReplaceAll(baseSQL, "\n", " ")
		baseSQL = strings.ReplaceAll(baseSQL, "\r", " ")

		// 处理 REPLACE INTO 或 INSERT INTO 语句
		if strings.HasPrefix(strings.ToUpper(baseSQL), "REPLACE INTO") ||
			strings.HasPrefix(strings.ToUpper(baseSQL), "INSERT INTO") {
			selectIndex := strings.Index(strings.ToUpper(baseSQL), "SELECT")
			if selectIndex == -1 {
				return 0, fmt.Errorf("无法从SQL语句中找到SELECT子句")
			}
			baseSQL = baseSQL[selectIndex:]
		}

		// 如果存在 where 条件，添加到 baseSQL 中
		if r.Parameter.Where != "" {
			// 检查 baseSQL 中是否已经包含 WHERE 子句
			whereIndex := strings.Index(strings.ToUpper(baseSQL), " WHERE ")
			if whereIndex == -1 {
				// 如果不包含 WHERE 子句，添加 where 条件
				baseSQL += " WHERE " + r.Parameter.Where
			} else {
				// 如果已包含 WHERE 子句，使用 AND 连接条件
				baseSQL = baseSQL[:whereIndex+7] + "(" + baseSQL[whereIndex+7:] + ") AND (" + r.Parameter.Where + ")"
			}
		}
		// 使用子查询方式获取总记录数
		query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS cnt", baseSQL)
	} else {
		// 构建基础查询
		baseSQL := fmt.Sprintf("SELECT * FROM `%s`", r.Parameter.Table)
		if r.Parameter.Where != "" {
			baseSQL += " WHERE " + r.Parameter.Where
		}
		// 使用子查询方式获取总记录数
		query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS cnt", baseSQL)
	}

	log.Printf("计算总记录数的SQL: %s", query)

	var count int64
	err := r.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取总记录数失败: %v", err)
	}

	return count, nil
}
