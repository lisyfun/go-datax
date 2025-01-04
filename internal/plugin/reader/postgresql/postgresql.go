package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

// Parameter PostgreSQL读取器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Database  string   `json:"database"`
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Where     string   `json:"where"`
	SelectSQL string   `json:"selectSql"`
	BatchSize int      `json:"batchSize"`
}

// PostgreSQLReader PostgreSQL读取器结构体
type PostgreSQLReader struct {
	Parameter *Parameter
	DB        *sql.DB
	offset    int // 当前读取位置
}

// NewPostgreSQLReader 创建新的PostgreSQL读取器实例
func NewPostgreSQLReader(parameter *Parameter) *PostgreSQLReader {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
	}
	if parameter.Schema == "" {
		parameter.Schema = "public"
	}

	return &PostgreSQLReader{
		Parameter: parameter,
		offset:    0,
	}
}

// Connect 连接PostgreSQL数据库
func (r *PostgreSQLReader) Connect() error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		r.Parameter.Host,
		r.Parameter.Port,
		r.Parameter.Username,
		r.Parameter.Password,
		r.Parameter.Database,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("连接PostgreSQL失败: %v", err)
	}

	// 设置连接池配置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(20)

	// 测试连接
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping PostgreSQL失败: %v", err)
	}

	r.DB = db
	return nil
}

// GetTotalCount 获取总记录数
func (r *PostgreSQLReader) GetTotalCount() (int64, error) {
	var query string
	if r.Parameter.SelectSQL != "" {
		query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) t", r.Parameter.SelectSQL)
	} else {
		whereClause := ""
		if r.Parameter.Where != "" {
			whereClause = "WHERE " + r.Parameter.Where
		}
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s %s",
			r.Parameter.Schema,
			r.Parameter.Table,
			whereClause,
		)
	}

	var count int64
	err := r.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取总记录数失败: %v", err)
	}

	return count, nil
}

// Read 读取数据
func (r *PostgreSQLReader) Read() ([]map[string]interface{}, error) {
	// 获取主键或唯一索引字段
	orderByColumns, err := r.GetPrimaryKeyColumns()
	if err != nil {
		log.Printf("获取主键字段失败: %v，将使用默认排序", err)
		orderByColumns = []string{"id"}
	}

	// 构建查询SQL
	var query string
	if r.Parameter.SelectSQL != "" {
		// 在子查询中添加排序
		query = fmt.Sprintf("SELECT * FROM (%s) t ORDER BY %s LIMIT %d OFFSET %d",
			r.Parameter.SelectSQL,
			strings.Join(orderByColumns, ","),
			r.Parameter.BatchSize,
			r.offset,
		)
	} else {
		var columns []string
		if len(r.Parameter.Columns) > 0 {
			for _, col := range r.Parameter.Columns {
				columns = append(columns, fmt.Sprintf("%s", col))
			}
		} else {
			columns = append(columns, "*")
		}

		whereClause := ""
		if r.Parameter.Where != "" {
			whereClause = "WHERE " + r.Parameter.Where
		}

		query = fmt.Sprintf("SELECT %s FROM %s.%s %s ORDER BY %s LIMIT %d OFFSET %d",
			strings.Join(columns, ","),
			r.Parameter.Schema,
			r.Parameter.Table,
			whereClause,
			strings.Join(orderByColumns, ","),
			r.Parameter.BatchSize,
			r.offset,
		)
	}

	log.Printf("执行查询: %s", query)

	// 执行查询
	rows, err := r.DB.Query(query)
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
	var result []map[string]interface{}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// 读取数据
	count := 0
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}

		// 将数据转换为map
		record := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				record[col] = nil
				continue
			}

			// 根据需要处理特定类型的数据
			switch v := val.(type) {
			case []byte:
				record[col] = string(v)
			default:
				record[col] = v
			}
		}
		result = append(result, record)
		count++
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("读取数据过程中发生错误: %v", err)
	}

	// 更新偏移量
	r.offset += count
	log.Printf("读取了 %d 条记录，当前偏移量: %d", count, r.offset)

	return result, nil
}

// Close 关闭数据库连接
func (r *PostgreSQLReader) Close() error {
	if r.DB != nil {
		return r.DB.Close()
	}
	return nil
}

// GetPrimaryKeyColumns 获取表的主键字段
func (r *PostgreSQLReader) GetPrimaryKeyColumns() ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisprimary;
	`

	tableName := fmt.Sprintf("%s.%s", r.Parameter.Schema, r.Parameter.Table)
	rows, err := r.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取主键字段失败: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("读取主键字段失败: %v", err)
		}
		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("获取主键字段过程中发生错误: %v", err)
	}

	if len(columns) == 0 {
		// 如果没有主键，尝试获取第一个唯一索引
		query = `
			SELECT a.attname
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = $1::regclass
			AND i.indisunique
			ORDER BY i.indisprimary DESC, i.indisunique DESC
			LIMIT 1;
		`

		rows, err := r.DB.Query(query, tableName)
		if err != nil {
			return nil, fmt.Errorf("获取唯一索引字段失败: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				return nil, fmt.Errorf("读取唯一索引字段失败: %v", err)
			}
			columns = append(columns, column)
		}

		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("获取唯一索引字段过程中发生错误: %v", err)
		}
	}

	return columns, nil
}
