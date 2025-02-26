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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		r.Parameter.Username,
		r.Parameter.Password,
		r.Parameter.Host,
		r.Parameter.Port,
		r.Parameter.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接MySQL失败: %v", err)
	}

	// 设置连接池配置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(20)
	db.SetConnMaxIdleTime(30 * time.Minute)

	// 测试连接
	err = db.Ping()
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

	// 构建查询SQL
	query := r.buildQuery()

	// 记录首次查询的SQL语句
	if r.offset == 0 {
		log.Printf("执行数据读取的SQL: %s", query)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 执行查询
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
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("读取数据过程中发生错误: %v", err)
	}

	// 更新偏移量
	r.offset += len(result)

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
		// 如果配置了完整的SQL语句，需要从中提取条件
		baseSQL := r.Parameter.SelectSQL

		// 提取FROM子句
		fromIndex := strings.Index(strings.ToUpper(baseSQL), " FROM ")
		if fromIndex == -1 {
			return 0, fmt.Errorf("无法从selectSql中解析FROM子句")
		}

		// 提取表名和条件
		fromClause := baseSQL[fromIndex:]

		// 检查是否同时存在 where 条件
		if r.Parameter.Where != "" {
			// 检查 fromClause 中是否已经包含 WHERE 子句
			whereIndex := strings.Index(strings.ToUpper(fromClause), " WHERE ")
			if whereIndex == -1 {
				// 如果不包含 WHERE 子句，添加 where 条件
				fromClause += " WHERE " + r.Parameter.Where
			} else {
				// 如果已包含 WHERE 子句，使用 AND 连接条件
				fromClause = fromClause[:whereIndex+7] + "(" + fromClause[whereIndex+7:] + ") AND (" + r.Parameter.Where + ")"
			}
		}

		// 移除可能存在的ORDER BY, LIMIT等子句
		orderByIndex := strings.Index(strings.ToUpper(fromClause), " ORDER BY ")
		limitIndex := strings.Index(strings.ToUpper(fromClause), " LIMIT ")
		groupByIndex := strings.Index(strings.ToUpper(fromClause), " GROUP BY ")

		if orderByIndex != -1 {
			fromClause = fromClause[:orderByIndex]
		} else if limitIndex != -1 {
			fromClause = fromClause[:limitIndex]
		} else if groupByIndex != -1 {
			fromClause = fromClause[:groupByIndex]
		}

		query = "SELECT COUNT(*)" + fromClause
	} else if r.Parameter.Where != "" {
		query = fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE %s",
			r.Parameter.Table,
			r.Parameter.Where,
		)
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", r.Parameter.Table)
	}

	log.Printf("计算总记录数的SQL: %s", query)

	var count int64
	err := r.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取总记录数失败: %v", err)
	}

	return count, nil
}
