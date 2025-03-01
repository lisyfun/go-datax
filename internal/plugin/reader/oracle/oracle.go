package oracle

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// Parameter Oracle读取器参数结构体
type Parameter struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Service   string   `json:"service"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Where     string   `json:"where"`
	SelectSQL string   `json:"selectSql"`
	BatchSize int      `json:"batchSize"`
}

// OracleReader Oracle读取器结构体
type OracleReader struct {
	Parameter *Parameter
	DB        *sql.DB
	offset    int // 用于记录当前读取位置
}

// NewOracleReader 创建新的Oracle读取器实例
func NewOracleReader(parameter *Parameter) *OracleReader {
	// 设置默认值
	if parameter.BatchSize == 0 {
		parameter.BatchSize = 1000
	}

	return &OracleReader{
		Parameter: parameter,
		offset:    0,
	}
}

// Connect 连接Oracle数据库
func (r *OracleReader) Connect() error {
	// 尝试不同的连接选项
	urlOptions := map[string]string{
		"AUTH TYPE": "SYSDBA", // 添加SYSDBA认证
	}

	// 使用大写的服务名
	service := strings.ToUpper(r.Parameter.Service)

	connStr := go_ora.BuildUrl(
		r.Parameter.Host,
		r.Parameter.Port,
		service,
		r.Parameter.Username,
		r.Parameter.Password,
		urlOptions,
	)

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return fmt.Errorf("连接Oracle失败: %v", err)
	}

	// 设置连接池配置
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// 验证连接是否真正可用
	err = db.Ping()
	if err != nil {
		db.Close()
		return fmt.Errorf("验证连接失败: %v", err)
	}

	r.DB = db
	return nil
}

// Read 读取数据
func (r *OracleReader) Read() ([][]any, error) {
	if r.DB == nil {
		return nil, fmt.Errorf("数据库连接未初始化")
	}

	// 构建查询SQL
	query := r.buildQuery()

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
	var result [][]any
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))

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
		row := make([]any, len(columns))
		for i, val := range values {
			switch v := val.(type) {
			case []byte:
				row[i] = string(v)
			case time.Time:
				row[i] = v.Format("2006-01-02 15:04:05")
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
func (r *OracleReader) buildQuery() string {
	if r.Parameter.SelectSQL != "" {
		// Oracle 分页查询
		return fmt.Sprintf(`
			SELECT * FROM (
				SELECT a.*, ROWNUM rnum FROM (
					%s
				) a WHERE ROWNUM <= %d
			) WHERE rnum > %d`,
			r.Parameter.SelectSQL,
			r.offset+r.Parameter.BatchSize,
			r.offset,
		)
	}

	// 否则根据配置构建SQL
	columnsStr := "*"
	if len(r.Parameter.Columns) > 0 {
		columnsStr = fmt.Sprintf(`"%s"`, r.Parameter.Columns[0])
		for _, col := range r.Parameter.Columns[1:] {
			columnsStr += fmt.Sprintf(`,"%s"`, col)
		}
	}

	baseQuery := fmt.Sprintf(`SELECT %s FROM "%s"`, columnsStr, r.Parameter.Table)

	if r.Parameter.Where != "" {
		baseQuery += " WHERE " + r.Parameter.Where
	}

	// Oracle 分页查询
	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT a.*, ROWNUM rnum FROM (
				%s
			) a WHERE ROWNUM <= %d
		) WHERE rnum > %d`,
		columnsStr,
		baseQuery,
		r.offset+r.Parameter.BatchSize,
		r.offset,
	)
}

// Close 关闭数据库连接
func (r *OracleReader) Close() error {
	if r.DB != nil {
		return r.DB.Close()
	}
	return nil
}

// GetTotalCount 获取总记录数
func (r *OracleReader) GetTotalCount() (int64, error) {
	if r.DB == nil {
		return 0, fmt.Errorf("数据库连接未初始化")
	}

	var query string
	if r.Parameter.Where != "" {
		query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE %s`,
			r.Parameter.Table,
			r.Parameter.Where,
		)
	} else {
		query = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, r.Parameter.Table)
	}

	var count int64
	err := r.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取总记录数失败: %v", err)
	}

	return count, nil
}
