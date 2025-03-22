package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"datax/internal/pkg/logger"
	"datax/internal/plugin/common"

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
	LogLevel  int      `json:"logLevel"` // 日志级别
}

// 批次大小的常量定义
const (
	DefaultBatchSize = 20000 // 默认批次大小
	MinBatchSize     = 1000  // 最小批次大小
	MaxBatchSize     = 50000 // 最大批次大小
)

// MySQLReader MySQL读取器结构体
type MySQLReader struct {
	Parameter *Parameter
	DB        *sql.DB
	offset    int // 用于记录当前读取位置
	logger    *logger.Logger
}

// Ensure MySQLReader implements ColumnAwareReader
var _ common.ColumnAwareReader = (*MySQLReader)(nil)

// NewMySQLReader 创建新的MySQL读取器实例
func NewMySQLReader(parameter *Parameter) *MySQLReader {
	// 设置默认批次大小
	if parameter.BatchSize == 0 {
		parameter.BatchSize = DefaultBatchSize
	}

	// 确保批次大小在合理范围内
	if parameter.BatchSize < MinBatchSize {
		parameter.BatchSize = MinBatchSize
	}
	if parameter.BatchSize > MaxBatchSize {
		parameter.BatchSize = MaxBatchSize
	}

	// 创建日志记录器
	l := logger.New(&logger.Option{
		Level:     logger.Level(parameter.LogLevel),
		Prefix:    "MySQLReader",
		WithTime:  true,
		WithLevel: true,
	})

	return &MySQLReader{
		Parameter: parameter,
		offset:    0,
		logger:    l,
	}
}

// Connect 连接MySQL数据库
func (r *MySQLReader) Connect() error {
	// 在DSN中设置连接超时
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&timeout=10s&writeTimeout=30s&readTimeout=30s",
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

// getTableColumns 获取表的所有列名
func (r *MySQLReader) getTableColumns() ([]string, error) {
	// 构建查询列名的SQL
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
		r.Parameter.Database,
		r.Parameter.Table)

	// 执行查询
	rows, err := r.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询表结构失败: %v", err)
	}
	defer rows.Close()

	// 收集列名
	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("读取列名失败: %v", err)
		}
		columns = append(columns, columnName)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列名结果集失败: %v", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("表 %s.%s 未找到任何列", r.Parameter.Database, r.Parameter.Table)
	}

	return columns, nil
}

// Read 读取数据
func (r *MySQLReader) Read() ([][]any, error) {
	if r.DB == nil {
		return nil, fmt.Errorf("数据库连接未初始化")
	}

	// 如果使用了星号，则需要获取实际的列名
	hasAsterisk := false
	for _, col := range r.Parameter.Columns {
		if col == "*" {
			hasAsterisk = true
			break
		}
	}

	// 如果使用了星号且是第一次读取(offset为0)，则获取实际列名
	if hasAsterisk && r.offset == 0 {
		columns, err := r.getTableColumns()
		if err != nil {
			r.logger.Warn("获取表结构失败: %v，将继续使用星号查询", err)
		} else {
			// 保存实际列名，但还是使用星号查询（保持兼容性）
			r.logger.Info("检测到通配符*，已获取表%s的全部字段: %v", r.Parameter.Table, strings.Join(columns, ", "))
			// 更新Parameter中的列名，但仍使用*作为查询
			r.Parameter.Columns = append([]string{"*"}, columns...)
		}
	}

	query := r.buildQuery()
	r.logger.Info("执行查询: %s [offset=%d, batchSize=%d]", query, r.offset, r.Parameter.BatchSize)

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

	// 如果是星号查询的第一次读取，更新Parameter中的实际列名（如果未通过信息模式获取）
	if hasAsterisk && r.offset == 0 && len(r.Parameter.Columns) <= 1 {
		r.Parameter.Columns = append([]string{"*"}, columns...)
		r.logger.Info("从查询结果更新获取表%s的全部字段: %v", r.Parameter.Table, strings.Join(columns, ", "))
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
			r.logger.Error("扫描行数据失败: %v", err)
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

	recordCount := len(result)
	r.logger.Info("成功读取 %d 条记录", recordCount)
	r.offset += recordCount

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
		// 检查是否包含星号
		hasAsterisk := false
		for _, col := range r.Parameter.Columns {
			if col == "*" {
				hasAsterisk = true
				break
			}
		}

		// 如果包含星号，直接使用*
		if hasAsterisk {
			columnsStr = "*"
		} else {
			columnsStr = fmt.Sprintf("`%s`", r.Parameter.Columns[0])
			for _, col := range r.Parameter.Columns[1:] {
				columnsStr += fmt.Sprintf(",`%s`", col)
			}
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

// GetTotalCount 获取总记录数并动态调整批次大小
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
			whereIndex := strings.Index(strings.ToUpper(baseSQL), " WHERE ")
			if whereIndex == -1 {
				baseSQL += " WHERE " + r.Parameter.Where
			} else {
				baseSQL = baseSQL[:whereIndex+7] + "(" + baseSQL[whereIndex+7:] + ") AND (" + r.Parameter.Where + ")"
			}
		}
		query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS cnt", baseSQL)
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", r.Parameter.Table)
		if r.Parameter.Where != "" {
			query += " WHERE " + r.Parameter.Where
		}
	}

	r.logger.Info("计算总记录数的SQL: %s", query)

	var totalCount int64
	err := r.DB.QueryRow(query).Scan(&totalCount)
	if err != nil {
		return 0, fmt.Errorf("获取总记录数失败: %v", err)
	}

	// 如果批次大小过大或过小，才进行调整
	// 避免不必要的批次大小调整，减少日志输出
	if totalCount > 0 && (r.Parameter.BatchSize > MaxBatchSize ||
		r.Parameter.BatchSize < MinBatchSize ||
		(totalCount < int64(r.Parameter.BatchSize) && totalCount > MinBatchSize)) {

		originalBatchSize := r.Parameter.BatchSize
		optimalBatchSize := calculateOptimalBatchSize(totalCount)

		// 只有当计算出的最优批次大小与当前批次大小差异较大时才调整
		// 差异阈值定为20%
		if optimalBatchSize > int(float64(originalBatchSize)*1.2) ||
			optimalBatchSize < int(float64(originalBatchSize)*0.8) {
			r.Parameter.BatchSize = optimalBatchSize
			r.logger.Info("根据数据量(%d)调整批次大小: %d -> %d",
				totalCount,
				originalBatchSize,
				r.Parameter.BatchSize,
			)
		}
	}

	return totalCount, nil
}

// calculateOptimalBatchSize 根据总记录数计算最优批次大小
func calculateOptimalBatchSize(totalCount int64) int {
	// 如果总记录数小于最小批次，直接返回总记录数
	if totalCount < MinBatchSize {
		return int(totalCount)
	}

	// 计算建议的批次数量（目标是将数据分成10-20个批次）
	suggestedBatches := 15

	// 计算每批次的记录数
	batchSize := int(totalCount) / suggestedBatches

	// 根据数据量范围调整批次大小
	switch {
	case totalCount <= 10000:
		// 数据量较小，使用较小的批次
		batchSize = MinBatchSize
	case totalCount <= 100000:
		// 中等数据量，批次大小在5000-10000之间
		batchSize = max(5000, min(batchSize, 10000))
	case totalCount <= 1000000:
		// 大数据量，批次大小在10000-30000之间
		batchSize = max(10000, min(batchSize, 30000))
	default:
		// 超大数据量，批次大小在20000-50000之间
		batchSize = max(20000, min(batchSize, MaxBatchSize))
	}

	// 确保批次大小在合理范围内
	if batchSize < MinBatchSize {
		return MinBatchSize
	}
	if batchSize > MaxBatchSize {
		return MaxBatchSize
	}

	return batchSize
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GetActualColumns 获取实际使用的列名（忽略星号）
func (r *MySQLReader) GetActualColumns() []string {
	// 如果columns包含星号，返回第二个元素开始的所有元素（这些是实际列名）
	for i, col := range r.Parameter.Columns {
		if col == "*" && len(r.Parameter.Columns) > i+1 {
			return r.Parameter.Columns[i+1:]
		}
	}

	// 如果没有星号或者没有保存实际列名，直接返回当前的columns
	return r.Parameter.Columns
}
