package common

// ColumnProvider 提供实际列名的接口
type ColumnProvider interface {
	// GetActualColumns 获取实际的列名（忽略通配符）
	GetActualColumns() []string
}

// Reader 数据读取器接口
type Reader interface {
	// Connect 连接数据源
	Connect() error
	// Read 读取一批数据
	Read() ([][]any, error)
	// Close 关闭连接
	Close() error
	// GetTotalCount 获取总记录数
	GetTotalCount() (int64, error)
}

// ColumnAwareReader 支持列名感知的读取器
type ColumnAwareReader interface {
	Reader
	ColumnProvider
}

// Writer 数据写入器接口
type Writer interface {
	// Connect 连接数据目标
	Connect() error
	// Write 写入一批数据
	Write(records [][]any) error
	// Close 关闭连接
	Close() error
	// PreProcess 预处理
	PreProcess() error
	// PostProcess 后处理
	PostProcess() error
}

// ColumnAwareWriter 支持接收列名的写入器
type ColumnAwareWriter interface {
	Writer
	// SetColumns 设置要写入的列名
	SetColumns(columns []string)
}
