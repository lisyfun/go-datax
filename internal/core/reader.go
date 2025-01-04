package core

import "fmt"

// Reader 定义了数据读取器的接口
type Reader interface {
	// Connect 连接数据源
	Connect() error
	// Read 读取一批数据
	Read() ([]map[string]interface{}, error)
	// Close 关闭连接
	Close() error
	// GetTotalCount 获取总记录数
	GetTotalCount() (int64, error)
}

// ReaderFactory 定义了创建Reader的工厂函数类型
type ReaderFactory func(parameter interface{}) (Reader, error)

var readerFactories = make(map[string]ReaderFactory)

// RegisterReader 注册Reader工厂函数
func RegisterReader(name string, factory ReaderFactory) {
	readerFactories[name] = factory
}

// CreateReader 创建Reader实例
func CreateReader(name string, parameter interface{}) (Reader, error) {
	factory, ok := readerFactories[name]
	if !ok {
		return nil, fmt.Errorf("未知的Reader类型: %s", name)
	}
	return factory(parameter)
}
