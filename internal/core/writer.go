package core

import "fmt"

// Writer 定义了数据写入器的接口
type Writer interface {
	// Connect 连接目标数据源
	Connect() error
	// PreProcess 写入前的预处理
	PreProcess() error
	// StartTransaction 开始事务
	StartTransaction() error
	// Write 写入一批数据
	Write(records []map[string]interface{}) error
	// CommitTransaction 提交事务
	CommitTransaction() error
	// RollbackTransaction 回滚事务
	RollbackTransaction() error
	// PostProcess 写入后的后处理
	PostProcess() error
	// Close 关闭连接
	Close() error
}

// WriterFactory 定义了创建Writer的工厂函数类型
type WriterFactory func(parameter interface{}) (Writer, error)

var writerFactories = make(map[string]WriterFactory)

// RegisterWriter 注册Writer工厂函数
func RegisterWriter(name string, factory WriterFactory) {
	writerFactories[name] = factory
}

// CreateWriter 创建Writer实例
func CreateWriter(name string, parameter interface{}) (Writer, error) {
	factory, ok := writerFactories[name]
	if !ok {
		return nil, fmt.Errorf("未知的Writer类型: %s", name)
	}
	return factory(parameter)
}
