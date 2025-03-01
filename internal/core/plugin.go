package core

import (
	"sync"
)

// Reader 数据读取器接口
type Reader interface {
	Connect() error
	Read() ([][]any, error)
	Close() error
	GetTotalCount() (int64, error)
}

// Writer 数据写入器接口
type Writer interface {
	Connect() error
	Write(records [][]any) error
	Close() error
	PreProcess() error
	PostProcess() error
}

// ReaderFactory Reader工厂函数类型
type ReaderFactory func(parameter any) (Reader, error)

// WriterFactory Writer工厂函数类型
type WriterFactory func(parameter any) (Writer, error)

var (
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex    sync.RWMutex
)

// RegisterReader 注册Reader工厂函数
func RegisterReader(name string, factory ReaderFactory) {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	readerFactories[name] = factory
}

// RegisterWriter 注册Writer工厂函数
func RegisterWriter(name string, factory WriterFactory) {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	writerFactories[name] = factory
}

// JobConfig 任务配置结构体
type JobConfig struct {
	Job struct {
		Content []struct {
			Reader struct {
				Name      string `json:"name"`
				Parameter any    `json:"parameter"`
			} `json:"reader"`
			Writer struct {
				Name      string `json:"name"`
				Parameter any    `json:"parameter"`
			} `json:"writer"`
		} `json:"content"`
		Setting struct {
			Speed struct {
				Channel int `json:"channel"`
				Bytes   int `json:"bytes"`
			} `json:"speed"`
			ErrorLimit struct {
				Record     int     `json:"record"`
				Percentage float64 `json:"percentage"`
			} `json:"errorLimit"`
		} `json:"setting"`
	} `json:"job"`
}
