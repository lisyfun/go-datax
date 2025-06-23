package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

// PluginRegistry 插件注册器
type PluginRegistry struct {
	readers map[string]ReaderFactory
	writers map[string]WriterFactory
	mutex   sync.RWMutex
}

// NewPluginRegistry 创建新的插件注册器
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		readers: make(map[string]ReaderFactory),
		writers: make(map[string]WriterFactory),
	}
}

// RegisterReader 注册Reader插件
func (r *PluginRegistry) RegisterReader(name string, factory ReaderFactory) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.readers[name] = factory
}

// RegisterWriter 注册Writer插件
func (r *PluginRegistry) RegisterWriter(name string, factory WriterFactory) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.writers[name] = factory
}

// CreateReader 创建Reader实例
func (r *PluginRegistry) CreateReader(name string, parameter any) (Reader, error) {
	r.mutex.RLock()
	factory, exists := r.readers[name]
	r.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("未找到Reader插件: %s", name)
	}
	
	return factory(parameter)
}

// CreateWriter 创建Writer实例
func (r *PluginRegistry) CreateWriter(name string, parameter any) (Writer, error) {
	r.mutex.RLock()
	factory, exists := r.writers[name]
	r.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("未找到Writer插件: %s", name)
	}
	
	return factory(parameter)
}

// GetRegisteredReaders 获取已注册的Reader插件名称列表
func (r *PluginRegistry) GetRegisteredReaders() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	names := make([]string, 0, len(r.readers))
	for name := range r.readers {
		names = append(names, name)
	}
	return names
}

// GetRegisteredWriters 获取已注册的Writer插件名称列表
func (r *PluginRegistry) GetRegisteredWriters() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	names := make([]string, 0, len(r.writers))
	for name := range r.writers {
		names = append(names, name)
	}
	return names
}

// HasReader 检查是否存在指定的Reader插件
func (r *PluginRegistry) HasReader(name string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	_, exists := r.readers[name]
	return exists
}

// HasWriter 检查是否存在指定的Writer插件
func (r *PluginRegistry) HasWriter(name string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	_, exists := r.writers[name]
	return exists
}

// ParameterConverter 参数转换器，用于将any类型转换为具体的参数类型
type ParameterConverter struct{}

// ConvertParameter 将any类型的参数转换为指定类型的参数
func (c *ParameterConverter) ConvertParameter(parameter any, target any) error {
	// 使用JSON编解码进行类型转换
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(parameter); err != nil {
		return fmt.Errorf("参数编码失败: %v", err)
	}
	
	decoder := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("参数解码失败: %v", err)
	}
	
	return nil
}

// CreateReaderFactory 创建Reader工厂函数的辅助方法
func CreateReaderFactory[T any](createFunc func(*T) Reader) ReaderFactory {
	converter := &ParameterConverter{}
	return func(parameter any) (Reader, error) {
		var param T
		if err := converter.ConvertParameter(parameter, &param); err != nil {
			return nil, err
		}
		return createFunc(&param), nil
	}
}

// CreateWriterFactory 创建Writer工厂函数的辅助方法
func CreateWriterFactory[T any](createFunc func(*T) Writer) WriterFactory {
	converter := &ParameterConverter{}
	return func(parameter any) (Writer, error) {
		var param T
		if err := converter.ConvertParameter(parameter, &param); err != nil {
			return nil, err
		}
		return createFunc(&param), nil
	}
}

// 全局插件注册器实例
var DefaultRegistry = NewPluginRegistry()

// 为了保持向后兼容性，保留原有的全局注册函数
func RegisterReader(name string, factory ReaderFactory) {
	DefaultRegistry.RegisterReader(name, factory)
}

func RegisterWriter(name string, factory WriterFactory) {
	DefaultRegistry.RegisterWriter(name, factory)
}

// PluginInfo 插件信息
type PluginInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // "reader" 或 "writer"
	Description string `json:"description,omitempty"`
	Version     string `json:"version,omitempty"`
}

// GetPluginInfo 获取所有插件信息
func (r *PluginRegistry) GetPluginInfo() []PluginInfo {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	var plugins []PluginInfo
	
	// 添加Reader插件信息
	for name := range r.readers {
		plugins = append(plugins, PluginInfo{
			Name: name,
			Type: "reader",
		})
	}
	
	// 添加Writer插件信息
	for name := range r.writers {
		plugins = append(plugins, PluginInfo{
			Name: name,
			Type: "writer",
		})
	}
	
	return plugins
}

// Clear 清空所有注册的插件（主要用于测试）
func (r *PluginRegistry) Clear() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.readers = make(map[string]ReaderFactory)
	r.writers = make(map[string]WriterFactory)
}
