package core

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestNewPluginRegistry(t *testing.T) {
	registry := NewPluginRegistry()
	
	if registry == nil {
		t.Error("NewPluginRegistry should return a non-nil registry")
	}
	
	if registry.readers == nil {
		t.Error("Registry should have initialized readers map")
	}
	
	if registry.writers == nil {
		t.Error("Registry should have initialized writers map")
	}
}

func TestPluginRegistry_RegisterReader(t *testing.T) {
	registry := NewPluginRegistry()
	
	factory := func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	}
	
	registry.RegisterReader("test-reader", factory)
	
	if !registry.HasReader("test-reader") {
		t.Error("Reader should be registered")
	}
	
	readers := registry.GetRegisteredReaders()
	if len(readers) != 1 || readers[0] != "test-reader" {
		t.Errorf("Expected [test-reader], got %v", readers)
	}
}

func TestPluginRegistry_RegisterWriter(t *testing.T) {
	registry := NewPluginRegistry()
	
	factory := func(parameter any) (Writer, error) {
		return &mockWriter{}, nil
	}
	
	registry.RegisterWriter("test-writer", factory)
	
	if !registry.HasWriter("test-writer") {
		t.Error("Writer should be registered")
	}
	
	writers := registry.GetRegisteredWriters()
	if len(writers) != 1 || writers[0] != "test-writer" {
		t.Errorf("Expected [test-writer], got %v", writers)
	}
}

func TestPluginRegistry_CreateReader(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 注册成功的factory
	registry.RegisterReader("success-reader", func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	})
	
	// 注册失败的factory
	registry.RegisterReader("error-reader", func(parameter any) (Reader, error) {
		return nil, errors.New("factory error")
	})
	
	// 测试成功创建
	reader, err := registry.CreateReader("success-reader", nil)
	if err != nil {
		t.Errorf("CreateReader should succeed, got error: %v", err)
	}
	if reader == nil {
		t.Error("CreateReader should return a reader instance")
	}
	
	// 测试factory错误
	reader, err = registry.CreateReader("error-reader", nil)
	if err == nil {
		t.Error("CreateReader should return error for error-reader")
	}
	if reader != nil {
		t.Error("CreateReader should return nil reader on error")
	}
	
	// 测试不存在的插件
	reader, err = registry.CreateReader("non-existent", nil)
	if err == nil {
		t.Error("CreateReader should return error for non-existent plugin")
	}
	expectedMsg := "未找到Reader插件: non-existent"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestPluginRegistry_CreateWriter(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 注册成功的factory
	registry.RegisterWriter("success-writer", func(parameter any) (Writer, error) {
		return &mockWriter{}, nil
	})
	
	// 注册失败的factory
	registry.RegisterWriter("error-writer", func(parameter any) (Writer, error) {
		return nil, errors.New("factory error")
	})
	
	// 测试成功创建
	writer, err := registry.CreateWriter("success-writer", nil)
	if err != nil {
		t.Errorf("CreateWriter should succeed, got error: %v", err)
	}
	if writer == nil {
		t.Error("CreateWriter should return a writer instance")
	}
	
	// 测试factory错误
	writer, err = registry.CreateWriter("error-writer", nil)
	if err == nil {
		t.Error("CreateWriter should return error for error-writer")
	}
	if writer != nil {
		t.Error("CreateWriter should return nil writer on error")
	}
	
	// 测试不存在的插件
	writer, err = registry.CreateWriter("non-existent", nil)
	if err == nil {
		t.Error("CreateWriter should return error for non-existent plugin")
	}
	expectedMsg := "未找到Writer插件: non-existent"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestPluginRegistry_GetRegisteredReaders(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 初始应该为空
	readers := registry.GetRegisteredReaders()
	if len(readers) != 0 {
		t.Errorf("Initial readers should be empty, got %v", readers)
	}
	
	// 注册多个reader
	registry.RegisterReader("reader1", func(parameter any) (Reader, error) { return &mockReader{}, nil })
	registry.RegisterReader("reader2", func(parameter any) (Reader, error) { return &mockReader{}, nil })
	
	readers = registry.GetRegisteredReaders()
	sort.Strings(readers) // 排序以便比较
	expected := []string{"reader1", "reader2"}
	
	if !reflect.DeepEqual(readers, expected) {
		t.Errorf("Expected %v, got %v", expected, readers)
	}
}

func TestPluginRegistry_GetRegisteredWriters(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 初始应该为空
	writers := registry.GetRegisteredWriters()
	if len(writers) != 0 {
		t.Errorf("Initial writers should be empty, got %v", writers)
	}
	
	// 注册多个writer
	registry.RegisterWriter("writer1", func(parameter any) (Writer, error) { return &mockWriter{}, nil })
	registry.RegisterWriter("writer2", func(parameter any) (Writer, error) { return &mockWriter{}, nil })
	
	writers = registry.GetRegisteredWriters()
	sort.Strings(writers) // 排序以便比较
	expected := []string{"writer1", "writer2"}
	
	if !reflect.DeepEqual(writers, expected) {
		t.Errorf("Expected %v, got %v", expected, writers)
	}
}

func TestParameterConverter_ConvertParameter(t *testing.T) {
	converter := &ParameterConverter{}
	
	// 测试结构体参数
	type TestParam struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	
	input := map[string]any{
		"name":  "test",
		"value": 42,
	}
	
	var output TestParam
	err := converter.ConvertParameter(input, &output)
	if err != nil {
		t.Errorf("ConvertParameter should succeed, got error: %v", err)
	}
	
	if output.Name != "test" {
		t.Errorf("Expected name 'test', got '%s'", output.Name)
	}
	
	if output.Value != 42 {
		t.Errorf("Expected value 42, got %d", output.Value)
	}
}

func TestCreateReaderFactory(t *testing.T) {
	type TestParam struct {
		Name string `json:"name"`
	}
	
	factory := CreateReaderFactory(func(param *TestParam) Reader {
		return &mockReader{}
	})
	
	input := map[string]any{
		"name": "test",
	}
	
	reader, err := factory(input)
	if err != nil {
		t.Errorf("Factory should succeed, got error: %v", err)
	}
	
	if reader == nil {
		t.Error("Factory should return a reader instance")
	}
}

func TestCreateWriterFactory(t *testing.T) {
	type TestParam struct {
		Name string `json:"name"`
	}
	
	factory := CreateWriterFactory(func(param *TestParam) Writer {
		return &mockWriter{}
	})
	
	input := map[string]any{
		"name": "test",
	}
	
	writer, err := factory(input)
	if err != nil {
		t.Errorf("Factory should succeed, got error: %v", err)
	}
	
	if writer == nil {
		t.Error("Factory should return a writer instance")
	}
}

func TestPluginRegistry_GetPluginInfo(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 注册插件
	registry.RegisterReader("test-reader", func(parameter any) (Reader, error) { return &mockReader{}, nil })
	registry.RegisterWriter("test-writer", func(parameter any) (Writer, error) { return &mockWriter{}, nil })
	
	info := registry.GetPluginInfo()
	
	if len(info) != 2 {
		t.Errorf("Expected 2 plugins, got %d", len(info))
	}
	
	// 检查插件信息
	var readerFound, writerFound bool
	for _, plugin := range info {
		if plugin.Name == "test-reader" && plugin.Type == "reader" {
			readerFound = true
		}
		if plugin.Name == "test-writer" && plugin.Type == "writer" {
			writerFound = true
		}
	}
	
	if !readerFound {
		t.Error("Reader plugin info not found")
	}
	
	if !writerFound {
		t.Error("Writer plugin info not found")
	}
}

func TestPluginRegistry_Clear(t *testing.T) {
	registry := NewPluginRegistry()
	
	// 注册插件
	registry.RegisterReader("test-reader", func(parameter any) (Reader, error) { return &mockReader{}, nil })
	registry.RegisterWriter("test-writer", func(parameter any) (Writer, error) { return &mockWriter{}, nil })
	
	// 验证插件已注册
	if !registry.HasReader("test-reader") {
		t.Error("Reader should be registered before clear")
	}
	if !registry.HasWriter("test-writer") {
		t.Error("Writer should be registered before clear")
	}
	
	// 清空
	registry.Clear()
	
	// 验证插件已清空
	if registry.HasReader("test-reader") {
		t.Error("Reader should be cleared")
	}
	if registry.HasWriter("test-writer") {
		t.Error("Writer should be cleared")
	}
	
	if len(registry.GetRegisteredReaders()) != 0 {
		t.Error("Readers should be empty after clear")
	}
	if len(registry.GetRegisteredWriters()) != 0 {
		t.Error("Writers should be empty after clear")
	}
}

func TestDefaultRegistry(t *testing.T) {
	// 清空默认注册器
	DefaultRegistry.Clear()
	
	// 测试全局注册函数
	RegisterReader("global-reader", func(parameter any) (Reader, error) { return &mockReader{}, nil })
	RegisterWriter("global-writer", func(parameter any) (Writer, error) { return &mockWriter{}, nil })
	
	// 验证注册成功
	if !DefaultRegistry.HasReader("global-reader") {
		t.Error("Global reader should be registered")
	}
	if !DefaultRegistry.HasWriter("global-writer") {
		t.Error("Global writer should be registered")
	}
	
	// 清理
	DefaultRegistry.Clear()
}
