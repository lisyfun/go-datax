package core

import (
	"errors"
	"testing"
)

// 模拟的Reader实现
type mockReader struct {
	connectErr    error
	readData      [][]any
	readErr       error
	closeErr      error
	totalCount    int64
	totalCountErr error
	readCount     int64 // 已读取次数
}

func (m *mockReader) Connect() error {
	return m.connectErr
}

func (m *mockReader) Read() ([][]any, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}

	// 如果已经读取了指定次数，返回空数据表示结束
	if m.readCount >= m.totalCount {
		return [][]any{}, nil
	}

	m.readCount++
	return m.readData, nil
}

func (m *mockReader) Close() error {
	return m.closeErr
}

func (m *mockReader) GetTotalCount() (int64, error) {
	return m.totalCount, m.totalCountErr
}

// 模拟的Writer实现
type mockWriter struct {
	connectErr     error
	writeErr       error
	closeErr       error
	preProcessErr  error
	postProcessErr error
}

func (m *mockWriter) Connect() error {
	return m.connectErr
}

func (m *mockWriter) Write(records [][]any) error {
	return m.writeErr
}

func (m *mockWriter) Close() error {
	return m.closeErr
}

func (m *mockWriter) PreProcess() error {
	return m.preProcessErr
}

func (m *mockWriter) PostProcess() error {
	return m.postProcessErr
}

func TestRegisterReader(t *testing.T) {
	// 清理注册表
	DefaultRegistry.Clear()

	// 测试注册Reader
	testFactory := func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	}

	RegisterReader("test-reader", testFactory)

	// 验证注册成功
	if !DefaultRegistry.HasReader("test-reader") {
		t.Error("Reader factory should be registered")
	}

	// 测试工厂函数
	reader, err := DefaultRegistry.CreateReader("test-reader", nil)
	if err != nil {
		t.Errorf("Factory should create reader without error, got: %v", err)
	}

	if reader == nil {
		t.Error("Factory should return a reader instance")
	}
}

func TestRegisterWriter(t *testing.T) {
	// 清理注册表
	DefaultRegistry.Clear()

	// 测试注册Writer
	testFactory := func(parameter any) (Writer, error) {
		return &mockWriter{}, nil
	}

	RegisterWriter("test-writer", testFactory)

	// 验证注册成功
	if !DefaultRegistry.HasWriter("test-writer") {
		t.Error("Writer factory should be registered")
	}

	// 测试工厂函数
	writer, err := DefaultRegistry.CreateWriter("test-writer", nil)
	if err != nil {
		t.Errorf("Factory should create writer without error, got: %v", err)
	}

	if writer == nil {
		t.Error("Factory should return a writer instance")
	}
}

func TestReaderFactory(t *testing.T) {
	// 清理注册表
	DefaultRegistry.Clear()

	// 测试工厂函数返回错误
	errorFactory := func(parameter any) (Reader, error) {
		return nil, errors.New("factory error")
	}

	RegisterReader("error-reader", errorFactory)

	if !DefaultRegistry.HasReader("error-reader") {
		t.Error("Error factory should be registered")
	}

	reader, err := DefaultRegistry.CreateReader("error-reader", nil)
	if err == nil {
		t.Error("Factory should return error")
	}

	if reader != nil {
		t.Error("Factory should return nil reader on error")
	}
}

func TestWriterFactory(t *testing.T) {
	// 清理注册表
	DefaultRegistry.Clear()

	// 测试工厂函数返回错误
	errorFactory := func(parameter any) (Writer, error) {
		return nil, errors.New("factory error")
	}

	RegisterWriter("error-writer", errorFactory)

	if !DefaultRegistry.HasWriter("error-writer") {
		t.Error("Error factory should be registered")
	}

	writer, err := DefaultRegistry.CreateWriter("error-writer", nil)
	if err == nil {
		t.Error("Factory should return error")
	}

	if writer != nil {
		t.Error("Factory should return nil writer on error")
	}
}

func TestJobConfigStructure(t *testing.T) {
	// 测试JobConfig结构体的基本功能
	config := &JobConfig{}

	// 测试嵌套结构
	config.Job.Content = make([]struct {
		Reader struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"reader"`
		Writer struct {
			Name      string `json:"name"`
			Parameter any    `json:"parameter"`
		} `json:"writer"`
	}, 1)

	config.Job.Content[0].Reader.Name = "test-reader"
	config.Job.Content[0].Writer.Name = "test-writer"

	if config.Job.Content[0].Reader.Name != "test-reader" {
		t.Error("Reader name should be set correctly")
	}

	if config.Job.Content[0].Writer.Name != "test-writer" {
		t.Error("Writer name should be set correctly")
	}
}

func TestConcurrentRegistration(t *testing.T) {
	// 清理注册表
	DefaultRegistry.Clear()

	// 测试并发注册
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			RegisterReader("concurrent-reader", func(parameter any) (Reader, error) {
				return &mockReader{}, nil
			})
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			RegisterWriter("concurrent-writer", func(parameter any) (Writer, error) {
				return &mockWriter{}, nil
			})
		}
		done <- true
	}()

	// 等待两个goroutine完成
	<-done
	<-done

	// 验证注册成功
	if !DefaultRegistry.HasReader("concurrent-reader") {
		t.Error("Concurrent reader registration should succeed")
	}

	if !DefaultRegistry.HasWriter("concurrent-writer") {
		t.Error("Concurrent writer registration should succeed")
	}
}

func TestMockReaderInterface(t *testing.T) {
	reader := &mockReader{
		readData:   [][]any{{"test", "data"}},
		totalCount: 1,
	}

	// 测试Connect
	err := reader.Connect()
	if err != nil {
		t.Errorf("Connect should succeed, got: %v", err)
	}

	// 测试Read
	data, err := reader.Read()
	if err != nil {
		t.Errorf("Read should succeed, got: %v", err)
	}
	if len(data) != 1 || len(data[0]) != 2 {
		t.Error("Read should return expected data")
	}

	// 测试GetTotalCount
	count, err := reader.GetTotalCount()
	if err != nil {
		t.Errorf("GetTotalCount should succeed, got: %v", err)
	}
	if count != 1 {
		t.Errorf("GetTotalCount should return 1, got: %d", count)
	}

	// 测试Close
	err = reader.Close()
	if err != nil {
		t.Errorf("Close should succeed, got: %v", err)
	}
}

func TestMockWriterInterface(t *testing.T) {
	writer := &mockWriter{}

	// 测试Connect
	err := writer.Connect()
	if err != nil {
		t.Errorf("Connect should succeed, got: %v", err)
	}

	// 测试PreProcess
	err = writer.PreProcess()
	if err != nil {
		t.Errorf("PreProcess should succeed, got: %v", err)
	}

	// 测试Write
	err = writer.Write([][]any{{"test", "data"}})
	if err != nil {
		t.Errorf("Write should succeed, got: %v", err)
	}

	// 测试PostProcess
	err = writer.PostProcess()
	if err != nil {
		t.Errorf("PostProcess should succeed, got: %v", err)
	}

	// 测试Close
	err = writer.Close()
	if err != nil {
		t.Errorf("Close should succeed, got: %v", err)
	}
}
