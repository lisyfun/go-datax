package core

import (
	"errors"
	"testing"
)

func TestNewDataXEngine(t *testing.T) {
	config := &JobConfig{}
	engine := NewDataXEngine(config)

	if engine == nil {
		t.Error("NewDataXEngine should return a non-nil engine")
	}

	if engine.jobConfig != config {
		t.Error("Engine should store the provided config")
	}

	if engine.logger == nil {
		t.Error("Engine should have a logger")
	}
}

func TestDataXEngine_Start_EmptyContent(t *testing.T) {
	config := &JobConfig{}
	engine := NewDataXEngine(config)

	err := engine.Start()
	if err == nil {
		t.Error("Start should return error for empty content")
	}

	expectedMsg := "任务配置中没有content"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_MissingReader(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	config := &JobConfig{}
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

	config.Job.Content[0].Reader.Name = "missing-reader"
	config.Job.Content[0].Writer.Name = "test-writer"

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error for missing reader")
	}

	expectedMsg := "创建Reader失败: 未找到Reader插件: missing-reader"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_MissingWriter(t *testing.T) {
	// 清理注册表并注册一个reader
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	RegisterReader("test-reader", func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	})

	config := &JobConfig{}
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
	config.Job.Content[0].Writer.Name = "missing-writer"

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error for missing writer")
	}

	expectedMsg := "创建Writer失败: 未找到Writer插件: missing-writer"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_ReaderFactoryError(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	RegisterReader("error-reader", func(parameter any) (Reader, error) {
		return nil, errors.New("reader factory error")
	})

	config := &JobConfig{}
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

	config.Job.Content[0].Reader.Name = "error-reader"

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error when reader factory fails")
	}

	expectedMsg := "创建Reader失败: reader factory error"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_WriterFactoryError(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	RegisterReader("test-reader", func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	})

	RegisterWriter("error-writer", func(parameter any) (Writer, error) {
		return nil, errors.New("writer factory error")
	})

	config := &JobConfig{}
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
	config.Job.Content[0].Writer.Name = "error-writer"

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error when writer factory fails")
	}

	expectedMsg := "创建Writer失败: writer factory error"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_ReaderConnectError(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	RegisterReader("test-reader", func(parameter any) (Reader, error) {
		return &mockReader{connectErr: errors.New("reader connect error")}, nil
	})

	RegisterWriter("test-writer", func(parameter any) (Writer, error) {
		return &mockWriter{}, nil
	})

	config := &JobConfig{}
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

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error when reader connect fails")
	}

	expectedMsg := "连接Reader失败: reader connect error"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_WriterConnectError(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	RegisterReader("test-reader", func(parameter any) (Reader, error) {
		return &mockReader{}, nil
	})

	RegisterWriter("test-writer", func(parameter any) (Writer, error) {
		return &mockWriter{connectErr: errors.New("writer connect error")}, nil
	})

	config := &JobConfig{}
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

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err == nil {
		t.Error("Start should return error when writer connect fails")
	}

	expectedMsg := "连接Writer失败: writer connect error"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestDataXEngine_Start_Success(t *testing.T) {
	// 清理注册表
	factoryMutex.Lock()
	readerFactories = make(map[string]ReaderFactory)
	writerFactories = make(map[string]WriterFactory)
	factoryMutex.Unlock()

	// 创建成功的mock
	RegisterReader("test-reader", func(parameter any) (Reader, error) {
		return &mockReader{
			readData:   [][]any{{"test", "data"}},
			totalCount: 1,
		}, nil
	})

	RegisterWriter("test-writer", func(parameter any) (Writer, error) {
		return &mockWriter{}, nil
	})

	config := &JobConfig{}
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

	// 设置默认的setting值
	config.Job.Setting.ErrorLimit.Record = 0 // 不限制错误数量

	engine := NewDataXEngine(config)
	err := engine.Start()

	if err != nil {
		t.Errorf("Start should succeed, got error: %v", err)
	}
}
