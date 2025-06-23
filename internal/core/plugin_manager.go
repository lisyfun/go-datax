package core

import (
	"fmt"

	mysqlReader "datax/internal/plugin/reader/mysql"
	oracleReader "datax/internal/plugin/reader/oracle"
	pgReader "datax/internal/plugin/reader/postgresql"
	mysqlWriter "datax/internal/plugin/writer/mysql"
	oracleWriter "datax/internal/plugin/writer/oracle"
	pgWriter "datax/internal/plugin/writer/postgresql"
)

// PluginManager 插件管理器
type PluginManager struct {
	registry *PluginRegistry
}

// NewPluginManager 创建新的插件管理器
func NewPluginManager(registry *PluginRegistry) *PluginManager {
	return &PluginManager{
		registry: registry,
	}
}

// RegisterAllPlugins 注册所有内置插件
func (pm *PluginManager) RegisterAllPlugins() error {
	// 注册Reader插件
	if err := pm.registerReaderPlugins(); err != nil {
		return fmt.Errorf("注册Reader插件失败: %v", err)
	}

	// 注册Writer插件
	if err := pm.registerWriterPlugins(); err != nil {
		return fmt.Errorf("注册Writer插件失败: %v", err)
	}

	return nil
}

// registerReaderPlugins 注册所有Reader插件
func (pm *PluginManager) registerReaderPlugins() error {
	// MySQL Reader
	pm.registry.RegisterReader("mysqlreader", CreateReaderFactory(func(param *mysqlReader.Parameter) Reader {
		return mysqlReader.NewMySQLReader(param)
	}))

	// PostgreSQL Reader
	pm.registry.RegisterReader("postgresqlreader", CreateReaderFactory(func(param *pgReader.Parameter) Reader {
		return pgReader.NewPostgreSQLReader(param)
	}))

	// Oracle Reader
	pm.registry.RegisterReader("oraclereader", CreateReaderFactory(func(param *oracleReader.Parameter) Reader {
		return oracleReader.NewOracleReader(param)
	}))

	return nil
}

// registerWriterPlugins 注册所有Writer插件
func (pm *PluginManager) registerWriterPlugins() error {
	// MySQL Writer
	pm.registry.RegisterWriter("mysqlwriter", CreateWriterFactory(func(param *mysqlWriter.Parameter) Writer {
		return mysqlWriter.NewMySQLWriter(param)
	}))

	// PostgreSQL Writer
	pm.registry.RegisterWriter("postgresqlwriter", CreateWriterFactory(func(param *pgWriter.Parameter) Writer {
		return pgWriter.NewPostgreSQLWriter(param)
	}))

	// Oracle Writer
	pm.registry.RegisterWriter("oraclewriter", CreateWriterFactory(func(param *oracleWriter.Parameter) Writer {
		return oracleWriter.NewOracleWriter(param)
	}))

	return nil
}

// RegisterPlugin 注册单个插件
func (pm *PluginManager) RegisterPlugin(pluginType, name string, parameter any) error {
	switch pluginType {
	case "reader":
		return pm.registerSingleReader(name, parameter)
	case "writer":
		return pm.registerSingleWriter(name, parameter)
	default:
		return fmt.Errorf("未知的插件类型: %s", pluginType)
	}
}

// registerSingleReader 注册单个Reader插件
func (pm *PluginManager) registerSingleReader(name string, parameter any) error {
	switch name {
	case "mysqlreader":
		pm.registry.RegisterReader(name, CreateReaderFactory(func(param *mysqlReader.Parameter) Reader {
			return mysqlReader.NewMySQLReader(param)
		}))
	case "postgresqlreader":
		pm.registry.RegisterReader(name, CreateReaderFactory(func(param *pgReader.Parameter) Reader {
			return pgReader.NewPostgreSQLReader(param)
		}))
	case "oraclereader":
		pm.registry.RegisterReader(name, CreateReaderFactory(func(param *oracleReader.Parameter) Reader {
			return oracleReader.NewOracleReader(param)
		}))
	default:
		return fmt.Errorf("未知的Reader类型: %s", name)
	}
	return nil
}

// registerSingleWriter 注册单个Writer插件
func (pm *PluginManager) registerSingleWriter(name string, parameter any) error {
	switch name {
	case "mysqlwriter":
		pm.registry.RegisterWriter(name, CreateWriterFactory(func(param *mysqlWriter.Parameter) Writer {
			return mysqlWriter.NewMySQLWriter(param)
		}))
	case "postgresqlwriter":
		pm.registry.RegisterWriter(name, CreateWriterFactory(func(param *pgWriter.Parameter) Writer {
			return pgWriter.NewPostgreSQLWriter(param)
		}))
	case "oraclewriter":
		pm.registry.RegisterWriter(name, CreateWriterFactory(func(param *oracleWriter.Parameter) Writer {
			return oracleWriter.NewOracleWriter(param)
		}))
	default:
		return fmt.Errorf("未知的Writer类型: %s", name)
	}
	return nil
}

// GetSupportedPlugins 获取支持的插件列表
func (pm *PluginManager) GetSupportedPlugins() map[string][]string {
	return map[string][]string{
		"readers": {"mysqlreader", "postgresqlreader", "oraclereader"},
		"writers": {"mysqlwriter", "postgresqlwriter", "oraclewriter"},
	}
}

// ValidatePlugin 验证插件是否支持
func (pm *PluginManager) ValidatePlugin(pluginType, name string) error {
	supported := pm.GetSupportedPlugins()
	
	var pluginList []string
	switch pluginType {
	case "reader":
		pluginList = supported["readers"]
	case "writer":
		pluginList = supported["writers"]
	default:
		return fmt.Errorf("未知的插件类型: %s", pluginType)
	}
	
	for _, supportedName := range pluginList {
		if supportedName == name {
			return nil
		}
	}
	
	return fmt.Errorf("不支持的%s插件: %s", pluginType, name)
}

// IsPluginRegistered 检查插件是否已注册
func (pm *PluginManager) IsPluginRegistered(pluginType, name string) bool {
	switch pluginType {
	case "reader":
		return pm.registry.HasReader(name)
	case "writer":
		return pm.registry.HasWriter(name)
	default:
		return false
	}
}

// GetRegistry 获取插件注册器
func (pm *PluginManager) GetRegistry() *PluginRegistry {
	return pm.registry
}

// 全局插件管理器实例
var DefaultPluginManager = NewPluginManager(DefaultRegistry)

// RegisterAllBuiltinPlugins 注册所有内置插件的便捷函数
func RegisterAllBuiltinPlugins() error {
	return DefaultPluginManager.RegisterAllPlugins()
}

// ValidateBuiltinPlugin 验证内置插件的便捷函数
func ValidateBuiltinPlugin(pluginType, name string) error {
	return DefaultPluginManager.ValidatePlugin(pluginType, name)
}

// GetSupportedBuiltinPlugins 获取支持的内置插件列表的便捷函数
func GetSupportedBuiltinPlugins() map[string][]string {
	return DefaultPluginManager.GetSupportedPlugins()
}
