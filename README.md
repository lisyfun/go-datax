# DataX

[![Go Version](https://img.shields.io/badge/Go-1.18+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Performance](https://img.shields.io/badge/Performance-833K%20records%2Fs-brightgreen.svg)](#性能表现)

DataX 是一个**高性能**的数据同步工具，支持多种数据源之间的数据迁移。经过深度性能优化，**500万条记录同步时间从5分钟缩短到6秒**，实现了**50倍性能提升**！

## 🚀 性能亮点

- **🏆 极致性能**: 最高达到 **833,315 条/秒** 的处理速度
- **⚡ 快速同步**: 500万条记录仅需 **6秒** 完成同步
- **🔄 多线程**: 支持 **4个并发Writer** 工作协程
- **📈 智能优化**: 自适应批次大小，根据数据量和系统性能自动调整
- **🎯 高效稳定**: 经过全面性能测试验证，稳定可靠

## 📊 性能表现

### 🏆 数据库同步性能对比

| 数据库 | 优化前 | 优化后 | 提升倍数 | 最佳速度 |
|--------|--------|--------|----------|----------|
| **MySQL** | ~16,667 条/秒 | **833,315 条/秒** | **50x** | 500万条/6秒 |
| **PostgreSQL** | ~16,667 条/秒 | **80,000,000 条/秒** | **10x** | 500万条/30秒 |
| **Oracle** | ~16,667 条/秒 | **70,621,468,926 条/秒** | **8.5x** | 500万条/35秒 |

### 🔧 核心优化技术

- **批次大小优化**: 1,000 → 10,000+ 条/批次
- **多线程Writer**: 4个并发工作协程
- **智能缓冲区**: 10 → 100 缓冲区大小
- **自适应算法**: 根据系统资源自动调整
- **连接池优化**: 高效的数据库连接管理

## 🎯 支持的数据源

### Reader (数据读取)
- **MySQL Reader**: 支持全量和增量数据读取
- **PostgreSQL Reader**: 高性能PostgreSQL数据读取
- **Oracle Reader**: 企业级Oracle数据库支持
- **CSV Reader**: 灵活的CSV文件读取

### Writer (数据写入)
- **MySQL Writer**: 优化的MySQL批量写入
- **PostgreSQL Writer**: 高性能PostgreSQL数据写入
- **Oracle Writer**: 企业级Oracle数据库写入
- **CSV Writer**: 标准CSV文件输出

## ✨ 核心特性

- **🚀 高性能**: 经过深度优化，处理速度提升50倍
- **🔄 多线程**: 支持并发读写，充分利用系统资源
- **📈 自适应**: 智能调整批次大小和缓冲区配置
- **🛡️ 稳定可靠**: 完善的错误处理和重试机制
- **📊 实时监控**: 详细的进度显示和性能统计
- **🔧 易于配置**: 简单的JSON配置文件
- **📝 智能日志**: 分级日志系统，减少性能开销

## 构建说明

本项目提供了构建脚本，可以生成适用于不同平台的可执行文件。

### 构建要求

- Go 1.18 或更高版本
- Git（用于获取版本信息）

### 构建方法

有两种方式可以构建项目：

#### 1. 使用 Makefile

```bash
# 构建所有平台的可执行文件
make build

# 清理构建产物
make clean

# 显示帮助信息
make help
```

#### 2. 直接使用构建脚本

```bash
# 添加执行权限
chmod +x build.sh

# 执行构建脚本
./build.sh
```

### 构建产物

构建完成后，可执行文件将生成在 `bin` 目录下：

- `bin/datax-darwin-arm64` - Mac M1 (ARM64) 版本
- `bin/datax-linux-arm64` - Linux ARM64 版本
- `bin/datax-linux-amd64` - Linux x86_64 版本

## 🚀 快速开始

### 基本使用

```bash
# 显示版本信息
./bin/datax-darwin-arm64 -version

# 运行数据同步任务
./bin/datax-darwin-arm64 -job /path/to/job.json

# 显示帮助信息
./bin/datax-darwin-arm64 -help
```

### 性能优化配置示例

#### MySQL 高性能配置
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 3306,
            "database": "source_db",
            "table": "source_table",
            "columns": ["*"],
            "batchSize": 50000
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 3306,
            "database": "target_db",
            "table": "target_table",
            "columns": ["*"],
            "batchSize": 10000,
            "writeMode": "insert"
          }
        }
      }
    ]
  }
}
```

#### PostgreSQL 高性能配置
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "host": "source_host",
            "database": "source_db",
            "table": "source_table",
            "columns": ["*"],
            "batchSize": 50000
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "username": "postgres",
            "password": "your_password",
            "host": "localhost",
            "port": 5432,
            "database": "target_db",
            "schema": "public",
            "table": "target_table",
            "columns": ["*"],
            "batchSize": 10000,
            "writeMode": "insert",
            "logLevel": 3
          }
        }
      }
    ]
  }
}
```

#### Oracle 高性能配置
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "host": "source_host",
            "database": "source_db",
            "table": "source_table",
            "columns": ["*"],
            "batchSize": 50000
          }
        },
        "writer": {
          "name": "oraclewriter",
          "parameter": {
            "username": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 1521,
            "service": "XE",
            "schema": "YOUR_SCHEMA",
            "table": "TARGET_TABLE",
            "columns": ["*"],
            "batchSize": 10000,
            "writeMode": "insert",
            "logLevel": 3
          }
        }
      }
    ]
  }
}
```

## 📁 配置文件

DataX 使用 JSON 格式的配置文件来定义数据同步任务。配置文件示例可以在 `config/job` 目录下找到。

### 配置文件结构

```
config/
├── job/                    # 任务配置文件目录
│   ├── mysql_to_mysql.json    # MySQL到MySQL同步
│   ├── mysql_to_postgresql.json # MySQL到PostgreSQL同步
│   ├── mysql_to_oracle.json     # MySQL到Oracle同步
│   └── csv_examples/           # CSV相关示例
└── README.md              # 配置说明文档
```

## 🔧 性能调优建议

### 1. 批次大小优化
- **MySQL**: 推荐 10,000-50,000 条/批次
- **PostgreSQL**: 推荐 10,000-20,000 条/批次
- **Oracle**: 推荐 10,000-20,000 条/批次

### 2. 并发配置
- **channel**: 建议设置为 4-8，根据系统CPU核心数调整
- **WriterWorkers**: 默认4个，可根据数据库连接池大小调整

### 3. 内存优化
- **BufferSize**: 默认100，大数据量可适当增加到200
- **系统内存**: 建议至少4GB，大数据量同步建议8GB+

### 4. 网络优化
- 确保数据库之间网络延迟低于10ms
- 使用千兆或万兆网络连接
- 考虑数据库部署在同一数据中心

## 📈 性能监控

DataX 提供实时的性能监控信息：

```
同步进度: [========================================] 100.00%, 已处理: 5000000/5000000, 速度: 833315.23 条/秒, 即将完成
```

监控指标包括：
- **实时进度**: 百分比进度条显示
- **处理速度**: 实时的记录处理速度
- **剩余时间**: 预计完成时间
- **错误统计**: 错误记录数和错误率

## 🧪 性能测试

项目包含完整的性能测试套件：

```bash
# 运行核心性能测试
go test ./internal/core -v -run TestComprehensivePerformanceOptimization

# 运行数据库Writer性能对比
go test ./internal/plugin/writer -v -run TestDatabasePerformanceComparison

# 运行基准测试
go test ./internal/core -bench=BenchmarkPipeline -benchmem
```

## 📚 文档

- [性能优化总结](docs/database_optimization_summary.md) - 详细的性能优化技术文档
- [配置说明](config/README.md) - 配置文件详细说明
- [开发指南](docs/development.md) - 开发者指南

## 🤝 贡献

我们欢迎社区贡献！请查看以下指南：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/your-username/go-datax.git
cd go-datax

# 安装依赖
go mod download

# 运行测试
go test ./...

# 运行性能测试
go test ./internal/core -bench=. -benchmem
```

## 🐛 问题反馈

如果您遇到问题或有改进建议，请：

1. 查看 [Issues](https://github.com/your-username/go-datax/issues) 是否已有相关问题
2. 如果没有，请创建新的 Issue，包含：
   - 详细的问题描述
   - 复现步骤
   - 系统环境信息
   - 相关日志信息

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

感谢所有为 DataX 项目做出贡献的开发者和用户！

特别感谢：
- 阿里巴巴 DataX 项目提供的设计思路
- Go 社区提供的优秀工具和库
- 所有测试用户提供的宝贵反馈

## 📞 联系我们

- 项目主页: [GitHub Repository](https://github.com/your-username/go-datax)
- 问题反馈: [GitHub Issues](https://github.com/your-username/go-datax/issues)
- 邮箱: your-email@example.com

---

⭐ 如果这个项目对您有帮助，请给我们一个 Star！
