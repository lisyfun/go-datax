# DataX

DataX 是一个高效的数据同步工具，支持多种数据源之间的数据迁移。

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

## 使用方法

```bash
# 显示版本信息
./bin/datax-darwin-arm64 -version

# 运行任务
./bin/datax-darwin-arm64 -job /path/to/job.json
```

## 配置文件

DataX 使用 JSON 格式的配置文件来定义数据同步任务。配置文件示例可以在 `config` 目录下找到。
