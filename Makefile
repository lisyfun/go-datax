.PHONY: build clean help

# 默认目标
.DEFAULT_GOAL := help

# 构建所有平台的可执行文件
build:
	@echo "构建所有平台的可执行文件..."
	@./build.sh

# 清理构建产物
clean:
	@echo "清理构建产物..."
	@rm -f bin/datax-*

# 帮助信息
help:
	@echo "DataX 构建工具"
	@echo ""
	@echo "可用命令:"
	@echo "  make build    - 构建所有平台的可执行文件"
	@echo "  make clean    - 清理构建产物"
	@echo "  make help     - 显示帮助信息"
	@echo ""
	@echo "直接运行 ./build.sh 也可以构建所有平台的可执行文件"
