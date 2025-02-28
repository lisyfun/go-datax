#!/bin/bash

# 设置颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 创建bin目录（如果不存在）
mkdir -p bin

echo -e "${YELLOW}开始构建 DataX 可执行文件...${NC}"

# 设置版本信息
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME=$(date "+%Y-%m-%d %H:%M:%S")
COMMIT_ID=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# 构建 Mac M1 (darwin/arm64) 版本
echo -e "${YELLOW}构建 Mac M1 (darwin/arm64) 版本...${NC}"
GOOS=darwin GOARCH=arm64 go build -ldflags="-X 'main.Version=${VERSION}' -X 'main.BuildTime=${BUILD_TIME}' -X 'main.CommitID=${COMMIT_ID}'" -o bin/datax-darwin-arm64 .
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Mac M1 版本构建成功: bin/datax-darwin-arm64${NC}"
else
    echo -e "\033[0;31mMac M1 版本构建失败${NC}"
fi

# 构建 Linux ARM64 版本
echo -e "${YELLOW}构建 Linux ARM64 版本...${NC}"
GOOS=linux GOARCH=arm64 go build -ldflags="-X 'main.Version=${VERSION}' -X 'main.BuildTime=${BUILD_TIME}' -X 'main.CommitID=${COMMIT_ID}'" -o bin/datax-linux-arm64 .
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Linux ARM64 版本构建成功: bin/datax-linux-arm64${NC}"
else
    echo -e "\033[0;31mLinux ARM64 版本构建失败${NC}"
fi

# 构建 Linux x86_64 版本
echo -e "${YELLOW}构建 Linux x86_64 版本...${NC}"
GOOS=linux GOARCH=amd64 go build -ldflags="-X 'main.Version=${VERSION}' -X 'main.BuildTime=${BUILD_TIME}' -X 'main.CommitID=${COMMIT_ID}'" -o bin/datax-linux-amd64 .
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Linux x86_64 版本构建成功: bin/datax-linux-amd64${NC}"
else
    echo -e "\033[0;31mLinux x86_64 版本构建失败${NC}"
fi

# 显示构建结果
echo -e "${YELLOW}构建完成，生成的可执行文件:${NC}"
ls -lh bin/datax-*

# 添加可执行权限
chmod +x bin/datax-*
