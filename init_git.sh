#!/bin/bash
# Git 初始化脚本
# 为 SOURCE 项目设置 Git 版本控制

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}开始初始化 Git 仓库...${NC}"

# 检查 Git 是否已安装
if ! command -v git &> /dev/null; then
    echo -e "${RED}错误: 未安装 Git。${NC}"
    echo -e "${YELLOW}请先安装 Git:${NC}"
    echo -e "  Ubuntu/Debian: sudo apt-get install git"
    echo -e "  CentOS/RHEL:   sudo yum install git"
    echo -e "  macOS:         brew install git"
    exit 1
fi

# 检查是否已经是 Git 仓库
if [ -d ".git" ]; then
    echo -e "${YELLOW}警告: .git 目录已存在，删除后重新初始化...${NC}"
    rm -rf .git
fi

# 初始化 Git 仓库
echo -e "${CYAN}正在初始化 Git 仓库...${NC}"
git init

# 配置 Git 用户信息（如果尚未配置）
GIT_USER_NAME=$(git config user.name || echo "")
GIT_USER_EMAIL=$(git config user.email || echo "")

if [ -z "$GIT_USER_NAME" ]; then
    echo -e "${YELLOW}请输入您的 Git 用户名:${NC}"
    read -r USER_NAME
    git config user.name "$USER_NAME"
    echo -e "${GREEN}已设置用户名: $USER_NAME${NC}"
fi

if [ -z "$GIT_USER_EMAIL" ]; then
    echo -e "${YELLOW}请输入您的 Git 邮箱:${NC}"
    read -r USER_EMAIL
    git config user.email "$USER_EMAIL"
    echo -e "${GREEN}已设置邮箱: $USER_EMAIL${NC}"
fi

# 显示当前 Git 配置
echo -e "\n${CYAN}当前 Git 配置:${NC}"
echo -e "用户名: $(git config user.name)"
echo -e "邮箱: $(git config user.email)"

# 添加所有文件到暂存区
echo -e "\n${CYAN}正在添加文件到 Git...${NC}"
git add .

# 显示状态
echo -e "\n${CYAN}Git 状态:${NC}"
git status --short | head -20
FILE_COUNT=$(git status --short | wc -l)
if [ "$FILE_COUNT" -gt 20 ]; then
    echo -e "... 和其他 $((FILE_COUNT - 20)) 个文件"
fi

# 询问是否进行首次提交
echo -e "\n${YELLOW}是否要进行首次提交? (y/N)${NC}"
read -r CONFIRM

if [ "$CONFIRM" = "y" ] || [ "$CONFIRM" = "Y" ]; then
    echo -e "${CYAN}正在进行首次提交...${NC}"
    if git commit -m "Initial commit: OceanBase database source code"; then
        echo -e "\n${GREEN}✓ Git 仓库初始化成功!${NC}"
        echo -e "${GREEN}✓ 已完成首次提交${NC}"
    else
        echo -e "\n${GREEN}✓ Git 仓库初始化成功!${NC}"
        echo -e "${YELLOW}! 首次提交失败，请手动执行: git commit -m 'Initial commit'${NC}"
    fi
else
    echo -e "\n${GREEN}✓ Git 仓库初始化成功!${NC}"
    echo -e "${YELLOW}提示: 您可以稍后手动提交，执行命令:${NC}"
    echo -e "  git commit -m 'Initial commit: OceanBase database source code'"
fi

echo -e "\n${CYAN}常用 Git 命令:${NC}"
echo -e "  git status          - 查看仓库状态"
echo -e "  git add <file>      - 添加文件到暂存区"
echo -e "  git commit -m 'msg' - 提交更改"
echo -e "  git log             - 查看提交历史"
echo -e "  git branch          - 查看分支"
echo -e "  git checkout -b <branch> - 创建并切换分支"
echo -e "\n${CYAN}详细使用指南请查看: GIT_GUIDE.md${NC}"
