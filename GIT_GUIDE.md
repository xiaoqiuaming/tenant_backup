# Git 版本控制指南

## 初始化 Git 仓库

本项目已配置好 `.gitignore` 文件，可以直接初始化 Git 仓库。

### 方式一：使用初始化脚本（推荐）

在项目根目录下运行：

```powershell
.\init_git.ps1
```

该脚本会自动完成以下操作：
1. 初始化 Git 仓库
2. 配置用户信息（如果需要）
3. 添加所有文件到暂存区
4. 提供首次提交选项

### 方式二：手动初始化

```bash
# 1. 初始化 Git 仓库
git init

# 2. 配置用户信息（首次使用 Git 时需要）
git config user.name "Your Name"
git config user.email "your.email@example.com"

# 3. 添加所有文件
git add .

# 4. 首次提交
git commit -m "Initial commit: OceanBase database source code"
```

## Git 基本工作流程

### 1. 查看状态
```bash
git status
```

### 2. 添加文件到暂存区
```bash
# 添加特定文件
git add <file_path>

# 添加所有修改的文件
git add .

# 添加所有 .cpp 文件
git add *.cpp
```

### 3. 提交更改
```bash
git commit -m "描述你的更改"
```

### 4. 查看历史
```bash
# 查看提交历史
git log

# 查看简洁的提交历史
git log --oneline

# 查看最近 5 条提交
git log -5
```

### 5. 查看差异
```bash
# 查看工作区与暂存区的差异
git diff

# 查看暂存区与最后一次提交的差异
git diff --staged

# 查看两个提交之间的差异
git diff <commit1> <commit2>
```

## 分支管理

### 创建分支
```bash
# 创建新分支
git branch <branch_name>

# 创建并切换到新分支
git checkout -b <branch_name>

# 或使用新语法
git switch -c <branch_name>
```

### 切换分支
```bash
git checkout <branch_name>

# 或使用新语法
git switch <branch_name>
```

### 查看分支
```bash
# 查看本地分支
git branch

# 查看所有分支（包括远程）
git branch -a
```

### 合并分支
```bash
# 先切换到目标分支（如 main）
git checkout main

# 合并其他分支
git merge <branch_name>
```

### 删除分支
```bash
# 删除已合并的分支
git branch -d <branch_name>

# 强制删除分支
git branch -D <branch_name>
```

## 远程仓库操作

### 添加远程仓库
```bash
git remote add origin <repository_url>
```

### 查看远程仓库
```bash
git remote -v
```

### 推送到远程仓库
```bash
# 首次推送
git push -u origin main

# 后续推送
git push
```

### 从远程仓库拉取
```bash
# 拉取并合并
git pull

# 仅拉取不合并
git fetch
```

## 撤销操作

### 撤销工作区的修改
```bash
# 撤销单个文件的修改
git checkout -- <file>

# 撤销所有修改
git checkout -- .
```

### 撤销暂存区的文件
```bash
# 将文件从暂存区移除（保留工作区修改）
git reset HEAD <file>

# 撤销所有暂存
git reset HEAD .
```

### 撤销提交
```bash
# 撤销最后一次提交，保留修改在工作区
git reset --soft HEAD^

# 撤销最后一次提交，保留修改在暂存区
git reset --mixed HEAD^

# 撤销最后一次提交，丢弃所有修改
git reset --hard HEAD^
```

### 修改最后一次提交
```bash
# 修改提交信息
git commit --amend -m "新的提交信息"

# 添加遗漏的文件到最后一次提交
git add <forgotten_file>
git commit --amend --no-edit
```

## 标签管理

### 创建标签
```bash
# 创建轻量标签
git tag v1.0.0

# 创建附注标签（推荐）
git tag -a v1.0.0 -m "Release version 1.0.0"

# 为历史提交创建标签
git tag -a v0.9.0 <commit_id> -m "Release version 0.9.0"
```

### 查看标签
```bash
# 列出所有标签
git tag

# 查看标签详情
git show v1.0.0
```

### 推送标签
```bash
# 推送单个标签
git push origin v1.0.0

# 推送所有标签
git push origin --tags
```

### 删除标签
```bash
# 删除本地标签
git tag -d v1.0.0

# 删除远程标签
git push origin --delete v1.0.0
```

## 项目特定说明

### .gitignore 配置

本项目已配置 `.gitignore` 文件，主要忽略：

- 编译生成的文件（`*.o`, `*.a`, `*.lo`, `*.la`）
- 构建目录和文件（`Makefile`, `.deps/`, `.libs/`）
- 临时文件（`*.tmp`, `*.pyc`）
- 二进制可执行文件（各种服务器和工具）
- 自动生成的配置文件（`configure`, `config.log` 等）
- 打包文件（`*.tar.gz`, `*.rpm`）
- Git 版本相关文件（`git_version.c`, `git_version.cpp`）

### 推荐的分支策略

```
main (或 master)     - 主分支，存储正式发布的历史
  ├─ develop          - 开发分支，集成功能分支
  ├─ feature/*        - 功能分支，开发新功能
  ├─ hotfix/*         - 热修复分支，紧急修复生产问题
  └─ release/*        - 发布分支，准备新的生产版本
```

### 提交信息规范

推荐使用规范的提交信息格式：

```
<type>(<scope>): <subject>

<body>

<footer>
```

类型（type）：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式化（不影响代码逻辑）
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

示例：
```bash
git commit -m "feat(sql): add support for new SQL syntax"
git commit -m "fix(rootserver): fix memory leak in checkpoint"
git commit -m "docs: update README with build instructions"
```

## 常见问题

### Q: 如何查看文件的修改历史？
```bash
git log --follow -p <file>
```

### Q: 如何暂存当前工作进度？
```bash
# 暂存
git stash

# 查看暂存列表
git stash list

# 恢复暂存
git stash pop

# 应用暂存但不删除
git stash apply
```

### Q: 如何忽略已经跟踪的文件？
```bash
# 从 Git 中移除但保留在本地
git rm --cached <file>

# 然后添加到 .gitignore
echo "<file>" >> .gitignore

# 提交更改
git commit -m "Stop tracking <file>"
```

### Q: 如何清理未跟踪的文件？
```bash
# 查看将要删除的文件
git clean -n

# 删除未跟踪的文件
git clean -f

# 删除未跟踪的文件和目录
git clean -fd
```

## 有用的 Git 配置

```bash
# 设置默认编辑器
git config --global core.editor "code --wait"

# 启用颜色输出
git config --global color.ui auto

# 设置默认分支名称
git config --global init.defaultBranch main

# 设置行尾符（Windows）
git config --global core.autocrlf true

# 配置别名
git config --global alias.st status
git config --global alias.co checkout
git config --global alias.br branch
git config --global alias.ci commit
git config --global alias.lg "log --graph --oneline --decorate --all"
```

## 参考资源

- [Git 官方文档](https://git-scm.com/doc)
- [Pro Git 书籍（中文版）](https://git-scm.com/book/zh/v2)
- [GitHub Git 备忘清单](https://training.github.com/downloads/zh_CN/github-git-cheat-sheet/)
- [Learn Git Branching（交互式教程）](https://learngitbranching.js.org/?locale=zh_CN)
