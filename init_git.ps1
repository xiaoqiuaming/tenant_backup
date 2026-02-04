# Git 初始化脚本
# 为 SOURCE 项目设置 Git 版本控制

Write-Host "开始初始化 Git 仓库..." -ForegroundColor Green

# 检查是否已经是 Git 仓库
if (Test-Path ".git") {
    Write-Host "警告: .git 目录已存在，删除后重新初始化..." -ForegroundColor Yellow
    Remove-Item -Path ".git" -Recurse -Force
}

# 初始化 Git 仓库
Write-Host "正在初始化 Git 仓库..." -ForegroundColor Cyan
git init

if ($LASTEXITCODE -ne 0) {
    Write-Host "错误: Git 初始化失败。请确保已安装 Git。" -ForegroundColor Red
    Write-Host "可以从 https://git-scm.com/downloads 下载安装 Git" -ForegroundColor Yellow
    exit 1
}

# 配置 Git 用户信息（如果尚未配置）
$gitUserName = git config user.name
$gitUserEmail = git config user.email

if ([string]::IsNullOrEmpty($gitUserName)) {
    Write-Host "请输入您的 Git 用户名:" -ForegroundColor Yellow
    $userName = Read-Host
    git config user.name "$userName"
    Write-Host "已设置用户名: $userName" -ForegroundColor Green
}

if ([string]::IsNullOrEmpty($gitUserEmail)) {
    Write-Host "请输入您的 Git 邮箱:" -ForegroundColor Yellow
    $userEmail = Read-Host
    git config user.email "$userEmail"
    Write-Host "已设置邮箱: $userEmail" -ForegroundColor Green
}

# 显示当前 Git 配置
Write-Host "`n当前 Git 配置:" -ForegroundColor Cyan
Write-Host "用户名: $(git config user.name)" -ForegroundColor White
Write-Host "邮箱: $(git config user.email)" -ForegroundColor White

# 添加所有文件到暂存区
Write-Host "`n正在添加文件到 Git..." -ForegroundColor Cyan
git add .

if ($LASTEXITCODE -ne 0) {
    Write-Host "警告: 部分文件添加失败" -ForegroundColor Yellow
}

# 显示状态
Write-Host "`nGit 状态:" -ForegroundColor Cyan
git status --short

# 询问是否进行首次提交
Write-Host "`n是否要进行首次提交? (Y/N)" -ForegroundColor Yellow
$confirm = Read-Host

if ($confirm -eq 'Y' -or $confirm -eq 'y') {
    Write-Host "正在进行首次提交..." -ForegroundColor Cyan
    git commit -m "Initial commit: OceanBase database source code"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✓ Git 仓库初始化成功!" -ForegroundColor Green
        Write-Host "✓ 已完成首次提交" -ForegroundColor Green
    } else {
        Write-Host "`n✓ Git 仓库初始化成功!" -ForegroundColor Green
        Write-Host "! 首次提交失败，请手动执行: git commit -m 'Initial commit'" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n✓ Git 仓库初始化成功!" -ForegroundColor Green
    Write-Host "提示: 您可以稍后手动提交，执行命令:" -ForegroundColor Yellow
    Write-Host "  git commit -m 'Initial commit: OceanBase database source code'" -ForegroundColor White
}

Write-Host "`n常用 Git 命令:" -ForegroundColor Cyan
Write-Host "  git status          - 查看仓库状态" -ForegroundColor White
Write-Host "  git add <file>      - 添加文件到暂存区" -ForegroundColor White
Write-Host "  git commit -m 'msg' - 提交更改" -ForegroundColor White
Write-Host "  git log             - 查看提交历史" -ForegroundColor White
Write-Host "  git branch          - 查看分支" -ForegroundColor White
Write-Host "  git checkout -b <branch> - 创建并切换分支" -ForegroundColor White
