@echo off
REM 同步本地项目到 GitHub（添加、提交、推送）
chcp 65001 >nul 2>&1
cd /d "%~dp0"

set "REPO_URL=https://github.com/Jian11323/EEWCN-Custom-Data-Console.git"
set "BRANCH=main"
set "DEFAULT_MSG=创建文件"

echo ========================================
echo 同步到 GitHub
echo 仓库: %REPO_URL%
echo 分支: %BRANCH%
echo ========================================
echo.

echo [Step 0] Git 用户配置
git config --global user.name "Jian11323"
git config --global user.email "mazhiyuan401@163.com"
if errorlevel 1 (
    echo [错误] git config 失败
    pause
    exit /b 1
)

git --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Git，请安装并加入 PATH
    pause
    exit /b 1
)

if not exist ".git" (
    echo [Step 1] 初始化本地仓库...
    git init -b %BRANCH%
) else (
    echo [Step 1] 已存在本地仓库
)

git remote get-url origin >nul 2>&1
if errorlevel 1 (
    echo [Step 2] 添加远程 origin...
    git remote add origin "%REPO_URL%"
) else (
    echo [Step 2] 更新远程 origin 地址...
    git remote set-url origin "%REPO_URL%"
)

echo.
echo [Step 3] 添加变更（遵循 .gitignore）...
git add -A
git status
if errorlevel 1 (
    echo [错误] git add 失败
    pause
    exit /b 1
)

git diff --cached --quiet
if not errorlevel 1 (
    echo.
    echo [提示] 没有需要提交的变更，跳过 commit
    goto :push
)

echo.
set "COMMIT_MSG=%DEFAULT_MSG%"
set /p COMMIT_MSG=提交说明（直接回车使用「%DEFAULT_MSG%」）: 
if "%COMMIT_MSG%"=="" set "COMMIT_MSG=%DEFAULT_MSG%"

echo [Step 4] 提交: %COMMIT_MSG%
git commit -m "%COMMIT_MSG%"
if errorlevel 1 (
    echo [错误] git commit 失败
    pause
    exit /b 1
)

:push
echo.
echo [Step 5] 推送到 origin/%BRANCH% ...
git push -u origin %BRANCH%
if errorlevel 1 (
    echo.
    echo [错误] 推送失败。若远程已有不同历史，请先备份后处理冲突，或按需使用 clear_github_repo.bat 清空远程后再同步。
    pause
    exit /b 1
)

echo.
echo ========================================
echo 同步完成
echo ========================================
pause
exit /b 0
