@echo off
REM 清空 GitHub 仓库中所有已跟踪文件（本地工作区文件保留，仅取消 Git 跟踪并推送空树）
chcp 65001 >nul 2>&1
cd /d "%~dp0"

set "REPO_URL=https://github.com/Jian11323/EEWCN-Custom-Data-Console.git"
set "BRANCH=main"
set "CLEAR_MSG=清空仓库"

echo ========================================
echo 清空 GitHub 仓库（远程将无项目文件）
echo 仓库: %REPO_URL%
echo 分支: %BRANCH%
echo 本地文件不会被删除，仅停止 Git 跟踪
echo ========================================
echo.
echo [警告] 此操作会向远程推送「清空仓库」提交，请确认已备份需要保留的内容。
echo.
choice /C YN /M "是否继续"
if errorlevel 2 (
    echo 已取消
    pause
    exit /b 0
)

echo.
echo [Step 0] Git 用户配置
git config --global user.name "Jian11323"
git config --global user.email "mazhiyuan401@163.com"

git --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Git
    pause
    exit /b 1
)

if not exist ".git" (
    echo [错误] 当前目录不是 Git 仓库，请先在本目录执行 sync_github.bat 或 git init
    pause
    exit /b 1
)

git remote get-url origin >nul 2>&1
if errorlevel 1 (
    git remote add origin "%REPO_URL%"
) else (
    git remote set-url origin "%REPO_URL%"
)

echo [Step 1] 移除所有已跟踪文件...
git ls-files >"%TEMP%\git_tracked_files.txt" 2>nul
set "HAS_TRACKED=0"
for /f "usebackq delims=" %%F in ("%TEMP%\git_tracked_files.txt") do (
    set "HAS_TRACKED=1"
    git rm -f --cached -- "%%F" >nul 2>&1
    if errorlevel 1 git rm -f -- "%%F" >nul 2>&1
)
del "%TEMP%\git_tracked_files.txt" 2>nul

git add -A
git status

echo [Step 2] 提交: %CLEAR_MSG%
git diff --cached --quiet
if not errorlevel 1 (
    git commit --allow-empty -m "%CLEAR_MSG%"
) else (
    git commit -m "%CLEAR_MSG%"
)
if errorlevel 1 (
    echo [错误] git commit 失败
    pause
    exit /b 1
)

echo [Step 3] 推送到 origin/%BRANCH% ...
git push -u origin %BRANCH%
if errorlevel 1 (
    echo [错误] 推送失败
    pause
    exit /b 1
)

echo.
echo ========================================
echo 远程仓库已清空（提交说明: %CLEAR_MSG%）
echo 本地源码仍在，可再次运行 sync_github.bat 上传
echo ========================================
pause
exit /b 0
