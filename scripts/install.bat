@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
echo ========================================
echo   Accrual Bot 安裝程式
echo ========================================
echo.

set "INSTALL_DIR=%~dp0"
set "WORKSPACE=%INSTALL_DIR%workspace"
set "PYTHONNOUSERSITE=1"
set "PYTHONDONTWRITEBYTECODE=1"

:: ============================================
:: Step 0: 偵測系統 Python
:: ============================================
set "PYTHON_CMD="

where python >nul 2>&1
if errorlevel 1 goto :NO_SYSTEM_PYTHON

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set "PY_VER=%%v"
echo [INFO] 偵測到系統 Python !PY_VER!

:: 解析主版本和次版本
for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
    set "PY_MAJOR=%%a"
    set "PY_MINOR=%%b"
)

:: 檢查版本 >= 3.11
if !PY_MAJOR! GEQ 3 if !PY_MINOR! GEQ 11 set "PYTHON_CMD=python"

if defined PYTHON_CMD goto :USE_SYSTEM_PYTHON

:NO_SYSTEM_PYTHON
echo [INFO] 未偵測到 Python 3.11+，將下載 Embedded Python...
goto :USE_EMBEDDED

:: ============================================
:: 路線 A: 系統 Python → venv
:: ============================================
:USE_SYSTEM_PYTHON
echo.
echo [1/3] 建立虛擬環境...
!PYTHON_CMD! -m venv "%INSTALL_DIR%accrual_venv"
call "%INSTALL_DIR%accrual_venv\Scripts\activate.bat"

echo [2/3] 安裝 Accrual Bot...
pip install "accrual-bot @ git+https://github.com/Allen15763/accrual_bot.git"

echo [3/3] 初始化工作區...
set "ACCRUAL_BOT_WORKSPACE=%WORKSPACE%"
accrual-bot init

goto :DONE

:: ============================================
:: 路線 B: Embedded Python
:: ============================================
:USE_EMBEDDED
set "PY_DIR=%INSTALL_DIR%python311"

if exist "%PY_DIR%\python.exe" (
    echo [INFO] 已存在 Embedded Python，跳過下載
    goto :SETUP_PTH
)

echo.
echo [1/5] 下載 Python 3.11.9 Embedded...
mkdir "%PY_DIR%" 2>nul
curl -L -o "%PY_DIR%\python.zip" ^
    "https://www.python.org/ftp/python/3.11.9/python-3.11.9-embed-amd64.zip"
if errorlevel 1 (
    echo [ERROR] 下載失敗！請檢查網路連線。
    goto :ERROR
)
tar -xf "%PY_DIR%\python.zip" -C "%PY_DIR%"
del "%PY_DIR%\python.zip"

:SETUP_PTH
echo [2/5] 配置 Python 環境...

:: 建立 Lib\site-packages 目錄
mkdir "%PY_DIR%\Lib\site-packages" 2>nul

:: 重寫 _pth 檔案（必須完整重寫，不可 append）
(
    echo python311.zip
    echo .
    echo Lib
    echo Lib\site-packages
    echo import site
) > "%PY_DIR%\python311._pth"

echo [3/5] 安裝 pip...
curl -L -o "%PY_DIR%\get-pip.py" "https://bootstrap.pypa.io/get-pip.py"
"%PY_DIR%\python.exe" "%PY_DIR%\get-pip.py" --no-warn-script-location
del "%PY_DIR%\get-pip.py"

:: 驗證 pip
"%PY_DIR%\python.exe" -m pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pip 安裝失敗！
    goto :ERROR
)

echo [4/5] 安裝 Accrual Bot...
"%PY_DIR%\python.exe" -m pip install ^
    "accrual-bot @ git+https://github.com/Allen15763/accrual_bot.git" ^
    --no-warn-script-location
if errorlevel 1 (
    echo [ERROR] 套件安裝失敗！
    goto :ERROR
)

echo [5/5] 初始化工作區...
set "ACCRUAL_BOT_WORKSPACE=%WORKSPACE%"
"%PY_DIR%\Scripts\accrual-bot.exe" init

:DONE
echo.
echo ========================================
echo   安裝完成！
echo ========================================
echo.
echo   Setup:
echo   1. Put credentials.json in %WORKSPACE%\secret\
echo      or set env var ACCRUAL_BOT_CREDENTIALS
echo   2. Edit %WORKSPACE%\config\paths.local.toml
echo   3. Double-click run.bat to launch
echo.
pause
exit /b 0

:ERROR
echo.
echo 安裝失敗，請聯繫開發團隊。
pause
exit /b 1
