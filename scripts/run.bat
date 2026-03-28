@echo off
setlocal
chcp 65001 >nul

set "INSTALL_DIR=%~dp0"
set "PYTHONNOUSERSITE=1"
set "ACCRUAL_BOT_WORKSPACE=%INSTALL_DIR%workspace"

:: Google Sheets credentials 路徑（修改為實際路徑）
set "ACCRUAL_BOT_CREDENTIALS=G:\共用雲端硬碟\INT_TWN_SEA_FN_Shared_Resources\00_Temp_Internal_share\To Allen\libraries\credentials.json"

:: 判斷使用 venv 或 embedded
if exist "%INSTALL_DIR%accrual_venv\Scripts\activate.bat" (
    call "%INSTALL_DIR%accrual_venv\Scripts\activate.bat"
    accrual-bot
) else if exist "%INSTALL_DIR%python311\Scripts\accrual-bot.exe" (
    "%INSTALL_DIR%python311\Scripts\accrual-bot.exe"
) else (
    echo [ERROR] 找不到 Accrual Bot，請先執行 install.bat
    pause
)
