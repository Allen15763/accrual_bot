@echo off
setlocal
chcp 65001 >nul

set "INSTALL_DIR=%~dp0"
set "PYTHONNOUSERSITE=1"

echo ========================================
echo   Accrual Bot 更新
echo ========================================
echo.

:: 判斷使用 venv 或 embedded
if exist "%INSTALL_DIR%accrual_venv\Scripts\activate.bat" (
    call "%INSTALL_DIR%accrual_venv\Scripts\activate.bat"
    pip install --upgrade "accrual-bot @ git+https://github.com/Allen15763/accrual_bot.git@packaging"
) else if exist "%INSTALL_DIR%python311\python.exe" (
    "%INSTALL_DIR%python311\python.exe" -m pip install --upgrade ^
        "accrual-bot @ git+https://github.com/Allen15763/accrual_bot.git@packaging" ^
        --no-warn-script-location
) else (
    echo [ERROR] 找不到安裝環境，請先執行 install.bat
    pause
    exit /b 1
)

echo.
echo 更新完成！
pause
