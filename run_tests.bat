@echo off
REM 測試運行腳本 (Windows PowerShell)
REM 使用方式: .\run_tests.bat [選項]
REM   無參數: 運行所有測試
REM   unit: 只運行單元測試
REM   integration: 只運行集成測試
REM   coverage: 生成覆蓋率報告

REM 啟動虛擬環境
call venv\Scripts\activate

if "%1"=="unit" (
    echo === Running Unit Tests ===
    pytest tests\unit\ -v -m unit
    goto end
)

if "%1"=="integration" (
    echo === Running Integration Tests ===
    pytest tests\integration\ -v -m integration
    goto end
)

if "%1"=="coverage" (
    echo === Generating Coverage Report ===
    pytest tests\ --cov=accrual_bot --cov-report=html --cov-report=term-missing
    echo.
    echo Coverage report: htmlcov\index.html
    goto end
)

REM 默認：運行所有測試
echo === Running All Tests ===
pytest tests\ -v

:end
