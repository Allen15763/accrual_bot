#!/bin/bash
# 僅執行 unit 測試
cd "$(dirname "$0")/.." || exit 1
python -m pytest tests/ -m unit -v --cov=accrual_bot --cov-report=term-missing "$@"
