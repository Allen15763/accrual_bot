#!/bin/bash
# 僅執行 integration 測試
cd "$(dirname "$0")/.." || exit 1
python -m pytest tests/ -m integration -v --cov=accrual_bot --cov-report=term-missing "$@"
