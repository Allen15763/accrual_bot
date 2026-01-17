"""
Configuration Page - Streamlit Entry Point

此檔案是 Streamlit multi-page 的進入點，直接執行實際頁面邏輯。
"""

import sys
from pathlib import Path

# 確保可以導入 accrual_bot
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 直接執行實際的頁面檔案
actual_page = project_root / "accrual_bot" / "ui" / "pages" / "1_configuration.py"
exec(open(actual_page, encoding='utf-8').read())
