"""
Configuration Page - Streamlit Entry Point

此檔案是 Streamlit multi-page 的進入點，實際邏輯在 accrual_bot.ui.pages 中。
"""

# 直接導入並執行實際的頁面模組
import sys
from pathlib import Path

# 確保可以導入 accrual_bot
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 導入實際的頁面實作
from accrual_bot.ui.pages import configuration_page

# 執行頁面
configuration_page.render()
