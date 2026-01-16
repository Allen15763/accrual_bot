"""
File Upload Page - Streamlit Entry Point

此檔案是 Streamlit multi-page 的進入點，實際邏輯在 accrual_bot.ui.pages 中。
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from accrual_bot.ui.pages import file_upload_page

file_upload_page.render()
