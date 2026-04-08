"""
UI Configuration Constants

定義 UI 專屬的設定常數（頁面配置、主題色彩）。
業務邏輯常數（entity、檔案需求等）已搬移至 accrual_bot.tasks.config，
此處 re-export 以維持向後相容。
"""

from typing import Dict, List, Tuple

# === 業務常數 re-export（向後相容） ===
from accrual_bot.tasks.config import (  # noqa: F401
    ENTITY_CONFIG,
    PROCESSING_TYPE_CONFIG,
    PROCUREMENT_SOURCE_TYPES,
    FILE_LABELS,
    REQUIRED_FILES,
    OPTIONAL_FILES,
    SUPPORTED_FILE_FORMATS,
    get_file_requirements,
)

# UI 主題色彩
THEME_COLORS: Dict[str, str] = {
    'primary': '#0068C9',
    'success': '#09AB3B',
    'warning': '#FFA500',
    'error': '#FF2B2B',
    'info': '#00C0F2',
}

# Streamlit 頁面設定
PAGE_CONFIG = {
    'page_title': 'Accrual Bot',
    'page_icon': '📊',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded',
}
