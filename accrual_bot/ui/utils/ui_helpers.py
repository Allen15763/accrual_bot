"""
UI Helper Functions

提供 UI 使用的輔助函數。
"""

from datetime import datetime
from typing import Optional


def format_date(date_int: int) -> str:
    """
    格式化 YYYYMM 日期為易讀格式

    Args:
        date_int: YYYYMM 格式的日期整數 (例如: 202512)

    Returns:
        格式化的日期字串 (例如: "2025年12月")
    """
    if date_int == 0:
        return "未設定"

    year = date_int // 100
    month = date_int % 100
    return f"{year}年{month:02d}月"


def format_duration(seconds: float) -> str:
    """
    格式化執行時間

    Args:
        seconds: 秒數

    Returns:
        格式化的時間字串 (例如: "1分23秒")
    """
    if seconds < 1:
        return f"{seconds:.2f}秒"
    elif seconds < 60:
        return f"{seconds:.1f}秒"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}分{secs:02d}秒"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}小時{minutes:02d}分"


def get_status_icon(status: str) -> str:
    """
    根據狀態返回對應的 icon

    Args:
        status: 狀態字串

    Returns:
        對應的 emoji icon
    """
    status_icons = {
        'idle': '⏸️',
        'running': '▶️',
        'completed': '✅',
        'failed': '❌',
        'paused': '⏸️',
        'pending': '⏳',
        'success': '✅',
        'error': '❌',
        'warning': '⚠️',
        'info': 'ℹ️',
    }
    return status_icons.get(status.lower(), '❓')


def truncate_text(text: str, max_length: int = 50) -> str:
    """
    截斷文字並加上省略號

    Args:
        text: 原始文字
        max_length: 最大長度

    Returns:
        截斷後的文字
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def format_file_size(size_bytes: int) -> str:
    """
    格式化檔案大小

    Args:
        size_bytes: 檔案大小 (bytes)

    Returns:
        格式化的檔案大小字串
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.1f} GB"
