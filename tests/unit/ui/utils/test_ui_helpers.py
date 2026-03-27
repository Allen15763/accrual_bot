"""
UI Helper Functions 單元測試

測試 ui_helpers 模組中所有輔助函數的正確性，
包含邊界條件、零值、無效輸入等情境。
"""

import pytest

from accrual_bot.ui.utils.ui_helpers import (
    format_date,
    format_duration,
    format_file_size,
    get_status_icon,
    truncate_text,
)


@pytest.mark.unit
class TestFormatDate:
    """測試 format_date() 函數"""

    @pytest.mark.parametrize(
        "date_int, expected",
        [
            (202512, "2025年12月"),
            (202501, "2025年01月"),
            (202306, "2023年06月"),
            (200001, "2000年01月"),
        ],
        ids=["十二月", "一月", "六月", "千禧年"],
    )
    def test_format_date_normal(self, date_int, expected):
        """正常 YYYYMM 日期格式化"""
        assert format_date(date_int) == expected

    def test_format_date_zero(self):
        """日期為 0 時應返回未設定"""
        assert format_date(0) == "未設定"

    def test_format_date_large_year(self):
        """大年份應正確處理"""
        assert format_date(999912) == "9999年12月"


@pytest.mark.unit
class TestFormatDuration:
    """測試 format_duration() 函數"""

    def test_sub_second(self):
        """小於 1 秒應顯示兩位小數"""
        assert format_duration(0.05) == "0.05秒"

    def test_zero_seconds(self):
        """零秒邊界條件"""
        assert format_duration(0.0) == "0.00秒"

    def test_seconds_range(self):
        """1 到 60 秒之間應顯示一位小數"""
        assert format_duration(30.5) == "30.5秒"

    def test_exactly_one_second(self):
        """恰好 1 秒的邊界"""
        assert format_duration(1.0) == "1.0秒"

    def test_minutes_range(self):
        """60 秒以上應顯示分秒格式"""
        assert format_duration(83) == "1分23秒"

    def test_exactly_one_minute(self):
        """恰好 60 秒的邊界"""
        assert format_duration(60) == "1分00秒"

    def test_hours_range(self):
        """3600 秒以上應顯示小時分鐘格式"""
        assert format_duration(3661) == "1小時01分"

    def test_exactly_one_hour(self):
        """恰好 3600 秒的邊界"""
        assert format_duration(3600) == "1小時00分"


@pytest.mark.unit
class TestGetStatusIcon:
    """測試 get_status_icon() 函數"""

    @pytest.mark.parametrize(
        "status, expected_icon",
        [
            ("idle", "\u23f8\ufe0f"),
            ("running", "\u25b6\ufe0f"),
            ("completed", "\u2705"),
            ("failed", "\u274c"),
            ("pending", "\u23f3"),
            ("success", "\u2705"),
            ("error", "\u274c"),
            ("warning", "\u26a0\ufe0f"),
            ("info", "\u2139\ufe0f"),
        ],
    )
    def test_known_statuses(self, status, expected_icon):
        """已知狀態應返回對應圖示"""
        assert get_status_icon(status) == expected_icon

    def test_case_insensitive(self):
        """狀態字串應不區分大小寫"""
        assert get_status_icon("RUNNING") == get_status_icon("running")
        assert get_status_icon("Completed") == get_status_icon("completed")

    def test_unknown_status(self):
        """未知狀態應返回問號圖示"""
        assert get_status_icon("unknown") == "\u2753"
        assert get_status_icon("something_else") == "\u2753"


@pytest.mark.unit
class TestTruncateText:
    """測試 truncate_text() 函數"""

    def test_short_text_unchanged(self):
        """短於最大長度的文字不應被截斷"""
        assert truncate_text("hello", 50) == "hello"

    def test_exact_length_unchanged(self):
        """恰好等於最大長度的文字不應被截斷"""
        text = "a" * 50
        assert truncate_text(text, 50) == text

    def test_long_text_truncated(self):
        """超過最大長度的文字應被截斷並加省略號"""
        text = "a" * 60
        result = truncate_text(text, 50)
        assert len(result) == 50
        assert result.endswith("...")

    def test_default_max_length(self):
        """預設最大長度為 50"""
        text = "a" * 51
        result = truncate_text(text)
        assert len(result) == 50
        assert result.endswith("...")

    def test_empty_string(self):
        """空字串應原樣返回"""
        assert truncate_text("", 50) == ""


@pytest.mark.unit
class TestFormatFileSize:
    """測試 format_file_size() 函數"""

    @pytest.mark.parametrize(
        "size_bytes, expected",
        [
            (0, "0 B"),
            (512, "512 B"),
            (1023, "1023 B"),
            (1024, "1.0 KB"),
            (1536, "1.5 KB"),
            (1048576, "1.0 MB"),
            (1572864, "1.5 MB"),
            (1073741824, "1.0 GB"),
            (1610612736, "1.5 GB"),
        ],
        ids=[
            "零位元組",
            "512B",
            "接近1KB邊界",
            "恰好1KB",
            "1.5KB",
            "恰好1MB",
            "1.5MB",
            "恰好1GB",
            "1.5GB",
        ],
    )
    def test_format_file_size(self, size_bytes, expected):
        """各級別檔案大小格式化"""
        assert format_file_size(size_bytes) == expected
