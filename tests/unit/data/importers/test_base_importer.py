"""
Tests for accrual_bot.data.importers.base_importer.BaseDataImporter

驗證基礎數據導入器的各項功能。
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path


@pytest.mark.unit
class TestBaseDataImporter:
    """BaseDataImporter 測試"""

    @pytest.fixture
    def importer(self):
        """建立 BaseDataImporter 實例（mock 外部依賴）"""
        with patch("accrual_bot.data.importers.base_importer.get_logger") as mock_logger, \
             patch("accrual_bot.data.importers.base_importer.CONCURRENT_SETTINGS",
                   {"MAX_WORKERS": 4, "TIMEOUT": 60}):
            mock_logger.return_value = MagicMock()
            from accrual_bot.data.importers.base_importer import BaseDataImporter
            instance = BaseDataImporter()
            yield instance

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """建立測試用 CSV 檔案"""
        csv_file = tmp_path / "test_202512_data.csv"
        df = pd.DataFrame({"col_a": ["1", "2", "3"], "col_b": ["a", "b", "c"]})
        df.to_csv(csv_file, index=False, encoding="utf-8")
        return str(csv_file)

    @pytest.fixture
    def sample_excel(self, tmp_path):
        """建立測試用 Excel 檔案"""
        xlsx_file = tmp_path / "test_202503_report.xlsx"
        df = pd.DataFrame({"x": [10, 20], "y": [30, 40]})
        df.to_excel(xlsx_file, index=False, engine="openpyxl")
        return str(xlsx_file)

    # --- __init__ ---

    def test_init_sets_attributes(self, importer):
        """驗證初始化設定 logger、max_workers、timeout"""
        assert importer.logger is not None
        assert importer.max_workers == 4
        assert importer.timeout == 60

    # --- import_file ---

    def test_import_csv_file(self, importer, sample_csv):
        """驗證導入 CSV 檔案"""
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=True):
            df = importer.import_file(sample_csv)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert list(df.columns) == ["col_a", "col_b"]

    def test_import_excel_file(self, importer, sample_excel):
        """驗證導入 Excel 檔案"""
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=False):
            df = importer.import_file(sample_excel)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert "x" in df.columns

    def test_import_file_invalid_path_raises(self, importer):
        """驗證無效路徑拋出 ValueError"""
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=False):
            with pytest.raises(ValueError, match="無效的檔案路徑"):
                importer.import_file("/nonexistent/file.csv")

    def test_import_file_unsupported_format_raises(self, importer, tmp_path):
        """驗證不支援的格式拋出 ValueError"""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("hello")
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.get_file_extension", return_value=".txt"):
            with pytest.raises(ValueError, match="不支援的檔案格式"):
                importer.import_file(str(txt_file))

    # --- _import_csv encoding fallback ---

    def test_import_csv_encoding_fallback(self, importer, tmp_path):
        """驗證 CSV 編碼錯誤時嘗試其他編碼"""
        csv_file = tmp_path / "big5_data.csv"
        df = pd.DataFrame({"col": ["測試"]})
        df.to_csv(csv_file, index=False, encoding="big5")

        # 直接呼叫 _import_csv，使用 ascii 會失敗然後 fallback
        result = importer._import_csv(str(csv_file), encoding="ascii")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    # --- import_multiple_files ---

    def test_import_multiple_files_empty_list(self, importer):
        """驗證空列表回傳空字典"""
        result = importer.import_multiple_files([])
        assert result == {}

    def test_import_multiple_files(self, importer, sample_csv, sample_excel):
        """驗證批量導入多個檔案"""
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", side_effect=lambda p: p.endswith(".xlsx")), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", side_effect=lambda p: p.endswith(".csv")):
            results = importer.import_multiple_files([sample_csv, sample_excel])
            assert len(results) == 2

    # --- validate_dataframe ---

    def test_validate_dataframe_valid(self, importer):
        """驗證有效的 DataFrame"""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        assert importer.validate_dataframe(df, required_columns=["a", "b"], min_rows=1) is True

    def test_validate_dataframe_empty(self, importer):
        """驗證空 DataFrame 回傳 False"""
        df = pd.DataFrame()
        assert importer.validate_dataframe(df) is False

    def test_validate_dataframe_missing_columns(self, importer):
        """驗證缺少必要列回傳 False"""
        df = pd.DataFrame({"a": [1]})
        assert importer.validate_dataframe(df, required_columns=["a", "missing"]) is False

    def test_validate_dataframe_insufficient_rows(self, importer):
        """驗證行數不足回傳 False"""
        df = pd.DataFrame({"a": [1]})
        assert importer.validate_dataframe(df, min_rows=5) is False

    # --- get_import_statistics ---

    def test_get_import_statistics_empty(self, importer):
        """驗證空結果回傳零值統計"""
        stats = importer.get_import_statistics({})
        assert stats["total_files"] == 0
        assert stats["total_rows"] == 0

    def test_get_import_statistics(self, importer):
        """驗證統計資訊正確"""
        results = {
            "file_a": pd.DataFrame({"x": [1, 2, 3]}),
            "file_b": pd.DataFrame({"y": [4, 5], "z": [6, 7]}),
        }
        stats = importer.get_import_statistics(results)
        assert stats["total_files"] == 2
        assert stats["total_rows"] == 5
        assert stats["total_columns"] == 3  # 1 + 2
        assert "file_a" in stats["file_details"]
        assert stats["file_details"]["file_a"]["rows"] == 3

    # --- extract_date_and_month_from_filename ---

    def test_extract_date_yyyymm(self, importer):
        """驗證從檔名提取 YYYYMM"""
        date_int, month = importer.extract_date_and_month_from_filename("report_202512_final.csv")
        assert date_int == 202512
        assert month == 12

    def test_extract_date_with_separator(self, importer):
        """驗證從檔名提取 YYYY-MM 格式"""
        date_int, month = importer.extract_date_and_month_from_filename("data_2025-03_v2.xlsx")
        assert date_int == 202503
        assert month == 3

    def test_extract_date_no_match(self, importer):
        """驗證無法匹配時回傳 None"""
        date_int, month = importer.extract_date_and_month_from_filename("no_date_here.csv")
        assert date_int is None
        assert month is None
