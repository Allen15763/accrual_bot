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


@pytest.mark.unit
class TestBaseDataImporterExtended:
    """BaseDataImporter 擴展測試 - 提高覆蓋率"""

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

    # --- import_file 錯誤處理路徑 ---

    def test_import_file_excel_read_error_logs_and_raises(self, importer, tmp_path):
        """驗證 Excel 讀取失敗時會記錄錯誤並拋出例外"""
        bad_file = tmp_path / "corrupt.xlsx"
        bad_file.write_bytes(b"not an excel file")
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=False):
            with pytest.raises(Exception):
                importer.import_file(str(bad_file))
        # 確認有記錄錯誤
        importer.logger.error.assert_called()

    def test_import_file_passes_kwargs_to_csv(self, importer, tmp_path):
        """驗證 import_file 正確傳遞 kwargs 給 _import_csv"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_bytes("a,b\n1,2\n".encode("utf-8"))
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=True):
            df = importer.import_file(str(csv_file), encoding='utf-8')
            assert len(df) == 1
            assert list(df.columns) == ["a", "b"]

    # --- import_multiple_files 批量導入 ---

    def test_import_multiple_files_skips_empty_path(self, importer, tmp_path):
        """驗證空路徑字串會被跳過"""
        csv_file = tmp_path / "good.csv"
        csv_file.write_bytes("x\n1\n".encode("utf-8"))
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=True):
            results = importer.import_multiple_files(["", str(csv_file), ""])
            assert len(results) == 1
            assert "good" in results

    def test_import_multiple_files_continues_on_single_failure(self, importer, tmp_path):
        """驗證單一檔案失敗時不影響其他檔案的導入"""
        good_csv = tmp_path / "good.csv"
        good_csv.write_bytes("col\nval\n".encode("utf-8"))
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_bytes(b"\x80\x81\x82")  # 無效的 binary

        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=True):
            results = importer.import_multiple_files([str(bad_csv), str(good_csv)])
            # good.csv 應成功導入，bad.csv 失敗但不中斷
            assert "good" in results

    def test_import_multiple_files_with_file_configs(self, importer, tmp_path):
        """驗證 file_configs 參數正確傳遞給 import_file"""
        csv_file = tmp_path / "configured.csv"
        csv_file.write_bytes("h1,h2\na,b\n".encode("utf-8"))
        with patch("accrual_bot.data.importers.base_importer.validate_file_path", return_value=True), \
             patch("accrual_bot.data.importers.base_importer.is_excel_file", return_value=False), \
             patch("accrual_bot.data.importers.base_importer.is_csv_file", return_value=True):
            results = importer.import_multiple_files(
                [str(csv_file)],
                file_configs={"configured": {"encoding": "utf-8"}}
            )
            assert "configured" in results
            assert len(results["configured"]) == 1

    # --- _import_excel engine fallback ---

    def test_import_excel_openpyxl_fallback_to_xlrd(self, importer, tmp_path):
        """驗證 openpyxl 失敗時會嘗試 xlrd engine"""
        xlsx_file = tmp_path / "test.xlsx"
        df = pd.DataFrame({"a": [1]})
        df.to_excel(xlsx_file, index=False, engine="openpyxl")

        call_count = 0
        original_read_excel = pd.read_excel

        def mock_read_excel(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs.get('engine') == 'openpyxl':
                raise Exception("openpyxl 模擬失敗")
            # xlrd 也會失敗（因為是 xlsx 格式），但我們驗證 fallback 邏輯有執行
            raise Exception("xlrd 模擬失敗")

        with patch("accrual_bot.data.importers.base_importer.pd.read_excel", side_effect=mock_read_excel):
            with pytest.raises(Exception, match="openpyxl 模擬失敗"):
                importer._import_excel(str(xlsx_file))
            # 確認嘗試了兩次（openpyxl + xlrd fallback）
            assert call_count == 2

    # --- _import_csv 編碼偵測 ---

    def test_import_csv_all_encodings_fail_raises_valueerror(self, importer, tmp_path):
        """驗證所有編碼都失敗時拋出 ValueError"""
        csv_file = tmp_path / "unreadable.csv"
        csv_file.write_bytes(b"\x80\x81\x82\xff\xfe")

        # iso-8859-1 can read any byte sequence, so we must mock pd.read_csv
        # to force UnicodeDecodeError on all encoding attempts
        with patch("accrual_bot.data.importers.base_importer.pd.read_csv",
                    side_effect=UnicodeDecodeError("codec", b"", 0, 1, "mock")):
            with pytest.raises(ValueError, match="無法使用任何編碼格式讀取"):
                importer._import_csv(str(csv_file), encoding="ascii")

    def test_import_csv_utf8_sig_encoding(self, importer, tmp_path):
        """驗證 UTF-8 BOM 編碼的 CSV 可正確讀取"""
        csv_file = tmp_path / "bom.csv"
        content = "\ufeffcol1,col2\nval1,val2\n"
        csv_file.write_bytes(content.encode("utf-8-sig"))
        result = importer._import_csv(str(csv_file), encoding="utf-8-sig")
        assert len(result) == 1

    # --- validate_dataframe None 輸入 ---

    def test_validate_dataframe_none_returns_false(self, importer):
        """驗證 None 輸入回傳 False"""
        assert importer.validate_dataframe(None) is False
