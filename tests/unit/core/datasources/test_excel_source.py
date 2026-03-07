"""
ExcelSource 單元測試
"""

import pytest
import pandas as pd
from pathlib import Path

from accrual_bot.core.datasources.excel_source import ExcelSource
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType


def _make_config(file_path: str, **kwargs) -> DataSourceConfig:
    """建立 ExcelSource 用的 DataSourceConfig"""
    params = {"file_path": file_path, **kwargs}
    return DataSourceConfig(
        source_type=DataSourceType.EXCEL,
        connection_params=params,
    )


def _create_sample_excel(path: Path, sheet_name: str = "Sheet1", df: pd.DataFrame = None):
    """寫入一個簡單的 Excel 檔案供測試使用"""
    if df is None:
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [90.5, 85.0, 92.3],
        })
    df.to_excel(path, sheet_name=sheet_name, index=False, engine="openpyxl")
    return df


def _create_multi_sheet_excel(path: Path):
    """建立含多個工作表的 Excel 檔案"""
    df1 = pd.DataFrame({"col_a": [1, 2]})
    df2 = pd.DataFrame({"col_b": [3, 4]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="First", index=False)
        df2.to_excel(writer, sheet_name="Second", index=False)
    return df1, df2


@pytest.mark.unit
class TestExcelSource:
    """ExcelSource 測試"""

    # ------------------------------------------------------------------ init
    def test_init_sets_attributes(self, tmp_path):
        """__init__ 正確設定 file_path 及各項讀取參數"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        config = _make_config(
            str(fp),
            sheet_name="Data",
            header=1,
            usecols="A:B",
            dtype="str",
            na_values=["N/A"],
            parse_dates=["date_col"],
        )
        source = ExcelSource(config)

        assert source.file_path == fp
        assert source.sheet_name == "Data"
        assert source.header == 1
        assert source.usecols == "A:B"
        assert source.dtype == "str"
        assert source.na_values == ["N/A"]
        assert source.parse_dates == ["date_col"]

    def test_init_defaults(self, tmp_path):
        """未提供可選參數時使用預設值"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))

        assert source.sheet_name == 0
        assert source.header == 0
        assert source.usecols is None
        assert source.dtype is None
        assert source.na_values is None
        assert source.parse_dates is None

    def test_init_file_not_found(self, tmp_path):
        """檔案不存在時應拋出 FileNotFoundError"""
        fp = tmp_path / "no_such_file.xlsx"
        with pytest.raises(FileNotFoundError):
            ExcelSource(_make_config(str(fp)))

    # ------------------------------------------------------------------ read
    @pytest.mark.asyncio
    async def test_read_basic(self, tmp_path):
        """read() 基本讀取，回傳正確的 DataFrame"""
        fp = tmp_path / "test.xlsx"
        expected = _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        result = await source.read()

        assert len(result) == len(expected)
        assert list(result.columns) == list(expected.columns)
        assert result["name"].tolist() == expected["name"].tolist()

    @pytest.mark.asyncio
    async def test_read_with_query(self, tmp_path):
        """read() 搭配 query 參數篩選資料"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        result = await source.read(query="age > 28")

        assert len(result) == 2  # Bob(30) and Charlie(35)

    @pytest.mark.asyncio
    async def test_read_with_nrows(self, tmp_path):
        """read() 支援 nrows 限制行數"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        result = await source.read(nrows=1)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_read_sheet_name_override(self, tmp_path):
        """read() 可透過 kwargs 覆蓋 sheet_name"""
        fp = tmp_path / "multi.xlsx"
        _, df2 = _create_multi_sheet_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        result = await source.read(sheet_name="Second")

        assert list(result.columns) == ["col_b"]
        assert result["col_b"].tolist() == df2["col_b"].tolist()

    # ------------------------------------------------------------------ write
    @pytest.mark.asyncio
    async def test_write_creates_file(self, tmp_path):
        """write() 建立新的 Excel 檔案"""
        fp = tmp_path / "output.xlsx"
        # 需要先建立檔案以通過 __init__ 驗證
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        df_new = pd.DataFrame({"x": [10, 20]})
        ok = await source.write(df_new, sheet_name="Result")

        assert ok is True
        # 驗證寫入內容
        written = pd.read_excel(fp, sheet_name="Result", engine="openpyxl")
        assert written["x"].tolist() == [10, 20]

    @pytest.mark.asyncio
    async def test_write_append_mode(self, tmp_path):
        """write() 以追加模式寫入新工作表"""
        fp = tmp_path / "output.xlsx"
        _create_sample_excel(fp, sheet_name="Sheet1")

        source = ExcelSource(_make_config(str(fp)))
        df_new = pd.DataFrame({"val": [99]})
        ok = await source.write(df_new, sheet_name="Extra", mode="a")

        assert ok is True
        sheets = pd.ExcelFile(fp, engine="openpyxl").sheet_names
        assert "Sheet1" in sheets
        assert "Extra" in sheets

    # ------------------------------------------------------------------ metadata
    def test_get_metadata(self, tmp_path):
        """get_metadata() 回傳正確的中繼資料"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        meta = source.get_metadata()

        assert meta["file_path"] == str(fp)
        assert meta["file_size"] > 0
        assert "sheet_names" in meta
        assert meta["num_sheets"] >= 1

    # ------------------------------------------------------------------ sheet names
    @pytest.mark.asyncio
    async def test_get_sheet_names(self, tmp_path):
        """get_sheet_names() 回傳所有工作表名稱"""
        fp = tmp_path / "multi.xlsx"
        _create_multi_sheet_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        names = await source.get_sheet_names()

        assert names == ["First", "Second"]

    # ------------------------------------------------------------------ read_all_sheets
    @pytest.mark.asyncio
    async def test_read_all_sheets(self, tmp_path):
        """read_all_sheets() 回傳所有工作表的 DataFrame 字典"""
        fp = tmp_path / "multi.xlsx"
        _create_multi_sheet_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        sheets = await source.read_all_sheets()

        assert set(sheets.keys()) == {"First", "Second"}
        assert list(sheets["First"].columns) == ["col_a"]
        assert list(sheets["Second"].columns) == ["col_b"]

    # ------------------------------------------------------------------ _apply_query
    def test_apply_query_invalid_returns_original(self, tmp_path):
        """_apply_query() 查詢失敗時回傳原始 DataFrame"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = source._apply_query(df, "nonexistent_col > 0")

        assert result.equals(df)

    # ------------------------------------------------------------------ append_data
    @pytest.mark.asyncio
    async def test_append_data(self, tmp_path):
        """append_data() 將新資料追加到現有工作表"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        extra = pd.DataFrame({"name": ["Dave"], "age": [40], "score": [88.0]})
        ok = await source.append_data(extra, sheet_name="Sheet1")

        assert ok is True
        result = pd.read_excel(fp, engine="openpyxl")
        assert len(result) == 4  # 原本 3 + 追加 1

    # ------------------------------------------------------------------ create_from_file
    def test_create_from_file(self, tmp_path):
        """create_from_file() 從路徑建立 ExcelSource"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource.create_from_file(str(fp), sheet_name="Sheet1")

        assert isinstance(source, ExcelSource)
        assert source.file_path == fp
        assert source.sheet_name == "Sheet1"

    # ------------------------------------------------------------------ close
    @pytest.mark.asyncio
    async def test_close_is_noop(self, tmp_path):
        """close() 不應拋出例外"""
        fp = tmp_path / "test.xlsx"
        _create_sample_excel(fp)

        source = ExcelSource(_make_config(str(fp)))
        await source.close()  # 不應拋出例外
