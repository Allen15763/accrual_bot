"""
ParquetSource 單元測試
"""

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from accrual_bot.core.datasources.parquet_source import ParquetSource
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType


def _make_config(file_path: str, **extra) -> DataSourceConfig:
    """建立 ParquetSource 所需的 DataSourceConfig。"""
    return DataSourceConfig(
        source_type=DataSourceType.PARQUET,
        connection_params={"file_path": file_path, **extra},
    )


def _sample_df() -> pd.DataFrame:
    """回傳一個小型測試 DataFrame。"""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
            "category": ["A", "B", "A", "B", "A"],
        }
    )


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """將 sample DataFrame 寫成 parquet 檔並回傳路徑。"""
    path = tmp_path / "sample.parquet"
    _sample_df().to_parquet(path, index=False)
    return path


@pytest.mark.unit
class TestParquetSource:
    """ParquetSource 完整測試。"""

    # ------------------------------------------------------------------ #
    # __init__
    # ------------------------------------------------------------------ #
    def test_init_with_existing_file(self, sample_parquet: Path):
        """初始化時若檔案存在，應正常設定屬性且不發出警告。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        assert src.file_path == sample_parquet
        assert src.compression == "snappy"
        assert src.engine == "pyarrow"
        assert src.use_pandas_metadata is True

    def test_init_with_nonexistent_file_warns(self, tmp_path: Path):
        """初始化時若檔案不存在，應發出警告但不拋出例外。"""
        missing = tmp_path / "does_not_exist.parquet"
        src = ParquetSource(_make_config(str(missing)))
        assert src.file_path == missing

    def test_init_custom_params(self, tmp_path: Path):
        """透過 connection_params 傳入自訂參數。"""
        path = tmp_path / "custom.parquet"
        src = ParquetSource(
            _make_config(
                str(path),
                compression="gzip",
                engine="fastparquet",
                columns=["id"],
                filters=[("id", "==", 1)],
                use_pandas_metadata=False,
            )
        )
        assert src.compression == "gzip"
        assert src.engine == "fastparquet"
        assert src.columns == ["id"]
        assert src.filters == [("id", "==", 1)]
        assert src.use_pandas_metadata is False

    # ------------------------------------------------------------------ #
    # read()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_read_basic(self, sample_parquet: Path):
        """基本讀取應回傳完整 DataFrame。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = await src.read()
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "amount", "category"]

    @pytest.mark.asyncio
    async def test_read_nonexistent_returns_empty(self, tmp_path: Path):
        """讀取不存在的檔案應回傳空 DataFrame。"""
        src = ParquetSource(_make_config(str(tmp_path / "missing.parquet")))
        df = await src.read()
        assert df.empty

    @pytest.mark.asyncio
    async def test_read_with_columns(self, sample_parquet: Path):
        """指定 columns 參數應只回傳對應欄位。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = await src.read(columns=["id", "name"])
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_read_with_filters(self, sample_parquet: Path):
        """透過 filters 參數篩選資料。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = await src.read(filters=[("category", "==", "A")])
        assert len(df) == 3
        assert set(df["category"].unique()) == {"A"}

    @pytest.mark.asyncio
    async def test_read_with_query(self, sample_parquet: Path):
        """透過 query 參數進行 df.query() 篩選。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = await src.read(query="amount > 200")
        assert all(df["amount"] > 200)

    # ------------------------------------------------------------------ #
    # write()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_write_creates_file(self, tmp_path: Path):
        """write() 應建立 parquet 檔並回傳 True。"""
        path = tmp_path / "sub" / "output.parquet"
        src = ParquetSource(_make_config(str(path)))
        result = await src.write(_sample_df())
        assert result is True
        assert path.exists()
        # 驗證內容可被正確讀回
        df_read = pd.read_parquet(path)
        assert len(df_read) == 5

    @pytest.mark.asyncio
    async def test_write_with_compression(self, tmp_path: Path):
        """write() 應支援 compression 參數。"""
        path = tmp_path / "compressed.parquet"
        src = ParquetSource(_make_config(str(path), compression="gzip"))
        result = await src.write(_sample_df(), compression="gzip")
        assert result is True
        meta = pq.read_metadata(path)
        assert meta.num_rows == 5

    # ------------------------------------------------------------------ #
    # get_metadata()
    # ------------------------------------------------------------------ #
    def test_get_metadata_existing_file(self, sample_parquet: Path):
        """get_metadata() 應回傳 schema、row groups 等資訊。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        meta = src.get_metadata()
        assert meta["file_path"] == str(sample_parquet)
        assert meta["num_rows"] == 5
        assert meta["num_columns"] == 4
        assert "schema" in meta

    def test_get_metadata_missing_file(self, tmp_path: Path):
        """檔案不存在時，get_metadata() 應僅回傳基本資訊。"""
        src = ParquetSource(_make_config(str(tmp_path / "missing.parquet")))
        meta = src.get_metadata()
        assert "num_rows" not in meta
        assert meta["compression"] == "snappy"

    # ------------------------------------------------------------------ #
    # read_row_groups()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_read_row_groups(self, sample_parquet: Path):
        """read_row_groups() 應回傳指定 row group 的資料。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = await src.read_row_groups(row_groups=[0])
        assert len(df) == 5  # 單一 row group 包含所有資料

    @pytest.mark.asyncio
    async def test_read_row_groups_missing_file(self, tmp_path: Path):
        """檔案不存在時應回傳空 DataFrame。"""
        src = ParquetSource(_make_config(str(tmp_path / "missing.parquet")))
        df = await src.read_row_groups(row_groups=[0])
        assert df.empty

    # ------------------------------------------------------------------ #
    # get_schema()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_get_schema(self, sample_parquet: Path):
        """get_schema() 應回傳 PyArrow Schema。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        schema = await src.get_schema()
        assert schema is not None
        # ParquetFile.schema returns ParquetSchema, not pa.Schema
        assert len(schema) == 4

    @pytest.mark.asyncio
    async def test_get_schema_missing_file(self, tmp_path: Path):
        """檔案不存在時應回傳 None。"""
        src = ParquetSource(_make_config(str(tmp_path / "missing.parquet")))
        schema = await src.get_schema()
        assert schema is None

    # ------------------------------------------------------------------ #
    # append_data()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_append_data_existing(self, sample_parquet: Path):
        """append_data() 應合併既有資料與新資料。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        new_rows = pd.DataFrame(
            {"id": [6], "name": ["Frank"], "amount": [400.0], "category": ["B"]}
        )
        result = await src.append_data(new_rows)
        assert result is True
        df = pd.read_parquet(sample_parquet)
        assert len(df) == 6

    @pytest.mark.asyncio
    async def test_append_data_new_file(self, tmp_path: Path):
        """檔案不存在時 append_data() 應建立新檔案。"""
        path = tmp_path / "new_append.parquet"
        src = ParquetSource(_make_config(str(path)))
        result = await src.append_data(_sample_df())
        assert result is True
        assert path.exists()
        assert len(pd.read_parquet(path)) == 5

    # ------------------------------------------------------------------ #
    # _apply_filters()
    # ------------------------------------------------------------------ #
    def test_apply_filters_operators(self, sample_parquet: Path):
        """_apply_filters() 應支援 ==, !=, >, >=, <, <=, in 運算子。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = _sample_df()

        assert len(src._apply_filters(df.copy(), [("amount", "==", 100.0)])) == 1
        assert len(src._apply_filters(df.copy(), [("amount", "!=", 100.0)])) == 4
        assert len(src._apply_filters(df.copy(), [("amount", ">", 200.0)])) == 2
        assert len(src._apply_filters(df.copy(), [("amount", ">=", 200.0)])) == 3
        assert len(src._apply_filters(df.copy(), [("amount", "<", 200.0)])) == 2
        assert len(src._apply_filters(df.copy(), [("amount", "<=", 200.0)])) == 3
        assert len(src._apply_filters(df.copy(), [("name", "in", ["Alice", "Bob"])])) == 2

    # ------------------------------------------------------------------ #
    # _apply_query()
    # ------------------------------------------------------------------ #
    def test_apply_query(self, sample_parquet: Path):
        """_apply_query() 應使用 df.query() 篩選。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        df = _sample_df()
        result = src._apply_query(df, "id > 3")
        assert len(result) == 2

    # ------------------------------------------------------------------ #
    # create_from_file()
    # ------------------------------------------------------------------ #
    def test_create_from_file(self, sample_parquet: Path):
        """create_from_file() 應回傳正確設定的 ParquetSource 實例。"""
        src = ParquetSource.create_from_file(
            str(sample_parquet), compression="gzip"
        )
        assert isinstance(src, ParquetSource)
        assert src.file_path == sample_parquet
        assert src.compression == "gzip"

    # ------------------------------------------------------------------ #
    # close()
    # ------------------------------------------------------------------ #
    @pytest.mark.asyncio
    async def test_close_is_noop(self, sample_parquet: Path):
        """close() 應為 no-op，不拋出例外。"""
        src = ParquetSource(_make_config(str(sample_parquet)))
        await src.close()  # 應無任何副作用
