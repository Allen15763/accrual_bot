"""CSVSource 單元測試"""

import pytest
import pandas as pd
from pathlib import Path

from accrual_bot.core.datasources.csv_source import CSVSource
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType


def _make_config(file_path: str, **kwargs) -> DataSourceConfig:
    connection_params = {"file_path": file_path, **kwargs}
    return DataSourceConfig(
        source_type=DataSourceType.CSV,
        connection_params=connection_params,
    )


@pytest.fixture
def sample_csv(tmp_path):
    csv_file = tmp_path / "sample.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    df.to_csv(csv_file, index=False)
    return csv_file


@pytest.fixture
def csv_source(sample_csv):
    config = _make_config(str(sample_csv))
    return CSVSource(config)


@pytest.mark.unit
class TestCSVSource:

    def test_init_sets_attributes(self, sample_csv):
        config = _make_config(
            str(sample_csv),
            sep=";",
            header=0,
            dtype="str",
            na_values=["N/A"],
            parse_dates=["date_col"],
            usecols=["id", "name"],
        )
        config.encoding = "utf-8-sig"
        config.chunk_size = 500
        source = CSVSource(config)

        assert source.file_path == sample_csv
        assert source.encoding == "utf-8-sig"
        assert source.sep == ";"
        assert source.header == 0
        assert source.dtype == "str"
        assert source.na_values == ["N/A"]
        assert source.parse_dates == ["date_col"]
        assert source.usecols == ["id", "name"]
        assert source.chunk_size == 500

    def test_init_raises_file_not_found(self, tmp_path):
        config = _make_config(str(tmp_path / "nonexistent.csv"))
        with pytest.raises(FileNotFoundError, match="CSV file not found"):
            CSVSource(config)

    @pytest.mark.asyncio
    async def test_read_returns_dataframe(self, csv_source):
        df = await csv_source.read()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "value"]

    @pytest.mark.asyncio
    async def test_read_with_nrows(self, csv_source):
        df = await csv_source.read(nrows=2)
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_read_with_skiprows(self, csv_source):
        df = await csv_source.read(skiprows=[1, 2])
        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_read_with_query(self, csv_source):
        df = await csv_source.read(query="value > 25")
        assert len(df) == 3
        assert all(df["value"] > 25)

    @pytest.mark.asyncio
    async def test_write_creates_csv(self, tmp_path):
        out_file = tmp_path / "output.csv"
        out_file.touch()  # CSVSource requires file to exist at init
        source = CSVSource(_make_config(str(out_file)))
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        result = await source.write(df)
        assert result is True

        written = pd.read_csv(out_file)
        assert len(written) == 2
        assert list(written.columns) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_write_with_index(self, tmp_path):
        out_file = tmp_path / "indexed.csv"
        out_file.touch()
        source = CSVSource(_make_config(str(out_file)))
        df = pd.DataFrame({"x": [10]})

        await source.write(df, index=True)
        content = out_file.read_text()
        # index=True adds an unnamed index column
        assert "x" in content

    def test_get_metadata(self, csv_source, sample_csv):
        meta = csv_source.get_metadata()
        assert meta["file_path"] == str(sample_csv)
        assert meta["encoding"] == "utf-8"
        assert meta["separator"] == ","
        assert meta["num_columns"] == 3
        assert meta["num_rows"] == 5
        assert "id" in meta["column_names"]
        assert meta["file_size"] > 0

    @pytest.mark.asyncio
    async def test_read_in_chunks(self, csv_source):
        chunks = await csv_source.read_in_chunks(chunk_size=2)
        assert isinstance(chunks, list)
        assert len(chunks) == 3  # 5 rows / 2 per chunk = 3 chunks
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 5

    @pytest.mark.asyncio
    async def test_append_data_to_existing(self, csv_source, sample_csv):
        new_data = pd.DataFrame({
            "id": [6],
            "name": ["Frank"],
            "value": [60.0],
        })
        result = await csv_source.append_data(new_data)
        assert result is True

        all_data = pd.read_csv(sample_csv)
        assert len(all_data) == 6

    def test_apply_query_returns_original_on_bad_query(self, csv_source):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = csv_source._apply_query(df, "invalid_col > 0")
        assert len(result) == 3

    def test_create_from_file(self, sample_csv):
        source = CSVSource.create_from_file(
            str(sample_csv), sep=",", encoding="utf-8"
        )
        assert isinstance(source, CSVSource)
        assert source.file_path == sample_csv
        assert source.sep == ","

    @pytest.mark.asyncio
    async def test_close_is_noop(self, csv_source):
        await csv_source.close()
        # Should not raise; read should still work after close
        df = await csv_source.read()
        assert len(df) == 5
