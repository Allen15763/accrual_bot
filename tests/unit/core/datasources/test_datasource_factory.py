"""DataSourceFactory 單元測試"""
import pytest
from unittest.mock import patch, MagicMock

from accrual_bot.core.datasources.config import DataSourceConfig
from accrual_bot.core.datasources.base import DataSourceType
from accrual_bot.core.datasources.factory import DataSourceFactory


@pytest.mark.unit
class TestDataSourceFactoryCreate:
    """create() 方法測試"""

    def test_create_csv_source(self, csv_config):
        """CSV 配置應成功建立 CSVSource 實例"""
        source = DataSourceFactory.create(csv_config)
        assert source is not None
        assert source.config.source_type == DataSourceType.CSV

    def test_create_excel_source(self, excel_config):
        """Excel 配置應成功建立 ExcelSource 實例"""
        source = DataSourceFactory.create(excel_config)
        assert source is not None
        assert source.config.source_type == DataSourceType.EXCEL

    def test_create_duckdb_source(self):
        """DuckDB 配置應成功建立 DuckDBSource 實例"""
        config = DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={'db_path': ':memory:'},
        )
        source = DataSourceFactory.create(config)
        assert source is not None
        assert source.config.source_type == DataSourceType.DUCKDB

    def test_create_with_invalid_config_raises(self):
        """無效配置應拋出錯誤（缺少必要參數）"""
        config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            connection_params={},  # 缺少 file_path
        )
        # 注意：由於 DataSourceType 存在雙重定義（base.py 和 config.py），
        # 使用 base 的 DataSourceType 時 validation 的 required_params 匹配不到，
        # 但 CSVSource.__init__ 仍會因缺少 file_path 而拋出 KeyError
        with pytest.raises((ValueError, KeyError)):
            DataSourceFactory.create(config)

    def test_create_unsupported_type_raises(self):
        """未實作的類型應拋出 NotImplementedError"""
        config = DataSourceConfig(
            source_type=DataSourceType.IN_MEMORY,
            connection_params={'dataframe': 'dummy'},
        )
        with pytest.raises(NotImplementedError, match="not implemented"):
            DataSourceFactory.create(config)


@pytest.mark.unit
class TestDataSourceFactoryCreateFromFile:
    """create_from_file() 方法測試"""

    def test_create_from_xlsx(self, sample_excel_file):
        """xlsx 副檔名應建立 ExcelSource"""
        source = DataSourceFactory.create_from_file(sample_excel_file)
        assert source.config.source_type == DataSourceType.EXCEL

    def test_create_from_csv(self, sample_csv_file):
        """csv 副檔名應建立 CSVSource"""
        source = DataSourceFactory.create_from_file(sample_csv_file)
        assert source.config.source_type == DataSourceType.CSV

    def test_create_from_unsupported_extension_raises(self, tmp_path):
        """不支援的副檔名應拋出 ValueError"""
        fake_file = tmp_path / "data.json"
        fake_file.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file type"):
            DataSourceFactory.create_from_file(str(fake_file))

    def test_create_from_file_passes_kwargs(self, sample_csv_file):
        """額外 kwargs 應被傳入 connection_params"""
        source = DataSourceFactory.create_from_file(
            sample_csv_file, sep=';', encoding='big5'
        )
        assert source.config.connection_params['sep'] == ';'
        assert source.config.connection_params['encoding'] == 'big5'


@pytest.mark.unit
class TestDataSourceFactoryGetSupportedTypes:
    """get_supported_types() 方法測試"""

    def test_get_supported_types_contains_core(self):
        """支援類型應至少包含 EXCEL, CSV, PARQUET, DUCKDB"""
        supported = DataSourceFactory.get_supported_types()
        assert DataSourceType.EXCEL in supported
        assert DataSourceType.CSV in supported
        assert DataSourceType.PARQUET in supported
        assert DataSourceType.DUCKDB in supported

    def test_get_supported_types_returns_list(self):
        """回傳值應為 list"""
        supported = DataSourceFactory.get_supported_types()
        assert isinstance(supported, list)


@pytest.mark.unit
class TestDataSourceFactoryCreateBatch:
    """create_batch() 方法測試"""

    def test_create_batch_success(self, csv_config, excel_config):
        """批量建立應回傳所有成功的數據源"""
        configs = [
            ('csv_source', csv_config),
            ('excel_source', excel_config),
        ]
        sources = DataSourceFactory.create_batch(configs)
        assert 'csv_source' in sources
        assert 'excel_source' in sources
        assert len(sources) == 2

    def test_create_batch_partial_failure(self, csv_config):
        """部分配置失敗時，成功的仍應回傳"""
        bad_config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            connection_params={},  # 無效
        )
        configs = [
            ('good', csv_config),
            ('bad', bad_config),
        ]
        sources = DataSourceFactory.create_batch(configs)
        assert 'good' in sources
        assert 'bad' not in sources

    def test_create_batch_empty_list(self):
        """空列表應回傳空字典"""
        sources = DataSourceFactory.create_batch([])
        assert sources == {}
