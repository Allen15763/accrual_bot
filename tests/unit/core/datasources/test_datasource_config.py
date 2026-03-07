"""DataSourceConfig 單元測試"""
import pytest
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType


@pytest.mark.unit
class TestDataSourceConfigValidate:
    """validate() 方法測試"""

    def test_validate_csv_valid(self, csv_config):
        """CSV 配置含有效 file_path 時應通過驗證"""
        is_valid, errors = csv_config.validate()
        assert is_valid is True
        assert errors == []

    def test_validate_excel_valid(self, excel_config):
        """Excel 配置含有效 file_path 時應通過驗證"""
        is_valid, errors = excel_config.validate()
        assert is_valid is True
        assert errors == []

    def test_validate_missing_required_param(self):
        """缺少必要參數時應回傳錯誤"""
        config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            connection_params={},  # 缺少 file_path
        )
        is_valid, errors = config.validate()
        assert is_valid is False
        assert any('file_path' in e for e in errors)

    def test_validate_file_not_found(self):
        """檔案路徑不存在時應回傳錯誤"""
        config = DataSourceConfig(
            source_type=DataSourceType.EXCEL,
            connection_params={'file_path': '/nonexistent/path/file.xlsx'},
        )
        is_valid, errors = config.validate()
        assert is_valid is False
        assert any('not found' in e.lower() or 'File not found' in e for e in errors)

    def test_validate_duckdb_valid(self):
        """DuckDB 配置含 db_path 時應通過驗證"""
        config = DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={'db_path': ':memory:'},
        )
        is_valid, errors = config.validate()
        assert is_valid is True
        assert errors == []

    def test_validate_postgres_missing_params(self):
        """Postgres 缺少多個必要參數時應列出所有錯誤"""
        config = DataSourceConfig(
            source_type=DataSourceType.POSTGRES,
            connection_params={'host': 'localhost'},  # 缺少 port, database, user, password
        )
        is_valid, errors = config.validate()
        assert is_valid is False
        assert len(errors) == 4  # port, database, user, password


@pytest.mark.unit
class TestDataSourceConfigConnectionString:
    """get_connection_string() 方法測試"""

    def test_get_connection_string_postgres(self):
        """Postgres 應回傳完整連線字串"""
        config = DataSourceConfig(
            source_type=DataSourceType.POSTGRES,
            connection_params={
                'host': 'localhost',
                'port': '5432',
                'database': 'testdb',
                'user': 'admin',
                'password': 'secret',
            },
        )
        conn_str = config.get_connection_string()
        assert conn_str == 'postgresql://admin:secret@localhost:5432/testdb'

    def test_get_connection_string_duckdb(self):
        """DuckDB 應回傳 db_path"""
        config = DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={'db_path': '/tmp/test.duckdb'},
        )
        assert config.get_connection_string() == '/tmp/test.duckdb'

    def test_get_connection_string_csv_returns_none(self, csv_config):
        """CSV 類型不支援連線字串，應回傳 None"""
        assert csv_config.get_connection_string() is None


@pytest.mark.unit
class TestDataSourceConfigCopy:
    """copy() 方法測試"""

    def test_copy_creates_independent_instance(self, csv_config):
        """copy 應產生獨立副本，修改副本不影響原件"""
        copied = csv_config.copy()
        copied.connection_params['extra'] = 'value'
        copied.cache_enabled = False

        assert 'extra' not in csv_config.connection_params
        assert csv_config.cache_enabled is True

    def test_copy_preserves_all_fields(self):
        """copy 應保留所有欄位值"""
        config = DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={'file_path': '/tmp/data.parquet'},
            cache_enabled=False,
            lazy_load=True,
            encoding='big5',
            chunk_size=5000,
            cache_ttl_seconds=600,
            cache_max_items=20,
            cache_eviction_policy='fifo',
        )
        copied = config.copy()
        assert copied.source_type == config.source_type
        assert copied.cache_enabled == config.cache_enabled
        assert copied.lazy_load == config.lazy_load
        assert copied.encoding == config.encoding
        assert copied.chunk_size == config.chunk_size
        assert copied.cache_ttl_seconds == config.cache_ttl_seconds
        assert copied.cache_max_items == config.cache_max_items
        assert copied.cache_eviction_policy == config.cache_eviction_policy


@pytest.mark.unit
class TestDataSourceConfigSerialization:
    """from_dict() / to_dict() 序列化測試"""

    def test_from_dict_with_string_type(self):
        """from_dict 應接受字串型別的 source_type"""
        d = {
            'source_type': 'csv',
            'connection_params': {'file_path': '/tmp/f.csv'},
            'cache_enabled': False,
        }
        config = DataSourceConfig.from_dict(d)
        assert config.source_type == DataSourceType.CSV
        assert config.cache_enabled is False

    def test_from_dict_with_enum_type(self):
        """from_dict 應接受 Enum 型別的 source_type"""
        d = {
            'source_type': DataSourceType.EXCEL,
            'connection_params': {'file_path': '/tmp/f.xlsx'},
        }
        config = DataSourceConfig.from_dict(d)
        assert config.source_type == DataSourceType.EXCEL

    def test_to_dict_structure(self, csv_config):
        """to_dict 應包含所有預期的鍵"""
        d = csv_config.to_dict()
        expected_keys = {
            'source_type', 'connection_params', 'cache_enabled',
            'lazy_load', 'encoding', 'chunk_size',
            'cache_ttl_seconds', 'cache_max_items', 'cache_eviction_policy',
        }
        assert set(d.keys()) == expected_keys
        # source_type 應為字串值
        assert d['source_type'] == 'csv'

    def test_roundtrip_dict(self):
        """to_dict → from_dict 應能完整還原配置"""
        original = DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={'file_path': '/tmp/test.parquet'},
            cache_enabled=False,
            lazy_load=True,
            encoding='big5',
            chunk_size=1000,
            cache_ttl_seconds=120,
            cache_max_items=5,
            cache_eviction_policy='fifo',
        )
        restored = DataSourceConfig.from_dict(original.to_dict())

        assert restored.source_type == original.source_type
        assert restored.connection_params == original.connection_params
        assert restored.cache_enabled == original.cache_enabled
        assert restored.lazy_load == original.lazy_load
        assert restored.encoding == original.encoding
        assert restored.chunk_size == original.chunk_size
        assert restored.cache_ttl_seconds == original.cache_ttl_seconds
        assert restored.cache_max_items == original.cache_max_items
        assert restored.cache_eviction_policy == original.cache_eviction_policy
