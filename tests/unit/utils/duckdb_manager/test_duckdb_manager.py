"""DuckDB Manager 單元測試"""
import pytest
import pandas as pd
from pathlib import Path

from accrual_bot.utils.duckdb_manager.config import DuckDBConfig
from accrual_bot.utils.duckdb_manager.manager import DuckDBManager
from accrual_bot.utils.duckdb_manager.exceptions import DuckDBConnectionError


@pytest.mark.unit
class TestDuckDBConfig:
    """DuckDBConfig 測試"""

    def test_default_values(self):
        config = DuckDBConfig()
        assert config.db_path == ":memory:"
        assert config.timezone == "Asia/Taipei"
        assert config.read_only is False
        assert config.connection_timeout == 30
        assert config.log_level == "INFO"
        assert config.enable_query_logging is True

    def test_custom_values(self):
        config = DuckDBConfig(
            db_path="./test.duckdb",
            timezone="UTC",
            read_only=True,
            log_level="DEBUG",
        )
        assert config.db_path == "./test.duckdb"
        assert config.timezone == "UTC"
        assert config.read_only is True
        assert config.log_level == "DEBUG"

    def test_invalid_log_level_raises(self):
        with pytest.raises(ValueError, match="無效的 log_level"):
            DuckDBConfig(log_level="INVALID")

    def test_log_level_normalized_to_upper(self):
        config = DuckDBConfig(log_level="debug")
        assert config.log_level == "DEBUG"

    def test_from_dict(self):
        config = DuckDBConfig.from_dict({
            "db_path": "./test.duckdb",
            "timezone": "UTC",
            "extra_key": "ignored",
        })
        assert config.db_path == "./test.duckdb"
        assert config.timezone == "UTC"

    def test_from_dict_ignores_unknown_keys(self):
        config = DuckDBConfig.from_dict({"unknown": "value"})
        assert config.db_path == ":memory:"

    def test_to_dict(self):
        config = DuckDBConfig()
        d = config.to_dict()
        assert "db_path" in d
        assert "timezone" in d
        assert "logger" not in d

    def test_copy(self):
        config = DuckDBConfig(db_path="./original.duckdb")
        copied = config.copy(db_path="./copied.duckdb")
        assert copied.db_path == "./copied.duckdb"
        assert config.db_path == "./original.duckdb"

    def test_from_path(self):
        config = DuckDBConfig.from_path("./test.duckdb")
        assert config.db_path == "./test.duckdb"

    def test_from_toml(self, tmp_path):
        toml_file = tmp_path / "config.toml"
        toml_file.write_text(
            '[database]\ndb_path = ":memory:"\ntimezone = "UTC"\n'
        )
        config = DuckDBConfig.from_toml(str(toml_file), section="database")
        assert config.timezone == "UTC"

    def test_from_toml_missing_file(self):
        with pytest.raises(FileNotFoundError):
            DuckDBConfig.from_toml("/nonexistent/config.toml")

    def test_from_toml_missing_section(self, tmp_path):
        toml_file = tmp_path / "config.toml"
        toml_file.write_text('[other]\nkey = "value"\n')
        with pytest.raises(KeyError):
            DuckDBConfig.from_toml(str(toml_file), section="database")

    def test_creates_parent_directory(self, tmp_path):
        db_path = str(tmp_path / "subdir" / "test.duckdb")
        config = DuckDBConfig(db_path=db_path)
        assert Path(db_path).parent.exists()


@pytest.mark.unit
class TestDuckDBManager:
    """DuckDBManager 測試"""

    def test_memory_database(self):
        with DuckDBManager() as db:
            assert db.is_memory_db is True
            assert db.is_connected is True

    def test_file_database(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        with DuckDBManager(db_path) as db:
            assert db.is_memory_db is False
            assert db.database_path == db_path

    def test_config_object(self):
        config = DuckDBConfig(db_path=":memory:", timezone="UTC")
        with DuckDBManager(config) as db:
            assert db.config.timezone == "UTC"

    def test_dict_config(self):
        with DuckDBManager({"db_path": ":memory:"}) as db:
            assert db.is_memory_db is True

    def test_invalid_config_type(self):
        with pytest.raises(TypeError, match="不支援的配置類型"):
            DuckDBManager(12345)

    def test_close_sets_conn_none(self):
        db = DuckDBManager()
        assert db.is_connected is True
        db.close()
        assert db.is_connected is False

    def test_context_manager_closes(self):
        db = DuckDBManager()
        with db:
            assert db.is_connected is True
        assert db.is_connected is False

    def test_repr(self):
        with DuckDBManager() as db:
            assert "DuckDBManager" in repr(db)
            assert ":memory:" in repr(db)

    def test_query_to_df(self):
        with DuckDBManager() as db:
            df = db.query_to_df("SELECT 1 AS a, 2 AS b")
            assert isinstance(df, pd.DataFrame)
            assert list(df.columns) == ['a', 'b']
            assert len(df) == 1

    def test_create_table_from_df(self):
        df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
        with DuckDBManager() as db:
            db.create_table_from_df("users", df)
            result = db.query_to_df("SELECT * FROM users")
            assert len(result) == 2
            assert 'name' in result.columns

    def test_insert_df_into_table(self):
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'id': [2], 'name': ['Bob']})
        with DuckDBManager() as db:
            db.create_table_from_df("users", df1)
            db.insert_df_into_table("users", df2)
            result = db.query_to_df("SELECT * FROM users ORDER BY id")
            assert len(result) == 2

    def test_none_config_uses_memory(self):
        with DuckDBManager(None) as db:
            assert db.is_memory_db is True
