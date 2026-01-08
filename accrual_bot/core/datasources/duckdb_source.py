"""
DuckDB數據源實現 - Phase 2 完成版（Transaction支持）
修復：內存數據庫使用持久連接，文件數據庫使用臨時連接
"""

import pandas as pd
import duckdb
from typing import Dict, Optional, Any, List, Union, Tuple, Callable
from pathlib import Path
from contextlib import contextmanager
import asyncio
import logging
import json
import threading
import os
import time
from concurrent.futures import ThreadPoolExecutor

try:
    from .base import DataSource
    from .config import DataSourceConfig, DataSourceType
except ImportError:
    from accrual_bot.core.datasources import DataSource
    from accrual_bot.core.datasources import DataSourceConfig, DataSourceType


class DuckDBSource(DataSource):
    """
    DuckDB數據源 - Phase 2 完成版
    
    主要改進：
    - ✅ 內存數據庫：使用線程本地持久連接（避免數據丟失）
    - ✅ 文件數據庫：使用 context manager 臨時連接（線程安全）
    - ✅ Transaction 支持（Phase 2）
    - ✅ 統一的重試邏輯
    - ✅ 向後兼容所有現有 API
    
    重要說明：
    - 內存DB適合單線程/快速測試
    - 文件DB適合生產環境/併發場景
    """
    
    # 類級別的線程池
    _executor = ThreadPoolExecutor(max_workers=4)
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化DuckDB數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.db_path = config.connection_params.get('db_path', ':memory:')
        self.table_name = config.connection_params.get('table_name')
        self.read_only = config.connection_params.get('read_only', False)
        self.is_memory_db = (self.db_path == ':memory:')
        
        # 對於內存數據庫，使用線程本地存儲保持持久連接
        if self.is_memory_db:
            self._local = threading.local()
            self._lock = threading.Lock()
            self.logger.info("Initialized in-memory DuckDB (persistent connections per thread)")
        else:
            self.logger.info(f"Initialized file-based DuckDB: {self.db_path}")
    
    # ===== 連接管理 =====
    
    def _get_memory_connection(self) -> duckdb.DuckDBPyConnection:
        """
        獲取內存數據庫的持久連接（每線程一個）
        
        Returns:
            duckdb.DuckDBPyConnection: 線程本地的持久連接
        """
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            with self._lock:
                if not hasattr(self._local, 'conn') or self._local.conn is None:
                    self._local.conn = duckdb.connect(':memory:', read_only=self.read_only)
                    self._local.conn.execute("SET memory_limit='4GB'")
                    self._local.conn.execute("SET threads TO 4")
                    self.logger.debug(
                        f"Created persistent memory connection for thread {threading.current_thread().name}")
        
        # 健康檢查
        try:
            self._local.conn.execute("SELECT 1").fetchone()
        except Exception:
            self.logger.warning("Memory connection invalid, recreating...")
            self._local.conn = duckdb.connect(':memory:', read_only=self.read_only)
            self._local.conn.execute("SET memory_limit='4GB'")
            self._local.conn.execute("SET threads TO 4")
        
        return self._local.conn
    
    @contextmanager
    def _connection(self):
        """
        獲取數據庫連接的 context manager
        
        - 內存數據庫：返回持久連接（不關閉）
        - 文件數據庫：創建臨時連接（自動關閉）
        
        Yields:
            duckdb.DuckDBPyConnection: DuckDB 連接對象
        """
        if self.is_memory_db:
            # 內存數據庫：使用持久連接
            conn = self._get_memory_connection()
            try:
                yield conn
            except Exception as e:
                self.logger.error(f"Memory DB operation error: {e}")
                raise
            # 注意：不關閉內存連接！
        else:
            # 文件數據庫：創建臨時連接
            conn = None
            try:
                conn = duckdb.connect(self.db_path, read_only=self.read_only)
                conn.execute("SET memory_limit='4GB'")
                conn.execute("SET threads TO 4")
                self.logger.debug(f"Created file connection for thread {threading.current_thread().name}")
                yield conn
            except Exception as e:
                self.logger.error(f"File DB operation error: {e}")
                raise
            finally:
                if conn is not None:
                    try:
                        conn.close()
                        self.logger.debug(f"Closed file connection for thread {threading.current_thread().name}")
                    except Exception as e:
                        self.logger.warning(f"Error closing file connection: {e}")
    
    # ===== Phase 2: Transaction 支持 =====
    
    @contextmanager
    def _transaction(self):
        """
        Transaction context manager
        提供原子操作支持 - 主要用於文件數據庫
        
        Yields:
            duckdb.DuckDBPyConnection: 處於 transaction 中的連接
        
        Example:
            def atomic_operation():
                with self._transaction() as conn:
                    conn.execute("DELETE FROM old_data")
                    conn.execute("INSERT INTO new_data SELECT * FROM source")
                # 自動 COMMIT，失敗則 ROLLBACK
        
        Note:
            內存數據庫也支援 transaction，但由於線程隔離，
            主要應用場景是文件數據庫的併發寫入
        """
        with self._connection() as conn:
            try:
                conn.execute("BEGIN TRANSACTION")
                self.logger.debug("Transaction started")
                yield conn
                conn.execute("COMMIT")
                self.logger.debug("Transaction committed")
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                    self.logger.warning(f"Transaction rolled back due to: {e}")
                except Exception as rollback_error:
                    self.logger.error(f"Rollback failed: {rollback_error}")
                raise
    
    # ===== 統一重試邏輯 =====
    
    def _with_retry(self, operation: Callable, max_retries: int = 3) -> Any:
        """
        統一的重試邏輯裝飾器
        
        Args:
            operation: 要執行的操作（callable）
            max_retries: 最大重試次數
            
        Returns:
            操作的返回值
            
        Raises:
            最後一次嘗試的異常
        """
        for attempt in range(max_retries):
            try:
                return operation()
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 0.1 * (attempt + 1)  # 指數退避
                    self.logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Operation failed after {max_retries} attempts: {e}")
                    raise
    
    # ===== 核心操作方法 =====
    
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取數據 - 保持 API 不變
        
        Args:
            query: SQL查詢語句
            **kwargs: 額外參數
                - limit: 限制返回行數
            
        Returns:
            pd.DataFrame: 查詢結果
        """
        # 準備查詢語句
        if query is None:
            if self.table_name:
                query = f"SELECT * FROM {self.table_name}"
            else:
                raise ValueError("No query provided and no default table specified")
        
        # 添加LIMIT子句（如果指定）
        limit = kwargs.get('limit')
        if limit and 'LIMIT' not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        def _read_once():
            """單次讀取操作"""
            with self._connection() as conn:
                self.logger.debug(f"Executing query: {query[:100]}...")
                result = conn.execute(query).df()
                self.logger.info(f"Query returned {len(result)} rows")
                return result
        
        def _read_with_retry():
            """帶重試的讀取"""
            return self._with_retry(_read_once)
        
        # 使用 asyncio.to_thread（Python 3.9+）或 run_in_executor
        try:
            return await asyncio.to_thread(_read_with_retry)
        except AttributeError:
            # Python 3.8 fallback
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _read_with_retry)
    
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入數據 - 保持 API 不變
        
        Args:
            data: 要寫入的DataFrame
            **kwargs: 額外參數
                - table_name: 表名
                - mode: 'replace' 或 'append'
            
        Returns:
            bool: 是否成功
        """
        table_name = kwargs.get('table_name', self.table_name)
        if not table_name:
            raise ValueError("No table name specified for writing")
        
        mode = kwargs.get('mode', 'replace')  # replace, append
        
        def _write_once():
            """單次寫入操作"""
            with self._connection() as conn:
                self.logger.info(f"Writing {len(data)} rows to {table_name} (mode={mode})")
                
                # 註冊DataFrame
                conn.register('temp_df', data)
                
                try:
                    if mode == 'replace':
                        # 刪除並重建表
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    elif mode == 'append':
                        # 檢查表是否存在
                        tables = conn.execute("SHOW TABLES").df()
                        table_exists = table_name in tables['name'].values if not tables.empty else False
                        
                        if table_exists:
                            # 追加到現有表
                            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                        else:
                            # 創建新表
                            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    else:
                        raise ValueError(f"Unsupported write mode: {mode}")
                    
                    self.logger.info(f"Successfully wrote data to {table_name}")
                    return True
                finally:
                    # 清理臨時表
                    try:
                        conn.unregister('temp_df')
                    except Exception:
                        pass
        
        def _write_with_retry():
            """帶重試的寫入"""
            return self._with_retry(_write_once)
        
        try:
            return await asyncio.to_thread(_write_with_retry)
        except AttributeError:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _write_with_retry)
    
    async def execute(self, sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        執行任意SQL語句 - 保持 API 不變
        
        Args:
            sql: SQL語句
            params: 參數化查詢的參數
            
        Returns:
            pd.DataFrame: 執行結果（如果有）
        """
        def _execute_once():
            """單次執行操作"""
            with self._connection() as conn:
                self.logger.debug(f"Executing SQL: {sql[:100]}...")
                
                if params:
                    result = conn.execute(sql, params)
                else:
                    result = conn.execute(sql)
                
                # 嘗試獲取結果
                try:
                    return result.df()
                except Exception:
                    # 某些語句（如 CREATE, DROP）沒有結果
                    return pd.DataFrame()
        
        def _execute_with_retry():
            """帶重試的執行"""
            return self._with_retry(_execute_once)
        
        try:
            return await asyncio.to_thread(_execute_with_retry)
        except AttributeError:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _execute_with_retry)
    
    # ===== Phase 2: Transaction 相關方法 =====
    
    async def write_atomic(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        原子寫入操作 - 新增方法，使用 transaction
        
        適用於需要保證原子性的寫入操作，特別是文件數據庫的併發場景
        
        Args:
            data: 要寫入的DataFrame
            **kwargs: 額外參數
                - table_name: 表名
                - mode: 'replace' 或 'append'
        
        Returns:
            bool: 是否成功
        
        Example:
            # 保證寫入的原子性
            success = await source.write_atomic(df, table_name='orders')
        """
        table_name = kwargs.get('table_name', self.table_name)
        if not table_name:
            raise ValueError("No table name specified for writing")
        
        mode = kwargs.get('mode', 'replace')
        
        def _write_atomic_once():
            """Atomic write operation with transaction"""
            with self._transaction() as conn:
                self.logger.info(f"Atomic write: {len(data)} rows to {table_name} (mode={mode})")
                
                # 註冊DataFrame
                conn.register('temp_df', data)
                
                try:
                    if mode == 'replace':
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    elif mode == 'append':
                        tables = conn.execute("SHOW TABLES").df()
                        table_exists = table_name in tables['name'].values if not tables.empty else False
                        
                        if table_exists:
                            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                        else:
                            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    else:
                        raise ValueError(f"Unsupported write mode: {mode}")
                    
                    self.logger.info(f"Atomic write completed: {table_name}")
                    return True
                finally:
                    try:
                        conn.unregister('temp_df')
                    except Exception:
                        pass
        
        def _write_atomic_with_retry():
            return self._with_retry(_write_atomic_once)
        
        try:
            return await asyncio.to_thread(_write_atomic_with_retry)
        except AttributeError:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _write_atomic_with_retry)
    
    async def execute_transaction(self, operations: List[Union[str, Tuple[str, Tuple]]]) -> bool:
        """
        執行一組 SQL 作為單一 transaction
        
        Args:
            operations: SQL 操作列表，每個可以是：
                - 字串：SQL 語句
                - Tuple[str, Tuple]: (SQL 語句, 參數)
        
        Returns:
            bool: 是否成功
        
        Example:
            # 執行多個 SQL 作為單一 transaction
            success = await source.execute_transaction([
                "DELETE FROM orders WHERE status='cancelled'",
                "INSERT INTO orders SELECT * FROM temp_orders",
                "UPDATE inventory SET quantity = quantity - 10"
            ])
        """
        def _execute_transaction_once():
            """Execute multiple SQL statements in a transaction"""
            with self._transaction() as conn:
                self.logger.info(f"Executing transaction with {len(operations)} operations")
                
                for i, operation in enumerate(operations):
                    if isinstance(operation, tuple):
                        sql, params = operation
                        self.logger.debug(f"  [{i+1}/{len(operations)}] {sql[:80]}... (with params)")
                        conn.execute(sql, params)
                    else:
                        sql = operation
                        self.logger.debug(f"  [{i+1}/{len(operations)}] {sql[:80]}...")
                        conn.execute(sql)
                
                self.logger.info("Transaction completed successfully")
                return True
        
        def _execute_transaction_with_retry():
            return self._with_retry(_execute_transaction_once)
        
        try:
            return await asyncio.to_thread(_execute_transaction_with_retry)
        except AttributeError:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _execute_transaction_with_retry)
    
    async def close(self):
        """
        關閉數據源 - 向後兼容
        
        - 內存數據庫：關閉線程本地連接
        - 文件數據庫：無需操作（每次自動關閉）
        """
        if self.is_memory_db and hasattr(self, '_local'):
            if hasattr(self._local, 'conn') and self._local.conn:
                try:
                    self._local.conn.close()
                    self.logger.info("Closed memory database connection")
                except Exception as e:
                    self.logger.warning(f"Error closing memory connection: {e}")
                finally:
                    self._local.conn = None
        
        self.logger.debug("Close completed")
        self.clear_cache()
        
        # Windows下可選等待
        if not self.is_memory_db and os.name == 'nt':
            await asyncio.sleep(0.1)
    
    # ===== 其他方法（保持不變）=====
    
    def get_metadata(self) -> Dict[str, Any]:
        """獲取數據庫元數據"""
        try:
            with self._connection() as conn:
                metadata = {
                    'db_path': self.db_path,
                    'read_only': self.read_only,
                    'is_memory_db': self.is_memory_db
                }
                
                # 獲取所有表
                tables = conn.execute("SHOW TABLES").df()
                metadata['tables'] = tables.to_dict('records') if not tables.empty else []
                metadata['num_tables'] = len(tables)
                
                # 獲取數據庫大小
                if not self.is_memory_db and Path(self.db_path).exists():
                    metadata['db_size'] = Path(self.db_path).stat().st_size
                
                # 獲取版本信息
                version_info = conn.execute("SELECT version()").fetchone()
                metadata['duckdb_version'] = version_info[0] if version_info else 'unknown'
                
                return metadata
        except Exception as e:
            self.logger.warning(f"Could not retrieve metadata: {str(e)}")
            return {'db_path': self.db_path, 'read_only': self.read_only, 'is_memory_db': self.is_memory_db}
    
    async def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """創建新表"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        
        try:
            await self.execute(create_sql)
            self.logger.info(f"Created table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}")
            return False
    
    async def drop_table(self, table_name: str) -> bool:
        """刪除表"""
        try:
            await self.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"Dropped table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop table: {str(e)}")
            return False
    
    async def list_tables(self) -> List[str]:
        """列出所有表"""
        try:
            result = await self.execute("SHOW TABLES")
            return result['name'].tolist() if 'name' in result.columns else []
        except Exception as e:
            self.logger.error(f"Failed to list tables: {str(e)}")
            return []
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """獲取表信息"""
        info = {}
        
        try:
            # 獲取列信息
            columns = await self.execute(f"DESCRIBE {table_name}")
            info['columns'] = columns.to_dict('records')
            
            # 獲取行數
            count_result = await self.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            info['row_count'] = int(count_result['count'].iloc[0])
            
            # 獲取表大小
            try:
                size_result = await self.execute(f"""
                    SELECT SUM(estimated_size) as size_bytes 
                    FROM duckdb_tables() 
                    WHERE table_name = '{table_name}'
                """)
                if not size_result.empty and size_result['size_bytes'].iloc[0] is not None:
                    info['size_bytes'] = int(size_result['size_bytes'].iloc[0])
            except Exception:
                pass
        except Exception as e:
            self.logger.warning(f"Could not get table info: {str(e)}")
        
        return info
    
    async def import_csv(self, csv_path: str, table_name: str, **kwargs) -> bool:
        """從CSV文件導入數據"""
        header = kwargs.get('header', True)
        delimiter = kwargs.get('delimiter', ',')
        
        sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto(
                '{csv_path}',
                header={header},
                delim='{delimiter}'
            )
        """
        
        try:
            await self.execute(sql)
            self.logger.info(f"Imported CSV to table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to import CSV: {str(e)}")
            return False
    
    async def export_csv(self, table_name: str, csv_path: str, **kwargs) -> bool:
        """導出表到CSV文件"""
        header = kwargs.get('header', True)
        delimiter = kwargs.get('delimiter', ',')
        
        sql = f"""
            COPY {table_name} TO '{csv_path}' 
            WITH (HEADER {header}, DELIMITER '{delimiter}')
        """
        
        try:
            await self.execute(sql)
            self.logger.info(f"Exported table {table_name} to CSV")
            return True
        except Exception as e:
            self.logger.error(f"Failed to export CSV: {str(e)}")
            return False
    
    async def import_parquet(self, parquet_path: str, table_name: str) -> bool:
        """從Parquet文件導入數據"""
        sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """
        
        try:
            await self.execute(sql)
            self.logger.info(f"Imported Parquet to table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to import Parquet: {str(e)}")
            return False
    
    async def export_parquet(self, table_name: str, parquet_path: str) -> bool:
        """導出表到Parquet文件"""
        sql = f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET)"
        
        try:
            await self.execute(sql)
            self.logger.info(f"Exported table {table_name} to Parquet")
            return True
        except Exception as e:
            self.logger.error(f"Failed to export Parquet: {str(e)}")
            return False
    
    async def create_index(self, table_name: str, index_name: str, columns: List[str]) -> bool:
        """創建索引"""
        columns_str = ', '.join(columns)
        sql = f"CREATE INDEX {index_name} ON {table_name} ({columns_str})"
        
        try:
            await self.execute(sql)
            self.logger.info(f"Created index {index_name} on {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create index: {str(e)}")
            return False
    
    # ===== 類方法（保持不變）=====
    
    @classmethod
    def create_memory_db(cls, **kwargs) -> 'DuckDBSource':
        """
        便捷方法：創建內存數據庫
        
        Args:
            **kwargs: 其他配置參數
            
        Returns:
            DuckDBSource: DuckDB數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={
                'db_path': ':memory:',
                **kwargs
            }
        )
        return cls(config)
    
    @classmethod
    def create_file_db(cls, db_path: str, **kwargs) -> 'DuckDBSource':
        """
        便捷方法：創建文件數據庫
        
        Args:
            db_path: 數據庫文件路徑
            **kwargs: 其他配置參數
            
        Returns:
            DuckDBSource: DuckDB數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={
                'db_path': db_path,
                **kwargs
            }
        )
        return cls(config)
    
    @classmethod
    def cleanup_executor(cls):
        """清理類級別的線程池（在程式結束時調用）"""
        if hasattr(cls, '_executor'):
            cls._executor.shutdown(wait=True)


# 工具函數：安全的檔案清理
def safe_file_cleanup(file_path: Path, max_retries: int = 3, delay: float = 0.5) -> bool:
    """
    安全的檔案清理，處理Windows文件鎖定問題
    
    Args:
        file_path: 要刪除的檔案路徑
        max_retries: 最大重試次數
        delay: 重試間隔（秒）
        
    Returns:
        bool: 是否成功刪除
    """
    for attempt in range(max_retries):
        try:
            if file_path.exists():
                file_path.unlink()
            return True
        except (PermissionError, OSError) as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                logging.getLogger(__name__).warning(
                    f"無法刪除檔案 {file_path}: {e}，跳過清理"
                )
                return False
    return False
