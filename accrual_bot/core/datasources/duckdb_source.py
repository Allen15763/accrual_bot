"""
DuckDB數據源實現 - 修復併發問題版本
"""

import pandas as pd
import duckdb
from typing import Dict, Optional, Any, List, Union, Tuple
from pathlib import Path
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
    """DuckDB數據源 - 修復版本"""
    
    # 類級別的線程池
    _executor = ThreadPoolExecutor(max_workers=2)  # DuckDB連接較重，減少線程數
    
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
        
        # 使用線程本地存儲來管理連接，避免線程間共享連接
        self._local = threading.local()
        self._lock = threading.Lock()  # 用於保護連接創建
        self._async_lock = asyncio.Lock()  # 異步操作鎖
        self._closed = False
        self._connections = set()  # 追蹤所有連接
        
        # 初始化主連接
        self._init_connection()
    
    def _init_connection(self):
        """初始化連接"""
        try:
            self.logger.info(f"Initializing DuckDB: {self.db_path}")
            # 主線程連接
            self._main_conn = duckdb.connect(self.db_path, read_only=self.read_only)
            self._connections.add(self._main_conn)
            
            # 設置一些優化參數
            self._main_conn.execute("SET memory_limit='4GB'")
            self._main_conn.execute("SET threads TO 4")
            
            # 如果是文件數據庫，啟用WAL模式提高併發性能
            if self.db_path != ':memory:' and not self.read_only:
                try:
                    self._main_conn.execute("PRAGMA journal_mode=WAL;")
                    self.logger.debug("Enabled WAL mode for better concurrency")
                except Exception as wal_error:
                    self.logger.warning(f"Could not enable WAL mode: {wal_error}")
            
            self.logger.info("Successfully initialized DuckDB")
        except Exception as e:
            self.logger.error(f"Failed to initialize DuckDB: {str(e)}")
            raise
    
    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        獲取當前線程的連接
        每個線程都有自己的連接，避免線程安全問題
        """
        if self._closed:
            raise RuntimeError("DuckDB connection has been closed")
            
        # 檢查當前線程是否已有連接
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            with self._lock:
                # 雙重檢查
                if not hasattr(self._local, 'conn') or self._local.conn is None:
                    # 為當前線程創建新連接
                    self.logger.debug(f"Creating new connection for thread {threading.current_thread().name}")
                    self._local.conn = duckdb.connect(self.db_path, read_only=self.read_only)
                    self._connections.add(self._local.conn)
                    
                    # 設置優化參數
                    self._local.conn.execute("SET memory_limit='4GB'")
                    self._local.conn.execute("SET threads TO 4")
                    
                    # 如果是文件數據庫，啟用WAL模式
                    if self.db_path != ':memory:' and not self.read_only:
                        try:
                            self._local.conn.execute("PRAGMA journal_mode=WAL;")
                        except Exception:
                            pass  # 忽略WAL模式錯誤
        
        return self._local.conn
    
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取數據
        
        Args:
            query: SQL查詢語句
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 查詢結果
        """
        # 如果沒有提供查詢，使用預設表名
        if query is None:
            if self.table_name:
                query = f"SELECT * FROM {self.table_name}"
            else:
                raise ValueError("No query provided and no default table specified")
        
        # 添加LIMIT子句（如果指定）
        limit = kwargs.get('limit')
        if limit and 'LIMIT' not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        def execute_query():
            try:
                conn = self._get_connection()
                self.logger.debug(f"Executing query: {query[:100]}...")
                
                # 使用事務確保讀取一致性
                with conn.begin():
                    result = conn.execute(query).df()
                
                self.logger.info(f"Query returned {len(result)} rows")
                return result
            except Exception as e:
                self.logger.error(f"Query execution failed: {str(e)}")
                raise
        
        # 使用異步鎖確保併發安全
        async with self._async_lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, execute_query)
    
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入數據 - 改進版本，確保事務完整性
        
        Args:
            data: 要寫入的DataFrame
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        table_name = kwargs.get('table_name', self.table_name)
        if not table_name:
            raise ValueError("No table name specified for writing")
        
        mode = kwargs.get('mode', 'replace')  # replace, append
        
        def write_data():
            conn = None
            try:
                conn = self._get_connection()
                self.logger.info(f"Writing {len(data)} rows to table {table_name}")
                
                # 使用顯式事務確保數據完整性
                with conn.begin():
                    # 註冊DataFrame到當前連接
                    conn.register('temp_df', data)
                    
                    if mode == 'replace':
                        # 刪除並重建表
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    elif mode == 'append':
                        # 檢查表是否存在
                        try:
                            tables = conn.execute("SHOW TABLES").df()
                            table_exists = table_name in tables['name'].values if not tables.empty else False
                        except Exception:
                            # 如果SHOW TABLES失敗，嘗試查詢表
                            try:
                                conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                                table_exists = True
                            except Exception:
                                table_exists = False
                        
                        if table_exists:
                            # 追加到現有表
                            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                        else:
                            # 創建新表
                            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                    else:
                        raise ValueError(f"Unsupported write mode: {mode}")
                    
                    # 確保事務提交前數據已寫入
                    conn.commit()
                
                # 清理臨時表
                try:
                    conn.unregister('temp_df')
                except Exception:
                    pass  # 忽略清理錯誤
                
                self.logger.info(f"Successfully wrote data to {table_name}")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to write data: {str(e)}")
                # 如果有事務，嘗試回滾
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                return False
        
        # 使用異步鎖確保寫入操作的原子性
        async with self._async_lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, write_data)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取數據庫元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        conn = self._get_connection()
        
        metadata = {
            'db_path': self.db_path,
            'read_only': self.read_only
        }
        
        try:
            # 獲取所有表
            tables = conn.execute("SHOW TABLES").df()
            metadata['tables'] = tables.to_dict('records') if not tables.empty else []
            metadata['num_tables'] = len(tables)
            
            # 獲取數據庫大小（如果是文件數據庫）
            if self.db_path != ':memory:' and Path(self.db_path).exists():
                metadata['db_size'] = Path(self.db_path).stat().st_size
            
            # 獲取版本信息
            version_info = conn.execute("SELECT version()").fetchone()
            metadata['duckdb_version'] = version_info[0] if version_info else 'unknown'
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve metadata: {str(e)}")
        
        return metadata
    
    async def execute(self, sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        執行任意SQL語句 - 改進版本
        
        Args:
            sql: SQL語句
            params: 參數化查詢的參數
            
        Returns:
            pd.DataFrame: 執行結果（如果有）
        """
        def execute_sql():
            try:
                conn = self._get_connection()
                self.logger.debug(f"Executing SQL: {sql[:100]}...")
                
                # 使用事務確保SQL執行的原子性
                with conn.begin():
                    if params:
                        result = conn.execute(sql, params)
                    else:
                        result = conn.execute(sql)
                    
                    # 嘗試獲取結果（某些語句可能沒有結果）
                    try:
                        df = result.df()
                        return df
                    except Exception:
                        return pd.DataFrame()
                        
            except Exception as e:
                self.logger.error(f"SQL execution failed: {str(e)}")
                raise
        
        async with self._async_lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._executor, execute_sql)
    
    async def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """
        創建新表
        
        Args:
            table_name: 表名
            schema: 列名到類型的映射
            
        Returns:
            bool: 是否成功
        """
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
        """
        刪除表
        
        Args:
            table_name: 表名
            
        Returns:
            bool: 是否成功
        """
        try:
            await self.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"Dropped table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop table: {str(e)}")
            return False
    
    async def list_tables(self) -> List[str]:
        """
        列出所有表
        
        Returns:
            List[str]: 表名列表
        """
        try:
            result = await self.execute("SHOW TABLES")
            return result['name'].tolist() if 'name' in result.columns else []
        except Exception as e:
            self.logger.error(f"Failed to list tables: {str(e)}")
            return []
    
    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        獲取表信息
        
        Args:
            table_name: 表名
            
        Returns:
            Dict[str, Any]: 表信息
        """
        info = {}
        
        try:
            # 獲取列信息
            columns = await self.execute(f"DESCRIBE {table_name}")
            info['columns'] = columns.to_dict('records')
            
            # 獲取行數
            count_result = await self.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            info['row_count'] = int(count_result['count'].iloc[0])
            
            # 獲取表大小（估算）
            try:
                size_result = await self.execute(f"""
                    SELECT 
                        SUM(estimated_size) as size_bytes 
                    FROM duckdb_tables() 
                    WHERE table_name = '{table_name}'
                """)
                if not size_result.empty and size_result['size_bytes'].iloc[0] is not None:
                    info['size_bytes'] = int(size_result['size_bytes'].iloc[0])
            except Exception:
                # 某些版本的DuckDB可能不支援這個函數
                pass
            
        except Exception as e:
            self.logger.warning(f"Could not get table info: {str(e)}")
        
        return info
    
    async def import_csv(self, csv_path: str, table_name: str, **kwargs) -> bool:
        """
        從CSV文件導入數據
        
        Args:
            csv_path: CSV文件路徑
            table_name: 目標表名
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
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
        """
        導出表到CSV文件
        
        Args:
            table_name: 源表名
            csv_path: 目標CSV文件路徑
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
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
        """
        從Parquet文件導入數據
        
        Args:
            parquet_path: Parquet文件路徑
            table_name: 目標表名
            
        Returns:
            bool: 是否成功
        """
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
        """
        導出表到Parquet文件
        
        Args:
            table_name: 源表名
            parquet_path: 目標Parquet文件路徑
            
        Returns:
            bool: 是否成功
        """
        sql = f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET)"
        
        try:
            await self.execute(sql)
            self.logger.info(f"Exported table {table_name} to Parquet")
            return True
        except Exception as e:
            self.logger.error(f"Failed to export Parquet: {str(e)}")
            return False
    
    async def create_index(self, table_name: str, index_name: str, 
                           columns: List[str]) -> bool:
        """
        創建索引
        
        Args:
            table_name: 表名
            index_name: 索引名
            columns: 索引列
            
        Returns:
            bool: 是否成功
        """
        columns_str = ', '.join(columns)
        sql = f"CREATE INDEX {index_name} ON {table_name} ({columns_str})"
        
        try:
            await self.execute(sql)
            self.logger.info(f"Created index {index_name} on {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create index: {str(e)}")
            return False
    
    async def close(self):
        """關閉數據庫連接 - 改進版本，確保資源清理"""
        async with self._async_lock:
            if self._closed:
                return
                
            self._closed = True
            
            try:
                # 等待所有正在執行的操作完成
                await asyncio.sleep(0.1)
                
                # 關閉所有追蹤的連接
                for conn in list(self._connections):
                    try:
                        if conn:
                            conn.close()
                    except Exception as e:
                        self.logger.warning(f"Error closing connection: {e}")
                
                self._connections.clear()
                
                # 關閉主連接
                if hasattr(self, '_main_conn') and self._main_conn:
                    try:
                        self._main_conn.close()
                        self._main_conn = None
                    except Exception as e:
                        self.logger.warning(f"Error closing main connection: {e}")
                
                # 清理線程本地連接
                if hasattr(self, '_local'):
                    if hasattr(self._local, 'conn') and self._local.conn:
                        try:
                            self._local.conn.close()
                            delattr(self._local, 'conn')
                        except Exception as e:
                            self.logger.warning(f"Error closing local connection: {e}")
                
                self.logger.info("Closed DuckDB connections")
                
                # Windows下額外等待文件釋放
                if self.db_path != ':memory:' and os.name == 'nt':
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                self.logger.error(f"Error during cleanup: {str(e)}")
    
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
                # 最後一次嘗試失敗，記錄警告但不拋出異常
                logging.getLogger(__name__).warning(
                    f"無法刪除檔案 {file_path}: {e}，跳過清理"
                )
                return False
    return False
