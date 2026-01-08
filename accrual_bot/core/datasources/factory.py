"""
數據源工廠類
用於創建不同類型的數據源實例
"""

from typing import Dict, Type, Optional, Any
import logging
from pathlib import Path
import atexit

try:
    from .base import DataSource, DataSourceType
    from .config import DataSourceConfig
    from .excel_source import ExcelSource
    from .csv_source import CSVSource
    from .parquet_source import ParquetSource
    from .duckdb_source import DuckDBSource
except ImportError:
    from accrual_bot.core.datasources import DataSource, DataSourceType
    from accrual_bot.core.datasources import DataSourceConfig
    from accrual_bot.core.datasources import ExcelSource
    from accrual_bot.core.datasources import CSVSource
    from accrual_bot.core.datasources import ParquetSource
    from accrual_bot.core.datasources import DuckDBSource


class DataSourceFactory:
    """數據源工廠"""
    
    # 註冊的數據源類型
    _sources: Dict[DataSourceType, Type[DataSource]] = {
        DataSourceType.EXCEL: ExcelSource,
        DataSourceType.CSV: CSVSource,
        DataSourceType.PARQUET: ParquetSource,
        DataSourceType.DUCKDB: DuckDBSource,
    }
    
    logger = logging.getLogger("DataSourceFactory")
    
    # 標記是否已註冊清理函數
    _cleanup_registered = False
    
    @classmethod
    def create(cls, config: DataSourceConfig) -> DataSource:
        """
        創建數據源實例
        
        Args:
            config: 數據源配置
            
        Returns:
            DataSource: 數據源實例
            
        Raises:
            ValueError: 配置無效
            NotImplementedError: 數據源類型未實現
        """
        # 註冊清理函數（只註冊一次）
        cls._register_cleanup()
        
        # 驗證配置
        is_valid, errors = config.validate()
        if not is_valid:
            error_msg = f"Invalid configuration: {', '.join(errors)}"
            cls.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 獲取對應的數據源類
        source_class = cls._sources.get(config.source_type)
        if not source_class:
            raise NotImplementedError(f"Data source {config.source_type} not implemented")
        
        # 創建實例
        cls.logger.info(f"Creating {config.source_type.value} data source")
        return source_class(config)
    
    @classmethod
    def create_from_file(cls, file_path: str, **kwargs) -> DataSource:
        """
        根據文件擴展名自動創建數據源
        
        Args:
            file_path: 文件路徑
            **kwargs: 額外配置參數
            
        Returns:
            DataSource: 數據源實例
            
        Raises:
            ValueError: 不支援的文件類型
        """
        # 註冊清理函數
        cls._register_cleanup()
        
        path = Path(file_path)
        extension = path.suffix.lower()
        
        # 根據擴展名判斷類型
        if extension in ['.xlsx', '.xls']:
            source_type = DataSourceType.EXCEL
        elif extension == '.csv':
            source_type = DataSourceType.CSV
        elif extension == '.parquet':
            source_type = DataSourceType.PARQUET
        elif extension == '.db' or extension == '.duckdb':
            source_type = DataSourceType.DUCKDB
        else:
            raise ValueError(f"Unsupported file type: {extension}")
        
        # 創建配置
        if source_type == DataSourceType.DUCKDB:
            config = DataSourceConfig(
                source_type=source_type,
                connection_params={
                    'db_path': file_path,
                    **kwargs
                }
            )
        else:
            config = DataSourceConfig(
                source_type=source_type,
                connection_params={
                    'file_path': file_path,
                    **kwargs
                }
            )
        
        return cls.create(config)
    
    @classmethod
    def register_source(cls, source_type: DataSourceType, 
                        source_class: Type[DataSource]):
        """
        註冊新的數據源類型
        
        Args:
            source_type: 數據源類型
            source_class: 數據源類
        """
        cls._sources[source_type] = source_class
        cls.logger.info(f"Registered new data source type: {source_type.value}")
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        獲取支援的數據源類型
        
        Returns:
            list: 支援的類型列表
        """
        return list(cls._sources.keys())
    
    @classmethod
    def create_batch(cls, configs: list) -> Dict[str, DataSource]:
        """
        批量創建數據源
        
        Args:
            configs: 配置列表，每個元素是(名稱, 配置)元組
            
        Returns:
            Dict[str, DataSource]: 名稱到數據源的映射
        """
        # 註冊清理函數
        cls._register_cleanup()
        
        sources = {}
        
        for name, config in configs:
            try:
                sources[name] = cls.create(config)
                cls.logger.info(f"Created data source: {name}")
            except Exception as e:
                cls.logger.error(f"Failed to create data source {name}: {str(e)}")
        
        return sources
    
    @classmethod
    def _register_cleanup(cls):
        """註冊清理函數（程式退出時自動調用）"""
        if not cls._cleanup_registered:
            atexit.register(cls._cleanup_all_executors)
            cls._cleanup_registered = True
    
    @classmethod
    def _cleanup_all_executors(cls):
        """清理所有數據源的線程池"""
        cls.logger.info("Cleaning up data source executors...")
        
        # 清理每個數據源類的執行器
        for source_class in cls._sources.values():
            if hasattr(source_class, 'cleanup_executor'):
                try:
                    source_class.cleanup_executor()
                    cls.logger.debug(f"Cleaned up executor for {source_class.__name__}")
                except Exception as e:
                    cls.logger.warning(f"Error cleaning up {source_class.__name__}: {e}")
        
        cls.logger.info("Data source cleanup completed")


class DataSourcePool:
    """
    數據源連接池（用於管理多個數據源）
    """
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
        self.logger = logging.getLogger("DataSourcePool")
    
    def add_source(self, name: str, source: DataSource):
        """
        添加數據源到池
        
        Args:
            name: 數據源名稱
            source: 數據源實例
        """
        self.sources[name] = source
        self.logger.info(f"Added data source to pool: {name}")
    
    def get_source(self, name: str) -> Optional[DataSource]:
        """
        獲取數據源
        
        Args:
            name: 數據源名稱
            
        Returns:
            Optional[DataSource]: 數據源實例
        """
        return self.sources.get(name)
    
    def remove_source(self, name: str) -> bool:
        """
        移除數據源
        
        Args:
            name: 數據源名稱
            
        Returns:
            bool: 是否成功
        """
        if name in self.sources:
            # 先關閉連接
            source = self.sources[name]
            try:
                import asyncio
                if asyncio.iscoroutinefunction(source.close):
                    asyncio.run(source.close())
            except Exception as err:
                self.logger.error(f'{err}')
            
            del self.sources[name]
            self.logger.info(f"Removed data source from pool: {name}")
            return True
        return False
    
    async def close_all(self):
        """關閉所有數據源"""
        for name, source in self.sources.items():
            try:
                await source.close()
                self.logger.info(f"Closed data source: {name}")
            except Exception as e:
                self.logger.error(f"Failed to close data source {name}: {str(e)}")
    
    def list_sources(self) -> list:
        """
        列出所有數據源
        
        Returns:
            list: 數據源名稱列表
        """
        return list(self.sources.keys())
    
    async def execute_on_all(self, method_name: str, *args, **kwargs) -> Dict[str, Any]:
        """
        在所有數據源上執行相同的方法
        
        Args:
            method_name: 方法名
            *args: 位置參數
            **kwargs: 關鍵字參數
            
        Returns:
            Dict[str, Any]: 執行結果
        """
        import asyncio
        results = {}
        
        # 創建任務列表
        tasks = []
        names = []
        
        for name, source in self.sources.items():
            method = getattr(source, method_name, None)
            if method and callable(method):
                if asyncio.iscoroutinefunction(method):
                    tasks.append(method(*args, **kwargs))
                    names.append(name)
                else:
                    # 同步方法，直接執行
                    try:
                        results[name] = method(*args, **kwargs)
                    except Exception as e:
                        self.logger.error(f"Failed to execute {method_name} on {name}: {str(e)}")
                        results[name] = None
        
        # 並發執行異步任務
        if tasks:
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            for name, result in zip(names, task_results):
                if isinstance(result, Exception):
                    self.logger.error(f"Failed to execute {method_name} on {name}: {str(result)}")
                    results[name] = None
                else:
                    results[name] = result
        
        return results
    
    def __del__(self):
        """析構函數，確保資源釋放"""
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.close_all())
            loop.close()
        except Exception as err:
            self.logger.error(f'{err}')


def create_quick_source(data: Any, source_type: str = 'auto') -> DataSource:
    """
    快速創建數據源的便捷函數
    
    Args:
        data: 數據（文件路徑、DataFrame等）
        source_type: 數據源類型，'auto'表示自動判斷
        
    Returns:
        DataSource: 數據源實例
    """
    import pandas as pd
    
    # 自動判斷類型
    if source_type == 'auto':
        if isinstance(data, str):
            # 文件路徑
            return DataSourceFactory.create_from_file(data)
        elif isinstance(data, pd.DataFrame):
            # DataFrame - 使用DuckDB內存數據庫
            source = DuckDBSource.create_memory_db()
            # 這裡需要異步操作，簡化處理
            import asyncio
            asyncio.run(source.write(data, table_name='data'))
            return source
        else:
            raise ValueError(f"Cannot determine source type for data type: {type(data)}")
    
    # 指定類型
    if source_type == 'excel':
        return ExcelSource.create_from_file(data)
    elif source_type == 'csv':
        return CSVSource.create_from_file(data)
    elif source_type == 'parquet':
        return ParquetSource.create_from_file(data)
    elif source_type == 'duckdb':
        return DuckDBSource.create_file_db(data)
    else:
        raise ValueError(f"Unknown source type: {source_type}")
