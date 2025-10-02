"""
Excel數據源實現
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Any, List, Union
from pathlib import Path
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

try:
    from .base import DataSource
    from .config import DataSourceConfig, DataSourceType
except ImportError:
    from accrual_bot.core.datasources import DataSource
    from accrual_bot.core.datasources import DataSourceConfig, DataSourceType


class ExcelSource(DataSource):
    """Excel文件數據源"""
    
    # 類級別的線程池，避免每個實例創建新的線程池
    _executor = ThreadPoolExecutor(max_workers=4)
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化Excel數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.sheet_name = config.connection_params.get('sheet_name', 0)
        self.header = config.connection_params.get('header', 0)
        self.usecols = config.connection_params.get('usecols')
        self.dtype = config.connection_params.get('dtype')
        self.na_values = config.connection_params.get('na_values')
        self.parse_dates = config.connection_params.get('parse_dates')
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
    
    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        異步讀取Excel文件
        
        Args:
            query: SQL查詢（不適用於Excel，但保留接口一致性）
            **kwargs: 額外參數，可覆蓋初始化時的設定
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        # 獲取參數
        sheet_name = kwargs.get('sheet_name', self.sheet_name)
        header = kwargs.get('header', self.header)
        usecols = kwargs.get('usecols', self.usecols)
        dtype = kwargs.get('dtype', self.dtype)
        nrows = kwargs.get('nrows')
        skiprows = kwargs.get('skiprows')
        
        def read_excel_sync():
            try:
                self.logger.info(f"Reading Excel file: {self.file_path}")
                
                # 構建讀取參數
                read_kwargs = {
                    'sheet_name': sheet_name,
                    'header': header,
                    'engine': 'openpyxl'  # 使用openpyxl引擎
                }
                
                if usecols is not None:
                    read_kwargs['usecols'] = usecols
                if dtype is not None:
                    read_kwargs['dtype'] = dtype
                if self.na_values is not None:
                    read_kwargs['na_values'] = self.na_values
                if self.parse_dates is not None:
                    read_kwargs['parse_dates'] = self.parse_dates
                if nrows is not None:
                    read_kwargs['nrows'] = nrows
                if skiprows is not None:
                    read_kwargs['skiprows'] = skiprows
                
                df = pd.read_excel(self.file_path, **read_kwargs)
                
                # 如果有查詢條件，應用篩選（簡單實現）
                if query:
                    df = self._apply_query(df, query)
                
                self.logger.info(f"Successfully read {len(df)} rows from Excel")
                return df
                
            except Exception as e:
                self.logger.error(f"Error reading Excel file: {str(e)}")
                raise
        
        # 使用類級別的線程池執行器
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, read_excel_sync)
    
    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        異步寫入Excel文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        # 獲取參數
        sheet_name = kwargs.get('sheet_name', 'Sheet1')
        index = kwargs.get('index', False)
        mode = kwargs.get('mode', 'w')  # 'w' for write, 'a' for append
        if_sheet_exists = kwargs.get('if_sheet_exists', 'replace')
        
        def write_excel_sync():
            try:
                self.logger.info(f"Writing {len(data)} rows to Excel: {self.file_path}")
                
                # 如果是追加模式且檔案存在
                if mode == 'a' and self.file_path.exists():
                    with pd.ExcelWriter(
                        self.file_path, 
                        mode='a',
                        engine='openpyxl',
                        if_sheet_exists=if_sheet_exists
                    ) as writer:
                        data.to_excel(writer, sheet_name=sheet_name, index=index)
                else:
                    # 覆寫模式
                    with pd.ExcelWriter(
                        self.file_path,
                        engine='openpyxl'
                    ) as writer:
                        data.to_excel(writer, sheet_name=sheet_name, index=index)
                
                self.logger.info("Successfully wrote data to Excel")
                return True
                
            except Exception as e:
                self.logger.error(f"Error writing Excel file: {str(e)}")
                return False
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, write_excel_sync)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取Excel文件元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        metadata = {
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
            'file_modified': self.file_path.stat().st_mtime if self.file_path.exists() else None
        }
        
        # 嘗試獲取工作表信息
        try:
            # 使用上下文管理器確保pd.ExcelFile正確關閉
            with pd.ExcelFile(self.file_path, engine='openpyxl') as excel_file:
                metadata['sheet_names'] = excel_file.sheet_names
                metadata['num_sheets'] = len(excel_file.sheet_names)
        except Exception as e:
            self.logger.warning(f"Could not read sheet information: {str(e)}")
            metadata['sheet_names'] = []
            metadata['num_sheets'] = 0
        
        return metadata
    
    async def get_sheet_names(self) -> List[str]:
        """
        獲取所有工作表名稱
        
        Returns:
            List[str]: 工作表名稱列表
        """
        def get_sheets_sync():
            try:
                # 使用上下文管理器確保pd.ExcelFile正確關閉
                with pd.ExcelFile(self.file_path, engine='openpyxl') as excel_file:
                    return excel_file.sheet_names
            except Exception as e:
                self.logger.error(f"Error getting sheet names: {str(e)}")
                return []
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, get_sheets_sync)
    
    async def read_all_sheets(self) -> Dict[str, pd.DataFrame]:
        """
        讀取所有工作表
        
        Returns:
            Dict[str, pd.DataFrame]: 工作表名稱到DataFrame的映射
        """
        sheet_names = await self.get_sheet_names()
        result = {}
        
        for sheet_name in sheet_names:
            try:
                df = await self.read(sheet_name=sheet_name)
                result[sheet_name] = df
            except Exception as e:
                self.logger.warning(f"Could not read sheet {sheet_name}: {str(e)}")
        
        return result
    
    def _apply_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """
        應用簡單的查詢條件
        
        Args:
            df: 原始數據
            query: 查詢條件（支援簡單的篩選語法）
            
        Returns:
            pd.DataFrame: 篩選後的數據
        """
        try:
            # 使用pandas的query方法
            return df.query(query)
        except Exception as e:
            self.logger.warning(f"Could not apply query '{query}': {str(e)}")
            return df
    
    async def append_data(self, data: pd.DataFrame, sheet_name: str = None) -> bool:
        """
        追加數據到現有Excel文件
        
        Args:
            data: 要追加的數據
            sheet_name: 工作表名稱
            
        Returns:
            bool: 是否成功
        """
        if not self.file_path.exists():
            # 如果檔案不存在，直接寫入
            return await self.write(data, sheet_name=sheet_name or 'Sheet1')
        
        # 讀取現有數據
        existing_data = await self.read(sheet_name=sheet_name)
        
        # 合併數據
        combined_data = pd.concat([existing_data, data], ignore_index=True)
        
        # 寫回
        return await self.write(combined_data, sheet_name=sheet_name or 'Sheet1')
    
    async def close(self):
        """關閉資源（Excel不需要特別關閉連接）"""
        pass
    
    @classmethod
    def create_from_file(cls, file_path: str, **kwargs) -> 'ExcelSource':
        """
        便捷方法：從檔案路徑創建Excel數據源
        
        Args:
            file_path: Excel檔案路徑
            **kwargs: 其他配置參數
            
        Returns:
            ExcelSource: Excel數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.EXCEL,
            connection_params={
                'file_path': file_path,
                **kwargs
            }
        )
        return cls(config)
    
    @classmethod
    def cleanup_executor(cls):
        """清理類級別的線程池（在程式結束時調用）"""
        if hasattr(cls, '_executor'):
            cls._executor.shutdown(wait=True)
