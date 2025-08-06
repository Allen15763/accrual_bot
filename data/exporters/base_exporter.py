"""
基礎匯出器

定義匯出器的基類和通用功能
"""

import os
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from datetime import datetime

from ...core.models.config_models import ExportConfig
from ...utils.logging import Logger


class BaseExporter(ABC):
    """匯出器基類"""
    
    def __init__(self, config: Optional[ExportConfig] = None):
        """
        初始化匯出器
        
        Args:
            config: 匯出配置
        """
        self.config = config or ExportConfig()
        self.logger = Logger().get_logger(__name__)
        
        # 確保輸出目錄存在
        self.output_dir = Path(self.config.output_filename).parent if self.config.output_filename else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def export(self, data: pd.DataFrame, output_path: Optional[str] = None) -> str:
        """
        匯出數據
        
        Args:
            data: 要匯出的DataFrame
            output_path: 輸出路徑（可選，會覆蓋配置中的路徑）
            
        Returns:
            str: 實際輸出的檔案路徑
        """
        pass
    
    def _generate_filename(self, base_name: str, extension: str) -> str:
        """
        生成檔案名稱
        
        Args:
            base_name: 基礎檔案名
            extension: 副檔名
            
        Returns:
            str: 生成的檔案名稱
        """
        if self.config.include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_name}_{timestamp}.{extension}"
        else:
            filename = f"{base_name}.{extension}"
        
        return filename
    
    def _prepare_data_for_export(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        準備要匯出的數據
        
        Args:
            data: 原始數據
            
        Returns:
            pd.DataFrame: 準備好的數據
        """
        # 選擇要匯出的欄位
        if self.config.export_columns:
            available_columns = [col for col in self.config.export_columns if col in data.columns]
            if available_columns:
                data = data[available_columns]
            else:
                self.logger.warning("指定的匯出欄位都不存在，將匯出所有欄位")
        
        # 清理數據
        data = data.copy()
        
        # 處理NaN值
        data = data.fillna('')
        
        return data
    
    def _validate_data(self, data: pd.DataFrame) -> bool:
        """
        驗證數據
        
        Args:
            data: 要驗證的數據
            
        Returns:
            bool: 是否有效
        """
        if data is None or data.empty:
            self.logger.error("數據為空，無法匯出")
            return False
        
        return True
    
    def get_export_summary(self, data: pd.DataFrame, output_path: str) -> Dict[str, Any]:
        """
        獲取匯出摘要
        
        Args:
            data: 匯出的數據
            output_path: 輸出路徑
            
        Returns:
            Dict[str, Any]: 匯出摘要
        """
        file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        
        return {
            "output_path": output_path,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / 1024 / 1024, 2),
            "total_rows": len(data),
            "total_columns": len(data.columns),
            "columns": list(data.columns),
            "export_time": datetime.now().isoformat(),
            "format": self.config.format
        }
    
    def export_multiple_sheets(self, 
                             data_dict: Dict[str, pd.DataFrame], 
                             output_path: Optional[str] = None) -> str:
        """
        匯出多個工作表（對於支援的格式）
        
        Args:
            data_dict: {sheet_name: dataframe} 的字典
            output_path: 輸出路徑
            
        Returns:
            str: 輸出檔案路徑
        """
        # 預設實現：只匯出第一個工作表
        if data_dict:
            first_sheet = next(iter(data_dict.values()))
            return self.export(first_sheet, output_path)
        else:
            raise ValueError("沒有數據可匯出")
    
    def supports_multiple_sheets(self) -> bool:
        """
        是否支援多工作表匯出
        
        Returns:
            bool: 是否支援
        """
        return False


class MultiFormatExporter:
    """多格式匯出器"""
    
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        self._exporters = {}
    
    def register_exporter(self, format_name: str, exporter_class: type):
        """
        註冊匯出器
        
        Args:
            format_name: 格式名稱
            exporter_class: 匯出器類別
        """
        self._exporters[format_name.lower()] = exporter_class
    
    def export(self, data: pd.DataFrame, 
               output_path: str, 
               format_type: str = "excel",
               config: Optional[ExportConfig] = None) -> str:
        """
        使用指定格式匯出數據
        
        Args:
            data: 要匯出的數據
            output_path: 輸出路徑
            format_type: 匯出格式
            config: 匯出配置
            
        Returns:
            str: 實際輸出路徑
        """
        format_type = format_type.lower()
        
        if format_type not in self._exporters:
            raise ValueError(f"不支援的匯出格式: {format_type}")
        
        exporter_class = self._exporters[format_type]
        exporter = exporter_class(config)
        
        return exporter.export(data, output_path)
    
    def get_supported_formats(self) -> List[str]:
        """
        獲取支援的格式列表
        
        Returns:
            List[str]: 支援的格式
        """
        return list(self._exporters.keys())


# 預設的多格式匯出器實例
_default_exporter = MultiFormatExporter()

def get_default_exporter() -> MultiFormatExporter:
    """獲取預設的多格式匯出器"""
    return _default_exporter

def register_default_exporters():
    """註冊預設的匯出器"""
    from .excel_exporter import ExcelExporter
    from .csv_exporter import CSVExporter
    from .json_exporter import JSONExporter
    
    _default_exporter.register_exporter("excel", ExcelExporter)
    _default_exporter.register_exporter("csv", CSVExporter)
    _default_exporter.register_exporter("json", JSONExporter)

# 自動註冊預設匯出器
register_default_exporters()
