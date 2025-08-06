"""
JSON匯出器

專門處理JSON格式的匯出功能
"""

import pandas as pd
import json
from typing import Optional, Dict, Any, Union
from pathlib import Path
from datetime import datetime
import numpy as np

from .base_exporter import BaseExporter
from core.models.config_models import ExportConfig
from utils.logging import Logger


class JSONExporter(BaseExporter):
    """JSON匯出器"""
    
    def __init__(self, config: Optional[ExportConfig] = None):
        super().__init__(config)
        
        # JSON特定設定
        self.indent = 2
        self.ensure_ascii = False
        self.date_format = 'iso'  # 'iso', 'epoch', 'string'
    
    def export(self, data: pd.DataFrame, output_path: Optional[str] = None) -> str:
        """
        匯出JSON檔案
        
        Args:
            data: 要匯出的DataFrame
            output_path: 輸出路徑
            
        Returns:
            str: 實際輸出路徑
        """
        if not self._validate_data(data):
            raise ValueError("數據驗證失敗")
        
        # 準備數據
        export_data = self._prepare_data_for_export(data)
        
        # 確定輸出路徑
        if output_path is None:
            filename = self._generate_filename(
                self.config.output_filename or "export", 
                "json"
            )
            output_path = str(self.output_dir / filename)
        
        try:
            # 轉換為JSON格式
            json_data = self._dataframe_to_json(export_data)
            
            # 寫入檔案
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(
                    json_data, 
                    f, 
                    indent=self.indent,
                    ensure_ascii=self.ensure_ascii,
                    default=self._json_serializer
                )
            
            self.logger.info(f"成功匯出JSON檔案: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"JSON匯出失敗: {e}")
            raise
    
    def _dataframe_to_json(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        將DataFrame轉換為JSON結構
        
        Args:
            data: 要轉換的DataFrame
            
        Returns:
            Dict[str, Any]: JSON數據結構
        """
        # 處理特殊值
        data_cleaned = data.copy()
        
        # 處理NaN值
        data_cleaned = data_cleaned.where(pd.notna(data_cleaned), None)
        
        # 處理日期
        for col in data_cleaned.columns:
            if pd.api.types.is_datetime64_any_dtype(data_cleaned[col]):
                if self.date_format == 'iso':
                    data_cleaned[col] = data_cleaned[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif self.date_format == 'epoch':
                    data_cleaned[col] = data_cleaned[col].astype('int64') // 10**9
        
        # 轉換為記錄格式
        records = data_cleaned.to_dict('records')
        
        # 創建完整的JSON結構
        json_structure = {
            "metadata": {
                "export_time": datetime.now().isoformat(),
                "total_records": len(data),
                "columns": list(data.columns),
                "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()},
                "export_config": {
                    "format": "json",
                    "date_format": self.date_format,
                    "include_index": self.config.include_index
                }
            },
            "data": records
        }
        
        return json_structure
    
    def _json_serializer(self, obj):
        """JSON序列化器，處理特殊類型"""
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        
        # 如果無法序列化，轉為字符串
        return str(obj)
    
    def export_multiple_sheets(self, 
                             data_dict: Dict[str, pd.DataFrame], 
                             output_path: Optional[str] = None) -> str:
        """
        匯出多工作表為單一JSON檔案
        
        Args:
            data_dict: {sheet_name: dataframe} 的字典
            output_path: 輸出路徑
            
        Returns:
            str: 輸出檔案路徑
        """
        if not data_dict:
            raise ValueError("沒有數據可匯出")
        
        # 確定輸出路徑
        if output_path is None:
            filename = self._generate_filename(
                self.config.output_filename or "export_multiple", 
                "json"
            )
            output_path = str(self.output_dir / filename)
        
        try:
            # 處理每個工作表
            sheets_data = {}
            total_records = 0
            
            for sheet_name, data in data_dict.items():
                if not self._validate_data(data):
                    self.logger.warning(f"跳過無效的工作表: {sheet_name}")
                    continue
                
                export_data = self._prepare_data_for_export(data)
                sheet_json = self._dataframe_to_json(export_data)
                sheets_data[sheet_name] = sheet_json
                total_records += len(data)
            
            # 創建完整的JSON結構
            complete_json = {
                "metadata": {
                    "export_time": datetime.now().isoformat(),
                    "total_sheets": len(sheets_data),
                    "total_records": total_records,
                    "sheet_names": list(sheets_data.keys()),
                    "export_config": {
                        "format": "json_multiple_sheets",
                        "date_format": self.date_format,
                        "include_index": self.config.include_index
                    }
                },
                "sheets": sheets_data
            }
            
            # 寫入檔案
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(
                    complete_json, 
                    f, 
                    indent=self.indent,
                    ensure_ascii=self.ensure_ascii,
                    default=self._json_serializer
                )
            
            self.logger.info(f"成功匯出多工作表JSON檔案: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"多工作表JSON匯出失敗: {e}")
            raise
    
    def export_as_array(self, data: pd.DataFrame, output_path: Optional[str] = None) -> str:
        """
        匯出為簡單的JSON陣列格式
        
        Args:
            data: 要匯出的DataFrame
            output_path: 輸出路徑
            
        Returns:
            str: 實際輸出路徑
        """
        if not self._validate_data(data):
            raise ValueError("數據驗證失敗")
        
        # 準備數據
        export_data = self._prepare_data_for_export(data)
        
        # 確定輸出路徑
        if output_path is None:
            filename = self._generate_filename(
                f"{self.config.output_filename or 'export'}_array", 
                "json"
            )
            output_path = str(self.output_dir / filename)
        
        try:
            # 處理特殊值
            data_cleaned = export_data.copy()
            data_cleaned = data_cleaned.where(pd.notna(data_cleaned), None)
            
            # 轉換為記錄陣列
            records = data_cleaned.to_dict('records')
            
            # 寫入檔案
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(
                    records, 
                    f, 
                    indent=self.indent,
                    ensure_ascii=self.ensure_ascii,
                    default=self._json_serializer
                )
            
            self.logger.info(f"成功匯出JSON陣列檔案: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"JSON陣列匯出失敗: {e}")
            raise
    
    def supports_multiple_sheets(self) -> bool:
        """支援多工作表匯出"""
        return True


def export_to_json(data: Union[pd.DataFrame, Dict[str, pd.DataFrame]], 
                  output_path: str,
                  config: Optional[ExportConfig] = None,
                  as_array: bool = False) -> str:
    """
    匯出到JSON的便捷函數
    
    Args:
        data: 要匯出的數據（單一DataFrame或多工作表字典）
        output_path: 輸出路徑
        config: 匯出配置
        as_array: 是否匯出為簡單陣列格式
        
    Returns:
        str: 實際輸出路徑
    """
    exporter = JSONExporter(config)
    
    if isinstance(data, dict):
        return exporter.export_multiple_sheets(data, output_path)
    else:
        if as_array:
            return exporter.export_as_array(data, output_path)
        else:
            return exporter.export(data, output_path)
