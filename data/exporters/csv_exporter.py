"""
CSV匯出器

專門處理CSV格式的匯出功能
"""

import pandas as pd
import csv
from typing import Optional, Dict, Any
from pathlib import Path

from .base_exporter import BaseExporter
from core.models.config_models import ExportConfig
from utils.logging import Logger


class CSVExporter(BaseExporter):
    """CSV匯出器"""
    
    def __init__(self, config: Optional[ExportConfig] = None):
        super().__init__(config)
        
        # CSV特定設定
        self.encoding = getattr(config, 'csv_encoding', 'utf-8-sig')
        self.separator = getattr(config, 'csv_separator', ',')
    
    def export(self, data: pd.DataFrame, output_path: Optional[str] = None) -> str:
        """
        匯出CSV檔案
        
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
                "csv"
            )
            output_path = str(self.output_dir / filename)
        
        try:
            # 匯出CSV
            export_data.to_csv(
                output_path,
                index=self.config.include_index,
                encoding=self.encoding,
                sep=self.separator,
                quoting=csv.QUOTE_NONNUMERIC,  # 對非數字值加引號
                escapechar='\\',
                lineterminator='\n'
            )
            
            self.logger.info(f"成功匯出CSV檔案: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"CSV匯出失敗: {e}")
            raise
    
    def export_multiple_sheets(self, 
                               data_dict: Dict[str, pd.DataFrame], 
                               output_path: Optional[str] = None) -> str:
        """
        匯出多個CSV檔案（每個工作表一個檔案）
        
        Args:
            data_dict: {sheet_name: dataframe} 的字典
            output_path: 輸出路徑基礎名稱
            
        Returns:
            str: 主要輸出目錄路徑
        """
        if not data_dict:
            raise ValueError("沒有數據可匯出")
        
        # 確定輸出目錄
        if output_path is None:
            base_name = self.config.output_filename or "export_multiple"
            output_dir = self.output_dir / f"{base_name}_csv"
        else:
            output_dir = Path(output_path).parent / f"{Path(output_path).stem}_csv"
        
        # 創建輸出目錄
        output_dir.mkdir(parents=True, exist_ok=True)
        
        exported_files = []
        
        try:
            for sheet_name, data in data_dict.items():
                if not self._validate_data(data):
                    self.logger.warning(f"跳過無效的工作表: {sheet_name}")
                    continue
                
                # 安全的檔案名稱
                safe_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')).strip()
                csv_filename = f"{safe_sheet_name}.csv"
                csv_path = output_dir / csv_filename
                
                # 匯出個別CSV
                self.export(data, str(csv_path))
                exported_files.append(str(csv_path))
            
            self.logger.info(f"成功匯出 {len(exported_files)} 個CSV檔案到: {output_dir}")
            return str(output_dir)
            
        except Exception as e:
            self.logger.error(f"多CSV檔案匯出失敗: {e}")
            raise
    
    def supports_multiple_sheets(self) -> bool:
        """支援多檔案匯出"""
        return True


def export_to_csv(data: pd.DataFrame, 
                  output_path: str,
                  config: Optional[ExportConfig] = None) -> str:
    """
    匯出到CSV的便捷函數
    
    Args:
        data: 要匯出的DataFrame
        output_path: 輸出路徑
        config: 匯出配置
        
    Returns:
        str: 實際輸出路徑
    """
    exporter = CSVExporter(config)
    return exporter.export(data, output_path)
