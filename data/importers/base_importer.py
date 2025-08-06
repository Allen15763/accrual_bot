"""
基礎數據導入器
提供通用的檔案導入功能
"""

import os
import pandas as pd
import numpy as np  
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union, Any
import concurrent.futures
import threading
import time

try:
    from ...utils import (
        get_logger, validate_file_path, is_excel_file, is_csv_file,
        get_file_extension, CONCURRENT_SETTINGS
    )
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from utils import (
        get_logger, validate_file_path, is_excel_file, is_csv_file,
        get_file_extension, CONCURRENT_SETTINGS
    )


class BaseDataImporter:
    """基礎數據導入器"""
    
    def __init__(self):
        """初始化基礎數據導入器"""
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = CONCURRENT_SETTINGS['MAX_WORKERS']
        self.timeout = CONCURRENT_SETTINGS['TIMEOUT']
        
        self.logger.info("初始化基礎數據導入器")
    
    def import_file(self, file_path: str, sheet_name: Optional[Union[str, int]] = None,
                   encoding: str = 'utf-8', **kwargs) -> pd.DataFrame:
        """
        導入單個檔案
        
        Args:
            file_path: 檔案路徑
            sheet_name: Excel工作表名稱或索引
            encoding: 編碼格式
            **kwargs: 其他參數
            
        Returns:
            pd.DataFrame: 導入的數據
        """
        try:
            if not validate_file_path(file_path):
                raise ValueError(f"無效的檔案路徑: {file_path}")
            
            self.logger.info(f"開始導入檔案: {Path(file_path).name}")
            
            start_time = time.time()
            
            if is_excel_file(file_path):
                df = self._import_excel(file_path, sheet_name, **kwargs)
            elif is_csv_file(file_path):
                df = self._import_csv(file_path, encoding, **kwargs)
            else:
                raise ValueError(f"不支援的檔案格式: {get_file_extension(file_path)}")
            
            import_time = time.time() - start_time
            
            self.logger.info(
                f"成功導入檔案: {Path(file_path).name}, "
                f"數據形狀: {df.shape}, 耗時: {import_time:.2f}秒"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(f"導入檔案失敗: {file_path}, 錯誤: {str(e)}", exc_info=True)
            raise
    
    def _import_excel(self, file_path: str, sheet_name: Optional[Union[str, int]] = None,
                     **kwargs) -> pd.DataFrame:
        """導入Excel檔案"""
        try:
            # 設置預設參數
            default_kwargs = {
                'engine': 'openpyxl',
                'dtype': str  # 預設為字符串類型，避免數據類型問題
            }
            
            if sheet_name is not None:
                default_kwargs['sheet_name'] = sheet_name
            
            # 合併用戶提供的參數
            default_kwargs.update(kwargs)
            
            return pd.read_excel(file_path, **default_kwargs)
            
        except Exception as e:
            # 嘗試使用xlrd引擎（用於舊版Excel檔案）
            try:
                default_kwargs['engine'] = 'xlrd'
                return pd.read_excel(file_path, **default_kwargs)
            except Exception:
                raise e
    
    def _import_csv(self, file_path: str, encoding: str = 'utf-8', **kwargs) -> pd.DataFrame:
        """導入CSV檔案"""
        try:
            # 設置預設參數
            default_kwargs = {
                'encoding': encoding,
                'dtype': str,  # 預設為字符串類型
                'keep_default_na': False,  # 不自動轉換為NaN
                'na_values': ['']  # 只將空字符串視為NaN
            }
            
            # 合併用戶提供的參數
            default_kwargs.update(kwargs)
            
            return pd.read_csv(file_path, **default_kwargs)
            
        except UnicodeDecodeError:
            # 嘗試其他編碼
            encodings = ['utf-8-sig', 'big5', 'gbk', 'iso-8859-1']
            for enc in encodings:
                try:
                    default_kwargs['encoding'] = enc
                    self.logger.warning(f"使用 {enc} 編碼重新嘗試導入")
                    return pd.read_csv(file_path, **default_kwargs)
                except UnicodeDecodeError:
                    continue
            
            raise ValueError(f"無法使用任何編碼格式讀取檔案: {file_path}")
    
    def import_multiple_files(self, file_paths: List[str], 
                             file_configs: Dict[str, Dict] = None) -> Dict[str, pd.DataFrame]:
        """
        導入多個檔案
        
        Args:
            file_paths: 檔案路徑列表
            file_configs: 每個檔案的特定配置
            
        Returns:
            Dict[str, pd.DataFrame]: 檔案名稱與DataFrame的對應字典
        """
        try:
            if not file_paths:
                return {}
            
            if file_configs is None:
                file_configs = {}
            
            results = {}
            
            self.logger.info(f"開始導入 {len(file_paths)} 個檔案")
            
            for file_path in file_paths:
                if not file_path:
                    continue
                
                file_name = Path(file_path).stem
                config = file_configs.get(file_name, {})
                
                try:
                    df = self.import_file(file_path, **config)
                    results[file_name] = df
                except Exception as e:
                    self.logger.error(f"導入檔案失敗: {file_name}, 錯誤: {str(e)}")
                    # 繼續處理其他檔案
                    continue
            
            self.logger.info(f"成功導入 {len(results)} 個檔案")
            return results
            
        except Exception as e:
            self.logger.error(f"批量導入檔案時出錯: {str(e)}", exc_info=True)
            return {}
    
    def concurrent_import_files(self, file_paths: List[str], 
                               file_configs: Dict[str, Dict] = None) -> Dict[str, pd.DataFrame]:
        """
        並發導入多個檔案
        
        Args:
            file_paths: 檔案路徑列表
            file_configs: 每個檔案的特定配置
            
        Returns:
            Dict[str, pd.DataFrame]: 檔案名稱與DataFrame的對應字典
        """
        try:
            if not file_paths:
                return {}
            
            if file_configs is None:
                file_configs = {}
            
            # 過濾有效的檔案路徑
            valid_files = [(path, Path(path).stem) for path in file_paths if path and validate_file_path(path)]
            
            if not valid_files:
                self.logger.warning("沒有有效的檔案路徑")
                return {}
            
            self.logger.info(f"開始並發導入 {len(valid_files)} 個檔案")
            
            results = {}
            
            # 使用線程池並發處理
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任務
                future_to_file = {}
                for file_path, file_name in valid_files:
                    config = file_configs.get(file_name, {})
                    future = executor.submit(self._import_single_file_safe, file_path, file_name, config)
                    future_to_file[future] = file_name
                
                # 收集結果
                for future in concurrent.futures.as_completed(future_to_file, timeout=self.timeout):
                    file_name = future_to_file[future]
                    try:
                        df = future.result()
                        if df is not None:
                            results[file_name] = df
                    except Exception as e:
                        self.logger.error(f"並發導入檔案失敗: {file_name}, 錯誤: {str(e)}")
            
            self.logger.info(f"並發導入完成，成功導入 {len(results)} 個檔案")
            return results
            
        except Exception as e:
            self.logger.error(f"並發導入檔案時出錯: {str(e)}", exc_info=True)
            return {}
    
    def _import_single_file_safe(self, file_path: str, file_name: str, config: Dict) -> Optional[pd.DataFrame]:
        """安全地導入單個檔案（用於並發處理）"""
        try:
            return self.import_file(file_path, **config)
        except Exception as e:
            self.logger.error(f"導入檔案失敗: {file_name}, 錯誤: {str(e)}")
            return None
    
    def extract_date_and_month_from_filename(self, filename: str) -> Tuple[Optional[int], Optional[int]]:
        """
        從檔案名稱提取日期和月份
        
        Args:
            filename: 檔案名稱
            
        Returns:
            Tuple[Optional[int], Optional[int]]: (日期YYYYMM, 月份)
        """
        try:
            import re
            
            # 嘗試匹配 YYYYMM 格式
            pattern = r'(\d{6})'
            match = re.search(pattern, filename)
            
            if match:
                date_str = match.group(1)
                year = int(date_str[:4])
                month = int(date_str[4:6])
                
                # 驗證日期有效性
                if 1 <= month <= 12 and 2000 <= year <= 2100:
                    date_int = int(date_str)
                    return date_int, month
            
            # 嘗試其他日期格式
            # 例如: 2024-01, 2024_01, 202401 等
            patterns = [
                r'(\d{4})[-_](\d{1,2})',
                r'(\d{4})(\d{2})'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, filename)
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    
                    if 1 <= month <= 12 and 2000 <= year <= 2100:
                        date_int = year * 100 + month
                        return date_int, month
            
            self.logger.warning(f"無法從檔案名稱提取日期: {filename}")
            return None, None
            
        except Exception as e:
            self.logger.error(f"提取日期時出錯: {str(e)}")
            return None, None
    
    def validate_dataframe(self, df: pd.DataFrame, required_columns: List[str] = None,
                          min_rows: int = 0) -> bool:
        """
        驗證DataFrame的有效性
        
        Args:
            df: 要驗證的DataFrame
            required_columns: 必要的列名列表
            min_rows: 最少行數
            
        Returns:
            bool: 是否有效
        """
        try:
            if df is None or df.empty:
                self.logger.warning("DataFrame為空")
                return False
            
            if len(df) < min_rows:
                self.logger.warning(f"DataFrame行數不足: {len(df)} < {min_rows}")
                return False
            
            if required_columns:
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    self.logger.warning(f"DataFrame缺少必要列: {missing_columns}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"驗證DataFrame時出錯: {str(e)}")
            return False
    
    def get_import_statistics(self, results: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        獲取導入統計信息
        
        Args:
            results: 導入結果字典
            
        Returns:
            Dict[str, Any]: 統計信息
        """
        try:
            if not results:
                return {'total_files': 0, 'total_rows': 0, 'total_columns': 0}
            
            total_files = len(results)
            total_rows = sum(len(df) for df in results.values())
            total_columns = sum(len(df.columns) for df in results.values())
            
            file_details = {}
            for name, df in results.items():
                file_details[name] = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
                }
            
            return {
                'total_files': total_files,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'file_details': file_details
            }
            
        except Exception as e:
            self.logger.error(f"獲取導入統計時出錯: {str(e)}")
            return {}
