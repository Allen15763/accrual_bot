"""
AsyncDataImporter - 異步數據導入器
提供與原始版本相同的並發導入功能，完全兼容原始SPX處理器的使用方式
"""

import os
import pandas as pd
import numpy as np
import concurrent.futures
import threading
import time
from typing import List, Dict, Tuple, Optional, Union, Any, Callable
from pathlib import Path

try:
    from .base_importer import BaseDataImporter
    from .excel_importer import ExcelImporter
    from .google_sheets_importer import GoogleSheetsImporter
    from ...utils import (
        get_logger, validate_file_path, is_excel_file, is_csv_file,
        CONCURRENT_SETTINGS, config_manager
    )
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from data.importers.base_importer import BaseDataImporter
    from data.importers.excel_importer import ExcelImporter
    from data.importers.google_sheets_importer import GoogleSheetsImporter
    from utils import (
        get_logger, validate_file_path, is_excel_file, is_csv_file,
        CONCURRENT_SETTINGS, config_manager
    )


class RetryableTask:
    """可重試的任務類"""
    
    def __init__(self, func: Callable, args: List, kwargs: Dict, 
                 max_retries: int = 3, retry_delay: float = 1.0,
                 retryable_exceptions: Tuple = (ConnectionError, TimeoutError)):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retryable_exceptions = retryable_exceptions
        self.logger = get_logger("RetryableTask")
        
    def execute(self):
        """執行任務，支持重試邏輯"""
        retries = 0
        last_exception = None
        
        while retries <= self.max_retries:
            try:
                return self.func(*self.args, **self.kwargs)
            except self.retryable_exceptions as e:
                # 可重試的異常
                retries += 1
                last_exception = e
                if retries <= self.max_retries:
                    # 指數退避策略
                    sleep_time = self.retry_delay * (2 ** (retries - 1))
                    self.logger.warning(f"任務失敗，將在 {sleep_time:.2f} 秒後重試 (第 {retries} 次): {str(e)}")
                    time.sleep(sleep_time)
                else:
                    # 達到最大重試次數
                    break
            except Exception as e:
                # 不可重試的異常立即失敗
                self.logger.error(f"任務發生不可重試的錯誤: {str(e)}", exc_info=True)
                raise
        
        # 重試失敗，拋出最後一個異常
        if last_exception:
            raise RuntimeError(f"重試 {self.max_retries} 次後任務仍失敗: {str(last_exception)}") from last_exception


class AsyncDataImporter(BaseDataImporter):
    """
    異步數據導入器
    完全兼容原始版本的使用方式，提供並發讀取功能
    """
    
    def __init__(self):
        """初始化異步數據導入器"""
        super().__init__()
        self.logger = get_logger(self.__class__.__name__)
        self.max_workers = CONCURRENT_SETTINGS.get('MAX_WORKERS', 5)
        self.timeout = CONCURRENT_SETTINGS.get('TIMEOUT', 300)
        self._lock = threading.Lock()
        
        # 初始化專用導入器
        self.excel_importer = ExcelImporter()
        self.sheets_importer = None  # 將在需要時初始化
        
        self.logger.info("初始化異步數據導入器")
    
    def set_max_workers(self, max_workers: int):
        """設置最大工作線程數"""
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.logger.warning(f"無效的工作線程數: {max_workers}，使用預設值: {self.max_workers}")
    
    def import_rawdata(self, url: str, name: str) -> Tuple[pd.DataFrame, int]:
        """導入原始PR數據 - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入原始數據文件: {name}")
            
            # 使用基礎導入器導入文件
            df = self.import_file(url)
            
            # 數據預處理
            df = self._preprocess_dataframe(df)
            
            # 從文件名獲取年月
            date_info, _ = self.extract_date_and_month_from_filename(name)
            if date_info is None:
                self.logger.warning(f"無法從文件名 {name} 獲取年月值，使用默認值0")
                date_info = 0
                
            self.logger.info(f"成功導入原始數據, 形狀: {df.shape}")
            return df, date_info
            
        except Exception as e:
            self.logger.error(f"導入原始數據文件 {name} 時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_rawdata_POonly(self, url: str, name: str) -> Tuple[pd.DataFrame, int, int]:
        """導入原始PO數據 - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入PO數據文件: {name}")
            
            # 使用基礎導入器導入文件
            df = self.import_file(url)
            
            # 數據預處理
            df = self._preprocess_dataframe(df, is_po=True)
            
            # 從文件名獲取年月和月份
            date_info, month_info = self.extract_date_and_month_from_filename(name)
            if date_info is None or month_info is None:
                self.logger.warning(f"無法從文件名 {name} 獲取年月值，使用默認值")
                date_info, month_info = 0, 0
                
            self.logger.info(f"成功導入PO數據, 形狀: {df.shape}")
            return df, date_info, month_info
            
        except Exception as e:
            self.logger.error(f"導入PO數據文件 {name} 時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_reference_data(self, entity_type: str = 'MOB') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """導入參考數據 - 兼容原始版本接口"""
        try:
            # 獲取參考數據路徑
            if entity_type.upper() == 'MOB':
                ref_path = config_manager.get('PATHS', 'ref_path_mob', '')
            elif entity_type.upper() == 'SPT':
                ref_path = config_manager.get('PATHS', 'ref_path_spt', '')
            elif entity_type.upper() == 'SPX':
                # SPX使用SPT的參考數據
                ref_path = config_manager.get('PATHS', 'ref_path_spt', '')
            else:
                raise ValueError(f"不支援的實體類型: {entity_type}")
            
            self.logger.info(f"正在導入 {entity_type} 參考數據: {ref_path}")
            
            ac_ref = self.import_file(ref_path)
            
            # 提取所需的列
            ref_for_ac = ac_ref.iloc[:, 1:3].copy()
            ref_for_liability = ac_ref.loc[:, ['Account', 'Liability']].copy()
            
            return ref_for_ac, ref_for_liability
            
        except Exception as e:
            self.logger.error(f"導入參考數據時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_procurement(self, url: str) -> pd.DataFrame:
        """導入採購底稿 - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入採購底稿: {url}")
            
            df = self.import_file(url)
            
            # 添加PR Line欄位
            if 'PR#' in df.columns and 'Line#' in df.columns:
                df['PR Line'] = df['PR#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿, 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入採購底稿時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_procurement_PO(self, url: str) -> pd.DataFrame:
        """導入採購底稿(PO) - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入採購底稿(PO): {url}")
            
            df = self.import_file(url)
            
            # 數據預處理
            if 'Line#' in df.columns:
                df['Line#'] = df['Line#'].astype(float).round(0).astype(int).astype(str)
            
            # 添加PO Line欄位
            if 'PO#' in df.columns and 'Line#' in df.columns:
                df['PO Line'] = df['PO#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿(PO), 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入採購底稿(PO)時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_previous_wp(self, url: str) -> pd.DataFrame:
        """導入前期底稿 - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入前期底稿: {url}")
            
            df = self.import_file(url)
            
            # 數據預處理
            if 'Line#' in df.columns:
                df['Line#'] = df['Line#'].astype(float).round(0).astype(int).astype(str)
            
            # 根據存在的欄位添加Line標識
            if 'PO#' in df.columns:
                df['PO Line'] = df['PO#'].astype(str) + "-" + df['Line#'].astype(str)
            elif 'PR#' in df.columns:
                df['PR Line'] = df['PR#'].astype(str) + "-" + df['Line#'].astype(str)
                
            self.logger.info(f"成功導入前期底稿, 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入前期底稿時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_closing_list(self, url: str) -> List[str]:
        """導入關單清單 - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入關單清單: {url}")
            
            df = self.import_file(url)
            
            # 獲取第一列作為關單清單
            mapping_list = df.iloc[:, 0].tolist()
            unique_list = list(set(mapping_list))
            
            self.logger.info(f"成功導入關單清單, 項目數: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"導入關單清單時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_spx_closing_list(self, config: Dict[str, Any]) -> pd.DataFrame:
        """導入SPX關單清單 - 兼容原始版本接口"""
        try:
            self.logger.info("正在導入關單清單(SPX)")
            
            # 初始化Google Sheets導入器
            if self.sheets_importer is None:
                self.sheets_importer = GoogleSheetsImporter(config)
            
            # 定義要查詢的工作表
            queries = [
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2023年_done', 'A:J'),
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2024年', 'A:J'),
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2025年', 'A:J')
            ]
            
            dfs = []
            for spreadsheet_id, sheet_name, range_value in queries:
                try:
                    df = self.sheets_importer.get_sheet_data(spreadsheet_id, sheet_name, range_value,
                                                             True, True)
                    if df is not None and not df.empty:
                        dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"讀取工作表 {sheet_name} 失敗: {str(e)}")
            
            if not dfs:
                self.logger.warning("未能獲取任何關單數據")
                return pd.DataFrame()
            
            # 合併所有DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # 數據清理和重命名
            combined_df.dropna(subset=['Date'], inplace=True)
            combined_df.rename(columns={
                'Date': 'date', 
                'Type': 'type', 
                'PO Number': 'po_no', 
                'Requester': 'requester', 
                'Supplier': 'supplier',
                'Line Number / ALL': 'line_no', 
                'Reason': 'reason', 
                'New PR Number': 'new_pr_no', 
                'Remark': 'remark', 
                'Done(V)': 'done_by_fn'
            }, inplace=True)
            
            combined_df = combined_df.query("date!=''").reset_index(drop=True)
            
            self.logger.info(f"成功導入關單清單(SPX), 形狀: {combined_df.shape}")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"導入SPX關單清單時出錯: {str(e)}", exc_info=True)
            raise
    
    def import_ap_invoice(self, url: str, cols: List[str] = None) -> pd.DataFrame:
        """導入AP invoice - 兼容原始版本接口"""
        try:
            self.logger.info(f"正在導入AP invoice: {url}")
            
            # 設置導入參數
            import_kwargs = {
                'sheet_name': 1,
                'header': 1
            }
            
            if cols:
                import_kwargs['usecols'] = cols
            
            df = self.import_file(url, **import_kwargs)
            
            self.logger.info(f"成功導入AP invoice, 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入AP invoice時出錯: {str(e)}", exc_info=True)
            raise
    
    def concurrent_import(self, 
                          import_tasks: List[Tuple[Callable, List, Dict]]) -> Tuple[List[Any], List[Tuple[int, str]]]:
        """並發執行多個導入任務 - 兼容原始版本接口"""
        try:
            self.logger.info(f"開始並發執行 {len(import_tasks)} 個導入任務")
            results = [None] * len(import_tasks)
            error_tasks = []
            
            # 特定函數名稱，用於類型檢查
            name_import_po_only = 'import_rawdata_POonly'
            name_import_raw = 'import_rawdata'
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(import_tasks), 
                                                                       self.max_workers)) as executor:
                # 提交所有任務
                futures = []
                for i, (import_func, args, kwargs) in enumerate(import_tasks):
                    task = RetryableTask(import_func, args, kwargs)
                    future = executor.submit(task.execute)
                    futures.append((i, future, import_func.__name__))
                
                # 處理所有任務結果
                for i, future, func_name in futures:
                    try:
                        with self._lock:
                            result = future.result()
                            
                            # 保留原有的特定函數返回類型處理邏輯
                            if func_name == name_import_po_only and isinstance(result, tuple) and len(result) == 3:
                                results[i] = result
                            elif func_name == name_import_raw and isinstance(result, tuple) and len(result) == 2:
                                results[i] = result
                            else:
                                results[i] = result
                                
                            self.logger.debug(f"任務 {i} ({func_name}) 執行成功")
                            
                    except Exception as e:
                        with self._lock:
                            self.logger.error(f"任務 {i} ({func_name}) 執行出錯: {str(e)}", exc_info=True)
                            results[i] = None
                            error_tasks.append((i, str(e)))
            
            succeeded = sum(1 for v in results if v is not None)
            self.logger.info(f"並發執行完成，成功執行 {succeeded}/{len(import_tasks)} 個任務")
            return results, error_tasks
            
        except Exception as e:
            self.logger.error(f"並發執行導入任務時出錯: {str(e)}", exc_info=True)
            raise
    
    def concurrent_read_files(self, file_types: List[str], file_paths: List[str], **kwargs) -> Dict[str, Any]:
        """
        並發讀取多個文件 - 完全兼容原始版本接口
        
        Args:
            file_types: 文件類型列表
            file_paths: 文件路徑列表
            **kwargs: 其他參數
            
        Returns:
            Dict[str, Any]: 讀取結果字典
        """
        try:
            self.logger.info(f"開始並發讀取 {len(file_paths)} 個文件")
            
            if len(file_types) != len(file_paths):
                raise ValueError("文件類型列表和文件路徑列表長度不一致")
            
            # 根據文件類型選擇導入方法
            import_tasks = []
            for i, (file_type, file_path) in enumerate(zip(file_types, file_paths)):
                file_name = kwargs.get('file_names', {}).get(file_type, os.path.basename(file_path))
                
                if file_type == 'raw':
                    import_tasks.append((self.import_rawdata, [file_path, file_name], {}))
                elif file_type == 'raw_po':
                    import_tasks.append((self.import_rawdata_POonly, [file_path, file_name], {}))
                elif file_type == 'closing':
                    import_tasks.append((self.import_closing_list, [file_path], {}))
                elif file_type == 'closing_po':
                    import_tasks.append((self.import_closing_list, [file_path], {}))
                elif file_type == 'previous':
                    import_tasks.append((self.import_previous_wp, [file_path], {}))
                elif file_type == 'previous_pr':
                    import_tasks.append((self.import_previous_wp, [file_path], {}))
                elif file_type == 'procurement_pr':
                    import_tasks.append((self.import_procurement, [file_path], {}))
                elif file_type == 'procurement_po':
                    import_tasks.append((self.import_procurement_PO, [file_path], {}))
                elif file_type == 'spx_closing':
                    import_tasks.append((self.import_spx_closing_list, [kwargs.get('config', {})], {}))
                elif file_type == 'ap_invoice':
                    import_tasks.append((self.import_ap_invoice, [file_path, kwargs.get('ap_columns', [])], {}))
                else:
                    self.logger.warning(f"未知的文件類型: {file_type}")
                    continue
            
            # 並發執行導入任務
            results, error_tasks = self.concurrent_import(import_tasks)

            # 提供詳細錯誤報告
            if error_tasks:
                error_report = "\n".join([f"- 文件類型 '{file_types[task_id]}': {error}" 
                                         for task_id, error in error_tasks])
                self.logger.warning(f"部分文件讀取失敗:\n{error_report}")
            
            # 構建結果字典
            result_dict = {}
            for i, file_type in enumerate(file_types):
                if i < len(results):
                    result_dict[file_type] = results[i]
            
            return result_dict
            
        except Exception as e:
            self.logger.error(f"並發讀取文件時出錯: {str(e)}", exc_info=True)
            raise
    
    def _preprocess_dataframe(self, df: pd.DataFrame, is_po: bool = False) -> pd.DataFrame:
        """預處理DataFrame - 兼容原始版本的處理邏輯"""
        try:
            df_copy = df.copy()
            
            # 基本數據處理
            if 'Line#' in df_copy.columns:
                df_copy['Line#'] = df_copy['Line#'].astype(float).round(0).astype(int).astype(str)
            
            if 'GL#' in df_copy.columns:
                df_copy['GL#'] = np.where(df_copy['GL#'] == 'N.A.', '666666', df_copy['GL#'])
                if is_po:
                    df_copy['GL#'] = df_copy['GL#'].fillna('666666').astype(float).round(0).astype(int).astype(str)
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"預處理DataFrame時出錯: {str(e)}", exc_info=True)
            return df
    
    def batch_import_reference_data(self, entity_types: List[str]) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        """並發導入多個實體的參考數據"""
        try:
            self.logger.info(f"開始並發導入 {len(entity_types)} 個實體的參考數據")
            
            import_tasks = []
            for entity_type in entity_types:
                import_tasks.append((self.import_reference_data, [entity_type], {}))
            
            results, error_tasks = self.concurrent_import(import_tasks)
            
            result_dict = {}
            for i, entity_type in enumerate(entity_types):
                if i < len(results) and results[i] is not None:
                    result_dict[entity_type] = results[i]
            
            return result_dict
            
        except Exception as e:
            self.logger.error(f"並發導入參考數據時出錯: {str(e)}", exc_info=True)
            raise


def create_async_data_importer() -> AsyncDataImporter:
    """創建AsyncDataImporter實例的便捷函數"""
    return AsyncDataImporter()