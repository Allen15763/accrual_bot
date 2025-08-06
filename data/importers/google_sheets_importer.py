"""
Google Sheets 數據導入器
提供Google Sheets的讀取和並發處理功能
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional, Union, Any
import concurrent.futures
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    from .base_importer import BaseDataImporter
    from ...utils import get_logger, GOOGLE_SHEETS
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from data.importers.base_importer import BaseDataImporter
    from utils import get_logger, GOOGLE_SHEETS


class GoogleSheetsImporter(BaseDataImporter):
    """Google Sheets 數據導入器"""
    
    def __init__(self, credentials_config: Dict[str, Any]):
        """
        初始化Google Sheets導入器
        
        Args:
            credentials_config: 憑證配置，包含certificate_path和scopes
        """
        super().__init__()
        self.logger = get_logger(self.__class__.__name__)
        
        self.credentials_config = credentials_config
        self.service = None
        
        # 初始化Google Sheets服務
        self._initialize_service()
        
        self.logger.info("初始化Google Sheets導入器完成")
    
    def _initialize_service(self) -> None:
        """初始化Google Sheets API服務"""
        try:
            certificate_path = self.credentials_config.get('certificate_path')
            scopes = self.credentials_config.get('scopes', GOOGLE_SHEETS['DEFAULT_SCOPES'])
            
            if not certificate_path:
                raise ValueError("未提供憑證檔案路徑")
            
            # 載入服務帳戶憑證
            credentials = service_account.Credentials.from_service_account_file(
                certificate_path, scopes=scopes
            )
            
            # 建立Google Sheets API服務
            self.service = build('sheets', 'v4', credentials=credentials)
            
            self.logger.info("Google Sheets API服務初始化成功")
            
        except Exception as e:
            self.logger.error(f"初始化Google Sheets API服務失敗: {str(e)}", exc_info=True)
            raise
    
    def get_sheet_data(self, spreadsheet_id: str, sheet_name: str, 
                       cell_range: str = None, header_row: bool = True) -> pd.DataFrame:
        """
        從Google Sheets獲取數據
        
        Args:
            spreadsheet_id: 試算表ID
            sheet_name: 工作表名稱
            cell_range: 儲存格範圍（例如'A:J'）
            header_row: 是否包含標題行
            
        Returns:
            pd.DataFrame: 獲取的數據
        """
        try:
            if not self.service:
                raise ValueError("Google Sheets API服務未初始化")
            
            # 構建範圍字符串
            if cell_range:
                range_str = f"{sheet_name}!{cell_range}"
            else:
                range_str = sheet_name
            
            self.logger.info(f"正在讀取Google Sheets: {spreadsheet_id}, 範圍: {range_str}")
            
            # 呼叫Google Sheets API
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_str
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                self.logger.warning("Google Sheets返回空數據")
                return pd.DataFrame()
            
            # 轉換為DataFrame
            if header_row and len(values) > 1:
                df = pd.DataFrame(values[1:], columns=values[0])
            else:
                df = pd.DataFrame(values)
            
            self.logger.info(f"成功讀取Google Sheets數據，形狀: {df.shape}")
            return df
            
        except HttpError as e:
            self.logger.error(f"Google Sheets API錯誤: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"讀取Google Sheets時出錯: {str(e)}", exc_info=True)
            raise
    
    def get_multiple_sheets_data(self, queries: List[Tuple[str, str, str, bool]]) -> List[pd.DataFrame]:
        """
        獲取多個工作表的數據
        
        Args:
            queries: 查詢列表，每個元素為(spreadsheet_id, sheet_name, cell_range, header_row)
            
        Returns:
            List[pd.DataFrame]: 數據列表
        """
        try:
            if not queries:
                return []
            
            results = []
            
            self.logger.info(f"開始讀取 {len(queries)} 個Google Sheets")
            
            for spreadsheet_id, sheet_name, cell_range, header_row in queries:
                try:
                    df = self.get_sheet_data(spreadsheet_id, sheet_name, cell_range, header_row)
                    results.append(df)
                except Exception as e:
                    self.logger.error(f"讀取工作表失敗: {sheet_name}, 錯誤: {str(e)}")
                    # 添加空DataFrame以保持順序
                    results.append(pd.DataFrame())
            
            self.logger.info(f"完成讀取，成功獲取 {len([df for df in results if not df.empty])} 個工作表數據")
            return results
            
        except Exception as e:
            self.logger.error(f"批量讀取Google Sheets時出錯: {str(e)}", exc_info=True)
            return []
    
    def concurrent_get_data(self, queries: List[Tuple[str, str, str, bool]]) -> List[pd.DataFrame]:
        """
        並發獲取多個工作表的數據
        
        Args:
            queries: 查詢列表，每個元素為(spreadsheet_id, sheet_name, cell_range, header_row)
            
        Returns:
            List[pd.DataFrame]: 數據列表
        """
        try:
            if not queries:
                return []
            
            self.logger.info(f"開始並發讀取 {len(queries)} 個Google Sheets")
            
            results = [None] * len(queries)  # 保持順序
            
            # 使用線程池並發處理
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任務
                future_to_index = {}
                for i, query in enumerate(queries):
                    future = executor.submit(self._get_sheet_data_safe, *query)
                    future_to_index[future] = i
                
                # 收集結果
                for future in concurrent.futures.as_completed(future_to_index, timeout=self.timeout):
                    index = future_to_index[future]
                    try:
                        df = future.result()
                        results[index] = df if df is not None else pd.DataFrame()
                    except Exception as e:
                        self.logger.error(f"並發讀取工作表失敗: 索引{index}, 錯誤: {str(e)}")
                        results[index] = pd.DataFrame()
            
            # 過濾掉None值
            results = [df if df is not None else pd.DataFrame() for df in results]
            
            successful_count = len([df for df in results if not df.empty])
            self.logger.info(f"並發讀取完成，成功獲取 {successful_count} 個工作表數據")
            
            return results
            
        except Exception as e:
            self.logger.error(f"並發讀取Google Sheets時出錯: {str(e)}", exc_info=True)
            return []
    
    def _get_sheet_data_safe(self, spreadsheet_id: str, sheet_name: str, 
                             cell_range: str, header_row: bool) -> Optional[pd.DataFrame]:
        """安全地獲取工作表數據（用於並發處理）"""
        try:
            return self.get_sheet_data(spreadsheet_id, sheet_name, cell_range, header_row)
        except Exception as e:
            self.logger.error(f"讀取工作表失敗: {sheet_name}, 錯誤: {str(e)}")
            return None
    
    def get_spreadsheet_info(self, spreadsheet_id: str) -> Dict[str, Any]:
        """
        獲取試算表的基本信息
        
        Args:
            spreadsheet_id: 試算表ID
            
        Returns:
            Dict[str, Any]: 試算表信息
        """
        try:
            if not self.service:
                raise ValueError("Google Sheets API服務未初始化")
            
            # 獲取試算表元數據
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            info = {
                'title': spreadsheet.get('properties', {}).get('title', ''),
                'sheets': []
            }
            
            # 獲取工作表信息
            for sheet in spreadsheet.get('sheets', []):
                sheet_properties = sheet.get('properties', {})
                sheet_info = {
                    'title': sheet_properties.get('title', ''),
                    'sheet_id': sheet_properties.get('sheetId', 0),
                    'row_count': sheet_properties.get('gridProperties', {}).get('rowCount', 0),
                    'column_count': sheet_properties.get('gridProperties', {}).get('columnCount', 0)
                }
                info['sheets'].append(sheet_info)
            
            return info
            
        except Exception as e:
            self.logger.error(f"獲取試算表信息時出錯: {str(e)}", exc_info=True)
            return {}
    
    def import_spx_closing_list(self) -> pd.DataFrame:
        """
        導入SPX關單清單（特殊用途）
        
        Returns:
            pd.DataFrame: 關單清單數據
        """
        try:
            from ...utils import SPX_CONSTANTS
            
            spreadsheet_id = SPX_CONSTANTS['CLOSING_SHEET_ID']
            sheet_names = SPX_CONSTANTS['CLOSING_SHEETS']
            cell_range = SPX_CONSTANTS['CLOSING_RANGE']
            
            # 準備查詢
            queries = [
                (spreadsheet_id, sheet_name, cell_range, True)
                for sheet_name in sheet_names
            ]
            
            # 並發獲取數據
            dfs = self.concurrent_get_data(queries)
            
            # 合併結果
            valid_dfs = [df for df in dfs if not df.empty]
            
            if not valid_dfs:
                self.logger.warning("未獲取到任何SPX關單數據")
                return pd.DataFrame()
            
            combined_df = pd.concat(valid_dfs, ignore_index=True)
            
            # 處理數據
            if not combined_df.empty:
                # 移除空白日期的記錄
                combined_df = combined_df.dropna(subset=['Date'])
                
                # 重命名欄位
                column_mapping = {
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
                }
                
                combined_df = combined_df.rename(columns=column_mapping)
                
                # 過濾空白日期
                combined_df = combined_df.query("date != ''").reset_index(drop=True)
            
            self.logger.info(f"成功導入SPX關單清單，共 {len(combined_df)} 筆記錄")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"導入SPX關單清單時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()


class AsyncGoogleSheetsImporter(GoogleSheetsImporter):
    """非同步Google Sheets導入器（保持與原始程式碼相容）"""
    
    def __init__(self, credentials_config: Dict[str, Any]):
        """
        初始化非同步Google Sheets導入器
        
        Args:
            credentials_config: 憑證配置
        """
        super().__init__(credentials_config)
        self.logger = get_logger(f"{self.__class__.__name__}")
        
        self.logger.info("初始化非同步Google Sheets導入器完成")
    
    def concurrent_get_data(self, queries: List[Tuple[str, str, str, bool]]) -> List[pd.DataFrame]:
        """
        並發獲取數據（重新命名以保持相容性）
        
        Args:
            queries: 查詢列表
            
        Returns:
            List[pd.DataFrame]: 數據列表
        """
        return super().concurrent_get_data(queries)
