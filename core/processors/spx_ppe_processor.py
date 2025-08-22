import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime
from pathlib import Path

try:
    from .base_processor import BaseDataProcessor
    from ...utils import (get_logger)
    from ...data.importers.google_sheets_importer import GoogleSheetsImporter
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys

    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.base_processor import BaseDataProcessor
    from utils import (
        get_logger, safe_string_operation, create_mapping_dict, apply_mapping_safely,
        STATUS_VALUES, format_numeric_columns
    )
    from data.importers.google_sheets_importer import GoogleSheetsImporter


class SpxPpeProcessor(BaseDataProcessor):
    """PPE處理器基類，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "SPX"):
        """
        初始化PPE處理器
        
        Args:
            entity_type: 實體類型，'SPX'
        """
        super().__init__(entity_type)
        self.logger = get_logger(f"{self.__class__.__name__}_{entity_type}")
        
        self.logger.info(f"初始化 {entity_type} PPE處理器")

    def process(self, *args, **kwargs):
        return self._get_depreciation_period(*args, **kwargs)

    def _get_depreciation_period(self, *args, **kwargs):
        self.logger.info("開始擷取續約資料表")
        
        if kwargs['contract_filing_list_url']:
            df_filing_raw = self.read_excel_file(
                kwargs['contract_filing_list_url'], 
                sheet_name=self.config_manager._config_data.get(self.entity_type).get('contract_filing_list_sheet'),
                usecols=self.config_manager._config_data.get(self.entity_type).get('contract_filing_list_range')
            )

        # 2. 讀取續約清單
        df_renewal_raw = self._get_consolidated_contract_data()

        # 3. 清理資料
        df_filing_clean = self.clean_dataframe(df_filing_raw)
        df_renewal_clean = self.clean_dataframe(df_renewal_raw)

        # 4. 標準化資料
        df_filing_std = self.standardize_filing_data(df_filing_clean)
        df_renewal_std = self.standardize_renewal_data(df_renewal_clean)

        # 5. 合併資料
        df_merge = self.merge_contract_data(
            df_filing_std, 
            df_renewal_std, 
            merge_on=self.config_manager.get_list(self.entity_type, 'key_for_merging_origin_and_renew_contract')
        ).drop_duplicates().reset_index(drop=True)

        updated_df = self.update_contract_dates(df_merge).drop_duplicates()

        selected_cols = ['sp_code', 'address', 'contract_start_day_filing', 'contract_end_day_renewal']
        result_df = self.calculate_month_difference(updated_df.loc[:, selected_cols], 
                                                    'contract_end_day_renewal', 
                                                    kwargs['current_month'],
                                                    'months_diff')
        return result_df

    def _get_consolidated_contract_data(self, spreadsheet_id: str = None, 
                                        credentials_path: str = None) -> pd.DataFrame:
        """
        從Google Sheets獲取並整合合約數據
        
        Args:
            spreadsheet_id (str): Google Sheets的ID
            credentials_path (str): Google服務帳號憑證檔案路徑
        
        Returns:
            pandas.DataFrame: 整合後的合約數據
        """
        spreadsheet_id = self.config_manager.get(self.entity_type, 'expanded_contract_wb')
        
        # 創建GoogleSheetsImporter實例
        sheets_importer = GoogleSheetsImporter(self.config_manager.get_credentials_config())
        
        try:
            # 獲取第三期續約案件數據
            df_thi = sheets_importer.get_sheet_data(spreadsheet_id, 
                                                    self.config_manager.get(self.entity_type, 
                                                                            'expanded_contract_sheet3'), 
                                                    self.config_manager.get(
                                                        self.entity_type, 
                                                        'expanded_contract_range'))
            df_thi = df_thi.iloc[:, [0, 2, *range(13, 17)]]
            
            # 獲取第二期續約案件數據
            df_sec = sheets_importer.get_sheet_data(spreadsheet_id, 
                                                    self.config_manager.get(self.entity_type, 
                                                                            'expanded_contract_sheet2'), 
                                                    self.config_manager.get(self.entity_type, 
                                                                            'expanded_contract_range'))
            df_sec = df_sec.iloc[:, [0, 2, *range(13, 17)]]
            
            # 統一列名（以第二期為標準）
            std_cols = df_sec.columns
            df_thi.columns = std_cols
            
            # 獲取第一期合約數據
            df_ft = sheets_importer.get_sheet_data(spreadsheet_id, 
                                                   self.config_manager.get(self.entity_type, 
                                                                           'expanded_contract_sheet1'), 
                                                   self.config_manager.get(self.entity_type, 
                                                                           'expanded_contract_range1'))
            df_ft = df_ft.iloc[:2576, [0, 6, 21, 24]].query(
                "起租日!='' and 起租日!='#N/A' and ~起租日.isna()"
            ).assign(
                tempA=df_ft.iloc[:, 21],
                tempB=df_ft.iloc[:, 24]
            )
            df_ft.columns = std_cols
            
            # 合併所有數據
            df_expanded_contract_ft2thi = pd.concat([df_ft, df_sec, df_thi], ignore_index=True)
            self.logger.info(f"完成{self.entity_type}續約資料表取得")
            
            return df_expanded_contract_ft2thi
            
        except Exception as e:
            self.logger.error(f"獲取數據時發生錯誤: {e}")
            raise pd.DataFrame()
        
    def validate_file_path(self, file_path: Union[str, Path]) -> bool:
        """
        驗證檔案路徑是否存在
        
        Args:
            file_path: 檔案路徑
            
        Returns:
            檔案是否存在
        """
        path = Path(file_path)
        return path.exists() and path.is_file()

    def parse_contract_period(self, period_str: str) -> Tuple[Optional[str], Optional[str]]:
        """
        解析合約期間字串
        
        Args:
            period_str: 合約期間字串，格式如 "2023-01-01 - 2024-12-31"
            
        Returns:
            Tuple[開始日期, 結束日期] 或 (None, None) 如果解析失敗
        """
        try:
            if pd.isna(period_str):
                return None, None
                
            # 移除加號並分割日期
            period_clean = str(period_str).replace('+', '').strip()
            
            # 支援多種分隔符號
            for separator in [' - ', '-', '~', '至']:
                if separator in period_clean:
                    dates = period_clean.split(separator)
                    break
            else:
                self.logger.warning(f"無法識別合約期間分隔符號: {period_str}")
                return None, None
            
            if len(dates) < 2:
                self.logger.warning(f"合約期間格式異常: {period_str}")
                return None, None
            
            start_date = pd.to_datetime(dates[0].strip()).strftime('%Y-%m-%d')
            end_date = pd.to_datetime(dates[-1].strip()).strftime('%Y-%m-%d')
            
            return start_date, end_date
            
        except Exception as e:
            self.logger.error(f"解析合約期間失敗: {period_str}, 錯誤: {e}")
            return None, None

    def read_excel_file(self, file_path: Union[str, Path], 
                        sheet_name: Union[str, int] = 0,
                        usecols: Union[str, List[str]] = None,
                        **kwargs) -> Optional[pd.DataFrame]:
        """
        讀取Excel檔案
        
        Args:
            file_path: 檔案路徑
            sheet_name: 工作表名稱
            usecols: 要讀取的欄位
            **kwargs: 其他pandas.read_excel參數
            
        Returns:
            DataFrame或None（如果失敗）
        """
        
        try:
            if not self.validate_file_path(file_path):
                self.logger.error(f"檔案不存在: {file_path}")
                return None
            
            self.logger.info(f"開始讀取檔案: {file_path}")
            
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                usecols=usecols,
                **kwargs
            )
            
            self.logger.info(f"檔案讀取完成，共 {len(df)} 筆記錄")
            return df
            
        except Exception as e:
            self.logger.error(f"讀取檔案失敗: {file_path}, 錯誤: {e}")
            return None

    def clean_dataframe(self, df: pd.DataFrame, 
                        drop_na: bool = True,
                        drop_na_subset: List[str] = None) -> pd.DataFrame:
        """
        清理DataFrame
        
        Args:
            df: 要清理的DataFrame
            drop_na: 是否移除包含空值的行
            drop_na_subset: 指定檢查空值的欄位
            
        Returns:
            清理後的DataFrame
        """
        df_clean = df.copy()
        
        if drop_na:
            if drop_na_subset:
                df_clean = df_clean.dropna(subset=drop_na_subset)
            else:
                df_clean = df_clean.dropna()
        
        return df_clean

    def standardize_filing_data(self, df: pd.DataFrame, 
                                column_mapping: Dict[str, str] = None) -> Optional[pd.DataFrame]:
        """
        標準化歸檔清單資料
        
        Args:
            df: 原始DataFrame
            column_mapping: 欄位映射字典 {原欄位名: 新欄位名}

        Returns:
            標準化後的DataFrame
        """
        try:
            # 預設欄位映射（假設前三個欄位分別是店號、地址、合約期間）
            if column_mapping is None:
                columns = df.columns.tolist()
                if len(columns) < 3:
                    self.logger.error(f"DataFrame欄位數不足，需要至少3個欄位，目前有 {len(columns)} 個")
                    return None
                column_mapping = {
                    columns[0]: '店號',
                    columns[1]: '物件地址', 
                    columns[2]: '合約期間'
                }
            
            df_std = pd.DataFrame()
            
            # 處理店號
            store_col = next((col for col in df.columns if any(key in str(col) for key in ['店號', 'store', 'sp'])), 
                             df.columns[0])
            df_std['sp_code'] = df[store_col].astype(int)
            
            # 處理地址
            addr_col = next((col for col in df.columns if any(key in str(col) for key in ['地址', 'address', '物件'])), 
                            df.columns[1])
            df_std['address'] = df[addr_col].astype(str)
            
            # 處理合約期間
            period_col = next((col for col in df.columns if any(key in str(col) for key in ['期間', 'period', '合約'])), 
                              df.columns[2])
            contract_periods = df[period_col].apply(lambda x: self.parse_contract_period(x))
            df_std['contract_start_day'] = [period[0] for period in contract_periods]
            df_std['contract_end_day'] = [period[1] for period in contract_periods]
            
            # 移除無效記錄
            df_std = df_std.dropna(subset=['sp_code', 'address', 'contract_start_day', 'contract_end_day'])
            
            self.logger.info(f"歸檔資料標準化完成，共 {len(df_std)} 筆有效記錄")
            return df_std
            
        except Exception as e:
            self.logger.error(f"標準化歸檔資料失敗: {e}")
            return None

    def standardize_renewal_data(self, df: pd.DataFrame,
                                 column_mapping: Dict[str, str] = None) -> Optional[pd.DataFrame]:
        """
        標準化續約清單資料
        
        Args:
            df: 原始DataFrame
            column_mapping: 欄位映射字典
            
        Returns:
            標準化後的DataFrame
        """
        try:
            # 預設欄位映射
            if column_mapping is None:
                column_mapping = {
                    '店號': 'sp_code',
                    '詳細地址': 'address',
                    '第一期合約起始日': 'contract_start_day',
                    '第二期合約截止日': 'contract_end_day'
                }
            
            df_std = pd.DataFrame()
            
            # 動態匹配欄位
            for source_col, target_col in column_mapping.items():
                matched_col = next((col for col in df.columns if source_col in str(col)), None)
                if matched_col is None:
                    self.logger.warning(f"找不到欄位: {source_col}")
                    continue
                    
                if target_col == 'sp_code':
                    df_std[target_col] = df[matched_col].astype(int)
                elif target_col == 'address':
                    df_std[target_col] = df[matched_col].astype(str)
                elif 'day' in target_col:
                    df_std[target_col] = pd.to_datetime(
                        df[matched_col].astype(str).str.strip()
                    ).dt.strftime('%Y-%m-%d')
                else:
                    df_std[target_col] = df[matched_col]
            
            # 移除無效記錄
            required_cols = ['sp_code', 'address']
            df_std = df_std.dropna(subset=required_cols)
            
            self.logger.info(f"續約資料標準化完成，共 {len(df_std)} 筆有效記錄")
            return df_std
            
        except Exception as e:
            self.logger.error(f"標準化續約資料失敗: {e}")
            return None

    def merge_contract_data(self, df_filing: pd.DataFrame,
                            df_renewal: pd.DataFrame,
                            merge_on: Union[str, List[str]] = 'address',
                            how: str = 'outer',
                            suffixes: Tuple[str, str] = ('_filing', '_renewal')) -> Optional[pd.DataFrame]:
        """
        合併合約資料
        
        Args:
            df_filing: 歸檔清單DataFrame
            df_renewal: 續約清單DataFrame
            merge_on: 合併鍵值
            how: 合併方式 ('left', 'right', 'outer', 'inner')
            suffixes: 重複欄位後綴
        Returns:
            合併後的DataFrame
        """
        try:
            self.logger.info(f"開始合併資料，基於欄位: {merge_on}")
            
            # 檢查合併鍵值是否存在
            merge_keys = [merge_on] if isinstance(merge_on, str) else merge_on
            for key in merge_keys:
                if key not in df_filing.columns:
                    self.logger.error(f"歸檔清單中缺少合併鍵值: {key}")
                    return None
                if key not in df_renewal.columns:
                    self.logger.error(f"續約清單中缺少合併鍵值: {key}")
                    return None
            
            df_merged = pd.merge(
                df_filing,
                df_renewal,
                on=merge_on,
                how=how,
                suffixes=suffixes
            )
            
            self.logger.info(f"資料合併完成，共 {len(df_merged)} 筆記錄")
            return df_merged
            
        except Exception as e:
            self.logger.error(f"合併資料失敗: {e}")
            return None

    def process_rental_contracts(self, filing_file: Union[str, Path],
                                 renewal_file: Union[str, Path],
                                 filing_sheet: str = '歸檔清單',
                                 filing_usecols: Union[str, List[str]] = 'D,H,P',
                                 merge_on: str = 'address',
                                 **kwargs) -> Optional[pd.DataFrame]:
        """
        完整的租金合約處理流程
        
        Args:
            filing_file: 歸檔清單檔案路徑
            renewal_file: 續約清單檔案路徑
            filing_sheet: 歸檔清單工作表名稱
            filing_usecols: 歸檔清單要讀取的欄位
            merge_on: 合併鍵值
            **kwargs: 其他參數
            
        Returns:
            處理結果DataFrame
        """
        try:
            # 1. 讀取歸檔清單
            df_filing_raw = self.read_excel_file(
                filing_file, 
                sheet_name=filing_sheet,
                usecols=filing_usecols
            )
            if df_filing_raw is None:
                return None
            
            # 2. 讀取續約清單
            df_renewal_raw = self.read_excel_file(renewal_file)
            if df_renewal_raw is None:
                return None
            
            # 3. 清理資料
            df_filing_clean = self.clean_dataframe(df_filing_raw)
            df_renewal_clean = self.clean_dataframe(df_renewal_raw)
            
            # 4. 標準化資料
            df_filing_std = self.standardize_filing_data(df_filing_clean)
            if df_filing_std is None:
                return None
                
            df_renewal_std = self.standardize_renewal_data(df_renewal_clean)
            if df_renewal_std is None:
                return None
            
            # 5. 合併資料
            df_result = self.merge_contract_data(
                df_filing_std, 
                df_renewal_std, 
                merge_on=merge_on
            )
            if df_result is None:
                return None
            
            self.logger.info("租金合約處理流程完成")
            return df_result
            
        except Exception as e:
            self.logger.error(f"處理流程失敗: {e}")
            return None

    def update_contract_dates(sefl, df):
        """
        更新合約日期，使同一個sp_code的所有記錄保持一致

        按sp_code分組處理每一組資料
            - 收集該組內所有的起始日期（filing + renewal）
            - 收集該組內所有的終止日期（filing + renewal）
            - 將統一後的最小起始日期和最大終止日期映射回該組的所有記錄
        
        Args:
            df (pandas.DataFrame): 包含合約資料的DataFrame
            
        Returns:
            pandas.DataFrame: 更新後的DataFrame
        """
        
        # 複製DataFrame避免修改原始資料
        df_updated = df.copy()
        
        # 將日期欄位轉換為datetime格式（如果還不是的話）
        date_columns = [
            'contract_start_day_filing', 
            'contract_end_day_filing',
            'contract_start_day_renewal', 
            'contract_end_day_renewal'
        ]
        
        for col in date_columns:
            if col in df_updated.columns:
                df_updated[col] = pd.to_datetime(df_updated[col], errors='coerce')
        
        # 按sp_code分組處理
        for sp_code in df_updated['sp_code'].unique():
            # 篩選出該sp_code的所有記錄
            mask = df_updated['sp_code'] == sp_code
            sp_data = df_updated[mask]
            
            # 收集所有起始日期和終止日期
            start_dates = []
            end_dates = []
            
            # 從filing欄位收集日期
            if 'contract_start_day_filing' in df_updated.columns:
                filing_starts = sp_data['contract_start_day_filing'].dropna()
                start_dates.extend(filing_starts.tolist())
                
            if 'contract_end_day_filing' in df_updated.columns:
                filing_ends = sp_data['contract_end_day_filing'].dropna()
                end_dates.extend(filing_ends.tolist())
            
            # 從renewal欄位收集日期
            if 'contract_start_day_renewal' in df_updated.columns:
                renewal_starts = sp_data['contract_start_day_renewal'].dropna()
                start_dates.extend(renewal_starts.tolist())
                
            if 'contract_end_day_renewal' in df_updated.columns:
                renewal_ends = sp_data['contract_end_day_renewal'].dropna()
                end_dates.extend(renewal_ends.tolist())
            
            # 找出最小起始日期和最大終止日期
            if start_dates:
                min_start_date = min(start_dates)
                # 更新所有該sp_code記錄的起始日期欄位
                if 'contract_start_day_filing' in df_updated.columns:
                    df_updated.loc[mask, 'contract_start_day_filing'] = min_start_date
                if 'contract_start_day_renewal' in df_updated.columns:
                    df_updated.loc[mask, 'contract_start_day_renewal'] = min_start_date
            
            if end_dates:
                max_end_date = max(end_dates)
                # 更新所有該sp_code記錄的終止日期欄位
                if 'contract_end_day_filing' in df_updated.columns:
                    df_updated.loc[mask, 'contract_end_day_filing'] = max_end_date
                if 'contract_end_day_renewal' in df_updated.columns:
                    df_updated.loc[mask, 'contract_end_day_renewal'] = max_end_date
        
        return df_updated

    def calculate_month_difference(self, df, date_column: str, target_ym: int, new_column_name='month_diff'):
        """
        計算DataFrame中日期欄位與目標年月的月份差異(result series + 1)
        
        Args:
            df (pandas.DataFrame): 輸入的DataFrame
            date_column (str): 日期欄位的名稱
            target_ym (int): 目標年月，格式為YYYYMM (例如: 202506)
            new_column_name (str): 新增欄位的名稱，預設為'month_diff'
            
        Returns:
            pandas.DataFrame: 包含新增月份差異欄位的DataFrame
        """
        
        # 複製DataFrame避免修改原始資料
        df_result = df.copy()
        
        # 確保日期欄位是datetime格式
        df_result[date_column] = pd.to_datetime(df_result[date_column])
        
        # 將target_ym轉換為年和月
        target_year = target_ym // 100
        target_month = target_ym % 100
        
        # 建立目標日期 (設定為該月的第一天)
        target_date = datetime(target_year, target_month, 1)
        
        # 計算月份差異的函數
        def months_difference(date1, date2):
            """
            計算兩個日期之間的月份差異
            date1 - date2 的月份數
            """
            return (date1.year - date2.year) * 12 + (date1.month - date2.month)
        
        # 計算每一行的月份差異
        df_result[new_column_name] = df_result[date_column].apply(
            lambda x: months_difference(x, target_date)
        ).add(1)
        
        return df_result
