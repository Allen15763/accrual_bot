"""
SPX PPE處理器
添加結構化設計但避免過度工程
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field

try:
    from .base_processor import BaseDataProcessor
    from ...utils import get_logger
    from ...data.importers.google_sheets_importer import GoogleSheetsImporter
    from ...core.models.data_models import ProcessingResult, ValidationResult, ValidationStatus
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.base_processor import BaseDataProcessor
    from utils import get_logger
    from data.importers.google_sheets_importer import GoogleSheetsImporter
    from core.models.data_models import ProcessingResult, ValidationResult, ValidationStatus


@dataclass
class PPEProcessingFiles:
    """PPE處理檔案配置"""
    # 主要檔案
    contract_filing_list_url: str  # 合約歸檔清單檔案路徑
    
    # 可選檔案
    google_sheets_id: Optional[str] = None  # Google Sheets ID (如果從配置讀取)
    credentials_path: Optional[str] = None  # Google服務帳號憑證路徑
    
    # 處理參數
    current_month: int = 202501  # 當前月份 (YYYYMM格式)
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        驗證檔案是否存在且參數有效
        
        Returns:
            Tuple[bool, List[str]]: (是否有效, 錯誤訊息列表)
        """
        errors = []
        
        # 驗證合約歸檔清單檔案
        if not self.contract_filing_list_url:
            errors.append("合約歸檔清單檔案路徑為空")
        elif not Path(self.contract_filing_list_url).exists():
            errors.append(f"合約歸檔清單檔案不存在: {self.contract_filing_list_url}")
        
        # 驗證當前月份格式
        if self.current_month:
            year = self.current_month // 100
            month = self.current_month % 100
            if year < 2020 or year > 2100:
                errors.append(f"年份不合理: {year}")
            if month < 1 or month > 12:
                errors.append(f"月份不合理: {month}")
        else:
            errors.append("未提供當前月份參數")
        
        return len(errors) == 0, errors
    
    def get_available_files(self) -> Dict[str, Any]:
        """獲取可用的檔案和參數"""
        files = {
            "contract_filing_list": self.contract_filing_list_url,
            "current_month": self.current_month
        }
        
        if self.google_sheets_id:
            files["google_sheets_id"] = self.google_sheets_id
        
        if self.credentials_path and Path(self.credentials_path).exists():
            files["credentials_path"] = self.credentials_path
        
        return files


class SpxPpeProcessor(BaseDataProcessor):
    """PPE處理器"""
    
    def __init__(self, entity_type: str = "SPX"):
        """
        初始化PPE處理器
        
        Args:
            entity_type: 實體類型，預設為'SPX'
        """
        super().__init__(entity_type)
        self.logger = get_logger(f"{self.__class__.__name__}_{entity_type}")
        self.logger.info(f"初始化 {entity_type} PPE處理器")
    
    def process(self, files: Optional[PPEProcessingFiles] = None, **kwargs) -> ProcessingResult:
        """
        PPE處理主流程入口
        
        Args:
            files: PPE處理檔案配置物件
            **kwargs: 向後相容的參數
            
        Returns:
            ProcessingResult: 處理結果物件
        """
        # 記錄開始時間
        start_time = datetime.now()
        result = ProcessingResult(
            success=False,
            message="處理尚未開始",
            start_time=start_time
        )
        
        try:
            # 處理向後相容性
            if files is None:
                files = self._create_files_from_kwargs(**kwargs)
            
            # 驗證輸入
            is_valid, errors = files.validate()
            if not is_valid:
                for error in errors:
                    result.add_error(error)
                result.message = "輸入驗證失敗"
                return result
            
            self.logger.info("開始處理PPE折舊期間計算")
            self.logger.info(f"處理參數: 當前月份={files.current_month}")
            
            # 執行主要處理邏輯
            df_result = self._process_depreciation_period(files, result)
            
            if df_result is not None and not df_result.empty:
                result.processed_data = df_result
                result.success = True
                result.message = "PPE折舊期間計算完成"
                result.total_records = len(df_result)
                result.processed_records = len(df_result)
                
                # 添加統計資訊到metadata
                result.metadata = {
                    "current_month": files.current_month,
                    "unique_sp_codes": df_result['sp_code'].nunique() if 'sp_code' in df_result.columns else 0,
                    "unique_addresses": df_result['address'].nunique() if 'address' in df_result.columns else 0,
                    "columns": df_result.columns.tolist()
                }
                
                self.logger.info(f"處理成功: {result.total_records} 筆記錄")
            else:
                result.message = "處理完成但無有效數據"
                result.add_warning("處理結果為空DataFrame")
            
        except Exception as e:
            self.logger.error(f"PPE處理發生錯誤: {str(e)}", exc_info=True)
            result.add_error(str(e))
            result.message = f"處理失敗: {str(e)}"
        
        finally:
            # 記錄結束時間
            result.end_time = datetime.now()
            processing_time = result.processing_time
            if processing_time:
                self.logger.info(f"處理耗時: {processing_time:.2f} 秒")
        
        return result
    
    def _create_files_from_kwargs(self, **kwargs) -> PPEProcessingFiles:
        """
        從kwargs創建PPEProcessingFiles物件（向後相容）
        
        Args:
            **kwargs: 關鍵字參數
            
        Returns:
            PPEProcessingFiles: 檔案配置物件
        """
        return PPEProcessingFiles(
            contract_filing_list_url=kwargs.get('contract_filing_list_url', ''),
            current_month=kwargs.get('current_month', 202501),
            google_sheets_id=kwargs.get('google_sheets_id'),
            credentials_path=kwargs.get('credentials_path')
        )
    
    def _process_depreciation_period(self, 
                                     files: PPEProcessingFiles, 
                                     result: ProcessingResult) -> Optional[pd.DataFrame]:
        """
        執行折舊期間計算的核心邏輯
        
        Args:
            files: 檔案配置物件
            result: 處理結果物件（用於記錄過程資訊）
            
        Returns:
            Optional[pd.DataFrame]: 處理結果DataFrame
        """
        try:
            # 1. 讀取歸檔清單
            self.logger.info("步驟1: 讀取合約歸檔清單")
            df_filing_raw = self._read_filing_list(files, result)
            if df_filing_raw is None:
                return None
            
            # 2. 讀取續約清單
            self.logger.info("步驟2: 讀取續約清單")
            df_renewal_raw = self._get_consolidated_contract_data(result)
            if df_renewal_raw is None or df_renewal_raw.empty:
                result.add_error("無法獲取續約清單數據")
                return None
            
            # 3. 清理資料
            self.logger.info("步驟3: 清理資料")
            df_filing_clean = self.clean_dataframe(df_filing_raw)
            df_renewal_clean = self.clean_dataframe(df_renewal_raw)
            
            # 記錄清理統計
            result.metadata['filing_records_before_clean'] = len(df_filing_raw)
            result.metadata['filing_records_after_clean'] = len(df_filing_clean)
            result.metadata['renewal_records_before_clean'] = len(df_renewal_raw)
            result.metadata['renewal_records_after_clean'] = len(df_renewal_clean)
            
            # 4. 標準化資料
            self.logger.info("步驟4: 標準化資料")
            df_filing_std = self.standardize_filing_data(df_filing_clean)
            df_renewal_std = self.standardize_renewal_data(df_renewal_clean)
            
            if df_filing_std is None or df_renewal_std is None:
                result.add_error("資料標準化失敗")
                return None
            
            # 5. 合併資料
            self.logger.info("步驟5: 合併資料")
            merge_keys = self.config_manager.get_list(
                self.entity_type, 
                'key_for_merging_origin_and_renew_contract'
            )
            
            df_merge = self.merge_contract_data(
                df_filing_std, 
                df_renewal_std, 
                merge_on=merge_keys
            )
            
            if df_merge is None or df_merge.empty:
                result.add_warning("合併後無匹配記錄")
                return pd.DataFrame()
            
            df_merge = df_merge.drop_duplicates().reset_index(drop=True)
            
            # 6. 更新合約日期
            self.logger.info("步驟6: 更新合約日期")
            updated_df = self.update_contract_dates(df_merge).drop_duplicates()
            
            # 7. 計算月份差異
            self.logger.info("步驟7: 計算月份差異")
            selected_cols = ['sp_code', 'address', 'contract_start_day_filing', 'contract_end_day_renewal']
            
            # 檢查必要欄位是否存在
            missing_cols = [col for col in selected_cols if col not in updated_df.columns]
            if missing_cols:
                result.add_error(f"缺少必要欄位: {missing_cols}")
                return None
            
            result_df = self.calculate_month_difference(
                updated_df[selected_cols], 
                'contract_end_day_renewal', 
                files.current_month,
                'months_diff'
            )
            
            # 添加驗證結果
            self._validate_result(result_df, result)
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"處理折舊期間時發生錯誤: {str(e)}", exc_info=True)
            result.add_error(str(e))
            return None
    
    def _read_filing_list(self, files: PPEProcessingFiles, result: ProcessingResult) -> Optional[pd.DataFrame]:
        """
        讀取合約歸檔清單
        
        Args:
            files: 檔案配置物件
            result: 處理結果物件
            
        Returns:
            Optional[pd.DataFrame]: 歸檔清單DataFrame
        """
        try:
            sheet_name = self.config_manager._config_data.get(self.entity_type, {}).get(
                'contract_filing_list_sheet', 0
            )
            use_cols = self.config_manager._config_data.get(self.entity_type, {}).get(
                'contract_filing_list_range', None
            )
            
            df = self.read_excel_file(
                files.contract_filing_list_url,
                sheet_name=sheet_name,
                usecols=use_cols
            )
            
            if df is not None:
                result.add_validation_result(
                    ValidationResult(
                        status=ValidationStatus.VALID,
                        message=f"成功讀取歸檔清單: {len(df)} 筆記錄",
                        field="contract_filing_list"
                    )
                )
            else:
                result.add_validation_result(
                    ValidationResult(
                        status=ValidationStatus.INVALID,
                        message="無法讀取歸檔清單",
                        field="contract_filing_list"
                    )
                )
            
            return df
            
        except Exception as e:
            self.logger.error(f"讀取歸檔清單失敗: {str(e)}")
            result.add_error(f"讀取歸檔清單失敗: {str(e)}")
            return None
    
    def _validate_result(self, df: pd.DataFrame, result: ProcessingResult) -> None:
        """
        驗證處理結果
        
        Args:
            df: 結果DataFrame
            result: 處理結果物件
        """
        if df is None or df.empty:
            result.add_validation_result(
                ValidationResult(
                    status=ValidationStatus.WARNING,
                    message="處理結果為空",
                    field="result"
                )
            )
            return
        
        # 檢查必要欄位
        required_columns = ['sp_code', 'address', 'months_diff']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            result.add_validation_result(
                ValidationResult(
                    status=ValidationStatus.WARNING,
                    message=f"結果缺少欄位: {missing_columns}",
                    field="result_columns"
                )
            )
        else:
            result.add_validation_result(
                ValidationResult(
                    status=ValidationStatus.VALID,
                    message="結果驗證通過",
                    field="result"
                )
            )
        
        # 檢查異常值
        if 'months_diff' in df.columns:
            invalid_months = df[df['months_diff'] < 0]
            if not invalid_months.empty:
                result.add_warning(f"發現 {len(invalid_months)} 筆月份差異為負值的記錄")
        
        # 檢查重複記錄
        if 'sp_code' in df.columns:
            duplicate_sp = df[df.duplicated(subset=['sp_code'], keep=False)]
            if not duplicate_sp.empty:
                result.add_warning(f"發現 {len(duplicate_sp)} 筆重複的店號記錄")
    
    def _get_consolidated_contract_data(self, result: ProcessingResult) -> pd.DataFrame:
        """
        從Google Sheets獲取並整合合約數據
        
        Args:
            result: 處理結果物件
        
        Returns:
            pandas.DataFrame: 整合後的合約數據
        """
        spreadsheet_id = self.config_manager.get(self.entity_type, 'expanded_contract_wb')
        
        if not spreadsheet_id:
            result.add_error("未配置Google Sheets ID")
            return pd.DataFrame()
        
        # 創建GoogleSheetsImporter實例
        try:
            sheets_importer = GoogleSheetsImporter(self.config_manager.get_credentials_config())
        except Exception as e:
            result.add_error(f"初始化Google Sheets Importer失敗: {str(e)}")
            return pd.DataFrame()
        
        try:
            # 獲取第三期續約案件數據
            df_thi = sheets_importer.get_sheet_data(
                spreadsheet_id,
                self.config_manager.get(self.entity_type, 'expanded_contract_sheet3'),
                self.config_manager.get(self.entity_type, 'expanded_contract_range')
            )
            df_thi = df_thi.iloc[:, [0, 2, *range(13, 17)]]
            
            # 獲取第二期續約案件數據
            df_sec = sheets_importer.get_sheet_data(
                spreadsheet_id,
                self.config_manager.get(self.entity_type, 'expanded_contract_sheet2'),
                self.config_manager.get(self.entity_type, 'expanded_contract_range')
            )
            df_sec = df_sec.iloc[:, [0, 2, *range(13, 17)]]
            
            # 統一列名（以第二期為標準）
            std_cols = df_sec.columns
            df_thi.columns = std_cols
            
            # 獲取第一期合約數據
            df_ft = sheets_importer.get_sheet_data(
                spreadsheet_id,
                self.config_manager.get(self.entity_type, 'expanded_contract_sheet1'),
                self.config_manager.get(self.entity_type, 'expanded_contract_range1')
            )
            df_ft = df_ft.iloc[:2576, [0, 6, 21, 24]].query(
                "起租日!='' and 起租日!='#N/A' and ~起租日.isna()"
            ).assign(
                tempA=df_ft.iloc[:, 21],
                tempB=df_ft.iloc[:, 24]
            )
            df_ft.columns = std_cols
            
            # 合併所有數據
            df_expanded_contract_ft2thi = pd.concat([df_ft, df_sec, df_thi], ignore_index=True)
            
            self.logger.info(f"成功獲取續約資料: 共 {len(df_expanded_contract_ft2thi)} 筆記錄")
            result.metadata['renewal_data_count'] = len(df_expanded_contract_ft2thi)
            
            return df_expanded_contract_ft2thi
            
        except Exception as e:
            self.logger.error(f"獲取續約數據時發生錯誤: {e}")
            result.add_error(f"獲取續約數據失敗: {str(e)}")
            return pd.DataFrame()
    
    # === 以下是原有的輔助方法，保持不變 ===
    
    def validate_file_path(self, file_path: Union[str, Path]) -> bool:
        """驗證檔案路徑是否存在"""
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
            # 預設欄位映射
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
    
    def update_contract_dates(self, df):
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
        
        # 將日期欄位轉換為datetime格式
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
    
    # === 向後相容的舊方法名稱 ===
    def _get_depreciation_period(self, *args, **kwargs):
        """向後相容的方法名稱"""
        self.logger.info("呼叫舊方法名稱 _get_depreciation_period，轉向新的 process 方法")
        return self.process(**kwargs)
