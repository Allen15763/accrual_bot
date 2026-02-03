"""
數據整合與清理

- 前期底稿、採購底稿與摘要期間解析(DateLogicStep)為通用步驟放在common
"""
import time
import os
import re
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder, 
    create_error_metadata
)
from accrual_bot import GoogleSheetsImporter
from accrual_bot.utils.helpers.data_utils import (classify_description, 
                                                  give_account_by_keyword,
                                                  clean_po_data)


class ColumnAdditionStep(PipelineStep):
    """
    添加 SPX 特有欄位
    
    功能:
    1. 調用 add_basic_columns() 添加基礎欄位
    2. 添加 SPX 特定欄位 (累計至本期驗收數量/金額等)
    
    輸入: DataFrame
    輸出: DataFrame with additional columns
    """
    
    def __init__(self, name: str = "ColumnAddition", **kwargs):
        super().__init__(name, description="Add SPX-specific columns", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行欄位添加"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()
            input_count = len(df)
            m = context.get_variable('processing_month')
            
            original_columns = set(df.columns)
            
            # 添加基礎欄位 (調用原 processor 的邏輯)
            # 這裡可以直接調用原有的方法，或在此處重新實作
            df, previous_month = self._add_basic_columns(df, m)
            
            # 添加 SPX 特定欄位
            if context.metadata.entity_type == 'SPX' and context.metadata.processing_type == 'PO':
                df['累計至本期驗收數量/金額'] = None
            df['GL DATE'] = None
            df['Remarked by Procurement PR'] = None
            df['Noted by Procurement PR'] = None
            df['Remarked by 上月 FN PR'] = None
            
            # 更新月份變數
            context.set_variable('processing_month', m)
            
            # 偵測輸入參數，含有PR的原始檔路徑時，修改欄位名稱為PR
            if 'raw_pr' in context.get_variable('file_paths').keys():
                df = df.rename(columns={'PO狀態': 'PR狀態'})
            context.update_data(df)
            
            new_columns = set(df.columns) - original_columns
            output_count = len(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            self.logger.info(f"Added {len(new_columns)} columns: {new_columns}")
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, output_count)
                        .set_process_counts(processed=output_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('columns_added', len(new_columns))
                        .add_custom('new_columns', list(new_columns))
                        .add_custom('total_columns', len(df.columns))
                        .add_custom('updated_month', m)
                        .build())
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Added {len(new_columns)} columns",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Column addition failed: {str(e)}", exc_info=True)
            context.add_error(f"Column addition failed: {str(e)}")
            error_metadata = create_error_metadata(
                e, context, self.name,
                stage='column_addition'
            )
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Column addition failed: {str(e)}",
                duration=duration,
                metadata=error_metadata
            )
    
    def _add_basic_columns(self, df: pd.DataFrame, month: int) -> Tuple[pd.DataFrame, int]:
        """
        添加基本必要列
        
        Args:
            df: 原始PO數據
            month: 月份
            
        Returns:
            Tuple[pd.DataFrame, int]: 添加了必要列的DataFrame和更新的月份
        """
        try:
            df_copy = df.copy()
            
            # 添加狀態欄位
            df_copy['是否結案'] = np.where(
                df_copy.get('Closed For Invoice', '0') == '0', 
                "未結案", 
                '結案'
            )
            
            # 計算結案差異數量
            if '是否結案' in df_copy.columns:
                df_copy['結案是否有差異數量'] = np.where(
                    df_copy['是否結案'] == '結案',
                    pd.to_numeric(df_copy.get('Entry Quantity', 0), errors='coerce') - 
                    pd.to_numeric(df_copy.get('Billed Quantity', 0), errors='coerce'),
                    '未結案'
                )
            
            # 檢查入帳金額
            df_copy['Check with Entry Invoice'] = np.where(
                pd.to_numeric(df_copy.get('Entry Billed Amount', 0), errors='coerce') > 0,
                pd.to_numeric(df_copy.get('Entry Amount', 0), errors='coerce') - 
                pd.to_numeric(df_copy.get('Entry Billed Amount', 0), errors='coerce'),
                '未入帳'
            )
            
            # 生成行號標識
            if 'PR#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PR Line'] = df_copy['PR#'].astype('string') + '-' + df_copy['Line#'].astype('string')
            
            if 'PO#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PO Line'] = df_copy['PO#'].astype('string') + '-' + df_copy['Line#'].astype('string')
            
            # 添加標記和備註欄位
            self._add_remark_columns(df_copy)
            
            # 添加計算欄位
            self._add_calculation_columns(df_copy)
            
            # 計算上月
            previous_month = 12 if month == 1 else month - 1
            
            self.logger.info("成功添加基本必要列")
            return df_copy, previous_month
            
        except Exception as e:
            self.logger.error(f"添加基本列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加基本列時出錯")
    
    def _add_remark_columns(self, df: pd.DataFrame) -> None:
        """添加備註相關欄位"""
        columns_to_add = [
            'Remarked by Procurement',
            'Noted by Procurement', 
            'Remarked by FN',
            'Noted by FN',
            'Remarked by 上月 Procurement',
            'Remarked by 上月 FN',
            'PO狀態'
        ]
        
        for col in columns_to_add:
            if col not in df.columns:
                df[col] = pd.NA
    
    def _add_calculation_columns(self, df: pd.DataFrame) -> None:
        """添加計算相關欄位"""
        calculation_columns = [
            '是否估計入帳',
            '是否為FA',
            '是否為S&M',
            'Account code',
            'Account Name',
            'Product code',
            'Region_c',
            'Dep.',
            'Currency_c',
            'Accr. Amount',
            'Liability',
            '是否有預付',
            'PR Product Code Check',
            'Question from Reviewer',
            'Check by AP'
        ]
        
        for col in calculation_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # 設置特定值
        df['是否為FA'] = self._determine_fa_status(df)
        df['是否為S&M'] = self._determine_sm_status(df)
    
    def _determine_fa_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為FA
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為FA的結果
        """
        fa_accounts: List = config_manager.get_list('FA_ACCOUNTS', 'spx')
        if 'GL#' in df.columns:
            return np.where(df['GL#'].astype('string').isin([str(x) for x in fa_accounts]), 'Y', '')
        return pd.Series('', index=df.index)
    
    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為S&M
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        if 'GL#' not in df.columns:
            return pd.Series('N', index=df.index)
        
        return np.where(
            (df['GL#'].astype('string') == '650003') | (df['GL#'].astype('string') == '450014'), 
            "Y", "N"
        )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for column addition")
            return False
        
        return True


class APInvoiceIntegrationStep(PipelineStep):
    """
    AP Invoice 整合步驟
    
    功能:
    從 AP Invoice 數據中提取 GL DATE 並填入 PO 數據
    排除月份 m 之後的期間
    
    輸入: DataFrame + AP Invoice auxiliary data
    輸出: DataFrame with GL DATE column
    """
    
    def __init__(self, name: str = "APInvoiceIntegration", **kwargs):
        super().__init__(name, description="Integrate AP Invoice GL DATE", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 AP Invoice 整合"""
        start_time = time.time()
        start_datetime = datetime.now()
        try:
            df = context.data.copy()
            input_count = len(df)
            df_ap = context.get_auxiliary_data('ap_invoice')
            yyyymm = context.get_variable('processing_date')
            
            if df_ap is None or df_ap.empty:
                self.logger.warning("No AP Invoice data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No AP Invoice data"
                )
            
            self.logger.info("Processing AP Invoice integration...")
            
            # 移除缺少 'PO Number' 的行
            df_ap = df_ap.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # 創建組合鍵
            df_ap['po_line'] = (
                df_ap['Company'].astype('string') + '-' + 
                df_ap['PO Number'].astype('string') + '-' + 
                df_ap['PO_LINE_NUMBER'].astype('string')
            )
            
            # 轉換 Period 為 yyyymm 格式
            df_ap['period'] = (
                pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
                .dt.strftime('%Y%m')
                .fillna('0')
                .astype('Int32')
            )
            
            df_ap['match_type'] = df_ap['Match Type'].fillna('system_filled')
            
            # 只保留期間在 yyyymm 之前的 AP 發票
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 合併到主 DataFrame
            df = df.merge(
                df_ap[['po_line', 'period', 'match_type']], 
                left_on='PO Line', 
                right_on='po_line', 
                how='left'
            )
            
            df['GL DATE'] = df['period']
            df.drop(columns=['po_line', 'period'], inplace=True)
            
            context.update_data(df)
            
            matched_count = df['GL DATE'].notna().sum()
            output_count = len(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            self.logger.info(f"AP Invoice integration completed: {matched_count} records matched")
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, output_count)
                        .set_process_counts(processed=output_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('matched_records', int(matched_count))
                        .add_custom('total_records', len(df))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Integrated GL DATE for {matched_count} records",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"AP Invoice integration failed: {str(e)}", exc_info=True)
            context.add_error(f"AP Invoice integration failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for AP Invoice integration")
            return False
        
        if 'PO Line' not in context.data.columns:
            self.logger.error("Missing 'PO Line' column")
            return False
        
        return True
    

class ClosingListIntegrationStep(PipelineStep):
    """
    關單清單整合步驟
    
    功能:
    1. 從 Google Sheets 獲取 SPX 關單數據
    2. 合併多個年份的關單記錄
    3. 清理和處理數據
    4. 將關單信息整合到主數據中
    
    輸入: DataFrame
    輸出: DataFrame with closing list info
    
    參考: async_data_importer.import_spx_closing_list()
    """
    
    def __init__(self, name: str = "ClosingListIntegration", **kwargs):
        super().__init__(name, description="Integrate closing list from Google Sheets", **kwargs)
        self.sheets_importer = None
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行關單清單整合"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date
            
            self.logger.info("Getting SPX closing list from Google Sheets...")
            
            # 準備配置
            config = self._prepare_config()
            
            # 獲取關單數據
            df_spx_closing = self._get_closing_note(config)
            
            if df_spx_closing is None or df_spx_closing.empty:
                self.logger.warning("No closing list data available")
                context.add_auxiliary_data('closing_list', pd.DataFrame())
            else:
                context.add_auxiliary_data('closing_list', df_spx_closing)
                self.logger.info(f"Loaded {len(df_spx_closing)} closing records")
            
            context.update_data(df)
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Closing list integrated: {len(df_spx_closing) if df_spx_closing is not None else 0} records",
                duration=duration,
                metadata={
                    'closing_records': len(df_spx_closing) if df_spx_closing is not None else 0
                }
            )
            
        except Exception as e:
            self.logger.error(f"Closing list integration failed: {str(e)}", exc_info=True)
            context.add_error(f"Closing list integration failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _prepare_config(self) -> Dict[str, Any]:
        """準備 Google Sheets API 配置"""
        
        config = {
            'certificate_path': config_manager.get_credentials_config().get('certificate_path', None),
            'scopes': config_manager.get_credentials_config().get('scopes', None)
        }
        
        return config
    
    def _get_closing_note(self, config: Dict[str, Any]) -> pd.DataFrame:
        """獲取 SPX 關單數據
        
        參考 async_data_importer.import_spx_closing_list() 的實現
        從多個工作表讀取並合併關單記錄
        """
        try:
            # 初始化 Google Sheets Importer
            
            if self.sheets_importer is None:
                self.sheets_importer = GoogleSheetsImporter(config)
            
            # 定義要查詢的工作表
            # Spreadsheet ID: SPX 關單清單
            spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'
            
            queries = [
                (spreadsheet_id, '2023年_done', 'A:J'),
                (spreadsheet_id, '2024年', 'A:J'),
                (spreadsheet_id, '2025年', 'A:J')
            ]
            
            dfs = []
            for sheet_id, sheet_name, range_value in queries:
                try:
                    self.logger.info(f"Reading sheet: {sheet_name}")
                    df = self.sheets_importer.get_sheet_data(
                        sheet_id, 
                        sheet_name, 
                        range_value,
                        header_row=True,
                        skip_first_row=True
                    )
                    
                    if df is not None and not df.empty:
                        dfs.append(df)
                        self.logger.info(f"Successfully read {len(df)} records from {sheet_name}")
                    else:
                        self.logger.warning(f"Sheet {sheet_name} is empty")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to read sheet {sheet_name}: {str(e)}")
                    continue
            
            if not dfs:
                self.logger.warning("No closing list data retrieved from any sheet")
                return pd.DataFrame()
            
            # 合併所有 DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Combined {len(combined_df)} total records from {len(dfs)} sheets")
            
            # 數據清理和重命名
            combined_df = self._clean_closing_data(combined_df)
            
            self.logger.info(f"After cleaning: {len(combined_df)} valid closing records")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error getting closing note: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def _clean_closing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理和處理關單數據
        
        參考 async_data_importer.import_spx_closing_list() 的清理邏輯
        """
        try:
            # 移除 Date 為空的記錄
            df_clean = df.dropna(subset=['Date']).copy()
            
            # 重命名欄位
            df_clean.rename(columns={
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
            
            # 過濾空的日期記錄
            df_clean = df_clean.query("date != ''").reset_index(drop=True)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cleaning closing data: {str(e)}")
            # 如果清理失敗，返回原始數據
            return df
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for closing list integration")
            return False
        
        return True
    

class ValidationDataProcessingStep(PipelineStep):
    """
    驗收數據處理步驟
    
    功能:
    1. 處理智取櫃驗收明細
    2. 處理繳費機驗收明細
    3. 將驗收數據應用到 PO DataFrame
    
    輸入: DataFrame + Validation file path
    輸出: DataFrame with validation data applied
    """
    
    def __init__(self, 
                 name: str = "ValidationDataProcessing",
                 validation_file_path: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Process validation data", **kwargs)
        self.validation_file_path = validation_file_path
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行驗收數據處理"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date
            
            # 從 context 獲取驗收文件路徑
            # 優先順序: self.validation_file_path > context variable > file_paths['ops_validation']
            validation_path = self.validation_file_path or context.get_variable('validation_file_path')

            # 如果還沒有路徑，嘗試從 file_paths 獲取（UI 上傳的情況）
            if not validation_path:
                file_paths_data = context.get_variable('file_paths')
                if file_paths_data and 'ops_validation' in file_paths_data:
                    ops_validation = file_paths_data['ops_validation']
                    # 處理字符串或字典格式
                    if isinstance(ops_validation, str):
                        validation_path = ops_validation
                    elif isinstance(ops_validation, dict) and 'path' in ops_validation:
                        validation_path = ops_validation['path']

            if not validation_path:
                self.logger.warning("No validation file path provided, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No validation file path"
                )
            
            if not os.path.exists(validation_path):
                self.logger.warning(f"Validation file not found: {validation_path}")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="Validation file not found"
                )
            
            self.logger.info("Processing validation data...")

            # 獲取 Excel 參數，提供默認值
            file_paths_data = context.get_variable('file_paths')
            ops_validation_config = file_paths_data.get('ops_validation')

            # 處理兩種情況：
            # 1. ops_validation 是字符串（UI 上傳）
            # 2. ops_validation 是字典（包含 params）
            if isinstance(ops_validation_config, str):
                # UI 上傳的情況，使用默認參數
                excel_params = {}
            elif isinstance(ops_validation_config, dict):
                excel_params = ops_validation_config.get('params', {})
            else:
                excel_params = {}

            # 提供默認的 Excel 讀取參數（來自 config/paths.toml）
            default_params = {
                'sheet_name': '智取櫃驗收明細',
                'header': 3,
                'usecols': 'A:AH',
                'kiosk_sheet_name': '繳費機驗收明細',
                'kiosk_usecols': 'A:G'
            }

            # 合併默認參數
            for key, default_value in default_params.items():
                if key not in excel_params or excel_params[key] is None:
                    excel_params[key] = default_value

            self.df_locker = pd.read_excel(
                validation_path,
                sheet_name=excel_params['sheet_name'],
                header=excel_params['header'],
                usecols=excel_params['usecols']
            )

            self.df_kiosk = pd.read_excel(
                validation_path,
                sheet_name=excel_params['kiosk_sheet_name'],
                usecols=excel_params['kiosk_usecols']
            )
            
            # 處理驗收數據
            locker_non_discount, locker_discount, discount_rate, kiosk_data = \
                self._process_validation_data(validation_path, processing_date)
            
            # 應用驗收數據
            df = self._apply_validation_data(df, locker_non_discount, locker_discount, 
                                             discount_rate, kiosk_data)
            
            if len(locker_non_discount) != 0:
                context.add_auxiliary_data('locker_non_discount', pd.DataFrame(locker_non_discount).T)
            else:
                context.add_auxiliary_data('locker_non_discount', pd.DataFrame())
            if len(locker_discount) != 0:
                context.add_auxiliary_data('locker_discount', pd.DataFrame(locker_discount).T)
            else:
                context.add_auxiliary_data('locker_discount', pd.DataFrame())
            if len(kiosk_data) != 0:
                context.add_auxiliary_data('kiosk_data', 
                                           pd.DataFrame(kiosk_data.values(), 
                                                        index=kiosk_data.keys(), 
                                                        columns=['當期驗收個數']))
            else:
                context.add_auxiliary_data('kiosk_data', pd.DataFrame())
            
            context.update_data(df)
            
            validation_count = (df['本期驗收數量/金額'] != 0).sum() if '本期驗收數量/金額' in df.columns else 0
            
            self.logger.info(f"Validation data processed: {validation_count} records updated")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Validation data applied to {validation_count} records",
                duration=duration,
                metadata={
                    'validation_records': int(validation_count),
                    'locker_non_discount_count': len(locker_non_discount),
                    'locker_discount_count': len(locker_discount),
                    'kiosk_count': len(kiosk_data)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Validation data processing failed: {str(e)}", exc_info=True)
            context.add_error(f"Validation data processing failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _process_validation_data(self, validation_file_path: str, target_date: int) -> Tuple[Dict, Dict, float, Dict]:
        """
        處理驗收數據 - 智取櫃和繳費機驗收明細
        
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Tuple[Dict, Dict, Dict]: (智取櫃非折扣驗收數量, 智取櫃折扣驗收數量, 折扣率, 繳費機驗收數量)
        """
        
        # 處理智取櫃驗收明細
        locker_data = self._process_locker_validation_data(validation_file_path, target_date)
        
        # 處理繳費機驗收明細
        kiosk_data = self._process_kiosk_validation_data(validation_file_path, target_date)
        
        return locker_data['non_discount'], locker_data['discount'], locker_data.get('discount_rate'), kiosk_data
    
    def _process_locker_validation_data(self, validation_file_path: str, target_date: int) -> Dict:
        """
        處理智取櫃驗收明細
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 包含非折扣和折扣驗收數量的字典
        """
        
        try:
            # 讀取智取櫃驗收明細
            df_locker = self.df_locker.copy()
            
            if df_locker.empty:
                return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
            
            # 設置欄位名稱
            locker_columns = config_manager._config_toml.get('spx').get('locker_columns')
            df_locker.columns = locker_columns
            
            # 過濾和轉換
            df_locker = df_locker.loc[~df_locker['驗收月份'].isna(), :].reset_index(drop=True)
            df_locker['validated_month'] = pd.to_datetime(
                df_locker['驗收月份'], errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            # 移除無效日期的記錄
            df_locker = df_locker.dropna(subset=['validated_month']).reset_index(drop=True)
            # 篩選目標月份的數據
            df_locker_filtered = df_locker.loc[df_locker['validated_month'] == target_date, :]
            
            if df_locker_filtered.empty:
                return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
            
            # 聚合欄位; _config_toml.get('spx').get('locker_columns')[5:-5]
            agg_cols = config_manager._config_toml.get('spx').get('locker_agg_columns')
            
            return self._categorize_validation_data(df_locker_filtered, agg_cols)
            
        except Exception as e:
            self.logger.error(f"Processing locker validation data failed: {str(e)}")
            return {'non_discount': {}, 'discount': {}, 'discount_rate': None}
    
    def _process_kiosk_validation_data(self, validation_file_path: str, target_date: int) -> Dict:
        """
        處理繳費機驗收明細
        Args:
            validation_file_path: 驗收明細檔案路徑
            target_date: 目標日期 (YYYYMM格式)
            
        Returns:
            Dict[str, dict]: 繳費機驗收數量字典
        """
        try:
            # 讀取繳費機驗收明細
            df_kiosk = self.df_kiosk
            
            if df_kiosk.empty:
                return {}
            
            # 過濾和轉換
            df_kiosk = df_kiosk.loc[~df_kiosk['驗收月份'].isna(), :].reset_index(drop=True)
            df_kiosk['validated_month'] = pd.to_datetime(
                df_kiosk['驗收月份'], errors='coerce'
            ).dt.strftime('%Y%m').astype('Int64')
            
            df_kiosk = df_kiosk.dropna(subset=['validated_month']).reset_index(drop=True)
            df_kiosk_filtered = df_kiosk.loc[df_kiosk['validated_month'] == target_date, :]
            
            if df_kiosk_filtered.empty:
                return {}
            
            # 取得當期驗收數
            kiosk_validation = df_kiosk_filtered['PO單號'].value_counts().to_dict()
            return kiosk_validation
            
        except Exception as e:
            self.logger.error(f"Processing kiosk validation data failed: {str(e)}")
            return {}
    
    def _categorize_validation_data(self, df: pd.DataFrame, agg_cols: List[str]) -> Dict:
        """
        分類驗收數據為折扣和非折扣
        Args:
            df: 驗收數據DataFrame
            agg_cols: 需要聚合的欄位列表
            
        Returns:
            Dict[str, dict]: 包含 'non_discount' 和 'discount'， 'discount_rate' 鍵的字典
        """
        
        validation_results = {'non_discount': {}, 'discount': {}, 'discount_rate': None}
        
        if 'discount' not in df.columns:
            self.logger.warning("智取櫃數據中沒有 discount 欄位，所有數據將歸類為非折扣")
            df['discount'] = ''
        
        # 確保 discount 欄位為字符串類型
        df['discount'] = df['discount'].fillna('').astype('string')
        
        # 非折扣驗收 (不包含 X折驗收/出貨 的記錄)
        locker_discount_pattern = config_manager.get('SPX', 'locker_discount_pattern', r'\d+折')
        non_discount_condition = ~df['discount'].str.contains(locker_discount_pattern, na=False, regex=True)
        df_non_discount = df.loc[non_discount_condition, :]
        
        if not df_non_discount.empty and 'PO單號' in df_non_discount.columns:
            validation_results['non_discount'] = (
                df_non_discount.groupby(['PO單號'])[agg_cols]
                .sum()
                .to_dict('index')
            )
        
        # 折扣驗收
        discount_condition = df['discount'].str.contains(locker_discount_pattern, na=False, regex=True)
        df_discount = df.loc[discount_condition, :]
        
        if not df_discount.empty and 'PO單號' in df_discount.columns:
            validation_results['discount'] = (
                df_discount.groupby(['PO單號'])[agg_cols]
                .sum()
                .to_dict('index')
            )
            # 提取折扣率
            validation_results['discount_rate'] = self._extract_discount_rate(df_discount['discount'].unique())
        
        return validation_results
    
    def _extract_discount_rate(self, 
                               discount_input: Optional[Union[str, np.ndarray, pd.arrays.StringArray]]
                               ) -> Optional[float]:
        """從輸入中提取折扣率。
        
        此函數能處理字串或包含字串的 NumPy 陣列。
        如果輸入為陣列，預設只會處理第一個元素。

        Args:
            discount_input: 折扣字串 (e.g., "8折驗收") 或包含此類字串的陣列。
            
        Returns:
            折扣率 (e.g., 0.8)，若無法提取或輸入無效則返回 None。
        """
        if discount_input is None:
            return None
        
        # --- 輸入正規化 ---
        target_str: Optional[str] = None
        
        if isinstance(discount_input, str):
            target_str = discount_input
        elif isinstance(discount_input, (np.ndarray, pd.arrays.StringArray)):
            if discount_input.size == 0:
                self.logger.debug("輸入的陣列為空，無法提取折扣率。")
                return None
            
            if discount_input.size > 1:
                self.logger.warning(
                    f"輸入為多值陣列，只處理第一個元素 '{discount_input[0]}'. "
                    f"被忽略的值: {list(discount_input[1:])}"
                )
            
            first_element = discount_input[0]
            # 處理 NaN 或 None
            if first_element is None or (isinstance(first_element, float) and np.isnan(first_element)):
                self.logger.debug("陣列第一個元素為空值，無法提取折扣率。")
                return None
            
            target_str = str(first_element)  # 確保取出的元素是字串
        else:
            self.logger.error(f"不支援的輸入類型: {type(discount_input)}")
            raise TypeError(
                f"Input must be str, np.ndarray, or pd.arrays.StringArray, not {type(discount_input)}"
            )

        # --- 核心提取邏輯 ---
        if not target_str or not target_str.strip():  # 處理空字串或 None 的情況
            return None

        # 支援小數點和空格: "8折", "8.5折", "8 折"
        match = re.search(r'(\d+(?:\.\d+)?)[\s]*折', target_str)
        if match:
            discount_num = float(match.group(1))
            
            # 驗證折扣數值合理性
            if not (0 < discount_num <= 10):
                self.logger.warning(f"提取到異常折扣數值: {discount_num}，超出合理範圍 (0-10]")
                return None
            
            rate = discount_num / 10.0
            self.logger.info(f"從 '{target_str}' 成功提取折扣率: {rate}")
            return rate

        self.logger.debug(f"在 '{target_str}' 中未找到符合 'N折' 格式的內容。")
        return None
    
    def _apply_validation_data(self, df: pd.DataFrame, locker_non_discount: Dict, 
                               locker_discount: Dict, discount_rate: float, kiosk_data: Dict) -> pd.DataFrame:
        """
        應用驗收數據到 PO DataFrame
        Args:
            df: PO DataFrame
            locker_non_discount: 智取櫃非折扣驗收數據 {PO#: {A:value, B:value, ...}}
            locker_discount: 智取櫃折扣驗收數據 {PO#: {A:value, B:value, ...}}
            discount_rate: 折扣率
            kiosk_data: 繳費機驗收數據 {PO#: value}
            
        Returns:
            pd.DataFrame: 更新後的PO DataFrame
        """
        
        # 初始化欄位
        df['本期驗收數量/金額'] = 0
        
        # 獲取供應商配置
        locker_suppliers = config_manager.get('SPX', 'locker_suppliers', '')
        kiosk_suppliers = config_manager.get('SPX', 'kiosk_suppliers', '')
        
        # 轉換為列表
        if isinstance(locker_suppliers, str):
            locker_suppliers = [s.strip() for s in locker_suppliers.split(',')]
        if isinstance(kiosk_suppliers, str):
            kiosk_suppliers = [s.strip() for s in kiosk_suppliers.split(',')]
        
        # 應用智取櫃非折扣驗收
        df = self._apply_locker_validation(df, locker_non_discount, locker_suppliers, is_discount=False)
        
        # 應用智取櫃折扣驗收
        df = self._apply_locker_validation(df, locker_discount, locker_suppliers, discount_rate, is_discount=True)
        
        # 應用繳費機驗收
        df = self._apply_kiosk_validation(df, kiosk_data, kiosk_suppliers)
        
        # 修改相關欄位
        df = self._modify_relevant_columns(df)

        # update 累計至本期驗收數量/金額 column
        df = self._update_cumulative_qty_for_ppe(df)
        
        return df
    
    def _apply_locker_validation(self, df: pd.DataFrame, locker_data: Dict, 
                                 locker_suppliers: List[str], discount_rate: float = None,
                                 is_discount: bool = False) -> pd.DataFrame:
        """
        應用智取櫃驗收數據
        Args:
            df: PO DataFrame
            locker_data: 智取櫃驗收數據 {PO#: {A:value, B:value, ...}}
            locker_suppliers: 智取櫃供應商列表
            discount_rate: 折扣率
            is_discount: 是否為折扣驗收
            
        Returns:
            pd.DataFrame: 更新後的DataFrame
        """
        if not locker_data:
            return df
        
        # 定義櫃體種類的正則表達式模式
        patterns = {
            # A~K類櫃體，後面非英文字母數字組合，但允許中文字符； whatever + locker ${type} + nonEng/digit
            #  e.g. 2025/12 SVP_SPX 門市智取櫃工程SPX locker XA 第二期款項 #SP-C-Leasehold； 後面是空白非英數->caught
            #       2025/12 SVP_SPX 門市智取櫃工程SPX locker XA第一期款項 #SP-C-Leasehold； 後面是中文非英數->caught
            'A': r'locker\s*A(?![A-Za-z0-9])',
            'B': r'locker\s*B(?![A-Za-z0-9])',
            'C': r'locker\s*C(?![A-Za-z0-9])',
            'D': r'locker\s*D(?![A-Za-z0-9])',
            'E': r'locker\s*E(?![A-Za-z0-9])',
            'F': r'locker\s*F(?![A-Za-z0-9])',
            'G': r'locker\s*G(?![A-Za-z0-9])',
            'H': r'locker\s*H(?![A-Za-z0-9])',
            'I': r'locker\s*I(?![A-Za-z0-9])',
            'J': r'locker\s*J(?![A-Za-z0-9])',
            'K': r'locker\s*K(?![A-Za-z0-9])',
            'DA': r'locker\s*控制主[櫃|機]',
            '控制系統': r'locker\s*控制系統',
            'XA': r'locker\s*XA(?![A-Za-z0-9])',
            'XB': r'locker\s*XB(?![A-Za-z0-9])',
            'XC': r'locker\s*XC(?![A-Za-z0-9])',
            'XD': r'locker\s*XD(?![A-Za-z0-9])',
            'XE': r'locker\s*XE(?![A-Za-z0-9])',
            'XF': r'locker\s*XF(?![A-Za-z0-9])',
            'XA30': r'locker\s*XA30(?![A-Za-z0-9])',
            'XC30': r'locker\s*XC30(?![A-Za-z0-9])',
            'XG': r'locker\s*XG(?![A-Za-z0-9])',
            '裝運費': r'locker\s*安裝運費',
            '超出櫃體安裝費': r'locker\s*超出櫃體安裝費',
            '超出櫃體運費': r'locker\s*超出櫃體運費'
        }
        
        # 遍歷DataFrame
        for idx, row in df.iterrows():
            try:
                po_number = row.get('PO#')
                item_desc = str(row.get('Item Description', ''))
                po_supplier = str(row.get('PO Supplier', ''))
                
                # 檢查條件; 不符合的資料該圈將被跳過
                # 檢查PO#是否在字典keys中
                if po_number not in locker_data:
                    continue
                # 檢查Item Description是否包含"門市智取櫃"
                if '門市智取櫃' not in item_desc:
                    continue
                # 對於非折扣驗收，檢查是否不包含"減價"
                if not is_discount and '減價' in item_desc:
                    continue
                # 檢查PO Supplier是否在配置的suppliers中
                if locker_suppliers and po_supplier not in locker_suppliers:
                    continue
                
                # 提取櫃體種類
                cabinet_type = None
                priority_order = config_manager._config_toml.get('spx').get('locker_priority_order')
                
                for ctype in priority_order:
                    if ctype in patterns:
                        if re.search(patterns[ctype], item_desc, re.IGNORECASE):
                            cabinet_type = ctype
                            break
                
                if cabinet_type and cabinet_type in locker_data[po_number]:
                    current_value = df.at[idx, '本期驗收數量/金額']
                    if current_value == 0:  # 只有當前值為0時才設置新值
                        validation_value = locker_data[po_number][cabinet_type]
                        df.at[idx, '本期驗收數量/金額'] = validation_value
                        
                        # 如果是折扣驗收，記錄折扣率
                        if is_discount and discount_rate:
                            df.at[idx, '折扣率'] = discount_rate
                
            except Exception as e:
                self.logger.debug(f"Error processing locker validation for row {idx}: {str(e)}")
                continue
        
        return df
    
    def _apply_kiosk_validation(self, df: pd.DataFrame, kiosk_data: Dict, 
                                kiosk_suppliers: List[str]) -> pd.DataFrame:
        """應用繳費機驗收數據"""
        if not kiosk_data:
            return df
        
        for idx, row in df.iterrows():
            try:
                po_number = row.get('PO#')
                item_desc = str(row.get('Item Description', ''))
                po_supplier = str(row.get('PO Supplier', ''))
                
                # 檢查條件
                if po_number not in kiosk_data:
                    continue
                if '門市繳費機' not in item_desc:
                    continue
                if kiosk_suppliers and po_supplier not in kiosk_suppliers:
                    continue
                
                current_value = df.at[idx, '本期驗收數量/金額']
                if current_value == 0:
                    validation_value = kiosk_data[po_number]
                    df.at[idx, '本期驗收數量/金額'] = validation_value
                
            except Exception as e:
                self.logger.debug(f"Error processing kiosk validation for row {idx}: {str(e)}")
                continue
        
        return df
    
    def _modify_relevant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """修改相關欄位"""
        
        need_to_accrual = df['本期驗收數量/金額'] != 0
        df.loc[need_to_accrual, '是否估計入帳'] = 'Y'
        
        # 設置 Account code
        fa_accounts = config_manager.get_list('SPX', 'fa_accounts')
        if fa_accounts:
            df.loc[need_to_accrual, 'Account code'] = fa_accounts[0]
        
        # 設置 Account Name
        df.loc[need_to_accrual, 'Account Name'] = 'AP,FA Clear Account'
        
        # 設置其他欄位
        df.loc[need_to_accrual, 'Product code'] = df.loc[need_to_accrual, 'Product Code']
        df.loc[need_to_accrual, 'Region_c'] = "TW"
        df.loc[need_to_accrual, 'Dep.'] = '000'
        df.loc[need_to_accrual, 'Currency_c'] = df.loc[need_to_accrual, 'Currency']
        
        # 計算 Accr. Amount
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') * df['本期驗收數量/金額'].fillna(0).astype('Float64')
        )
        
        # 套用折扣
        if '折扣率' in df.columns:
            has_discount = df['折扣率'].notna()
            df.loc[has_discount, 'temp_amount'] = (
                df.loc[has_discount, 'temp_amount'] * df.loc[has_discount, '折扣率'].astype('Float64')
            )
            df.drop('折扣率', axis=1, inplace=True)
        
        non_shipping = ~df['Item Description'].str.contains('運費|安裝費', na=False)
        df.loc[need_to_accrual & non_shipping, 'Accr. Amount'] = \
            df.loc[need_to_accrual & non_shipping, 'temp_amount']
        df.loc[need_to_accrual & ~non_shipping, 'Accr. Amount'] = \
            df.loc[need_to_accrual & ~non_shipping, '本期驗收數量/金額']
        df.drop('temp_amount', axis=1, inplace=True)
        
        # 設置 Liability
        df.loc[need_to_accrual, 'Liability'] = '200414'
        
        return df
    
    def _update_cumulative_qty_for_ppe(
            self, df: pd.DataFrame,
            raw_col: str = '累計至上期驗收數量/金額',
            updated_col: str = '累計至本期驗收數量/金額') -> pd.DataFrame:
        """更新會計摘要

        Args:
            df: 元資料
            raw_col: 保留(複製)從前期底稿撈回來的累計至本期驗收數量/金額欄位資訊. Defaults to '累計至上期驗收數量/金額'.
            updated_col: 更新qty資訊. Defaults to '累計至本期驗收數量/金額'.

        Returns:
            Dataframe
        """
        df_copy = df.copy()
        df_copy[raw_col] = df_copy[updated_col].astype('string')

        def convert_qty(x):
            try:
                return float(x)
            except Exception as err:
                return 0
            
        temp_qty = df_copy[updated_col].map(convert_qty)
        df_copy[updated_col] = df_copy['本期驗收數量/金額'].add(temp_qty, fill_value=0)
        
        is_raw_col_not_na = df_copy[raw_col].notna()
        is_updated_col_zero = df_copy[updated_col] == 0
        # 從原始累計至本期驗收數量/金額欄位判斷不是null的值且加總後仍等於零，使用原始資訊
        df_copy[updated_col] = np.where(is_raw_col_not_na & is_updated_col_zero,
                                        df_copy[raw_col],
                                        df_copy[updated_col])
        # To avoid from output error for parquet due to mix dtype in a series.
        df_copy[updated_col] = df_copy[updated_col].astype('string')
        return df_copy
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for validation processing")
            return False
        
        return True
    

class DataReformattingStep(PipelineStep):
    """
    數據格式化和重組步驟
    
    功能:
    1. 格式化數值列
    2. 格式化日期列
    3. 清理 nan 值
    4. 重新排列欄位順序
    5. 添加分類和關鍵字匹配
    
    輸入: DataFrame
    輸出: Formatted DataFrame ready for export
    """
    
    def __init__(self, name: str = "DataReformatting", **kwargs):
        super().__init__(name, description="Reformat and reorganize data", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據格式化"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            self.logger.info("Reformatting data...")
            
            # 格式化數值列
            df = self._format_numeric_columns(df)
            
            # 格式化日期列
            df = self._reformat_dates(df)
            
            # 清理 nan 值
            df = self._clean_nan_values(df)
            
            # 重新排列欄位
            df = self._rearrange_columns(df)
            
            # 添加分類
            df = self._add_classification(df)
            
            # 添加關鍵字匹配
            df = self._add_keyword_matching(df)
            
            # 添加分期標記
            df = self._add_installment_flag(df)
            df = self._installment_over_ppe_limit(df)
            
            # 重新命名欄位名稱、資料型態
            df = self._rename_columns_dtype(df)

            # 確保review AP等欄位在最後
            df = self._rearrange_columns(df)
            
            # 將含有暫時性計算欄位的結果存為附件
            if isinstance(df, pd.DataFrame) and not df.empty:
                data_name = 'result_with_temp_cols'
                data_copy = df.copy()
                context.add_auxiliary_data(data_name, data_copy)
                self.logger.info(
                    f"Added auxiliary data: {data_name} {data_copy.shape} shape)"
                )

            # 移除臨時欄位
            df = self._remove_temp_columns(df)
            # reformat ERM
            df = self._reformat_erm(df)

            # 減少輸出欄位
            processing_type = context.metadata.processing_type
            entity = context.metadata.entity_type
            if entity == 'SPX' and processing_type == 'PO':
                df = self._remove_columns(df)
            
            context.update_data(df)
            
            self.logger.info("Data reformatting completed")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Data reformatted successfully",
                duration=duration,
                metadata={
                    'total_columns': len(df.columns),
                    'total_rows': len(df)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data reformatting failed: {str(e)}", exc_info=True)
            context.add_error(f"Data reformatting failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _format_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化數值列"""
        # 整數列
        int_columns = ['Line#', 'GL#']
        for col in int_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        # 浮點數列
        float_columns = ['Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                         'Entry Billed Amount', 'Entry Prepay Amount', 
                         'PO Entry full invoiced status', 'Accr. Amount']
        for col in float_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        return df
    
    def _reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化日期列"""
        date_columns = ['Creation Date', 'Expected Received Month', 'Last Update Date']
        
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"Failed to format date column {col}: {str(e)}")
        
        return df
    
    def _remove_temp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除臨時計算列"""
        temp_columns = ['檔案日期', 'Expected Received Month_轉換格式', 'YMs of Item Description',
                        'expected_received_month_轉換格式', 'yms_of_item_description',
                        'PR Product Code Check', 'pr_product_code_check',
                        ]
        
        for col in temp_columns:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        
        return df
    
    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理 nan 值"""
        columns_to_clean = [
            '是否估計入帳', 'PR Product Code Check', 'PO狀態',
            'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
        ]
        
        for col in columns_to_clean:
            if col in df.columns:
                df[col] = df[col].replace('nan', pd.NA)
                df[col] = df[col].replace('<NA>', pd.NA)
        
        # 特殊處理 Accr. Amount
        if 'Accr. Amount' in df.columns:
            try:
                df['Accr. Amount'] = (
                    df['Accr. Amount'].astype('string').str.replace(',', '')
                    .replace('nan', '0')
                    .replace('<NA>', '0')
                    .astype('Float64')
                )
                df['Accr. Amount'] = df['Accr. Amount'].apply(lambda x: x if x != 0 else None)
            except Exception as e:
                self.logger.warning(f"Failed to clean Accr. Amount: {str(e)}")
        
        return df
    
    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        # 重新排列上月備註欄位位置
        if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
            fn_index = df.columns.get_loc('Remarked by FN') + 1
            last_month_col = df.pop('Remarked by 上月 FN')
            df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
        
        if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_month_pr_col = df.pop('Remarked by 上月 FN PR')
            df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)
        
        # 重新排列 PO 狀態欄位位置
        if 'PO狀態' in df.columns and '是否估計入帳' in df.columns:
            accrual_index = df.columns.get_loc('是否估計入帳') - 1
            po_status_col = df.pop('PO狀態')
            df.insert(accrual_index, 'PO狀態', po_status_col)
        
        # 重新排列 PR 欄位位置
        if 'Noted by Procurement' in df.columns:
            noted_index = df.columns.get_loc('Noted by Procurement') + 1
            
            for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                if col_name in df.columns:
                    col = df.pop(col_name)
                    df.insert(noted_index, col_name, col)
                    noted_index += 1
        
        # 把本期驗收數量/金額移到 累計至本期驗收數量/金額 前面
        if '本期驗收數量/金額' in df.columns and '累計至本期驗收數量/金額' in df.columns:
            memo_index = df.columns.get_loc('累計至本期驗收數量/金額')
            validation_col = df.pop('本期驗收數量/金額')
            df.insert(memo_index, '本期驗收數量/金額', validation_col)
        
        if 'Question from Reviewer' in df.columns and 'Check by AP' in df.columns:
            # Get all columns except the two you want to move
            cols = [col for col in df.columns if col not in ['Question from Reviewer', 'Check by AP']]
            # Add the two columns at the end
            cols = cols + ['Question from Reviewer', 'Check by AP']
            # Reorder the dataframe
            df = df[cols]
        
        if len([col for col in df.columns if col in ['question_from_reviewer', 'check_by_ap']]) == 2:
            cols = [col for col in df.columns if col not in ['question_from_reviewer', 'check_by_ap']]
            cols = cols + ['question_from_reviewer', 'check_by_ap']
            df = df[cols]

        return df
    
    def _add_classification(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加分類"""
        try:
            df['category_from_desc'] = df['Item Description'].apply(classify_description)
        except Exception as e:
            self.logger.warning(f"Failed to add classification: {str(e)}")
            df['category_from_desc'] = pd.NA
        
        return df
    
    def _add_keyword_matching(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加關鍵字匹配"""
        try:
            df = give_account_by_keyword(df, 'Item Description', export_keyword=True)
        except Exception as e:
            self.logger.warning(f"Failed to add keyword matching: {str(e)}")
        
        return df
    
    def _add_installment_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加分期標記"""
        if 'Item Description' not in df.columns:
            return df
        
        mask1 = df['Item Description'].str.contains('裝修', na=False)
        mask2 = df['Item Description'].str.contains('第[一|二|三]期款項', na=False)
        
        conditions = [
            (mask1 & mask2),  # Condition for 'Installment'
            (mask1)           # Condition for 'General'
        ]
        choices = ['分期', '一般']
        
        df['裝修一般/分期'] = np.select(conditions, choices, default=pd.NA)
        
        return df
    
    def _installment_over_ppe_limit(self, df: pd.DataFrame, 
                                    limit: int = int(config_manager.get('SPX', 'ppe_limit')),
                                    key_col: str = 'PO#') -> pd.DataFrame:
        mask = df['裝修一般/分期'].notna()
        target_po_number = df.loc[mask, key_col].unique()
        
        # 沒有裝修單的話，跳出
        if len(target_po_number) == 0:
            return df
        
        data = (df.loc[df[key_col].isin(target_po_number)].groupby(key_col)['Entry Amount'].sum())

        # 是否超出PPE額度
        is_under_limit = data.loc[lambda x: x < limit].empty
        if is_under_limit:
            po_number = data.loc[lambda x: x < limit].index
            df.loc[df[key_col].isin(po_number), '裝修一般/分期'] = (df.
                                                              loc[df[key_col].isin(po_number), '裝修一般/分期']
                                                              .astype(str) + f'_小於{limit}')
            return df
        else:
            return df
    
    def _rename_columns_dtype(self, df):
        df_copy = df.copy()
        df_copy = df_copy.rename(columns={'Product code': 'product_code_c'})
        return clean_po_data(df_copy)
    
    def _reformat_erm(self, df):
        df['expected_receive_month'] = (
            pd.to_datetime(df['expected_receive_month'], 
                           format='%b-%y',
                           errors='coerce')
            .dt.strftime('%Y/%m')
        )
        return df
    
    def _remove_columns(self, df):
        df_copy = df.copy()
        try:
            cols = config_manager._config_toml.get("spx").get("output_columns_before_nlp")
            return df_copy[cols]
        except KeyError as err:
            self.logger.warning(f"與預設輸出欄位不符: {err}")
            cols = config_manager._config_toml.get("spx").get("output_columns_before_nlp")
            renew_cols = [col for col in cols if col in df_copy.columns]
            return df_copy[renew_cols]
        except Exception as err:
            self.logger.error(f"返回原DataFrame: {err}")
            return df_copy
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for reformatting")
            return False
        
        return True
    

class PRDataReformattingStep(DataReformattingStep):
    """
    數據格式化和重組步驟
    
    功能:
    1. 格式化數值列
    2. 格式化日期列
    3. 清理 nan 值
    4. 重新排列欄位順序
    5. 添加分類和關鍵字匹配
    
    輸入: DataFrame
    輸出: Formatted DataFrame ready for export
    """
    
    def __init__(self, name: str = "DataReformatting", **kwargs):
        super().__init__(name, **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據格式化"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            self.logger.info("Reformatting data...")
            
            # 格式化數值列
            df = self._format_numeric_columns(df)
            
            # 格式化日期列
            df = self._reformat_dates(df)
            
            # 清理 nan 值
            df = self._clean_nan_values(df)
            
            # 重新排列欄位
            df = self._rearrange_columns(df)
            
            # 添加分類
            df = self._add_classification(df)
            
            # 添加關鍵字匹配
            df = self._add_keyword_matching(df)
            
            # 添加分期標記
            df = self._add_installment_flag(df)
            df = self._installment_over_ppe_limit(df, key_col='PR#')
            
            # 重新命名欄位名稱、資料型態
            df = self._rename_columns_dtype(df)

            # 確保review AP等欄位在最後
            df = self._rearrange_columns(df)
            
            # 將含有暫時性計算欄位的結果存為附件
            if isinstance(df, pd.DataFrame) and not df.empty:
                data_name = 'result_with_temp_cols'
                data_copy = df.copy()
                context.add_auxiliary_data(data_name, data_copy)
                self.logger.info(
                    f"Added auxiliary data: {data_name} {data_copy.shape} shape)"
                )

            # 移除臨時欄位
            df = self._remove_temp_columns(df)
            # reformat ERM
            df = self._reformat_erm(df)
            
            # 減少輸出欄位
            processing_type = context.metadata.processing_type
            entity = context.metadata.entity_type
            if entity == 'SPX' and processing_type == 'PR':
                df = self._remove_columns(df)

            context.update_data(df)
            
            self.logger.info("Data reformatting completed")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Data reformatted successfully",
                duration=duration,
                metadata={
                    'total_columns': len(df.columns),
                    'total_rows': len(df)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data reformatting failed: {str(e)}", exc_info=True)
            context.add_error(f"Data reformatting failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理 nan 值"""
        columns_to_clean = [
            '是否估計入帳', 'PR Product Code Check', 'PR狀態',
            'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
        ]
        
        for col in columns_to_clean:
            if col in df.columns:
                df[col] = df[col].replace('nan', pd.NA)
                df[col] = df[col].replace('<NA>', pd.NA)
        
        # 特殊處理 Accr. Amount
        if 'Accr. Amount' in df.columns:
            try:
                df['Accr. Amount'] = (
                    df['Accr. Amount'].astype('string').str.replace(',', '')
                    .replace('nan', '0')
                    .replace('<NA>', '0')
                    .astype('Float64')
                )
                df['Accr. Amount'] = df['Accr. Amount'].apply(lambda x: x if x != 0 else None)
            except Exception as e:
                self.logger.warning(f"Failed to clean Accr. Amount: {str(e)}")
        
        return df
    
    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        # 重新排列上月備註欄位位置
        if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
            fn_index = df.columns.get_loc('Remarked by FN') + 1
            last_month_col = df.pop('Remarked by 上月 FN')
            df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
        
        if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_month_pr_col = df.pop('Remarked by 上月 FN PR')
            df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)
        
        # 重新排列 PR 狀態欄位位置
        if 'PR狀態' in df.columns and '是否估計入帳' in df.columns:
            accrual_index = df.columns.get_loc('是否估計入帳') - 1
            po_status_col = df.pop('PR狀態')
            df.insert(accrual_index, 'PR狀態', po_status_col)
        
        # 重新排列 PR 欄位位置
        if 'Noted by Procurement' in df.columns:
            noted_index = df.columns.get_loc('Noted by Procurement') + 1
            
            for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                if col_name in df.columns:
                    col = df.pop(col_name)
                    df.insert(noted_index, col_name, col)
                    noted_index += 1
        
        if 'Question from Reviewer' in df.columns and 'Check by AP' in df.columns:
            # Get all columns except the two you want to move
            cols = [col for col in df.columns if col not in ['Question from Reviewer', 'Check by AP']]
            # Add the two columns at the end
            cols = cols + ['Question from Reviewer', 'Check by AP']
            # Reorder the dataframe
            df = df[cols]
        
        if len([col for col in df.columns if col in ['question_from_reviewer', 'check_by_ap']]) == 2:
            cols = [col for col in df.columns if col not in ['question_from_reviewer', 'check_by_ap']]
            cols = cols + ['question_from_reviewer', 'check_by_ap']
            df = df[cols]

        return df
    
    def _remove_temp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除臨時計算列"""
        temp_columns = ['檔案日期', 'Expected Received Month_轉換格式', 'YMs of Item Description',
                        'expected_received_month_轉換格式', 'yms_of_item_description',
                        'remarked_by_procurement_pr', 'noted_by_procurement_pr', 'remarked_by_上月_fn_pr',
                        'PR Product Code Check', 'pr_product_code_check',
                        ]
        
        for col in temp_columns:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        
        return df
    
    def _remove_columns(self, df):
        df_copy = df.copy()
        cols = config_manager._config_toml.get("spx").get("output_columns_before_nlp_pr")
        return df_copy[cols]

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for reformatting")
            return False
        
        return True
    

class PPEDataCleaningStep(PipelineStep):
    """
    PPE 數據清理與標準化步驟
    
    功能：
    1. 清理歸檔清單和續約清單
    2. 標準化欄位名稱
    3. 移除無效記錄
    """
    
    def __init__(self, name: str = "PPEDataCleaning", **kwargs):
        super().__init__(name, description="Clean and standardize PPE data", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據清理"""
        start_time = datetime.now()
        
        try:
            df_filing = context.get_auxiliary_data('filing_list')
            df_renewal = context.get_auxiliary_data('renewal_list')
            
            # 清理數據
            df_filing_clean = self._clean_dataframe(df_filing)
            df_renewal_clean = self._clean_dataframe(df_renewal)
            
            # 標準化數據
            df_filing_std = self._standardize_filing_data(df_filing_clean)
            df_renewal_std = self._standardize_renewal_data(df_renewal_clean)
            
            # 更新 context
            context.add_auxiliary_data('filing_list_clean', df_filing_std)
            context.add_auxiliary_data('renewal_list_clean', df_renewal_std)
            context.update_data(df_filing_std)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(len(df_filing), len(df_filing_std))
                        .set_time_info(start_time, datetime.now())
                        .add_custom('filing_removed', len(df_filing) - len(df_filing_std))
                        .add_custom('renewal_removed', len(df_renewal) - len(df_renewal_std))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_filing_std,
                message="數據清理完成",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"數據清理失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理 DataFrame"""
        return df.dropna()
    
    def _standardize_filing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """標準化歸檔清單數據"""
        # 實現標準化邏輯（複製自 SpxPpeProcessor）
        df_std = pd.DataFrame()
        
        # 處理店號
        df_std['sp_code'] = df.iloc[:, 0].astype(int)
        # 處理地址
        df_std['address'] = df.iloc[:, 1].astype(str)
        # 處理合約期間
        contract_periods = df.iloc[:, 2].apply(self._parse_contract_period)
        df_std['contract_start_day'] = [period[0] for period in contract_periods]
        df_std['contract_end_day'] = [period[1] for period in contract_periods]
        
        return df_std.dropna()
    
    def _standardize_renewal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        標準化續約清單資料
        
        Args:
            df: 原始DataFrame
            
        Returns:
            標準化後的DataFrame
        """
        # 預設欄位映射
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
    
    def _parse_contract_period(self, period_str: str):
        """解析合約期間"""
        # 實現解析邏輯（複製自 SpxPpeProcessor）
        try:
            if pd.isna(period_str):
                return None, None
            
            period_clean = str(period_str).replace('+', '').strip()
            
            for separator in [' - ', '-', '~', '至']:
                if separator in period_clean:
                    dates = period_clean.split(separator)
                    break
            else:
                return None, None
            
            if len(dates) < 2:
                return None, None
            
            start_date = pd.to_datetime(dates[0].strip()).strftime('%Y-%m-%d')
            end_date = pd.to_datetime(dates[-1].strip()).strftime('%Y-%m-%d')
            
            return start_date, end_date
        except Exception as e:
            self.logger.error(e)
            return None, None
        
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for cleaning")
            return False
        
        return True
    

class PPEDataMergeStep(PipelineStep):
    """
    PPE 數據合併步驟
    
    功能：
    1. 合併歸檔清單和續約清單
    2. 基於 address 或 sp_code 合併
    """
    
    def __init__(self, 
                 name: str = "PPEDataMerge",
                 merge_keys: list = None,
                 **kwargs):
        super().__init__(name, description="Merge PPE contract data", **kwargs)
        self.merge_keys = merge_keys or ['address']
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據合併"""
        start_time = datetime.now()
        
        try:
            df_filing = context.get_auxiliary_data('filing_list_clean')
            df_renewal = context.get_auxiliary_data('renewal_list_clean')
            
            # 合併數據
            df_merged = pd.merge(
                df_filing,
                df_renewal,
                on=self.merge_keys,
                how='outer',
                suffixes=('_filing', '_renewal')
            )
            
            # 移除重複
            df_merged = df_merged.drop_duplicates().reset_index(drop=True)
            
            context.update_data(df_merged)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(len(df_filing), len(df_merged))
                        .set_time_info(start_time, datetime.now())
                        .add_custom('merge_keys', self.merge_keys)
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_merged,
                message=f"成功合併數據: {len(df_merged)} 筆",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"數據合併失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
        
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for merging")
            return False
        
        return True
