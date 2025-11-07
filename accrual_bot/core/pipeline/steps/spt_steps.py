"""
SPT實體特定處理步驟
包含SPT特有的業務邏輯
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext
from .post_processing import BasePostProcessingStep
from accrual_bot.utils.helpers.data_utils import (
    classify_description,
    clean_po_data
)


class SPTStatusStep(PipelineStep):
    """
    SPT狀態判斷步驟
    實現SPT特定的狀態評估邏輯
    """
    
    def __init__(self, name: str = "SPTStatus", **kwargs):
        super().__init__(name, description="SPT-specific status evaluation", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行SPT狀態評估
        
        SPT狀態規則：
        1. 支援提早完成（特殊標記）
        2. 部分收貨狀態細分
        3. 跨月處理邏輯
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # 獲取處理日期
            processing_date = context.metadata.processing_date
            
            # 應用SPT狀態規則
            df[status_col] = df.apply(
                lambda row: self._evaluate_spt_status(row, processing_date),
                axis=1
            )
            
            # SPT特殊處理：提早完成標記
            if 'Early Completion' in df.columns:
                early_mask = df['Early Completion'] == 'Y'
                df.loc[early_mask, status_col] = '提早完成'
            
            # 跨月項目特殊處理
            if 'Item Description' in df.columns:
                cross_month_mask = df['Item Description'].str.contains(
                    '跨月|Cross Month', case=False, na=False
                )
                df.loc[cross_month_mask, '跨月標記'] = 'Y'
            
            context.update_data(df)
            
            # 統計
            status_counts = df[status_col].value_counts().to_dict()
            cross_month_count = df['跨月標記'].notna().sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPT status evaluation completed",
                metadata={
                    'status_counts': status_counts,
                    'cross_month_items': cross_month_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPT status evaluation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _evaluate_spt_status(self, row: pd.Series, processing_date: int) -> str:
        """
        評估單行SPT狀態
        
        Args:
            row: 數據行
            processing_date: 處理日期 (YYYYMM)
            
        Returns:
            str: 狀態值
        """
        # 獲取Expected Receive Month
        expected_month = row.get('Expected Receive Month', '')
        
        try:
            if pd.notna(expected_month) and expected_month != '':
                # 轉換為YYYYMM格式
                expected_yyyymm = int(expected_month.replace('-', ''))
                
                # 檢查是否已關單
                if 'Status' in row and pd.notna(row['Status']):
                    if 'Closed' in str(row['Status']):
                        return '已關單'
                    elif 'Cancelled' in str(row['Status']):
                        return '已取消'
                
                # 檢查收貨情況
                received_qty = row.get('Received Quantity', 0)
                entry_qty = row.get('Entry Quantity', 0)
                
                # SPT特殊：提早完成判斷
                if expected_yyyymm > processing_date:
                    if pd.notna(received_qty) and received_qty >= entry_qty:
                        return '提早完成'
                    else:
                        return '未到期'
                else:
                    # 到期或過期
                    if pd.notna(received_qty):
                        if received_qty >= entry_qty:
                            return '已完成'
                        elif received_qty > 0:
                            completion_rate = received_qty / entry_qty if entry_qty > 0 else 0
                            if completion_rate >= 0.8:
                                return '接近完成'
                            elif completion_rate >= 0.5:
                                return '部分收貨'
                            else:
                                return '少量收貨'
                    return '待收貨'
            else:
                return '資訊不足'
                
        except Exception:
            return '資訊不足'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.metadata.entity_type != "SPT":
            self.logger.error("This step is only for SPT entity")
            return False
        
        required_cols = ['Expected Receive Month']
        return all(col in context.data.columns for col in required_cols)


class SPTDepartmentStep(PipelineStep):
    """
    SPT部門處理步驟
    處理SPT特殊的部門代碼轉換邏輯
    """
    
    def __init__(self, name: str = "SPTDepartment", **kwargs):
        super().__init__(name, description="SPT department conversion", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行SPT部門轉換
        
        SPT部門規則：
        1. Account code 1,2,9開頭 -> 000
        2. Account code 5,4開頭 -> 000  
        3. 其他 -> Department前3碼
        4. 特殊部門映射
        """
        try:
            df = context.data.copy()
            
            # 初始化部門代碼
            df['Department_Code'] = df['Department'].astype(str).str[:3]
            
            # SPT特殊規則1：Account code開頭判斷
            if 'Account code' in df.columns:
                # 1,2,9開頭
                mask_129 = df['Account code'].astype(str).str[0].isin(['1', '2', '9'])
                df.loc[mask_129, 'Department_Code'] = '000'
                
                # 5,4開頭  
                mask_54 = df['Account code'].astype(str).str[0].isin(['5', '4'])
                df.loc[mask_54, 'Department_Code'] = '000'
            
            # SPT特殊部門映射
            special_mapping = {
                'SPT_OPS': '100',
                'SPT_WAREHOUSE': '200',
                'SPT_DELIVERY': '300',
                'SPT_CS': '400',
                'SPT_HQ': '000'
            }
            
            for key, value in special_mapping.items():
                mask = df['Department'].str.contains(key, case=False, na=False)
                df.loc[mask, 'Department_Code'] = value
            
            # 處理ShopeeOPS相關
            ops_mask = df['Department'].str.contains('ShopeeOPS', case=False, na=False)
            df.loc[ops_mask, 'Department_Code'] = '100'
            
            # 空值處理
            df['Department_Code'].fillna('999', inplace=True)
            
            # 驗證部門代碼格式
            invalid_dept = ~df['Department_Code'].str.match(r'^\d{3}$', na=False)
            df.loc[invalid_dept, 'Department_Code'] = '999'
            
            context.update_data(df)
            
            # 統計
            dept_counts = df['Department_Code'].value_counts().to_dict()
            special_count = (df['Department_Code'] == '000').sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPT department conversion completed",
                metadata={
                    'department_distribution': dept_counts,
                    'special_department_count': special_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPT department conversion failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'Department' in context.data.columns


class SPTAccrualStep(PipelineStep):
    """
    SPT預估入帳步驟
    處理SPT特定的預估邏輯
    """
    
    def __init__(self, name: str = "SPTAccrual", **kwargs):
        super().__init__(name, description="SPT accrual processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行SPT預估入帳處理
        
        SPT預估規則：
        1. 已完成、提早完成、接近完成 -> 預估
        2. 部分收貨根據比例判斷
        3. 跨月項目特殊處理
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # 初始化預估欄位
            df['是否估計入帳'] = 'N'
            
            # 基本規則：特定狀態預估
            accrual_statuses = ['已完成', '提早完成', '接近完成']
            accrual_mask = df[status_col].isin(accrual_statuses)
            df.loc[accrual_mask, '是否估計入帳'] = 'Y'
            
            # 部分收貨特殊處理（>80%完成度）
            if 'Received Quantity' in df.columns and 'Entry Quantity' in df.columns:
                partial_mask = df[status_col] == '部分收貨'
                completion_rate = df['Received Quantity'] / df['Entry Quantity'].replace(0, np.nan)
                high_completion = partial_mask & (completion_rate >= 0.8)
                df.loc[high_completion, '是否估計入帳'] = 'Y'
            
            # 跨月項目處理
            if '跨月標記' in df.columns:
                cross_month = df['跨月標記'] == 'Y'
                df.loc[cross_month, '是否估計入帳'] = 'Y'
                df.loc[cross_month, 'Accrual Note'] = '跨月項目-全額預估'
            
            # 檢查採購備註
            if '採購備註' in df.columns:
                no_accrual_mask = df['採購備註'].str.contains(
                    '不預估|不估計|勿估|暫緩', case=False, na=False
                )
                df.loc[no_accrual_mask, '是否估計入帳'] = 'N'
                df.loc[no_accrual_mask, 'Accrual Note'] = '採購備註-不預估'
            
            # FA科目標記
            config = context.get_entity_config()
            fa_accounts = config.get('fa_accounts', ['151101', '151201'])
            df['是否為FA'] = df['GL#'].isin(fa_accounts).map({True: 'Y', False: 'N'})
            
            # 設置Account code
            accrual_mask = df['是否估計入帳'] == 'Y'
            df.loc[accrual_mask, 'Account code'] = df.loc[accrual_mask, 'GL#']
            
            context.update_data(df)
            
            # 統計
            accrual_count = (df['是否估計入帳'] == 'Y').sum()
            fa_count = (df['是否為FA'] == 'Y').sum()
            cross_month_accrual = ((df['跨月標記'] == 'Y') & (df['是否估計入帳'] == 'Y')).sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPT accrual processing completed",
                metadata={
                    'accrual_items': accrual_count,
                    'fa_items': fa_count,
                    'cross_month_accruals': cross_month_accrual
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPT accrual processing failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = [context.get_status_column(), 'GL#']
        return all(col in context.data.columns for col in required_cols)


class SPTValidationStep(PipelineStep):
    """
    SPT驗證步驟
    執行SPT特定的業務規則驗證
    """
    
    def __init__(self, name: str = "SPTValidation", **kwargs):
        super().__init__(name, description="SPT-specific validation", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行SPT驗證
        
        SPT驗證規則：
        1. 部門代碼與Account code一致性
        2. 跨月項目日期範圍驗證
        3. 提早完成項目合理性檢查
        """
        try:
            df = context.data
            errors = []
            warnings = []
            
            # 驗證1：部門代碼一致性
            if 'Department_Code' in df.columns and 'Account code' in df.columns:
                # Account code 1,2,9開頭必須是部門000
                mask_129 = df['Account code'].astype(str).str[0].isin(['1', '2', '9'])
                invalid_dept = df[mask_129 & (df['Department_Code'] != '000')]
                if not invalid_dept.empty:
                    errors.append(
                        f"Found {len(invalid_dept)} items with Account code 1/2/9 "
                        f"but Department code not 000"
                    )
            
            # 驗證2：跨月項目驗證
            if '跨月標記' in df.columns:
                cross_month = df[df['跨月標記'] == 'Y']
                # 檢查是否有日期範圍
                if 'Date_Start' in df.columns and 'Date_End' in df.columns:
                    missing_dates = cross_month[
                        cross_month['Date_Start'].isna() | 
                        cross_month['Date_End'].isna()
                    ]
                    if not missing_dates.empty:
                        warnings.append(
                            f"Found {len(missing_dates)} cross-month items "
                            f"without complete date range"
                        )
            
            # 驗證3：提早完成合理性
            status_col = context.get_status_column()
            early_completion = df[df[status_col] == '提早完成']
            if not early_completion.empty:
                # 檢查是否真的有收貨
                if 'Received Quantity' in df.columns:
                    no_receipt = early_completion[
                        early_completion['Received Quantity'].isna() | 
                        (early_completion['Received Quantity'] == 0)
                    ]
                    if not no_receipt.empty:
                        warnings.append(
                            f"Found {len(no_receipt)} early completion items "
                            f"without actual receipt"
                        )
            
            # 驗證4：金額檢查
            if '是否估計入帳' in df.columns and 'Entry Amount' in df.columns:
                accrual_mask = df['是否估計入帳'] == 'Y'
                zero_amounts = df[accrual_mask & (df['Entry Amount'] == 0)]
                if not zero_amounts.empty:
                    warnings.append(
                        f"Found {len(zero_amounts)} accrual items with zero amount"
                    )
            
            # 記錄驗證結果
            for error in errors:
                context.add_error(f"[SPT Validation] {error}")
            for warning in warnings:
                context.add_warning(f"[SPT Validation] {warning}")
            
            if errors:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="SPT validation failed",
                    metadata={'errors': errors, 'warnings': warnings}
                )
            else:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SUCCESS,
                    message="SPT validation passed",
                    metadata={'warnings': warnings}
                )
                
        except Exception as e:
            self.logger.error(f"SPT validation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.metadata.entity_type == "SPT"


class SPTPostProcessingStep(BasePostProcessingStep):
    """SPT PO/PR 後處理步驟
    SPT PO/PR 數據格式化和重組步驟
    
    功能:
    1. 格式化數值列
    2. 格式化日期列
    3. 清理 nan 值
    4. 重新排列欄位順序
    5. 添加分類
    6. 重新命名欄位和資料型態
    7. 移除臨時欄位
    8. 格式化 ERM
    
    輸入: DataFrame
    輸出: Formatted DataFrame ready for export
    """
    
    def __init__(self, name: str = "SPT_Data_Reformatting", **kwargs):
        super().__init__(
            name, 
            description="SPT PO/PR data reformatting and reorganization",
            **kwargs
        )
    
    def _process_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> pd.DataFrame:
        """執行數據格式化"""
        
        self.logger.info("開始 SPT 數據格式化...")
        
        # 1. 格式化數值列
        df = self._format_numeric_columns(df)
        self._add_note("完成數值列格式化")
        
        # 2. 格式化日期列
        df = self._reformat_dates(df)
        self._add_note("完成日期列格式化")
        
        # 3. 清理 nan 值
        df = self._clean_nan_values(df)
        self._add_note("完成 nan 值清理")
        
        # 4. 重新排列欄位
        df = self._rearrange_columns(df)
        self._add_note("完成欄位重新排列")
        
        # 5. 添加分類
        df = self._add_classification(df)
        self._add_note("完成分類添加")
        
        # 6. 重新命名欄位名稱、資料型態
        df = self._rename_columns_dtype(df)
        self._add_note("完成欄位重命名和型態轉換")
        
        # 7. 確保 review AP 等欄位在最後
        df = self._rearrange_columns(df)
        self._add_note("完成最終欄位排列")
        
        # 8. 將含有暫時性計算欄位的結果存為附件
        self._save_temp_columns_data(df, context)
        self._add_note("完成暫時性數據保存")
        
        # 9. 移除臨時欄位
        df = self._remove_temp_columns(df)
        self._add_note("完成臨時欄位移除")
        
        # 10. 格式化 ERM
        df = self._reformat_erm(df)
        self._add_note("完成 ERM 格式化")
        
        self.logger.info("SPT 數據格式化完成")
        return df
    
    def _format_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化數值列
        
        將指定欄位轉換為適當的數值類型
        """
        # 整數列
        int_columns = ['Line#', 'GL#']
        for col in int_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        # 浮點數列
        float_columns = [
            'Unit Price', 'Entry Amount', 'Entry Invoiced Amount',
            'Entry Billed Amount', 'Entry Prepay Amount',
            'PO Entry full invoiced status', 'Accr. Amount'
        ]
        for col in float_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                except Exception as e:
                    self.logger.warning(f"Failed to format {col}: {str(e)}")
        
        return df
    
    def _reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化日期列
        
        將日期欄位統一轉換為 YYYY-MM-DD 格式
        """
        date_columns = ['Creation Date', 'Expected Received Month', 'Last Update Date']
        
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"Failed to format date column {col}: {str(e)}")
        
        return df
    
    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理 nan 值
        
        將字符串 'nan' 和 '<NA>' 替換為真正的 NA
        """
        status_column: str = [col for col in df.columns if '狀態' in col][0]
        columns_to_clean = [
            '是否估計入帳', 'PR Product Code Check', status_column,
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
        """
        重新排列欄位順序
        
        將重要欄位移到前面，review 欄位移到最後
        """
        # 重新排列上月備註欄位位置
        if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
            fn_index = df.columns.get_loc('Remarked by FN') + 1
            last_month_col = df.pop('Remarked by 上月 FN')
            df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
        
        if 'Remarked by 上月 FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
            fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_month_pr_col = df.pop('Remarked by 上月 FN PR')
            df.insert(fn_pr_index, 'Remarked by 上月 FN PR', last_month_pr_col)
        
        # 重新排列 PO/PR 狀態欄位位置(是否估計入帳前面)
        status_column: str = [col for col in df.columns if '狀態' in col][0]
        if status_column in df.columns and '是否估計入帳' in df.columns:
            accrual_index = df.columns.get_loc('是否估計入帳') - 1
            po_status_col = df.pop(status_column)
            df.insert(accrual_index, status_column, po_status_col)
        
        # 重新排列 PR 欄位位置
        if 'Noted by Procurement' in df.columns:
            noted_index = df.columns.get_loc('Noted by Procurement') + 1
            
            for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                if col_name in df.columns:
                    col = df.pop(col_name)
                    df.insert(noted_index, col_name, col)
                    noted_index += 1
        
        # 把 Question from Reviewer 和 Check by AP 移到最後
        if 'Question from Reviewer' in df.columns and 'Check by AP' in df.columns:
            cols = [col for col in df.columns if col not in ['Question from Reviewer', 'Check by AP']]
            cols = cols + ['Question from Reviewer', 'Check by AP']
            df = df[cols]
        
        if len([col for col in df.columns if col in ['question_from_reviewer', 'check_by_ap']]) == 2:
            cols = [col for col in df.columns if col not in ['question_from_reviewer', 'check_by_ap']]
            cols = cols + ['question_from_reviewer', 'check_by_ap']
            df = df[cols]
        
        return df
    
    def _add_classification(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加分類
        
        根據 Item Description 進行分類
        """
        try:
            df['category_from_desc'] = df['Item Description'].apply(classify_description)
        except Exception as e:
            self.logger.warning(f"Failed to add classification: {str(e)}")
            df['category_from_desc'] = pd.NA
        
        return df
    
    def _rename_columns_dtype(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重新命名欄位名稱、資料型態
        
        使用 clean_po_data 進行統一的欄位重命名和型態轉換
        """
        try:
            df = df.rename(columns={'Product code': 'product_code_c'})
            df = clean_po_data(df)
        except Exception as e:
            self.logger.warning(f"Failed to rename columns and convert dtypes: {str(e)}")
        
        return df
    
    def _save_temp_columns_data(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> None:
        """
        將含有暫時性計算欄位的結果存為附件
        
        在移除臨時欄位之前保存完整數據
        """
        if isinstance(df, pd.DataFrame) and not df.empty:
            data_name = 'result_with_temp_cols'
            data_copy = df.copy()
            context.add_auxiliary_data(data_name, data_copy)
            self.logger.info(
                f"Added auxiliary data: {data_name} (shape: {data_copy.shape})"
            )
    
    def _remove_temp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        移除臨時計算列
        
        刪除以 _ 開頭或特定名稱的臨時欄位
        """
        temp_columns = [
            '檔案日期', 
            'Expected Received Month_轉換格式', 
            'YMs of Item Description',
            'expected_received_month_轉換格式', 
            'yms_of_item_description',

            'PR Product Code Check',
            'pr_product_code_check',
        ]
        
        for col in temp_columns:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        return df
    
    def _reformat_erm(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化 ERM (Expected Receive Month)
        
        將 ERM 從 'MMM-YY' 格式轉換為 'YYYY/MM' 格式
        """
        try:
            if 'expected_receive_month' in df.columns:
                df['expected_receive_month'] = (
                    pd.to_datetime(
                        df['expected_receive_month'],
                        format='%b-%y',
                        errors='coerce'
                    )
                    .dt.strftime('%Y/%m')
                )
        except Exception as e:
            self.logger.warning(f"Failed to reformat ERM: {str(e)}")
        
        return df
    
    def _validate_result(
        self,
        df: pd.DataFrame,
        context: ProcessingContext
    ) -> Dict[str, Any]:
        """
        結果驗證
        
        檢查格式化後的數據是否符合要求
        """
        issues = []
        
        # 驗證 1: 檢查數值欄位
        numeric_cols = ['Entry Amount', 'Accr. Amount']
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype not in ['float64', 'Float64', 'int64', 'Int64']:
                    issues.append(f"欄位 '{col}' 的資料型態不是數值型: {df[col].dtype}")
        
        # 驗證 2: 檢查分類是否添加成功
        if 'category_from_desc' not in df.columns:
            issues.append("缺少 'category_from_desc' 欄位")
        
        # 驗證 3: 檢查臨時欄位是否已移除
        temp_cols_found = [col for col in df.columns if col in [
            '檔案日期', 
            'Expected Received Month_轉換格式',
            'expected_received_month_轉換格式'
        ]]
        if temp_cols_found:
            issues.append(f"臨時欄位未完全移除: {temp_cols_found}")
        
        if issues:
            return {
                'is_valid': False,
                'message': f"驗證發現 {len(issues)} 個問題",
                'details': {'issues': issues}
            }
        
        return {
            'is_valid': True,
            'message': '所有驗證通過',
            'details': {
                'total_rows': len(df),
                'total_columns': len(df.columns)
            }
        }
    
