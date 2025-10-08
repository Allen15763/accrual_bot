"""
邏輯判斷、數據計算與更新
"""
import time
import re
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder


class StatusStage1Step(PipelineStep):
    """
    第一階段狀態判斷步驟
    
    功能:
    根據關單清單給予初始狀態
    
    輸入: DataFrame + Closing list
    輸出: DataFrame with initial status
    """
    
    def __init__(self, name: str = "StatusStage1", **kwargs):
        super().__init__(name, description="Evaluate status stage 1", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行第一階段狀態判斷"""
        start_time = time.time()
        try:
            df = context.data.copy()
            df_spx_closing = context.get_auxiliary_data('closing_list')
            processing_date = context.metadata.processing_date
            
            self.logger.info("Evaluating status stage 1...")
            
            if df_spx_closing is None or df_spx_closing.empty:
                self.logger.warning("No closing list data, skipping status stage 1")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No closing list data"
                )
            
            # 給予第一階段狀態
            df = self._give_status_stage_1(df, 
                                           df_spx_closing, 
                                           processing_date,
                                           entity_type=context.metadata.entity_type)
            
            context.update_data(df)
            
            status_counts = df['PO狀態'].value_counts().to_dict() if 'PO狀態' in df.columns else {}
            
            self.logger.info("Status stage 1 evaluation completed")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Status stage 1 evaluated",
                duration=duration,
                metadata={'status_counts': status_counts}
            )
            
        except Exception as e:
            self.logger.error(f"Status stage 1 evaluation failed: {str(e)}", exc_info=True)
            context.add_error(f"Status stage 1 evaluation failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    def _give_status_stage_1(self, 
                             df: pd.DataFrame, 
                             df_spx_closing: pd.DataFrame, 
                             date, 
                             **kwargs) -> pd.DataFrame:
        #     # 這裡實現類似原始 give_status_stage_1 的邏輯
        #     # 根據關單清單標記已關單的 PO
        """給予第一階段狀態 - SPX特有邏輯
        
        Args:
            df: PO/PR DataFrame
            df_spx_closing: SPX關單數據DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        if 'entity_type' in kwargs:
            entity_type = kwargs.get('entity_type')
        else:
            entity_type = 'context transfer error'

        utility_suppliers = config_manager.get(entity_type, 'utility_suppliers')
        if 'PO狀態' in df.columns:
            tag_column = 'PO狀態'
            # 依據已關單條件取得對應的PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'po_no'].unique() if c1.any() else []
            closed = df_spx_closing.loc[c2, 'po_no'].unique() if c2.any() else []
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                (df['Remarked by 上月 FN'].str.contains('刪|關', na=False)) | 
                (df['Remarked by 上月 FN PR'].astype('string').str.contains('刪|關', na=False))
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN PR'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            cond1 = \
                df['Item Description'].str.contains(config_manager.get(entity_type, 'deposit_keywords'), 
                                                    na=False)
            is_fa = df['GL#'].astype('string') == config_manager.get('FA_ACCOUNTS', entity_type, '199999')
            cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
            df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                config_manager.get(entity_type, 'deposit_keywords_label')
            
            # 條件2：供應商與類別對應，做GL調整
            bao_supplier: list = config_manager.get_list(entity_type, 'bao_supplier')
            bao_categories: list = config_manager.get_list(entity_type, 'bao_categories')
            cond2 = (df['PO Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
            df.loc[cond2, tag_column] = 'GL調整'
            
            # 條件3：該PO#在待關單清單中
            cond3 = df['PO#'].astype('string').isin([str(x) for x in to_be_close])
            df.loc[cond3, tag_column] = '待關單'
            
            # 條件4：該PO#在已關單清單中
            cond4 = df['PO#'].astype('string').isin([str(x) for x in closed])
            df.loc[cond4, tag_column] = '已關單'
            
            # 條件5：上月FN備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, tag_column] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態(xxxxxx入FA)
            # 部分完成xxxxxx入FA不計入，前期FN備註如果是部分完成的會掉到erm邏輯判斷
            cond6 = (
                (df['Remarked by 上月 FN'].str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN'].str.contains('部分完成', na=False))
            )
            if cond6.any():
                extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, tag_column] = extracted_fn
            
            # 條件7：若「Remarked by 上月 FN PR」含有「入FA」，則提取該數字，並更新狀態
            cond7 = (
                (df['Remarked by 上月 FN PR'].astype('string').str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN PR'].astype('string').str.contains('部分完成', na=False))
            )
            if cond7.any():
                extracted_pr = self.extract_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                df.loc[cond7, tag_column] = extracted_pr

            # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
            cond8 = df['PO Supplier'].fillna('system_filled').str.contains(utility_suppliers)
            df.loc[cond8, tag_column] = '授扣GL調整'

            # 費用類按申請人篩選
            is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
            ops_rent: str = config_manager.get(entity_type, 'ops_for_rent')
            account_rent: str = config_manager.get(entity_type, 'account_rent')
            ops_intermediary: str = config_manager.get(entity_type, 'ops_for_intermediary')
            ops_other: str = config_manager.get(entity_type, 'ops_for_other')
            
            mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
            mask_account_rent = df['GL#'] == account_rent
            mask_ops_rent = df['PR Requester'] == ops_rent
            mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype('Int64') == date
            mask_desc_contains_intermediary = df['Item Description'].fillna('na').str.contains('(?i)intermediary')
            mask_ops_intermediary = df['PR Requester'] == ops_intermediary

            combined_cond = is_non_labeled & mask_erm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            combined_cond = is_non_labeled & mask_descerm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            # 租金已入帳
            booked_in_ap = (~df['GL DATE'].isna()) & ((df['GL DATE'] != '') | (df['GL DATE'] != 'nan'))
            df.loc[(df[tag_column] == '已完成_租金') & (booked_in_ap), tag_column] = '已入帳'

            uncompleted_rent = (
                ((df['Remarked by Procurement'] != 'error') &
                    is_non_labeled &
                    mask_ops_rent &
                    mask_account_rent &
                    (df['Item Description'].str.contains('(?i)租金', na=False))
                 ) &
                
                (
                    ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     ) |
                    ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     )
                )
                    

            )
            df.loc[uncompleted_rent, tag_column] = '未完成_租金'

            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                ((df['Expected Received Month_轉換格式'] == date) |
                    ((df['Expected Received Month_轉換格式'] < date) & (df['Remarked by 上月 FN'].str.contains('已完成')))
                 )
            df.loc[combined_cond, tag_column] = '已完成_intermediary'
            
            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                (df['Expected Received Month_轉換格式'] > date)
            df.loc[combined_cond, tag_column] = '未完成_intermediary'

            # 要判斷OPS驗收數
            kiosk_suppliers: list = config_manager.get_list(entity_type, 'kiosk_suppliers')
            locker_suppliers: list = config_manager.get_list(entity_type, 'locker_suppliers')
            asset_suppliers: list = kiosk_suppliers + locker_suppliers

            # Exclude both general '入FA' but Include specific patterns(部分入)
            po_general_fa = df['Remarked by 上月 FN'].str.contains('入FA', na=False)
            po_specific_pattern = df['Remarked by 上月 FN'].str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True)

            pr_general_fa = df['Remarked by 上月 FN PR'].astype('string').str.contains('入FA', na=False)
            pr_specific_pattern = (df['Remarked by 上月 FN PR']
                                   .astype('string').str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True))

            doesnt_contain_fa = (~pr_general_fa & ~po_general_fa)
            specific_pattern = (pr_specific_pattern | po_specific_pattern)
            ignore_closed = ~df[tag_column].str.contains('關', na=False)
            mask = ((df['PO Supplier'].isin(asset_suppliers)) & 
                    (doesnt_contain_fa | specific_pattern) & 
                    (ignore_closed))
            df.loc[mask, tag_column] = 'Pending_validating'
            
            self.logger.info("成功給予第一階段狀態")
            return df
        else:
            tag_column = 'PR狀態'
            # 依據已關單條件取得對應的PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'new_pr_no'].unique() if c1.any() else []
            closed = df_spx_closing.loc[c2, 'new_pr_no'].unique() if c2.any() else []
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                df['Remarked by 上月 FN'].astype('string').str.contains('刪|關', na=False)
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            cond1 = \
                df['Item Description'].str.contains(config_manager.get(entity_type, 'deposit_keywords'), 
                                                    na=False)
            is_fa = df['GL#'].astype('string') == config_manager.get('FA_ACCOUNTS', entity_type, '199999')
            cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
            df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                config_manager.get(entity_type, 'deposit_keywords_label')
            
            # 條件2：供應商與類別對應，做GL調整
            bao_supplier: list = config_manager.get_list(entity_type, 'bao_supplier')
            bao_categories: list = config_manager.get_list(entity_type, 'bao_categories')
            cond2 = (df['PR Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
            df.loc[cond2, tag_column] = 'GL調整'
            
            # 條件3：該PR#在待關單清單中
            cond3 = df['PR#'].astype('string').isin([str(x) for x in to_be_close])
            df.loc[cond3, tag_column] = '待關單'
            
            # 條件4：該PR#在已關單清單中
            cond4 = df['PR#'].astype('string').isin([str(x) for x in closed])
            df.loc[cond4, tag_column] = '已關單'
            
            # 條件5：上月FN備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, tag_column] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態(xxxxxx入FA)
            # 部分完成xxxxxx入FA不計入，前期FN備註如果是部分完成的會掉到erm邏輯判斷
            cond6 = (
                (df['Remarked by 上月 FN'].astype('string').str.contains('入FA', na=False)) & 
                (~df['Remarked by 上月 FN'].astype('string').str.contains('部分完成', na=False))
            )
            if cond6.any():
                extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, tag_column] = extracted_fn
            
            # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
            cond8 = df['PR Supplier'].fillna('system_filled').str.contains(utility_suppliers)
            df.loc[cond8, tag_column] = '授扣GL調整'

            # 費用類按申請人篩選
            is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
            ops_rent: str = config_manager.get(entity_type, 'ops_for_rent')
            account_rent: str = config_manager.get(entity_type, 'account_rent')
            ops_intermediary: str = config_manager.get(entity_type, 'ops_for_intermediary')
            ops_other: str = config_manager.get(entity_type, 'ops_for_other')
            
            mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
            mask_account_rent = df['GL#'] == account_rent
            mask_ops_rent = df['Requester'] == ops_rent
            mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype('Int64') == date
            mask_desc_contains_intermediary = df['Item Description'].fillna('na').str.contains('(?i)intermediary')
            mask_ops_intermediary = df['Requester'] == ops_intermediary

            combined_cond = is_non_labeled & mask_erm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            combined_cond = is_non_labeled & mask_descerm_equals_current & mask_account_rent & mask_ops_rent
            df.loc[combined_cond, tag_column] = '已完成_租金'

            uncompleted_rent = (
                ((df['Remarked by Procurement'] != 'error') &
                    is_non_labeled &
                    mask_ops_rent &
                    mask_account_rent &
                    (df['Item Description'].str.contains('(?i)租金', na=False))
                 ) &
                
                (
                    ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     ) |
                    ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('Int32')) &
                        (df['Expected Received Month_轉換格式'] > date) &
                        (df['YMs of Item Description'] != '100001,100002')
                     )
                )

            )
            df.loc[uncompleted_rent, tag_column] = '未完成_租金'

            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                ((df['Expected Received Month_轉換格式'] == date) |
                    ((df['Expected Received Month_轉換格式'] < date) & (df['Remarked by 上月 FN']
                                                                    .astype('string').str.contains('已完成')))
                 )
            df.loc[combined_cond, tag_column] = '已完成_intermediary'
            
            combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                (df['Expected Received Month_轉換格式'] > date)
            df.loc[combined_cond, tag_column] = '未完成_intermediary'

            # PR的智取櫃與繳費機，不會在PR驗收不估
            kiosk_suppliers: list = config_manager.get_list(entity_type, 'kiosk_suppliers')
            locker_suppliers: list = config_manager.get_list(entity_type, 'locker_suppliers')
            asset_suppliers: list = kiosk_suppliers + locker_suppliers
            ignore_closed = ~df[tag_column].str.contains('關', na=False)
            mask = ((df['PR Supplier'].isin(asset_suppliers)) & 
                    (ignore_closed))
            df.loc[mask, tag_column] = '智取櫃與繳費機'

            self.logger.info("成功給予第一階段狀態")
            # return df
        
        if 'PO#' in df_spx_closing.columns and 'PO#' in df.columns:
            closed_po_list = df_spx_closing['PO#'].unique().tolist()
            
            # 標記已關單的 PO
            df.loc[df['PO#'].isin(closed_po_list), 'Closing_Status'] = 'Closed'
        
        return df
    
    def is_closed_spx(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """判斷SPX關單狀態
        
        Args:
            df: 關單數據DataFrame
            
        Returns:
            Tuple[pd.Series, pd.Series]: (待關單條件, 已關單條件)
        """
        # [0]有新的PR編號，但FN未上系統關單的
        condition_to_be_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (df['done_by_fn'].isna())
        )
        
        # [1]有新的PR編號，但FN已經上系統關單的
        condition_closed = (
            (~df['new_pr_no'].isna()) & 
            (df['new_pr_no'] != '') & 
            (~df['done_by_fn'].isna())
        )
        
        return condition_to_be_closed, condition_closed
    
    def convert_date_format_in_remark(self, series: pd.Series) -> pd.Series:
        """轉換備註中的日期格式 (YYYY/MM -> YYYYMM)
        
        Args:
            series: 包含日期的Series
            
        Returns:
            pd.Series: 轉換後的Series
        """
        try:
            return series.astype('string').str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
        except Exception as e:
            self.logger.error(f"轉換日期格式時出錯: {str(e)}", exc_info=True)
            return series
        
    def extract_fa_remark(self, series: pd.Series) -> pd.Series:
        """提取FA備註中的日期
        
        Args:
            series: 包含FA備註的Series
            
        Returns:
            pd.Series: 提取的日期Series
        """
        try:
            return series.astype('string').str.extract(r'(\d{6}入FA)', expand=False)
        except Exception as e:
            self.logger.error(f"提取FA備註時出錯: {str(e)}", exc_info=True)
            return series
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for status stage 1")
            return False
        
        return True


@dataclass
class ERMConditions:
    """ERM 判斷條件集合 - 提高可讀性"""
    # 基礎條件組件
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series
    quantity_matched: pd.Series
    not_billed: pd.Series
    has_billing: pd.Series
    fully_billed: pd.Series
    has_unpaid_amount: pd.Series
    
    # 備註條件
    procurement_completed_or_rent: pd.Series
    fn_completed_or_posted: pd.Series
    pr_not_incomplete: pd.Series
    
    # FA 條件
    is_fa: pd.Series
    
    # 錯誤條件
    procurement_not_error: pd.Series
    out_of_date_range: pd.Series
    format_error: pd.Series


class SPXERMLogicStep(PipelineStep):
    """
    SPX ERM 邏輯步驟 - 完整實現版本
    
    功能：
    1. 設置檔案日期
    2. 判斷 11 種 PO 狀態（已入帳、已完成、Check收貨等）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（Account code, Product code, Dep.等）
    5. 計算預估金額（Accr. Amount）
    6. 處理預付款和負債科目
    7. 檢查 PR Product Code
    
    業務規則：
    - SPX 邏輯：「已完成」狀態的項目需要估列入帳
    - 其他狀態一律不估列（是否估計入帳 = N）
    
    輸入：
    - DataFrame with required columns
    - Reference data (科目映射、負債科目)
    - Processing date
    
    輸出：
    - DataFrame with PO狀態, 是否估計入帳, and accounting fields
    """
    
    def __init__(self, name: str = "SPX_ERM_Logic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SPX ERM logic with 11 status conditions",
            **kwargs
        )
        
        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('SPX', 'fa_accounts', ['199999'])
        self.dept_accounts = config_manager.get_list('SPX', 'dept_accounts', [])
        
        self.logger.info(f"Initialized {name} with FA accounts: {self.fa_accounts}")
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 ERM 邏輯"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.get_variable('processing_date')
            
            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            ref_liability = context.get_auxiliary_data('reference_liability')
            
            if ref_account is None or ref_liability is None:
                raise ValueError("缺少參考數據：科目映射或負債科目")
            
            self.logger.info(f"開始 ERM 邏輯處理，處理日期：{processing_date}")
            
            # ========== 階段 1: 設置基本欄位 ==========
            df = self._set_file_date(df, processing_date)
            
            # ========== 階段 2: 構建判斷條件 ==========
            conditions = self._build_conditions(df, processing_date)
            
            # ========== 階段 3: 應用 11 個狀態條件 ==========
            df = self._apply_status_conditions(df, conditions)
            
            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions)
            
            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df)
            
            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_accounting_fields(df, ref_account, ref_liability)
            
            # ========== 階段 7: 檢查 PR Product Code ==========
            df = self._check_pr_product_code(df)
            
            # 更新上下文
            context.update_data(df)
            
            # 生成統計資訊
            stats = self._generate_statistics(df)
            
            self.logger.info(
                f"ERM 邏輯完成 - "
                f"需估列: {stats['accrual_count']} 筆, "
                f"總計: {stats['total_count']} 筆"
            )
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"ERM 邏輯已應用，{stats['accrual_count']} 筆需估列",
                duration=duration,
                metadata=stats
            )
            
        except Exception as e:
            self.logger.error(f"ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"ERM 邏輯失敗: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )
    
    # ========== 階段 1: 基本設置 ==========
    
    def _set_file_date(self, df: pd.DataFrame, processing_date: int) -> pd.DataFrame:
        """設置檔案日期"""
        df['檔案日期'] = processing_date
        self.logger.debug(f"已設置檔案日期：{processing_date}")
        return df
    
    # ========== 階段 2: 構建條件 ==========
    
    def _build_conditions(self, df: pd.DataFrame, file_date: int) -> ERMConditions:
        """
        構建所有判斷條件
        
        將條件邏輯集中在此處，提高可讀性和維護性
        """
        # 基礎狀態條件
        no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # 日期範圍條件
        ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
        ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
        erm = df['Expected Received Month_轉換格式']
        
        in_date_range = erm.between(ym_start, ym_end, inclusive='both')
        erm_before_or_equal_file_date = erm <= file_date
        erm_after_file_date = erm > file_date
        
        # 數量條件
        quantity_matched = df['Entry Quantity'] == df['Received Quantity']
        
        # 帳務條件
        not_billed = df['Entry Billed Amount'].astype('Float64') == 0
        has_billing = df['Billed Quantity'] != '0'
        fully_billed = (
            df['Entry Amount'].astype('Float64') - 
            df['Entry Billed Amount'].astype('Float64')
        ) == 0
        has_unpaid_amount = (
            df['Entry Amount'].astype('Float64') - 
            df['Entry Billed Amount'].astype('Float64')
        ) != 0
        
        # 備註條件
        procurement_completed_or_rent = df['Remarked by Procurement'].str.contains(
            '(?i)已完成|rent', na=False
        )
        fn_completed_or_posted = df['Remarked by 上月 FN'].str.contains(
            '(?i)已完成|已入帳', na=False
        )
        pr_not_incomplete = ~df['Remarked by 上月 FN PR'].str.contains(
            '(?i)未完成', na=False
        )
        
        # FA 條件
        is_fa = df['GL#'].astype('string').isin([str(x) for x in self.fa_accounts])
        
        # 錯誤條件
        procurement_not_error = df['Remarked by Procurement'] != 'error'
        out_of_date_range = (
            (in_date_range == False) & 
            (df['YMs of Item Description'] != '100001,100002')
        )
        format_error = df['YMs of Item Description'] == '100001,100002'
        
        return ERMConditions(
            no_status=no_status,
            in_date_range=in_date_range,
            erm_before_or_equal_file_date=erm_before_or_equal_file_date,
            erm_after_file_date=erm_after_file_date,
            quantity_matched=quantity_matched,
            not_billed=not_billed,
            has_billing=has_billing,
            fully_billed=fully_billed,
            has_unpaid_amount=has_unpaid_amount,
            procurement_completed_or_rent=procurement_completed_or_rent,
            fn_completed_or_posted=fn_completed_or_posted,
            pr_not_incomplete=pr_not_incomplete,
            is_fa=is_fa,
            procurement_not_error=procurement_not_error,
            out_of_date_range=out_of_date_range,
            format_error=format_error
        )
    
    # ========== 階段 3: 應用狀態條件 ==========
    
    def _apply_status_conditions(self, df: pd.DataFrame, 
                                 cond: ERMConditions) -> pd.DataFrame:
        """
        應用 11 個狀態判斷條件
        
        條件優先順序從上到下，符合的條件會被優先設置
        """
        
        # === 條件 1: 已入帳（前期FN明確標註）===
        condition_1 = df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False)
        df.loc[condition_1, 'PO狀態'] = '已入帳'
        self._log_condition_result("已入帳（前期FN明確標註）", condition_1.sum())
        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 2: 已入帳（有 GL DATE 且符合其他條件）===
        condition_2 = (
            (~df['GL DATE'].isna()) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            cond.has_billing &
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            (~cond.is_fa)
        )
        df.loc[condition_2, 'PO狀態'] = '已入帳'
        self._log_condition_result("已入帳（GL DATE）", condition_2.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 3: 已完成 ===
        condition_3 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.pr_not_incomplete &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            cond.not_billed
        )
        df.loc[condition_3, 'PO狀態'] = '已完成'
        self._log_condition_result("已完成", condition_3.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 4: 全付完，未關單 ===
        condition_4 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.fully_billed
        )
        df.loc[condition_4, 'PO狀態'] = '全付完，未關單?'
        self._log_condition_result("全付完，未關單", condition_4.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 5: 已完成但有未付款部分 ===
        condition_5 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.has_unpaid_amount
        )
        df.loc[condition_5, 'PO狀態'] = '已完成'
        self._log_condition_result("已完成（有未付款）", condition_5.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 6: Check收貨 ===
        condition_6 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            (~cond.quantity_matched)
        )
        df.loc[condition_6, 'PO狀態'] = 'Check收貨'
        self._log_condition_result("Check收貨", condition_6.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 7: 未完成 ===
        condition_7 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_after_file_date
        )
        df.loc[condition_7, 'PO狀態'] = '未完成'
        self._log_condition_result("未完成", condition_7.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 8: 範圍錯誤_租金 ===
        condition_8 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)租金', na=False))
        )
        df.loc[condition_8, 'PO狀態'] = 'error(Description Period is out of ERM)_租金'
        self._log_condition_result("範圍錯誤_租金", condition_8.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 9: 範圍錯誤_薪資 ===
        condition_9 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
        )
        df.loc[condition_9, 'PO狀態'] = 'error(Description Period is out of ERM)_薪資'
        self._log_condition_result("範圍錯誤_薪資", condition_9.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 10: 範圍錯誤（一般）===
        condition_10 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range
        )
        df.loc[condition_10, 'PO狀態'] = 'error(Description Period is out of ERM)'
        self._log_condition_result("範圍錯誤（一般）", condition_10.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')
        
        # === 條件 11: 部分完成ERM ===
        condition_11 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Received Quantity'].astype('Float64') != 0) &
            (~cond.quantity_matched)
        )
        df.loc[condition_11, 'PO狀態'] = '部分完成ERM'
        self._log_condition_result("部分完成ERM", condition_11.sum())
        
        return df
    
    def _log_condition_result(self, condition_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"條件 [{condition_name}]: {count} 筆符合")
    
    # ========== 階段 4: 處理格式錯誤 ==========
    
    def _handle_format_errors(self, df: pd.DataFrame, 
                              cond: ERMConditions) -> pd.DataFrame:
        """處理格式錯誤的記錄"""
        mask_format_error = cond.no_status & cond.format_error
        df.loc[mask_format_error, 'PO狀態'] = '格式錯誤，退單'
        
        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"發現 {error_count} 筆格式錯誤")
        
        return df
    
    # ========== 階段 5: 設置是否估計入帳 ==========
    
    def _set_accrual_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根據 PO狀態 設置是否估計入帳
        
        SPX 邏輯：只有「已完成」狀態需要估列入帳
        """
        mask_completed = df['PO狀態'].str.contains('已完成', na=False)
        
        df.loc[mask_completed, '是否估計入帳'] = 'Y'
        df.loc[~mask_completed, '是否估計入帳'] = 'N'
        
        accrual_count = mask_completed.sum()
        self.logger.info(f"設置估列標記：{accrual_count} 筆需估列")
        
        return df
    
    # ========== 階段 6: 設置會計欄位 ==========
    
    def _set_accounting_fields(self, df: pd.DataFrame,
                               ref_account: pd.DataFrame,
                               ref_liability: pd.DataFrame) -> pd.DataFrame:
        """設置所有會計相關欄位"""
        
        need_accrual = df['是否估計入帳'] == 'Y'
        
        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']
        
        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account, need_accrual)
        
        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']
        
        # 4. Region_c（SPX 固定值）
        df.loc[need_accrual, 'Region_c'] = "TW"
        
        # 5. Dep.（部門代碼）
        df = self._set_department(df, need_accrual)
        
        # 6. Currency_c
        df.loc[need_accrual, 'Currency_c'] = df.loc[need_accrual, 'Currency']
        
        # 7. Accr. Amount（預估金額）
        df = self._calculate_accrual_amount(df, need_accrual)
        
        # 8. 預付款處理
        df = self._handle_prepayment(df, need_accrual, ref_liability)
        
        self.logger.info("會計欄位設置完成")
        
        return df
    
    def _set_account_name(self, df: pd.DataFrame, ref_account: pd.DataFrame,
                          mask: pd.Series) -> pd.DataFrame:
        """設置會計科目名稱"""
        if ref_account.empty:
            self.logger.warning("參考科目資料為空")
            return df
        
        # 使用 merge 從參考資料取得科目名稱
        merged = pd.merge(
            df, 
            ref_account[['Account', 'Account Desc']],
            how='left',
            left_on='Account code',
            right_on='Account'
        )
        
        df['Account Name'] = merged['Account Desc']
        
        return df
    
    def _set_department(self, df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
        """
        設置部門代碼
        
        規則：
        - 如果科目在 dept_accounts 清單中，取 Department 前3碼
        - 否則設為 '000'
        """
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )
        
        # 在 dept_accounts 中的科目
        df.loc[mask & isin_dept, 'Dep.'] = \
            df.loc[mask & isin_dept, 'Department'].str[:3]
        
        # 不在 dept_accounts 中的科目
        df.loc[mask & ~isin_dept, 'Dep.'] = '000'
        
        return df
    
    def _calculate_accrual_amount(self, df: pd.DataFrame, 
                                  mask: pd.Series) -> pd.DataFrame:
        """
        計算預估金額
        
        公式：Unit Price × (Entry Quantity - Billed Quantity)
        """
        df['temp_amount'] = (
            df['Unit Price'].astype('Float64') * 
            (df['Entry Quantity'].astype('Float64') - 
             df['Billed Quantity'].astype('Float64'))
        )
        
        df.loc[mask, 'Accr. Amount'] = df.loc[mask, 'temp_amount']
        df.drop('temp_amount', axis=1, inplace=True)
        
        return df
    
    def _handle_prepayment(self, df: pd.DataFrame, mask: pd.Series,
                           ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        處理預付款和負債科目
        
        規則：
        - 有預付款：是否有預付 = 'Y'，Liability = '111112'
        - 無預付款：從參考資料查找 Liability
        """
        is_prepayment = df['Entry Prepay Amount'] != '0'
        df.loc[mask & is_prepayment, '是否有預付'] = 'Y'
        
        # 設置 Liability（無預付款的情況）
        if not ref_liability.empty:
            merged = pd.merge(
                df,
                ref_liability[['Account', 'Liability']],
                how='left',
                left_on='Account code',
                right_on='Account'
            )
            df['Liability'] = merged['Liability_y']
        
        # 有預付款的情況，覆蓋為 '111112'
        df.loc[mask & is_prepayment, 'Liability'] = '111112'
        
        return df
    
    # ========== 階段 7: PR Product Code 檢查 ==========
    
    def _check_pr_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        檢查 PR 的 Product Code 是否與 Project 一致
        
        規則：
        從 Project 欄位提取第一個詞，與 Product code 比對
        - 一致：good
        - 不一致：bad
        """
        if 'Product code' not in df.columns or 'Project' not in df.columns:
            self.logger.warning("缺少 Product code 或 Project 欄位，跳過檢查")
            return df
        
        mask = df['Product code'].notnull()
        
        try:
            # 提取 Project 的第一個詞
            project_first_word = df.loc[mask, 'Project'].str.findall(
                r'^(\w+(?:))'
            ).apply(lambda x: x[0] if len(x) > 0 else '')
            
            # 比對
            product_match = (project_first_word == df.loc[mask, 'Product code'])
            
            df.loc[mask, 'PR Product Code Check'] = np.where(
                product_match, 'good', 'bad'
            )
            
            bad_count = (~product_match).sum()
            if bad_count > 0:
                self.logger.warning(f"發現 {bad_count} 筆 PR Product Code 不一致")
                
        except Exception as e:
            self.logger.error(f"PR Product Code 檢查失敗: {str(e)}")
        
        return df
    
    # ========== 輔助方法 ==========
    
    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """生成統計資訊"""
        stats = {
            'total_count': len(df),
            'accrual_count': (df['是否估計入帳'] == 'Y').sum(),
            'status_distribution': {}
        }
        
        if 'PO狀態' in df.columns:
            status_counts = df['PO狀態'].value_counts().to_dict()
            stats['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }
        
        return stats
    
    # ========== 驗證方法 ==========
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入數據"""
        df = context.data
        
        if df is None or df.empty:
            self.logger.error("輸入數據為空")
            context.add_error("輸入數據為空")
            return False
        
        # 檢查必要欄位
        required_columns = [
            'GL#', 'Expected Received Month_轉換格式',
            'YMs of Item Description', 'Entry Quantity',
            'Received Quantity', 'Billed Quantity',
            'Entry Amount', 'Entry Billed Amount',
            'Item Description', 'Remarked by Procurement',
            'Remarked by 上月 FN', 'Unit Price', 'Currency',
            'Product Code'
        ]
        
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            self.logger.error(f"缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False
        
        # 檢查參考數據
        ref_account = context.get_auxiliary_data('reference_account')
        ref_liability = context.get_auxiliary_data('reference_liability')
        
        if ref_account is None or ref_liability is None:
            self.logger.error("缺少參考數據")
            context.add_error("缺少參考數據")
            return False
        
        # 檢查處理日期
        processing_date = context.get_variable('processing_date')
        if processing_date is None:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False
        
        self.logger.info("輸入驗證通過")
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾 ERM 邏輯：{str(error)}")
        # SPX ERM 步驟通常不需要特殊回滾操作


class PPEContractDateUpdateStep(PipelineStep):
    """
    PPE 合約日期更新步驟
    
    功能：
    統一同一店號（sp_code）的合約起止日期
    """
    
    def __init__(self, name: str = "PPEContractDateUpdate", **kwargs):
        super().__init__(name, description="Update contract dates", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期更新"""
        start_time = datetime.now()
        
        try:
            df = context.data.copy()
            
            # 更新合約日期
            df_updated = self._update_contract_dates(df)
            
            context.update_data(df_updated)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_updated,
                message="合約日期更新完成",
                duration=duration
            )
            
        except Exception as e:
            self.logger.error(f"日期更新失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _update_contract_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """更新合約日期（複製自 SpxPpeProcessor）"""
        df_updated = df.copy()
        
        # 轉換日期格式
        date_columns = [
            'contract_start_day_filing', 
            'contract_end_day_filing',
            'contract_start_day_renewal', 
            'contract_end_day_renewal'
        ]
        
        for col in date_columns:
            if col in df_updated.columns:
                df_updated[col] = pd.to_datetime(df_updated[col], errors='coerce')
        
        # 按 sp_code 分組更新
        for sp_code in df_updated['sp_code'].unique():
            mask = df_updated['sp_code'] == sp_code
            sp_data = df_updated[mask]
            
            # 收集所有日期
            start_dates = []
            end_dates = []
            
            for col in ['contract_start_day_filing', 'contract_start_day_renewal']:
                if col in df_updated.columns:
                    dates = sp_data[col].dropna().tolist()
                    start_dates.extend(dates)
            
            for col in ['contract_end_day_filing', 'contract_end_day_renewal']:
                if col in df_updated.columns:
                    dates = sp_data[col].dropna().tolist()
                    end_dates.extend(dates)
            
            # 更新為最小起始日和最大結束日
            if start_dates:
                min_start = min(start_dates)
                for col in ['contract_start_day_filing', 'contract_start_day_renewal']:
                    if col in df_updated.columns:
                        df_updated.loc[mask, col] = min_start
            
            if end_dates:
                max_end = max(end_dates)
                for col in ['contract_end_day_filing', 'contract_end_day_renewal']:
                    if col in df_updated.columns:
                        df_updated.loc[mask, col] = max_end
        
        return df_updated.drop_duplicates()

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for update")
            return False
        
        return True

class PPEMonthDifferenceStep(PipelineStep):
    """
    PPE 月份差異計算步驟
    
    功能：
    計算合約結束日期與當前月份的差異
    """
    
    def __init__(self, 
                 name: str = "PPEMonthDifference",
                 current_month: int = None,
                 **kwargs):
        super().__init__(name, description="Calculate month difference", **kwargs)
        self.current_month = current_month
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行月份差異計算"""
        start_time = datetime.now()
        
        try:
            df = context.data.copy()
            
            # 獲取當前月份
            current_month = (self.current_month or 
                             context.get_variable('current_month'))
            
            if not current_month:
                raise ValueError("未提供當前月份參數")
            
            # 選擇必要欄位
            selected_cols = [
                'sp_code', 
                'address', 
                'contract_start_day_filing', 
                'contract_end_day_renewal'
            ]
            
            # 計算月份差異
            df_result = self._calculate_month_difference(
                df[selected_cols],
                'contract_end_day_renewal',
                current_month
            )
            
            # 新增截斷地址欄位（用於地址模糊匹配）
            df_result['truncated_address'] = df_result['address'].apply(
                self._truncate_address_at_hao
            )
            
            context.update_data(df_result)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(len(df), len(df_result))
                        .set_time_info(start_time, datetime.now())
                        .add_custom('current_month', current_month)
                        .add_custom('average_months_diff', 
                                    float(df_result['months_diff'].mean()))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_result,
                message=f"月份差異計算完成: 當前月份 {current_month}",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"月份差異計算失敗: {str(e)}", exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _calculate_month_difference(self, df: pd.DataFrame, 
                                    date_column: str, 
                                    target_ym: int) -> pd.DataFrame:
        """計算月份差異"""
        df_result = df.copy()
        
        # 確保日期格式
        df_result[date_column] = pd.to_datetime(df_result[date_column])
        
        # 目標日期
        target_year = target_ym // 100
        target_month = target_ym % 100
        target_date = datetime(target_year, target_month, 1)
        
        # 計算差異
        def months_difference(date1, date2):
            return (date1.year - date2.year) * 12 + (date1.month - date2.month)
        
        df_result['months_diff'] = df_result[date_column].apply(
            lambda x: months_difference(x, target_date)
        ).add(1)
        
        return df_result
    
    def _truncate_address_at_hao(self, address: str) -> str:
        """截斷地址到「號」"""
        if not isinstance(address, str):
            return address
        
        pattern = r'^.*?號'
        match = re.search(pattern, address)
        return match.group(0) if match else address

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for calculating difference")
            return False
        
        return True