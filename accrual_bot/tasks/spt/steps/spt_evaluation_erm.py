import time
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Any, Union
import pandas as pd
import numpy as np

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder


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


class SPTERMLogicStep(PipelineStep):
    """
    SPT ERM 邏輯步驟 - 完整實現版本
    
    功能：
    1. 設置檔案日期
    2. 判斷 11 種 PO/PR 狀態（已入帳、已完成、Check收貨等）
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（Account code, Product code, Dep.等）
    5. 計算預估金額（Accr. Amount）
    6. 處理預付款和負債科目
    7. 檢查 PR Product Code
    
    業務規則：
    - SPT 邏輯：「已完成」狀態的項目需要估列入帳
    - 其他狀態一律不估列（是否估計入帳 = N）
    
    輸入：
    - DataFrame with required columns
    - Reference data (科目映射、負債科目)
    - Processing date
    
    輸出：
    - DataFrame with PO/PR狀態, 是否估計入帳, and accounting fields
    """
    
    def __init__(self, name: str = "SPT_ERM_Logic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SPT ERM logic with 11 status conditions",
            **kwargs
        )
        
        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('SPT', 'fa_accounts', ['199999'])
        self.dept_accounts = config_manager.get_list('SPT', 'dept_accounts', [])
        
        self.logger.info(f"Initialized {name} with FA accounts: {self.fa_accounts}")
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 ERM 邏輯"""
        start_time = time.time()
        try:
            df = context.data.copy()
            processing_date = context.metadata.processing_date
            
            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            ref_liability = context.get_auxiliary_data('reference_liability')
            
            if ref_account is None or ref_liability is None:
                raise ValueError("缺少參考數據：科目映射或負債科目")
            
            self.logger.info(f"開始 ERM 邏輯處理，處理日期：{processing_date}")
            
            # ========== 階段 1: 設置基本欄位 ==========
            df = self._set_file_date(df, processing_date)
            
            # ========== 階段 2: 構建判斷條件 ==========
            status_column: str = self._get_status_column(df, context)
            conditions = self._build_conditions(df, processing_date, status_column)
            
            # ========== 階段 3: 應用 11 個狀態條件 ==========
            df = self._apply_status_conditions(df, conditions, status_column)
            
            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions, status_column)
            
            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df, status_column)
            
            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_accounting_fields(df, ref_account, ref_liability)
            
            # ========== 階段 7: 檢查 PR Product Code ==========
            df = self._check_pr_product_code(df)
            
            # 更新上下文
            context.update_data(df)
            
            # 生成統計資訊
            stats = self._generate_statistics(df, status_column)
            
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
    
    def _get_status_column(self, df: pd.DataFrame, context: ProcessingContext) -> str:
        """動態判斷狀態欄位"""
        if 'PO狀態' in df.columns:
            return 'PO狀態'
        elif 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            # 根據 context 創建欄位
            processing_type = context.metadata.processing_type
            return f"{processing_type}狀態"
    
    # ========== 階段 2: 構建條件 ==========
    
    def _build_conditions(self, df: pd.DataFrame, file_date: int,
                          status_column: str) -> ERMConditions:
        """
        構建所有判斷條件
        
        將條件邏輯集中在此處，提高可讀性和維護性
        """
        # 基礎狀態條件
        no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
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
                                 cond: ERMConditions,
                                 status_column: str) -> pd.DataFrame:
        """
        應用 11 個狀態判斷條件
        
        條件優先順序從上到下，符合的條件會被優先設置
        """
        
        # === 條件 1: 已入帳（前期FN明確標註）===
        condition_1 = df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False)
        df.loc[condition_1, status_column] = '已入帳'
        self._log_condition_result("已入帳（前期FN明確標註）", condition_1.sum())
        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 2: 已入帳（有 GL DATE 且符合其他條件）===
        condition_2 = (
            (~df['GL DATE'].isna()) &
            (df['match_type'] == 'ITEM_TO_RECEIPT') &
            cond.no_status &
            cond.quantity_matched &
            (~cond.is_fa)
        )
        df.loc[condition_2, status_column] = '已入帳'
        self._log_condition_result("已入帳（GL DATE）", condition_2.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 3: 已完成(not_billed) ===
        condition_3 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.pr_not_incomplete &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            cond.not_billed
        )
        df.loc[condition_3, status_column] = '已完成(not_billed)'
        self._log_condition_result("已完成(not_billed)", condition_3.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 4: 已完成(fully_billed) ===
        # ERM小於等於結帳月 and ERM在摘要期間內 and Entry Qty等於Received Qty and Entry Amount - Entry Billed Amount = 0--> 理論上要估計
        condition_4 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.fully_billed
        )
        df.loc[condition_4, status_column] = '已完成(fully_billed)'
        self._log_condition_result("已完成(fully_billed)", condition_4.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 5: 已完成(partially_billed) ===
        condition_5 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') != 0) &
            cond.has_unpaid_amount
        )
        df.loc[condition_5, status_column] = '已完成(partially_billed)'
        self._log_condition_result("已完成(partially_billed)", condition_5.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')

        # === 條件 5.1: 已完成(not_billed) ===
        condition_5 = (
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.quantity_matched &
            (df['Entry Billed Amount'].astype('Float64') == 0) &
            cond.has_unpaid_amount
        )
        df.loc[condition_5, status_column] = '已完成(not_billed)'
        self._log_condition_result("已完成(not_billed)", condition_5.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 6: Check收貨 ===
        # ERM小於等於結帳月 and ERM在摘要期間內 and Entry Qty不等於Received Qty --> 理論上要估計
        condition_6 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            (~cond.quantity_matched)
        )
        df.loc[condition_6, status_column] = 'Check收貨'
        self._log_condition_result("Check收貨", condition_6.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 7: 未完成 ===
        condition_7 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_after_file_date
        )
        df.loc[condition_7, status_column] = '未完成'
        self._log_condition_result("未完成", condition_7.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 8: 範圍錯誤_租金 ===
        condition_8 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)租金', na=False))
        )
        df.loc[condition_8, status_column] = 'error(Description Period is out of ERM)_租金'
        self._log_condition_result("範圍錯誤_租金", condition_8.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 9: 範圍錯誤_薪資 ===
        condition_9 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
        )
        df.loc[condition_9, status_column] = 'error(Description Period is out of ERM)_薪資'
        self._log_condition_result("範圍錯誤_薪資", condition_9.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 10: 範圍錯誤（一般）===
        condition_10 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range
        )
        df.loc[condition_10, status_column] = 'error(Description Period is out of ERM)'
        self._log_condition_result("範圍錯誤（一般）", condition_10.sum())

        # 🔴 新增：更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')
        
        # === 條件 11: 部分完成ERM ===
        condition_11 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Received Quantity'].astype('Float64') != 0) &
            (~cond.quantity_matched)
        )
        df.loc[condition_11, status_column] = '部分完成ERM'
        self._log_condition_result("部分完成ERM", condition_11.sum())
        
        return df
    
    def _log_condition_result(self, condition_name: str, count: int):
        """記錄條件判斷結果"""
        if count > 0:
            self.logger.debug(f"條件 [{condition_name}]: {count} 筆符合")
    
    # ========== 階段 4: 處理格式錯誤 ==========
    
    def _handle_format_errors(self, df: pd.DataFrame, 
                              cond: ERMConditions,
                              status_column: str) -> pd.DataFrame:
        """處理格式錯誤的記錄"""
        mask_format_error = cond.no_status & cond.format_error
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'
        
        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"發現 {error_count} 筆格式錯誤")
        
        return df
    
    # ========== 階段 5: 設置是否估計入帳 ==========
    
    def _set_accrual_flag(self, df: pd.DataFrame, status_column: str) -> pd.DataFrame:
        """
        根據 PO/PR狀態 設置是否估計入帳
        
        SPT 邏輯：只有「已完成」狀態需要估列入帳
        """
        mask_completed = df[status_column].str.contains('已完成', na=False)
        
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
        product_isnull = df['Product code'].isna()
        
        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']
        
        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account, need_accrual)
        
        # 3. Product code; 分潤有給product code的則以分潤的結果為主。
        df.loc[(need_accrual & product_isnull), 'Product code'] = (
            df.loc[(need_accrual & product_isnull), 'Product Code']
        )
        
        # 4. Region_c（SPT 固定值）
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
    
    def _generate_statistics(self, df: pd.DataFrame, status_column: str) -> Dict[str, Any]:
        """生成統計資訊"""
        stats = {
            'total_count': len(df),
            'accrual_count': (df['是否估計入帳'] == 'Y').sum(),
            'status_distribution': {}
        }
        
        if status_column in df.columns:
            status_counts = df[status_column].value_counts().to_dict()
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
        processing_date = context.metadata.processing_date
        if not processing_date:
            self.logger.error("缺少處理日期")
            context.add_error("缺少處理日期")
            return False
        
        self.logger.info("輸入驗證通過")
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """回滾操作（如需要）"""
        self.logger.warning(f"回滾 ERM 邏輯：{str(error)}")
        # SPT ERM 步驟通常不需要特殊回滾操作
