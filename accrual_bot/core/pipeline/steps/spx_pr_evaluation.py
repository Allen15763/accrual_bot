"""
SPX PR ERM 邏輯判斷與評估
專門處理 PR (Purchase Request) 的狀態評估和會計欄位設置

與 PO 的核心差異：
1. 不判斷收貨狀態（無 Received Quantity 相關邏輯）
2. 不判斷入賬狀態（無 Billed Amount 相關邏輯）
3. 基於業務類型（租金/Intermediary/資產）進行判斷
4. 簡化的會計欄位設置（不處理預付款和負債科目）

作者: Claude
創建日期: 2025-10-27
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


@dataclass
class PRERMConditions:
    """
    PR ERM 判斷條件集合
    
    相比 PO 的 ERMConditions，移除了收貨和入賬相關條件
    
    屬性：
        基礎條件：
            no_status: PR狀態為空
            in_date_range: ERM 在摘要月份範圍內
            erm_before_or_equal_file_date: ERM 小於等於檔案日期
            erm_after_file_date: ERM 大於檔案日期
        
        備註條件：
            procurement_completed_or_rent
            fn_completed_or_posted: 前期 FN 備註含已完成或已入帳
            pr_not_incomplete: PR 備註不含未完成
            procurement_incomplete: 採購備註未完成
        
        FA 條件：
            is_fa: 是否為 FA 科目
        
        錯誤條件：
            procurement_not_error: Procurement 備註不是錯誤
            out_of_date_range: 摘要月份超出 ERM 範圍
            format_error: 摘要月份格式錯誤
        
        PR 特有條件：
            is_rent_related: 租金相關（申請人+科目+關鍵字）
            is_intermediary: Intermediary 相關
            is_asset_related: 資產相關（智取櫃/繳費機）
    """
    # 基礎條件組件
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series
    
    # 備註條件
    procurement_completed_or_rent: pd.Series
    fn_completed_or_posted: pd.Series
    pr_not_incomplete: pd.Series
    procurement_incomplete: pd.Series
    
    # FA 條件
    is_fa: pd.Series
    
    # 錯誤條件
    procurement_not_error: pd.Series
    out_of_date_range: pd.Series
    format_error: pd.Series
    
    # PR 特有條件
    is_rent_related: pd.Series
    is_intermediary: pd.Series
    is_asset_related: pd.Series


class SPXPRERMLogicStep(PipelineStep):
    """
    SPX PR ERM 邏輯步驟 - PR 專用版本
    
    功能：
    1. 設置檔案日期
    2. 判斷 5-7 種 PR 狀態
    3. 根據狀態設置是否估計入帳
    4. 設置會計相關欄位（簡化版本）
    
    業務規則：
    - PR 邏輯：基於 ERM、摘要月份、業務類型判斷狀態
    - 完成狀態的項目需要估列入帳
    - 未完成狀態一律不估列（是否估計入帳 = N）
    
    與 PO 的主要差異：
    - 移除收貨相關判斷（無 Received Quantity）
    - 移除入賬相關判斷（無 Billed Amount）
    - 移除預付款處理
    - 移除負債科目設置
    - 基於業務類型（租金/Intermediary/資產）進行判斷
    
    輸入：
    - DataFrame with required columns for PR
    - Reference data (科目映射)
    - Processing date
    
    輸出：
    - DataFrame with PR狀態, 是否估計入帳, and simplified accounting fields
    """
    
    def __init__(self, name: str = "SPX_PR_ERM_Logic", **kwargs):
        super().__init__(
            name=name,
            description="Apply SPX PR ERM logic with simplified status conditions",
            **kwargs
        )
        
        # 從配置讀取關鍵參數
        self.fa_accounts = config_manager.get_list('SPX', 'fa_accounts', ['199999'])
        self.dept_accounts = config_manager.get_list('SPX', 'dept_accounts', [])
        
        # PR 特有配置
        self.ops_for_rent = config_manager.get('SPX', 'ops_for_rent', '')
        self.ops_for_intermediary = config_manager.get('SPX', 'ops_for_intermediary', '')
        self.account_rent = config_manager.get('SPX', 'account_rent', '')
        
        self.kiosk_suppliers = config_manager.get_list('SPX', 'kiosk_suppliers', [])
        self.locker_suppliers = config_manager.get_list('SPX', 'locker_suppliers', [])
        
        self.logger.info(f"Initialized {name} for PR processing")
        self.logger.info(f"  - FA accounts: {self.fa_accounts}")
        self.logger.info(f"  - Rent ops: {self.ops_for_rent}")
        self.logger.info(f"  - Intermediary ops: {self.ops_for_intermediary}")
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 PR ERM 邏輯"""
        start_time = time.time()
        
        try:
            df = context.data.copy()
            processing_date = context.get_variable('processing_date')
            
            # 獲取參考數據
            ref_account = context.get_auxiliary_data('reference_account')
            
            if ref_account is None:
                raise ValueError("缺少參考數據：科目映射")
            
            self.logger.info("=" * 70)
            self.logger.info("🚀 開始 PR ERM 邏輯處理")
            self.logger.info(f"📅 處理日期：{processing_date}")
            self.logger.info(f"📊 輸入記錄數：{len(df):,}")
            self.logger.info("=" * 70)
            
            # ========== 階段 1: 設置基本欄位 ==========
            df = self._set_file_date(df, processing_date)
            
            # ========== 階段 2: 構建判斷條件 ==========
            status_column = self._get_status_column(df)
            self.logger.info(f"📋 狀態欄位：{status_column}")
            
            conditions = self._build_pr_conditions(df, processing_date, status_column)
            
            # ========== 階段 3: 應用 PR 狀態條件 ==========
            df = self._apply_pr_status_conditions(df, conditions, status_column)
            
            # ========== 階段 4: 處理格式錯誤 ==========
            df = self._handle_format_errors(df, conditions, status_column)
            
            # ========== 階段 5: 設置是否估計入帳 ==========
            df = self._set_accrual_flag(df, status_column)
            
            # ========== 階段 6: 設置會計欄位 ==========
            df = self._set_pr_accounting_fields(df, ref_account)
            
            # 更新上下文
            context.update_data(df)
            
            # 生成統計資訊
            stats = self._generate_statistics(df, status_column)
            
            # 記錄統計摘要
            self._log_summary_statistics(stats, status_column)
            
            duration = time.time() - start_time
            
            self.logger.info("=" * 70)
            self.logger.info("✅ PR ERM 邏輯完成")
            self.logger.info(f"⏱️  耗時：{duration:.2f} 秒")
            self.logger.info(f"📈 需估列：{stats['accrual_count']:,} 筆")
            self.logger.info(f"📊 總計：{stats['total_count']:,} 筆")
            self.logger.info("=" * 70)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"PR ERM 邏輯已應用，{stats['accrual_count']:,} 筆需估列",
                duration=duration,
                metadata=stats
            )
            
        except Exception as e:
            self.logger.error(f"❌ PR ERM 邏輯處理失敗: {str(e)}", exc_info=True)
            context.add_error(f"PR ERM 邏輯失敗: {str(e)}")
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
        self.logger.debug(f"✓ 已設置檔案日期：{processing_date}")
        return df
    
    def _get_status_column(self, df: pd.DataFrame) -> str:
        """
        獲取狀態欄位名稱
        
        PR 應該使用 'PR狀態' 欄位
        """
        if 'PR狀態' in df.columns:
            return 'PR狀態'
        else:
            # 如果沒有，創建一個
            df['PR狀態'] = np.nan
            return 'PR狀態'
    
    # ========== 階段 2: 構建條件 ==========
    
    def _build_pr_conditions(self, df: pd.DataFrame, file_date: int,
                             status_column: str) -> PRERMConditions:
        """
        構建所有 PR 判斷條件
        
        與 PO 的差異：
        - 移除收貨相關條件（quantity_matched）
        - 移除入賬相關條件（not_billed, has_billing, fully_billed, has_unpaid_amount）
        - 新增業務類型相關條件（租金、Intermediary、資產）
        
        Args:
            df: PR DataFrame
            file_date: 處理日期（YYYYMM 格式）
            status_column: 狀態欄位名稱
            
        Returns:
            PRERMConditions: PR 專用條件集合
        """
        self.logger.info("🔧 開始構建 PR 判斷條件...")
        
        # 基礎狀態條件
        no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # 日期範圍條件
        ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
        ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
        erm = df['Expected Received Month_轉換格式']
        
        in_date_range = erm.between(ym_start, ym_end, inclusive='both')
        erm_before_or_equal_file_date = erm <= file_date
        erm_after_file_date = erm > file_date
        
        # 備註條件
        procurement_completed_or_rent = df['Remarked by Procurement'].str.contains(
            '(?i)已完成|rent', na=False
        )
        fn_completed_or_posted = df['Remarked by 上月 FN'].astype('string').str.contains(
            '(?i)已完成|已入帳', na=False
        )
        pr_not_incomplete = ~df['Remarked by 上月 FN'].astype('string').str.contains(
            '(?i)未完成', na=False
        )
        procurement_incomplete = df['Remarked by Procurement'].str.contains(
            '(?i)未完成|取消', na=False
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
        
        # ========== PR 特有條件 ==========
        
        # 租金相關條件
        is_rent_ops = df['Requester'] == self.ops_for_rent
        is_rent_account = df['GL#'] == self.account_rent
        has_rent_keyword = df['Item Description'].str.contains('(?i)租金', na=False)
        is_rent_related = is_rent_ops & is_rent_account & has_rent_keyword
        
        # Intermediary 相關條件
        is_intermediary_ops = df['Requester'] == self.ops_for_intermediary
        has_intermediary_keyword = df['Item Description'].fillna('na').str.contains('(?i)intermediary', na=False)
        is_intermediary = is_intermediary_ops & has_intermediary_keyword
        
        # 資產相關條件（智取櫃與繳費機）
        asset_suppliers = self.kiosk_suppliers + self.locker_suppliers
        is_asset_related = df['PR Supplier'].isin(asset_suppliers)
        
        # 記錄條件統計
        self.logger.debug(f"  ├─ 無狀態記錄：{no_status.sum():,}")
        self.logger.debug(f"  ├─ ERM 在範圍內：{in_date_range.sum():,}")
        self.logger.debug(f"  ├─ ERM <= 檔案日期：{erm_before_or_equal_file_date.sum():,}")
        self.logger.debug(f"  ├─ ERM > 檔案日期：{erm_after_file_date.sum():,}")
        self.logger.debug(f"  ├─ 租金相關：{is_rent_related.sum():,}")
        self.logger.debug(f"  ├─ Intermediary：{is_intermediary.sum():,}")
        self.logger.debug(f"  └─ 資產相關：{is_asset_related.sum():,}")
        
        return PRERMConditions(
            no_status=no_status,
            in_date_range=in_date_range,
            erm_before_or_equal_file_date=erm_before_or_equal_file_date,
            erm_after_file_date=erm_after_file_date,
            procurement_completed_or_rent=procurement_completed_or_rent,
            fn_completed_or_posted=fn_completed_or_posted,
            procurement_not_error=procurement_not_error,
            pr_not_incomplete=pr_not_incomplete,
            procurement_incomplete=procurement_incomplete,
            is_fa=is_fa,
            out_of_date_range=out_of_date_range,
            format_error=format_error,
            is_rent_related=is_rent_related,
            is_intermediary=is_intermediary,
            is_asset_related=is_asset_related
        )
    
    # ========== 階段 3: 應用狀態條件 ==========
    
    def _apply_pr_status_conditions(self, df: pd.DataFrame, 
                                    cond: PRERMConditions,
                                    status_column: str) -> pd.DataFrame:
        """
        應用 PR 狀態判斷條件
        
        PR 狀態條件（共 7 個）：
        1. 已入帳（前期 FN 明確標註）
        2. 已完成（ERM 在小於等於當期且在摘要區間內，有採購或會計備註）
        2-1. 已完成（ERM 在小於等於當期且在摘要區間內，沒有採購或會計備註）
        3. 未完成（ERM > 檔案日期）
        4. 範圍錯誤_租金
        5. 範圍錯誤_薪資
        6. 範圍錯誤（一般）
        7. 資產類特殊處理
        
        條件優先順序從上到下，符合的條件會被優先設置
        
        Args:
            df: PR DataFrame
            cond: PR 條件集合
            status_column: 狀態欄位名稱
            
        Returns:
            pd.DataFrame: 設置狀態後的 DataFrame
        """
        self.logger.info("🏷️  開始應用 PR 狀態條件...")
        
        # === 條件 1: 已入帳（前期 FN 明確標註）===
        condition_1 = df['Remarked by 上月 FN'].astype('string').str.contains('(?i)已入帳', na=False)
        df.loc[condition_1, status_column] = '已入帳'
        self._log_condition_result("已入帳（前期 FN 明確標註）", condition_1.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 2: 已完成 ===
        # ERM 在範圍內 & ERM <= 檔案日期 & 採購備註為已完成或Rent相關 | 前期 FN 已完成或已入帳
        condition_2 = (
            (cond.procurement_completed_or_rent | cond.fn_completed_or_posted) &
            cond.pr_not_incomplete &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.procurement_not_error
        )
        df.loc[condition_2, status_column] = '已完成'
        self._log_condition_result("已完成", condition_2.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')

        # === 條件 2-1: 已完成 ===
        # ERM 在範圍內 & ERM <= 檔案日期 & 沒有採購或會計備註
        condition_2 = (
            ~cond.procurement_incomplete &
            cond.pr_not_incomplete &
            cond.no_status &
            cond.in_date_range &
            cond.erm_before_or_equal_file_date &
            cond.procurement_not_error
        )
        df.loc[condition_2, status_column] = '已完成'
        self._log_condition_result("已完成", condition_2.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 3: 未完成 ===
        # ERM 在範圍內 & ERM > 檔案日期
        condition_3 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.in_date_range &
            cond.erm_after_file_date
        )
        df.loc[condition_3, status_column] = '未完成'
        self._log_condition_result("未完成", condition_3.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 4: 範圍錯誤_租金 ===
        condition_4 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            cond.is_rent_related
        )
        df.loc[condition_4, status_column] = 'error(Description Period is out of ERM)_租金'
        self._log_condition_result("範圍錯誤_租金", condition_4.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 5: 範圍錯誤_薪資 ===
        condition_5 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range &
            (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
        )
        df.loc[condition_5, status_column] = 'error(Description Period is out of ERM)_薪資'
        self._log_condition_result("範圍錯誤_薪資", condition_5.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 6: 範圍錯誤（一般）===
        condition_6 = (
            cond.procurement_not_error &
            cond.no_status &
            cond.out_of_date_range
        )
        df.loc[condition_6, status_column] = 'error(Description Period is out of ERM)'
        self._log_condition_result("範圍錯誤（一般）", condition_6.sum())
        
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        # === 條件 7: 資產類特殊處理 ===
        # 智取櫃與繳費機不進行一般的 ERM 判斷
        # 這個條件應該在最後，因為它不考慮 ERM
        condition_7 = (
            cond.no_status &
            cond.is_asset_related
        )
        df.loc[condition_7, status_column] = '資產類_待確認'
        self._log_condition_result("資產類_待確認", condition_7.sum())
        
        self.logger.info("✓ PR 狀態條件應用完成")
        
        return df
    
    def _log_condition_result(self, condition_name: str, count: int):
        """
        記錄條件判斷結果
        
        Args:
            condition_name: 條件名稱
            count: 符合條件的記錄數
        """
        if count > 0:
            self.logger.info(f"  ✓ [{condition_name:40s}]: {count:6,} 筆")
    
    # ========== 階段 4: 處理格式錯誤 ==========
    
    def _handle_format_errors(self, df: pd.DataFrame, 
                              cond: PRERMConditions,
                              status_column: str) -> pd.DataFrame:
        """
        處理格式錯誤的記錄
        
        Args:
            df: PR DataFrame
            cond: PR 條件集合
            status_column: 狀態欄位名稱
            
        Returns:
            pd.DataFrame: 處理後的 DataFrame
        """
        # 更新 no_status（最後一次）
        no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        mask_format_error = no_status & cond.format_error
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'
        # 🔴 更新 no_status
        cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan') | (df[status_column] == '')
        
        error_count = mask_format_error.sum()
        if error_count > 0:
            self.logger.warning(f"⚠️  發現 {error_count:,} 筆格式錯誤")
        
        # 其他
        condition_others = (
            cond.no_status
        )
        df.loc[condition_others, status_column] = '其他'
        self._log_condition_result("其他", condition_others.sum())

        return df
    
    # ========== 階段 5: 設置是否估計入帳 ==========
    
    def _set_accrual_flag(self, df: pd.DataFrame, status_column: str) -> pd.DataFrame:
        """
        根據 PR狀態 設置是否估計入帳
        
        PR 邏輯：
        - '已完成'：需要估列入帳（Y）
        - '已入帳'：已經入帳，不需估列（N）
        - 其他狀態：不估列（N）
        
        Args:
            df: PR DataFrame
            status_column: 狀態欄位名稱
            
        Returns:
            pd.DataFrame: 設置估列標記後的 DataFrame
        """
        self.logger.info("⚙️  設置估列標記...")
        
        # 初始化為 'N'
        df['是否估計入帳'] = 'N'
        
        # 需要估列的狀態
        accrual_statuses = ['已完成']
        mask_need_accrual = df[status_column].isin(accrual_statuses)
        
        df.loc[mask_need_accrual, '是否估計入帳'] = 'Y'
        
        accrual_count = mask_need_accrual.sum()
        non_accrual_count = (~mask_need_accrual).sum()
        
        self.logger.info(f"  ├─ 需估列（Y）：{accrual_count:,} 筆")
        self.logger.info(f"  └─ 不估列（N）：{non_accrual_count:,} 筆")
        
        return df
    
    # ========== 階段 6: 設置會計欄位 ==========
    
    def _set_pr_accounting_fields(self, df: pd.DataFrame,
                                  ref_account: pd.DataFrame) -> pd.DataFrame:
        """
        設置 PR 會計相關欄位（簡化版本）
        
        與 PO 的差異：
        - 不處理預付款（無 Entry Prepay Amount）
        - 不設置 Liability（無負債科目）
        - 不計算 Accr. Amount（直接使用 Entry Amount）
        
        設置的欄位：
        1. Account code (從 GL#)
        2. Account Name (從參考資料)
        3. Product code (從 Product Code)
        4. Region_c (固定 "TW")
        5. Dep. (部門代碼)
        6. Currency_c (從 Currency)
        
        Args:
            df: PR DataFrame
            ref_account: 科目映射參考資料
            
        Returns:
            pd.DataFrame: 設置會計欄位後的 DataFrame
        """
        self.logger.info("💼 設置會計欄位...")
        
        need_accrual = df['是否估計入帳'] == 'Y'
        accrual_count = need_accrual.sum()
        
        if accrual_count == 0:
            self.logger.info("  └─ 無需估列記錄，跳過會計欄位設置")
            return df
        
        self.logger.info(f"  處理 {accrual_count:,} 筆需估列記錄...")
        
        # 1. Account code
        df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']
        self.logger.debug("  ✓ Account code 設置完成")
        
        # 2. Account Name（通過 merge）
        df = self._set_account_name(df, ref_account, need_accrual)
        self.logger.debug("  ✓ Account Name 設置完成")
        
        # 3. Product code
        df.loc[need_accrual, 'Product code'] = df.loc[need_accrual, 'Product Code']
        self.logger.debug("  ✓ Product code 設置完成")
        
        # 4. Region_c（SPX 固定值）
        df.loc[need_accrual, 'Region_c'] = "TW"
        self.logger.debug("  ✓ Region_c 設置完成")
        
        # 5. Dep.（部門代碼）
        df = self._set_department(df, need_accrual)
        self.logger.debug("  ✓ Dep. 設置完成")
        
        # 6. Currency_c
        df.loc[need_accrual, 'Currency_c'] = df.loc[need_accrual, 'Currency']
        self.logger.debug("  ✓ Currency_c 設置完成")
        
        # 7. Accr. Amount（PR 直接使用 Entry Amount，不需要計算）
        df.loc[need_accrual, 'Accr. Amount'] = df.loc[need_accrual, 'Entry Amount'].astype('Float64')
        self.logger.debug("  ✓ Accr. Amount 設置完成")
        
        self.logger.info("✓ 會計欄位設置完成")
        
        return df
    
    def _set_account_name(self, df: pd.DataFrame, ref_account: pd.DataFrame,
                          mask: pd.Series) -> pd.DataFrame:
        """
        設置會計科目名稱
        
        從參考資料中查找科目名稱
        
        Args:
            df: PR DataFrame
            ref_account: 科目映射參考資料
            mask: 需要設置的記錄遮罩
            
        Returns:
            pd.DataFrame: 設置科目名稱後的 DataFrame
        """
        if ref_account.empty:
            self.logger.warning("⚠️  參考科目資料為空，無法設置 Account Name")
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
        
        # 檢查遺漏
        missing_count = df.loc[mask, 'Account Name'].isna().sum()
        if missing_count > 0:
            self.logger.warning(f"⚠️  {missing_count:,} 筆記錄無法找到對應的 Account Name")
        
        return df
    
    def _set_department(self, df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
        """
        設置部門代碼
        
        規則：
        - 如果科目在 dept_accounts 清單中，取 Department 前 3 碼
        - 否則設為 '000'
        
        Args:
            df: PR DataFrame
            mask: 需要設置的記錄遮罩
            
        Returns:
            pd.DataFrame: 設置部門代碼後的 DataFrame
        """
        isin_dept = df['Account code'].astype('string').isin(
            [str(x) for x in self.dept_accounts]
        )
        
        # 在 dept_accounts 中的科目
        dept_mask = mask & isin_dept
        if dept_mask.any():
            df.loc[dept_mask, 'Dep.'] = df.loc[dept_mask, 'Department'].str[:3]
        
        # 不在 dept_accounts 中的科目
        non_dept_mask = mask & ~isin_dept
        if non_dept_mask.any():
            df.loc[non_dept_mask, 'Dep.'] = '000'
        
        return df
    
    # ========== 輔助方法 ==========
    
    def _generate_statistics(self, df: pd.DataFrame, status_column: str) -> Dict[str, Any]:
        """
        生成統計資訊
        
        Args:
            df: PR DataFrame
            status_column: 狀態欄位名稱
            
        Returns:
            Dict: 包含統計資訊的字典
        """
        total_count = len(df)
        accrual_count = (df['是否估計入帳'] == 'Y').sum()
        
        stats = {
            'total_count': total_count,
            'accrual_count': accrual_count,
            'accrual_percentage': round((accrual_count / total_count * 100), 2) if total_count > 0 else 0,
            'status_distribution': {}
        }
        
        # 狀態分布統計
        if status_column in df.columns:
            status_counts = df[status_column].value_counts().to_dict()
            stats['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }
            
            # Top 5 狀態
            top_5 = dict(sorted(status_counts.items(), key=lambda x: x[1], reverse=True)[:5])
            stats['top_5_statuses'] = {str(k): int(v) for k, v in top_5.items()}
        
        return stats
    
    def _log_summary_statistics(self, stats: Dict[str, Any], status_column: str):
        """
        記錄統計摘要到 logger
        
        Args:
            stats: 統計資訊字典
            status_column: 狀態欄位名稱
        """
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info(f"📊 {status_column} 處理摘要")
        self.logger.info("=" * 70)
        
        # 總覽統計
        self.logger.info(f"📈 總記錄數：{stats['total_count']:,}")
        self.logger.info(f"   ├─ 需估列：{stats['accrual_count']:,} "
                         f"({stats['accrual_percentage']:.1f}%)")
        self.logger.info(f"   └─ 不估列：{stats['total_count'] - stats['accrual_count']:,}")
        
        # Top 5 狀態
        if 'top_5_statuses' in stats:
            self.logger.info("")
            self.logger.info("🏆 Top 5 狀態分布：")
            for i, (status, count) in enumerate(stats['top_5_statuses'].items(), 1):
                percentage = (count / stats['total_count'] * 100)
                self.logger.info(f"   {i}. {status:40s}: {count:6,} ({percentage:5.1f}%)")
        
        self.logger.info("=" * 70)
    
    # ========== 驗證方法 ==========
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入數據
        
        檢查：
        1. DataFrame 不為空
        2. 必要欄位存在
        3. 參考數據可用
        4. 處理日期存在
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 驗證是否通過
        """
        df = context.data
        
        # 檢查 DataFrame
        if df is None or df.empty:
            self.logger.error("❌ 輸入數據為空")
            context.add_error("輸入數據為空")
            return False
        
        # 檢查必要欄位
        required_columns = [
            'GL#',
            'Expected Received Month_轉換格式',
            'YMs of Item Description',
            'Item Description',
            'Remarked by Procurement',
            'Remarked by 上月 FN',
            'Currency',
            'Product Code',
            'Requester',
            'PR Supplier',
            'Entry Amount',
            'Department'
        ]
        
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            self.logger.error(f"❌ 缺少必要欄位: {missing}")
            context.add_error(f"缺少必要欄位: {missing}")
            return False
        
        # 檢查參考數據
        ref_account = context.get_auxiliary_data('reference_account')
        
        if ref_account is None:
            self.logger.error("❌ 缺少參考數據：科目映射")
            context.add_error("缺少參考數據：科目映射")
            return False
        
        # 檢查處理日期
        processing_date = context.get_variable('processing_date')
        if processing_date is None:
            self.logger.error("❌ 缺少處理日期")
            context.add_error("缺少處理日期")
            return False
        
        self.logger.info("✅ 輸入驗證通過")
        return True
    
    async def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作
        
        PR ERM 步驟通常不需要特殊回滾操作
        
        Args:
            context: 處理上下文
            error: 發生的錯誤
        """
        self.logger.warning(f"⚠️  回滾 PR ERM 邏輯：{str(error)}")
        # 如有需要，可在此處添加清理邏輯

