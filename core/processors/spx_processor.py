"""
SPX處理器
專門處理SPX實體的特殊業務邏輯
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
import re

try:
    from .po_processor import BasePOProcessor
    from ...utils import (
        get_logger, safe_string_operation, convert_date_format_in_string,
        extract_pattern_from_string, create_mapping_dict, apply_mapping_safely,
        SPX_CONSTANTS
    )
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.po_processor import BasePOProcessor
    from utils import (
        get_logger, safe_string_operation, convert_date_format_in_string,
        extract_pattern_from_string, create_mapping_dict, apply_mapping_safely,
        SPX_CONSTANTS
    )


class SpxProcessor(BasePOProcessor):
    """SPX處理器，繼承自BasePOProcessor"""
    
    def __init__(self):
        """初始化SPX處理器"""
        super().__init__("SPX")
        self.logger = get_logger(self.__class__.__name__)
        
        # SPX特有配置
        self.product_code_filter = SPX_CONSTANTS['PRODUCT_CODE_FILTER']
        self.default_product_code = SPX_CONSTANTS['DEFAULT_PRODUCT_CODE']
        self.default_region = SPX_CONSTANTS['DEFAULT_REGION']
        self.dept_accounts = SPX_CONSTANTS['DEPT_ACCOUNTS']
        self.bao_categories = SPX_CONSTANTS['BAO_CATEGORIES']
        self.bao_supplier = SPX_CONSTANTS['BAO_SUPPLIER']
        
        self.logger.info("初始化SPX處理器完成")
    
    def filter_spx_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        過濾SPX產品代碼
        
        Args:
            df: 原始PO數據
            
        Returns:
            pd.DataFrame: 過濾後的DataFrame
        """
        try:
            if 'Product Code' not in df.columns:
                self.logger.warning("未找到Product Code欄位")
                return df
            
            mask_spx = safe_string_operation(
                df['Product Code'], 'contains', self.product_code_filter, na=False
            )
            
            filtered_df = df.loc[mask_spx, :].reset_index(drop=True)
            
            self.logger.info(f"SPX產品過濾: {len(df)} -> {len(filtered_df)} 筆記錄")
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"過濾SPX產品代碼時出錯: {str(e)}", exc_info=True)
            return df
    
    def add_spx_specific_columns(self, df: pd.DataFrame, month: int) -> Tuple[pd.DataFrame, int]:
        """
        添加SPX特有的欄位
        
        Args:
            df: PO DataFrame
            month: 月份
            
        Returns:
            Tuple[pd.DataFrame, int]: 添加了SPX特有欄位的DataFrame和更新的月份
        """
        try:
            # 先執行父類別的邏輯
            df_copy, previous_month = self.add_basic_columns(df, month)
            
            # 添加SPX特有欄位
            spx_columns = [
                'memo',
                'GL DATE',
                'Remarked by Procurement PR',
                'Noted by Procurement PR',
                'Remarked by 上月 FN PR'
            ]
            
            for col in spx_columns:
                if col not in df_copy.columns:
                    df_copy[col] = np.nan
            
            return df_copy, previous_month
            
        except Exception as e:
            self.logger.error(f"添加SPX特有欄位時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加SPX特有欄位時出錯")
    
    def process_spx_previous_workpaper(self, df: pd.DataFrame, previous_wp: pd.DataFrame, 
                                       month: int, previous_wp_pr: pd.DataFrame = None) -> pd.DataFrame:
        """
        處理SPX前期底稿（包括PO和PR）
        
        Args:
            df: PO DataFrame
            previous_wp: PO前期底稿DataFrame
            month: 月份
            previous_wp_pr: PR前期底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 先處理PO前期底稿
            df_copy = self.process_previous_workpaper(df, previous_wp, month)
            
            # 處理memo欄位
            if previous_wp is not None and not previous_wp.empty:
                memo_mapping = create_mapping_dict(previous_wp, 'PO Line', 'memo')
                df_copy['memo'] = apply_mapping_safely(df_copy['PO Line'], memo_mapping)
            
            # 處理PR前期底稿
            if previous_wp_pr is not None and not previous_wp_pr.empty:
                pr_fn_mapping = create_mapping_dict(previous_wp_pr, 'PR Line', 'Remarked by FN')
                df_copy['Remarked by 上月 FN PR'] = apply_mapping_safely(
                    df_copy['PR Line'], pr_fn_mapping
                )
            
            self.logger.info("成功處理SPX前期底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理SPX前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPX前期底稿時出錯")
    
    def process_spx_procurement_workpaper(self, df: pd.DataFrame, procurement: pd.DataFrame, 
                                          procurement_pr: pd.DataFrame = None) -> pd.DataFrame:
        """
        處理SPX採購底稿（包括PO和PR）
        
        Args:
            df: PO DataFrame
            procurement: PO採購底稿DataFrame
            procurement_pr: PR採購底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 先處理PO採購底稿
            df_copy = self.process_procurement_workpaper(df, procurement)
            
            # 移除SPT模組給的狀態（SPX有自己的狀態邏輯）
            df_copy.loc[df_copy['PO狀態'] == 'Not In Procurement WP', 'PO狀態'] = pd.NA
            
            # 處理PR採購底稿
            if procurement_pr is not None and not procurement_pr.empty:
                pr_procurement_mapping = create_mapping_dict(
                    procurement_pr, 'PR Line', 'Remarked by Procurement'
                )
                df_copy['Remarked by Procurement PR'] = apply_mapping_safely(
                    df_copy['PR Line'], pr_procurement_mapping
                )
                
                pr_noted_mapping = create_mapping_dict(
                    procurement_pr, 'PR Line', 'Noted by Procurement'
                )
                df_copy['Noted by Procurement PR'] = apply_mapping_safely(
                    df_copy['PR Line'], pr_noted_mapping
                )
            
            self.logger.info("成功處理SPX採購底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理SPX採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPX採購底稿時出錯")
    
    def add_gl_date_from_ap_invoice(self, df: pd.DataFrame, ap_invoice_df: pd.DataFrame, 
                                    file_date: int) -> pd.DataFrame:
        """
        從AP發票數據中添加GL DATE
        
        Args:
            df: PO DataFrame
            ap_invoice_df: AP發票DataFrame
            file_date: 檔案日期 (YYYYMM格式)
            
        Returns:
            pd.DataFrame: 添加了GL DATE的DataFrame
        """
        try:
            if ap_invoice_df is None or ap_invoice_df.empty:
                self.logger.info("AP發票數據為空，跳過GL DATE處理")
                return df
            
            df_copy = df.copy()
            ap_copy = ap_invoice_df.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # 創建組合鍵
            ap_copy['po_line'] = (
                ap_copy['Company'].astype(str) + '-' + 
                ap_copy['PO Number'].astype(str) + '-' + 
                ap_copy['PO_LINE_NUMBER'].astype(str)
            )
            
            # 轉換Period為YYYYMM格式
            ap_copy['period'] = pd.to_datetime(
                ap_copy['Period'], format='%b-%y', errors='coerce'
            ).dt.strftime('%Y%m').fillna('0').astype('int32')
            
            # 只保留檔案日期之前的記錄，並為每個po_line保留最新的period
            ap_filtered = (
                ap_copy.loc[ap_copy['period'] <= file_date, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 合併GL DATE
            if 'PO Line' in df_copy.columns:
                gl_date_mapping = create_mapping_dict(ap_filtered, 'po_line', 'period')
                df_copy['GL DATE'] = apply_mapping_safely(df_copy['PO Line'], gl_date_mapping)
            
            self.logger.info("成功添加GL DATE")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"添加GL DATE時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加GL DATE時出錯")
    
    def apply_spx_stage_1_status(self, df: pd.DataFrame, closing_data: pd.DataFrame) -> pd.DataFrame:
        """
        應用SPX第一階段狀態邏輯
        
        Args:
            df: PO DataFrame
            closing_data: 關單數據DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 獲取關單狀態
            to_be_closed, closed = self._get_closing_status(closing_data)
            
            # 轉換日期格式
            df_copy['Remarked by 上月 FN'] = convert_date_format_in_string(
                df_copy['Remarked by 上月 FN'].astype(str)
            )
            df_copy['Remarked by 上月 FN PR'] = convert_date_format_in_string(
                df_copy['Remarked by 上月 FN PR'].astype(str)
            )
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
            self._apply_deposit_condition(df_copy)
            
            # 條件2：供應商與類別對應，做GL調整
            self._apply_gl_adjustment_condition(df_copy)
            
            # 條件3：該PO#在待關單清單中
            self._apply_to_be_closed_condition(df_copy, to_be_closed)
            
            # 條件4：該PO#在已關單清單中
            self._apply_closed_condition(df_copy, closed)
            
            # 條件5：上月FN備註含有「刪」或「關」
            self._apply_last_month_close_condition(df_copy)
            
            # 條件6&7：處理「入FA」備註
            self._apply_fa_entry_condition(df_copy)
            
            self.logger.info("成功應用SPX第一階段狀態")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用SPX第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用SPX第一階段狀態時出錯")
    
    def _get_closing_status(self, closing_data: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """獲取關單狀態"""
        if closing_data is None or closing_data.empty:
            return [], []
        
        # 有新的PR編號，但FN未上系統關單的
        condition_1 = (
            (~closing_data['new_pr_no'].isna()) & 
            (closing_data['new_pr_no'] != '') & 
            (closing_data['done_by_fn'].isna())
        )
        
        # 有新的PR編號，但FN已經上系統關單的
        condition_2 = (
            (~closing_data['new_pr_no'].isna()) & 
            (closing_data['new_pr_no'] != '') & 
            (~closing_data['done_by_fn'].isna())
        )
        
        to_be_closed = closing_data.loc[condition_1, 'po_no'].unique().tolist()
        closed = closing_data.loc[condition_2, 'po_no'].unique().tolist()
        
        return to_be_closed, closed
    
    def _apply_deposit_condition(self, df: pd.DataFrame) -> None:
        """應用押金/保證金條件"""
        if 'Item Description' in df.columns and 'GL#' in df.columns:
            deposit_mask = safe_string_operation(
                df['Item Description'], 'contains', 
                r'(?i)押金|保證金|Deposit|找零金|定存', na=False
            )
            
            # 檢查是否為FA科目
            fa_account = self.config_manager.get('FA_ACCOUNTS', 'spx', '199999')
            is_fa = df['GL#'].astype(str) == str(fa_account)
            
            # 繳費機訂金屬FA，避免前端選錯加強過濾
            payment_machine_mask = safe_string_operation(
                df['Item Description'], 'contains', r'(?i)繳費機訂金', na=False
            )
            
            mask_deposit_condition = deposit_mask & ~is_fa & ~payment_machine_mask
            df.loc[mask_deposit_condition, 'PO狀態'] = '摘要內有押金/保證金/Deposit/找零金'
    
    def _apply_gl_adjustment_condition(self, df: pd.DataFrame) -> None:
        """應用GL調整條件"""
        if 'PO Supplier' in df.columns and 'Category' in df.columns:
            mask_bao = (
                (df['PO Supplier'] == self.bao_supplier) & 
                (df['Category'].isin(self.bao_categories))
            )
            df.loc[mask_bao, 'PO狀態'] = 'GL調整'
    
    def _apply_to_be_closed_condition(self, df: pd.DataFrame, to_be_closed: List[str]) -> None:
        """應用待關單條件"""
        if 'PO#' in df.columns and to_be_closed:
            mask_to_be_closed = df['PO#'].astype(str).isin([str(x) for x in to_be_closed])
            df.loc[mask_to_be_closed, 'PO狀態'] = '待關單'
    
    def _apply_closed_condition(self, df: pd.DataFrame, closed: List[str]) -> None:
        """應用已關單條件"""
        if 'PO#' in df.columns and closed:
            mask_closed = df['PO#'].astype(str).isin([str(x) for x in closed])
            df.loc[mask_closed, 'PO狀態'] = '已關單'
    
    def _apply_last_month_close_condition(self, df: pd.DataFrame) -> None:
        """應用上月關單條件"""
        close_mask = (
            safe_string_operation(df['Remarked by 上月 FN'], 'contains', '刪|關', na=False) |
            safe_string_operation(df['Remarked by 上月 FN PR'], 'contains', '刪|關', na=False)
        )
        df.loc[close_mask, 'PO狀態'] = '參照上月關單'
    
    def _apply_fa_entry_condition(self, df: pd.DataFrame) -> None:
        """應用入FA條件"""
        # 處理「Remarked by 上月 FN」含有「入FA」
        fn_fa_mask = (
            safe_string_operation(df['Remarked by 上月 FN'], 'contains', '入FA', na=False) &
            ~safe_string_operation(df['Remarked by 上月 FN'], 'contains', '部分完成', na=False)
        )
        
        if fn_fa_mask.any():
            fa_remarks = df.loc[fn_fa_mask, 'Remarked by 上月 FN'].apply(
                lambda x: extract_pattern_from_string(x, r'(\d{6}入FA)')
            )
            df.loc[fn_fa_mask, 'PO狀態'] = fa_remarks
        
        # 處理「Remarked by 上月 FN PR」含有「入FA」
        pr_fa_mask = (
            safe_string_operation(df['Remarked by 上月 FN PR'], 'contains', '入FA', na=False) &
            ~safe_string_operation(df['Remarked by 上月 FN PR'], 'contains', '部分完成', na=False)
        )
        
        if pr_fa_mask.any():
            fa_remarks_pr = df.loc[pr_fa_mask, 'Remarked by 上月 FN PR'].apply(
                lambda x: extract_pattern_from_string(x, r'(\d{6}入FA)')
            )
            df.loc[pr_fa_mask, 'PO狀態'] = fa_remarks_pr
    
    def apply_spx_erm_logic(self, df: pd.DataFrame, file_date: int, 
                            ref_accounts: pd.DataFrame, ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        應用SPX ERM邏輯
        
        Args:
            df: PO DataFrame
            file_date: 檔案日期 (YYYYMM格式)
            ref_accounts: 科目參考數據
            ref_liability: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 設置檔案日期
            df_copy['檔案日期'] = file_date
            
            # 確保已經解析日期
            if 'YMs of Item Description' not in df_copy.columns:
                df_copy = self.parse_date_from_description(df_copy)
            
            # 應用SPX特有的ERM狀態邏輯
            df_copy = self._apply_spx_erm_status_logic(df_copy)
            
            # 設置估計入帳（SPX邏輯：已完成->入帳，其餘N）
            mask_completed = df_copy['PO狀態'] == '已完成'
            df_copy.loc[mask_completed, '是否估計入帳'] = 'Y'
            df_copy.loc[~mask_completed, '是否估計入帳'] = 'N'
            
            # 設置會計相關欄位
            df_copy = self._set_spx_accounting_fields(df_copy, ref_accounts, ref_liability)
            
            self.logger.info("成功應用SPX ERM邏輯")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用SPX ERM邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用SPX ERM邏輯時出錯")
    
    def _apply_spx_erm_status_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用SPX特有的ERM狀態邏輯"""
        # 檢查FA科目
        fa_account = self.config_manager.get('FA_ACCOUNTS', 'spx', '199999')
        is_fa = df['GL#'].astype(str) == str(fa_account)
        
        # 提取日期範圍組件
        date_ranges = df['YMs of Item Description'].str.split(',', expand=True)
        if date_ranges.shape[1] >= 2:
            start_dates = pd.to_numeric(date_ranges[0], errors='coerce').fillna(0).astype('int32')
            end_dates = pd.to_numeric(date_ranges[1], errors='coerce').fillna(0).astype('int32')
        else:
            start_dates = pd.Series([0] * len(df), index=df.index)
            end_dates = pd.Series([0] * len(df), index=df.index)
        
        expected_month = df.get('Expected Received Month_轉換格式', pd.Series([0] * len(df)))
        file_date = df.get('檔案日期', pd.Series([0] * len(df)))
        
        # 在日期範圍內
        in_date_range = expected_month.between(start_dates, end_dates, inclusive='both')
        
        # 基本狀態條件
        no_status_mask = df['PO狀態'].isna() | (df['PO狀態'] == 'nan')
        
        # 條件定義
        conditions = []
        results = []
        
        # 已入帳
        mask_already_posted = safe_string_operation(
            df['Remarked by 上月 FN'], 'contains', r'(?i)已入帳', na=False
        )
        conditions.append(mask_already_posted)
        results.append('已入帳')
        
        # 已入帳（有GL DATE且符合其他條件）
        conditions.append(
            (~df['GL DATE'].isna()) & no_status_mask & in_date_range & 
            (expected_month <= file_date) & 
            (df['Entry Quantity'] == df['Received Quantity']) & 
            (df['Billed Quantity'] != '0') & 
            ((safe_string_operation(df['Remarked by Procurement'], 'contains', r'(?i)已完成|rent', na=False)) |
             (safe_string_operation(df['Remarked by 上月 FN'], 'contains', r'(?i)已完成|已入帳', na=False))) &
            (~is_fa)
        )
        results.append('已入帳')
        
        # 已完成
        conditions.append(
            ((safe_string_operation(df['Remarked by Procurement'], 'contains', r'(?i)已完成|rent', na=False)) |
             (safe_string_operation(df['Remarked by 上月 FN'], 'contains', r'(?i)已完成', na=False))) &
            (~safe_string_operation(df['Remarked by 上月 FN PR'], 'contains', r'(?i)未完成', na=False)) &
            no_status_mask & in_date_range & (expected_month <= file_date) &
            (df['Entry Quantity'] == df['Received Quantity']) &
            (pd.to_numeric(df['Entry Billed Amount'], errors='coerce').fillna(0) == 0)
        )
        results.append('已完成')
        
        # 其他條件...（可以繼續添加更多SPX特有的條件）
        
        # 應用條件
        df['PO狀態'] = np.select(conditions, results, default=df['PO狀態'])
        
        return df
    
    def _set_spx_accounting_fields(self, df: pd.DataFrame, ref_accounts: pd.DataFrame, 
                                   ref_liability: pd.DataFrame) -> pd.DataFrame:
        """設置SPX會計相關欄位"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        # 設置Account code
        df.loc[mask_accrual, 'Account code'] = df.loc[mask_accrual, 'GL#']
        
        # 設置Account Name
        if ref_accounts is not None and not ref_accounts.empty:
            account_mapping = create_mapping_dict(ref_accounts, 'Account', 'Account Desc')
            df['Account Name'] = apply_mapping_safely(df['Account code'], account_mapping)
        
        # 設置Product code（SPX固定值）
        df.loc[mask_accrual, 'Product code'] = self.default_product_code
        
        # 設置Region_c（SPX固定值）
        df.loc[mask_accrual, 'Region_c'] = self.default_region
        
        # 設置Dep.
        self._set_spx_department_code(df)
        
        # 設置Currency_c
        if 'Currency' in df.columns:
            df.loc[mask_accrual, 'Currency_c'] = df.loc[mask_accrual, 'Currency']
        
        # 設置Accr. Amount（SPX特殊計算）
        self._set_spx_accrual_amount(df)
        
        # 設置是否有預付
        if 'Entry Prepay Amount' in df.columns:
            is_prepayment = df['Entry Prepay Amount'] != '0'
            df.loc[mask_accrual & is_prepayment, '是否有預付'] = 'Y'
        
        # 設置Liability
        if ref_liability is not None and not ref_liability.empty:
            liability_mapping = create_mapping_dict(ref_liability, 'Account', 'Liability_y')
            df['Liability'] = apply_mapping_safely(df['Account code'], liability_mapping)
            
            # 有預付款時，Liability設為特定值
            if 'Entry Prepay Amount' in df.columns:
                is_prepayment = df['Entry Prepay Amount'] != '0'
                df.loc[mask_accrual & is_prepayment, 'Liability'] = '111112'
        
        return df
    
    def _set_spx_department_code(self, df: pd.DataFrame) -> None:
        """設置SPX部門代碼"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        if 'Account code' in df.columns and 'Department' in df.columns:
            # 特定科目需要使用實際部門代碼
            mask_dept_accounts = df['Account code'].astype(str).isin(self.dept_accounts)
            
            df.loc[mask_accrual & mask_dept_accounts, 'Dep.'] = \
                df.loc[mask_accrual & mask_dept_accounts, 'Department'].astype(str).str[:3]
            
            df.loc[mask_accrual & ~mask_dept_accounts, 'Dep.'] = '000'
    
    def _set_spx_accrual_amount(self, df: pd.DataFrame) -> None:
        """設置SPX應計金額（特殊計算方式）"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        if all(col in df.columns for col in ['Unit Price', 'Entry Quantity', 'Billed Quantity']):
            # 計算公式：單價 * (入庫數量 - 已開票數量)
            unit_price = pd.to_numeric(df['Unit Price'], errors='coerce').fillna(0)
            entry_qty = pd.to_numeric(df['Entry Quantity'], errors='coerce').fillna(0)
            billed_qty = pd.to_numeric(df['Billed Quantity'], errors='coerce').fillna(0)
            
            df.loc[mask_accrual, 'Accr. Amount'] = unit_price * (entry_qty - billed_qty)
    
    def finalize_spx_data_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        最終SPX數據格式化
        
        Args:
            df: SPX PO DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            # 先執行基本格式化
            df_copy = self.finalize_data_format(df)
            
            # SPX特有的欄位重排
            df_copy = self._rearrange_spx_columns(df_copy)
            
            self.logger.info("成功完成SPX數據格式化")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"格式化SPX數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化SPX數據時出錯")
    
    def _rearrange_spx_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列SPX特有的欄位順序"""
        try:
            # 移動上月FN PR備註
            if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN PR' in df.columns:
                fn_index = df.columns.get_loc('Remarked by FN') + 2  # 跳過上月FN
                if 'Remarked by 上月 FN PR' in df.columns:
                    pr_col = df.pop('Remarked by 上月 FN PR')
                    df.insert(fn_index, 'Remarked by 上月 FN PR', pr_col)
            
            # 移動PR相關欄位到適當位置
            if 'Noted by Procurement' in df.columns:
                noted_index = df.columns.get_loc('Noted by Procurement') + 1
                
                for col_name in ['Remarked by Procurement PR', 'Noted by Procurement PR']:
                    if col_name in df.columns:
                        col = df.pop(col_name)
                        df.insert(noted_index, col_name, col)
                        noted_index += 1
            
            return df
        except Exception as e:
            self.logger.warning(f"重新排列SPX欄位時出錯: {str(e)}")
            return df
