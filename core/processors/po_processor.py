"""
PO處理器基類
繼承自BaseDataProcessor，提供PO特有的處理功能
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime

try:
    from .base_processor import BaseDataProcessor
    from ...utils import (
        get_logger, safe_string_operation, create_mapping_dict, apply_mapping_safely,
        STATUS_VALUES, format_numeric_columns
    )
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.base_processor import BaseDataProcessor
    from utils import (
        get_logger, safe_string_operation, create_mapping_dict, apply_mapping_safely,
        STATUS_VALUES, format_numeric_columns
    )


class BasePOProcessor(BaseDataProcessor):
    """PO處理器基類，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化PO處理器
        
        Args:
            entity_type: 實體類型，'MOB'、'SPT'或'SPX'
        """
        super().__init__(entity_type)
        self.logger = get_logger(f"{self.__class__.__name__}_{entity_type}")
        
        # PO特有的配置
        self._load_po_specific_config()
        
        self.logger.info(f"初始化 {entity_type} PO處理器")
    
    def _load_po_specific_config(self) -> None:
        """加載PO特有的配置"""
        try:
            # 可以在這裡加載PO特有的配置
            pass
        except Exception as e:
            self.logger.warning(f"載入PO特有配置時出錯: {str(e)}")
    
    def add_basic_columns(self, df: pd.DataFrame, month: int) -> Tuple[pd.DataFrame, int]:
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
                df_copy['PR Line'] = df_copy['PR#'].astype(str) + '-' + df_copy['Line#'].astype(str)
            
            if 'PO#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PO Line'] = df_copy['PO#'].astype(str) + '-' + df_copy['Line#'].astype(str)
            
            # 添加標記和備註欄位
            self._add_remark_columns(df_copy, month)
            
            # 添加計算欄位
            self._add_calculation_columns(df_copy)
            
            # 計算上月
            previous_month = 12 if month == 1 else month - 1
            
            self.logger.info("成功添加基本必要列")
            return df_copy, previous_month
            
        except Exception as e:
            self.logger.error(f"添加基本列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加基本列時出錯")
    
    def _add_remark_columns(self, df: pd.DataFrame, month: int) -> None:
        """添加備註相關欄位"""
        columns_to_add = [
            'Remarked by Procurement',
            'Noted by Procurement', 
            'Remarked by FN',
            'Noted by FN',
            f'Remarked by {month}月 Procurement',
            'Remarked by 上月 FN',
            'PO狀態'
        ]
        
        for col in columns_to_add:
            if col not in df.columns:
                df[col] = np.nan
    
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
                df[col] = np.nan
        
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
        if 'GL#' in df.columns:
            return np.where(df['GL#'].astype(str).isin([str(x) for x in self.fa_accounts]), 'Y', '')
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
        
        if self.entity_type == 'MOB':
            return np.where(df['GL#'].astype(str).str.startswith('65'), "Y", "N")
        else:  # SPT or SPX
            return np.where(
                (df['GL#'].astype(str) == '650003') | (df['GL#'].astype(str) == '450014'), 
                "Y", "N"
            )
    
    def process_closing_list(self, df: pd.DataFrame, closing_list: List[str]) -> pd.DataFrame:
        """
        處理關單清單
        
        Args:
            df: PO DataFrame
            closing_list: 關單清單
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            if not closing_list:
                self.logger.info("關單清單為空，跳過處理")
                return df
            
            df_copy = df.copy()
            
            # 設置在關單清單中的PO狀態
            if 'PO#' in df_copy.columns:
                mask_in_closing = df_copy['PO#'].astype(str).isin([str(x) for x in closing_list])
                df_copy.loc[mask_in_closing, 'PO狀態'] = STATUS_VALUES['TO_BE_CLOSED']
                df_copy.loc[mask_in_closing, '是否估計入帳'] = "N"
                
                found_count = mask_in_closing.sum()
                self.logger.info(f"成功處理關單清單，找到 {found_count} 個在關單清單中的PO")
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理關單清單時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理關單清單時出錯")
    
    def process_previous_workpaper(self, df: pd.DataFrame, previous_wp: pd.DataFrame, 
                                  month: int) -> pd.DataFrame:
        """
        處理前期底稿
        
        Args:
            df: PO DataFrame
            previous_wp: 前期底稿DataFrame
            month: 月份
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            if previous_wp is None or previous_wp.empty:
                self.logger.info("前期底稿為空，跳過處理")
                return df
            
            df_copy = df.copy()
            
            # 重命名前期底稿中的列
            previous_wp_renamed = previous_wp.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                    'Remarked by Procurement': 'Remark by PR Team_l'
                }
            )

            # 獲取前期FN備註
            if 'PO Line' in df_copy.columns:
                fn_mapping = create_mapping_dict(previous_wp_renamed, 'PO Line', 'Remarked by FN_l')
                df_copy['Remarked by 上月 FN'] = apply_mapping_safely(
                    df_copy['PO Line'], fn_mapping
                )
                
                # 獲取前期採購備註
                procurement_mapping = create_mapping_dict(
                    previous_wp_renamed, 'PO Line', 'Remark by PR Team_l'
                )
                df_copy[f'Remarked by {month}月 Procurement'] = apply_mapping_safely(
                    df_copy['PO Line'], procurement_mapping
                )
            
            self.logger.info("成功處理前期底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def process_procurement_workpaper(self, df: pd.DataFrame, procurement_wp: pd.DataFrame) -> pd.DataFrame:
        """
        處理採購底稿
        
        Args:
            df: PO DataFrame
            procurement_wp: 採購底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            if procurement_wp is None or procurement_wp.empty:
                self.logger.info("採購底稿為空，跳過處理")
                return df
            
            df_copy = df.copy()
            
            # 重命名採購底稿中的列
            procurement_wp_renamed = procurement_wp.rename(
                columns={
                    'Remarked by Procurement': 'Remark by PR Team',
                    'Noted by Procurement': 'Noted by PR'
                }
            )
            
            # 通過PO Line獲取備註
            if 'PO Line' in df_copy.columns:
                procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Remark by PR Team'
                )
                df_copy['Remarked by Procurement'] = apply_mapping_safely(
                    df_copy['PO Line'], procurement_mapping
                )
                
                noted_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Noted by PR'
                )
                df_copy['Noted by Procurement'] = apply_mapping_safely(
                    df_copy['PO Line'], noted_mapping
                )
            
            # 通過PR Line獲取備註（如果PO Line沒有匹配到）
            if 'PR Line' in df_copy.columns:
                pr_procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PR Line', 'Remark by PR Team'
                )
                
                # 只更新尚未匹配的記錄
                mask_no_remark = df_copy['Remarked by Procurement'].isna()
                df_copy.loc[mask_no_remark, 'Remarked by Procurement'] = apply_mapping_safely(
                    df_copy.loc[mask_no_remark, 'PR Line'], pr_procurement_mapping
                )
            
            # 設置FN備註為採購備註
            df_copy['Remarked by FN'] = df_copy['Remarked by Procurement']
            
            # 標記不在採購底稿中的PO
            if 'PO Line' in df_copy.columns and 'PR Line' in df_copy.columns:
                po_list = procurement_wp_renamed.get('PO Line', pd.Series([])).tolist()
                pr_list = procurement_wp_renamed.get('PR Line', pd.Series([])).tolist()
                
                mask_not_in_wp = (
                    (~df_copy['PO Line'].isin(po_list)) & 
                    (~df_copy['PR Line'].isin(pr_list))
                )
                df_copy.loc[mask_not_in_wp, 'PO狀態'] = STATUS_VALUES['NOT_IN_PROCUREMENT']
            
            self.logger.info("成功處理採購底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def apply_date_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        應用日期邏輯處理
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 處理分潤合作
            if 'Item Description' in df_copy.columns:
                mask_profit_sharing = safe_string_operation(
                    df_copy['Item Description'], 'contains', '分潤合作', na=False
                )
                
                mask_no_status = (
                    df_copy['PO狀態'].isna() | (df_copy['PO狀態'] == 'nan')
                )
                
                df_copy.loc[mask_profit_sharing & mask_no_status, 'PO狀態'] = '分潤'
                
                if self.entity_type == 'SPT':
                    df_copy.loc[df_copy['PO狀態'] == '分潤', '是否估計入帳'] = '分潤'
            
            # 處理已入帳
            if 'PO Entry full invoiced status' in df_copy.columns:
                mask_posted = (
                    (df_copy['PO狀態'].isna() | (df_copy['PO狀態'] == 'nan')) & 
                    (df_copy['PO Entry full invoiced status'].astype(str) == '1')
                )
                df_copy.loc[mask_posted, 'PO狀態'] = STATUS_VALUES['POSTED']
                df_copy.loc[df_copy['PO狀態'] == STATUS_VALUES['POSTED'], '是否估計入帳'] = "N"
            
            # 解析日期
            df_copy = self.parse_date_from_description(df_copy)
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用日期邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用日期邏輯時出錯")
    
    def apply_erm_logic(self, df: pd.DataFrame, file_date: int, 
                       ref_accounts: pd.DataFrame, ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        應用ERM邏輯處理
        
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
            
            # 應用ERM狀態邏輯
            df_copy = self._apply_erm_status_logic(df_copy)
            
            # 設置估計入帳標識
            df_copy = self._set_accrual_estimation(df_copy)
            
            # 設置會計相關欄位
            df_copy = self._set_accounting_fields(df_copy, ref_accounts, ref_liability)
            
            self.logger.info("成功應用ERM邏輯")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用ERM邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用ERM邏輯時出錯")
    
    def _apply_erm_status_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用ERM狀態邏輯"""
        # 這個方法會在子類中被重寫以實現特定的ERM邏輯
        return self.evaluate_status_based_on_dates(df, 'PO狀態')
    
    def _set_accrual_estimation(self, df: pd.DataFrame) -> pd.DataFrame:
        """設置估計入帳標識"""
        return self.update_estimation_based_on_status(df, 'PO狀態')
    
    def _set_accounting_fields(self, df: pd.DataFrame, ref_accounts: pd.DataFrame, 
                              ref_liability: pd.DataFrame) -> pd.DataFrame:
        """設置會計相關欄位"""
        df_copy = df.copy()
        
        try:
            # 設置Account code
            df_copy = self.judge_ac_code(df_copy)
            
            # 設置Account Name
            if ref_accounts is not None and not ref_accounts.empty:
                account_mapping = create_mapping_dict(ref_accounts, 'Account', 'Account Desc')
                df_copy['Account Name'] = apply_mapping_safely(
                    df_copy['Account code'], account_mapping
                )
            
            # 設置Product code
            mask_accrual = df_copy['是否估計入帳'] == 'Y'
            if 'Product Code' in df_copy.columns:
                df_copy.loc[mask_accrual, 'Product code'] = df_copy.loc[mask_accrual, 'Product Code']
            
            # 設置Region_c
            self._set_region_code(df_copy)
            
            # 設置Dep.
            self._set_department_code(df_copy)
            
            # 設置Currency_c
            if 'Currency' in df_copy.columns:
                df_copy.loc[mask_accrual, 'Currency_c'] = df_copy.loc[mask_accrual, 'Currency']
            
            # 設置Accr. Amount
            self._set_accrual_amount(df_copy)
            
            # 設置Liability
            if ref_liability is not None and not ref_liability.empty:
                liability_mapping = create_mapping_dict(ref_liability, 'Account', 'Liability_y')
                df_copy['Liability'] = apply_mapping_safely(
                    df_copy['Account code'], liability_mapping
                )
            
            # 設置是否有預付
            if 'Entry Prepay Amount' in df_copy.columns:
                df_copy.loc[mask_accrual, '是否有預付'] = df_copy.loc[mask_accrual, 'Entry Prepay Amount']
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"設置會計欄位時出錯: {str(e)}", exc_info=True)
            return df_copy
    
    def _set_region_code(self, df: pd.DataFrame) -> None:
        """設置Region代碼"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        if self.entity_type == 'MOB':
            if 'Region' in df.columns:
                df.loc[mask_accrual, 'Region_c'] = df.loc[mask_accrual, 'Region']
        else:  # SPT
            if 'Account code' in df.columns and 'Region' in df.columns:
                mask_income_expense = df['Account code'].astype(str).str.match(r'^[4-6]', na=False)
                df.loc[mask_accrual & mask_income_expense, 'Region_c'] = \
                    df.loc[mask_accrual & mask_income_expense, 'Region']
                df.loc[mask_accrual & ~mask_income_expense.fillna(False), 'Region_c'] = '000'
    
    def _set_department_code(self, df: pd.DataFrame) -> None:
        """設置部門代碼"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        if self.entity_type == 'MOB':
            if 'Department' in df.columns:
                df.loc[mask_accrual, 'Dep.'] = df.loc[mask_accrual, 'Department'].astype(str).str[:3]
        else:  # SPT
            # 使用convert_dep_code方法
            df['Dep.'] = self.convert_dep_code(df)
    
    def _set_accrual_amount(self, df: pd.DataFrame) -> None:
        """設置應計金額"""
        mask_accrual = df['是否估計入帳'] == 'Y'
        
        if 'Entry Amount' in df.columns and 'Entry Billed Amount' in df.columns:
            df.loc[mask_accrual, 'Accr. Amount'] = (
                pd.to_numeric(df.loc[mask_accrual, 'Entry Amount'], errors='coerce').fillna(0) - 
                pd.to_numeric(df.loc[mask_accrual, 'Entry Billed Amount'], errors='coerce').fillna(0)
            )
    
    def finalize_data_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        最終數據格式化
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 格式化數值列
            int_cols = ['Line#', 'GL#']
            float_cols = [
                'Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                'Entry Billed Amount', 'Entry Prepay Amount', 
                'PO Entry full invoiced status', 'Accr. Amount'
            ]
            
            df_copy = self.format_numeric_columns_safely(df_copy, int_cols, float_cols)
            
            # 格式化日期
            df_copy = self.reformat_dates(df_copy)
            
            # 移除臨時計算列
            temp_columns = ['檔案日期', 'Expected Received Month_轉換格式', 'YMs of Item Description']
            for col in temp_columns:
                if col in df_copy.columns:
                    df_copy.drop(columns=[col], inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PO狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df_copy = self.clean_nan_values(df_copy, columns_to_clean)
            
            # 處理特殊值（SPT特有）
            if self.entity_type == 'SPT':
                if '是否估計入帳' in df_copy.columns:
                    df_copy['是否估計入帳'] = df_copy['是否估計入帳'].astype(str).str.replace('分潤', '').replace("0.0", "")
                
                # 移除PR Product Code Check列（SPT不需要）
                if 'PR Product Code Check' in df_copy.columns:
                    df_copy.drop(columns=['PR Product Code Check'], inplace=True)
            
            # 重新排列欄位順序
            df_copy = self._rearrange_columns(df_copy)
            
            self.logger.info("成功完成數據格式化")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")
    
    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        try:
            # 這個方法可以在子類中重寫以實現特定的欄位排列
            if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
                # 移動上月備註到FN備註後面
                fn_index = df.columns.get_loc('Remarked by FN') + 1
                last_month_col = df.pop('Remarked by 上月 FN')
                df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
            
            if 'PO狀態' in df.columns and '是否估計入帳' in df.columns:
                # 移動PO狀態到是否估計入帳前面
                accrual_index = df.columns.get_loc('是否估計入帳')
                po_status_col = df.pop('PO狀態')
                df.insert(accrual_index, 'PO狀態', po_status_col)
            
            return df
        except Exception as e:
            self.logger.warning(f"重新排列欄位時出錯: {str(e)}")
            return df
