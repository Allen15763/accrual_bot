"""
PR處理器基類
繼承自BaseDataProcessor，提供PR特有的處理功能
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

try:
    from .base_processor import BaseDataProcessor
    from ...utils import (
        get_logger, create_mapping_dict, apply_mapping_safely,
        STATUS_VALUES
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
        get_logger, create_mapping_dict, apply_mapping_safely,
        STATUS_VALUES
    )


class BasePRProcessor(BaseDataProcessor):
    """PR處理器基類，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化PR處理器
        
        Args:
            entity_type: 實體類型，'MOB'、'SPT'或'SPX'
        """
        super().__init__(entity_type)
        self.logger = get_logger(f"{self.__class__.__name__}_{entity_type}")
        
        self.logger.info(f"初始化 {entity_type} PR處理器")
    
    def add_basic_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加基本必要列
        
        Args:
            df: 原始PR數據
            
        Returns:
            pd.DataFrame: 添加了必要列的數據框
        """
        try:
            df_copy = df.copy()
            
            # 生成PR Line
            if 'PR#' in df_copy.columns and 'Line#' in df_copy.columns:
                df_copy['PR Line'] = df_copy['PR#'].astype(str) + "-" + df_copy['Line#'].astype(str)
            
            # 添加標記和備註欄位
            remark_columns = [
                'Remarked by Procurement',
                'Noted by Procurement',
                'Remarked by FN',
                'Noted by FN',
                'Remarked by 上月 FN',
                'PR狀態'
            ]
            
            for col in remark_columns:
                if col not in df_copy.columns:
                    df_copy[col] = np.nan
            
            # 添加計算欄位
            calculation_columns = [
                '是否估計入帳',
                '是否為FA',
                '是否為S&M',
                'Account code',
                'Account Name',
                'Product code_c',
                'Region_c',
                'Dep.',
                'Currency_c',
                'Accr. Amount',
                'Liability',
                'PR Product Code Check',
                'Question from Reviewer',
                'Check by AP',
                'Memo'
            ]
            
            for col in calculation_columns:
                if col not in df_copy.columns:
                    df_copy[col] = np.nan
            
            # 設置特定值
            df_copy['是否為FA'] = self._determine_fa_status(df_copy)
            df_copy['是否為S&M'] = self._determine_sm_status(df_copy)
            
            self.logger.info("成功添加基本必要列")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"添加基本列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加基本列時出錯")
    
    def _determine_fa_status(self, df: pd.DataFrame) -> pd.Series:
        """
        確定是否為FA
        
        Args:
            df: PR DataFrame
            
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
            df: PR數據框
            
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
    
    def process_with_procurement(self, df: pd.DataFrame, procurement_df: pd.DataFrame) -> pd.DataFrame:
        """
        處理採購底稿
        
        Args:
            df: PR數據框
            procurement_df: 採購底稿數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            if procurement_df is None or procurement_df.empty:
                self.logger.info("採購底稿為空，跳過處理")
                return df
            
            df_copy = df.copy()
            
            # 獲取採購底稿中的Remarked by Procurement
            if 'PR Line' in df_copy.columns:
                procurement_mapping = create_mapping_dict(
                    procurement_df, 'PR Line', 'Remarked by Procurement'
                )
                df_copy['Remarked by Procurement'] = apply_mapping_safely(
                    df_copy['PR Line'], procurement_mapping
                )
                
                # 獲取Noted by Procurement
                noted_mapping = create_mapping_dict(
                    procurement_df, 'PR Line', 'Noted by Procurement'
                )
                df_copy['Noted by Procurement'] = apply_mapping_safely(
                    df_copy['PR Line'], noted_mapping
                )
            
            # 設置FN備註為採購備註
            df_copy['Remarked by FN'] = df_copy['Remarked by Procurement']
            
            self.logger.info("成功處理採購底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def process_previous_workpaper(self, df: pd.DataFrame, previous_wp: pd.DataFrame) -> pd.DataFrame:
        """
        處理前期底稿
        
        Args:
            df: PR數據框
            previous_wp: 前期底稿數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            if previous_wp is None or previous_wp.empty:
                self.logger.info("前期底稿為空，跳過處理")
                return df
            
            df_copy = df.copy()
            
            # 獲取前期FN備註
            if 'PR Line' in df_copy.columns:
                fn_mapping = create_mapping_dict(previous_wp, 'PR Line', 'Remarked by FN')
                df_copy['Remarked by 上月 FN'] = apply_mapping_safely(
                    df_copy['PR Line'], fn_mapping
                )
            
            self.logger.info("成功處理前期底稿")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def apply_date_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        應用日期邏輯處理
        
        Args:
            df: PR DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 解析日期
            df_copy = self.parse_date_from_description(df_copy)
            
            # PR特有的日期邏輯可以在這裡添加
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用日期邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用日期邏輯時出錯")
    
    def apply_status_logic(self, df: pd.DataFrame, file_date: int) -> pd.DataFrame:
        """
        應用狀態邏輯處理
        
        Args:
            df: PR DataFrame
            file_date: 檔案日期 (YYYYMM格式)
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 設置檔案日期
            df_copy['檔案日期'] = file_date
            
            # 應用狀態邏輯
            df_copy = self.evaluate_status_based_on_dates(df_copy, 'PR狀態')
            
            # 設置估計入帳標識
            df_copy = self.update_estimation_based_on_status(df_copy, 'PR狀態')
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"應用狀態邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("應用狀態邏輯時出錯")
    
    def set_accounting_fields(self, df: pd.DataFrame, ref_accounts: pd.DataFrame, 
                              ref_liability: pd.DataFrame) -> pd.DataFrame:
        """
        設置會計相關欄位
        
        Args:
            df: PR DataFrame
            ref_accounts: 科目參考數據
            ref_liability: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
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
                df_copy.loc[mask_accrual, 'Product code_c'] = df_copy.loc[mask_accrual, 'Product Code']
            
            # 檢查Product code與Project的一致性
            self._check_product_code_consistency(df_copy)
            
            # 設置Region_c
            self._set_region_code(df_copy)
            
            # 設置Dep.
            self._set_department_code(df_copy)
            
            # 設置Currency_c
            if 'Currency' in df_copy.columns:
                df_copy.loc[mask_accrual, 'Currency_c'] = df_copy.loc[mask_accrual, 'Currency']
            
            # 設置Accr. Amount
            if 'Entry Amount' in df_copy.columns:
                df_copy.loc[mask_accrual, 'Accr. Amount'] = df_copy.loc[mask_accrual, 'Entry Amount']
            
            # 設置Liability
            if ref_liability is not None and not ref_liability.empty:
                liability_mapping = create_mapping_dict(ref_liability, 'Account', 'Liability_y')
                df_copy['Liability'] = apply_mapping_safely(
                    df_copy['Account code'], liability_mapping
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"設置會計欄位時出錯: {str(e)}", exc_info=True)
            raise ValueError("設置會計欄位時出錯")
    
    def _check_product_code_consistency(self, df: pd.DataFrame) -> None:
        """檢查Product code與Project的一致性"""
        try:
            if 'Product code_c' not in df.columns or 'Project' not in df.columns:
                return
            
            mask_has_product_code = df['Product code_c'].notna()
            
            if mask_has_product_code.any():
                # 從Project中提取product code
                project_codes = df.loc[mask_has_product_code, 'Project'].astype(str).str.extract(
                    r'^(\w+)', expand=False
                ).fillna('')
                
                product_codes = df.loc[mask_has_product_code, 'Product code_c'].astype(str)
                
                # 比較一致性
                consistency_check = (project_codes == product_codes)
                
                df.loc[mask_has_product_code, 'PR Product Code Check'] = np.where(
                    consistency_check, 'good', 'bad'
                )
                
        except Exception as e:
            self.logger.warning(f"檢查Product code一致性時出錯: {str(e)}")
    
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
    
    def finalize_data_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        最終數據格式化
        
        Args:
            df: PR DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 格式化數值列
            int_cols = ['Line#', 'GL#']
            float_cols = ['Entry Amount', 'Accr. Amount']
            
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
                '是否估計入帳', 'PR Product Code Check', 'PR狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df_copy = self.clean_nan_values(df_copy, columns_to_clean)
            
            # 重新排列欄位順序
            df_copy = self._rearrange_columns(df_copy)
            
            self.logger.info("成功完成PR數據格式化")
            return df_copy
            
        except Exception as e:
            self.logger.error(f"格式化PR數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化PR數據時出錯")
    
    def _rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        try:
            # 移動上月備註到FN備註後面
            if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
                fn_index = df.columns.get_loc('Remarked by FN') + 1
                last_month_col = df.pop('Remarked by 上月 FN')
                df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)
            
            # 移動PR狀態到是否估計入帳前面
            if 'PR狀態' in df.columns and '是否估計入帳' in df.columns:
                accrual_index = df.columns.get_loc('是否估計入帳')
                pr_status_col = df.pop('PR狀態')
                df.insert(accrual_index, 'PR狀態', pr_status_col)
            
            return df
        except Exception as e:
            self.logger.warning(f"重新排列PR欄位時出錯: {str(e)}")
            return df
