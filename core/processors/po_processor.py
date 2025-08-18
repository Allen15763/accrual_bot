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
            f'Remarked by {self.calculate_month(month)}月 Procurement',
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
                df_copy['Remarked by 上月 FN'] = df_copy['PO Line'].map(fn_mapping)
                
                # 獲取前期採購備註
                procurement_mapping = create_mapping_dict(
                    previous_wp_renamed, 'PO Line', 'Remark by PR Team_l'
                )
                df_copy[f'Remarked by {self.calculate_month(month)}月 Procurement'] = \
                    df_copy['PO Line'].map(procurement_mapping)
            
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
                df_copy['Remarked by Procurement'] = df_copy['PO Line'].map(procurement_mapping)
                
                noted_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PO Line', 'Noted by PR'
                )
                df_copy['Noted by Procurement'] = df_copy['PO Line'].map(noted_mapping)
            
            # 通過PR Line獲取備註（如果PO Line沒有匹配到）
            if 'PR Line' in df_copy.columns:
                pr_procurement_mapping = create_mapping_dict(
                    procurement_wp_renamed, 'PR Line', 'Remark by PR Team'
                )
                
                # 只更新尚未匹配的記錄
                df_copy['Remarked by Procurement'] = \
                    (df_copy.apply(lambda x: pr_procurement_mapping.get(x['PR Line'], None) 
                                   if x['Remarked by Procurement'] is np.nan else x['Remarked by Procurement'], axis=1))
            
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
        # # 特定的ERM邏輯for PO暫時寫在裡面，用欄位區分PO/PR
        # return self.evaluate_status_based_on_dates(df, 'PO狀態')
        return self.evaluate_status_based_on_dates_integrated(df, 'PO狀態')
    
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
                df_copy['Account Name'] = df_copy['Account code'].map(account_mapping)
            
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
                liability_mapping = create_mapping_dict(ref_liability, 'Account', 'Liability')
                df_copy['Liability'] = df_copy['Account code'].map(liability_mapping)
            
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
            
            if 'PO狀態' in df.columns and 'Remarked by 上月 FN' in df.columns:
                # 移動PO狀態到Remarked by 上月 FN後面
                accrual_index = df.columns.get_loc('Remarked by 上月 FN') + 1
                po_status_col = df.pop('PO狀態')
                df.insert(accrual_index, 'PO狀態', po_status_col)
            
            return df
        except Exception as e:
            self.logger.warning(f"重新排列欄位時出錯: {str(e)}")
            return df
    
    def process(self, raw_data_file: str, filename: str, 
                previous_workpaper: Optional[str] = None,
                procurement_file: Optional[str] = None,
                closing_list_file: Optional[str] = None,
                **kwargs):
        """
        統一的PO處理入口方法
        
        Args:
            raw_data_file: 原始PO數據檔案路徑
            filename: 原始數據檔案名稱
            previous_workpaper: 前期底稿檔案路徑（可選）
            procurement_file: 採購底稿檔案路徑（可選）
            closing_list_file: 關單清單檔案路徑（可選）
            **kwargs: 其他參數
            
        Returns:
            ProcessingResult: 處理結果
        """
        try:
            from ..models.data_models import ProcessingResult
            
            start_time = datetime.now()
            self.logger.info(f"開始處理PO檔案: {filename}")
            
            # 1. 讀取原始PO數據
            df = self._read_raw_data(raw_data_file)
            if df is None or df.empty:
                raise ValueError(f"無法讀取或空的原始數據檔案: {raw_data_file}")
            
            # 2. 提取月份資訊
            month = self._extract_month_from_filename(filename)
            
            # 3. 添加基本欄位
            df, previous_month = self.add_basic_columns(df, month)
            
            # 4. 處理前期底稿（如果提供）
            if previous_workpaper:
                previous_wp_df = self._read_workpaper(previous_workpaper)
                if previous_wp_df is not None:
                    df = self.process_previous_workpaper(df, previous_wp_df, month)
            
            # 5. 處理採購底稿（如果提供）
            if procurement_file:
                procurement_df = self._read_workpaper(procurement_file)
                if procurement_df is not None:
                    df = self.process_procurement_workpaper(df, procurement_df)
            
            # 6. 處理關單清單（如果提供）
            if closing_list_file:
                closing_list = self._read_closing_list(closing_list_file)
                if closing_list:
                    df = self.process_closing_list(df, closing_list)
            
            # 7. 應用日期邏輯
            df = self.apply_date_logic(df)
            
            # 8. 應用ERM邏輯
            file_date = self._convert_month_to_file_date(month)
            df = self.apply_erm_logic(df, file_date, None, None)  # 參考數據可以後續添加
            
            # 9. 最終格式化
            df = self.finalize_data_format(df)
            
            # 10. 輸出結果
            output_file = self._save_output(df, filename)
            
            end_time = datetime.now()
            
            return ProcessingResult(
                success=True,
                message="PO處理完成",
                processed_data=df,
                total_records=len(df),
                processed_records=len(df),
                start_time=start_time,
                end_time=end_time,
                output_files=[output_file] if output_file else []
            )
            
        except Exception as e:
            end_time = datetime.now()
            error_msg = f"PO處理失敗: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            return ProcessingResult(
                success=False,
                message=error_msg,
                start_time=start_time,
                end_time=end_time,
                errors=[error_msg]
            )
    
    def _read_raw_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取原始數據檔案"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
            else:
                raise ValueError(f"不支援的檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取原始數據: {len(df)} 行")
            return df
            
        except Exception as e:
            self.logger.error(f"讀取原始數據失敗: {e}")
            return None
    
    def _read_workpaper(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取底稿檔案"""
        try:
            if file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            else:
                raise ValueError(f"不支援的底稿檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取底稿檔案: {len(df)} 行")
            return df
            
        except Exception as e:
            self.logger.error(f"讀取底稿檔案失敗: {e}")
            return None
    
    def _read_closing_list(self, file_path: str) -> List[str]:
        """讀取關單清單"""
        try:
            if file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
                # 假設關單清單在第一欄
                closing_list = df.iloc[:, 0].dropna().astype(str).tolist()
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str)
                closing_list = df.iloc[:, 0].dropna().astype(str).tolist()
            else:
                raise ValueError(f"不支援的關單清單檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取關單清單: {len(closing_list)} 項")
            return closing_list
            
        except Exception as e:
            self.logger.error(f"讀取關單清單失敗: {e}")
            return []
    
    def _extract_month_from_filename(self, filename: str) -> int:
        """從檔案名稱提取月份"""
        import re
        
        try:
            # 嘗試從檔案名稱中提取YYYYMM格式的日期
            match = re.search(r'(\d{6})', filename)
            if match:
                date_str = match.group(1)
                month = int(date_str[4:6])  # 取月份部分
                return month
            
            # 如果沒找到，嘗試其他格式
            match = re.search(r'(\d{4})(\d{2})', filename)
            if match:
                month = int(match.group(2))
                return month
            
            # 預設返回當前月份
            from datetime import datetime
            return datetime.now().month
            
        except Exception as e:
            self.logger.warning(f"無法從檔案名稱提取月份: {e}")
            return datetime.now().month
    
    def _convert_month_to_file_date(self, month: int) -> int:
        """將月份轉換為檔案日期格式"""
        from datetime import datetime
        current_year = datetime.now().year
        return current_year * 100 + month
    
    def _save_output(self, df: pd.DataFrame, original_filename: str) -> Optional[str]:
        """保存輸出檔案"""
        try:
            import os
            
            # 生成輸出檔案名稱
            base_name = os.path.splitext(original_filename)[0]
            output_filename = f"{base_name}_processed_{self.entity_type}.xlsx"
            
            # 創建輸出目錄
            output_dir = "output"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            output_path = os.path.join(output_dir, output_filename)
            
            # 保存Excel檔案
            df.to_excel(output_path, index=False)
            
            self.logger.info(f"輸出檔案已保存: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存輸出檔案失敗: {e}")
            return None

    def calculate_month(self, m):
        if m == 1:
            return 12
        else:
            return m - 1
        
    def process_spt_specific(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理SPT特有邏輯(僅當entity_type為SPT時調用)
        
        Args:
            df: PO數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        if self.entity_type != 'SPT':
            return df
            
        try:
            # Project含SPX, Remarked by FN=SPX
            df.loc[df['Product Code'].str.contains('(?i)SPX'), 'Remarked by FN'] = 'SPX'
            df.loc[df['Remarked by FN'] == 'SPX', '是否估計入帳'] = "N"
            
            # 處理分潤
            self._update_commission_data(df)
            
            self.logger.info("成功處理SPT特有邏輯")
            return df
            
        except Exception as e:
            self.logger.error(f"處理SPT特有邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPT特有邏輯時出錯")
    
    def _update_commission_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """更新分潤數據
        
        Args:
            df: PO數據框
            
        Returns:
            pd.DataFrame: 更新後的數據框
        """
        try:
            def update_remark(df, type_=True):
                if type_:
                    keywords = '(?i)Affiliate commission|Shopee commission|蝦皮分潤計劃會員分潤金'
                    k1, k2 = 'Affiliate分潤合作', '品牌加碼'
                    gl_value = '650022'
                    product_code = 'EC_SPE_COM'
                else:
                    keywords = '(?i)AMS commission'
                    k1, k2 = 'Affiliate分潤合作', '品牌加碼'
                    gl_value = '650019'
                    product_code = 'EC_AMS_COST'
                
                # 條件
                condition = df['Item Description'].str.contains(keywords) | (
                    df['Item Description'].str.contains(k1) &
                    (~df['Item Description'].str.contains(k2)) if type_ else
                    df['Item Description'].str.contains(k2)
                )
                
                # 更新 Remarked by FN, GL#, Product code
                df.loc[condition, 'Remarked by FN'] = '分潤'
                df.loc[condition, 'GL#'] = gl_value
                df.loc[condition, 'Account code'] = gl_value
                df.loc[condition, 'Product code_c'] = product_code
                
                return df
            
            # 分兩種情況更新分潤數據
            df = update_remark(df)
            df = update_remark(df, type_=False)
            
            # 設置分潤估計入帳
            df.loc[(((df['GL#'] == '650022') | (df['GL#'] == '650019')) &
                   (df['Remarked by FN'] == '分潤') &
                   (df['PO狀態'].str.contains('已完成'))), '是否估計入帳'] = "Y"
            
            return df
            
        except Exception as e:
            self.logger.error(f"更新分潤數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("更新分潤數據時出錯")