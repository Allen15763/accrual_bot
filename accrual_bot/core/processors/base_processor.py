"""
基礎數據處理器
提供PO和PR處理器的共同功能
"""

import os
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime

try:
    # 從 accrual_bot/core/processors/ 到 accrual_bot/utils/ 需要向上兩層
    from ...utils import (
        config_manager, get_logger, clean_nan_values, format_numeric_columns,
        parse_date_string, extract_date_range_from_description, create_mapping_dict,
        safe_string_operation, DEFAULT_DATE_RANGE, EXCEL_FORMAT, get_unique_filename
    )
    from ...utils.config import REF_PATH_MOB, REF_PATH_SPT
    from ...data.importers.async_data_importer import AsyncDataImporter
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from utils import (
        config_manager, get_logger, clean_nan_values, format_numeric_columns,
        parse_date_string, extract_date_range_from_description, create_mapping_dict,
        safe_string_operation, DEFAULT_DATE_RANGE, EXCEL_FORMAT, get_unique_filename
    )
    from utils.config import REF_PATH_MOB, REF_PATH_SPT
    from data.importers.async_data_importer import AsyncDataImporter


class BaseDataProcessor:
    """處理數據的基類，抽象出PR和PO處理器的共同功能"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化數據處理器
        
        Args:
            entity_type: 實體類型，'MOB'、'SPT'或'SPX'
        """
        self.entity_type = entity_type.upper()
        self.logger = get_logger(self.__class__.__name__)
        
        # 從配置中加載設定
        self._load_entity_config()
        
        self.logger.info(f"初始化 {self.entity_type} 數據處理器")

        self.config_manager = config_manager
    
    def _load_entity_config(self) -> None:
        """加載實體特定的配置"""
        try:
            # 加載FA帳戶列表
            self.fa_accounts = config_manager.get_fa_accounts(self.entity_type.lower())
            
            # 加載正規表達式模式
            self.regex_patterns = config_manager.get_regex_patterns()
            
            # 加載透視表配置（如果需要）
            if hasattr(self, '_load_pivot_config'):
                self._load_pivot_config()
                
            self.logger.debug(f"載入 {self.entity_type} 配置完成")
            
        except Exception as e:
            self.logger.error(f"載入配置時出錯: {str(e)}", exc_info=True)
            # 設置預設值
            self.fa_accounts = []
            self.regex_patterns = {}
    
    def clean_nan_values(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        清理DataFrame中的nan值
        
        Args:
            df: 要處理的DataFrame
            columns: 要清理nan值的列名列表
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            return clean_nan_values(df, columns)
        except Exception as e:
            self.logger.error(f"清理nan值時出錯: {str(e)}", exc_info=True)
            return df
    
    def reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化日期字段
        
        Args:
            df: 要處理的DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 格式化提交日期
            if 'Submission Date' in df_copy.columns:
                df_copy['Submission Date'] = df_copy['Submission Date'].apply(
                    lambda x: parse_date_string(x, '%d-%b-%y', '%Y-%m-%d')
                ).astype(str)
            
            # 格式化預期接收月份
            if 'Expected Receive Month' in df_copy.columns:
                df_copy['Expected Receive Month'] = df_copy['Expected Receive Month'].apply(
                    lambda x: parse_date_string(x, '%b-%y', '%Y-%m-%d')
                ).astype(str)
            
            # 格式化創建日期
            create_date_col = 'PR Create Date' if 'PR Create Date' in df_copy.columns else 'PO Create Date'
            if create_date_col in df_copy.columns:
                df_copy[create_date_col] = df_copy[create_date_col].apply(
                    lambda x: parse_date_string(x, output_format='%Y/%m/%d') if pd.notna(x) else ''
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"格式化日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("日期格式化時出錯")
    
    def format_numeric_columns_safely(self, df: pd.DataFrame, int_cols: List[str], 
                                      float_cols: List[str]) -> pd.DataFrame:
        """
        安全地格式化數值列，包括千分位
        
        Args:
            df: 要處理的DataFrame
            int_cols: 整數列名列表
            float_cols: 浮點數列名列表
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            return format_numeric_columns(df, int_cols, float_cols)
        except Exception as e:
            self.logger.error(f"格式化數值列時出錯: {str(e)}", exc_info=True)
            raise ValueError("數值格式化時出錯")
    
    def parse_date_from_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        從描述欄位解析日期範圍
        
        Args:
            df: 包含Item Description的DataFrame
            
        Returns:
            pd.DataFrame: 添加了解析結果的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 將Expected Receive Month轉換為數值格式以便比較
            if 'Expected Receive Month' in df_copy.columns:
                df_copy['Expected Received Month_轉換格式'] = pd.to_datetime(
                    df_copy['Expected Receive Month'], 
                    format='%b-%y',
                    errors='coerce'
                ).dt.strftime('%Y%m').fillna('0').astype('int32')
            
            # 解析Item Description中的日期範圍
            if 'Item Description' in df_copy.columns:
                df_copy['YMs of Item Description'] = df_copy['Item Description'].apply(
                    lambda x: extract_date_range_from_description(x, self.regex_patterns)
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"解析描述中的日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("解析日期時出錯")
    
    def evaluate_status_based_on_dates(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """
        根據日期範圍評估狀態
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態' 或 'PO狀態')
            
        Returns:
            pd.DataFrame: 更新了狀態的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 確保日期解析已完成
            if 'YMs of Item Description' not in df_copy.columns:
                df_copy = self.parse_date_from_description(df_copy)
            
            # 定義條件邏輯
            na_mask = df_copy[status_col].isna() | (df_copy[status_col] == 'nan') | (df_copy[status_col] == '')
            
            # 提取日期範圍
            date_ranges = df_copy['YMs of Item Description'].str.split(',', expand=True)
            if date_ranges.shape[1] >= 2:
                start_dates = pd.to_numeric(date_ranges[0], errors='coerce').fillna(0).astype('int32')
                end_dates = pd.to_numeric(date_ranges[1], errors='coerce').fillna(0).astype('int32')
            else:
                start_dates = pd.Series([0] * len(df_copy), index=df_copy.index)
                end_dates = pd.Series([0] * len(df_copy), index=df_copy.index)
            
            expected_month = df_copy.get('Expected Received Month_轉換格式', 
                                         pd.Series([0] * len(df_copy), index=df_copy.index))
            file_date = df_copy.get('檔案日期', 
                                    pd.Series([0] * len(df_copy), index=df_copy.index))
            
            if 'Received Quantity' in df_copy.columns:
                # for PO
                conditions = [
                    # 條件1：格式錯誤
                    (df_copy['YMs of Item Description'] == DEFAULT_DATE_RANGE) & na_mask,
                    
                    # 條件2：已完成（日期在範圍內且預期接收月已過, EQ=RQ, EBA=0）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask &
                    (df_copy['Entry Quantity'] == df_copy['Received Quantity']) &
                    (df_copy['Entry Billed Amount'].astype(float) == 0),

                    # 全付完，未關單（日期在範圍內且預期接收月已過, EQ=RQ, EBA!=0, EA-EBA=0）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask &
                    (df_copy['Entry Quantity'] == df_copy['Received Quantity']) &
                    (df_copy['Entry Billed Amount'].astype(float) != 0) &
                    (df_copy['Entry Amount'].astype(float) - df_copy['Entry Billed Amount'].astype(float) == 0),

                    # 已完成但有未付款部分（日期在範圍內且預期接收月已過, EQ=RQ, EBA!=0, EA-EBA!=0）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask &
                    (df_copy['Entry Quantity'] == df_copy['Received Quantity']) &
                    (df_copy['Entry Billed Amount'].astype(float) != 0) &
                    (df_copy['Entry Amount'].astype(float) - df_copy['Entry Billed Amount'].astype(float) != 0),

                    # 需檢查收貨（日期在範圍內且預期接收月已過, EQ!=RQ）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask &
                    (df_copy['Entry Quantity'] != df_copy['Received Quantity']),
                    
                    # 條件3：未完成（日期在範圍內但預期接收月尚未到）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month > file_date)) & na_mask
                ]
                
                choices = ['格式錯誤', '已完成', '全付完，未關單', '已完成但有未付款部分', 'Check收貨',
                           '未完成']
                
                # 應用條件
                df_copy[status_col] = np.select(
                    conditions, 
                    choices, 
                    default=df_copy[status_col]
                )
                
                # 處理其他情況
                df_copy[status_col] = df_copy[status_col].fillna('error(Description Period is out of ERM)')
                
                return df_copy
            else:
                # for PR
                conditions = [
                    # 條件1：格式錯誤
                    (df_copy['YMs of Item Description'] == DEFAULT_DATE_RANGE) & na_mask,
                    
                    # 條件2：已完成（日期在範圍內且預期接收月已過, EQ=RQ, EBA=0）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask,

                    # 條件3：未完成（日期在範圍內但預期接收月尚未到）
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month > file_date)) & na_mask
                ]
                
                choices = ['格式錯誤', '已完成', '未完成']
                
                # 應用條件
                df_copy[status_col] = np.select(
                    conditions, 
                    choices, 
                    default=df_copy[status_col]
                )
                
                # 處理其他情況
                df_copy[status_col] = df_copy[status_col].fillna('error(Description Period is out of ERM)')
                
                return df_copy
            
        except Exception as e:
            self.logger.error(f"根據日期評估狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("根據日期評估狀態時出錯")
    
    def evaluate_status_based_on_dates_integrated(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """
        根據日期範圍評估狀態 - 整合版（包含完整的11個條件）
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態' 或 'PO狀態')
            
        Returns:
            pd.DataFrame: 更新了狀態的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 確保日期解析已完成
            if 'YMs of Item Description' not in df_copy.columns:
                df_copy = self.parse_date_from_description(df_copy)
            
            # === 基礎條件準備 ===
            na_mask = df_copy[status_col].isna() | (df_copy[status_col] == 'nan') | (df_copy[status_col] == '')
            
            # 提取日期範圍
            date_ranges = df_copy['YMs of Item Description'].str.split(',', expand=True)
            if date_ranges.shape[1] >= 2:
                start_dates = pd.to_numeric(date_ranges[0], errors='coerce').fillna(0).astype('int32')
                end_dates = pd.to_numeric(date_ranges[1], errors='coerce').fillna(0).astype('int32')
            else:
                start_dates = pd.Series([0] * len(df_copy), index=df_copy.index)
                end_dates = pd.Series([0] * len(df_copy), index=df_copy.index)
            
            expected_month = df_copy.get('Expected Received Month_轉換格式', 
                                         pd.Series([0] * len(df_copy), index=df_copy.index))
            file_date = df_copy.get('檔案日期', 
                                    pd.Series([0] * len(df_copy), index=df_copy.index))
            
            # === PO狀態判斷邏輯 ===
            if 'Received Quantity' in df_copy.columns:  # PO處理
                
                # 前置條件：Remarked by Procurement != 'error'
                base_mask = (df_copy.get('Remarked by Procurement', '') != 'error') & na_mask
                
                # 日期範圍條件
                in_range_past = \
                    expected_month.between(start_dates, end_dates, inclusive='both') & (expected_month <= file_date)
                in_range_future = \
                    expected_month.between(start_dates, end_dates, inclusive='both') & (expected_month > file_date)
                out_of_range = ~expected_month.between(start_dates, end_dates, inclusive='both')
                
                # 數量條件
                eq_equals_rq = df_copy['Entry Quantity'] == df_copy['Received Quantity']
                eq_not_equals_rq = df_copy['Entry Quantity'] != df_copy['Received Quantity']
                rq_is_zero = pd.to_numeric(df_copy['Received Quantity'], errors='coerce').fillna(0) == 0
                rq_not_zero = pd.to_numeric(df_copy['Received Quantity'], errors='coerce').fillna(0) != 0
                
                # 金額條件
                eba_is_zero = pd.to_numeric(df_copy['Entry Billed Amount'], errors='coerce').fillna(0) == 0
                eba_not_zero = pd.to_numeric(df_copy['Entry Billed Amount'], errors='coerce').fillna(0) != 0
                ea_minus_eba_zero = (pd.to_numeric(df_copy['Entry Amount'], errors='coerce').fillna(0) - 
                                     pd.to_numeric(df_copy['Entry Billed Amount'], errors='coerce').fillna(0)) == 0
                ea_minus_eba_not_zero = (pd.to_numeric(df_copy['Entry Amount'], errors='coerce').fillna(0) - 
                                         pd.to_numeric(df_copy['Entry Billed Amount'], errors='coerce').fillna(0)) != 0
                
                # 公司條件
                is_mobtw = df_copy.get('Company', '') == 'MOBTW'
                is_spttw = df_copy.get('Company', '') == 'SPTTW'
                
                # 格式檢查
                format_error = df_copy['YMs of Item Description'] == '100001,100002'
                
                # === 建立11個條件（按優先順序） ===
                conditions = []
                choices = []
                
                # 條件0：格式錯誤（特殊處理）
                conditions.append(format_error & na_mask)
                choices.append('格式錯誤')
                
                # --- 日期在範圍內且已過（條件1-4）---
                # 條件1：已完成（EQ=RQ, EBA=0）
                conditions.append(base_mask & in_range_past & eq_equals_rq & eba_is_zero)
                choices.append('已完成')
                
                # 條件2：全付完，未關單（EQ=RQ, EBA!=0, EA-EBA=0）
                conditions.append(base_mask & in_range_past & eq_equals_rq & eba_not_zero & ea_minus_eba_zero)
                choices.append('全付完，未關單')
                
                # 條件3：已完成但有未付款部分（EQ=RQ, EBA!=0, EA-EBA!=0）
                conditions.append(base_mask & in_range_past & eq_equals_rq & eba_not_zero & ea_minus_eba_not_zero)
                choices.append('已完成')
                
                # 條件4：Check收貨（EQ!=RQ）
                conditions.append(base_mask & in_range_past & eq_not_equals_rq)
                choices.append('Check收貨')
                
                # --- 日期在範圍內但未到（條件5-7）---
                if self.entity_type == 'MOB':
                    # 條件5：未完成（MOBTW）
                    conditions.append(base_mask & in_range_future & is_mobtw)
                    choices.append('未完成')
                
                elif self.entity_type == 'SPT':
                    # 條件6：未完成（SPTTW，RQ=0）
                    conditions.append(base_mask & in_range_future & is_spttw & rq_is_zero)
                    choices.append('未完成')
                    
                    # 條件7：提早完成?（SPTTW，RQ!=0）
                    conditions.append(base_mask & in_range_future & is_spttw & rq_not_zero)
                    choices.append('提早完成?')
                
                # --- 日期不在範圍內（條件8-11）---
                if self.entity_type == 'MOB':
                    # 條件8：範圍錯誤（MOBTW）
                    conditions.append(base_mask & out_of_range & is_mobtw & ~format_error)
                    choices.append('error(Description Period is out of ERM)')
                
                elif self.entity_type == 'SPT':
                    # 條件9：已完成ERM（SPTTW，RQ!=0，EQ=RQ）
                    conditions.append(base_mask & out_of_range & is_spttw & ~format_error & rq_not_zero & eq_equals_rq)
                    choices.append('已完成ERM')
                    
                    # 條件10：部分完成ERM（SPTTW，RQ!=0，EQ!=RQ）
                    conditions.append(
                        base_mask & out_of_range & is_spttw & ~format_error & rq_not_zero & eq_not_equals_rq)
                    choices.append('部分完成ERM')
                    
                    # 條件11：未完成ERM（SPTTW，RQ=0）
                    conditions.append(base_mask & out_of_range & is_spttw & ~format_error & rq_is_zero)
                    choices.append('未完成ERM')
                
                # === 應用條件 ===
                df_copy[status_col] = np.select(conditions, choices, default=df_copy[status_col])
                
                # 處理剩餘的空值（不符合任何條件的情況）
                remaining_mask = df_copy[status_col].isna() | (df_copy[status_col] == '')
                df_copy.loc[remaining_mask, status_col] = 'error(Description Period is out of ERM)'
                
            else:  # PR處理
                # PR的邏輯相對簡單，保持原有實現
                conditions = [
                    # 條件1：格式錯誤
                    (df_copy['YMs of Item Description'] == '100001,100002') & na_mask,
                    
                    # 條件2：已完成
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month <= file_date)) & na_mask,
                    
                    # 條件3：未完成
                    (expected_month.between(start_dates, end_dates, inclusive='both') & 
                     (expected_month > file_date)) & na_mask
                ]
                
                choices = ['格式錯誤', '已完成', '未完成']
                
                df_copy[status_col] = np.select(conditions, choices, default=df_copy[status_col])
                df_copy[status_col] = df_copy[status_col].fillna('error(Description Period is out of ERM)')
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"根據日期評估狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("根據日期評估狀態時出錯")

    def update_estimation_based_on_status(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """
        根據狀態更新估計入帳標識
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態' 或 'PO狀態')
            
        Returns:
            pd.DataFrame: 更新了估計入帳的DataFrame
        """
        try:
            df_copy = df.copy()

            # 定義不應估計的採購備註
            procurement_no_accrual = ['未完成', '勞報', 'Payroll', '不預估', 'error', 'SPX']
            
            # 定義不應估計的狀態
            status_no_accrual = ['不預估', '未完成', 'Payroll', '待關單', '未完成ERM', 
                                 '格式錯誤', 'error(Description Period is out of ERM)', 'Check收貨',
                                 '提早完成?']
            
            # 邏輯1：先檢查採購備註（採購備註有最高優先權）
            mask_procurement_no = df_copy['Remarked by Procurement'].isin(procurement_no_accrual)
            df_copy.loc[mask_procurement_no, '是否估計入帳'] = 'N'
            
            # 邏輯2：再根據狀態設置（但不覆蓋已設為N的）
            mask_completed = (
                (df_copy[status_col] == '已完成') & 
                (~mask_procurement_no) &  # 排除採購說不行的
                (df_copy['是否估計入帳'].isna() | (df_copy['是否估計入帳'] == ''))
            )
            df_copy.loc[mask_completed, '是否估計入帳'] = 'Y'
            
            # 邏輯3：狀態為不預估清單中的，設為N
            mask_status_no = df_copy[status_col].isin(status_no_accrual)
            df_copy.loc[mask_status_no, '是否估計入帳'] = 'N'
            
            # 邏輯4：採購備註為"已完成"且狀態允許的，設為Y
            mask_procurement_yes = (
                (df_copy['Remarked by Procurement'] == '已完成') &
                (~df_copy[status_col].isin(status_no_accrual)) &
                (df_copy['是否估計入帳'].isna() | (df_copy['是否估計入帳'] == ''))
            )
            df_copy.loc[mask_procurement_yes, '是否估計入帳'] = 'Y'
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"根據狀態更新估計入帳時出錯: {str(e)}", exc_info=True)
            raise ValueError("根據狀態更新估計入帳時出錯")
    
    def judge_ac_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        判斷科目代碼
        
        Args:
            df: 要處理的DataFrame
            
        Returns:
            pd.DataFrame: 更新了科目代碼的DataFrame
        """
        try:
            df_copy = df.copy()
            
            # 設置Account code
            df_copy['Account code'] = np.where(
                df_copy['是否估計入帳'] == 'Y', 
                df_copy['GL#'], 
                np.nan
            )
            
            # 設置是否為FA
            if 'GL#' in df_copy.columns:
                df_copy['是否為FA'] = np.where(
                    df_copy['GL#'].isin(self.fa_accounts), 
                    'Y', 
                    pd.NA
                )
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"判斷科目代碼時出錯: {str(e)}", exc_info=True)
            raise ValueError("判斷科目代碼時出錯")
    
    def convert_dep_code(self, df: pd.DataFrame) -> pd.Series:
        """
        轉換部門代碼（主要用於SPT實體）
        
        Args:
            df: DataFrame
            
        Returns:
            pd.Series: 轉換後的部門代碼
        """
        try:
            df_copy = df.copy()
            result = pd.Series('', index=df_copy.index)
            
            # 條件1：是否估計入帳為Y且Account code開頭為1, 2, 9
            condition1 = (
                (df_copy['是否估計入帳'] == 'Y') & 
                (df_copy['Account code'].astype(str).str[0].isin(['1', '2', '9']))
            )
            
            # 條件2：是否估計入帳為Y且Account code不是開頭為5或4
            condition2 = (
                (df_copy['是否估計入帳'] == 'Y') & 
                (~df_copy['Account code'].astype(str).str[0].isin(['5', '4']))
            )
            
            # 條件3：是否估計入帳為Y且Account code開頭為5或4
            condition3 = (
                (df_copy['是否估計入帳'] == 'Y') & 
                (df_copy['Account code'].astype(str).str[0].isin(['5', '4']))
            )
            
            # 應用條件
            result.loc[condition1] = '000'
            result.loc[condition2 & ~condition1] = \
                df_copy.loc[condition2 & ~condition1, 'Department'].astype(str).str[:3]
            result.loc[condition3] = '000'
            
            return result
            
        except Exception as e:
            self.logger.error(f"轉換部門代碼時出錯: {str(e)}", exc_info=True)
            return pd.Series('', index=df.index)
    
    def get_mapping_dict(self, df: pd.DataFrame, key_col: str, value_col: str) -> Dict[str, Any]:
        """
        獲取映射字典
        
        Args:
            df: DataFrame
            key_col: 鍵列名
            value_col: 值列名
            
        Returns:
            Dict[str, Any]: 映射字典
        """
        try:
            return create_mapping_dict(df, key_col, value_col)
        except Exception as e:
            self.logger.error(f"創建映射字典時出錯: {str(e)}", exc_info=True)
            return {}
    
    def export_file(self, df: pd.DataFrame, date: int, file_prefix: str, 
                    output_dir: str = None) -> None:
        """
        導出文件
        
        Args:
            df: 要導出的DataFrame
            date: 日期值
            file_prefix: 文件前綴
            output_dir: 輸出目錄
            
        Returns:
            None
        """
        try:
            # 清理DataFrame中的<NA>值
            df_export = df.replace('<NA>', np.nan)
            
            # 生成文件名
            file_name = f"{date}-{file_prefix} Compare Result.xlsx"
            
            if output_dir:
                file_path = os.path.join(output_dir, file_name)
            else:
                file_path = file_name
            
            # 確保文件名唯一
            file_path = get_unique_filename(os.path.dirname(file_path) or '.', 
                                            os.path.basename(file_path))
            
            self.logger.info(f"正在導出文件: {file_path}")
            
            try:
                # 嘗試使用UTF-8編碼
                df_export.to_excel(
                    file_path, 
                    index=EXCEL_FORMAT['INDEX'], 
                    encoding=EXCEL_FORMAT['ENCODING'], 
                    engine=EXCEL_FORMAT['ENGINE']
                )
                self.logger.info(f"成功導出文件: {file_path}")
            except Exception:
                # 如果UTF-8編碼失敗，使用默認編碼
                df_export.to_excel(
                    file_path, 
                    index=EXCEL_FORMAT['INDEX'], 
                    engine=EXCEL_FORMAT['ENGINE']
                )
                self.logger.info(f"成功導出文件(使用默認編碼): {file_path}")
                
        except Exception as e:
            self.logger.error(f"導出文件時出錯: {str(e)}", exc_info=True)
            raise ValueError("導出文件時出錯")
    
    def validate_required_columns(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        驗證DataFrame是否包含必要的列
        
        Args:
            df: 要驗證的DataFrame
            required_columns: 必要列名列表
            
        Returns:
            bool: 是否包含所有必要列
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            self.logger.error(f"DataFrame缺少必要的列: {missing_columns}")
            return False
        
        return True
    
    def log_data_info(self, df: pd.DataFrame, stage: str) -> None:
        """
        記錄數據信息
        
        Args:
            df: DataFrame
            stage: 處理階段
        """
        try:
            self.logger.info(f"{stage} - 數據形狀: {df.shape}")
            if hasattr(df, 'memory_usage'):
                memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                self.logger.debug(f"{stage} - 記憶體使用: {memory_mb:.2f} MB")
        except Exception as e:
            self.logger.warning(f"記錄數據信息時出錯: {str(e)}")

    def import_reference_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """導入參考數據

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 科目參考數據和負債參考數據
        """
        try:
            ref_path, ref_constant = \
                ('ref_path_mob', REF_PATH_MOB) if self.entity_type == 'MOB' else ('ref_path_spt', REF_PATH_SPT)
            url = self.config_manager._config_data.get('PATHS').get(ref_path, ref_constant)
            
            ac_ref = pd.read_excel(url, dtype=str)
            
            ref_for_ac = ac_ref.iloc[:, 1:3]
            ref_for_liability = ac_ref.loc[:, ['Account', 'Liability']]
            
            return ref_for_ac, ref_for_liability
            
        except Exception as e:
            self.logger.error(f"導入參考數據時出錯: {str(e)}", exc_info=True)
            raise

    def give_status_stage_1(self, df: pd.DataFrame, df_spx_closing: pd.DataFrame, date) -> pd.DataFrame:
        """給予第一階段狀態 - SPX特有邏輯
        
        Args:
            df: PO/PR DataFrame
            df_spx_closing: SPX關單數據DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            utility_suppliers = self.config_manager.get(self.entity_type, 'utility_suppliers')
            if 'PO狀態' in df.columns:
                tag_column = 'PO狀態'
                # 依據已關單條件取得對應的PO#
                c1, c2 = self.is_closed_spx(df_spx_closing)
                to_be_close = df_spx_closing.loc[c1, 'po_no'].unique() if c1.any() else []
                closed = df_spx_closing.loc[c2, 'po_no'].unique() if c2.any() else []
                
                # 定義「上月FN」備註關單條件
                remarked_close_by_fn_last_month = (
                    df['Remarked by 上月 FN'].str.contains('刪|關', na=False) | 
                    df['Remarked by 上月 FN PR'].astype(str).str.contains('刪|關', na=False)
                )
                
                # 統一轉換日期格式
                df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
                df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN PR'])
                
                # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
                cond1 = \
                    df['Item Description'].str.contains(self.config_manager.get(self.entity_type, 'deposit_keywords'), 
                                                        na=False)
                is_fa = df['GL#'].astype(str) == self.config_manager.get('FA_ACCOUNTS', self.entity_type, '199999')
                cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
                df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                    self.config_manager.get(self.entity_type, 'deposit_keywords_label')
                
                # 條件2：供應商與類別對應，做GL調整
                bao_supplier: list = self.config_manager.get_list(self.entity_type, 'bao_supplier')
                bao_categories: list = self.config_manager.get_list(self.entity_type, 'bao_categories')
                cond2 = (df['PO Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
                df.loc[cond2, tag_column] = 'GL調整'
                
                # 條件3：該PO#在待關單清單中
                cond3 = df['PO#'].astype(str).isin([str(x) for x in to_be_close])
                df.loc[cond3, tag_column] = '待關單'
                
                # 條件4：該PO#在已關單清單中
                cond4 = df['PO#'].astype(str).isin([str(x) for x in closed])
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
                    (df['Remarked by 上月 FN PR'].astype(str).str.contains('入FA', na=False)) & 
                    (~df['Remarked by 上月 FN PR'].astype(str).str.contains('部分完成', na=False))
                )
                if cond7.any():
                    extracted_pr = self.extract_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                    df.loc[cond7, tag_column] = extracted_pr

                # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
                cond8 = df['PO Supplier'].fillna('system_filled').str.contains(utility_suppliers)
                df.loc[cond8, tag_column] = '授扣GL調整'

                # 費用類按申請人篩選
                is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
                ops_rent: str = self.config_manager.get(self.entity_type, 'ops_for_rent')
                account_rent: str = self.config_manager.get(self.entity_type, 'account_rent')
                ops_intermediary: str = self.config_manager.get(self.entity_type, 'ops_for_intermediary')
                ops_other: str = self.config_manager.get(self.entity_type, 'ops_for_other')
                
                mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
                mask_account_rent = df['GL#'] == account_rent
                mask_ops_rent = df['PR Requester'] == ops_rent
                mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype(int) == date
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
                        ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('int32')) &
                         (df['Expected Received Month_轉換格式'] > date) &
                         (df['YMs of Item Description'] != '100001,100002')
                         ) |
                        ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('int32')) &
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
                kiosk_suppliers: list = self.config_manager.get_list(self.entity_type, 'kiosk_suppliers')
                locker_suppliers: list = self.config_manager.get_list(self.entity_type, 'locker_suppliers')
                asset_suppliers: list = kiosk_suppliers + locker_suppliers

                # Exclude both general '入FA' but Include specific patterns(部分入)
                po_general_fa = df['Remarked by 上月 FN'].str.contains('入FA', na=False)
                po_specific_pattern = df['Remarked by 上月 FN'].str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True)

                pr_general_fa = df['Remarked by 上月 FN PR'].astype(str).str.contains('入FA', na=False)
                pr_specific_pattern = (df['Remarked by 上月 FN PR']
                                       .astype(str).str.contains(r'部分完成.*\d{6}入FA', na=False, regex=True))

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
                    df['Remarked by 上月 FN'].astype(str).str.contains('刪|關', na=False)
                )
                
                # 統一轉換日期格式
                df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
                
                # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是FA相關科目
                cond1 = \
                    df['Item Description'].str.contains(self.config_manager.get(self.entity_type, 'deposit_keywords'), 
                                                        na=False)
                is_fa = df['GL#'].astype(str) == self.config_manager.get('FA_ACCOUNTS', self.entity_type, '199999')
                cond_exclude = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA
                df.loc[cond1 & ~is_fa & ~cond_exclude, tag_column] = \
                    self.config_manager.get(self.entity_type, 'deposit_keywords_label')
                
                # 條件2：供應商與類別對應，做GL調整
                bao_supplier: list = self.config_manager.get_list(self.entity_type, 'bao_supplier')
                bao_categories: list = self.config_manager.get_list(self.entity_type, 'bao_categories')
                cond2 = (df['PR Supplier'].isin(bao_supplier)) & (df['Category'].isin(bao_categories))
                df.loc[cond2, tag_column] = 'GL調整'
                
                # 條件3：該PR#在待關單清單中
                cond3 = df['PR#'].astype(str).isin([str(x) for x in to_be_close])
                df.loc[cond3, tag_column] = '待關單'
                
                # 條件4：該PR#在已關單清單中
                cond4 = df['PR#'].astype(str).isin([str(x) for x in closed])
                df.loc[cond4, tag_column] = '已關單'
                
                # 條件5：上月FN備註含有「刪」或「關」
                cond5 = remarked_close_by_fn_last_month
                df.loc[cond5, tag_column] = '參照上月關單'
                
                # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態(xxxxxx入FA)
                # 部分完成xxxxxx入FA不計入，前期FN備註如果是部分完成的會掉到erm邏輯判斷
                cond6 = (
                    (df['Remarked by 上月 FN'].astype(str).str.contains('入FA', na=False)) & 
                    (~df['Remarked by 上月 FN'].astype(str).str.contains('部分完成', na=False))
                )
                if cond6.any():
                    extracted_fn = self.extract_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                    df.loc[cond6, tag_column] = extracted_fn
                
                # 條件8：該筆資料supplier是"台電"、"台水"、"北水"等公共費用
                cond8 = df['PR Supplier'].fillna('system_filled').str.contains(utility_suppliers)
                df.loc[cond8, tag_column] = '授扣GL調整'

                # 費用類按申請人篩選
                is_non_labeled = (df[tag_column].isna()) | (df[tag_column] == '') | (df[tag_column] == 'nan')
                ops_rent: str = self.config_manager.get(self.entity_type, 'ops_for_rent')
                account_rent: str = self.config_manager.get(self.entity_type, 'account_rent')
                ops_intermediary: str = self.config_manager.get(self.entity_type, 'ops_for_intermediary')
                ops_other: str = self.config_manager.get(self.entity_type, 'ops_for_other')
                
                mask_erm_equals_current = df['Expected Received Month_轉換格式'] == date
                mask_account_rent = df['GL#'] == account_rent
                mask_ops_rent = df['Requester'] == ops_rent
                mask_descerm_equals_current = df['YMs of Item Description'].str[:6].astype(int) == date
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
                        ((df['Expected Received Month_轉換格式'] <= df['YMs of Item Description'].str[:6].astype('int32')) &
                         (df['Expected Received Month_轉換格式'] > date) &
                         (df['YMs of Item Description'] != '100001,100002')
                         ) |
                        ((df['Expected Received Month_轉換格式'] > df['YMs of Item Description'].str[:6].astype('int32')) &
                         (df['Expected Received Month_轉換格式'] > date) &
                         (df['YMs of Item Description'] != '100001,100002')
                         )
                    )

                )
                df.loc[uncompleted_rent, tag_column] = '未完成_租金'

                combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                    ((df['Expected Received Month_轉換格式'] == date) |
                     ((df['Expected Received Month_轉換格式'] < date) & (df['Remarked by 上月 FN']
                                                                     .astype(str).str.contains('已完成')))
                     )
                df.loc[combined_cond, tag_column] = '已完成_intermediary'
                
                combined_cond = is_non_labeled & mask_ops_intermediary & mask_desc_contains_intermediary & \
                    (df['Expected Received Month_轉換格式'] > date)
                df.loc[combined_cond, tag_column] = '未完成_intermediary'

                # PR的智取櫃與繳費機，不會在PR驗收不估
                kiosk_suppliers: list = self.config_manager.get_list(self.entity_type, 'kiosk_suppliers')
                locker_suppliers: list = self.config_manager.get_list(self.entity_type, 'locker_suppliers')
                asset_suppliers: list = kiosk_suppliers + locker_suppliers
                ignore_closed = ~df[tag_column].str.contains('關', na=False)
                mask = ((df['PR Supplier'].isin(asset_suppliers)) & 
                        (ignore_closed))
                df.loc[mask, tag_column] = '智取櫃與繳費機'

                self.logger.info("成功給予第一階段狀態")
                return df
        
        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")
        
    def get_closing_note(self) -> pd.DataFrame:
        """獲取關單數據 - 優化版本支持並發處理
        
        Returns:
            pd.DataFrame: 關單數據框
        """
        try:
            # 獲取Google Sheets配置
            config = {
                'certificate_path': self.config_manager.get_credentials_config().get('certificate_path', None),
                'scopes': self.config_manager.get_credentials_config().get('scopes', None)
            }
            
            # 使用AsyncDataImporter導入SPX關單數據
            async_importer = AsyncDataImporter()
            combined_df = async_importer.import_spx_closing_list(config)
            
            self.logger.info(f"成功獲取關單數據，共 {len(combined_df)} 筆記錄")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"獲取關單數據時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
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
            return series.astype(str).str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
        except Exception as e:
            self.logger.error(f"轉換日期格式時出錯: {str(e)}", exc_info=True)
            return series
    