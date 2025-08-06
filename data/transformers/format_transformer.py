"""
格式轉換器

處理數值格式化、文字清理和標準化
"""

import re
import pandas as pd
import numpy as np
from typing import Union, Optional, Dict, List, Any
from decimal import Decimal, InvalidOperation
from ...utils.logging import Logger


class FormatTransformer:
    """格式轉換器類別"""
    
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        
        # 常見的部門名稱標準化映射
        self.department_mapping = {
            'hr': 'HR',
            'human resources': 'HR',
            'information technology': 'IT',
            'it': 'IT',
            'finance': 'Finance',
            'fin': 'Finance',
            'accounting': 'Accounting',
            'acc': 'Accounting',
            'operations': 'Operations',
            'ops': 'Operations',
            'sales': 'Sales',
            'marketing': 'Marketing',
            'mkt': 'Marketing',
            'procurement': 'Procurement',
            'logistics': 'Logistics',
            'log': 'Logistics'
        }
        
        # 幣別符號映射
        self.currency_symbols = {
            'TWD': 'NT$',
            'USD': '$',
            'HKD': 'HK$',
            'EUR': '€',
            'JPY': '¥',
            'CNY': '¥'
        }
    
    def clean_text_data(self, text: Union[str, int, float]) -> str:
        """清理文字數據
        
        Args:
            text: 要清理的文字
            
        Returns:
            str: 清理後的文字
        """
        if pd.isna(text) or text == '' or str(text).lower() == 'nan':
            return ''
        
        # 轉為字符串
        text = str(text).strip()
        
        # 移除多餘的空白字符
        text = re.sub(r'\s+', ' ', text)
        
        # 移除特殊控制字符
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # 移除前後空白
        text = text.strip()
        
        return text
    
    def format_currency(self, amount: Union[str, int, float], 
                       currency: str = 'TWD',
                       decimal_places: int = 2,
                       include_symbol: bool = True,
                       thousands_separator: bool = True) -> str:
        """格式化貨幣金額
        
        Args:
            amount: 金額
            currency: 幣別
            decimal_places: 小數位數
            include_symbol: 是否包含貨幣符號
            thousands_separator: 是否使用千分位分隔符
            
        Returns:
            str: 格式化後的金額字符串
        """
        if pd.isna(amount) or amount == '' or str(amount).lower() == 'nan':
            return '0.00' if not include_symbol else f"{self.currency_symbols.get(currency, '')}0.00"
        
        try:
            # 轉換為浮點數
            if isinstance(amount, str):
                # 清理字符串中的非數字字符（除了小數點和負號）
                amount = re.sub(r'[^\d.-]', '', amount)
            
            amount_float = float(amount)
            
            # 格式化數字
            if thousands_separator:
                formatted = f"{amount_float:,.{decimal_places}f}"
            else:
                formatted = f"{amount_float:.{decimal_places}f}"
            
            # 添加貨幣符號
            if include_symbol:
                symbol = self.currency_symbols.get(currency.upper(), currency)
                formatted = f"{symbol}{formatted}"
            
            return formatted
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"無法格式化金額 {amount}: {e}")
            return '0.00' if not include_symbol else f"{self.currency_symbols.get(currency, '')}0.00"
    
    def normalize_account_code(self, account_code: Union[str, int, float]) -> str:
        """標準化會計科目代碼
        
        Args:
            account_code: 會計科目代碼
            
        Returns:
            str: 標準化後的代碼
        """
        if pd.isna(account_code) or account_code == '' or str(account_code).lower() == 'nan':
            return ''
        
        # 轉為字符串並清理
        code = str(account_code).strip()
        
        # 移除非數字字符（會計科目通常是純數字）
        code = re.sub(r'[^\d]', '', code)
        
        # 確保長度一致（通常為4位數）
        if len(code) < 4 and code.isdigit():
            code = code.zfill(4)
        
        return code
    
    def standardize_department_name(self, department: Union[str, int, float]) -> str:
        """標準化部門名稱
        
        Args:
            department: 部門名稱
            
        Returns:
            str: 標準化後的部門名稱
        """
        if pd.isna(department) or department == '' or str(department).lower() == 'nan':
            return ''
        
        # 清理文字
        dept = self.clean_text_data(department).lower()
        
        # 查找映射
        for key, value in self.department_mapping.items():
            if key in dept:
                return value
        
        # 如果沒有找到映射，返回首字母大寫的形式
        return str(department).strip().title()
    
    def format_percentage(self, value: Union[str, int, float], decimal_places: int = 2) -> str:
        """格式化百分比
        
        Args:
            value: 數值（0-1 或 0-100）
            decimal_places: 小數位數
            
        Returns:
            str: 格式化後的百分比字符串
        """
        if pd.isna(value) or value == '' or str(value).lower() == 'nan':
            return '0.00%'
        
        try:
            val = float(value)
            
            # 如果值在0-1之間，假設已經是比例
            if 0 <= val <= 1:
                val *= 100
            
            return f"{val:.{decimal_places}f}%"
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"無法格式化百分比 {value}: {e}")
            return '0.00%'
    
    def normalize_boolean(self, value: Union[str, int, float, bool]) -> str:
        """標準化布林值
        
        Args:
            value: 布林值或相關字符串
            
        Returns:
            str: 標準化後的布林字符串
        """
        if pd.isna(value) or value == '':
            return ''
        
        val = str(value).lower().strip()
        
        # True值
        true_values = ['true', '1', 'yes', 'y', '是', '有', 'x']
        if val in true_values:
            return '是'
        
        # False值
        false_values = ['false', '0', 'no', 'n', '否', '無', '']
        if val in false_values:
            return '否'
        
        # 其他值直接返回
        return str(value).strip()
    
    def clean_numeric_string(self, value: Union[str, int, float]) -> Optional[float]:
        """清理數字字符串，轉換為浮點數
        
        Args:
            value: 數字字符串或數字
            
        Returns:
            Optional[float]: 清理後的數字，失敗時返回None
        """
        if pd.isna(value) or value == '' or str(value).lower() == 'nan':
            return None
        
        try:
            # 如果已經是數字
            if isinstance(value, (int, float)):
                return float(value)
            
            # 字符串處理
            val = str(value).strip()
            
            # 移除貨幣符號和千分位分隔符
            val = re.sub(r'[^\d.-]', '', val)
            
            # 處理多個負號
            if val.count('-') > 1:
                # 只保留第一個負號
                val = '-' + val.replace('-', '')
            
            return float(val) if val and val != '-' else None
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"無法清理數字字符串 {value}: {e}")
            return None
    
    def transform_dataframe_formats(self, df: pd.DataFrame, 
                                  format_config: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """批量轉換DataFrame格式
        
        Args:
            df: 要處理的DataFrame
            format_config: 格式配置字典
                         格式：{column_name: {type: 'currency|text|percentage|boolean', ...}}
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        df_copy = df.copy()
        
        for col, config in format_config.items():
            if col not in df_copy.columns:
                continue
            
            format_type = config.get('type', 'text')
            
            if format_type == 'currency':
                currency = config.get('currency', 'TWD')
                decimal_places = config.get('decimal_places', 2)
                df_copy[col] = df_copy[col].apply(
                    lambda x: self.format_currency(x, currency, decimal_places)
                )
            
            elif format_type == 'text':
                df_copy[col] = df_copy[col].apply(self.clean_text_data)
            
            elif format_type == 'percentage':
                decimal_places = config.get('decimal_places', 2)
                df_copy[col] = df_copy[col].apply(
                    lambda x: self.format_percentage(x, decimal_places)
                )
            
            elif format_type == 'boolean':
                df_copy[col] = df_copy[col].apply(self.normalize_boolean)
            
            elif format_type == 'account':
                df_copy[col] = df_copy[col].apply(self.normalize_account_code)
            
            elif format_type == 'department':
                df_copy[col] = df_copy[col].apply(self.standardize_department_name)
        
        return df_copy
    
    def validate_numeric_columns(self, df: pd.DataFrame, 
                               numeric_columns: List[str]) -> Dict[str, Dict[str, Any]]:
        """驗證數字列的數據品質
        
        Args:
            df: 要驗證的DataFrame
            numeric_columns: 數字列名列表
            
        Returns:
            Dict[str, Dict[str, Any]]: 驗證結果
        """
        results = {}
        
        for col in numeric_columns:
            if col not in df.columns:
                continue
            
            # 清理並轉換數字
            cleaned_values = df[col].apply(self.clean_numeric_string)
            valid_values = cleaned_values.dropna()
            
            results[col] = {
                'total_count': len(df),
                'valid_count': len(valid_values),
                'invalid_count': len(df) - len(valid_values),
                'min_value': valid_values.min() if len(valid_values) > 0 else None,
                'max_value': valid_values.max() if len(valid_values) > 0 else None,
                'mean_value': valid_values.mean() if len(valid_values) > 0 else None,
                'sum_value': valid_values.sum() if len(valid_values) > 0 else None,
                'zero_count': (valid_values == 0).sum() if len(valid_values) > 0 else 0,
                'negative_count': (valid_values < 0).sum() if len(valid_values) > 0 else 0
            }
        
        return results


# 便捷函數
def clean_text_data(text: Union[str, int, float]) -> str:
    """清理文字數據的便捷函數"""
    transformer = FormatTransformer()
    return transformer.clean_text_data(text)


def format_currency(amount: Union[str, int, float], currency: str = 'TWD') -> str:
    """格式化貨幣的便捷函數"""
    transformer = FormatTransformer()
    return transformer.format_currency(amount, currency)


def normalize_account_code(account_code: Union[str, int, float]) -> str:
    """標準化會計科目代碼的便捷函數"""
    transformer = FormatTransformer()
    return transformer.normalize_account_code(account_code)


def standardize_department_name(department: Union[str, int, float]) -> str:
    """標準化部門名稱的便捷函數"""
    transformer = FormatTransformer()
    return transformer.standardize_department_name(department)
