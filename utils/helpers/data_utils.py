"""
數據處理相關工具函數
"""

import re
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import concurrent.futures
import threading

from ..config.constants import REGEX_PATTERNS, DEFAULT_DATE_RANGE


def clean_nan_values(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    清理DataFrame中的nan值
    
    Args:
        df: 要處理的DataFrame
        columns: 要清理nan值的列名列表
        
    Returns:
        pd.DataFrame: 處理後的DataFrame
    """
    df_copy = df.copy()
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str).replace('nan', '')
    return df_copy


def safe_string_operation(series: pd.Series, operation: str, pattern: str = None, 
                         replacement: str = None, **kwargs) -> pd.Series:
    """
    安全的字符串操作（處理NaN值）
    
    Args:
        series: pandas Series
        operation: 操作類型 ('contains', 'replace', 'extract', 'findall', 'match')
        pattern: 正規表達式模式
        replacement: 替換字符串
        **kwargs: 其他參數
        
    Returns:
        pd.Series: 處理後的Series
    """
    try:
        # 確保Series為字符串類型，NaN轉換為空字符串
        str_series = series.astype(str).fillna('')
        
        if operation == 'contains':
            return str_series.str.contains(pattern, na=False, **kwargs)
        elif operation == 'replace':
            return str_series.str.replace(pattern, replacement, **kwargs)
        elif operation == 'extract':
            return str_series.str.extract(pattern, **kwargs)
        elif operation == 'findall':
            return str_series.str.findall(pattern, **kwargs)
        elif operation == 'match':
            return str_series.str.match(pattern, **kwargs)
        else:
            return series
            
    except Exception:
        # 如果操作失敗，返回原始Series
        return series


def format_numeric_with_thousands(value: Union[int, float, str], decimal_places: int = 0) -> str:
    """
    格式化數值，添加千分位符號
    
    Args:
        value: 數值
        decimal_places: 小數位數
        
    Returns:
        str: 格式化後的數值字符串
    """
    try:
        if pd.isna(value) or value == '' or value == 'nan':
            return '0'
        
        num_value = float(value)
        
        if decimal_places == 0:
            return format(int(num_value), ',')
        else:
            return format(num_value, f',.{decimal_places}f')
            
    except (ValueError, TypeError):
        return str(value) if value is not None else '0'


def format_numeric_columns(df: pd.DataFrame, int_cols: List[str], 
                          float_cols: List[str]) -> pd.DataFrame:
    """
    格式化數值列，包括千分位
    
    Args:
        df: DataFrame
        int_cols: 整數列名列表
        float_cols: 浮點數列名列表
        
    Returns:
        pd.DataFrame: 處理後的DataFrame
    """
    df_copy = df.copy()
    
    try:
        # 處理整數列
        for col in int_cols:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].fillna('0')
                df_copy[col] = df_copy[col].apply(
                    lambda x: format_numeric_with_thousands(x, 0)
                )
        
        # 處理浮點數列
        for col in float_cols:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].fillna('0')
                df_copy[col] = df_copy[col].apply(
                    lambda x: format_numeric_with_thousands(x, 2)
                )
        
        return df_copy
    except Exception as e:
        raise ValueError(f"格式化數值列時出錯: {str(e)}")


def parse_date_string(date_str: str, input_format: str = None, 
                     output_format: str = None) -> Optional[str]:
    """
    解析日期字符串
    
    Args:
        date_str: 日期字符串
        input_format: 輸入格式
        output_format: 輸出格式
        
    Returns:
        Optional[str]: 格式化後的日期字符串
    """
    try:
        if pd.isna(date_str) or date_str == '' or date_str == 'nan':
            return None
        
        if input_format:
            date_obj = datetime.strptime(str(date_str), input_format)
        else:
            # 嘗試自動解析
            date_obj = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date_obj):
                return None
        
        if output_format:
            return date_obj.strftime(output_format)
        else:
            return date_obj.strftime('%Y/%m/%d')
            
    except (ValueError, TypeError):
        return None


def extract_date_range_from_description(description: str, patterns: Dict[str, str] = None) -> str:
    """
    從描述中提取日期範圍
    
    Args:
        description: 描述文字
        patterns: 正規表達式模式字典
        
    Returns:
        str: 日期範圍字符串 (格式: "YYYYMM,YYYYMM")
    """
    if patterns is None:
        patterns = REGEX_PATTERNS
    
    try:
        if pd.isna(description) or description == '':
            return DEFAULT_DATE_RANGE
        
        desc_str = str(description)
        
        # 檢查各種日期格式
        pt_YM = patterns.get('DATE_YM', r'(\d{4}\/\d{2})')
        pt_YMD = patterns.get('DATE_YMD', r'(\d{4}\/\d{2}\/\d{2})')
        pt_YMtoYM = patterns.get('DATE_YM_TO_YM', r'(\d{4}\/\d{2}[-]\d{4}\/\d{2})')
        pt_YMDtoYMD = patterns.get('DATE_YMD_TO_YMD', r'(\d{4}\/\d{2}\/\d{2}[-]\d{4}\/\d{2}\/\d{2})')
        
        # 組合單一日期模式
        pt_YMYMD = f'({pt_YM}|{pt_YMD})'
        
        # 檢查日期範圍格式
        if re.match(pt_YMDtoYMD, desc_str):
            # YYYY/MM/DD-YYYY/MM/DD 格式
            start_date = desc_str[:7].replace('/', '')
            end_date = desc_str[11:18].replace('/', '')
            return f"{start_date},{end_date}"
        elif re.match(pt_YMtoYM, desc_str):
            # YYYY/MM-YYYY/MM 格式
            start_date = desc_str[:7].replace('/', '')
            end_date = desc_str[8:15].replace('/', '')
            return f"{start_date},{end_date}"
        elif re.match(pt_YMYMD, desc_str):
            # 單一日期格式
            single_date = desc_str[:7].replace('/', '')
            return f"{single_date},{single_date}"
        else:
            # 無法解析的格式
            return DEFAULT_DATE_RANGE
            
    except Exception:
        return DEFAULT_DATE_RANGE


def convert_date_format_in_string(text: str, from_pattern: str = r'(\d{4})/(\d{2})', 
                                 to_pattern: str = r'\1\2') -> str:
    """
    轉換字符串中的日期格式
    
    Args:
        text: 原始文字
        from_pattern: 來源格式正規表達式
        to_pattern: 目標格式
        
    Returns:
        str: 轉換後的文字
    """
    try:
        if pd.isna(text) or text == '':
            return text
        
        return re.sub(from_pattern, to_pattern, str(text))
    except Exception:
        return str(text) if text is not None else ''


def extract_pattern_from_string(text: str, pattern: str, group: int = 0) -> Optional[str]:
    """
    從字符串中提取符合模式的內容
    
    Args:
        text: 原始文字
        pattern: 正規表達式模式
        group: 捕獲群組索引
        
    Returns:
        Optional[str]: 提取的內容
    """
    try:
        if pd.isna(text) or text == '':
            return None
        
        match = re.search(pattern, str(text))
        if match:
            return match.group(group)
        return None
    except Exception:
        return None


def safe_numeric_operation(series: pd.Series, operation: str, **kwargs) -> pd.Series:
    """
    安全的數值操作
    
    Args:
        series: pandas Series
        operation: 操作類型 ('add', 'subtract', 'multiply', 'divide', 'round')
        **kwargs: 操作參數
        
    Returns:
        pd.Series: 處理後的Series
    """
    try:
        # 轉換為數值類型，無法轉換的設為NaN
        numeric_series = pd.to_numeric(series, errors='coerce')
        
        if operation == 'add':
            return numeric_series + kwargs.get('value', 0)
        elif operation == 'subtract':
            return numeric_series - kwargs.get('value', 0)
        elif operation == 'multiply':
            return numeric_series * kwargs.get('value', 1)
        elif operation == 'divide':
            divisor = kwargs.get('value', 1)
            return numeric_series / divisor if divisor != 0 else numeric_series
        elif operation == 'round':
            return numeric_series.round(kwargs.get('decimals', 0))
        else:
            return numeric_series
            
    except Exception:
        return series


def create_mapping_dict(df: pd.DataFrame, key_col: str, value_col: str, 
                       filter_condition: pd.Series = None) -> Dict[Any, Any]:
    """
    創建映射字典
    
    Args:
        df: DataFrame
        key_col: 鍵列名
        value_col: 值列名
        filter_condition: 過濾條件
        
    Returns:
        Dict[Any, Any]: 映射字典
    """
    try:
        if key_col not in df.columns or value_col not in df.columns:
            return {}
        
        # 應用過濾條件
        if filter_condition is not None:
            filtered_df = df[filter_condition]
        else:
            # 預設過濾掉值列中的NaN
            filtered_df = df[~df[value_col].isna()]
        
        return filtered_df.set_index(key_col)[value_col].to_dict()
        
    except Exception:
        return {}


def apply_mapping_safely(series: pd.Series, mapping_dict: Dict[Any, Any], 
                        default_value: Any = None) -> pd.Series:
    """
    安全地應用映射字典
    
    Args:
        series: pandas Series
        mapping_dict: 映射字典
        default_value: 預設值
        
    Returns:
        pd.Series: 映射後的Series
    """
    try:
        return series.map(mapping_dict).fillna(default_value)
    except Exception:
        return series


def validate_dataframe_columns(df: pd.DataFrame, required_columns: List[str], 
                              raise_error: bool = True) -> bool:
    """
    驗證DataFrame是否包含必要的列
    
    Args:
        df: DataFrame
        required_columns: 必要列名列表
        raise_error: 是否在驗證失敗時拋出錯誤
        
    Returns:
        bool: 是否驗證通過
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        if raise_error:
            raise ValueError(f"DataFrame缺少必要的列: {missing_columns}")
        return False
    
    return True


def concat_dataframes_safely(dfs: List[pd.DataFrame], **kwargs) -> pd.DataFrame:
    """
    安全地合併多個DataFrame
    
    Args:
        dfs: DataFrame列表
        **kwargs: pandas.concat的參數
        
    Returns:
        pd.DataFrame: 合併後的DataFrame
    """
    try:
        # 過濾掉空的DataFrame
        valid_dfs = [df for df in dfs if df is not None and not df.empty]
        
        if not valid_dfs:
            return pd.DataFrame()
        
        if len(valid_dfs) == 1:
            return valid_dfs[0].copy()
        
        # 設定預設參數
        default_kwargs = {'ignore_index': True, 'sort': False}
        default_kwargs.update(kwargs)
        
        return pd.concat(valid_dfs, **default_kwargs)
        
    except Exception as e:
        raise ValueError(f"合併DataFrame時出錯: {str(e)}")


def parallel_apply(df: pd.DataFrame, func: callable, column: str = None, 
                  max_workers: int = None, **kwargs) -> pd.Series:
    """
    並行應用函數到DataFrame的列
    
    Args:
        df: DataFrame
        func: 要應用的函數
        column: 列名，如果為None則應用到整個DataFrame
        max_workers: 最大工作線程數
        **kwargs: 傳遞給函數的額外參數
        
    Returns:
        pd.Series: 處理結果
    """
    try:
        if max_workers is None:
            max_workers = min(4, len(df) // 1000 + 1)
        
        if column:
            data = df[column]
        else:
            data = df
        
        # 分割數據
        chunk_size = max(1, len(data) // max_workers)
        chunks = [data.iloc[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        # 並行處理
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(lambda chunk: chunk.apply(func, **kwargs), chunks))
        
        # 合併結果
        return pd.concat(results, ignore_index=True)
        
    except Exception:
        # 如果並行處理失敗，回退到串行處理
        if column:
            return df[column].apply(func, **kwargs)
        else:
            return df.apply(func, **kwargs)


def memory_efficient_operation(df: pd.DataFrame, operation: callable, 
                              chunk_size: int = 10000, **kwargs) -> pd.DataFrame:
    """
    記憶體高效的DataFrame操作
    
    Args:
        df: DataFrame
        operation: 要執行的操作函數
        chunk_size: 分塊大小
        **kwargs: 傳遞給操作函數的參數
        
    Returns:
        pd.DataFrame: 處理後的DataFrame
    """
    try:
        if len(df) <= chunk_size:
            return operation(df, **kwargs)
        
        results = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            result_chunk = operation(chunk, **kwargs)
            results.append(result_chunk)
        
        return concat_dataframes_safely(results)
        
    except Exception as e:
        raise ValueError(f"記憶體高效操作時出錯: {str(e)}")
