"""
數據處理相關工具函數
"""
from pathlib import Path
import re
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import concurrent.futures
import logging
import tomllib

from ..config.constants import REGEX_PATTERNS, DEFAULT_DATE_RANGE


toml_path = None
def load_config_from_toml(file_path: str = None, 
                          key: str = None, 
                          output_format: str = None
                          ) -> Union[List, Dict]:
    """
    從 TOML 配置文件載入數據
    
    Args:
        file_path: 配置文件路徑（可選，默認使用 stagging.toml）
        key: TOML 檔案的抬頭
        output_format: 輸出格式
    Returns:
        list: 一個包含 (account, regex_pattern) 元組的列表。
        OR
        raw: [dict]
    """
    
    # ========== 關鍵修復：使用絕對路徑 ==========
    if file_path is None:
        # 默認使用 stagging.toml
        file_path = 'stagging.toml'
    
    # 如果傳入的是文件名而不是完整路徑
    if not Path(file_path).is_absolute():
        # 獲取當前模組所在目錄
        current_dir = Path(__file__).parent  # utils/helpers/

        parts = list(current_dir.parts)
        # 要移除的連續層級
        parts_to_remove = ['utils', 'helpers']
        # 尋找要移除的層級的起始索引
        try:
            # 找到 'utils' 的索引
            index = parts.index(parts_to_remove[0])
            
            # 確認 'utils' 的下一個就是 'helpers'
            if parts[index:index + len(parts_to_remove)] == parts_to_remove:
                # 從列表中移除這兩個元素
                del parts[index:index + len(parts_to_remove)]

                # 重新組合路徑
                # parts[0] 是磁碟機代號 (例如 'C:\\')
                # parts[1:] 是後面的所有部分
                new_path = Path(parts[0]).joinpath(*parts[1:])  # accrual_bot/accrual_bot/

        except ValueError:
            print("在路徑中找不到 'utils' 層級，無需變更。")
            new_path = current_dir

        # 構建配置文件的絕對路徑
        config_dir = new_path / 'config'  # accrual_bot/accrual_bot/config/
        file_path = config_dir / file_path
    
    # ========== 讀取配置 ==========
    try:
        if output_format is not None:
            with open(file_path, "rb") as f:
                data = tomllib.load(f)
            
            # 處理 key 和 output_format
            # 將字典轉換為原始程式碼所使用的 (key, value) 元組列表格式
            if key:
                data = data.get(key, {})
            
            if output_format == 'list':
                return list(data.items())
            
            return data
        else:
            with open(file_path, "rb") as f:
                data = tomllib.load(f)
            if key:
                return data.get(key, {})
            return data
    
    except FileNotFoundError:
        print(f"❌ 找不到配置文件: {file_path}")
        print("   請確認文件已正確打包")
        raise
    except Exception as e:
        print(f"❌ 讀取配置文件失敗: {e}")
        raise
    
ACCOUNT_RULES = load_config_from_toml(toml_path, "account_rules", output_format='list')
CATEGORY_PATTERNS_BY_DESC = load_config_from_toml(toml_path, "category_patterns_by_desc")
DATE_PATTERNS = load_config_from_toml(toml_path, "date_patterns")
COLAB_ZIP_PATH = load_config_from_toml(toml_path, "paths").get("colab_zip")


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
            return str_series.str.contains(pattern, **kwargs)
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


def _validate_date_format(date_str: str, has_day: bool = False) -> bool:
    """驗證日期格式是否有效"""
    try:
        parts = date_str.split('/')
        year, month = int(parts[0]), int(parts[1])
        
        if not (1900 <= year <= 9999 and 1 <= month <= 12):
            return False
        
        if has_day and len(parts) == 3:
            day = int(parts[2])
            # 簡單驗證（不考慮閏年等複雜情況）
            if not (1 <= day <= 31):
                return False
        
        return True
    except (ValueError, IndexError):
        return False


def extract_date_range_from_description(
    description: str, 
    patterns: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None
) -> str:
    """
    從描述中提取日期範圍
    
    Args:
        description: 描述文字，可能包含日期範圍資訊
        patterns: 自訂正規表達式模式字典（可選）
        
    Returns:
        str: 日期範圍字符串 (格式: "YYYYMM,YYYYMM")，
             無法解析時返回 DEFAULT_DATE_RANGE ("100001,100002")
             
    Examples:
        >>> extract_date_range_from_description("2024/01-2024/12")
        "202401,202412"
        >>> extract_date_range_from_description("期間：2024/01/01 - 2024/12/31")
        "202401,202412"
    """
    if patterns is None:
        patterns = DATE_PATTERNS
    
    try:
        # 處理空值
        if pd.isna(description) or not description or str(description).strip() == '':
            logger.warning("描述為空，返回預設日期範圍")
            return DEFAULT_DATE_RANGE
        
        desc_str = str(description).strip()
        
        # 按照從最具體到最一般的順序檢查
        # 1. 日期範圍（含日）：YYYY/MM/DD - YYYY/MM/DD
        if match := re.search(patterns['DATE_YMD_TO_YMD'], desc_str):
            start_full, end_full = match.groups()
            if _validate_date_format(start_full, has_day=True) and \
               _validate_date_format(end_full, has_day=True):
                start_date = start_full[:7].replace('/', '')  # YYYY/MM -> YYYYMM
                end_date = end_full[:7].replace('/', '')
                return f"{start_date},{end_date}"
            else:
                logger.warning(f"日期格式無效: {start_full} 或 {end_full}")
        
        # 2. 日期範圍（月）：YYYY/MM - YYYY/MM
        if match := re.search(patterns['DATE_YM_TO_YM'], desc_str):
            start_ym, end_ym = match.groups()
            if _validate_date_format(start_ym) and _validate_date_format(end_ym):
                start_date = start_ym.replace('/', '')
                end_date = end_ym.replace('/', '')
                return f"{start_date},{end_date}"
            else:
                logger.warning(f"日期格式無效: {start_ym} 或 {end_ym}")
        
        # 3. 單一日期（含日）：YYYY/MM/DD
        if match := re.search(patterns['DATE_YMD'], desc_str):
            date_full = match.group(1)
            if _validate_date_format(date_full, has_day=True):
                single_date = date_full[:7].replace('/', '')
                return f"{single_date},{single_date}"
            else:
                logger.warning(f"日期格式無效: {date_full}")
        
        # 4. 單一日期（月）：YYYY/MM
        if match := re.search(patterns['DATE_YM'], desc_str):
            date_ym = match.group(1)
            if _validate_date_format(date_ym):
                single_date = date_ym.replace('/', '')
                return f"{single_date},{single_date}"
            else:
                logger.warning(f"日期格式無效: {date_ym}")
        
        # 無法匹配任何格式
        # logger.warning(f"無法從描述中提取日期: {desc_str}")
        return DEFAULT_DATE_RANGE
            
    except (ValueError, AttributeError, IndexError) as e:
        logger.warning(f"解析日期時發生錯誤: {description}, 錯誤: {e}")
        return DEFAULT_DATE_RANGE
    except Exception as e:
        logger.warning(f"未預期的錯誤: {description}, 錯誤: {e}", exc_info=True)
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
        if isinstance(df, dict):
            return df

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
    
def classify_description(description: str) -> str:
    """
    Classifies a description string into a category based on regex patterns.

    Args:
        description: The description string to classify.

    Returns:
        The category label for the description.
    """
    patterns = CATEGORY_PATTERNS_BY_DESC

    # Iterate through the patterns and return the first match
    for label, pattern in patterns.items():
        if re.search(pattern, description):
            return label

    # If no pattern matches, classify as Miscellaneous
    return 'Miscellaneous'

def give_account_by_keyword(df, column_name, rules=None, export_keyword=False):
    """
    根據指定欄位中的關鍵字，為 DataFrame 新增科目代碼欄位。
    可選擇性地匯出匹配到的關鍵字。

    Args:
        df (pd.DataFrame): 要處理的 DataFrame。
        column_name (str): 包含關鍵字描述的欄位名稱。
        rules (list): 一個包含 (account, regex_pattern) 元組的規則列表。
        export_keyword (bool, optional): 如果為 True，則會額外新增一個 'Matched_Keyword' 欄位。
                                         預設為 False。

    Returns:
        pd.DataFrame: 'Predicted_Account' 和 (可選的) 'Matched_Keyword' 欄位的 DataFrame。
    """
    # 步驟 1: 規則列表現在從函數參數傳入，不再硬編碼。
    rules = ACCOUNT_RULES
    
    # 步驟 2: 修改輔助函數，使其返回 (科目, 匹配到的關鍵字) 的元組 (tuple)
    def find_match_details(text):
        if not isinstance(text, str):
            return None, None

        for account, keywords_regex in rules:
            # re.search 返回一個 match object，如果沒有匹配則返回 None
            match = re.search(keywords_regex, text, re.IGNORECASE)
            if match:
                # match.group(0) 會返回整個匹配到的字串
                return account, match.group(0)

        # 如果所有規則都沒匹配到，返回兩個 None
        return None, None

    # 步驟 3: 應用函數並根據 export_keyword 參數決定輸出
    # .apply 會返回一個包含 (account, keyword) 元組的 Series
    results = df[column_name].apply(find_match_details)

    # 將元組 Series 拆分成兩個新的欄位
    # .str[0] 提取每個元組的第一個元素 (科目)
    # .str[1] 提取每個元組的第二個元素 (關鍵字)
    df['Predicted_Account'] = results.str[0]

    if export_keyword:
        df['Matched_Keyword'] = results.str[1]

    return df

def extract_clean_description(desc):
    """
    根據不同的規則，從 Item Description 中擷取乾淨的字串
    """
    # Check if input is a Series
    if isinstance(desc, pd.Series):
        return desc.apply(lambda x: extract_clean_description(x))

    desc = desc.strip()

    # --- 特殊情況處理 (硬編碼) ---
    # 這些情況無法單純用擷取完成，需要翻譯或補充資訊
    # if 'EDL SOC winding roller' in desc:
    #     return 'SOC新增EDL捲繞滾筒輸送機及供給線'
    # if 'Large Material Measuring Equipment' in desc:
    #     return '南北倉大材量測設備拉電工程'
    # if '員林莒光 辦公室裝修工程' in desc:
    #     # 預期結果包含的地址資訊不存在於來源字串中，這裡根據您的範例給出固定結果
    #     return '辦公室裝修工程-員林莒光(彰化縣員林市莒光路332號1樓)'

    # --- 規則一：門市裝修工程 (有地址和期數) ---
    # e.g., ...門市裝修工程-北投建民 (...)SPX...decoration 第一期款項#...
    # 使用 regex 捕獲 (描述+地址) 和 (期數)
    pattern1 = r'(門市裝修工程-.*?\(.*?\))\s*SPX\s*store decoration\s*(.*?)\s*#'
    match1 = re.search(pattern1, desc, re.IGNORECASE)
    if match1:
        description_part = match1.group(1).strip()
        payment_term = match1.group(2).strip()
        return f"{description_part}_{payment_term}"

    # --- 規則二：有地址但沒有期數的工程項目 ---
    # e.g., ...冷氣裝修工程Air-con equipment(...) #...
    # e.g., ...門市招牌安裝工程signboard installation(...) #...
    # 捕獲 (工程名稱) 和 (地址)
    pattern2 = r'SVP_?(?:SPX)?\s*(.*?)(?:\(|（)([^)）]+)(?:\)|）)'
    match2 = re.search(pattern2, desc)
    if match2:
        project_name = match2.group(1).strip()
        address = match2.group(2).strip()
        # 移除工程名稱中夾雜的英文
        project_name = re.sub(r'[a-zA-Z\s-]+$', '', project_name).strip()
        return f"{project_name}({address})"

    # --- 通用規則 (適用於剩餘情況) ---
    # 建立一個臨時變數來進行一系列的清理
    core_content = desc

    # 1. 移除結尾的 #... 標籤
    core_content = re.sub(r'\s*#.*$', '', core_content).strip()

    # 2. 移除結尾的英文描述
    # 例如 "payment machine..." 或 "_SPX N-SOC..."
    core_content = re.sub(r'\s*payment machine.*$', '', core_content, flags=re.IGNORECASE)
    core_content = re.sub(r'_SPX N-SOC.*$', '', core_content, flags=re.IGNORECASE)

    # 3. 移除前面的日期和公司前綴
    core_content = re.sub(r'^(\d{4}/\d{2}/\d{2})\s*-\s*(\d{4}/\d{2}/\d{2})', '', core_content)
    core_content = re.sub(r'^\d{4}/\d{2}\s*_?SVP_?(?:SPX)?\s*', '', core_content)

    # 4. 對於 IT 等沒有 SVP 的特殊項目，單獨移除日期
    core_content = re.sub(r'^\d{4}/\d{2}\s*', '', core_content)

    # 5. 清理多餘的空白，並加上 SPX_ 前綴
    core_content = re.sub(r'\s+', ' ', core_content).strip()

    # 如果內容已經是 SPX 開頭，就不要再加 SPX_
    if core_content.upper().startswith('SPX '):
        # 將 'SPX ' 替換為 'SPX_'
        core_content = re.sub(r'^SPX\s', 'SPX_', core_content, flags=re.IGNORECASE)
        return core_content
    else:
        return f"{core_content}"

def clean_pr_data(df: pd.DataFrame) -> pd.DataFrame:
    """清理PR資料"""
    # 欄位名稱標準化
    df.columns = [(col.lower()
                      .replace(' ', '_')
                      .replace('#', '_number')
                      .replace('(', '')
                      .replace(')', '')) for col in df.columns]
    
    # 處理日期欄位
    date_columns = ['pr_create_date', 'submission_date', 'last_action_date', 'pr_approved_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 處理數值欄位
    numeric_columns = ['total_amount', 'total_amount_usd', 'unit_price', 
                       'entry_quantity', 'entry_amount', 'entry_amount_usd']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def clean_po_data(df: pd.DataFrame) -> pd.DataFrame:
    """清理PO資料"""
    # 欄位名稱標準化
    df.columns = [(col.lower()
                      .replace(' ', '_')
                      .replace('#', '_number')
                      .replace('(', '')
                      .replace(')', '')) for col in df.columns]
    
    # 處理日期欄位
    date_columns = ['po_create_date', 'submission_date', 'last_action_date', 
                    'po_approved_date', 'notified_finance_date', 'expected_payment_date', 
                    'actual_receive_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 處理數值欄位
    numeric_columns = ['po_total_amount', 'po_total_amount_usd', 'total_invoice_amount',
                       'unit_price', 'entry_quantity', 'received_quantity', 
                       'billed_quantity', 'prepay_quantity', 'entry_amount', 
                       'entry_amount_usd', 'entry_invoiced_amount', 
                       'entry_billed_amount', 'entry_prepay_amount']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def get_ref_on_colab(ref_data_path):
    """work for colab env
    to get account ref in zip

    Args:
        ref_data_path: _description_
    """
    def is_colab():  
        try:  
            import google.colab  
            return True  
        except Exception:  
            return False

    if is_colab():
        import zipfile
        root_url = COLAB_ZIP_PATH
        with zipfile.ZipFile(root_url, 'r') as zf:
            if ref_data_path in zf.namelist():
                # Open the Excel file from within the zip archive
                with zf.open(ref_data_path) as excel_file:
                    ref_ac = pd.read_excel(excel_file, dtype=str)
        return ref_ac
    else:
        return None
