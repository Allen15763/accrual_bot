"""
日期轉換器

專門處理日期解析、格式化和驗證
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Optional, Union, List, Dict, Any
from ...utils.logging import Logger


class DateTransformer:
    """日期轉換器類別"""
    
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        
        # 常見日期格式模式
        self.date_patterns = [
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD 或 YYYY-MM-DD
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # MM/DD/YYYY 或 DD/MM/YYYY
            r'(\d{4})(\d{2})(\d{2})',              # YYYYMMDD
            r'(\d{1,2})/(\d{1,2})/(\d{2})',       # M/D/YY
        ]
        
        # Excel日期序列號的基準日期 (1900-01-01)
        self.excel_epoch = datetime(1900, 1, 1)
    
    def parse_date_string(self, date_str: Union[str, int, float, datetime, pd.Timestamp]) -> Optional[datetime]:
        """解析日期字符串
        
        Args:
            date_str: 日期字符串、數字或日期對象
            
        Returns:
            Optional[datetime]: 解析後的日期對象，失敗時返回None
        """
        if pd.isna(date_str) or date_str == '' or date_str == 'nan':
            return None
        
        # 如果已經是datetime對象
        if isinstance(date_str, (datetime, pd.Timestamp)):
            return date_str.replace(tzinfo=None) if hasattr(date_str, 'tzinfo') else date_str
        
        # 如果是date對象
        if isinstance(date_str, date):
            return datetime.combine(date_str, datetime.min.time())
        
        # 如果是數字（Excel序列號）
        if isinstance(date_str, (int, float)):
            return self._parse_excel_serial(date_str)
        
        # 字符串處理
        date_str = str(date_str).strip()
        
        # 嘗試pandas解析
        try:
            parsed = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed):
                return parsed.to_pydatetime()
        except:
            pass
        
        # 使用正則表達式模式匹配
        for pattern in self.date_patterns:
            match = re.search(pattern, date_str)
            if match:
                return self._parse_date_groups(match.groups(), pattern)
        
        self.logger.warning(f"無法解析日期字符串: {date_str}")
        return None
    
    def _parse_excel_serial(self, serial: Union[int, float]) -> Optional[datetime]:
        """解析Excel序列號
        
        Args:
            serial: Excel日期序列號
            
        Returns:
            Optional[datetime]: 解析後的日期
        """
        try:
            # Excel的序列號從1開始，但datetime需要從0開始計算
            if serial < 1:
                return None
            
            # Excel有一個bug，認為1900年是閏年，所以需要調整
            if serial > 59:  # 1900年3月1日之後
                serial -= 1
            
            # 計算日期
            delta_days = serial - 1  # 調整為從0開始
            result_date = self.excel_epoch + pd.Timedelta(days=delta_days)
            
            return result_date.to_pydatetime()
        except Exception as e:
            self.logger.warning(f"無法解析Excel序列號 {serial}: {e}")
            return None
    
    def _parse_date_groups(self, groups: tuple, pattern: str) -> Optional[datetime]:
        """根據正則表達式組解析日期
        
        Args:
            groups: 正則表達式匹配組
            pattern: 使用的模式
            
        Returns:
            Optional[datetime]: 解析後的日期
        """
        try:
            if len(groups) == 3:
                g1, g2, g3 = groups
                
                # YYYY/MM/DD 或 YYYY-MM-DD 模式
                if 'YYYY' in pattern or len(g1) == 4:
                    year, month, day = int(g1), int(g2), int(g3)
                # MM/DD/YYYY 模式（美式）
                elif len(g3) == 4:
                    month, day, year = int(g1), int(g2), int(g3)
                # YYYYMMDD 模式
                elif len(''.join(groups)) == 8:
                    year, month, day = int(g1), int(g2), int(g3)
                # M/D/YY 模式
                else:
                    month, day, year = int(g1), int(g2), int(g3)
                    # 處理兩位年份
                    if year < 50:
                        year += 2000
                    elif year < 100:
                        year += 1900
                
                return datetime(year, month, day)
        except ValueError as e:
            self.logger.warning(f"日期值錯誤: {groups}, 錯誤: {e}")
        
        return None
    
    def format_date_for_export(self, date_obj: Optional[datetime], format_str: str = "%Y-%m-%d") -> str:
        """格式化日期用於匯出
        
        Args:
            date_obj: 日期對象
            format_str: 格式字符串
            
        Returns:
            str: 格式化後的日期字符串
        """
        if date_obj is None:
            return ""
        
        try:
            return date_obj.strftime(format_str)
        except Exception as e:
            self.logger.warning(f"日期格式化失敗: {date_obj}, 錯誤: {e}")
            return str(date_obj)
    
    def validate_date_range(self, date_obj: Optional[datetime], 
                          min_date: Optional[datetime] = None,
                          max_date: Optional[datetime] = None) -> bool:
        """驗證日期是否在指定範圍內
        
        Args:
            date_obj: 要驗證的日期
            min_date: 最小日期
            max_date: 最大日期
            
        Returns:
            bool: 是否在範圍內
        """
        if date_obj is None:
            return False
        
        if min_date and date_obj < min_date:
            return False
        
        if max_date and date_obj > max_date:
            return False
        
        return True
    
    def transform_dataframe_dates(self, df: pd.DataFrame, 
                                date_columns: List[str],
                                output_format: str = "%Y-%m-%d") -> pd.DataFrame:
        """轉換DataFrame中的日期列
        
        Args:
            df: 要處理的DataFrame
            date_columns: 日期列名列表
            output_format: 輸出格式
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        df_copy = df.copy()
        
        for col in date_columns:
            if col in df_copy.columns:
                # 解析日期
                df_copy[col] = df_copy[col].apply(self.parse_date_string)
                
                # 格式化輸出
                if output_format:
                    df_copy[col] = df_copy[col].apply(
                        lambda x: self.format_date_for_export(x, output_format)
                    )
        
        return df_copy
    
    def get_date_statistics(self, dates: List[Optional[datetime]]) -> Dict[str, Any]:
        """獲取日期統計信息
        
        Args:
            dates: 日期列表
            
        Returns:
            Dict[str, Any]: 統計信息
        """
        valid_dates = [d for d in dates if d is not None]
        
        if not valid_dates:
            return {
                "total_count": len(dates),
                "valid_count": 0,
                "invalid_count": len(dates),
                "min_date": None,
                "max_date": None,
                "date_range_days": None
            }
        
        min_date = min(valid_dates)
        max_date = max(valid_dates)
        
        return {
            "total_count": len(dates),
            "valid_count": len(valid_dates),
            "invalid_count": len(dates) - len(valid_dates),
            "min_date": min_date,
            "max_date": max_date,
            "date_range_days": (max_date - min_date).days if min_date != max_date else 0
        }


# 便捷函數
def parse_date_string(date_str: Union[str, int, float, datetime, pd.Timestamp]) -> Optional[datetime]:
    """解析日期字符串的便捷函數"""
    transformer = DateTransformer()
    return transformer.parse_date_string(date_str)


def format_date_for_export(date_obj: Optional[datetime], format_str: str = "%Y-%m-%d") -> str:
    """格式化日期的便捷函數"""
    transformer = DateTransformer()
    return transformer.format_date_for_export(date_obj, format_str)


def validate_date_range(date_obj: Optional[datetime], 
                       min_date: Optional[datetime] = None,
                       max_date: Optional[datetime] = None) -> bool:
    """驗證日期範圍的便捷函數"""
    transformer = DateTransformer()
    return transformer.validate_date_range(date_obj, min_date, max_date)
