"""
欄位名稱解析工具
提供大小寫不敏感的欄位名稱解析功能
"""

from typing import Optional, List, Dict
import pandas as pd


class ColumnResolver:
    """
    大小寫不敏感的欄位名稱解析工具類

    支援標準格式 ('PO Line') 和 snake_case ('po_line') 兩種變體的欄位名稱。

    使用方式：
        # 解析單一欄位
        actual_col = ColumnResolver.resolve(df, 'po_line')

        # 檢查欄位是否存在
        if ColumnResolver.has_column(df, 'po_line'):
            ...

    Examples:
        >>> df = pd.DataFrame({'PO Line': [1, 2, 3]})
        >>> ColumnResolver.resolve(df, 'po_line')
        'PO Line'

        >>> df = pd.DataFrame({'po_line': [1, 2, 3]})
        >>> ColumnResolver.resolve(df, 'po_line')
        'po_line'
    """

    # 預定義的欄位名稱模式 (標準名稱 -> 正則表達式模式)
    COLUMN_PATTERNS: Dict[str, str] = {
        'po_line': r'(?i)^(po[_\s]?line)$',
        'pr_line': r'(?i)^(pr[_\s]?line)$',
        'remarked_by_fn': r'(?i)^(remarked[_\s]?by[_\s]?fn)$',
        'remarked_by_procurement': r'(?i)^(remarked?[_\s]?by[_\s]?(procurement|pr[_\s]?team))$',
        'noted_by_fn': r'(?i)^(noted[_\s]?by[_\s]?fn)$',
        'noted_by_procurement': r'(?i)^(noted[_\s]?by[_\s]?(procurement|pr))$',
        'liability': r'(?i)^(liability)$',
        'current_month_reviewed_by': r'(?i)^(current[_\s]?month[_\s]?reviewed[_\s]?by)$',
        'cumulative_qty': r'(?i)^(累計至本期驗收數量/金額)$',
    }

    @classmethod
    def resolve(cls, df: pd.DataFrame, canonical_name: str) -> Optional[str]:
        """
        解析標準欄位名稱到 DataFrame 中的實際欄位名稱 (大小寫不敏感)

        Args:
            df: 要搜尋的 DataFrame
            canonical_name: 標準欄位名稱 (e.g., 'po_line')

        Returns:
            實際的欄位名稱，如果找不到則返回 None

        Examples:
            >>> df = pd.DataFrame({'PO Line': [1, 2, 3]})
            >>> ColumnResolver.resolve(df, 'po_line')
            'PO Line'
        """
        # 優先使用預定義的正則模式
        pattern = cls.COLUMN_PATTERNS.get(canonical_name)
        if pattern:
            matches = df.filter(regex=pattern).columns
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                # 如果有多個匹配，返回第一個
                return matches[0]

        # 回退到大小寫不敏感的直接匹配
        canonical_normalized = canonical_name.lower().replace(' ', '_')
        for col in df.columns:
            col_normalized = str(col).lower().replace(' ', '_')
            if col_normalized == canonical_normalized:
                return col

        return None

    @classmethod
    def resolve_or_raise(cls, df: pd.DataFrame, canonical_name: str) -> str:
        """
        解析欄位名稱，找不到時拋出 ValueError

        Args:
            df: 要搜尋的 DataFrame
            canonical_name: 標準欄位名稱

        Returns:
            實際的欄位名稱

        Raises:
            ValueError: 如果找不到欄位
        """
        result = cls.resolve(df, canonical_name)
        if result is None:
            raise ValueError(f"Cannot resolve column '{canonical_name}' in DataFrame")
        return result

    @classmethod
    def has_column(cls, df: pd.DataFrame, canonical_name: str) -> bool:
        """
        檢查欄位是否存在 (大小寫不敏感)

        Args:
            df: 要檢查的 DataFrame
            canonical_name: 標準欄位名稱

        Returns:
            如果欄位存在返回 True，否則返回 False
        """
        return cls.resolve(df, canonical_name) is not None

    @classmethod
    def resolve_multiple(cls, df: pd.DataFrame, canonical_names: List[str]) -> Dict[str, Optional[str]]:
        """
        一次解析多個欄位名稱

        Args:
            df: 要搜尋的 DataFrame
            canonical_names: 標準欄位名稱列表

        Returns:
            標準名稱 -> 實際名稱的字典

        Examples:
            >>> df = pd.DataFrame({'PO Line': [1], 'PR Line': [2]})
            >>> ColumnResolver.resolve_multiple(df, ['po_line', 'pr_line'])
            {'po_line': 'PO Line', 'pr_line': 'PR Line'}
        """
        return {name: cls.resolve(df, name) for name in canonical_names}

    @classmethod
    def add_pattern(cls, canonical_name: str, pattern: str) -> None:
        """
        動態新增欄位模式 (用於擴展支援)

        Args:
            canonical_name: 標準欄位名稱
            pattern: 正則表達式模式
        """
        cls.COLUMN_PATTERNS[canonical_name] = pattern
