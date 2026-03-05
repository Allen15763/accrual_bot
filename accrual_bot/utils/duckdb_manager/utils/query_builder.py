"""
SQL 安全工具模組

提供 SQL 字串安全處理的工具函數，防止 SQL 注入攻擊。
"""

import re
from typing import List, Any


class SafeSQL:
    """
    SQL 安全工具類

    提供識別符和字串值的安全轉義方法。

    Example:
        >>> SafeSQL.quote_identifier("table name")
        '"table name"'
        >>> SafeSQL.escape_string("O'Brien")
        "O''Brien"
        >>> SafeSQL.quote_value("hello")
        "'hello'"
    """

    # 允許的識別符字符 (字母、數字、底線)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    @staticmethod
    def quote_identifier(name: str) -> str:
        """
        安全地引用 SQL 識別符 (表名、欄位名)

        使用雙引號包裹識別符，並轉義內部的雙引號。

        Args:
            name: 識別符名稱

        Returns:
            str: 安全的識別符字串

        Example:
            >>> SafeSQL.quote_identifier("users")
            '"users"'
            >>> SafeSQL.quote_identifier('my"table')
            '"my""table"'
        """
        # 轉義雙引號
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    @staticmethod
    def escape_string(value: str) -> str:
        """
        轉義 SQL 字串值中的特殊字符

        Args:
            value: 字串值

        Returns:
            str: 轉義後的字串

        Example:
            >>> SafeSQL.escape_string("O'Brien")
            "O''Brien"
        """
        return value.replace("'", "''")

    @staticmethod
    def quote_value(value: Any) -> str:
        """
        將值轉換為 SQL 字面值

        Args:
            value: 任意值

        Returns:
            str: SQL 字面值

        Example:
            >>> SafeSQL.quote_value("hello")
            "'hello'"
            >>> SafeSQL.quote_value(123)
            '123'
            >>> SafeSQL.quote_value(None)
            'NULL'
        """
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            escaped = SafeSQL.escape_string(value)
            return f"'{escaped}'"
        # 其他類型轉為字串
        return f"'{SafeSQL.escape_string(str(value))}'"

    @staticmethod
    def quote_values(values: List[Any]) -> str:
        """
        將值列表轉換為 SQL IN 子句的值

        Args:
            values: 值列表

        Returns:
            str: 逗號分隔的 SQL 值

        Example:
            >>> SafeSQL.quote_values([1, 2, 3])
            '1, 2, 3'
            >>> SafeSQL.quote_values(["a", "b"])
            "'a', 'b'"
        """
        return ", ".join(SafeSQL.quote_value(v) for v in values)

    @staticmethod
    def is_safe_identifier(name: str) -> bool:
        """
        檢查識別符是否安全 (不需要引號)

        Args:
            name: 識別符名稱

        Returns:
            bool: 是否安全

        Example:
            >>> SafeSQL.is_safe_identifier("users")
            True
            >>> SafeSQL.is_safe_identifier("my table")
            False
        """
        return bool(SafeSQL.IDENTIFIER_PATTERN.match(name))

    @staticmethod
    def escape_like_pattern(pattern: str) -> str:
        """
        轉義 LIKE 模式中的特殊字符

        Args:
            pattern: LIKE 模式字串

        Returns:
            str: 轉義後的模式

        Example:
            >>> SafeSQL.escape_like_pattern("100%")
            '100\\%'
        """
        # 轉義 LIKE 特殊字符
        result = pattern.replace('\\', '\\\\')
        result = result.replace('%', '\\%')
        result = result.replace('_', '\\_')
        return result

    @staticmethod
    def build_in_clause(column: str, values: List[Any]) -> str:
        """
        建立安全的 IN 子句

        Args:
            column: 欄位名稱
            values: 值列表

        Returns:
            str: IN 子句

        Example:
            >>> SafeSQL.build_in_clause("id", [1, 2, 3])
            '"id" IN (1, 2, 3)'
        """
        col = SafeSQL.quote_identifier(column)
        vals = SafeSQL.quote_values(values)
        return f"{col} IN ({vals})"

    @staticmethod
    def build_where_equals(conditions: dict) -> str:
        """
        建立安全的 WHERE 等值條件

        Args:
            conditions: 欄位名稱 -> 值 的字典

        Returns:
            str: WHERE 條件 (不含 WHERE 關鍵字)

        Example:
            >>> SafeSQL.build_where_equals({"name": "John", "age": 30})
            '"name" = \'John\' AND "age" = 30'
        """
        parts = []
        for col, val in conditions.items():
            quoted_col = SafeSQL.quote_identifier(col)
            quoted_val = SafeSQL.quote_value(val)
            parts.append(f"{quoted_col} = {quoted_val}")
        return " AND ".join(parts)


# 便利函數別名
quote_identifier = SafeSQL.quote_identifier
escape_string = SafeSQL.escape_string
quote_value = SafeSQL.quote_value
is_safe_identifier = SafeSQL.is_safe_identifier
