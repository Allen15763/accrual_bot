"""
Schema 差異比對模組

提供 DataFrame Schema 與資料庫表格 Schema 的比對功能。
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from ..manager import DuckDBManager

from ..utils.type_mapping import get_duckdb_dtype


class ChangeType(Enum):
    """變更類型"""
    ADDED = "added"           # 新增欄位
    REMOVED = "removed"       # 移除欄位
    TYPE_CHANGED = "type_changed"  # 類型變更
    RENAMED = "renamed"       # 重新命名 (需要額外提示)


@dataclass
class ColumnChange:
    """
    欄位變更描述

    Attributes:
        column_name: 欄位名稱
        change_type: 變更類型
        old_type: 原類型 (僅 TYPE_CHANGED)
        new_type: 新類型 (ADDED 或 TYPE_CHANGED)
        new_name: 新名稱 (僅 RENAMED)
    """
    column_name: str
    change_type: ChangeType
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    new_name: Optional[str] = None

    def __str__(self) -> str:
        if self.change_type == ChangeType.ADDED:
            return f"+ {self.column_name} ({self.new_type})"
        elif self.change_type == ChangeType.REMOVED:
            return f"- {self.column_name}"
        elif self.change_type == ChangeType.TYPE_CHANGED:
            return f"~ {self.column_name}: {self.old_type} -> {self.new_type}"
        elif self.change_type == ChangeType.RENAMED:
            return f"* {self.column_name} -> {self.new_name}"
        return f"? {self.column_name}"


@dataclass
class SchemaDiff:
    """
    Schema 差異結果

    Attributes:
        table_name: 表格名稱
        changes: 變更列表
        current_schema: 當前 Schema (column_name -> type)
        target_schema: 目標 Schema (column_name -> type)
    """
    table_name: str
    changes: List[ColumnChange] = field(default_factory=list)
    current_schema: Dict[str, str] = field(default_factory=dict)
    target_schema: Dict[str, str] = field(default_factory=dict)

    @property
    def has_changes(self) -> bool:
        """是否有變更"""
        return len(self.changes) > 0

    @property
    def added_columns(self) -> List[ColumnChange]:
        """新增的欄位"""
        return [c for c in self.changes if c.change_type == ChangeType.ADDED]

    @property
    def removed_columns(self) -> List[ColumnChange]:
        """移除的欄位"""
        return [c for c in self.changes if c.change_type == ChangeType.REMOVED]

    @property
    def type_changed_columns(self) -> List[ColumnChange]:
        """類型變更的欄位"""
        return [c for c in self.changes if c.change_type == ChangeType.TYPE_CHANGED]

    @property
    def is_safe(self) -> bool:
        """
        是否為安全遷移 (只有新增欄位，沒有移除或類型變更)
        """
        return (
            len(self.removed_columns) == 0 and
            len(self.type_changed_columns) == 0
        )

    def report(self) -> str:
        """
        生成變更報告

        Returns:
            str: 格式化的變更報告
        """
        if not self.has_changes:
            return f"Schema Diff for '{self.table_name}': No changes"

        lines = [f"Schema Diff for '{self.table_name}':"]

        if self.added_columns:
            lines.append("\n  Added columns:")
            for change in self.added_columns:
                lines.append(f"    {change}")

        if self.removed_columns:
            lines.append("\n  Removed columns:")
            for change in self.removed_columns:
                lines.append(f"    {change}")

        if self.type_changed_columns:
            lines.append("\n  Type changed columns:")
            for change in self.type_changed_columns:
                lines.append(f"    {change}")

        # 統計
        lines.append(f"\n  Summary: {len(self.added_columns)} added, "
                    f"{len(self.removed_columns)} removed, "
                    f"{len(self.type_changed_columns)} type changed")

        if self.is_safe:
            lines.append("  Status: SAFE (can migrate without data loss)")
        else:
            lines.append("  Status: REQUIRES REVIEW (potential data loss)")

        return "\n".join(lines)

    @classmethod
    def compare(
        cls,
        db_manager: "DuckDBManager",
        table_name: str,
        target_df: pd.DataFrame,
        ignore_case: bool = False
    ) -> "SchemaDiff":
        """
        比對資料庫表格與 DataFrame 的 Schema 差異

        Args:
            db_manager: DuckDBManager 實例
            table_name: 表格名稱
            target_df: 目標 DataFrame
            ignore_case: 是否忽略欄位名稱大小寫

        Returns:
            SchemaDiff: 差異結果
        """
        changes = []

        # 取得當前 Schema
        current_schema = {}
        if db_manager._table_exists(table_name):
            schema_df = db_manager.conn.sql(f'DESCRIBE "{table_name}"').df()
            for _, row in schema_df.iterrows():
                col_name = row['column_name']
                if ignore_case:
                    col_name = col_name.lower()
                current_schema[col_name] = row['column_type']

        # 取得目標 Schema
        target_schema = {}
        for col in target_df.columns:
            col_key = col.lower() if ignore_case else col
            target_schema[col_key] = get_duckdb_dtype(str(target_df[col].dtype))

        # 比對差異
        current_cols = set(current_schema.keys())
        target_cols = set(target_schema.keys())

        # 新增的欄位
        for col in target_cols - current_cols:
            original_col = col
            if ignore_case:
                # 找回原始名稱
                for c in target_df.columns:
                    if c.lower() == col:
                        original_col = c
                        break
            changes.append(ColumnChange(
                column_name=original_col,
                change_type=ChangeType.ADDED,
                new_type=target_schema[col]
            ))

        # 移除的欄位
        for col in current_cols - target_cols:
            changes.append(ColumnChange(
                column_name=col,
                change_type=ChangeType.REMOVED,
                old_type=current_schema[col]
            ))

        # 類型變更的欄位
        for col in current_cols & target_cols:
            current_type = current_schema[col].upper()
            target_type = target_schema[col].upper()

            # 正規化類型名稱進行比較
            if not cls._types_compatible(current_type, target_type):
                original_col = col
                if ignore_case:
                    for c in target_df.columns:
                        if c.lower() == col:
                            original_col = c
                            break
                changes.append(ColumnChange(
                    column_name=original_col,
                    change_type=ChangeType.TYPE_CHANGED,
                    old_type=current_type,
                    new_type=target_type
                ))

        return cls(
            table_name=table_name,
            changes=changes,
            current_schema=current_schema,
            target_schema=target_schema
        )

    @staticmethod
    def _types_compatible(type1: str, type2: str) -> bool:
        """
        檢查兩個類型是否相容

        Args:
            type1: 第一個類型
            type2: 第二個類型

        Returns:
            bool: 是否相容
        """
        # 正規化類型名稱
        type1 = type1.upper().strip()
        type2 = type2.upper().strip()

        # 完全相同
        if type1 == type2:
            return True

        # 類型別名映射
        type_aliases = {
            'INT': ['INTEGER', 'INT4', 'SIGNED'],
            'BIGINT': ['INT8', 'LONG'],
            'SMALLINT': ['INT2', 'SHORT'],
            'TINYINT': ['INT1'],
            'DOUBLE': ['FLOAT8', 'NUMERIC', 'DECIMAL'],
            'REAL': ['FLOAT4', 'FLOAT'],
            'VARCHAR': ['STRING', 'TEXT', 'CHAR', 'BPCHAR'],
            'BOOLEAN': ['BOOL', 'LOGICAL'],
            'TIMESTAMP': ['DATETIME', 'TIMESTAMP WITH TIME ZONE'],
        }

        for canonical, aliases in type_aliases.items():
            all_types = [canonical] + aliases
            if type1 in all_types and type2 in all_types:
                return True

        return False
