"""
Schema 遷移策略模組

定義不同的遷移策略及其行為。
"""

from enum import Enum
from typing import List
from dataclasses import dataclass

from .schema_diff import SchemaDiff, ChangeType


class MigrationStrategy(Enum):
    """
    遷移策略枚舉

    - SAFE: 安全模式，只執行不會造成資料遺失的操作
    - FORCE: 強制模式，執行所有變更（可能造成資料遺失）
    - BACKUP_FIRST: 先備份再遷移
    - DRY_RUN: 乾跑模式，只顯示將執行的操作，不實際執行
    """
    SAFE = "safe"
    FORCE = "force"
    BACKUP_FIRST = "backup_first"
    DRY_RUN = "dry_run"


@dataclass
class MigrationPlan:
    """
    遷移計劃

    Attributes:
        strategy: 使用的遷移策略
        diff: Schema 差異
        operations: 將執行的 SQL 操作列表
        warnings: 警告訊息列表
        will_execute: 是否會實際執行
        backup_required: 是否需要備份
    """
    strategy: MigrationStrategy
    diff: SchemaDiff
    operations: List[str]
    warnings: List[str]
    will_execute: bool
    backup_required: bool = False

    def report(self) -> str:
        """生成遷移計劃報告"""
        lines = [
            f"Migration Plan for '{self.diff.table_name}'",
            f"Strategy: {self.strategy.value}",
            f"Will Execute: {'Yes' if self.will_execute else 'No (dry run)'}",
        ]

        if self.backup_required:
            lines.append("Backup: Required before migration")

        if self.warnings:
            lines.append("\nWarnings:")
            for warning in self.warnings:
                lines.append(f"  ! {warning}")

        if self.operations:
            lines.append(f"\nOperations ({len(self.operations)} total):")
            for i, op in enumerate(self.operations, 1):
                lines.append(f"  {i}. {op}")

        return "\n".join(lines)


class MigrationPlanner:
    """
    遷移計劃生成器

    根據 Schema 差異和策略生成遷移計劃。
    """

    @classmethod
    def create_plan(
        cls,
        diff: SchemaDiff,
        strategy: MigrationStrategy = MigrationStrategy.SAFE
    ) -> MigrationPlan:
        """
        建立遷移計劃

        Args:
            diff: Schema 差異
            strategy: 遷移策略

        Returns:
            MigrationPlan: 遷移計劃
        """
        operations = []
        warnings = []
        will_execute = strategy != MigrationStrategy.DRY_RUN
        backup_required = strategy == MigrationStrategy.BACKUP_FIRST

        table_name = diff.table_name

        # 處理新增欄位 (所有策略都支援)
        for change in diff.added_columns:
            col = change.column_name
            dtype = change.new_type
            operations.append(
                f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype}'
            )

        # 處理移除欄位
        for change in diff.removed_columns:
            col = change.column_name
            if strategy in [MigrationStrategy.FORCE, MigrationStrategy.BACKUP_FIRST]:
                operations.append(
                    f'ALTER TABLE "{table_name}" DROP COLUMN "{col}"'
                )
            else:
                warnings.append(
                    f"Column '{col}' would be removed but skipped in {strategy.value} mode"
                )

        # 處理類型變更
        for change in diff.type_changed_columns:
            col = change.column_name
            old_type = change.old_type
            new_type = change.new_type

            if strategy in [MigrationStrategy.FORCE, MigrationStrategy.BACKUP_FIRST]:
                # 使用 TRY_CAST 嘗試轉換
                operations.append(
                    f'ALTER TABLE "{table_name}" ALTER COLUMN "{col}" TYPE {new_type}'
                )
                warnings.append(
                    f"Type change '{col}': {old_type} -> {new_type} may cause data loss"
                )
            else:
                warnings.append(
                    f"Type change '{col}': {old_type} -> {new_type} skipped in {strategy.value} mode"
                )

        # 在 SAFE 模式下，如果有任何危險操作，不執行
        if strategy == MigrationStrategy.SAFE:
            if diff.removed_columns or diff.type_changed_columns:
                will_execute = will_execute and len(operations) > 0
                if diff.removed_columns or diff.type_changed_columns:
                    warnings.append(
                        "Some changes skipped in safe mode. Use 'force' or 'backup_first' strategy for full migration."
                    )

        return MigrationPlan(
            strategy=strategy,
            diff=diff,
            operations=operations,
            warnings=warnings,
            will_execute=will_execute,
            backup_required=backup_required
        )

    @classmethod
    def can_auto_migrate(cls, diff: SchemaDiff) -> bool:
        """
        檢查是否可以自動遷移（無需人工確認）

        只有純新增欄位的情況才能自動遷移。

        Args:
            diff: Schema 差異

        Returns:
            bool: 是否可以自動遷移
        """
        return diff.is_safe and len(diff.added_columns) > 0
