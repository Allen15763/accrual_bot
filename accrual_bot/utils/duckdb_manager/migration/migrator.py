"""
Schema 遷移器模組

提供 Schema 遷移的主要功能類。
"""

from typing import TYPE_CHECKING, Optional, Union
import pandas as pd
from datetime import datetime

if TYPE_CHECKING:
    from ..manager import DuckDBManager

from .schema_diff import SchemaDiff
from .strategies import MigrationStrategy, MigrationPlan, MigrationPlanner
from ..exceptions import DuckDBMigrationError


class SchemaMigrator:
    """
    Schema 遷移器

    提供資料庫表格 Schema 遷移功能。

    Example:
        >>> with DuckDBManager("./data.duckdb") as db:
        ...     migrator = SchemaMigrator(db)
        ...
        ...     # 比對 schema
        ...     diff = migrator.compare_schema("users", new_df)
        ...     print(diff.report())
        ...
        ...     # 執行遷移
        ...     result = migrator.migrate("users", new_df, strategy="safe")
    """

    def __init__(self, db_manager: "DuckDBManager"):
        """
        初始化遷移器

        Args:
            db_manager: DuckDBManager 實例
        """
        self.db = db_manager
        self.logger = db_manager.logger

    def compare_schema(
        self,
        table_name: str,
        target_df: pd.DataFrame,
        ignore_case: bool = False
    ) -> SchemaDiff:
        """
        比對表格與 DataFrame 的 Schema 差異

        Args:
            table_name: 表格名稱
            target_df: 目標 DataFrame
            ignore_case: 是否忽略欄位名稱大小寫

        Returns:
            SchemaDiff: 差異結果
        """
        return SchemaDiff.compare(
            db_manager=self.db,
            table_name=table_name,
            target_df=target_df,
            ignore_case=ignore_case
        )

    def create_migration_plan(
        self,
        table_name: str,
        target_df: pd.DataFrame,
        strategy: Union[str, MigrationStrategy] = MigrationStrategy.SAFE
    ) -> MigrationPlan:
        """
        建立遷移計劃

        Args:
            table_name: 表格名稱
            target_df: 目標 DataFrame
            strategy: 遷移策略 ("safe", "force", "backup_first", "dry_run")

        Returns:
            MigrationPlan: 遷移計劃
        """
        # 轉換字串策略
        if isinstance(strategy, str):
            strategy = MigrationStrategy(strategy.lower())

        diff = self.compare_schema(table_name, target_df)
        return MigrationPlanner.create_plan(diff, strategy)

    def migrate(
        self,
        table_name: str,
        target_df: pd.DataFrame,
        strategy: Union[str, MigrationStrategy] = MigrationStrategy.SAFE,
        backup_format: str = "parquet"
    ) -> dict:
        """
        執行 Schema 遷移

        Args:
            table_name: 表格名稱
            target_df: 目標 DataFrame
            strategy: 遷移策略 ("safe", "force", "backup_first", "dry_run")
            backup_format: 備份格式 ("parquet", "csv", "json")

        Returns:
            dict: 遷移結果，包含:
                - success: 是否成功
                - plan: 執行的遷移計劃
                - backup_path: 備份檔案路徑 (如果有備份)
                - executed_operations: 實際執行的操作數
                - errors: 錯誤訊息列表

        Raises:
            DuckDBMigrationError: 遷移失敗時
        """
        # 轉換字串策略
        if isinstance(strategy, str):
            strategy = MigrationStrategy(strategy.lower())

        result = {
            "success": False,
            "plan": None,
            "backup_path": None,
            "executed_operations": 0,
            "errors": []
        }

        try:
            # 建立遷移計劃
            plan = self.create_migration_plan(table_name, target_df, strategy)
            result["plan"] = plan

            self.logger.info(f"Migration plan created:\n{plan.report()}")

            # 檢查是否需要執行
            if not plan.will_execute:
                self.logger.info("Dry run mode - no operations executed")
                result["success"] = True
                return result

            if not plan.operations:
                self.logger.info("No operations to execute")
                result["success"] = True
                return result

            # 執行備份 (如果需要)
            if plan.backup_required:
                backup_path = self._backup_table(table_name, backup_format)
                result["backup_path"] = backup_path

            # 執行遷移操作
            for i, operation in enumerate(plan.operations, 1):
                try:
                    self.logger.info(f"Executing operation {i}/{len(plan.operations)}: {operation}")
                    self.db.conn.sql(operation)
                    result["executed_operations"] += 1
                except Exception as e:
                    error_msg = f"Operation {i} failed: {operation}\nError: {e}"
                    self.logger.error(error_msg)
                    result["errors"].append(error_msg)

                    if strategy != MigrationStrategy.FORCE:
                        raise DuckDBMigrationError(table_name, error_msg)

            result["success"] = len(result["errors"]) == 0

            if result["success"]:
                self.logger.info(
                    f"Migration completed successfully. "
                    f"Executed {result['executed_operations']} operations."
                )
            else:
                self.logger.warning(
                    f"Migration completed with {len(result['errors'])} errors."
                )

            return result

        except DuckDBMigrationError:
            raise
        except Exception as e:
            error_msg = f"Migration failed: {e}"
            self.logger.error(error_msg)
            result["errors"].append(error_msg)
            raise DuckDBMigrationError(table_name, error_msg)

    def _backup_table(self, table_name: str, backup_format: str) -> str:
        """
        備份表格

        Args:
            table_name: 表格名稱
            backup_format: 備份格式

        Returns:
            str: 備份檔案路徑
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{table_name}_migration_backup_{timestamp}.{backup_format}"

        self.logger.info(f"Creating backup: {backup_path}")
        self.db.backup_table(table_name, backup_format, backup_path)

        return backup_path

    def auto_migrate(
        self,
        table_name: str,
        df: pd.DataFrame,
        create_if_not_exists: bool = True
    ) -> dict:
        """
        自動遷移表格

        - 如果表格不存在且 create_if_not_exists=True，則建立新表格
        - 如果有安全的 schema 變更（只有新增欄位），自動執行
        - 如果有危險的變更，返回警告但不執行

        Args:
            table_name: 表格名稱
            df: DataFrame
            create_if_not_exists: 表格不存在時是否建立

        Returns:
            dict: 遷移結果
        """
        result = {
            "action": None,
            "success": False,
            "message": "",
            "diff": None
        }

        # 檢查表格是否存在
        if not self.db._table_exists(table_name):
            if create_if_not_exists:
                self.logger.info(f"Table '{table_name}' does not exist. Creating...")
                success = self.db.create_table_from_df(table_name, df)
                result["action"] = "created"
                result["success"] = success
                result["message"] = f"Created new table '{table_name}'"
            else:
                result["action"] = "skipped"
                result["message"] = f"Table '{table_name}' does not exist"
            return result

        # 比對 schema
        diff = self.compare_schema(table_name, df)
        result["diff"] = diff

        if not diff.has_changes:
            result["action"] = "no_change"
            result["success"] = True
            result["message"] = "No schema changes detected"
            return result

        # 檢查是否可以自動遷移
        if MigrationPlanner.can_auto_migrate(diff):
            self.logger.info("Safe migration detected. Auto-migrating...")
            migrate_result = self.migrate(table_name, df, MigrationStrategy.SAFE)
            result["action"] = "auto_migrated"
            result["success"] = migrate_result["success"]
            result["message"] = f"Auto-migrated {len(diff.added_columns)} new columns"
        else:
            result["action"] = "requires_review"
            result["success"] = False
            result["message"] = (
                f"Schema changes require manual review:\n"
                f"{diff.report()}"
            )
            self.logger.warning(result["message"])

        return result
