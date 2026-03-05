"""
資料清理操作 Mixin

提供資料清理與欄位轉換相關操作。
"""

import pandas as pd
from typing import Optional, List

from .base import OperationMixin


class DataCleaningMixin(OperationMixin):
    """
    資料清理操作 Mixin

    提供資料清理與轉換操作:
    - alter_column_type: 修改欄位類型
    - clean_numeric_column: 清理數字欄位
    - clean_and_convert_column: 清理並轉換欄位
    - preview_column_values: 預覽欄位值
    """

    def alter_column_type(
        self,
        table_name: str,
        column_name: str,
        new_type: str,
        validate_conversion: bool = True
    ) -> bool:
        """
        修改表格欄位的資料型態

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            new_type: 新的資料型態 (如 'BIGINT', 'VARCHAR', 'DOUBLE' 等)
            validate_conversion: 是否先驗證資料能否轉換

        Returns:
            bool: 是否成功
        """
        try:
            self.logger.info(
                f"開始修改表格 '{table_name}' 的欄位 '{column_name}' "
                f"型態為 {new_type}"
            )

            if validate_conversion:
                self.logger.debug(
                    f"驗證 '{column_name}' 欄位資料是否能轉換為 {new_type}"
                )

                if new_type.upper() in ['BIGINT', 'INTEGER', 'DOUBLE', 'REAL']:
                    validation_query = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                    """

                    invalid_result = self.conn.sql(validation_query).df()
                    invalid_count = (
                        invalid_result.iloc[0]['invalid_count']
                        if not invalid_result.empty else 0
                    )

                    if invalid_count > 0:
                        sample_query = f"""
                        SELECT "{column_name}" as invalid_value
                        FROM "{table_name}"
                        WHERE "{column_name}" IS NOT NULL
                        AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                        LIMIT 5
                        """
                        samples = self.conn.sql(sample_query).df()
                        self.logger.error(
                            f"發現 {invalid_count} 筆無法轉換的資料，"
                            f"範例: {samples['invalid_value'].tolist()}"
                        )
                        return False

                    self.logger.info(f"所有資料都能成功轉換為 {new_type}")

            # 執行欄位型態修改
            alter_query = (
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{column_name}" TYPE {new_type}'
            )
            self.conn.sql(alter_query)

            self.logger.info(f"成功修改欄位 '{column_name}' 型態為 {new_type}")

            # 驗證修改結果
            schema = self.conn.sql(f'DESCRIBE "{table_name}"').df()
            if schema is not None:
                column_info = schema[schema['column_name'] == column_name]
                if not column_info.empty:
                    actual_type = column_info.iloc[0]['column_type']
                    self.logger.info(
                        f"確認: 欄位 '{column_name}' 目前型態為 {actual_type}"
                    )

            return True

        except Exception as e:
            self.logger.error(f"修改欄位型態失敗: {e}")
            return False

    def clean_numeric_column(
        self,
        table_name: str,
        column_name: str,
        remove_chars: List[str] = None,
        preview_only: bool = False
    ) -> bool:
        """
        清理數字欄位中的非數字字符

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱；for VARCHAR column
            remove_chars: 要移除的字符列表
            preview_only: 僅預覽清理結果，不實際執行更新

        Returns:
            bool: 是否成功
        """
        try:
            if remove_chars is None:
                # 常見的千分位符號和貨幣符號
                remove_chars = [',', '$', '€', '¥', ' ', '￥', '₩', '£', '_', '-']

            self.logger.info(f"開始清理表格 '{table_name}' 的欄位 '{column_name}'")
            self.logger.debug(f"將移除字符: {remove_chars}")

            # 檢查需要清理的資料數量
            check_conditions = [
                f'"{column_name}" LIKE \'%{char}%\''
                for char in remove_chars
            ]

            check_query = f"""
            SELECT COUNT(*) as dirty_count
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            dirty_result = self.conn.sql(check_query).df()
            dirty_count = (
                dirty_result.iloc[0]['dirty_count']
                if not dirty_result.empty else 0
            )

            if dirty_count == 0:
                self.logger.info(f"欄位 '{column_name}' 無需清理")
                return True

            self.logger.info(f"發現 {dirty_count} 筆需要清理的資料")

            # 建立清理邏輯
            cleaned_expression = f'"{column_name}"'
            for char in remove_chars:
                cleaned_expression = (
                    f"REPLACE({cleaned_expression}, '{char}', '')"
                )

            # 預覽範例
            sample_query = f"""
            SELECT
                "{column_name}" as original_value,
                {cleaned_expression} as cleaned_value
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            LIMIT 10
            """

            sample_result = self.conn.sql(sample_query).df()

            self.logger.info("清理範例:")
            for _, row in sample_result.iterrows():
                self.logger.info(
                    f"  '{row['original_value']}' -> '{row['cleaned_value']}'"
                )

            if preview_only:
                self.logger.info("預覽模式：未執行實際更新")
                return True

            # 執行清理
            update_query = f"""
            UPDATE "{table_name}"
            SET "{column_name}" = {cleaned_expression}
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            self.conn.sql(update_query)

            # 驗證清理結果
            verify_query = f"""
            SELECT COUNT(*) as remaining_dirty
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            verify_result = self.conn.sql(verify_query).df()
            remaining_dirty = (
                verify_result.iloc[0]['remaining_dirty']
                if not verify_result.empty else 0
            )

            if remaining_dirty == 0:
                self.logger.info(f"成功清理 {dirty_count} 筆資料")
            else:
                self.logger.warning(
                    f"清理完成，但仍有 {remaining_dirty} 筆資料"
                    "可能需要額外處理"
                )

            return True

        except Exception as e:
            self.logger.error(f"清理數據失敗: {e}")
            return False

    def clean_and_convert_column(
        self,
        table_name: str,
        column_name: str,
        target_type: str,
        remove_chars: List[str] = None,
        handle_empty_as_null: bool = True
    ) -> bool:
        """
        清理並轉換欄位型態的一站式方法

        所有步驟在同一個 Transaction 中執行，任何步驟失敗自動回滾。

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            target_type: 目標資料型態
            remove_chars: 要移除的字符列表
            handle_empty_as_null: 是否將空字串轉換為 NULL

        Returns:
            bool: 是否成功
        """
        try:
            self.logger.info(
                f"開始清理並轉換欄位 '{column_name}' 為 {target_type}"
            )

            if remove_chars is None:
                remove_chars = [',', '$', '€', '¥', ' ', '￥', '₩', '£', '_', '-']

            # 先驗證 (在事務外，只讀操作)
            validation_success = self._validate_conversion(
                table_name, column_name, target_type
            )

            # 建立清理 SQL
            cleaned_expression = f'"{column_name}"'
            for char in remove_chars:
                cleaned_expression = (
                    f"REPLACE({cleaned_expression}, '{char}', '')"
                )

            check_conditions = [
                f'"{column_name}" LIKE \'%{char}%\''
                for char in remove_chars
            ]

            # 原子操作: UPDATE (清理) + UPDATE (空→NULL) + ALTER TYPE
            with self._atomic():
                # Step 1: 清理非數字字符
                update_query = f"""
                UPDATE "{table_name}"
                SET "{column_name}" = {cleaned_expression}
                WHERE "{column_name}" IS NOT NULL
                AND ({' OR '.join(check_conditions)})
                """
                self.conn.sql(update_query)
                self.logger.debug("Step 1: 清理非數字字符完成")

                # Step 2: 處理空字串
                if handle_empty_as_null:
                    empty_query = f"""
                    UPDATE "{table_name}"
                    SET "{column_name}" = NULL
                    WHERE "{column_name}" = '' OR "{column_name}" = ' '
                    """
                    self.conn.sql(empty_query)
                    self.logger.debug("Step 2: 空字串轉 NULL 完成")

                # Step 3: 驗證轉換可行性 (在事務內再次驗證)
                if target_type.upper() in [
                    'BIGINT', 'INTEGER', 'DOUBLE', 'REAL'
                ]:
                    invalid_result = self.conn.sql(f"""
                    SELECT COUNT(*) as invalid_count
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                    """).df()
                    invalid_count = (
                        invalid_result.iloc[0]['invalid_count']
                        if not invalid_result.empty else 0
                    )
                    if invalid_count > 0:
                        raise ValueError(
                            f"清理後仍有 {invalid_count} 筆"
                            f"無法轉換為 {target_type}"
                        )

                # Step 4: 執行型態轉換
                self.conn.sql(
                    f'ALTER TABLE "{table_name}" '
                    f'ALTER COLUMN "{column_name}" TYPE {target_type}'
                )
                self.logger.debug("Step 4: 型態轉換完成")

            self.logger.info(
                f"成功完成清理和轉換！"
                f"欄位 '{column_name}' 現在是 {target_type} 型態"
            )
            return True

        except Exception as e:
            self.logger.error(f"清理和轉換過程失敗: {e}")
            return False

    def _validate_conversion(
        self,
        table_name: str,
        column_name: str,
        target_type: str
    ) -> bool:
        """
        內部方法：驗證清理後的資料是否能成功轉換

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            target_type: 目標資料型態

        Returns:
            bool: 是否可以轉換
        """
        try:
            if target_type.upper() in ['BIGINT', 'INTEGER', 'DOUBLE', 'REAL']:
                validation_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                """

                invalid_result = self.conn.sql(validation_query).df()
                invalid_count = (
                    invalid_result.iloc[0]['invalid_count']
                    if not invalid_result.empty else 0
                )

                if invalid_count > 0:
                    sample_query = f"""
                    SELECT "{column_name}" as problematic_value
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                    LIMIT 5
                    """
                    samples = self.conn.sql(sample_query).df()
                    self.logger.error(
                        f"清理後仍有 {invalid_count} 筆無法轉換的資料"
                    )
                    self.logger.error(
                        f"範例: {samples['problematic_value'].tolist()}"
                    )
                    return False

                self.logger.info(
                    f"清理後所有資料都能成功轉換為 {target_type}"
                )

            return True

        except Exception as e:
            self.logger.error(f"驗證轉換失敗: {e}")
            return False

    def preview_column_values(
        self,
        table_name: str,
        column_name: str,
        limit: int = 20,
        show_unique: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        預覽欄位的值，用於了解資料格式

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            limit: 顯示筆數限制
            show_unique: 是否只顯示唯一值

        Returns:
            pd.DataFrame: 預覽結果
        """
        try:
            if show_unique:
                query = f"""
                SELECT DISTINCT "{column_name}" as value, COUNT(*) as count
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                ORDER BY count DESC
                LIMIT {limit}
                """
            else:
                query = f"""
                SELECT "{column_name}" as value
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                LIMIT {limit}
                """

            result = self.conn.sql(query).df()
            self.logger.info(f"欄位 '{column_name}' 的範例資料:")
            return result

        except Exception as e:
            self.logger.error(f"預覽資料失敗: {e}")
            return None

    # ========== 便利方法 ==========

    def add_column(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        default: any = None
    ) -> bool:
        """
        新增欄位到表格

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            column_type: 欄位類型
            default: 預設值

        Returns:
            bool: 是否成功
        """
        try:
            if default is not None:
                default_clause = f" DEFAULT {repr(default)}"
            else:
                default_clause = ""

            self.conn.sql(
                f'ALTER TABLE "{table_name}" '
                f'ADD COLUMN "{column_name}" {column_type}{default_clause}'
            )

            self.logger.info(
                f"成功新增欄位 '{column_name}' ({column_type}) "
                f"到表格 '{table_name}'"
            )
            return True

        except Exception as e:
            self.logger.error(f"新增欄位失敗: {e}")
            return False

    def rename_column(
        self,
        table_name: str,
        old_name: str,
        new_name: str
    ) -> bool:
        """
        重新命名欄位

        Args:
            table_name: 表格名稱
            old_name: 原欄位名稱
            new_name: 新欄位名稱

        Returns:
            bool: 是否成功
        """
        try:
            self.conn.sql(
                f'ALTER TABLE "{table_name}" '
                f'RENAME COLUMN "{old_name}" TO "{new_name}"'
            )

            self.logger.info(
                f"成功重新命名欄位: '{old_name}' -> '{new_name}'"
            )
            return True

        except Exception as e:
            self.logger.error(f"重新命名欄位失敗: {e}")
            return False

    def drop_column(self, table_name: str, column_name: str) -> bool:
        """
        刪除欄位

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱

        Returns:
            bool: 是否成功
        """
        try:
            self.conn.sql(
                f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'
            )

            self.logger.info(
                f"成功刪除欄位 '{column_name}' 從表格 '{table_name}'"
            )
            return True

        except Exception as e:
            self.logger.error(f"刪除欄位失敗: {e}")
            return False
