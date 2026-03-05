"""
事務處理操作 Mixin

提供事務處理與資料驗證相關操作。
"""

import pandas as pd
from typing import Dict, Any, List

from .base import OperationMixin
from ..exceptions import DuckDBTransactionError


class TransactionMixin(OperationMixin):
    """
    事務處理操作 Mixin

    提供事務處理與資料驗證操作:
    - execute_transaction: 執行事務操作
    - validate_data_integrity: 驗證資料完整性
    """

    def execute_transaction(self, operations: List[str]) -> bool:
        """
        執行事務操作

        Args:
            operations: SQL 操作列表

        Returns:
            bool: 是否成功執行所有操作
        """
        try:
            self.logger.info(f"開始執行事務操作 (共 {len(operations)} 個操作)")

            # 開始事務
            self.conn.sql("BEGIN TRANSACTION")

            for i, operation in enumerate(operations, 1):
                try:
                    self.logger.debug(
                        f"執行操作 {i}/{len(operations)}: {operation[:100]}..."
                    )
                    self.conn.sql(operation)
                except Exception as e:
                    self.logger.error(f"操作 {i} 失敗: {e}")
                    self.conn.sql("ROLLBACK")
                    self.logger.error("事務已回滾")
                    raise DuckDBTransactionError(i, str(e))

            # 提交事務
            self.conn.sql("COMMIT")
            self.logger.info(f"成功執行所有 {len(operations)} 個操作")
            return True

        except DuckDBTransactionError:
            raise
        except Exception as e:
            self.logger.error(f"事務執行失敗: {e}")
            try:
                self.conn.sql("ROLLBACK")
                self.logger.error("事務已回滾")
            except Exception:
                pass
            return False

    def validate_data_integrity(
        self,
        table_name: str,
        checks: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        驗證資料完整性

        Args:
            table_name: 表格名稱
            checks: 自定義檢查規則 (名稱: SQL)

        Returns:
            dict: 驗證結果，包含:
                - table_name: 表格名稱
                - total_rows: 總行數
                - null_counts: 各欄位的 NULL 計數
                - duplicate_rows: 重複行數
                - data_types: 各欄位的資料類型
                - custom_checks: 自定義檢查結果
        """
        try:
            self.logger.info(f"開始驗證表格 '{table_name}' 的資料完整性")

            results = {
                'table_name': table_name,
                'total_rows': 0,
                'null_counts': {},
                'duplicate_rows': 0,
                'data_types': {},
                'custom_checks': {}
            }

            # 基本統計
            total_rows = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']
            results['total_rows'] = total_rows

            # 檢查每個欄位的 NULL 值
            schema = self.conn.sql(f'DESCRIBE "{table_name}"').df()
            if schema is not None:
                for _, col_info in schema.iterrows():
                    col_name = col_info['column_name']
                    null_count = self.conn.sql(
                        f'SELECT COUNT(*) as count FROM "{table_name}" '
                        f'WHERE "{col_name}" IS NULL'
                    ).df().iloc[0]['count']
                    results['null_counts'][col_name] = null_count
                    results['data_types'][col_name] = col_info['column_type']

            # 檢查重複行
            duplicate_count = self.conn.sql(f'''
                SELECT COUNT(*) as count FROM (
                    SELECT COUNT(*) as row_count
                    FROM "{table_name}"
                    GROUP BY *
                    HAVING COUNT(*) > 1
                )
            ''').df().iloc[0]['count']
            results['duplicate_rows'] = duplicate_count

            # 自定義檢查
            if checks:
                for check_name, check_sql in checks.items():
                    try:
                        check_result = self.conn.sql(
                            check_sql.format(table_name=table_name)
                        ).df()
                        results['custom_checks'][check_name] = (
                            check_result.to_dict('records')
                        )
                    except Exception as e:
                        results['custom_checks'][check_name] = f"Error: {e}"

            self.logger.info("完成資料完整性驗證")
            return results

        except Exception as e:
            self.logger.error(f"資料完整性驗證失敗: {e}")
            return {}

    # ========== 便利方法 ==========

    def check_null_values(
        self,
        table_name: str,
        columns: List[str] = None
    ) -> Dict[str, int]:
        """
        檢查指定欄位的 NULL 值數量

        Args:
            table_name: 表格名稱
            columns: 要檢查的欄位列表，為 None 時檢查所有欄位

        Returns:
            dict: 欄位名稱 -> NULL 計數
        """
        try:
            if columns is None:
                schema = self.conn.sql(f'DESCRIBE "{table_name}"').df()
                columns = schema['column_name'].tolist() if schema is not None else []

            null_counts = {}
            for col_name in columns:
                count = self.conn.sql(
                    f'SELECT COUNT(*) as count FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NULL'
                ).df().iloc[0]['count']
                null_counts[col_name] = count

            return null_counts

        except Exception as e:
            self.logger.error(f"檢查 NULL 值失敗: {e}")
            return {}

    def check_duplicates(
        self,
        table_name: str,
        key_columns: List[str]
    ) -> pd.DataFrame:
        """
        檢查基於指定欄位的重複記錄

        Args:
            table_name: 表格名稱
            key_columns: 用於判斷重複的欄位列表

        Returns:
            pd.DataFrame: 重複記錄及其計數
        """
        try:
            key_cols_sql = ", ".join(f'"{col}"' for col in key_columns)
            query = f"""
            SELECT {key_cols_sql}, COUNT(*) as duplicate_count
            FROM "{table_name}"
            GROUP BY {key_cols_sql}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            """
            return self.conn.sql(query).df()

        except Exception as e:
            self.logger.error(f"檢查重複記錄失敗: {e}")
            return pd.DataFrame()
