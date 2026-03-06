"""
CRUD 操作 Mixin

提供 Create, Read, Update, Delete 操作。
"""

import pandas as pd
from typing import Optional, List

from .base import OperationMixin
from ..exceptions import (
    DuckDBTableExistsError,
    DuckDBTableNotFoundError,
)
from ..utils.type_mapping import get_duckdb_dtype


class CRUDMixin(OperationMixin):
    """
    CRUD 操作 Mixin

    提供基本的資料庫 CRUD 操作:
    - create_table_from_df: 從 DataFrame 建立表格
    - insert_df_into_table: 插入資料
    - upsert_df_into_table: 更新或插入資料
    - query_to_df: 執行查詢並返回 DataFrame
    - delete_data: 刪除資料
    """

    def create_table_from_df(
        self,
        table_name: str,
        df: pd.DataFrame,
        if_exists: str = 'fail'
    ) -> bool:
        """
        從 DataFrame 建立表格

        Args:
            table_name: 表格名稱
            df: pandas DataFrame
            if_exists: 'fail' (報錯), 'replace' (替換), 'append' (附加)

        Returns:
            bool: 是否成功建立
        """
        try:
            if self.config.enable_query_logging:
                self.logger.info(f"開始建立表格 '{table_name}'，模式: {if_exists}")

            # 檢查表格是否已存在
            table_exists = self._table_exists(table_name)

            if table_exists:
                self.logger.debug(f"表格 '{table_name}' 已存在")
                if if_exists == 'fail':
                    raise DuckDBTableExistsError(table_name)
                elif if_exists == 'replace':
                    self.logger.warning(f"替換現有表格 '{table_name}'")
                    # 原子操作: DROP + CREATE + INSERT
                    columns_with_types = []
                    for col in df.columns:
                        dtype_str = str(df[col].dtype)
                        duckdb_dtype = get_duckdb_dtype(dtype_str)
                        columns_with_types.append(f'"{col}" {duckdb_dtype}')
                        self.logger.debug(
                            f"欄位 '{col}': {dtype_str} -> {duckdb_dtype}"
                        )
                    columns_sql = ", ".join(columns_with_types)

                    with self._atomic():
                        self.conn.sql(
                            f'DROP TABLE IF EXISTS "{table_name}"'
                        )
                        self.conn.sql(
                            f'CREATE TABLE "{table_name}" ({columns_sql})'
                        )
                        self.conn.sql(
                            f'INSERT INTO "{table_name}" SELECT * FROM df'
                        )

                    self.logger.info(
                        f"成功替換表格 '{table_name}'，"
                        f"插入 {len(df):,} 筆資料"
                    )
                    return True
                elif if_exists == 'append':
                    self.logger.info(f"將資料附加到現有表格 '{table_name}'")
                    return self.insert_df_into_table(table_name, df)

            # 建立欄位定義 (表格不存在時)
            columns_with_types = []
            for col in df.columns:
                dtype_str = str(df[col].dtype)
                duckdb_dtype = get_duckdb_dtype(dtype_str)
                columns_with_types.append(f'"{col}" {duckdb_dtype}')
                self.logger.debug(f"欄位 '{col}': {dtype_str} -> {duckdb_dtype}")

            columns_sql = ", ".join(columns_with_types)

            # 建立表格
            self.conn.sql(f'CREATE TABLE "{table_name}" ({columns_sql})')
            self.logger.debug(f"表格結構建立完成: {columns_sql}")

            # 插入資料
            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')

            self.logger.info(
                f"成功建立表格 '{table_name}'，插入 {len(df):,} 筆資料"
            )
            return True

        except DuckDBTableExistsError:
            raise
        except Exception as e:
            self.logger.error(f"建立表格 '{table_name}' 失敗: {e}")
            return False

    def insert_df_into_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        插入資料到現有表格

        Args:
            table_name: 表格名稱
            df: pandas DataFrame

        Returns:
            bool: 是否成功插入
        """
        try:
            if not self._table_exists(table_name):
                raise DuckDBTableNotFoundError(table_name)

            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')
            self.logger.info(f"成功插入 {len(df):,} 筆資料到 '{table_name}'")
            return True

        except DuckDBTableNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"插入資料到 '{table_name}' 失敗: {e}")
            return False

    def upsert_df_into_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        key_columns: List[str]
    ) -> bool:
        """
        更新或插入資料 (upsert)

        Args:
            table_name: 表格名稱
            df: 要插入的資料
            key_columns: 用於判斷重複的欄位

        Returns:
            bool: 是否成功
        """
        try:
            self.logger.info(
                f"開始 upsert 操作到 '{table_name}'，使用鍵: {key_columns}"
            )

            if not self._table_exists(table_name):
                raise DuckDBTableNotFoundError(table_name)

            # 建構 WHERE 條件
            key_conditions = []
            for key_col in key_columns:
                unique_values = df[key_col].unique()
                if len(unique_values) > 0:
                    # 安全轉義值
                    escaped_values = [
                        str(v).replace("'", "''") for v in unique_values
                    ]
                    values_str = "', '".join(escaped_values)
                    key_conditions.append(f'"{key_col}" IN (\'{values_str}\')')

            # 原子操作: DELETE + INSERT
            with self._atomic():
                if key_conditions:
                    where_clause = " AND ".join(key_conditions)
                    deleted_result = self.conn.sql(
                        f'SELECT COUNT(*) as count FROM "{table_name}" '
                        f'WHERE {where_clause}'
                    ).df()
                    deleted_count = (
                        deleted_result.iloc[0]['count']
                        if not deleted_result.empty else 0
                    )

                    self.conn.sql(
                        f'DELETE FROM "{table_name}" WHERE {where_clause}'
                    )
                    self.logger.info(f"刪除了 {deleted_count} 筆重複記錄")

                # 直接 INSERT (不透過 insert_df_into_table 以保持事務一致性)
                self.conn.sql(
                    f'INSERT INTO "{table_name}" SELECT * FROM df'
                )

            self.logger.info(
                f"Upsert 完成: 插入 {len(df):,} 筆資料到 '{table_name}'"
            )
            return True

        except DuckDBTableNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"Upsert 操作失敗: {e}")
            return False

    def query_to_df(self, query: str) -> Optional[pd.DataFrame]:
        """
        執行查詢並返回 DataFrame

        Args:
            query: SQL 查詢語句

        Returns:
            DataFrame 或 None (查詢失敗時)
        """
        try:
            if self.config.enable_query_logging:
                self.logger.debug(f"執行查詢: {query[:100]}...")
            result = self.conn.sql(query).df()
            self.logger.debug(f"查詢返回 {len(result)} 筆記錄")
            return result
        except Exception as e:
            self.logger.error(f"查詢失敗: {e}")
            return None

    def delete_data(self, query: str) -> bool:
        """
        執行 DELETE 語句

        Args:
            query: DELETE SQL 語句

        Returns:
            bool: 是否成功
        """
        try:
            if self.config.enable_query_logging:
                self.logger.debug(f"執行刪除: {query[:100]}...")
            self.conn.sql(query)
            self.logger.debug("成功刪除資料")
            return True
        except Exception as e:
            self.logger.error(f"刪除失敗: {e}")
            return False

    # ========== 便利方法 ==========

    def create_or_replace_table(
        self,
        table_name: str,
        df: pd.DataFrame
    ) -> bool:
        """
        建立或替換表格的便捷方法

        Args:
            table_name: 表格名稱
            df: pandas DataFrame

        Returns:
            bool: 是否成功
        """
        return self.create_table_from_df(table_name, df, if_exists='replace')

    def query_single_value(self, query: str) -> any:
        """
        執行查詢並返回單一值

        Args:
            query: SQL 查詢語句

        Returns:
            查詢結果的第一個值，或 None
        """
        result = self.query_to_df(query)
        if result is not None and not result.empty:
            return result.iloc[0, 0]
        return None

    def query_single_row(self, query: str) -> Optional[dict]:
        """
        執行查詢並返回單一行

        Args:
            query: SQL 查詢語句

        Returns:
            dict 或 None
        """
        result = self.query_to_df(query)
        if result is not None and not result.empty:
            return result.iloc[0].to_dict()
        return None

    def count_rows(self, table_name: str, where: str = None) -> int:
        """
        計算表格行數

        Args:
            table_name: 表格名稱
            where: 可選的 WHERE 條件

        Returns:
            int: 行數
        """
        query = f'SELECT COUNT(*) as count FROM "{table_name}"'
        if where:
            query += f" WHERE {where}"
        result = self.query_single_value(query)
        return int(result) if result is not None else 0
