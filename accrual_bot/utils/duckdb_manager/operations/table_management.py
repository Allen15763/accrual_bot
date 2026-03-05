"""
表格管理操作 Mixin

提供表格結構管理相關操作。
"""

import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime

from .base import OperationMixin


class TableManagementMixin(OperationMixin):
    """
    表格管理操作 Mixin

    提供表格結構管理操作:
    - show_tables: 顯示所有表格
    - describe_table: 描述表格結構
    - get_table_info: 獲取表格詳細資訊
    - drop_table: 刪除表格
    - truncate_table: 清空表格
    - backup_table: 備份表格
    """

    def show_tables(self) -> Optional[pd.DataFrame]:
        """
        顯示所有表格

        Returns:
            pd.DataFrame: 表格列表
        """
        self.logger.debug("獲取所有表格列表")
        return self.conn.sql("SHOW TABLES").df()

    def describe_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        描述表格結構

        Args:
            table_name: 表格名稱

        Returns:
            pd.DataFrame: 表格結構描述
        """
        self.logger.debug(f"獲取表格 '{table_name}' 的結構")
        try:
            return self.conn.sql(f'DESCRIBE "{table_name}"').df()
        except Exception as e:
            self.logger.error(f"描述表格失敗: {e}")
            return None

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        獲取表格詳細資訊

        Args:
            table_name: 表格名稱

        Returns:
            dict: 包含 table_name, row_count, columns, schema 的字典
        """
        try:
            self.logger.debug(f"獲取表格 '{table_name}' 的詳細資訊")

            row_count = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']
            schema = self.describe_table(table_name)

            info = {
                'table_name': table_name,
                'row_count': row_count,
                'columns': (
                    schema['column_name'].tolist()
                    if schema is not None else []
                ),
                'schema': schema
            }

            self.logger.info(
                f"表格 '{table_name}' 包含 {row_count:,} 筆記錄，"
                f"{len(info['columns'])} 個欄位"
            )
            return info

        except Exception as e:
            self.logger.error(f"獲取表格 '{table_name}' 資訊失敗: {e}")
            return {}

    def drop_table(
        self,
        table_name: str,
        if_exists: bool = True,
        confirm: bool = True
    ) -> bool:
        """
        刪除表格

        Args:
            table_name: 表格名稱
            if_exists: 如果為 True，表格不存在時不報錯
            confirm: 是否記錄確認日誌

        Returns:
            bool: 是否成功
        """
        try:
            table_exists = self._table_exists(table_name)

            if not table_exists and not if_exists:
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False
            elif not table_exists and if_exists:
                self.logger.warning(f"表格 '{table_name}' 不存在，無需刪除")
                return True

            # 獲取表格資訊用於日誌
            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            if confirm:
                self.logger.warning(
                    f"即將刪除表格 '{table_name}' (包含 {row_count:,} 筆資料)"
                )

            # 執行刪除
            drop_sql = (
                f'DROP TABLE {"IF EXISTS " if if_exists else ""}"{table_name}"'
            )
            self.conn.sql(drop_sql)

            self.logger.info(
                f"成功刪除表格 '{table_name}' (原有 {row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"刪除表格 '{table_name}' 失敗: {e}")
            return False

    def truncate_table(self, table_name: str) -> bool:
        """
        清空表格資料但保留結構

        Args:
            table_name: 表格名稱

        Returns:
            bool: 是否成功
        """
        try:
            if not self._table_exists(table_name):
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False

            row_count = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']

            self.conn.sql(f'DELETE FROM "{table_name}"')

            self.logger.info(
                f"成功清空表格 '{table_name}' (刪除了 {row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"清空表格 '{table_name}' 失敗: {e}")
            return False

    def backup_table(
        self,
        table_name: str,
        backup_format: str = 'parquet',
        backup_path: str = None
    ) -> bool:
        """
        備份表格資料

        Args:
            table_name: 表格名稱
            backup_format: 備份格式 ('parquet', 'csv', 'json')
            backup_path: 備份檔案路徑

        Returns:
            bool: 是否成功
        """
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{table_name}_backup_{timestamp}.{backup_format}"

            if not self._table_exists(table_name):
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False

            # 安全轉義路徑
            safe_path = backup_path.replace("'", "''")

            # 執行備份
            if backup_format.lower() == 'parquet':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{safe_path}' (FORMAT PARQUET)"
                )
            elif backup_format.lower() == 'csv':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{safe_path}' (FORMAT CSV, HEADER)"
                )
            elif backup_format.lower() == 'json':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{safe_path}' (FORMAT JSON)"
                )
            else:
                raise ValueError(f"不支援的備份格式: {backup_format}")

            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            self.logger.info(
                f"成功備份表格 '{table_name}' 到 '{backup_path}' "
                f"({row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"備份表格 '{table_name}' 失敗: {e}")
            return False

    # ========== 便利方法 ==========

    def table_exists(self, table_name: str) -> bool:
        """
        公開的表格存在檢查方法

        Args:
            table_name: 表格名稱

        Returns:
            bool: 表格是否存在
        """
        return self._table_exists(table_name)

    def list_tables_with_info(self) -> pd.DataFrame:
        """
        列出所有表格及其基本資訊

        Returns:
            pd.DataFrame: 包含 name, row_count, column_count 的 DataFrame
        """
        tables_df = self.show_tables()
        if tables_df is None or tables_df.empty:
            return pd.DataFrame(columns=['name', 'row_count', 'column_count'])

        info_list = []
        for table_name in tables_df['name']:
            info = self.get_table_info(table_name)
            info_list.append({
                'name': table_name,
                'row_count': info.get('row_count', 0),
                'column_count': len(info.get('columns', []))
            })

        return pd.DataFrame(info_list)

    def get_table_ddl(self, table_name: str) -> Optional[str]:
        """
        取得表格的 CREATE TABLE DDL 語句

        Args:
            table_name: 表格名稱

        Returns:
            str: DDL 語句或 None
        """
        try:
            schema = self.describe_table(table_name)
            if schema is None:
                return None

            columns_sql = ", ".join(
                f'"{row["column_name"]}" {row["column_type"]}'
                for _, row in schema.iterrows()
            )
            return f'CREATE TABLE "{table_name}" ({columns_sql})'

        except Exception as e:
            self.logger.error(f"生成 DDL 失敗: {e}")
            return None

    def clone_table_schema(
        self,
        source_table: str,
        target_table: str
    ) -> bool:
        """
        複製表格結構（不含資料）

        Args:
            source_table: 來源表格名稱
            target_table: 目標表格名稱

        Returns:
            bool: 是否成功
        """
        try:
            if not self._table_exists(source_table):
                self.logger.error(f"來源表格 '{source_table}' 不存在")
                return False

            if self._table_exists(target_table):
                self.logger.error(f"目標表格 '{target_table}' 已存在")
                return False

            self.conn.sql(
                f'CREATE TABLE "{target_table}" AS '
                f'SELECT * FROM "{source_table}" WHERE 1=0'
            )

            self.logger.info(
                f"成功複製表格結構: '{source_table}' -> '{target_table}'"
            )
            return True

        except Exception as e:
            self.logger.error(f"複製表格結構失敗: {e}")
            return False
