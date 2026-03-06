"""
Google Sheets 數據導入器（向後兼容包裝層）

此模組已整合至 accrual_bot.core.datasources.google_sheet_source.GoogleSheetsSource，
本模組保留為薄包裝層以維持向後兼容性。

建議改用：
    from accrual_bot.core.datasources import GoogleSheetsSource
    from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType

    config = DataSourceConfig(
        source_type=DataSourceType.GOOGLE_SHEETS,
        connection_params={
            'credentials_path': '...',
            'spreadsheet_url': '...',   # 或 spreadsheet_key
            'default_sheet': 'Sheet1',
        }
    )
    source = GoogleSheetsSource(config)

    # 讀取（DataSource 介面）
    df = await source.read(sheet_name='Sheet1')

    # 多試算表並發讀取（原 GoogleSheetsImporter 風格）
    queries = [(spreadsheet_id, sheet_name, cell_range, header_row), ...]
    results = source.concurrent_get_data(queries)
"""

import warnings
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

from accrual_bot.core.datasources.google_sheet_source import GoogleSheetsSource
from accrual_bot.utils.logging import get_logger
from accrual_bot.utils.config.constants import GOOGLE_SHEETS


class GoogleSheetsImporter(GoogleSheetsSource):
    """
    [已廢棄] Google Sheets 數據導入器

    原 accrual_bot 的 GoogleSheetsImporter，現為 GoogleSheetsSource 的薄包裝層。

    向後兼容的初始化方式：
        credentials_config = {'certificate_path': '...', 'scopes': [...]}
        importer = GoogleSheetsImporter(credentials_config)

    原有 API 完全保留：
        importer.get_sheet_data(spreadsheet_id, sheet_name, cell_range, header_row, skip_first_row)
        importer.concurrent_get_data(queries)
        importer.get_multiple_sheets_data(queries)
        importer.get_spreadsheet_info(spreadsheet_id)
        importer.import_spx_closing_list()
    """

    def __init__(self, credentials_config: Dict[str, Any]):
        """
        Args:
            credentials_config: 憑證配置，格式：
                {'certificate_path': '路徑/credentials.json', 'scopes': [...]}
        """
        warnings.warn(
            "GoogleSheetsImporter 已廢棄，請改用 GoogleSheetsSource",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(credentials_config=credentials_config)

    def import_spx_closing_list(self) -> pd.DataFrame:
        """
        導入 SPX 關單清單（業務專屬方法）

        使用 stagging.toml 中 SPX_CONSTANTS 設定的 spreadsheet ID 與工作表清單
        並發讀取，合併後回傳。

        Returns:
            pd.DataFrame: 合併後的 SPX 關單清單
        """
        try:
            spreadsheet_id = GOOGLE_SHEETS.get('CLOSING_SHEET_ID', '')
            sheet_names = GOOGLE_SHEETS.get('CLOSING_SHEETS', [])
            cell_range = GOOGLE_SHEETS.get('CLOSING_RANGE', '')

            if not spreadsheet_id or not sheet_names:
                self.logger.warning("SPX 關單清單設定不完整，請確認 GOOGLE_SHEETS 常數")
                return pd.DataFrame()

            queries = [
                (spreadsheet_id, sheet_name, cell_range, True)
                for sheet_name in sheet_names
            ]
            dfs = self.concurrent_get_data(queries)
            valid_dfs = [df for df in dfs if not df.empty]

            if not valid_dfs:
                self.logger.warning("未取得任何 SPX 關單資料")
                return pd.DataFrame()

            combined_df = pd.concat(valid_dfs, ignore_index=True)

            if not combined_df.empty:
                combined_df = combined_df.dropna(subset=['Date'])
                column_mapping = {
                    'Date': 'date',
                    'Type': 'type',
                    'PO Number': 'po_no',
                    'Requester': 'requester',
                    'Supplier': 'supplier',
                    'Line Number / ALL': 'line_no',
                    'Reason': 'reason',
                    'New PR Number': 'new_pr_no',
                    'Remark': 'remark',
                    'Done(V)': 'done_by_fn',
                }
                combined_df = combined_df.rename(columns=column_mapping)
                combined_df = combined_df.query("date != ''").reset_index(drop=True)

            self.logger.info(f"成功導入 SPX 關單清單，共 {len(combined_df)} 筆")
            return combined_df

        except Exception as e:
            self.logger.error(f"導入 SPX 關單清單時出錯: {e}", exc_info=True)
            return pd.DataFrame()


class AsyncGoogleSheetsImporter(GoogleSheetsImporter):
    """[已廢棄] 請直接使用 GoogleSheetsSource"""

    def __init__(self, credentials_config: Dict[str, Any]):
        warnings.warn(
            "AsyncGoogleSheetsImporter 已廢棄，請改用 GoogleSheetsSource",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(credentials_config)
