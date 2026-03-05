"""
Google Sheets 統一數據源

整合來源：
  - accrual_bot/data/importers/google_sheets_importer.py
    （GoogleSheetsImporter：spreadsheetId 存取、並發讀取、skip_first_row、ZIP fallback）
  - spe_bank_recon/src/core/datasources/google_sheet_source.py
    （GoogleSheetsManager：DataSource 架構、gspread 讀寫、工作表管理）

統一採用 gspread 作為底層函式庫（gspread 封裝 googleapiclient，支援 spreadsheetId 與 URL 兩種存取方式），
並保留兩個專案的所有功能。
"""

import asyncio
import concurrent.futures
import json
import warnings
import zipfile
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from accrual_bot.core.datasources.base import DataSource
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType
from accrual_bot.utils.logging import get_logger


_DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# Colab / 打包環境中的 ZIP 路徑候選（依序嘗試）
_ZIP_CANDIDATES = [
    r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\accrual_bot.zip',
    '/content/drive/Shareddrives/INT_TWN_SEA_FN_Shared_Resources/15_FBA/Allen/accrual_bot.zip',
]
_JSON_IN_ZIP = 'accrual_bot/secret/credentials.json'


class GoogleSheetsSource(DataSource):
    """
    Google Sheets 統一數據源

    整合 GoogleSheetsImporter（accrual_bot）與 GoogleSheetsManager（spe_bank_recon）
    的所有功能，符合 DataSource 架構。

    初始化方式（推薦）：使用 DataSourceConfig
        connection_params 需包含：
          - credentials_path: Service Account JSON 路徑
          - spreadsheet_url : Google Sheets 完整 URL（與 spreadsheet_key 二選一）
          - spreadsheet_key : Spreadsheet ID（與 spreadsheet_url 二選一）
          - default_sheet   : 預設工作表名稱（選填，預設 'Sheet1'）
          - scopes          : API 權限範圍列表（選填，有合理預設值）

    初始化方式（向後兼容 GoogleSheetsImporter 風格）：
        credentials_config = {'certificate_path': '...', 'scopes': [...]}

    主要功能：
        DataSource 介面（async）：
          read()  — 讀取預設或指定工作表
          write() — 寫入（支援覆寫 / 追加模式）
          get_metadata() — 元數據

        多試算表讀取（from GoogleSheetsImporter）：
          get_sheet_data()         — 依 spreadsheet_id 讀取，支援 skip_first_row
          get_multiple_sheets_data() — 依序批量讀取
          concurrent_get_data()    — ThreadPoolExecutor 並發讀取
          get_spreadsheet_info()   — 試算表基本資訊

        工作表管理（from GoogleSheetsManager）：
          recreate_and_write()  — 刪除舊表 + 建立新表 + 寫入
          get_all_worksheets()  — 列出所有工作表名稱
          create_worksheet()    — 建立新工作表
          delete_worksheet()    — 刪除指定工作表

        向後兼容（已廢棄，仍可用）：
          get_data()   — 請改用 read()
          write_data() — 請改用 write()
    """

    def __init__(
        self,
        config: Optional[DataSourceConfig] = None,
        credentials_config: Optional[Dict[str, Any]] = None,
        max_workers: int = 5,
        timeout: int = 60,
    ):
        """
        Args:
            config: DataSourceConfig（推薦）
            credentials_config: 向後兼容，格式 {'certificate_path': '...', 'scopes': [...]}
            max_workers: concurrent_get_data 的最大並發數
            timeout: concurrent_get_data 的逾時秒數
        """
        # 向後兼容：GoogleSheetsImporter 傳入 credentials_config 的初始化方式
        if config is None and credentials_config is not None:
            certificate_path = credentials_config.get('certificate_path')
            scopes = credentials_config.get('scopes', _DEFAULT_SCOPES)
            config = DataSourceConfig(
                source_type=DataSourceType.GOOGLE_SHEETS,
                connection_params={
                    'credentials_path': certificate_path,
                    'scopes': scopes,
                },
                cache_enabled=False,
            )
            warnings.warn(
                "使用 credentials_config 初始化已過時，建議改用 DataSourceConfig",
                DeprecationWarning,
                stacklevel=2,
            )

        if config is None:
            raise ValueError("必須提供 DataSourceConfig 或 credentials_config")

        super().__init__(config)

        params = config.connection_params
        self.credentials_path: Optional[str] = params.get('credentials_path')
        self.spreadsheet_url: Optional[str] = params.get('spreadsheet_url')
        self.spreadsheet_key: Optional[str] = params.get('spreadsheet_key')
        self.default_sheet: str = params.get('default_sheet', 'Sheet1')
        self.scopes: List[str] = params.get('scopes', _DEFAULT_SCOPES)
        self.max_workers = max_workers
        self.timeout = timeout

        # gspread 客戶端與預設試算表 handle
        self._gc: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None

        self._init_connection()
        self.logger.info("GoogleSheetsSource 初始化完成")

    # ────────────────────────────────────────────────
    # 連線初始化
    # ────────────────────────────────────────────────

    def _init_connection(self) -> None:
        """初始化 gspread 連線（支援 ZIP fallback）"""
        try:
            creds = self._load_credentials_from_file()
            self._gc = gspread.authorize(creds)
            self._open_default_spreadsheet()
        except FileNotFoundError:
            self.logger.warning("憑證檔案不存在，嘗試從 ZIP 載入")
            self._initialize_service_from_zip()
        except Exception as e:
            self.logger.error(f"初始化 Google Sheets 連線失敗: {e}")
            raise

    def _load_credentials_from_file(self) -> Credentials:
        """從檔案路徑載入 Service Account 憑證"""
        if not self.credentials_path:
            raise ValueError("未設定 credentials_path")
        return Credentials.from_service_account_file(
            self.credentials_path, scopes=self.scopes
        )

    def _open_default_spreadsheet(self) -> None:
        """若設定了 spreadsheet_url 或 spreadsheet_key，開啟預設試算表"""
        if not self._gc:
            return
        if self.spreadsheet_url:
            self._spreadsheet = self._gc.open_by_url(self.spreadsheet_url)
            self.logger.info(f"已連接試算表（URL）: {self._spreadsheet.title}")
        elif self.spreadsheet_key:
            self._spreadsheet = self._gc.open_by_key(self.spreadsheet_key)
            self.logger.info(f"已連接試算表（ID）: {self._spreadsheet.title}")

    def _initialize_service_from_zip(self) -> None:
        """從 ZIP 檔案載入憑證（Colab 或打包環境 fallback）"""
        for zip_path in _ZIP_CANDIDATES:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    if _JSON_IN_ZIP not in zf.namelist():
                        continue
                    service_account_info = json.loads(
                        zf.read(_JSON_IN_ZIP).decode('utf-8')
                    )
                    creds = Credentials.from_service_account_info(
                        service_account_info, scopes=self.scopes
                    )
                    self._gc = gspread.authorize(creds)
                    self._open_default_spreadsheet()
                    self.logger.info(f"從 ZIP 成功初始化 Google Sheets 連線: {zip_path}")
                    return
            except FileNotFoundError:
                continue
            except Exception as e:
                self.logger.error(f"從 ZIP 初始化失敗 ({zip_path}): {e}")
                continue

        self.logger.error("所有 ZIP 路徑均無法初始化，Google Sheets 連線不可用")

    # ────────────────────────────────────────────────
    # DataSource 標準介面（async）
    # ────────────────────────────────────────────────

    async def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取 Google Sheets 數據（DataSource 規範）

        Args:
            query: 工作表名稱（可選，預設使用 default_sheet）
            **kwargs:
                sheet_name: 工作表名稱（優先於 query）
                range_name: 儲存格範圍（如 'A1:D10'）
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_read, query, kwargs)

    def _sync_read(self, query: Optional[str], kwargs: dict) -> pd.DataFrame:
        """同步讀取（由 read() 在 executor 中呼叫）"""
        sheet_name = kwargs.get('sheet_name') or query or self.default_sheet
        range_name = kwargs.get('range_name')
        try:
            if not self._spreadsheet:
                raise ValueError("試算表未連接，請設定 spreadsheet_url 或 spreadsheet_key")
            worksheet = self._spreadsheet.worksheet(sheet_name)
            if range_name:
                data = worksheet.get(range_name)
                df = pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame()
            else:
                df = pd.DataFrame(worksheet.get_all_records())
            self.logger.info(f"從工作表 '{sheet_name}' 讀取 {len(df)} 行")
            return df
        except Exception as e:
            self.logger.error(f"讀取工作表 '{sheet_name}' 失敗: {e}")
            raise

    async def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入數據到 Google Sheets（DataSource 規範）

        Args:
            data: 要寫入的 DataFrame
            **kwargs:
                sheet_name: 工作表名稱（預設使用 default_sheet）
                is_append:  True 表示追加，False 表示覆寫（預設 False）
                clear_range: 覆寫前清除的範圍（None 表示清除整張工作表）
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_write, data, kwargs)

    def _sync_write(self, data: pd.DataFrame, kwargs: dict) -> bool:
        """同步寫入（由 write() 在 executor 中呼叫）"""
        sheet_name = kwargs.get('sheet_name', self.default_sheet)
        is_append = kwargs.get('is_append', False)
        clear_range = kwargs.get('clear_range')
        try:
            if not self._spreadsheet:
                raise ValueError("試算表未連接")
            sheet = self._spreadsheet.worksheet(sheet_name)
            safe_data = data.fillna('').astype(object)
            if is_append:
                sheet.append_rows(safe_data.values.tolist(), value_input_option='RAW')
                self.logger.info(f"追加 {len(data)} 行到工作表 '{sheet_name}'")
            else:
                if clear_range:
                    sheet.batch_clear([clear_range])
                else:
                    sheet.clear()
                sheet.update(
                    [safe_data.columns.values.tolist()] + safe_data.values.tolist()
                )
                self.logger.info(f"寫入 {len(data)} 行到工作表 '{sheet_name}'")
            return True
        except Exception as e:
            self.logger.error(f"寫入工作表 '{sheet_name}' 失敗: {e}")
            return False

    def get_metadata(self) -> Dict[str, Any]:
        """取得 Google Sheets 元數據（DataSource 規範）"""
        try:
            if not self._spreadsheet:
                return {'source_type': 'google_sheets', 'connected': False}
            worksheets = self._spreadsheet.worksheets()
            return {
                'source_type': 'google_sheets',
                'spreadsheet_url': self.spreadsheet_url,
                'spreadsheet_key': self.spreadsheet_key,
                'spreadsheet_title': self._spreadsheet.title,
                'credentials_path': self.credentials_path,
                'default_sheet': self.default_sheet,
                'available_sheets': [ws.title for ws in worksheets],
                'total_sheets': len(worksheets),
            }
        except Exception as e:
            self.logger.error(f"取得元數據失敗: {e}")
            return {'source_type': 'google_sheets', 'error': str(e)}

    # ────────────────────────────────────────────────
    # 多試算表讀取（from GoogleSheetsImporter）
    # ────────────────────────────────────────────────

    def get_sheet_data(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        cell_range: Optional[str] = None,
        header_row: bool = True,
        skip_first_row: bool = False,
    ) -> pd.DataFrame:
        """
        依 spreadsheet_id 讀取指定工作表

        向後兼容 GoogleSheetsImporter.get_sheet_data()，額外支援 skip_first_row。

        Args:
            spreadsheet_id: 試算表 ID
            sheet_name: 工作表名稱
            cell_range: 儲存格範圍（如 'A:J'，None 表示全部）
            header_row: 第一行（或 skip 後的第一行）是否為標題
            skip_first_row: 跳過第一行（第二行為標題，第三行起為資料）
        """
        try:
            if not self._gc:
                raise ValueError("Google Sheets 連線未初始化")
            spreadsheet = self._gc.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            values = worksheet.get(cell_range) if cell_range else worksheet.get_all_values()

            if not values:
                self.logger.warning(f"工作表 '{sheet_name}' 返回空資料")
                return pd.DataFrame()

            if skip_first_row:
                if header_row and len(values) > 2:
                    df = pd.DataFrame(values[2:], columns=values[1])
                elif header_row and len(values) > 1:
                    df = pd.DataFrame([], columns=values[1])
                else:
                    df = pd.DataFrame(values[1:])
            else:
                if header_row and len(values) > 1:
                    df = pd.DataFrame(values[1:], columns=values[0])
                elif header_row:
                    df = pd.DataFrame([], columns=values[0]) if values else pd.DataFrame()
                else:
                    df = pd.DataFrame(values)

            self.logger.info(
                f"讀取 {spreadsheet_id!r} / '{sheet_name}': {df.shape[0]} 行"
                f"（skip_first_row={skip_first_row}）"
            )
            return df

        except Exception as e:
            self.logger.error(f"讀取 '{sheet_name}' (id={spreadsheet_id}) 失敗: {e}")
            raise

    def get_multiple_sheets_data(
        self, queries: List[Tuple[str, str, str, bool]]
    ) -> List[pd.DataFrame]:
        """
        依序讀取多個工作表

        Args:
            queries: [(spreadsheet_id, sheet_name, cell_range, header_row), ...]

        Returns:
            List[pd.DataFrame]: 與 queries 對應的資料列表
        """
        results = []
        self.logger.info(f"開始依序讀取 {len(queries)} 個工作表")
        for spreadsheet_id, sheet_name, cell_range, header_row in queries:
            try:
                df = self.get_sheet_data(spreadsheet_id, sheet_name, cell_range, header_row)
            except Exception as e:
                self.logger.error(f"讀取 '{sheet_name}' 失敗: {e}")
                df = pd.DataFrame()
            results.append(df)
        success = sum(1 for df in results if not df.empty)
        self.logger.info(f"依序讀取完成，成功 {success}/{len(queries)}")
        return results

    def concurrent_get_data(
        self, queries: List[Tuple[str, str, str, bool]]
    ) -> List[pd.DataFrame]:
        """
        並發讀取多個工作表（ThreadPoolExecutor）

        Args:
            queries: [(spreadsheet_id, sheet_name, cell_range, header_row), ...]

        Returns:
            List[pd.DataFrame]: 與 queries 對應的資料列表（保持順序）
        """
        if not queries:
            return []
        self.logger.info(f"開始並發讀取 {len(queries)} 個工作表")
        results: List[Optional[pd.DataFrame]] = [None] * len(queries)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self._get_sheet_data_safe, *q): i
                for i, q in enumerate(queries)
            }
            for future in concurrent.futures.as_completed(
                future_to_index, timeout=self.timeout
            ):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result() or pd.DataFrame()
                except Exception as e:
                    self.logger.error(f"並發讀取失敗（索引 {idx}）: {e}")
                    results[idx] = pd.DataFrame()

        results = [df if df is not None else pd.DataFrame() for df in results]
        success = sum(1 for df in results if not df.empty)
        self.logger.info(f"並發讀取完成，成功 {success}/{len(queries)}")
        return results

    def _get_sheet_data_safe(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        cell_range: str,
        header_row: bool,
    ) -> Optional[pd.DataFrame]:
        """安全版 get_sheet_data，用於並發讀取內部呼叫"""
        try:
            return self.get_sheet_data(spreadsheet_id, sheet_name, cell_range, header_row)
        except Exception as e:
            self.logger.error(f"安全讀取 '{sheet_name}' 失敗: {e}")
            return None

    def get_spreadsheet_info(self, spreadsheet_id: str) -> Dict[str, Any]:
        """
        取得試算表基本資訊

        向後兼容 GoogleSheetsImporter.get_spreadsheet_info()。

        Args:
            spreadsheet_id: 試算表 ID

        Returns:
            Dict: {'title': ..., 'sheets': [{'title', 'sheet_id', 'row_count', 'column_count'}]}
        """
        try:
            if not self._gc:
                raise ValueError("Google Sheets 連線未初始化")
            spreadsheet = self._gc.open_by_key(spreadsheet_id)
            return {
                'title': spreadsheet.title,
                'sheets': [
                    {
                        'title': ws.title,
                        'sheet_id': ws.id,
                        'row_count': ws.row_count,
                        'column_count': ws.col_count,
                    }
                    for ws in spreadsheet.worksheets()
                ],
            }
        except Exception as e:
            self.logger.error(f"取得試算表資訊失敗 (id={spreadsheet_id}): {e}")
            return {}

    # ────────────────────────────────────────────────
    # 工作表管理（from GoogleSheetsManager）
    # ────────────────────────────────────────────────

    def recreate_and_write(
        self,
        df: pd.DataFrame,
        sheet_name_old: str,
        sheet_name_new: str,
        rows: Optional[int] = None,
        cols: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        刪除舊工作表並建立新工作表寫入資料

        Args:
            df: 要寫入的 DataFrame
            sheet_name_old: 要刪除的舊工作表名稱
            sheet_name_new: 要建立的新工作表名稱
            rows: 新工作表列數（None 表示自動）
            cols: 新工作表欄數（None 表示自動）
        """
        try:
            if not self._spreadsheet:
                raise ValueError("試算表未連接")
            old_ws = self._spreadsheet.worksheet(sheet_name_old)
            self._spreadsheet.del_worksheet(old_ws)
            self.logger.info(f"已刪除工作表: {sheet_name_old}")
            new_ws = self._spreadsheet.add_worksheet(
                title=sheet_name_new, rows=rows, cols=cols
            )
            self.logger.info(f"已建立工作表: {sheet_name_new}")
            safe_data = df.fillna('').astype(object)
            new_ws.update(
                [safe_data.columns.values.tolist()] + safe_data.values.tolist()
            )
            self.logger.info(f"已寫入 {len(df)} 行到工作表 '{sheet_name_new}'")
            return {'success': True}
        except Exception as e:
            self.logger.error(f"recreate_and_write 失敗: {e}")
            return {'success': False, 'error': str(e)}

    def get_all_worksheets(self) -> List[str]:
        """取得試算表中所有工作表名稱"""
        try:
            if not self._spreadsheet:
                return []
            names = [ws.title for ws in self._spreadsheet.worksheets()]
            self.logger.debug(f"工作表列表: {names}")
            return names
        except Exception as e:
            self.logger.error(f"取得工作表列表失敗: {e}")
            return []

    def create_worksheet(
        self, sheet_name: str, rows: int = 1000, cols: int = 26
    ) -> gspread.Worksheet:
        """
        建立新工作表

        Args:
            sheet_name: 新工作表名稱
            rows: 列數（預設 1000）
            cols: 欄數（預設 26）
        """
        try:
            if not self._spreadsheet:
                raise ValueError("試算表未連接")
            ws = self._spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
            self.logger.info(f"已建立工作表: {sheet_name} ({rows}x{cols})")
            return ws
        except Exception as e:
            self.logger.error(f"建立工作表 '{sheet_name}' 失敗: {e}")
            raise

    def delete_worksheet(self, sheet_name: str) -> None:
        """
        刪除指定工作表

        Args:
            sheet_name: 要刪除的工作表名稱
        """
        try:
            if not self._spreadsheet:
                raise ValueError("試算表未連接")
            ws = self._spreadsheet.worksheet(sheet_name)
            self._spreadsheet.del_worksheet(ws)
            self.logger.info(f"已刪除工作表: {sheet_name}")
        except Exception as e:
            self.logger.error(f"刪除工作表 '{sheet_name}' 失敗: {e}")
            raise

    # ────────────────────────────────────────────────
    # 向後兼容廢棄方法（from GoogleSheetsManager）
    # ────────────────────────────────────────────────

    def get_data(
        self, sheet_name: str = 'Sheet1', range_name: Optional[str] = None
    ) -> pd.DataFrame:
        """[已廢棄] 請改用 read(sheet_name=..., range_name=...)"""
        warnings.warn(
            "get_data() 已廢棄，請改用 read(sheet_name=..., range_name=...)",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._sync_read(sheet_name, {'range_name': range_name})

    def write_data(
        self,
        df: pd.DataFrame,
        sheet_name: str = 'Sheet1',
        is_append: bool = False,
        clear_range: Optional[str] = None,
    ) -> Dict[str, Any]:
        """[已廢棄] 請改用 write(data, sheet_name=..., is_append=...)"""
        warnings.warn(
            "write_data() 已廢棄，請改用 write(data, sheet_name=..., is_append=...)",
            DeprecationWarning,
            stacklevel=2,
        )
        success = self._sync_write(
            df, {'sheet_name': sheet_name, 'is_append': is_append, 'clear_range': clear_range}
        )
        return {'success': success}


# ────────────────────────────────────────────────
# 向後兼容別名
# ────────────────────────────────────────────────

class GoogleSheetsManager(GoogleSheetsSource):
    """
    [向後兼容] 請改用 GoogleSheetsSource

    保留原 spe_bank_recon GoogleSheetsManager 的初始化簽名：
        GoogleSheetsManager(config=...)
        GoogleSheetsManager(credentials_path=..., spreadsheet_url=...)
    """

    def __init__(
        self,
        config: Optional[DataSourceConfig] = None,
        credentials_path: Optional[str] = None,
        spreadsheet_url: Optional[str] = None,
        **kwargs,
    ):
        if config is None and credentials_path and spreadsheet_url:
            config = DataSourceConfig(
                source_type=DataSourceType.GOOGLE_SHEETS,
                connection_params={
                    'credentials_path': credentials_path,
                    'spreadsheet_url': spreadsheet_url,
                    'default_sheet': 'Sheet1',
                },
                cache_enabled=False,
            )
            warnings.warn(
                "GoogleSheetsManager 已廢棄，請改用 GoogleSheetsSource",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(config, **kwargs)
