"""
Dify Workflow API 客戶端

提供 Dify Workflow API 的呼叫封裝，包含：
- 多層級 API key 解析（環境變數 → workspace/.env → 相對路徑 fallback）
- 重試機制（exponential backoff）
- 超時控制
- 錯誤處理

credential 解析模式仿照 google_sheet_source.py 的 _resolve_credentials()。
"""

import asyncio
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from accrual_bot.utils.config.config_manager import resolve_flexible_path
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


class DifyAPIError(Exception):
    """Dify API 呼叫錯誤"""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class DifyClient:
    """
    Dify Workflow API 客戶端

    API key 解析順序（仿 _resolve_credentials 模式）：
    1. 明確傳入的 api_key 參數
    2. DIFY_API_KEY 環境變數
    3. ACCRUAL_BOT_WORKSPACE/secret/.env 檔案
    4. ./secret/.env fallback
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or self._resolve_api_key()
        if not self.api_key:
            raise DifyAPIError(
                "無法找到 Dify API key。請設定 DIFY_API_KEY 環境變數"
                "或將 .env 檔案放置於 workspace/secret/ 目錄"
            )

    @staticmethod
    def _resolve_api_key() -> Optional[str]:
        """
        多層級 API key 解析

        Returns:
            API key 字串，若找不到則回傳 None
        """
        # 1. 環境變數
        env_key = os.environ.get("DIFY_API_KEY")
        if env_key:
            logger.debug("使用環境變數 DIFY_API_KEY")
            return env_key

        # 2. 工作區 secret/.env
        workspace = os.environ.get("ACCRUAL_BOT_WORKSPACE")
        if workspace:
            ws_env = Path(workspace) / "secret" / ".env"
            if ws_env.exists():
                parsed = DifyClient._parse_env_file(str(ws_env))
                if "DIFY_API_KEY" in parsed:
                    logger.debug(f"從 {ws_env} 讀取 DIFY_API_KEY")
                    return parsed["DIFY_API_KEY"]

        # 3. 彈性路徑 fallback（仿 google_sheet_source 的 _resolve_credentials 模式）
        fallback = resolve_flexible_path("./secret/.env")
        if fallback and Path(fallback).exists():
            parsed = DifyClient._parse_env_file(str(fallback))
            if "DIFY_API_KEY" in parsed:
                logger.debug(f"從 {fallback} 讀取 DIFY_API_KEY")
                return parsed["DIFY_API_KEY"]

        return None

    @staticmethod
    def _parse_env_file(env_path: str) -> Dict[str, str]:
        """
        解析 .env 檔案（簡單 KEY=VALUE 格式）

        支援：
        - KEY=VALUE
        - KEY="VALUE"（去除引號）
        - # 註解行
        - 空行

        Args:
            env_path: .env 檔案路徑

        Returns:
            解析後的鍵值對字典
        """
        result: Dict[str, str] = {}
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    # 去除引號
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                        value = value[1:-1]
                    result[key] = value
        except Exception as e:
            logger.warning(f"無法解析 .env 檔案 {env_path}: {e}")
        return result

    async def run_workflow(
        self,
        url: str,
        inputs: Dict[str, Any],
        timeout: int = 300,
        max_retries: int = 2,
    ) -> Dict[str, Any]:
        """
        呼叫 Dify Workflow API

        使用 requests.post 透過 asyncio.to_thread 包裝為 async，
        支援 exponential backoff 重試。

        Args:
            url: API endpoint URL
            inputs: request payload（會作為 form data 傳送）
            timeout: 超時秒數（預設 300 秒，LLM workflow 可能較慢）
            max_retries: 最大重試次數（預設 2 次）

        Returns:
            API 回應的 JSON dict

        Raises:
            DifyAPIError: API 呼叫失敗時
        """
        headers = {"Authorization": f"Bearer {self.api_key}"}

        last_error: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                logger.info(
                    f"呼叫 Dify API (attempt {attempt + 1}/{max_retries + 1}): {url}"
                )

                response = await asyncio.to_thread(
                    self._do_request, url, headers, inputs, timeout
                )

                if response.status_code != 200:
                    raise DifyAPIError(
                        f"API 回傳非 200 狀態碼: {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text,
                    )

                result = response.json()
                logger.info("Dify API 呼叫成功")
                return result

            except DifyAPIError:
                raise
            except requests.exceptions.Timeout as e:
                last_error = e
                logger.warning(f"API 呼叫超時 (attempt {attempt + 1}): {e}")
            except requests.exceptions.ConnectionError as e:
                last_error = e
                logger.warning(f"API 連線錯誤 (attempt {attempt + 1}): {e}")
            except Exception as e:
                last_error = e
                logger.warning(f"API 呼叫失敗 (attempt {attempt + 1}): {e}")

            # Exponential backoff（最後一次不需等待）
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.info(f"等待 {wait_time} 秒後重試...")
                await asyncio.sleep(wait_time)

        raise DifyAPIError(
            f"API 呼叫失敗，已重試 {max_retries} 次: {last_error}",
        )

    @staticmethod
    def _do_request(
        url: str,
        headers: Dict[str, str],
        data: Dict[str, Any],
        timeout: int,
    ) -> requests.Response:
        """
        同步 HTTP POST 請求（供 asyncio.to_thread 呼叫）

        Args:
            url: API endpoint
            headers: HTTP headers
            data: form data
            timeout: 超時秒數

        Returns:
            requests.Response
        """
        return requests.post(
            url,
            headers=headers,
            data=data,
            timeout=timeout,
        )
