"""
DifyClient 單元測試

測試 API key 解析、.env 解析、API 呼叫（mock）、重試機制、錯誤處理。
"""

import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from accrual_bot.utils.api.dify_client import DifyClient, DifyAPIError


# =============================================================================
# .env 檔案解析
# =============================================================================
class TestParseEnvFile:
    """測試 .env 檔案解析"""

    def test_parse_basic_key_value(self, tmp_path):
        """基本 KEY=VALUE 格式"""
        env_file = tmp_path / ".env"
        env_file.write_text("DIFY_API_KEY=test_key_123\n")
        result = DifyClient._parse_env_file(str(env_file))
        assert result == {"DIFY_API_KEY": "test_key_123"}

    def test_parse_quoted_value(self, tmp_path):
        """帶引號的值"""
        env_file = tmp_path / ".env"
        env_file.write_text('DIFY_API_KEY="quoted_key"\n')
        result = DifyClient._parse_env_file(str(env_file))
        assert result == {"DIFY_API_KEY": "quoted_key"}

    def test_parse_single_quoted_value(self, tmp_path):
        """帶單引號的值"""
        env_file = tmp_path / ".env"
        env_file.write_text("DIFY_API_KEY='single_quoted'\n")
        result = DifyClient._parse_env_file(str(env_file))
        assert result == {"DIFY_API_KEY": "single_quoted"}

    def test_parse_skips_comments_and_empty_lines(self, tmp_path):
        """跳過註解和空行"""
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\n\nDIFY_API_KEY=key1\n# another\nOTHER=val\n")
        result = DifyClient._parse_env_file(str(env_file))
        assert result == {"DIFY_API_KEY": "key1", "OTHER": "val"}

    def test_parse_nonexistent_file(self):
        """不存在的檔案回傳空 dict"""
        result = DifyClient._parse_env_file("/nonexistent/.env")
        assert result == {}

    def test_parse_multiple_entries(self, tmp_path):
        """多筆 key-value"""
        env_file = tmp_path / ".env"
        env_file.write_text("KEY1=val1\nKEY2=val2\nKEY3=val3\n")
        result = DifyClient._parse_env_file(str(env_file))
        assert len(result) == 3
        assert result["KEY1"] == "val1"

    def test_parse_line_without_equals(self, tmp_path):
        """沒有 = 的行會被忽略"""
        env_file = tmp_path / ".env"
        env_file.write_text("invalid_line\nDIFY_API_KEY=ok\n")
        result = DifyClient._parse_env_file(str(env_file))
        assert result == {"DIFY_API_KEY": "ok"}


# =============================================================================
# API key 解析
# =============================================================================
class TestResolveApiKey:
    """測試多層級 API key 解析"""

    @patch.dict(os.environ, {"DIFY_API_KEY": "env_key"}, clear=False)
    def test_resolve_from_env_var(self):
        """優先使用環境變數"""
        key = DifyClient._resolve_api_key()
        assert key == "env_key"

    @patch.dict(os.environ, {}, clear=True)
    def test_resolve_from_workspace_env(self, tmp_path):
        """從 ACCRUAL_BOT_WORKSPACE/secret/.env 讀取"""
        secret_dir = tmp_path / "secret"
        secret_dir.mkdir()
        env_file = secret_dir / ".env"
        env_file.write_text("DIFY_API_KEY=workspace_key\n")

        with patch.dict(os.environ, {"ACCRUAL_BOT_WORKSPACE": str(tmp_path)}):
            key = DifyClient._resolve_api_key()
            assert key == "workspace_key"

    @patch.dict(os.environ, {}, clear=True)
    @patch('accrual_bot.utils.api.dify_client.resolve_flexible_path', return_value=None)
    def test_resolve_returns_none_when_not_found(self, mock_resolve):
        """找不到時回傳 None"""
        key = DifyClient._resolve_api_key()
        assert key is None


# =============================================================================
# DifyClient 初始化
# =============================================================================
class TestDifyClientInit:
    """測試 DifyClient 初始化"""

    def test_init_with_explicit_key(self):
        """明確傳入 api_key"""
        client = DifyClient(api_key="explicit_key")
        assert client.api_key == "explicit_key"

    @patch.dict(os.environ, {}, clear=True)
    @patch('accrual_bot.utils.api.dify_client.resolve_flexible_path', return_value=None)
    def test_init_raises_without_key(self, mock_resolve):
        """無 API key 時拋出 DifyAPIError"""
        with pytest.raises(DifyAPIError, match="無法找到 Dify API key"):
            DifyClient()


# =============================================================================
# API 呼叫（mock requests.post）
# =============================================================================
class TestRunWorkflow:
    """測試 run_workflow API 呼叫"""

    @pytest.fixture
    def client(self):
        return DifyClient(api_key="test_key")

    @pytest.fixture
    def mock_response_success(self):
        """成功的 API 回應"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "status": "succeeded",
                "elapsed_time_ms": 5000,
                "outputs": {
                    "result_df": '[{"po_line": "123-1"}]',
                    "executive_summary": "摘要",
                    "top_5_insight": "洞察",
                },
            }
        }
        return mock_resp

    @pytest.mark.asyncio
    @patch("accrual_bot.utils.api.dify_client.requests.post")
    async def test_successful_call(self, mock_post, client, mock_response_success):
        """成功呼叫 API"""
        mock_post.return_value = mock_response_success

        result = await client.run_workflow(
            url="https://example.com/api",
            inputs={"key": "value"},
            timeout=60,
            max_retries=0,
        )

        assert result["data"]["status"] == "succeeded"
        mock_post.assert_called_once()

    @pytest.mark.asyncio
    @patch("accrual_bot.utils.api.dify_client.requests.post")
    async def test_passes_auth_header(self, mock_post, client, mock_response_success):
        """驗證 Authorization header"""
        mock_post.return_value = mock_response_success

        await client.run_workflow(
            url="https://example.com/api",
            inputs={},
            max_retries=0,
        )

        call_args = mock_post.call_args
        assert call_args[1]["headers"]["Authorization"] == "Bearer test_key"

    @pytest.mark.asyncio
    @patch("accrual_bot.utils.api.dify_client.requests.post")
    async def test_non_200_raises_error(self, mock_post, client):
        """非 200 狀態碼拋出 DifyAPIError"""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        with pytest.raises(DifyAPIError, match="非 200"):
            await client.run_workflow(
                url="https://example.com/api",
                inputs={},
                max_retries=0,
            )

    @pytest.mark.asyncio
    @patch("accrual_bot.utils.api.dify_client.requests.post")
    @patch("accrual_bot.utils.api.dify_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_retry_on_timeout(self, mock_sleep, mock_post, client):
        """超時後重試"""
        import requests as req

        mock_post.side_effect = [
            req.exceptions.Timeout("timeout"),
            MagicMock(status_code=200, json=lambda: {"data": {}}),
        ]

        result = await client.run_workflow(
            url="https://example.com/api",
            inputs={},
            max_retries=1,
        )

        assert mock_post.call_count == 2
        assert result == {"data": {}}

    @pytest.mark.asyncio
    @patch("accrual_bot.utils.api.dify_client.requests.post")
    @patch("accrual_bot.utils.api.dify_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_all_retries_exhausted(self, mock_sleep, mock_post, client):
        """所有重試用盡後拋出錯誤"""
        import requests as req

        mock_post.side_effect = req.exceptions.ConnectionError("refused")

        with pytest.raises(DifyAPIError, match="已重試 2 次"):
            await client.run_workflow(
                url="https://example.com/api",
                inputs={},
                max_retries=2,
            )

        assert mock_post.call_count == 3  # 1 initial + 2 retries


# =============================================================================
# DifyAPIError
# =============================================================================
class TestDifyAPIError:
    """測試 DifyAPIError"""

    def test_error_attributes(self):
        """錯誤物件屬性"""
        err = DifyAPIError("test", status_code=500, response_body="body")
        assert str(err) == "test"
        assert err.status_code == 500
        assert err.response_body == "body"

    def test_error_default_attributes(self):
        """預設屬性為 None"""
        err = DifyAPIError("msg")
        assert err.status_code is None
        assert err.response_body is None
