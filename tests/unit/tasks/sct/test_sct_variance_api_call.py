"""
SCT 差異分析 - API 呼叫步驟單元測試
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.tasks.sct.steps.sct_variance_api_call import SCTVarianceAPICallStep
from accrual_bot.utils.api.dify_client import DifyAPIError


@pytest.fixture
def context():
    """建立包含預處理資料的 context"""
    ctx = ProcessingContext(
        data=pd.DataFrame(), entity_type='SCT',
        processing_date=202603, processing_type='VARIANCE'
    )
    ctx.data = pd.DataFrame({
        'item_description': ['Item A'],
        'po_line': ['PO001-1'],
        'amount': ['1000'],
    })
    ctx.set_auxiliary_data('previous_worksheet', pd.DataFrame({
        'item_description': ['Item A'],
        'po_line': ['PO001-1'],
        'amount': ['900'],
    }))
    return ctx


@pytest.fixture
def mock_api_response():
    """模擬成功的 API 回應"""
    return {
        'data': {
            'status': 'succeeded',
            'elapsed_time_ms': 5000,
            'outputs': {
                'result_df': '[{"po_line": "PO001-1", "diff": 100}]',
                'executive_summary': '本期新增 1 筆 PO',
                'top_5_insight': '1. PO001 金額增加 100',
            },
        }
    }


class TestSCTVarianceAPICallStep:
    """測試差異分析 API 呼叫步驟"""

    def test_instantiation(self):
        """基本實例化"""
        step = SCTVarianceAPICallStep()
        assert step.name == "SCTVarianceAPICall"

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.sct.steps.sct_variance_api_call.DifyClient')
    async def test_successful_api_call(self, MockClient, context, mock_api_response):
        """成功呼叫 API"""
        mock_instance = MagicMock()
        mock_instance.run_workflow = AsyncMock(return_value=mock_api_response)
        MockClient.return_value = mock_instance

        step = SCTVarianceAPICallStep()
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert context.get_variable('api_response') == mock_api_response
        assert result.metadata['api_status'] == 'succeeded'
        assert result.metadata['elapsed_time_ms'] == 5000

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.sct.steps.sct_variance_api_call.DifyClient')
    async def test_api_error_handled(self, MockClient, context):
        """API 錯誤處理"""
        mock_instance = MagicMock()
        mock_instance.run_workflow = AsyncMock(
            side_effect=DifyAPIError("API 失敗", status_code=500)
        )
        MockClient.return_value = mock_instance

        step = SCTVarianceAPICallStep()
        result = await step.execute(context)

        assert result.status == StepStatus.FAILED
        assert "API 呼叫失敗" in result.message
        assert result.metadata.get('status_code') == 500

    @pytest.mark.asyncio
    async def test_no_current_data_fails(self):
        """無當期資料時失敗"""
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202603, processing_type='VARIANCE'
        )

        step = SCTVarianceAPICallStep()
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert "當期或前期底稿未載入" in result.message

    @pytest.mark.asyncio
    @patch('accrual_bot.tasks.sct.steps.sct_variance_api_call.DifyClient')
    async def test_payload_uses_config_keys(self, MockClient, context, mock_api_response):
        """payload 使用 TOML 配置的 key 名稱"""
        mock_instance = MagicMock()
        mock_instance.run_workflow = AsyncMock(return_value=mock_api_response)
        MockClient.return_value = mock_instance

        step = SCTVarianceAPICallStep()
        result = await step.execute(context)

        # 驗證 run_workflow 被呼叫，inputs 包含正確 key
        call_args = mock_instance.run_workflow.call_args
        inputs = call_args[1].get('inputs') or call_args[0][1]

        # 預設 key 是 prev_wp 和 curr_wp（從 TOML 配置讀取）
        prev_key = step.config.get('api_request_prev_key', 'prev_wp')
        curr_key = step.config.get('api_request_curr_key', 'curr_wp')
        assert prev_key in inputs
        assert curr_key in inputs

    @pytest.mark.asyncio
    async def test_validate_input_pass(self, context):
        """validate_input 通過"""
        step = SCTVarianceAPICallStep()
        assert await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_validate_input_empty_data(self):
        """validate_input 失敗：空資料"""
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202603, processing_type='VARIANCE'
        )
        ctx.data = pd.DataFrame()

        step = SCTVarianceAPICallStep()
        assert not await step.validate_input(ctx)

    @pytest.mark.asyncio
    async def test_no_api_url_fails(self, context):
        """未設定 API URL 時失敗"""
        step = SCTVarianceAPICallStep()
        # 強制清除 api_url
        step.config = {}
        result = await step.execute(context)

        assert result.status == StepStatus.FAILED
        assert "未設定 API URL" in result.message
