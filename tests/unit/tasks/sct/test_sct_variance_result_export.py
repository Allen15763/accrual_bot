"""
SCT 差異分析 - 結果解析與匯出步驟單元測試
"""

import json
import os
import pytest
import pandas as pd

from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.tasks.sct.steps.sct_variance_result_export import (
    SCTVarianceResultExportStep,
)


@pytest.fixture
def context():
    """建立包含 API 回應的 context"""
    ctx = ProcessingContext(
        data=pd.DataFrame(), entity_type='SCT',
        processing_date=202603, processing_type='VARIANCE'
    )
    ctx.set_variable('api_response', {
        'data': {
            'status': 'succeeded',
            'outputs': {
                'result_df': json.dumps([
                    {'po_line': 'PO001-1', 'amount_diff': 100, 'status': '新增'},
                    {'po_line': 'PO002-1', 'amount_diff': -200, 'status': '減少'},
                ]),
                'executive_summary': '本期新增 1 筆，減少 1 筆',
                'top_5_insight': '1. PO001 金額增加 100\n2. PO002 金額減少 200',
            }
        }
    })
    return ctx


class TestSCTVarianceResultExportStep:
    """測試差異分析結果解析與匯出步驟"""

    def test_instantiation(self):
        """基本實例化"""
        step = SCTVarianceResultExportStep()
        assert step.name == "SCTVarianceResultExport"

    # ===== _extract_by_path 測試 =====

    def test_extract_by_path_basic(self):
        """基本路徑提取"""
        data = {'data': {'outputs': {'result_df': 'value'}}}
        result = SCTVarianceResultExportStep._extract_by_path(
            data, 'data.outputs.result_df'
        )
        assert result == 'value'

    def test_extract_by_path_missing(self):
        """路徑不存在回傳 None"""
        data = {'data': {}}
        result = SCTVarianceResultExportStep._extract_by_path(
            data, 'data.outputs.missing'
        )
        assert result is None

    def test_extract_by_path_single_key(self):
        """單層路徑"""
        data = {'key': 'val'}
        result = SCTVarianceResultExportStep._extract_by_path(data, 'key')
        assert result == 'val'

    # ===== _parse_result_df 測試 =====

    def test_parse_result_df_json_string(self):
        """JSON 字串 → DataFrame"""
        step = SCTVarianceResultExportStep()
        raw = json.dumps([{'a': 1}, {'a': 2}])
        df = step._parse_result_df(raw)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_parse_result_df_list_of_dicts(self):
        """list of dict → DataFrame"""
        step = SCTVarianceResultExportStep()
        raw = [{'a': 1}, {'a': 2}]
        df = step._parse_result_df(raw)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_parse_result_df_already_dataframe(self):
        """已是 DataFrame → 直接回傳"""
        step = SCTVarianceResultExportStep()
        df_input = pd.DataFrame({'a': [1, 2]})
        df = step._parse_result_df(df_input)
        assert df is df_input

    def test_parse_result_df_none(self):
        """None → None"""
        step = SCTVarianceResultExportStep()
        assert step._parse_result_df(None) is None

    def test_parse_result_df_invalid_string(self):
        """無法解析的字串 → None"""
        step = SCTVarianceResultExportStep()
        assert step._parse_result_df("not json") is None

    # ===== execute 測試 =====

    @pytest.mark.asyncio
    async def test_execute_success(self, context, tmp_path):
        """成功解析 API 回應"""
        step = SCTVarianceResultExportStep()

        # 覆寫輸出目錄
        with pytest.MonkeyPatch.context() as m:
            m.setenv('ACCRUAL_BOT_WORKSPACE', str(tmp_path))

            result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert isinstance(context.data, pd.DataFrame)
        assert len(context.data) == 2
        assert context.get_variable('executive_summary') == '本期新增 1 筆，減少 1 筆'
        assert '1. PO001' in context.get_variable('top_5_insight')
        assert result.metadata['result_rows'] == 2
        assert result.metadata['has_summary'] is True
        assert result.metadata['has_insights'] is True

    @pytest.mark.asyncio
    async def test_execute_no_api_response(self):
        """無 API 回應時失敗"""
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202603, processing_type='VARIANCE'
        )

        step = SCTVarianceResultExportStep()
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert "無 API 回應" in result.message

    @pytest.mark.asyncio
    async def test_execute_invalid_result_df(self):
        """result_df 無法解析時失敗"""
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202603, processing_type='VARIANCE'
        )
        ctx.set_variable('api_response', {
            'data': {'outputs': {'result_df': 'not valid json at all {'}}
        })

        step = SCTVarianceResultExportStep()
        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert "無法解析" in result.message

    @pytest.mark.asyncio
    async def test_excel_export(self, context, tmp_path):
        """Excel 匯出驗證"""
        step = SCTVarianceResultExportStep()

        with pytest.MonkeyPatch.context() as m:
            m.setenv('ACCRUAL_BOT_WORKSPACE', str(tmp_path))
            result = await step.execute(context)

        export_path = context.get_variable('export_path')
        assert export_path is not None
        assert os.path.exists(export_path)
        assert export_path.endswith('.xlsx')

        # 驗證 Excel 內容
        with pd.ExcelFile(export_path) as xl:
            assert '差異明細' in xl.sheet_names
            assert '分析摘要' in xl.sheet_names

            detail_df = xl.parse('差異明細')
            assert len(detail_df) == 2

            summary_df = xl.parse('分析摘要')
            assert len(summary_df) == 2  # summary + insights

    @pytest.mark.asyncio
    async def test_validate_input_pass(self, context):
        """validate_input 通過"""
        step = SCTVarianceResultExportStep()
        assert await step.validate_input(context)

    @pytest.mark.asyncio
    async def test_validate_input_no_response(self):
        """validate_input 失敗"""
        ctx = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202603, processing_type='VARIANCE'
        )
        step = SCTVarianceResultExportStep()
        assert not await step.validate_input(ctx)
