"""SCT 會計科目預測步驟單元測試

測試 SCTAccountPredictionStep：
- Product Code / Department 篩選（"0" = 不篩選）
- Item Description 關鍵字匹配
- 金額範圍（min_amount / max_amount）
- predicted_liability 輸出
- 先匹配先贏（first-match-wins）
- 無匹配保持 NA
- validate_input 驗證
- 空規則處理
- pipeline orchestrator 整合
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

def _make_rules(rules_list):
    """構建 mock config_toml 包含指定規則"""
    return {
        'sct_account_prediction': {'rules': rules_list},
    }


SAMPLE_RULES = [
    {
        'rule_id': 1,
        'account': '199999',
        'product_code': '0',
        'department': '0',
        'description_keywords': '路由器|Router|交換器|門市裝修工程',
        'min_amount': 30000,
        'condition_desc': 'FA資產，金額>=30,000',
    },
    {
        'rule_id': 2,
        'account': '520012',
        'product_code': 'RT_B2C_COM',
        'department': '0',
        'description_keywords': 'warehouse rental fee|停車場租賃',
        'liability_account': '200412',
        'condition_desc': 'Product Code=RT_B2C_COM，倉庫租金',
    },
    {
        'rule_id': 8,
        'account': '520019',
        'product_code': 'RT_B2C_COM',
        'department': '0',
        'description_keywords': 'PDA|refrigerator|工程',
        'min_amount': 3000,
        'max_amount': 30000,
        'liability_account': '200412',
        'condition_desc': 'Product Code=RT_B2C_COM，設備，金額>=3,000且<30,000',
    },
    {
        'rule_id': 15,
        'account': '600301',
        'product_code': 'RT_B2C_COM',
        'department': 'G03',
        'description_keywords': 'employee health check',
        'liability_account': '200412',
        'condition_desc': 'Product Code=RT_B2C_COM，Department=G03，員工健檢',
    },
    {
        'rule_id': 17,
        'account': '620008',
        'product_code': 'RT_B2C_COM',
        'department': 'G03',
        'description_keywords': 'Health Nurse|Health Doctor|仲介費|agency fee',
        'liability_account': '200406',
        'condition_desc': 'Product Code=RT_B2C_COM，Department=G03，人力仲介',
    },
]


@pytest.fixture
def mock_prediction_deps():
    """Mock SCTAccountPredictionStep 的外部依賴"""
    with patch(
        'accrual_bot.tasks.sct.steps.sct_account_prediction.config_manager'
    ) as mock_cm:
        mock_cm._config_toml = _make_rules(SAMPLE_RULES)
        yield mock_cm


def _create_test_df():
    """建立測試 DataFrame"""
    return pd.DataFrame({
        'Product Code': [
            'RT_B2C_COM', 'RT_B2C_COM', 'RT_B2C_COM',
            'RT_B2C_COM', 'OTHER_CODE', 'RT_B2C_COM',
        ],
        'Department': [
            'D01', 'D01', 'G03',
            'G03', 'D01', 'D01',
        ],
        'Item Description': [
            'warehouse rental fee - Jan',
            'PDA device purchase',
            'employee health check 2026',
            'Health Nurse service fee',
            '門市裝修工程 - 台北店',
            'random unmatched item',
        ],
        'Entry Amount': [
            '10000', '5000', '2000',
            '8000', '50000', '100',
        ],
    })


def _create_context(df: pd.DataFrame) -> ProcessingContext:
    """建立 ProcessingContext"""
    return ProcessingContext(
        data=df,
        entity_type='SCT',
        processing_date=202512,
        processing_type='PO',
    )


# ============================================================
# Tests
# ============================================================

class TestSCTAccountPredictionStep:
    """SCTAccountPredictionStep 測試"""

    @pytest.mark.asyncio
    async def test_rule_with_product_code_and_keywords(self, mock_prediction_deps):
        """規則 2: product_code=RT_B2C_COM + 倉庫租金關鍵字 → 520012"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[0]  # warehouse rental fee
        assert row['predicted_account'] == '520012'
        assert row['predicted_liability'] == '200412'

    @pytest.mark.asyncio
    async def test_rule_fa_asset_with_min_amount(self, mock_prediction_deps):
        """規則 1: 門市裝修工程 + 金額 >= 30,000 → 199999"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[4]  # 門市裝修工程, 50000, OTHER_CODE
        assert row['predicted_account'] == '199999'
        assert pd.isna(row['predicted_liability'])  # 規則 1 沒有 liability

    @pytest.mark.asyncio
    async def test_rule_fa_asset_below_min_amount(self, mock_prediction_deps):
        """規則 1: 金額 < 30,000 → 不匹配此規則"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['OTHER'],
            'Department': ['D01'],
            'Item Description': ['門市裝修工程'],
            'Entry Amount': ['29999'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert pd.isna(ctx.data.iloc[0]['predicted_account'])

    @pytest.mark.asyncio
    async def test_rule_with_amount_range(self, mock_prediction_deps):
        """規則 8: 金額 >= 3,000 且 < 30,000 → 520019"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[1]  # PDA device, 5000
        assert row['predicted_account'] == '520019'
        assert row['predicted_liability'] == '200412'

    @pytest.mark.asyncio
    async def test_rule_amount_range_below_min(self, mock_prediction_deps):
        """規則 8: 金額 < 3,000 → 不匹配"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['D01'],
            'Item Description': ['PDA device'],
            'Entry Amount': ['2999'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert pd.isna(ctx.data.iloc[0]['predicted_account'])

    @pytest.mark.asyncio
    async def test_rule_amount_range_above_max(self, mock_prediction_deps):
        """規則 8: 金額 >= 30,000 → 不匹配規則 8（但可能匹配規則 1）"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['D01'],
            'Item Description': ['PDA device'],
            'Entry Amount': ['30000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        # PDA 不在規則 1 的關鍵字裡，規則 8 金額超出 → 不匹配
        assert pd.isna(ctx.data.iloc[0]['predicted_account'])

    @pytest.mark.asyncio
    async def test_rule_with_product_code_and_dept(self, mock_prediction_deps):
        """規則 15: product_code=RT_B2C_COM + dept=G03 → 600301"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[2]  # employee health check, G03
        assert row['predicted_account'] == '600301'
        assert row['predicted_liability'] == '200412'

    @pytest.mark.asyncio
    async def test_liability_account_varies(self, mock_prediction_deps):
        """規則 17: 負債科目 200406（與其他規則的 200412 不同）"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[3]  # Health Nurse, G03
        assert row['predicted_account'] == '620008'
        assert row['predicted_liability'] == '200406'

    @pytest.mark.asyncio
    async def test_first_match_wins(self, mock_prediction_deps):
        """多條規則符合時，rule_id 最小的優先"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        # 建立同時符合規則 1 和規則 8 的資料
        # 門市裝修工程 + RT_B2C_COM + 金額 35000
        # 規則 1: keywords 匹配 + 金額 >= 30000 → 199999
        # 規則 8: keywords 包含 "工程" + 金額 3000~30000 → 不匹配(金額超出)
        # 所以規則 1 應該先匹配
        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['D01'],
            'Item Description': ['門市裝修工程'],
            'Entry Amount': ['35000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.data.iloc[0]['predicted_account'] == '199999'

    @pytest.mark.asyncio
    async def test_no_match_stays_na(self, mock_prediction_deps):
        """無匹配時 predicted_account = NA"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        row = ctx.data.iloc[5]  # random unmatched item
        assert pd.isna(row['predicted_account'])
        assert pd.isna(row['predicted_liability'])
        assert pd.isna(row['matched_conditions'])

    @pytest.mark.asyncio
    async def test_product_code_zero_means_any(self, mock_prediction_deps):
        """product_code="0" 不篩選 — 任何 Product Code 都能匹配"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        # 規則 1: product_code="0", 門市裝修工程, 金額 >= 30000
        df = pd.DataFrame({
            'Product Code': ['ANYTHING'],
            'Department': ['D99'],
            'Item Description': ['門市裝修工程'],
            'Entry Amount': ['50000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.data.iloc[0]['predicted_account'] == '199999'

    @pytest.mark.asyncio
    async def test_department_zero_means_any(self, mock_prediction_deps):
        """department="0" 不篩選 — 任何 Department 都能匹配"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['Z99_ANY'],
            'Item Description': ['warehouse rental fee'],
            'Entry Amount': ['10000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.data.iloc[0]['predicted_account'] == '520012'

    @pytest.mark.asyncio
    async def test_department_filter_excludes_non_match(self, mock_prediction_deps):
        """Department 不符合時不匹配"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        # 規則 15: dept=G03，但資料是 D01
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['D01'],
            'Item Description': ['employee health check'],
            'Entry Amount': ['2000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert pd.isna(ctx.data.iloc[0]['predicted_account'])

    @pytest.mark.asyncio
    async def test_product_code_filter_excludes_non_match(self, mock_prediction_deps):
        """Product Code 不符合時不匹配"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        # 規則 2: product_code=RT_B2C_COM，但資料是 OTHER
        df = pd.DataFrame({
            'Product Code': ['OTHER'],
            'Department': ['D01'],
            'Item Description': ['warehouse rental fee'],
            'Entry Amount': ['10000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert pd.isna(ctx.data.iloc[0]['predicted_account'])

    @pytest.mark.asyncio
    async def test_validate_input_missing_column(self, mock_prediction_deps):
        """缺少必要欄位 → False"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({'Product Code': ['A'], 'Department': ['B']})
        ctx = _create_context(df)
        result = await step.validate_input(ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_validate_input_success(self, mock_prediction_deps):
        """所有必要欄位存在 → True"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.validate_input(ctx)

        assert result is True

    @pytest.mark.asyncio
    async def test_validate_input_empty_df(self, mock_prediction_deps):
        """空 DataFrame → False"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame()
        ctx = _create_context(df)
        result = await step.validate_input(ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_empty_rules(self):
        """規則為空 → 全部 NA"""
        with patch(
            'accrual_bot.tasks.sct.steps.sct_account_prediction.config_manager'
        ) as mock_cm:
            mock_cm._config_toml = _make_rules([])
            from accrual_bot.tasks.sct.steps.sct_account_prediction import (
                SCTAccountPredictionStep,
            )

            step = SCTAccountPredictionStep()
            df = _create_test_df()
            ctx = _create_context(df)
            result = await step.execute(ctx)

            assert result.status == StepStatus.SUCCESS
            assert ctx.data['predicted_account'].isna().all()
            assert ctx.data['predicted_liability'].isna().all()

    @pytest.mark.asyncio
    async def test_statistics_in_metadata(self, mock_prediction_deps):
        """驗證 metadata 統計資訊"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert 'match_rate' in result.metadata
        assert 'account_distribution' in result.metadata
        # 6 筆中應有 5 筆匹配（第 6 筆 random unmatched）
        assert result.metadata['records_processed'] == 5
        assert result.metadata['records_skipped'] == 1

    @pytest.mark.asyncio
    async def test_rollback_removes_columns(self, mock_prediction_deps):
        """rollback 應移除新增的欄位"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        await step.execute(ctx)

        assert 'predicted_account' in ctx.data.columns
        assert 'predicted_liability' in ctx.data.columns

        await step.rollback(ctx, RuntimeError("test"))

        assert 'predicted_account' not in ctx.data.columns
        assert 'predicted_liability' not in ctx.data.columns
        assert 'matched_conditions' not in ctx.data.columns

    @pytest.mark.asyncio
    async def test_case_insensitive_keywords(self, mock_prediction_deps):
        """關鍵字匹配不分大小寫"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = pd.DataFrame({
            'Product Code': ['RT_B2C_COM'],
            'Department': ['D01'],
            'Item Description': ['WAREHOUSE RENTAL FEE'],
            'Entry Amount': ['10000'],
        })
        ctx = _create_context(df)
        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS
        assert ctx.data.iloc[0]['predicted_account'] == '520012'

    @pytest.mark.asyncio
    async def test_matched_conditions_populated(self, mock_prediction_deps):
        """matched_conditions 欄位應包含條件描述"""
        from accrual_bot.tasks.sct.steps.sct_account_prediction import (
            SCTAccountPredictionStep,
        )

        step = SCTAccountPredictionStep()
        df = _create_test_df()
        ctx = _create_context(df)
        await step.execute(ctx)

        row = ctx.data.iloc[0]  # warehouse rental fee
        assert '倉庫租金' in row['matched_conditions']


class TestSCTAccountPredictionOrchestrator:
    """Pipeline orchestrator 整合測試"""

    def test_pipeline_registration(self):
        """orchestrator 正確註冊 SCTAccountPrediction 步驟"""
        with patch(
            'accrual_bot.tasks.sct.pipeline_orchestrator.config_manager'
        ) as mock_cm:
            mock_cm._config_toml = {
                'pipeline': {
                    'sct': {
                        'enabled_po_steps': ['SCTAccountPrediction'],
                        'enabled_pr_steps': ['SCTAccountPrediction'],
                    }
                },
                'sct_account_prediction': {'rules': SAMPLE_RULES},
            }

            from accrual_bot.tasks.sct.pipeline_orchestrator import (
                SCTPipelineOrchestrator,
            )

            orchestrator = SCTPipelineOrchestrator()
            pipeline = orchestrator.build_po_pipeline(file_paths={})

            step_names = [s.name for s in pipeline.steps]
            assert 'SCTAccountPrediction' in step_names

    def test_pr_pipeline_registration(self):
        """PR pipeline 也正確註冊 SCTAccountPrediction 步驟"""
        with patch(
            'accrual_bot.tasks.sct.pipeline_orchestrator.config_manager'
        ) as mock_cm:
            mock_cm._config_toml = {
                'pipeline': {
                    'sct': {
                        'enabled_po_steps': [],
                        'enabled_pr_steps': ['SCTAccountPrediction'],
                    }
                },
                'sct_account_prediction': {'rules': SAMPLE_RULES},
            }

            from accrual_bot.tasks.sct.pipeline_orchestrator import (
                SCTPipelineOrchestrator,
            )

            orchestrator = SCTPipelineOrchestrator()
            pipeline = orchestrator.build_pr_pipeline(file_paths={})

            step_names = [s.name for s in pipeline.steps]
            assert 'SCTAccountPrediction' in step_names
