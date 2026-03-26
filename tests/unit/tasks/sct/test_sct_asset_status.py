"""SCT PPE 資產狀態更新步驟單元測試

測試 SCTAssetStatusUpdateStep：
- PPE PO 辨識（FA 科目 / 關鍵字）
- 驗收品項 ERM 判定邏輯
- 保固品項排除
- 無驗收品項 fallback 到採購備註
- 保護狀態不覆蓋
- 估列標記更新
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from accrual_bot.core.pipeline.base import StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_asset_deps():
    """Mock SCTAssetStatusUpdateStep 的外部依賴"""
    with patch('accrual_bot.tasks.sct.steps.sct_asset_status.config_manager') as mock_cm:
        config_toml = {
            'sct': {
                'ppe': {
                    'trigger_keywords': ['訂金', '安裝款', '驗收款', '保固'],
                    'acceptance_keyword': '驗收',
                    'warranty_keyword': '保固',
                    'completed_status': '已完成(PPE)',
                    'incomplete_status': '未完成(PPE)',
                    'protected_statuses': [
                        '已入帳', '上期已入PPE',
                        '上期FN備註已完成或Voucher number',
                        'Outright', 'Consignment', 'Outsourcing',
                    ],
                }
            },
            'fa_accounts': {'sct': ['199999']},
        }
        mock_cm._config_toml = config_toml
        mock_cm.get_list.return_value = ['199999']
        yield mock_cm


def _create_ppe_po_df():
    """建立含 PPE 品項的 PO 測試 DataFrame

    PO-001: 訂金 + 安裝款 + 驗收款 + 保固（典型 PPE PO）
    PO-002: 一般品項（非 PPE）
    PO-003: FA 科目品項（PPE by GL#）
    """
    return pd.DataFrame({
        'PO#': [
            'PO-001', 'PO-001', 'PO-001', 'PO-001',  # PPE PO (4 items)
            'PO-002', 'PO-002',                         # 一般 PO
            'PO-003',                                    # FA PO
        ],
        'GL#': [
            '100001', '100002', '100003', '100004',
            '100005', '100006',
            '199999',  # FA 科目
        ],
        'Item Description': [
            '設備訂金', '安裝款-Phase1', '驗收款-Final', '保固服務-2Y',
            '一般辦公用品', '文具',
            'Server Equipment',
        ],
        'Expected Received Month_轉換格式': [
            202501, 202506, 202509, 202612,  # 驗收=202509, 保固=202612
            202512, 202512,
            202510,
        ],
        'PO狀態': [
            'error(Description Period is out of ERM)', '未完成', '未完成', '未完成',
            '已完成(not_billed)', '已完成(not_billed)',
            '未完成',
        ],
        'Remarked by Procurement': [
            pd.NA, pd.NA, pd.NA, pd.NA,
            pd.NA, pd.NA,
            pd.NA,
        ],
        '是否估計入帳': ['N', 'N', 'N', 'N', 'Y', 'Y', 'N'],
        'matched_condition_on_status': [pd.NA] * 7,
    })


# ============================================================
# TestSCTAssetStatusUpdateStep
# ============================================================

class TestSCTAssetStatusUpdateStep:
    """SCTAssetStatusUpdateStep 測試"""

    def test_instantiation(self, mock_asset_deps):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        assert step.name == "SCTAssetStatusUpdate"
        assert step.acceptance_keyword == '驗收'
        assert step.warranty_keyword == '保固'
        assert step.completed_status == '已完成(PPE)'
        assert step.incomplete_status == '未完成(PPE)'
        assert '199999' in step.fa_accounts

    def test_identify_ppe_pos_by_keyword(self, mock_asset_deps):
        """關鍵字觸發 PPE PO 辨識"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        ppe_pos = step._identify_ppe_pos(df)
        assert 'PO-001' in ppe_pos  # 含 訂金/安裝款/驗收款/保固
        assert 'PO-002' not in ppe_pos  # 一般品項
        assert 'PO-003' in ppe_pos  # FA 科目

    def test_identify_ppe_pos_by_fa_account(self, mock_asset_deps):
        """FA 科目觸發 PPE PO 辨識"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-FA'],
            'GL#': ['199999'],
            'Item Description': ['Generic Item'],
            'Expected Received Month_轉換格式': [202512],
            'PO狀態': [pd.NA],
        })

        ppe_pos = step._identify_ppe_pos(df)
        assert 'PO-FA' in ppe_pos

    def test_find_acceptance_erm_basic(self, mock_asset_deps):
        """找到驗收品項的 ERM"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        po_mask = df['PO#'] == 'PO-001'
        erm = step._find_acceptance_erm(df, po_mask)
        assert erm == 202509  # 驗收款的 ERM

    def test_find_acceptance_erm_excludes_warranty(self, mock_asset_deps):
        """保固品項排除於驗收判定"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-X', 'PO-X'],
            'Item Description': ['保固驗收款', '安裝款'],  # 「保固驗收款」應被排除
            'Expected Received Month_轉換格式': [202612, 202506],
        })

        po_mask = df['PO#'] == 'PO-X'
        erm = step._find_acceptance_erm(df, po_mask)
        # 「保固驗收款」含保固 → 排除；「安裝款」不含驗收 → 不是驗收品項
        assert erm is None

    def test_find_acceptance_erm_none(self, mock_asset_deps):
        """無驗收品項時回傳 None"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-Y', 'PO-Y'],
            'Item Description': ['訂金', '保固服務'],
            'Expected Received Month_轉換格式': [202501, 202612],
        })

        po_mask = df['PO#'] == 'PO-Y'
        erm = step._find_acceptance_erm(df, po_mask)
        assert erm is None

    def test_find_acceptance_erm_nan_values(self, mock_asset_deps):
        """驗收品項 ERM 為 NaN 時回傳 None"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-Z'],
            'Item Description': ['驗收款'],
            'Expected Received Month_轉換格式': [pd.NA],
        })

        po_mask = df['PO#'] == 'PO-Z'
        erm = step._find_acceptance_erm(df, po_mask)
        assert erm is None

    @pytest.mark.asyncio
    async def test_execute_acceptance_completed(self, mock_asset_deps):
        """驗收 ERM ≤ processing_date → 已完成(PPE)"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        assert result.status == StepStatus.SUCCESS

        updated_df = context.data
        # PO-001 所有品項應為 已完成(PPE)（驗收 ERM 202509 ≤ 202512）
        po001 = updated_df[updated_df['PO#'] == 'PO-001']
        assert (po001['PO狀態'] == '已完成(PPE)').all()
        assert (po001['是否估計入帳'] == 'Y').all()

    @pytest.mark.asyncio
    async def test_execute_acceptance_incomplete(self, mock_asset_deps):
        """驗收 ERM > processing_date → 未完成(PPE)"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202506, processing_type='PO',
        )

        result = await step.execute(context)
        assert result.status == StepStatus.SUCCESS

        updated_df = context.data
        po001 = updated_df[updated_df['PO#'] == 'PO-001']
        assert (po001['PO狀態'] == '未完成(PPE)').all()
        assert (po001['是否估計入帳'] == 'N').all()

    @pytest.mark.asyncio
    async def test_execute_no_ppe_pos(self, mock_asset_deps):
        """無 PPE PO 時跳過"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-A'],
            'GL#': ['100001'],
            'Item Description': ['一般辦公用品'],
            'Expected Received Month_轉換格式': [202512],
            'PO狀態': ['未完成'],
            'Remarked by Procurement': [pd.NA],
            '是否估計入帳': ['N'],
            'matched_condition_on_status': [pd.NA],
        })

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        assert result.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_execute_protected_statuses(self, mock_asset_deps):
        """保護狀態不被覆蓋"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-P', 'PO-P'],
            'GL#': ['199999', '199999'],
            'Item Description': ['驗收款', '訂金'],
            'Expected Received Month_轉換格式': [202509, 202501],
            'PO狀態': ['已入帳', '未完成'],
            'Remarked by Procurement': [pd.NA, pd.NA],
            '是否估計入帳': ['N', 'N'],
            'matched_condition_on_status': [pd.NA, pd.NA],
        })

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        updated_df = context.data

        # 「已入帳」不被覆蓋
        assert updated_df.iloc[0]['PO狀態'] == '已入帳'
        # 「未完成」被更新為「已完成(PPE)」
        assert updated_df.iloc[1]['PO狀態'] == '已完成(PPE)'

    @pytest.mark.asyncio
    async def test_execute_fallback_procurement_remark(self, mock_asset_deps):
        """無驗收品項 + 採購備註已完成 → fallback"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-F', 'PO-F'],
            'GL#': ['199999', '199999'],
            'Item Description': ['訂金', '保固服務'],  # 無驗收品項
            'Expected Received Month_轉換格式': [202501, 202612],
            'PO狀態': ['未完成', '未完成'],
            'Remarked by Procurement': ['已完成', '已完成'],
            '是否估計入帳': ['N', 'N'],
            'matched_condition_on_status': [pd.NA, pd.NA],
        })

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        updated_df = context.data

        assert (updated_df['PO狀態'] == '已完成(PPE)').all()
        assert (updated_df['是否估計入帳'] == 'Y').all()

    @pytest.mark.asyncio
    async def test_execute_fallback_no_remark_keeps_original(self, mock_asset_deps):
        """無驗收品項 + 無採購備註 → 維持原狀態"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-N', 'PO-N'],
            'GL#': ['199999', '199999'],
            'Item Description': ['訂金', '保固服務'],
            'Expected Received Month_轉換格式': [202501, 202612],
            'PO狀態': ['未完成', '未完成'],
            'Remarked by Procurement': [pd.NA, pd.NA],
            '是否估計入帳': ['N', 'N'],
            'matched_condition_on_status': [pd.NA, pd.NA],
        })

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        updated_df = context.data

        # 無變更
        assert (updated_df['PO狀態'] == '未完成').all()

    @pytest.mark.asyncio
    async def test_execute_non_ppe_po_untouched(self, mock_asset_deps):
        """非 PPE PO 不受影響"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        updated_df = context.data

        # PO-002 不是 PPE PO，應維持原狀態
        po002 = updated_df[updated_df['PO#'] == 'PO-002']
        assert (po002['PO狀態'] == '已完成(not_billed)').all()

    @pytest.mark.asyncio
    async def test_execute_multiple_acceptance_items(self, mock_asset_deps):
        """多個驗收品項取 max ERM"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-M', 'PO-M', 'PO-M'],
            'GL#': ['199999', '199999', '199999'],
            'Item Description': ['驗收款-Phase1', '驗收款-Phase2', '訂金'],
            'Expected Received Month_轉換格式': [202506, 202509, 202501],
            'PO狀態': ['未完成', '未完成', '未完成'],
            'Remarked by Procurement': [pd.NA, pd.NA, pd.NA],
            '是否估計入帳': ['N', 'N', 'N'],
            'matched_condition_on_status': [pd.NA, pd.NA, pd.NA],
        })

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.execute(context)
        updated_df = context.data

        # max(202506, 202509) = 202509 ≤ 202512 → 已完成(PPE)
        assert (updated_df['PO狀態'] == '已完成(PPE)').all()

    @pytest.mark.asyncio
    async def test_validate_input_pass(self, mock_asset_deps):
        """驗證通過"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()
        df = _create_ppe_po_df()

        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.validate_input(context)
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_input_empty(self, mock_asset_deps):
        """空 DataFrame 驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        context = ProcessingContext(
            data=pd.DataFrame(), entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.validate_input(context)
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_input_missing_columns(self, mock_asset_deps):
        """缺少必要欄位驗證失敗"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({'PO#': ['PO-1'], 'GL#': ['100001']})
        context = ProcessingContext(
            data=df, entity_type='SCT',
            processing_date=202512, processing_type='PO',
        )

        result = await step.validate_input(context)
        assert result is False

    def test_matched_condition_traceability(self, mock_asset_deps):
        """matched_condition_on_status 追溯記錄"""
        from accrual_bot.tasks.sct.steps.sct_asset_status import SCTAssetStatusUpdateStep
        step = SCTAssetStatusUpdateStep()

        df = pd.DataFrame({
            'PO#': ['PO-T', 'PO-T'],
            'GL#': ['199999', '199999'],
            'Item Description': ['驗收款', '訂金'],
            'Expected Received Month_轉換格式': [202509, 202501],
            'PO狀態': ['未完成', '未完成'],
            'Remarked by Procurement': [pd.NA, pd.NA],
            '是否估計入帳': ['N', 'N'],
            'matched_condition_on_status': [pd.NA, pd.NA],
        })

        result = step._process_ppe_pos(df, ['PO-T'], 202512)

        assert result['updated_count'] == 2
        assert result['completed_pos'] == 1
        # 檢查追溯欄位已填寫
        assert df['matched_condition_on_status'].notna().all()
        assert '驗收款 ERM' in df['matched_condition_on_status'].iloc[0]


# ============================================================
# TestSCTOrchestratorAssetStep
# ============================================================

class TestSCTOrchestratorAssetStep:
    """Orchestrator 中 SCTAssetStatusUpdate 的註冊測試"""

    def test_po_steps_include_asset_status(self, mock_asset_deps):
        """PO pipeline 包含 SCTAssetStatusUpdate"""
        with patch('accrual_bot.tasks.sct.pipeline_orchestrator.config_manager') as mock_cm:
            mock_cm._config_toml = {
                'pipeline': {
                    'sct': {
                        'enabled_po_steps': [
                            'SCTDataLoading', 'SCTERMLogic',
                            'SCTAssetStatusUpdate',
                        ]
                    }
                },
                'sct_erm_status_rules': {'conditions': []},
                'sct_column_defaults': {},
                'fa_accounts': {'sct': ['199999']},
                'sct': {'ppe': {}},
            }
            mock_cm.get_list.return_value = ['199999']

            from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
            orch = SCTPipelineOrchestrator()
            steps = orch.get_enabled_steps('PO')
            assert 'SCTAssetStatusUpdate' in steps

    def test_pr_steps_no_asset_status(self, mock_asset_deps):
        """PR pipeline 不包含 SCTAssetStatusUpdate"""
        with patch('accrual_bot.tasks.sct.pipeline_orchestrator.config_manager') as mock_cm:
            mock_cm._config_toml = {
                'pipeline': {
                    'sct': {
                        'enabled_pr_steps': [
                            'SCTPRDataLoading', 'SCTPRERMLogic',
                        ]
                    }
                },
                'sct_pr_erm_status_rules': {'conditions': []},
                'sct_column_defaults': {},
                'fa_accounts': {'sct': ['199999']},
                'sct': {'ppe': {}},
            }
            mock_cm.get_list.return_value = ['199999']

            from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
            orch = SCTPipelineOrchestrator()
            steps = orch.get_enabled_steps('PR')
            assert 'SCTAssetStatusUpdate' not in steps
