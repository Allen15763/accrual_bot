"""SCT Data Loading Steps 單元測試

測試 SCTBaseDataLoadingStep / SCTDataLoadingStep / SCTPRDataLoadingStep：
- 子類正確宣告 get_required_file_type()
- _extract_primary_data 驗證邏輯
- _load_reference_data 各分支
- _load_primary_file 處理
- _get_custom_file_loader ap_invoice 載入
- _set_additional_context_variables 資料快照
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from accrual_bot.core.pipeline.context import ProcessingContext


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_sct_loading_deps():
    """Mock SCT loading 步驟的外部依賴"""
    with patch('accrual_bot.tasks.sct.steps.sct_loading.config_manager') as mock_cm, \
         patch('accrual_bot.tasks.sct.steps.sct_loading.get_ref_on_colab') as mock_ref, \
         patch('accrual_bot.tasks.sct.steps.sct_loading.DataSourceFactory') as mock_dsf:
        mock_cm._config_toml = {
            'paths': {'ref_path_sct': '/tmp/ref_SCTTW.xlsx'},
            'data_shape_summary': {'enabled': False},
        }
        mock_cm.get_list.return_value = []
        mock_ref.return_value = None
        yield {
            'config_manager': mock_cm,
            'get_ref_on_colab': mock_ref,
            'DataSourceFactory': mock_dsf,
        }


def _create_valid_df():
    """建立有效的 SCT 主資料"""
    return pd.DataFrame({
        'Product Code': ['P001', 'P002'],
        'Item Description': ['Item A', 'Item B'],
        'GL#': ['100000', '100001'],
        'Line#': ['1', '2'],
    })


# ============================================================
# SCTDataLoadingStep / SCTPRDataLoadingStep 測試
# ============================================================

class TestSCTDataLoadingStep:
    """測試 SCT PO 數據載入步驟"""

    @pytest.mark.unit
    def test_instantiation(self, mock_sct_loading_deps):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        assert step.name == "SCTDataLoading"

    @pytest.mark.unit
    def test_get_required_file_type(self, mock_sct_loading_deps):
        """PO 載入回傳 'raw_po'"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        assert step.get_required_file_type() == 'raw_po'

    @pytest.mark.unit
    def test_extract_primary_data_valid(self, mock_sct_loading_deps):
        """有效資料通過驗證"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        df = _create_valid_df()
        result = step._extract_primary_data(df)
        assert len(result) == 2

    @pytest.mark.unit
    def test_extract_primary_data_empty(self, mock_sct_loading_deps):
        """空資料引發 ValueError"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        with pytest.raises(ValueError, match="empty"):
            step._extract_primary_data(pd.DataFrame())

    @pytest.mark.unit
    def test_extract_primary_data_none(self, mock_sct_loading_deps):
        """None 引發 ValueError"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        with pytest.raises(ValueError, match="empty"):
            step._extract_primary_data(None)

    @pytest.mark.unit
    def test_extract_primary_data_missing_columns(self, mock_sct_loading_deps):
        """缺少必要欄位引發 ValueError"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        df = pd.DataFrame({'SomeColumn': [1, 2]})
        with pytest.raises(ValueError, match="Missing required columns"):
            step._extract_primary_data(df)

    @pytest.mark.unit
    def test_get_custom_file_loader_ap_invoice(self, mock_sct_loading_deps):
        """ap_invoice 有自定義載入器"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        loader = step._get_custom_file_loader('ap_invoice')
        assert loader is not None

    @pytest.mark.unit
    def test_get_custom_file_loader_other(self, mock_sct_loading_deps):
        """其他類型無自定義載入器"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()
        loader = step._get_custom_file_loader('raw_po')
        assert loader is None


class TestSCTPRDataLoadingStep:
    """測試 SCT PR 數據載入步驟"""

    @pytest.mark.unit
    def test_instantiation(self, mock_sct_loading_deps):
        """正確初始化"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTPRDataLoadingStep
        step = SCTPRDataLoadingStep()
        assert step.name == "SCTPRDataLoading"

    @pytest.mark.unit
    def test_get_required_file_type(self, mock_sct_loading_deps):
        """PR 載入回傳 'raw_pr'"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTPRDataLoadingStep
        step = SCTPRDataLoadingStep()
        assert step.get_required_file_type() == 'raw_pr'


class TestSCTLoadReferenceData:
    """測試 SCT 參考資料載入"""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_load_reference_data_from_colab(self, mock_sct_loading_deps):
        """Colab 環境：從 ZIP 載入參考資料"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()

        ref_df = pd.DataFrame({
            'Index': ['A', 'B'],
            'Account': ['100000', '100001'],
            'Account Desc': ['Cash', 'Recv'],
            'Liability': ['211111', '211112'],
        })
        mock_sct_loading_deps['get_ref_on_colab'].return_value = ref_df

        ctx = ProcessingContext(
            data=pd.DataFrame({'col': [1]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        count = await step._load_reference_data(ctx)
        assert count == 2
        assert ctx.get_auxiliary_data('reference_account') is not None
        assert ctx.get_auxiliary_data('reference_liability') is not None

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_load_reference_data_file_not_found(self, mock_sct_loading_deps):
        """檔案不存在時設置空 DataFrame"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()

        ctx = ProcessingContext(
            data=pd.DataFrame({'col': [1]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        count = await step._load_reference_data(ctx)
        assert count == 0
        ref_account = ctx.get_auxiliary_data('reference_account')
        assert ref_account is not None
        assert ref_account.empty

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_load_reference_data_exception(self, mock_sct_loading_deps):
        """異常時安全返回 0"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()

        mock_sct_loading_deps['config_manager']._config_toml = {}

        ctx = ProcessingContext(
            data=pd.DataFrame({'col': [1]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        count = await step._load_reference_data(ctx)
        assert count == 0


class TestSCTSetAdditionalContextVariables:
    """測試 _set_additional_context_variables"""

    @pytest.mark.unit
    def test_snapshot_enabled(self, mock_sct_loading_deps):
        """data_shape_summary 啟用時存入 raw_data_snapshot"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()

        mock_sct_loading_deps['config_manager']._config_toml['data_shape_summary'] = {'enabled': True}

        ctx = ProcessingContext(
            data=pd.DataFrame({'col': [1, 2, 3]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        step._set_additional_context_variables(ctx, {}, {})
        snapshot = ctx.get_auxiliary_data('raw_data_snapshot')
        assert snapshot is not None
        assert len(snapshot) == 3

    @pytest.mark.unit
    def test_snapshot_disabled(self, mock_sct_loading_deps):
        """data_shape_summary 停用時不存入"""
        from accrual_bot.tasks.sct.steps.sct_loading import SCTDataLoadingStep
        step = SCTDataLoadingStep()

        ctx = ProcessingContext(
            data=pd.DataFrame({'col': [1, 2]}),
            entity_type='SCT',
            processing_date=202503,
            processing_type='PO',
        )
        step._set_additional_context_variables(ctx, {}, {})
        snapshot = ctx.get_auxiliary_data('raw_data_snapshot')
        assert snapshot is None
