"""SPXPipelineOrchestrator 單元測試"""
import pytest
from unittest.mock import Mock, patch
from accrual_bot.tasks.spx.pipeline_orchestrator import SPXPipelineOrchestrator
from accrual_bot.core.pipeline import Pipeline


# 用於 SPX orchestrator 的 mock config
SPX_MOCK_CONFIG = {
    'enabled_po_steps': [
        'SPXDataLoading',
        'ColumnAddition',
        'ClosingListIntegration',
        'StatusStage1',
        'SPXERMLogic',
        'DepositStatusUpdate',
        'ValidationDataProcessing',
        'SPXExport'
    ],
    'enabled_pr_steps': [
        'SPXPRDataLoading',
        'ColumnAddition',
        'StatusStage1',
        'SPXPRERMLogic',
        'SPXPRExport'
    ]
}


@pytest.fixture
def mock_spx_config():
    """Mock config_manager 在 SPX orchestrator 模組中"""
    import copy
    with patch('accrual_bot.tasks.spx.pipeline_orchestrator.config_manager') as mock_cm:
        mock_cm._config_toml = {
            'pipeline': {'spx': copy.deepcopy(SPX_MOCK_CONFIG)}
        }
        yield mock_cm


@pytest.fixture
def orchestrator(mock_spx_config):
    """創建 orchestrator 實例"""
    return SPXPipelineOrchestrator()


@pytest.fixture
def file_paths():
    """測試用文件路徑"""
    return {
        'po_file': '/tmp/spx_po.xlsx',
        'pr_file': '/tmp/spx_pr.xlsx',
        'reference': '/tmp/reference.xlsx'
    }


@pytest.mark.unit
class TestSPXPipelineOrchestrator:
    """SPXPipelineOrchestrator 測試套件"""

    # --- 初始化測試 ---

    def test_init_reads_config(self, orchestrator):
        """測試初始化時讀取配置"""
        assert orchestrator.entity_type == 'SPX'
        assert orchestrator.config is not None
        assert 'enabled_po_steps' in orchestrator.config
        assert 'enabled_pr_steps' in orchestrator.config

    # --- build_po_pipeline 測試 ---

    def test_build_po_pipeline_with_config(self, orchestrator, file_paths):
        """測試使用配置構建 PO pipeline"""
        pipeline = orchestrator.build_po_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPX_PO_Processing"
        assert pipeline.config.entity_type == 'SPX'
        assert pipeline.config.stop_on_error is True
        assert len(pipeline.steps) == 8

    def test_build_po_pipeline_with_default_steps(self, mock_spx_config, file_paths):
        """測試配置為空時使用默認步驟"""
        mock_spx_config._config_toml['pipeline']['spx']['enabled_po_steps'] = []
        orchestrator = SPXPipelineOrchestrator()

        pipeline = orchestrator.build_po_pipeline(file_paths)
        # 默認步驟: 8 個
        assert len(pipeline.steps) == 8

    # --- build_pr_pipeline 測試 ---

    def test_build_pr_pipeline(self, orchestrator, file_paths):
        """測試構建 PR pipeline"""
        pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPX_PR_Processing"
        assert len(pipeline.steps) == 5

    # --- _create_step 測試 ---

    def test_create_step_data_loading(self, orchestrator, file_paths):
        """測試創建 DataLoadingStep"""
        step = orchestrator._create_step('SPXDataLoading', file_paths, 'PO')
        assert step is not None
        assert step.name == "SPXDataLoading"

    def test_create_step_erm_logic(self, orchestrator, file_paths):
        """測試創建 SPXERMLogicStep"""
        step = orchestrator._create_step('SPXERMLogic', file_paths, 'PO')
        assert step is not None
        assert step.name == "SPXERMLogic"

    def test_create_step_unknown_step_name(self, orchestrator, file_paths, capsys):
        """測試未知步驟名稱返回 None"""
        step = orchestrator._create_step('UnknownStep', file_paths, 'PO')
        assert step is None
        captured = capsys.readouterr()
        assert "Warning: Unknown step 'UnknownStep'" in captured.out

    # --- get_enabled_steps 測試 ---

    def test_get_enabled_steps_po(self, orchestrator):
        """測試獲取 PO 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PO')
        assert isinstance(steps, list)
        assert len(steps) == 8
        assert 'SPXDataLoading' in steps
        assert 'SPXERMLogic' in steps

    def test_get_enabled_steps_pr(self, orchestrator):
        """測試獲取 PR 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PR')
        assert isinstance(steps, list)
        assert len(steps) == 5
        assert 'SPXPRDataLoading' in steps

    def test_get_enabled_steps_empty_config(self, mock_spx_config):
        """測試配置為空時返回空列表"""
        mock_spx_config._config_toml['pipeline']['spx'] = {}
        orchestrator = SPXPipelineOrchestrator()
        steps = orchestrator.get_enabled_steps('PO')
        assert steps == []


# 參數化測試
@pytest.mark.unit
@pytest.mark.parametrize("processing_type,expected_steps", [
    ('PO', 8),
    ('PR', 5),
])
def test_orchestrator_processing_types(processing_type, expected_steps, mock_spx_config):
    """參數化測試不同處理類型"""
    orchestrator = SPXPipelineOrchestrator()
    file_paths = {'input': '/tmp/test.xlsx'}

    if processing_type == 'PO':
        pipeline = orchestrator.build_po_pipeline(file_paths)
    else:
        pipeline = orchestrator.build_pr_pipeline(file_paths)

    assert len(pipeline.steps) == expected_steps
