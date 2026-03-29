"""SPTPipelineOrchestrator 單元測試"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from accrual_bot.tasks.spt.pipeline_orchestrator import SPTPipelineOrchestrator
from accrual_bot.core.pipeline import Pipeline


# 用於 SPT orchestrator 的 mock config
SPT_MOCK_CONFIG = {
    'enabled_po_steps': [
        'SPTDataLoading',
        'CommissionDataUpdate',
        'PayrollDetection',
        'SPTERMLogic',
        'SPTStatusLabel',
        'SPTAccountPrediction'
    ],
    'enabled_pr_steps': [
        'SPTPRDataLoading',
        'CommissionDataUpdate',
        'PayrollDetection',
        'SPTERMLogic',
        'SPTStatusLabel',
        'SPTAccountPrediction'
    ]
}


@pytest.fixture
def mock_spt_config():
    """Mock config_manager 在 orchestrator 模組中"""
    import copy
    with patch('accrual_bot.tasks.spt.pipeline_orchestrator.config_manager') as mock_cm:
        mock_cm._config_toml = {
            'pipeline': {'spt': copy.deepcopy(SPT_MOCK_CONFIG)}
        }
        yield mock_cm


@pytest.fixture
def orchestrator(mock_spt_config):
    """創建 orchestrator 實例"""
    return SPTPipelineOrchestrator()


@pytest.fixture
def file_paths():
    """測試用文件路徑"""
    return {
        'po_file': '/tmp/spt_po.xlsx',
        'pr_file': '/tmp/spt_pr.xlsx',
        'reference': '/tmp/reference.xlsx'
    }


@pytest.mark.unit
class TestSPTPipelineOrchestrator:
    """SPTPipelineOrchestrator 測試套件"""

    # --- 初始化測試 ---

    def test_init_reads_config(self, orchestrator):
        """測試初始化時讀取配置"""
        assert orchestrator.entity_type == 'SPT'
        assert orchestrator.config is not None
        assert 'enabled_po_steps' in orchestrator.config
        assert 'enabled_pr_steps' in orchestrator.config

    # --- build_po_pipeline 測試 ---

    def test_build_po_pipeline_with_config(self, orchestrator, file_paths):
        """測試使用配置構建 PO pipeline"""
        pipeline = orchestrator.build_po_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPT_PO_Processing"
        assert pipeline.config.entity_type == 'SPT'
        assert pipeline.config.stop_on_error is True
        assert len(pipeline.steps) == 6

    def test_build_po_pipeline_with_default_steps(self, mock_spt_config, file_paths):
        """測試配置為空時使用默認步驟"""
        mock_spt_config._config_toml['pipeline']['spt']['enabled_po_steps'] = []
        orchestrator = SPTPipelineOrchestrator()

        pipeline = orchestrator.build_po_pipeline(file_paths)
        # 默認步驟: 6 個
        assert len(pipeline.steps) == 6

    def test_build_po_pipeline_with_custom_steps(self, orchestrator, file_paths):
        """測試添加自定義步驟"""
        custom_step = Mock(name='CustomStep')
        pipeline = orchestrator.build_po_pipeline(file_paths, custom_steps=[custom_step])

        # 6 個配置步驟 + 1 個自定義步驟
        assert len(pipeline.steps) == 7
        assert pipeline.steps[-1] == custom_step

    # --- build_pr_pipeline 測試 ---

    def test_build_pr_pipeline(self, orchestrator, file_paths):
        """測試構建 PR pipeline"""
        pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPT_PR_Processing"
        assert len(pipeline.steps) == 6

    # --- _create_step 測試 ---

    def test_create_step_data_loading(self, orchestrator, file_paths):
        """測試創建 DataLoadingStep"""
        step = orchestrator._create_step('SPTDataLoading', file_paths, 'PO')
        assert step is not None
        assert step.name == "SPTDataLoading"

    def test_create_step_erm_logic(self, orchestrator, file_paths):
        """測試創建 SPTERMLogicStep"""
        step = orchestrator._create_step('SPTERMLogic', file_paths, 'PO')
        assert step is not None
        assert step.name == "SPTERMLogic"

    def test_create_step_unknown_step_name(self, orchestrator, file_paths):
        """測試未知步驟名稱返回 None（警告已透過 logger 發出而非 print）"""
        step = orchestrator._create_step('UnknownStep', file_paths, 'PO')
        assert step is None

    # --- get_enabled_steps 測試 ---

    def test_get_enabled_steps_po(self, orchestrator):
        """測試獲取 PO 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PO')
        assert isinstance(steps, list)
        assert len(steps) == 6
        assert 'SPTDataLoading' in steps
        assert 'SPTERMLogic' in steps

    def test_get_enabled_steps_pr(self, orchestrator):
        """測試獲取 PR 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PR')
        assert isinstance(steps, list)
        assert len(steps) == 6
        assert 'SPTPRDataLoading' in steps

    def test_get_enabled_steps_empty_config(self, mock_spt_config):
        """測試配置為空時返回空列表"""
        mock_spt_config._config_toml['pipeline']['spt'] = {}
        orchestrator = SPTPipelineOrchestrator()
        steps = orchestrator.get_enabled_steps('PO')
        assert steps == []

    # --- 步驟順序測試 ---

    def test_pipeline_step_order(self, orchestrator, file_paths):
        """測試 pipeline 步驟順序與配置一致"""
        pipeline = orchestrator.build_po_pipeline(file_paths)

        expected_order = [
            'SPTDataLoading',
            'CommissionDataUpdate',
            'PayrollDetection',
            'SPTERMLogic',
            'SPTStatusLabel',
            'SPTAccountPrediction'
        ]

        assert len(pipeline.steps) == len(expected_order)
        for i, expected_name in enumerate(expected_order):
            assert pipeline.steps[i].name == expected_name


# 參數化測試
@pytest.mark.unit
@pytest.mark.parametrize("processing_type,expected_steps", [
    ('PO', 6),
    ('PR', 6),
])
def test_orchestrator_processing_types(processing_type, expected_steps, mock_spt_config):
    """參數化測試不同處理類型"""
    orchestrator = SPTPipelineOrchestrator()
    file_paths = {'input': '/tmp/test.xlsx'}

    if processing_type == 'PO':
        pipeline = orchestrator.build_po_pipeline(file_paths)
    else:
        pipeline = orchestrator.build_pr_pipeline(file_paths)

    assert len(pipeline.steps) == expected_steps
