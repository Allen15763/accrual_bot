"""SPXPipelineOrchestrator 單元測試"""
import pytest
from unittest.mock import Mock, patch
from accrual_bot.tasks.spx.pipeline_orchestrator import SPXPipelineOrchestrator
from accrual_bot.core.pipeline import Pipeline


@pytest.mark.unit
class TestSPXPipelineOrchestrator:
    """SPXPipelineOrchestrator 測試套件"""

    @pytest.fixture
    def orchestrator(self, mock_config_manager):
        """創建 orchestrator 實例"""
        return SPXPipelineOrchestrator()

    @pytest.fixture
    def file_paths(self):
        """測試用文件路徑"""
        return {
            'po_file': '/tmp/spx_po.xlsx',
            'pr_file': '/tmp/spx_pr.xlsx',
            'reference': '/tmp/reference.xlsx'
        }

    # --- 初始化測試 ---

    def test_init_reads_config(self, orchestrator, mock_config_manager):
        """測試初始化時讀取配置"""
        assert orchestrator.entity_type == 'SPX'
        assert orchestrator.config is not None
        assert 'enabled_po_steps' in orchestrator.config
        assert 'enabled_pr_steps' in orchestrator.config

    # --- build_po_pipeline 測試 ---

    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXDataLoadingStep')
    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.ColumnAdditionStep')
    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXERMLogicStep')
    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXExportStep')
    def test_build_po_pipeline_with_config(
        self, mock_export, mock_erm, mock_column, mock_loading,
        orchestrator, file_paths, mock_config_manager
    ):
        """測試使用配置構建 PO pipeline"""
        # 設置 mock
        mock_loading.return_value = Mock(name='SPXDataLoading')
        mock_column.return_value = Mock(name='ColumnAddition')
        mock_erm.return_value = Mock(name='SPXERMLogic')
        mock_export.return_value = Mock(name='SPXExport')

        # 執行
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證
        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPX_PO_Processing"
        assert pipeline.config.entity_type == 'SPX'
        assert pipeline.config.stop_on_error is True
        assert len(pipeline.steps) == 8  # 8個啟用的步驟

    def test_build_po_pipeline_with_default_steps(
        self, orchestrator, file_paths, mock_config_manager
    ):
        """測試配置為空時使用默認步驟"""
        # 清空配置
        mock_config_manager._config_toml['pipeline']['spx']['enabled_po_steps'] = []

        with patch.multiple(
            'accrual_bot.tasks.spx.pipeline_orchestrator',
            SPXDataLoadingStep=Mock(return_value=Mock()),
            ColumnAdditionStep=Mock(return_value=Mock()),
            ClosingListIntegrationStep=Mock(return_value=Mock()),
            StatusStage1Step=Mock(return_value=Mock()),
            SPXERMLogicStep=Mock(return_value=Mock()),
            DepositStatusUpdateStep=Mock(return_value=Mock()),
            ValidationDataProcessingStep=Mock(return_value=Mock()),
            SPXExportStep=Mock(return_value=Mock())
        ):
            pipeline = orchestrator.build_po_pipeline(file_paths)

            # 應使用默認步驟（8個）
            assert len(pipeline.steps) == 8

    # --- build_pr_pipeline 測試 ---

    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXPRDataLoadingStep')
    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXPRERMLogicStep')
    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXPRExportStep')
    def test_build_pr_pipeline(
        self, mock_export, mock_erm, mock_pr_loading,
        orchestrator, file_paths, mock_config_manager
    ):
        """測試構建 PR pipeline"""
        mock_pr_loading.return_value = Mock(name='SPXPRDataLoading')
        mock_erm.return_value = Mock(name='SPXPRERMLogic')
        mock_export.return_value = Mock(name='SPXPRExport')

        pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPX_PR_Processing"
        assert len(pipeline.steps) == 5

    # --- _create_step 測試 ---

    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXDataLoadingStep')
    def test_create_step_data_loading(
        self, mock_loading, orchestrator, file_paths
    ):
        """測試創建 DataLoadingStep"""
        mock_loading.return_value = Mock(name='SPXDataLoading')

        step = orchestrator._create_step('SPXDataLoading', file_paths, 'PO')

        assert step is not None
        mock_loading.assert_called_once_with(
            name="SPXDataLoading",
            file_paths=file_paths
        )

    @patch('accrual_bot.tasks.spx.pipeline_orchestrator.SPXERMLogicStep')
    def test_create_step_no_file_paths_needed(
        self, mock_erm, orchestrator, file_paths
    ):
        """測試創建不需要 file_paths 的步驟"""
        mock_erm.return_value = Mock(name='SPXERMLogic')

        step = orchestrator._create_step('SPXERMLogic', file_paths, 'PO')

        assert step is not None
        mock_erm.assert_called_once_with(name="SPXERMLogic")

    def test_create_step_unknown_step_name(
        self, orchestrator, file_paths, capsys
    ):
        """測試未知步驟名稱返回 None"""
        step = orchestrator._create_step('UnknownStep', file_paths, 'PO')

        assert step is None
        # 程式碼使用 print() 輸出，改用 capsys 捕獲 stdout
        captured = capsys.readouterr()
        assert "Warning: Unknown step 'UnknownStep'" in captured.out

    # --- get_enabled_steps 測試 ---

    def test_get_enabled_steps_po(
        self, orchestrator, mock_config_manager
    ):
        """測試獲取 PO 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PO')

        assert isinstance(steps, list)
        assert len(steps) == 8
        assert 'SPXDataLoading' in steps
        assert 'SPXERMLogic' in steps

    def test_get_enabled_steps_pr(
        self, orchestrator, mock_config_manager
    ):
        """測試獲取 PR 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PR')

        assert isinstance(steps, list)
        assert len(steps) == 5
        assert 'SPXPRDataLoading' in steps

    def test_get_enabled_steps_empty_config(
        self, mock_config_manager
    ):
        """測試配置為空時返回空列表"""
        # 先修改 mock，再創建 orchestrator
        mock_config_manager._config_toml['pipeline']['spx'] = {}
        
        # 重新創建 orchestrator 以獲取新的配置
        orchestrator = SPXPipelineOrchestrator()

        steps = orchestrator.get_enabled_steps('PO')

        assert steps == []


# 參數化測試
@pytest.mark.parametrize("processing_type,expected_steps", [
    ('PO', 8),
    ('PR', 5),
])
def test_orchestrator_processing_types(
    processing_type, expected_steps, mock_config_manager
):
    """參數化測試不同處理類型"""
    orchestrator = SPXPipelineOrchestrator()

    file_paths = {'input': '/tmp/test.xlsx'}

    with patch.multiple(
        'accrual_bot.tasks.spx.pipeline_orchestrator',
        SPXDataLoadingStep=Mock(return_value=Mock()),
        SPXPRDataLoadingStep=Mock(return_value=Mock()),
        ColumnAdditionStep=Mock(return_value=Mock()),
        ClosingListIntegrationStep=Mock(return_value=Mock()),
        StatusStage1Step=Mock(return_value=Mock()),
        SPXERMLogicStep=Mock(return_value=Mock()),
        SPXPRERMLogicStep=Mock(return_value=Mock()),
        DepositStatusUpdateStep=Mock(return_value=Mock()),
        ValidationDataProcessingStep=Mock(return_value=Mock()),
        SPXExportStep=Mock(return_value=Mock()),
        SPXPRExportStep=Mock(return_value=Mock())
    ):
        if processing_type == 'PO':
            pipeline = orchestrator.build_po_pipeline(file_paths)
        else:
            pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert len(pipeline.steps) == expected_steps
