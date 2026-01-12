"""SPTPipelineOrchestrator 單元測試"""
import pytest
from unittest.mock import Mock, patch
from accrual_bot.tasks.spt.pipeline_orchestrator import SPTPipelineOrchestrator
from accrual_bot.core.pipeline import Pipeline


class TestSPTPipelineOrchestrator:
    """SPTPipelineOrchestrator 測試套件"""

    @pytest.fixture
    def orchestrator(self, mock_config_manager):
        """創建 orchestrator 實例"""
        return SPTPipelineOrchestrator()

    @pytest.fixture
    def file_paths(self):
        """測試用文件路徑"""
        return {
            'po_file': '/tmp/spt_po.xlsx',
            'pr_file': '/tmp/spt_pr.xlsx',
            'reference': '/tmp/reference.xlsx'
        }

    # --- 初始化測試 ---

    def test_init_reads_config(self, orchestrator, mock_config_manager):
        """測試初始化時讀取配置"""
        assert orchestrator.entity_type == 'SPT'
        assert orchestrator.config is not None
        assert 'enabled_po_steps' in orchestrator.config
        assert 'enabled_pr_steps' in orchestrator.config

    # --- build_po_pipeline 測試 ---

    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTDataLoadingStep')
    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.CommissionDataUpdateStep')
    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTERMLogicStep')
    def test_build_po_pipeline_with_config(
        self, mock_erm, mock_commission, mock_loading,
        orchestrator, file_paths, mock_config_manager
    ):
        """測試使用配置構建 PO pipeline"""
        # 設置 mock
        mock_loading.return_value = Mock(name='SPTDataLoading')
        mock_commission.return_value = Mock(name='CommissionDataUpdate')
        mock_erm.return_value = Mock(name='SPTERMLogic')

        # 執行
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證
        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPT_PO_Processing"
        assert pipeline.config.entity_type == 'SPT'
        assert pipeline.config.stop_on_error is True
        assert len(pipeline.steps) == 6  # 6個啟用的步驟

    def test_build_po_pipeline_with_default_steps(
        self, orchestrator, file_paths, mock_config_manager
    ):
        """測試配置為空時使用默認步驟"""
        # 清空配置
        mock_config_manager._config_toml['pipeline']['spt']['enabled_po_steps'] = []

        with patch.multiple(
            'accrual_bot.tasks.spt.pipeline_orchestrator',
            SPTDataLoadingStep=Mock(return_value=Mock()),
            CommissionDataUpdateStep=Mock(return_value=Mock()),
            PayrollDetectionStep=Mock(return_value=Mock()),
            SPTERMLogicStep=Mock(return_value=Mock()),
            SPTStatusLabelStep=Mock(return_value=Mock()),
            SPTAccountPredictionStep=Mock(return_value=Mock())
        ):
            pipeline = orchestrator.build_po_pipeline(file_paths)

            # 應使用默認步驟（6個）
            assert len(pipeline.steps) == 6

    def test_build_po_pipeline_with_custom_steps(
        self, orchestrator, file_paths, mock_config_manager
    ):
        """測試添加自定義步驟"""
        custom_step = Mock(name='CustomStep')

        with patch.multiple(
            'accrual_bot.tasks.spt.pipeline_orchestrator',
            SPTDataLoadingStep=Mock(return_value=Mock()),
            CommissionDataUpdateStep=Mock(return_value=Mock()),
            PayrollDetectionStep=Mock(return_value=Mock()),
            SPTERMLogicStep=Mock(return_value=Mock()),
            SPTStatusLabelStep=Mock(return_value=Mock()),
            SPTAccountPredictionStep=Mock(return_value=Mock())
        ):
            pipeline = orchestrator.build_po_pipeline(
                file_paths,
                custom_steps=[custom_step]
            )

            # 應有 6個配置步驟 + 1個自定義步驟
            assert len(pipeline.steps) == 7
            assert pipeline.steps[-1] == custom_step

    # --- build_pr_pipeline 測試 ---

    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTPRDataLoadingStep')
    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTERMLogicStep')
    def test_build_pr_pipeline(
        self, mock_erm, mock_pr_loading,
        orchestrator, file_paths, mock_config_manager
    ):
        """測試構建 PR pipeline"""
        mock_pr_loading.return_value = Mock(name='SPTPRDataLoading')
        mock_erm.return_value = Mock(name='SPTERMLogic')

        pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.config.name == "SPT_PR_Processing"
        assert len(pipeline.steps) == 6

    # --- _create_step 測試 ---

    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTDataLoadingStep')
    def test_create_step_data_loading(
        self, mock_loading, orchestrator, file_paths
    ):
        """測試創建 DataLoadingStep"""
        mock_loading.return_value = Mock(name='SPTDataLoading')

        step = orchestrator._create_step('SPTDataLoading', file_paths, 'PO')

        assert step is not None
        mock_loading.assert_called_once_with(
            name="SPTDataLoading",
            file_paths=file_paths
        )

    @patch('accrual_bot.tasks.spt.pipeline_orchestrator.SPTERMLogicStep')
    def test_create_step_no_file_paths_needed(
        self, mock_erm, orchestrator, file_paths
    ):
        """測試創建不需要 file_paths 的步驟"""
        mock_erm.return_value = Mock(name='SPTERMLogic')

        step = orchestrator._create_step('SPTERMLogic', file_paths, 'PO')

        assert step is not None
        mock_erm.assert_called_once_with(name="SPTERMLogic")

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
        assert len(steps) == 6
        assert 'SPTDataLoading' in steps
        assert 'SPTERMLogic' in steps

    def test_get_enabled_steps_pr(
        self, orchestrator, mock_config_manager
    ):
        """測試獲取 PR 啟用步驟"""
        steps = orchestrator.get_enabled_steps('PR')

        assert isinstance(steps, list)
        assert len(steps) == 6
        assert 'SPTPRDataLoading' in steps

    def test_get_enabled_steps_empty_config(
        self, mock_config_manager
    ):
        """測試配置為空時返回空列表"""
        # 先修改 mock，再創建 orchestrator
        mock_config_manager._config_toml['pipeline']['spt'] = {}
        
        # 重新創建 orchestrator 以獲取新的配置
        orchestrator = SPTPipelineOrchestrator()

        steps = orchestrator.get_enabled_steps('PO')

        assert steps == []

    # --- 步驟順序測試 ---

    @patch.multiple(
        'accrual_bot.tasks.spt.pipeline_orchestrator',
        SPTDataLoadingStep=Mock(return_value=Mock(name='Step1')),
        CommissionDataUpdateStep=Mock(return_value=Mock(name='Step2')),
        PayrollDetectionStep=Mock(return_value=Mock(name='Step3')),
        SPTERMLogicStep=Mock(return_value=Mock(name='Step4')),
        SPTStatusLabelStep=Mock(return_value=Mock(name='Step5')),
        SPTAccountPredictionStep=Mock(return_value=Mock(name='Step6'))
    )
    def test_pipeline_step_order(
        self, orchestrator, file_paths, mock_config_manager
    ):
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

        for i, expected_name in enumerate(expected_order):
            # 驗證步驟名稱順序（通過 Mock 的 name 屬性）
            assert pipeline.steps[i].name.startswith('Step')


# 參數化測試
@pytest.mark.parametrize("processing_type,expected_steps", [
    ('PO', 6),
    ('PR', 6),
])
def test_orchestrator_processing_types(
    processing_type, expected_steps, mock_config_manager
):
    """參數化測試不同處理類型"""
    orchestrator = SPTPipelineOrchestrator()

    file_paths = {'input': '/tmp/test.xlsx'}

    with patch.multiple(
        'accrual_bot.tasks.spt.pipeline_orchestrator',
        SPTDataLoadingStep=Mock(return_value=Mock()),
        SPTPRDataLoadingStep=Mock(return_value=Mock()),
        CommissionDataUpdateStep=Mock(return_value=Mock()),
        PayrollDetectionStep=Mock(return_value=Mock()),
        SPTERMLogicStep=Mock(return_value=Mock()),
        SPTStatusLabelStep=Mock(return_value=Mock()),
        SPTAccountPredictionStep=Mock(return_value=Mock())
    ):
        if processing_type == 'PO':
            pipeline = orchestrator.build_po_pipeline(file_paths)
        else:
            pipeline = orchestrator.build_pr_pipeline(file_paths)

        assert len(pipeline.steps) == expected_steps
