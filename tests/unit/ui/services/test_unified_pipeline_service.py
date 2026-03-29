"""
UnifiedPipelineService 單元測試

測試 UI 服務層的 pipeline 建構與查詢功能。
"""

import pytest
from unittest.mock import patch, Mock, MagicMock

from accrual_bot.ui.services.unified_pipeline_service import UnifiedPipelineService


@pytest.fixture
def service():
    """建立 UnifiedPipelineService 實例"""
    return UnifiedPipelineService()


@pytest.fixture
def mock_spt_orchestrator():
    """Mock SPTPipelineOrchestrator"""
    mock = MagicMock()
    mock.get_enabled_steps.return_value = [
        'SPTDataLoading', 'CommissionDataUpdate', 'SPTERMLogic'
    ]
    mock.build_po_pipeline.return_value = MagicMock(name='spt_po_pipeline')
    mock.build_pr_pipeline.return_value = MagicMock(name='spt_pr_pipeline')
    mock.build_procurement_pipeline.return_value = MagicMock(name='spt_procurement_pipeline')
    return mock


@pytest.fixture
def mock_spx_orchestrator():
    """Mock SPXPipelineOrchestrator"""
    mock = MagicMock()
    mock.get_enabled_steps.return_value = [
        'SPXDataLoading', 'ColumnAddition', 'SPXERMLogic'
    ]
    mock.build_po_pipeline.return_value = MagicMock(name='spx_po_pipeline')
    mock.build_pr_pipeline.return_value = MagicMock(name='spx_pr_pipeline')
    mock.build_ppe_pipeline.return_value = MagicMock(name='spx_ppe_pipeline')
    mock.build_ppe_desc_pipeline.return_value = MagicMock(name='spx_ppe_desc_pipeline')
    return mock


# =============================================================================
# get_available_entities 測試
# =============================================================================

@pytest.mark.unit
class TestGetAvailableEntities:
    """測試 get_available_entities 方法"""

    def test_returns_entity_list(self, service):
        """應回傳包含 SPT 和 SPX 的清單"""
        entities = service.get_available_entities()
        assert isinstance(entities, list)
        assert 'SPT' in entities
        assert 'SPX' in entities

    def test_mob_not_included(self, service):
        """MOB 應被排除在可用 entity 之外"""
        entities = service.get_available_entities()
        assert 'MOB' not in entities


# =============================================================================
# get_entity_config 測試
# =============================================================================

@pytest.mark.unit
class TestGetEntityConfig:
    """測試 get_entity_config 方法"""

    def test_returns_config_for_valid_entity(self, service):
        """應回傳有效 entity 的配置字典"""
        config = service.get_entity_config('SPX')
        assert isinstance(config, dict)
        assert 'types' in config
        assert 'display_name' in config

    def test_returns_empty_dict_for_unknown_entity(self, service):
        """未知 entity 應回傳空字典"""
        config = service.get_entity_config('UNKNOWN')
        assert config == {}


# =============================================================================
# get_entity_types 測試
# =============================================================================

@pytest.mark.unit
class TestGetEntityTypes:
    """測試 get_entity_types 方法"""

    def test_spt_types(self, service):
        """SPT 應包含 PO, PR, PROCUREMENT 類型"""
        types = service.get_entity_types('SPT')
        assert 'PO' in types
        assert 'PR' in types
        assert 'PROCUREMENT' in types

    def test_spx_types(self, service):
        """SPX 應包含 PO, PR, PPE, PPE_DESC 類型"""
        types = service.get_entity_types('SPX')
        assert 'PO' in types
        assert 'PR' in types
        assert 'PPE' in types
        assert 'PPE_DESC' in types

    def test_unknown_entity_returns_empty(self, service):
        """未知 entity 應回傳空清單"""
        types = service.get_entity_types('UNKNOWN')
        assert types == []


# =============================================================================
# get_enabled_steps 測試
# =============================================================================

@pytest.mark.unit
class TestGetEnabledSteps:
    """測試 get_enabled_steps 方法"""

    @patch('accrual_bot.ui.services.unified_pipeline_service.SPTPipelineOrchestrator')
    def test_spt_po_steps(self, mock_class, service):
        """SPT PO 應回傳已啟用步驟清單"""
        mock_instance = MagicMock()
        mock_instance.get_enabled_steps.return_value = ['SPTDataLoading', 'SPTERMLogic']
        mock_class.return_value = mock_instance

        steps = service.get_enabled_steps('SPT', 'PO')
        assert isinstance(steps, list)
        assert len(steps) > 0
        mock_instance.get_enabled_steps.assert_called_once_with('PO')

    @patch('accrual_bot.ui.services.unified_pipeline_service.SPXPipelineOrchestrator')
    def test_spx_pr_steps(self, mock_class, service):
        """SPX PR 應回傳已啟用步驟清單"""
        mock_instance = MagicMock()
        mock_instance.get_enabled_steps.return_value = ['SPXPRDataLoading', 'ColumnAddition']
        mock_class.return_value = mock_instance

        steps = service.get_enabled_steps('SPX', 'PR')
        assert steps == ['SPXPRDataLoading', 'ColumnAddition']

    @patch('accrual_bot.ui.services.unified_pipeline_service.SPTPipelineOrchestrator')
    def test_procurement_with_source_type(self, mock_class, service):
        """PROCUREMENT 帶 source_type 時應傳入 source_type 參數"""
        mock_instance = MagicMock()
        mock_instance.get_enabled_steps.return_value = ['Step1', 'Step2']
        mock_class.return_value = mock_instance

        steps = service.get_enabled_steps('SPT', 'PROCUREMENT', source_type='PO')
        mock_instance.get_enabled_steps.assert_called_once_with('PROCUREMENT', source_type='PO')

    def test_unknown_entity_raises_error(self, service):
        """未知 entity 應拋出 ValueError"""
        with pytest.raises(ValueError, match="不支援的 entity"):
            service.get_enabled_steps('UNKNOWN', 'PO')


# =============================================================================
# build_pipeline 測試
# =============================================================================

@pytest.mark.unit
class TestBuildPipeline:
    """測試 build_pipeline 方法"""

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    @patch('accrual_bot.ui.services.unified_pipeline_service.SPTPipelineOrchestrator')
    def test_build_spt_po_pipeline(self, mock_orch_class, mock_config_cls, service):
        """應成功建立 SPT PO pipeline"""
        # Mock ConfigManager 讓 _enrich_file_paths 不報錯
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        mock_orch = MagicMock()
        mock_pipeline = MagicMock(name='spt_po_pipeline')
        mock_orch.build_po_pipeline.return_value = mock_pipeline
        mock_orch_class.return_value = mock_orch

        file_paths = {'raw_po': '/tmp/test.csv'}
        result = service.build_pipeline('SPT', 'PO', file_paths)

        assert result == mock_pipeline
        mock_orch.build_po_pipeline.assert_called_once()

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    @patch('accrual_bot.ui.services.unified_pipeline_service.SPXPipelineOrchestrator')
    def test_build_spx_pr_pipeline(self, mock_orch_class, mock_config_cls, service):
        """應成功建立 SPX PR pipeline"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        mock_orch = MagicMock()
        mock_pipeline = MagicMock(name='spx_pr_pipeline')
        mock_orch.build_pr_pipeline.return_value = mock_pipeline
        mock_orch_class.return_value = mock_orch

        file_paths = {'raw_pr': '/tmp/test.csv'}
        result = service.build_pipeline('SPX', 'PR', file_paths)

        assert result == mock_pipeline
        mock_orch.build_pr_pipeline.assert_called_once()

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    @patch('accrual_bot.ui.services.unified_pipeline_service.SPXPipelineOrchestrator')
    def test_build_spx_ppe_requires_date(self, mock_orch_class, mock_config_cls, service):
        """SPX PPE 未提供 processing_date 應拋出 ValueError"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        mock_orch_class.return_value = MagicMock()

        with pytest.raises(ValueError, match="PPE 處理需要提供 processing_date"):
            service.build_pipeline('SPX', 'PPE', {'contract_filing_list': '/tmp/test.xlsx'})

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    @patch('accrual_bot.ui.services.unified_pipeline_service.SPXPipelineOrchestrator')
    def test_build_spx_ppe_with_date(self, mock_orch_class, mock_config_cls, service):
        """SPX PPE 提供 processing_date 應成功建立"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        mock_orch = MagicMock()
        mock_pipeline = MagicMock(name='spx_ppe_pipeline')
        mock_orch.build_ppe_pipeline.return_value = mock_pipeline
        mock_orch_class.return_value = mock_orch

        result = service.build_pipeline(
            'SPX', 'PPE',
            {'contract_filing_list': '/tmp/test.xlsx'},
            processing_date=202512
        )
        assert result == mock_pipeline
        mock_orch.build_ppe_pipeline.assert_called_once()

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    @patch('accrual_bot.ui.services.unified_pipeline_service.SPTPipelineOrchestrator')
    def test_build_procurement_requires_source_type(self, mock_orch_class, mock_config_cls, service):
        """PROCUREMENT 未提供 source_type 應拋出 ValueError"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        mock_orch_class.return_value = MagicMock()

        with pytest.raises(ValueError, match="PROCUREMENT 需要指定 source_type"):
            service.build_pipeline('SPT', 'PROCUREMENT', {'raw_po': '/tmp/test.csv'})

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_build_pipeline_unsupported_type(self, mock_config_cls, service):
        """不支援的處理類型應拋出 ValueError"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {}
        mock_config_cls.return_value = mock_config_instance

        with pytest.raises(ValueError, match="不支援的處理類型"):
            service.build_pipeline('SPT', 'INVALID', {})

    def test_build_pipeline_unknown_entity(self, service):
        """未知 entity 應拋出 ValueError"""
        with pytest.raises(ValueError, match="不支援的 entity"):
            service.build_pipeline('UNKNOWN', 'PO', {})


# =============================================================================
# _enrich_file_paths 測試
# =============================================================================

@pytest.mark.unit
class TestEnrichFilePaths:
    """測試 _enrich_file_paths 方法"""

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_enrich_with_params(self, mock_config_cls, service):
        """當 paths.toml 有對應參數時，應將 path 包裝成帶 params 的字典"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {
            'raw_po': {'encoding': 'utf-8', 'sep': ','},
        }
        mock_config_cls.return_value = mock_config_instance

        result = service._enrich_file_paths(
            {'raw_po': '/tmp/test.csv'},
            'SPX', 'PO'
        )
        assert isinstance(result['raw_po'], dict)
        assert result['raw_po']['path'] == '/tmp/test.csv'
        assert result['raw_po']['params'] == {'encoding': 'utf-8', 'sep': ','}

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_enrich_without_params(self, mock_config_cls, service):
        """當 paths.toml 無對應參數時，應保持原始字串路徑"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {
            'other_key': {'encoding': 'utf-8'},
        }
        mock_config_cls.return_value = mock_config_instance

        result = service._enrich_file_paths(
            {'raw_po': '/tmp/test.csv'},
            'SPX', 'PO'
        )
        assert result['raw_po'] == '/tmp/test.csv'

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_enrich_with_source_type_suffix(self, mock_config_cls, service):
        """PROCUREMENT 帶 source_type 時，應嘗試 suffixed key 查找參數"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = {
            'procurement_previous_po': {'sheet_name': 0, 'header': 0},
        }
        mock_config_cls.return_value = mock_config_instance

        result = service._enrich_file_paths(
            {'procurement_previous': '/tmp/prev.xlsx'},
            'SPT', 'PROCUREMENT',
            source_type='PO'
        )
        assert isinstance(result['procurement_previous'], dict)
        assert result['procurement_previous']['params'] == {'sheet_name': 0, 'header': 0}

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_enrich_fallback_on_exception(self, mock_config_cls, service):
        """ConfigManager 拋出異常時，應回傳原始 file_paths"""
        mock_config_cls.side_effect = Exception("Config error")

        file_paths = {'raw_po': '/tmp/test.csv'}
        result = service._enrich_file_paths(file_paths, 'SPX', 'PO')
        assert result == file_paths

    @patch('accrual_bot.ui.services.unified_pipeline_service.ConfigManager')
    def test_enrich_with_none_params_config(self, mock_config_cls, service):
        """當 get_paths_config 回傳 None 時，應回傳原始 file_paths"""
        mock_config_instance = MagicMock()
        mock_config_instance.get_paths_config.return_value = None
        mock_config_cls.return_value = mock_config_instance

        file_paths = {'raw_po': '/tmp/test.csv'}
        result = service._enrich_file_paths(file_paths, 'SPX', 'PO')
        assert result == file_paths


# =============================================================================
# _get_orchestrator 測試
# =============================================================================

@pytest.mark.unit
class TestGetOrchestrator:
    """測試 _get_orchestrator 方法"""

    @patch('accrual_bot.ui.services.unified_pipeline_service.SPTPipelineOrchestrator')
    def test_returns_spt_orchestrator(self, mock_class, service):
        """SPT 應回傳 SPTPipelineOrchestrator 實例"""
        service._get_orchestrator('SPT')
        mock_class.assert_called_once()

    @patch('accrual_bot.ui.services.unified_pipeline_service.SPXPipelineOrchestrator')
    def test_returns_spx_orchestrator(self, mock_class, service):
        """SPX 應回傳 SPXPipelineOrchestrator 實例"""
        service._get_orchestrator('SPX')
        mock_class.assert_called_once()

    def test_unknown_entity_raises_error(self, service):
        """未知 entity 應拋出 ValueError"""
        with pytest.raises(ValueError, match="不支援的 entity"):
            service._get_orchestrator('UNKNOWN')
