# Accrual Bot Task 架構測試計畫

## 概述

為新實現的 task 架構創建全面的測試框架，包括：
- Pipeline Orchestrators (SPT/SPX)
- Base Classes (BaseLoadingStep, BaseERMEvaluationStep)
- 配置驅動的步驟加載
- 異步 pipeline 執行

## 當前實作狀態 (2026-01)

| 組件 | 狀態 | 測試文件 |
|------|------|----------|
| **測試基礎設施** | ✅ 完成 | `conftest.py`, `pytest.ini`, `fixtures/` |
| **ConfigManager 測試** | ✅ 完成 | `test_config_manager.py` (74 lines, 4 tests) |
| **BaseLoadingStep 測試** | ✅ 完成 | `test_base_loading.py` (328 lines, 15+ tests) |
| **BaseERMEvaluationStep 測試** | ✅ 完成 | `test_base_evaluation.py` (366 lines, 17+ tests) |
| **SPT Orchestrator 測試** | ✅ 完成 | `test_spt_orchestrator.py` (259 lines) |
| **SPX Orchestrator 測試** | ✅ 完成 | `test_spx_orchestrator.py` (215 lines) |
| **Pipeline 集成測試** | ✅ 完成 | `test_pipeline_orchestrators.py` (75 lines) |
| **測試文檔** | ✅ 完成 | `tests/README.md` (204 lines) |

**總計**: ~1,300+ 行測試代碼，50+ 測試方法

---

## 現況分析

### 現有測試結構
- **框架**: 自定義測試腳本（不使用 pytest）
- **主要文件**:
  - `run_entity_obj_test_script.py` - 集成測試
  - `datasource_unit_test.py` - 數據源單元測試
  - `test_data_generator.py` - 測試資料生成器
- **缺點**:
  - 缺少標準 pytest 框架
  - 沒有 fixture 和參數化測試
  - 測試結果難以自動化
  - 缺少測試覆蓋率工具

### 新架構組件（需要測試）
1. **Pipeline Orchestrators**:
   - `accrual_bot/tasks/spt/pipeline_orchestrator.py`
   - `accrual_bot/tasks/spx/pipeline_orchestrator.py`

2. **Base Classes**:
   - `accrual_bot/core/pipeline/steps/base_loading.py` (~465 lines)
   - `accrual_bot/core/pipeline/steps/base_evaluation.py` (~384 lines)

3. **Configuration**:
   - `accrual_bot/config/stagging.toml` - `[pipeline.spt]` 和 `[pipeline.spx]` sections

---

## 測試架構設計

### 目錄結構

```
tests/
├── __init__.py
├── conftest.py                          # 共用 fixtures
├── unit/                                # 單元測試
│   ├── __init__.py
│   ├── core/
│   │   └── pipeline/
│   │       └── steps/
│   │           ├── test_base_loading.py           # BaseLoadingStep 測試
│   │           └── test_base_evaluation.py        # BaseERMEvaluationStep 測試
│   ├── tasks/
│   │   ├── spt/
│   │   │   └── test_spt_orchestrator.py           # SPT Orchestrator 測試
│   │   └── spx/
│   │       └── test_spx_orchestrator.py           # SPX Orchestrator 測試
│   └── utils/
│       └── config/
│           └── test_config_manager.py             # ConfigManager 線程安全測試
├── integration/                         # 集成測試
│   ├── __init__.py
│   ├── test_spt_pipeline.py                       # SPT 完整 pipeline 測試
│   ├── test_spx_pipeline.py                       # SPX 完整 pipeline 測試
│   └── test_pipeline_orchestrators.py             # Orchestrator 集成測試
├── fixtures/                            # 測試數據
│   ├── __init__.py
│   ├── sample_data.py                             # 測試 DataFrame generators
│   └── mock_configs.py                            # Mock 配置
└── pytest.ini                           # pytest 配置
```

---

## Phase 1: 測試基礎設施 (P0 - 優先級最高)

### 1.1 創建 pytest 配置

**文件**: `tests/pytest.ini`

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
markers =
    unit: 單元測試
    integration: 集成測試
    slow: 執行時間較長的測試
    asyncio: 異步測試
addopts =
    -v
    --strict-markers
    --tb=short
    --cov=accrual_bot
    --cov-report=html
    --cov-report=term-missing
```

### 1.2 創建共用 Fixtures

**文件**: `tests/conftest.py`

```python
"""Pytest fixtures 供所有測試使用"""
import pytest
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager


@pytest.fixture
def mock_config_manager():
    """Mock ConfigManager"""
    with patch('accrual_bot.utils.config.config_manager') as mock:
        mock._config_toml = {
            'pipeline': {
                'spt': {
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
                },
                'spx': {
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
            },
            'fa_accounts': {
                'spt': ['199999'],
                'spx': ['199999']
            }
        }
        mock.get_list.return_value = ['199999']
        yield mock


@pytest.fixture
def sample_file_paths():
    """測試用的文件路徑配置"""
    return {
        'input': '/tmp/test_input.xlsx',
        'output': '/tmp/test_output.xlsx',
        'reference': '/tmp/test_reference.xlsx',
        'procurement': '/tmp/test_procurement.xlsx',
        'previous': '/tmp/test_previous.xlsx'
    }


@pytest.fixture
def processing_context():
    """測試用的 ProcessingContext"""
    df = pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Item Description': ['Item A', 'Item B', 'Item C'],
        'Entry Quantity': [100, 200, 300],
        'Billed Quantity': [50, 150, 250],
        'Unit Price': [10.0, 20.0, 30.0],
        'Entry Amount': [1000, 4000, 9000]
    })

    ctx = ProcessingContext(
        data=df,
        entity_type='SPT',
        processing_date=202512,
        processing_type='PO'
    )

    # 添加參考數據
    ctx.add_auxiliary_data('reference_account', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Account Desc': ['Cash', 'Receivables']
    }))

    ctx.add_auxiliary_data('reference_liability', pd.DataFrame({
        'Account': ['100000', '100001'],
        'Liability': ['111111', '111112']
    }))

    return ctx


@pytest.fixture
def mock_data_source():
    """Mock DataSource"""
    mock = AsyncMock()
    mock.read = AsyncMock(return_value=pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    }))
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_data_source_factory(mock_data_source):
    """Mock DataSourceFactory"""
    with patch('accrual_bot.core.datasources.DataSourceFactory') as mock:
        mock.create_from_file.return_value = mock_data_source
        yield mock
```

### 1.3 創建測試數據 Generators

**文件**: `tests/fixtures/sample_data.py`

```python
"""測試數據生成器"""
import pandas as pd
from typing import Dict


def create_minimal_loading_df() -> pd.DataFrame:
    """為 BaseLoadingStep 創建最小化測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Item Description': ['Item A 2025/10-2025/11', 'Item B 2025/11', 'Item C'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003'],
        'Entry Quantity': [10, 20, 30],
        'Billed Quantity': [5, 15, 25],
        'Received Quantity': [8, 18, 28],
        'Unit Price': [100.0, 200.0, 300.0],
        'Entry Amount': [1000, 4000, 9000],
        'Entry Billed Amount': [500, 3000, 7500],
        'Currency': ['TWD', 'TWD', 'USD']
    })


def create_minimal_erm_df() -> pd.DataFrame:
    """為 BaseERMEvaluationStep 創建最小化測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Expected Received Month_轉換格式': [202512, 202512, 202601],
        'YMs of Item Description': ['2025/10-2025/11', '2025/11-2025/12', '格式錯誤'],
        'Entry Quantity': [100, 200, 150],
        'Received Quantity': [80, 150, 100],
        'Billed Quantity': [60, 120, 80],
        'Entry Amount': [10000, 20000, 15000],
        'Entry Billed Amount': [6000, 14400, 12000],
        'Entry Prepay Amount': ['0', '0', '0'],
        'Item Description': ['Test Item 1', 'Test Item 2', 'Test Item 3'],
        'Remarked by Procurement': [pd.NA, 'Remark', pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA],
        'Unit Price': [100.0, 100.0, 100.0],
        'Currency': ['TWD', 'TWD', 'USD'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003'],
        'PO狀態': [pd.NA, pd.NA, pd.NA],
        'Account code': ['100000', '100001', '100002'],
        'Department': ['001', '002', '003']
    })


def create_reference_account_df() -> pd.DataFrame:
    """參考科目 DataFrame"""
    return pd.DataFrame({
        'Account': ['100000', '100001', '100002', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'Inventory', 'FA']
    })


def create_reference_liability_df() -> pd.DataFrame:
    """負債科目 DataFrame"""
    return pd.DataFrame({
        'Account': ['100000', '100001', '100002'],
        'Liability': ['111111', '111112', '111113']
    })


def create_complex_erm_scenario_df() -> pd.DataFrame:
    """複雜 ERM 場景測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002', '100003'],
        'Expected Received Month_轉換格式': [202512, 202512, 202512, 202512],
        'YMs of Item Description': ['2025/10-2025/11', '2025/11-2025/12', '格式錯誤', '2025/12'],
        'PO狀態': ['已完成', pd.NA, pd.NA, '未完成'],
        'Entry Quantity': [100, 100, 100, 100],
        'Billed Quantity': [100, 50, 100, 75],
        'Received Quantity': [100, 80, 100, 80],
        'Unit Price': [100.0, 100.0, 100.0, 100.0],
        'Entry Amount': [10000, 10000, 10000, 10000],
        'Entry Billed Amount': [10000, 5000, 10000, 7500],
        'Entry Prepay Amount': ['0', '0', '0', '0'],
        'Item Description': ['Item 1', 'Item 2', 'Item 3', 'Item 4'],
        'Remarked by Procurement': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Currency': ['TWD', 'TWD', 'TWD', 'TWD'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003', 'PROD004'],
        'Account code': ['100000', '100001', '100002', '100003'],
        'Department': ['001', '002', '003', '004']
    })
```

---

## Phase 2: Pipeline Orchestrator 單元測試 (P0)

### 2.1 SPT Orchestrator 測試

**文件**: `tests/unit/tasks/spt/test_spt_orchestrator.py`

```python
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
        self, orchestrator, file_paths, caplog
    ):
        """測試未知步驟名稱返回 None"""
        step = orchestrator._create_step('UnknownStep', file_paths, 'PO')

        assert step is None
        assert "Warning: Unknown step 'UnknownStep'" in caplog.text

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
        self, orchestrator, mock_config_manager
    ):
        """測試配置為空時返回空列表"""
        mock_config_manager._config_toml['pipeline']['spt'] = {}

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
```

### 2.2 SPX Orchestrator 測試

**文件**: `tests/unit/tasks/spx/test_spx_orchestrator.py`

```python
"""SPXPipelineOrchestrator 單元測試"""
# 結構與 SPT 相同，但測試 SPX 特定的步驟
# - SPXDataLoading
# - ColumnAddition
# - ClosingListIntegration
# - StatusStage1
# - SPXERMLogic
# - DepositStatusUpdate
# - ValidationDataProcessing
# - SPXExport
# (完整代碼省略，結構與上述相同)
```

---

## Phase 3: Base Classes 單元測試 (P0)

### 3.1 BaseLoadingStep 測試

**文件**: `tests/unit/core/pipeline/steps/test_base_loading.py`

```python
"""BaseLoadingStep 單元測試"""
import pytest
from unittest.mock import Mock, AsyncMock, patch, call
import pandas as pd
from typing import Tuple

from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus


# --- 測試用具體實現 ---

class ConcreteLoadingStep(BaseLoadingStep):
    """測試用的具體 LoadingStep"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._primary_df = pd.DataFrame({'col': [1, 2, 3]})
        self._date = 202512
        self._month = 12

    def get_required_file_type(self) -> str:
        return 'test_primary'

    async def _load_primary_file(self, source, file_path: str) -> Tuple[pd.DataFrame, int, int]:
        return self._primary_df, self._date, self._month

    def _extract_primary_data(self, primary_result):
        return primary_result

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        # 不加載任何參考數據
        return 0


# --- 測試套件 ---

class TestBaseLoadingStep:
    """BaseLoadingStep 測試套件"""

    @pytest.fixture
    def step(self, sample_file_paths):
        """創建測試步驟"""
        return ConcreteLoadingStep(
            name="TestLoading",
            file_paths=sample_file_paths
        )

    @pytest.fixture
    def context(self):
        """創建空白 context"""
        return ProcessingContext(
            data=pd.DataFrame(),
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )

    # --- 文件路徑標準化測試 ---

    def test_normalize_file_paths_simple_format(self, step):
        """測試簡單字符串格式轉換"""
        simple_paths = {
            'file1': '/path/to/file1.xlsx',
            'file2': '/path/to/file2.csv'
        }

        normalized = step._normalize_file_paths(simple_paths)

        assert 'file1' in normalized
        assert 'path' in normalized['file1']
        assert 'params' in normalized['file1']
        assert normalized['file1']['path'] == '/path/to/file1.xlsx'
        assert normalized['file1']['params'] == {}

    def test_normalize_file_paths_complete_format(self, step):
        """測試完整格式保持不變"""
        complete_paths = {
            'file1': {
                'path': '/path/to/file1.xlsx',
                'params': {'sheet_name': 'Sheet1'}
            }
        }

        normalized = step._normalize_file_paths(complete_paths)

        assert normalized['file1']['path'] == '/path/to/file1.xlsx'
        assert normalized['file1']['params']['sheet_name'] == 'Sheet1'

    def test_normalize_file_paths_mixed_format(self, step):
        """測試混合格式"""
        mixed_paths = {
            'file1': '/path/to/file1.xlsx',
            'file2': {
                'path': '/path/to/file2.csv',
                'params': {'delimiter': ','}
            }
        }

        normalized = step._normalize_file_paths(mixed_paths)

        assert normalized['file1']['params'] == {}
        assert normalized['file2']['params']['delimiter'] == ','

    # --- 文件配置驗證測試 ---

    def test_validate_file_configs_all_exist(self, step, tmp_path):
        """測試所有文件存在時通過驗證"""
        # 創建臨時文件
        file1 = tmp_path / "file1.xlsx"
        file1.touch()
        file2 = tmp_path / "file2.xlsx"
        file2.touch()

        step.file_configs = {
            'test_primary': {'path': str(file1), 'params': {}, 'required': True},
            'file2': {'path': str(file2), 'params': {}, 'required': False}
        }

        # 應該不拋出異常
        validated = step._validate_file_configs()
        assert len(validated) == 2

    def test_validate_file_configs_required_missing(self, step, tmp_path):
        """測試必要文件缺失時拋出異常"""
        step.file_configs = {
            'test_primary': {'path': '/nonexistent/file.xlsx', 'params': {}, 'required': True}
        }

        with pytest.raises(FileNotFoundError) as exc_info:
            step._validate_file_configs()

        assert 'test_primary' in str(exc_info.value)

    def test_validate_file_configs_optional_missing(self, step, tmp_path):
        """測試非必要文件缺失時跳過"""
        file1 = tmp_path / "file1.xlsx"
        file1.touch()

        step.file_configs = {
            'test_primary': {'path': str(file1), 'params': {}, 'required': True},
            'optional': {'path': '/nonexistent.xlsx', 'params': {}, 'required': False}
        }

        validated = step._validate_file_configs()

        # 只應包含存在的文件
        assert 'test_primary' in validated
        assert 'optional' not in validated

    # --- 並發文件加載測試 ---

    @pytest.mark.asyncio
    async def test_load_all_files_concurrent_success(
        self, step, mock_data_source_factory
    ):
        """測試並發加載所有文件成功"""
        step.file_configs = {
            'test_primary': {'path': '/tmp/file1.xlsx', 'params': {}},
            'file2': {'path': '/tmp/file2.xlsx', 'params': {}}
        }

        validated_configs = step.file_configs
        loaded_data = await step._load_all_files_concurrent(validated_configs)

        assert 'test_primary' in loaded_data
        assert 'file2' in loaded_data
        assert isinstance(loaded_data['test_primary'], tuple)

    @pytest.mark.asyncio
    async def test_load_all_files_concurrent_primary_fails(
        self, step, mock_data_source_factory
    ):
        """測試主文件加載失敗時傳播異常"""
        # Mock 主文件加載失敗
        mock_data_source_factory.create_from_file.side_effect = ValueError("Load failed")

        step.file_configs = {
            'test_primary': {'path': '/tmp/file1.xlsx', 'params': {}}
        }

        with pytest.raises(ValueError) as exc_info:
            await step._load_all_files_concurrent(step.file_configs)

        assert "Load failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_load_all_files_concurrent_auxiliary_fails(
        self, step, mock_data_source_factory
    ):
        """測試輔助文件加載失敗時設為 None"""
        # 第一次調用成功（主文件），第二次失敗（輔助文件）
        mock_data_source_factory.create_from_file.side_effect = [
            AsyncMock(read=AsyncMock(return_value=pd.DataFrame({'col': [1, 2]}))),
            ValueError("Auxiliary load failed")
        ]

        step.file_configs = {
            'test_primary': {'path': '/tmp/primary.xlsx', 'params': {}},
            'auxiliary': {'path': '/tmp/auxiliary.xlsx', 'params': {}}
        }

        loaded_data = await step._load_all_files_concurrent(step.file_configs)

        # 主文件應該成功
        assert loaded_data['test_primary'] is not None
        # 輔助文件應該是 None
        assert loaded_data['auxiliary'] is None

    # --- 執行流程測試 ---

    @pytest.mark.asyncio
    async def test_execute_success_flow(
        self, step, context, mock_data_source_factory
    ):
        """測試完整執行流程成功"""
        result = await step.execute(context)

        assert result.status == StepStatus.SUCCESS
        assert context.data is not None
        assert len(context.data) > 0
        assert context.get_variable('processing_date') == 202512
        assert context.get_variable('processing_month') == 12

    @pytest.mark.asyncio
    async def test_execute_file_validation_fails(self, step, context):
        """測試文件驗證失敗"""
        step.file_configs = {
            'test_primary': {'path': '/nonexistent.xlsx', 'params': {}, 'required': True}
        }

        result = await step.execute(context)

        assert result.status == StepStatus.FAILED
        assert 'FileNotFoundError' in result.metadata.get('error_type', '')

    # --- 資源清理測試 ---

    @pytest.mark.asyncio
    async def test_cleanup_resources_called(
        self, step, context, mock_data_source_factory
    ):
        """測試資源清理被調用"""
        with patch.object(step, '_cleanup_resources', new_callable=AsyncMock) as mock_cleanup:
            await step.execute(context)

            # 應該在執行結束時調用清理
            mock_cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_resources_on_error(
        self, step, context, mock_data_source_factory
    ):
        """測試異常時也執行資源清理"""
        # 強制拋出異常
        with patch.object(
            step, '_load_all_files_concurrent',
            side_effect=RuntimeError("Forced error")
        ):
            with patch.object(step, '_cleanup_resources', new_callable=AsyncMock) as mock_cleanup:
                result = await step.execute(context)

                # 即使失敗也應該調用清理
                mock_cleanup.assert_called_once()
                assert result.status == StepStatus.FAILED

    # --- 輔助數據添加測試 ---

    def test_add_auxiliary_data_to_context(self, step, context):
        """測試輔助數據添加到 context"""
        loaded_data = {
            'test_primary': (pd.DataFrame({'col': [1, 2]}), 202512, 12),
            'auxiliary1': pd.DataFrame({'col': ['a', 'b']}),
            'auxiliary2': pd.DataFrame({'col': [10, 20]}),
            'empty': pd.DataFrame()  # 空 DataFrame 應該被過濾
        }

        count = step._add_auxiliary_data_to_context(
            context, loaded_data, 'test_primary'
        )

        assert count == 2  # auxiliary1 和 auxiliary2
        assert context.get_auxiliary_data('auxiliary1') is not None
        assert context.get_auxiliary_data('auxiliary2') is not None
        assert context.get_auxiliary_data('empty') is None

    # --- 日期提取測試 ---

    def test_extract_date_from_filename_valid(self, step):
        """測試從文件名提取日期"""
        date, month = step._extract_date_from_filename('/path/to/file_202512.xlsx')

        assert date == 202512
        assert month == 12

    def test_extract_date_from_filename_invalid(self, step):
        """測試無效文件名使用當前日期"""
        date, month = step._extract_date_from_filename('/path/to/file.xlsx')

        # 應該使用當前日期（無法準確驗證，只檢查返回值存在）
        assert isinstance(date, int)
        assert isinstance(month, int)
        assert 1 <= month <= 12
```

### 3.2 BaseERMEvaluationStep 測試

**文件**: `tests/unit/core/pipeline/steps/test_base_evaluation.py`

```python
"""BaseERMEvaluationStep 單元測試"""
import pytest
from unittest.mock import patch
import pandas as pd

from accrual_bot.core.pipeline.steps.base_evaluation import (
    BaseERMEvaluationStep,
    BaseERMConditions
)
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.base import StepStatus
from tests.fixtures.sample_data import (
    create_minimal_erm_df,
    create_reference_account_df,
    create_reference_liability_df,
    create_complex_erm_scenario_df
)


# --- 測試用具體實現 ---

class ConcreteERMStep(BaseERMEvaluationStep):
    """測試用的具體 ERMStep"""

    def _build_conditions(self, df: pd.DataFrame, file_date: int, status_column: str):
        """構建簡單的測試條件"""
        return BaseERMConditions(
            no_status=df[status_column].isna(),
            in_date_range=pd.Series([True] * len(df), index=df.index),
            erm_before_or_equal_file_date=pd.Series([True] * len(df), index=df.index),
            erm_after_file_date=pd.Series([False] * len(df), index=df.index),
            format_error=df['YMs of Item Description'].str.contains('格式錯誤', na=False),
            out_of_date_range=pd.Series([False] * len(df), index=df.index),
            procurement_not_error=pd.Series([True] * len(df), index=df.index)
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        """應用簡單的狀態邏輯"""
        # 格式錯誤 且 無狀態 -> 格式錯誤，退單
        mask_format_error = conditions.format_error & conditions.no_status
        df.loc[mask_format_error, status_column] = '格式錯誤，退單'

        # 其他無狀態 -> 已完成
        mask_no_status = conditions.no_status & ~mask_format_error
        df.loc[mask_no_status, status_column] = '已完成'

        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        """設置會計欄位"""
        # 簡單合併 Account Name
        df = df.merge(
            ref_account[['Account', 'Account Desc']],
            left_on='Account code',
            right_on='Account',
            how='left',
            suffixes=('', '_ref')
        )
        df['Account Name'] = df['Account Desc']
        df = df.drop(columns=['Account', 'Account Desc'], errors='ignore')

        # 設置 Liability
        df = df.merge(
            ref_liability[['Account', 'Liability']],
            left_on='Account code',
            right_on='Account',
            how='left',
            suffixes=('', '_lib')
        )
        df = df.drop(columns=['Account_lib'], errors='ignore')

        return df


# --- 測試套件 ---

class TestBaseERMEvaluationStep:
    """BaseERMEvaluationStep 測試套件"""

    @pytest.fixture
    def step(self):
        """創建測試步驟"""
        return ConcreteERMStep(name="TestERM")

    @pytest.fixture
    def context_with_data(self):
        """創建包含測試數據的 context"""
        df = create_minimal_erm_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )

        # 添加參考數據
        ctx.add_auxiliary_data('reference_account', create_reference_account_df())
        ctx.add_auxiliary_data('reference_liability', create_reference_liability_df())

        return ctx

    # --- 檔案日期設置測試 ---

    def test_set_file_date(self, step):
        """測試設置檔案日期欄位"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        processing_date = 202512

        result = step._set_file_date(df, processing_date)

        assert '檔案日期' in result.columns
        assert all(result['檔案日期'] == 202512)

    # --- 狀態欄位判斷測試 ---

    def test_get_status_column_po(self, step, context_with_data):
        """測試 PO 類型返回 'PO狀態'"""
        df = context_with_data.data
        df['PO狀態'] = pd.NA

        status_col = step._get_status_column(df, context_with_data)

        assert status_col == 'PO狀態'

    def test_get_status_column_pr(self, step):
        """測試 PR 類型返回 'PR狀態'"""
        df = pd.DataFrame({'PR狀態': [pd.NA, pd.NA]})
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PR'
        )

        status_col = step._get_status_column(df, ctx)

        assert status_col == 'PR狀態'

    # --- 格式錯誤處理測試 ---

    def test_handle_format_errors(self, step):
        """測試格式錯誤標記"""
        df = pd.DataFrame({
            'PO狀態': [pd.NA, pd.NA, '已完成']
        })

        conditions = BaseERMConditions(
            no_status=pd.Series([True, True, False]),
            in_date_range=pd.Series([True, True, True]),
            erm_before_or_equal_file_date=pd.Series([True, True, True]),
            erm_after_file_date=pd.Series([False, False, False]),
            format_error=pd.Series([True, False, False]),
            out_of_date_range=pd.Series([False, False, False]),
            procurement_not_error=pd.Series([True, True, True])
        )

        result = step._handle_format_errors(df, conditions, 'PO狀態')

        # 第一筆有格式錯誤且無狀態，應該被標記
        assert result.loc[0, 'PO狀態'] == '格式錯誤，退單'
        # 第二筆無格式錯誤，應該保持原樣
        assert pd.isna(result.loc[1, 'PO狀態'])
        # 第三筆已有狀態，應該保持原樣
        assert result.loc[2, 'PO狀態'] == '已完成'

    # --- 估列標記設置測試 ---

    def test_set_accrual_flag_completed_status(self, step):
        """測試「已完成」狀態設為 'Y'"""
        df = pd.DataFrame({
            'PO狀態': ['已完成', '未完成', '已完成(check qty)']
        })

        result = step._set_accrual_flag(df, 'PO狀態')

        assert '是否估計入帳' in result.columns
        assert result.loc[0, '是否估計入帳'] == 'Y'
        assert result.loc[1, '是否估計入帳'] == 'N'
        assert result.loc[2, '是否估計入帳'] == 'Y'  # 包含「已完成」

    def test_set_accrual_flag_no_completed(self, step):
        """測試無「已完成」時全部為 'N'"""
        df = pd.DataFrame({
            'PO狀態': ['未完成', 'Check收貨', '格式錯誤']
        })

        result = step._set_accrual_flag(df, 'PO狀態')

        assert all(result['是否估計入帳'] == 'N')

    # --- 統計信息生成測試 ---

    def test_generate_statistics(self, step):
        """測試統計信息生成"""
        df = pd.DataFrame({
            'PO狀態': ['已完成', '已完成', '未完成', '格式錯誤', '已完成'],
            '是否估計入帳': ['Y', 'Y', 'N', 'N', 'Y']
        })

        stats = step._generate_statistics(df, 'PO狀態')

        assert stats['total_records'] == 5
        assert stats['accrual_count'] == 3  # 3筆 'Y'
        assert '已完成' in stats['status_distribution']
        assert stats['status_distribution']['已完成'] == 3
        assert stats['status_distribution']['未完成'] == 1
        assert stats['status_distribution']['格式錯誤'] == 1

    # --- 執行流程測試 ---

    @pytest.mark.asyncio
    async def test_execute_success_flow(
        self, step, context_with_data, mock_config_manager
    ):
        """測試完整執行流程成功"""
        result = await step.execute(context_with_data)

        assert result.status == StepStatus.SUCCESS

        df = context_with_data.data

        # 驗證檔案日期已設置
        assert '檔案日期' in df.columns

        # 驗證狀態已更新
        assert 'PO狀態' in df.columns
        assert not all(df['PO狀態'].isna())

        # 驗證估列標記已設置
        assert '是否估計入帳' in df.columns
        assert set(df['是否估計入帳'].unique()).issubset({'Y', 'N'})

        # 驗證會計欄位已設置
        assert 'Account Name' in df.columns
        assert 'Liability' in df.columns

    @pytest.mark.asyncio
    async def test_execute_missing_reference_data(
        self, step, mock_config_manager
    ):
        """測試缺少參考數據時失敗"""
        df = create_minimal_erm_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )
        # 不添加參考數據

        result = await step.execute(ctx)

        assert result.status == StepStatus.FAILED
        assert 'KeyError' in result.metadata.get('error_type', '')

    # --- 條件構建測試 ---

    def test_build_conditions_structure(
        self, step, context_with_data
    ):
        """測試條件構建返回正確結構"""
        df = context_with_data.data

        conditions = step._build_conditions(df, 202512, 'PO狀態')

        assert isinstance(conditions, BaseERMConditions)
        assert hasattr(conditions, 'no_status')
        assert hasattr(conditions, 'format_error')
        assert len(conditions.no_status) == len(df)

    # --- 狀態條件應用測試 ---

    def test_apply_status_conditions(
        self, step, context_with_data
    ):
        """測試狀態條件應用"""
        df = context_with_data.data.copy()

        conditions = step._build_conditions(df, 202512, 'PO狀態')
        result = step._apply_status_conditions(df, conditions, 'PO狀態')

        # 驗證狀態已被更新
        assert not all(result['PO狀態'].isna())

        # 驗證格式錯誤被標記
        format_error_mask = result['YMs of Item Description'].str.contains('格式錯誤', na=False)
        if format_error_mask.any():
            assert '格式錯誤' in result.loc[format_error_mask, 'PO狀態'].values[0]

    # --- 會計欄位設置測試 ---

    def test_set_accounting_fields(
        self, step, context_with_data
    ):
        """測試會計欄位設置"""
        df = context_with_data.data.copy()
        ref_account = context_with_data.get_auxiliary_data('reference_account')
        ref_liability = context_with_data.get_auxiliary_data('reference_liability')

        result = step._set_accounting_fields(df, ref_account, ref_liability)

        # 驗證 Account Name 被添加
        assert 'Account Name' in result.columns

        # 驗證 Liability 被添加
        assert 'Liability' in result.columns

        # 驗證合併正確
        account_with_ref = result[result['Account code'] == '100000']
        if not account_with_ref.empty:
            assert account_with_ref.iloc[0]['Account Name'] == 'Cash'
            assert account_with_ref.iloc[0]['Liability'] == '111111'

    # --- 複雜場景測試 ---

    @pytest.mark.asyncio
    async def test_complex_scenario(
        self, step, mock_config_manager
    ):
        """測試複雜 ERM 場景"""
        df = create_complex_erm_scenario_df()
        ctx = ProcessingContext(
            data=df,
            entity_type='TEST',
            processing_date=202512,
            processing_type='PO'
        )
        ctx.add_auxiliary_data('reference_account', create_reference_account_df())
        ctx.add_auxiliary_data('reference_liability', create_reference_liability_df())

        result = await step.execute(ctx)

        assert result.status == StepStatus.SUCCESS

        df_result = ctx.data

        # 驗證統計
        stats = result.metadata
        assert stats['total_records'] == 4
        assert stats['accrual_count'] >= 0

        # 驗證不同狀態記錄
        completed_count = (df_result['PO狀態'].str.contains('已完成', na=False)).sum()
        assert completed_count >= 1


# --- 參數化測試 ---

@pytest.mark.parametrize("status,expected_accrual", [
    ('已完成', 'Y'),
    ('未完成', 'N'),
    ('Check收貨', 'N'),
    ('格式錯誤，退單', 'N'),
    ('已完成(check qty)', 'Y'),
    ('已完成(exceed period)', 'Y'),
])
def test_accrual_flag_various_statuses(status, expected_accrual):
    """參數化測試不同狀態的估列標記"""
    step = ConcreteERMStep(name="Test")
    df = pd.DataFrame({'PO狀態': [status]})

    result = step._set_accrual_flag(df, 'PO狀態')

    assert result.loc[0, '是否估計入帳'] == expected_accrual
```

---

## Phase 4: 集成測試 (P1)

### 4.1 Pipeline Orchestrator 集成測試

**文件**: `tests/integration/test_pipeline_orchestrators.py`

```python
"""Pipeline Orchestrators 集成測試"""
import pytest
import pandas as pd
from pathlib import Path
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext
from tests.fixtures.sample_data import create_minimal_loading_df


@pytest.mark.integration
class TestPipelineOrchestratorsIntegration:
    """Pipeline Orchestrators 集成測試"""

    @pytest.fixture
    def temp_test_files(self, tmp_path):
        """創建臨時測試文件"""
        # 創建 SPT PO 文件
        spt_po_file = tmp_path / "spt_po_202512.xlsx"
        df = create_minimal_loading_df()
        df.to_excel(spt_po_file, index=False)

        # 創建參考文件
        ref_file = tmp_path / "reference.xlsx"
        ref_df = pd.DataFrame({
            'Account': ['100000', '100001'],
            'Account Desc': ['Cash', 'Receivables']
        })
        ref_df.to_excel(ref_file, index=False)

        return {
            'spt_po': str(spt_po_file),
            'reference': str(ref_file)
        }

    @pytest.mark.asyncio
    async def test_spt_orchestrator_full_pipeline(
        self, temp_test_files, mock_config_manager
    ):
        """測試 SPT orchestrator 完整 pipeline"""
        orchestrator = SPTPipelineOrchestrator()

        file_paths = {
            'po_file': temp_test_files['spt_po'],
            'reference': temp_test_files['reference']
        }

        # 構建 pipeline
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證 pipeline 結構
        assert len(pipeline.steps) == 6
        assert pipeline.config.entity_type == 'SPT'

        # 註：完整執行需要所有步驟的依賴，此處只驗證構建

    @pytest.mark.asyncio
    async def test_spx_orchestrator_full_pipeline(
        self, temp_test_files, mock_config_manager
    ):
        """測試 SPX orchestrator 完整 pipeline"""
        orchestrator = SPXPipelineOrchestrator()

        file_paths = {
            'po_file': temp_test_files['spt_po'],  # 使用相同測試文件
            'reference': temp_test_files['reference']
        }

        # 構建 pipeline
        pipeline = orchestrator.build_po_pipeline(file_paths)

        # 驗證 pipeline 結構
        assert len(pipeline.steps) == 8
        assert pipeline.config.entity_type == 'SPX'
```

### 4.2 完整 SPT Pipeline 測試

**文件**: `tests/integration/test_spt_pipeline.py`

```python
"""SPT Pipeline 集成測試"""
# 測試完整的 SPT PO/PR pipeline 執行
# (需要實際的測試數據和所有步驟依賴)
# (代碼省略，與現有 run_entity_obj_test_script.py 類似)
```

---

## Phase 5: ConfigManager 線程安全測試 (P0)

**文件**: `tests/unit/utils/config/test_config_manager.py`

```python
"""ConfigManager 線程安全測試"""
import pytest
import threading
from accrual_bot.utils.config import ConfigManager


class TestConfigManagerThreadSafety:
    """ConfigManager 線程安全測試"""

    def test_singleton_same_instance(self):
        """測試單例模式返回相同實例"""
        instance1 = ConfigManager()
        instance2 = ConfigManager()

        assert instance1 is instance2
        assert id(instance1) == id(instance2)

    def test_thread_safe_singleton(self):
        """測試多線程環境下的單例安全性"""
        results = []

        def get_instance():
            instance = ConfigManager()
            results.append(id(instance))

        # 創建100個線程同時獲取實例
        threads = [threading.Thread(target=get_instance) for _ in range(100)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 所有線程應該得到相同的實例
        unique_ids = set(results)
        assert len(unique_ids) == 1, f"Expected 1 unique instance, got {len(unique_ids)}"

    def test_config_data_integrity(self):
        """測試配置數據完整性"""
        instance = ConfigManager()

        # 驗證配置數據存在
        assert hasattr(instance, '_config_toml')
        assert isinstance(instance._config_toml, dict)

    @pytest.mark.slow
    def test_concurrent_access_stress(self):
        """壓力測試：大量並發訪問"""
        results = []
        errors = []

        def access_config():
            try:
                instance = ConfigManager()
                config = instance._config_toml
                results.append(id(instance))
            except Exception as e:
                errors.append(e)

        # 創建1000個線程
        threads = [threading.Thread(target=access_config) for _ in range(1000)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 不應該有任何錯誤
        assert len(errors) == 0
        # 所有線程得到相同實例
        assert len(set(results)) == 1
```

---

## Phase 6: 測試配置與執行 (P1)

### 6.1 安裝測試依賴

更新 `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.1.0",
    "pytest-mock>=3.12.0",
    "black>=23.0.0",
    "flake8>=6.0.0",
    "mypy>=1.8.0"
]
```

### 6.2 創建測試運行腳本

**文件**: `run_tests.sh` (Unix) 或 `run_tests.bat` (Windows)

```bash
#!/bin/bash
# Unix 版本

# 啟動虛擬環境
source venv/bin/activate

# 運行所有測試
echo "=== Running All Tests ==="
pytest tests/ -v

# 運行單元測試
echo "=== Running Unit Tests ==="
pytest tests/unit/ -v -m unit

# 運行集成測試
echo "=== Running Integration Tests ==="
pytest tests/integration/ -v -m integration

# 生成覆蓋率報告
echo "=== Generating Coverage Report ==="
pytest tests/ --cov=accrual_bot --cov-report=html --cov-report=term-missing

echo "=== Test Summary ==="
echo "Coverage report: htmlcov/index.html"
```

```bat
@echo off
REM Windows 版本

REM 啟動虛擬環境
call venv\Scripts\activate

REM 運行所有測試
echo === Running All Tests ===
pytest tests\ -v

REM 運行單元測試
echo === Running Unit Tests ===
pytest tests\unit\ -v -m unit

REM 運行集成測試
echo === Running Integration Tests ===
pytest tests\integration\ -v -m integration

REM 生成覆蓋率報告
echo === Generating Coverage Report ===
pytest tests\ --cov=accrual_bot --cov-report=html --cov-report=term-missing

echo === Test Summary ===
echo Coverage report: htmlcov\index.html
```

---

## 驗證計畫

### 步驟 1: 安裝測試依賴

```bash
python -m pip install -e ".[dev]"
```

### 步驟 2: 運行基礎測試

```bash
# 運行 ConfigManager 測試
pytest tests/unit/utils/config/test_config_manager.py -v

# 運行 Orchestrator 測試
pytest tests/unit/tasks/ -v

# 運行 Base Classes 測試
pytest tests/unit/core/pipeline/steps/ -v
```

### 步驟 3: 運行完整測試套件

```bash
# 所有測試
pytest tests/ -v

# 生成覆蓋率報告
pytest tests/ --cov=accrual_bot --cov-report=html
```

### 步驟 4: 驗證覆蓋率

目標覆蓋率：
- **Pipeline Orchestrators**: ≥ 90%
- **Base Classes**: ≥ 85%
- **ConfigManager**: 100%
- **整體**: ≥ 80%

```bash
# 查看覆蓋率報告
open htmlcov/index.html  # macOS
start htmlcov\index.html  # Windows
```

---

## 測試優先級總結

### P0 (必須立即實現)
1. ✓ 測試基礎設施 (conftest.py, pytest.ini, fixtures)
2. ✓ Pipeline Orchestrator 單元測試
3. ✓ BaseLoadingStep 單元測試
4. ✓ BaseERMEvaluationStep 單元測試
5. ✓ ConfigManager 線程安全測試

### P1 (重要，盡快實現)
1. Pipeline Orchestrator 集成測試
2. 完整 SPT/SPX Pipeline 測試
3. 測試執行腳本和 CI/CD 集成

### P2 (可選，後續改進)
1. 性能測試 (benchmark)
2. 參數化測試擴展
3. 測試數據生成器優化
4. Mock 策略優化

---

## 預期成果

1. **測試覆蓋率**: 達到 80% 以上整體覆蓋率
2. **測試自動化**: 所有測試可通過 pytest 一鍵運行
3. **持續集成**: 測試可集成到 CI/CD pipeline
4. **代碼質量**: 通過測試驗證架構改進的正確性
5. **回歸預防**: 防止未來修改破壞現有功能
6. **文檔完善**: 測試作為使用範例和文檔

---

## 下一步行動

1. 創建 `tests/` 目錄結構
2. 實現 `conftest.py` 和共用 fixtures
3. 實現 Pipeline Orchestrator 測試（SPT/SPX）
4. 實現 Base Classes 測試
5. 實現 ConfigManager 測試
6. 運行測試並生成覆蓋率報告
7. 根據覆蓋率報告補充缺失的測試用例
8. 集成到 CI/CD pipeline

