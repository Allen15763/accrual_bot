# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Accrual Bot is an async data processing system for PO/PR (Purchase Order/Purchase Request) accrual processing. It handles monthly financial data reconciliation for two active business entities: SPT and SPX. The system includes both a command-line pipeline execution mode and a Streamlit-based Web UI.

## Common Commands

```bash
# Activate virtual environment (Windows PowerShell)
./venv/Scripts/activate

# Install dependencies
python -m pip install -r requirements.txt
# Or use pyproject.toml
python -m pip install .
# With UI dependencies
python -m pip install ".[ui]"

# Run the main pipeline (CLI mode)
python main_pipeline.py

# Run Streamlit UI
streamlit run main_streamlit.py

# Run tests
python -m pytest tests/

# Run tests by category
python -m pytest tests/ -m unit          # Unit tests only
python -m pytest tests/ -m integration   # Integration tests only

# Run with coverage
python -m pytest tests/ --cov=accrual_bot --cov-report=html

# Type checking
python -m mypy accrual_bot/

# Code formatting
python -m black .
```

## Architecture

### Four-Layer Architecture

The codebase follows a four-layer architecture pattern:

```
┌─────────────────────────────────────────────────────────────┐
│                    UI Layer (Streamlit)                      │
│  pages/ → components/ → services/ → Session State           │
├─────────────────────────────────────────────────────────────┤
│                    Tasks Layer (Orchestrators)               │
│  tasks/spt/ | tasks/spx/ | tasks/common/                     │
├─────────────────────────────────────────────────────────────┤
│                    Core Layer (Framework)                    │
│  Pipeline | PipelineStep | ProcessingContext | DataSources  │
├─────────────────────────────────────────────────────────────┤
│                    Utils Layer (Cross-cutting)               │
│  ConfigManager | Logger | Data Utilities                    │
└─────────────────────────────────────────────────────────────┘
```

- **ui/**: Streamlit Web UI (see [UI Architecture](#streamlit-ui-architecture) below)
  - `pages/`: 5-page workflow (Configuration → Upload → Execution → Results → Checkpoint)
  - `components/`: Reusable UI components
  - `services/`: UnifiedPipelineService, StreamlitPipelineRunner, FileHandler
  - `models/`: Session state dataclasses

- **core/**: Framework and reusable components
  - Pipeline infrastructure (Pipeline, PipelineStep, ProcessingContext)
  - Base classes with template method pattern (BaseLoadingStep, BaseERMEvaluationStep)
  - Common steps shared across entities
  - Data sources abstraction layer

- **tasks/**: Entity-specific implementations
  - `tasks/spt/`: SPT-specific steps and pipeline orchestrator (PO/PR/PROCUREMENT)
  - `tasks/spx/`: SPX-specific steps and pipeline orchestrator (PO/PR/PPE/PPE_DESC)
  - `tasks/common/`: Shared task steps (DataShapeSummaryStep)
  - Each task module contains entity-specific business logic

- **utils/**: Cross-cutting concerns
  - Thread-safe ConfigManager singleton
  - Unified logging framework
  - Data utilities

### Pipeline System (core/pipeline/)

The application uses a step-based async pipeline architecture with template method pattern:

- **PipelineStep**: Abstract base class for all processing steps. Each step implements `execute()` and `validate_input()` methods.
- **BaseLoadingStep**: Template base class for data loading steps (~593 lines of shared logic):
  - Concrete methods: `_normalize_file_paths()`, `_load_all_files_concurrent()`, `_validate_file_configs()`
  - Abstract hooks: `get_required_file_type()`, `_load_primary_file()`, `_load_reference_data()`
- **BaseERMEvaluationStep**: Template base class for ERM evaluation steps (~518 lines of shared logic):
  - Concrete methods: `_set_file_date()`, `_get_status_column()`, `_set_accrual_flag()`, `_generate_statistics()`
  - Abstract hooks: `_build_conditions()`, `_apply_status_conditions()`, `_set_accounting_fields()`
- **ProcessingContext**: Carries data and state between pipeline steps. Contains main DataFrame, auxiliary data, variables, and execution history.
- **PipelineBuilder**: Fluent interface for constructing pipelines with chained `.add_step()` calls.
- **CheckpointManager**: Enables saving/resuming pipeline execution from specific steps.

Pipeline execution flow:
```
DataLoading → Filtering → ColumnAddition → Integration → BusinessLogic → PostProcessing → Export
```

### Entity-Specific Processing

Two active entity types with configuration-driven pipeline orchestration (config split into per-entity TOML files):

- **SPT** ([tasks/spt/](accrual_bot/tasks/spt/)):
  - PO Pipeline: SPTDataLoading → ProductFilter → ColumnAddition → APInvoiceIntegration → PreviousWorkpaperIntegration → ProcurementIntegration → CommissionDataUpdate → PayrollDetection → DateLogic → SPTERMLogic → SPTStatusLabel → SPTAccountPrediction → SPTPostProcessing → SPTExport → DataShapeSummary
  - PR Pipeline: SPTPRDataLoading → ProductFilter → ColumnAddition → PreviousWorkpaperIntegration → ProcurementIntegration → CommissionDataUpdate → PayrollDetection → DateLogic → SPXPRERMLogic → SPTStatusLabel → SPTAccountPrediction → SPTPostProcessing → SPTExport → DataShapeSummary
  - PROCUREMENT Pipeline (PO/PR/COMBINED):
    - PO: SPTProcurementDataLoading → ColumnInitialization → ProcurementPreviousMapping → DateLogic → SPTProcurementStatusEvaluation → SPTProcurementExport
    - PR: SPTProcurementPRDataLoading → ColumnInitialization → ProcurementPreviousMapping → DateLogic → SPTProcurementStatusEvaluation → SPTProcurementExport
    - COMBINED: CombinedProcurementDataLoading → ProcurementPreviousValidation → CombinedProcurementProcessing → CombinedProcurementExport
  - Advanced features: commission handling, payroll detection, account prediction rules, media data integration

- **SPX** ([tasks/spx/](accrual_bot/tasks/spx/)):
  - PO Pipeline: SPXDataLoading → ProductFilter → ColumnAddition → APInvoiceIntegration → PreviousWorkpaperIntegration → ProcurementIntegration → DateLogic → ClosingListIntegration → StatusStage1 → SPXERMLogic → ValidationDataProcessing → DepositStatusUpdate → DataReformatting → SPXExport → DataShapeSummary
  - PR Pipeline: SPXPRDataLoading → ProductFilter → ColumnAddition → PreviousWorkpaperIntegration → ProcurementIntegration → DateLogic → ClosingListIntegration → StatusStage1 → SPXPRERMLogic → PRDataReformatting → SPXPRExport → DataShapeSummary
  - PPE Pipeline: PPEDataLoading → PPEDataCleaning → PPEDataMerge → PPEContractDateUpdate → PPEMonthDifference
  - PPE_DESC Pipeline: PPEDescDataLoading → DescriptionExtraction → ContractPeriodMapping → PPEDescExport
  - Complex processing: config-driven condition engine (SPXConditionEngine), deposit/rental identification, locker/kiosk asset validation

Pipeline steps can be enabled/disabled via configuration in TOML files:
```toml
# config/stagging_spt.toml
[pipeline.spt]
enabled_po_steps = ["SPTDataLoading", "ProductFilter", "ColumnAddition",
    "APInvoiceIntegration", "PreviousWorkpaperIntegration", "ProcurementIntegration",
    "CommissionDataUpdate", "PayrollDetection", "DateLogic", "SPTERMLogic",
    "SPTStatusLabel", "SPTAccountPrediction", "SPTPostProcessing", "SPTExport",
    "DataShapeSummary"]
enabled_procurement_po_steps = ["SPTProcurementDataLoading", "ColumnInitialization",
    "ProcurementPreviousMapping", "DateLogic", "SPTProcurementStatusEvaluation",
    "SPTProcurementExport"]
enabled_procurement_combined_steps = ["CombinedProcurementDataLoading", ...]

# config/stagging_spx.toml
[pipeline.spx]
enabled_po_steps = ["SPXDataLoading", "ProductFilter", "ColumnAddition",
    "APInvoiceIntegration", "PreviousWorkpaperIntegration", "ProcurementIntegration",
    "DateLogic", "ClosingListIntegration", "StatusStage1", "SPXERMLogic",
    "ValidationDataProcessing", "DepositStatusUpdate", "DataReformatting",
    "SPXExport", "DataShapeSummary"]
enabled_pr_steps = ["SPXPRDataLoading", "ProductFilter", "ColumnAddition", ...]
```

### Data Sources (core/datasources/)

Unified async data access layer with `DataSource` abstract base class, `DataSourceFactory` factory, and `DataSourcePool` connection pooling:

- **ExcelSource** — `.xlsx`/`.xls`（支援 sheet_name, header, usecols, dtype）
- **CSVSource** — `.csv`（支援 encoding, sep, dtype）
- **ParquetSource** — `.parquet`（Checkpoint 儲存用，型別安全）
- **DuckDBSource** — DuckDB 記憶體/檔案 DB（SQL 查詢）
- **GoogleSheetsSource** — Google Sheets 統一數據源（optional dependency，需 `gspread`）
  - 整合 `GoogleSheetsImporter`（accrual_bot）和 `GoogleSheetsManager`（spe_bank_recon）
  - DataSource 介面（async read/write）+ 多試算表並發讀取 + 工作表管理（create/delete/recreate）
  - Service Account JSON 認證 + ZIP fallback（離線環境）
  - 向後兼容別名 `GoogleSheetsManager`

All sources implement the same `DataSource` interface with thread-safe operations, shared thread pools, and LRU caching (TTL 300s, max 10 items).

### Configuration

Six configuration files:

- **config/config.ini**: Legacy INI configuration (general settings, regex patterns, credentials)

- **config/paths.toml**: File paths and read parameters per entity/type:
  ```toml
  [spx.po]
  raw_po = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv"
  previous = "{resources}/{YYYYMM}/前期底稿/SPX/{PREV_YYYYMM}_PO_FN.xlsx"

  [spx.po.params]
  raw_po = { encoding = "utf-8", sep = ",", dtype = "str" }
  previous = { sheet_name = 0, header = 0, dtype = "str" }
  ops_validation = { sheet_name = "智取櫃驗收明細", header = 3, usecols = "A:AH" }
  ```

- **config/stagging.toml**: Shared TOML configuration containing:
  - File paths and reference paths
  - Date regex patterns
  - Data shape summary configuration
  - Category patterns by description (shared keyword-to-category mappings)
  - Shared settings across entities

- **config/stagging_spt.toml**: SPT-specific configuration:
  - Pipeline configuration (enabled steps for PO/PR/PROCUREMENT)
  - SPT status label rules and account prediction rules
  - SPT pivot configurations

- **config/stagging_spx.toml**: SPX-specific configuration:
  - Pipeline configuration (enabled steps for PO/PR)
  - SPX column mappings and business rules (ap_columns, deposit_keywords, etc.)
  - SPX pivot configurations (po/pr_pivot_index, cr_pivot_cols)
  - Supplier lists (bao, kiosk, locker suppliers)
  - SPX condition engine rules (spx_status_stage1_rules, spx_erm_status_rules)

- **config/run_config.toml**: Runtime configuration for pipeline execution

Configuration is accessed via **thread-safe singleton** `ConfigManager` from `accrual_bot.utils.config`:
- Implements double-checked locking pattern with `threading.Lock()` to prevent race conditions
- Safe to use across multiple threads and async contexts
- Automatically loads configuration on first access

All pipeline modules use the unified `get_logger()` function from `accrual_bot.utils.logging` for consistent log formatting.

## Streamlit UI Architecture

The UI provides a 5-page guided workflow for pipeline execution:

```
Page 1: Configuration    → Select Entity (SPT/SPX), Type (PO/PR/PPE), Date
Page 2: File Upload      → Upload required/optional files with validation
Page 3: Execution        → Run pipeline with progress tracking and logs
Page 4: Results          → Preview data, download CSV/Excel
Page 5: Checkpoint       → Manage saved pipeline states
```

### UI Directory Structure

```
accrual_bot/ui/
├── app.py                      # Session state initialization
├── config.py                   # UI configuration (ENTITY_CONFIG, REQUIRED_FILES, etc.)
├── models/state_models.py      # Dataclass state models
├── components/                 # Reusable UI components
│   ├── entity_selector.py      # Entity/Type/Date selection
│   ├── file_uploader.py        # Dynamic file upload
│   ├── progress_tracker.py     # Execution progress
│   ├── step_preview.py         # Pipeline step preview
│   └── data_preview.py         # Result preview
├── services/                   # Service layer
│   ├── unified_pipeline_service.py  # Pipeline service (KEY)
│   ├── pipeline_runner.py      # Async execution wrapper
│   └── file_handler.py         # File management
├── pages/                      # Streamlit pages
│   ├── 1_configuration.py
│   ├── 2_file_upload.py
│   ├── 3_execution.py
│   ├── 4_results.py
│   └── 5_checkpoint.py
└── utils/
    ├── async_bridge.py         # Sync/Async bridge for Streamlit
    └── ui_helpers.py           # Formatting utilities
```

### Key UI Service: UnifiedPipelineService

The service layer bridges UI and backend pipelines:

```python
from accrual_bot.ui.services import UnifiedPipelineService

service = UnifiedPipelineService()

# Query available entities and types
entities = service.get_available_entities()      # ['SPT', 'SPX']
types = service.get_entity_types('SPX')          # ['PO', 'PR', 'PPE']
steps = service.get_enabled_steps('SPX', 'PO')   # ['SPXDataLoading', ...]

# Build and execute pipeline
pipeline = service.build_pipeline(
    entity='SPX',
    proc_type='PO',
    file_paths={'raw_po': '/path/to/file.csv', ...},
    processing_date=202512
)
```

### UI Configuration (ui/config.py)

```python
# Entity configuration
ENTITY_CONFIG = {
    'SPT': {
        'display_name': 'SPT',
        'types': ['PO', 'PR', 'PROCUREMENT'],
        'icon': '🛒',
    },
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE', 'PPE_DESC'],
        'icon': '📦',
    },
}

# PROCUREMENT sub-types
PROCUREMENT_SOURCE_TYPES = {'PO': ..., 'PR': ...}  # COMBINED hidden for now

# Required files per entity/type (supports 2-tuple and 3-tuple keys)
REQUIRED_FILES = {
    ('SPT', 'PO'): ['raw_po'],
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PPE'): ['contract_filing_list'],
    ('SPX', 'PPE_DESC'): ['workpaper', 'contract_periods'],
    ('SPT', 'PROCUREMENT', 'PO'): ['raw_po'],
    ...
}

# Optional files per entity/type
OPTIONAL_FILES = {
    ('SPT', 'PO'): ['previous', 'procurement_po', 'ap_invoice', 'previous_pr',
                     'procurement_pr', 'media_finished', 'media_left', 'media_summary'],
    ('SPX', 'PO'): ['previous', 'procurement_po', 'ap_invoice', 'previous_pr',
                     'procurement_pr', 'ops_validation'],
    ('SPT', 'PROCUREMENT', 'PO'): ['procurement_previous', 'media_finished',
                                    'media_left', 'media_summary'],
    ...
}
```

### Dual-Layer Pages Architecture

To overcome Streamlit's emoji filename limitation, the project uses a dual-layer pages architecture:

```
Project Root/
├── pages/                          # Streamlit Entry Points (emoji filenames)
│   ├── 1_⚙️_配置.py                 # Entry point (17 lines)
│   ├── 2_📁_檔案上傳.py             # Entry point (17 lines)
│   ├── 3_▶️_執行.py                 # Entry point (17 lines)
│   ├── 4_📊_結果.py                 # Entry point (17 lines)
│   └── 5_💾_Checkpoint.py          # Entry point (17 lines)
│         ↓ exec()
│         ↓
└── accrual_bot/ui/pages/           # Actual Implementation (standard filenames)
    ├── 1_configuration.py          # Business logic (65 lines)
    ├── 2_file_upload.py            # Business logic (80 lines)
    ├── 3_execution.py              # Business logic (205 lines)
    ├── 4_results.py                # Business logic (149 lines)
    └── 5_checkpoint.py             # Business logic (142 lines)
```

**Why two layers?**
- **Streamlit requirement**: Multi-page apps need emoji filenames in `pages/` for sidebar navigation
- **Best practice**: Avoid emoji in actual code files (cross-platform, git compatibility)
- **Separation of concerns**: Entry points (thin wrappers) vs business logic (testable, reusable)

**Entry point example**:
```python
# pages/1_⚙️_配置.py
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
actual_page = project_root / "accrual_bot" / "ui" / "pages" / "1_configuration.py"
exec(open(actual_page, encoding='utf-8').read())
```

**Navigation**: All `st.switch_page()` calls must use emoji filenames:
```python
st.switch_page("pages/1_⚙️_配置.py")  # ✓ Correct
st.switch_page("pages/1_configuration.py")  # ✗ Wrong - Streamlit won't find it
```

**For detailed UI documentation, see [doc/UI_Architecture.md](doc/UI_Architecture.md)**

## Testing

The project uses pytest with async support. **725 tests collected** (674 unit + 12 integration + 39 unmarked), **723 passing** (as of 2026-03). 2 pre-existing failures in `test_previous_workpaper.py` (`_determine_key_type` returns None for 'PO Line'). Overall coverage: 38%.

### Test Structure

```
tests/
├── conftest.py                        # Shared fixtures
├── pytest.ini                         # pytest configuration with markers
├── fixtures/
│   ├── sample_data.py                 # Test data generators
│   └── test_data_generators.py        # Entity-specific data generators
├── unit/                              # Unit tests (@pytest.mark.unit)
│   ├── conftest.py                    # Unit-level shared fixtures
│   ├── core/
│   │   ├── conftest.py                # Core-level fixtures
│   │   ├── pipeline/
│   │   │   ├── test_context.py        # ProcessingContext (100% coverage)
│   │   │   ├── test_base_classes.py   # PipelineStep, StepResult, StepStatus
│   │   │   ├── test_pipeline.py       # Pipeline execution, add/remove steps
│   │   │   ├── test_pipeline_builder.py # PipelineBuilder fluent API
│   │   │   ├── test_checkpoint.py     # CheckpointManager save/load
│   │   │   └── steps/
│   │   │       ├── test_base_loading.py       # BaseLoadingStep template
│   │   │       ├── test_base_evaluation.py    # BaseERMEvaluationStep template
│   │   │       ├── test_previous_workpaper.py # PreviousWorkpaperStep
│   │   │       ├── test_common_steps.py       # StepMetadataBuilder, DateLogicStep
│   │   │       ├── test_business_steps.py     # StatusEvaluation, AccountMapping
│   │   │       └── test_post_processing.py    # DataQualityCheck, Statistics
│   │   └── datasources/
│   │       ├── test_datasource_config.py  # DataSourceConfig (100% coverage)
│   │       ├── test_datasource_factory.py # DataSourceFactory
│   │       ├── test_csv_source.py         # CSVSource read/write/metadata
│   │       ├── test_excel_source.py       # ExcelSource read/write/sheets
│   │       └── test_parquet_source.py     # ParquetSource read/write/schema
│   ├── tasks/
│   │   ├── conftest.py                # Task-level fixtures (ERM DataFrames)
│   │   ├── spt/
│   │   │   ├── test_spt_orchestrator.py       # SPTPipelineOrchestrator
│   │   │   ├── test_spt_loading.py            # SPTDataLoadingStep validation
│   │   │   ├── test_spt_evaluation_erm.py     # SPTERMLogicStep (96% coverage)
│   │   │   └── test_spt_account_prediction.py # SPTAccountPredictionStep
│   │   └── spx/
│   │       ├── test_spx_orchestrator.py       # SPXPipelineOrchestrator
│   │       ├── test_spx_loading.py            # SPXDataLoadingStep validation
│   │       ├── test_spx_condition_engine.py   # Config-driven condition engine
│   │       ├── test_spx_evaluation.py         # StatusStage1, SPXERMLogic
│   │       └── test_spx_ppe_steps.py          # PPE description extraction (PPE_DESC)
│   ├── utils/
│   │   ├── config/
│   │   │   └── test_config_manager.py     # ConfigManager thread-safety
│   │   ├── helpers/
│   │   │   ├── test_column_utils.py       # ColumnResolver (100% coverage)
│   │   │   ├── test_data_utils.py         # TOML loading, regex patterns
│   │   │   └── test_file_utils.py         # File validation, copy, hash
│   │   ├── logging/
│   │   │   └── test_logger.py             # Singleton, thread-safety
│   │   ├── duckdb_manager/
│   │   │   └── test_duckdb_manager.py     # DuckDBConfig, Manager CRUD
│   │   └── metadata_builder/
│   │       └── test_metadata_builder.py   # SourceSpec, ColumnSpec, SchemaConfig
│   ├── ui/
│   │   ├── services/
│   │   │   ├── test_unified_pipeline_service.py  # Pipeline service (94%)
│   │   │   └── test_file_handler.py              # File handler (91%)
│   │   └── models/
│   │       └── test_state_models.py       # State dataclasses (100%)
│   └── data/
│       └── importers/
│           └── test_base_importer.py      # BaseDataImporter
└── integration/                       # Integration tests (@pytest.mark.integration)
    ├── test_pipeline_orchestrators.py # SPT/SPX orchestrator integration
    └── test_checkpoint_roundtrip.py   # Checkpoint save → load roundtrip
```

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run by category
python -m pytest tests/ -m unit          # Unit tests only (674 tests, 39 unmarked not included)
python -m pytest tests/ -m integration   # Integration tests only (12 tests)

# Run with coverage
python -m pytest tests/ --cov=accrual_bot --cov-report=html

# Run specific module tests
python -m pytest tests/unit/core/pipeline/ -v
python -m pytest tests/unit/tasks/spx/ -v

# Use scripts (from scripts/ directory)
scripts/run_unit.bat                     # Unit tests
scripts/run_integration.bat              # Integration tests
scripts/run_coverage.bat                 # Full coverage report
```

### Coverage Summary

Overall: **38%** (14030 statements, 8661 missed). Key modules with high coverage:

| Module | Coverage |
|--------|----------|
| `core/pipeline/context.py` | 100% |
| `core/pipeline/base.py` | 90% |
| `core/pipeline/pipeline.py` | 86% |
| `core/pipeline/steps/post_processing.py` | 88% |
| `core/pipeline/steps/business.py` | 84% |
| `core/pipeline/steps/base_loading.py` | 80% |
| `core/pipeline/steps/base_evaluation.py` | 65% |
| `core/datasources/config.py` | 100% |
| `core/datasources/csv_source.py` | 77% |
| `core/datasources/excel_source.py` | 80% |
| `core/datasources/parquet_source.py` | 82% |
| `utils/helpers/column_utils.py` | 100% |
| `utils/helpers/file_utils.py` | 79% |
| `utils/duckdb_manager/config.py` | 77% |
| `utils/duckdb_manager/manager.py` | 81% |
| `utils/metadata_builder/config.py` | 81% |
| `ui/services/unified_pipeline_service.py` | 94% |
| `ui/models/state_models.py` | 100% |

Low coverage modules (large/complex, many external dependencies):
- `core/pipeline/steps/common.py` (40%), `core/datasources/duckdb_source.py` (15%), `utils/duckdb_manager/operations/*` (8-34%), `utils/metadata_builder/processors/*` (19-26%)

### Key Fixtures

**`tests/conftest.py`** (shared):
- `mock_config_manager`: Mocks ConfigManager with pipeline configuration
- `processing_context`: ProcessingContext with sample DataFrame and auxiliary data
- `mock_data_source_factory`: AsyncMock for data source operations

**`tests/unit/tasks/conftest.py`** (task-level):
- `spt_file_paths` / `spx_file_paths`: Typical file path dicts
- `spt_erm_df` / `spx_erm_df`: ERM test DataFrames with all required columns
- `spt_erm_context` / `spx_erm_context`: ProcessingContext with auxiliary data
- `mock_spt_orchestrator_config` / `mock_spx_orchestrator_config`: Patched config_manager

## Key Patterns

### Using Pipeline Orchestrators (Recommended)

Pipeline orchestrators provide configuration-driven pipeline construction:

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator

# Create SPT PO pipeline
spt_orchestrator = SPTPipelineOrchestrator()
pipeline = spt_orchestrator.build_po_pipeline(file_paths={'po_file': 'path/to/po.xlsx'})

# Create SPX PR pipeline
spx_orchestrator = SPXPipelineOrchestrator()
pipeline = spx_orchestrator.build_pr_pipeline(file_paths={'pr_file': 'path/to/pr.xlsx'})

# Create SPT PROCUREMENT pipeline (PO/PR/COMBINED)
pipeline = spt_orchestrator.build_procurement_pipeline(
    file_paths={'raw_po': 'path/to/po.xlsx'},
    source_type='PO'  # or 'PR' or 'COMBINED'
)

# Create SPX PPE pipeline
pipeline = spx_orchestrator.build_ppe_pipeline(
    file_paths={'contract_filing_list': {'path': 'path/to/file.xlsx'}},
    processing_date=202512
)

# Create SPX PPE_DESC pipeline
pipeline = spx_orchestrator.build_ppe_desc_pipeline(
    file_paths={'workpaper': 'path/to/workpaper.xlsx'},
    processing_date=202512
)

# Get enabled steps for a processing type
enabled_steps = spt_orchestrator.get_enabled_steps('PO')  # Returns list from config
```

Steps are loaded based on `[pipeline.spt]` in `stagging_spt.toml` or `[pipeline.spx]` in `stagging_spx.toml`.

### Creating a New Pipeline (Manual)

```python
from accrual_bot.core.pipeline import PipelineBuilder, steps

pipeline = (PipelineBuilder("Pipeline_Name", "SPX")
    .with_description("Description")
    .with_stop_on_error(False)
    .add_step(steps.DataLoadingStep(...))
    .add_step(steps.BusinessLogicStep(...))
    .build())
```

### Executing with Checkpoints

```python
from accrual_bot.core.pipeline import execute_pipeline_with_checkpoint

result = await execute_pipeline_with_checkpoint(
    file_paths=file_paths_dict,
    processing_date=202512,  # YYYYMM format
    pipeline_func=create_pipeline_function,
    entity='SPX',
    processing_type='PO',
    save_checkpoints=True
)
```

### Creating a New Step

**For data loading steps**, inherit from BaseLoadingStep:

```python
from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from typing import Tuple
import pandas as pd

class MyEntityDataLoadingStep(BaseLoadingStep):
    def get_required_file_type(self) -> str:
        return 'po_file'  # or 'pr_file'

    async def _load_primary_file(self, source, path: str) -> Tuple[pd.DataFrame, int, int]:
        # Load entity-specific primary file
        df = await source.read()
        raw_rows, filtered_rows = len(df), len(df)
        return df, raw_rows, filtered_rows

    async def _load_reference_data(self, context) -> int:
        # Load entity-specific reference data into context
        ref_df = await self._load_reference_file('ref_file')
        context.set_auxiliary_data('reference', ref_df)
        return len(ref_df)
```

**For ERM evaluation steps**, inherit from BaseERMEvaluationStep:

```python
from accrual_bot.core.pipeline.steps.base_evaluation import BaseERMEvaluationStep, BaseERMConditions
import pandas as pd

class MyEntityERMLogicStep(BaseERMEvaluationStep):
    def _build_conditions(self, df: pd.DataFrame, file_date, status_column: str):
        # Build entity-specific evaluation conditions
        return BaseERMConditions(
            erp_completed=(df['status'] == 'Closed'),
            invalid_format=(df['description'].isna()),
            # ... add more conditions
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        # Apply entity-specific status logic
        df.loc[conditions.erp_completed, status_column] = '已完成'
        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        # Set entity-specific accounting fields
        return df
```

**For generic steps**, inherit from PipelineStep:

```python
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus

class CustomStep(PipelineStep):
    async def execute(self, context: ProcessingContext) -> StepResult:
        # Access data via context.data (pandas DataFrame)
        # Access auxiliary data via context.get_auxiliary_data('name')
        # Set variables via context.set_variable('key', value)
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

    async def validate_input(self, context: ProcessingContext) -> bool:
        return 'required_column' in context.data.columns
```

## Extending the System

### Adding a New Processing Type to Existing Entity

Example: Adding 'INV' (Invoice) type to SPX

**Files to modify:**

| # | File | Changes |
|---|------|---------|
| 1 | `ui/config.py` | Add 'INV' to `ENTITY_CONFIG['SPX']['types']` |
| 2 | `ui/config.py` | Add `REQUIRED_FILES[('SPX', 'INV')]` |
| 3 | `ui/config.py` | Add `OPTIONAL_FILES[('SPX', 'INV')]` |
| 4 | `ui/config.py` | Add file labels to `FILE_LABELS` |
| 5 | `config/paths.toml` | Add `[spx.inv]` and `[spx.inv.params]` sections |
| 6 | `config/stagging_spx.toml` | Add `enabled_inv_steps` to `[pipeline.spx]` section |
| 7 | `tasks/spx/pipeline_orchestrator.py` | Add `build_inv_pipeline()` method |
| 8 | `tasks/spx/pipeline_orchestrator.py` | Register steps in `_create_step()` |
| 9 | `tasks/spx/pipeline_orchestrator.py` | Update `get_enabled_steps()` |
| 10 | `ui/services/unified_pipeline_service.py` | Add elif branch in `build_pipeline()` |

### Adding a New Entity

Example: Adding a new entity (e.g. 'NEW')

**Additional files to create:**

| # | File | Purpose |
|---|------|---------|
| 1 | `tasks/new/__init__.py` | Module init, export orchestrator |
| 2 | `tasks/new/pipeline_orchestrator.py` | NEWPipelineOrchestrator class |
| 3 | `tasks/new/steps/*.py` | Entity-specific steps (if needed) |
| 4 | `config/stagging_new.toml` | Entity-specific pipeline and business rules |

**Additional modifications:**

| # | File | Changes |
|---|------|---------|
| 5 | `ui/services/unified_pipeline_service.py` | Register in `_get_orchestrator()` |

**For detailed extension guide, see [doc/UI_Architecture.md#14-擴充指南新增-pipeline-類型](doc/UI_Architecture.md)**

## File Structure Notes

```
accrual_bot/
├── core/
│   ├── pipeline/               # Pipeline framework
│   │   ├── pipeline.py         # Pipeline class
│   │   ├── context.py          # ProcessingContext class
│   │   ├── checkpoint.py       # CheckpointManager
│   │   └── steps/
│   │       ├── base_loading.py     # BaseLoadingStep (~593 lines)
│   │       ├── base_evaluation.py  # BaseERMEvaluationStep (~518 lines)
│   │       ├── common.py           # Shared steps (~1254 lines): DateLogic, ProductFilter, etc.
│   │       ├── business.py         # StatusEvaluation, AccountMapping, etc.
│   │       ├── post_processing.py  # DataQualityCheck, Statistics
│   │       └── *.py                # Entity-specific shims (backward compat re-exports)
│   └── datasources/            # Unified data access layer
│       ├── base.py             # DataSource (ABC), DataSourceType
│       ├── config.py           # DataSourceConfig, ConnectionPool
│       ├── factory.py          # DataSourceFactory, DataSourcePool
│       ├── excel_source.py     # ExcelSource
│       ├── csv_source.py       # CSVSource
│       ├── parquet_source.py   # ParquetSource
│       ├── duckdb_source.py    # DuckDBSource
│       └── google_sheet_source.py  # GoogleSheetsSource (optional, gspread)
├── tasks/                      # Entity-specific implementations
│   ├── common/                 # Shared task steps (DataShapeSummaryStep)
│   ├── spt/
│   │   ├── pipeline_orchestrator.py  # SPTPipelineOrchestrator
│   │   └── steps/              # PO/PR/PROCUREMENT steps (18 files)
│   └── spx/
│       ├── pipeline_orchestrator.py  # SPXPipelineOrchestrator
│       └── steps/              # PO/PR/PPE/PPE_DESC steps (12 files)
├── ui/                         # Streamlit UI
│   ├── config.py               # UI configuration constants
│   ├── services/
│   │   └── unified_pipeline_service.py  # KEY: UI-Pipeline bridge
│   ├── pages/                  # 5 workflow pages
│   └── components/             # Reusable UI components
├── config/
│   ├── config.ini              # Legacy INI config
│   ├── paths.toml              # File paths and read params
│   ├── run_config.toml         # Runtime execution config
│   ├── stagging.toml           # Shared config (paths, date patterns, category patterns)
│   ├── stagging_spt.toml       # SPT pipeline steps, pivot config, business rules
│   └── stagging_spx.toml       # SPX pipeline steps, supplier lists, condition rules
├── runner/                     # Pipeline execution (config_loader, step_executor)
├── data/                       # Importers (base_importer, google_sheets_importer)
└── utils/
    ├── config/
    │   ├── config_manager.py   # Thread-safe singleton
    │   └── constants.py        # Shared constants
    ├── helpers/
    │   ├── column_utils.py     # ColumnResolver
    │   ├── data_utils.py       # TOML loading, regex patterns
    │   └── file_utils.py       # File validation, copy, hash
    ├── logging/logger.py       # Unified logging
    ├── duckdb_manager/         # DuckDB operations, migration, config
    └── metadata_builder/       # Schema config, processors, transformers

# Project root
├── main_pipeline.py            # CLI entry point
├── main_streamlit.py           # Streamlit UI entry point
├── checkpoints/                # Saved pipeline states (git-ignored)
├── output/                     # Processed results (git-ignored)
└── doc/
    └── UI_Architecture.md      # Detailed UI documentation
```

- **main_pipeline.py**: Entry point with example pipeline executions for each entity type
- **accrual_bot/core/pipeline/**: Framework components
  - `pipeline.py`, `context.py`: Core pipeline infrastructure
  - `steps/base_loading.py`: Template base class for loading steps (~593 lines)
  - `steps/base_evaluation.py`: Template base class for ERM evaluation (~518 lines)
  - `steps/common.py`: Shared steps (~1254 lines) — DateLogic, ProductFilter, DataIntegration, PreviousWorkpaper, etc.
  - `steps/business.py`: Business logic steps — StatusEvaluation, AccountMapping
  - `steps/post_processing.py`: Post-processing steps — DataQualityCheck, Statistics
  - `steps/*.py`: Entity-specific shim files (backward compatibility re-exports to tasks/)
- **accrual_bot/tasks/**: Entity-specific implementations
  - `common/`: Shared task steps (DataShapeSummaryStep)
  - `spt/pipeline_orchestrator.py`: SPT pipeline configuration (PO/PR/PROCUREMENT with PO/PR/COMBINED sub-types)
  - `spx/pipeline_orchestrator.py`: SPX pipeline configuration (PO/PR/PPE/PPE_DESC)
  - `spt/steps/`: SPT-specific steps — loading, ERM, status label, account prediction, procurement, combined procurement
  - `spx/steps/`: SPX-specific steps — loading, condition engine, evaluation, exporting, integration, PPE, PPE_DESC
- **accrual_bot/config/stagging_spt.toml**, **stagging_spx.toml**: Entity-specific configuration with `[pipeline.spt]` and `[pipeline.spx]` sections for step enablement
- **checkpoints/**: Saved pipeline states (excluded from git)
- **output/**: Processed results (excluded from git)

## Architecture Improvements (January 2026)

The codebase underwent significant refactoring to improve code quality and maintainability:

### Phase 1: Critical Fixes (P0)
- **Thread-Safe ConfigManager**: Fixed race condition in singleton pattern using double-checked locking
- **Unified Logging**: Migrated all pipeline modules to use consistent `get_logger()` framework

### Phase 2: Code Deduplication (P1)
- **BaseLoadingStep**: Extracted ~400-500 lines of duplicated code from SPT/SPX loading steps using template method pattern
- **BaseERMEvaluationStep**: Extracted ~300-400 lines of duplicated ERM evaluation logic
- **Impact**: Eliminated ~750 lines of duplication (~5% code reduction)

### Phase 3: Structure & Extensibility (P2)
- **Tasks Directory**: Created entity-specific modules under `tasks/` (spt, spx, common)
- **Pipeline Orchestrators**: Implemented configuration-driven step loading via `SPTPipelineOrchestrator` and `SPXPipelineOrchestrator`
- **Backward Compatibility**: All existing imports continue to work via re-exports

### Phase 4: Streamlit UI (January 2026)
- **5-Page Workflow**: Configuration → Upload → Execution → Results → Checkpoint
- **Service Layer**: UnifiedPipelineService decouples UI from pipeline implementation
- **Async Bridge**: Handles sync/async conversion for Streamlit compatibility
- **Configuration-Driven**: UI content driven by `ui/config.py` and `paths.toml`

### Phase 5: UI Optimization & Cleanup (2026-01-17)
- **Removed Deprecated Template System**: Deleted `template_picker.py` and cleaned template-related code from 7 files (~150 lines)
- **Cleaned Duplicate Pages**: Removed 5 redundant `*_page.py` files (~400 lines)
- **Added Log Export**: Execution page now allows downloading logs as `.txt` files
- **Fixed Dual-Layer Pages**: Corrected Entry Point files to use `exec()` instead of imports
- **Fixed ProcessingContext**: Added `auxiliary_data` property for UI access
- **Impact**: Removed ~558 lines of code (~22% reduction in UI layer)

### Phase 6: Comprehensive Test Suite (2026-03)
- **725 tests**: 674 unit + 12 integration + 39 unmarked, covering core pipeline, data sources, tasks, utilities, and UI
- **Three-phase rollout**: P0 core infrastructure → P1 business logic → P2 extended coverage
- **Scripts**: `scripts/` directory with `.sh`/`.bat` pairs for running unit, integration, coverage, and quick tests
- **Test data generators**: Synthetic data factories in `tests/fixtures/test_data_generators.py`

### Phase 7: Entity Config Split & Procurement Pipeline (2026-03)
- **Config split**: `stagging.toml` split into `stagging_spt.toml` and `stagging_spx.toml` for entity-specific configuration
- **SPT PROCUREMENT**: New pipeline type for procurement staff (PO/PR/COMBINED variants)
- **SPX PPE_DESC**: New pipeline type for PO/PR description extraction with contract period mapping

### Phase 8: Utils Layer Bug Fixes (2026-03-14)
- **metadata_builder (3 fixes)**: `validate_only()` silent-pass bug (exception swallowed, `cb_result is None` incorrectly treated as success); `cast_failures` counted pre-existing NULLs rather than conversion-induced NULLs; `ColumnMappingError` defined but never raised (Regex errors silently returned `None`)
- **duckdb_manager (5 fixes)**: `clean_and_convert_column()` ignored `_validate_conversion()` return value (transaction started unconditionally); `upsert_df_into_table()` and `backup_table()` used manual string escaping instead of existing `SafeSQL` utilities; `execute_transaction()` called `conn.sql("BEGIN/COMMIT/ROLLBACK")` directly, bypassing `OperationMixin._rollback()`'s built-in silent try/except; `connection_timeout: int = 30` was dead config (DuckDB Python API does not support this parameter) — field removed from `DuckDBConfig`

### Phase 9: SPX Tasks Bug Fixes (2026-03-17)
- **Orphaned steps (Fix 1)**: 6 legacy prototype steps in `spx_steps.py` (`SPXDepositCheckStep` etc.) removed from `__all__` and marked deprecated in docstrings — were in public API but absent from orchestrator step registry
- **Hardcoded Sheets ID (Fix 2)**: `ClosingListIntegrationStep` Spreadsheet ID and sheet name list moved to `stagging_spx.toml` (`closing_list_spreadsheet_id`, `closing_list_sheet_names`, `closing_list_sheet_range`) — adding a new year now requires only a TOML edit
- **PPE pipeline consistency (Fix 3)**: `build_ppe_pipeline()` and `build_ppe_desc_pipeline()` refactored to config-driven pattern; `enabled_ppe_steps`/`enabled_ppe_desc_steps` added to TOML; PPE step factory entries added to `_create_step()`
- **Implicit coupling (Fix 4)**: `ColumnAdditionStep` replaced `'raw_pr' in file_paths.keys()` detection with `context.metadata.processing_type == 'PR'` — eliminates silent failure if key name changes
- **Documentation mismatch (Fix 5)**: `spx_evaluation_2.py` module docstring path corrected; `DepositStatusUpdateStep` default name unified to `"DepositStatusUpdate"` (matches orchestrator registry key)
- **Async blocking (Fix 6)**: `PPEDataLoadingStep._load_renewal_list()` three synchronous `get_sheet_data()` calls wrapped with `asyncio.to_thread()` — releases event loop during Google Sheets API I/O

### Phase 10: SPT Tasks Bug Fixes (2026-03-17)
- **KeyError guard (Fix 1)**: `SPTPostProcessingStep._rearrange_reviewer_col()` — added column existence check before `df.pop()`; missing columns now log a warning and return early instead of raising `KeyError`
- **DRY refactoring (Fix 5, includes Fix 2)**: `spt_loading.py` rewritten from 1164 → 182 lines (84% reduction) — introduced `SPTBaseDataLoadingStep(BaseLoadingStep)` with four template hooks; `SPTDataLoadingStep` and `SPTPRDataLoadingStep` each reduced to `get_required_file_type()` declarations; `df.rename()` silent bug fixed via `BaseLoadingStep._process_common_columns()` which already assigns correctly
- **IndexError guard (Fix 3)**: `PayrollDetectionStep.execute()` — replaced unsafe `[0]` list index on status column lookup with guard block; empty match now logs warning and skips update gracefully
- **Logger in orchestrator (Fix 4)**: `SPTPipelineOrchestrator.__init__` now initializes `self.logger = get_logger(__name__)`; unknown-step warning replaced `print()` with `self.logger.warning()`
- **Orphaned steps (Fix 6)**: 4 legacy prototype steps (`SPTStatusStep`, `SPTDepartmentStep`, `SPTAccrualStep`, `SPTValidationStep`) removed from `spt/steps/__init__.py` `__all__`; deprecation docstrings added to each class
- **Config-driven commission/payroll (Fix 7)**: `COMMISSION_CONFIG` and `PAYROLL_CONFIG` business rules moved to `stagging_spt.toml` under `[spt.commission.affiliate]`, `[spt.commission.ams]`, `[spt.payroll]`; `__init__` reads from config with class-level constants as fallback
- **Sub-context metadata propagation (Fix 8)**: `CombinedProcurementProcessingStep._process_po_data()` and `_process_pr_data()` now fully propagate `entity_type`, `processing_type`, and `processing_date` from parent context to sub-context

### Phase 11: Runner Module Bug Fixes (2026-03-17)
- **AttributeError prevention (Fix 1, P0)**: `load_file_paths()` now performs early entity/type validation with explicit `ValueError` (message lists available entities/types), replacing the silent `AttributeError: 'NoneType' object has no attribute 'get'` when an unknown entity or type is passed
- **Dead code removal (Fix 2, P1)**: `StepByStepExecutor._save_checkpoint()` — removed 5-line redundant `checkpoint_name` construction; now uses the return value of `save_checkpoint()` directly, ensuring the debug log matches the actual file saved on disk
- **Verbose mode implemented (Fix 3, P3)**: `RunConfig.verbose = True` now sets `logging.getLogger('accrual_bot')` to `DEBUG` level — the previously-documented but unimplemented feature now works
- **Glob multi-match warning (Fix 4, P2)**: When a wildcard path matches multiple files, a `WARNING` log now names the selected file and lists the excluded ones — silent lexicographic selection still applies but is no longer invisible
- **Template unresolved-variable detection (Fix 5, P2)**: `_resolve_path_template()` uses `re.findall` to detect leftover `{VAR}` tokens after substitution; typos like `{YYYMM}` now emit a `WARNING` with the original template, instead of silently passing a malformed path to downstream I/O
- **Local path override (Fix 7, P2)**: `load_file_paths()` deep-merges a gitignored `accrual_bot/config/paths.local.toml` when present; `paths.local.toml.example` committed as a template; `_deep_merge()` helper added; `.gitignore` updated — eliminates hardcoded Windows paths for cross-machine use
- **Backward-compat functions accept date (Fix 8, P3)**: `run_spx_po_full_pipeline()`, `run_spx_pr_full_pipeline()`, `run_spt_po_full_pipeline()`, `run_spt_pr_full_pipeline()` all accept `processing_date: int = 202512` — callers can now specify a month without editing source code
- **Result dict alignment (Fix 9, P1)**: Normal execution path adds `result.setdefault("aborted", False)` after `pipeline.execute()`, aligning with `StepByStepExecutor`'s result structure and eliminating `KeyError` for callers that check `result["aborted"]`
- **Dead branch removed (Fix 10, P3)**: `_convert_params()` `elif key == "keep_default_na"` branch deleted — it was identical to the `else` branch and TOML already parses booleans natively

### Benefits
- **Maintainability**: Single source of truth for shared logic reduces bug surface area
- **Extensibility**: New entities/types can be added via configuration + orchestrator updates
- **Testability**: Template methods enable focused unit testing of entity-specific hooks
- **Safety**: Thread-safe configuration eliminates race conditions in concurrent environments
- **Usability**: Web UI provides guided workflow for non-technical users

## Documentation

| Document | Description |
|----------|-------------|
| `CLAUDE.md` | This file - development guidance |
| `doc/Project_Design_Reference.md` | Project design reference (architecture, patterns, templates) |
| `doc/UI_Architecture.md` | Detailed UI architecture, components, and extension guide |
| `doc/SPT_PROCUREMENT_Implementation.md` | SPT Procurement pipeline implementation details |
| `doc/SPX_ConditionEngine_Implementation.md` | SPX condition engine design and implementation |
| `doc/Unified_System_Design_Reference.md` | Unified system design overview |
| `doc/SPE_Project_Architecture_Reference.md` | SPE project architecture reference |
| `doc/Project_Review_And_Merger_Analysis.md` | Project review and merger analysis |
| `doc/Task Pipeline Structure Unit Test Plan.md` | Test plan for pipeline structure |
| `tests/README.md` | Test suite guide (725+ tests) |

## Language

Code comments and documentation are in Traditional Chinese (繁體中文). Use Chinese for user-facing strings and comments.
