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
│  tasks/spt/ | tasks/spx/ | tasks/mob/                       │
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
  - `tasks/spt/`: SPT-specific steps and pipeline orchestrator
  - `tasks/spx/`: SPX-specific steps and pipeline orchestrator
  - `tasks/mob/`: MOB-specific steps and pipeline orchestrator (inactive)
  - Each task module contains entity-specific business logic

- **utils/**: Cross-cutting concerns
  - Thread-safe ConfigManager singleton
  - Unified logging framework
  - Data utilities

### Pipeline System (core/pipeline/)

The application uses a step-based async pipeline architecture with template method pattern:

- **PipelineStep**: Abstract base class for all processing steps. Each step implements `execute()` and `validate_input()` methods.
- **BaseLoadingStep**: Template base class for data loading steps (~570 lines of shared logic):
  - Concrete methods: `_normalize_file_paths()`, `_load_all_files_concurrent()`, `_validate_file_configs()`
  - Abstract hooks: `get_required_file_type()`, `_load_primary_file()`, `_load_reference_data()`
- **BaseERMEvaluationStep**: Template base class for ERM evaluation steps (~465 lines of shared logic):
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

Two active entity types with configuration-driven pipeline orchestration:

- **SPT** ([tasks/spt/](accrual_bot/tasks/spt/)):
  - PO Pipeline: SPTDataLoading → CommissionDataUpdate → PayrollDetection → SPTERMLogic → SPTStatusLabel → SPTAccountPrediction
  - PR Pipeline: SPTPRDataLoading → CommissionDataUpdate → PayrollDetection → SPTERMLogic → SPTStatusLabel → SPTAccountPrediction
  - Advanced features: commission handling, payroll detection, account prediction rules

- **SPX** ([tasks/spx/](accrual_bot/tasks/spx/)):
  - PO Pipeline: SPXDataLoading → ColumnAddition → ClosingListIntegration → StatusStage1 → SPXERMLogic → DepositStatusUpdate → ValidationDataProcessing → SPXExport
  - PR Pipeline: SPXPRDataLoading → ColumnAddition → StatusStage1 → SPXPRERMLogic → SPXPRExport
  - PPE Pipeline: PPEDataLoading → PPEDataCleaning → PPEDataMerge → PPEContractDateUpdate → PPEMonthDifference
  - Complex processing: 11-condition status evaluation, deposit/rental identification, locker/kiosk asset validation

Pipeline steps can be enabled/disabled via configuration in [config/stagging.toml](accrual_bot/config/stagging.toml):
```toml
[pipeline.spt]
enabled_po_steps = ["SPTDataLoading", "CommissionDataUpdate", ...]

[pipeline.spx]
enabled_po_steps = ["SPXDataLoading", "ColumnAddition", ...]
enabled_pr_steps = ["SPXPRDataLoading", "ColumnAddition", ...]
enabled_ppe_steps = ["PPEDataLoading", "PPEDataCleaning", ...]
```

### Data Sources (core/datasources/)

Unified async data access layer supporting:
- Excel (ExcelSource)
- CSV (CSVSource)
- Parquet (ParquetSource)
- DuckDB (DuckDBSource)

All sources implement the same interface with thread-safe operations and shared thread pools.

### Configuration

Three configuration files:

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

- **config/stagging.toml**: Main TOML configuration containing:
  - Pipeline configuration (enabled steps per entity)
  - Date regex patterns
  - Entity-specific pivot configurations
  - SPT status label rules and account prediction rules
  - SPX column mappings and business rules
  - Account code to keyword mappings

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
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE'],
        'icon': '📦',
    },
}

# Required files per entity/type
REQUIRED_FILES = {
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PPE'): ['contract_filing_list'],
}

# Optional files per entity/type
OPTIONAL_FILES = {
    ('SPX', 'PO'): ['previous', 'procurement_po', 'ap_invoice', 'ops_validation'],
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

The project uses pytest with async support. **674 unit tests + 12 integration tests = 686 passing tests** (as of 2026-03).

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
│   │       └── test_spx_ppe_steps.py          # PPE description extraction
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
python -m pytest tests/ -m unit          # Unit tests only (674 tests)
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

### Coverage Summary (Key Modules)

| Module | Coverage |
|--------|----------|
| `core/pipeline/context.py` | 100% |
| `core/pipeline/base.py` | 90% |
| `core/pipeline/pipeline.py` | 86% |
| `core/pipeline/steps/post_processing.py` | 88% |
| `core/pipeline/steps/business.py` | 84% |
| `core/pipeline/steps/base_loading.py` | 80% |
| `core/datasources/config.py` | 100% |
| `core/datasources/{csv,excel,parquet}_source.py` | 77-82% |
| `tasks/spt/steps/spt_evaluation_erm.py` | 96% |
| `tasks/spx/steps/spx_evaluation.py` | 67% |
| `utils/helpers/column_utils.py` | 100% |
| `utils/helpers/file_utils.py` | 79% |
| `utils/duckdb_manager/manager.py` | 81% |
| `ui/services/unified_pipeline_service.py` | 94% |
| `ui/models/state_models.py` | 100% |

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

# Create SPX PPE pipeline
pipeline = spx_orchestrator.build_ppe_pipeline(
    file_paths={'contract_filing_list': {'path': 'path/to/file.xlsx'}},
    processing_date=202512
)

# Get enabled steps for a processing type
enabled_steps = spt_orchestrator.get_enabled_steps('PO')  # Returns list from config
```

Steps are loaded based on `[pipeline.spt]` or `[pipeline.spx]` configuration in stagging.toml.

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
| 6 | `config/stagging.toml` | Add `enabled_inv_steps` to `[pipeline.spx]` |
| 7 | `tasks/spx/pipeline_orchestrator.py` | Add `build_inv_pipeline()` method |
| 8 | `tasks/spx/pipeline_orchestrator.py` | Register steps in `_create_step()` |
| 9 | `tasks/spx/pipeline_orchestrator.py` | Update `get_enabled_steps()` |
| 10 | `ui/services/unified_pipeline_service.py` | Add elif branch in `build_pipeline()` |

### Adding a New Entity

Example: Adding 'MOB' entity

**Additional files to create:**

| # | File | Purpose |
|---|------|---------|
| 1 | `tasks/mob/__init__.py` | Module init, export orchestrator |
| 2 | `tasks/mob/pipeline_orchestrator.py` | MOBPipelineOrchestrator class |
| 3 | `tasks/mob/steps/*.py` | Entity-specific steps (if needed) |

**Additional modifications:**

| # | File | Changes |
|---|------|---------|
| 4 | `ui/services/unified_pipeline_service.py` | Register in `_get_orchestrator()` |

**For detailed extension guide, see [doc/UI_Architecture.md#14-擴充指南新增-pipeline-類型](doc/UI_Architecture.md)**

## File Structure Notes

```
accrual_bot/
├── core/pipeline/              # Framework components
│   ├── pipeline.py             # Pipeline class
│   ├── context.py              # ProcessingContext class
│   ├── checkpoint.py           # CheckpointManager
│   └── steps/
│       ├── base_loading.py     # BaseLoadingStep (~570 lines)
│       ├── base_evaluation.py  # BaseERMEvaluationStep (~465 lines)
│       └── *.py                # Shared steps
├── tasks/                      # Entity-specific implementations
│   ├── spt/
│   │   ├── pipeline_orchestrator.py
│   │   └── steps/
│   ├── spx/
│   │   ├── pipeline_orchestrator.py
│   │   └── steps/
│   └── mob/
│       └── steps/
├── ui/                         # Streamlit UI
│   ├── config.py               # UI configuration constants
│   ├── services/
│   │   └── unified_pipeline_service.py  # KEY: UI-Pipeline bridge
│   ├── pages/                  # 5 workflow pages
│   └── components/             # Reusable UI components
├── config/
│   ├── config.ini              # Legacy INI config
│   ├── paths.toml              # File paths and read params
│   └── stagging.toml           # Pipeline steps and business rules
├── data/                       # Importers, exporters, transformers
└── utils/
    ├── config/config_manager.py # Thread-safe singleton
    └── logging/logger.py        # Unified logging

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
  - `steps/base_loading.py`: Template base class for loading steps (~570 lines)
  - `steps/base_evaluation.py`: Template base class for ERM evaluation (~465 lines)
  - `steps/`: Other shared pipeline steps
- **accrual_bot/tasks/**: Entity-specific implementations
  - `spt/pipeline_orchestrator.py`: SPT pipeline configuration and construction
  - `spx/pipeline_orchestrator.py`: SPX pipeline configuration and construction
  - `spt/steps/`, `spx/steps/`, `mob/steps/`: Entity-specific step implementations (re-exported from core for backward compatibility)
- **accrual_bot/config/stagging.toml**: Configuration file with `[pipeline.spt]` and `[pipeline.spx]` sections for step enablement
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
- **Tasks Directory**: Created entity-specific modules under `tasks/` (spt, spx, mob)
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
| `doc/UI_Architecture.md` | Detailed UI architecture, components, and extension guide |
| `README.md` | Project overview and quick start |

## Language

Code comments and documentation are in Traditional Chinese (繁體中文). Use Chinese for user-facing strings and comments.
