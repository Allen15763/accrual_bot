# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Accrual Bot is an async data processing system for PO/PR (Purchase Order/Purchase Request) accrual processing. It handles monthly financial data reconciliation for three business entities: MOB, SPT, and SPX.

## Common Commands

```bash
# Activate virtual environment (Windows PowerShell)
./venv/Scripts/activate

# Install dependencies
python -m pip install -r requirements.txt
# Or use pyproject.toml
python -m pip install .

# Run the main pipeline
python main_pipeline.py

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

### Three-Layer Architecture

The codebase follows a three-layer architecture pattern inspired by the SPE Bank Reconciliation reference project:

- **core/**: Framework and reusable components
  - Pipeline infrastructure (Pipeline, PipelineStep, ProcessingContext)
  - Base classes with template method pattern (BaseLoadingStep, BaseERMEvaluationStep)
  - Common steps shared across entities
  - Data sources abstraction layer

- **tasks/**: Entity-specific implementations
  - `tasks/spt/`: SPT-specific steps and pipeline orchestrator
  - `tasks/spx/`: SPX-specific steps and pipeline orchestrator
  - `tasks/mob/`: MOB-specific steps and pipeline orchestrator
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

Three entity types with configuration-driven pipeline orchestration:

- **SPT** ([tasks/spt/](accrual_bot/tasks/spt/)):
  - PO Pipeline: SPTDataLoading → CommissionDataUpdate → PayrollDetection → SPTERMLogic → SPTStatusLabel → SPTAccountPrediction
  - PR Pipeline: SPTPRDataLoading → CommissionDataUpdate → PayrollDetection → SPTERMLogic → SPTStatusLabel → SPTAccountPrediction
  - Advanced features: commission handling, payroll detection, account prediction rules

- **SPX** ([tasks/spx/](accrual_bot/tasks/spx/)):
  - PO Pipeline: SPXDataLoading → ColumnAddition → ClosingListIntegration → StatusStage1 → SPXERMLogic → DepositStatusUpdate → ValidationDataProcessing → SPXExport
  - PR Pipeline: SPXPRDataLoading → ColumnAddition → StatusStage1 → SPXPRERMLogic → SPXPRExport
  - Complex processing: 11-condition status evaluation, deposit/rental identification, locker/kiosk asset validation

- **MOB** ([tasks/mob/](accrual_bot/tasks/mob/)):
  - Standard processing with basic status determination

Pipeline steps can be enabled/disabled via configuration in [config/stagging.toml](accrual_bot/config/stagging.toml):
```toml
[pipeline.spt]
enabled_po_steps = ["SPTDataLoading", "CommissionDataUpdate", ...]
```

### Data Sources (core/datasources/)

Unified async data access layer supporting:
- Excel (ExcelSource)
- CSV (CSVSource)
- Parquet (ParquetSource)
- DuckDB (DuckDBSource)

All sources implement the same interface with thread-safe operations and shared thread pools.

### Configuration

- **config/config.ini**: Legacy INI configuration (general settings, regex patterns, credentials)
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

## Testing

The project uses pytest with async support. Test structure:

```
tests/
├── conftest.py                    # Shared fixtures (mock_config_manager, processing_context, etc.)
├── pytest.ini                     # pytest configuration with markers
├── fixtures/sample_data.py        # Test data generators
├── unit/                          # Unit tests (@pytest.mark.unit)
│   ├── core/pipeline/steps/       # BaseLoadingStep, BaseERMEvaluationStep tests
│   ├── tasks/spt/                 # SPT Orchestrator tests
│   ├── tasks/spx/                 # SPX Orchestrator tests
│   └── utils/config/              # ConfigManager thread-safety tests
└── integration/                   # Integration tests (@pytest.mark.integration)
```

**Coverage targets**: Overall ≥80%, ConfigManager 100%, Base Classes ≥85%, Orchestrators ≥90%

**Key fixtures** (from `conftest.py`):
- `mock_config_manager`: Mocks ConfigManager with pipeline configuration
- `processing_context`: ProcessingContext with sample DataFrame and auxiliary data
- `mock_data_source_factory`: AsyncMock for data source operations

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

### Benefits
- **Maintainability**: Single source of truth for shared logic reduces bug surface area
- **Extensibility**: New entities can be added without modifying core framework
- **Testability**: Template methods enable focused unit testing of entity-specific hooks
- **Safety**: Thread-safe configuration eliminates race conditions in concurrent environments

## File Structure Notes

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

## Language

Code comments and documentation are in Traditional Chinese (繁體中文). Use Chinese for user-facing strings and comments.
