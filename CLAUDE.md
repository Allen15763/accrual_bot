# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Accrual Bot is an async data processing system for PO/PR (Purchase Order/Purchase Request) accrual processing. It handles monthly financial data reconciliation for three active business entities: SPT, SPX, and SCT. The system includes both a command-line pipeline execution mode and a Streamlit-based Web UI.

## Common Commands

### Windows PowerShell

```powershell
# Activate virtual environment
./venv/Scripts/activate

# Install dependencies
python -m pip install -r requirements.txt
python -m pip install .            # Or use pyproject.toml
python -m pip install ".[ui]"      # With UI dependencies

# Run the main pipeline (CLI mode)
python main_pipeline.py

# Run Streamlit UI
streamlit run main_streamlit.py

# Run tests
python -m pytest tests/
python -m pytest tests/ -m unit          # Unit tests only
python -m pytest tests/ -m integration   # Integration tests only
python -m pytest tests/ --cov=accrual_bot --cov-report=html

# Type checking & formatting
python -m mypy accrual_bot/
python -m black .
```

### WSL (Claude Code 環境)

> **注意**：專案使用 Windows venv (Python 3.11)。WSL 系統的 Python 3.10 缺少 `tomllib`，無法執行專案。
> `source venv/Scripts/activate` 因 CRLF 行尾而失敗，須直接呼叫 `.exe`。
> `scripts/*.sh` 同樣有 CRLF 問題，不可直接執行。

```bash
# 直接使用 Windows venv 的 python.exe（不需 activate）
./venv/Scripts/python.exe -m pip install -r requirements.txt

# Run the main pipeline
./venv/Scripts/python.exe main_pipeline.py

# Run Streamlit UI
./venv/Scripts/streamlit.exe run main_streamlit.py

# Run tests
./venv/Scripts/python.exe -m pytest tests/
./venv/Scripts/python.exe -m pytest tests/ -m unit
./venv/Scripts/python.exe -m pytest tests/ -m integration
./venv/Scripts/python.exe -m pytest tests/ --cov=accrual_bot --cov-report=html

# Type checking & formatting
./venv/Scripts/python.exe -m mypy accrual_bot/
./venv/Scripts/python.exe -m black .

# Git（WSL git 可正常使用）
git status
git diff
git log --oneline -10
```

## Architecture

### Four-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    UI Layer (Streamlit)                      │
│  pages/ → components/ → services/ → Session State           │
├─────────────────────────────────────────────────────────────┤
│                    Tasks Layer (Orchestrators)               │
│  tasks/spt/ | tasks/spx/ | tasks/sct/ | tasks/common/       │
├─────────────────────────────────────────────────────────────┤
│                    Core Layer (Framework)                    │
│  Pipeline | PipelineStep | ProcessingContext | DataSources   │
├─────────────────────────────────────────────────────────────┤
│                    Utils Layer (Cross-cutting)               │
│  ConfigManager | Logger | Data Utilities                     │
└─────────────────────────────────────────────────────────────┘
```

- **ui/**: Streamlit Web UI — 5-page workflow (Configuration → Upload → Execution → Results → Checkpoint). See [doc/UI_Architecture.md](doc/UI_Architecture.md).
- **core/**: Framework — Pipeline, PipelineStep, ProcessingContext, BaseLoadingStep, BaseERMEvaluationStep, DataSources abstraction layer.
- **tasks/**: Entity-specific implementations — SPT (PO/PR/PROCUREMENT), SPX (PO/PR/PPE/PPE_DESC), SCT (PO/PR). Each entity has a `pipeline_orchestrator.py` and `steps/` directory.
- **utils/**: Cross-cutting — Thread-safe ConfigManager singleton, unified logging (`get_logger()`), data/file/column utilities.

### Pipeline System

Step-based async pipeline with template method pattern:

- **PipelineStep**: Abstract base class. Implements `execute()` and `validate_input()`.
- **BaseLoadingStep**: Template for data loading. Hooks: `get_required_file_type()`, `_load_primary_file()`, `_load_reference_data()`.
- **BaseERMEvaluationStep**: Template for ERM evaluation. Hooks: `_build_conditions()`, `_apply_status_conditions()`, `_set_accounting_fields()`.
- **ProcessingContext**: Carries data (DataFrame + auxiliary data + variables) between steps. `processing_date` source of truth: `context.metadata.processing_date`.
- **PipelineBuilder**: Fluent interface for constructing pipelines.
- **CheckpointManager**: Save/resume pipeline execution from specific steps.

Pipeline flow: `DataLoading → Filtering → ColumnAddition → Integration → BusinessLogic → PostProcessing → Export`

### Configuration

Seven config files in `accrual_bot/config/`:

| File | Purpose |
|------|---------|
| `config.ini` | Legacy INI (general settings, regex, credentials) |
| `paths.toml` | File paths and read parameters per entity/type |
| `paths.local.toml` | Local path overrides (gitignored, see `.example`) |
| `run_config.toml` | Runtime execution config (processing_date, verbose) |
| `stagging.toml` | Shared config (reference paths, date patterns, categories) |
| `stagging_spt.toml` | SPT pipeline steps, status/account rules, pivot config |
| `stagging_spx.toml` | SPX pipeline steps, supplier lists, condition rules |
| `stagging_sct.toml` | SCT pipeline steps, ERM rules, account prediction, reformatting |

Config accessed via thread-safe singleton `ConfigManager` (double-checked locking). Steps enabled/disabled via `enabled_*_steps` arrays in entity TOML files.

## Key Patterns

### Using Pipeline Orchestrators (Recommended)

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.tasks.sct import SCTPipelineOrchestrator

# Create pipelines
spt_orchestrator = SPTPipelineOrchestrator()
pipeline = spt_orchestrator.build_po_pipeline(file_paths={'po_file': 'path/to/po.xlsx'})

spx_orchestrator = SPXPipelineOrchestrator()
pipeline = spx_orchestrator.build_pr_pipeline(file_paths={'pr_file': 'path/to/pr.xlsx'})

sct_orchestrator = SCTPipelineOrchestrator()
pipeline = sct_orchestrator.build_po_pipeline(file_paths={'raw_po': 'path/to/po.xlsx'})

# SPT PROCUREMENT pipeline (PO/PR/COMBINED)
pipeline = spt_orchestrator.build_procurement_pipeline(
    file_paths={'raw_po': 'path/to/po.xlsx'}, source_type='PO'
)

# Get enabled steps
enabled_steps = spt_orchestrator.get_enabled_steps('PO')
```

### Creating a New Step

**Data loading** — inherit `BaseLoadingStep`:

```python
from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
import pandas as pd

class MyEntityDataLoadingStep(BaseLoadingStep):
    def get_required_file_type(self) -> str:
        return 'po_file'

    async def _load_primary_file(self, source, path: str) -> pd.DataFrame:
        df = await source.read()
        return df

    async def _load_reference_data(self, context) -> int:
        ref_df = await self._load_reference_file('ref_file')
        context.set_auxiliary_data('reference', ref_df)
        return len(ref_df)
```

**ERM evaluation** — inherit `BaseERMEvaluationStep`:

```python
from accrual_bot.core.pipeline.steps.base_evaluation import BaseERMEvaluationStep, BaseERMConditions

class MyEntityERMLogicStep(BaseERMEvaluationStep):
    def _build_conditions(self, df, file_date, status_column):
        return BaseERMConditions(
            erp_completed=(df['status'] == 'Closed'),
            invalid_format=(df['description'].isna()),
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        df.loc[conditions.erp_completed, status_column] = '已完成'
        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        return df
```

**Generic step** — inherit `PipelineStep`:

```python
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus

class CustomStep(PipelineStep):
    async def execute(self, context: ProcessingContext) -> StepResult:
        # context.data = main DataFrame
        # context.get_auxiliary_data('name'), context.set_variable('key', value)
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

    async def validate_input(self, context: ProcessingContext) -> bool:
        return 'required_column' in context.data.columns
```

### Executing with Checkpoints

```python
from accrual_bot.core.pipeline import execute_pipeline_with_checkpoint

result = await execute_pipeline_with_checkpoint(
    file_paths=file_paths_dict,
    processing_date=202512,  # YYYYMM format
    pipeline_func=create_pipeline_function,
    entity='SPX', processing_type='PO', save_checkpoints=True
)
```

## Extending the System

### Adding a New Processing Type to Existing Entity

| # | File | Changes |
|---|------|---------|
| 1 | `ui/config.py` | Add type to `ENTITY_CONFIG`, `REQUIRED_FILES`, `OPTIONAL_FILES`, `FILE_LABELS` |
| 2 | `config/paths.toml` | Add path and params sections |
| 3 | `config/stagging_<entity>.toml` | Add `enabled_<type>_steps` |
| 4 | `tasks/<entity>/pipeline_orchestrator.py` | Add `build_<type>_pipeline()`, register steps, update `get_enabled_steps()` |
| 5 | `ui/services/unified_pipeline_service.py` | Add elif branch in `build_pipeline()` |

### Adding a New Entity

| # | File | Changes |
|---|------|---------|
| 1 | `tasks/<new>/` | Create `__init__.py`, `pipeline_orchestrator.py`, `steps/*.py` |
| 2 | `config/stagging_<new>.toml` | Pipeline and business rules |
| 3 | `utils/config/config_manager.py` | Add TOML to loading list |
| 4 | `ui/services/unified_pipeline_service.py` | Register in `_get_orchestrator()` |
| 5 | `ui/config.py` | Add to `ENTITY_CONFIG`, `REQUIRED_FILES`, `OPTIONAL_FILES` |
| 6 | `config/paths.toml` | Add path/params sections |
| 7 | `config/stagging.toml` | Add FA accounts, reference paths |

See SCT implementation (`tasks/sct/`) as a complete reference. Detailed guide: [doc/UI_Architecture.md](doc/UI_Architecture.md).

## Testing

779 tests (661 unit + 12 integration + 106 unmarked), 777 passing. 2 pre-existing failures in `test_previous_workpaper.py`. Coverage: 38%.

```bash
# Windows PowerShell（activate 後）
python -m pytest tests/                                      # All tests
python -m pytest tests/ -m unit                              # Unit only
python -m pytest tests/ -m integration                       # Integration only
python -m pytest tests/ --cov=accrual_bot --cov-report=html  # Coverage

# WSL
./venv/Scripts/python.exe -m pytest tests/
./venv/Scripts/python.exe -m pytest tests/ -m unit
```

See [tests/README.md](tests/README.md) for test structure, fixtures, and coverage details.

## Streamlit UI

5-page workflow: Configuration → Upload → Execution → Results → Checkpoint.

Key service: `UnifiedPipelineService` bridges UI and backend pipelines. Dual-layer pages architecture uses emoji filenames in `pages/` (Streamlit requirement) delegating to `accrual_bot/ui/pages/` (actual logic).

**Navigation must use emoji filenames**: `st.switch_page("pages/1_⚙️_配置.py")`

See [doc/UI_Architecture.md](doc/UI_Architecture.md) for detailed UI architecture and extension guide.

## Documentation

| Document | Description |
|----------|-------------|
| `CLAUDE.md` | This file — development guidance |
| `doc/Changelog.md` | Architecture improvements changelog (Phase 1-14) |
| `doc/Project_Design_Reference.md` | Architecture, patterns, templates |
| `doc/UI_Architecture.md` | UI architecture, components, extension guide |
| `doc/SPT_PROCUREMENT_Implementation.md` | SPT Procurement pipeline details |
| `doc/Unified_System_Design_Reference.md` | Unified system design overview |
| `doc/SPE_Project_Architecture_Reference.md` | SPE project architecture reference |
| `doc/Project_Review_And_Merger_Analysis.md` | Project review and merger analysis |
| `doc/Task Pipeline Structure Unit Test Plan.md` | Test plan for pipeline structure |
| `tests/README.md` | Test suite guide (779 tests) |

## Language

Code comments and documentation are in Traditional Chinese (繁體中文). Use Chinese for user-facing strings and comments.
