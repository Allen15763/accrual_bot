# Architecture Improvements Changelog

本文件記錄 Accrual Bot 自 2026 年 1 月起的架構重構與改進歷程。

## Phase 1: Critical Fixes (P0)
- **Thread-Safe ConfigManager**: Fixed race condition in singleton pattern using double-checked locking
- **Unified Logging**: Migrated all pipeline modules to use consistent `get_logger()` framework

## Phase 2: Code Deduplication (P1)
- **BaseLoadingStep**: Extracted ~400-500 lines of duplicated code from SPT/SPX loading steps using template method pattern
- **BaseERMEvaluationStep**: Extracted ~300-400 lines of duplicated ERM evaluation logic
- **Impact**: Eliminated ~750 lines of duplication (~5% code reduction)

## Phase 3: Structure & Extensibility (P2)
- **Tasks Directory**: Created entity-specific modules under `tasks/` (spt, spx, common)
- **Pipeline Orchestrators**: Implemented configuration-driven step loading via `SPTPipelineOrchestrator` and `SPXPipelineOrchestrator`
- **Backward Compatibility**: All existing imports continue to work via re-exports

## Phase 4: Streamlit UI (January 2026)
- **5-Page Workflow**: Configuration → Upload → Execution → Results → Checkpoint
- **Service Layer**: UnifiedPipelineService decouples UI from pipeline implementation
- **Async Bridge**: Handles sync/async conversion for Streamlit compatibility
- **Configuration-Driven**: UI content driven by `ui/config.py` and `paths.toml`

## Phase 5: UI Optimization & Cleanup (2026-01-17)
- **Removed Deprecated Template System**: Deleted `template_picker.py` and cleaned template-related code from 7 files (~150 lines)
- **Cleaned Duplicate Pages**: Removed 5 redundant `*_page.py` files (~400 lines)
- **Added Log Export**: Execution page now allows downloading logs as `.txt` files
- **Fixed Dual-Layer Pages**: Corrected Entry Point files to use `exec()` instead of imports
- **Fixed ProcessingContext**: Added `auxiliary_data` property for UI access
- **Impact**: Removed ~558 lines of code (~22% reduction in UI layer)

## Phase 6: Comprehensive Test Suite (2026-03)
- **779 tests**: 661 unit + 12 integration + 106 unmarked, covering core pipeline, data sources, tasks, utilities, and UI
- **Three-phase rollout**: P0 core infrastructure → P1 business logic → P2 extended coverage
- **Scripts**: `scripts/` directory with `.sh`/`.bat` pairs for running unit, integration, coverage, and quick tests
- **Test data generators**: Synthetic data factories in `tests/fixtures/test_data_generators.py`

## Phase 7: Entity Config Split & Procurement Pipeline (2026-03)
- **Config split**: `stagging.toml` split into `stagging_spt.toml` and `stagging_spx.toml` for entity-specific configuration
- **SPT PROCUREMENT**: New pipeline type for procurement staff (PO/PR/COMBINED variants)
- **SPX PPE_DESC**: New pipeline type for PO/PR description extraction with contract period mapping

## Phase 8: Utils Layer Bug Fixes (2026-03-14)
- **metadata_builder (3 fixes)** *(now in standalone package `seafin-metadata-builder`)*: `validate_only()` silent-pass bug; `cast_failures` counted pre-existing NULLs; `ColumnMappingError` defined but never raised
- **duckdb_manager (5 fixes)** *(now in standalone package `seafin-duckdb-manager`)*: `clean_and_convert_column()` ignored validation; manual string escaping instead of `SafeSQL`; `execute_transaction()` bypassed `_rollback()`; `connection_timeout` dead config removed

## Phase 9: SPX Tasks Bug Fixes (2026-03-17)
- **Orphaned steps (Fix 1)**: 6 legacy prototype steps in `spx_steps.py` (`SPXDepositCheckStep` etc.) removed from `__all__` and marked deprecated in docstrings — were in public API but absent from orchestrator step registry
- **Hardcoded Sheets ID (Fix 2)**: `ClosingListIntegrationStep` Spreadsheet ID and sheet name list moved to `stagging_spx.toml` (`closing_list_spreadsheet_id`, `closing_list_sheet_names`, `closing_list_sheet_range`) — adding a new year now requires only a TOML edit
- **PPE pipeline consistency (Fix 3)**: `build_ppe_pipeline()` and `build_ppe_desc_pipeline()` refactored to config-driven pattern; `enabled_ppe_steps`/`enabled_ppe_desc_steps` added to TOML; PPE step factory entries added to `_create_step()`
- **Implicit coupling (Fix 4)**: `ColumnAdditionStep` replaced `'raw_pr' in file_paths.keys()` detection with `context.metadata.processing_type == 'PR'` — eliminates silent failure if key name changes
- **Documentation mismatch (Fix 5)**: `spx_evaluation_2.py` module docstring path corrected; `DepositStatusUpdateStep` default name unified to `"DepositStatusUpdate"` (matches orchestrator registry key)
- **Async blocking (Fix 6)**: `PPEDataLoadingStep._load_renewal_list()` three synchronous `get_sheet_data()` calls wrapped with `asyncio.to_thread()` — releases event loop during Google Sheets API I/O

## Phase 10: SPT Tasks Bug Fixes (2026-03-17)
- **KeyError guard (Fix 1)**: `SPTPostProcessingStep._rearrange_reviewer_col()` — added column existence check before `df.pop()`; missing columns now log a warning and return early instead of raising `KeyError`
- **DRY refactoring (Fix 5, includes Fix 2)**: `spt_loading.py` rewritten from 1164 → 182 lines (84% reduction) — introduced `SPTBaseDataLoadingStep(BaseLoadingStep)` with four template hooks; `SPTDataLoadingStep` and `SPTPRDataLoadingStep` each reduced to `get_required_file_type()` declarations; `df.rename()` silent bug fixed via `BaseLoadingStep._process_common_columns()` which already assigns correctly
- **IndexError guard (Fix 3)**: `PayrollDetectionStep.execute()` — replaced unsafe `[0]` list index on status column lookup with guard block; empty match now logs warning and skips update gracefully
- **Logger in orchestrator (Fix 4)**: `SPTPipelineOrchestrator.__init__` now initializes `self.logger = get_logger(__name__)`; unknown-step warning replaced `print()` with `self.logger.warning()`
- **Orphaned steps (Fix 6)**: 4 legacy prototype steps (`SPTStatusStep`, `SPTDepartmentStep`, `SPTAccrualStep`, `SPTValidationStep`) removed from `spt/steps/__init__.py` `__all__`; deprecation docstrings added to each class
- **Config-driven commission/payroll (Fix 7)**: `COMMISSION_CONFIG` and `PAYROLL_CONFIG` business rules moved to `stagging_spt.toml` under `[spt.commission.affiliate]`, `[spt.commission.ams]`, `[spt.payroll]`; `__init__` reads from config with class-level constants as fallback
- **Sub-context metadata propagation (Fix 8)**: `CombinedProcurementProcessingStep._process_po_data()` and `_process_pr_data()` now fully propagate `entity_type`, `processing_type`, and `processing_date` from parent context to sub-context

## Phase 11: Runner Module Bug Fixes (2026-03-17)
- **AttributeError prevention (Fix 1, P0)**: `load_file_paths()` now performs early entity/type validation with explicit `ValueError` (message lists available entities/types), replacing the silent `AttributeError: 'NoneType' object has no attribute 'get'` when an unknown entity or type is passed
- **Dead code removal (Fix 2, P1)**: `StepByStepExecutor._save_checkpoint()` — removed 5-line redundant `checkpoint_name` construction; now uses the return value of `save_checkpoint()` directly, ensuring the debug log matches the actual file saved on disk
- **Verbose mode implemented (Fix 3, P3)**: `RunConfig.verbose = True` now sets `logging.getLogger('accrual_bot')` to `DEBUG` level — the previously-documented but unimplemented feature now works
- **Glob multi-match warning (Fix 4, P2)**: When a wildcard path matches multiple files, a `WARNING` log now names the selected file and lists the excluded ones — silent lexicographic selection still applies but is no longer invisible
- **Template unresolved-variable detection (Fix 5, P2)**: `_resolve_path_template()` uses `re.findall` to detect leftover `{VAR}` tokens after substitution; typos like `{YYYMM}` now emit a `WARNING` with the original template, instead of silently passing a malformed path to downstream I/O
- **Local path override (Fix 7, P2)**: `load_file_paths()` deep-merges a gitignored `accrual_bot/config/paths.local.toml` when present; `paths.local.toml.example` committed as a template; `_deep_merge()` helper added; `.gitignore` updated — eliminates hardcoded Windows paths for cross-machine use
- **Backward-compat functions accept date (Fix 8, P3)**: `run_spx_po_full_pipeline()`, `run_spx_pr_full_pipeline()`, `run_spt_po_full_pipeline()`, `run_spt_pr_full_pipeline()` all accept `processing_date: int = 202512` — callers can now specify a month without editing source code
- **Result dict alignment (Fix 9, P1)**: Normal execution path adds `result.setdefault("aborted", False)` after `pipeline.execute()`, aligning with `StepByStepExecutor`'s result structure and eliminating `KeyError` for callers that check `result["aborted"]`
- **Dead branch removed (Fix 10, P3)**: `_convert_params()` `elif key == "keep_default_na"` branch deleted — it was identical to the `else` branch and TOML already parses booleans natively

## Phase 12: SCT Entity Pipeline (2026-03)
- **New entity: SCT** — Third business entity added with full PO/PR pipeline support (10 PO steps, 8 PR steps)
- **Files created**: `tasks/sct/` module with `SCTPipelineOrchestrator`, `SCTBaseDataLoadingStep` (BaseLoadingStep template), `SCTColumnAdditionStep` (SPX variant without cumulative receipt), `config/stagging_sct.toml` (894 lines)
- **Key differences from SPT/SPX**: Raw data format is xlsx (not CSV); reference data from `ref_SCTTW.xlsx`; no ProductFilter; SCT-specific ColumnAddition
- **Reused steps**: `APInvoiceIntegrationStep`, `PreviousWorkpaperIntegrationStep`, `ProcurementIntegrationStep`, `DateLogicStep` from core
- **SCT-specific steps**: `SCTERMLogicStep` (18 PO status rules), `SCTPRERMLogicStep` (13 PR status rules), `SCTAssetStatusUpdateStep` (PPE asset detection, PO only), `SCTAccountPredictionStep` (18 account prediction rules), `SCTPostProcessingStep` (config-driven reformatting and output selection)
- **Config updates**: `config_manager.py` loads `stagging_sct.toml`; `paths.toml` has `[sct.po]`/`[sct.pr]` sections; `stagging.toml` has `ref_path_sct` under `[paths]` and `sct` under `[fa_accounts]`
- **UI integration**: SCT registered in `ENTITY_CONFIG`, `REQUIRED_FILES`, `OPTIONAL_FILES`, and `UnifiedPipelineService._get_orchestrator()`
- **Tests**: 105 unit tests across 4 test files (evaluation, asset status, account prediction, post-processing)

## Phase 13: Plugin Module Extraction to Standalone Packages (2026-03-27)
- **Extracted `duckdb_manager`** → standalone GitHub package [`seafin-duckdb-manager`](https://github.com/Allen15763/seafin-duckdb-manager) (v2.1.0)
- **Extracted `metadata_builder`** → standalone GitHub package [`seafin-metadata-builder`](https://github.com/Allen15763/seafin-metadata-builder) (v1.0.0)
- **Rationale**: Both modules had zero business logic coupling to the host project; different dependency trees (`duckdb` vs `numpy+openpyxl`); independent version lifecycles
- **Changes**: Removed `accrual_bot/utils/duckdb_manager/` (18 files) and `accrual_bot/utils/metadata_builder/` (13 files) from the project; cleaned `utils/__init__.py` re-exports; removed corresponding test directories (51 tests moved to standalone repos)
- **Installation** (if needed): `pip install "seafin-duckdb-manager @ git+https://github.com/Allen15763/seafin-duckdb-manager.git@v2.1.0"` / `pip install "seafin-metadata-builder @ git+https://github.com/Allen15763/seafin-metadata-builder.git@v1.0.0"`
- **Impact**: Test count reduced from 830 → 779; no business logic tests affected

## Phase 14: Unified processing_date Source (2026-03-27)
- **Problem**: `processing_date` had two unsynchronized propagation paths — `context.metadata.processing_date` (set at ProcessingContext creation from UI/CLI) vs `context.variables['processing_date']` (set by loading steps from filename regex `(\d{6})`). If filename lacked YYYYMM, variable would fallback to `datetime.now()`, diverging from user intent.
- **Solution**: Unified to `context.metadata.processing_date` as single source of truth. CLI uses `run_config.toml`; UI uses user-selected date. Filenames with or without YYYYMM no longer affect task execution.
- **API change**: `_load_primary_file()` and `_extract_primary_data()` return type simplified from `Tuple[pd.DataFrame, int, int]` to `pd.DataFrame` — date/month no longer part of the loading API
- **Loading steps** (`base_loading.py`, `spt_loading.py`, `sct_loading.py`, `spt_procurement_loading.py`, `spx_loading.py`, `spt_combined_procurement_loading.py`): Removed filename date extraction from main flow; `_extract_date_from_filename()` marked deprecated but kept for backward compat
- **Consumer steps** (~11 files): Changed from `context.get_variable('processing_date')` to `context.metadata.processing_date`; `validate_input()` checks unified to `if not processing_date:` (catches both `None` and `0`)
- **Backward compat**: Variables `processing_date` and `processing_month` still set in loading steps (values sourced from metadata) for any code reading them
- **Impact**: ~21 files modified; test suite 777 passed (2 pre-existing failures unchanged)

## Benefits

- **Maintainability**: Single source of truth for shared logic reduces bug surface area
- **Extensibility**: New entities/types can be added via configuration + orchestrator updates
- **Testability**: Template methods enable focused unit testing of entity-specific hooks
- **Safety**: Thread-safe configuration eliminates race conditions in concurrent environments
- **Usability**: Web UI provides guided workflow for non-technical users
