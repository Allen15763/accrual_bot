# Accrual Bot 測試指南

## 概述

本項目使用 pytest 測試框架，包含 **779 測試**（661 unit + 12 integration + 106 unmarked），
覆蓋 core pipeline、data sources、tasks（SPT/SPX/SCT）、utilities、UI 等模組。

> **注意**：`duckdb_manager` 和 `metadata_builder` 的測試已隨模組提取至獨立套件（[seafin-duckdb-manager](https://github.com/Allen15763/seafin-duckdb-manager)、[seafin-metadata-builder](https://github.com/Allen15763/seafin-metadata-builder)）。

## 測試結構

```
tests/
├── conftest.py                              # 共用 fixtures（mock_config_manager 等）
├── pytest.ini                               # pytest 配置與 markers
├── fixtures/
│   ├── sample_data.py                       # 基本測試資料
│   └── test_data_generators.py              # 合成資料產生器
├── unit/                                    # 單元測試（@pytest.mark.unit）
│   ├── conftest.py                          # unit 層級共用 fixtures
│   ├── core/
│   │   ├── conftest.py                      # core 層級 fixtures
│   │   ├── pipeline/
│   │   │   ├── test_context.py              # ProcessingContext 測試
│   │   │   ├── test_base_classes.py         # PipelineStep / StepResult 測試
│   │   │   ├── test_pipeline.py             # Pipeline 執行測試
│   │   │   ├── test_pipeline_builder.py     # PipelineBuilder fluent API 測試
│   │   │   ├── test_checkpoint.py           # CheckpointManager 測試
│   │   │   └── steps/
│   │   │       ├── test_base_loading.py     # BaseLoadingStep 測試
│   │   │       ├── test_base_evaluation.py  # BaseERMEvaluationStep 測試
│   │   │       ├── test_previous_workpaper.py # 前期底稿步驟測試
│   │   │       ├── test_common_steps.py     # 共用步驟測試
│   │   │       ├── test_business_steps.py   # 業務邏輯步驟測試
│   │   │       └── test_post_processing.py  # 後處理步驟測試
│   │   └── datasources/
│   │       ├── test_datasource_config.py    # DataSourceConfig 測試
│   │       ├── test_datasource_factory.py   # DataSourceFactory 測試
│   │       ├── test_csv_source.py           # CSVSource 測試
│   │       ├── test_excel_source.py         # ExcelSource 測試
│   │       └── test_parquet_source.py       # ParquetSource 測試
│   ├── tasks/
│   │   ├── conftest.py                      # Task 共用 fixtures（ERM DF 產生器）
│   │   ├── spt/
│   │   │   ├── test_spt_orchestrator.py     # SPT Orchestrator 測試
│   │   │   ├── test_spt_loading.py          # SPT 資料載入測試
│   │   │   ├── test_spt_evaluation_erm.py   # SPT ERM 評估測試
│   │   │   └── test_spt_account_prediction.py # SPT 科目預測測試
│   │   ├── spx/
│   │   │   ├── test_spx_orchestrator.py     # SPX Orchestrator 測試
│   │   │   ├── test_spx_loading.py          # SPX 資料載入測試
│   │   │   ├── test_spx_condition_engine.py # SPX 條件引擎測試
│   │   │   ├── test_spx_evaluation.py       # SPX 評估步驟測試
│   │   │   └── test_spx_ppe_steps.py        # SPX PPE 步驟測試
│   │   └── sct/
│   │       ├── test_sct_evaluation.py       # SCT ERM 評估測試
│   │       ├── test_sct_asset_status.py     # SCT 資產狀態測試
│   │       ├── test_sct_account_prediction.py # SCT 科目預測測試
│   │       └── test_sct_post_processing.py  # SCT 後處理測試
│   ├── utils/
│   │   ├── config/
│   │   │   └── test_config_manager.py       # ConfigManager 執行緒安全測試
│   │   ├── helpers/
│   │   │   ├── test_column_utils.py         # ColumnResolver 測試
│   │   │   ├── test_data_utils.py           # 資料工具函式測試
│   │   │   └── test_file_utils.py           # 檔案工具函式測試
│   │   └── logging/
│   │       └── test_logger.py               # Logger 單例 / 執行緒安全測試
│   ├── ui/
│   │   ├── services/
│   │   │   ├── test_unified_pipeline_service.py # UnifiedPipelineService 測試
│   │   │   └── test_file_handler.py         # FileHandler 測試
│   │   └── models/
│   │       └── test_state_models.py         # UI 狀態模型測試
│   └── data/
│       └── importers/
│           └── test_base_importer.py        # BaseDataImporter 測試
└── integration/
    ├── test_pipeline_orchestrators.py       # Pipeline 端對端測試
    └── test_checkpoint_roundtrip.py         # Checkpoint 存取還原測試
```

## 安裝測試依賴

```bash
# 啟動虛擬環境
./venv/Scripts/activate  # Windows

# 安裝開發依賴
python -m pip install -e ".[dev]"
```

## 運行測試

### 使用腳本（推薦）

```bash
# Windows
.\run_tests.bat              # 全部測試
.\run_tests.bat unit         # 僅 unit
.\run_tests.bat integration  # 僅 integration
.\run_tests.bat coverage     # 含覆蓋率報告

# Linux / macOS（若有 scripts/ 目錄）
bash scripts/run_all.sh
bash scripts/run_unit.sh
bash scripts/run_coverage.sh
```

### 使用 pytest 命令

```bash
# 全部測試
python -m pytest tests/ -v

# 依標記運行
python -m pytest tests/ -v -m unit
python -m pytest tests/ -v -m integration

# 指定檔案
python -m pytest tests/unit/core/pipeline/test_context.py -v

# 指定類別
python -m pytest tests/unit/tasks/spt/test_spt_orchestrator.py::TestSPTPipelineOrchestrator -v

# 覆蓋率報告
python -m pytest tests/ --cov=accrual_bot --cov-report=html --cov-report=term-missing

# 覆蓋率門檻
python -m pytest tests/ --cov=accrual_bot --cov-fail-under=80
```

查看 HTML 覆蓋率報告：

```bash
# Windows
start htmlcov\index.html
```

## 覆蓋率概況

| 模組 | 覆蓋率 | 說明 |
|------|--------|------|
| `utils/config/config_manager.py` | 100% | 執行緒安全單例 |
| `core/pipeline/context.py` | ~95% | ProcessingContext |
| `core/pipeline/base.py` | ~85% | PipelineStep 基礎類別 |
| `core/pipeline/pipeline.py` | ~80% | Pipeline 執行引擎 |
| `core/pipeline/checkpoint.py` | ~77% | Checkpoint 管理 |
| `utils/helpers/column_utils.py` | ~90% | 欄位解析工具 |
| `utils/helpers/data_utils.py` | ~85% | 資料工具函式 |
| `utils/logging/logger.py` | ~80% | 日誌框架 |
| `tasks/spt/pipeline_orchestrator.py` | ~90% | SPT 編排器 |
| `tasks/spx/pipeline_orchestrator.py` | ~90% | SPX 編排器 |

## 測試標記

| 標記 | 說明 |
|------|------|
| `@pytest.mark.unit` | 單元測試（快速，無外部依賴） |
| `@pytest.mark.integration` | 集成測試（可能需要真實檔案） |
| `@pytest.mark.slow` | 執行時間較長的測試 |
| `@pytest.mark.asyncio` | 異步測試（自動由 pytest-asyncio 處理） |

## 關鍵 Fixtures

### 全域（`tests/conftest.py`）

| Fixture | 說明 |
|---------|------|
| `mock_config_manager` | Mock ConfigManager，含 pipeline 配置 |
| `processing_context` | 帶範例 DataFrame 和輔助資料的 ProcessingContext |
| `mock_data_source_factory` | AsyncMock 資料來源工廠 |
| `empty_context` | 空的 ProcessingContext |
| `spx_processing_context` | SPX 專用處理上下文 |
| `spt_processing_context` | SPT 專用處理上下文 |
| `tmp_checkpoint_dir` | 臨時 checkpoint 目錄 |

### Unit 層級（`tests/unit/conftest.py`）

| Fixture | 說明 |
|---------|------|
| `dummy_success_step` | 回傳 SUCCESS 的具體步驟 |
| `dummy_fail_step` | 回傳 FAILED 的具體步驟 |
| `dummy_skip_step` | 回傳 SKIPPED 的具體步驟 |

### Task 層級（`tests/unit/tasks/conftest.py`）

| Fixture | 說明 |
|---------|------|
| `spx_erm_df` | SPX ERM 測試用 DataFrame（含 Liability 等欄位） |
| `spt_erm_df` | SPT ERM 測試用 DataFrame（含 Liability 等欄位） |
| `mock_config_for_steps` | 步驟測試用的 Mock 配置 |

## 測試資料策略

- **Unit tests**: 使用 `fixtures/sample_data.py` 和 `fixtures/test_data_generators.py` 的合成資料，確保快速且確定性
- **Integration tests**: 使用 `tests/test_data/202602/` 的真實檔案，以 `pytest.skip()` 保護不存在的情況
- **tmp_path**: 大量使用 pytest 的 `tmp_path` fixture 建立臨時檔案（Excel、CSV、Parquet、TOML 等）

## 編寫新測試

### 單元測試範例

```python
import pytest
from accrual_bot.your_module import YourClass


@pytest.mark.unit
class TestYourClass:
    """YourClass 測試"""

    def test_basic(self):
        result = YourClass().method()
        assert result == expected_value

    @pytest.mark.asyncio
    async def test_async_method(self):
        result = await YourClass().async_method()
        assert result is not None
```

### 使用共用 Fixtures

```python
def test_with_context(processing_context):
    """使用 conftest.py 中定義的 fixture"""
    assert processing_context.entity_type == 'SPT'
    assert len(processing_context.data) > 0
```

## 最佳實踐

1. **測試命名**: 使用描述性名稱，例如 `test_build_po_pipeline_with_config`
2. **測試隔離**: 每個測試應獨立運行，不依賴其他測試的執行順序
3. **使用 Fixtures**: 利用 pytest fixtures 與 conftest.py 減少重複代碼
4. **Mock 外部依賴**: 使用 `unittest.mock` 或 `pytest-mock` mock 外部服務與 I/O
5. **參數化測試**: 使用 `@pytest.mark.parametrize` 測試多種輸入組合
6. **異步測試**: 使用 `@pytest.mark.asyncio` 標記，搭配 `asyncio_mode = "auto"` 配置
7. **Windows 編碼**: 寫入含中文的 TOML 測試檔時，使用 `write_bytes(content.encode('utf-8'))` 避免 cp950 問題

## 常見問題

### 1. 找不到模塊

確保已安裝開發依賴並在虛擬環境中運行：

```bash
./venv/Scripts/activate
python -m pip install -e ".[dev]"
```

### 2. 異步測試錯誤

確保安裝了 `pytest-asyncio`：

```bash
python -m pip install pytest-asyncio>=0.23.0
```

### 3. TOML 測試的 UnicodeDecodeError

在 Windows 上，`Path.write_text()` 預設使用 cp950 編碼。測試中寫入 TOML 檔請使用：

```python
toml_file.write_bytes(toml_content.encode("utf-8"))
```

### 4. PyArrow Schema API 差異

`ParquetFile.schema` 回傳 `ParquetSchema`，非 `pa.Schema`。取得 Arrow schema 需呼叫 `.to_arrow_schema()`。
測試中避免 `isinstance(schema, pa.Schema)` 斷言。
