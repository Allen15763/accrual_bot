# Accrual Bot — 專案設計參考文件

> **目的**：本文件將 Accrual Bot 的架構知識、設計決策與實作模式彙整為一份可供**下一個類似專案快速參考**的設計藍圖。閱讀對象為需要設計新資料處理系統的開發者。

---

## 目錄

1. [專案定性與適用場景](#1-專案定性與適用場景)
2. [四層架構全貌](#2-四層架構全貌)
3. [目錄結構](#3-目錄結構)
4. [核心抽象與介面](#4-核心抽象與介面)
5. [設計模式目錄](#5-設計模式目錄)
6. [配置驅動機制](#6-配置驅動機制)
7. [資料流與執行流程](#7-資料流與執行流程)
8. [依賴關係圖](#8-依賴關係圖)
9. [UI 整合策略](#9-ui-整合策略)
10. [擴充指南](#10-擴充指南)
11. [關鍵決策與取捨](#11-關鍵決策與取捨)
12. [可複用的程式碼模板](#12-可複用的程式碼模板)

---

## 1. 專案定性與適用場景

### 系統本質

Accrual Bot 是一個**批次式、多實體、多步驟的非同步資料處理管線系統**，負責每月財務應計數據的對帳與分類。

### 適用場景

當你的新專案符合以下特徵時，可參考本架構：

| 特徵 | 說明 |
|------|------|
| **多實體** | 相同邏輯框架但不同業務規則（如 SPT/SPX 有各自的欄位和判斷條件） |
| **多類型** | 同一實體有多種處理類型（PO/PR/PPE），各類型步驟序列不同 |
| **多步驟** | 處理流程由 10～20 個有序步驟組成，需要中斷點恢復能力 |
| **配置驅動** | 業務規則、步驟序列、檔案路徑需可透過設定檔調整，不改程式碼 |
| **混合使用者** | 同時需要 CLI（技術人員）和 Web UI（非技術人員）入口 |
| **大量重複邏輯** | 不同實體/類型之間有 60-80% 相同的處理骨架 |

---

## 2. 四層架構全貌

```
┌──────────────────────────────────────────────────────────────────┐
│                      第一層：UI 層 (Streamlit)                    │
│                                                                  │
│  pages/          components/       services/       models/       │
│  (5頁工作流)      (可複用元件)      (服務橋接層)    (Session 狀態) │
│                                                                  │
│  1_配置 → 2_上傳 → 3_執行 → 4_結果 → 5_Checkpoint               │
└─────────────────────────────┬────────────────────────────────────┘
                              │ 呼叫 UnifiedPipelineService
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  第二層：任務編排層 (Orchestrators)               │
│                                                                  │
│   tasks/spt/pipeline_orchestrator.py  → SPT 特定步驟序列         │
│   tasks/spx/pipeline_orchestrator.py  → SPX 特定步驟序列         │
│   tasks/common/                      → 共用任務步驟             │
│                                                                  │
│  職責：從 stagging_{entity}.toml 讀取啟用步驟，動態組裝 Pipeline  │
└─────────────────────────────┬────────────────────────────────────┘
                              │ 呼叫 Pipeline / PipelineStep
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  第三層：核心框架層 (Core Framework)              │
│                                                                  │
│  Pipeline          PipelineStep        ProcessingContext          │
│  PipelineBuilder   PipelineConfig      CheckpointManager         │
│                                                                  │
│  步驟基類：                                                       │
│  BaseLoadingStep (~593行)   ← 模板方法：資料載入                  │
│  BaseERMEvaluationStep (~518行) ← 模板方法：業務規則評估          │
│                                                                  │
│  共用步驟（common.py, ~1254行）：                                │
│  DateLogic / AccountMapping / DataIntegration / Filter ...       │
└─────────────────────────────┬────────────────────────────────────┘
                              │ 呼叫 DataSource / ConfigManager
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  第四層：工具層 (Cross-cutting Concerns)          │
│                                                                  │
│  ConfigManager      Logger / StructuredLogger                    │
│  （執行緒安全單例）  （統一日誌框架）                              │
│                                                                  │
│  DataSourceFactory  DataSourcePool  ExcelSource / CSVSource      │
│  （工廠 + 連接池）   （資源管理）     （多格式資料存取）           │
└──────────────────────────────────────────────────────────────────┘
```

### 各層職責邊界

| 層 | 只知道 | 不知道 |
|----|--------|--------|
| UI 層 | 服務層 API、Session State | 具體 Pipeline 步驟實作 |
| 編排層 | 步驟類別、設定檔 | UI 框架細節 |
| 核心框架 | 步驟介面、DataSource | 具體業務實體 (SPT/SPX) |
| 工具層 | 系統資源（檔案/執行緒/日誌） | 業務邏輯 |

---

## 3. 目錄結構

```
project_root/
│
├── main_pipeline.py            # CLI 入口點（技術人員使用）
├── main_streamlit.py           # Web UI 入口點
│
├── pages/                      # Streamlit 多頁導航（entry points，帶 emoji 檔名）
│   ├── 1_⚙️_配置.py            # 轉發至 ui/pages/1_configuration.py
│   ├── 2_📁_檔案上傳.py
│   ├── 3_▶️_執行.py
│   ├── 4_📊_結果.py
│   └── 5_💾_Checkpoint.py
│
├── accrual_bot/                # 主套件
│   │
│   ├── core/                   # ★ 框架核心（不含業務邏輯）
│   │   ├── pipeline/
│   │   │   ├── pipeline.py         # Pipeline, PipelineBuilder, PipelineConfig
│   │   │   ├── base.py             # PipelineStep (ABC), StepResult, StepStatus
│   │   │   ├── context.py          # ProcessingContext（步驟間資料容器）
│   │   │   ├── checkpoint.py       # CheckpointManager
│   │   │   └── steps/
│   │   │       ├── base_loading.py     # ★ BaseLoadingStep（模板方法）
│   │   │       ├── base_evaluation.py  # ★ BaseERMEvaluationStep（模板方法）
│   │   │       └── common.py           # 共用步驟（DateLogic、AccountMapping 等）
│   │   └── datasources/
│   │       ├── base.py             # DataSource (ABC)
│   │       ├── factory.py          # DataSourceFactory
│   │       ├── excel_source.py
│   │       ├── csv_source.py
│   │       ├── parquet_source.py
│   │       └── duckdb_source.py
│   │
│   ├── tasks/                  # ★ 實體特定實作（業務邏輯所在）
│   │   ├── common/
│   │   │   └── data_shape_summary.py      # 共用 DataShapeSummaryStep
│   │   ├── spt/
│   │   │   ├── __init__.py
│   │   │   ├── pipeline_orchestrator.py   # ★ SPTPipelineOrchestrator
│   │   │   └── steps/                     # SPT 特定步驟 (18 檔)
│   │   │       ├── spt_loading.py         # SPTDataLoadingStep, SPTPRDataLoadingStep
│   │   │       ├── spt_evaluation_erm.py  # SPTERMLogicStep
│   │   │       ├── spt_steps.py           # SPTStatusLabelStep, CommissionDataUpdate 等
│   │   │       ├── spt_account_prediction.py
│   │   │       ├── spt_procurement_*.py   # Procurement 系列步驟
│   │   │       ├── spt_combined_procurement_*.py  # Combined Procurement 步驟
│   │   │       └── ...
│   │   └── spx/
│   │       ├── pipeline_orchestrator.py   # ★ SPXPipelineOrchestrator
│   │       └── steps/                     # SPX 特定步驟 (12 檔)
│   │           ├── spx_loading.py         # SPXDataLoadingStep, SPXPRDataLoadingStep
│   │           ├── spx_evaluation.py      # StatusStage1Step, SPXERMLogicStep
│   │           ├── spx_condition_engine.py # SPXConditionEngine
│   │           ├── spx_ppe_desc.py        # PPE_DESC 步驟
│   │           └── ...
│   │
│   ├── ui/                     # Web UI 完整實作
│   │   ├── app.py              # Session state 初始化
│   │   ├── config.py           # ★ UI 配置常數（ENTITY_CONFIG, REQUIRED_FILES 等）
│   │   ├── models/
│   │   │   └── state_models.py # Dataclass 狀態模型
│   │   ├── services/
│   │   │   ├── unified_pipeline_service.py  # ★ UI 橋接層
│   │   │   ├── pipeline_runner.py           # 非同步執行包裝
│   │   │   └── file_handler.py              # 暫存檔案管理
│   │   ├── components/         # 可複用 UI 元件
│   │   │   ├── entity_selector.py
│   │   │   ├── file_uploader.py
│   │   │   ├── progress_tracker.py
│   │   │   ├── step_preview.py
│   │   │   └── data_preview.py
│   │   ├── pages/              # 實際業務邏輯頁面（標準檔名）
│   │   │   ├── 1_configuration.py
│   │   │   ├── 2_file_upload.py
│   │   │   ├── 3_execution.py
│   │   │   ├── 4_results.py
│   │   │   └── 5_checkpoint.py
│   │   └── utils/
│   │       ├── async_bridge.py  # Streamlit sync/async 橋接
│   │       └── ui_helpers.py
│   │
│   ├── utils/                  # 跨切面工具（可直接搬移至新專案）
│   │   ├── config/
│   │   │   ├── config_manager.py   # ★ 執行緒安全設定管理器（單例）
│   │   │   └── constants.py        # 共用常數
│   │   ├── helpers/
│   │   │   ├── column_utils.py     # ColumnResolver
│   │   │   ├── data_utils.py       # TOML 載入、正則模式
│   │   │   └── file_utils.py       # 檔案驗證、複製、雜湊
│   │   ├── logging/
│   │   │   └── logger.py           # ★ 統一日誌框架
│   │   ├── duckdb_manager/         # DuckDB 操作、遷移、設定
│   │   └── metadata_builder/       # Schema 設定、處理器、轉換器
│   │
│   └── config/                 # 設定檔
│       ├── config.ini           # 一般設定、正則表達式、憑證
│       ├── paths.toml           # 檔案路徑模板（含變數替換）
│       ├── run_config.toml      # 執行時組態
│       ├── stagging.toml        # 共用設定（路徑、日期模式、分類模式）
│       ├── stagging_spt.toml    # SPT Pipeline 步驟 + 業務規則
│       └── stagging_spx.toml    # SPX Pipeline 步驟 + 條件引擎規則
│
├── tests/
│   ├── conftest.py
│   ├── pytest.ini
│   ├── fixtures/
│   ├── unit/
│   └── integration/
│
├── doc/                        # 文件
├── checkpoints/                # 執行中斷點（git-ignored）
└── output/                     # 輸出結果（git-ignored）
```

---

## 4. 核心抽象與介面

### 4.1 PipelineStep（所有步驟的基礎）

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any
import pandas as pd

class StepStatus(Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    SKIPPED  = "skipped"
    RETRY    = "retry"

@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    data: Optional[pd.DataFrame] = None
    error: Optional[Exception] = None
    message: Optional[str] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

class PipelineStep(ABC):
    """所有步驟的抽象基類"""

    def __init__(self, name: str, description: str = "", max_retries: int = 0):
        self.name = name
        self.description = description
        self.max_retries = max_retries

    @abstractmethod
    async def execute(self, context: 'ProcessingContext') -> StepResult:
        """核心業務邏輯（子類必須實作）"""
        ...

    @abstractmethod
    async def validate_input(self, context: 'ProcessingContext') -> bool:
        """執行前驗證（子類必須實作）"""
        ...

    async def __call__(self, context: 'ProcessingContext') -> StepResult:
        """完整執行流程：驗證 → 前置 → 執行（含重試）→ 後置"""
        ...
```

### 4.2 ProcessingContext（步驟間資料容器）

```python
@dataclass
class ContextMetadata:
    entity_type: str        # 'SPT' | 'SPX' | 'MOB'
    processing_date: int    # YYYYMM 格式，如 202512
    processing_type: str    # 'PO' | 'PR' | 'PPE'

class ProcessingContext:
    """Pipeline 執行中的共享狀態容器"""

    # 主資料
    data: pd.DataFrame
    metadata: ContextMetadata

    # 輔助資料（參考表、對照清單等）
    _auxiliary_data: Dict[str, pd.DataFrame]
    def add_auxiliary_data(name: str, df: pd.DataFrame)
    def get_auxiliary_data(name: str) -> Optional[pd.DataFrame]
    @property
    def auxiliary_data -> Dict[str, pd.DataFrame]  # UI 讀取用

    # 步驟間共享變數
    _variables: Dict[str, Any]
    def set_variable(key: str, value: Any)
    def get_variable(key: str, default=None) -> Any

    # 錯誤追蹤
    errors: List[str]
    warnings: List[str]

    # 執行歷史
    _history: List[Dict]
    def add_history(step_name, status, **kwargs)

    # 便利方法
    def get_status_column() -> str   # 'PO狀態' 或 'PR狀態'
    def update_data(df: pd.DataFrame)
```

### 4.3 Pipeline 與 PipelineBuilder

```python
@dataclass
class PipelineConfig:
    name: str
    entity_type: str
    description: str = ""
    stop_on_error: bool = True
    parallel_execution: bool = False
    max_concurrent_steps: int = 5

class Pipeline:
    config: PipelineConfig
    steps: List[PipelineStep]

    async def execute(context: ProcessingContext) -> Dict[str, Any]
    async def _execute_sequential(context) -> List[StepResult]
    async def _execute_parallel(context) -> List[StepResult]

class PipelineBuilder:
    """流式 API 構建 Pipeline"""

    def __init__(self, name: str, entity_type: str) -> 'PipelineBuilder'
    def with_description(desc: str) -> 'PipelineBuilder'
    def with_stop_on_error(flag: bool) -> 'PipelineBuilder'
    def add_step(step: PipelineStep) -> 'PipelineBuilder'
    def build() -> Pipeline
```

### 4.4 DataSource（資料存取抽象）

```python
class DataSource(ABC):
    """統一資料存取介面"""

    @abstractmethod
    async def read(query: str = None, **kwargs) -> pd.DataFrame

    @abstractmethod
    async def write(data: pd.DataFrame, **kwargs) -> bool

    @abstractmethod
    def get_metadata() -> Dict

    # 提供預設實作
    async def read_with_cache(query, **kwargs) -> pd.DataFrame
    async def validate_connection() -> bool
    async def get_row_count() -> int
    async def get_column_names() -> List[str]

# 具體實作
ExcelSource   # .xlsx / .xls（支援 sheet_name, header, usecols 等）
CSVSource     # .csv（支援 encoding, sep, dtype 等）
ParquetSource # .parquet（Checkpoint 儲存用）
DuckDBSource  # DuckDB（含記憶體 DB）
```

### 4.5 BaseLoadingStep（載入步驟模板）

```python
class BaseLoadingStep(PipelineStep):
    """
    ★ 模板方法模式：資料載入步驟基類

    提供：並發載入、路徑正規化、檔案驗證
    要求子類實作：主檔案類型、主檔案載入、參考資料載入
    """

    # === 子類必須實作（鉤子方法）===
    @abstractmethod
    def get_required_file_type(self) -> str:
        """回傳必要主檔案的 key，如 'raw_po'"""
        ...

    @abstractmethod
    async def _load_primary_file(
        self, source, path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        """載入主檔案，回傳 (df, 原始行數, 過濾後行數)"""
        ...

    @abstractmethod
    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """載入參考資料並存入 context，回傳行數"""
        ...

    # === 框架提供（共用邏輯）===
    async def execute(self, context) -> StepResult:         # 模板方法
    def _normalize_file_paths(self, file_paths) -> Dict    # 路徑格式正規化
    async def _load_all_files_concurrent(self, configs)    # asyncio.gather 並發載入
    async def _load_reference_file(self, key) -> pd.DataFrame  # 便利方法
```

### 4.6 BaseERMEvaluationStep（評估步驟模板）

```python
@dataclass
class BaseERMConditions:
    """共用條件集合，子類可擴充欄位"""
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    erm_after_file_date: pd.Series
    format_error: pd.Series
    out_of_date_range: pd.Series
    procurement_not_error: pd.Series

class BaseERMEvaluationStep(PipelineStep):
    """
    ★ 模板方法模式：業務規則評估步驟基類

    定義 ERM 評估的完整流程骨架，子類只需實作業務差異點
    """

    # === 模板方法（定義骨架）===
    async def execute(self, context) -> StepResult:
        # 1. 設定檔案日期         _set_file_date()
        # 2. 建立判斷條件         _build_conditions()        [鉤子]
        # 3. 套用狀態條件         _apply_status_conditions() [鉤子]
        # 4. 處理格式錯誤         _handle_format_errors()
        # 5. 設定應計入帳標記     _set_accrual_flag()
        # 6. 設定會計欄位         _set_accounting_fields()   [鉤子]
        # 7. 實體特定後處理       _post_process()            [虛擬鉤子]
        # 8. 產生統計資訊         _generate_statistics()

    # === 子類必須實作（鉤子）===
    @abstractmethod
    def _build_conditions(self, df, file_date, status_column) -> BaseERMConditions: ...

    @abstractmethod
    def _apply_status_conditions(self, df, conditions, status_column) -> pd.DataFrame: ...

    @abstractmethod
    def _set_accounting_fields(self, df, ref_account, ref_liability) -> pd.DataFrame: ...

    # === 框架提供 ===
    def _set_file_date(self, df, processing_date) -> pd.DataFrame
    def _handle_format_errors(self, df, conditions, status_column) -> pd.DataFrame
    def _set_accrual_flag(self, df, status_column) -> pd.DataFrame
    def _generate_statistics(self, df, status_column) -> Dict
```

### 4.7 PipelineOrchestrator（編排器介面）

```python
class BasePipelineOrchestrator:
    """
    編排器共用介面（非強制繼承，可作為設計規範）
    """

    config: Dict              # 從 stagging_{entity}.toml 讀取
    entity_type: str          # 'SPT' | 'SPX'

    def build_po_pipeline(self, file_paths: Dict, custom_steps=None) -> Pipeline
    def build_pr_pipeline(self, file_paths: Dict, custom_steps=None) -> Pipeline

    def get_enabled_steps(self, processing_type: str, **kwargs) -> List[str]
        # 從 config 讀取 enabled_{type}_steps 清單

    def _create_step(self, step_name: str, file_paths: Dict, processing_type: str) -> PipelineStep
        # 根據步驟名稱動態建立步驟實例（工廠方法）
```

---

## 5. 設計模式目錄

### 5.1 模板方法模式（Template Method Pattern）

**問題**：SPT 和 SPX 的資料載入步驟有 85% 相同邏輯（路徑正規化、並發載入、驗證），只有主檔案格式和參考資料不同。

**解法**：`BaseLoadingStep` 定義公共骨架，差異點抽取為 3 個抽象鉤子方法。

```
BaseLoadingStep.execute()        ← 模板（固定流程）
├── _validate_file_configs()     ← 共用
├── _load_all_files_concurrent() ← 共用（asyncio.gather）
├── _extract_primary_data()      ← 共用
├── _add_auxiliary_data()        ← 共用
└── _load_reference_data()       ← ★ 鉤子（子類差異化）

SPTDataLoadingStep               ← 只寫差異部分（~50行）
SPXDataLoadingStep               ← 只寫差異部分（~50行）
```

**效益**：消除約 500 行重複程式碼（每個基類）。

---

### 5.2 工廠方法模式（Factory Method Pattern）

#### DataSourceFactory（資料源工廠）

```python
# 根據副檔名自動選擇實作
source = DataSourceFactory.create_from_file('/path/to/data.csv')
# → CSVSource

source = DataSourceFactory.create_from_file('/path/to/data.xlsx')
# → ExcelSource

# 批量建立
sources = DataSourceFactory.create_batch([
    DataSourceConfig(DataSourceType.EXCEL, {'file_path': 'a.xlsx'}),
    DataSourceConfig(DataSourceType.CSV, {'file_path': 'b.csv'}),
])
```

#### Orchestrator._create_step()（步驟工廠方法）

```python
# 編排器根據設定名稱動態建立步驟
def _create_step(self, step_name, file_paths, processing_type):
    if step_name == 'SPTDataLoading':
        return SPTDataLoadingStep(name=step_name, file_paths=file_paths)
    elif step_name == 'CommissionDataUpdate':
        return CommissionDataUpdateStep(name=step_name)
    elif step_name == 'SPTERMLogic':
        return SPTERMLogicStep(name=step_name)
    # ... 對應所有步驟名稱
```

---

### 5.3 單例模式（Singleton Pattern）+ 執行緒安全

**雙重檢查鎖定（Double-Checked Locking）**：避免多執行緒下重複初始化。

```python
class ConfigManager:
    _instance = None
    _initialized = False
    _lock = threading.Lock()          # 類別級別鎖

    def __new__(cls):
        if cls._instance is None:     # 第一次檢查（無鎖，快速）
            with cls._lock:
                if cls._instance is None:  # 第二次檢查（加鎖，安全）
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:     # 防止重複初始化
            with self._lock:
                if not self._initialized:
                    self._load_config()
                    self._initialized = True
```

**適用於**：ConfigManager、Logger（兩者均實作此模式）。

---

### 5.4 流式介面模式（Fluent Interface / Builder Pattern）

```python
# 可讀性高的 Pipeline 構建
pipeline = (PipelineBuilder("SPT_PO", "SPT")
    .with_description("SPT 採購單月結處理")
    .with_stop_on_error(True)
    .add_step(SPTDataLoadingStep(...))
    .add_step(CommissionDataUpdateStep(...))
    .add_step(PayrollDetectionStep(...))
    .add_step(SPTERMLogicStep(...))
    .add_step(SPTStatusLabelStep(...))
    .add_step(SPTExportStep(...))
    .build())
```

---

### 5.5 策略模式（Strategy Pattern）

**DataSource 層**：執行時動態決定資料存取策略（Excel / CSV / Parquet / DuckDB）。

```python
# 呼叫端不需知道底層格式
async def load_file(source: DataSource) -> pd.DataFrame:
    return await source.read()

# 可在執行時替換策略
if use_parquet_cache:
    source = ParquetSource(cache_path)
else:
    source = CSVSource(original_path)

df = await load_file(source)
```

---

### 5.6 責任鏈模式（Chain of Responsibility）

Pipeline 步驟序列：每個步驟處理後把 `ProcessingContext` 傳給下一步驟。

```
context → Step1 → context' → Step2 → context'' → ... → StepN → 最終結果
```

- 每個步驟只需負責自己的轉換
- `stop_on_error=True` 時，任何步驟失敗即中斷鏈

---

### 5.7 觀察者模式（Observer Pattern）

Logger 系統支援多個 Handler 同時接收日誌事件：

```python
logger = Logger()
logger.add_custom_handler('console', StreamHandler(sys.stdout))   # 終端輸出
logger.add_custom_handler('file', FileHandler('app.log'))          # 檔案輸出
logger.add_custom_handler('ui', StreamlitLogHandler())             # UI 即時顯示

# 所有 logger 自動廣播到所有 handlers
my_logger = logger.get_logger('my_module')
my_logger.info("訊息")  # → 同時出現在終端、log 檔、UI
```

---

### 5.8 連接池模式（Pool Pattern）

`DataSourcePool` 管理多個 DataSource 實例的生命週期：

```python
pool = DataSourcePool()
pool.add_source('raw_po', CSVSource('/path/to/po.csv'))
pool.add_source('previous', ExcelSource('/path/to/prev.xlsx'))
pool.add_source('ap_invoice', ExcelSource('/path/to/inv.xlsx'))

# 並發讀取所有資料源
results = await pool.execute_on_all('read')

# 程式結束時統一清理
await pool.close_all()
```

---

## 6. 配置驅動機制

### 6.1 配置體系

```
config/
├── config.ini          ← 一般設定（正則表達式、憑證、資源路徑）
├── paths.toml          ← ★ 檔案路徑模板（支援變數替換）
├── run_config.toml     ← 執行時組態
├── stagging.toml       ← 共用設定（路徑、日期模式、分類模式）
├── stagging_spt.toml   ← ★ SPT Pipeline 步驟清單 + 業務規則
└── stagging_spx.toml   ← ★ SPX Pipeline 步驟清單 + 條件引擎規則
```

### 6.2 paths.toml — 檔案路徑模板

**核心設計**：路徑中使用變數佔位符，執行時替換。

```toml
[base]
resources = "C:/SEA/Accrual/resources"
output = "./output"

[spx.po]
# 變數替換：{YYYYMM}, {PREV_YYYYMM}, {YYMM}, {resources}
raw_po   = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv"
previous = "{resources}/{YYYYMM}/前期底稿/SPX/{PREV_YYYYMM}_PO_FN.xlsx"

[spx.po.params]
# pandas 讀取參數（同步到對應的 DataSource）
raw_po   = { encoding = "utf-8", sep = ",", dtype = "str" }
previous = { sheet_name = 0, header = 0, dtype = "str" }
ops_validation = { sheet_name = "智取櫃驗收明細", header = 3, usecols = "A:AH" }
```

**優點**：
- 開發/正式環境只需改 `[base]` 的根路徑
- 不同月份處理只需改 `processing_date`
- 檔案讀取參數集中管理，不散落在程式碼中

### 6.3 stagging_{entity}.toml — 步驟啟用清單

**核心設計**：步驟序列由設定檔控制，可在不改程式碼的情況下啟用/停用步驟。每個實體有獨立的 TOML 檔案。

```toml
# config/stagging_spt.toml
[pipeline.spt]
enabled_po_steps = [
    "SPTDataLoading",
    "ProductFilter",
    "ColumnAddition",
    "APInvoiceIntegration",
    "PreviousWorkpaperIntegration",
    "ProcurementIntegration",
    "CommissionDataUpdate",
    "PayrollDetection",
    "DateLogic",
    "SPTERMLogic",
    "SPTStatusLabel",
    "SPTAccountPrediction",
    "SPTPostProcessing",
    "SPTExport",
    "DataShapeSummary",
]

enabled_pr_steps = [...]
enabled_procurement_po_steps = [...]
enabled_procurement_combined_steps = [...]

# config/stagging_spx.toml
[pipeline.spx]
enabled_po_steps = [
    "SPXDataLoading",
    "ProductFilter",
    "ColumnAddition",
    "APInvoiceIntegration",
    "PreviousWorkpaperIntegration",
    "ProcurementIntegration",
    "DateLogic",
    "ClosingListIntegration",
    "StatusStage1",
    "SPXERMLogic",
    "ValidationDataProcessing",
    "DepositStatusUpdate",
    "DataReformatting",
    "SPXExport",
    "DataShapeSummary",
]

enabled_pr_steps = [...]
```

**優點**：
- 偵錯時可暫時停用某步驟，無需改程式碼
- 新增步驟時只需在清單末尾加一行
- Orchestrator 的 `_create_step()` 是唯一需要修改的程式碼

### 6.4 ConfigManager 使用方式

```python
from accrual_bot.utils.config import ConfigManager

config = ConfigManager()  # 永遠回傳同一實例

# 讀取 config.ini
regex_pattern = config.get('regex', 'date_pattern')

# 讀取 stagging_spt.toml（Pipeline 步驟）
enabled_steps = config.get_list('spt', 'enabled_po_steps')

# 讀取 paths.toml（檔案路徑參數）
path_params = config.get_paths_config('spt', 'po', 'params')
```

---

## 7. 資料流與執行流程

### 7.1 CLI 執行流程

```
main_pipeline.py
    │
    ├─ 1. 讀取執行設定（entity='SPT', type='PO', date=202512）
    │
    ├─ 2. 從 paths.toml 解析檔案路徑（變數替換）
    │      file_paths = {
    │          'raw_po': '/resources/202512/Original Data/202512_purchase_order_*.csv',
    │          'previous': '/resources/202512/前期底稿/SPT/202511_PO_FN.xlsx',
    │          ...
    │      }
    │
    ├─ 3. 建立 Orchestrator + Pipeline
    │      orchestrator = SPTPipelineOrchestrator()
    │      pipeline = orchestrator.build_po_pipeline(file_paths)
    │      # → Pipeline with 15 steps from stagging_spt.toml
    │
    ├─ 4. 建立 ProcessingContext
    │      context = ProcessingContext(
    │          data=pd.DataFrame(),
    │          entity_type='SPT',
    │          processing_date=202512,
    │          processing_type='PO'
    │      )
    │
    ├─ 5. 執行 Pipeline（可選擇 Checkpoint 模式）
    │      result = await execute_pipeline_with_checkpoint(
    │          pipeline=pipeline,
    │          context=context,
    │          save_checkpoints=True
    │      )
    │
    └─ 6. 輸出結果
           → output/202512_SPT_PO_FN.xlsx
```

### 7.2 ProcessingContext 資料流

```
Step1: SPTDataLoadingStep
  context.data = DataFrame(4000 rows, 30 cols)
  context.auxiliary_data['reference_account'] = DataFrame(200 rows)
  context.auxiliary_data['commission'] = DataFrame(50 rows)
        ↓
Step2: CommissionDataUpdateStep
  context.data = DataFrame(4000 rows, 31 cols)  ← 新增 commission 欄位
        ↓
Step5: DateLogicStep
  context.set_variable('file_date', date(2025, 12, 31))
        ↓
Step6: SPTERMLogicStep
  context.data = DataFrame(4000 rows, 35 cols)  ← 新增 PO狀態/應計欄位
  context.set_variable('erm_stats', {'已完成': 2100, '待確認': 900, ...})
        ↓
Step14: SPTExportStep
  → 寫出 Excel 檔案
  context.set_variable('output_path', '/output/202512_SPT_PO_FN.xlsx')
```

### 7.3 Checkpoint 機制

```
Pipeline 執行時：
  Step1 完成 → 儲存 checkpoint（data.parquet + auxiliary_data/ + vars.json）
  Step2 完成 → 儲存 checkpoint
  ...
  Step6 失敗！→ 記錄失敗點

下次執行：
  resume_from_step(checkpoint="after_Step5", start_from="Step6")
  → 從 parquet 還原 context
  → 只執行 Step6 ~ Step14
  → 節省重新載入和前 5 步的時間
```

---

## 8. 依賴關係圖

### 8.1 模組依賴（呼叫方向）

```
main_pipeline.py / main_streamlit.py
       │
       ▼
ui/services/unified_pipeline_service.py
       │
       ▼
tasks/{spt|spx}/pipeline_orchestrator.py
       │
       ├──▶ core/pipeline/pipeline.py
       │         │
       │         ▼
       │    core/pipeline/base.py (PipelineStep)
       │         │
       │         ├──▶ core/pipeline/steps/base_loading.py
       │         │         └──▶ core/datasources/
       │         │
       │         └──▶ core/pipeline/steps/base_evaluation.py
       │
       └──▶ tasks/{entity}/steps/*.py  ← 實體特定步驟
                  │
                  └──▶ core/pipeline/steps/common.py  ← 共用步驟

全域依賴（所有模組均可呼叫）：
  utils/config/config_manager.py
  utils/logging/logger.py
```

### 8.2 設定檔依賴

```
paths.toml
    └── unified_pipeline_service._enrich_file_paths()
            └── 替換路徑變數後傳給 Orchestrator

stagging.toml
    └── 共用設定（日期模式、分類模式、路徑）
            └── 所有模組讀取共用配置

stagging_spt.toml / stagging_spx.toml
    └── orchestrator.get_enabled_steps(proc_type)
            └── 決定 Pipeline 中的步驟序列
    └── 業務規則（status_label_rules、condition_engine 等）

config.ini
    └── ConfigManager
            └── 正則表達式、資源路徑、系統參數
```

### 8.3 類別繼承關係

```
PipelineStep (ABC)
├── BaseLoadingStep (ABC)
│   ├── SPTDataLoadingStep           # tasks/spt/steps/spt_loading.py
│   ├── SPTPRDataLoadingStep         # tasks/spt/steps/spt_loading.py
│   ├── SPXDataLoadingStep           # tasks/spx/steps/spx_loading.py
│   ├── SPXPRDataLoadingStep         # tasks/spx/steps/spx_loading.py
│   ├── PPEDataLoadingStep           # tasks/spx/steps/spx_loading.py
│   ├── SPTProcurementDataLoadingStep    # tasks/spt/steps/spt_procurement_loading.py
│   ├── SPTProcurementPRDataLoadingStep  # tasks/spt/steps/spt_procurement_loading.py
│   └── CombinedProcurementDataLoadingStep  # tasks/spt/steps/spt_combined_procurement_loading.py
│
├── BaseERMEvaluationStep (ABC)
│   ├── SPTERMLogicStep              # tasks/spt/steps/spt_evaluation_erm.py
│   ├── SPXERMLogicStep              # tasks/spx/steps/spx_evaluation.py
│   ├── SPXPRERMLogicStep            # tasks/spx/steps/spx_pr_evaluation.py
│   └── SPTProcurementStatusEvaluationStep  # tasks/spt/steps/spt_procurement_evaluation.py
│
└── （直接繼承的共用步驟）
    ├── DateLogicStep                # core/pipeline/steps/common.py
    ├── ProductFilterStep            # core/pipeline/steps/common.py
    ├── DataIntegrationStep          # core/pipeline/steps/common.py
    ├── PreviousWorkpaperIntegrationStep  # core/pipeline/steps/common.py
    ├── ProcurementIntegrationStep   # core/pipeline/steps/common.py
    ├── AccountCodeMappingStep       # core/pipeline/steps/business.py
    ├── StatusStage1Step             # tasks/spx/steps/spx_evaluation.py（含 SPXConditionEngine）
    ├── ColumnAdditionStep           # tasks/spx/steps/spx_steps.py
    ├── SPTStatusLabelStep           # tasks/spt/steps/spt_steps.py
    ├── SPTAccountPredictionStep     # tasks/spt/steps/spt_account_prediction.py
    ├── DataShapeSummaryStep         # tasks/common/data_shape_summary.py
    └── ...（更多實體特定步驟）

DataSource (ABC)
├── ExcelSource
├── CSVSource
├── ParquetSource
└── DuckDBSource
```

---

## 9. UI 整合策略

### 9.1 雙層頁面架構（解決框架限制）

**問題**：Streamlit 多頁應用需要 emoji 檔名才能在側邊欄顯示，但 emoji 檔名在跨平台/Git 中有相容性問題。

**解法**：兩層分離

```
pages/1_⚙️_配置.py              ← Streamlit 入口（帶 emoji，只有 17 行）
    └── exec(open('accrual_bot/ui/pages/1_configuration.py').read())
                ↓
accrual_bot/ui/pages/1_configuration.py   ← 實際業務邏輯（標準檔名，可測試）
```

**Entry Point 範本**：

```python
# pages/1_⚙️_配置.py
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

actual_page = project_root / "accrual_bot" / "ui" / "pages" / "1_configuration.py"
exec(open(actual_page, encoding='utf-8').read())
```

**注意**：`st.switch_page()` 必須使用 emoji 檔名：
```python
st.switch_page("pages/2_📁_檔案上傳.py")  # ✓ 正確
st.switch_page("pages/2_file_upload.py")   # ✗ Streamlit 找不到
```

### 9.2 UnifiedPipelineService（UI 橋接層）

**設計原則**：UI 頁面不直接接觸 Orchestrator，所有互動透過 `UnifiedPipelineService`。

```python
class UnifiedPipelineService:
    """UI 和 Pipeline 的唯一橋接點"""

    # 查詢方法（UI 用於顯示選項）
    def get_available_entities(self) -> List[str]
    def get_entity_types(self, entity: str) -> List[str]
    def get_enabled_steps(self, entity: str, proc_type: str) -> List[str]

    # Pipeline 構建（UI 用於觸發執行）
    def build_pipeline(
        self,
        entity: str,           # 'SPT' | 'SPX'
        proc_type: str,        # 'PO' | 'PR' | 'PPE' | 'PPE_DESC' | 'PROCUREMENT'
        file_paths: Dict,      # 使用者上傳的檔案路徑
        processing_date: int = None,  # YYYYMM（PPE/PPE_DESC 必填）
        source_type: str = None       # PROCUREMENT 子類型 ('PO'|'PR'|'COMBINED')
    ) -> Pipeline

    # 內部方法
    def _get_orchestrator(self, entity: str)  # 根據 entity 選擇 Orchestrator
    def _enrich_file_paths(self, file_paths, entity, proc_type, source_type)
        # 從 paths.toml 讀取 params，合併到 file_paths 中
```

### 9.3 UI 配置管理（ui/config.py）

所有 UI 顯示邏輯的中央配置，新增實體/類型只需修改此檔案：

```python
# 實體定義
ENTITY_CONFIG = {
    'SPT': {
        'display_name': 'SPT',
        'types': ['PO', 'PR', 'PROCUREMENT'],
        'description': 'SPT 採購單/請購單處理',
        'icon': '🛒',
    },
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE', 'PPE_DESC'],
        'icon': '📦',
    },
}

# 必填/選填檔案定義
REQUIRED_FILES = {
    ('SPT', 'PO'): ['raw_po'],
    ('SPT', 'PR'): ['raw_pr'],
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PR'): ['raw_pr'],
    ('SPX', 'PPE'): ['contract_filing_list'],
    ('SPX', 'PPE_DESC'): ['workpaper', 'contract_periods'],
    ('SPT', 'PROCUREMENT', 'PO'): ['raw_po'],
    ('SPT', 'PROCUREMENT', 'PR'): ['raw_pr'],
    ...
}

OPTIONAL_FILES = {
    ('SPT', 'PO'): ['previous', 'procurement_po', 'ap_invoice', 'previous_pr',
                     'procurement_pr', 'media_finished', 'media_left', 'media_summary'],
    ('SPX', 'PO'): ['previous', 'procurement_po', 'ap_invoice', 'previous_pr',
                     'procurement_pr', 'ops_validation'],
    ('SPT', 'PROCUREMENT', 'PO'): ['procurement_previous', 'media_finished',
                                    'media_left', 'media_summary'],
    ...
}

# 顯示標籤
FILE_LABELS = {
    'raw_po': '採購單原始資料 (必填)',
    'previous': '前期底稿 (選填)',
    'procurement_po': '採購 PO 底稿 (選填)',
    ...
}
```

### 9.4 Sync/Async 橋接（Streamlit 限制）

Streamlit 在同步環境執行，Pipeline 是非同步的，需要橋接：

```python
# ui/utils/async_bridge.py
import asyncio
import threading

def run_async_in_thread(coro):
    """在新執行緒中執行非同步協程（解決 Streamlit event loop 問題）"""
    result = None
    exception = None

    def thread_target():
        nonlocal result, exception
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(coro)
        except Exception as e:
            exception = e
        finally:
            loop.close()

    thread = threading.Thread(target=thread_target)
    thread.start()
    thread.join()

    if exception:
        raise exception
    return result
```

---

## 10. 擴充指南

### 10.1 新增實體（如 MOB）

需修改/新增的位置：

| # | 檔案 | 動作 |
|---|------|------|
| 1 | `tasks/mob/__init__.py` | 建立模組 |
| 2 | `tasks/mob/pipeline_orchestrator.py` | 實作 `MOBPipelineOrchestrator` |
| 3 | `tasks/mob/steps/*.py` | 實體特定步驟（可複用 common.py） |
| 4 | `config/stagging_mob.toml` | 新增 `[pipeline.mob]` 區段 + 業務規則 |
| 5 | `config/paths.toml` | 新增 `[mob.po]` 等路徑區段 |
| 6 | `ui/config.py` | 在 `ENTITY_CONFIG` 加入 `MOB` |
| 7 | `ui/config.py` | 新增 `REQUIRED_FILES[('MOB', 'PO')]` |
| 8 | `ui/services/unified_pipeline_service.py` | 在 `_get_orchestrator()` 加入 `'MOB'` |

### 10.2 新增處理類型（如 INV）

以現有實體 SPX 新增 INV（Invoice）類型為例：

| # | 檔案 | 動作 |
|---|------|------|
| 1 | `ui/config.py` | 在 `ENTITY_CONFIG['SPX']['types']` 加 `'INV'` |
| 2 | `ui/config.py` | 新增 `REQUIRED_FILES[('SPX', 'INV')]` |
| 3 | `ui/config.py` | 新增 `OPTIONAL_FILES[('SPX', 'INV')]` |
| 4 | `ui/config.py` | 新增 `FILE_LABELS` 標籤 |
| 5 | `config/paths.toml` | 新增 `[spx.inv]` 和 `[spx.inv.params]` |
| 6 | `config/stagging_spx.toml` | 新增 `enabled_inv_steps` |
| 7 | `tasks/spx/pipeline_orchestrator.py` | 新增 `build_inv_pipeline()` |
| 8 | `tasks/spx/pipeline_orchestrator.py` | 在 `_create_step()` 註冊新步驟 |
| 9 | `tasks/spx/pipeline_orchestrator.py` | 在 `get_enabled_steps()` 加 `'INV'` 分支 |
| 10 | `ui/services/unified_pipeline_service.py` | 在 `build_pipeline()` 加 `elif` 分支 |

### 10.3 新增步驟

**Step 1**：選擇繼承方式

```python
# 情境 A：資料載入步驟 → 繼承 BaseLoadingStep
class MOBDataLoadingStep(BaseLoadingStep):
    def get_required_file_type(self) -> str:
        return 'raw_mo'

    async def _load_primary_file(self, source, path):
        df = await source.read()
        return df, len(df), len(df)

    async def _load_reference_data(self, context) -> int:
        ref = await self._load_reference_file('mob_reference')
        context.add_auxiliary_data('mob_reference', ref)
        return len(ref)

# 情境 B：業務規則評估 → 繼承 BaseERMEvaluationStep
class MOBERMLogicStep(BaseERMEvaluationStep):
    def _build_conditions(self, df, file_date, status_column):
        return BaseERMConditions(
            no_status=(df[status_column].isna()),
            in_date_range=(df['date'] >= file_date),
            # ...
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        df.loc[conditions.no_status, status_column] = '待評估'
        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        df['account_code'] = df['type'].map(ref_account)
        return df

# 情境 C：一般業務邏輯 → 直接繼承 PipelineStep
class MOBCustomStep(PipelineStep):
    async def execute(self, context: ProcessingContext) -> StepResult:
        try:
            df = context.data.copy()
            # 自訂邏輯
            context.update_data(df)
            return StepResult(step_name=self.name, status=StepStatus.SUCCESS)
        except Exception as e:
            return StepResult(step_name=self.name, status=StepStatus.FAILED, error=e)

    async def validate_input(self, context: ProcessingContext) -> bool:
        return 'required_column' in context.data.columns
```

**Step 2**：在 Orchestrator 的 `_create_step()` 中註冊

```python
def _create_step(self, step_name, file_paths, processing_type):
    if step_name == 'MOBDataLoading':
        return MOBDataLoadingStep(name=step_name, file_paths=file_paths)
    elif step_name == 'MOBERMLogic':
        return MOBERMLogicStep(name=step_name)
    elif step_name == 'MOBCustom':
        return MOBCustomStep(name=step_name)
```

**Step 3**：在 `stagging_mob.toml` 中啟用

```toml
# config/stagging_mob.toml
[pipeline.mob]
enabled_po_steps = [
    "MOBDataLoading",
    "MOBCustom",
    "MOBERMLogic",
    ...
]
```

---

## 11. 關鍵決策與取捨

### 決策 1：選擇 TOML 而非資料庫儲存業務規則

**選擇**：`stagging_{entity}.toml` 儲存步驟序列和業務規則

**取捨**：
- ✅ 無需資料庫依賴，部署簡單
- ✅ 版本控制友好（純文字 diff）
- ✅ 修改即時生效（重啟 ConfigManager）
- ❌ 規則複雜時 TOML 可讀性下降
- ❌ 無法即時修改（需重啟服務）

**建議**：規則 < 200 條時適合 TOML；更複雜的場景考慮 SQLite 或 JSON Schema。

---

### 決策 2：模板方法模式 vs 組合模式

**選擇**：`BaseLoadingStep` 使用模板方法（繼承）

**取捨**：
- ✅ 程式碼消除效果最好（500+ 行重複）
- ✅ 子類只需寫差異部分（~50 行）
- ❌ 繼承關係較緊密，基類修改影響所有子類
- ❌ 若需要組合多種行為，繼承會受限

**替代方案**：Composition（組合多個 Strategy 物件），適合差異點更多、更動態的場景。

---

### 決策 3：CheckpointManager 使用 Parquet 格式

**選擇**：Parquet 儲存中間結果（而非 CSV 或 Pickle）

**取捨**：
- ✅ 讀寫速度快 10 倍（vs CSV）
- ✅ 型別安全（不會遺失 dtype）
- ✅ 壓縮比高（節省磁碟空間）
- ❌ 需要 `pyarrow` 或 `fastparquet` 依賴
- ❌ 人工無法直接閱讀檔案內容

---

### 決策 4：雙層頁面架構（Streamlit 特定）

**選擇**：`pages/` 只放 17 行 entry point，實際邏輯在 `accrual_bot/ui/pages/`

**取捨**：
- ✅ 解決 emoji 檔名的跨平台問題
- ✅ 業務邏輯可被測試（標準 Python 模組）
- ❌ 引入 `exec()` 稍微降低可讀性
- ❌ 需要維護兩個目錄（但 entry point 只有 17 行）

---

### 決策 5：ConfigManager 執行緒安全單例

**選擇**：雙重檢查鎖定（Double-Checked Locking）

**為何不用其他方式**：
- `functools.lru_cache`：不支援 `__new__` 覆寫
- `threading.local()`：每個執行緒各一個實例，非單例
- Metaclass Singleton：過度工程化

**建議**：Python 3.3+ 可改用 `importlib.import_module` 的模組級單例（更簡潔）。

---

## 12. 可複用的程式碼模板

### 12.1 執行緒安全單例

```python
import threading

class MySingleton:
    _instance = None
    _initialized = False
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    # 初始化邏輯
                    self._initialized = True
```

### 12.2 資料載入步驟模板

```python
from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from typing import Tuple
import pandas as pd

class MyEntityDataLoadingStep(BaseLoadingStep):
    """
    [實體名] 資料載入步驟
    載入主檔案 + 參考資料，初始化 ProcessingContext
    """

    def get_required_file_type(self) -> str:
        return 'raw_po'  # 必填主檔案的 key（對應 paths.toml）

    async def _load_primary_file(
        self, source, file_path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        df = await source.read()
        raw_rows = len(df)
        # 可加入過濾邏輯
        filtered_rows = len(df)
        return df, raw_rows, filtered_rows

    async def _load_reference_data(self, context) -> int:
        # 載入參考表（選填）
        try:
            ref_df = await self._load_reference_file('reference_account')
            context.add_auxiliary_data('reference_account', ref_df)
            return len(ref_df)
        except FileNotFoundError:
            return 0  # 選填檔案不存在時不中斷
```

### 12.3 ERM 評估步驟模板

```python
from accrual_bot.core.pipeline.steps.base_evaluation import (
    BaseERMEvaluationStep, BaseERMConditions
)
import pandas as pd

class MyEntityERMStep(BaseERMEvaluationStep):
    """
    [實體名] 業務規則評估步驟
    根據日期、狀態等條件判斷應計狀態
    """

    def _build_conditions(self, df: pd.DataFrame, file_date, status_column: str):
        return BaseERMConditions(
            no_status=(df[status_column].isna()),
            in_date_range=(df['需求日期'] >= file_date),
            erm_before_or_equal_file_date=(df['ERM日期'] <= file_date),
            erm_after_file_date=(df['ERM日期'] > file_date),
            format_error=(df['ERM日期'].isna() & df[status_column].notna()),
            out_of_date_range=(df['需求日期'] < file_date),
            procurement_not_error=(df['採購類型'] != '錯誤'),
        )

    def _apply_status_conditions(self, df: pd.DataFrame, conditions, status_column: str):
        df.loc[conditions.no_status, status_column] = '待評估'
        df.loc[conditions.in_date_range & conditions.erm_before_or_equal_file_date,
               status_column] = '已完成'
        df.loc[conditions.in_date_range & conditions.erm_after_file_date,
               status_column] = '未完成'
        return df

    def _set_accounting_fields(self, df: pd.DataFrame, ref_account, ref_liability):
        df['會計科目'] = df['科目代碼'].map(ref_account)
        df['負債科目'] = df['科目代碼'].map(ref_liability)
        return df
```

### 12.4 Pipeline Orchestrator 模板

```python
from accrual_bot.core.pipeline import Pipeline, PipelineBuilder, PipelineConfig
from accrual_bot.utils.config import ConfigManager
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)

class MyEntityPipelineOrchestrator:
    """
    [實體名] Pipeline 編排器
    從 stagging_{entity}.toml 讀取步驟清單，動態組裝 Pipeline
    """

    def __init__(self):
        config = ConfigManager()
        self.config = config._config_toml.get('pipeline', {}).get('my_entity', {})
        self.entity_type = 'MY_ENTITY'

    def build_po_pipeline(self, file_paths: dict, custom_steps=None) -> Pipeline:
        enabled_steps = self.get_enabled_steps('PO')
        steps = [self._create_step(name, file_paths, 'PO') for name in enabled_steps]

        pipeline = PipelineBuilder(f"{self.entity_type}_PO", self.entity_type)
        for step in steps:
            pipeline.add_step(step)
        return pipeline.build()

    def get_enabled_steps(self, processing_type: str) -> list:
        key = f'enabled_{processing_type.lower()}_steps'
        return self.config.get(key, [])

    def _create_step(self, step_name: str, file_paths: dict, processing_type: str):
        """步驟工廠方法：根據名稱建立步驟實例"""
        if step_name == 'MyEntityDataLoading':
            return MyEntityDataLoadingStep(name=step_name, file_paths=file_paths)
        elif step_name == 'MyEntityERMLogic':
            return MyEntityERMStep(name=step_name)
        # ... 更多步驟
        else:
            raise ValueError(f"未知步驟: {step_name}")
```

### 12.5 統一日誌使用

```python
from accrual_bot.utils.logging import get_logger, get_structured_logger

# 一般日誌
logger = get_logger(__name__)
logger.info("開始處理 %s 資料", entity_type)
logger.debug("DataFrame shape: %s", df.shape)
logger.warning("找不到參考資料，使用預設值")
logger.error("處理失敗: %s", str(e), exc_info=True)

# 結構化日誌（語意化方法）
s_logger = get_structured_logger(__name__)
s_logger.log_operation_start('data_loading', entity='SPT', type='PO')
s_logger.log_data_processing('purchase_orders', record_count=4000, processing_time=1.5)
s_logger.log_file_operation('read', file_path='/path/to/file.csv', success=True)
s_logger.log_operation_end('data_loading', success=True, duration=2.3)
```

---

## 附錄：技術選型摘要

| 技術 | 用途 | 選擇理由 |
|------|------|---------|
| **Python asyncio** | Pipeline 非同步執行 | I/O 密集型（大量檔案讀寫），asyncio 提供高並發 |
| **pandas** | 資料處理核心 | 財務資料的欄位操作、條件篩選、pivot 等 |
| **Parquet (pyarrow)** | Checkpoint 儲存 | 比 CSV 快 10 倍，保留 dtype，壓縮率高 |
| **TOML** | 配置檔格式 | 比 INI 更豐富（陣列、巢狀），比 YAML 更嚴格 |
| **Streamlit** | Web UI | Python-native，無需前端技能，快速原型 |
| **pytest + pytest-asyncio** | 測試框架 | 原生支援 async 測試，fixtures 系統完善 |
| **threading.Lock** | 執行緒安全 | 輕量，適合保護 ConfigManager/Logger 初始化 |
| **DuckDB** | 分析查詢 | 可直接查詢 pandas DataFrame，比 SQLite 更適合 OLAP |

---

---

## 13. 業務邏輯配置驅動：SPX 完整範例

> 本章以 SPX 的 `StatusStage1Step` 和 `SPXERMLogicStep` 為主線，展示**如何將複雜多條件的業務判斷邏輯從程式碼中抽離、寫入 TOML 設定檔**，並透過通用條件引擎執行。這是「配置驅動業務規則」的最完整體現。

### 13.1 問題背景：業務規則爆炸

SPX PO 處理的狀態判斷原本是這樣的：

```python
# ❌ 舊做法：業務規則硬編碼在程式碼中（難以維護）
if '押金' in row['Item Description'] or '保證金' in row['Item Description']:
    row['PO狀態'] = '押金'
elif row['PO Supplier'] in bao_suppliers and row['Category'] in water_categories:
    row['PO狀態'] = 'GL調整'
elif '刪' in row['Remarked by 上月 FN'] or '關' in row['Remarked by 上月 FN']:
    row['PO狀態'] = '參照上月關單'
# ... 還有 12 個以上的 elif
```

**問題**：
- 每次業務規則變更（如新增供應商、調整關鍵字）都需要改程式碼
- 規則之間優先順序不透明
- 難以撰寫測試、難以向非技術人員解釋規則

---

### 13.2 解決方案：條件引擎（SPXConditionEngine）

**核心概念**：將規則的「形式」（如何判斷）寫在程式，規則的「內容」（判斷什麼）寫在 TOML。

```
stagging_spx.toml                      SPXConditionEngine
┌─────────────────────────────┐        ┌─────────────────────────────┐
│ [[spx_erm_status_rules       │  載入  │                             │
│   .conditions]]              │ ─────▶ │  _load_rules()              │
│   priority = 2               │        │  → 按 priority 排序         │
│   status_value = "已入帳"    │        │                             │
│   combine = "and"            │        │  apply_rules(df, status_col)│
│   [[...checks]]              │        │  → 依序對每條規則：         │
│     type = "is_not_null"     │        │    1. 建構 boolean mask      │
│     field = "GL DATE"        │        │    2. 限縮未命中的列         │
│   [[...checks]]              │        │    3. 寫入狀態值             │
│     type = "erm_le_date"     │        │    4. 更新 no_status mask    │
│   [[...checks]]              │        │                             │
│     type = "not_fa"          │        └─────────────────────────────┘
└─────────────────────────────┘
```

**引擎的兩個核心方法**：

```python
class SPXConditionEngine:
    """
    從 stagging_spx.toml 讀取規則，動態建構 pandas boolean mask 並依序應用。

    支援兩個規則區段：
    - spx_status_stage1_rules  → StatusStage1Step（第一階段：押金/租金/關單等特殊狀態）
    - spx_erm_status_rules     → SPXERMLogicStep（第二階段：ERM 邏輯評估）
    """

    def __init__(self, config_section: str):
        """
        config_section: TOML 區段名稱
                        'spx_status_stage1_rules' 或 'spx_erm_status_rules'
        """
        self.rules = self._load_rules()  # 按 priority 排序的規則列表

    def apply_rules(
        self,
        df: pd.DataFrame,
        status_column: str,         # 'PO狀態' 或 'PR狀態'
        context: Dict[str, Any],    # 包含 processing_date、prebuilt_masks 等
        processing_type: str = "PO"
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        依序應用所有規則：
        1. 先找出「尚無狀態」的列（no_status mask）
        2. 按 priority 排序，逐條處理規則
        3. 每條規則：建構 mask → 限縮至 no_status → 寫入狀態 → 更新 no_status
        4. 回傳更新後的 df 和各規則命中數統計
        """
```

---

### 13.3 TOML 規則格式詳解

#### 13.3.1 規則頂層結構

```toml
# 每條規則是一個 [[section.conditions]] 陣列元素
[[spx_erm_status_rules.conditions]]
priority      = 2                    # 執行優先序（數字越小越優先）
status_value  = "已入帳"             # 命中時寫入的狀態值
note          = "有GL DATE、ERM<=..."  # 人類可讀的說明（不影響邏輯）
combine       = "and"               # checks 間的邏輯：'and' 或 'or'
apply_to      = ["PO", "PR"]        # 適用的處理類型（預設兩者都適用）
override_statuses = ["已完成_租金"]  # 可覆蓋的現有狀態（可選）

# checks 是子陣列，每個元素是一個單一條件
[[spx_erm_status_rules.conditions.checks]]
type  = "is_not_null"
field = "GL DATE"

[[spx_erm_status_rules.conditions.checks]]
type = "erm_le_date"   # 不需要 field（預先計算的 mask）

[[spx_erm_status_rules.conditions.checks]]
type = "not_fa"        # 引用 fa_accounts.spx 配置
```

#### 13.3.2 支援的 check type 完整清單

| 類別 | type | 說明 | 需要 field |
|------|------|------|-----------|
| **欄位比對** | `contains` | 正則匹配（需 `pattern` 或 `pattern_key`） | ✅ |
| | `not_contains` | 正則不匹配 | ✅ |
| | `equals` | 完全相等（可指定 `cast` 型別） | ✅ |
| | `not_equals` | 不等於 | ✅ |
| | `in_list` | 在列表中（需 `values` 或 `list_key`） | ✅ |
| | `not_in_list` | 不在列表中 | ✅ |
| **欄位狀態** | `is_not_null` | 欄位有值（非空、非 ''、非 'nan'） | ✅ |
| | `is_null` | 欄位為空 | ✅ |
| | `no_status` | 狀態欄位為空 | ❌（固定） |
| **ERM/日期** | `erm_le_date` | ERM 月份 ≤ 處理月份 | ❌ |
| | `erm_gt_date` | ERM 月份 > 處理月份 | ❌ |
| | `erm_in_range` | ERM 在摘要期間區間內 | ❌ |
| | `out_of_range` | ERM 不在摘要期間內（且非格式錯誤） | ❌ |
| | `desc_erm_le_date` | 摘要起始月 ≤ 處理月份 | ❌ |
| | `desc_erm_gt_date` | 摘要結束月 > 處理月份 | ❌ |
| **帳務** | `qty_matched` | 驗收數量 = 入庫數量 | ❌ |
| | `qty_not_matched` | 驗收數量 ≠ 入庫數量 | ❌ |
| | `not_billed` | Entry Billed Amount = 0 | ❌ |
| | `has_billing` | Billed Quantity ≠ 0 | ❌ |
| | `fully_billed` | Amount - Billed Amount = 0 | ❌ |
| | `has_unpaid` | Amount - Billed Amount ≠ 0 | ❌ |
| | `format_error` | 摘要格式錯誤標記 | ❌ |
| **備註** | `remark_completed` | 採購/FN 備註含「已完成/rent」 | ❌ |
| | `pr_not_incomplete` | FN PR 備註不含「未完成」 | ❌ |
| | `not_error` | 採購備註 ≠ 'error' | ❌ |
| **FA 類** | `is_fa` | GL# 在固定資產科目清單中 | ❌ |
| | `not_fa` | GL# 不在固定資產科目清單中 | ❌ |

#### 13.3.3 值解析：直接值 vs. 引用鍵

引擎支援兩種值的指定方式，讓規則可以引用 TOML 中其他位置的設定，避免重複：

```toml
# 方式 A：直接值
[[spx_status_stage1_rules.conditions.checks]]
field = "GL#"
type = "not_equals"
value = "199999"          # 直接寫死的值

# 方式 B：引用鍵（點分隔路徑，解析到 TOML 中對應欄位）
[[spx_status_stage1_rules.conditions.checks]]
field = "GL#"
type = "not_equals"
value_key = "fa_accounts.spx"      # → config_toml['fa_accounts']['spx']

# 方式 C：引用鍵到列表
[[spx_status_stage1_rules.conditions.checks]]
field = "PO Supplier"
type = "in_list"
list_key = "spx.dw_suppliers"      # → config_toml['spx']['dw_suppliers']

# 方式 D：引用鍵到正則模式
[[spx_status_stage1_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern_key = "spx.deposit_keywords"   # → config_toml['spx']['deposit_keywords']

# 方式 E：status_value 也支援引用鍵
[[spx_status_stage1_rules.conditions]]
status_value_key = "spx.deposit_keywords_label"  # 動態讀取狀態標籤
```

**特殊引用**：`spx.asset_suppliers` 會自動合併 `kiosk_suppliers + locker_suppliers`：

```python
# _resolve_ref() 中的特殊處理
if key == 'spx.asset_suppliers':
    kiosk = config_manager._config_toml['spx']['kiosk_suppliers']
    locker = config_manager._config_toml['spx']['locker_suppliers']
    return kiosk + locker
```

---

### 13.4 混合模式（Mixed Mode）

`StatusStage1Step` 採用「混合模式」：**部分邏輯配置驅動，部分邏輯保留在程式碼**。

```python
class StatusStage1Step(PipelineStep):
    """
    混合模式範例：
    - 配置驅動：押金識別、GL調整、關單、公共費用、租金、intermediary、資產待驗收
    - 程式碼保留：關單清單比對（DataFrame JOIN）、FA備註提取（正則）、日期格式轉換
    """

    def __init__(self, name: str = "StatusStage1", **kwargs):
        super().__init__(name, **kwargs)
        # ★ 初始化時載入對應的規則區段
        self.engine = SPXConditionEngine('spx_status_stage1_rules')

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()
        df_closing = context.get_auxiliary_data('closing_list')
        processing_date = context.metadata.processing_date
        status_column = context.get_status_column()

        # === 程式碼保留的部分：資料驅動，不適合配置化 ===

        # 1. 關單清單比對（需要 DataFrame 合併操作）
        if df_closing is not None:
            matched = df.merge(df_closing, on='PO#', how='left', indicator=True)
            df.loc[matched['_merge'] == 'both', status_column] = '已關單'

        # 2. FA 備註提取（正則提取，結果因資料不同而異）
        fa_pattern = r'(\d{6})入FA'
        df['FA_entry_remark'] = df['Remarked by 上月 FN'].str.extract(fa_pattern)

        # 3. 日期格式轉換（需要 pandas 日期函式）
        df['Expected Received Month_轉換格式'] = pd.to_datetime(
            df['Expected Received Month'], errors='coerce'
        ).dt.strftime('%Y%m').astype('Int64')

        # === 配置驅動的部分：透過引擎套用 TOML 規則 ===

        # 預先計算常用 mask（效能優化，避免引擎重複計算）
        prebuilt_masks = {
            'no_status': df[status_column].isna() | (df[status_column] == ''),
            'erm_le_date': df['Expected Received Month_轉換格式'] <= processing_date,
            'erm_gt_date': df['Expected Received Month_轉換格式'] > processing_date,
            'is_fa': df['GL#'].isin(fa_accounts),
            'not_fa': ~df['GL#'].isin(fa_accounts),
        }

        engine_context = {
            'processing_date': processing_date,
            'prebuilt_masks': prebuilt_masks,
        }

        # ★ 核心：引擎套用 TOML 規則
        df, stats = self.engine.apply_rules(
            df, status_column, engine_context,
            processing_type=context.metadata.processing_type
        )

        context.update_data(df)
        context.set_variable('stage1_stats', stats)

        return StepResult(step_name=self.name, status=StepStatus.SUCCESS, data=df)
```

**何時用程式碼，何時用 TOML**：

| 邏輯類型 | 建議方式 | 原因 |
|----------|----------|------|
| 欄位值比對（關鍵字、供應商名）| TOML | 業務人員可直接修改 |
| 欄位存在性判斷（is_null）| TOML | 通用且穩定 |
| 預先定義的日期比較（ERM vs 月份）| TOML（引用 prebuilt_masks）| 邏輯固定，值由程式計算 |
| DataFrame 合併（JOIN 操作） | 程式碼 | 需要 pandas 操作，配置無法表達 |
| 正則提取結果（extract）| 程式碼 | 結果動態，非 boolean 判斷 |
| 型別轉換（日期 parsing）| 程式碼 | 依賴 pandas 函式 |

---

### 13.5 與 SPT 的設計比較

SPT 和 SPX 的業務規則配置採用不同的 TOML 格式，反映兩種不同的設計思路：

#### SPX 方式：通用條件引擎（優先序陣列）

```toml
# 單一通用格式，由引擎解釋
[[spx_erm_status_rules.conditions]]
priority = 6
status_value = "Check收貨"
combine = "and"
[[spx_erm_status_rules.conditions.checks]]
type = "not_error"
[[spx_erm_status_rules.conditions.checks]]
type = "no_status"
[[spx_erm_status_rules.conditions.checks]]
type = "erm_in_range"
[[spx_erm_status_rules.conditions.checks]]
type = "qty_not_matched"
```

**優點**：
- 新增規則只需在 TOML 加一個 `[[...conditions]]` 區塊
- 無需修改任何程式碼
- 條件類型可複用

**缺點**：
- 複雜組合邏輯（如多 OR 子句）在 TOML 中表達受限
- 需要維護引擎的 check type 清單

---

#### SPT 方式：具名規則對（Key-Value 結構）

```toml
# 具名的 key-value 規則（更直覺，但擴充性較低）
[spt_status_label_rules.priority_conditions]
blaire_ssp = {
    keywords = '(?i)SSP',
    field = 'Item Description',
    status = '不估計(Blaire)',
    remark = 'Blaire',
    note = 'Item Description包含SSP'
}
blaire_logistics_fee = {
    keywords = '(?i)Logistics fee|Logistic fee',
    field = 'Item Description',
    status = '不估計(Blaire)',
    remark = 'Blaire'
}
```

**優點**：
- 每條規則有語意明確的名稱（如 `blaire_ssp`）
- TOML 結構更扁平易讀
- 業務人員容易理解

**缺點**：
- 只支援單欄位條件，多欄位 AND/OR 難以表達
- 擴充新條件類型需要修改程式碼讀取邏輯

---

#### SPT 方式：帳號預測規則（Table 陣列，多欄位條件）

```toml
# [[...]] 陣列格式，每條規則獨立一個 table
[[spt_account_prediction.rules]]
rule_id = 1
account = "450014"
departments = ["S01 - Marketing & Publishing", "S02 - Business Development"]
description_keywords = "代收代付"
condition_desc = "Department 為 S01/S02/G07，摘要包含代收代付"

[[spt_account_prediction.rules]]
rule_id = 7
account = "650019"
departments = ["S01 - Marketing & Publishing"]
description_keywords = "AMS commission"
condition_desc = "Department 為行銷部門，Item Description 包含 AMS commission"
```

**程式碼端讀取**：

```python
rules = config_manager._config_toml.get('spt_account_prediction', {}).get('rules', [])
# → List of dicts，每個 dict 代表一條規則

for rule in sorted(rules, key=lambda r: r['rule_id']):
    matched = True

    if 'departments' in rule:
        if row['Department'] not in rule['departments']:
            matched = False

    if 'supplier' in rule and matched:
        if row['Supplier'] != rule['supplier']:
            matched = False

    if 'description_keywords' in rule and matched:
        if not re.search(rule['description_keywords'], row['Item Description'], re.IGNORECASE):
            matched = False

    if matched:
        row['account'] = rule['account']
        break  # 只匹配第一條規則
```

---

### 13.6 配置驅動業務規則的設計決策框架

在新專案設計時，可根據以下決策樹選擇配置方式：

```
業務規則需要配置化？
     │
     ├── 是 ──▶ 規則條件有多少個欄位？
     │               │
     │               ├── 單欄位（只比較一個欄的值）
     │               │       └──▶ SPT 具名規則（Key-Value TOML）
     │               │
     │               └── 多欄位（AND/OR 組合條件）
     │                       └──▶ SPX 條件引擎（[[conditions.checks]] 陣列）
     │
     └── 否 ──▶ 邏輯固定且簡單
                     └──▶ 直接寫在步驟的 execute() 中
```

**選擇 SPX 條件引擎的時機**：
1. 規則數量多（> 10 條）且業務人員需要自行調整
2. 每條規則有複雜的 AND/OR 多條件組合
3. 需要引用其他 TOML 設定（如 `fa_accounts`、`deposit_keywords`）
4. 不同處理類型（PO/PR）需要套用不同規則子集（用 `apply_to` 控制）

**選擇 SPT 具名規則的時機**：
1. 規則簡單（單欄位關鍵字比對）
2. 每條規則需要語意名稱（如 `blaire_ssp`）以便追溯
3. 業務人員需要直接閱讀 TOML 並理解規則意圖

---

### 13.7 完整的配置驅動業務邏輯實作範本

以下是一個從零開始設計新實體業務規則系統的完整範本：

#### Step 1：定義 TOML 規則

```toml
# config/stagging_my_entity.toml

# 參考資料（供規則引用）
[my_entity]
deposit_keywords = "押金|保證金|Deposit"
fa_accounts = ["199999", "180000"]
ops_requesters = ["John Smith", "Jane Doe"]

# Stage 1 規則（特殊狀態辨識）
[[my_entity_stage1_rules.conditions]]
priority = 1
status_value = "押金"
note = "Item Description 包含押金相關關鍵字"
combine = "and"
[[my_entity_stage1_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern_key = "my_entity.deposit_keywords"    # 引用上方定義的關鍵字
[[my_entity_stage1_rules.conditions.checks]]
field = "GL#"
type = "not_in_list"
list_key = "my_entity.fa_accounts"            # 引用上方 FA 科目清單

[[my_entity_stage1_rules.conditions]]
priority = 2
status_value = "已完成_租金"
note = "OPS 申請人 + 租金科目 + ERM <= 當月"
apply_to = ["PO"]                             # 只套用到 PO
combine = "and"
[[my_entity_stage1_rules.conditions.checks]]
field = "Requester"
type = "in_list"
list_key = "my_entity.ops_requesters"
[[my_entity_stage1_rules.conditions.checks]]
type = "erm_le_date"                          # 引用 prebuilt_masks
[[my_entity_stage1_rules.conditions.checks]]
type = "no_status"

# ERM 規則（主要業務邏輯判斷）
[[my_entity_erm_rules.conditions]]
priority = 1
status_value = "已入帳"
note = "有 GL DATE、ERM <= 當月、數量匹配、非 FA"
combine = "and"
[[my_entity_erm_rules.conditions.checks]]
field = "GL DATE"
type = "is_not_null"
[[my_entity_erm_rules.conditions.checks]]
type = "no_status"
[[my_entity_erm_rules.conditions.checks]]
type = "erm_le_date"
[[my_entity_erm_rules.conditions.checks]]
type = "qty_matched"
[[my_entity_erm_rules.conditions.checks]]
type = "not_fa"

[[my_entity_erm_rules.conditions]]
priority = 2
status_value = "未完成"
note = "ERM > 當月且在摘要期間內"
combine = "and"
[[my_entity_erm_rules.conditions.checks]]
type = "no_status"
[[my_entity_erm_rules.conditions.checks]]
type = "erm_in_range"
[[my_entity_erm_rules.conditions.checks]]
type = "erm_gt_date"
```

#### Step 2：實作 Stage1 步驟（混合模式）

```python
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine
import pandas as pd

class MyEntityStage1Step(PipelineStep):
    """
    第一階段狀態判斷（混合模式）
    - 配置驅動：押金、GL調整、關單等特殊狀態（透過引擎）
    - 程式碼保留：DataFrame 合併、日期格式轉換
    """

    def __init__(self, name: str = "MyEntityStage1", **kwargs):
        super().__init__(name, description="Stage 1 status evaluation", **kwargs)
        # ★ 指定 TOML 規則區段
        self.engine = SPXConditionEngine('my_entity_stage1_rules')

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()
        status_col = context.get_status_column()
        processing_date = context.metadata.processing_date

        # --- 程式碼保留的部分 ---
        # 日期格式轉換（引擎需要的 prebuilt_masks 所依賴的欄位）
        df['ERM_numeric'] = pd.to_datetime(
            df['Expected Received Month'], errors='coerce'
        ).dt.strftime('%Y%m').astype('Int64')

        # FA 科目清單
        from accrual_bot.utils.config import config_manager
        fa_accounts = config_manager._config_toml.get('my_entity', {}).get('fa_accounts', [])

        # --- 預先計算常用 mask（效能優化）---
        prebuilt_masks = {
            'no_status': df[status_col].isna() | (df[status_col] == ''),
            'erm_le_date': df['ERM_numeric'] <= processing_date,
            'erm_gt_date': df['ERM_numeric'] > processing_date,
            'is_fa': df['GL#'].isin(fa_accounts),
            'not_fa': ~df['GL#'].isin(fa_accounts),
            'qty_matched': df['Entry Quantity'] == df['Received Quantity'],
        }

        engine_context = {
            'processing_date': processing_date,
            'prebuilt_masks': prebuilt_masks,
        }

        # --- 配置驅動的部分：套用 TOML 規則 ---
        df, stats = self.engine.apply_rules(
            df, status_col, engine_context,
            processing_type=context.metadata.processing_type
        )

        context.update_data(df)
        context.set_variable('stage1_stats', stats)

        self.logger.info("Stage1 完成，規則命中統計：%s", stats)
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS, data=df)

    async def validate_input(self, context: ProcessingContext) -> bool:
        required = ['Item Description', 'GL#', 'Expected Received Month']
        return all(col in context.data.columns for col in required)
```

#### Step 3：實作 ERM 步驟（繼承 BaseERMEvaluationStep）

```python
from accrual_bot.core.pipeline.steps.base_evaluation import (
    BaseERMEvaluationStep, BaseERMConditions
)
from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine

class MyEntityERMStep(BaseERMEvaluationStep):
    """
    ERM 邏輯評估（配置驅動）
    繼承基類模板，但以條件引擎替換 _build_conditions/_apply_status_conditions
    """

    def __init__(self, name: str = "MyEntityERM", **kwargs):
        super().__init__(name, **kwargs)
        self.engine = SPXConditionEngine('my_entity_erm_rules')

    def _build_conditions(self, df, file_date, status_column):
        # 回傳基礎條件（引擎只需要 prebuilt_masks，不使用傳統 conditions 物件）
        return BaseERMConditions(
            no_status=(df[status_column].isna()),
            in_date_range=(df['ERM_numeric'] >= file_date),
            erm_before_or_equal_file_date=(df['ERM_numeric'] <= file_date),
            erm_after_file_date=(df['ERM_numeric'] > file_date),
            format_error=pd.Series([False] * len(df), index=df.index),
            out_of_date_range=(df['ERM_numeric'] < file_date),
            procurement_not_error=(df['採購備註'] != 'error'),
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        # ★ 用引擎替代傳統的逐條 df.loc 寫法
        fa_accounts = config_manager._config_toml.get('my_entity', {}).get('fa_accounts', [])
        prebuilt_masks = {
            'no_status': conditions.no_status,
            'erm_le_date': conditions.erm_before_or_equal_file_date,
            'erm_gt_date': conditions.erm_after_file_date,
            'erm_in_range': conditions.in_date_range,
            'not_fa': ~df['GL#'].isin(fa_accounts),
            'qty_matched': df['Entry Quantity'] == df['Received Quantity'],
            'qty_not_matched': ~(df['Entry Quantity'] == df['Received Quantity']),
            'not_error': conditions.procurement_not_error,
        }
        engine_context = {
            'processing_date': self._processing_date,
            'prebuilt_masks': prebuilt_masks,
        }
        df, _ = self.engine.apply_rules(df, status_column, engine_context)
        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        df['會計科目'] = df['GL#'].map(ref_account)
        df['負債科目'] = df['GL#'].map(ref_liability)
        return df
```

#### Step 4：在 stagging_my_entity.toml 啟用步驟

```toml
# config/stagging_my_entity.toml
[pipeline.my_entity]
enabled_po_steps = [
    "MyEntityDataLoading",
    "ColumnAddition",
    "DateLogic",
    "MyEntityStage1",      # ← 第一階段（特殊狀態辨識）
    "MyEntityERM",         # ← 第二階段（ERM 邏輯）
    "MyEntityExport",
]
```

---

### 13.8 配置驅動業務規則的整體流程圖

```
開發流程：
業務需求 → 寫入 TOML → 引擎自動執行 → 結果驗證

TOML 更新（無需改程式碼）：
  ↓
SPXConditionEngine._load_rules()
  ↓ 按 priority 排序
  ↓
apply_rules(df, status_column, context)
  ├── for each rule (按 priority 排序)：
  │       ├── 過濾 apply_to（PO/PR）
  │       ├── _build_combined_mask()
  │       │       └── for each check：
  │       │               └── _evaluate_check()
  │       │                       ├── 查 prebuilt_masks（快取）
  │       │                       ├── 欄位比對（contains/equals/in_list）
  │       │                       ├── ERM 日期計算
  │       │                       └── 帳務/備註/FA 判斷
  │       ├── 限縮至 no_status（+override_statuses）
  │       ├── df.loc[mask, status_col] = status_value
  │       └── 更新 no_status mask（已命中列不再參與後續規則）
  │
  └── return (df, stats)     ← stats = {'priority_1_已入帳': 120, ...}
```

---

*本文件最後更新：2026-03-07*
*對應專案版本：Accrual Bot（March 2026 — Entity Config Split + Procurement Pipeline）*
