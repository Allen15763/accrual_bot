# SPE Bank Recon - 專案架構參考手冊

> 本文件彙整 SPE Bank Reconciliation Automation 專案的架構設計、設計模式、介面定義及知識點，
> 作為未來類似任務（月結自動化、對帳系統、資料管線）的快速開發參考。

---

## 目錄

1. [專案概要](#1-專案概要)
2. [目錄結構](#2-目錄結構)
3. [分層架構總覽](#3-分層架構總覽)
4. [Layer 1: Core Pipeline 框架](#4-layer-1-core-pipeline-框架)
5. [Layer 2: Task 任務層](#5-layer-2-task-任務層)
6. [Layer 3: Data Sources 資料源層](#6-layer-3-data-sources-資料源層)
7. [Utils 工具層](#7-utils-工具層)
8. [配置系統](#8-配置系統)
9. [設計模式總覽](#9-設計模式總覽)
10. [關鍵介面與類別關係圖](#10-關鍵介面與類別關係圖)
11. [資料流全貌](#11-資料流全貌)
12. [開發慣例與規範](#12-開發慣例與規範)
13. [擴展指南](#13-擴展指南)
14. [常見問題與決策紀錄](#14-常見問題與決策紀錄)

---

## 1. 專案概要

| 項目 | 說明 |
|------|------|
| **名稱** | SPE Bank Reconciliation Automation |
| **用途** | 銀行對帳月結自動化，將原始銀行報表轉換為會計工作底稿與分錄 |
| **Tech Stack** | Python 3.11+, Pandas, DuckDB, Google Sheets API, TOML |
| **架構風格** | 三層管線框架 (Pipeline Framework) + 配置驅動 |
| **核心特性** | Checkpoint 斷點續跑、Template Method 消除重複、Mixin 可插拔、配置驅動銀行步驟 |

### 業務流程摘要

```
原始銀行報表 (Excel/DuckDB)
    │
    ▼
┌──────────────────────────────────────────┐
│ Phase 1: Escrow 對帳 (5家銀行)            │ Steps 1-7
│   載入參數 → 各銀行處理 → 匯總發票        │
├──────────────────────────────────────────┤
│ Phase 2: 分期報表處理                     │ Steps 8-9
│   載入分期 → 計算 Trust Account Fee       │
├──────────────────────────────────────────┤
│ Phase 3: Daily Check 驗證                │ Steps 10-14
│   FRR/DFR 處理 → APCC 手續費 → 核對      │
├──────────────────────────────────────────┤
│ Phase 4: 會計分錄生成                     │ Steps 15-16
│   分錄準備 → 輸出工作底稿 & Google Sheets  │
└──────────────────────────────────────────┘
    │
    ▼
輸出: Excel 工作底稿、Google Sheets、DuckDB 累積表
```

---

## 2. 目錄結構

```
project/
├── main.py                          # 入口點
├── pyproject.toml                   # 依賴管理
├── src/
│   ├── __init__.py
│   ├── core/                        # === 核心框架 (可移植) ===
│   │   ├── pipeline/                # Pipeline 引擎
│   │   │   ├── base.py              #   PipelineStep, StepResult, StepStatus
│   │   │   ├── pipeline.py          #   Pipeline, PipelineConfig, PipelineBuilder
│   │   │   ├── context.py           #   ProcessingContext (資料載體)
│   │   │   ├── checkpoint.py        #   CheckpointManager (斷點續跑)
│   │   │   └── steps/               #   通用步驟 (FunctionStep, ConditionalStep 等)
│   │   └── datasources/             # 資料源抽象
│   │       ├── base.py              #   DataSource ABC (含 TTL+LRU 快取)
│   │       ├── config.py            #   DataSourceConfig, DataSourceType
│   │       ├── excel_source.py      #   ExcelSource
│   │       ├── csv_source.py        #   CSVSource
│   │       ├── parquet_source.py    #   ParquetSource
│   │       ├── google_sheet_source.py # GoogleSheetsManager
│   │       └── factory.py           #   DataSourceFactory, DataSourcePool
│   │
│   ├── tasks/                       # === 業務任務層 ===
│   │   └── bank_recon/              # 銀行對帳任務
│   │       ├── __init__.py          #   BankReconTask 導出
│   │       ├── pipeline_orchestrator.py  # 任務編排 (6種執行模式)
│   │       ├── models/              #   資料模型
│   │       │   └── bank_data_container.py  # BankDataContainer, InstallmentReportData
│   │       ├── steps/               #   16 個處理步驟
│   │       │   ├── base_bank_step.py     # BaseBankProcessStep (Template Method)
│   │       │   ├── step_01_load_parameters.py
│   │       │   ├── step_02_process_cub.py   ~ step_06_process_taishi.py
│   │       │   ├── step_07_aggregate_escrow.py
│   │       │   ├── step_08 ~ step_16       # 分期、Daily Check、Entry 步驟
│   │       │   └── __init__.py
│   │       └── utils/               #   業務工具
│   │           ├── bank_processor.py      # BankProcessor 基類
│   │           ├── summary_formatter.py   # 統一摘要輸出
│   │           ├── apcc_calculator.py     # APCC 手續費計算
│   │           ├── entry_processor.py     # 會計分錄處理
│   │           ├── entry_transformer.py   # 分錄格式轉換
│   │           ├── frr_processor.py       # FRR 處理器
│   │           ├── dfr_processor.py       # DFR 處理器
│   │           ├── output_formatter.py    # Excel 輸出格式化
│   │           └── validation.py          # 驗證邏輯
│   │
│   ├── utils/                       # === 通用工具層 (可移植) ===
│   │   ├── config/                  #   ConfigManager (Singleton, Thread-safe)
│   │   ├── logging/                 #   Logger, StructuredLogger, ColoredFormatter
│   │   ├── helpers/                 #   file_utils
│   │   ├── database/               #   DuckDBManager 簡易包裝
│   │   ├── duckdb_manager/         #   DuckDB Manager 插件 (完整版)
│   │   │   ├── manager.py          #     DuckDBManager (Mixin 組合)
│   │   │   ├── config.py           #     DuckDBConfig
│   │   │   ├── exceptions.py       #     自定義異常
│   │   │   ├── operations/         #     操作 Mixin 集
│   │   │   │   ├── base.py         #       OperationMixin (事務 Helper)
│   │   │   │   ├── crud.py         #       CRUDMixin
│   │   │   │   ├── table_management.py  #  TableManagementMixin
│   │   │   │   ├── data_cleaning.py     #  DataCleaningMixin
│   │   │   │   └── transaction.py       #  TransactionMixin
│   │   │   ├── migration/          #     Schema 遷移
│   │   │   │   ├── schema_diff.py  #       Schema 差異比對
│   │   │   │   ├── strategies.py   #       遷移策略
│   │   │   │   └── migrator.py     #       遷移執行器
│   │   │   └── utils/              #     輔助工具
│   │   │       ├── type_mapping.py #       Pandas→DuckDB 型別對映
│   │   │       ├── query_builder.py#       SQL 建構器
│   │   │       └── logging.py      #       可插拔日誌
│   │   └── metadata_builder/       #   Metadata Builder 插件 (Bronze/Silver)
│   │       ├── builder.py          #     MetadataBuilder 核心類
│   │       ├── config.py           #     SourceSpec, SchemaConfig, ColumnSpec
│   │       ├── reader.py           #     SourceReader (強健讀取)
│   │       ├── exceptions.py       #     自定義異常
│   │       ├── processors/         #     Bronze/Silver 處理器
│   │       ├── transformers/       #     Column Mapper, Type Caster
│   │       └── validation/         #     Circuit Breaker
│   │
│   └── config/                      # === 配置檔案 ===
│       ├── config.toml              #   全域配置 (logging, paths, pipeline)
│       ├── bank_recon_config.toml   #   任務配置 (日期, 銀行, 業務規則, 輸出)
│       └── bank_recon_entry_monthly.toml  # 每月變動配置 (期初數, 特殊分錄)
│
├── tests/                           # 測試
├── checkpoints/                     # Checkpoint 儲存
├── output/                          # 產出檔案
├── logs/                            # 日誌
└── doc/                             # 文件
```

### 可移植模組對照

| 模組 | 位置 | 可移植性 | 說明 |
|------|------|---------|------|
| Pipeline Framework | `src/core/pipeline/` | 直接複製 | 通用管線引擎 |
| DataSource | `src/core/datasources/` | 直接複製 | 資料源抽象 + 快取 |
| DuckDB Manager | `src/utils/duckdb_manager/` | 直接複製 | 完整 DB 操作套件 |
| Metadata Builder | `src/utils/metadata_builder/` | 直接複製 | 髒資料處理工具 |
| Config Manager | `src/utils/config/` | 直接複製 | TOML 配置管理 |
| Logger | `src/utils/logging/` | 直接複製 | 彩色日誌 + 結構化日誌 |

---

## 3. 分層架構總覽

```
┌─────────────────────────────────────────────────────────────────┐
│                        main.py (入口)                           │
│                    BankReconTask.execute()                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│         Layer 2: Task Orchestration (任務編排層)                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ BankReconTask    │  │ PipelineOrch.    │  │ Steps (x16)   │  │
│  │ - execute(mode)  │→ │ - build_pipeline │→ │ - execute()   │  │
│  │ - resume()       │  │ - 6 modes        │  │ - validate()  │  │
│  └─────────────────┘  └──────────────────┘  └───────────────┘  │
│                                                                  │
│  Models: BankDataContainer     Utils: BankProcessor, Formatter   │
└────────────────────────────┬────────────────────────────────────┘
                             │ 使用
┌────────────────────────────▼────────────────────────────────────┐
│         Layer 1: Core Infrastructure (核心框架層)                 │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────────────┐  │
│  │ Pipeline     │  │ ProcessingCtx │  │ CheckpointManager   │  │
│  │ - add_step() │  │ - data        │  │ - save/load/resume  │  │
│  │ - execute()  │  │ - aux_data    │  │ - cleanup           │  │
│  │              │  │ - variables   │  │                     │  │
│  └──────────────┘  └───────────────┘  └─────────────────────┘  │
│                                                                  │
│  DataSource (ABC) → ExcelSource, CSVSource, GoogleSheets...      │
│  DataSourceFactory → 自動偵測類型建立                             │
└────────────────────────────┬────────────────────────────────────┘
                             │ 使用
┌────────────────────────────▼────────────────────────────────────┐
│         Layer 0: Utils (工具層)                                   │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────────────┐  │
│  │ DuckDBManager│  │ MetadataBlder │  │ ConfigManager       │  │
│  │ (Mixin架構)  │  │ (Bronze/Slvr) │  │ (Singleton)         │  │
│  ├──────────────┤  ├───────────────┤  ├─────────────────────┤  │
│  │ Logger       │  │ file_utils    │  │ helpers             │  │
│  └──────────────┘  └───────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**設計原則**：上層依賴下層，下層不依賴上層。工具層可獨立移植。

---

## 4. Layer 1: Core Pipeline 框架

### 4.1 PipelineStep (步驟基類)

**位置**: `src/core/pipeline/base.py`

```python
class PipelineStep(ABC, Generic[T]):
    """
    所有處理步驟的基類。

    子類必須實現 execute()，可選覆寫 validate_input() 和 rollback()。
    透過 __call__() 提供完整的執行生命週期：
    1. validate_input()      — 前置驗證
    2. _prerequisites 執行   — 前置動作
    3. execute() (含重試)    — 主邏輯
    4. _post_actions 執行    — 後置動作
    """

    def __init__(self, name, description="", required=True, retry_count=0, timeout=None):
        ...

    @abstractmethod
    def execute(self, context: ProcessingContext) -> StepResult: ...
    def validate_input(self, context) -> bool: ...       # 可選覆寫
    def rollback(self, context, error): ...              # 可選覆寫
    def add_prerequisite(self, action: Callable): ...    # 鏈式 API
    def add_post_action(self, action: Callable): ...     # 鏈式 API
```

**內建步驟變體**:

| 類別 | 用途 | 關鍵特性 |
|------|------|---------|
| `FunctionStep` | 用 lambda/函數定義簡單步驟 | 適合一次性邏輯 |
| `ConditionalStep` | 條件分支 | true_step / false_step |
| `SequentialStep` | 子步驟序列 | 可嵌套，stop_on_failure 控制 |

### 4.2 StepResult (步驟結果)

```python
@dataclass
class StepResult:
    step_name: str
    status: StepStatus          # SUCCESS / FAILED / SKIPPED / PENDING / RUNNING / RETRY
    data: Optional[DataFrame]   # 可選：步驟產出的 DataFrame
    error: Optional[Exception]
    message: Optional[str]
    duration: float
    metadata: Dict[str, Any]    # 自定義 metadata
```

### 4.3 ProcessingContext (資料載體)

**核心概念**：Context 是整條 Pipeline 的「記憶體」，在步驟間傳遞一切狀態。

```python
class ProcessingContext:
    # 主資料
    data: DataFrame                    # update_data(), get_data_copy()

    # 輔助資料 (多張表)
    _auxiliary_data: Dict[str, Any]    # add_auxiliary_data(), get_auxiliary_data()

    # 變數 (字串、數值等)
    _variables: Dict[str, Any]        # set_variable(), get_variable()

    # 錯誤/警告追蹤
    errors: List[str]                  # add_error()
    warnings: List[str]                # add_warning()

    # 驗證結果
    _validations: Dict[str, ValidationResult]  # add_validation()

    # 執行歷史
    _history: List[Dict]               # add_history()
```

**使用模式**：
- Step 1 用 `set_variable('db_path', ...)` 設定全域參數
- Step 2-6 用 `add_auxiliary_data('cub_containers', containers)` 存入處理結果
- Step 7 用 `get_auxiliary_data('cub_containers')` 取出所有銀行結果做匯總
- Step 16 用 `get_variable('output_dir')` 取得輸出路徑

### 4.4 Pipeline (管線主類)

```python
class Pipeline:
    def __init__(self, config: PipelineConfig): ...
    def add_step(self, step) -> Pipeline: ...      # 鏈式 API
    def add_steps(self, steps: List) -> Pipeline: ...
    def execute(self, context) -> Dict[str, Any]: ...  # 回傳完整執行報告
    def clone(self) -> Pipeline: ...
```

**PipelineConfig**:
```python
@dataclass
class PipelineConfig:
    name: str
    description: str = ""
    task_type: str = "transform"    # transform / compare / report
    stop_on_error: bool = True
    log_level: str = "INFO"
```

### 4.5 Checkpoint 機制

**目的**：長時間 Pipeline 中，任一步驟失敗後可從上次成功點續跑，無需重頭執行。

```
checkpoints/bank_recon_transform_after_Process_CTBC/
├── data.parquet              # 主 DataFrame
├── checkpoint_info.json      # 變數、metadata、歷史
└── auxiliary_data/           # 輔助 DataFrame
    ├── cub_containers.pkl
    └── ctbc_containers.pkl
```

**API**:
```python
# 執行時自動存 checkpoint
executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
result = executor.execute_with_checkpoint(context, save_after_each_step=True)

# 從 checkpoint 恢復
context = checkpoint_manager.load_checkpoint("bank_recon_transform_after_Process_CTBC")
result = executor.execute_with_checkpoint(context, start_from_step="Process_NCCC")

# 快速測試單一步驟
result = quick_test_step(checkpoint_name, step_to_test, pipeline)
```

**儲存格式選擇**：
- 優先 Parquet（高效、型別保留）
- Fallback Pickle（Parquet 失敗時，如含不支援的型別）
- 非 DataFrame 物件用 Pickle 儲存

---

## 5. Layer 2: Task 任務層

### 5.1 BankReconTask (任務主類)

**位置**: `src/tasks/bank_recon/pipeline_orchestrator.py`

```python
class BankReconTask:
    SUPPORTED_MODES = ['full', 'escrow', 'installment', 'daily_check', 'entry', 'full_with_entry']

    def __init__(self, config_path=None, config=None): ...
    def execute(self, mode='full', save_checkpoints=True, **kwargs) -> Dict: ...
    def resume(self, checkpoint_name, start_from_step, save_checkpoints=True) -> Dict: ...
    def validate_inputs(self, mode='full') -> Dict: ...
    def list_checkpoints(self) -> List[Dict]: ...
    def build_pipeline(self, mode='full') -> Pipeline: ...
```

**6 種執行模式**:

| 模式 | 步驟 | 用途 |
|------|------|------|
| `full` | 1-9 | 日常：Escrow 對帳 + 分期報表 |
| `full_with_entry` | 1-16 | 完整月結：含會計分錄 |
| `escrow` | 1-7 | 僅 Escrow 對帳 |
| `installment` | 1, 8-9 | 僅分期報表 |
| `daily_check` | 1, 10-14 | 僅 Daily Check |
| `entry` | 1, 10-16 | Daily Check + 會計分錄 |

**Pipeline 構建模式**：不使用 Builder Pattern，而是直接用 `Pipeline(config)` + `add_step()`，搭配私有輔助方法按模式組裝。

### 5.2 BaseBankProcessStep (Template Method Pattern)

**位置**: `src/tasks/bank_recon/steps/base_bank_step.py`

**消除重複的核心設計**：5 家銀行的處理邏輯重複度 87.4%，透過 Template Method 將共同流程提取到基類。

```python
class BaseBankProcessStep(PipelineStep):
    """
    定義統一的處理流程 (模板方法):
    1. _extract_parameters()  — 從 context 取公共參數
    2. _process_categories()  — 遍歷所有類別呼叫 Processor
    3. _store_results()       — 存入 context
    4. _log_totals()          — 記錄總計
    """

    @abstractmethod
    def get_bank_code(self) -> str: ...      # 返回 'cub', 'ctbc' 等
    @abstractmethod
    def get_processor_class(self): ...        # 返回 CUBProcessor 等

    # 可選覆寫
    def get_categories(self) -> List[str]: ... # 從配置讀取
```

**子類只需 15 行**:
```python
class ProcessCUBStep(BaseBankProcessStep):
    def get_bank_code(self) -> str:
        return 'cub'
    def get_processor_class(self):
        return CUBProcessor
```

**效果**：每個銀行步驟從 120-390 行 → 15-20 行，共減少 450+ 行。

### 5.3 BankDataContainer (資料模型)

```python
@dataclass
class BankDataContainer:
    bank_code: str          # 'cub', 'ctbc', 'nccc', 'ub', 'taishi'
    bank_name: str          # 顯示名稱
    category: str           # 'individual', 'nonindividual', 'installment', 'default'
    raw_data: DataFrame     # 原始查詢結果
    aggregated_data: Optional[DataFrame]

    # 金額欄位
    recon_amount: int                               # 當期請款
    recon_service_fee: int                          # 當期手續費
    recon_amount_for_trust_account_fee: int          # Trust Account Fee 金額
    amount_claimed_last_period_paid_by_current: int  # 前期發票當期撥款
    adj_service_fee: int                            # 調整手續費
    invoice_amount_claimed: int
    invoice_service_fee: Optional[int]
    metadata: Dict[str, Any]
```

### 5.4 配置驅動的銀行步驟

**新增銀行零代碼改動**：

```toml
# bank_recon_config.toml
[pipeline.bank_processing]
enabled_banks = ["cub", "ctbc", "nccc", "ub", "taishi", "new_bank"]  # 加入新銀行
respect_bank_enabled_flag = true

[banks.new_bank]
code = "new_bank"
name = "新銀行"
enabled = true
categories = ["default"]
```

Orchestrator 自動根據 `enabled_banks` 動態添加步驟，支援：
- 調整處理順序
- 暫時停用某銀行 (`enabled = false`)
- 無需改動 Python 程式碼

---

## 6. Layer 3: Data Sources 資料源層

### 6.1 DataSource 基類

```python
class DataSource(ABC):
    @abstractmethod
    def read(self, query=None, **kwargs) -> DataFrame: ...
    @abstractmethod
    def write(self, data: DataFrame, **kwargs) -> bool: ...
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]: ...

    # 內建增強功能
    def read_with_cache(self, query=None, **kwargs) -> DataFrame: ...  # TTL + LRU
    def clear_cache(self): ...
    def validate_connection(self) -> bool: ...
```

### 6.2 快取機制 (TTL + LRU)

```python
DataSourceConfig(
    cache_enabled=True,
    cache_ttl_seconds=300,     # 5 分鐘過期
    cache_max_items=10,        # 最多 10 個條目
    cache_eviction_policy="lru"
)
```

- Cache Key = MD5(query + kwargs)
- TTL 到期自動失效
- 超過 max_items 時 LRU 驅逐最舊條目

### 6.3 DataSourceFactory

```python
# 自動偵測類型
source = DataSourceFactory.create_from_file('./data.xlsx')

# 明確指定
config = DataSourceConfig(source_type=DataSourceType.EXCEL, connection_params={...})
source = DataSourceFactory.create(config)

# 註冊自定義類型
DataSourceFactory.register_source(DataSourceType.PARQUET, CustomParquetSource)
```

---

## 7. Utils 工具層

### 7.1 DuckDB Manager

**設計**: Mixin 組合模式，將功能拆分為獨立的 Mixin 類別。

```
DuckDBManager
  ├── CRUDMixin            → create_table_from_df, query_to_df, insert, upsert, delete
  ├── TableManagementMixin → table_exists, list_tables, backup, archive
  ├── DataCleaningMixin    → 資料清理與轉換
  └── TransactionMixin     → 事務處理、資料驗證
      └── OperationMixin   → _table_exists, _execute_sql, _atomic (context manager)
```

**配置多元化**:
```python
# 方式 1: 路徑
with DuckDBManager("./data.duckdb") as db: ...

# 方式 2: 配置物件
config = DuckDBConfig(db_path="./data.duckdb", timezone="Asia/Taipei")
with DuckDBManager(config) as db: ...

# 方式 3: 字典
with DuckDBManager({"db_path": "./data.duckdb"}) as db: ...

# 方式 4: TOML 檔案
config = DuckDBConfig.from_toml("config.toml", section="database")
```

**Schema 遷移系統**:
```
migration/
├── schema_diff.py    # 比對 DataFrame Schema vs DB Table Schema
│   ├── ChangeType: ADDED, REMOVED, TYPE_CHANGED, RENAMED
│   └── SchemaDiff.compare(db, table_name, df) -> SchemaDiffResult
├── strategies.py     # 遷移策略: RECREATE, ADD_COLUMNS, SAFE_MIGRATE
└── migrator.py       # 執行遷移: SchemaMigrator.migrate()
```

### 7.2 Metadata Builder (Bronze/Silver ELT)

**用途**: 處理高度不可控的源資料（Excel），採用 Bronze/Silver 架構。

```
MetadataBuilder
  ├── extract(file_path, sheet_name, header_row)   → Bronze: 全 string 讀取
  ├── transform(df, schema_config)                 → Silver: 欄位映射 + 型別轉換
  └── build(file_path, schema_config)              → Bronze + Silver 一次完成
```

**配置類**:
```python
# 源檔案規格
SourceSpec(file_type="excel", sheet_name="Sheet1", header_row=2, read_as_string=True)

# Schema 定義
SchemaConfig(columns=[
    ColumnSpec(source="交易日期", target="date", dtype="DATE", required=True),
    ColumnSpec(source=".*備註.*", target="remarks", dtype="VARCHAR"),  # 支援 regex
], circuit_breaker_threshold=0.3)
```

**在 Pipeline Step 中使用**:
```python
class LoadDataStep(PipelineStep):
    def execute(self, context):
        builder = MetadataBuilder()
        df_raw = builder.extract('./bank.xlsx', sheet_name='Sheet1', header_row=3)
        df_clean = builder.transform(df_raw, schema_config)

        with DuckDBManager(db_path) as db:
            db.create_table_from_df('bronze_table', df_raw, if_exists='replace')
            db.create_table_from_df('silver_table', df_clean, if_exists='replace')

        context.add_auxiliary_data('clean_data', df_clean)
```

### 7.3 ConfigManager

**特性**: Singleton + Thread-safe (Double-Checked Locking)

```python
from src.utils.config import ConfigManager

config = ConfigManager()  # 全域單例

# 讀取方式
config.get('dates', 'current_period_start')   # section, key
config.get('dates.current_period_start')       # 點號路徑
config.get_int('logging', 'max_file_size_mb')  # 型別安全
config.get_boolean('logging', 'color')
config.get_list('pipeline.bank_processing', 'enabled_banks')
config.get_nested('banks', 'cub', 'name')      # 深層巢狀
config.get_path('paths', 'log_path')            # 自動轉 Path
```

### 7.4 Logger

**結構化日誌** + **彩色輸出** + **檔案輪替**

```python
from src.utils import get_logger, get_structured_logger

# 一般日誌
logger = get_logger("pipeline.step_02")

# 結構化日誌 (帶語義方法)
slogger = get_structured_logger("bank_recon")
slogger.log_operation_start("Process CUB", bank="cub")
slogger.log_data_processing("escrow", record_count=500, processing_time=1.2)
slogger.log_step_result("Process_CUB", "success", duration=3.5)
slogger.log_error(exception, context="CUB Processing")
```

**日誌格式**:
```
# Console (彩色)
2026-01-15 14:30:00 | INFO     | pipeline.step_02 | execute:45 | 處理國泰世華

# File (詳細)
2026-01-15 14:30:00 | INFO     | pipeline.step_02 | step_02_process_cub.execute:45 | 12345-67890 | 處理國泰世華
```

---

## 8. 配置系統

### 8.1 配置架構

```
src/config/
├── config.toml                    # 全域配置 (不常改)
│   ├── [general]                  #   專案名稱、版本
│   ├── [logging]                  #   日誌等級、格式、輸出
│   ├── [paths]                    #   日誌、輸出、輸入、暫存目錄
│   ├── [datasource]               #   編碼、引擎、快取
│   ├── [pipeline]                 #   遇錯停止、重試
│   └── [task]                     #   預設類型、超時
│
├── bank_recon_config.toml         # 任務配置 (每月調整)
│   ├── [task]                     #   任務名稱、版本
│   ├── [pipeline.bank_processing] #   啟用銀行列表、處理模式
│   ├── [dates]                    #   ★ 當期/前期日期 (Manual)
│   ├── [database]                 #   DB 路徑
│   ├── [output]                   #   輸出路徑、檔名範本
│   ├── [business_rules]           #   ★ 調扣、匯費、回饋金 (Manual)
│   ├── [banks.*]                  #   5 家銀行配置
│   ├── [installment]              #   分期報表路徑、格式
│   ├── [daily_check]              #   FRR/DFR 路徑、欄位映射
│   ├── [entry]                    #   會計科目、分錄映射
│   └── [google_sheets]            #   輸出 Sheet 名稱
│
└── bank_recon_entry_monthly.toml  # 每月變動配置
    └── [opening_balance]          #   ★ 期初數 (Manual)
```

### 8.2 配置載入方式

**Task 級配置**: 使用 `tomllib` 直接載入
```python
class BankReconTask:
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path or default_path)
```

**全域配置**: 使用 `ConfigManager` 單例
```python
from src.utils.config import ConfigManager
config = ConfigManager()
```

**DuckDB 配置**: 使用 `DuckDBConfig` dataclass
```python
config = DuckDBConfig.from_toml("config.toml", section="database")
```

### 8.3 配置設計原則

1. **靜態 vs 動態分離**：不常變的放 config.toml，每月調整的放 task_config.toml
2. **Manual 標記**：需人工確認的配置項用 `# Manual` 註解標記
3. **模板變數**：支援 `{period}` 等模板變數在路徑中替換
4. **向後兼容**：新增配置項都有預設值，不影響舊配置

---

## 9. 設計模式總覽

### 9.1 使用的設計模式

| 模式 | 位置 | 用途 |
|------|------|------|
| **Template Method** | `BaseBankProcessStep` | 統一銀行處理流程，子類只實現差異點 |
| **Strategy** | `BankProcessor` 各實現 | 不同銀行的計算邏輯作為策略替換 |
| **Factory** | `DataSourceFactory` | 根據檔案類型自動建立資料源 |
| **Singleton** | `ConfigManager`, `Logger` | 全域唯一實例，Thread-safe |
| **Mixin** | `DuckDBManager` operations | 功能按職責拆分，組合使用 |
| **Pipeline** | `Pipeline` + `PipelineStep` | 步驟序列化執行，支援條件分支 |
| **Context Object** | `ProcessingContext` | 步驟間共享狀態的載體 |
| **Checkpoint/Memento** | `CheckpointManager` | 儲存/恢復 Pipeline 執行狀態 |
| **Builder** | `PipelineBuilder` (備用) | 流式 API 構建 Pipeline |
| **Observer** | `_prerequisites` / `_post_actions` | 步驟執行前後的鉤子 |
| **Dataclass as Value Object** | `BankDataContainer`, `StepResult` | 不可變資料傳輸 |

### 9.2 Template Method 詳解 (核心模式)

```
BaseBankProcessStep (abstract)
│
├── execute()                    ← 模板方法 (不可覆寫的流程)
│   ├── _extract_parameters()    ← 固定：從 context 取參數
│   ├── _process_categories()    ← 固定：遍歷類別
│   │   ├── _create_processor()  ← 固定：建立 Processor
│   │   └── processor.process()  ← ★ 策略：由 get_processor_class() 決定
│   ├── _store_results()         ← 固定：存入 context
│   └── _log_totals()            ← 固定：記錄總計
│
├── get_bank_code() → str        ← ★ 子類必須實現
└── get_processor_class() → Type ← ★ 子類必須實現
```

### 9.3 Mixin 組合模式詳解

```
OperationMixin (基類)
├── _table_exists()
├── _execute_sql()
├── _atomic()         ← context manager 事務

CRUDMixin(OperationMixin)
├── create_table_from_df()
├── insert_df_into_table()
├── upsert_df_into_table()
├── query_to_df()
└── delete_data()

TableManagementMixin(OperationMixin)
├── list_tables()
├── backup_table()
└── archive_table()

DataCleaningMixin(OperationMixin)
└── clean_data()

TransactionMixin(OperationMixin)
└── validate_data()

DuckDBManager(CRUDMixin, TableManagementMixin, DataCleaningMixin, TransactionMixin)
├── __init__() → _connect(), _setup_timezone()
├── close()
└── __enter__/__exit__  ← Context Manager
```

**Mixin 設計要點**：
- 所有 Mixin 繼承 `OperationMixin`，它定義了 `conn`, `config`, `logger` 的型別提示
- 實際的 `conn`, `config`, `logger` 由 `DuckDBManager.__init__()` 提供
- 用 `TYPE_CHECKING` 避免循環 import

---

## 10. 關鍵介面與類別關係圖

### 10.1 Pipeline 框架類別圖

```
                    ┌──────────────────┐
                    │  PipelineConfig  │
                    │  - name          │
                    │  - stop_on_error │
                    └────────┬─────────┘
                             │ 1:1
                    ┌────────▼─────────┐
                    │    Pipeline      │
                    │  - steps: List   │
                    │  + add_step()    │
                    │  + execute()     │
                    └────────┬─────────┘
                             │ 1:N
               ┌─────────────┴─────────────┐
               │                           │
      ┌────────▼─────────┐      ┌─────────▼─────────┐
      │  PipelineStep    │      │  ProcessingContext  │
      │  (ABC)           │─────→│  - data             │
      │  + execute()     │      │  - _auxiliary_data   │
      │  + validate()    │      │  - _variables        │
      │  + rollback()    │      │  - errors/warnings   │
      └────────┬─────────┘      └─────────────────────┘
               │
    ┌──────────┼──────────────┬──────────────┐
    │          │              │              │
┌───▼───┐ ┌───▼───────┐ ┌───▼──────┐ ┌────▼──────────┐
│Function│ │Conditional│ │Sequential│ │BaseBankProcess│
│Step    │ │Step       │ │Step      │ │Step (Template)│
└────────┘ └───────────┘ └──────────┘ └───────┬───────┘
                                               │
                              ┌────────┬───────┴───────┬────────┐
                              │        │               │        │
                         ProcessCUB  ProcessCTBC  ProcessNCCC  ...
                         (15 lines)  (15 lines)   (15 lines)
```

### 10.2 DuckDB Manager 類別圖

```
┌──────────────────────────────────────────────────────────────┐
│                     DuckDBConfig                              │
│  db_path, timezone, read_only, logger, log_level             │
│  + from_dict(), from_toml(), from_yaml(), from_path()        │
│  + copy(**overrides)                                          │
└──────────────────────────────────┬───────────────────────────┘
                                   │ 1:1
┌──────────────────────────────────▼───────────────────────────┐
│  DuckDBManager(CRUD, TableMgmt, DataCleaning, Transaction)   │
│                                                               │
│  ┌─ CRUDMixin ────────────────────────────────────────────┐  │
│  │  create_table_from_df(table, df, if_exists)            │  │
│  │  query_to_df(sql) → DataFrame                          │  │
│  │  insert_df_into_table(table, df)                       │  │
│  │  upsert_df_into_table(table, df, key_columns)          │  │
│  │  delete_data(table, condition)                          │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌─ TableManagementMixin ─────────────────────────────────┐  │
│  │  list_tables(), table_exists()                         │  │
│  │  backup_table(), archive_table()                       │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌─ OperationMixin (共用) ────────────────────────────────┐  │
│  │  _table_exists(), _execute_sql(), _atomic()            │  │
│  │  _begin(), _commit(), _rollback()                      │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  Migration: SchemaDiff → SchemaMigrator (策略模式)             │
└───────────────────────────────────────────────────────────────┘
```

### 10.3 資料源類別圖

```
                    ┌──────────────────┐
                    │ DataSourceConfig │
                    │ - source_type    │
                    │ - cache_ttl      │
                    │ - cache_max_items│
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │   DataSource     │
                    │   (ABC)          │
                    │ + read()         │
                    │ + write()        │
                    │ + read_with_cache│
                    └────────┬─────────┘
                             │
          ┌──────────┬───────┴───────┬──────────────┐
          │          │               │              │
     ExcelSource  CSVSource   ParquetSource  GoogleSheets
                                               Manager
          │          │               │              │
          └──────────┴───────┬───────┴──────────────┘
                             │
                    DataSourceFactory
                    + create(config)
                    + create_from_file(path)
                    + register_source(type, cls)
```

---

## 11. 資料流全貌

### 11.1 完整執行流程 (full_with_entry)

```
main.py
  │
  ▼
BankReconTask.execute(mode='full_with_entry')
  │
  ├─ validate_inputs()         → 檢查檔案、DB、配置
  ├─ prepare_context()         → 建立 ProcessingContext
  ├─ build_pipeline()          → 組裝 16 步驟
  └─ PipelineWithCheckpoint.execute_with_checkpoint()
      │
      ├─ Step 01: LoadParameters
      │   └─ 從 TOML 載入日期、路徑 → context.set_variable()
      │
      ├─ Step 02-06: Process [CUB|CTBC|NCCC|UB|Taishi]
      │   └─ BaseBankProcessStep.execute()
      │       ├─ _extract_parameters() ← context.get_variable()
      │       ├─ for category in categories:
      │       │   ├─ Processor.load_data(DuckDBManager)
      │       │   ├─ Processor.calculate_recon_amounts()
      │       │   └─ → BankDataContainer
      │       └─ _store_results() → context.add_auxiliary_data()
      │
      ├─ Step 07: AggregateEscrow
      │   └─ 取出所有銀行 containers → 匯總發票 → 輸出 Excel
      │
      ├─ Step 08-09: Installment + TrustAccount
      │   └─ 讀取分期報表 → 計算 Trust Account Fee
      │
      ├─ Step 10: LoadDailyCheckParams
      │   └─ 載入 FRR/DFR 配置、手續費率
      │
      ├─ Step 11-12: Process FRR/DFR
      │   └─ 讀取 Excel → 標準化 → 存入 context
      │
      ├─ Step 13: CalculateAPCC
      │   └─ APCC 手續費計算
      │
      ├─ Step 14: ValidateDailyCheck
      │   └─ FRR 手續費 vs 各銀行請款 核對
      │
      ├─ Step 15: PrepareEntries
      │   └─ 整理科目 → 寬轉長格式 → 生成大 Entry
      │
      └─ Step 16: OutputWorkpaper
          └─ 輸出 Excel + 寫入 Google Sheets
```

### 11.2 Context 資料流

```
Step 01 ──set_variable──→  beg_date, end_date, db_path, ...
Step 02 ──add_aux_data──→  cub_containers: [Container, Container]
Step 03 ──add_aux_data──→  ctbc_containers: [Container, Container]
Step 04 ──add_aux_data──→  nccc_container: Container
Step 05 ──add_aux_data──→  ub_containers: [Container, Container]
Step 06 ──add_aux_data──→  taishi_container: Container
Step 07 ←─get_aux_data──  所有 *_container(s) → 匯總
Step 07 ──add_aux_data──→  escrow_summary: DataFrame
Step 08 ──add_aux_data──→  installment_data: Dict
Step 09 ──add_aux_data──→  trust_account_fee: DataFrame
Step 10 ──set_variable──→  frr_config, dfr_config, fee_rates, ...
Step 11 ──add_aux_data──→  frr_data: DataFrame
Step 12 ──add_aux_data──→  dfr_data: DataFrame
Step 13 ──add_aux_data──→  apcc_result: DataFrame
Step 14 ──add_aux_data──→  validation_result: DataFrame
Step 15 ──add_aux_data──→  entries: DataFrame, big_entry: DataFrame
Step 16 ←─get_aux_data──  所有結果 → 輸出
```

---

## 12. 開發慣例與規範

### 12.1 命名規範

| 項目 | 規範 | 範例 |
|------|------|------|
| 步驟檔案 | `step_##_descriptive_name.py` | `step_02_process_cub.py` |
| 步驟類別 | `Process{Bank}Step` | `ProcessCUBStep` |
| 處理器類別 | `{Bank}Processor` | `CUBProcessor` |
| 配置檔案 | `{task_name}_config.toml` | `bank_recon_config.toml` |
| 期間格式 | `YYYYMM` | `202512` |
| 日期格式 | `YYYY-MM-DD` | `2025-12-01` |
| 銀行代碼 | 小寫英文 | `cub`, `ctbc`, `nccc`, `ub`, `taishi` |

### 12.2 Python 特性使用

- **Type Hints**: 所有函數必須標註
- **`|` Union 語法**: `str | int` 取代 `Union[str, int]`
- **`match` 語句**: 適用時使用
- **`@dataclass`**: 用於資料容器、配置類、結果類
- **`ABC`**: 用於定義抽象介面

### 12.3 錯誤處理策略

```
Step 層級:
├── 每個 Step 的 execute() 內部 try-catch
├── 失敗時回傳 StepResult(status=FAILED, error=e)
├── required=True 的 Step 失敗 → Pipeline 停止
└── required=False 的 Step 失敗 → 記錄警告，繼續

Pipeline 層級:
├── stop_on_error=True → 遇到 FAILED 立即停止
└── 所有錯誤記錄在 context.errors

Context 層級:
├── context.add_error() → 記錄嚴重錯誤
├── context.add_warning() → 記錄警告
└── context.add_validation() → 結構化驗證結果
```

### 12.4 日誌規範

```python
# 步驟層級
self.logger.info(f"處理期間: {beg_date} ~ {end_date}")          # 關鍵操作
self.logger.info(f"載入 {table_name} 資料: {len(data)} 筆")     # 資料量
self.logger.warning(f"找不到銀行配置: {bank_code}")              # 非致命問題
self.logger.error(f"處理失敗: {str(e)}")                         # 錯誤

# 結構化日誌
slogger.log_operation_start("Process CUB")
slogger.log_data_processing("escrow", record_count=500)
slogger.log_step_result("Process_CUB", "success", duration=3.5)
```

---

## 13. 擴展指南

### 13.1 新增銀行 (最少改動)

1. **配置**: `bank_recon_config.toml` 加入銀行區段 + `enabled_banks` 列表
2. **Processor**: 新增 `{bank}_processor.py`，繼承 `BankProcessor`
3. **Step**: 新增 `step_##_process_{bank}.py`，只需 15 行
4. **Orchestrator**: 在 `step_classes` dict 加入映射
5. **測試**: 驗證

### 13.2 新增 Pipeline 模式

```python
# pipeline_orchestrator.py
class BankReconTask:
    SUPPORTED_MODES = [..., 'new_mode']

    def _create_new_mode_pipeline(self) -> Pipeline:
        pipeline = Pipeline(self._get_pipeline_config())
        self._add_parameter_step(pipeline)
        # 加入需要的步驟
        return pipeline
```

### 13.3 新增資料源類型

```python
# 1. 實現 DataSource 子類
class MongoSource(DataSource):
    def read(self, query=None, **kwargs) -> DataFrame: ...
    def write(self, data: DataFrame, **kwargs) -> bool: ...
    def get_metadata(self) -> Dict: ...

# 2. 擴展 DataSourceType
class DataSourceType(Enum):
    MONGO = "mongo"

# 3. 註冊到工廠
DataSourceFactory.register_source(DataSourceType.MONGO, MongoSource)
```

### 13.4 新增 DuckDB 操作 (Mixin)

```python
# operations/analytics.py
class AnalyticsMixin(OperationMixin):
    def pivot_table(self, table_name, ...): ...
    def window_function(self, ...): ...

# manager.py - 加入 Mixin
class DuckDBManager(CRUDMixin, TableManagementMixin, ..., AnalyticsMixin): ...
```

### 13.5 新增任務 (全新業務)

```
src/tasks/new_task/
├── __init__.py
├── pipeline_orchestrator.py    # NewTask 主類
├── steps/
│   ├── step_01_xxx.py         # 繼承 PipelineStep
│   └── step_02_xxx.py
├── models/
│   └── data_container.py      # 業務資料模型
└── utils/
    └── processor.py           # 業務處理器

src/config/
└── new_task_config.toml       # 任務配置
```

---

## 14. 常見問題與決策紀錄

### 14.1 為什麼用 ProcessingContext 而不是函數參數傳遞？

**問題**: 16 個步驟之間需要傳遞大量異質資料（DataFrame、變數、驗證結果等）。

**決策**: 使用 Context Object 模式。所有步驟共享一個 `ProcessingContext`，每個步驟從中取資料、處理後放回。

**好處**:
- 步驟的 `execute()` 簽名統一，只接收 context
- 新增共享資料不需改動介面
- Checkpoint 只需序列化一個 context 即可

### 14.2 為什麼不用 PipelineBuilder 而用 Pipeline 直接組裝？

**決策**: 實際專案中 Pipeline 構建邏輯較複雜（配置驅動、條件判斷），Builder 模式反而增加間接層。直接用 `Pipeline(config)` + 私有輔助方法更清晰。

### 14.3 為什麼 DuckDB Manager 用 Mixin 而不是繼承？

**決策**: 功能劃分為 CRUD、TableManagement、DataCleaning、Transaction 四個正交維度，Mixin 允許按需組合。未來可輕鬆加入 AnalyticsMixin 等。

**注意**: Mixin 中透過 `TYPE_CHECKING` + 型別提示確保 IDE 支援，但實際的 `conn`, `config`, `logger` 由主類提供。

### 14.4 為什麼 Metadata Builder 和 DuckDB Manager 分開？

**決策**: 分離關注點。
- MetadataBuilder 負責「髒資料讀取與清洗」(Bronze/Silver)
- DuckDBManager 負責「DB 操作」(存取、遷移)
- 呼叫者 (Pipeline Step) 決定何時存入 DB、存到哪張表

### 14.5 為什麼用 TOML 而不是 YAML？

**決策**:
- Python 3.11+ 內建 `tomllib`，無需額外依賴
- TOML 的巢狀結構（`[banks.cub.tables]`）適合配置檔
- 型別明確（不像 YAML 有隱式轉換問題）

### 14.6 Checkpoint 儲存格式選擇

| 格式 | 用途 | 優點 | 缺點 |
|------|------|------|------|
| Parquet | DataFrame 主要格式 | 高效壓縮、保留型別 | 不支援所有 Python 型別 |
| Pickle | Fallback + 非 DataFrame 物件 | 支援任何 Python 物件 | 安全性低、不跨版本 |
| JSON | 變數和 metadata | 人類可讀 | 不支援 DataFrame |

### 14.7 執行流程設計：Round 1 + Round 2

實際月結作業分兩輪：
1. **Round 1** (拿到銀行報表後): `task.execute(mode='full')` — Escrow + 分期
2. **Round 2** (拿到 FRR/DFR 後): `task.execute(mode='full_with_entry')` — 完整流程

這就是為什麼 `main.py` 中有兩次 execute 呼叫。

---

## 附錄: 快速啟動範本

### A. 最小可行 Pipeline

```python
from src.core.pipeline import Pipeline, PipelineConfig, PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext

class MyStep(PipelineStep):
    def execute(self, context: ProcessingContext) -> StepResult:
        # 你的業務邏輯
        data = context.get_variable('input_data')
        result = process(data)
        context.add_auxiliary_data('result', result)
        return StepResult(step_name=self.name, status=StepStatus.SUCCESS)

# 組裝並執行
pipeline = Pipeline(PipelineConfig(name="my_pipeline"))
pipeline.add_step(MyStep(name="My_Step"))

context = ProcessingContext(task_name="my_task")
context.set_variable('input_data', ...)

result = pipeline.execute(context)
```

### B. 最小可行 Task

```python
class MyTask:
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)

    def execute(self, mode='default'):
        pipeline = self._build_pipeline(mode)
        context = self._prepare_context()
        return pipeline.execute(context)

    def _build_pipeline(self, mode):
        pipeline = Pipeline(PipelineConfig(name="my_task"))
        # 根據 mode 添加步驟
        return pipeline

    def _prepare_context(self):
        context = ProcessingContext(task_name="my_task")
        # 設定變數
        return context
```

### C. DuckDB Manager 常見操作

```python
with DuckDBManager("./data.duckdb") as db:
    # 建表
    db.create_table_from_df("users", df, if_exists="replace")

    # 查詢
    result = db.query_to_df("SELECT * FROM users WHERE age > 18")

    # 原子操作
    with db._atomic():
        db.delete_data("users", "status = 'inactive'")
        db.insert_df_into_table("archive_users", inactive_df)
```

---

> **文件版本**: v1.0 | **更新日期**: 2026-03-05 | **基於專案版本**: 2.0.1
