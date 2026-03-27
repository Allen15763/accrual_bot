# 財務資料處理系統 — 整合設計參考手冊

> **目的**：整合 Accrual Bot 與 SPE Bank Recon 兩個專案的架構知識與實戰經驗，提煉成一份**下一個類似專案的設計藍圖**。
> **預設情境**：80% 以上的資料來自人工整理的 Excel、Google Sheet、銀行報表等半結構/非結構資料，沒有上下游的資料格式共識。
> **閱讀對象**：需要快速設計資料處理/月結自動化/財務對帳系統的工程師。

---

## 目錄

1. [兩個專案的對比與互補](#1-兩個專案的對比與互補)
2. [整合後的分層架構](#2-整合後的分層架構)
3. [推薦目錄結構（Starter Kit）](#3-推薦目錄結構starter-kit)
4. [髒資料處理策略（最關鍵課題）](#4-髒資料處理策略最關鍵課題)
5. [核心框架層：Pipeline 系統](#5-核心框架層pipeline-系統)
6. [業務規則配置化：兩種路徑](#6-業務規則配置化兩種路徑)
7. [資料持久化：DuckDB 作為中間層](#7-資料持久化duckdb-作為中間層)
8. [設計模式整合目錄](#8-設計模式整合目錄)
9. [配置系統設計](#9-配置系統設計)
10. [工具層：可直接搬移的模組](#10-工具層可直接搬移的模組)
11. [新專案啟動路徑圖](#11-新專案啟動路徑圖)
12. [關鍵決策框架](#12-關鍵決策框架)
13. [完整程式碼模板集](#13-完整程式碼模板集)

---

## 1. 兩個專案的對比與互補

### 1.1 專案定性

| 項目 | Accrual Bot | SPE Bank Recon |
|------|-------------|----------------|
| **核心任務** | PO/PR 應計數據分類與對帳 | 銀行存款月結自動化 |
| **主要資料源** | ERP 匯出 CSV、人工 Excel 底稿 | 銀行報表 Excel、DuckDB 存量表 |
| **輸出** | 月結 Excel 工作底稿 | 會計分錄、底稿、Google Sheets |
| **複雜度來源** | 業務分類規則多（11+ 條件）、多實體 | 多銀行異構格式、複雜計算 |
| **UI 需求** | 有 Streamlit Web UI | 純 CLI |
| **Pipeline 長度** | 10-15 步 | 16 步（分 4 階段）|
| **執行模式** | 1 種（按類型） | 6 種（full/escrow/entry 等）|

### 1.2 互補優勢

```
Accrual Bot 的強項：
  ✅ 配置驅動條件引擎（SPXConditionEngine）— TOML 定義業務分類規則
  ✅ Streamlit Web UI 整合（雙層頁面架構）
  ✅ 豐富的共用步驟庫（DateLogic、AccountMapping 等）
  ✅ 複雜多層 ERM 評估邏輯（模板方法基類）

SPE Bank Recon 的強項：
  ✅ Bronze/Silver ELT 架構 — 專為雜亂 Excel 設計
  ✅ DuckDB 作為高效中間層（SQL 查詢 + Schema 遷移）
  ✅ TTL+LRU 多層快取機制
  ✅ Mixin 組合的 DuckDB Manager（職責清晰分離）
  ✅ 多執行模式 + Checkpoint 斷點續跑
  ✅ BankDataContainer 作為顯式領域模型
```

### 1.3 整合後的核心觀念

兩個專案共享同一套**核心框架核**，差異在業務層：

```
共享核心（可直接複製）：
  Pipeline + PipelineStep + ProcessingContext + CheckpointManager
  DataSource 抽象（Excel/CSV/Parquet/DuckDB/GoogleSheets）
  ConfigManager（單例、Thread-safe）
  Logger（彩色 + 結構化）

accrual_bot 特有：
  SPXConditionEngine（TOML 規則引擎）
  BaseLoadingStep / BaseERMEvaluationStep（評估模板）
  Streamlit UI 層

spe_bank_recon 特有：
  MetadataBuilder（Bronze/Silver）
  DuckDBManager（Mixin 架構 + Schema 遷移）
  TTL+LRU Cache
```

---

## 2. 整合後的分層架構

```
┌──────────────────────────────────────────────────────────────────────┐
│                        第 0 層：入口（Entry）                          │
│  main.py / main_streamlit.py                                          │
│  └── TaskClass.execute(mode='full') 或 UI 觸發                        │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────┐
│                    第 1 層：UI 層（選配）                              │
│  [僅在需要非技術人員操作時使用 Streamlit]                               │
│                                                                       │
│  pages/          components/       services/       models/            │
│  (導向工作流)    (可複用元件)       (Pipeline 橋接)  (Session State)    │
│                                                                       │
│  關鍵：UnifiedPipelineService 作為 UI 與 Pipeline 的唯一橋接點          │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────┐
│                    第 2 層：任務編排層（Task）                          │
│                                                                       │
│  TaskClass（如 BankReconTask、SPTOrchestrator）                        │
│  ├── execute(mode)        ← 多種執行模式（full/escrow/entry 等）        │
│  ├── resume(checkpoint)   ← 斷點續跑                                  │
│  ├── build_pipeline(mode) ← 根據模式動態組裝步驟序列                   │
│  └── validate_inputs()    ← 前置驗證                                  │
│                                                                       │
│  步驟配置：從 {task}_config.toml 讀取啟用的步驟列表                    │
│  步驟工廠：_create_step(step_name, ...) 根據名稱建立實例               │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────┐
│                    第 3 層：核心框架層（Core）                          │
│                                                                       │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────────┐    │
│  │   Pipeline     │  │ ProcessingContext │  │ CheckpointManager  │    │
│  │  PipelineStep  │  │  data / aux /    │  │  save/load/resume  │    │
│  │  PipelineConfig│  │  variables/      │  │  Parquet+Pickle    │    │
│  └────────────────┘  │  errors/warnings │  └────────────────────┘    │
│                       └──────────────────┘                            │
│                                                                       │
│  步驟基類（Template Method）：                                         │
│  BaseLoadingStep      ← 資料載入骨架（並發載入、路徑正規化）            │
│  BaseERMEvaluationStep← 業務評估骨架（條件建構→狀態套用→會計欄位）      │
│  BaseBankProcessStep  ← 銀行處理骨架（參數提取→類別迴圈→結果存儲）      │
│                                                                       │
│  DataSource 層：                                                       │
│  ExcelSource / CSVSource / ParquetSource / DuckDBSource               │
│  GoogleSheetsManager / DataSourceFactory / DataSourcePool             │
│                       + TTL+LRU 快取                                  │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────┐
│                    第 4 層：工具層（Utils）                            │
│                                                                       │
│  ConfigManager     ← TOML 配置單例（Thread-safe）                     │
│  Logger            ← 彩色 + 結構化日誌                                │
│  MetadataBuilder   ← Bronze/Silver ELT（★ 髒資料核心工具）            │
│  DuckDBManager     ← Mixin 組合的 DB 操作套件 + Schema 遷移            │
│  file_utils        ← 路徑驗證、目錄建立、安全複製                     │
└──────────────────────────────────────────────────────────────────────┘
```

**依賴方向**：上層依賴下層，下層完全不知道上層存在。工具層（第 4 層）可獨立移植到任何新專案。

---

## 3. 推薦目錄結構（Starter Kit）

```
project/
│
├── main.py                          # CLI 入口點
│
├── src/
│   │
│   ├── core/                        # ★ 核心框架（不含業務邏輯，可直接複製）
│   │   ├── pipeline/
│   │   │   ├── base.py              #   PipelineStep(ABC), StepResult, StepStatus
│   │   │   ├── pipeline.py          #   Pipeline, PipelineConfig
│   │   │   ├── context.py           #   ProcessingContext（資料載體）
│   │   │   └── checkpoint.py        #   CheckpointManager（Parquet+Pickle）
│   │   └── datasources/
│   │       ├── base.py              #   DataSource(ABC) + TTL/LRU 快取
│   │       ├── config.py            #   DataSourceConfig, DataSourceType
│   │       ├── excel_source.py
│   │       ├── csv_source.py
│   │       ├── parquet_source.py
│   │       ├── google_sheet_source.py
│   │       └── factory.py           #   DataSourceFactory + DataSourcePool
│   │
│   ├── tasks/                       # ★ 業務任務（每個任務一個子目錄）
│   │   └── {task_name}/
│   │       ├── __init__.py          #   TaskClass 導出
│   │       ├── pipeline_orchestrator.py  # 任務主類 + Pipeline 組裝
│   │       ├── steps/               #   步驟實作（繼承核心基類）
│   │       │   ├── base_{task}_step.py   # 任務特有基類（可選，消除步驟間重複）
│   │       │   ├── step_01_xxx.py
│   │       │   ├── step_02_xxx.py
│   │       │   └── ...
│   │       ├── models/              #   領域資料模型（@dataclass）
│   │       │   └── data_container.py
│   │       └── utils/               #   業務工具（Processor、Formatter、計算器）
│   │           ├── processor.py
│   │           ├── formatter.py
│   │           └── validation.py
│   │
│   ├── ui/                          # ★ Web UI（選配，技術+非技術用戶並存時才需要）
│   │   ├── app.py                   #   Session state 初始化
│   │   ├── config.py                #   ENTITY_CONFIG, REQUIRED_FILES, FILE_LABELS
│   │   ├── services/
│   │   │   └── unified_pipeline_service.py  # UI 橋接層（唯一橋接點）
│   │   ├── components/              #   可複用 UI 元件
│   │   └── pages/                   #   業務邏輯頁面（標準檔名）
│   │       ├── 1_configuration.py
│   │       ├── 2_file_upload.py
│   │       ├── 3_execution.py
│   │       ├── 4_results.py
│   │       └── 5_checkpoint.py
│   │
│   ├── utils/                       # ★ 工具層（可直接移植）
│   │   ├── config/
│   │   │   └── config_manager.py    #   Thread-safe 單例（Double-Checked Locking）
│   │   ├── logging/
│   │   │   └── logger.py            #   Logger + StructuredLogger + ColoredFormatter
│   │   ├── helpers/
│   │   │   └── file_utils.py        #   路徑驗證、目錄建立、安全複製
│   │   └── database/
│   │       └── duckdb_manager.py    #   Mixin 組合的 DuckDB 操作套件
│   │   # NOTE: metadata_builder/ 和 duckdb_manager/ 插件已提取為獨立套件
│   │   # → seafin-metadata-builder (github.com/Allen15763/seafin-metadata-builder)
│   │   # → seafin-duckdb-manager (github.com/Allen15763/seafin-duckdb-manager)
│   │       ├── transformers/        #     ColumnMapper, TypeCaster
│   │       └── validation/          #     CircuitBreaker（NULL 率保護）
│   │
│   └── config/                      # ★ 配置檔案
│       ├── config.toml              #   全域配置（日誌、路徑、資料源）
│       ├── {task}_config.toml       #   任務配置（日期、路徑、業務規則）
│       └── {task}_monthly.toml      #   每月變動配置（期初數、特殊參數）
│
├── pages/                           # Streamlit 多頁入口（只在有 UI 時需要）
│   ├── 1_⚙️_配置.py                  #   Entry point（exec 轉發，帶 emoji 給 Streamlit）
│   └── ...
│
├── tests/                           # 測試
│   ├── conftest.py
│   ├── unit/
│   └── integration/
│
├── checkpoints/                     # Checkpoint 儲存（git-ignored）
├── output/                          # 輸出結果（git-ignored）
└── logs/                            # 日誌（git-ignored）
```

---

## 4. 髒資料處理策略（最關鍵課題）

> **核心假設**：80% 的資料源是人工整理的 Excel/Google Sheet，存在以下問題：不固定的 header 位置、合併儲存格、欄位名稱前後空白、資料夾沒有命名規範、格式隨手工習慣改變。

### 4.1 Bronze/Silver ELT 架構（最佳實踐）

不要嘗試「一次性讀取並清洗」，而是分兩層處理：

```
原始檔案（Excel / CSV / Google Sheet）
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  Bronze 層：強健讀取，保留原始樣態        │
  │  - 全部讀為 string（不讓 pandas 猜型別）  │
  │  - 標準化欄位名稱（去空白、統一大小寫）   │
  │  - 添加 metadata 欄位（來源、批次 ID）   │
  │  - 不過濾任何行（即使看起來是空行）       │
  └──────────────────┬──────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────┐
  │  Silver 層：欄位映射 + 型別轉換 + 驗證   │
  │  - 欄位名稱映射（支援 regex）            │
  │  - 安全型別轉換（失敗 → NULL，不拋例外） │
  │  - 過濾真正的空行（非「看起來空白」的行） │
  │  - Circuit Breaker（NULL 率超標 → 告警） │
  └──────────────────┬──────────────────────┘
                     │
                     ▼
         乾淨的 DataFrame（可信任的型別）
```

### 4.2 MetadataBuilder 核心 API

```python
from seafin_metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec, SourceSpec
# pip install "seafin-metadata-builder @ git+https://github.com/Allen15763/seafin-metadata-builder.git@v1.0.0"

builder = MetadataBuilder()

# 方式 A：分開操作（對複雜情況有更多控制）
df_bronze = builder.extract(
    './bank_report.xlsx',
    sheet_name='B2B',
    header_row=2          # Excel header 在第 3 行（0-indexed = 2）
)

schema_config = SchemaConfig(columns=[
    ColumnSpec(source='交易日期',    target='txn_date',  dtype='DATE',    required=True),
    ColumnSpec(source='.*備註.*',    target='remarks',   dtype='VARCHAR'),  # regex 匹配
    ColumnSpec(source='金額',        target='amount',    dtype='DECIMAL'),
    ColumnSpec(source='銀行代碼',    target='bank_code', dtype='VARCHAR'),
], circuit_breaker_threshold=0.3)  # 超過 30% NULL 即告警

df_silver = builder.transform(df_bronze, schema_config)

# 方式 B：一次完成
df = builder.build('./bank_report.xlsx', schema_config, sheet_name='B2B', header_row=2)

# 方式 C：預覽（開發時了解檔案結構）
info = builder.extract_and_preview('./unknown_file.xlsx')
print(info['columns'])  # 查看欄位名稱
print(info['preview'])  # 查看前 10 行
sheets = builder.get_excel_sheets('./file.xlsx')  # 查看所有 sheet 名稱
```

### 4.3 ColumnSpec 配置詳解

```python
ColumnSpec(
    source='交易日期',          # 原始欄位名稱
                                # 支援 regex：'.*日期.*' 匹配任何含「日期」的欄位
    target='txn_date',          # 目標欄位名稱（snake_case）
    dtype='DATE',               # 型別：DATE / DECIMAL / INTEGER / VARCHAR / BOOLEAN
    required=True,              # True = 欄位必須存在，否則拋例外
    nullable=False,             # False = 此欄不允許 NULL（結合 circuit_breaker）
    default=None,               # 欄位缺失時的預設值
    transform=None,             # 自定義轉換函數 (Optional[Callable])
)
```

### 4.4 Circuit Breaker 保護機制

```python
SchemaConfig(
    columns=[...],
    circuit_breaker_threshold=0.3,   # 超過 30% NULL 時觸發
    # → 拋出 CircuitBreakerError，防止髒資料靜默流入下游
)
```

**何時調整閾值**：
- 財務核心欄位（金額、日期）：`threshold=0.05`（5%）
- 備註/說明欄位：`threshold=0.8`（80%，人工備註欄本來就常空）

### 4.5 處理常見的 Excel 問題

```python
# 問題 1：不固定的 header 位置
# → 傳入 header_row 參數，或先 extract_and_preview 找到 header 行
df = builder.extract('./file.xlsx', header_row=3)

# 問題 2：欄位名稱有空白或特殊字元
# → Bronze 層自動 strip() + 標準化
# → ColumnSpec 支援 regex：source='.*金額.*'

# 問題 3：日期格式五花八門（'2025/12/01' vs '2025-12-01' vs '112/12/01' 民國年）
# → Silver 層 TypeCaster 預設處理多種格式
# → 加入自定義 transform：
ColumnSpec(
    source='交易日期', target='txn_date', dtype='DATE',
    transform=lambda x: parse_tw_date(x)  # 自訂民國年轉換
)

# 問題 4：合併儲存格（merged cells）
# → pandas 讀取 Excel 時，合併儲存格的非首行自動變 NaN
# → Bronze 層不填充，Silver 層可加 forward fill：
df_silver = df_silver.ffill()  # 根據業務需求決定是否填充

# 問題 5：多個 Sheet 需要判斷讀哪個
sheets = builder.get_excel_sheets('./file.xlsx')
target_sheet = next(s for s in sheets if '對帳' in s or 'recon' in s.lower())
df = builder.extract('./file.xlsx', sheet_name=target_sheet)
```

---

## 5. 核心框架層：Pipeline 系統

### 5.1 核心抽象概覽

```python
# 步驟基類
class PipelineStep(ABC):
    def execute(self, context: ProcessingContext) -> StepResult: ...   # 必須實作
    def validate_input(self, context) -> bool: ...                     # 可選覆寫
    def rollback(self, context, error): ...                            # 可選覆寫

# 步驟結果
@dataclass
class StepResult:
    step_name: str
    status: StepStatus      # SUCCESS / FAILED / SKIPPED / RETRY
    data: Optional[DataFrame]
    error: Optional[Exception]
    message: Optional[str]
    duration: float
    metadata: Dict[str, Any]

# 資料載體（所有步驟共享）
class ProcessingContext:
    data: DataFrame                       # 主資料
    _auxiliary_data: Dict[str, Any]       # 輔助資料（參考表、計算結果等）
    _variables: Dict[str, Any]            # 共享變數（路徑、日期、參數等）
    errors: List[str]                     # 錯誤追蹤
    warnings: List[str]                   # 警告追蹤
    _validations: Dict[str, Any]          # 結構化驗證結果
    _history: List[Dict]                  # 執行歷史

# Pipeline 主類
class Pipeline:
    def add_step(self, step) -> 'Pipeline': ...   # 鏈式 API
    def execute(self, context) -> Dict: ...        # 回傳完整執行報告

@dataclass
class PipelineConfig:
    name: str
    task_type: str = "transform"   # transform / compare / report
    stop_on_error: bool = True
    log_level: str = "INFO"
```

### 5.2 ProcessingContext 的使用規範

**Context 是「整條 Pipeline 的記憶體」**，步驟之間透過它傳遞所有狀態：

```python
# Step 1（載入步驟）：寫入
context.set_variable('processing_date', 202512)
context.set_variable('db_path', './data/bank.duckdb')
context.add_auxiliary_data('reference_account', ref_df)   # 參考表
context.add_auxiliary_data('cub_result', cub_container)   # 計算結果

# Step 2（業務步驟）：讀取後更新主資料
processing_date = context.get_variable('processing_date')
ref = context.get_auxiliary_data('reference_account')
df = context.data.copy()
# ... 處理 df ...
context.update_data(df)

# Step N（匯出步驟）：匯總所有結果
cub_result = context.get_auxiliary_data('cub_result')
ctbc_result = context.get_auxiliary_data('ctbc_result')
output_path = context.get_variable('output_path')
```

**命名規範**：
- `set_variable`：純量值（日期、路徑、數字、字串）
- `add_auxiliary_data`：DataFrame 或領域物件（容器、結果集）

### 5.3 TaskClass 設計範本

```python
class MyTask:
    """
    任務主類，管理整個執行生命週期。
    多種執行模式透過 mode 參數控制。
    """
    SUPPORTED_MODES = ['full', 'phase1_only', 'phase2_only', 'custom']

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.checkpoint_manager = CheckpointManager('./checkpoints')

    def execute(self, mode: str = 'full', save_checkpoints: bool = True) -> Dict:
        pipeline = self.build_pipeline(mode)
        context = self._prepare_context()

        executor = PipelineWithCheckpoint(pipeline, self.checkpoint_manager)
        return executor.execute_with_checkpoint(
            context,
            save_after_each_step=save_checkpoints
        )

    def resume(self, checkpoint_name: str, start_from_step: str) -> Dict:
        """從 Checkpoint 恢復執行"""
        context = self.checkpoint_manager.load_checkpoint(checkpoint_name)
        pipeline = self.build_pipeline('full')  # 需要完整的步驟定義
        executor = PipelineWithCheckpoint(pipeline, self.checkpoint_manager)
        return executor.execute_with_checkpoint(
            context,
            start_from_step=start_from_step
        )

    def build_pipeline(self, mode: str = 'full') -> Pipeline:
        pipeline = Pipeline(PipelineConfig(name=f"my_task_{mode}"))

        # 根據模式添加步驟
        if mode in ('full', 'phase1_only'):
            pipeline.add_step(LoadParametersStep(...))
            pipeline.add_step(ProcessDataStep(...))

        if mode in ('full', 'phase2_only'):
            pipeline.add_step(AggregateStep(...))
            pipeline.add_step(ExportStep(...))

        return pipeline

    def validate_inputs(self, mode: str = 'full') -> Dict:
        """執行前驗證（可選，建議實作）"""
        errors = []
        # 檢查必要檔案、DB 連線、配置完整性
        return {'is_valid': len(errors) == 0, 'errors': errors}

    def list_checkpoints(self) -> List[Dict]:
        return self.checkpoint_manager.list_checkpoints()
```

### 5.4 Checkpoint 機制

Checkpoint 讓長時間執行的 Pipeline 在任意步驟失敗後可從上次成功點恢復，無需重頭執行。

**儲存格式**（按優先序）：

| 格式 | 用途 | 選擇原因 |
|------|------|---------|
| Parquet | DataFrame 主資料 | 快、壓縮好、保留型別 |
| Pickle | Fallback + 非 DataFrame 物件 | 支援任何 Python 物件 |
| JSON | 變數和 metadata | 人類可讀 |

**儲存結構**：
```
checkpoints/{task}_{type}_after_{step_name}/
├── data.parquet              # 主 DataFrame
├── checkpoint_info.json      # 變數、warnings、執行 metadata
└── auxiliary_data/           # 輔助 DataFrame 或 Pickle
    ├── reference_account.parquet
    └── cub_containers.pkl    # 無法序列化為 Parquet 時的 fallback
```

---

## 6. 業務規則配置化：兩種路徑

根據業務規則的複雜度選擇合適的配置化方式。

### 6.1 路徑 A：具名規則（適合簡單單欄位條件）

**適用時機**：條件簡單（比對單一欄位）、規則需要語意名稱（易追溯）。

**TOML 格式**（SPT style）：

```toml
# {task}_config.toml

[classification_rules.priority_conditions]
# key = rule_name  value = rule definition
blaire_ssp = {
    keywords = '(?i)SSP',
    field = 'Item Description',
    status = '不估計(Blaire)',
    remark = 'Blaire',
    note = 'Item Description 包含 SSP'
}

blaire_logistics = {
    keywords = '(?i)Logistics fee',
    field = 'Item Description',
    status = '不估計(Blaire)'
}

cindy_ctbc = {
    supplier = 'TW_中國信託商業銀行',
    status = '不估計(Cindy)'
}
```

**程式碼讀取**：

```python
rules = config_manager._config_toml.get('classification_rules', {}).get('priority_conditions', {})
for rule_name, rule in rules.items():
    if 'keywords' in rule:
        mask = df[rule['field']].str.contains(rule['keywords'], na=False, regex=True)
    elif 'supplier' in rule:
        mask = df['Supplier'] == rule['supplier']
    if mask.any():
        df.loc[mask, '狀態'] = rule['status']
```

---

### 6.2 路徑 B：條件引擎（適合複雜多欄位 AND/OR 組合）

**適用時機**：需要多欄位 AND/OR 組合條件、規則數量多（>10）、業務人員需自行調整。

**TOML 格式**（SPX ConditionEngine style）：

```toml
# 參考資料（供規則引用，避免重複）
[my_task]
deposit_keywords = "押金|保證金|Deposit"
fa_account_codes = ["199999", "180000"]
ops_requesters = ["Alice Chen", "Bob Wang"]

# 規則定義（有序陣列，按 priority 執行）
[[my_task_classification_rules.conditions]]
priority = 1
status_value = "已完成"
note = "有入帳日期、ERM <= 月結月份、數量匹配"
combine = "and"                    # 所有 checks 都要符合（and/or）
apply_to = ["PO"]                  # 只套用到 PO（可選，預設兩者都套）

[[my_task_classification_rules.conditions.checks]]
field = "GL DATE"
type = "is_not_null"               # 欄位有值

[[my_task_classification_rules.conditions.checks]]
type = "erm_le_date"               # 預先計算的 mask（引用 prebuilt_masks）

[[my_task_classification_rules.conditions.checks]]
type = "qty_matched"               # 數量匹配（預先計算）

# ─────────────────────────────────────
[[my_task_classification_rules.conditions]]
priority = 2
status_value = "押金"
note = "Item Description 包含押金關鍵字，GL# 非 FA 科目"
combine = "and"

[[my_task_classification_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern_key = "my_task.deposit_keywords"  # 引用 [my_task] 中的字串

[[my_task_classification_rules.conditions.checks]]
field = "GL#"
type = "not_in_list"
list_key = "my_task.fa_account_codes"    # 引用列表
```

**條件引擎核心實作**（直接搬移 `SPXConditionEngine`）：

```python
class ConditionEngine:
    """配置驅動條件引擎（從 stagging.toml 讀規則，動態套用至 DataFrame）"""

    def __init__(self, config_section: str):
        self.rules = self._load_and_sort_rules(config_section)

    def apply_rules(
        self,
        df: pd.DataFrame,
        status_column: str,
        context: Dict[str, Any],           # 含 processing_date、prebuilt_masks
        processing_type: str = 'default'
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        依序套用規則，只處理「尚無狀態」的列：
        1. 按 priority 排序
        2. 過濾 apply_to
        3. 建構 boolean mask（_build_combined_mask）
        4. 限縮至 no_status
        5. df.loc[mask, status_column] = status_value
        6. 更新 no_status（已命中的列不再參與後續規則）
        """
        stats = {}
        no_status = (df[status_column].isna() | (df[status_column] == ''))

        for rule in self.rules:
            if processing_type not in rule.get('apply_to', ['PO', 'PR', 'default']):
                continue

            mask = self._build_combined_mask(df, rule['checks'], rule.get('combine', 'and'), context)
            if mask is None:
                continue

            # 支援 override_statuses（允許覆蓋特定現有狀態）
            override = rule.get('override_statuses', [])
            applicable = no_status | (df[status_column].isin(override) if override else pd.Series(False, index=df.index))
            final_mask = mask & applicable

            count = int(final_mask.sum())
            if count > 0:
                df.loc[final_mask, status_column] = self._resolve_status_value(rule)
                no_status = no_status & ~final_mask  # 更新 no_status

            stats[f"p{rule['priority']}_{self._resolve_status_value(rule)}"] = count

        return df, stats
```

---

### 6.3 路徑 C：規則表格（適合帳號預測、映射類規則）

**適用時機**：多欄位條件的「IF A AND B AND C → 結果」，且規則需要明確的優先順序 ID。

```toml
# 每條規則是一個 [[...rules]] 陣列元素，按 rule_id 執行
[[account_prediction.rules]]
rule_id = 1
result_account = "450014"
departments = ["S01 - Marketing", "S02 - Business Development"]
description_keywords = "代收代付"
condition_desc = "行銷/業務部門，摘要含代收代付"

[[account_prediction.rules]]
rule_id = 2
result_account = "650003"
departments = ["S01 - Marketing"]
supplier = "TW_特定供應商"
condition_desc = "行銷部門，特定供應商"
```

**程式碼讀取**（第一條匹配即採用，類似資料庫的 CASE WHEN）：

```python
rules = config['account_prediction']['rules']
for rule in sorted(rules, key=lambda r: r['rule_id']):
    matched = True
    if 'departments' in rule and row['Department'] not in rule['departments']:
        matched = False
    if 'supplier' in rule and matched and row['Supplier'] != rule['supplier']:
        matched = False
    if 'description_keywords' in rule and matched:
        if not re.search(rule['description_keywords'], row['Description'], re.IGNORECASE):
            matched = False
    if matched:
        row['account'] = rule['result_account']
        break
```

---

### 6.4 選擇路徑的決策樹

```
規則需要配置化？
     │
     ├── 是 ──▶ 條件有幾個欄位？
     │               │
     │               ├── 1-2 個欄位（簡單比對）
     │               │       └──▶ 路徑 A（具名規則 Key-Value TOML）
     │               │
     │               └── 3+ 欄位（AND/OR 組合）
     │                       │
     │                       ├── 有固定的「IF→結果」映射（帳號預測）
     │                       │       └──▶ 路徑 C（Table 陣列 TOML）
     │                       │
     │                       └── 動態條件組合（狀態分類）
     │                               └──▶ 路徑 B（條件引擎 ConditionEngine）
     │
     └── 否 ──▶ 邏輯固定（如 DataFrame join、日期格式轉換）
                     └──▶ 直接寫在步驟的 execute() 中
```

---

## 7. 資料持久化：DuckDB 作為中間層

### 7.1 為什麼用 DuckDB 而不只用 pandas

| 場景 | pandas | DuckDB |
|------|--------|--------|
| 簡單欄位操作 | ✅ | ✅ |
| 複雜 SQL 查詢（GROUP BY、Window）| 難寫 | ✅ 原生支援 |
| 跨月累積資料 | 無法持久化 | ✅ 持久化 |
| 大檔案（GB 級）| 記憶體爆炸 | ✅ 外部記憶體 |
| Schema 版本管理 | 手工 | ✅ 自動遷移 |

**建議用法**：
- pandas：步驟內的轉換邏輯（欄位計算、mask 操作）
- DuckDB：跨步驟的累積資料、複雜聚合、持久化存量表

### 7.2 DuckDB Manager（Mixin 組合架構）

```python
# 職責清晰分離，未來可按需加入新 Mixin
class DuckDBManager(CRUDMixin, TableManagementMixin, DataCleaningMixin, TransactionMixin):
    """
    DuckDB 操作套件（Context Manager 使用）
    """
    def __init__(self, config: str | Path | DuckDBConfig | dict): ...

# 常見操作
with DuckDBManager("./data.duckdb") as db:
    # 建表（從 DataFrame）
    db.create_table_from_df("transactions", df, if_exists="replace")

    # 查詢
    result = db.query_to_df("""
        SELECT bank_code, SUM(amount) as total
        FROM transactions
        WHERE txn_date BETWEEN ? AND ?
        GROUP BY bank_code
    """, params=[beg_date, end_date])

    # 原子操作（事務）
    with db._atomic():
        db.delete_data("transactions", "status = 'pending'")
        db.insert_df_into_table("archive", pending_df)

    # 查看表清單
    tables = db.list_tables()
    exists = db.table_exists("transactions")
```

### 7.3 Schema 遷移系統

當資料表結構需要隨時間演進時（新增欄位、型別變更），避免手工 ALTER TABLE：

```python
from seafin_duckdb_manager.migration import SchemaMigrator, MigrationStrategy
# pip install "seafin-duckdb-manager @ git+https://github.com/Allen15763/seafin-duckdb-manager.git@v2.1.0"

migrator = SchemaMigrator(db)

# 比對現有 Schema 與新 DataFrame 的差異
diff = migrator.compare_schema("my_table", new_df)
# diff.changes → [ADDED: 'new_col VARCHAR', TYPE_CHANGED: 'amount INT → DECIMAL']

# 套用遷移策略
migrator.migrate(
    "my_table",
    new_df,
    strategy=MigrationStrategy.ADD_COLUMNS  # 安全：只加新欄位
    # 或 MigrationStrategy.RECREATE         # 激進：重建（丟失原有資料）
    # 或 MigrationStrategy.SAFE_MIGRATE     # 智慧：能加則加，不能則 RECREATE
)
```

### 7.4 DuckDB 配置選項

```python
# 方式 1：路徑字串（最簡單）
with DuckDBManager("./data.duckdb") as db: ...

# 方式 2：dataclass（可指定時區、唯讀等）
config = DuckDBConfig(
    db_path="./data.duckdb",
    timezone="Asia/Taipei",
    read_only=False
)
with DuckDBManager(config) as db: ...

# 方式 3：從 TOML 讀取
config = DuckDBConfig.from_toml("config.toml", section="database")

# 方式 4：in-memory（測試用）
with DuckDBManager(":memory:") as db: ...
```

### 7.5 在 Pipeline Step 中整合 MetadataBuilder + DuckDB

```python
class LoadBankDataStep(PipelineStep):
    """示範：從 Excel 讀入髒資料 → Bronze/Silver 清洗 → 存入 DuckDB"""

    def execute(self, context: ProcessingContext) -> StepResult:
        db_path = context.get_variable('db_path')
        file_path = context.get_variable('bank_file_path')

        # 1. Bronze/Silver 清洗
        builder = MetadataBuilder()
        schema_config = SchemaConfig(columns=[
            ColumnSpec(source='交易日期', target='txn_date', dtype='DATE', required=True),
            ColumnSpec(source='.*金額.*', target='amount',   dtype='DECIMAL'),
            ColumnSpec(source='備註',     target='remarks',  dtype='VARCHAR'),
        ], circuit_breaker_threshold=0.1)

        df_bronze = builder.extract(file_path, sheet_name=0, header_row=2)
        df_silver = builder.transform(df_bronze, schema_config)

        # 2. 存入 DuckDB（Bronze 層保留原始樣態，Silver 層提供乾淨資料）
        with DuckDBManager(db_path) as db:
            db.create_table_from_df('bronze_bank', df_bronze, if_exists='replace')
            db.create_table_from_df('silver_bank', df_silver, if_exists='replace')

        # 3. 存入 Context 供後續步驟使用
        context.add_auxiliary_data('clean_bank_data', df_silver)
        context.set_variable('bank_record_count', len(df_silver))

        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message=f"已載入 {len(df_silver)} 筆銀行資料",
            metadata={'bronze_rows': len(df_bronze), 'silver_rows': len(df_silver)}
        )
```

---

## 8. 設計模式整合目錄

### 8.1 完整模式清單

| 模式 | 位置 | 用途 | 解決的問題 |
|------|------|------|-----------|
| **Template Method** | `BaseLoadingStep`、`BaseERMEvaluationStep`、`BaseBankProcessStep` | 定義骨架，子類填差異 | 多實體/多銀行 60-87% 的重複程式碼 |
| **Factory** | `DataSourceFactory`、`_create_step()` | 根據配置建立實例 | 解耦建立與使用，支援新類型擴充 |
| **Singleton** | `ConfigManager`、`Logger` | 全域唯一實例 | 配置/日誌的全域一致性，執行緒安全 |
| **Mixin 組合** | `DuckDBManager` | 功能按維度拆分 | 職責清晰，可按需組合，避免繼承爆炸 |
| **Pipeline/責任鏈** | `Pipeline` + `PipelineStep` | 有序步驟執行 | 業務邏輯模組化，支援跳過/重試/條件分支 |
| **Context Object** | `ProcessingContext` | 步驟間共享狀態 | 避免函數參數爆炸，簡化 Checkpoint 序列化 |
| **Checkpoint/Memento** | `CheckpointManager` | 儲存/恢復執行狀態 | 長時間流程的容錯與斷點續跑 |
| **Strategy** | `DataSource` 各實作、`BankProcessor` | 替換演算法 | 支援多種資料源格式、多家銀行計算邏輯 |
| **Observer** | Pipeline 的 prerequisite/post_action | 步驟前後鉤子 | 非侵入式的橫切關注點（日誌、驗證） |
| **Value Object** | `BankDataContainer`、`StepResult` | 不可變資料傳輸 | 結果的封裝與類型安全 |
| **Condition Engine** | `SPXConditionEngine` | TOML 規則 → pandas mask | 業務規則與程式碼解耦，非技術人員可維護 |
| **Bronze/Silver ELT** | `MetadataBuilder` | 髒資料分層清洗 | 安全讀取不可信資料源，不讓錯誤靜默傳播 |

### 8.2 Template Method 深度解析

**何時應該建立基類**：當 2+ 個步驟有 >60% 相同骨架時，提取基類。

```
BaseBankProcessStep（銀行處理基類）
execute()                     ← 模板（固定流程，子類不可覆寫）
├── _extract_parameters()     ← 固定實作（所有銀行共用）
├── _process_categories()     ← 固定實作（迴圈邏輯）
│   ├── _create_processor()   ← 固定實作（呼叫子類的 get_processor_class）
│   └── processor.process()   ← ★ 策略（由子類決定用哪個 Processor）
├── _store_results()          ← 固定實作（存入 Context）
└── _log_totals()             ← 固定實作（日誌）

get_bank_code() → str         ← ★ 子類必須實作（2 行）
get_processor_class() → Type  ← ★ 子類必須實作（2 行）

效果：每個銀行步驟 = 15 行（vs 原來 120-390 行）
```

### 8.3 Mixin 組合 vs 繼承的選擇

```
選繼承（Template Method）：
  ✅ 步驟之間有固定的執行順序
  ✅ 子類差異集中在「做什麼」，不在「怎麼做」
  範例：BaseLoadingStep、BaseBankProcessStep

選 Mixin（組合）：
  ✅ 功能是正交的（CRUD ≠ 事務 ≠ Schema 管理）
  ✅ 不同場景需要不同功能組合
  ✅ 避免多層繼承鏈
  範例：DuckDBManager = CRUDMixin + TableManagementMixin + DataCleaningMixin

選策略（Strategy）：
  ✅ 同一個流程需要替換「演算法」（不同銀行的計算方式）
  ✅ 需要執行時動態決定
  範例：BankProcessor 的各銀行實作
```

---

## 9. 配置系統設計

### 9.1 三層配置體系

```
config/
├── config.toml                  # 全域配置（不常改）
│   ├── [general]                #   專案名稱、版本
│   ├── [logging]                #   日誌等級、格式、輸出路徑、輪替
│   ├── [paths]                  #   日誌、輸出、暫存目錄
│   └── [datasource]             #   編碼、快取、引擎
│
├── {task}_config.toml           # 任務配置（每月調整）
│   ├── [dates]                  #   ★ 處理期間（Manual）
│   ├── [pipeline.steps]         #   啟用的步驟列表（配置驅動）
│   ├── [paths]                  #   輸入/輸出路徑（含 {YYYYMM} 變數）
│   ├── [business_rules]         #   ★ 業務參數（費率、金額上限）（Manual）
│   ├── [{entity}.*]             #   實體/銀行特定配置
│   └── [output]                 #   輸出格式、檔名範本
│
└── {task}_monthly.toml          # 每月變動配置（需人工確認的參數）
    ├── [opening_balance]        #   ★ 期初數（Manual）
    └── [special_entries]        #   ★ 特殊分錄（Manual）
```

**設計原則**：
1. **靜態 vs 動態分離**：不常改的放 config.toml，每月改的放 task_config.toml
2. **`# Manual` 標記**：需人工確認的欄位加 `# Manual` 註解提醒
3. **路徑模板變數**：支援 `{YYYYMM}`、`{PREV_YYYYMM}`、`{resources}` 等佔位符
4. **向後相容**：新增配置項都有預設值，不影響舊配置

### 9.2 paths.toml 路徑模板設計

```toml
[base]
resources = "C:/SEA/data/resources"    # 根路徑（開發/正式只改這裡）
output = "./output"

[entity.po]
# 變數替換：{YYYYMM} → 202512, {PREV_YYYYMM} → 202511, {resources} → 上方路徑
raw_data = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv"
previous = "{resources}/{YYYYMM}/前期底稿/{PREV_YYYYMM}_FN.xlsx"

[entity.po.params]
# pandas 讀取參數，同步到 DataSource
raw_data = { encoding = "utf-8", sep = ",", dtype = "str" }
previous = { sheet_name = 0, header = 0, dtype = "str" }
special  = { sheet_name = "摘要", header = 3, usecols = "A:AH" }
```

### 9.3 ConfigManager 使用方式

```python
from src.utils.config import ConfigManager

config = ConfigManager()  # 永遠回傳同一實例（Thread-safe）

# 讀取方式
config.get('dates', 'current_period_start')    # section, key
config.get('dates.current_period_start')        # 點號路徑
config.get_int('limits', 'max_retries')         # 型別安全
config.get_boolean('features', 'enable_cache')
config.get_list('pipeline.steps', 'enabled')    # 回傳 List
config.get_nested('banks', 'cub', 'name')       # 深層巢狀
config.get_path('paths', 'output_dir')          # 自動轉 Path 物件
```

---

## 10. 工具層：可直接搬移的模組

下列模組與業務邏輯完全解耦，可直接複製到新專案：

| 模組 | 路徑 | 說明 | 搬移方式 |
|------|------|------|---------|
| **Pipeline Framework** | `src/core/pipeline/` | Pipeline/Step/Context/Checkpoint | 直接複製目錄 |
| **DataSource** | `src/core/datasources/` | Excel/CSV/Parquet/GSheets + 快取 | 直接複製目錄 |
| **MetadataBuilder** | ~~`src/utils/metadata_builder/`~~ → [`seafin-metadata-builder`](https://github.com/Allen15763/seafin-metadata-builder) | Bronze/Silver ELT | `pip install` from GitHub |
| **DuckDB Manager** | ~~`src/utils/duckdb_manager/`~~ → [`seafin-duckdb-manager`](https://github.com/Allen15763/seafin-duckdb-manager) | Mixin DB 操作 + Schema 遷移 | `pip install` from GitHub |
| **ConfigManager** | `src/utils/config/` | Thread-safe TOML 單例 | 直接複製目錄 |
| **Logger** | `src/utils/logging/` | 彩色 + 結構化日誌 | 直接複製目錄 |
| **file_utils** | `src/utils/helpers/` | 路徑驗證、目錄建立 | 直接複製檔案 |

### 10.1 Logger 使用規範

```python
from src.utils.logging import get_logger, get_structured_logger

# 一般日誌
logger = get_logger(__name__)
logger.info("載入 %d 筆資料", len(df))
logger.warning("找不到參考資料，使用預設值")
logger.error("處理失敗: %s", str(e), exc_info=True)

# 結構化日誌（語意方法，輸出 JSON 友好格式）
slogger = get_structured_logger(__name__)
slogger.log_operation_start('data_loading', entity='SPT', file='po.csv')
slogger.log_data_processing('purchase_orders', record_count=4000, processing_time=1.5)
slogger.log_file_operation('read', file_path='/path/to/file.csv', success=True)
slogger.log_step_result('LoadData', 'success', duration=2.3)
slogger.log_error(exception, context='CUB Processing')
slogger.log_operation_end('data_loading', success=True, duration=3.1)
```

**日誌格式**：
```
# 終端（彩色）
2026-03-05 14:30:00 | INFO  | my_module | execute:45 | 載入 4000 筆資料

# 檔案（詳細）
2026-03-05 14:30:00 | INFO  | my_module.execute:45 | 12345-67890 | 載入 4000 筆資料
```

---

## 11. 新專案啟動路徑圖

### 11.1 決策清單（開始設計前先回答）

```
□ 資料源類型？
  → Excel/Google Sheet：需要 MetadataBuilder（Bronze/Silver）
  → DuckDB/SQL：直接用 DuckDB Manager
  → 兩者混合：都需要

□ 資料品質？
  → 人工整理/不可信：必用 Bronze/Silver
  → 系統匯出/可信：可直接用 DataSource 讀取

□ 業務規則複雜度？
  → 10 條以上的多條件分類 → ConditionEngine（路徑 B）
  → 簡單關鍵字比對 → 具名規則 TOML（路徑 A）
  → 帳號映射類 → Table 陣列 TOML（路徑 C）

□ 使用者類型？
  → 技術人員 → CLI 入口（main.py）
  → 非技術人員也需要 → 加 Streamlit UI

□ Pipeline 長度？
  → < 5 步 → 不一定需要完整 Pipeline 框架
  → 5-20 步 → 建議使用（Checkpoint 很值得）
  → 多個子流程 → 多種執行 mode

□ 是否有累積資料？
  → 否 → 每次獨立執行即可
  → 是 → 需要 DuckDB 作為持久化層
```

### 11.2 最小可行專案（五步建立）

**Step 1**：複製工具層
```bash
# 從現有專案複製
cp -r src/core/ new_project/src/core/
cp -r src/utils/ new_project/src/utils/
```

**Step 2**：定義配置檔

```toml
# src/config/config.toml（全域，不常改）
[logging]
level = "INFO"
color = true
log_path = "./logs"

[paths]
output_path = "./output"
checkpoint_path = "./checkpoints"

# src/config/my_task_config.toml（任務，每次/每月改）
[dates]
current_period_start = "2026-03-01"  # Manual
current_period_end = "2026-03-31"    # Manual

[pipeline.steps]
enabled_steps = ["LoadData", "CleanData", "BusinessLogic", "Export"]

[paths]
input_file = "./data/{YYYYMM}/report.xlsx"
output_file = "./output/{YYYYMM}_result.xlsx"
```

**Step 3**：建立 Pipeline Steps

```python
# src/tasks/my_task/steps/step_01_load_data.py
from src.core.pipeline.base import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from seafin_metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec

class LoadDataStep(PipelineStep):
    def __init__(self, file_path: str, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path

    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            builder = MetadataBuilder()
            schema = SchemaConfig(columns=[
                ColumnSpec(source='日期', target='date', dtype='DATE', required=True),
                ColumnSpec(source='.*金額.*', target='amount', dtype='DECIMAL'),
            ])
            df = builder.build(self.file_path, schema, header_row=1)

            context.update_data(df)
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"載入 {len(df)} 筆資料"
            )
        except Exception as e:
            return StepResult(step_name=self.name, status=StepStatus.FAILED, error=e)

    def validate_input(self, context: ProcessingContext) -> bool:
        return True  # 載入步驟不需要前置資料
```

**Step 4**：建立 TaskClass（編排器）

```python
# src/tasks/my_task/pipeline_orchestrator.py
from src.core.pipeline.pipeline import Pipeline, PipelineConfig
from src.core.pipeline.context import ProcessingContext
from src.core.pipeline.checkpoint import CheckpointManager, PipelineWithCheckpoint
from src.utils.config import ConfigManager
from .steps.step_01_load_data import LoadDataStep
from .steps.step_02_business_logic import BusinessLogicStep
from .steps.step_03_export import ExportStep

class MyTask:
    SUPPORTED_MODES = ['full', 'load_only']

    def __init__(self, config_path: str = None):
        import tomllib
        with open(config_path or 'src/config/my_task_config.toml', 'rb') as f:
            self.config = tomllib.load(f)
        self.checkpoint_manager = CheckpointManager('./checkpoints')

    def execute(self, mode: str = 'full', save_checkpoints: bool = True) -> dict:
        pipeline = self.build_pipeline(mode)
        context = self._prepare_context()
        executor = PipelineWithCheckpoint(pipeline, self.checkpoint_manager)
        return executor.execute_with_checkpoint(context, save_after_each_step=save_checkpoints)

    def build_pipeline(self, mode: str) -> Pipeline:
        pipeline = Pipeline(PipelineConfig(name=f"my_task_{mode}"))
        enabled = self.config.get('pipeline', {}).get('steps', {}).get('enabled_steps', [])

        step_registry = {
            'LoadData':      lambda: LoadDataStep(name='LoadData', file_path=self._get_input_path()),
            'BusinessLogic': lambda: BusinessLogicStep(name='BusinessLogic', config=self.config),
            'Export':        lambda: ExportStep(name='Export'),
        }

        for step_name in enabled:
            factory = step_registry.get(step_name)
            if factory:
                pipeline.add_step(factory())

        return pipeline

    def _prepare_context(self) -> ProcessingContext:
        context = ProcessingContext(task_name='my_task')
        context.set_variable('processing_date', self.config['dates']['current_period_start'])
        return context

    def _get_input_path(self) -> str:
        return self.config['paths']['input_file'].replace('{YYYYMM}', '202603')
```

**Step 5**：入口點

```python
# main.py
from src.tasks.my_task import MyTask

task = MyTask()
result = task.execute(mode='full', save_checkpoints=True)

if result['success']:
    print(f"完成 {result['successful_steps']}/{result['total_steps']} 步驟")
else:
    print(f"失敗：{result['errors']}")
    # 下次可從 checkpoint 恢復
    checkpoints = task.list_checkpoints()
    print(f"可用的 Checkpoint：{[c['name'] for c in checkpoints]}")
```

---

## 12. 關鍵決策框架

### 12.1 何時加 UI

| 情境 | 建議 |
|------|------|
| 只有工程師使用 | CLI 入口，不需要 UI |
| 非技術業務人員需要上傳檔案、觸發執行、下載結果 | 加 Streamlit |
| 需要即時查看執行進度 | Streamlit 的 `st.progress()` + log streaming |
| 需要設定複雜參數 | Streamlit 的多頁工作流（配置→上傳→執行→結果）|

**Streamlit 特有設計**：
- 多頁應用需要 emoji 檔名（`pages/1_⚙️_配置.py`），實際邏輯放標準檔名
- UI 到 Pipeline 的橋接點：`UnifiedPipelineService`（唯一橋接，UI 不直接呼叫 Orchestrator）
- Sync/Async 橋接：用 `threading.Thread + asyncio.new_event_loop()`

### 12.2 何時用 DuckDB 而非純 pandas

| 情境 | 建議 |
|------|------|
| 只需跑一次、結果不需持久 | pandas 即可 |
| 需要累積多月資料 | DuckDB |
| 需要複雜 SQL（Window、CTE、Pivot）| DuckDB |
| DataFrame 超過可用記憶體 | DuckDB |
| 需要 Schema 版本管理 | DuckDB + SchemaMigrator |

### 12.3 架構複雜度選擇

```
小型任務（< 5 步，單一資料流）：
  → 不需要 Pipeline 框架
  → 直接寫函數串接即可
  → MetadataBuilder + DuckDB 依舊有用

中型任務（5-15 步，固定流程）：
  → 使用 Pipeline 框架
  → 一種執行模式
  → Checkpoint 建議啟用

大型任務（15+ 步，多流程、多實體）：
  → Pipeline 框架 + 多種 mode
  → 配置驅動步驟（TOML）
  → 考慮條件引擎（業務規則多）
  → 考慮 Streamlit UI（非技術用戶）
```

### 12.4 錯誤處理策略

```
每個 Step：
├── execute() 內部 try-catch
├── 失敗：回傳 StepResult(status=FAILED, error=e)
├── required=True  → Pipeline 停止（影響後續步驟）
└── required=False → 記錄警告，繼續（如選填資料載入）

Pipeline 層：
├── stop_on_error=True → 遇 FAILED 立即停止（預設）
└── 所有錯誤記錄在 context.errors

資料驗證：
├── MetadataBuilder.CircuitBreaker → NULL 率超標告警
├── context.add_validation()       → 結構化驗證結果
└── validate_inputs()              → 執行前前置驗證

不要做的事：
❌ 在 execute() 中直接 raise（破壞 Pipeline 的執行流）
❌ 靜默吞掉例外（logger.warning 之後繼續，但不記錄）
❌ 在 required=False 的步驟中 raise（應回傳 FAILED）
```

---

## 13. 完整程式碼模板集

### 13.1 最小 Pipeline Step

```python
import time
from src.core.pipeline.base import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils.logging import get_logger

logger = get_logger(__name__)

class MyBusinessStep(PipelineStep):
    """一句話說明步驟的職責"""

    def __init__(self, name: str, some_config: dict, **kwargs):
        super().__init__(name, description="步驟描述", **kwargs)
        self.some_config = some_config

    def execute(self, context: ProcessingContext) -> StepResult:
        start = time.time()
        try:
            df = context.data.copy()

            # ─── 業務邏輯 ───
            df['new_column'] = df['source_column'].map(self.some_config)
            # ─────────────────

            context.update_data(df)
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"處理完成，新增 new_column",
                duration=time.time() - start,
                metadata={'rows': len(df)}
            )
        except Exception as e:
            logger.error("步驟 %s 失敗: %s", self.name, str(e), exc_info=True)
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=time.time() - start
            )

    def validate_input(self, context: ProcessingContext) -> bool:
        return 'source_column' in context.data.columns
```

### 13.2 繼承 BaseLoadingStep（資料載入）

```python
from src.core.pipeline.steps.base_loading import BaseLoadingStep
from seafin_metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec
from typing import Tuple
import pandas as pd

class MyEntityLoadingStep(BaseLoadingStep):
    """[實體名] 資料載入步驟"""

    def get_required_file_type(self) -> str:
        return 'raw_data'  # 對應 paths.toml 的 key

    async def _load_primary_file(self, source, path: str) -> Tuple[pd.DataFrame, int, int]:
        builder = MetadataBuilder()
        schema = SchemaConfig(columns=[
            ColumnSpec(source='日期',  target='date',   dtype='DATE', required=True),
            ColumnSpec(source='.*金額.*', target='amount', dtype='DECIMAL'),
        ])
        df = builder.build(path, schema, header_row=1)
        return df, len(df), len(df)

    async def _load_reference_data(self, context) -> int:
        try:
            ref_df = await self._load_reference_file('reference_table')
            context.add_auxiliary_data('reference_table', ref_df)
            return len(ref_df)
        except FileNotFoundError:
            return 0  # 選填資料不存在時不中斷
```

### 13.3 繼承 BaseBankProcessStep（同質多物件處理）

適合「同一套處理骨架，套用在多個對象（銀行、供應商、部門）」的場景：

```python
from src.tasks.bank_recon.steps.base_bank_step import BaseBankProcessStep

class ProcessMySubjectStep(BaseBankProcessStep):
    """處理 [某個對象] 的步驟（模板方法，只需 15 行）"""

    def get_bank_code(self) -> str:
        return 'my_subject'   # 對應 config.toml 中的 [banks.my_subject]

    def get_processor_class(self):
        from ..utils.my_subject_processor import MySubjectProcessor
        return MySubjectProcessor
```

### 13.4 Thread-safe 單例（可直接複用）

```python
import threading
from pathlib import Path
import tomllib

class ConfigManager:
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
                    self._load_config()
                    self._initialized = True

    def _load_config(self):
        config_path = Path(__file__).parent.parent.parent / 'config' / 'config.toml'
        with open(config_path, 'rb') as f:
            self._config = tomllib.load(f)

    def get(self, *keys: str, fallback=None):
        current = self._config
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, fallback)
            else:
                return fallback
        return current
```

### 13.5 領域資料容器（Value Object）

```python
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import pandas as pd

@dataclass
class ProcessingResult:
    """處理結果容器（不可變資料傳輸物件）"""
    entity_code: str               # 實體代碼
    entity_name: str               # 顯示名稱
    category: str                  # 類別（'individual' / 'corporate'）
    raw_data: pd.DataFrame         # 原始查詢結果
    processed_data: Optional[pd.DataFrame] = None

    # 金額欄位（業務語意明確的欄位）
    total_amount: int = 0
    service_fee: int = 0
    adjustment: int = 0

    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """建立後的不變式驗證"""
        if not self.entity_code:
            raise ValueError("entity_code 不能為空")
```

---

## 附錄：兩個專案的技術選型對照

| 技術/工具 | 兩個專案是否共用 | 用途 | 備注 |
|-----------|----------------|------|------|
| Python 3.11+ | ✅ 共用 | 核心語言 | 用 `match`、`|` union、`tomllib` |
| pandas | ✅ 共用 | 資料處理核心 | 財務欄位操作 |
| DuckDB | spe_bank_recon | 中間層持久化 + SQL | 跨月累積資料 |
| TOML (tomllib) | ✅ 共用 | 配置檔格式 | Python 3.11 內建，無需依賴 |
| Parquet (pyarrow) | ✅ 共用 | Checkpoint 儲存 | 比 CSV 快 10 倍，保型別 |
| Streamlit | accrual_bot | Web UI | 非技術人員操作介面 |
| Google Sheets API | spe_bank_recon | 輸出共享報表 | 月結後推送給業務 |
| pytest | ✅ 共用 | 測試框架 | 搭配 pytest-asyncio |
| asyncio | accrual_bot | 非同步執行 | I/O 密集型並發 |
| threading.Lock | ✅ 共用 | 執行緒安全 | 保護 Singleton 初始化 |

---

*文件版本：v1.0 | 最後更新：2026-03-05*
*整合自：Accrual Bot（2026-01 重構版）+ SPE Bank Recon（2026-01 第三次迭代版）*
