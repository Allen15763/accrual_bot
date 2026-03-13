# core/pipeline 模組深度研究報告

> **範圍**：`accrual_bot/core/pipeline/` — 26 個 Python 檔案，5,647 行
> **研究角度**：軟體工程最佳實踐（設計模式、可維護性、可測試性、效能、邊界案例）
> **研究日期**：2026-03-12

---

## 目錄

1. [背景](#1-背景)
2. [用途](#2-用途)
3. [設計思路](#3-設計思路)
4. [各項知識點](#4-各項知識點)
5. [應用範例](#5-應用範例)
6. [優缺分析](#6-優缺分析)
7. [延伸議題](#7-延伸議題)
8. [其他](#8-其他)

---

## 1. 背景

### 1.1 模組誕生的脈絡

Accrual Bot 是一套每月執行的財務應計（Accrual）自動化系統，負責處理 SPT、SPX 兩個業務實體的採購訂單（PO）與採購申請（PR）資料，產出可供會計師稽核的底稿。

在重構之前（Phase 1 & 2 之前），SPT 與 SPX 的處理邏輯分散在多個獨立腳本中，彼此高度重複。主要痛點如下：

| 問題 | 影響 |
|------|------|
| 每個實體維護自己的「載入→清理→評估→輸出」流程，共約 750+ 行重複代碼 | Bug 修一個地方，另一個地方不會同步修正 |
| 無統一的錯誤處理與日誌格式 | 問題排查耗時 |
| 中間資料無法儲存，除錯必須重跑全流程 | 開發迭代成本高 |
| 步驟間耦合緊密，無法單獨測試 | 測試覆蓋率低 |

`core/pipeline` 模組即為解決上述問題而設計的框架層，提供：
- 統一的步驟介面
- 資料在步驟間流動的容器（Context）
- 可儲存/恢復執行狀態的 Checkpoint 機制
- 可供 SPT/SPX/MOB 共用的基礎步驟庫

### 1.2 模組演進歷史

```
Phase 1 (Jan 2026):  base.py、context.py、pipeline.py — 核心框架建立
Phase 2 (Jan 2026):  BaseLoadingStep、BaseERMEvaluationStep — 提取 750 行重複代碼
Phase 3 (Jan 2026):  checkpoint.py — 合併兩個代碼庫的 Checkpoint 邏輯
Phase 4 (Jan 2026):  Streamlit UI 整合，新增 PipelineWithCheckpoint
Phase 5 (Feb 2026):  common.py 重構為配置驅動，PreviousWorkpaperIntegrationStep 配置化
Phase 6 (Mar 2026):  15 個 shim 檔案建立，完成向後兼容遷移至 tasks/ 目錄
```

---

## 2. 用途

### 2.1 整體定位

`core/pipeline` 是整個系統的**執行框架（Execution Framework）**，負責：

```
┌─────────────────────────────────────────────────────┐
│              外部調用層 (tasks/ orchestrators)        │
│   SPTPipelineOrchestrator / SPXPipelineOrchestrator  │
└──────────────────────────┬──────────────────────────┘
                           │ 使用
┌──────────────────────────▼──────────────────────────┐
│              core/pipeline 框架層                    │
│  Pipeline → ProcessingContext → PipelineStep[]       │
│  CheckpointManager → PipelineWithCheckpoint          │
└──────────────────────────┬──────────────────────────┘
                           │ 讀取/寫入
┌──────────────────────────▼──────────────────────────┐
│              core/datasources 數據層                 │
│     ExcelSource / CSVSource / DuckDBSource ...       │
└─────────────────────────────────────────────────────┘
```

### 2.2 各子模組用途

| 檔案 | 行數 | 核心用途 |
|------|------|---------|
| `__init__.py` | 118 | 統一公開 API，34 個 export |
| `base.py` | 466 | 定義步驟介面（`PipelineStep`）及組合步驟（Conditional/Parallel/Sequential） |
| `context.py` | 343 | 步驟間資料載體（`ProcessingContext`） |
| `pipeline.py` | 547 | 步驟編排引擎（`Pipeline`、`PipelineBuilder`、`PipelineExecutor`） |
| `checkpoint.py` | 618 | 中間狀態持久化與恢復（`CheckpointManager`、`PipelineWithCheckpoint`） |
| `steps/base_loading.py` | 593 | 並發資料載入的模板基類（`BaseLoadingStep`） |
| `steps/base_evaluation.py` | 518 | ERM 邏輯評估的模板基類（`BaseERMEvaluationStep`） |
| `steps/common.py` | 1254 | 10 個共用步驟 + 2 個工具類別 |
| `steps/business.py` | 324 | 4 個業務邏輯步驟 |
| `steps/post_processing.py` | 704 | 後處理框架（品質檢查、統計生成） |
| `steps/*_shim.py` × 15 | ~76 | 向後兼容重新導出（re-export shims） |

### 2.3 資料流概覽

```
檔案系統
    │
    ▼
BaseLoadingStep.execute()        ← DataSourceFactory 並發讀取
    │ 填入
    ▼
ProcessingContext {
    .data: DataFrame              ← 主數據（逐步轉換）
    ._auxiliary_data: {           ← 輔助數據（參考底稿、AP Invoice 等）
        'previous': DataFrame,
        'procurement_po': DataFrame,
        ...
    }
    ._variables: {                ← 跨步驟共享狀態
        'processing_date': 202509,
        'file_paths': {...},
        ...
    }
}
    │ 傳遞給
    ▼
[步驟鏈]
ProductFilterStep
    → PreviousWorkpaperIntegrationStep
    → ProcurementIntegrationStep
    → DateLogicStep
    → BaseERMEvaluationStep (SPT/SPX 子類)
    → DataQualityCheckStep
    → StatisticsGenerationStep
    → ExportStep
    │
    ▼
CheckpointManager.save_checkpoint()   ← 每步成功後儲存
```

---

## 3. 設計思路

### 3.1 核心設計模式

#### 3.1.1 Template Method Pattern（模板方法）

**使用位置**：`BaseLoadingStep`、`BaseERMEvaluationStep`、`BasePostProcessingStep`

這是整個 pipeline 框架最核心的設計決策。基類定義「演算法骨架」（固定流程），子類只填入「鉤子方法」（可變細節）：

```python
# base_loading.py — 固定流程（模板方法）
async def execute(self, context):
    validated_configs = self._validate_file_configs()          # ① 驗證
    loaded_data = await self._load_all_files_concurrent(...)   # ② 並發載入
    df, date, m = self._extract_primary_data(...)              # ③ 提取主數據
    context.update_data(df)
    auxiliary_count = self._add_auxiliary_data_to_context(...) # ④ 添加輔助數據
    ref_count = await self._load_reference_data(context)       # ⑤ 載入參考數據
    ...

# 子類只需實現（鉤子方法）：
@abstractmethod
def get_required_file_type(self) -> str: ...          # e.g. 'raw_po'

@abstractmethod
async def _load_primary_file(self, source, path): ... # 載入主文件

@abstractmethod
def _extract_primary_data(self, result): ...           # 提取主數據

@abstractmethod
async def _load_reference_data(self, context): ...     # 載入參考數據
```

類似地，`BaseERMEvaluationStep.execute()` 的固定流程為：
> 設置檔案日期 → 構建條件 → 應用狀態條件 → 處理格式錯誤 → 設置估列標記 → 設置會計欄位 → 後處理

子類只需實現 `_build_conditions()`、`_apply_status_conditions()`、`_set_accounting_fields()`。

**效益**：消除了 SPT/SPX 載入步驟中約 750 行的重複代碼，且新增實體（如 MOB）只需實現 4 個鉤子方法。

#### 3.1.2 Context Object Pattern（上下文物件）

**使用位置**：`ProcessingContext`（context.py）

這是解決「步驟間如何傳遞資料」的核心設計。所有步驟共享同一個 `ProcessingContext` 實例，透過它讀寫資料，而不是透過方法參數直接傳遞：

```python
class ProcessingContext:
    data: pd.DataFrame           # 主數據（每步驟可修改）
    _auxiliary_data: Dict        # 輔助數據（載入後只讀）
    _variables: Dict             # 共享狀態（跨步驟通訊）
    errors: List[str]            # 錯誤累積
    warnings: List[str]          # 警告累積
    _history: List[Dict]         # 執行歷史
    _validations: Dict           # 驗證結果
```

這讓步驟之間完全解耦——`DateLogicStep` 不需要知道 `PreviousWorkpaperIntegrationStep` 存在，只需從 context 讀取需要的欄位即可。

**Context 作為「記憶體資料庫」**：`_auxiliary_data` 的 key 名稱（如 `'previous'`、`'procurement_po'`、`'reference_account'`）充當「虛擬資料表名稱」，步驟按需查詢，形成鬆散耦合。

#### 3.1.3 Builder Pattern（建構器）

**使用位置**：`PipelineBuilder`（pipeline.py）、`StepMetadataBuilder`（common.py）

`PipelineBuilder` 提供流式 API（Fluent Interface），讓 Pipeline 的建構代碼具有高可讀性：

```python
pipeline = (
    PipelineBuilder("SPX_PO_Pipeline", entity_type="SPX")
    .with_description("SPX PO 處理 pipeline")
    .with_stop_on_error(True)
    .add_step(SPXDataLoadingStep(...))
    .add_step(ProductFilterStep())
    .add_step(DateLogicStep())
    .build()
)
```

`StepMetadataBuilder` 同樣採用鏈式 API，讓每個步驟能標準化其回報的 metadata：

```python
metadata = (
    StepMetadataBuilder()
    .set_row_counts(0, len(df))
    .set_process_counts(processed=len(df))
    .set_time_info(start_datetime, end_datetime)
    .add_custom('filter_rate', f"{filter_rate:.2f}%")
    .build()
)
```

#### 3.1.4 Command Pattern（命令模式）

**使用位置**：`PipelineStep.__call__()`（base.py:133）

`PipelineStep` 透過實作 `__call__()` 讓步驟實例可以直接被「呼叫」，並在此加入橫切關注點（cross-cutting concerns）：

```python
async def __call__(self, context) -> StepResult:
    start_time = datetime.now()

    # 前置驗證
    if not await self.validate_input(context):
        return StepResult(status=StepStatus.SKIPPED, ...)

    # 執行前置動作（prerequisites）
    for action in self._prerequisites:
        await action(context)

    # 執行主邏輯（支援 retry + timeout）
    for attempt in range(self.retry_count + 1):
        try:
            if self.timeout:
                result = await asyncio.wait_for(
                    self.execute(context), timeout=self.timeout
                )
            else:
                result = await self.execute(context)
            break
        except Exception as e:
            await asyncio.sleep(2 ** attempt)  # 指數退避

    # 執行後置動作（post_actions）
    for action in self._post_actions:
        await action(context)

    result.duration = (datetime.now() - start_time).total_seconds()
    return result
```

此設計將 retry、timeout、pre/post hooks 封裝在基類中，子類的 `execute()` 只需關注純粹的業務邏輯。

#### 3.1.5 Strategy Pattern（策略模式）

**使用位置**：`Pipeline._execute_sequential()` 與 `Pipeline._execute_parallel()`（pipeline.py）

`PipelineConfig.parallel_execution` 在 runtime 決定執行策略：

```python
if self.config.parallel_execution:
    results = await self._execute_parallel(context)
else:
    results = await self._execute_sequential(context)
```

並行模式使用 `asyncio.Semaphore(max_concurrent_steps)` 限制同時執行的步驟數，搭配 `asyncio.as_completed()` 實現快速失敗（fail-fast）。

#### 3.1.6 Composite Pattern（組合模式）

**使用位置**：`ConditionalStep`、`ParallelStep`、`SequentialStep`（base.py）

這三個「容器步驟」本身也是 `PipelineStep`，可以包含其他 `PipelineStep`，形成樹狀結構，讓複雜的步驟組合能以遞歸方式執行：

```python
ConditionalStep(
    condition=lambda ctx: ctx.metadata.entity_type == 'SPX',
    true_step=SPXSpecialStep(),
    false_step=StandardStep()
)
```

### 3.2 架構分層決策

整個 `core/pipeline` 分為三個清晰的子層：

```
執行引擎層 (Execution Engine)
  Pipeline, PipelineExecutor, PipelineWithCheckpoint
      ↕
步驟介面層 (Step Interface)
  PipelineStep(ABC), StepResult, StepStatus
      ↕
資料容器層 (Data Container)
  ProcessingContext, ValidationResult, ContextMetadata
```

這種分層確保「如何執行」（Engine）與「執行什麼」（Steps）與「傳遞什麼資料」（Context）三者完全解耦。

### 3.3 Checkpoint 設計哲學

Checkpoint 的核心設計原則是**儲存完整狀態，支援任意點恢復**。採用 directory-per-checkpoint 結構：

```
checkpoints/
└── SPX_PO_202509_after_ProductFilter/
    ├── data.parquet        (主數據 - 優先 Parquet，失敗 fallback Pickle)
    ├── checkpoint_info.json (元數據 + 變數 - JSON 安全序列化)
    └── auxiliary_data/
        ├── previous.parquet
        ├── procurement_po.parquet
        └── reference_account.parquet
```

Checkpoint 名稱格式：`{entity}_{type}_{date}_after_{step_name}`，人類可讀且支援過濾查詢。

---

## 4. 各項知識點

### 4.1 `PipelineStep[T]` — 泛型宣告卻未實際使用

```python
# base.py:63-66
T = TypeVar('T')

class PipelineStep(ABC, Generic[T]):
    ...
```

`PipelineStep` 繼承 `Generic[T]`，意圖是讓子類聲明其輸出類型，如 `class MyStep(PipelineStep[pd.DataFrame])`。但**整個代碼庫中沒有任何具體子類指定 `T`**，所有類都繼承 `PipelineStep` 而非 `PipelineStep[SomeType]`。

這是一個「規劃中但未完成的泛型化」，目前 `T` 不提供任何型別安全保證。若要完整實現，`execute()` 的回傳值應改為 `StepResult[T]`，`StepResult.data` 也應為 `Optional[T]`。

### 4.2 `StepResult.data` 欄位的「虛設」問題

```python
@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    data: Optional[pd.DataFrame] = None   # ← 幾乎未被使用
    ...
```

觀察所有具體步驟的 `execute()` 回傳，雖然大多數都設置了 `data=df`，但 `Pipeline._execute_sequential()` 完全不使用 `result.data`：

```python
# pipeline.py:220-228
for i, step in enumerate(self.steps, 1):
    result = await step(context)
    results.append(result)
    # ← result.data 未被讀取，context.data 才是真正的資料流
    if result.status == StepStatus.FAILED and self.config.stop_on_error:
        break
```

**步驟間的資料傳遞完全依賴 `context.update_data(df)`**，而非 `StepResult.data`。`StepResult.data` 僅作為除錯工具存在，不影響 pipeline 的正確執行。這是一個潛在的混淆點，也是「設計意圖」與「實際使用」脫節的例子。

### 4.3 `PipelineWithCheckpoint` 繞過 `__call__` 包裝器 ✅ 已修復（2026-03-14）

> **此問題已修復。** `checkpoint.py:439` 已由 `await step.execute(context)` 改為 `await step(context)`。

**歷史紀錄**：`PipelineWithCheckpoint.execute_with_checkpoint()` 原直接呼叫 `step.execute(context)`，**完全繞過了 `PipelineStep.__call__()` 提供的**：
- Retry 重試邏輯（`retry_count`）
- Timeout 超時控制（`asyncio.wait_for`）
- 指數退避（`asyncio.sleep(2 ** attempt)`）
- Prerequisites/Post-actions hooks
- 自動時間計算

對比之下，`Pipeline._execute_sequential()` 正確地呼叫 `await step(context)`（透過 `__call__`）。

**修復方式**（單行改動）：
```python
# checkpoint.py:439 — 修復後
result = await step(context)           # ← 透過 __call__，享有 retry/timeout/hooks

# 原有寫法（已移除）：
# result = await step.execute(context)  # ← 繞過了 __call__ 包裝器
```

### 4.4 `asyncio.as_completed()` 的 Fail-Fast 陷阱

```python
# pipeline.py:255-264 — 並行快速失敗模式
if self.config.stop_on_error:
    for task in asyncio.as_completed(tasks):
        result = await task
        results.append(result)

        if result.status == StepStatus.FAILED:
            for t in tasks:
                if not t.done():
                    t.cancel()   # ← 嘗試取消其他 task
            break
```

這裡有一個微妙的問題：**`asyncio.as_completed()` 接受的是協程列表（coroutines），但 `t.cancel()` 是 `Task` 的方法，不適用於未被 `asyncio.create_task()` 包裝的協程**。

實際上，`tasks = [execute_with_semaphore(step) for step in self.steps]` 產生的是協程物件（coroutines），不是 `asyncio.Task` 物件，因此 `t.cancel()` 在這裡會拋出 `AttributeError`（協程物件沒有 `.cancel()` 方法）或靜默失敗（取決於 Python 版本）。

正確的做法是先用 `asyncio.ensure_future()` 或 `asyncio.create_task()` 包裝成 Task。

### 4.5 `PipelineExecutor.execute_multiple()` — 貌似並行實為順序

```python
# pipeline.py:498-517
tasks = []
for pipeline_name in pipelines:
    task = self.execute_pipeline(...)   # ← 建立協程
    tasks.append((pipeline_name, task))

results = {}
for pipeline_name, task in tasks:
    result = await task   # ← 順序 await！
    results[pipeline_name] = result
```

儘管方法名稱和設計意圖暗示「並行執行多個 Pipeline」，實際上是**順序 await** 每一個 Pipeline，完全沒有並行效益。

正確的並行實現應改為：
```python
results_list = await asyncio.gather(
    *[self.execute_pipeline(name, data.copy(), date, **kwargs) for name in pipelines],
    return_exceptions=True
)
```

### 4.6 `ProcessingContext._variables` 的直接存取

在 `checkpoint.py` 中多次直接存取私有屬性：

```python
# checkpoint.py:151 — 直接存取私有屬性
for k, v in context._variables.items():
    try:
        json.dumps(v)
        safe_variables[k] = v
    except (TypeError, ValueError):
        safe_variables[k] = str(v)

# checkpoint.py:247 — 載入時也直接操作
error_metadata['context_variables'] = {
    k: str(v)[:100] for k, v in context._variables.items()
}
```

雖然 Python 的 `_` 前綴是「約定性私有」（非強制），但這種直接存取違背了封裝原則。`ProcessingContext` 已提供 `get_variable()`、`set_variable()` 等公開 API。更好的做法是在 `ProcessingContext` 上新增 `get_all_variables()` 方法，讓 `CheckpointManager` 透過公開介面存取。

### 4.7 Checkpoint 的 JSON 安全序列化機制

```python
# checkpoint.py:150-156
safe_variables = {}
for k, v in context._variables.items():
    try:
        json.dumps(v)           # ← 嘗試序列化
        safe_variables[k] = v   # ← 成功：保留原值
    except (TypeError, ValueError):
        safe_variables[k] = str(v)   # ← 失敗：轉為字串
```

這個「Try-and-Fallback」機制優雅地解決了 `context._variables` 中可能混雜不可序列化物件（如 `datetime`、`Path`、自定義 dataclass）的問題，確保 JSON 寫入不會失敗。

**潛在問題**：轉為字串後，載入時無法自動還原為原始型別。例如，若 `processing_date = 202509`（int），儲存後為 `"202509"`（str），後續步驟可能因型別不符而出錯。目前透過程式碼慣例規避（大多數步驟用 `str()` 做型別容錯），但這隱藏了一個潛在風險。

### 4.8 Parquet/Pickle 雙重 Fallback 序列化

```python
# checkpoint.py:182-197
def _save_dataframe(self, df, parquet_path, pkl_path, label):
    try:
        df.to_parquet(parquet_path, index=False)   # 優先 Parquet
    except Exception as e:
        self.logger.warning(f"{label} Parquet 儲存失敗: {e}")
        try:
            df.to_pickle(pkl_path)   # Fallback: Pickle
        except Exception as e2:
            self.logger.error(f"{label} Pickle 亦失敗: {e2}")
```

Parquet 是首選因為：高壓縮率、型別保留、跨語言相容（Python/R/SQL）。Pickle 作為 Fallback 解決 Parquet 不支援某些 pandas 擴展型別的問題（如 `pd.StringDtype`、`pd.Int64Dtype` 在舊版 PyArrow 可能有問題）。

載入時的策略也對應地「Parquet 優先，Pickle 補充」：

```python
# checkpoint.py:258-274
for aux_file in sorted(aux_data_dir.glob("*.parquet")):
    context.add_auxiliary_data(aux_name, pd.read_parquet(aux_file))

for aux_file in sorted(aux_data_dir.glob("*.pkl")):
    aux_name = aux_file.stem
    if context.has_auxiliary_data(aux_name):
        continue   # ← 不覆蓋已從 Parquet 載入的
    context.add_auxiliary_data(aux_name, pickle.load(f))
```

特別注意 `auxiliary_data/` 中有一個特殊處理：`ops_validation` 的 `discount` 欄位強制轉為 `str`（checkpoint.py:133），這是為了修復 PyArrow 無法序列化混合型別欄位的已知問題。

### 4.9 `ColumnResolver` — 靈活欄位名稱解析

在 `PreviousWorkpaperIntegrationStep` 中，透過 `ColumnResolver.resolve()` 支援不同版本底稿中欄位名稱不一致的問題：

```python
# common.py:694
key_col_canonical = f'{key_type}_line'      # 標準名稱如 'po_line'
df_key = ColumnResolver.resolve(df, key_col_canonical)
# 能自動匹配 'PO Line'、'po_line'、'PO#'、'PO Number' 等別名
```

這實現了「欄位名稱標準化」與「歷史底稿相容」的平衡，是資料工程中處理 schema drift 的常見手段。

### 4.10 `PreviousWorkpaperIntegrationStep` 的配置驅動映射

```python
def _load_mapping_config(self) -> None:
    config = config_manager._config_toml.get('previous_workpaper_integration', {})
    self.column_patterns = config.get('column_patterns', {})
    self.po_mappings = config.get('po_mappings', {}).get('fields', [])
    self.pr_mappings = config.get('pr_mappings', {}).get('fields', [])
    self.reviewer_config = config.get('reviewer_mapping', {})
```

欄位映射規則全部由 TOML 配置驅動，每個映射可攜帶 `entities` 限制（只對特定實體生效）：

```toml
[[previous_workpaper_integration.po_mappings.fields]]
source = "current_month_reviewed_by"
target = "Reviewed by 本月 FN"
entities = ["SPX"]   # ← 只對 SPX 實體執行
fill_na = true
```

這讓添加新欄位映射不需修改 Python 代碼，只需修改 TOML 配置，大幅降低維護成本。

### 4.11 `StepMetadataBuilder` — 標準化 Metadata 的建構者

透過鏈式 API，確保所有步驟的 metadata 結構一致，便於 UI 層解析和展示：

```python
class StepMetadataBuilder:
    def set_row_counts(self, input_rows, output_rows) -> 'StepMetadataBuilder':
        self.metadata['input_rows'] = int(input_rows)
        self.metadata['output_rows'] = int(output_rows)
        self.metadata['rows_changed'] = int(output_rows - input_rows)
        return self

    def add_custom(self, key, value) -> 'StepMetadataBuilder':
        self.metadata[key] = value
        return self

    def build(self) -> Dict[str, Any]:
        return self.metadata.copy()   # ← 回傳副本，避免外部修改
```

所有數值強制轉為 `int()` 是關鍵細節：numpy 整數（如 `np.int64`）不能被 JSON 序列化，但 Python `int` 可以。

### 4.12 `DateLogicStep` 中的 Status Column 動態解析

```python
# common.py:1061-1065
def get_status_column() -> str:
    if context.get_variable('file_paths').get('raw_po'):
        return 'PO狀態'
    else:
        return 'PR狀態'
```

`DateLogicStep` 透過檢查 `file_paths` 是否包含 `raw_po` 來動態決定狀態欄位名稱，而不是直接使用 `context.get_status_column()`（後者依賴 `processing_type`）。這兩種方式通常會得到相同結果，但邏輯路徑不同，是個輕微的不一致點。

---

## 5. 應用範例

### 5.1 最小可用 Pipeline（從零開始）

```python
import asyncio
import pandas as pd
from accrual_bot.core.pipeline import (
    PipelineBuilder, ProcessingContext,
    StepResult, StepStatus, PipelineStep
)

class AddColumnStep(PipelineStep):
    async def execute(self, context):
        df = context.data.copy()
        df['processed'] = True
        context.update_data(df)
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message=f"Added 'processed' column to {len(df)} rows"
        )

    async def validate_input(self, context):
        return context.data is not None and not context.data.empty


async def main():
    pipeline = (
        PipelineBuilder("Demo", entity_type="SPX")
        .with_stop_on_error(True)
        .add_step(AddColumnStep("AddColumn"))
        .build()
    )

    context = ProcessingContext(
        data=pd.DataFrame({'PO#': ['PO001'], 'Amount': [1000]}),
        entity_type="SPX",
        processing_date=202509,
        processing_type="PO"
    )

    result = await pipeline.execute(context)
    print(f"Success: {result['success']}")
    print(f"Processed data:\n{context.data}")

asyncio.run(main())
```

### 5.2 使用 BaseLoadingStep 建立載入步驟

```python
from typing import Tuple
import pandas as pd
from accrual_bot.core.pipeline.steps.base_loading import BaseLoadingStep
from accrual_bot.core.pipeline.context import ProcessingContext


class MyDataLoadingStep(BaseLoadingStep):
    """自定義 PO 資料載入步驟"""

    def get_required_file_type(self) -> str:
        return 'raw_po'   # 必要的主檔案 key

    async def _load_primary_file(
        self, source, file_path: str
    ) -> Tuple[pd.DataFrame, int, int]:
        """載入主要 PO 檔案，並返回 (DataFrame, YYYYMM, month)"""
        df = await source.read(sheet_name='PO Data', header=1)
        df = self._process_common_columns(df)   # 處理 Line#、GL# 等通用欄位
        date, m = self._extract_date_from_filename(file_path)
        return df, date, m

    def _extract_primary_data(self, primary_result):
        """驗證並提取主數據"""
        df, date, m = primary_result
        assert '產品代碼' in df.columns, "缺少必要欄位：產品代碼"
        return df, date, m

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        """載入科目對照表"""
        # reference_account 通常已在 _add_auxiliary_data_to_context 中載入
        # 此處可做額外的參考資料載入
        return 0


# 使用方式：
step = MyDataLoadingStep(
    name="MyDataLoading",
    file_paths={
        'raw_po': {'path': '/data/202509_PO.xlsx', 'params': {'sheet_name': 0}},
        'previous': '/data/previous_workpaper.xlsx',   # 簡單格式也支援
    }
)
```

### 5.3 使用 BaseERMEvaluationStep 建立評估步驟

```python
from dataclasses import dataclass
import pandas as pd
from accrual_bot.core.pipeline.steps.base_evaluation import (
    BaseERMEvaluationStep, BaseERMConditions
)


@dataclass
class MyConditions(BaseERMConditions):
    """擴展條件集合，加入自定義條件"""
    is_rental: pd.Series = None
    is_early_complete: pd.Series = None


class MyERMLogicStep(BaseERMEvaluationStep):

    def _build_conditions(self, df, file_date, status_column):
        """構建所有判斷條件"""
        no_status = df[status_column].isna() | (df[status_column] == 'nan')

        # 標準日期範圍條件
        erm = df['Expected Received Month_轉換格式'].astype('Int32')
        in_range = erm == file_date
        before_equal = erm <= file_date
        after = erm > file_date

        return MyConditions(
            no_status=no_status,
            in_date_range=in_range,
            erm_before_or_equal_file_date=before_equal,
            erm_after_file_date=after,
            format_error=df['YMs of Item Description'].isna(),
            out_of_date_range=~in_range,
            procurement_not_error=(
                df['Remarked by Procurement'].isna() |
                ~df['Remarked by Procurement'].str.contains('不預估', na=False)
            ),
            is_rental=df['GL#'].astype('string').str.startswith('622'),
        )

    def _apply_status_conditions(self, df, conditions, status_column):
        """應用狀態邏輯"""
        mask_complete = (
            conditions.no_status &
            conditions.erm_before_or_equal_file_date &
            conditions.procurement_not_error
        )
        df.loc[mask_complete, status_column] = '已完成'

        mask_pending = conditions.no_status & conditions.erm_after_file_date
        df.loc[mask_pending, status_column] = '尚未到期'

        return df

    def _set_accounting_fields(self, df, ref_account, ref_liability):
        """設置會計欄位"""
        mask = df['是否估計入帳'] == 'Y'
        df = self._set_account_name(df, ref_account, mask)
        df = self._calculate_accrual_amount(df, mask)
        df = self._handle_prepayment(df, mask, ref_liability)
        return df
```

### 5.4 使用 Checkpoint 執行並恢復

```python
import asyncio
from accrual_bot.core.pipeline.checkpoint import (
    execute_pipeline_with_checkpoint,
    resume_from_step,
    list_available_checkpoints
)

# 首次完整執行，每步後自動儲存 checkpoint
result = await execute_pipeline_with_checkpoint(
    file_paths={
        'raw_po': {'path': '/data/202509_PO.csv', 'params': {'encoding': 'utf-8'}},
        'previous': '/data/previous.xlsx',
    },
    processing_date=202509,
    pipeline_func=create_my_pipeline,   # 接受 file_paths 的 factory function
    entity='SPX',
    processing_type='PO',
    save_checkpoints=True
)

# 查看可用的 checkpoint
checkpoints = list_available_checkpoints(filter_by_entity='SPX')
for cp in checkpoints:
    print(f"{cp['name']}: {cp['data_shape']} @ {cp['timestamp']}")

# 從特定步驟恢復（跳過前面的載入和清理步驟）
result = await resume_from_step(
    checkpoint_name="SPX_PO_202509_after_DateLogic",
    start_from_step="SPXERMLogic",    # 從 ERM 邏輯步驟開始
    pipeline_func=create_my_pipeline,
    save_checkpoints=True
)
```

### 5.5 使用 ConditionalStep 實現 SPT/SPX 分支邏輯

```python
from accrual_bot.core.pipeline import ConditionalStep, PipelineBuilder

pipeline = (
    PipelineBuilder("Multi_Entity_Pipeline")
    .add_step(CommonLoadingStep())
    .add_step(CommonFilterStep())
    .add_step(
        ConditionalStep(
            name="EntityBranch",
            condition=lambda ctx: ctx.metadata.entity_type == 'SPX',
            true_step=SPXERMLogicStep("SPXEvaluation"),
            false_step=SPTERMLogicStep("SPTEvaluation")
        )
    )
    .add_step(CommonExportStep())
    .build()
)
```

### 5.6 使用 ParallelStep 並發執行獨立步驟

```python
from accrual_bot.core.pipeline import ParallelStep, PipelineBuilder

# AP Invoice 整合與 Procurement 整合彼此獨立，可並發執行
pipeline = (
    PipelineBuilder("SPX_PO")
    .add_step(DataLoadingStep())
    .add_step(
        ParallelStep(
            name="ParallelIntegrations",
            steps=[
                APInvoiceIntegrationStep(),
                ProcurementIntegrationStep(),
                ClosingListIntegrationStep(),
            ],
            fail_fast=False   # 允許部分失敗，繼續執行
        )
    )
    .add_step(DateLogicStep())
    .build()
)
```

### 5.7 使用 DataQualityCheckStep 進行品質驗證

```python
from accrual_bot.core.pipeline.steps.post_processing import (
    DataQualityCheckStep,
    StatisticsGenerationStep,
    create_post_processing_chain
)

# 建立後處理步驟鏈
post_processing_steps = create_post_processing_chain(
    DataQualityCheckStep(
        name="QualityCheck",
        required_columns=['PO#', 'GL#', 'PO狀態', '是否估計入帳'],
        max_null_ratio=0.3,
        check_duplicates=True
    ),
    StatisticsGenerationStep(
        name="Statistics",
        group_by_columns=['PO狀態', '是否估計入帳'],
        agg_columns=['Entry Amount', 'Accr. Amount']
    )
)

# 添加到 pipeline
for step in post_processing_steps:
    pipeline.add_step(step)
```

### 5.8 使用 StepMetadataBuilder 標準化 Metadata

```python
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder, create_error_metadata

# 成功時
metadata = (
    StepMetadataBuilder()
    .set_row_counts(input_count=1000, output_count=850)
    .set_process_counts(processed=850, skipped=150, failed=0)
    .set_time_info(start_datetime, end_datetime)
    .add_custom('accrual_items', 320)
    .add_custom('filter_reason', 'Product code not matching LG_SPX')
    .build()
)

# 失敗時
error_metadata = create_error_metadata(
    error=exception,
    context=context,
    step_name="SPXEvaluation",
    stage='erm_evaluation',
    custom_field='value'
)
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ 高度可測試性
Template Method Pattern 讓每個「鉤子方法」（`_build_conditions`、`_load_primary_file` 等）成為獨立的測試單元，無需建立完整的 Pipeline 上下文。這是測試套件能達到 90%+ 覆蓋率的關鍵原因（`base_evaluation.py` 65%、`base_loading.py` 80%）。

#### ✅ 清晰的責任分離
Context Object Pattern 確保步驟之間不直接依賴彼此，只依賴 `ProcessingContext` 介面。新增步驟不需修改任何現有步驟，符合 Open/Closed Principle。

#### ✅ 開發迭代效率
Checkpoint 機制讓開發者在調試某個步驟時（如 ERM 邏輯），可以從前一步驟的 checkpoint 恢復，不需每次從頭執行 5-10 分鐘的載入流程。

#### ✅ 配置驅動的靈活性
`PreviousWorkpaperIntegrationStep` 的 TOML 配置映射、`ProductFilterStep` 的 pattern 配置，讓業務規則的調整不需改動 Python 代碼，降低了財務人員與開發人員的溝通成本。

#### ✅ 標準化的可觀測性
`StepMetadataBuilder` 讓每個步驟都輸出一致結構的 metadata（input_rows、output_rows、start_time、end_time 等），UI 層和日誌分析工具可以無差別地解析所有步驟的執行結果。

### 6.2 缺點與已知問題

#### ✅ `PipelineWithCheckpoint` 繞過 `__call__` 包裝器（已修復，2026-03-14）

如 [4.3 節](#43-pipelinewithcheckpoint-繞過-__call__-包裝器) 所述，此問題已修復。`checkpoint.py:439` 現已改為呼叫 `await step(context)`，Checkpoint 模式下的重試邏輯和 hooks 均正常生效。

#### ❌ `PipelineExecutor.execute_multiple()` 未真正並行（低嚴重度）

如 [4.5 節](#45-pipelineexecutorexecute_multiple----貌似並行實為順序) 所述，`execute_multiple()` 實為順序執行。目前實際使用此方法的場景很少（主要是 API 設計），影響有限。

#### ❌ 部分步驟的 `execute()` 為 Stub（設計意圖與實現脫節）

以下步驟的核心邏輯未完全實現：

| 步驟 | 問題 | 位置 |
|------|------|------|
| `StatusEvaluationStep` | `_evaluate_row_status()` 直接返回 `"待評估"` | business.py:100 |
| `DateParsingStep.execute()` | 核心邏輯以注釋取代，未調用 `extract_date_range_from_description` | common.py:192-202 |
| `DepartmentConversionStep._convert_spt_department()` | `result = df['Department'].str[:3]`，忽略 FA 科目規則 | business.py:314 |

這些 Stub 在 Streamlit UI 呼叫 `UnifiedPipelineService.build_pipeline()` 時不會被使用（實際使用的是 `tasks/spt/steps/` 和 `tasks/spx/steps/` 中的具體實現）。但如果有人直接實例化 `business.py` 中的步驟，會得到錯誤的結果。

#### ❌ `context.get_entity_config()` 硬編碼業務配置（低嚴重度）

```python
# context.py:297-317 — 業務配置硬編碼在框架層
configs = {
    "MOB": {"fa_accounts": ["151101", "151201"], ...},
    "SPT": {"fa_accounts": ["151101", "151201"], ...},
    "SPX": {"fa_accounts": ["151101", "151201", "199999"], ...}
}
```

FA 帳號（`fa_accounts`）等業務配置直接硬編碼在 `ProcessingContext`（框架層），違反了架構設計原則——框架層不應知道業務細節。這些配置應從 `ConfigManager` 動態讀取（已在 `stagging.toml` 中定義了 `[fa_accounts]` section）。

另外，此方法中 `kiosk_suppliers` 和 `locker_suppliers` 的列表也是舊版供應商列表，可能已與 TOML 配置不同步。

#### ❌ `_determine_key_type()` 的已知 Bug（已知但保留）

```python
# common.py:806-819
def _determine_key_type(self, df, df_ref):
    has_po = (ColumnResolver.has_column(df, 'po_line') and
              ColumnResolver.has_column(df_ref, 'po_line'))
    has_pr = (ColumnResolver.has_column(df, 'pr_line') and
              ColumnResolver.has_column(df_ref, 'pr_line'))

    if has_po and has_pr:
        return 'po'
    elif has_pr and not has_po:
        return 'pr'
    return None   # ← 若兩者都有/都沒有以外的情況會返回 None
```

當 `df` 有 `PO Line`（被 ColumnResolver 識別）但 `df_ref` 沒有時，`has_po = False`，`has_pr = False`，返回 `None`，導致 Reviewer 資訊更新被跳過。這是 CLAUDE.md 中記錄的 2 個預先存在的測試失敗之一（`test_previous_workpaper.py`）。

---

## 7. 延伸議題

### 7.1 補完 `PipelineStep[T]` 泛型

目前 `PipelineStep[T]` 的泛型宣告未被利用。若要充分利用，可以：

```python
from typing import TypeVar, Generic
T = TypeVar('T', pd.DataFrame, dict, str)

class PipelineStep(ABC, Generic[T]):
    @abstractmethod
    async def execute(self, context: ProcessingContext) -> StepResult[T]:
        ...

@dataclass
class StepResult(Generic[T]):
    data: Optional[T] = None
    ...
```

這讓靜態型別檢查器（如 `mypy`）能驗證步驟的輸出型別，防止類型不符的步驟被串接。

### 7.2 引入 `asyncio.TaskGroup`（Python 3.11+）

目前並行執行使用 `asyncio.gather()` 或 `asyncio.as_completed()`。Python 3.11 引入的 `asyncio.TaskGroup` 提供更嚴格的任務生命週期管理，並能正確取消未完成的任務：

```python
# 取代 asyncio.gather(*tasks, return_exceptions=True)
async with asyncio.TaskGroup() as tg:
    tasks = [tg.create_task(step(context)) for step in self.steps]
results = [t.result() for t in tasks]
```

`TaskGroup` 會在任何子任務拋出異常時自動取消所有其他子任務，解決了 [4.4 節](#44-asyncioas_completed-的-fail-fast-陷阱) 中描述的 `.cancel()` 問題。

### 7.3 Checkpoint 的增量儲存（Incremental Checkpointing）

現行的 checkpoint 策略是每步完整保存整個 `context.data`，對大型資料集（如百萬行 PO 資料）會有顯著的 I/O 開銷。可以考慮引入「僅儲存增量（diff）」的策略：

```python
# 概念：只儲存本步驟新增/修改的欄位
def save_incremental_checkpoint(context, step_name, previous_columns):
    new_columns = set(context.data.columns) - set(previous_columns)
    modified_data = context.data[list(new_columns)]
    # 儲存 + 欄位清單，載入時合併
```

這對 15+ 步驟的完整 Pipeline 可大幅減少磁碟用量（估計節省 70-80%）。

### 7.4 步驟依賴圖（Dependency Graph）

目前 `Pipeline.steps` 是一個線性列表，無法表達「步驟 A 和 B 可以並發，但 C 必須等待 A 和 B 都完成」的拓撲依賴。可以引入 DAG（有向無環圖）表示：

```python
class PipelineDAG:
    def add_step(self, step: PipelineStep, depends_on: List[str] = []):
        ...

    async def execute_topological(self, context):
        # 拓撲排序後，可並發的步驟自動並發執行
        ...
```

這讓 Pipeline 能自動最大化並發性，無需手動使用 `ParallelStep` 包裝。

### 7.5 Context 的不變性（Immutability）

目前 `context.data` 是可變的，每個步驟都可以直接修改它。如果某個步驟錯誤地修改了其他步驟依賴的欄位，難以追蹤。可以考慮引入「寫時複製」（Copy-on-Write）或快照（Snapshot）機制：

```python
class ProcessingContext:
    def create_snapshot(self) -> 'ContextSnapshot':
        """創建當前狀態的快照，供 rollback 使用"""
        return ContextSnapshot(
            data=self.data.copy(),
            variables=self._variables.copy(),
            ...
        )

    def restore_snapshot(self, snapshot: 'ContextSnapshot'):
        """從快照恢復"""
        self.data = snapshot.data
        ...
```

搭配現有的 `rollback()` 機制，能讓步驟失敗時真正回到前一個安全狀態。

### 7.6 Observable Pipeline — 事件驅動監控

目前步驟執行結果（成功/失敗/統計）只能在執行完成後從 `results` 列表中取出。若要支援即時監控（如 Streamlit UI 的進度條），可以引入觀察者（Observer）機制：

```python
class PipelineEventEmitter:
    def on_step_start(self, step_name: str): ...
    def on_step_complete(self, step_name: str, result: StepResult): ...
    def on_step_failed(self, step_name: str, error: Exception): ...

class Pipeline:
    def add_observer(self, observer: PipelineEventEmitter): ...
```

目前 Streamlit UI 透過 `StreamlitPipelineRunner` 橋接 async/sync，若有事件機制可大幅簡化這個橋接層。

---

## 8. 其他

### 8.1 完整類別繼承樹

```
PipelineStep(ABC, Generic[T])
├── ConditionalStep           # 條件分支
├── ParallelStep              # 並行執行
├── SequentialStep            # 順序執行
├── BaseLoadingStep           # 載入步驟基類
│   ├── SPTDataLoadingStep    (tasks/spt/steps/)
│   ├── SPTPRDataLoadingStep  (tasks/spt/steps/)
│   ├── SPXDataLoadingStep    (tasks/spx/steps/)
│   ├── SPXPRDataLoadingStep  (tasks/spx/steps/)
│   └── PPEDataLoadingStep    (tasks/spx/steps/)
├── BaseERMEvaluationStep     # ERM 評估基類
│   ├── SPTERMLogicStep       (tasks/spt/steps/)
│   └── SPXERMLogicStep       (tasks/spx/steps/)
├── BasePostProcessingStep    # 後處理基類
│   ├── DataQualityCheckStep
│   └── StatisticsGenerationStep
├── DataCleaningStep
├── DateFormattingStep
├── DateParsingStep
├── ValidationStep
├── ExportStep
├── DataIntegrationStep
├── ProductFilterStep
├── PreviousWorkpaperIntegrationStep
├── ProcurementIntegrationStep
├── DateLogicStep
├── StatusEvaluationStep      (部分 stub)
├── AccountingAdjustmentStep  (部分 stub)
├── AccountCodeMappingStep
└── DepartmentConversionStep  (部分 stub)
```

### 8.2 關鍵類別關係圖

```
PipelineConfig ──────────────── Pipeline
      ↑ 使用                       ├── steps: List[PipelineStep]
      │                            └── execute() → Dict
PipelineBuilder ─────────── builds ─┘
                                    ↓ 傳入
ProcessingContext ──────────────────────────────────────────────
      ├── .data: DataFrame          ← 步驟間主資料流
      ├── ._auxiliary_data: Dict    ← 輔助資料（參考底稿等）
      ├── ._variables: Dict         ← 跨步驟共享狀態
      └── .metadata: ContextMetadata
                    ↕ 讀寫
PipelineStep (執行時)
      ├── execute(context) → StepResult  ← 實際邏輯
      └── validate_input(context) → bool ← 前置驗證

CheckpointManager ──────────── 持久化 ──────────── Context + Variables
PipelineWithCheckpoint ──── 使用 Pipeline + CheckpointManager
```

### 8.3 Checkpoint 檔案系統結構

```
checkpoints/
├── SPX_PO_202509_after_SPXDataLoading/
│   ├── data.parquet (或 data.pkl)
│   ├── checkpoint_info.json
│   │   ├── step_name, entity_type, processing_date, processing_type
│   │   ├── variables: {processing_date: 202509, file_paths: {...}}
│   │   ├── warnings: [], errors: []
│   │   ├── data_shape: [1500, 45]
│   │   └── timestamp: "20260309_143022"
│   └── auxiliary_data/
│       ├── previous.parquet
│       ├── procurement_po.parquet
│       └── ap_invoice.parquet
├── SPX_PO_202509_after_ProductFilter/
│   └── ...
└── SPX_PO_202509_after_DateLogic/
    └── ...
```

### 8.4 步驟執行狀態機

```
         ┌─────────┐
         │ PENDING │  ← 初始狀態
         └────┬────┘
              │ __call__() 開始
              ▼
         ┌─────────┐
         │ RUNNING │  ← validate_input() 通過，開始 execute()
         └────┬────┘
     ┌────────┼────────┐
     ▼        ▼        ▼
 ┌───────┐ ┌──────┐ ┌─────────┐
 │SUCCESS│ │FAILED│ │ SKIPPED │
 └───────┘ └──┬───┘ └─────────┘
              │
         retry_count > 0?
              │ Yes
              ▼
         ┌───────┐
         │ RETRY │ → 指數退避 → 重新 RUNNING
         └───────┘
```

### 8.5 向後兼容 Shim 檔案列表

15 個 shim 檔案實現了從 `core/pipeline/steps/` 到 `tasks/` 的透明遷移：

| Shim 檔案 | 重導出目標 |
|-----------|-----------|
| `spt_loading.py` | `tasks.spt.steps.loading` |
| `spt_steps.py` | `tasks.spt.steps.{processing, export}` |
| `spt_evaluation_erm.py` | `tasks.spt.steps.spt_evaluation_erm` |
| `spt_evaluation_affiliate.py` | `tasks.spt.steps.spt_evaluation_affiliate` |
| `spt_evaluation_accountant.py` | `tasks.spt.steps.spt_evaluation_accountant` |
| `spt_account_prediction.py` | `tasks.spt.steps.spt_account_prediction` |
| `spx_loading.py` | `tasks.spx.steps.loading` |
| `spx_steps.py` | `tasks.spx.steps.{processing, ...}` |
| `spx_evaluation.py` | `tasks.spx.steps.spx_evaluation` |
| `spx_evaluation_2.py` | `tasks.spx.steps.spx_evaluation_2` |
| `spx_condition_engine.py` | `tasks.spx.steps.spx_condition_engine` |
| `spx_exporting.py` | `tasks.spx.steps.spx_exporting` |
| `spx_integration.py` | `tasks.spx.steps.spx_integration` |
| `spx_pr_evaluation.py` | `tasks.spx.steps.spx_pr_evaluation` |
| `spx_ppe_qty_validation.py` | `tasks.spx.steps.spx_ppe_qty_validation` |

這讓現有的 `from accrual_bot.core.pipeline.steps.spt_loading import SPTDataLoadingStep` 仍然有效，無需修改任何使用者代碼。

### 8.6 測試覆蓋率參考

| 模組 | 覆蓋率 | 備注 |
|------|--------|------|
| `context.py` | 100% | `test_context.py` 完整覆蓋 |
| `base.py` | 90% | `test_base_classes.py` |
| `pipeline.py` | 86% | `test_pipeline.py`, `test_pipeline_builder.py` |
| `checkpoint.py` | ~60% | `test_checkpoint.py` |
| `steps/base_loading.py` | 80% | `test_base_loading.py` |
| `steps/base_evaluation.py` | 65% | `test_base_evaluation.py` |
| `steps/post_processing.py` | 88% | `test_post_processing.py` |
| `steps/business.py` | 84% | `test_business_steps.py` |
| `steps/common.py` | 40% | `test_common_steps.py`（複雜整合測試較難覆蓋） |

### 8.7 已知問題快速查找

| 問題 | 嚴重度 | 位置 | 說明 |
|------|--------|------|------|
| ~~`PipelineWithCheckpoint` 繞過 `__call__`~~ | ~~中~~ | ~~`checkpoint.py:439`~~ | ✅ 已修復（2026-03-14）— 改為 `await step(context)` |
| `execute_multiple()` 非真並行 | 低 | `pipeline.py:509` | 可接受，使用場景少 |
| `as_completed()` `.cancel()` 無效 | 低 | `pipeline.py:261-264` | 並行模式下取消邏輯無效 |
| `StatusEvaluationStep` stub | 中 | `business.py:100` | 直接使用會得到錯誤結果 |
| `context.get_entity_config()` 硬編碼 | 低 | `context.py:297-317` | 可能與 TOML 配置不同步 |
| `_determine_key_type()` 返回 None | 低 | `common.py:806` | 已知測試失敗（2 tests） |
| `_variables` 直接存取 | 低 | `checkpoint.py:151` | 違反封裝，但功能正確 |
| `StepResult.data` 未被使用 | 低 | `pipeline.py:220-228` | 設計意圖與實現脫節 |
| `Generic[T]` 未實際使用 | 低 | `base.py:66` | 泛型宣告形同虛設 |

---

*本文件基於對 26 個 Python 檔案（5,647 行）的逐行閱讀，並以軟體工程最佳實踐角度進行深度分析。*
