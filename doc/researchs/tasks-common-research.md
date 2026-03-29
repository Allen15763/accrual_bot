# tasks/common 模組技術研究文件

> 研究對象：`accrual_bot/tasks/common/`
> 撰寫日期：2026-03-13
> 版本：1.0

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

### 1.1 專案架構脈絡

`accrual_bot` 是一套以非同步管道（async pipeline）為核心的財務資料處理系統，主要處理 SPT、SPX 與 SCT 三個業務實體的應計費用（accrual）調節作業。整體架構採四層設計：

```
UI Layer       → Streamlit 使用者介面
Tasks Layer    → 各實體專屬的業務邏輯（tasks/spt/, tasks/spx/, tasks/sct/, tasks/common/）
Core Layer     → 管道框架（Pipeline, PipelineStep, ProcessingContext）
Utils Layer    → 跨模組工具（ConfigManager, Logger, Data Utilities）
```

在「Tasks Layer」中，SPT、SPX 與 SCT 各自維護獨立的步驟模組（steps/）和管道協調器（pipeline orchestrator）。隨著系統演進，部分業務邏輯是多個實體共用的，如果分別在 `tasks/spt/`、`tasks/spx/` 和 `tasks/sct/` 中各維護一份，必然造成程式碼重複與維護負擔。

### 1.2 tasks/common 的誕生

`tasks/common/` 模組正是為了解決跨實體共用邏輯而設立的。它的定位是：

- **不屬於** core 框架層（不是管道基礎建設）
- **不屬於** 特定實體（不是 SPT 或 SPX 專屬）
- **屬於** 可被多個實體的 Orchestrator 直接引用的共用步驟

這種分層策略避免了「共用邏輯放到 core 層導致業務污染框架」以及「共用邏輯在各實體重複實作導致維護發散」兩種反模式。

### 1.3 目前模組內容

截至 2026-03，`tasks/common/` 只包含一個步驟：

```
tasks/common/
├── __init__.py                  # 匯出 DataShapeSummaryStep
└── data_shape_summary.py        # 資料完整性驗證步驟（411 行）
```

雖然目前只有一個步驟，但此模組已建立清晰的擴充路徑：未來若有其他跨實體共用步驟，均可在此新增。

---

## 2. 用途

### 2.1 核心功能

`DataShapeSummaryStep` 是管道末端的**資料完整性驗證步驟**，其主要功能包含：

1. **原始資料摘要**：對管道最初載入的原始資料（raw data snapshot）建立 pivot table，呈現各產品代碼（Product Code）× 幣別（Currency）的金額加總與筆數。

2. **處理後資料摘要**：對管道最終輸出的資料（`context.data`）執行相同的 pivot table 分析，但欄位名稱已轉換為小寫（系統內部規範）。

3. **比對摘要**：產生一份「原始 vs 處理後」的比較表，揭露資料列數、欄位數、金額合計的差異，讓財務人員快速確認資料在處理過程中是否發生非預期的損耗或增加。

4. **可選 Excel 輸出**：將上述三份摘要匯出為 Excel 檔案，分別存於不同工作表（sheet），供財務審計使用。

### 2.2 在管道中的位置

```
DataLoading → ... → BusinessLogic → PostProcessing → Export → DataShapeSummary
```

`DataShapeSummaryStep` 永遠是管道的**最後一個步驟**。這個位置選擇不是偶然的——它需要：
- `context.data` 已是管道完整處理結果
- `auxiliary_data['raw_data_snapshot']` 已由 loading step 在最初儲存

### 2.3 雙重執行模式

除了在管道中作為步驟執行外，此模組還支援**獨立執行模式**（standalone mode），可直接從 checkpoint parquet 檔案或原始 Excel/CSV 檔案載入資料，無需啟動完整管道。這對於事後分析、排錯、或開發階段的快速驗證非常有用。

---

## 3. 設計思路

### 3.1 為何選擇「末端驗證」而非「逐步驗證」

最直觀的完整性驗證方式是在每個步驟後都記錄資料狀態。然而，這種方式在財務資料處理場景中有明顯缺點：

- **效能開銷大**：每個步驟後都需要執行 pivot 計算
- **輸出資訊爆炸**：管道有 10-15 個步驟，中間態的摘要價值有限
- **財務需求聚焦**：財務審計人員真正需要的是「原始輸入」與「最終輸出」的對比

因此，設計選擇了「首尾快照比對」策略：在管道開頭（loading step）存入原始資料快照，在管道末尾（DataShapeSummaryStep）取出比較。這種設計以最小的侵入性達成最有意義的驗證。

### 3.2 快照分離模式（Snapshot Separation Pattern）的設計含義

快照（snapshot）的存入與讀取被刻意分離到兩個不同的步驟：

```
SPTDataLoadingStep.execute()
    └── context.add_auxiliary_data('raw_data_snapshot', df.copy())  # 存入

DataShapeSummaryStep.execute()
    └── context.get_auxiliary_data('raw_data_snapshot')             # 讀取
```

這個設計有以下深層含義：

**職責清晰**：Loading step 是唯一知道「原始資料」的步驟。讓它負責存入快照是自然的。DataShapeSummaryStep 不應該、也沒有辦法直接訪問原始檔案（檔案路徑可能已不在 context 中）。

**不影響中間步驟**：`auxiliary_data` 是 ProcessingContext 的側帶存儲（side channel），中間的 filter、column addition、ERM logic 等步驟完全不知道有快照存在，也不會誤用或修改它。

**`df.copy()` 的必要性**：如果直接存入 `df`（參考），後續步驟對 `context.data` 的 in-place 修改可能影響快照。使用 `.copy()` 建立深拷貝是防禦性程式設計的展現。

### 3.3 與 SPT/SPX Orchestrator 的關係

`DataShapeSummaryStep` 被 SPT 和 SPX 的 orchestrator 以完全相同的方式引用：

```python
# tasks/spt/pipeline_orchestrator.py（同樣邏輯也在 tasks/spx/pipeline_orchestrator.py）
from accrual_bot.tasks.common import DataShapeSummaryStep

'DataShapeSummary': lambda: DataShapeSummaryStep(
    name="DataShapeSummary",
    export_excel=True,
    output_dir="output",
    required=False
)
```

這種設計展現了幾個架構決策：

1. **Lambda 延遲實例化**：步驟工廠使用 `lambda` 而非直接實例化，確保每次建立管道時都得到新的步驟物件，避免步驟物件跨管道共用狀態。

2. **`required=False` 的非阻斷設計**：驗證步驟不應該阻斷主業務流程。即使 `DataShapeSummaryStep` 失敗（例如因欄位名稱不符），主業務資料已經成功匯出，管道只會記錄警告而繼續完成。

3. **共用不是繼承**：SPT 和 SPX 不是繼承 `DataShapeSummaryStep` 再覆寫，而是直接使用同一個類別。因為驗證邏輯本身就應該是通用的，實體差異透過配置（TOML 欄位名稱）而非繼承來表達。

### 3.4 配置驅動（Configuration-Driven）的欄位映射

不同實體、不同階段的資料欄位命名規範不同：

- 原始資料（來自外部 ERP）：`Product Code`、`Currency`、`Entry Amount`（大寫、有空格）
- 處理後資料（系統內部規範）：`product_code`、`currency`、`entry_amount`（小寫、底線）

這個映射關係被定義在 `stagging.toml` 的 `[data_shape_summary]` 段落中，而非硬編碼在步驟類別裡。這確保了：

- 欄位名稱若有變更，只需改 TOML，不需修改程式碼
- 不同實體如果將來有不同的欄位名稱，可以透過配置擴充（雖然目前 SPT 和 SPX 共用同一組欄位名稱配置）

然而，TOML 配置的讀取方式有設計瑕疵，詳見第 6 章優缺分析。

### 3.5 獨立執行模式的設計哲學

`run_standalone_summary()` 是一個模組級的非同步函數（而非類別方法），搭配 `_load_file()` 輔助函數，構成完整的獨立執行介面。

這種設計遵循了「讓最常見的操作最容易執行」原則：在開發或排錯時，使用者只需知道兩個檔案路徑，不需理解整個管道框架。

```python
# 一行呼叫，立即得到結果
asyncio.run(run_standalone_summary(
    raw_data_path='checkpoints/SPX_PO_202601_after_SPXDataLoading/data.parquet',
    processed_data_path='checkpoints/SPX_PO_202601_after_DataReformatting/data.parquet',
))
```

`_load_file()` 使用 Python 3.10 的 `match` 語法支援三種格式（parquet/Excel/CSV），讓使用者可以混搭不同格式的原始與處理後資料（例如原始 CSV + 處理後 parquet）。

### 3.6 Enabled Flag 的職責設計（與其問題）

系統設計中，`data_shape_summary.enabled` 旗標的語義是「是否在 loading step 存入快照」，而非「是否執行 DataShapeSummaryStep」。這個設計選擇造成了一個微妙的不一致：

- **管道步驟是否執行**：由 orchestrator 的 `enabled_po_steps` 清單決定
- **快照是否被存入**：由 loading step 讀取 `data_shape_summary.enabled` 決定
- **DataShapeSummaryStep 本身**：讀取了 `_summary_config` 但從未使用其中的 `enabled` 欄位

這個三方不一致是一個潛在的維護陷阱，詳見第 6 章的具體分析。

---

## 4. 各項知識點

### 4.1 pandas `pivot_table` 高級用法

`DataShapeSummaryStep._create_pivot_summary()` 使用了 `pandas.DataFrame.pivot_table` 的幾個進階特性：

#### 4.1.1 多重 aggfunc

```python
df.pivot_table(
    index=[product_col],
    columns=[currency_col],
    values='amt',
    aggfunc=['sum', 'count'],   # 同時計算 sum 和 count
    margins=True,
    margins_name='Total'
)
```

當 `aggfunc` 為列表時，結果是**多層欄位（MultiIndex columns）**：

```
                 sum                    count
currency         TWD       USD  Total   TWD    USD   Total
product_code
A001           1000.0    500.0  1500.0    5      2       7
A002            800.0      0.0   800.0    4      0       4
Total          1800.0    500.0  2300.0    9      2      11
```

這種多層結構在 Excel 中顯示為合併欄頭，對財務人員相當直觀。

#### 4.1.2 `margins=True` 自動合計

`margins=True` 會在最後新增一個名為 `margins_name`（此處為 `'Total'`）的合計行與合計欄。注意：
- 合計行（index 維度）：每個幣別的總金額/筆數
- 合計欄（columns 維度）：每個產品的跨幣別總金額/筆數
- 右下角交叉格：全域合計

#### 4.1.3 `.assign()` 的鏈式用法

```python
df[required_cols].assign(
    amt=lambda row: pd.to_numeric(row[amount_col], errors='coerce').fillna(0)
)
```

`.assign()` 回傳新的 DataFrame（不修改原始），其中新增或覆寫欄位。配合 `lambda` 引用同一 DataFrame 的其他欄位，是避免 `SettingWithCopyWarning` 的最佳實踐。

`pd.to_numeric(..., errors='coerce')` 將無法轉換的值（如文字「N/A」）轉為 `NaN`，再用 `.fillna(0)` 填補，確保計算不受非數字資料影響。

#### 4.1.4 欄位防禦性檢查

```python
available_cols = [c for c in required_cols if c in df.columns]
if len(available_cols) < 3:
    logger.warning(...)
    return pd.DataFrame()
```

當欄位缺失時回傳空 DataFrame 而非拋出例外，讓呼叫端（`execute()`）用 `if not raw_pivot.empty` 跳過後續處理。這是一種「防禦性降級」設計。

### 4.2 `pd.ExcelWriter` Context Manager 用法

```python
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    for sheet_name, df in summaries.items():
        df.to_excel(writer, sheet_name=sheet_name)
```

#### 4.2.1 為何使用 Context Manager

`pd.ExcelWriter` 實作了 `__enter__` / `__exit__`，確保：
- 所有 sheet 寫入完成後，`__exit__` 會呼叫 `writer.save()`（在較新版本中為 `writer.close()`）
- 即使中途發生例外，檔案也會被正確關閉（不會產生損壞的 `.xlsx`）

#### 4.2.2 MultiIndex 欄位的 Excel 輸出

當 DataFrame 有 MultiIndex 欄位（如 pivot_table 的 sum/count 雙層）時，`df.to_excel()` 會自動處理欄頭的合併，輸出時每一層都會佔用一行表頭。

#### 4.2.3 engine='openpyxl' vs 'xlsxwriter'

- `openpyxl`：支援讀寫、修改現有檔案，但格式控制較繁瑣
- `xlsxwriter`：只支援寫入、格式控制能力強（顏色、圖表），但不能修改現有檔案

此處選用 `openpyxl` 是因為只需基本的多 sheet 寫入，不需要複雜格式。

### 4.3 Python 3.10 `match` 語法

`_load_file()` 函數使用了 Python 3.10 引入的結構化模式匹配（Structural Pattern Matching）：

```python
def _load_file(path: str) -> pd.DataFrame:
    p = Path(path)
    match p.suffix.lower():
        case '.parquet':
            return pd.read_parquet(path)
        case '.xlsx' | '.xls':
            return pd.read_excel(path)
        case '.csv':
            return pd.read_csv(path, dtype=str)
        case _:
            raise ValueError(f"不支援的檔案格式: {p.suffix}")
```

#### 4.3.1 `|` OR 模式

`case '.xlsx' | '.xls':` 是 match 語法的「OR 模式」，等同於 `if suffix in ('.xlsx', '.xls')`，比傳統 `if-elif` 更簡潔。

#### 4.3.2 `case _:` 萬用模式

`_` 是 match 語法的萬用匹配（wildcard pattern），等同於 `else`，永遠匹配但不綁定變數。

#### 4.3.3 與 if-elif 的差異

match 語法在**純值匹配**場景下比 if-elif 更有可讀性，但真正的威力在於**結構解構**（如匹配資料類別的屬性、列表的特定元素等），此處的用法是相對基礎的值匹配。

**注意**：此語法要求 Python 3.10+。若專案需要支援更舊版本，需改為 if-elif。

### 4.4 `@staticmethod` 使用原則

`DataShapeSummaryStep` 中有兩個靜態方法：

```python
@staticmethod
def _create_pivot_summary(df, product_col, currency_col, amount_col) -> pd.DataFrame:
    ...

@staticmethod
def _create_comparison_summary(raw_df, final_df, raw_amount_col, processed_amount_col) -> pd.DataFrame:
    ...
```

#### 4.4.1 何時使用 @staticmethod

靜態方法適用於：
- **不訪問 `self`**（實例狀態）
- **不訪問 `cls`**（類別狀態）
- **邏輯上屬於類別**（語義上是輔助函數，但放在類別外部顯得突兀）

`_create_pivot_summary` 和 `_create_comparison_summary` 完全依賴傳入的參數運作，不需要知道步驟的任何狀態，因此設為 `@staticmethod` 是正確的。

#### 4.4.2 @staticmethod vs @classmethod vs 模組級函數

| 類型 | 訪問 self | 訪問 cls | 可覆寫 | 語意 |
|------|----------|----------|--------|------|
| 實例方法 | ✓ | ✓ | ✓ | 操作實例狀態 |
| `@classmethod` | ✗ | ✓ | ✓ | 操作類別狀態（工廠方法）|
| `@staticmethod` | ✗ | ✗ | △ | 純函數，語意屬於類別 |
| 模組級函數 | ✗ | ✗ | ✗ | 純函數，與類別無直接關聯 |

#### 4.4.3 測試友善性

`@staticmethod` 方法可以不實例化類別直接呼叫：

```python
# 可以直接測試靜態方法，不需要 mock PipelineStep 的依賴
result = DataShapeSummaryStep._create_pivot_summary(df, 'Product Code', 'Currency', 'Entry Amount')
```

這讓單元測試更簡單。

### 4.5 PipelineStep 繼承模式

`DataShapeSummaryStep` 繼承自 `PipelineStep` 抽象基類：

```python
class DataShapeSummaryStep(PipelineStep):
    async def execute(self, context: ProcessingContext) -> StepResult:
        ...

    async def validate_input(self, context: ProcessingContext) -> bool:
        ...
```

#### 4.5.1 必須實作的抽象方法

`PipelineStep` 定義了兩個必須實作的方法：
- `execute(context)` → `StepResult`：步驟的核心業務邏輯
- `validate_input(context)` → `bool`：執行前的前置條件驗證

若子類別未實作這兩個方法，Python 會在**實例化時**（而非定義時）拋出 `TypeError`。

#### 4.5.2 `StepResult` 的標準回傳格式

```python
return StepResult(
    step_name=self.name,
    status=StepStatus.SUCCESS,   # 或 StepStatus.FAILED
    message="...",
    duration=duration,
    metadata=metadata
)
```

`StepResult` 是不可變的資料容器（dataclass），統一了管道中所有步驟的回傳格式，讓 Pipeline 能夠以一致的方式收集執行歷史。

#### 4.5.3 `required=False` 的語義

在 `PipelineStep.__init__()` 中，`required` 參數影響 Pipeline 遇到步驟失敗時的行為：
- `required=True`：失敗 → 管道停止（如果 `stop_on_error=True`）
- `required=False`：失敗 → 記錄 FAILED，但管道繼續執行

`DataShapeSummaryStep` 預設 `required=False`，因為驗證失敗不應阻斷已完成的業務資料匯出。

#### 4.5.4 `self.logger` 的繼承

`PipelineStep.__init__()` 內部通常會建立 `self.logger`。子類別的 `execute()` 方法可直接使用 `self.logger`，無需重新初始化。這確保了日誌格式的一致性（包含步驟名稱前綴等）。

### 4.6 ProcessingContext `auxiliary_data` 機制

`ProcessingContext` 是管道步驟之間的資料載體，其 `auxiliary_data` 是一個 `Dict[str, Any]` 的側帶存儲：

```python
# 存入
context.add_auxiliary_data('raw_data_snapshot', df.copy())

# 讀取
raw_snapshot = context.get_auxiliary_data('raw_data_snapshot')
```

#### 4.6.1 primary data vs auxiliary data

| 屬性 | 用途 | 存取方式 |
|------|------|----------|
| `context.data` | 主要處理中的 DataFrame（管道主流） | 直接屬性 |
| `context.auxiliary_data` | 側帶資料（參考表、快照、中間結果） | `add_auxiliary_data()` / `get_auxiliary_data()` |

將原始資料快照存入 `auxiliary_data` 而非修改 `context.data`，確保了主流程資料的純淨性。

#### 4.6.2 命名空間與鍵值設計

`DataShapeSummaryStep` 使用固定的 key 名稱：
- `'raw_data_snapshot'`（由 loading step 寫入）
- `'shape_summary_raw_data'`（由 DataShapeSummaryStep 寫入）
- `'shape_summary_processed_data'`
- `'shape_summary_comparison'`

這種前綴命名（`shape_summary_*`）避免與其他步驟的輔助資料發生命名衝突，是一種隱性的命名空間設計。

#### 4.6.3 型別安全問題

`auxiliary_data` 的值型別是 `Any`，沒有靜態型別保證。呼叫端需要自行確認取出的資料是預期型別：

```python
raw_snapshot = context.get_auxiliary_data('raw_data_snapshot')
if raw_snapshot is not None and not raw_snapshot.empty:  # 假設它是 DataFrame
    ...
```

若 loading step 因某種原因存入了非 DataFrame 的物件，`DataShapeSummaryStep` 的 `.empty` 屬性訪問會在執行時拋出 `AttributeError`，但這個錯誤會被 `execute()` 的 `try/except` 捕獲，並以 `StepStatus.FAILED` 回傳。

### 4.7 `StepMetadataBuilder` 的鏈式建構模式

```python
metadata = (
    StepMetadataBuilder()
    .set_time_info(start_datetime, end_datetime)
    .add_custom('sheets_generated', list(summaries.keys()))
    .add_custom('output_path', str(output_path) if output_path else None)
    .add_custom('raw_snapshot_available', raw_snapshot is not None)
    .build()
)
```

`StepMetadataBuilder` 使用流暢介面（Fluent Interface）模式，每個方法回傳 `self`，允許連鎖呼叫。最終 `.build()` 建立不可變的 metadata 物件。

這種模式的優點是：
- 語義清晰（每個 `.add_custom()` 代表一個獨立的 metadata 欄位）
- 擴充容易（新增欄位只需在呼叫鏈中加一行 `.add_custom()`）
- 不強制所有欄位（相比需要在建構子傳入所有參數）

---

## 5. 應用範例

### 5.1 在新實體 Orchestrator 中引用（推薦寫法）

假設新增一個 `NEW` 實體，在其 orchestrator 中引用 `DataShapeSummaryStep`：

```python
# tasks/new/pipeline_orchestrator.py
from accrual_bot.tasks.common import DataShapeSummaryStep
from accrual_bot.core.pipeline import PipelineBuilder

class NEWPipelineOrchestrator:
    def _create_step(self, step_name: str):
        step_registry = {
            # ... NEW 實體專屬步驟 ...
            'DataShapeSummary': lambda: DataShapeSummaryStep(
                name="DataShapeSummary",
                export_excel=True,
                output_dir="output",
                required=False
            ),
        }
        factory = step_registry.get(step_name)
        if factory is None:
            raise ValueError(f"未知步驟: {step_name}")
        return factory()

    def build_po_pipeline(self, file_paths: dict):
        enabled_steps = ['NEWDataLoading', ..., 'DataShapeSummary']
        builder = PipelineBuilder("NEW_PO_Pipeline", "NEW")
        for step_name in enabled_steps:
            builder.add_step(self._create_step(step_name))
        return builder.build()
```

### 5.2 在 Loading Step 中正確存入快照

```python
# tasks/new/steps/new_loading.py
from accrual_bot.utils.config import config_manager

class NEWDataLoadingStep(BaseLoadingStep):
    async def _load_primary_file(self, source, path: str) -> pd.DataFrame:
        df = await source.read()

        # 存入原始快照（在任何過濾前）
        shape_summary_cfg = config_manager._config_toml.get('data_shape_summary', {})
        if shape_summary_cfg.get('enabled', False):
            context.add_auxiliary_data('raw_data_snapshot', df.copy())

        # ... 後續過濾邏輯 ...
        return df  # 日期從 context.metadata.processing_date 取得，不再從檔名擷取
```

**注意**：快照應在**任何過濾或轉換之前**存入，確保反映真實的原始資料狀態。

### 5.3 獨立執行模式（事後分析）

```python
# 場景：管道已執行完畢，checkpoint 檔案已存在
# 想要重新產生驗證報告，不需要重跑完整管道

import asyncio
from accrual_bot.tasks.common.data_shape_summary import run_standalone_summary

result = asyncio.run(run_standalone_summary(
    raw_data_path='checkpoints/SPX_PO_202601_after_SPXDataLoading/data.parquet',
    processed_data_path='checkpoints/SPX_PO_202601_after_DataReformatting/data.parquet',
    entity='SPX',
    processing_type='PO',
    processing_date=202601,
    output_dir='output/reports'
))

print(f"執行結果: {result.status.value}")
print(f"訊息: {result.message}")
```

### 5.4 直接測試靜態方法（單元測試）

```python
# tests/unit/tasks/common/test_data_shape_summary.py
import pytest
import pandas as pd
from accrual_bot.tasks.common.data_shape_summary import DataShapeSummaryStep

class TestCreatePivotSummary:
    def test_basic_pivot(self):
        df = pd.DataFrame({
            'Product Code': ['A001', 'A001', 'A002'],
            'Currency': ['TWD', 'USD', 'TWD'],
            'Entry Amount': ['1000', '500', '800'],
        })
        result = DataShapeSummaryStep._create_pivot_summary(
            df,
            product_col='Product Code',
            currency_col='Currency',
            amount_col='Entry Amount'
        )
        assert not result.empty
        assert ('sum', 'TWD') in result.columns  # MultiIndex column

    def test_missing_column_returns_empty(self):
        df = pd.DataFrame({
            'Product Code': ['A001'],
            'Currency': ['TWD'],
            # 缺少 'Entry Amount'
        })
        result = DataShapeSummaryStep._create_pivot_summary(
            df, 'Product Code', 'Currency', 'Entry Amount'
        )
        assert result.empty

    def test_non_numeric_amount_coerced(self):
        df = pd.DataFrame({
            'Product Code': ['A001'],
            'Currency': ['TWD'],
            'Entry Amount': ['N/A'],  # 非數字值
        })
        result = DataShapeSummaryStep._create_pivot_summary(
            df, 'Product Code', 'Currency', 'Entry Amount'
        )
        # 非數字應被 coerce 為 0，不應拋出例外
        assert not result.empty

class TestCreateComparisonSummary:
    def test_comparison_rows(self):
        raw_df = pd.DataFrame({'Entry Amount': ['100', '200']})
        final_df = pd.DataFrame({'entry_amount': ['90'], 'new_col': ['x']})
        result = DataShapeSummaryStep._create_comparison_summary(
            raw_df, final_df, 'Entry Amount', 'entry_amount'
        )
        assert len(result) == 3  # 資料列數、欄位數、金額合計
        row_counts = result[result['指標'] == '資料列數'].iloc[0]
        assert row_counts['原始資料'] == 2
        assert row_counts['處理後資料'] == 1
        assert row_counts['差異'] == -1
```

### 5.5 自訂輸出目錄與不匯出 Excel

```python
# 在測試環境或不需要 Excel 輸出的場景
step = DataShapeSummaryStep(
    name="DataShapeSummary",
    export_excel=False,     # 不產生 Excel 檔案
    output_dir="/tmp/test", # 覆寫預設 output/ 目錄
    required=False
)
```

### 5.6 從管道結果取得摘要 DataFrame

```python
# 管道執行完畢後，從 context 取得摘要（供程式化分析使用）
context = pipeline.last_context  # 假設 Pipeline 提供此屬性

raw_pivot = context.get_auxiliary_data('shape_summary_raw_data')
processed_pivot = context.get_auxiliary_data('shape_summary_processed_data')
comparison = context.get_auxiliary_data('shape_summary_comparison')

if comparison is not None:
    # 檢查金額差異是否在容許範圍內
    amount_diff_row = comparison[comparison['指標'] == '金額合計'].iloc[0]
    if abs(amount_diff_row['差異']) > 1000:
        print("警告：金額差異超出容許範圍！")
```

---

## 6. 優缺分析

### 6.1 優點

#### 6.1.1 設計層次清晰

`tasks/common/` 作為獨立模組，清楚表達了「此步驟跨實體共用」的語義，比直接放在 `core/` 中（污染框架層）或在 `tasks/spt/` 和 `tasks/spx/` 中各自維護（重複程式碼）都更合理。

#### 6.1.2 非阻斷的驗證設計

`required=False` 確保驗證失敗不影響主業務流程，符合「輔助功能不應干擾核心業務」的軟體設計原則。

#### 6.1.3 雙模式支援

管道模式與獨立執行模式的共存設計，讓同一邏輯能在不同情境下復用，減少了「排錯工具」與「正式流程」之間的程式碼分歧。

#### 6.1.4 `@staticmethod` 的正確使用

`_create_pivot_summary` 和 `_create_comparison_summary` 確實不依賴實例狀態，設為靜態方法讓這兩個純計算函數更易於獨立測試與推理。

#### 6.1.5 防禦性降級

欄位缺失時回傳空 DataFrame 而非拋出例外，讓步驟在面對不完整資料時能優雅降級，而不是中止整個管道。

---

### 6.2 缺點與具體程式碼問題

#### 問題 1：直接存取私有屬性 `_config_toml`（封裝違反）

**位置**：`data_shape_summary.py` 第 63 行（`__init__`）

```python
# 有問題的寫法
self._summary_config = config_manager._config_toml.get('data_shape_summary', {})
```

`_config_toml` 是 `ConfigManager` 的私有屬性（Python 慣例：底線前綴表示非公開 API）。直接存取私有屬性違反了封裝原則：

- 若 `ConfigManager` 重構內部結構（例如將 `_config_toml` 改名或改為 property），此處會靜默失效（得到 `AttributeError`）
- `ConfigManager` 的公開 API 應該是 `config_manager.get_nested('data_shape_summary')` 之類的方法，讓實作細節對外隱藏

**正確寫法應為**：
```python
# 應透過公開 API 存取
self._summary_config = config_manager.get_section('data_shape_summary')
# 或若 ConfigManager 有 get_nested 方法：
self._summary_config = config_manager.get_nested('data_shape_summary', default={})
```

同樣的問題也出現在 loading steps 中（`tasks/spt/steps/spt_loading.py` 第 122 行等）。

#### 問題 2：`enabled` flag 職責不一致（三方分離）

`data_shape_summary.enabled` 旗標在系統中有三個相關位置，但職責分裂：

**TOML 配置**：
```toml
[data_shape_summary]
enabled = true   # 定義在這裡
```

**Loading Step 讀取**（決定快照是否存入）：
```python
shape_summary_cfg = config_manager._config_toml.get('data_shape_summary', {})
if shape_summary_cfg.get('enabled', False):
    context.add_auxiliary_data('raw_data_snapshot', df.copy())
```

**DataShapeSummaryStep.__init__**（讀取但從未使用 enabled）：
```python
self._summary_config = config_manager._config_toml.get('data_shape_summary', {})
# execute() 中從未使用 self._summary_config.get('enabled')
```

**問題核心**：
- `enabled=false` → 快照不存入 → `DataShapeSummaryStep` 執行時 `raw_data_snapshot` 為 `None` → 只產出 `processed_data` pivot，無 comparison，**但步驟本身仍然執行**
- `DataShapeSummaryStep` 讀取了 `_summary_config` 卻從不使用其 `enabled` 欄位，是無效程式碼
- 步驟是否執行的控制（`enabled_po_steps` 清單）與功能是否啟用的控制（`enabled` 旗標）是兩個不同層次的開關，但語義相互纏繞

**建議改進**：
```python
# DataShapeSummaryStep.execute() 中應明確檢查（若此語義是設計需求）
if not self._summary_config.get('enabled', True):
    return StepResult(step_name=self.name, status=StepStatus.SKIPPED, ...)
```
或者移除 `__init__` 中對 `_summary_config` 的讀取，讓「步驟是否執行」完全由 orchestrator 的步驟清單控制。

#### 問題 3：兩套 logger 並存（不一致的日誌源）

**模組頂部**：
```python
logger = get_logger(__name__)   # 模組級 logger，__name__ = 'accrual_bot.tasks.common.data_shape_summary'
```

**`_create_pivot_summary` 靜態方法內**：
```python
logger.warning("欄位不足，需要 ...")   # 使用模組級 logger
```

**`execute()` 方法內**：
```python
self.logger.info(msg)          # 使用繼承自 PipelineStep 的實例 logger
self.logger.error(...)
```

這導致同一個步驟的日誌訊息分別來自兩個不同的 logger 源，在日誌聚合工具中追蹤時會看到不一致的 logger 名稱（一個帶有步驟名稱前綴，一個沒有）。

**根本原因**：靜態方法無法訪問 `self.logger`，所以必須使用模組級 logger。

**可能的解決方案**：
1. 將靜態方法改為普通方法，使用 `self.logger`（但會失去 `@staticmethod` 的好處）
2. 在靜態方法中接受 `logger` 參數（較繁瑣但保留靜態性質）：
   ```python
   @staticmethod
   def _create_pivot_summary(df, product_col, currency_col, amount_col, logger=None) -> pd.DataFrame:
       _log = logger or get_logger(__name__)
       ...
   ```
3. 接受現狀，記錄在文件中（此為現有選擇，代價是日誌不一致）

#### 問題 4：缺乏對應的單元測試

~~目前 `tests/` 目錄中**沒有** `test_data_shape_summary.py`~~

> **2026-03-28 更新**：Phase 15 已新增 `tests/unit/tasks/common/test_data_shape_summary.py`（10 tests），覆蓋 `DataShapeSummaryStep` 的核心邏輯（`execute()`、`_create_pivot_summary`、`_create_comparison_summary`）及獨立執行模式。此測試缺口已填補。

#### 問題 5：output 目錄檔案累積問題

每次執行都產生帶有 timestamp 的 Excel 檔案：

```python
filename = f"DataShape_Summary_{entity}_{proc_type}_{date}_{timestamp}.xlsx"
# 例：DataShape_Summary_SPX_PO_202601_20260113_143022.xlsx
```

雖然 timestamp 避免了覆寫問題，但也意味著：
- 每次管道執行都在 `output/` 目錄新增一個 Excel 檔案
- 無自動清理機制
- 長期運行後 `output/` 目錄可能累積數百個歷史檔案
- 這些檔案是否在 `.gitignore` 中取決於 `output/` 是否被排除（目前 CLAUDE.md 指出 `output/` 是 git-ignored 的）

**建議**：考慮加入清理策略，例如保留最近 N 個檔案，或依日期分資料夾。

#### 問題 6：`validate_input` 的檢查不夠完整

```python
async def validate_input(self, context: ProcessingContext) -> bool:
    if context.data is None or context.data.empty:
        self.logger.warning("無可用資料進行驗證摘要")
        return False
    return True
```

`validate_input` 只檢查 `context.data` 是否存在，但沒有檢查：
- `context.metadata` 是否有效（`entity_type`、`processing_type`、`processing_date`）
- 這些欄位在 `_export_to_excel` 中被直接使用，若為 `None` 會導致 `AttributeError`，但此錯誤只會在 `execute()` 執行時被捕獲，而非在 `validate_input` 階段被提早發現

---

## 7. 延伸議題

### 7.1 如何新增更多驗證指標

目前 `_create_comparison_summary` 只比較三個指標：資料列數、欄位數、金額合計。實際財務審計可能需要更多驗證維度：

**可新增的驗證指標**：

```python
@staticmethod
def _create_extended_comparison(raw_df, final_df, config) -> pd.DataFrame:
    comparisons = []

    # 現有三個指標
    comparisons.extend([
        {'指標': '資料列數', '原始資料': len(raw_df), '處理後資料': len(final_df)},
        {'指標': '欄位數', '原始資料': len(raw_df.columns), '處理後資料': len(final_df.columns)},
        {'指標': '金額合計', ...},
    ])

    # 新增：缺失值比率
    if '金額欄位' in final_df.columns:
        null_ratio = final_df['金額欄位'].isna().mean()
        comparisons.append({'指標': '金額欄位缺失率', '處理後資料': f"{null_ratio:.2%}"})

    # 新增：唯一產品代碼數
    prod_col_raw = config.get('raw_columns', {}).get('product_col', 'Product Code')
    prod_col_proc = config.get('processed_columns', {}).get('product_col', 'product_code')
    if prod_col_raw in raw_df.columns:
        comparisons.append({'指標': '唯一產品代碼數',
                            '原始資料': raw_df[prod_col_raw].nunique(),
                            '處理後資料': final_df.get(prod_col_proc, pd.Series()).nunique()})

    # 新增：特定狀態欄位分布（如 'status' 欄位）
    if 'status' in final_df.columns:
        status_counts = final_df['status'].value_counts().to_dict()
        for status, count in status_counts.items():
            comparisons.append({'指標': f'狀態:{status}', '處理後資料': count})

    df = pd.DataFrame(comparisons)
    df['差異'] = df.get('處理後資料', 0) - df.get('原始資料', 0)
    return df
```

這些指標的配置可以新增到 TOML 中：

```toml
[data_shape_summary.extended_checks]
check_null_ratio = true
check_unique_product_count = true
check_status_distribution = true
status_column = "status"
```

### 7.2 泛化為通用驗證框架（Validation Framework）

目前 `DataShapeSummaryStep` 是一個特定功能的步驟，但可以演進為一個可配置的通用驗證框架：

#### 7.2.1 驗證規則抽象化

```python
from abc import ABC, abstractmethod
from typing import Any

class ValidationRule(ABC):
    """通用驗證規則基類"""
    @abstractmethod
    def validate(self, raw_df: pd.DataFrame, final_df: pd.DataFrame) -> dict:
        """回傳 {'name': '...', 'raw': ..., 'processed': ..., 'passed': bool}"""
        pass

class RowCountValidation(ValidationRule):
    def __init__(self, max_loss_ratio: float = 0.05):
        self.max_loss_ratio = max_loss_ratio

    def validate(self, raw_df, final_df):
        raw_count = len(raw_df)
        final_count = len(final_df)
        loss_ratio = (raw_count - final_count) / raw_count if raw_count > 0 else 0
        return {
            'name': '資料列數',
            'raw': raw_count,
            'processed': final_count,
            'passed': loss_ratio <= self.max_loss_ratio,
            'detail': f"損耗率 {loss_ratio:.2%}"
        }

class AmountToleranceValidation(ValidationRule):
    def __init__(self, tolerance: float = 0.001):
        self.tolerance = tolerance
    ...
```

#### 7.2.2 配置驅動的規則載入

```toml
[data_shape_summary.validation_rules]
row_count = { enabled = true, max_loss_ratio = 0.05 }
amount_tolerance = { enabled = true, tolerance = 0.001 }
null_ratio = { enabled = true, target_columns = ["status", "entry_amount"] }
```

#### 7.2.3 驗證報告格式

驗證框架可以產生結構化的驗證報告，區分「警告」（差異在容許範圍內）和「錯誤」（差異超出容許範圍）：

```python
class ValidationReport:
    passed: list[ValidationRule]
    warnings: list[ValidationRule]
    errors: list[ValidationRule]

    @property
    def is_acceptable(self) -> bool:
        return len(self.errors) == 0
```

### 7.3 與 CI/CD 整合的可能性

#### 7.3.1 作為品質關卡（Quality Gate）

在 CI/CD 管道中，每次程式碼變更後可以對固定的測試資料集執行管道，並比較 `DataShapeSummaryStep` 的輸出是否與基準線一致：

```yaml
# .github/workflows/pipeline_validation.yml
jobs:
  pipeline-regression:
    steps:
      - name: Run pipeline on test data
        run: python -m pytest tests/integration/ -v
      - name: Compare shape summary
        run: python scripts/compare_shape_summary.py \
          --baseline output/baseline_summary.xlsx \
          --current output/latest_summary.xlsx
      - name: Fail if significant drift
        run: python scripts/check_drift.py --threshold 0.01
```

#### 7.3.2 Parquet 格式作為驗證基準

由於 `_load_file` 支援讀取 parquet 格式，可以將每次的 `context.data` checkpoint 儲存為基準（baseline），並在後續版本執行後比較：

```bash
# 建立基準
python -m accrual_bot.tasks.common.data_shape_summary \
  --raw checkpoints/v1.0/after_loading.parquet \
  --processed checkpoints/v1.0/final.parquet \
  --output baselines/v1.0_summary.xlsx

# 比較新版本
python -m accrual_bot.tasks.common.data_shape_summary \
  --raw checkpoints/v1.1/after_loading.parquet \
  --processed checkpoints/v1.1/final.parquet \
  --output output/v1.1_summary.xlsx \
  --compare baselines/v1.0_summary.xlsx
```

#### 7.3.3 自動化告警

當金額差異超過閾值時，可以透過 webhook 或 email 發送告警，而不只是寫入 log：

```python
class DataShapeSummaryStep(PipelineStep):
    def __init__(self, ..., alert_threshold: float = 1000.0, alert_webhook: str = None):
        self.alert_threshold = alert_threshold
        self.alert_webhook = alert_webhook

    def _check_and_alert(self, comparison: pd.DataFrame):
        amount_row = comparison[comparison['指標'] == '金額合計'].iloc[0]
        if abs(amount_row['差異']) > self.alert_threshold and self.alert_webhook:
            import requests
            requests.post(self.alert_webhook, json={
                'text': f"⚠️ 金額差異異常：{amount_row['差異']:,.2f}"
            })
```

### 7.4 效能考量：大型資料集的 pivot 計算

當資料集較大（例如 10 萬行以上）時，`pivot_table` 的計算可能成為管道的效能瓶頸。可以考慮：

#### 7.4.1 採樣模式

```python
def _create_pivot_summary(df, ..., sample_size: int = None) -> pd.DataFrame:
    if sample_size and len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=42)
        logger.warning(f"資料集較大，採樣 {sample_size} 筆進行摘要")
    ...
```

#### 7.4.2 非同步化 pivot 計算

目前 `_create_pivot_summary` 是同步的，在 async pipeline 中會阻塞事件迴圈。對於大型資料集，應使用 `asyncio.get_event_loop().run_in_executor()` 將計算移至執行緒池：

```python
async def execute(self, context: ProcessingContext) -> StepResult:
    loop = asyncio.get_event_loop()
    raw_pivot = await loop.run_in_executor(
        None,
        DataShapeSummaryStep._create_pivot_summary,
        raw_snapshot, product_col, currency_col, amount_col
    )
```

### 7.5 多快照支援（Multi-Snapshot）

目前系統只支援一個「原始資料快照」（在 loading step 存入）。但在複雜的多步驟管道中，可能希望追蹤多個中間狀態：

```python
# 在 ProductFilter step 後存入快照
context.add_auxiliary_data('snapshot_after_filter', df.copy())

# 在 APInvoiceIntegration 後存入快照
context.add_auxiliary_data('snapshot_after_invoice', df.copy())
```

DataShapeSummaryStep 可以演進為接受一個「要比較的快照清單」，產生多階段的比較報告，讓財務人員追蹤每個步驟的資料變化量。

---

## 8. 其他

### 8.1 模組初始化設計

`tasks/common/__init__.py` 只有 7 行，但其設計值得關注：

```python
"""
跨實體共用的管道步驟
"""
from .data_shape_summary import DataShapeSummaryStep
__all__ = ['DataShapeSummaryStep']
```

`__all__` 的顯式宣告讓模組的公開介面一目了然：`from accrual_bot.tasks.common import *` 只會匯出 `DataShapeSummaryStep`，不會意外暴露 `run_standalone_summary`、`_load_file` 等輔助函數。

這是 Python 模組設計的良好實踐：**隱式 vs 顯式匯出**。

### 8.2 Python 3.10 match 語法的版本限制風險

`_load_file()` 函數使用的 `match` 語法是 Python 3.10+ 的特性。若專案的生產環境 Python 版本低於 3.10，此處會在執行時拋出 `SyntaxError`。

建議在 `pyproject.toml` 或 `requirements.txt` 中明確指定 Python 版本限制：

```toml
# pyproject.toml
[project]
requires-python = ">=3.10"
```

### 8.3 `context.set_variable` 的副作用

```python
context.set_variable('data_shape_summary_path', str(output_path))
```

這行程式碼讓 Excel 輸出路徑成為 `ProcessingContext` 的一個變數，可被後續步驟（若有）讀取。但 `DataShapeSummaryStep` 是管道的最後一步，沒有後續步驟，因此這個變數實際上只有在管道執行完畢後、外部程式碼讀取 context 時才有用（例如 UI 層顯示下載連結）。

這個設計隱含了一個假設：管道執行後，`context` 物件仍然可被外部存取。這個假設目前在 Streamlit UI 的執行流程中是成立的（`pipeline_runner.py` 會保留 context），但若未來改變管道生命週期管理方式，此設計可能失效。

### 8.4 文件字串（Docstring）的品質

`data_shape_summary.py` 的模組級 docstring 包含了使用範例（Usage），這是 Google Style Docstring 的良好實踐。但類別的 docstring 使用了「資料來源」和「產出」兩個非正式的小節，而非標準的 `Args:`/`Returns:`/`Raises:` 格式。

若使用 Sphinx 或 mkdocs 自動產生 API 文件，建議統一採用 Google Style 或 NumPy Style：

```python
class DataShapeSummaryStep(PipelineStep):
    """
    資料完整性驗證步驟。

    Args:
        name: 步驟名稱，預設 "DataShapeSummary"。
        export_excel: 是否匯出 Excel 報告，預設 True。
        output_dir: Excel 輸出目錄，預設 "output"。
        required: 步驟失敗是否中止管道，預設 False。

    Note:
        此步驟需要 loading step 在 auxiliary_data['raw_data_snapshot'] 中
        存入原始資料快照，且需啟用 TOML 配置中的 data_shape_summary.enabled。
    """
```

### 8.5 與 `CheckpointManager` 的互動潛力

目前 `DataShapeSummaryStep` 產生的摘要 DataFrame 存入 `auxiliary_data`，但 `CheckpointManager` 在儲存 checkpoint 時通常只儲存 `context.data`（主要 DataFrame），`auxiliary_data` 中的摘要 DataFrame 可能不會被持久化。

這意味著若管道從 checkpoint 恢復執行，並從 DataShapeSummaryStep 之後的步驟繼續，shape summary 的結果會遺失。但因為 DataShapeSummaryStep 是最後一步，這個問題在實際使用中幾乎不會發生。

若未來 `auxiliary_data` 的持久化需求增加，`CheckpointManager` 需要相應擴充以支援 `auxiliary_data` 的序列化（parquet 格式可以處理 DataFrame，但 `Dict[str, Any]` 的通用序列化需要額外設計）。

---

*文件終*
