# `accrual_bot/tasks/spx` 深度技術研究文件

> 作者：Claude Code 自動研究報告
> 研究範圍：`accrual_bot/tasks/spx/` 全部 13 個 Python 原始碼檔案
> 研究日期：2026-03-13
> **最後更新**：2026-03-17（記錄六個 Bug 修復：§6.2 孤立步驟、硬編碼 ID、PPE 一致性、ColumnAddition 耦合、文件不一致、sync/async 阻塞）

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

### 1.1 業務背景

SPX（Shopee Express）是 accrual_bot 系統所支援的三大業務實體之一（另外兩個是 SPT 和 SCT）。其應付帳款估計（Accrual）流程涵蓋四種作業類型：

| 類型 | 說明 | 資料來源 |
|------|------|---------|
| **PO** | 採購訂單估計（主流程） | HRIS 系統匯出的 CSV |
| **PR** | 請購單估計（輔助流程） | HRIS 系統匯出的 CSV |
| **PPE** | 固定資產合約折舊期計算 | 合約歸檔清單 + Google Sheets 續約表 |
| **PPE_DESC** | PO/PR 底稿品項說明提取 + 年限對應 | 已處理的底稿 Excel + 年限表 Excel |

SPX 業務特有的複雜性包括：
- **智取櫃（Locker）** 與 **繳費機（Kiosk）** 兩類自動化設備的硬體採購，含複雜的驗收確認邏輯
- **OPS 驗收底稿**（`ops_validation`）：線下的 IT OPS 部門驗收明細，與會計底稿需要交叉比對
- **關單清單**來自 Google Sheets（跨年度多 Sheet），而非系統內部
- **折扣驗收**（如「8折驗收」）的金額計算

### 1.2 模組位置與歷史

`tasks/spx/` 是 2026-01 架構重構（Phase 3）的產物，將原本散落在 `core/pipeline/steps/` 的 SPX 特有邏輯集中到此模組。同時在 2026-03（Phase 7）完成了 PPE_DESC Pipeline 的新增。

### 1.3 檔案清單

```
accrual_bot/tasks/spx/
├── __init__.py                   (7 行)   - 模組初始化，導出 SPXPipelineOrchestrator
├── pipeline_orchestrator.py      (434 行) - Pipeline 組裝與步驟工廠
└── steps/
    ├── __init__.py               (84 行)  - 子模組初始化
    ├── spx_condition_engine.py   (22 行)  - 向後兼容 re-export（核心邏輯已移至 core/pipeline/engines/condition_engine.py, 560 行）
    ├── spx_evaluation.py         (1302行) - 第一/第二階段狀態評估 + PPE 步驟
    ├── spx_evaluation_2.py       (415 行) - 訂金 PO 特殊狀態更新
    ├── spx_pr_evaluation.py      (592 行) - PR 專屬 ERM 邏輯
    ├── spx_integration.py        (1973行) - 資料整合（欄位增補、AP整合、關單、驗收）
    ├── spx_loading.py            (1877行) - 資料載入（PO/PR/PPE/OPS）
    ├── spx_exporting.py          (815 行) - 匯出（PO/PR/OPS驗收）
    ├── spx_steps.py              (690 行) - 傳統/實驗性步驟（已不在主流程中使用）
    ├── spx_ppe_desc.py           (659 行) - PPE_DESC 管道 + 純函式業務邏輯
    └── spx_ppe_qty_validation.py (593 行) - 會計 vs OPS 數量比對驗證
```

總計原始碼：約 **8,963 行**（不含 `core/pipeline/engines/condition_engine.py` 的 560 行，該檔案由 `spx_condition_engine.py` re-export）

---

## 2. 用途

### 2.1 模組公開 API

整個 `tasks/spx/` 模組對外只暴露一個入口：

```python
from accrual_bot.tasks.spx import SPXPipelineOrchestrator

orchestrator = SPXPipelineOrchestrator()
```

透過 Orchestrator 建構四種 Pipeline：

```python
# 1. PO Pipeline（配置驅動）
pipeline = orchestrator.build_po_pipeline(file_paths={...})

# 2. PR Pipeline（配置驅動）
pipeline = orchestrator.build_pr_pipeline(file_paths={...})

# 3. PPE Pipeline（硬編碼序列）
pipeline = orchestrator.build_ppe_pipeline(
    file_paths={'contract_filing_list': {'path': '...'}},
    processing_date=202512
)

# 4. PPE_DESC Pipeline（硬編碼序列）
pipeline = orchestrator.build_ppe_desc_pipeline(
    file_paths={'workpaper': '...', 'contract_periods': '...'},
    processing_date=202512
)
```

### 2.2 Pipeline 完整步驟序列

#### SPX PO Pipeline（典型配置，14 步驟）

```
SPXDataLoading
    → ProductFilter
    → ColumnAddition
    → APInvoiceIntegration
    → PreviousWorkpaperIntegration
    → ProcurementIntegration
    → DateLogic
    → ClosingListIntegration
    → StatusStage1
    → SPXERMLogic
    → ValidationDataProcessing
    → DepositStatusUpdate
    → DataReformatting
    → SPXExport
    → DataShapeSummary
```

#### SPX PR Pipeline（典型配置，10 步驟）

```
SPXPRDataLoading
    → ProductFilter
    → ColumnAddition
    → PreviousWorkpaperIntegration
    → ProcurementIntegration
    → DateLogic
    → ClosingListIntegration
    → StatusStage1
    → SPXPRERMLogic
    → PRDataReformatting
    → SPXPRExport
    → DataShapeSummary
```

#### SPX PPE Pipeline（固定 5 步驟）

```
PPEDataLoading → PPEDataCleaning → PPEDataMerge → PPEContractDateUpdate → PPEMonthDifference
```

#### SPX PPE_DESC Pipeline（固定 4 步驟）

```
PPEDescDataLoading → DescriptionExtraction → ContractPeriodMapping → PPEDescExport
```

---

## 3. 設計思路

### 3.1 整體架構原則

`tasks/spx/` 採用三層職責分離：

```
┌────────────────────────────────────────────────────┐
│   Pipeline Orchestrator（組裝層）                   │
│   - 讀取配置決定步驟順序                            │
│   - step factory 以 lambda dict 實作                │
├────────────────────────────────────────────────────┤
│   Pipeline Steps（執行層）                          │
│   - 每個步驟獨立、可測試                            │
│   - 統一介面：execute() / validate_input()          │
├────────────────────────────────────────────────────┤
│   Business Logic / Pure Functions（業務層）          │
│   - spx_ppe_desc.py 的模組級函式                    │
│   - SPXConditionEngine 的規則評估                   │
└────────────────────────────────────────────────────┘
```

### 3.2 配置驅動條件引擎（核心設計）

`SPXConditionEngine` 是本模組最具創新性的設計。核心思想是：

> **把「什麼條件 → 什麼狀態」的映射從程式碼中分離出去，放入 TOML 配置檔，讓業務人員可以在不修改程式碼的情況下調整判斷規則。**

TOML 規則格式（`stagging_spx.toml`）：

```toml
[spx_erm_status_rules]
[[spx_erm_status_rules.conditions]]
priority = 1
status_value = "已完成"
note = "ERM<=檔案日期，收貨數量=採購數量，無帳務問題，無錯誤備註"
combine = "and"
apply_to = ["PO"]
checks = [
    {type = "erm_le_date"},
    {type = "qty_matched"},
    {type = "not_billed"},
    {type = "not_error"}
]
```

引擎運作流程：

```
1. __init__: 從 TOML 載入規則 → 按 priority 排序
2. apply_rules: 依序遍歷規則
   ├── 跳過不適用的 processing_type
   ├── _build_combined_mask: 將多個 checks 用 and/or 組合
   │   └── _evaluate_check: 每個 check 轉為 pandas boolean mask
   │       ├── 優先查 prebuilt_masks（預先計算的 mask）
   │       └── 按需計算（含 ERM/帳務/備註/FA 類型）
   ├── 限縮至「尚無狀態」的列（+ override_statuses）
   └── 賦值 + 更新 no_status
```

### 3.3 混合模式（Hybrid Mode）

`StatusStage1Step` 體現了「配置驅動 + 程式碼保留」的混合設計哲學：

| 類型 | 條件 | 說明 |
|------|------|------|
| **程式碼保留** | 關單清單比對（待關單/已關單） | 涉及複雜資料關聯，需要 DataFrame merge |
| **程式碼保留** | FA 備註提取（xxxxxx 入 FA） | 需要 regex extract，結果是動態值而非固定標籤 |
| **程式碼保留** | 日期格式轉換（YYYY/MM → YYYYMM） | 格式轉換，非狀態判斷 |
| **配置驅動** | 押金/保證金識別 | 固定標籤，可配置的關鍵字 |
| **配置驅動** | BAO 供應商 GL 調整 | 可配置的供應商清單 |
| **配置驅動** | 租金/Intermediary 狀態 | 可配置的判斷條件 |

### 3.4 prebuilt_masks 機制

`SPXERMLogicStep` 展示了更進階的使用方式：

```python
# 步驟一：在強型別的 ERMConditions 中預先計算所有 boolean masks
conditions = self._build_conditions(df, processing_date, status_column)

# 步驟二：轉換為引擎識別的 prebuilt_masks dict
prebuilt_masks = {
    'no_status': cond.no_status,
    'erm_le_date': cond.erm_before_or_equal_file_date,
    'qty_matched': cond.quantity_matched,
    # ...16 個 mask
}

# 步驟三：引擎直接從 prebuilt_masks 取用，不重複計算
df, stats = self.engine.apply_rules(df, status_column, engine_context)
```

這種設計將「條件計算」與「條件應用順序」分離，提高了可測試性。

### 3.5 PPE_DESC 的純函式設計

`spx_ppe_desc.py` 採用了不同的設計策略：業務邏輯以**模組級純函式**實作，Pipeline Steps 只是輕薄的包裝器：

```python
# 純函式（可獨立測試）
def extract_clean_description(desc: str) -> str: ...
def extract_locker_info(text: str) -> Optional[str]: ...
def extract_address_from_dataframe(df, column_name) -> pd.DataFrame: ...
def _process_description(df: pd.DataFrame) -> pd.DataFrame: ...   # 組合器
def _process_contract_period(df, df_dep) -> pd.DataFrame: ...

# Pipeline Step（只是封裝）
class DescriptionExtractionStep(PipelineStep):
    async def execute(self, context):
        df_po = _process_description(context.data.copy())  # 委派給純函式
        ...
```

---

## 4. 各項知識點

### 4.1 SPXConditionEngine 詳解

#### 支援的 Check Types

引擎支援 24 種 check type，分為六大類：

| 類別 | Check Types | 說明 |
|------|-------------|------|
| **欄位比對類** | `contains`, `not_contains`, `equals`, `not_equals`, `in_list`, `not_in_list` | 基本的欄位值比對，支援正則表達式 |
| **欄位狀態類** | `is_not_null`, `is_null`, `no_status` | 空值檢查 |
| **ERM/日期類** | `erm_le_date`, `erm_gt_date`, `erm_in_range`, `out_of_range`, `desc_erm_le_date`, `desc_erm_gt_date`, `desc_erm_not_error` | Expected Received Month 相關判斷 |
| **帳務類** | `qty_matched`, `qty_not_matched`, `not_billed`, `has_billing`, `fully_billed`, `has_unpaid`, `format_error` | 數量與金額比對 |
| **備註類** | `remark_completed`, `pr_not_incomplete`, `not_error` | Procurement/FN 備註判斷 |
| **FA 類** | `is_fa`, `not_fa` | 固定資產科目判斷 |

#### `{TYPE}` 佔位符機制

config 中的 `field` 可使用 `{TYPE}` 佔位符，在執行時自動替換為實際的 `PO` 或 `PR`：

```toml
[[spx_status_stage1_rules.conditions]]
checks = [{type = "contains", field = "{TYPE} Supplier", pattern = "^BAO.*"}]
```

引擎執行時：
```python
if '{TYPE}' in check.get('field', ''):
    check = {**check, 'field': check['field'].replace('{TYPE}', processing_type)}
```

#### 引用解析（`_resolve_ref()`）

值可以是 TOML 直接值，也可以是點分隔路徑引用：

```toml
# 直接值
{type = "in_list", values = ["押金", "保證金"]}

# 引用（從 config 動態讀取）
{type = "in_list", list_key = "spx.deposit_keywords"}

# 特殊組合引用
{type = "in_list", list_key = "spx.asset_suppliers"}
# → 等同於 spx.kiosk_suppliers + spx.locker_suppliers 的合集
```

#### override_statuses 機制

允許規則覆蓋已有特定狀態的列：

```toml
[[spx_erm_status_rules.conditions]]
priority = 5
status_value = "已完成"
override_statuses = ["待關單"]  # 允許覆蓋「待關單」的列
```

### 4.2 關單清單的線段解析（`_closing_by_line()`）

SPX 的關單清單格式複雜，`_closing_by_line()` 解析 `line_no` 欄位的多種格式：

```
ALL                → 整張 PO 關單
Line2              → 指定第 2 行關單
Line2,5,7          → 指定第 2、5、7 行（半形逗號分隔）
Line2、5、7        → 指定第 2、5、7 行（頓號分隔）
Line2~12           → 第 2 到第 12 行（範圍）
Line2~5,8~12       → 第 2-5 行 + 第 8-12 行（混合）
```

解析後的 PO Line 格式為 `SPTTW-{PO#}-{line#}`（套用 HRIS 的 PO# 前綴格式）。

### 4.3 智取櫃（Locker）驗收數量比對邏輯

`ValidationDataProcessingStep` 的核心是一套複雜的模糊匹配邏輯：

#### 智取櫃類型識別（20+ 種）

```python
patterns = {
    'A': r'locker\s*A(?![A-Za-z0-9])',   # A 型，後不跟英數
    'XA': r'locker\s*XA(?![A-Za-z0-9])', # XA 型
    'DA': r'locker\s*控制主[櫃|機]',       # 控制主機
    '裝運費': r'locker\s*安裝運費',        # 安裝運費（特殊類型）
    ...
}
```

#### 匹配優先順序

由 `locker_priority_order` 配置驅動，防止 XA 被誤認為 A：

```toml
locker_priority_order = ["XA30", "XC30", "XA", "XB", "XC", ..., "A", "B", "C"]
```

#### 折扣驗收支援

驗收明細中的折扣欄位（如 `8折驗收`）透過 regex 提取折扣率：

```python
match = re.search(r'(\d+(?:\.\d+)?)[\s]*折', target_str)
if match:
    rate = float(match.group(1)) / 10.0  # 8折 → 0.8
```

`Accr. Amount` 計算：`Unit Price × 本期驗收數量/金額 × discount_rate`

#### 運費/安裝費特殊處理

含「運費」或「安裝費」的品項，`Accr. Amount` 直接取「本期驗收數量/金額」（不乘以 Unit Price）：

```python
non_shipping = ~df['Item Description'].str.contains('運費|安裝費', na=False)
df.loc[need_to_accrual & non_shipping, 'Accr. Amount'] = (
    df.loc[need_to_accrual & non_shipping, 'temp_amount']  # Unit Price × qty
)
df.loc[need_to_accrual & ~non_shipping, 'Accr. Amount'] = (
    df.loc[need_to_accrual & ~non_shipping, '本期驗收數量/金額']  # 直接取驗收值
)
```

### 4.4 ERMConditions Dataclass 設計

`ERMConditions` 是一個 `@dataclass`，強型別地容納 16 個 boolean Series：

```python
@dataclass
class ERMConditions:
    no_status: pd.Series
    in_date_range: pd.Series
    erm_before_or_equal_file_date: pd.Series
    quantity_matched: pd.Series
    not_billed: pd.Series
    has_billing: pd.Series
    fully_billed: pd.Series
    has_unpaid_amount: pd.Series
    procurement_completed_or_rent: pd.Series
    fn_completed_or_posted: pd.Series
    pr_not_incomplete: pd.Series
    is_fa: pd.Series
    procurement_not_error: pd.Series
    out_of_date_range: pd.Series
    format_error: pd.Series
```

好處：IDE 自動補全、避免字串 key 拼寫錯誤、可讀性高。

### 4.5 `YMs of Item Description` 欄位格式

SPX 的品項摘要欄位包含 14 字元格式的期間範圍：

```
格式：YYYYMM,YYYYMM
示例：202501,202512     → 2025年1月到2025年12月
      100001,100002     → 格式錯誤標記
```

引擎中的日期檢查直接對此欄位做字串切片：

```python
ym_start = df['YMs of Item Description'].str[:6].astype('Int32')
ym_end = df['YMs of Item Description'].str[7:].astype('Int32')
in_date_range = erm.between(ym_start, ym_end, inclusive='both')
```

`100001,100002` 作為特殊的「格式錯誤」哨兵值：

```python
format_error = df['YMs of Item Description'] == '100001,100002'
df.loc[format_error, status_column] = '格式錯誤，退單'
```

### 4.6 PPE_DESC 的多規則描述提取

`extract_clean_description()` 實作了三層規則的優先序：

**規則一（最高優先）：門市裝修工程（含地址和期數）**
```python
pattern1 = r'(門市裝修工程-.*?\(.*?\))\s*SPX\s*store decoration\s*(.*?)\s*#'
# 輸入：門市裝修工程-南港(南港路)SPX store decoration 第一期款項 #SP-XX
# 輸出：SPX_門市裝修工程-南港(南港路)_第一期款項
```

**規則二：有地址但無期數的工程項目**
```python
pattern2 = r'SVP_?(?:SPX)?\s*(.*?)(?:\(|（)([^)）]+)(?:\)|）)'
# 輸入：SVP_SPX 辦公室翻新工程(信義路五段7號)
# 輸出：SPX_辦公室翻新工程(信義路五段7號)
```

**規則三（通用）：清洗並加 SPX_ 前綴**
- 移除 `#...` 標籤
- 移除日期前綴（YYYY/MM）
- 移除公司前綴（SVP_SPX、SVP_...）
- 多空白整理

#### 台灣地址提取 Regex

```python
regex_pattern = r'\(((?:.{2,3}[縣市])?.{1,3}[區鄉鎮市].*?)\)'
# 匹配括號內以縣市區結構開頭的地址
# 支援：(台北市信義區...)、(信義區...)
```

#### HD 智取櫃特殊處理

因為「SPX HD locker」有控制主機（主櫃）、安裝運費等細分，`_hd_locker_info()` 用三個 mask 優先順序處理：
1. `SPX HD locker 控制主櫃` → `HD主櫃`
2. `SPX HD locker 安裝運費` → `HD安裝運費`
3. `SPX HD locker` → `HD櫃`

### 4.7 PPE 合約日期標準化

`PPEContractDateUpdateStep._update_contract_dates()` 對同一 `sp_code` 的記錄做日期標準化：

```python
# 同一 sp_code 的所有行：
# - contract_start: 取所有起租日的最小值
# - contract_end: 取所有終止日的最大值
for sp_code in df['sp_code'].unique():
    mask = df['sp_code'] == sp_code
    min_start = min(start_dates)  # 最早開始
    max_end = max(end_dates)      # 最晚結束
    df.loc[mask, ...] = min_start / max_end
```

`PPEMonthDifferenceStep` 則計算 `months_diff`：
```python
months_diff = (end.year - target.year) * 12 + (end.month - target.month) + 1
```

並加 1 是因為月份差異採用「包含」計算。

### 4.8 AccountingOPSValidationStep 的比對邏輯

此步驟比對「會計底稿」與「OPS 驗收底稿」兩份文件的 locker 數量是否一致：

```
1. OPS: 篩選驗收月份 <= processing_date
2. OPS: 按 (PO#, locker_type) 聚合數量
3. 會計: 從 Item Description 提取 locker_type
4. 會計: 按 (PO#, locker_type) 聚合，同單同類型去重
5. 比對: outer join，計算 matched / mismatched / only_in_accounting / only_in_ops
```

### 4.9 Step Registry Pattern（步驟工廠）

`SPXPipelineOrchestrator._create_step()` 使用 lambda dict 實作步驟工廠：

```python
step_registry = {
    'SPXDataLoading': lambda: SPXDataLoadingStep(
        name="SPXDataLoading",
        file_paths=file_paths
    ),
    'ProductFilter': lambda: ProductFilterStep(
        name="ProductFilter",
        product_pattern='(?i)LG_SPX',
        required=True
    ),
    # ...20 個步驟
}

step_factory = step_registry.get(step_name)
if step_factory:
    return step_factory()  # 延遲實例化（Lazy Instantiation）
```

Lambda 的作用是**延遲執行**：只有當該步驟真的被啟用時才建立實例，避免所有步驟在 Orchestrator 初始化時就被創建。

### 4.10 DataSourcePool 資源管理

`SPXDataLoadingStep` 使用 `DataSourcePool` 統一管理所有資料來源的生命週期：

```python
def __init__(self, ...):
    self.pool = DataSourcePool()  # 連接池

async def execute(self, context):
    try:
        source = DataSourceFactory.create_from_file(file_path, **params)
        self.pool.add_source(file_type, source)  # 登記
        df = await source.read()
        ...
    finally:
        await self._cleanup_resources()  # 確保釋放

async def _cleanup_resources(self):
    await self.pool.close_all()
```

`finally` 子句確保即使發生例外，資源也會被釋放（RAII pattern）。

### 4.11 AP Invoice 整合的時間過濾

`APInvoiceIntegrationStep` 處理 AP Invoice 的「Oct-24」格式期間欄位：

```python
df_ap['period'] = (
    pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
    .dt.strftime('%Y%m')
    .fillna('0')
    .astype('Int32')
)

# 只保留 processing_date 月份之前的最後一筆（最新入帳記錄）
df_ap = (
    df_ap.loc[df_ap['period'] <= yyyymm, :]
    .sort_values(by=['po_line', 'period'])
    .drop_duplicates(subset='po_line', keep='last')
)
```

### 4.12 Context Variable / Metadata 的隱式依賴鏈

Steps 之間透過 `ProcessingContext` 傳遞狀態，形成隱式依賴鏈。
處理日期統一從 `context.metadata.processing_date` 讀取（由 UI 使用者選擇或 CLI `run_config.toml` 設定），不再從檔名萃取。
Loading 步驟仍會將 `processing_date`/`processing_month` 寫入 variables（向後相容），但下游步驟已全部改用 metadata。

| Step | 寫入 (variables / aux) | 讀取 |
|------|------|------|
| `SPXDataLoadingStep` | `processing_date`(var), `processing_month`(var), `validation_file_path`(var), `file_paths`(var), `closing_list`(aux), `reference_account`(aux) | `context.metadata.processing_date` |
| `ColumnAdditionStep` | `PO狀態`/`PR狀態` 欄位重命名 | `context.metadata.processing_date` |
| `APInvoiceIntegrationStep` | `GL DATE` 欄位 | `ap_invoice`(aux), `context.metadata.processing_date` |
| `ClosingListIntegrationStep` | `closing_list`(aux) | — |
| `StatusStage1Step` | `PO狀態`/`PR狀態`, `matched_condition_on_status` | `closing_list`(aux), `context.metadata.processing_date` |
| `SPXERMLogicStep` | `是否估計入帳`, `Accr. Amount`, `Liability` | `reference_account`(aux), `reference_liability`(aux), `context.metadata.processing_date` |
| `ValidationDataProcessingStep` | `本期驗收數量/金額`, `locker_*`(aux), `kiosk_data`(aux) | `validation_file_path`(var), `file_paths`(var), `context.metadata.processing_date` |
| `SPXExportStep` | 檔案系統 | `locker_non_discount`(aux), `locker_discount`(aux), `kiosk_data`(aux), `context.metadata.processing_date` |

---

## 5. 應用範例

### 5.1 基本 PO Pipeline 執行

```python
import asyncio
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext

async def run_spx_po():
    orchestrator = SPXPipelineOrchestrator()

    # 新格式：帶參數配置
    file_paths = {
        'raw_po': {
            'path': 'resources/202501/Original Data/202501_purchase_order_spx.csv',
            'params': {'encoding': 'utf-8', 'sep': ',', 'dtype': 'str'}
        },
        'previous': {
            'path': 'resources/202501/前期底稿/SPX/202412_PO_FN.xlsx',
            'params': {'sheet_name': 0, 'header': 0, 'dtype': 'str'}
        },
        'ap_invoice': {
            'path': 'resources/202501/AP Invoice/ap_invoice.xlsx',
            'params': {}
        },
        'ops_validation': {
            'path': 'resources/202501/OPS驗收/ops_validation_202501.xlsx',
            'params': {
                'sheet_name': '智取櫃驗收明細',
                'header': 3,
                'usecols': 'A:AH'
            }
        }
    }

    pipeline = orchestrator.build_po_pipeline(file_paths=file_paths)

    context = ProcessingContext(
        entity_type='SPX',
        processing_date=202501,
        processing_type='PO'
    )

    result = await pipeline.execute(context)

    if result.is_success:
        print(f"成功處理 {len(context.data)} 筆記錄")
        print(f"輸出路徑: {context.get_variable('export_output_path')}")
    else:
        print(f"處理失敗: {result.error}")

asyncio.run(run_spx_po())
```

### 5.2 單獨使用 SPXConditionEngine

```python
import pandas as pd
from accrual_bot.tasks.spx.steps.spx_condition_engine import SPXConditionEngine

# 載入引擎（讀取 spx_status_stage1_rules 配置）
engine = SPXConditionEngine('spx_status_stage1_rules')

# 準備資料
df = pd.DataFrame({
    'PO狀態': [None, '已關單', None, None],
    'PO Supplier': ['台積電', 'BAO供應商', '其他', '智取 IT'],
    'GL#': ['622101', '199999', '651001', '199999'],
    'Item Description': ['一般採購', '押金', '辦公用品', '保證金退還'],
    'Expected Received Month_轉換格式': [202501, 202412, 202501, 202501]
})

# 執行規則
engine_context = {
    'processing_date': 202501,
    'prebuilt_masks': {},
}

df_result, stats = engine.apply_rules(
    df, 'PO狀態', engine_context,
    processing_type='PO'
)

print("狀態分布:", df_result['PO狀態'].value_counts().to_dict())
print("規則命中統計:", stats)
```

### 5.3 PPE_DESC 純函式直接呼叫

```python
from accrual_bot.tasks.spx.steps.spx_ppe_desc import (
    extract_clean_description,
    extract_address_from_dataframe,
    _process_description
)
import pandas as pd

# 單一描述清洗
desc = "2024/12 SVP_SPX 門市裝修工程-松山(南京東路五段)SPX store decoration 第一期款項 #SP-C-Leasehold"
cleaned = extract_clean_description(desc)
print(cleaned)  # SPX_門市裝修工程-松山(南京東路五段)_第一期款項

# 批次處理 DataFrame
df = pd.DataFrame({
    'Item Description': [
        "2024/12 SVP_SPX 辦公室翻新工程(信義路五段7號)",
        "2025/01 SPX payment machine 繳費機採購 #SP-XX-001",
        "門市智取櫃工程SPX locker XA 第一期款項 #SP-C-001"
    ]
})

df_processed = _process_description(df)
print(df_processed[['Item Description', 'New_Extracted_Result', 'locker_type', 'extracted_address']])
```

### 5.4 查詢啟用步驟

```python
from accrual_bot.tasks.spx import SPXPipelineOrchestrator

orchestrator = SPXPipelineOrchestrator()

# 查詢 PO Pipeline 的啟用步驟
po_steps = orchestrator.get_enabled_steps('PO')
print("PO Steps:", po_steps)

# 查詢 PR Pipeline 的啟用步驟
pr_steps = orchestrator.get_enabled_steps('PR')
print("PR Steps:", pr_steps)

# PPE_DESC 的步驟是硬編碼的
ppe_desc_steps = orchestrator.get_enabled_steps('PPE_DESC')
print("PPE_DESC Steps:", ppe_desc_steps)
```

### 5.5 AccountingOPSValidationStep 單獨使用

```python
# 此步驟可用於 OPS 團隊定期比對底稿
from accrual_bot.tasks.spx.steps import AccountingOPSValidationStep, AccountingOPSDataLoadingStep

async def run_accounting_ops_validation():
    from accrual_bot.core.pipeline.context import ProcessingContext

    context = ProcessingContext(entity_type='SPX', processing_date=202501)

    # 載入資料
    loading_step = AccountingOPSDataLoadingStep(
        name="Load",
        file_paths={
            'accounting_workpaper': {
                'path': 'output/SPX_PO_202412_processed.xlsx',
                'params': {'sheet_name': 'PO', 'header': 1}
            },
            'ops_validation': {
                'path': 'resources/202501/OPS驗收/ops_validation_202501.xlsx',
                'params': {'sheet_name': '智取櫃驗收明細', 'header': 3}
            }
        }
    )
    await loading_step.execute(context)

    # 比對
    validation_step = AccountingOPSValidationStep(name="Validate")
    result = await validation_step.execute(context)

    report = result.metadata
    print(f"比對結果: {report['matched_count']} 筆吻合, {report['mismatched_count']} 筆差異")
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ 配置驅動引擎大幅降低維護成本

`SPXConditionEngine` 是本模組最成功的設計。業務規則（「什麼條件 → 什麼狀態」）被外化到 TOML，讓業務人員或財務 IT 無需改程式碼就能調整判斷邏輯：

- 新增狀態條件：在 TOML 新增一筆 `[[conditions]]` 即可
- 調整優先順序：修改 `priority` 欄位即可
- 切換 and/or：修改 `combine` 欄位即可

#### ✅ prebuilt_masks 機制避免重複計算

`SPXERMLogicStep` 預先計算所有 boolean masks，傳給引擎使用，避免引擎為每條規則重複計算相同條件（如多條規則都需要 `erm_le_date`）。

#### ✅ PPE_DESC 純函式設計易於測試

`spx_ppe_desc.py` 的模組級函式（`extract_clean_description()` 等）可以在不建立 Pipeline context 的情況下直接單元測試，測試覆蓋率高（見 `tests/unit/tasks/spx/test_spx_ppe_steps.py`）。

#### ✅ 統一的步驟結構

所有步驟都遵循一致的模式：
- `execute()` 返回 `StepResult`
- 例外時記錄 log 並返回 `FAILED` 而非拋出例外
- 使用 `StepMetadataBuilder` 建立標準化 metadata
- `validate_input()` 與 `execute()` 分離關注點

#### ✅ file_paths 的向後兼容設計

`SPXDataLoadingStep._normalize_file_paths()` 自動處理舊格式（字串）和新格式（dict with params），降低了調用方的遷移成本：

```python
# 舊格式（仍支援）
file_paths = {'raw_po': 'path/to/file.csv'}

# 新格式
file_paths = {'raw_po': {'path': '...', 'params': {'encoding': 'utf-8'}}}
```

#### ✅ 7階段步驟結構清晰

`DepositStatusUpdateStep` 以「階段」概念組織 `execute()`：

```python
# === 階段 1: 數據驗證 ===
# === 階段 2: 篩選訂金相關記錄 ===
# === 階段 3: 按 PO# 分組並找出最大月份 ===
# === 階段 4: 判斷並更新狀態 ===
# === 階段 5: 生成詳細統計 ===
# === 階段 6: 記錄詳細日誌 ===
# === 階段 7: 更新上下文 ===
```

這種風格讓程式碼的執行意圖一目了然。

---

### 6.2 缺點與風險

#### ~~❌ `spx_steps.py` 中有 6 個孤立步驟~~ ✅ 已修復（2026-03-17）

`SPXDepositCheckStep`、`SPXClosingListIntegrationStep`、`SPXRentProcessingStep`、`SPXAssetValidationStep`、`SPXComplexStatusStep`、`SPXPPEProcessingStep` 都在 `__all__` 中，但**不在 Orchestrator 的 step registry 中**：

```python
# pipeline_orchestrator.py 的 step_registry 中找不到這些 class
step_registry = {
    'SPXDataLoading': ...,
    'StatusStage1': ...,    # StatusStage1Step（不是 SPXComplexStatusStep）
    ...
    # SPXDepositCheckStep, SPXAssetValidationStep 等六個都不在此
}
```

這些步驟有功能重疊（如 `SPXComplexStatusStep` 與 `StatusStage1Step` + `SPXERMLogicStep` 功能重疊），容易混淆新進工程師。應加以文件標注或移除。

**修復**：採方案 A（標記廢棄）——6 個步驟從 `__init__.py` 的 `__all__` 中移除；每個 class docstring 加入廢棄標注，說明對應的現行步驟，程式碼本身保留供業務邏輯參考。

#### ~~❌ `ClosingListIntegrationStep` 中有硬編碼的 Google Sheets ID~~ ✅ 已修復（2026-03-17）

```python
spreadsheet_id = '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE'  # 硬編碼

queries = [
    (spreadsheet_id, '2023年_done', 'A:J'),
    (spreadsheet_id, '2024年', 'A:J'),
    (spreadsheet_id, '2025年', 'A:J')    # 當年度需手動更新
]
```

**問題**：
1. 跨年時（如到 2026 年）需要手動在程式碼中加新 sheet
2. Spreadsheet ID 應在配置中管理，不應硬編碼

**修復**：Spreadsheet ID、sheet 名稱清單、cell range 移至 `stagging_spx.toml`（`closing_list_spreadsheet_id`、`closing_list_sheet_names`、`closing_list_sheet_range`）；程式碼改用 `config_manager.get_list()` 讀取，跨年只需在 TOML 追加一個 sheet 名稱。

#### ❌ Row-by-row 迴圈（`_apply_locker_validation()`）的效能問題

```python
for idx, row in df.iterrows():   # O(n) Python 迴圈，無法利用 pandas 向量化
    ...
    for ctype in priority_order:
        if re.search(patterns[ctype], item_desc):
            ...
```

對大型 DataFrame（如數千筆 locker PO），`iterrows()` + `re.search()` 的組合效率極差。可以用 `df['Item Description'].str.extract()` 或向量化 regex 替代。

#### ~~❌ PPE 和 PO/PR pipeline 一致性缺失~~ ✅ 已修復（2026-03-17）

PO/PR pipeline 是**配置驅動**的（讀 `enabled_po_steps`），但 PPE 和 PPE_DESC pipeline 是**硬編碼**的：

```python
def build_ppe_pipeline(self, ...):
    # 直接 pipeline.add_step(PPEDataLoadingStep(...))
    # 沒有讀取任何配置

def build_po_pipeline(self, ...):
    enabled_steps = self.config.get('enabled_po_steps', [...])  # 配置驅動
```

若要禁用 PPE Pipeline 的某個步驟（如臨時跳過 `PPEDataMerge`），目前需要修改程式碼，無法透過配置控制。

**修復**：`stagging_spx.toml` 加入 `enabled_ppe_steps` 和 `enabled_ppe_desc_steps`；`_create_step()` 新增 PPE/PPE_DESC 步驟的 factory entries（處理有特殊建構參數的步驟如 `PPEDataMergeStep(merge_keys=...)`）；`build_ppe_pipeline()` 和 `build_ppe_desc_pipeline()` 改為讀取配置動態組裝，`get_enabled_steps('PPE_DESC')` 也改為讀 config 而非 hardcoded list。

#### ❌ `SPXExportStep` 直接硬寫 auxiliary data 的 key 名稱

```python
async def execute(self, context):
    with pd.ExcelWriter(output_path) as writer:
        df_export.to_excel(writer, sheet_name='PO', index=False)
        context.get_auxiliary_data('locker_non_discount').to_excel(...)  # 硬編碼 key
        context.get_auxiliary_data('locker_discount').to_excel(...)
        context.get_auxiliary_data('kiosk_data').to_excel(...)
```

若驗收步驟被跳過（`ValidationDataProcessingStep` 返回 SKIPPED），這三個 `get_auxiliary_data()` 會返回 `None`，導致 `.to_excel()` 拋出 AttributeError。相比之下 `AccountingOPSExportingStep` 有適當的 None 檢查，設計更健壯。

#### ~~❌ `ColumnAdditionStep` 依賴 context variable 偵測 PO/PR~~ ✅ 已修復（2026-03-17）

```python
if 'raw_pr' in context.get_variable('file_paths').keys():
    df = df.rename(columns={'PO狀態': 'PR狀態'})
```

這種「透過 file_paths 的 key 名稱偵測業務類型」的方式是一種隱式耦合。如果未來呼叫方的 key 名稱改變，此步驟會靜默地失效（不重命名欄位），導致下游步驟讀取錯誤的欄位名。

**修復**：改用 `context.metadata.processing_type == 'PR'`（同一方法的第 59 行已使用此 API），消除對 `file_paths` key 名稱的隱式依賴。一行修改。

#### ~~❌ `spx_evaluation_2.py` 的模組說明與位置不一致~~ ✅ 已修復（2026-03-17）

```python
# 模組 docstring 中寫的是錯誤的路徑：
# 文件位置: accrual_bot/core/pipeline/steps/spx_evaluation.py
# 實際路徑: accrual_bot/tasks/spx/steps/spx_evaluation_2.py
```

此外，`DepositStatusUpdateStep` 的預設名稱是 `"Update_Deposit_PO_Status"`，與其他步驟的命名慣例（`DepositStatusUpdate`）不一致。

**修復**：模組 docstring 路徑修正為實際路徑；`DepositStatusUpdateStep.__init__()` 的 `name` 預設值從 `"Update_Deposit_PO_Status"` 改為 `"DepositStatusUpdate"`（與 orchestrator registry key 一致）；對應測試斷言同步更新。

#### ~~❌ `PPEDataLoadingStep._load_renewal_list()` 混用同步與非同步~~ ✅ 已修復（2026-03-17）

```python
async def _load_renewal_list(self, context) -> pd.DataFrame:
    sheets_importer = GoogleSheetsImporter(credentials_config)  # 同步初始化
    df = sheets_importer.get_sheet_data(...)  # 同步呼叫，在 async 函式中阻塞 event loop
```

在 `async def` 中直接呼叫同步的 `get_sheet_data()`，如果該函式耗時（如 Google Sheets API 網路請求），會阻塞整個 asyncio event loop，可能導致並發性能下降。

**修復**：三個 `get_sheet_data()` 呼叫改為 `await asyncio.to_thread(sheets_importer.get_sheet_data, ...)`。`GoogleSheetsImporter.get_sheet_data()` 無 async 版本，`asyncio.to_thread()` 是最小侵入性的解法（Python 3.9+），將同步 I/O 推至執行緒池，event loop 在網路等待期間可繼續排程其他 coroutine。

---

## 7. 延伸議題

### 7.1 `spx_steps.py` 的定位與處理建議（✅ 已依方案 A 修復，2026-03-17）

本檔案包含 6 個較早期寫的步驟（`SPXDepositCheckStep` 等），其設計模式（使用 `context.get_entity_config()`、`context.get_id_column()` 等非標準 context API）與現有架構存在差異。

**建議處理方式**：

```
方案 A（推薦，已採用）：標記廢棄
  - 在每個 class 上加 @deprecated 裝飾器或 docstring 說明
  - 從 __init__.py 的 __all__ 中移除
  - 計畫在下個版本刪除

方案 B：整合主流
  - 若有業務需要，重寫以符合現有設計（使用 SPXConditionEngine + context.data/get_variable）
  - 加入 Orchestrator 的 step_registry

方案 C：移至 archive 資料夾
  - 保留程式碼參考價值，但不再公開導出
```

### 7.2 `ClosingListIntegrationStep` 的配置化改造（✅ 已修復，2026-03-17）

將硬編碼的 Spreadsheet ID 和 Sheet 名稱遷入 `stagging_spx.toml`：

```toml
[spx.closing_list]
spreadsheet_id = "1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE"
sheets = [
    {name = "2023年_done", range = "A:J"},
    {name = "2024年",      range = "A:J"},
    {name = "2025年",      range = "A:J"},
]
```

每年只需更新 TOML，不需改程式碼。

### 7.3 Locker 驗收比對效能改進

將 `_apply_locker_validation()` 的 `iterrows()` 改為向量化操作：

```python
# 目前（O(n×m) Python 迴圈）
for idx, row in df.iterrows():
    for ctype in priority_order:
        if re.search(patterns[ctype], item_desc):
            cabinet_type = ctype; break

# 改善方案（向量化）
for ctype in priority_order:  # 依優先序處理，較少型別通常排前面
    mask = df['Item Description'].str.contains(patterns[ctype], regex=True, na=False)
    still_unmatched = df['cabinet_type'].isna()
    df.loc[mask & still_unmatched, 'cabinet_type'] = ctype
```

### 7.4 PPE Pipeline 配置化（✅ 已修復，2026-03-17）

使 PPE Pipeline 也支援配置驅動的步驟控制，與 PO/PR 保持一致：

```toml
# stagging_spx.toml
[pipeline.spx]
enabled_ppe_steps = ["PPEDataLoading", "PPEDataCleaning", "PPEDataMerge",
                     "PPEContractDateUpdate", "PPEMonthDifference"]
enabled_ppe_desc_steps = ["PPEDescDataLoading", "DescriptionExtraction",
                           "ContractPeriodMapping", "PPEDescExport"]
```

### 7.5 `SPXExportStep` 的空值安全加固

```python
# 目前（有 NoneType AttributeError 風險）
context.get_auxiliary_data('locker_non_discount').to_excel(...)

# 建議改為
locker_non_discount = context.get_auxiliary_data('locker_non_discount')
if locker_non_discount is not None and not locker_non_discount.empty:
    locker_non_discount.to_excel(writer, sheet_name='locker_non_discount')
```

### 7.6 多年 PPE 合約的邊界情況

`PPEMonthDifferenceStep._calculate_month_difference()` 的計算在跨年時有邊界情況：

```python
# 當 contract_end 為 NaT 時
df_result['months_diff'] = df_result[date_column].apply(
    lambda x: months_difference(x, target_date)  # x 可能是 NaT
)
```

若某筆記錄的 `contract_end_day_renewal` 無法被 `pd.to_datetime()` 解析（返回 NaT），`months_difference(NaT, target)` 會拋出 TypeError。建議加入：

```python
lambda x: months_difference(x, target_date) if pd.notna(x) else None
```

### 7.7 `context.set_auxiliary_data()` vs `add_auxiliary_data()` 一致性

`DescriptionExtractionStep` 使用了 `context.set_auxiliary_data('pr_data', df_pr)`，而其他多數步驟使用 `context.add_auxiliary_data()`。這兩個方法行為可能不同（`set` 覆蓋，`add` 可能拋錯如果 key 已存在），需要確認語義並統一使用。

### 7.8 Google Sheets 依賴的可測試性問題

`PPEDataLoadingStep._load_renewal_list()` 和 `ClosingListIntegrationStep._get_closing_note()` 都直接實例化 `GoogleSheetsImporter`，導致無法在無網路的 CI 環境下單元測試。

建議使用依賴注入（Dependency Injection）：

```python
class ClosingListIntegrationStep(PipelineStep):
    def __init__(self, ..., sheets_importer=None):
        # 允許在測試中注入 mock
        self.sheets_importer = sheets_importer or GoogleSheetsImporter
```

---

## 8. 其他

### 8.1 程式碼量統計

| 檔案 | 行數 | Classes | Functions/Methods |
|------|------|---------|-------------------|
| `pipeline_orchestrator.py` | ~427 | 1 | 7 |
| `spx_condition_engine.py` | ~551 | 1 | 13 |
| `spx_evaluation.py` | ~1300 | 5 | ~35 |
| `spx_evaluation_2.py` | ~415 | 1 | 9 |
| `spx_pr_evaluation.py` | ~592 | 1 | 10 |
| `spx_integration.py` | ~1600 | 7 | ~40 |
| `spx_loading.py` | ~1300 | 3 | ~20 |
| `spx_exporting.py` | ~500 | 3 | ~15 |
| `spx_steps.py` | ~690 | 6 | ~18 |
| `spx_ppe_desc.py` | ~659 | 4 + 6純函式 | ~20 |
| `spx_ppe_qty_validation.py` | ~946 | 1 | ~15 |
| **合計** | **~8,183** | **33** | **~202** |

### 8.2 公開步驟分類

`steps/__init__.py` 的 32 個公開符號分類如下：

| 分類 | 步驟 |
|------|------|
| **資料載入** | `SPXDataLoadingStep`, `SPXPRDataLoadingStep`, `PPEDataLoadingStep`, `AccountingOPSDataLoadingStep`, `PPEDescDataLoadingStep` |
| **欄位整合** | `ColumnAdditionStep`, `APInvoiceIntegrationStep`, `ClosingListIntegrationStep`, `ValidationDataProcessingStep`, `DataReformattingStep`, `PRDataReformattingStep`, `PPEDataCleaningStep`, `PPEDataMergeStep` |
| **狀態評估** | `StatusStage1Step`, `ERMConditions`, `SPXERMLogicStep`, `SPXPRERMLogicStep`, `DepositStatusUpdateStep` |
| **PPE 計算** | `PPEContractDateUpdateStep`, `PPEMonthDifferenceStep`, `DescriptionExtractionStep`, `ContractPeriodMappingStep` |
| **匯出** | `SPXExportStep`, `SPXPRExportStep`, `AccountingOPSExportingStep`, `PPEDescExportStep` |
| **驗證** | `AccountingOPSValidationStep` |
| **歷史步驟** | `SPXDepositCheckStep`, `SPXClosingListIntegrationStep`, `SPXRentProcessingStep`, `SPXAssetValidationStep`, `SPXComplexStatusStep`, `SPXPPEProcessingStep` |

### 8.3 相關測試檔案

> **2026-03-28 更新**：Phase 15 新增 4 個測試檔案，覆蓋 integration、exporting、pr_evaluation、loading execute 層。

| 測試檔案 | 覆蓋範圍 |
|---------|---------|
| `tests/unit/tasks/spx/test_spx_orchestrator.py` | `SPXPipelineOrchestrator` 的 Pipeline 建構邏輯 |
| `tests/unit/tasks/spx/test_spx_loading.py` | `SPXDataLoadingStep` 的輸入驗證與檔案格式處理 |
| `tests/unit/tasks/spx/test_spx_loading_execute.py` | `SPXDataLoadingStep`、`PPEDataLoadingStep`、`SPXPRDataLoadingStep` 的 execute 層（37 tests） |
| `tests/unit/tasks/spx/test_spx_condition_engine.py` | `SPXConditionEngine` 的各類 check type（含 11 種未測 check type 補充） |
| `tests/unit/tasks/spx/test_spx_evaluation.py` | `StatusStage1Step`、`SPXERMLogicStep` 的狀態判斷邏輯 |
| `tests/unit/tasks/spx/test_spx_ppe_steps.py` | PPE_DESC 純函式 regex + PPE qty validation（擴充） |
| `tests/unit/tasks/spx/test_spx_integration.py` | `ColumnAdditionStep`、`APInvoiceIntegrationStep`、`ClosingListIntegrationStep`、`ValidationDataProcessingStep` 等 8 個整合步驟（35 tests） |
| `tests/unit/tasks/spx/test_spx_exporting.py` | `SPXExportStep`、`AccountingOPSExportingStep`、`SPXPRExportStep`（25 tests） |
| `tests/unit/tasks/spx/test_spx_pr_evaluation.py` | `SPXPRERMLogicStep` PR 評估邏輯（30 tests） |

### 8.4 配置依賴清單

本模組依賴以下 `stagging_spx.toml` 配置 section：

| Section | 使用的步驟 |
|---------|-----------|
| `[pipeline.spx]` | `SPXPipelineOrchestrator` |
| `[spx]` | `SPXERMLogicStep`, `ColumnAdditionStep`, `ValidationDataProcessingStep` |
| `[spx_column_defaults]` | `SPXERMLogicStep`, `SPXPRERMLogicStep`, `ValidationDataProcessingStep` |
| `[spx_status_stage1_rules]` | `StatusStage1Step`（via `SPXConditionEngine`） |
| `[spx_erm_status_rules]` | `SPXERMLogicStep`（via `SPXConditionEngine`） |
| `[spx_pr_erm_status_rules]` | `SPXPRERMLogicStep`（via `SPXConditionEngine`） |

以及以下 `stagging.toml` 配置 section：

| Section | 使用的步驟 |
|---------|-----------|
| `[fa_accounts]` | `SPXERMLogicStep`, `ValidationDataProcessingStep` |
| `[data_shape_summary]` | `SPXDataLoadingStep`（決定是否儲存 snapshot） |

---

*文件結束*
