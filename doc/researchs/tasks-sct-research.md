# `accrual_bot/tasks/sct` 深度技術研究文件

> 作者：Claude Code 自動研究報告
> 研究範圍：`accrual_bot/tasks/sct/` 全部 10 個 Python 原始碼檔案
> 研究日期：2026-03-27

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

SCT（Shopee Commerce Taiwan）是 accrual_bot 系統所支援的三大業務實體之一（另外兩個是 SPT 和 SPX）。其應付帳款估計（Accrual）流程涵蓋兩種作業類型：

| 類型 | 說明 | 資料來源 |
|------|------|---------|
| **PO** | 採購訂單估計（主流程） | HRIS 系統匯出的 xlsx |
| **PR** | 請購單估計（輔助流程） | HRIS 系統匯出的 xlsx |

SCT 業務特有的複雜性包括：
- **PPE（固定資產）** 項目的多期款項追蹤（訂金 → 安裝款 → 驗收款 → 保固），以雙重確認（ERM 日期 + 採購備註）判定完成狀態
- **物流供應商（Logistics Supplier）** 的 AR 估計識別，針對特定供應商的物流運費單獨處理
- **押金/保證金** 等特殊摘要關鍵字的識別與分類
- **會計科目預測**：18 條配置驅動規則，根據產品代碼、部門、摘要關鍵字、金額範圍自動預測 GL 科目
- **原始資料格式為 xlsx**（有別於 SPT/SPX 使用 CSV），參考資料來源為 `ref_SCTTW.xlsx`

### 1.2 模組位置與歷史

`tasks/sct/` 建立於 2026-03（Phase 12），是系統的第三個業務實體模組。初期僅實作至 `ProcurementIntegration` 步驟，後續在同一 sprint 內完成了完整的核心邏輯：

| 階段 | 內容 | 關鍵 Commit |
|------|------|------------|
| Phase 12a | Data Loading、Column Addition、前期整合 | `210d867` |
| Phase 12b | 條件引擎重構至 core、ERM 邏輯步驟 | `c043f7a` |
| Phase 12c | 採購備註覆蓋、PPE 資產狀態、遮罩修復 | `805a3cb` |
| Phase 12d | PPE 已完成雙重確認（ERM + 採購備註 AND） | `b051250` |
| Phase 12e | PR ERM 條件優先序更新 | `f986ad4` |
| Phase 12f | 會計科目預測步驟 | `d42c279` |
| Phase 12g | 資料格式化步驟 | `475e2d4` |

### 1.3 檔案清單

```
accrual_bot/tasks/sct/
├── __init__.py                    (8 行)   - 模組初始化，導出 SCTPipelineOrchestrator
├── pipeline_orchestrator.py       (256 行) - Pipeline 組裝與步驟工廠
└── steps/
    ├── __init__.py                (26 行)  - 子模組初始化，10 個符號
    ├── sct_loading.py             (216 行) - PO/PR 數據載入（xlsx）
    ├── sct_column_addition.py     (253 行) - SCT 專屬欄位添加
    ├── sct_integration.py         (139 行) - AP Invoice VOUCHER_NUMBER 整合
    ├── sct_evaluation.py          (541 行) - PO ERM 狀態評估（18 條規則）
    ├── sct_pr_evaluation.py       (377 行) - PR ERM 狀態評估（13 條規則）
    ├── sct_asset_status.py        (395 行) - PPE 資產狀態更新
    ├── sct_account_prediction.py  (338 行) - 會計科目預測（18 條規則）
    └── sct_post_processing.py     (373 行) - 資料格式化與輸出篩選
```

**合計**：2,921 行程式碼（含 `__init__.py`）

---

## 2. 用途

### 2.1 整體業務目標

SCT 模組的核心任務是**自動化台灣業務的月度應計費用計算**，具體包括：

1. **資料載入**：從 xlsx 原始數據與參考資料（`ref_SCTTW.xlsx`）載入
2. **欄位初始化**：添加結案判斷、差異數量、FA/S&M 標記等計算欄位
3. **輔助資料整合**：整合 AP Invoice（提取 VOUCHER_NUMBER）、前期底稿、採購備註
4. **日期邏輯**：解析 Item Description 中的日期範圍，建立 ERM 轉換格式
5. **ERM 狀態評估**：根據 18 條（PO）或 13 條（PR）配置驅動規則，判定每筆記錄的估計狀態
6. **PPE 資產狀態**：對含固定資產項目的 PO 進行驗收款追蹤與狀態更新
7. **科目預測**：根據業務規則自動預測 GL 會計科目
8. **資料格式化**：數值/日期格式化、NaN 清理、欄位排序、臨時欄位移除、輸出欄位篩選

### 2.2 Pipeline 流程概覽

#### PO Pipeline（10 步驟）

```
SCTDataLoading              ← xlsx 原始 PO 數據 + ref_SCTTW.xlsx
    ↓
SCTColumnAddition           ← 添加結案/差異/FA/S&M/備註欄位
    ↓
APInvoiceIntegration        ← 提取 VOUCHER_NUMBER（AP Invoice xlsx）
    ↓
PreviousWorkpaperIntegration ← 整合前期底稿備註與狀態
    ↓
ProcurementIntegration      ← 整合採購備註
    ↓
DateLogic                   ← 解析 Item Description 日期範圍
    ↓
SCTERMLogic                 ← 18 條規則判定 PO 狀態 + 會計欄位
    ↓
SCTAssetStatusUpdate        ← PPE 資產驗收狀態（雙重確認）
    ↓
SCTAccountPrediction        ← 18 條規則預測 GL 科目
    ↓
SCTPostProcessing           ← 格式化 → 輸出（62 欄位）
```

#### PR Pipeline（8 步驟）

```
SCTPRDataLoading            ← xlsx 原始 PR 數據 + ref_SCTTW.xlsx
    ↓
SCTColumnAddition           ← 添加欄位（PR 模式：PO狀態 → PR狀態）
    ↓
PreviousWorkpaperIntegration ← 整合前期底稿
    ↓
ProcurementIntegration      ← 整合採購備註
    ↓
DateLogic                   ← 解析日期範圍
    ↓
SCTPRERMLogic               ← 13 條規則判定 PR 狀態 + 會計欄位
    ↓
SCTAccountPrediction        ← 科目預測
    ↓
SCTPostProcessing           ← 格式化 → 輸出（57 欄位）
```

**PO vs PR 關鍵差異**：
- PR 無 `APInvoiceIntegration`（無發票對帳）
- PR 無 `SCTAssetStatusUpdate`（無收貨/PPE 追蹤）
- PR ERM 規則較簡化：無收貨數量、無入帳金額、Accr. Amount = Entry Amount 直接取值

### 2.3 資料流轉

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────┐
│ xlsx 原始資料 │────→│ context.data     │────→│ 格式化輸出 DataFrame │
│ (PO/PR)      │     │ (主 DataFrame)   │     │ (PO: 62 cols)     │
└─────────────┘     └─────────────────┘     │ (PR: 57 cols)     │
                           ↑                  └──────────────────┘
┌─────────────┐     ┌─────────────────┐
│ ref_SCTTW   │────→│ auxiliary_data:   │
│ AP Invoice  │     │ reference_account │
│ 前期底稿    │     │ reference_liability│
│ 採購備註    │     │ ap_invoice        │
└─────────────┘     │ previous_po/pr   │
                     │ procurement_po/pr│
                     │ result_with_temp │
                     └─────────────────┘
```

---

## 3. 設計思路

### 3.1 四層架構定位

```
UI Layer          → UnifiedPipelineService._get_orchestrator('SCT')
Tasks Layer       → SCTPipelineOrchestrator + steps/   ← 本模組
Core Layer        → BaseLoadingStep, ConditionEngine, BasePostProcessingStep
Utils Layer       → config_manager, get_logger, clean_po_data
```

SCT 模組位於 **Tasks 層**，負責封裝業務邏輯。核心框架（Pipeline、ProcessingContext、ConditionEngine）由 Core 層提供。

### 3.2 配置驅動原則

SCT 模組將所有可變業務規則外移至 `config/stagging_sct.toml`（894 行），包含：

| 配置區段 | 用途 | 消費者 |
|----------|------|--------|
| `[pipeline.sct]` | 啟用步驟清單 | `SCTPipelineOrchestrator` |
| `[sct_column_defaults]` | SM 科目、Region、Department 預設值 | `SCTColumnAdditionStep` |
| `[sct.ppe]` | PPE 觸發關鍵字、完成/未完成標籤 | `SCTAssetStatusUpdateStep` |
| `[sct]` | AP 欄位、物流供應商、押金關鍵字 | Loading + ERM |
| `[sct_erm_status_rules]` | 18 條 PO ERM 狀態規則 | `SCTERMLogicStep` via `ConditionEngine` |
| `[sct_pr_erm_status_rules]` | 13 條 PR ERM 狀態規則 | `SCTPRERMLogicStep` via `ConditionEngine` |
| `[sct_account_prediction]` | 18 條科目預測規則 | `SCTAccountPredictionStep` |
| `[sct_reformatting]` | 數值/日期/臨時/輸出欄位清單 | `SCTPostProcessingStep` |

**好處**：新增/修改業務規則時只需編輯 TOML，無需改動程式碼。

### 3.3 Step Registry 模式

`SCTPipelineOrchestrator._create_step()` 使用 lambda 工廠字典，根據 TOML 中的步驟名稱動態實例化：

```python
step_registry = {
    'SCTDataLoading':    lambda: SCTDataLoadingStep(name="SCTDataLoading", file_paths=file_paths),
    'SCTERMLogic':       lambda: SCTERMLogicStep(name="SCTERMLogic", required=True),
    'SCTPostProcessing': lambda: SCTPostProcessingStep(name="SCTPostProcessing", required=True),
    # ... 共 12 個步驟
}
```

此模式與 SPT/SPX 一致，確保跨實體的 Orchestrator 介面統一。

### 3.4 跨實體元件複用策略

SCT 複用了多個 Core 層和 SPX 層的共用步驟：

| 步驟 | 來源 | SCT 適配方式 |
|------|------|-------------|
| `PreviousWorkpaperIntegrationStep` | `core/pipeline/steps/common.py` | 直接複用 |
| `ProcurementIntegrationStep` | `core/pipeline/steps/common.py` | 直接複用 |
| `DateLogicStep` | `core/pipeline/steps/common.py` | 直接複用 |
| `BaseLoadingStep` | `core/pipeline/steps/base_loading.py` | 繼承模板方法 |
| `BasePostProcessingStep` | `core/pipeline/steps/post_processing.py` | 繼承模板方法 |
| `ConditionEngine` | `core/pipeline/engines/` | 傳入 SCT 專屬規則鍵 |

**SCT 獨有步驟**：`SCTColumnAdditionStep`、`APInvoiceIntegrationStep`（SCT 版）、`SCTERMLogicStep`、`SCTPRERMLogicStep`、`SCTAssetStatusUpdateStep`、`SCTAccountPredictionStep`、`SCTPostProcessingStep`

### 3.5 模板方法模式

SCT 使用了兩個關鍵的模板方法基類：

**BaseLoadingStep**（資料載入）：
```
SCTBaseDataLoadingStep(BaseLoadingStep)
├── get_required_file_type()    → 'raw_po' 或 'raw_pr'（子類實作）
├── _load_primary_file()        → 載入 xlsx + 日期提取（SCT 實作）
├── _extract_primary_data()     → 驗證必要欄位（SCT 實作）
├── _load_reference_data()      → ref_SCTTW.xlsx（SCT 實作）
├── _get_custom_file_loader()   → AP Invoice 自訂載入器（SCT 實作）
└── _set_additional_context_variables() → raw_data_snapshot（SCT 實作）
    ├── SCTDataLoadingStep      → get_required_file_type() = 'raw_po'
    └── SCTPRDataLoadingStep    → get_required_file_type() = 'raw_pr'
```

**BasePostProcessingStep**（資料格式化）：
```
SCTPostProcessingStep(BasePostProcessingStep)
├── _process_data()    → 10 階段格式化流水線（SCT 實作）
└── _validate_result() → 檢查數值型態 + 臨時欄位（SCT 實作）
```

---

## 4. 各項知識點

### 4.1 模組初始化與公開 API

**`tasks/sct/__init__.py`**：
```python
from accrual_bot.tasks.sct.pipeline_orchestrator import SCTPipelineOrchestrator
__all__ = ["SCTPipelineOrchestrator"]
```

**`tasks/sct/steps/__init__.py`**（10 個導出符號）：
```python
__all__ = [
    'SCTDataLoadingStep', 'SCTPRDataLoadingStep',
    'SCTColumnAdditionStep',
    'SCTERMLogicStep', 'SCTERMConditions',
    'SCTPRERMLogicStep',
    'SCTAssetStatusUpdateStep',
    'SCTAccountPredictionStep',
    'SCTPostProcessingStep',
    'APInvoiceIntegrationStep',
]
```

### 4.2 SCTPipelineOrchestrator 詳解

**檔案**：`pipeline_orchestrator.py`（256 行）

#### 初始化

```python
def __init__(self):
    self.config = config_manager._config_toml.get('pipeline', {}).get('sct', {})
    self.entity_type = 'SCT'
    self.logger = get_logger(__name__)
```

#### 公開方法

| 方法 | 參數 | 回傳 | 說明 |
|------|------|------|------|
| `build_po_pipeline()` | `file_paths, custom_steps=None` | `Pipeline` | 建構 PO Pipeline（10 步驟） |
| `build_pr_pipeline()` | `file_paths, custom_steps=None` | `Pipeline` | 建構 PR Pipeline（8 步驟） |
| `get_enabled_steps()` | `processing_type, source_type=None` | `List[str]` | 查詢配置中的啟用步驟 |

#### 步驟工廠（_create_step）

完整 12 個注册項：

| 步驟名稱 | 步驟類別 | 來源 | 備註 |
|----------|---------|------|------|
| `SCTDataLoading` | `SCTDataLoadingStep` | sct/steps | PO 載入 |
| `SCTPRDataLoading` | `SCTPRDataLoadingStep` | sct/steps | PR 載入 |
| `SCTColumnAddition` | `SCTColumnAdditionStep` | sct/steps | 欄位添加 |
| `APInvoiceIntegration` | `APInvoiceIntegrationStep` | sct/steps | VOUCHER 提取 |
| `PreviousWorkpaperIntegration` | `PreviousWorkpaperIntegrationStep` | core/steps | 前期底稿 |
| `ProcurementIntegration` | `ProcurementIntegrationStep` | core/steps | 採購備註 |
| `DateLogic` | `DateLogicStep` | core/steps | 日期解析 |
| `SCTERMLogic` | `SCTERMLogicStep` | sct/steps | PO ERM |
| `SCTPRERMLogic` | `SCTPRERMLogicStep` | sct/steps | PR ERM |
| `SCTAssetStatusUpdate` | `SCTAssetStatusUpdateStep` | sct/steps | PPE 狀態 |
| `SCTAccountPrediction` | `SCTAccountPredictionStep` | sct/steps | 科目預測 |
| `SCTPostProcessing` | `SCTPostProcessingStep` | sct/steps | 格式化 |

#### 預設步驟（config 缺失時的 fallback）

- **PO 預設**：`SCTDataLoading → SCTColumnAddition → APInvoiceIntegration → PreviousWorkpaperIntegration → ProcurementIntegration`
- **PR 預設**：`SCTPRDataLoading → SCTColumnAddition → PreviousWorkpaperIntegration → ProcurementIntegration`

### 4.3 資料載入步驟群

**檔案**：`sct_loading.py`（216 行）

#### SCTBaseDataLoadingStep（基類）

繼承 `BaseLoadingStep`，實作 SCT 通用載入邏輯：

| 方法 | 說明 |
|------|------|
| `_load_primary_file()` | 透過 `source.read()` 載入 xlsx，呼叫 `_process_common_columns()` 正規化 Line#/GL#/Project，從檔名提取日期 |
| `_extract_primary_data()` | 驗證 tuple 格式、DataFrame 非空、必要欄位（Product Code, Item Description, GL#） |
| `_load_reference_data()` | 從 `ref_SCTTW.xlsx` 載入 `reference_account`（GL# → Account 對應）和 `reference_liability`（Account → Liability 對應），支援 Colab 環境 ZIP fallback |
| `_get_custom_file_loader()` | AP Invoice 使用自訂載入器（`header=1`, `sheet_name=1`, `usecols` 從 config 讀取） |
| `_set_additional_context_variables()` | 若 `data_shape_summary.enabled`，保存 `raw_data_snapshot` |

#### SCTDataLoadingStep / SCTPRDataLoadingStep

僅覆寫 `get_required_file_type()`：
- `SCTDataLoadingStep` → `'raw_po'`
- `SCTPRDataLoadingStep` → `'raw_pr'`

### 4.4 欄位添加步驟

**檔案**：`sct_column_addition.py`（253 行）

`SCTColumnAdditionStep(PipelineStep)` — 基於 SPX `ColumnAdditionStep` 但**移除「累計至本期驗收數量/金額」**。

#### 添加的欄位

**計算欄位**：
| 欄位 | 計算邏輯 |
|------|---------|
| `是否結案` | "未結案"（Closed For Invoice == '0'）或 "結案" |
| `結案是否有差異數量` | Entry Qty - Billed Qty（結案時）或 "未結案" |
| `Check with Entry Invoice` | Entry Amount - Entry Billed Amount（> 0 時）或 "未入帳" |
| `PR Line` | PR# + '-' + Line#（串接） |
| `PO Line` | PO# + '-' + Line#（串接） |

**備註欄位**（初始為 `pd.NA`）：
`Remarked by Procurement`、`Noted by Procurement`、`Remarked by FN`、`Noted by FN`、`Remarked by 上月 Procurement`、`Remarked by 上月 FN`、`PO狀態`

**PR 額外欄位**：`Remarked by Procurement PR`、`Noted by Procurement PR`、`Remarked by 上月 FN PR`

**會計初始欄位**（初始為 `pd.NA`）：
`是否估計入帳`、`是否為FA`、`是否為S&M`、`Account code`、`Account Name`、`Product code`、`Region_c`、`Dep.`、`Currency_c`、`Accr. Amount`、`Liability`、`是否有預付`、`PR Product Code Check`、`Question from Reviewer`、`Check by AP`

**FA/S&M 判定**：
- `是否為FA`：GL# ∈ `config[fa_accounts.sct]` → 'Y'，否則 ''
- `是否為S&M`：GL# ∈ `config[sct_column_defaults.sm_accounts]`（預設 650003/450014）→ 'Y'，否則 'N'

**PR 模式特殊處理**：`PO狀態` 重新命名為 `PR狀態`

### 4.5 AP Invoice 整合步驟

**檔案**：`sct_integration.py`（139 行）

`APInvoiceIntegrationStep(PipelineStep)` — 從 AP Invoice 提取 VOUCHER_NUMBER 對應至 PO 數據。

#### 處理流程

1. 從 `context.get_auxiliary_data('ap_invoice')` 取得 AP Invoice（若無則 SKIPPED）
2. 構建複合鍵 `po_line`：Company + '-' + PO Number + '-' + PO_LINE_NUMBER
3. 轉換 Period（`%b-%y` → `%Y%m`），篩選 period ≤ processing_date
4. 排序 + 去重（保留最新記錄）
5. Left join 回主 DataFrame，取得 `voucher_number`

### 4.6 ERM 邏輯評估步驟（PO）

**檔案**：`sct_evaluation.py`（541 行）

#### SCTERMConditions（Dataclass）

16 個布林遮罩欄位：

| 分類 | 欄位名稱 | 說明 |
|------|---------|------|
| **基本** | `no_status` | 記錄尚無狀態 |
| | `in_date_range` | ERM 在描述期間範圍內 |
| | `erm_before_or_equal_file_date` | ERM ≤ 檔案日期 |
| | `erm_after_file_date` | ERM > 檔案日期 |
| **數量** | `quantity_matched` | Entry Qty == Received Qty |
| | `not_billed` | Entry Billed Amount == 0 |
| | `has_billing` | Billed Qty != '0' |
| | `fully_billed` | Entry Amount - Entry Billed Amount == 0 |
| | `has_unpaid_amount` | Entry Amount - Entry Billed Amount != 0 |
| **備註** | `procurement_completed_or_rent` | 採購備註含 '已完成' 或 'rent' |
| | `fn_completed_or_posted` | 上月 FN 備註含 '已完成' 或 '已入帳' |
| | `pr_not_incomplete` | 上月 FN PR 備註不含 '未完成' |
| **FA** | `is_fa` | GL# ∈ FA 科目列表 |
| **錯誤** | `procurement_not_error` | 採購備註 != 'error' |
| | `out_of_date_range` | ERM 不在描述期間且非格式錯誤 |
| | `format_error` | YMs == '100001,100002'（格式解析失敗） |

#### SCTERMLogicStep 執行流程（7 階段）

| 階段 | 方法 | 說明 |
|------|------|------|
| 1 | `_set_file_date()` | 設定 `檔案日期` = processing_date |
| 2 | `_build_conditions()` | 構建 16 個布林遮罩 → `SCTERMConditions` |
| 3 | `_apply_status_conditions()` | 將遮罩映射為 `prebuilt_masks` 字典，呼叫 `ConditionEngine.apply_rules()` |
| 4 | `_handle_format_errors()` | `no_status & format_error` → '格式錯誤，退單' |
| 5 | `_set_accrual_flag()` | 狀態含 '已完成' → '是否估計入帳' = 'Y'，否則 'N' |
| 6 | `_set_accounting_fields()` | 設定 8 個會計欄位（Account code/Name、Product code、Region、Dep.、Currency、Accr. Amount、Liability/預付） |
| 7 | `_check_pr_product_code()` | 驗證 PR Product Code 與 Project 欄位一致性 |

#### 18 條 PO ERM 狀態規則

| 優先序 | 狀態值 | 核心條件 | 組合 |
|--------|--------|---------|------|
| 1 | 需求取消 | FN 或採購備註含 `刪\|關` | OR |
| 2 | 現狀結案 | FN 含 '已完成' AND 採購含 `現狀結案\|需求取消` | AND |
| 3 | Outright | Supplier 含 Outright | AND |
| 4 | Consignment | Supplier 含 Consignment | AND |
| 5 | Outsourcing | Supplier 含 Outsourcing | AND |
| 6 | 上期已入PPE | FN 含 '上期已入PPE' | AND |
| 7 | 上期FN備註已完成或Voucher number | FN 含 `已完成\|\d{8}` | AND |
| 8 | 已入帳 | voucher + no_status + erm_in_range + erm_le_date + qty_matched + has_billing + remark_completed + not_fa | AND |
| 9 | 已入帳 | voucher + no_status + erm_in_range + erm_le_date + qty_matched + not_fa（放寬 billing） | AND |
| 10 | AR估 | 物流供應商 + 摘要含 `物流運費\|Shipping fee` | AND |
| 11 | 摘要內有押金/保證金 | 摘要含押金關鍵字 + GL# ∉ FA | AND |
| 12 | 已完成(not_billed) | remark_completed + pr_not_incomplete + no_status + erm_in_range + erm_le_date + qty_matched + not_billed | AND |
| 13 | Check收貨 | not_error + no_status + erm_in_range + erm_le_date + qty_not_matched | AND |
| 14 | 未完成 | not_error + no_status + erm_in_range + erm_gt_date | AND |
| 15 | error(...)_租金 | not_error + no_status + out_of_range + 摘要含 `租金` | AND |
| 16 | error(...)_薪資 | not_error + no_status + out_of_range + 摘要含 `派遣\|Salary\|Agency Fee` | AND |
| 17 | error(...) | not_error + no_status + out_of_range | AND |
| 18 | 已完成(Procurement備註) | 採購含 '已完成'，**覆蓋** 規則 13-17 及格式錯誤 | AND |

**prebuilt_masks 字典**（條件引擎的輸入映射）：

```python
prebuilt_masks = {
    'no_status', 'erm_in_range', 'erm_le_date', 'erm_gt_date',
    'qty_matched', 'not_billed', 'has_billing', 'fully_billed', 'has_unpaid',
    'remark_completed',  # = procurement_completed_or_rent | fn_completed_or_posted
    'pr_not_incomplete', 'is_fa', 'not_fa', 'not_error', 'out_of_range', 'format_error'
}
```

#### 會計欄位設定邏輯（Phase 6）

| 欄位 | 來源 |
|------|------|
| `Account code` | GL# 直接複製 |
| `Account Name` | GL# merge ref_SCTTW.xlsx['Account', 'Account Desc'] |
| `Product code` | Product Code 直接複製 |
| `Region_c` | config `sct_column_defaults.region`（預設 'TW'） |
| `Dep.` | GL# ∈ dept_accounts → Department 前 3 碼；否則 config `default_department`（預設 '000'） |
| `Currency_c` | Currency 直接複製 |
| `Accr. Amount` | Unit Price × (Entry Qty - Billed Qty) |
| `Liability` | 有預付 → config `prepay_liability`（預設 '111112'）；否則 merge reference_liability |
| `是否有預付` | Entry Prepay Amount != '0' → 'Y' |

### 4.7 ERM 邏輯評估步驟（PR）

**檔案**：`sct_pr_evaluation.py`（377 行）

`SCTPRERMLogicStep(PipelineStep)` — PR 專屬，**簡化版** PO ERM 邏輯。

#### 與 PO 版本的關鍵差異

| 面向 | PO (`SCTERMLogicStep`) | PR (`SCTPRERMLogicStep`) |
|------|----------------------|-------------------------|
| 規則數 | 18 條 | 13 條 |
| Dataclass | `SCTERMConditions`（16 欄位） | 無（inline mask） |
| 收貨邏輯 | qty_matched / qty_not_matched | 無 |
| 入帳邏輯 | has_billing / not_billed / voucher | 無 |
| 預付處理 | 有（Entry Prepay Amount） | 無 |
| Liability | merge reference_liability | 不設定 |
| Accr. Amount | Unit Price × (Entry Qty - Billed Qty) | Entry Amount 直接取值 |
| 必要欄位 | 22 個 | 10 個 |
| 輔助資料 | reference_account + reference_liability | reference_account |

#### 13 條 PR ERM 狀態規則

| 優先序 | 狀態值 | 核心條件 |
|--------|--------|---------|
| 1-5 | 需求取消/現狀結案/Outright/Consignment/Outsourcing | 同 PO |
| 6 | AR估 | 物流供應商 + 物流運費 |
| 7 | 摘要內有工程 | 摘要含 '工程'（**PR 獨有**） |
| 8 | 已完成 | remark_completed + FN NOT 未完成 + erm_in_range + erm_le_date |
| 9 | 已完成 | Procurement NOT 未完成/取消 + FN NOT 未完成 + erm_in_range + erm_le_date |
| 10 | 未完成 | not_error + erm_in_range + erm_gt_date |
| 11 | error(...)_薪資 | not_error + out_of_range + 派遣/Salary/Agency Fee |
| 12 | error(...) | not_error + out_of_range |
| 13 | 已完成(Procurement備註) | 覆蓋規則 |

#### 殘餘狀態處理

PR 版本在 `_handle_format_errors()` 中增加了**格式錯誤後的最終兜底**：仍然無狀態的記錄標記為 `'其他'`。

### 4.8 PPE 資產狀態步驟

**檔案**：`sct_asset_status.py`（395 行）

`SCTAssetStatusUpdateStep(PipelineStep)` — **PO 專屬**，針對含 PPE（固定資產）項目的 PO 進行狀態更新。

#### 配置項（來自 `[sct.ppe]`）

| 配置項 | 預設值 | 說明 |
|--------|--------|------|
| `trigger_keywords` | `['訂金', '安裝款', '驗收款', '保固']` | 觸發 PPE 評估的關鍵字 |
| `acceptance_keyword` | `'驗收'` | 驗收款識別字 |
| `warranty_keyword` | `'保固'` | 保固款識別字（排除驗收） |
| `completed_status` | `'已完成(PPE)'` | 完成標籤 |
| `incomplete_status` | `'未完成(PPE)'` | 未完成標籤 |
| `protected_statuses` | `['已入帳', '上期已入PPE', ...]` | 受保護狀態（不覆蓋） |

#### 執行邏輯

```
Phase 1: 識別 PPE PO
    GL# ∈ FA 科目 OR Item Description 含 trigger_keywords
    → 收集唯一 PO# 列表

Phase 2: 逐 PO 處理
    對每個 PPE PO：
    ├── 排除 protected_statuses 的行
    ├── 尋找驗收款 ERM（含 acceptance_keyword 但不含 warranty_keyword 的最大 ERM）
    ├── 若找到驗收款 ERM：
    │   └── 雙重確認（AND）：
    │       ├── acceptance_erm ≤ processing_date ？
    │       └── Remarked by Procurement 含 '已完成' ？
    │       → 兩者皆 True → 已完成(PPE)
    │       → 否則 → 未完成(PPE)
    └── 若無驗收款 ERM（fallback）：
        └── 僅檢查 Remarked by Procurement 含 '已完成'
            → True → 已完成(PPE)
            → False → 保持原狀態

Phase 3: 更新 Accrual Flag
    已完成(PPE) → 是否估計入帳 = 'Y'
    未完成(PPE) → 是否估計入帳 = 'N'
```

#### 額外產出欄位

`matched_condition_on_status`：記錄 PPE 狀態判定的原因說明。

### 4.9 科目預測步驟

**檔案**：`sct_account_prediction.py`（338 行）

`SCTAccountPredictionStep(PipelineStep)` — 根據 18 條配置規則預測 GL 會計科目。

#### AccountPredictionConditions（Dataclass）

```python
@dataclass
class AccountPredictionConditions:
    matched: pd.Series  # 布林 Series，追蹤已匹配的行
```

#### 規則結構

每條規則包含以下欄位（來自 TOML `[[sct_account_prediction.rules]]`）：

| 欄位 | 類型 | 說明 |
|------|------|------|
| `rule_id` | int | 排序鍵（升序執行） |
| `account` | str | 預測的 GL 科目 |
| `product_code` | str | 產品代碼篩選（"0" = 不篩選） |
| `department` | str | 部門篩選（"0" = 不篩選） |
| `description_keywords` | str | 摘要關鍵字（regex, case-insensitive） |
| `min_amount` | float? | 最小金額（含） |
| `max_amount` | float? | 最大金額（不含） |
| `liability_account` | str? | 預測的 Liability 科目 |
| `condition_desc` | str | 條件描述（寫入 matched_conditions） |

#### 執行流程

1. 初始化三個新欄位：`predicted_account`、`predicted_liability`、`matched_conditions`（全 pd.NA）
2. 建立 `AccountPredictionConditions.matched`（全 False）
3. 按 rule_id 順序逐條應用規則：
   - 構建條件遮罩（`_build_rule_condition`）：AND 組合所有非零篩選條件 + `~already_matched`
   - 若有匹配行：設定 predicted_account/liability/conditions，標記為 matched
4. **First-match-wins**：一行一旦匹配就不再參與後續規則

#### 18 條科目預測規則總覽

| Rule ID | Account | Product Code | Dept. | 關鍵字類別 | 金額範圍 | Liability |
|---------|---------|-------------|-------|-----------|---------|-----------|
| 1 | 199999 | 0（不限） | 0 | 固定資產（路由器/交換器等） | ≥ 30,000 | — |
| 2 | 520012 | RT_B2C_COM | 0 | 倉庫租金 | — | 200412 |
| 3 | 520013 | RT_B2C_COM | 0 | 水電/租賃 | — | 200412 |
| 4 | 520014 | RT_B2C_COM | 0 | 清潔/保全 | — | 200412 |
| 5 | 520016 | RT_B2C_COM | 0 | 辦公用品/文具 | — | 200412 |
| 6 | 520017 | RT_B2C_COM | 0 | 包材/標籤 | — | 200412 |
| 7 | 520018 | RT_B2C_COM | 0 | 搬運費 | — | 200412 |
| 8 | 520019 | RT_B2C_COM | 0 | 設備（PDA/冰箱等） | 3k~30k | 200412 |
| 9 | 520025 | RT_B2C_COM | 0 | 管理費 | — | 200412 |
| 10 | 520026 | RT_B2C_COM | 0 | 維修/保養 | — | 200412 |
| 11 | 520028 | RT_B2C_COM | 0 | 桶裝水 | — | 200412 |
| 12 | 520029 | RT_B2C_COM | 0 | 保險 | — | 200412 |
| 13 | 520030 | RT_B2C_COM | 0 | 倉庫臨時薪資 | — | 200412 |
| 14 | 520033 | RT_B2C_COM | 0 | 設備租金（棧板/堆高機等） | — | 200412 |
| 15 | 600301 | RT_B2C_COM | G03 | 員工健檢 | — | 200412 |
| 16 | 610307 | RT_B2C_COM | G03 | 罰款 | — | 200412 |
| 17 | 620008 | RT_B2C_COM | G03 | 人力仲介/護理 | — | 200406 |
| 18 | 630001 | RT_B2C_COM | G03 | 交通費 | — | 200412 |

**模式觀察**：
- Rule 1 是唯一不限 Product Code 的規則（跨產品的固定資產判定）
- Rules 2-14 都限於 `RT_B2C_COM` Product Code，不限部門
- Rules 15-18 進一步限於 `G03` 部門（行政相關）
- Liability 大多為 `200412`（應計費用），Rule 17 例外使用 `200406`（人力仲介費用）

### 4.10 資料格式化步驟

**檔案**：`sct_post_processing.py`（373 行）

`SCTPostProcessingStep(BasePostProcessingStep)` — 10 階段資料格式化流水線。

#### 10 階段流程

| 階段 | 方法 | 說明 |
|------|------|------|
| 1 | `_format_numeric_columns()` | 整數列 → Int64，浮點列 → float64 round(2) |
| 2 | `_reformat_dates()` | 日期列 → YYYY-MM-DD |
| 3 | `_clean_nan_values()` | 'nan'/'<NA>' → pd.NA，Accr. Amount 特殊處理（去逗號、0→None） |
| 4 | `_rearrange_columns()` | 備註欄位歸位、狀態移到是否估計入帳前、review/AP 移到最後 |
| 5 | `_rename_columns_dtype()` | 'Product code' → 'product_code_c'，呼叫 `clean_po_data()` snake_case 化 |
| 6 | `_rearrange_columns()` | rename 後再次排列 |
| 7 | `_save_temp_columns_data()` | 保存含臨時欄位的完整數據至 `context.auxiliary_data['result_with_temp_cols']` |
| 8 | `_remove_temp_columns()` | 移除臨時欄位（PO: 6 個，PR: 額外 3 個） |
| 9 | `_reformat_erm()` | `expected_receive_month`: `%b-%y` → `%Y/%m` |
| 10 | `_select_output_columns()` | 根據 TOML 篩選輸出欄位（PO: 62 個, PR: 57 個） |

#### TOML 配置鍵

| 鍵 | 型別 | 預設值 | 說明 |
|----|------|--------|------|
| `int_columns` | List[str] | `['Line#', 'GL#']` | 整數格式化欄位 |
| `float_columns` | List[str] | 13 個 | 浮點格式化欄位 |
| `date_columns` | List[str] | 8 個 | 日期格式化欄位 |
| `nan_clean_columns` | List[str] | 6 個 | NaN 清理欄位 |
| `temp_columns` | List[str] | 5 個 | 基礎臨時欄位 |
| `pr_extra_temp_columns` | List[str] | 3 個 | PR 額外臨時欄位 |
| `tail_columns` | List[str] | 2 個 | 尾端欄位（原始名） |
| `tail_columns_snake` | List[str] | 2 個 | 尾端欄位（snake_case） |
| `output_columns_po` | List[str] | 62 個 | PO 輸出欄位序 |
| `output_columns_pr` | List[str] | 57 個 | PR 輸出欄位序 |

#### 容錯設計

- 欄位不存在時 log warning 並跳過（不 crash）
- 數值轉換失敗時 try/except 包裹（逐欄處理）
- 輸出篩選時只保留實際存在的欄位

---

## 5. 應用範例

### 5.1 建立並執行 PO Pipeline

```python
from accrual_bot.tasks.sct import SCTPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext

orchestrator = SCTPipelineOrchestrator()
pipeline = orchestrator.build_po_pipeline(
    file_paths={'raw_po': 'path/to/202603_purchase_order.xlsx'}
)

context = ProcessingContext(
    entity_type='SCT',
    processing_type='PO',
    processing_date=202603
)
result = await pipeline.execute(context)
```

### 5.2 建立 PR Pipeline

```python
pipeline = orchestrator.build_pr_pipeline(
    file_paths={'raw_pr': 'path/to/202603_purchase_request.xlsx'}
)
```

### 5.3 查詢啟用的步驟清單

```python
po_steps = orchestrator.get_enabled_steps('PO')
# ['SCTDataLoading', 'SCTColumnAddition', 'APInvoiceIntegration',
#  'PreviousWorkpaperIntegration', 'ProcurementIntegration',
#  'DateLogic', 'SCTERMLogic', 'SCTAssetStatusUpdate',
#  'SCTAccountPrediction', 'SCTPostProcessing']

pr_steps = orchestrator.get_enabled_steps('PR')
# ['SCTPRDataLoading', 'SCTColumnAddition', ... (8 steps)]
```

### 5.4 從 UnifiedPipelineService 呼叫

```python
from accrual_bot.ui.services import UnifiedPipelineService

service = UnifiedPipelineService()
pipeline = service.build_pipeline(
    entity='SCT',
    proc_type='PO',
    file_paths={'raw_po': '/path/to/file.xlsx'},
    processing_date=202603
)
```

---

## 6. 優缺分析

### 6.1 設計優點

| 優點 | 說明 |
|------|------|
| **高度配置驅動** | 894 行 TOML 涵蓋所有業務規則，新增/調整規則無需改程式碼 |
| **模板方法複用** | `BaseLoadingStep`、`BasePostProcessingStep` 消除資料載入和格式化的重複邏輯 |
| **ConditionEngine 統一** | PO/PR ERM 評估透過同一引擎驅動，規則格式與 SPX 一致 |
| **清晰的 PO/PR 分離** | 獨立的評估步驟（SCTERMLogic vs SCTPRERMLogic）避免條件判斷混亂 |
| **PPE 雙重確認** | 資產狀態需 ERM 日期 + 採購備註兩者皆符合，降低誤判風險 |
| **First-match-wins 科目預測** | 規則優先序明確，消除多規則衝突的歧義 |
| **容錯格式化** | 缺欄位時 warning 而非 crash，提升穩健性 |

### 6.2 已知限制

| 限制 | 說明 | 嚴重度 |
|------|------|--------|
| **無 Export 步驟** | SCT 目前無獨立的 `SCTExportStep`，依賴 PostProcessing 輸出 | 低 |
| **無 DataShapeSummary** | `enabled_steps` 未包含 `DataShapeSummary`，但 Loading 已預備 `raw_data_snapshot` | 低 |
| **PR 無 Liability** | PR 版 ERM 不設定 Liability 欄位，可能需要後續補充 | 低 |
| **SCTERMLogicStep 無獨立 Dataclass（PR）** | PO 使用 `SCTERMConditions` dataclass，PR 使用 inline mask，風格不一致 | 低 |
| **APInvoiceIntegrationStep 重複定義** | SCT 版在 `sct_integration.py`，與 core 版功能相似但 VOUCHER_NUMBER 提取邏輯略有不同 | 中 |

### 6.3 與 SPT/SPX 的比較

| 面向 | SPT | SPX | SCT |
|------|-----|-----|-----|
| Pipeline 類型 | PO/PR/PROCUREMENT | PO/PR/PPE/PPE_DESC | PO/PR |
| 原始資料格式 | CSV | CSV | **xlsx** |
| ERM 條件引擎 | 硬編碼 | ConditionEngine | ConditionEngine |
| PPE 處理 | 無 | 獨立 Pipeline | **嵌入 PO Pipeline** |
| 科目預測 | 硬編碼規則 | 無 | **配置驅動規則** |
| 特殊業務 | Commission/Payroll | Locker/Kiosk/Deposit | **物流供應商/押金** |
| 後處理 | SPTPostProcessingStep | DataReformattingStep | SCTPostProcessingStep |
| TOML 配置量 | ~400 行 | ~600 行 | **894 行** |

---

## 7. 延伸議題

### 7.1 ConditionEngine 共用模式

SCT 的 ERM 評估直接複用了 Phase 12b 從 SPX 重構到 `core/pipeline/engines/` 的 `ConditionEngine`。此引擎支援：
- 優先序排序（priority 欄位）
- AND/OR 組合邏輯（combine 欄位）
- 預建遮罩映射（prebuilt_masks）
- 狀態覆蓋機制（override_statuses）
- 欄位型別檢查（contains, is_not_null, in_list 等）

SCT 透過傳入不同的 `rule_section_key`（`'sct_erm_status_rules'` / `'sct_pr_erm_status_rules'`）切換規則集。

### 7.2 PPE 嵌入式 vs 獨立式處理

SPX 將 PPE 作為獨立的 Pipeline 類型（`build_ppe_pipeline()`），而 SCT 將 PPE 狀態更新**嵌入** PO Pipeline 的一個步驟中。這是因為：
- SCT 的 PPE 項目混合在一般 PO 資料中（非獨立資料源）
- PPE 判定僅需檢查同一 PO 內的驗收款項目
- 不需要額外的合約歸檔清單或 Google Sheets 續約表

### 7.3 科目預測的可擴展性

`SCTAccountPredictionStep` 的 rule-based 設計使得新增科目規則只需在 TOML 中添加 `[[sct_account_prediction.rules]]` 條目。潛在的演進方向：
- 將此模式提升到 Core 層，供 SPT/SPX 共用
- 引入 fallback rule（目前未匹配的行保持 pd.NA）
- 支援多條件 OR 組合（目前僅 AND）

### 7.4 Export 步驟的缺失

SCT 目前依賴 `SCTPostProcessingStep` 的輸出欄位篩選作為最終輸出。若需要：
- 匯出到特定格式（Excel with styling、Google Sheets）
- 生成 Pivot 彙總表
- 與 DuckDB 整合

則需建立獨立的 `SCTExportStep`，可參考 SPX 的 `SPXExportStep`。

---

## 8. 其他

### 8.1 配置檔案對應

| 配置檔 | 區段 | 消費步驟 |
|--------|------|---------|
| `stagging_sct.toml` | `[pipeline.sct]` | Orchestrator |
| | `[sct_column_defaults]` | ColumnAddition, ERM |
| | `[sct.ppe]` | AssetStatus |
| | `[sct]` | Loading, ERM |
| | `[sct_erm_status_rules]` | SCTERMLogic (18 rules) |
| | `[sct_pr_erm_status_rules]` | SCTPRERMLogic (13 rules) |
| | `[sct_account_prediction]` | AccountPrediction (18 rules) |
| | `[sct_reformatting]` | PostProcessing |
| `stagging.toml` | `[paths].ref_path_sct` | Loading |
| | `[fa_accounts].sct` | ColumnAddition, ERM, AssetStatus |
| `config.ini` | `ref_path_sct` | Loading (legacy fallback) |
| `paths.toml` | `[sct.po]` / `[sct.pr]` | Runner |

### 8.2 測試覆蓋率現況

| 測試檔案 | 測試數 | 測試範圍 |
|----------|--------|---------|
| `test_sct_evaluation.py` | 28 | SCTERMLogicStep + SCTPRERMLogicStep |
| `test_sct_asset_status.py` | 10 | SCTAssetStatusUpdateStep |
| `test_sct_account_prediction.py` | 2 | SCTAccountPredictionStep |
| `test_sct_post_processing.py` | 21 | SCTPostProcessingStep |
| **合計** | **61** | — |

**未覆蓋的步驟**：`SCTDataLoadingStep`、`SCTPRDataLoadingStep`、`SCTColumnAdditionStep`、`APInvoiceIntegrationStep`、`SCTPipelineOrchestrator`

### 8.3 檔案行數統計

| 檔案 | 行數 | 佔比 |
|------|------|------|
| `sct_evaluation.py` | 541 | 18.5% |
| `sct_asset_status.py` | 395 | 13.5% |
| `sct_pr_evaluation.py` | 377 | 12.9% |
| `sct_post_processing.py` | 373 | 12.8% |
| `sct_account_prediction.py` | 338 | 11.6% |
| `pipeline_orchestrator.py` | 256 | 8.8% |
| `sct_column_addition.py` | 253 | 8.7% |
| `sct_loading.py` | 216 | 7.4% |
| `sct_integration.py` | 139 | 4.8% |
| `steps/__init__.py` | 26 | 0.9% |
| `__init__.py` | 8 | 0.3% |
| **合計** | **2,922** | 100% |

### 8.4 ProcessingContext 互動總覽

| 步驟 | 讀取 context.data | 寫入 context.data | 讀取 auxiliary | 寫入 auxiliary | 讀取 variables | 寫入 variables |
|------|:--:|:--:|:--:|:--:|:--:|:--:|
| DataLoading | — | ✓ | — | reference_account, reference_liability, raw_data_snapshot | — | processing_date, processing_month |
| ColumnAddition | ✓ | ✓ | — | — | processing_month | — |
| APInvoiceIntegration | ✓ | ✓ | ap_invoice | — | processing_date | — |
| PreviousWorkpaper | ✓ | ✓ | previous_po/pr | — | — | — |
| ProcurementIntegration | ✓ | ✓ | procurement_po/pr | — | — | — |
| DateLogic | ✓ | ✓ | — | — | — | — |
| SCTERMLogic | ✓ | ✓ | reference_account, reference_liability | — | processing_date | — |
| SCTAssetStatusUpdate | ✓ | ✓ | — | — | processing_date | — |
| SCTAccountPrediction | ✓ | ✓ | — | — | — | — |
| SCTPostProcessing | ✓ | ✓ | — | result_with_temp_cols | — | — |

### 8.5 類別繼承樹

```
PipelineStep (core/pipeline/base.py)
├── BaseLoadingStep (core/pipeline/steps/base_loading.py)
│   └── SCTBaseDataLoadingStep (sct_loading.py)
│       ├── SCTDataLoadingStep
│       └── SCTPRDataLoadingStep
├── BasePostProcessingStep (core/pipeline/steps/post_processing.py)
│   └── SCTPostProcessingStep (sct_post_processing.py)
├── SCTColumnAdditionStep (sct_column_addition.py)
├── APInvoiceIntegrationStep (sct_integration.py)
├── SCTERMLogicStep (sct_evaluation.py)
├── SCTPRERMLogicStep (sct_pr_evaluation.py)
├── SCTAssetStatusUpdateStep (sct_asset_status.py)
└── SCTAccountPredictionStep (sct_account_prediction.py)
```
