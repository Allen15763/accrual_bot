# `accrual_bot/tasks/spt` 深度技術研究文件

> 作者：Claude Code 自動研究報告
> 研究範圍：`accrual_bot/tasks/spt/` 全部 17 個 Python 原始碼檔案
> 研究日期：2026-03-13

---

## 目錄

1. [背景](#一背景)
   - 1.1 [專案定位](#11-專案定位)
   - 1.2 [模組歷史沿革](#12-模組歷史沿革)
   - 1.3 [與整體架構的關係](#13-與整體架構的關係)
2. [用途](#二用途)
   - 2.1 [整體業務目標](#21-整體業務目標)
   - 2.2 [支援的 Pipeline 類型](#22-支援的-pipeline-類型)
   - 2.3 [資料流轉概覽](#23-資料流轉概覽)
3. [設計思路](#三設計思路)
   - 3.1 [四層架構定位](#31-四層架構定位)
   - 3.2 [配置驅動原則](#32-配置驅動原則)
   - 3.3 [Step Registry 模式](#33-step-registry-模式)
   - 3.4 [跨實體元件複用策略](#34-跨實體元件複用策略)
   - 3.5 [Sub-Context 隔離設計](#35-sub-context-隔離設計)
4. [各項知識點](#四各項知識點)
   - 4.1 [模組初始化與公開 API](#41-模組初始化與公開-api)
   - 4.2 [SPTPipelineOrchestrator 詳解](#42-sptpipelineorchestrator-詳解)
   - 4.3 [資料載入步驟群](#43-資料載入步驟群)
   - 4.4 [ERM 邏輯評估步驟](#44-erm-邏輯評估步驟)
   - 4.5 [分潤與薪資偵測步驟](#45-分潤與薪資偵測步驟)
   - 4.6 [會計標籤步驟](#46-會計標籤步驟)
   - 4.7 [科目預測步驟](#47-科目預測步驟)
   - 4.8 [後處理步驟](#48-後處理步驟)
   - 4.9 [孤兒步驟群分析](#49-孤兒步驟群分析)
   - 4.10 [採購流程步驟群](#410-採購流程步驟群)
   - 4.11 [COMBINED 模式步驟群](#411-combined-模式步驟群)
5. [應用範例](#五應用範例)
   - 5.1 [建立並執行 PO Pipeline](#51-建立並執行-po-pipeline)
   - 5.2 [建立 PR Pipeline](#52-建立-pr-pipeline)
   - 5.3 [建立 PROCUREMENT Pipeline（PO/PR/COMBINED）](#53-建立-procurement-pipelinepoprсombined)
   - 5.4 [查詢啟用的步驟清單](#54-查詢啟用的步驟清單)
   - 5.5 [自訂步驟注入](#55-自訂步驟注入)
   - 5.6 [從 UnifiedPipelineService 呼叫](#56-從-unifiedpipelineservice-呼叫)
6. [優缺分析](#六優缺分析)
   - 6.1 [設計優點](#61-設計優點)
   - 6.2 [已知問題與缺陷](#62-已知問題與缺陷)
   - 6.3 [技術債務盤點](#63-技術債務盤點)
7. [延伸議題](#七延伸議題)
   - 7.1 [重構建議：載入層統一](#71-重構建議載入層統一)
   - 7.2 [孤兒步驟的處置策略](#72-孤兒步驟的處置策略)
   - 7.3 [跨實體耦合的影響分析](#73-跨實體耦合的影響分析)
   - 7.4 [ERM 條件的可測試性](#74-erm-條件的可測試性)
   - 7.5 [採購前期底稿載入的架構演進](#75-採購前期底稿載入的架構演進)
8. [其他](#八其他)
   - 8.1 [配置檔案對應](#81-配置檔案對應)
   - 8.2 [測試覆蓋率現況](#82-測試覆蓋率現況)
   - 8.3 [檔案行數統計](#83-檔案行數統計)
   - 8.4 [已知 BUG 速查表](#84-已知-bug-速查表)

---

## 一、背景

### 1.1 專案定位

`accrual_bot/tasks/spt/` 是 Accrual Bot 系統中針對 **SPT（Shopee Pte. Ltd.）** 業務實體所設計的 Task 層模組。Accrual Bot 是一套非同步資料處理系統，專門用於每月財務底稿（Workpaper）的應計費用（Accrual）計算與對帳，確保 PO（採購單）和 PR（請購單）的財務狀態能正確地記入當期帳目。

SPT 模組是整個系統中功能最完整、業務邏輯最複雜的實體模組。相較於 SPX 模組，SPT 的特殊之處在於：

- 同時處理 **PO、PR、PROCUREMENT** 三種不同的業務類型
- 包含 **分潤（Commission）** 和 **薪資（Payroll）** 的特殊業務識別邏輯
- 擁有 **採購專用（Procurement）** 的獨立 Pipeline，供採購人員使用
- PROCUREMENT 類型支援 **COMBINED 模式**，能在同一次執行中同時處理 PO 和 PR

### 1.2 模組歷史沿革

根據 `CLAUDE.md` 的架構改進紀錄，`tasks/spt/` 模組的形成經歷以下階段：

**Phase 2（2026 年 1 月）— 程式碼去重**

抽取了 SPT 和 SPX 載入步驟中重複的約 400–500 行程式碼，形成 `BaseLoadingStep` 模板基類。

> **更新（Phase 10, 2026-03-17）**：`spt_loading.py` 已完成重構（Fix 5），從 1164 行縮減至 182 行（84% 減量）。引入了 `SPTBaseDataLoadingStep(BaseLoadingStep)` 中介基類，提供四個模板鉤子；`SPTDataLoadingStep` 和 `SPTPRDataLoadingStep` 現已繼承 `SPTBaseDataLoadingStep`，不再直接繼承 `PipelineStep`。採購子模組的載入步驟（`spt_procurement_loading.py`）同樣使用 `BaseLoadingStep`。

**Phase 3（2026 年 1 月）— 結構化**

在 `tasks/` 目錄下建立實體專屬子模組，實作 `SPTPipelineOrchestrator`，並以配置驅動方式取代硬編碼的步驟清單。

**Phase 7（2026 年 3 月）— 採購 Pipeline 新增**

增加了 PROCUREMENT Pipeline 類型，支援 PO、PR、COMBINED 三種子類型，同時對 `stagging.toml` 進行了分割，形成 `stagging_spt.toml` 和 `stagging_spx.toml` 兩個實體專屬配置檔。

### 1.3 與整體架構的關係

在四層架構中，`tasks/spt/` 屬於 **Tasks Layer（Orchestrators）**，位於 Core 層和 UI 層之間：

```
UI Layer（Streamlit）
    ↓ 呼叫
UnifiedPipelineService（ui/services/）
    ↓ 呼叫
SPTPipelineOrchestrator（tasks/spt/pipeline_orchestrator.py）  ← 本模組頂層
    ↓ 建構
Pipeline（core/pipeline/pipeline.py）
    ↓ 包含
PipelineStep 實例群（tasks/spt/steps/*.py）  ← 本模組主體
    ↓ 使用
ProcessingContext（core/pipeline/context.py）
DataSourcePool（core/datasources/）
ConfigManager（utils/config/）
```

`SPTPipelineOrchestrator` 是本模組對外的唯一公開介面（`__init__.py` 僅匯出此類），UI 層和 CLI 層均透過此類建立 Pipeline 物件。

---

## 二、用途

### 2.1 整體業務目標

SPT Task 模組的核心業務目標是：

1. **讀取 ERP 系統匯出的月底採購資料**（raw_po、raw_pr），以及各種輔助資料（上月底稿、AP 發票、採購備註等）
2. **套用 ERM（Expected Receive Month）邏輯**，判斷每筆 PO/PR 的財務狀態（已完成、未完成、待確認等）
3. **整合分潤和薪資識別**，正確標記特殊業務類型的記錄
4. **套用會計師的標籤規則**，對特定條件的記錄進行人工覆蓋
5. **預測會計科目**，為審計人員提供參考
6. **輸出格式化的財務底稿**，供後續審核、入帳使用

### 2.2 支援的 Pipeline 類型

| Pipeline 類型 | 子類型 | 主要步驟 | 適用人員 |
|---|---|---|---|
| PO | — | 載入 → 過濾 → 整合 → ERM 評估 → 標籤 → 預測 → 後處理 → 匯出 | 財務（FN）人員 |
| PR | — | 與 PO 流程相近，使用 PR 專用欄位 | 財務（FN）人員 |
| PROCUREMENT | PO | 載入 → 欄位初始化 → 前期映射 → 日期邏輯 → 採購狀態評估 → 匯出 | 採購人員 |
| PROCUREMENT | PR | 同上，使用 PR 資料 | 採購人員 |
| PROCUREMENT | COMBINED | 同時載入 PO + PR → 分別處理 → 統一匯出到雙 Sheet Excel | 採購人員 |

### 2.3 資料流轉概覽

**PO Pipeline 資料流：**

```
原始資料（raw_po CSV/Excel）
    → SPTDataLoadingStep    → 主 DataFrame（ProcessingContext.data）
                              + auxiliary: previous, procurement, ap_invoice
                              + auxiliary: reference_account, reference_liability
    → ProductFilterStep     → 排除 SPX 產品
    → ColumnAdditionStep    → 新增計算欄位
    → APInvoiceIntegration  → 整合 AP 發票
    → PreviousWorkpaperIntegration → 整合上月底稿
    → ProcurementIntegration → 整合採購備註
    → CommissionDataUpdate  → 更新分潤記錄的 GL# 和 Product Code
    → PayrollDetection      → 標記 Payroll 記錄
    → DateLogic             → 解析 Item Description 中的日期範圍
    → SPTERMLogic           → 11 個條件的 ERM 狀態評估 + 會計欄位設定
    → SPTStatusLabel        → 套用 TOML 配置規則（會計師覆蓋）
    → SPTAccountPrediction  → 預測 predicted_account 輔助欄位
    → SPTPostProcessing     → 格式化、欄位重排、移除臨時欄位
    → SPTExport（SPXPRExportStep）→ 輸出 Excel
    → DataShapeSummary      → 記錄資料形狀統計
```

**COMBINED 採購 Pipeline 資料流：**

```
原始資料（raw_po + raw_pr + procurement_previous）
    → CombinedProcurementDataLoading → auxiliary: po_data, pr_data,
                                        procurement_previous_po, procurement_previous_pr
    → ProcurementPreviousValidation  → 驗證前期底稿格式
    → CombinedProcurementProcessing  → 建立 sub-context，分別執行：
        ├── PO：ColumnInitialization → PreviousMapping → DateLogic → StatusEvaluation
        └── PR：同上，使用 PR 欄位
        → 結果存入 auxiliary: po_result, pr_result
    → CombinedProcurementExport      → PO/PR 分別寫入 .xlsx 的兩個 Sheet
```

---

## 三、設計思路

### 3.1 四層架構定位

SPT 模組嚴格遵循四層架構的邊界：

- **不直接讀取資料庫或檔案系統**：所有 IO 操作透過 `core/datasources/` 的 DataSource 抽象層
- **不直接存取 UI 狀態**：透過 `ProcessingContext` 傳遞資料
- **不自行管理配置**：透過 `ConfigManager` 單例讀取 TOML/INI 配置
- **不直接呼叫其他 Tasks 的步驟**：部分例外（跨實體複用，見 3.4 節）

這種邊界隔離使各層可以獨立測試，也使得 Pipeline 的步驟可以被替換或重組，而不影響其他層的程式碼。

### 3.2 配置驅動原則

SPT 模組有三種配置驅動機制，從強到弱排列：

**等級一：Pipeline 步驟清單配置（最強）**

`stagging_spt.toml` 的 `[pipeline.spt]` 段落定義哪些步驟會被啟用：

```toml
[pipeline.spt]
enabled_po_steps = ["SPTDataLoading", "ProductFilter", "ColumnAddition",
    "APInvoiceIntegration", "PreviousWorkpaperIntegration", ...]
enabled_pr_steps = [...]
enabled_procurement_po_steps = [...]
enabled_procurement_pr_steps = [...]
enabled_procurement_combined_steps = [...]
```

當配置為空時，Orchestrator 使用硬編碼的預設清單（fallback），保證系統在沒有完整配置的情況下也能執行。

**等級二：規則集配置（中）**

`SPTStatusLabelStep`、`SPTAccountPredictionStep`、`SPTProcurementStatusEvaluationStep` 三個步驟完全依賴 TOML 中的規則集，新增或修改業務規則**無需修改程式碼**：

```toml
[spt_status_label_rules.priority_conditions]
[spt_status_label_rules.erm_conditions]
[spt_account_prediction]
rules = [...]
[[spt_procurement_status_rules.conditions]]
```

**等級三：硬編碼常數（弱）**

`CommissionDataUpdateStep` 使用類別級別的 `COMMISSION_CONFIG` 字典，`PayrollDetectionStep` 使用 `PAYROLL_CONFIG`，這些是在程式碼中直接定義的常數，而非 TOML 配置。相較等級一和二，修改時需要改程式碼。

### 3.3 Step Registry 模式

`SPTPipelineOrchestrator._create_step()` 方法使用了 **懶初始化的 Step Registry 模式**：

```python
step_registry = {
    'SPTDataLoading': lambda: SPTDataLoadingStep(
        name="SPTDataLoading",
        file_paths=file_paths
    ),
    'ProductFilter': lambda: ProductFilterStep(
        name="ProductFilter",
        product_pattern='(?i)SPX',
        exclude=True,
        required=True
    ),
    # ... 共 25 個 lambda 工廠
}

step_factory = step_registry.get(step_name)
if step_factory:
    return step_factory()
else:
    print(f"Warning: Unknown step '{step_name}' for SPT {processing_type} pipeline")
    return None
```

這個設計的優點是：
1. **集中管理**：所有步驟的建立邏輯集中在一個方法中
2. **懶初始化**：只有被配置選中的步驟才會實例化，節省資源
3. **閉包注入**：`file_paths` 和 `processing_type` 透過 lambda 閉包注入，無需全域狀態

缺點是：
1. `step_registry` 是區域變數字典，**每次呼叫都重新建立**，有輕微效能開銷
2. 未知步驟使用 `print()` 警告，而非 `self.logger.warning()`，日誌層級不一致

### 3.4 跨實體元件複用策略

SPT Orchestrator 匯入了 4 個來自 SPX 的步驟：

```python
from accrual_bot.tasks.spx.steps import (
    SPXPRExportStep,       # SPT Export 使用 SPX 的實作
    SPXPRERMLogicStep,     # SPT PR 使用 SPX PR 的 ERM 邏輯
    ColumnAdditionStep,    # SPT 使用 SPX 的欄位新增步驟
    APInvoiceIntegrationStep,  # SPT 使用 SPX 的 AP 整合步驟
)
```

這種設計反映了一個業務現實：SPT 和 SPX 某些處理邏輯完全相同，不需要重複實作。然而，這種設計也建立了 tasks/spt 對 tasks/spx 的直接依賴，違反了模組間應相互獨立的理想。如果 SPX 的這些步驟未來發生破壞性變更，SPT 的功能也會受到影響。

另外，從 `core/pipeline/steps/` 共用的步驟（`ProductFilterStep`、`PreviousWorkpaperIntegrationStep`、`ProcurementIntegrationStep`、`DateLogicStep`）不在此問題的範疇，因為這些是架構上明確設計為共用的 Core 層步驟。

### 3.5 Sub-Context 隔離設計

COMBINED 採購模式引入了一個創新的 **Sub-Context 模式**（見 `spt_combined_procurement_processing.py`）：

```python
# 在 _process_po_data() 中
sub_context = ProcessingContext()
sub_context.update_data(po_data.copy())

file_date = parent_context.metadata.processing_date
sub_context.set_variable('file_date', file_date)

prev_po = parent_context.get_auxiliary_data('procurement_previous_po')
if prev_po is not None:
    sub_context.set_auxiliary_data('procurement_previous', prev_po)

# 執行子步驟
for step in steps:
    result = await step.execute(sub_context)
```

這種設計的關鍵洞見是：PO 和 PR 的處理流程是完全對稱的，但它們的資料和狀態欄位不同。透過為每個類型建立獨立的 `ProcessingContext`，可以直接複用現有的 `ColumnInitializationStep`、`ProcurementPreviousMappingStep` 等步驟，**無需為 COMBINED 模式撰寫特殊邏輯**。

唯一的缺點是：子步驟的日誌輸出和 metadata 被完全捨棄（除了失敗狀態判斷外），不如單一模式的 Pipeline 那樣有完整的執行紀錄。

---

## 四、各項知識點

### 4.1 模組初始化與公開 API

**`__init__.py`（7 行）**

```python
"""
SPT Task Module - SPT entity-specific implementations
"""

from accrual_bot.tasks.spt.pipeline_orchestrator import SPTPipelineOrchestrator

__all__ = ["SPTPipelineOrchestrator"]
```

模組的公開 API 只有一個類別：`SPTPipelineOrchestrator`。這是刻意為之的最小化設計——外部呼叫者不需要知道 SPT 內部有哪些步驟，只需要透過 Orchestrator 建立 Pipeline 即可。

**`steps/__init__.py`（56 行）**

步驟子模組的 `__all__` 匯出了 25 個符號，包含所有步驟類別和一個 dataclass（`AccountPredictionConditions`）。這個 `__all__` 的存在主要是為了讓 `pipeline_orchestrator.py` 可以用整潔的 import 語法一次匯入所有步驟，而不是在 orchestrator 中到處散落各種獨立 import。

> **更新（2026-03-17）**：四個孤兒步驟（`SPTStatusStep`、`SPTDepartmentStep`、`SPTAccrualStep`、`SPTValidationStep`）已從 `__all__` 移除，僅保留 deprecation docstring。

### 4.2 SPTPipelineOrchestrator 詳解

**類別初始化**

```python
def __init__(self):
    self.config = config_manager._config_toml.get('pipeline', {}).get('spt', {})
    self.entity_type = 'SPT'
```

注意：`config_manager._config_toml` 是直接存取 ConfigManager 的私有屬性，而非透過公開的 `get()` 方法。這表示此 Orchestrator 依賴 ConfigManager 的內部實作細節，若 ConfigManager 的內部儲存結構改變，這裡需要對應修改。

**三個 build 方法**

| 方法 | Pipeline 名稱 | 步驟清單配置鍵 |
|---|---|---|
| `build_po_pipeline()` | `SPT_PO_Processing` | `enabled_po_steps` |
| `build_pr_pipeline()` | `SPT_PR_Processing` | `enabled_pr_steps` |
| `build_procurement_pipeline(source_type='PO')` | `SPT_PROCUREMENT_PO_Processing` | `enabled_procurement_po_steps` |
| `build_procurement_pipeline(source_type='PR')` | `SPT_PROCUREMENT_PR_Processing` | `enabled_procurement_pr_steps` |
| `build_procurement_pipeline(source_type='COMBINED')` | `SPT_PROCUREMENT_COMBINED_Processing` | `enabled_procurement_combined_steps` |

**路徑正規化邏輯（`_normalize_procurement_paths`）**

採購 Pipeline 有一個特殊的路徑正規化流程，用於統一不同子類型的路徑命名：

```python
def _normalize_procurement_paths(self, file_paths, source_type):
    paths = file_paths.copy()

    if source_type == 'PO':
        paths.pop('raw_pr', None)
        paths.pop('procurement_previous_pr', None)
        # procurement_previous_po → procurement_previous
        if 'procurement_previous_po' in paths:
            paths['procurement_previous'] = paths.pop('procurement_previous_po')

    elif source_type == 'PR':
        paths.pop('raw_po', None)
        paths.pop('procurement_previous_po', None)
        # procurement_previous_pr → procurement_previous
        if 'procurement_previous_pr' in paths:
            paths['procurement_previous'] = paths.pop('procurement_previous_pr')

    # COMBINED: 保留所有路徑
    return paths
```

這個設計的目的是：`paths.toml` 中為 PROCUREMENT 類型定義了 `procurement_previous_po` 和 `procurement_previous_pr` 兩個獨立路徑，但個別處理步驟（`ProcurementPreviousMappingStep`）只接受統一的 `procurement_previous` 鍵。正規化函式在路徑進入步驟之前，先依據 `source_type` 選擇正確的路徑並重命名。

**PR/PO 狀態欄位的動態注入**

Orchestrator 在建立某些步驟時，依據 `processing_type` 動態注入正確的狀態欄位名稱：

```python
'CommissionDataUpdate': lambda: CommissionDataUpdateStep(
    name="CommissionDataUpdate",
    status_column="PR狀態" if processing_type == 'PR' else "PO狀態",
    required=True
),
'SPTStatusLabel': lambda: SPTStatusLabelStep(
    name="SPTStatusLabel",
    status_column="PR狀態" if processing_type == 'PR' else "PO狀態",
    remark_column="Remarked by FN"
),
```

這確保了 PO 和 PR Pipeline 共用相同的步驟類別，只是操作的欄位不同。

**`get_enabled_steps()` 方法**

```python
def get_enabled_steps(self, processing_type='PO', source_type=None):
    if processing_type == 'PO':
        return self.config.get('enabled_po_steps', [])
    elif processing_type == 'PR':
        return self.config.get('enabled_pr_steps', [])
    elif processing_type == 'PROCUREMENT':
        if source_type == 'PR':
            return self.config.get('enabled_procurement_pr_steps', [])
        elif source_type == 'COMBINED':
            return self.config.get('enabled_procurement_combined_steps', [])
        else:
            return self.config.get('enabled_procurement_po_steps', [])
    else:
        return []
```

此方法主要供 UI 層的 `UnifiedPipelineService.get_enabled_steps()` 呼叫，用於在 Configuration 頁面預覽 Pipeline 將執行哪些步驟。

### 4.3 資料載入步驟群

SPT 的資料載入分為三個子群，採用**不同的架構模式**，形成了設計上的不一致性。

#### 4.3.1 SPTDataLoadingStep（PO 主要載入）

**繼承關係**：`PipelineStep`（直接繼承，非 BaseLoadingStep）

**核心方法流程**：

```
execute()
  └── _validate_file_configs()         # 驗證檔案路徑，缺少 raw_po 則拋出 FileNotFoundError
  └── _load_all_files_concurrent()     # asyncio.gather 並發載入
        └── _load_single_file()        # 每個檔案獨立 task
              ├── raw_po  → _load_raw_po_file()   # 特殊處理
              ├── ap_invoice → _load_ap_invoice() # 特殊處理（含 BUG）
              └── others → source.read()           # 通用讀取
  └── _extract_raw_po_data()          # 驗證主 DataFrame
  └── context.update_data(df)
  └── _load_reference_data()          # 非並發，順序執行
  └── cleanup（finally）→ pool.close_all()
```

**`_load_raw_po_file()` 的資料標準化邏輯**

```python
async def _load_raw_po_file(self, source, file_path):
    df = await source.read()

    if 'Line#' in df.columns:
        df['Line#'] = df['Line#'].astype('Float64').round(0).astype('Int64').astype('string')

    if 'GL#' in df.columns:
        df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
        df['GL#'] = df['GL#'].fillna('666666').astype('Float64').round(0).astype('Int64').astype('string')

    # 注意：以下 rename 使用了錯誤語法，不會實際生效（忘記賦值）
    if 'Project Number' in df.columns:
        df.rename(columns={'Project Number': 'Project'})  # BUG：缺少 inplace=True 或賦值
```

三段標準化邏輯的意義：
- **Line# 正規化**：ERP 系統匯出的 Line# 為浮點數字串（如 `"1.0"`），透過 Float64→Int64→string 鏈轉換為整數字串 `"1"`
- **GL# 替換**：`'N.A.'` 是 ERP 中科目未指定的佔位符，統一替換為 `'666666'`（虛擬科目代碼）
- **GL# NaN 填充**：空的 GL# 同樣填入 `'666666'`

**`_load_ap_invoice()` 的 BUG**

```python
async def _load_ap_invoice(self, source):
    df = await source.read(
        usecols=config_manager.get_list('SPX', 'ap_columns'),  # BUG：應為 'SPT'
        header=1,
        sheet_name=1,
        dtype=str
    )
    return df
```

此處使用 `'SPX'` 作為實體識別碼讀取 `ap_columns` 配置，但這是 **SPT 的資料載入步驟**，應使用 `'SPT'`。這個 BUG 在以下情況才會顯現：當 SPT 和 SPX 的 `ap_columns` 配置不同時，SPT AP 發票的欄位選取會使用 SPX 的定義，導致欄位錯誤或遺漏。

**`_load_reference_data()` 的 Colab 容錯**

```python
ref_ac = get_ref_on_colab(ref_data_path)
if ref_ac is not None and isinstance(ref_ac, pd.DataFrame):
    # 從 ZIP 取得（Colab 離線環境）
    context.add_auxiliary_data('reference_account', ref_ac.iloc[:, 1:3].copy())
    context.add_auxiliary_data('reference_liability', ref_ac.loc[:, ['Account', 'Liability']].copy())
    return count

if Path(ref_data_path).exists():
    # 從本地檔案取得
    source = DataSourceFactory.create_from_file(str(ref_data_path))
    ref_ac = await source.read(dtype=str)
    ...
```

這段程式碼保留了對 Google Colab 環境的支援（透過 `get_ref_on_colab()`），在離線環境中從 ZIP 備份讀取參考資料。這是系統設計初期的歷史遺留，現在的生產環境通常不需要 Colab 路徑。

**`DataSourcePool` 的 RAII 設計**

```python
self.pool = DataSourcePool()  # 在 __init__ 建立

# 在 _load_single_file 中
self.pool.add_source(file_type, source)

# 在 execute 的 finally 中
finally:
    await self._cleanup_resources()
        # → await self.pool.close_all()
```

這確保即使執行過程中發生異常，所有已開啟的資料源也會被正確關閉，避免資源洩漏。

#### 4.3.2 SPTPRDataLoadingStep（PR 主要載入）

**繼承關係**：`PipelineStep`（直接繼承，非 BaseLoadingStep）

`SPTPRDataLoadingStep` 與 `SPTDataLoadingStep` 的程式碼幾乎完全相同，差異僅在：
- 必要檔案改為 `raw_pr`（而非 `raw_po`）
- 呼叫 `_load_raw_pr_file()`（而非 `_load_raw_po_file()`）
- 沒有 `_load_ap_invoice()` 的特殊處理分支
- 日誌訊息中的 PO/PR 字樣

這是一個**嚴重的 DRY 違反**，兩個類別合計大約 900 行，但實際差異不超過 50 行。如果兩個類別都繼承 `BaseLoadingStep`，只需各自實作 `get_required_file_type()`、`_load_primary_file()` 和 `_load_reference_data()` 三個抽象方法，程式碼量可以縮減至約 200 行。

#### 4.3.3 SPTProcurementDataLoadingStep / SPTProcurementPRDataLoadingStep

**繼承關係**：`BaseLoadingStep`（正確使用模板方法模式）

```python
class SPTProcurementDataLoadingStep(BaseLoadingStep):
    def get_required_file_type(self) -> str:
        return 'raw_po'

    async def _load_primary_file(self, source, path: str) -> Tuple[pd.DataFrame, int, int]:
        df = await source.read()
        date, month = self._extract_date_from_filename(path)
        return df, date, month

    def _extract_primary_data(self, primary_result) -> Tuple[pd.DataFrame, int, int]:
        return primary_result

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        # 暫時棄用：實際透過 file_configs 的自動載入完成
        return 0
```

這裡出現了一個有趣的現象：`_load_reference_data()` 方法體全部被注解掉，原因是採購前期底稿的路徑已經包含在 `file_configs` 字典中，`BaseLoadingStep` 的 `_load_all_files_concurrent()` 會自動載入所有在 `file_configs` 中的檔案，不需要在 `_load_reference_data()` 中額外處理。

這個「暫時棄用」的備注代表在架構設計時，開發者原本預期在此方法中手動載入前期底稿，但後來發現自動載入機制已經涵蓋了這個需求。

### 4.4 ERM 邏輯評估步驟

`SPTERMLogicStep` 是 SPT 模組最核心的業務邏輯步驟，包含 697 行程式碼，實作了 SPT 財務底稿的狀態判斷規則。

#### 4.4.1 ERMConditions Dataclass

```python
@dataclass
class ERMConditions:
    # 基礎條件組件
    no_status: pd.Series           # 尚未有狀態的記錄
    in_date_range: pd.Series       # ERM 在 Item Description 日期範圍內
    erm_before_or_equal_file_date: pd.Series  # ERM ≤ 結帳月
    erm_after_file_date: pd.Series  # ERM > 結帳月
    quantity_matched: pd.Series    # Entry Qty == Received Qty
    not_billed: pd.Series          # Entry Billed Amount == 0
    has_billing: pd.Series         # Billed Qty != '0'
    fully_billed: pd.Series        # Amount - Billed Amount == 0
    has_unpaid_amount: pd.Series   # Amount - Billed Amount != 0

    # 備註條件
    procurement_completed_or_rent: pd.Series  # 採購備註含「已完成|rent」
    fn_completed_or_posted: pd.Series  # 上月FN備註含「已完成|已入帳」
    pr_not_incomplete: pd.Series   # 上月FN PR備註不含「未完成」

    # FA 條件
    is_fa: pd.Series               # GL# 在 fa_accounts 清單中

    # 錯誤條件
    procurement_not_error: pd.Series  # 採購備註不等於 'error'
    out_of_date_range: pd.Series   # ERM 不在日期範圍（且非格式錯誤哨兵值）
    format_error: pd.Series        # YMs == '100001,100002'（格式錯誤哨兵值）
```

使用 Dataclass 組織 Boolean Series 集合的優點：
- 提高可讀性：條件名稱清楚描述業務含義
- 減少重複計算：各條件只計算一次，在多個狀態判斷中複用
- 方便測試：可以獨立測試 `_build_conditions()` 方法

**`no_status` 的可變狀態設計**

這是一個刻意的設計決策，但也是一個潛在的陷阱：

```python
# 在 _apply_status_conditions 中
condition_1 = df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False)
df.loc[condition_1, status_column] = '已入帳'
# 🔴 新增：更新 no_status
cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')

condition_2 = (
    (~df['GL DATE'].isna()) &
    (df['match_type'] == 'ITEM_TO_RECEIPT') &
    cond.no_status &  # 使用已更新的 no_status
    ...
)
```

在每個條件應用後，`cond.no_status` 都被重新計算，確保後面的條件不會對已有狀態的記錄重複賦值。雖然這個設計有效，但它利用了 dataclass 的可變性（dataclass 預設是可變的），並以一種隱式的方式傳遞狀態，在閱讀程式碼時需要特別注意每個 `cond.no_status` 在被使用時已被更新到哪個時間點。

#### 4.4.2 七個執行階段

```
階段 1: _set_file_date()             → 設置 '檔案日期' 欄位
階段 2: _get_status_column()         → 動態判斷 PO狀態/PR狀態
          _build_conditions()         → 計算 16 個 Boolean Series
階段 3: _apply_status_conditions()   → 11 個條件的狀態賦值
階段 4: _handle_format_errors()      → 處理格式錯誤哨兵值
階段 5: _set_accrual_flag()          → 設置 '是否估計入帳'
階段 6: _set_accounting_fields()     → 設置 Account code, Name, Product code, Region, Dep., Currency, Accr.Amount, 預付
階段 7: _check_pr_product_code()     → PR Product Code vs Project 一致性檢查
```

#### 4.4.3 11 個狀態條件的業務含義

| 條件序號 | 狀態值 | 業務含義 |
|---|---|---|
| 1 | `已入帳` | 上月 FN 備註明確標記「已入帳」 |
| 2 | `已入帳` | 有 GL DATE + ITEM_TO_RECEIPT 對帳 + 數量匹配 + 非 FA 科目 |
| 3 | `已完成(not_billed)` | 採購/FN 確認完成 + ERM 在範圍內且已到期 + 數量匹配 + 尚未請款 |
| 4 | `已完成(fully_billed)` | 同上，但已全額請款 |
| 5 | `已完成(partially_billed)` | 同上，但部分請款且有剩餘金額 |
| 5.1 | `已完成(not_billed)` | ERM 到期 + 數量匹配 + 未請款 + 有剩餘金額（不需要採購/FN 確認）|
| 6 | `Check收貨` | ERM 在範圍且已到期，但數量不匹配（需人工確認收貨狀態）|
| 7 | `未完成` | ERM 在範圍但尚未到期（ERM > 結帳月）|
| 8 | `error(...)_租金` | ERM 超出日期範圍且描述含「租金」|
| 9 | `error(...)_薪資` | ERM 超出日期範圍且描述含「派遣|Salary|Agency Fee」|
| 10 | `error(...)` | ERM 超出日期範圍（一般情況）|
| 11 | `部分完成ERM` | ERM 超出日期範圍，但已有部分收貨且數量未匹配 |

**條件 5 和 5.1 的命名衝突問題**

```python
# === 條件 5: 已完成(partially_billed) ===
condition_5 = (...)
df.loc[condition_5, status_column] = '已完成(partially_billed)'

cond.no_status = (df[status_column].isna()) | (df[status_column] == 'nan')

# === 條件 5.1: 已完成(not_billed) ===  ← 這裡仍使用 condition_5 作為變數名
condition_5 = (...)                         # 覆蓋了前面的 condition_5
df.loc[condition_5, status_column] = '已完成(not_billed)'
```

條件 5.1 的程式碼重用了 `condition_5` 這個變數名，對偵錯造成困擾，因為如果需要在兩個條件之間插入偵錯程式碼，很容易混淆哪個 `condition_5` 是哪個。

#### 4.4.4 會計欄位設定邏輯

```python
def _set_accounting_fields(self, df, ref_account, ref_liability):
    need_accrual = df['是否估計入帳'] == 'Y'

    # Account code = GL#（對於需估列的記錄）
    df.loc[need_accrual, 'Account code'] = df.loc[need_accrual, 'GL#']

    # Account Name = merge 到參考科目表
    df = self._set_account_name(df, ref_account, need_accrual)

    # Product code：只填充空值（分潤步驟可能已填入）
    product_isnull = df['Product code'].isna()
    df.loc[(need_accrual & product_isnull), 'Product code'] = (
        df.loc[(need_accrual & product_isnull), 'Product Code']  # 注意大小寫
    )

    # Region_c = 'TW'（SPT 固定值）
    df.loc[need_accrual, 'Region_c'] = "TW"

    # Dep.：依據 dept_accounts 配置決定取部門前3碼或固定 '000'
    df = self._set_department(df, need_accrual)

    # Accr.Amount = Unit Price × (Entry Qty - Billed Qty)
    df = self._calculate_accrual_amount(df, need_accrual)

    # 預付款：Entry Prepay Amount != '0' → 是否有預付='Y', Liability='111112'
    df = self._handle_prepayment(df, need_accrual, ref_liability)
```

**Product code vs Product Code 的大小寫問題**

在 `_set_accounting_fields()` 中，有兩個欄位：
- `'Product code'`：小寫 c，是輸出目標欄位（可能為空）
- `'Product Code'`：大寫 C，是原始 ERP 資料中的欄位名稱

這個細節很容易在閱讀時造成混淆，但這是 ERP 系統的命名習慣，不是 bug。

**Liability 科目邏輯**

```python
# 先從 ref_liability 表 merge 查找
merged = pd.merge(df, ref_liability[['Account', 'Liability']], ...)
df['Liability'] = merged['Liability_y']

# 有預付款的記錄強制覆蓋為 '111112'
df.loc[mask & is_prepayment, 'Liability'] = '111112'
```

`'111112'` 是預付款科目的固定代碼，被硬編碼在程式碼中（非配置）。

### 4.5 分潤與薪資偵測步驟

#### 4.5.1 CommissionDataUpdateStep（分潤更新）

此步驟處理 SPT 特有的分潤業務。分潤是蝦皮向品牌商收取的推薦費用（Affiliate Commission）和廣告服務費（AMS Commission）。

**類別級常數設定**

```python
COMMISSION_CONFIG = {
    'affiliate': {
        'keywords': r'(?i)Affiliate commission|Shopee commission|蝦皮分潤計劃會員分潤金',
        'exclude_keywords': ['品牌加碼'],
        'gl_number': '650022',
        'product_code': 'EC_SPE_COM',
        'remark': '分潤',
        'name': 'Affiliate/Shopee分潤'
    },
    'ams': {
        'keywords': r'(?i)AMS commission',
        'include_and_keywords': ['Affiliate分潤合作', '品牌加碼'],
        'gl_number': '650019',
        'product_code': 'EC_AMS_COST',
        'remark': '分潤',
        'name': 'AMS分潤'
    }
}
```

**Affiliate vs AMS 的識別邏輯**

```python
def _identify_commission_records(self, df):
    # Affiliate/Shopee 分潤：包含關鍵字 AND 不含「品牌加碼」
    affiliate_mask = df[self.description_column].str.contains(
        affiliate_config['keywords'], na=False, regex=True
    )
    for exclude_kw in affiliate_config['exclude_keywords']:
        affiliate_mask &= ~df[self.description_column].str.contains(exclude_kw, na=False)

    # AMS 分潤：情況1（AMS commission）OR 情況2（Affiliate分潤合作 AND 品牌加碼）
    ams_mask_1 = df[self.description_column].str.contains(
        ams_config['keywords'], na=False, regex=True
    )
    ams_mask_2 = (
        df[self.description_column].str.contains('Affiliate分潤合作', na=False) &
        df[self.description_column].str.contains('品牌加碼', na=False)
    )
    ams_mask = ams_mask_1 | ams_mask_2

    return affiliate_mask, ams_mask
```

這個邏輯解釋了為什麼「品牌加碼」的處理看起來矛盾：
- 「品牌加碼」在 affiliate 中是**排除條件**（排除「Affiliate commission + 品牌加碼」的組合）
- 但「Affiliate分潤合作 + 品牌加碼」的組合反而被識別為 **AMS** 分潤

這反映了業務上的一個細微區別：單純提及 Affiliate commission 的是 Shopee 分潤，但 Affiliate 分潤合作與品牌加碼組合出現時，是 AMS 廣告服務的分潤。

**估計入帳的後延設計**

```python
def _set_accrual_estimation(self, df):
    # 只有分潤記錄 AND 狀態已包含「已完成」 → 設定估計
    accrual_mask = (
        ((df['GL#'] == '650022') | (df['GL#'] == '650019')) &
        (df['Remarked by FN'] == '分潤') &
        (df[self.status_column].astype(str).str.contains('已完成', na=False))
    )
    df.loc[accrual_mask, '是否估計入帳'] = "Y"
```

注意：這個步驟在 `DateLogicStep` 之前執行，此時 `status_column` 可能尚未有完整的狀態值。因此，這個方法的實際效果在大多數情況下可能不會設定任何 `'Y'`，真正的估計入帳設定是在後面的 `SPTERMLogicStep` 中完成的。步驟的備注也說明了這一點：「此步驟僅標記 remark，後續依據狀態更新估計」。

**Entity Guard 設計**

```python
async def execute(self, context: ProcessingContext) -> StepResult:
    if context.metadata.entity_type != 'SPT':
        return self._create_skipped_result(
            context.data,
            "Commission update only applies to SPT entity",
            time.time() - start_time
        )
```

CommissionDataUpdateStep 和 PayrollDetectionStep 都在執行開始時檢查 `entity_type`，若非 SPT 則返回 `SKIPPED` 狀態。這個設計允許這兩個步驟在理論上被加入到非 SPT 的 Pipeline 中，而不會造成錯誤，只是會被跳過。

#### 4.5.2 PayrollDetectionStep（薪資偵測）

```python
PAYROLL_CONFIG = {
    'keywords': r'(?i)payroll',
    'label': 'Payroll',
    'name': 'Payroll標籤'
}
```

**非破壞性更新策略**

```python
# 只更新 Remarked by FN 為空的記錄
empty_remark_mask = (df[self.remark_column].isna() |
                     (df[self.remark_column] == '') |
                     (df[self.remark_column] == 'nan'))
update_mask = payroll_mask & empty_remark_mask
```

這確保已有備注（如「分潤」）的記錄不會被 Payroll 標籤覆蓋。這是一個保守的設計原則，優先尊重先前步驟已設定的標籤。

**狀態欄位的動態查找**

```python
status_column = [i for i in df.columns if '狀態' in i][0]
is_status_na = df[status_column].isna()
df.loc[update_mask & is_status_na, status_column] = self.PAYROLL_CONFIG['label']
```

這裡使用了一個危險的模式：假設 DataFrame 中恰好有一個包含「狀態」的欄位，若欄位不存在或有多個，`[0]` 會拋出 `IndexError`。這應該使用更健壯的方式（如先確認欄位存在）。

**EBS Task 的優雅降級**

```python
if self.ebs_task_column in df.columns:
    ebs_mask = df[self.ebs_task_column].astype(str).str.contains(...)
    payroll_mask |= ebs_mask
```

EBS Task 欄位不一定存在於所有資料集中。若不存在，步驟只查看 Item Description，不拋出錯誤，體現了良好的防禦性設計。

### 4.6 會計標籤步驟

`SPTStatusLabelStep` 實作了會計師（FN Accountant）的人工標籤規則，允許財務人員透過 TOML 配置定義業務例外情況。

#### 4.6.1 雙規則集架構

```
spt_status_label_rules.priority_conditions  →  同時更新「狀態欄位」和「備注欄位」
spt_status_label_rules.erm_conditions       →  只更新「備注欄位」（狀態由 ERM 決定）
```

這個區別的業務含義是：
- **優先級條件**：直接覆蓋 ERM 邏輯的狀態，例如採購人員確認已完成但 ERM 尚未到期的情況
- **ERM 條件**：不改變 ERM 計算出的狀態，只在備注欄位新增標記，供審核時參考

#### 4.6.2 十種條件類型

`_build_rule_condition()` 方法支援的 10 種條件類型：

| 條件類型 | TOML 鍵 | 匹配邏輯 |
|---|---|---|
| 關鍵字匹配 | `keywords` + `field` | `df[field].str.contains(keywords, regex=True)` |
| 供應商精確匹配 | `supplier` | `df[Supplier] == value` |
| 部門精確匹配 | `dept` | `df['Department'] == value` |
| 部門前綴匹配 | `dept_prefix` | `df['Department'].str.startswith(prefix)` |
| 部門非前綴匹配 | `dept_exclude_prefix` | `~df['Department'].str.startswith(prefix)` |
| 部門包含（regex） | `dept_include` | `df['Department'].str.contains(pattern)` |
| 部門不包含（regex） | `dept_exclude` | `~df['Department'].str.contains(pattern)` |
| 申請人精確匹配 | `requester` | `df[Requester] == value` |
| 狀態欄位包含 | `status_value_contains` | `df[status_col].str.contains(pattern)` |
| 採購備注精確匹配 | `remarked_by_procurement` | `df[Remarked by Procurement] == value` |

所有條件以 AND 方式組合，即一個規則內的所有條件必須同時滿足才算匹配。

**例外規則清單**

```python
exception_rules = ['exceed_period_but_pq_confirmed',
                   'check_qty_and_pq_confirmed',
                   'parsing_err_but_pq_confirmed',
                   'incomplete_but_pq_confirmed',
                   'hris_bug']
```

這 5 個規則名稱在配置驗證時被豁免「必須有 remark 欄位」的要求。這些規則的名稱暗示了它們的業務含義：
- `pq_confirmed`：採購已確認（Procurement Quotation Confirmed）
- `parsing_err_but_pq_confirmed`：解析錯誤但採購已確認
- `hris_bug`：HR 系統（HRIS）的資料問題

#### 4.6.3 可追蹤性設計

```python
df.loc[condition, self.remark_column] = remark
df.loc[condition, 'matched_condition_on_status'] = matched_condition  # 記錄觸發規則
```

除了更新業務欄位，步驟還會在 `matched_condition_on_status` 欄位中記錄觸發的規則名稱，為審核人員提供完整的規則觸發軌跡。

#### 4.6.4 動態狀態欄位偵測

```python
def _get_status_column(self, df):
    if 'PO狀態' in df.columns:
        return 'PO狀態'
    elif 'PR狀態' in df.columns:
        return 'PR狀態'
    else:
        df['PO狀態'] = pd.NA  # 自動創建
        return 'PO狀態'
```

此方法在 `execute()` 開始時呼叫，覆蓋了 `__init__` 時由 Orchestrator 注入的 `status_column`。這個覆蓋行為有些令人困惑，因為 `__init__` 已接收了明確的 `status_column` 參數，但 `execute()` 時又重新偵測。這可能導致 Orchestrator 注入的 `status_column` 被忽略。

### 4.7 科目預測步驟

`SPTAccountPredictionStep` 實作了一個輔助的科目預測功能，**不影響主要的財務狀態欄位**，只新增 `predicted_account` 和 `matched_conditions` 兩個欄位供審核參考。

#### 4.7.1 AccountPredictionConditions Dataclass

```python
@dataclass
class AccountPredictionConditions:
    matched: pd.Series  # 追蹤已匹配的記錄（防止重複匹配）
```

相較於 `ERMConditions` 的 16 個欄位，`AccountPredictionConditions` 只有一個欄位。它的作用是在多個規則的迭代過程中，追蹤哪些記錄已被匹配，確保**第一個匹配的規則優先**（first-match-wins）。

#### 4.7.2 規則條件類型

```python
def _build_rule_condition(self, df, rule, already_matched):
    condition = ~already_matched  # 從未匹配的記錄開始

    if 'departments' in rule:
        condition &= df['Department'].isin(rule['departments'])

    if 'supplier' in rule:
        supplier_col = df.filter(regex='(?i)supplier').columns[0]
        condition &= df[supplier_col] == rule['supplier']

    if 'description_keywords' in rule:
        condition &= df['Item Description'].str.contains(
            rule['description_keywords'], case=False, na=False
        )

    if 'max_amount' in rule:
        condition &= pd.to_numeric(df['Entry Amount'], errors='coerce') < rule['max_amount']

    return condition
```

四種條件類型：部門清單、供應商、描述關鍵字、金額上限。這比 `SPTStatusLabelStep` 的 10 種簡單許多，反映了科目預測只是一個輔助功能，不需要處理複雜的例外情況。

#### 4.7.3 Rollback 支援

```python
async def rollback(self, context, error):
    if context.data is not None:
        columns_to_remove = ['predicted_account', 'matched_conditions']
        for col in columns_to_remove:
            if col in context.data.columns:
                context.data.drop(col, axis=1, inplace=True)
```

`SPTAccountPredictionStep` 是少數實作了 `rollback()` 方法的步驟，當後續步驟失敗需要回滾時，可以清理此步驟新增的欄位。

### 4.8 後處理步驟

`SPTPostProcessingStep` 繼承 `BasePostProcessingStep`，實作了 11 個處理階段：

#### 4.8.1 11 個處理階段

```python
def _process_data(self, df, context):
    # 1. 格式化數值列
    df = self._format_numeric_columns(df)    # Line#→Int64, 浮點數→round(2)

    # 2. 格式化日期列
    df = self._reformat_dates(df)             # → '%Y-%m-%d'

    # 3. 清理 nan 值
    df = self._clean_nan_values(df)           # 'nan'/'<NA>' → pd.NA

    # 4. 重新排列欄位（第一次）
    df = self._rearrange_columns(df)          # FN 備注相鄰, 審核欄到末尾

    # 5. 添加分類
    df = self._add_classification(df)         # classify_description()

    # 6. 重新命名欄位
    df = self._rename_columns_dtype(df)       # Product code → product_code_c, clean_po_data()

    # 7. 確保 review 欄位在最後（第二次）
    df = self._rearrange_columns(df)          # 重複呼叫

    # 8. 保存含臨時欄位的完整資料
    self._save_temp_columns_data(df, context) # → auxiliary_data['result_with_temp_cols']

    # 9. 移除臨時欄位
    df = self._remove_temp_columns(df, processing_type)

    # 10. 格式化 ERM
    df = self._reformat_erm(df)               # 'MMM-YY' → 'YYYY/MM'

    # 11. 重排審核欄位
    df = self._rearrange_reviewer_col(df)     # pop + assign（含 BUG）

    return df
```

**步驟 7 是步驟 4 的重複**

`_rearrange_columns()` 被呼叫了兩次（步驟 4 和步驟 7）。這可能是因為步驟 5 和 6 可能引入新欄位，需要重新排列；但兩次呼叫的邏輯完全相同，代表步驟 4 的排列效果在步驟 6 後可能會被打亂。這個重複呼叫的設計可能有其必要性，但文件說明不足。

**`_save_temp_columns_data()` 的設計意圖**

步驟 8 在移除臨時欄位之前，將完整的 DataFrame（含所有臨時欄位）儲存到 `auxiliary_data['result_with_temp_cols']`。這個設計允許後續（如 DataShapeSummary 步驟或偵錯流程）仍能存取已被移除的臨時計算欄位。

**`_rearrange_reviewer_col()` 的潛在 KeyError**

```python
def _rearrange_reviewer_col(self, df):
    a = df.pop('previous_month_reviewed_by')   # 若欄位不存在 → KeyError！
    b = df.pop('current_month_reviewed_by')    # 若欄位不存在 → KeyError！
    df = df.assign(
        previous_month_reviewed_by=a,
        current_month_reviewed_by=b
    )
    return df
```

`pd.DataFrame.pop()` 在欄位不存在時會拋出 `KeyError`，而此方法沒有任何防護措施。如果這兩個欄位（`previous_month_reviewed_by` 和 `current_month_reviewed_by`）因某種原因不存在於 DataFrame 中，整個後處理步驟都會失敗，且錯誤訊息不夠直觀。

#### 4.8.2 ERM 格式轉換

```python
def _reformat_erm(self, df):
    if 'expected_receive_month' in df.columns:
        df['expected_receive_month'] = (
            pd.to_datetime(df['expected_receive_month'], format='%b-%y', errors='coerce')
            .dt.strftime('%Y/%m')
        )
```

注意欄位名稱：`expected_receive_month`（小寫、有底線），這是步驟 6 的 `_rename_columns_dtype()` → `clean_po_data()` 重命名後的結果（原始欄位 `Expected Receive Month` 已被重命名）。

### 4.9 孤兒步驟群分析

`spt_steps.py` 中有 4 個步驟（約 440 行）**未被** Orchestrator 的 Step Registry 收錄，也不在任何 `enabled_*_steps` 配置清單中：

| 類別 | 功能描述 | 行數 |
|---|---|---|
| `SPTStatusStep` | 基於數量比率的狀態判斷（0.8/0.5 閾值）| 約 133 行 |
| `SPTDepartmentStep` | 部門代碼轉換邏輯 | 88 行 |
| `SPTAccrualStep` | 預估入帳邏輯 | 91 行 |
| `SPTValidationStep` | 業務規則驗證 | 103 行 |

**SPTStatusStep 的狀態判斷邏輯**

```python
def _evaluate_spt_status(self, row, processing_date):
    expected_yyyymm = int(expected_month.replace('-', ''))

    if expected_yyyymm > processing_date:
        if received_qty >= entry_qty:
            return '提早完成'
        else:
            return '未到期'
    else:
        if received_qty >= entry_qty:
            return '已完成'
        elif completion_rate >= 0.8:
            return '接近完成'
        elif completion_rate >= 0.5:
            return '部分收貨'
        else:
            return '少量收貨'
```

這是一個基於**收貨比率**的狀態判斷，邏輯比 `SPTERMLogicStep` 的 11 條件更簡單，但也更粗糙（忽略了 YMs 日期範圍、帳務狀態等細節）。這個步驟可能是在 ERM 邏輯開發完成之前的早期版本原型。

**SPTAccrualStep 的特殊邏輯**

```python
# 跨月項目強制全額估列
if '跨月標記' in df.columns:
    cross_month = df['跨月標記'] == 'Y'
    df.loc[cross_month, '是否估計入帳'] = 'Y'
    df.loc[cross_month, 'Accrual Note'] = '跨月項目-全額預估'

# 採購備注否決估列
if '採購備註' in df.columns:
    no_accrual_mask = df['採購備註'].str.contains(
        '不預估|不估計|勿估|暫緩', case=False, na=False
    )
    df.loc[no_accrual_mask, '是否估計入帳'] = 'N'
```

`SPTAccrualStep` 中的「跨月標記」和「採購備注否決」邏輯是 `SPTStatusLabelStep` 和 `SPTERMLogicStep` 的更早期、更簡陋的版本。

**孤兒步驟的問題**

這 4 個步驟仍然存在於 `steps/__init__.py` 的 `__all__` 中並被匯出，表示它們雖然未被使用，但也未被標記為廢棄或刪除。這造成了幾個問題：
1. 增加維護負擔（新開發者可能不知道這些步驟是否有效）
2. 測試套件需要決定是否要為這些步驟撰寫測試
3. 若有人手動嘗試將這些步驟加入 Pipeline，可能因為依賴未設置的欄位而失敗

### 4.10 採購流程步驟群

#### 4.10.1 ColumnInitializationStep（欄位初始化）

```python
async def execute(self, context):
    df = context.data.copy()

    if self._is_pr(context):
        self.status_column = "PR狀態"
        df['PR Line'] = df['PR#'].fillna('') + '-' + df['Line#'].fillna('')
        df['Supplier'] = df['PR Supplier']
    else:
        df['PO Line'] = df['PO#'].fillna('') + '-' + df['Line#'].fillna('')
        df['Supplier'] = df['PO Supplier']

    if self.status_column not in df.columns:
        df[self.status_column] = pd.NA

    df['Remarked by Procurement'] = pd.NA
    df['Noted by Procurement'] = pd.NA
```

這個步驟的核心是確保後續步驟所需的欄位都存在，特別是：
- `PO Line` / `PR Line`：用作前期底稿映射的鍵值
- `Supplier`：統一的供應商欄位名（PR Supplier 和 PO Supplier 的統一化）
- `PO狀態` / `PR狀態`：狀態欄位（為空）
- `Remarked by Procurement` / `Noted by Procurement`：採購備注欄（為空）

`_is_pr()` 的邏輯很簡單：

```python
def _is_pr(self, context) -> bool:
    if 'PR' in self.status_column:
        return True
    else:
        return False
```

這裡依賴的是 `status_column` 的字串內容，而非 `context.metadata.processing_type`。如果 Orchestrator 傳入了 `status_column="PO狀態"` 但處理的實際上是 PR 資料，這個判斷就會出錯。不過在實際使用中，Orchestrator 會根據 `source_type` 正確設定 `status_column`，所以這個問題不太可能出現。

#### 4.10.2 ProcurementPreviousMappingStep（前期底稿映射）

這個步驟是採購專用的前期底稿整合，類似但有別於 FN 使用的 `PreviousWorkpaperIntegrationStep`。

**自動修復前期底稿缺少 PO/PR Line 的邏輯**

```python
def _fix_missing_mapping_key(self, df, processing_type):
    if processing_type == 'po':
        processing_type = 'PO#'
    elif processing_type == 'pr':
        processing_type = 'PR#'

    if processing_type[:2] + ' Line' not in df.columns:
        df[f'{processing_type[:2]} Line'] = (
            df[processing_type].astype('string') + df['Line#'].astype('string')
        )
    return df
```

採購人員的前期底稿可能不包含 `PO Line` 欄位（有時只有 `PO#` 和 `Line#` 分開存儲），此方法自動建立串接後的鍵值欄位。

**fill_na vs override 映射模式**

```python
if fill_na:
    # 只填充空值：使用來源資料的值填充目標欄位中的空白
    df[target_col] = df[df_key].map(mapping_dict).fillna(pd.NA)
else:
    # 允許覆蓋：先映射，再用原值填充空的映射結果
    mapped = df[df_key].map(mapping_dict)
    if target_col in df.columns:
        df[target_col] = mapped.fillna(df[target_col])
    else:
        df[target_col] = mapped
```

TOML 配置中每個映射欄位可以指定 `fill_na: true/false`：
- `fill_na=true`（預設）：只填充目前為空的值，不覆蓋已有的值
- `fill_na=false`：優先使用前期底稿的值，若前期底稿沒有則保留原值

#### 4.10.3 SPTProcurementStatusEvaluationStep（採購狀態評估）

這是採購 Pipeline 的核心業務步驟，**完全配置驅動**：

```python
def _load_conditions_from_config(self):
    config = config_manager._config_toml.get('spt_procurement_status_rules', {})
    self.conditions = config.get('conditions', [])
    self.conditions = sorted(self.conditions, key=lambda x: x.get('priority', 999))
```

**重置邏輯**

```python
# 重置狀態欄位，common的DateLogicStep有預先給狀態，採購端不看該條件。
df[self.status_column] = pd.NA
```

`DateLogicStep` 在前面已經設置了一些狀態值，但採購 Pipeline 使用不同的狀態判斷邏輯，因此直接清空重來。

**7 種 check 類型**

| check_type | 說明 |
|---|---|
| `contains` | 欄位正則包含 |
| `not_contains` | 欄位正則不包含 |
| `equals` | 欄位精確等於 |
| `not_equals` | 欄位精確不等於 |
| `erm_in_range` | ERM 在 Item Description 日期範圍內 |
| `erm_le_closing` | ERM ≤ 結帳月 |
| `erm_gt_closing` | ERM > 結帳月 |

多個 checks 可以用 `combine: and/or` 組合，提供了相當彈性的規則定義能力。

**`_simple_clean()` 的欄位移除**

```python
def _simple_clean(self, df):
    remove_cols = [
        'Supplier',
        'Expected Received Month_轉換格式',
        'YMs of Item Description',
        '是否估計入帳'
    ]
    for col in remove_cols:
        if col in df_copy.columns:
            df_copy.pop(col)
    return df_copy
```

採購報告不需要顯示這些計算中間欄位，因此在輸出前清除。注意 `'是否估計入帳'` 也被移除，表示採購 Pipeline 的輸出不包含估計入帳欄位（與 FN Pipeline 不同）。

#### 4.10.4 ProcurementPreviousValidationStep（前期底稿驗證）

此步驟設計了一個值得注意的 **嚴格/寬鬆模式**：

```python
def __init__(self, name, strict_mode: bool = False, **kwargs):
    self.strict_mode = strict_mode
```

當 `strict_mode=False`（預設）時，驗證失敗返回 `SKIPPED`，Pipeline 繼續執行。當 `strict_mode=True` 時，驗證失敗返回 `FAILED`，Pipeline 中止。

Orchestrator 以 `strict_mode=False` 建立此步驟，因此即使前期底稿格式有問題，Pipeline 也不會停止，只是會在後續映射步驟中因找不到資料而跳過。

`validate_input()` 方法有一個特殊設計：

```python
async def validate_input(self, context):
    return True  # 總是返回 True，因為驗證邏輯在 execute 中
```

這與大多數步驟的設計相反（通常 `validate_input()` 做前置檢查，`execute()` 做主要邏輯）。這裡將驗證邏輯放在 `execute()` 中，是因為驗證結果本身就是此步驟的輸出，而非前置條件。

### 4.11 COMBINED 模式步驟群

#### 4.11.1 CombinedProcurementDataLoadingStep（COMBINED 載入）

COMBINED 模式載入步驟繼承 `PipelineStep`（非 `BaseLoadingStep`），因為它需要同時載入 PO 和 PR 兩個主要資料集，這不符合 `BaseLoadingStep` 的「一個主要資料集 + 參考資料」設計。

**關鍵設計決策：空的 context.data**

```python
# 設置主 data 為空 DataFrame（後續步驟會分別處理 PO 和 PR）
context.update_data(pd.DataFrame())
```

COMBINED 模式下，`context.data` 保持為空，所有資料都儲存在 `auxiliary_data` 中：
- `context.auxiliary_data['po_data']` = PO DataFrame
- `context.auxiliary_data['pr_data']` = PR DataFrame
- `context.auxiliary_data['procurement_previous_po']` = 前期底稿 PO sheet
- `context.auxiliary_data['procurement_previous_pr']` = 前期底稿 PR sheet

**DataSourceFactory 的呼叫差異**

```python
# 注意：這裡使用的是 async DataSourceFactory.create_source()
# 而其他步驟通常使用 sync DataSourceFactory.create_from_file()
source = await DataSourceFactory.create_source(file_path, **params)
```

在同一個系統中混用了 `create_source()`（async）和 `create_from_file()`（sync），這可能造成混淆，因為兩者的使用場合和特性不同。

#### 4.11.2 CombinedProcurementProcessingStep（COMBINED 處理）

這個步驟的核心是 Sub-Context 模式（見 3.5 節）。主要邏輯在 `_process_po_data()` 和 `_process_pr_data()` 中。

**步驟失敗的靜默處理**

```python
result = await step.execute(sub_context)
if result.status == StepStatus.FAILED:
    self.logger.error(f"PO sub-step failed: {step.name} - {result.message}")
    return None  # 靜默返回 None，不拋出異常
```

當子步驟失敗時，方法返回 `None`，而不是拋出異常或傳遞 FAILED 狀態。父步驟（`CombinedProcurementProcessingStep`）在收到 `None` 時會記錄警告，但仍繼續執行另一個類型（PO 失敗時仍嘗試 PR）。這種設計允許 PO 和 PR 其中一個失敗時，另一個仍能完成，但也意味著失敗資訊的傳遞不夠清晰。

**缺少前期底稿的 `processing_date` 傳遞**

```python
file_date = parent_context.metadata.processing_date
sub_context.set_variable('file_date', file_date)
```

注意：sub_context 的 `metadata.processing_date` 沒有被設置，只有透過 `set_variable('file_date', ...)` 傳遞。`SPTProcurementStatusEvaluationStep` 使用 `context.metadata.processing_date`，但 sub_context 的 metadata 可能為預設值（0 或 None），可能導致狀態判斷錯誤。這是一個潛在的 bug，需要確認 `SPTProcurementStatusEvaluationStep` 實際上從哪裡取得 `file_date`。

#### 4.11.3 CombinedProcurementExportStep（COMBINED 匯出）

**重試機制**

```python
for attempt in range(self.retry_count):  # 預設 3 次
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            po_result.to_excel(writer, sheet_name='PO', ...)
            pr_result.to_excel(writer, sheet_name='PR', ...)
        success = True
        break

    except PermissionError as e:
        # 檔案被鎖定（例如 Excel 已開啟）
        output_path = self._prepare_output_path(context, suffix=f"_{attempt + 1}")
        # 第二次嘗試用 _1 後綴，第三次用 _2 後綴
```

這個重試機制特別處理了 Windows 環境下 Excel 檔案被佔用的常見問題：當輸出路徑的 Excel 檔案已在 Excel 程式中開啟時，會使用帶後綴的替代檔名（如 `202501_PROCUREMENT_COMBINED_1.xlsx`）。

**`_prepare_output_path()` 的模板替換**

```python
filename = self.filename_template.replace('{YYYYMM}', yyyymm)
```

若 `processing_date` 為 None（例如 COMBINED 模式載入失敗），則使用當前日期作為備用：

```python
if file_date:
    yyyymm = str(file_date)
else:
    yyyymm = datetime.now().strftime('%Y%m')
```

---

## 五、應用範例

### 5.1 建立並執行 PO Pipeline

```python
import asyncio
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext

async def run_spt_po():
    orchestrator = SPTPipelineOrchestrator()

    # 新格式（推薦）：同時指定路徑和讀取參數
    file_paths = {
        'raw_po': {
            'path': '/data/202501/202501_purchase_order.csv',
            'params': {'encoding': 'utf-8', 'sep': ',', 'dtype': 'str'}
        },
        'previous': {
            'path': '/data/202501/前期底稿/202412_PO_FN.xlsx',
            'params': {'sheet_name': 0, 'header': 0, 'dtype': 'str'}
        },
        'procurement_po': {
            'path': '/data/202501/採購備注/202501_procurement.xlsx',
            'params': {'sheet_name': 'PO', 'header': 0}
        },
        'ap_invoice': {
            'path': '/data/202501/AP_Invoice_202501.xlsx',
            'params': {'sheet_name': 1, 'header': 1}
        }
    }

    # 建立 Pipeline
    pipeline = orchestrator.build_po_pipeline(file_paths=file_paths)

    # 建立初始 Context
    context = ProcessingContext(
        entity_type='SPT',
        processing_type='PO',
        processing_date=202501
    )

    # 執行 Pipeline
    result = await pipeline.execute(context)

    if result.is_success:
        print(f"PO Pipeline 執行成功，共 {len(context.data)} 筆記錄")
    else:
        print(f"Pipeline 失敗：{result.error_message}")

asyncio.run(run_spt_po())
```

### 5.2 建立 PR Pipeline

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator

orchestrator = SPTPipelineOrchestrator()

file_paths = {
    'raw_pr': {
        'path': '/data/202501/202501_purchase_requisition.xlsx',
        'params': {'sheet_name': 0, 'dtype': 'str'}
    },
    'previous_pr': {
        'path': '/data/202501/前期底稿/202412_PR_FN.xlsx',
        'params': {'sheet_name': 0, 'dtype': 'str'}
    }
}

pipeline = orchestrator.build_pr_pipeline(file_paths=file_paths)
```

### 5.3 建立 PROCUREMENT Pipeline（PO/PR/COMBINED）

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator

orchestrator = SPTPipelineOrchestrator()

# COMBINED 模式的 file_paths 格式
file_paths = {
    'raw_po': '/data/202501/PO_data.csv',
    'raw_pr': '/data/202501/PR_data.xlsx',
    'procurement_previous': '/data/202501/前期採購底稿/202412_PROCUREMENT.xlsx',
    # 注意：使用 procurement_previous（非 _po/_pr），
    # 因為 COMBINED 模式的 _normalize_procurement_paths 會保留所有路徑
}

# 建立 COMBINED Pipeline
pipeline = orchestrator.build_procurement_pipeline(
    file_paths=file_paths,
    source_type='COMBINED'
)

# 建立 PO 專用 Pipeline（內部會移除 raw_pr 並重命名路徑）
# 傳入的 file_paths 可以包含 procurement_previous_po / procurement_previous_pr
file_paths_with_split = {
    'raw_po': '/data/202501/PO_data.csv',
    'raw_pr': '/data/202501/PR_data.xlsx',  # 會被移除
    'procurement_previous_po': '/data/202501/前期採購底稿/202412_PROCUREMENT.xlsx',
    'procurement_previous_pr': '/data/202501/前期採購底稿/202412_PROCUREMENT.xlsx',  # 會被移除
}
pipeline_po = orchestrator.build_procurement_pipeline(
    file_paths=file_paths_with_split,
    source_type='PO'
)
```

### 5.4 查詢啟用的步驟清單

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator

orchestrator = SPTPipelineOrchestrator()

# 查詢 PO Pipeline 的步驟
po_steps = orchestrator.get_enabled_steps('PO')
print(f"PO 步驟清單：{po_steps}")

# 查詢 PR Pipeline 的步驟
pr_steps = orchestrator.get_enabled_steps('PR')

# 查詢採購 COMBINED Pipeline 的步驟
combined_steps = orchestrator.get_enabled_steps('PROCUREMENT', source_type='COMBINED')

# 查詢採購 PR Pipeline 的步驟
procurement_pr_steps = orchestrator.get_enabled_steps('PROCUREMENT', source_type='PR')
```

### 5.5 自訂步驟注入

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext

class MyCustomStep(PipelineStep):
    """自訂後處理步驟"""

    def __init__(self):
        super().__init__(name="MyCustomStep")

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()
        # 自訂邏輯
        df['custom_flag'] = 'processed'
        context.update_data(df)
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message="Custom processing done"
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return context.data is not None

# 注入自訂步驟（會被附加在 TOML 配置步驟之後）
orchestrator = SPTPipelineOrchestrator()
pipeline = orchestrator.build_po_pipeline(
    file_paths=file_paths,
    custom_steps=[MyCustomStep()]
)
```

### 5.6 從 UnifiedPipelineService 呼叫

```python
from accrual_bot.ui.services import UnifiedPipelineService

service = UnifiedPipelineService()

# 查詢 SPT 支援的處理類型
types = service.get_entity_types('SPT')
# 返回：['PO', 'PR', 'PROCUREMENT']

# 查詢啟用的步驟
steps = service.get_enabled_steps('SPT', 'PO')

# 建立並執行 PO Pipeline
pipeline = service.build_pipeline(
    entity='SPT',
    proc_type='PO',
    file_paths={'raw_po': '/path/to/file.csv'},
    processing_date=202501
)
```

---

## 六、優缺分析

### 6.1 設計優點

#### 配置驅動的彈性

SPT 模組最大的優點是其高度的配置驅動設計。從 Pipeline 步驟清單到個別步驟的業務規則，幾乎所有業務邏輯都可以透過 TOML 配置調整，無需修改程式碼：

| 可配置項目 | 配置位置 | 影響範圍 |
|---|---|---|
| Pipeline 步驟清單 | `stagging_spt.toml [pipeline.spt]` | 整個 Pipeline 的結構 |
| ERM 條件 | `stagging_spt.toml [spt_status_label_rules]` | 所有 ERM 後的標籤標記 |
| 科目預測規則 | `stagging_spt.toml [spt_account_prediction]` | 科目預測的準確性 |
| 採購狀態評估條件 | `stagging_spt.toml [spt_procurement_status_rules]` | 採購 Pipeline 的狀態判斷 |
| 採購前期底稿映射 | `stagging_spt.toml [spt_procurement_previous_mapping]` | 前期資料的欄位對應 |

#### 完整的非同步支援

所有步驟的 `execute()` 方法都是 `async`，`asyncio.gather()` 用於並發載入多個檔案，整個執行鏈完全非同步，不會阻塞 UI 的事件迴圈。

#### 標準化的 Metadata

每個步驟使用 `StepMetadataBuilder` 建立標準化的 metadata，包含執行時間、處理行數、成功/失敗計數等資訊，便於 UI 層顯示進度和統計。

#### 多層次容錯設計

- 可選檔案載入失敗不中斷 Pipeline
- `_cleanup_resources()` 在 `finally` 中確保資源釋放
- `ProcurementPreviousValidationStep` 的寬鬆模式
- COMBINED 模式下 PO/PR 互相獨立，一個失敗不影響另一個

#### Sub-Context 的優雅複用

COMBINED 模式透過 Sub-Context 模式完整複用了現有的採購處理步驟，沒有程式碼重複，這是一個設計上的亮點。

### 6.2 已知問題與缺陷

> **2026-03-17 更新**：BUG-2 至 BUG-7 均已修復（8 個 SPT fixes）。BUG-1 仍為開放狀態，待後續處理。

#### BUG-1：`_load_ap_invoice` 使用錯誤實體識別碼

**位置**：`spt_loading.py`，`SPTBaseDataLoadingStep._load_ap_invoice()`

```python
# 錯誤：使用 'SPX'（已知 BUG，待修復）
df = await source.read(
    usecols=config_manager.get_list('SPX', 'ap_columns'),  # 應為 'SPT'
    ...
)
```

**影響**：當 SPT 和 SPX 的 `ap_columns` 配置不同時，SPT AP 發票的欄位選取會錯誤，導致讀取到錯誤的欄位或 KeyError。

**修復方向**：將 `'SPX'` 改為 `'SPT'`。

#### ~~BUG-2：`_rearrange_reviewer_col` 缺少防護~~ ✅ 已修復（2026-03-17）

**位置**：`spt_steps.py`，`SPTPostProcessingStep._rearrange_reviewer_col()`

**修復內容**：加入欄位存在檢查，缺少 `previous_month_reviewed_by` 或 `current_month_reviewed_by` 時 log warning 並提前返回，不再拋出 `KeyError`。

```python
def _rearrange_reviewer_col(self, df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ['previous_month_reviewed_by', 'current_month_reviewed_by']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        self.logger.warning(f"_rearrange_reviewer_col: 缺少欄位 {missing}，跳過排序")
        return df
    a = df.pop('previous_month_reviewed_by')
    b = df.pop('current_month_reviewed_by')
    df = df.assign(previous_month_reviewed_by=a, current_month_reviewed_by=b)
    return df
```

#### ~~BUG-3：`df.rename()` 遺漏賦值~~ ✅ 已修復（2026-03-17）

**位置**：原 `spt_loading.py`，`_load_raw_po_file()` 和 `_load_raw_pr_file()`

**修復內容**：透過 Fix 5（DRY 重構）一併修復。兩個步驟現在繼承 `SPTBaseDataLoadingStep`，主檔載入改由 `_load_primary_file()` 呼叫 `BaseLoadingStep._process_common_columns()`，其中 `df = df.rename(...)` 已正確賦值。原有問題程式碼已從 1164 行的舊檔案完全移除。

#### ~~BUG-4：`PayrollDetectionStep` 的狀態欄位動態查找不安全~~ ✅ 已修復（2026-03-17）

**位置**：`spt_evaluation_affiliate.py`，`PayrollDetectionStep.execute()`

**修復內容**：先取清單再判斷是否為空，空時 log warning 並跳過狀態更新，不再拋出 `IndexError`。

```python
status_cols = [i for i in df.columns if '狀態' in i]
if not status_cols:
    self.logger.warning("未找到含'狀態'的欄位，跳過狀態欄位更新")
else:
    status_column = status_cols[0]
    is_status_na = df[status_column].isna()
    df.loc[update_mask & is_status_na, status_column] = self.payroll_config['label']
```

#### ~~BUG-5：警告使用 `print()` 而非 logger~~ ✅ 已修復（2026-03-17）

**位置**：`pipeline_orchestrator.py`，`_create_step()`

**修復內容**：`SPTPipelineOrchestrator.__init__` 加入 `self.logger = get_logger(__name__)`；`print()` 改為 `self.logger.warning()`，警告現在進入統一日誌框架。

### 6.3 技術債務盤點

> **2026-03-17 更新**：高嚴重度及多個低嚴重度項目已修復。

| 項目 | 嚴重程度 | 說明 |
|---|---|---|
| ~~`SPTDataLoadingStep` 和 `SPTPRDataLoadingStep` 的重複程式碼~~ | ~~高~~ | ~~約 1164 行中有 1000+ 行重複，應重構為繼承 `BaseLoadingStep`~~ ✅ **已修復（2026-03-17）**：引入 `SPTBaseDataLoadingStep`，檔案縮減至 182 行 |
| ~~4 個孤兒步驟留在程式碼庫中~~ | ~~中~~ | ~~造成混淆，應刪除或明確標記為廢棄~~ ✅ **已修復（2026-03-17）**：從 `__all__` 移除，各類別加廢棄標注 |
| 跨實體依賴（SPT → SPX） | 中 | 違反模組獨立性，若 SPX 重構可能影響 SPT |
| ~~`COMMISSION_CONFIG` 和 `PAYROLL_CONFIG` 硬編碼~~ | ~~低~~ | ~~相較其他配置驅動的設計，這些常數仍在程式碼中~~ ✅ **已修復（2026-03-17）**：移至 `stagging_spt.toml` `[spt.commission.*]` / `[spt.payroll]` |
| ~~Sub-Context 的 `processing_date` 傳遞問題~~ | ~~低~~ | ~~COMBINED 模式可能使用錯誤的處理日期進行狀態判斷~~ ✅ **已修復（2026-03-17）**：三個 metadata 欄位均完整傳播 |
| `_load_reference_data` 的 Colab 路徑邏輯 | 低 | 歷史遺留程式碼，生產環境無需此路徑 |

---

## 七、延伸議題

### 7.1 ~~重構建議：載入層統一~~ ✅ 已完成（2026-03-17）

> **2026-03-17**：此建議已實施。

原本 SPT 模組中存在兩種截然不同的載入架構，現已統一：

| 載入步驟 | 繼承自 | 並發方式 | 資源管理 |
|---|---|---|---|
| `SPTDataLoadingStep` | `SPTBaseDataLoadingStep` → `BaseLoadingStep` | 由 Base 管理 | 由 Base 管理 |
| `SPTPRDataLoadingStep` | `SPTBaseDataLoadingStep` → `BaseLoadingStep` | 由 Base 管理 | 由 Base 管理 |
| `SPTProcurementDataLoadingStep` | `BaseLoadingStep` | 由 Base 管理 | 由 Base 管理 |
| `SPTProcurementPRDataLoadingStep` | `BaseLoadingStep` | 由 Base 管理 | 由 Base 管理 |

**實施結果**：引入 `SPTBaseDataLoadingStep(BaseLoadingStep)` 中間抽象層，兩個子類各只需宣告 `get_required_file_type()`（返回 `'raw_po'` 或 `'raw_pr'`）。`spt_loading.py` 從 1164 行縮減至 182 行（84% 減少）。`df.rename()` 靜默 bug（BUG-3）在此過程中一併修復。

### 7.2 ~~孤兒步驟的處置策略~~ ✅ 已完成（2026-03-17）

> **2026-03-17**：採用方案一（標記廢棄）處理。

四個孤兒步驟（`SPTStatusStep`、`SPTDepartmentStep`、`SPTAccrualStep`、`SPTValidationStep`）已從 `spt/steps/__init__.py` 的 `__all__` 移除，並在各類別的 docstring 加入廢棄標注，說明對應的現行替代步驟。類別本身保留於 `spt_steps.py` 作為業務邏輯參考。

### 7.3 跨實體耦合的影響分析

SPT Orchestrator 直接依賴 SPX 的 4 個步驟：

| SPX 步驟 | 在 SPT 中的用途 | 若 SPX 修改此步驟的影響 |
|---|---|---|
| `SPXPRExportStep` | SPT PO/PR 的匯出、採購匯出 | 匯出格式改變 |
| `SPXPRERMLogicStep` | SPT PR 的 ERM 邏輯 | PR 狀態判斷邏輯改變 |
| `ColumnAdditionStep` | SPT PO/PR 的欄位新增 | 欄位定義改變 |
| `APInvoiceIntegrationStep` | SPT PO/PR 的 AP 整合 | AP 整合邏輯改變 |

**降低耦合的方案**：

1. 將這 4 個步驟移至 `core/pipeline/steps/common.py` 或 `tasks/common/`，作為共用步驟，不再屬於任何特定實體
2. 若 SPT 和 SPX 的邏輯有差異，可在 `tasks/spt/steps/` 中建立 `spt_export.py` 等步驟，繼承 SPX 版本並覆寫差異部分

### 7.4 ERM 條件的可測試性

`SPTERMLogicStep._build_conditions()` 建立了 16 個 Boolean Series，每一個代表一個業務條件。目前的測試（`test_spt_evaluation_erm.py`）主要測試整個 `execute()` 方法的輸出，而非個別條件。

**建議的單元測試策略**：

```python
# 測試個別條件的正確性
class TestERMConditions:
    def test_in_date_range(self, sample_df):
        conditions = step._build_conditions(sample_df, 202501, 'PO狀態')
        # ERM = 202501，YMs = '202501,202503' → should be True
        assert conditions.in_date_range[0] == True

    def test_format_error_detection(self, sample_df):
        sample_df['YMs of Item Description'] = '100001,100002'
        conditions = step._build_conditions(sample_df, 202501, 'PO狀態')
        assert conditions.format_error.all()

    def test_no_status_updates_between_conditions(self, sample_df):
        # 測試 cond.no_status 在每個條件後正確更新
        ...
```

### 7.5 採購前期底稿載入的架構演進

目前 `SPTProcurementDataLoadingStep._load_reference_data()` 被完全注解（標注為「暫時棄用」），原因是自動載入機制已涵蓋此需求。但這個「暫時」狀態已持續了相當長的時間，造成了程式碼中存在大量無效的注解程式碼。

**建議的清理方向**：

1. 移除注解程式碼，只保留空方法 `return 0` 加上清晰的說明：

```python
async def _load_reference_data(self, context: ProcessingContext) -> int:
    """
    採購前期底稿已透過 BaseLoadingStep._load_all_files_concurrent()
    中的自動載入機制處理（file_configs 中的 'procurement_previous' 鍵）。
    此方法無需額外操作。
    """
    return 0
```

---

## 八、其他

### 8.1 配置檔案對應

以下表格列出 SPT 模組使用的配置檔案及對應的段落：

| 配置鍵/段落 | 檔案 | 使用步驟 |
|---|---|---|
| `[pipeline.spt]` | `stagging_spt.toml` | `SPTPipelineOrchestrator.__init__()` |
| `[spt_status_label_rules]` | `stagging_spt.toml` | `SPTStatusLabelStep._load_rules()` |
| `[spt_account_prediction]` | `stagging_spt.toml` | `SPTAccountPredictionStep._load_rules_from_config()` |
| `[spt_procurement_status_rules]` | `stagging_spt.toml` | `SPTProcurementStatusEvaluationStep._load_conditions_from_config()` |
| `[spt_procurement_previous_mapping]` | `stagging_spt.toml` | `ProcurementPreviousMappingStep._load_mapping_config()` |
| `[data_shape_summary]` | `stagging.toml` | `SPTDataLoadingStep.execute()` |
| `SPT` section | `config.ini` | `SPTERMLogicStep.__init__()` (fa_accounts, dept_accounts) |
| `[PATHS]` section | `config.ini` | `SPTDataLoadingStep._load_reference_data()` (ref_path_spt) |

### 8.2 測試覆蓋率現況

根據 `CLAUDE.md` 的記錄，SPT 相關測試位於：

| 測試檔案 | 主要覆蓋內容 | 覆蓋率 |
|---|---|---|
| `tests/unit/tasks/spt/test_spt_orchestrator.py` | `SPTPipelineOrchestrator` | 中等 |
| `tests/unit/tasks/spt/test_spt_loading.py` | `SPTDataLoadingStep` 的輸入驗證 | 中等 |
| `tests/unit/tasks/spt/test_spt_evaluation_erm.py` | `SPTERMLogicStep` 的 ERM 邏輯 | 96% |
| `tests/unit/tasks/spt/test_spt_account_prediction.py` | `SPTAccountPredictionStep` | 中等 |

`spt_evaluation_affiliate.py`、`spt_evaluation_accountant.py`、`spt_steps.py`（後處理步驟）、所有採購相關步驟的測試覆蓋率較低或沒有直接的測試檔案。

### 8.3 檔案行數統計

> **2026-03-17 更新**：`spt_loading.py` 重構後大幅縮減；`steps/__init__.py` 移除 4 個孤兒名稱。

| 檔案 | 行數 | 主要職責 |
|---|---|---|
| `__init__.py` | 7 | 模組入口 |
| `pipeline_orchestrator.py` | 518 | Pipeline 建構與管理 |
| `steps/__init__.py` | ~55 | 步驟匯出（移除 4 個孤兒名稱） |
| `steps/spt_loading.py` | **182** | PO/PR 資料載入（原 ~1164 行，84% 減少） |
| `steps/spt_steps.py` | 805 | 後處理 + 4 個已廢棄孤兒步驟 |
| `steps/spt_evaluation_erm.py` | 697 | ERM 邏輯評估 |
| `steps/spt_evaluation_affiliate.py` | 795 | 分潤/薪資偵測 |
| `steps/spt_evaluation_accountant.py` | 461 | 會計標籤標記 |
| `steps/spt_account_prediction.py` | 339 | 科目預測 |
| `steps/spt_procurement_evaluation.py` | 330 | 採購狀態評估 |
| `steps/spt_combined_procurement_loading.py` | 302 | COMBINED 載入 |
| `steps/spt_combined_procurement_processing.py` | 274 | COMBINED 處理 |
| `steps/spt_procurement_mapping.py` | 226 | 前期底稿映射 |
| `steps/spt_combined_procurement_export.py` | 241 | COMBINED 匯出 |
| `steps/spt_procurement_validation.py` | 268 | 前期底稿驗證 |
| `steps/spt_procurement_loading.py` | 142 | 採購 PO/PR 載入 |
| `steps/spt_column_initialization.py` | 97 | 採購欄位初始化 |
| **合計** | **~5,739** | （原 ~6,511，減少 ~772 行） |

### 8.4 已知 BUG 速查表

> **2026-03-17 更新**：BUG-2 至 BUG-7 全部修復。BUG-1 為唯一開放項目。

| 編號 | 位置 | 類型 | 觸發條件 | 症狀 | 狀態 |
|---|---|---|---|---|---|
| BUG-1 | `spt_loading.py`（`_load_ap_invoice`） | 設計錯誤 | SPT 和 SPX 的 ap_columns 配置不同 | AP 欄位使用錯誤的定義 | 🔴 開放 |
| BUG-2 | `spt_steps.py`（`_rearrange_reviewer_col`） | KeyError 風險 | `previous_month_reviewed_by` 欄位不存在 | 後處理步驟整體失敗 | ✅ 已修復（2026-03-17） |
| BUG-3 | `spt_loading.py`（已移除） | 賦值遺漏 | `Project Number` 欄位存在於原始資料 | 欄位名稱未被重命名 | ✅ 已修復（2026-03-17，隨 Fix 5 一併修復） |
| BUG-4 | `spt_evaluation_affiliate.py`（`PayrollDetectionStep`） | IndexError 風險 | DataFrame 中沒有含「狀態」的欄位 | `list index out of range` | ✅ 已修復（2026-03-17） |
| BUG-5 | `pipeline_orchestrator.py`（`_create_step`） | 日誌不一致 | 配置中出現未知步驟名稱 | 警告訊息不進入日誌系統 | ✅ 已修復（2026-03-17） |
| BUG-6 | `spt_evaluation_accountant.py`（`status_column` 邏輯） | 邏輯覆蓋 | `status_column` 由 `__init__` 注入後被 `execute()` 覆蓋 | Orchestrator 注入的 `status_column` 可能被忽略 | ✅ 已修復（2026-03-17，隨 Fix 5 整合確認） |
| BUG-7 | `spt_combined_procurement_processing.py`（sub_context） | 潛在數值錯誤 | COMBINED 模式下 sub_context 未設置 metadata | 採購狀態評估可能使用預設值（0）進行日期比較 | ✅ 已修復（2026-03-17） |

---

*本文件由 Claude Code 自動研究生成，基於對 `accrual_bot/tasks/spt/` 目錄下 17 個 Python 檔案的深度程式碼分析。文件涵蓋模組的設計思路、實作細節、已知問題及改善建議。*

*2026-03-17 更新：根據 8 個 SPT Tasks Bug Fixes 實施結果更新已知問題狀態、技術債務盤點、DRY 重構建議、孤兒步驟處置策略、檔案行數統計及 BUG 速查表。唯一仍開放的問題為 BUG-1（`_load_ap_invoice` 使用 `'SPX'` 識別碼）。*
