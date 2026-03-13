# `accrual_bot/runner` 模組深度研究文件

> **文件資訊**
> - 本文件為自動生成之技術研究文件，供開發團隊參考使用
> - 生成日期：2026-03-13
> - 分析對象：`accrual_bot/runner/` 模組（共 3 個檔案，506 行程式碼）
> - 分析視角：軟體工程最佳實踐（設計模式、可維護性、可擴展性、潛在缺陷）

---

## 目錄

1. [背景（Background）](#1-背景background)
   - 1.1 [專案脈絡](#11-專案脈絡)
   - 1.2 [為何需要 runner 模組](#12-為何需要-runner-模組)
   - 1.3 [架構定位](#13-架構定位)
   - 1.4 [歷史演進](#14-歷史演進)
2. [用途（Purpose）](#2-用途purpose)
   - 2.1 [config_loader.py 的雙重職責](#21-config_loaderpy-的雙重職責)
   - 2.2 [step_executor.py 的互動式除錯功能](#22-step_executorpy-的互動式除錯功能)
   - 2.3 [__init__.py 的公開 API 設計](#23-__init__py-的公開-api-設計)
   - 2.4 [main_pipeline.py 的整合模式](#24-main_pipelinepy-的整合模式)
   - 2.5 [雙模式執行架構](#25-雙模式執行架構)
3. [設計思路（Design Philosophy）](#3-設計思路design-philosophy)
   - 3.1 [Configuration-as-Code 理念](#31-configuration-as-code-理念)
   - 3.2 [RunConfig 作為值物件（Value Object）](#32-runconfig-作為值物件value-object)
   - 3.3 [路徑模板引擎設計](#33-路徑模板引擎設計)
   - 3.4 [Glob 最新選擇策略](#34-glob-最新選擇策略)
   - 3.5 [關注點分離（Separation of Concerns）](#35-關注點分離separation-of-concerns)
   - 3.6 [StepByStepExecutor 繞過 Pipeline.execute() 的設計決策](#36-stepbystepexecutor-繞過-pipelineexecute-的設計決策)
   - 3.7 [EOFError 優雅降級機制](#37-eoferror-優雅降級機制)
4. [各項知識點（Key Technical Concepts）](#4-各項知識點key-technical-concepts)
   - 4.1 [TOML 配置載入（Python 3.11+ tomllib）](#41-toml-配置載入python-311-tomllib)
   - 4.2 [RunConfig Dataclass 設計](#42-runconfig-dataclass-設計)
   - 4.3 [路徑模板替換系統（Template Engine）](#43-路徑模板替換系統template-engine)
   - 4.4 [日期計算（YYYYMM Arithmetic）](#44-日期計算yyyymm-arithmetic)
   - 4.5 [Glob 萬用字元解析](#45-glob-萬用字元解析)
   - 4.6 [params 類型轉換（_convert_params）](#46-params-類型轉換_convert_params)
   - 4.7 [StepByStepExecutor 的互動式 REPL 模式](#47-stepbystepexecutor-的互動式-repl-模式)
   - 4.8 [Bypass Pipeline.execute() 的設計決策](#48-bypass-pipelineexecute-的設計決策)
   - 4.9 [Checkpoint 命名策略](#49-checkpoint-命名策略)
   - 4.10 [結果字典的雙重結構問題](#410-結果字典的雙重結構問題)
5. [應用範例（Usage Examples）](#5-應用範例usage-examples)
   - 5.1 [最簡執行範例（run_config.toml 驅動）](#51-最簡執行範例run_configtoml-驅動)
   - 5.2 [程式化呼叫 load_file_paths](#52-程式化呼叫-load_file_paths)
   - 5.3 [逐步執行模式](#53-逐步執行模式)
   - 5.4 [從 Checkpoint 恢復（Resume 模式）](#54-從-checkpoint-恢復resume-模式)
   - 5.5 [非互動式環境中使用 StepByStepExecutor](#55-非互動式環境中使用-stepbystepexecutor)
6. [優缺分析（Strengths and Weaknesses）](#6-優缺分析strengths-and-weaknesses)
   - 6.1 [優點](#61-優點)
   - 6.2 [缺點與設計問題](#62-缺點與設計問題)
7. [延伸議題（Advanced Topics & Future Considerations）](#7-延伸議題advanced-topics--future-considerations)
   - 7.1 [配置驗證層（Configuration Validation）](#71-配置驗證層configuration-validation)
   - 7.2 [路徑模板引擎的強化](#72-路徑模板引擎的強化)
   - 7.3 [StepByStepExecutor 的 UI 解耦](#73-stepbystepexecutor-的-ui-解耦)
   - 7.4 [Pipeline.execute() Hook 機制](#74-pipelineexecute-hook-機制)
   - 7.5 [verbose 欄位的完整實作](#75-verbose-欄位的完整實作)
   - 7.6 [多環境配置支援（Environment-aware Paths）](#76-多環境配置支援environment-aware-paths)
   - 7.7 [_convert_params 的型別轉換表設計](#77-_convert_params-的型別轉換表設計)
8. [其他（Miscellaneous）](#8-其他miscellaneous)
   - 8.1 [模組在系統中的定位圖](#81-模組在系統中的定位圖)
   - 8.2 [與 UI 層的對應關係](#82-與-ui-層的對應關係)
   - 8.3 [測試現況](#83-測試現況)
   - 8.4 [程式碼統計](#84-程式碼統計)

---

## 1. 背景（Background）

### 1.1 專案脈絡

`accrual_bot` 是一套為 SPT 與 SPX 兩個業務實體設計的**月度財務數據對帳系統**。每月執行一次的 PO（採購單）/ PR（採購申請）應計項目（accruals）處理是其核心業務需求：從多個來源匯入原始數據、套用複雜的商業規則、與前期底稿比對，最終輸出財務工作底稿供會計人員使用。

這類月度批次系統的特點是：
- **執行頻率低**：每月僅執行一到數次，非持續性服務
- **數據量適中**：足以用 pandas 處理，不需要分散式框架
- **邏輯複雜度高**：跨多份檔案的 ERM 評估邏輯，含大量條件判斷
- **可重現性要求**：同一月份的數據重跑應產生相同結果（deterministic）
- **人工介入需求**：部分步驟結果需人工審視確認後再繼續

正是最後這個特點——人工介入需求——催生了 `runner` 模組中互動式逐步執行功能的需求。

### 1.2 為何需要 runner 模組

在沒有 `runner` 模組之前，pipeline 的執行配置（要跑哪個 entity、哪個月份）是硬編碼在 `main_pipeline.py` 中的，或是透過直接修改程式碼來切換。這帶來幾個問題：

1. **配置與程式碼耦合**：每次更換處理月份都需要修改 `main_pipeline.py`，即使只是改一個數字
2. **路徑解析分散**：各個 pipeline 函數各自拼湊路徑字串，邏輯重複且難以維護
3. **無法統一除錯**：當某個步驟出錯時，開發者必須手動修改程式碼加入中斷點或日誌

`runner` 模組的出現，將以上三個問題統一解決：

| 問題 | runner 的解法 |
|------|--------------|
| 配置硬編碼 | `run_config.toml` 外部化配置 |
| 路徑解析分散 | `load_file_paths()` 統一路徑解析 |
| 無法統一除錯 | `StepByStepExecutor` 提供互動式逐步執行 |

### 1.3 架構定位

`accrual_bot` 採用四層架構，`runner` 模組在此架構中扮演特殊的「CLI 編排層」角色：

```
┌─────────────────────────────────────────────────────────────┐
│                    UI 層（Streamlit）                         │
│  pages/ → components/ → services/ → Session State           │
│                    [ui/services/ 是 runner 的 UI 對應物]      │
├─────────────────────────────────────────────────────────────┤
│              [runner/ — CLI 編排層，不在正式四層內]            │
│  config_loader.py    step_executor.py                        │
│  RunConfig           StepByStepExecutor                      │
├─────────────────────────────────────────────────────────────┤
│                    Tasks 層（業務編排）                        │
│  tasks/spt/ | tasks/spx/ | tasks/common/                     │
├─────────────────────────────────────────────────────────────┤
│                    Core 層（框架）                            │
│  Pipeline | PipelineStep | ProcessingContext | DataSources   │
├─────────────────────────────────────────────────────────────┤
│                    Utils 層（橫切關注點）                      │
│  ConfigManager | Logger | Data Utilities                     │
└─────────────────────────────────────────────────────────────┘
```

`runner` 模組在正式四層架構之外，定位為**進入點層（Entry Layer）的支援組件**：它不包含任何業務邏輯，只負責從環境（檔案系統、TOML 配置）讀取執行意圖（RunConfig），並將此意圖轉化為 Tasks 層可接受的輸入（file_paths dict）。

這種定位使得 `runner` 模組具有極低的向內依賴（只依賴 Core 和 Utils），同時成為 `main_pipeline.py` 可以完全信賴的工具箱。

### 1.4 歷史演進

從 `main_pipeline.py` 底部仍存在的向後相容函數（`run_spx_po_full_pipeline()`、`run_spt_po_full_pipeline()` 等）可以推斷系統的演進軌跡：

**第一階段（硬編碼時期）**：所有路徑與配置直接寫在函數內，日期硬編碼為 `202512`，每月更換時需手動修改程式碼。

**第二階段（配置外部化）**：引入 `run_config.toml` 和 `paths.toml`，`runner/config_loader.py` 負責載入。路徑模板替換系統讓月份切換只需修改一個 TOML 欄位。

**第三階段（互動式除錯）**：`StepByStepExecutor` 的加入回應了開發者在除錯複雜 pipeline 時「想在某個步驟後暫停查看」的需求。

**第四階段（UI 化）**：`ui/services/UnifiedPipelineService` 承擔了與 `runner` 相似的職責，但面向 Streamlit Web UI 而非 CLI。此時兩套配置載入邏輯並存，形成了一定程度的重複。

向後相容函數底部的 `202512` 硬編碼日期是歷史痕跡的最直接證明——在 runner 模組成熟之前，這些函數是系統的主要進入點。

---

## 2. 用途（Purpose）

### 2.1 config_loader.py 的雙重職責

`config_loader.py` 承擔兩個在概念上相關但功能上獨立的職責：

**職責一：執行意圖載入（Execution Intent Loading）**

`load_run_config()` 讀取 `run_config.toml`，將「本次要執行什麼」的配置封裝為 `RunConfig` dataclass：

```toml
[run]
entity = "SPX"           # 哪個業務實體
processing_type = "PO"   # 哪種處理類型
processing_date = 202602  # 哪個月份
```

這些資訊回答的是**「跑什麼」**的問題。

**職責二：數據存取路徑解析（Data Access Path Resolution）**

`load_file_paths()` 讀取 `paths.toml`，根據 entity/type/date 三個維度，解析出當月所有相關檔案的實際路徑與讀取參數：

```python
# 輸入：entity="SPX", processing_type="PO", processing_date=202602
# 輸出：
{
    "raw_po": {
        "path": "C:/SEA/.../202602/Original Data/202602_purchase_order_20260201.csv",
        "params": {"encoding": "utf-8", "sep": ",", "dtype": str}
    },
    "previous": {
        "path": "C:/SEA/.../202602/前期底稿/SPX/202601_PO_FN.xlsx",
        "params": {"sheet_name": 0, "header": 0, "dtype": str}
    }
}
```

這些資訊回答的是**「數據在哪裡、怎麼讀」**的問題。

這兩個職責合理地放在同一檔案中，因為它們在使用上總是一起出現（`main_pipeline.py` 每次都同時呼叫兩者），而且複雜度都不高，分成兩個檔案反而增加了目錄複雜度。

### 2.2 step_executor.py 的互動式除錯功能

`StepByStepExecutor` 是一個**互動式 CLI 除錯工具**，設計目的是讓開發者或資深使用者能夠：

1. 在每個步驟執行前先預覽步驟名稱
2. 決定是否執行、跳過或中止
3. 執行後查看步驟結果（狀態、耗時、數據行數）
4. 若步驟失敗，決定是否繼續執行後續步驟

這個功能在以下場景特別有用：
- **除錯新步驟**：新增步驟後，想逐一確認每個步驟的輸出正確性
- **部分重跑**：已知前幾個步驟正確，想跳過它們直接從問題步驟開始（雖然 Checkpoint Resume 是更好的方案）
- **人工審視關鍵步驟**：在某些高風險步驟（如 ERM 評估）後暫停，手動確認數據符合預期

值得注意的是，`StepByStepExecutor` **不是**生產環境的標準執行路徑，它是一個輔助除錯工具。生產環境應使用 `pipeline.execute()` 搭配 Checkpoint 機制。

### 2.3 __init__.py 的公開 API 設計

`runner/__init__.py` 只有 20 行，但其設計傳達了清晰的 API 意圖：

```python
from .config_loader import (
    load_run_config,
    load_file_paths,
    RunConfig,
)
from .step_executor import StepByStepExecutor

__all__ = [
    'load_run_config',
    'load_file_paths',
    'RunConfig',
    'StepByStepExecutor',
]
```

**明確導出策略**：`__all__` 清楚宣告哪些是公開 API，哪些是模組內部實作細節（`_calculate_date_vars`、`_resolve_path_template`、`_convert_params` 均未導出）。

**扁平化介面**：外部使用者只需 `from accrual_bot.runner import load_file_paths`，無需知道它在 `config_loader.py` 中。這遵循了 Python 的「扁平優於巢狀（flat is better than nested）」原則。

**型別優先**：`RunConfig` dataclass 被導出，讓外部程式碼可以做型別標注（`config: RunConfig = load_run_config()`），而不是用 `Dict[str, Any]`。

### 2.4 main_pipeline.py 的整合模式

`main_pipeline.py` 作為 CLI 進入點，將 runner 模組的各組件串接成完整執行流程：

```
load_run_config()
    ↓
    RunConfig(entity, processing_type, processing_date, ...)
    ↓
load_file_paths(entity, processing_type, processing_date)
    ↓
    file_paths dict
    ↓
orchestrator.build_{type}_pipeline(file_paths)
    ↓
    Pipeline object
    ↓
if config.step_by_step:
    StepByStepExecutor(pipeline, context).run()
elif config.resume_enabled:
    PipelineWithCheckpoint(...).run()   ← 來自 core，非 runner
else:
    pipeline.execute(context)
```

這個流程展示了 `runner` 模組作為**橋接層**的角色：它將外部環境（TOML 檔案）與內部框架（Pipeline、ProcessingContext）連接起來，但本身不包含任何業務邏輯。

### 2.5 雙模式執行架構

系統支援兩種執行模式，形成清晰的使用場景分離：

| 執行模式 | 啟動條件 | 使用的組件 | 適用場景 |
|---------|---------|-----------|---------|
| 正常批次模式 | `step_by_step = false` | `pipeline.execute()` | 生產環境月度執行 |
| 逐步除錯模式 | `step_by_step = true` | `StepByStepExecutor` | 開發除錯、人工審視 |
| 斷點恢復模式 | `resume.enabled = true` | `PipelineWithCheckpoint` | 從失敗點繼續執行 |

值得注意的是，**逐步除錯模式**和**斷點恢復模式**是互斥的——`main_pipeline.py` 的條件判斷中，`step_by_step` 優先，`resume_enabled` 次之，正常模式最後。這個優先順序是否符合直覺是一個值得討論的設計問題（若同時設定兩者，使用者的意圖是什麼？）。

---

## 3. 設計思路（Design Philosophy）

### 3.1 Configuration-as-Code 理念

`runner` 模組的核心設計理念是 **Configuration-as-Code**：將執行行為的控制從程式碼本體中分離，委託給外部配置檔案。

這符合 [12-Factor App](https://12factor.net/config) 的第三原則（**Config**）：「在環境中存儲配置（Store config in the environment）」。雖然 TOML 檔案嚴格來說仍是版本控制中的檔案而非環境變數，但相較於硬編碼在程式碼中，它已大幅提高了配置的可見性和可修改性。

```toml
# run_config.toml 讓非技術人員也能理解「現在要跑什麼」
[run]
entity = "SPX"
processing_type = "PO"
processing_date = 202602
```

這段配置的表達力遠超等效的 Python 程式碼，且修改它不需要理解 Python 語法或 pipeline 架構。

TOML 格式在此場景下比 JSON 或 YAML 更合適：
- **比 JSON** 好：支援註解（`# 這是月份配置`），人類可讀性更高
- **比 YAML** 好：無縮排敏感問題，無隱式型別轉換陷阱（YAML 的 `no` 會被解析為 `false`）
- **比 INI** 好：支援巢狀結構、陣列、整數原生型別（`202602` 不會被讀成字串）

### 3.2 RunConfig 作為值物件（Value Object）

`RunConfig` 採用 `@dataclass` 設計，在領域驅動設計（DDD）中這類物件屬於**值物件（Value Object）**的範疇——它的身份由其所有欄位值決定，而不是由物件識別碼（id）決定。

值物件通常應設計為不可變（immutable），但 `RunConfig` 使用了可變的 `@dataclass`（非 `frozen=True`）。這在實際使用中不是問題（沒有程式碼修改它），但從語義上來說是一個錯失的機會：

```python
# 現狀（可變，雖然實際上不會被修改）
@dataclass
class RunConfig:
    entity: str
    ...

# 更精確的語義（不可變）
@dataclass(frozen=True)
class RunConfig:
    entity: str
    ...
```

`frozen=True` 的好處：
1. 讓物件可雜湊（hashable），可用於 set/dict key
2. 明確傳達「此物件建立後不應被修改」的意圖
3. 防止意外的屬性賦值（`config.entity = "SPT"` 會 raise `FrozenInstanceError`）

### 3.3 路徑模板引擎設計

`paths.toml` 中的路徑模板系統是一個**輕量級變數替換引擎**，核心概念如下：

**設計目標**：讓路徑配置與月份無關——同一份 `paths.toml` 可服務於任意月份的處理，月份參數在執行時注入。

**變數空間設計**：

| 變數 | 計算方式 | 範例（date=202602） | 用途 |
|------|---------|-------------------|------|
| `{YYYYMM}` | 直接轉字串 | `202602` | 當月目錄名、檔名前綴 |
| `{PREV_YYYYMM}` | 前月計算 | `202601` | 前期底稿路徑 |
| `{YYMM}` | 年份取末兩位 | `2602` | 部分舊式命名的檔案 |
| `{YYYY}` | 年份 | `2026` | 年度目錄 |
| `{MM}` | 月份（兩位） | `02` | 月份目錄 |
| `{resources}` | 來自 `[base]` 區段 | `C:/SEA/...` | 根目錄基路徑 |

**注入順序**：`resources` 最後注入（在 `_calculate_date_vars` 之後額外加入），因為它來自配置而非計算。

這個設計的巧妙之處在於**兩層模板**：`{resources}` 本身可能在 `paths.toml` 的 `[base]` 區段中被定義，而路徑模板又使用 `{resources}`，形成了一種輕量級的變數引用。

### 3.4 Glob 最新選擇策略

對於包含萬用字元的路徑（如 `{YYYYMM}_purchase_order_*.csv`），`load_file_paths()` 採用「字典序最大者」策略：

```python
if "*" in resolved_path:
    matches = glob(resolved_path)
    if matches:
        resolved_path = sorted(matches)[-1]  # 取字典序最大
```

這個策略基於一個隱性假設：**檔名包含日期或序號，且日期/序號越大越新**。對於 `20260201_purchase_order_v1.csv` 和 `20260215_purchase_order_v2.csv` 這樣的命名，`sorted()[-1]` 確實會選到較新的版本。

然而這個策略在以下情況會失效：
- 命名不含日期的檔案（`purchase_order_final.csv` vs `purchase_order_revised.csv`）
- 日期不在固定位置的命名
- 多個月份的檔案混在同一目錄（雖然路徑模板已包含 `{YYYYMM}`，通常能避免）

### 3.5 關注點分離（Separation of Concerns）

`runner` 模組在整個系統中體現了兩個層次的關注點分離：

**層次一：執行意圖 vs 數據存取**
- `RunConfig`：描述「要做什麼」（entity, type, date, mode flags）
- `file_paths dict`：描述「數據在哪裡」（resolved paths + read params）

這兩者雖然都由 `config_loader.py` 產生，但語義上完全不同——前者是執行控制流程，後者是 I/O 配置。

**層次二：配置載入 vs Pipeline 執行**
- `runner/`：只負責「準備好執行所需的一切」
- `tasks/`：只負責「執行業務邏輯」
- `core/pipeline/`：只負責「框架級別的步驟排序與狀態管理」

`runner` 模組刻意不包含任何業務邏輯，也不直接操作 DataFrame——它只是「把正確的鑰匙交給正確的鎖」。

### 3.6 StepByStepExecutor 繞過 Pipeline.execute() 的設計決策

`StepByStepExecutor.run()` 直接迭代 `self.pipeline.steps` 而不是呼叫 `pipeline.execute()`，這是一個**刻意的設計選擇**，有其必要性但也有代價：

**必要性**：`pipeline.execute()` 是一個黑盒子——它執行所有步驟並返回最終結果，中間沒有暫停點。若要在每個步驟後暫停等待用戶確認，只能繞過 `execute()` 直接控制迭代。

**代價**：
1. `Pipeline.stop_on_error` 旗標不生效（StepByStepExecutor 有自己的錯誤處理邏輯）
2. Pipeline 的 pre/post execution hooks（若日後加入）不會被觸發
3. 步驟執行邏輯（異常捕捉、計時、結果記錄）被重複實作

這個設計權衡在功能上是合理的，但它制造了一個**維護陷阱**：當 `Pipeline.execute()` 的核心邏輯變更時，`StepByStepExecutor` 不會自動跟進，需要手動同步。

### 3.7 EOFError 優雅降級機制

`StepByStepExecutor` 的 `_prompt_action()` 和 `_confirm_continue_on_error()` 方法都捕捉了 `EOFError`：

```python
try:
    response = input(f"\n執行 '{step_name}'? [Enter/s/q]: ").strip().lower()
    ...
except EOFError:
    return "continue"  # 非互動式模式：自動繼續
```

`EOFError` 發生的場景：
- **管道輸入**：`echo "" | python main_pipeline.py`
- **CI/CD 環境**：stdin 未連接到 TTY
- **腳本化執行**：`python main_pipeline.py < /dev/null`
- **Jupyter Notebook**：stdin 不是標準終端機

這個設計讓 `StepByStepExecutor` 在非互動式環境中退化為「自動繼續所有步驟」的行為——等效於直接執行 `pipeline.execute()`，而不是崩潰。這是一個符合**最小驚喜原則（Principle of Least Surprise）**的優雅降級設計。

---

## 4. 各項知識點（Key Technical Concepts）

### 4.1 TOML 配置載入（Python 3.11+ tomllib）

**背景**：Python 3.11 將 TOML 解析器 `tomllib` 納入標準函式庫（[PEP 680](https://peps.python.org/pep-0680/)）。在此之前，讀取 TOML 需要安裝第三方套件（`toml`、`tomli`）。

**使用方式的特殊性**：`tomllib` 要求以**二進位模式**（`"rb"`）開啟檔案：

```python
with open(config_path, "rb") as f:
    config = tomllib.load(f)
```

這與 `json.load()` 使用文字模式不同。原因是 TOML 標準規定必須為 UTF-8 編碼，`tomllib` 在二進位層面自行處理編碼，避免了系統預設編碼（locale-dependent encoding）造成的潛在問題。

**安全的 get() 鏈式存取**：`load_run_config()` 全面使用有預設值的 `get()` 呼叫：

```python
run = config.get("run", {})
debug = config.get("debug", {})
# ...
entity=run.get("entity", "SPX"),
step_by_step=debug.get("step_by_step", False),
```

這個模式確保即使 TOML 檔案缺少某個區段或鍵值，系統仍能以預設值運行，而不是 raise `KeyError`。

**TOML vs 其他格式比較**：

| 特性 | TOML | JSON | YAML | INI |
|------|------|------|------|-----|
| 支援註解 | 是 | 否 | 是 | 是 |
| 原生整數型別 | 是 | 是 | 是 | 否（全為字串） |
| 縮排敏感 | 否 | 否 | 是 | 否 |
| 隱式型別轉換 | 無 | 無 | 有（`yes`→`true`） | 無 |
| Python stdlib 支援 | 3.11+ | 全版本 | 否 | 全版本 |
| 人類可讀性 | 高 | 中 | 高 | 中 |

對於人類經常編輯的配置檔（如 `run_config.toml`），TOML 是最佳選擇。

### 4.2 RunConfig Dataclass 設計

**欄位順序的約束**：Python `@dataclass` 要求**有預設值的欄位必須排在無預設值的欄位之後**。`RunConfig` 遵循了這個規則：

```python
@dataclass
class RunConfig:
    # 必填欄位（無預設值）
    entity: str
    processing_type: str
    processing_date: int
    # 選填欄位（有預設值）
    source_type: str = ""
    step_by_step: bool = False
    save_checkpoints: bool = True
    # ...
```

這不只是語法要求，也體現了語義——前三個欄位是執行任何 pipeline 的必要資訊，缺少任何一個就無法決定「做什麼」。

**空字串 sentinel 模式**：`source_type: str = ""` 使用空字串表示「不適用」（PROCUREMENT 以外的類型不需要此欄位）。相比 `Optional[str] = None`，這個選擇有利有弊：

| | `source_type: str = ""`  | `source_type: Optional[str] = None` |
|--|--------------------------|--------------------------------------|
| 型別安全 | 不夠精確（`""` 和 `"PO"` 都是 `str`） | 更精確（`None` 明確表示缺失） |
| 使用便利性 | 可直接用在 f-string 中不報錯 | 需先 `if source_type is not None` |
| 意圖清晰度 | 「空字串代表不適用」需要文件說明 | `None` 的語義更直觀 |
| TOML 相容性 | TOML 的 `source_type = ""` 直接映射 | 需要處理 TOML 中 null 的表示 |

在此場景下，空字串選擇是合理的，因為 TOML 沒有原生的 null 值（TOML 的 `null` 是特定版本才有的特性，且不常用）。

**`verbose` 欄位的設計問題**：`RunConfig` 包含 `verbose: bool = False` 欄位，但 `main_pipeline.py` 從未根據此值調整日誌級別。這是一個**定義了介面但未實作**的情況，屬於技術債（technical debt）。

### 4.3 路徑模板替換系統（Template Engine）

`_resolve_path_template()` 實作了一個極簡的變數替換引擎：

```python
def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    result = template
    for var_name, var_value in vars.items():
        result = result.replace(f"{{{var_name}}}", var_value)
    return result
```

**選擇 `str.replace()` 而非 `str.format_map()`**：

Python 有更 Pythonic 的方式做字串模板替換：

```python
# Python 原生方式
result = template.format_map(vars)
# 或
from string import Template
t = Template(template)
result = t.safe_substitute(vars)
```

但當前實作刻意選擇了手動 `str.replace()` 迴圈，可能的原因是：
1. 避免 `format_map()` 在模板含有 `{unknown_var}` 時 raise `KeyError`
2. 避免 `str.format()` 誤解路徑中的 `{` `}` 字元

然而這個「安全性」是虛假的——它只是靜默地將未解析的 `{UNKNOWN}` 保留在路徑中，而不是明確報錯。**靜默失敗比顯式失敗更難診斷**。

**無未解析變數檢測**：若路徑模板含有未定義的變數（如拼錯的 `{YYYMM}`），系統不會在路徑解析時報錯，而是在後續 I/O 嘗試存取含有 `{YYYMM}` 字面量的不存在路徑時才失敗。錯誤訊息與根本原因（拼錯變數名）相距甚遠，增加除錯難度。

**跨平台路徑問題**：`paths.toml` 中的 `resources` 路徑使用 Windows 絕對路徑格式（`C:/SEA/...`）。在 Linux 或 macOS 開發環境下，此路徑完全無效。目前缺乏環境切換機制（如 `.env` 覆蓋或環境變數注入）。

### 4.4 日期計算（YYYYMM Arithmetic）

系統使用整數表示年月（`202602` = 2026年2月），這是一個**刻意回避 datetime 複雜性的設計選擇**。

**整數日期算術的實作**：

```python
def _calculate_date_vars(processing_date: int) -> Dict[str, str]:
    year = processing_date // 100   # 整數除法取年份
    month = processing_date % 100   # 取餘數得月份

    if month > 1:
        prev_year = year
        prev_month = month - 1
    else:  # month == 1，跨年
        prev_year = year - 1
        prev_month = 12

    return {
        "YYYYMM": str(processing_date),        # "202602"
        "PREV_YYYYMM": f"{prev_year}{prev_month:02d}",  # "202601"
        "YYMM": f"{year % 100:02d}{month:02d}",  # "2602"
        "YYYY": str(year),                     # "2026"
        "MM": f"{month:02d}",                  # "02"
    }
```

**為何不用 `datetime`**：

```python
# 等效的 datetime 做法（更多依賴，但更正確）
from datetime import date
from dateutil.relativedelta import relativedelta

current = date(year, month, 1)
previous = current - relativedelta(months=1)
```

整數算術的優點：
- **無外部依賴**：`dateutil` 非標準函式庫
- **無時區問題**：`datetime` 物件在某些操作下會涉及時區
- **語義明確**：YYYYMM 整數在整個系統中作為唯一識別符使用，不需要完整日期物件
- **確定性**：相同輸入永遠得到相同輸出，無環境依賴

**邊界條件處理**：月份邊界（1月→前年12月）正確處理。但缺乏對無效輸入的防禦：`processing_date = 202613`（13月）會產生 `PREV_YYYYMM = "202612"`（因為 `13 > 1` 成立），不會報錯但語義錯誤。

**`YYMM` 的千年問題（Year 2000 Issue）**：`year % 100` 會在 2100 年後產生問題（`2100 % 100 = 0`，得到 `0002`）。雖然這不是近期問題，但值得注意。

### 4.5 Glob 萬用字元解析

`glob.glob()` 搭配 `sorted()[-1]` 的組合實作了「自動選最新版本」的功能：

```python
if "*" in resolved_path:
    matches = glob(resolved_path)
    if matches:
        resolved_path = sorted(matches)[-1]
        logger.debug(f"  萬用字元解析: {key} -> {resolved_path}")
    else:
        logger.warning(f"  找不到符合的檔案: {path_template}")
```

**`glob.glob()` 的特性**：
- 回傳列表，順序取決於作業系統（不保證排序）
- Windows 大小寫不敏感，Linux 大小寫敏感
- 不遞迴（`**` 萬用字元需額外處理）

**`sorted()[-1]` 的字典序假設**：

排序依賴字典序（lexicographic order），這在檔名開頭是 YYYYMM 格式時有效：
```
202602_purchase_order_A.csv   →  A
202602_purchase_order_B.csv   →  B  ← sorted()[-1] 選此
202602_purchase_order_AA.csv  →  AA ← 實際上字典序更大，但業務意圖可能是選 B
```

**更健壯的替代方案**：

```python
# 方案一：按修改時間排序（選最新修改的）
import os
resolved_path = max(matches, key=os.path.getmtime)

# 方案二：多於一個匹配時明確報錯
if len(matches) > 1:
    raise ValueError(f"找到多個符合的檔案，請使用精確路徑: {matches}")

# 方案三：讓使用者在 run_config.toml 中手動指定
```

當前實作的靜默選擇行為可能在命名慣例改變時產生難以察覺的錯誤——系統不會報錯，但可能處理了錯誤的檔案。

### 4.6 params 類型轉換（_convert_params）

TOML 存儲的是純文字，但 pandas 讀取函數需要 Python 型別物件。`_convert_params()` 填補這個落差：

```python
def _convert_params(params: Dict[str, Any]) -> Dict[str, Any]:
    result = {}
    for key, value in params.items():
        if key == "dtype" and value == "str":
            result[key] = str          # 字串 "str" → Python type str
        elif key == "keep_default_na" and isinstance(value, bool):
            result[key] = value        # TOML bool 直接傳遞
        else:
            result[key] = value        # 其他值直接傳遞
    return result
```

**已處理的轉換**：

| TOML 值 | 轉換後 | 用途 |
|---------|-------|------|
| `dtype = "str"` | `dtype = str` | pandas 讀取時所有欄位為字串 |
| `keep_default_na = false` | `keep_default_na = False` | TOML bool 與 Python bool 相容，直接傳遞 |

**未處理但安全的情況**：

| TOML 值 | 傳入 pandas | 結果 |
|---------|-----------|------|
| `sheet_name = 0` | `sheet_name = 0` | 整數直接傳遞，pandas 正確處理 |
| `header = 0` | `header = 0` | 同上 |
| `encoding = "utf-8"` | `encoding = "utf-8"` | 字串直接傳遞，正確 |
| `sep = ","` | `sep = ","` | 字串直接傳遞，正確 |
| `usecols = "A:AH"` | `usecols = "A:AH"` | 字串，Excel 可接受此格式 |

**潛在問題**：若未來需要 `dtype = {"col_a": "int64", "col_b": str}`（欄位級別的 dtype 映射），當前架構無法處理——`"int64"` 字串在 pandas 中是被接受的（作為 numpy dtype 名稱），但若需要混合型別 dict 則需要擴展此函數。

**硬編碼的擴展性問題**：新增支援的轉換類型需要修改 `_convert_params` 函數本體，而不是修改配置。這違反了**開放封閉原則（Open/Closed Principle）**——對擴展應開放，對修改應封閉。

### 4.7 StepByStepExecutor 的互動式 REPL 模式

`StepByStepExecutor` 在概念上實作了一個簡化的 **REPL（Read-Eval-Print Loop）**：

```
讀取用戶輸入（Read）→ 評估動作（Eval）→ 執行步驟（Execute）→ 顯示結果（Print）→ 循環
```

**`input()` 在 async 函數中的問題**：

`StepByStepExecutor.run()` 是 `async def`，但內部呼叫了同步的 `input()` 函數。這會**阻塞整個 asyncio 事件迴圈**，在此期間無法處理任何其他協程。

在 `accrual_bot` 的使用場景中（CLI 工具，單一執行流程），這不是問題——等待用戶輸入時本來就不應該執行其他任務。但若日後需要在 asyncio 環境中並發使用 `StepByStepExecutor`，需要改為：

```python
# asyncio 兼容的用戶輸入
import asyncio
response = await asyncio.get_event_loop().run_in_executor(
    None, input, f"\n執行 '{step_name}'? [Enter/s/q]: "
)
```

**未公開的 "c" 鍵支援**：

```python
def _prompt_action(self, step_name: str) -> str:
    while True:
        try:
            response = input(f"\n執行 '{step_name}'? [Enter/s/q]: ").strip().lower()
            if response == "" or response == "c":  # "c" 是未文件化的指令
                return "continue"
```

使用者介面顯示 `[Enter/s/q]`，但 `"c"` 也被接受為「繼續」。這可能是開發過程中的殘留程式碼，或是為習慣輸入 `c` 的 GDB 使用者提供的便利——但未記錄在文件中，增加了維護者的困惑。

**`while True` 的輸入驗證迴圈**：

```python
while True:
    response = input(...).strip().lower()
    if response in ("", "c"):
        return "continue"
    elif response == "s":
        return "skip"
    elif response == "q":
        return "abort"
    else:
        print("無效輸入，請重試 (Enter=繼續, s=跳過, q=中止)")
```

這個「拒絕無效輸入，要求重試」的模式是互動式 CLI 的正確做法，防止使用者意外按到其他鍵就跳過或中止步驟。

### 4.8 Bypass Pipeline.execute() 的設計決策

`StepByStepExecutor.run()` 直接存取 `self.pipeline.steps`（列表），而非呼叫 `self.pipeline.execute()`：

```python
async def run(self) -> Dict[str, Any]:
    for i, step in enumerate(self.pipeline.steps):  # 直接迭代步驟列表
        ...
        result = await step.execute(self.context)    # 直接呼叫步驟
        ...
```

**Pipeline.execute() 做了什麼而 StepByStepExecutor 沒做**：

根據系統架構，`Pipeline.execute()` 通常包含：
1. 執行前驗證（`validate_input()`）
2. `stop_on_error` 旗標判斷
3. 結果的統一收集與格式化
4. 執行歷史記錄（`context.execution_history`）

`StepByStepExecutor` 重新實作了部分邏輯（步驟迭代、異常捕捉、計時），但**跳過了 `validate_input()`**——這意味著步驟的前置條件驗證不會在逐步執行時進行。

**抽象洩漏（Abstraction Leak）**：直接存取 `pipeline.steps`（列表）暴露了 Pipeline 的內部實作細節。若日後 `Pipeline` 改為延遲初始化步驟，或步驟列表變為 generator，`StepByStepExecutor` 需要同步修改。

理想情況下，`Pipeline` 應提供一個 hook 機制讓外部程式碼在步驟間介入：

```python
# 理想的 Pipeline hook 設計
await pipeline.execute(
    context,
    pre_step_hook=lambda step, i, total: executor._prompt_action(step.name),
    post_step_hook=lambda step, result: executor._print_step_result(result)
)
```

### 4.9 Checkpoint 命名策略

`_save_checkpoint()` 中存在一個死碼（dead code）問題：

```python
def _save_checkpoint(self, step_name: str):
    if self.checkpoint_manager is None:
        return
    try:
        # 建構了詳細的 checkpoint 名稱...
        checkpoint_name = (
            f"{self.context.metadata.entity_type}_"
            f"{self.context.metadata.processing_type}_"
            f"{self.context.metadata.processing_date}_"
            f"after_{step_name}"
        )
        # ...但傳入的是 step_name，不是 checkpoint_name！
        self.checkpoint_manager.save_checkpoint(self.context, step_name)
        logger.debug(f"Checkpoint saved: {checkpoint_name}")  # 日誌顯示的名稱與實際不符
    except Exception as e:
        logger.warning(f"Failed to save checkpoint: {e}")
```

`checkpoint_name` 變數被建構但從未使用，`save_checkpoint()` 收到的是原始的 `step_name`（例如 `"SPXDataLoading"`），而不是完整的 `"SPX_PO_202602_after_SPXDataLoading"`。

**影響分析**：
- 實際的 checkpoint 命名由 `CheckpointManager.save_checkpoint()` 的內部邏輯決定
- 日誌中顯示的名稱（`checkpoint_name`）與實際存檔名稱不一致，可能造成混淆
- 從 checkpoint 恢復時，若用戶根據日誌中顯示的名稱填入 `run_config.toml`，可能找不到對應的 checkpoint 檔案

**正確修復**：

```python
# 選項一：傳入建構好的 checkpoint_name
self.checkpoint_manager.save_checkpoint(self.context, checkpoint_name)

# 選項二：直接刪除 checkpoint_name 變數，統一用 step_name
self.checkpoint_manager.save_checkpoint(self.context, step_name)
logger.debug(f"Checkpoint saved after step: {step_name}")
```

### 4.10 結果字典的雙重結構問題

`StepByStepExecutor.run()` 和 `pipeline.execute()` 回傳的結果字典結構不一致，是系統中一個值得關注的介面設計問題。

**StepByStepExecutor 的結果結構**：

```python
{
    "pipeline": "SPX_PO_Pipeline",
    "success": True,
    "aborted": False,
    "start_time": datetime(2026, 2, 15, 10, 30, 0),
    "end_time": datetime(2026, 2, 15, 10, 32, 30),
    "duration": 150.0,
    "total_steps": 15,
    "executed_steps": 15,
    "successful_steps": 15,
    "failed_steps": 0,
    "skipped_steps": 0,
    "results": [...],
    "context": <ProcessingContext>,
    "errors": [],
    "warnings": [],
}
```

**pipeline.execute() 的結果**（`main_pipeline.py` 手動增補 `context`）：

```python
result = await pipeline.execute(context)
result["context"] = context  # 手動加入
# 此時 result 缺少 "successful_steps"、"failed_steps" 等欄位
```

**消費端的防禦性程式碼**：

`main_pipeline.py` 的 `_print_result_summary()` 必須用 `get()` 防禦兩種格式：

```python
def _print_result_summary(result: dict):
    successful = result.get("successful_steps", "N/A")  # 防禦：StepByStepExecutor 有，pipeline.execute 無
    failed = result.get("failed_steps", "N/A")
```

**理想設計**：兩條執行路徑應回傳相同結構的結果物件（而非 dict），讓消費端不需要知道使用了哪條路徑：

```python
@dataclass
class PipelineExecutionResult:
    pipeline_name: str
    success: bool
    duration: float
    successful_steps: int
    failed_steps: int
    skipped_steps: int
    context: ProcessingContext
    errors: List[str]
    warnings: List[str]
```

---

## 5. 應用範例（Usage Examples）

### 5.1 最簡執行範例（run_config.toml 驅動）

完整的 CLI 執行流程由 `main_pipeline.py` 協調，使用者只需設定 `run_config.toml`：

```toml
# config/run_config.toml
[run]
entity = "SPX"
processing_type = "PO"
processing_date = 202602

[debug]
step_by_step = false
save_checkpoints = true
verbose = false

[resume]
enabled = false
checkpoint_name = ""
from_step = ""

[output]
output_dir = "./output"
auto_export = true
```

然後直接執行：

```bash
python main_pipeline.py
```

系統會自動：
1. 讀取 `run_config.toml` → `RunConfig`
2. 讀取 `paths.toml` → `file_paths`
3. 根據 entity/type 選擇正確的 orchestrator
4. 建立 pipeline 並執行
5. 輸出結果到 `./output/`

### 5.2 程式化呼叫 load_file_paths

若需要在自訂腳本中程式化地存取路徑配置：

```python
from accrual_bot.runner import load_file_paths

# 載入 SPX PO 2026年2月的路徑配置
file_paths = load_file_paths("SPX", "PO", 202602)

# 回傳結果範例：
# {
#   "raw_po": {
#       "path": "C:/SEA/Accrual/.../202602/Original Data/202602_purchase_order_20260201.csv",
#       "params": {
#           "encoding": "utf-8",
#           "sep": ",",
#           "dtype": str,           # 注意：已從字串 "str" 轉換為 Python type
#           "keep_default_na": False
#       }
#   },
#   "previous": {
#       "path": "C:/SEA/Accrual/.../202602/前期底稿/SPX/202601_PO_FN.xlsx",
#       "params": {
#           "sheet_name": 0,
#           "header": 0,
#           "dtype": str
#       }
#   },
#   "procurement_po": {
#       "path": "C:/SEA/Accrual/.../202602/procurement_202602.xlsx",
#       "params": {...}
#   },
#   ...
# }

# 存取特定檔案的路徑
raw_po_path = file_paths["raw_po"]["path"]
raw_po_params = file_paths["raw_po"]["params"]

# 直接傳入 pandas
import pandas as pd
df = pd.read_csv(raw_po_path, **raw_po_params)
```

**使用自訂 paths.toml 路徑**（測試或多環境場景）：

```python
from pathlib import Path
from accrual_bot.runner import load_file_paths

# 使用測試環境的路徑配置
test_paths_file = Path("tests/fixtures/test_paths.toml")
file_paths = load_file_paths("SPX", "PO", 202602, paths_file=test_paths_file)
```

### 5.3 逐步執行模式

設定 `run_config.toml` 的 `step_by_step = true` 後執行，或直接在程式碼中使用：

```python
from accrual_bot.runner import load_run_config, load_file_paths, StepByStepExecutor
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.core.pipeline import ProcessingContext
import pandas as pd
import asyncio

# 1. 載入配置
config = load_run_config()
file_paths = load_file_paths(config.entity, config.processing_type, config.processing_date)

# 2. 建立 pipeline
orchestrator = SPXPipelineOrchestrator()
pipeline = orchestrator.build_po_pipeline(file_paths)

# 3. 建立初始 context
context = ProcessingContext(
    data=pd.DataFrame(),
    entity_type="SPX",
    processing_date=202602,
    processing_type="PO"
)
context.set_variable('file_paths', file_paths)

# 4. 建立逐步執行器
executor = StepByStepExecutor(
    pipeline=pipeline,
    context=context,
    save_checkpoints=True,
    checkpoint_dir="./checkpoints"
)

# 5. 執行（互動式）
result = asyncio.run(executor.run())
```

執行時的 CLI 輸出範例：

```
============================================================
Pipeline: SPX_PO_Pipeline
Entity: SPX
Processing Date: 202602
Total Steps: 15
============================================================

逐步執行模式已啟用
指令: [Enter]=繼續 | [s]=跳過 | [q]=中止

------------------------------------------------------------
步驟 1/15: SPXDataLoading
------------------------------------------------------------
執行 'SPXDataLoading'? [Enter/s/q]:
[按 Enter 繼續]

[OK] Status: success
    Duration: 3.42s
    Message: 載入完成
    Data rows: 1523

------------------------------------------------------------
步驟 2/15: ProductFilter
------------------------------------------------------------
執行 'ProductFilter'? [Enter/s/q]: s
[按 s 跳過此步驟]

[SKIP] Status: skipped
    Message: User skipped

------------------------------------------------------------
步驟 3/15: ColumnAddition
------------------------------------------------------------
執行 'ColumnAddition'? [Enter/s/q]: q
[按 q 中止]
用戶中止執行

============================================================
執行中止
============================================================
成功: 1 | 失敗: 0 | 跳過: 1
總耗時: 4.15s
============================================================
```

### 5.4 從 Checkpoint 恢復（Resume 模式）

當 pipeline 在中途失敗或中止，可從 checkpoint 恢復：

**步驟一**：確認 checkpoint 存在（`checkpoints/` 目錄下的 `.parquet` 檔案）

**步驟二**：設定 `run_config.toml`：

```toml
[run]
entity = "SPX"
processing_type = "PO"
processing_date = 202602

[resume]
enabled = true
checkpoint_name = "SPX_PO_202602_after_SPXDataLoading"  # checkpoint 名稱
from_step = "ProductFilter"                              # 從哪個步驟重新開始

[debug]
step_by_step = false  # Resume 模式下通常不需要逐步執行
```

**步驟三**：執行（系統會跳過 `SPXDataLoading`，從 `ProductFilter` 開始）：

```bash
python main_pipeline.py
```

**注意**：Resume 模式使用 `PipelineWithCheckpoint`（來自 `core/pipeline/`），**不是** `StepByStepExecutor`。`runner` 模組提供的是配置載入，實際的 checkpoint 恢復邏輯由 `core` 層負責。

### 5.5 非互動式環境中使用 StepByStepExecutor

`StepByStepExecutor` 設計為在非互動式環境（無 TTY stdin）自動繼續所有步驟：

```bash
# 方式一：將 /dev/null 重定向到 stdin（所有 input() 立即得到 EOFError）
python main_pipeline.py < /dev/null

# 方式二：管道輸入（等效）
echo "" | python main_pipeline.py
```

在 CI/CD 環境中（如 GitHub Actions、Jenkins），stdin 通常不是 TTY，因此 `EOFError` 會自動被捕捉，`StepByStepExecutor` 退化為「自動繼續模式」。

在 Python 程式碼中模擬此行為：

```python
import io
import sys
import asyncio

# 替換 stdin 為空 BytesIO，所有 input() 立即 raise EOFError
old_stdin = sys.stdin
sys.stdin = io.StringIO("")

try:
    result = asyncio.run(executor.run())
finally:
    sys.stdin = old_stdin  # 恢復 stdin

# 等效於自動確認所有步驟
assert result["aborted"] == False
```

---

## 6. 優缺分析（Strengths and Weaknesses）

### 6.1 優點

**1. TOML 配置驅動的執行控制**

`run_config.toml` 讓操作者無需修改任何 Python 程式碼即可切換 entity、processing type、date 和執行模式。這完全符合 12-Factor App 的配置原則：「配置在環境中，代碼在倉庫中」。對於月度批次系統而言，每月只需修改 `processing_date` 這一個數字，大幅降低操作錯誤的可能性。

**2. 路徑模板系統的表達力**

`{YYYYMM}`, `{PREV_YYYYMM}`, `{resources}` 等變數讓 `paths.toml` 成為跨月份可復用的路徑配置。相比在程式碼中硬編碼路徑，或在每次執行時手動修改路徑，模板系統將路徑邏輯集中在一個地方，減少了重複和出錯機會。glob 萬用字元支援（`*`）進一步處理了檔名不完全固定的情況。

**3. RunConfig dataclass 的型別安全**

相比傳遞 raw `Dict[str, Any]`，`RunConfig` dataclass 提供了：
- IDE autocomplete（`config.entity` vs `config["entity"]`）
- mypy 靜態型別檢查支援
- 清晰的 API 文件（欄位名稱、型別、預設值一目瞭然）
- 防止拼錯欄位名（`config.entiy` 會在程式碼檢查時發現，但 `config["entiy"]` 需要執行時才報錯）

**4. EOFError 優雅降級**

`_prompt_action()` 和 `_confirm_continue_on_error()` 對 `EOFError` 的捕捉使 `StepByStepExecutor` 在非互動式環境中安全運行（CI/CD、腳本化執行），而不是崩潰。這個設計使同一套工具可在不同環境下使用，無需額外分支邏輯。

**5. 小而聚焦的模組**

506 行程式碼（3 個檔案）處理了明確的職責：配置載入 + 互動式執行。沒有額外的 God class，沒有過度抽象。這種簡潔性使模組易於理解、測試和修改。

**6. 日期算術的純整數設計**

整個系統使用 `int` 型別的 `YYYYMM` 格式，避免了 `datetime` 物件在不同時區、DST 切換下的行為不一致問題。對於月度批次系統，年月精度已足夠，不需要完整的日期時間物件。

**7. 參數型別轉換的透明性**

`_convert_params()` 明確處理 TOML 值到 pandas 參數的型別差異（`"str"` → `str`），這個轉換在其他方案（如直接傳 raw dict）中容易被忽略而導致難以察覺的 bug。

### 6.2 缺點與設計問題

**1. `load_file_paths` 的潛在 AttributeError（P0 - 高嚴重性）**

```python
# 第 113-114 行：
paths_section = paths_config.get(entity_lower, {}).get(type_lower, {})
# 以上一行安全，但下一行有風險：
params_section = paths_config.get(entity_lower).get(f"{type_lower}").get('params')
```

若 `entity_lower` 不在 `paths_config` 中，`paths_config.get(entity_lower)` 回傳 `None`，接著 `None.get(...)` 會 raise `AttributeError`，而不是清楚的錯誤訊息。

> **警告**：這是一個隱性的崩潰點。當用戶輸入了不存在的 entity 名稱時，錯誤訊息是 `AttributeError: 'NoneType' object has no attribute 'get'`，完全無法從錯誤訊息推斷根本原因。

**正確修復**：

```python
# 方案一：鏈式 .get() 加預設值
entity_section = paths_config.get(entity_lower, {})
type_section = entity_section.get(type_lower, {})
params_section = type_section.get('params', {})

# 方案二：提前驗證並給出清楚錯誤訊息
if entity_lower not in paths_config:
    raise ValueError(f"paths.toml 中找不到 entity '{entity_lower}'，可用的 entity: {list(paths_config.keys())}")
```

**2. `_save_checkpoint` 中的死碼（P1 - 中嚴重性）**

```python
def _save_checkpoint(self, step_name: str):
    checkpoint_name = (
        f"{self.context.metadata.entity_type}_"
        f"{self.context.metadata.processing_type}_"
        f"{self.context.metadata.processing_date}_"
        f"after_{step_name}"
    )
    self.checkpoint_manager.save_checkpoint(self.context, step_name)  # 用 step_name，非 checkpoint_name
    logger.debug(f"Checkpoint saved: {checkpoint_name}")  # 日誌顯示的名稱與實際不符
```

建構的 `checkpoint_name` 完全未使用，導致日誌訊息顯示的名稱與實際 checkpoint 檔案名稱不一致。

**3. `verbose` 欄位定義但未實作（P2 - 低嚴重性）**

`RunConfig` 定義了 `verbose: bool = False`，但 `main_pipeline.py` 從未根據此值調整日誌 level。這是一個**承諾了但未兌現**的功能，會讓使用者誤以為設定 `verbose = true` 會產生更詳細的日誌輸出。

**4. Glob 字典序選擇的隱性假設（P1 - 中嚴重性）**

`sorted(matches)[-1]` 隱含假設「字典序最大 = 最新版本」，但這個假設在：
- 檔名不包含日期
- 版本號超過個位數（`v9` < `v10` 在字典序中）
- 命名慣例改變

等情況下會靜默選錯檔案，且沒有任何警告。

**5. 路徑模板無未解析變數驗證（P1 - 中嚴重性）**

`_resolve_path_template` 靜默忽略未解析的變數：

```python
# paths.toml 中拼錯變數名：
raw_po = "{resources}/{YYYMM}/..."  # YYYMM 是錯誤的，應為 YYMM

# 解析結果：
# "C:/SEA/Accrual/{YYYMM}/..."  ← {YYYMM} 字面量保留在路徑中
# 後續 I/O 失敗時的錯誤訊息與根本原因相距甚遠
```

**6. `StepByStepExecutor` 繞過 `pipeline.execute()`（P2 - 低嚴重性）**

直接迭代 `pipeline.steps` 跳過了 Pipeline 自身的執行邏輯，包括 `stop_on_error` 行為和潛在的 pre/post hooks。這制造了一個維護陷阱：`Pipeline.execute()` 行為改變時，`StepByStepExecutor` 不會自動同步。

**7. 雙執行路徑結果字典結構不一致（P2 - 低嚴重性）**

`StepByStepExecutor.run()` 和 `pipeline.execute()` 回傳格式不同，消費端必須做防禦性存取（`result.get("successful_steps")`），暗示了一個**隱性的 interface contract**，缺乏顯式定義。

**8. `_convert_params` 的硬編碼轉換表（P3 - 技術債）**

目前只硬編碼處理 `dtype="str"` 的轉換，未來若需要支援更多型別轉換（如 `dtype="int64"`），需修改函數本體而非配置，違反開放封閉原則。

**9. Windows 絕對路徑硬編碼在 `paths.toml`（P2 - 環境依賴）**

```toml
[base]
resources = "C:/SEA/Accrual/prpo_bot/resources/頂一下"
```

此路徑在 Linux/macOS 上無效，在容器化部署時需手動修改，缺乏環境變數覆蓋機制。

**10. 向後相容函數的 hardcoded 日期（P3 - 技術債）**

`main_pipeline.py` 底部的 `run_spx_po_full_pipeline()` 等函數硬編碼 `processing_date = 202512`，在 2026 年後已是過時日期。若有人誤用這些函數，會處理 2025年12月的數據而非當月數據，且不會有任何警告。

---

## 7. 延伸議題（Advanced Topics & Future Considerations）

### 7.1 配置驗證層（Configuration Validation）

目前 `load_run_config()` 完全信任 TOML 內容，不進行任何業務規則驗證。這對於開發者自用的工具來說可以接受，但若系統需要更廣泛地被使用，應引入驗證層。

**使用 `__post_init__` 驗證**（無額外依賴）：

```python
from dataclasses import dataclass
from typing import Set

SUPPORTED_ENTITIES: Set[str] = {"SPX", "SPT"}
SUPPORTED_TYPES: dict = {
    "SPX": {"PO", "PR", "PPE", "PPE_DESC"},
    "SPT": {"PO", "PR", "PROCUREMENT"},
}

@dataclass
class RunConfig:
    entity: str
    processing_type: str
    processing_date: int
    # ... 其他欄位

    def __post_init__(self):
        # 驗證 entity
        if self.entity not in SUPPORTED_ENTITIES:
            raise ValueError(
                f"不支援的 entity: '{self.entity}'。"
                f"可用的 entity: {sorted(SUPPORTED_ENTITIES)}"
            )

        # 驗證 processing_type
        valid_types = SUPPORTED_TYPES.get(self.entity, set())
        if self.processing_type not in valid_types:
            raise ValueError(
                f"entity '{self.entity}' 不支援 type '{self.processing_type}'。"
                f"可用的 type: {sorted(valid_types)}"
            )

        # 驗證 processing_date 格式
        year = self.processing_date // 100
        month = self.processing_date % 100
        if not (1 <= month <= 12):
            raise ValueError(
                f"無效的 processing_date: {self.processing_date}。"
                f"月份 {month} 不在 1-12 範圍內"
            )
        if year < 2020:
            raise ValueError(f"processing_date {self.processing_date} 的年份 {year} 可能有誤")

        # 驗證 resume 模式的完整性
        if self.resume_enabled and not self.checkpoint_name:
            raise ValueError("resume.enabled = true 時，必須指定 resume.checkpoint_name")
```

**使用 Pydantic**（更完整的驗證，但需要額外依賴）：

```python
from pydantic import BaseModel, validator, Field

class RunConfig(BaseModel):
    entity: str
    processing_type: str
    processing_date: int = Field(..., ge=202001, le=209912)

    @validator('entity')
    def validate_entity(cls, v):
        if v not in SUPPORTED_ENTITIES:
            raise ValueError(f"不支援的 entity: {v}")
        return v

    @validator('processing_date')
    def validate_date(cls, v):
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"無效月份: {month}")
        return v

    class Config:
        frozen = True  # 不可變
```

### 7.2 路徑模板引擎的強化

當前的 `str.replace()` 實作可以幾個方向強化：

**方向一：使用 `str.format_map()` 並處理 KeyError**

```python
def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    try:
        return template.format_map(vars)
    except KeyError as e:
        raise ValueError(
            f"路徑模板 '{template}' 中有未定義的變數: {e}。"
            f"可用的變數: {list(vars.keys())}"
        ) from e
```

注意：`str.format_map()` 要求模板中的 `{` `}` 是變數占位符，若路徑中本身有字面量大括號（如 Windows 路徑中罕見但可能出現），需要跳脫為 `{{` `}}`。

**方向二：後驗證未解析變數**

```python
import re

def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    result = template
    for var_name, var_value in vars.items():
        result = result.replace(f"{{{var_name}}}", var_value)

    # 驗證是否還有未解析的變數
    unresolved = re.findall(r'\{[A-Z_]+\}', result)
    if unresolved:
        raise ValueError(
            f"路徑模板解析後仍有未解析的變數: {unresolved}。"
            f"原始模板: '{template}'"
        )
    return result
```

**方向三：`string.Template`（最保守）**

```python
from string import Template

def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    # 將 {VAR} 格式轉換為 ${VAR} 格式
    tmpl_str = re.sub(r'\{([A-Z_]+)\}', r'${\1}', template)
    tmpl = Template(tmpl_str)
    try:
        return tmpl.substitute(vars)  # 嚴格模式：未知變數 raise ValueError
    except (KeyError, ValueError) as e:
        raise ValueError(f"路徑模板替換失敗: {e}。原始模板: '{template}'") from e
```

### 7.3 StepByStepExecutor 的 UI 解耦

目前 `StepByStepExecutor` 的 prompt 和 print 直接寫入 `stdout`，UI 邏輯（顯示）與業務邏輯（步驟執行）耦合。若需要重用邏輯於 Streamlit 或其他介面，應引入 callback 介面：

```python
from typing import Callable, Awaitable, Optional

class StepByStepExecutor:
    def __init__(
        self,
        pipeline: Pipeline,
        context: ProcessingContext,
        save_checkpoints: bool = True,
        checkpoint_dir: str = "./checkpoints",
        # 可注入的 UI callbacks
        prompt_fn: Optional[Callable[[str], Awaitable[str]]] = None,
        display_fn: Optional[Callable[[str], None]] = None,
    ):
        self._prompt = prompt_fn or self._default_prompt
        self._display = display_fn or print

    async def _default_prompt(self, step_name: str) -> str:
        """預設的 CLI prompt 實作"""
        try:
            response = input(f"\n執行 '{step_name}'? [Enter/s/q]: ").strip().lower()
            if response in ("", "c"):
                return "continue"
            elif response == "s":
                return "skip"
            elif response == "q":
                return "abort"
        except EOFError:
            return "continue"
```

這樣 Streamlit UI 可以注入自訂的非阻塞 prompt（如彈出對話框），而 CLI 使用者繼續使用預設的 `input()` 行為。

### 7.4 Pipeline.execute() Hook 機制

`StepByStepExecutor` 繞過 `pipeline.execute()` 的根本原因是 `Pipeline` 沒有提供步驟間的鉤子（hook）機制。最優雅的解法是在 `Pipeline.execute()` 中加入可選的 hook 參數：

```python
from typing import Optional, Callable, Awaitable

class Pipeline:
    async def execute(
        self,
        context: ProcessingContext,
        pre_step_hook: Optional[Callable] = None,
        post_step_hook: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        執行 pipeline 中的所有步驟。

        Args:
            pre_step_hook: async (step, index, total) -> str
                           回傳 "continue"/"skip"/"abort"
            post_step_hook: async (step, result) -> None
        """
        for i, step in enumerate(self.steps):
            # Pre-step hook
            if pre_step_hook is not None:
                action = await pre_step_hook(step, i, len(self.steps))
                if action == "skip":
                    continue
                elif action == "abort":
                    break

            # 執行步驟
            result = await step.execute(context)

            # Post-step hook
            if post_step_hook is not None:
                await post_step_hook(step, result)

            # ... 其他邏輯
```

這樣 `StepByStepExecutor` 可以重寫為：

```python
class StepByStepExecutor:
    async def run(self) -> Dict[str, Any]:
        result = await self.pipeline.execute(
            self.context,
            pre_step_hook=self._pre_step,
            post_step_hook=self._post_step,
        )
        return result

    async def _pre_step(self, step, index, total):
        self._print_step_header(index + 1, total, step.name)
        return self._prompt_action(step.name)

    async def _post_step(self, step, result):
        self._print_step_result(result)
        if self.save_checkpoints and result.is_success:
            self._save_checkpoint(step.name)
```

這樣就**完全不需要繞過** `pipeline.execute()`，所有 Pipeline 的執行語義（`stop_on_error`、`validate_input()`、hooks 等）都會被正確執行。

### 7.5 verbose 欄位的完整實作

若要真正支援 verbose 模式（更詳細的日誌輸出），需要在 `main_pipeline.py` 的 `main()` 函數開頭加入：

```python
import logging
from accrual_bot.runner import load_run_config

async def main():
    config = load_run_config()

    # 根據 verbose 設定調整日誌 level
    if config.verbose:
        # 將 root logger 和所有 accrual_bot logger 設為 DEBUG
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("accrual_bot").setLevel(logging.DEBUG)
        logger.debug("Verbose 模式已啟用，日誌 level 設為 DEBUG")
    else:
        # 生產環境使用 INFO
        logging.getLogger("accrual_bot").setLevel(logging.INFO)

    # ... 其他執行邏輯
```

或者更細緻地，讓 `verbose` 只影響特定模組：

```python
if config.verbose:
    # 只顯示 runner 和 tasks 的詳細日誌，不顯示 core/datasources 的 debug 日誌
    logging.getLogger("accrual_bot.runner").setLevel(logging.DEBUG)
    logging.getLogger("accrual_bot.tasks").setLevel(logging.DEBUG)
```

### 7.6 多環境配置支援（Environment-aware Paths）

`paths.toml` 中的硬編碼 Windows 路徑是跨平台部署的主要障礙。有幾種方案可以解決：

**方案一：環境變數覆蓋**

```toml
# paths.toml 中使用環境變數占位符
[base]
resources = "${ACCRUAL_RESOURCES_DIR}"
```

```python
import os

def _resolve_path_template(template: str, vars: Dict[str, str]) -> str:
    # 先替換 date vars
    result = template
    for var_name, var_value in vars.items():
        result = result.replace(f"{{{var_name}}}", var_value)

    # 再替換環境變數
    result = os.path.expandvars(result)  # ${VAR} → 環境變數值
    return result
```

**方案二：覆蓋層機制（類 docker-compose override）**

```
config/
├── paths.toml         # 基礎配置（相對路徑）
└── paths.local.toml   # 本地覆蓋（gitignored，絕對路徑）
```

```python
def load_file_paths(..., paths_file=None):
    if paths_file is None:
        base_file = get_config_dir() / "paths.toml"
        local_file = get_config_dir() / "paths.local.toml"

        with open(base_file, "rb") as f:
            paths_config = tomllib.load(f)

        if local_file.exists():
            with open(local_file, "rb") as f:
                local_config = tomllib.load(f)
            # 深度合併（local 覆蓋 base）
            paths_config = _deep_merge(paths_config, local_config)
```

**方案三：使用相對路徑 + 工作目錄**

```toml
[base]
resources = "./resources"  # 相對於工作目錄
```

相對路徑的好處是零配置跨平台，但需要使用者從特定目錄執行程式。

### 7.7 _convert_params 的型別轉換表設計

當前的 if-elif 轉換邏輯可改為宣告式設計，提高可擴展性：

```python
from typing import Any, Callable, Dict, Union

# 宣告式轉換表
_DTYPE_MAP: Dict[str, Any] = {
    "str": str,
    "string": str,
    "int": int,
    "int64": "int64",       # numpy dtype 字串（pandas 接受）
    "float": float,
    "float64": "float64",
    "bool": bool,
    "category": "category",  # pandas categorical dtype
}

_PARAM_CONVERTERS: Dict[str, Callable[[Any], Any]] = {
    "dtype": lambda v: _DTYPE_MAP.get(v, v) if isinstance(v, str) else {
        k: _DTYPE_MAP.get(val, val) for k, val in v.items()
    } if isinstance(v, dict) else v,
    "header": lambda v: int(v) if isinstance(v, str) and v.isdigit() else v,
    "sheet_name": lambda v: int(v) if isinstance(v, str) and v.isdigit() else v,
    "na_values": lambda v: v,   # 直接傳遞（list 或字串均可）
    "usecols": lambda v: v,     # 直接傳遞（字串或 list 均可）
}

def _convert_params(params: Dict[str, Any]) -> Dict[str, Any]:
    result = {}
    for key, value in params.items():
        converter = _PARAM_CONVERTERS.get(key)
        result[key] = converter(value) if converter else value
    return result
```

這樣新增支援的參數型別只需在 `_PARAM_CONVERTERS` 字典中加入一條記錄，無需修改函數本體。

---

## 8. 其他（Miscellaneous）

### 8.1 模組在系統中的定位圖

以下架構圖展示了 `runner` 模組在整個 `accrual_bot` 系統中的位置與依賴關係：

```
                    ┌─────────────────────────────────┐
                    │       main_pipeline.py           │
                    │       （CLI 進入點）               │
                    │                                  │
                    │  1. load_run_config()            │
                    │  2. load_file_paths()            │
                    │  3. 建立 pipeline                │
                    │  4. 執行 pipeline                │
                    └──────────┬───────────────────────┘
                               │ import
                               │
                    ┌──────────▼───────────────────────┐
                    │     accrual_bot/runner/           │ ← 本模組
                    │                                  │
                    │  ┌───────────────────────────┐   │
                    │  │    config_loader.py        │   │
                    │  │                            │   │
                    │  │  RunConfig (dataclass)     │   │
                    │  │  load_run_config()    ─────┼───┼──→ run_config.toml
                    │  │  load_file_paths()    ─────┼───┼──→ paths.toml
                    │  │  _calculate_date_vars()    │   │
                    │  │  _resolve_path_template()  │   │
                    │  │  _convert_params()         │   │
                    │  └───────────────────────────┘   │
                    │                                  │
                    │  ┌───────────────────────────┐   │
                    │  │    step_executor.py        │   │
                    │  │                            │   │
                    │  │  StepByStepExecutor        │   │
                    │  │  └── run()                 │   │
                    │  │      ├── _prompt_action()  │   │
                    │  │      ├── _print_*()        │   │
                    │  │      └── _save_checkpoint()│   │
                    │  └───────────────────────────┘   │
                    └──────────┬───────────────────────┘
                               │ 呼叫
         ┌─────────────────────┼──────────────────────┐
         ▼                     ▼                      ▼
  ┌─────────────┐     ┌─────────────────┐    ┌────────────────────┐
  │ tasks/spt/  │     │   tasks/spx/    │    │  core/pipeline/    │
  │             │     │                 │    │                    │
  │ SPTPipeline │     │  SPXPipeline    │    │  Pipeline          │
  │ Orchestrator│     │  Orchestrator   │    │  ProcessingContext │
  └─────────────┘     └─────────────────┘    │  CheckpointManager │
                                             └────────────────────┘
                               │ 使用
                    ┌──────────▼───────────────────────┐
                    │       utils/                     │
                    │  get_logger()                    │
                    └──────────────────────────────────┘
```

**依賴方向**：
- `runner` → `core/pipeline/`（`Pipeline`, `ProcessingContext`, `StepResult` 等型別）
- `runner` → `utils/logging`（`get_logger()`）
- `runner` → 檔案系統（`run_config.toml`, `paths.toml`）
- `runner` **不依賴** `tasks/`（runner 不知道 SPX 或 SPT 的存在）

這個依賴圖是乾淨的——`runner` 只向下依賴（core + utils），不向橫依賴（tasks），避免了循環依賴。

### 8.2 與 UI 層的對應關係

`runner` 模組與 UI 層的 `UnifiedPipelineService` 在職責上高度重疊，形成了 CLI 和 Web UI 的鏡像結構：

| 職責 | CLI 實作 | Web UI 實作 |
|------|---------|------------|
| 載入執行配置 | `load_run_config()` | Streamlit session state（頁面一填寫） |
| 載入並解析路徑 | `load_file_paths()` | `UnifiedPipelineService._enrich_file_paths()` |
| 建立 pipeline | `orchestrator.build_*_pipeline()` | `UnifiedPipelineService.build_pipeline()` |
| 執行 pipeline | `StepByStepExecutor` / `pipeline.execute()` | `StreamlitPipelineRunner.run_pipeline()` |
| 進度回饋 | `print()` 到 stdout | Streamlit `st.progress()` + callback |

**關鍵差異**：

`load_file_paths()` 與 `_enrich_file_paths()` 的路徑解析邏輯相近，但針對不同場景：

- CLI 的 `load_file_paths()`：從 `paths.toml` 讀取模板，解析為實際路徑
- UI 的 `_enrich_file_paths()`：從使用者上傳的檔案（`UploadedFile`）建立臨時路徑

兩者理應共享路徑解析邏輯（特別是日期計算和 glob 解析部分），但目前是各自獨立實作。這是一個潛在的重構機會：將 `_calculate_date_vars()` 等純函數提取到 `utils/helpers/` 中供兩端共用。

### 8.3 測試現況

> **警告**：`runner` 模組目前在 `tests/` 目錄中沒有對應的單元測試。

這是顯著的測試覆蓋空白，尤其考慮到 `config_loader.py` 中包含了幾個具有業務邏輯的純函數，這些函數天然易於測試。

**可立即加入的測試（無 mock 需求）**：

```python
# tests/unit/runner/test_config_loader.py

import pytest
from accrual_bot.runner.config_loader import (
    _calculate_date_vars,
    _resolve_path_template,
    _convert_params,
)

class TestCalculateDateVars:
    def test_normal_month(self):
        vars = _calculate_date_vars(202602)
        assert vars["YYYYMM"] == "202602"
        assert vars["PREV_YYYYMM"] == "202601"
        assert vars["YYYY"] == "2026"
        assert vars["MM"] == "02"
        assert vars["YYMM"] == "2602"

    def test_january_crosses_year(self):
        """1月的前月應為前一年的12月"""
        vars = _calculate_date_vars(202601)
        assert vars["PREV_YYYYMM"] == "202512"

    def test_december(self):
        vars = _calculate_date_vars(202512)
        assert vars["PREV_YYYYMM"] == "202511"


class TestResolvePathTemplate:
    def test_basic_substitution(self):
        template = "{resources}/{YYYYMM}/data.csv"
        vars = {"resources": "C:/data", "YYYYMM": "202602"}
        assert _resolve_path_template(template, vars) == "C:/data/202602/data.csv"

    def test_unresolved_variable_silently_remains(self):
        """當前實作：未解析的變數靜默保留（這是一個已知的設計問題）"""
        template = "{resources}/{UNKNOWN}/data.csv"
        vars = {"resources": "C:/data", "YYYYMM": "202602"}
        result = _resolve_path_template(template, vars)
        assert "{UNKNOWN}" in result  # 仍然含有未解析的變數


class TestConvertParams:
    def test_dtype_str_converts_to_type(self):
        result = _convert_params({"dtype": "str"})
        assert result["dtype"] is str

    def test_bool_passthrough(self):
        result = _convert_params({"keep_default_na": False})
        assert result["keep_default_na"] is False

    def test_other_params_passthrough(self):
        result = _convert_params({"sheet_name": 0, "header": 3, "encoding": "utf-8"})
        assert result["sheet_name"] == 0
        assert result["header"] == 3
        assert result["encoding"] == "utf-8"
```

**需要 mock 的測試**：

```python
# tests/unit/runner/test_config_loader_io.py

import pytest
import tomllib
from pathlib import Path
from unittest.mock import mock_open, patch

class TestLoadRunConfig:
    def test_load_with_custom_path(self, tmp_path):
        """使用 tmp_path fixture 測試自訂配置路徑"""
        config_content = b"""
[run]
entity = "SPT"
processing_type = "PR"
processing_date = 202603

[debug]
step_by_step = true
save_checkpoints = false
"""
        config_file = tmp_path / "test_run_config.toml"
        config_file.write_bytes(config_content)

        from accrual_bot.runner import load_run_config
        config = load_run_config(config_file)

        assert config.entity == "SPT"
        assert config.processing_type == "PR"
        assert config.processing_date == 202603
        assert config.step_by_step == True
        assert config.save_checkpoints == False

    def test_defaults_when_sections_missing(self, tmp_path):
        """測試 TOML 缺少某些 section 時的預設值"""
        config_file = tmp_path / "minimal.toml"
        config_file.write_bytes(b"""
[run]
entity = "SPX"
processing_type = "PO"
processing_date = 202602
""")
        from accrual_bot.runner import load_run_config
        config = load_run_config(config_file)

        assert config.step_by_step == False   # debug section 預設
        assert config.resume_enabled == False  # resume section 預設
        assert config.output_dir == "./output" # output section 預設
```

**StepByStepExecutor 的測試策略**：

由於 `input()` 阻塞事件迴圈且依賴 stdin，可使用 `EOFError` 路徑測試自動繼續行為：

```python
# tests/unit/runner/test_step_executor.py

import asyncio
import sys
import io
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

class TestStepByStepExecutorNonInteractive:
    """測試 EOFError 路徑（非互動式模式）"""

    @pytest.fixture
    def mock_pipeline(self):
        pipeline = MagicMock()
        pipeline.config.name = "TEST_PIPELINE"
        pipeline.steps = []
        return pipeline

    @pytest.mark.asyncio
    async def test_empty_pipeline_completes_successfully(self, mock_pipeline):
        from accrual_bot.runner import StepByStepExecutor

        context = MagicMock()
        context.metadata.entity_type = "TEST"
        context.metadata.processing_date = 202602
        context.data = None
        context.errors = []
        context.warnings = []

        executor = StepByStepExecutor(mock_pipeline, context, save_checkpoints=False)

        # 替換 stdin 為空，觸發 EOFError 路徑
        with patch('builtins.input', side_effect=EOFError):
            result = await executor.run()

        assert result["success"] == True
        assert result["aborted"] == False
        assert result["total_steps"] == 0
```

### 8.4 程式碼統計

| 檔案 | 行數 | 函數/方法數 | Classes | 主要職責 |
|------|------|------------|---------|---------|
| `__init__.py` | 20 | 0 | 0 | 公開 API 匯出 |
| `config_loader.py` | 240 | 6 | 1（RunConfig dataclass） | 配置載入 + 路徑解析 |
| `step_executor.py` | 246 | 9 | 1（StepByStepExecutor） | 互動式逐步執行 |
| **合計** | **506** | **15** | **2** | — |

**各函數行數分佈**：

| 函數 | 所在檔案 | 行數 | 說明 |
|------|---------|------|------|
| `StepByStepExecutor.run()` | step_executor.py | ~50 | 主執行迴圈 |
| `load_file_paths()` | config_loader.py | ~40 | 路徑解析主邏輯 |
| `StepByStepExecutor._build_execution_result()` | step_executor.py | ~30 | 建構結果字典 |
| `load_run_config()` | config_loader.py | ~25 | RunConfig 載入 |
| `_calculate_date_vars()` | config_loader.py | ~15 | 日期變數計算 |
| `StepByStepExecutor._print_header()` | step_executor.py | ~10 | 輸出抬頭資訊 |

**複雜度分析**：

- `config_loader.py`：整體複雜度低，最複雜的是 `load_file_paths()`（兩層巢狀迴圈 + glob 邏輯）
- `step_executor.py`：整體複雜度中等，最複雜的是 `run()`（狀態機模式的 async 迴圈）
- 無遞迴調用，無深層巢狀（最深三層）

**外部依賴**：

| 依賴 | 類型 | 用途 |
|------|------|------|
| `tomllib` | Python 3.11+ stdlib | TOML 解析 |
| `glob` | stdlib | 萬用字元路徑展開 |
| `pathlib` | stdlib | 路徑操作 |
| `dataclasses` | stdlib | RunConfig dataclass |
| `datetime` | stdlib | 執行時間記錄 |
| `time` | stdlib | 步驟計時 |
| `accrual_bot.core.pipeline` | 內部 | Pipeline, ProcessingContext |
| `accrual_bot.core.pipeline.base` | 內部 | StepResult, StepStatus |
| `accrual_bot.core.pipeline.checkpoint` | 內部 | CheckpointManager |
| `accrual_bot.utils.logging` | 內部 | get_logger() |

所有外部依賴均為 Python stdlib，無第三方套件依賴，使 `runner` 模組的部署門檻極低。

---

*本文件涵蓋 `accrual_bot/runner/` 模組的完整技術分析，包含設計決策的背景脈絡、已知缺陷的精確定位、以及具體可行的改進建議。文件中指出的問題嚴重性（P0-P3）僅為參考，實際優先順序應根據業務影響評估。*
