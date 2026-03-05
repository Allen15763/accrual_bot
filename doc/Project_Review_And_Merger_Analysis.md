# 專案審查與合併可行性分析報告

> **撰寫日期**：2026-03-05
> **範圍**：accrual_bot 與 spe_bank_recon 兩個專案的完整架構審查，以及合併可行性評估
> **方法**：逐一對照參考文件（Project_Design_Reference.md、Project_Architecture_Reference.md、Unified_System_Design_Reference.md）與實際程式碼

---

## 目錄

1. [accrual_bot — 現況審查](#1-accrual_bot--現況審查)
2. [spe_bank_recon — 現況審查](#2-spe_bank_recon--現況審查)
3. [合併可行性分析](#3-合併可行性分析)
4. [建議行動清單](#4-建議行動清單)

---

## 1. accrual_bot — 現況審查

### 1.1 架構整體評估

整體架構符合 `Project_Design_Reference.md` 描述的四層架構，核心 Pipeline 框架實作完整，配置驅動機制正常運作。但在**步驟歸屬位置**與**文件同步度**上有明顯落差。

**整體評分**：架構設計 ✅ 良好，文件同步度 ⚠️ 待更新，程式碼組織 ⚠️ 部分偏差

---

### 1.2 與參考文件的差異清單

#### [差異 1] 步驟檔案位置偏離設計原則（中優先）

**參考文件設計**（Project_Design_Reference.md §3）：
```
tasks/spt/steps/    ← SPT 特定步驟的實作
tasks/spx/steps/    ← SPX 特定步驟的實作
core/pipeline/steps/  ← 只放共用步驟
```

**實際現況**：
- `core/pipeline/steps/` 包含了大量實體特定步驟（`spt_loading.py`、`spt_evaluation_erm.py`、`spx_loading.py`、`spx_integration.py`、`spx_evaluation.py` 等 ~20 個檔案）
- `tasks/spt/steps/` 只有 Procurement 相關步驟（真正的 SPT 特有新功能）
- `tasks/spx/steps/` 只有 `spx_ppe_desc.py` 一個檔案
- `tasks/*/steps/__init__.py` 透過 re-export 提供向後相容

**影響**：`core/pipeline/steps/` 膨脹為混合層（共用邏輯 + 實體特定邏輯），違反「核心層不含業務邏輯」原則，但向後相容機制保證執行不受影響。

**建議**：此現況是歷史遷移未完成的結果。長期應繼續把 `core/pipeline/steps/` 中的 `spt_*` 和 `spx_*` 檔案移動到對應的 `tasks/` 目錄，完成架構遷移。短期可維持現狀，不影響功能。

---

#### [差異 2] `runner/` 模組未記錄於參考文件（低優先）

**新增模組**：`accrual_bot/runner/`（不在任何參考文件中）

| 檔案 | 功能 |
|------|------|
| `config_loader.py` | 載入 `run_config.toml`，提供 `RunConfig` dataclass 和路徑解析函數 |
| `step_executor.py` | `StepByStepExecutor`——互動式逐步執行，每步後等待用戶確認 |

**背景**：這是一個有價值的**開發輔助工具**，搭配 `run_config.toml` 使用，讓開發者可以 F5 快速切換執行目標，並以逐步模式 debug。

**建議**：
1. 更新 `Project_Design_Reference.md` 補充 `runner/` 模組說明
2. 更新 `CLAUDE.md` 說明 `run_config.toml` 第四個配置檔案的用途

---

#### [差異 3] `run_config.toml` 未在文件中記錄（低優先）

**參考文件定義**的配置體系（三個檔案）：
```
config.ini        ← 一般設定
paths.toml        ← 檔案路徑模板
stagging.toml     ← Pipeline 步驟 + 業務規則
```

**實際存在四個**：
```
config.ini        ← 一般設定
paths.toml        ← 檔案路徑模板
stagging.toml     ← Pipeline 步驟 + 業務規則
run_config.toml   ← 執行控制（entity、type、date、resume、step_by_step）  ← 未記錄
```

`run_config.toml` 內容：
```toml
[run]
entity = "SPX"
processing_type = "PO"
processing_date = 202602

[debug]
step_by_step = false
save_checkpoints = true

[resume]
enabled = false
checkpoint_name = "..."
from_step = "..."
```

**建議**：更新參考文件，將「四層配置體系」的說明補入。

---

#### [差異 4] PROCUREMENT 和 PPE_DESC 處理類型未記錄（中優先）

**CLAUDE.md 定義的 SPT 處理類型**：PO、PR
**實際支援**：PO、PR、**PROCUREMENT**（新增，含 PO/PR/COMBINED 三個子模式）

**CLAUDE.md 定義的 SPX 處理類型**：PO、PR、PPE
**實際支援**：PO、PR、PPE、**PPE_DESC**（新增）

SPT PROCUREMENT 是較複雜的新功能，包含完整的步驟序列：
- `SPTProcurementDataLoadingStep`、`SPTProcurementPRDataLoadingStep`
- `ProcurementPreviousMappingStep`、`SPTProcurementStatusEvaluationStep`
- `ColumnInitializationStep`、`ProcurementPreviousValidationStep`
- `CombinedProcurementDataLoadingStep`、`CombinedProcurementProcessingStep`、`CombinedProcurementExportStep`

**建議**：
1. 更新 CLAUDE.md 的 SPT 處理類型說明，補充 PROCUREMENT 類型
2. 更新 SPX 說明補充 PPE_DESC 類型
3. 在 `ui/config.py` 確認 PROCUREMENT 是否需要加入 UI 流程

---

#### [差異 5] `tasks/mob/` 目錄不存在（低優先）

**CLAUDE.md 和參考文件提及**：`tasks/mob/`（待擴充）
**實際現況**：該目錄完全不存在（連 `__init__.py` 都沒有）

**影響**：無功能影響，純為文件描述超前於實作。

**建議**：CLAUDE.md 中保留「MOB 待擴充」說明即可，不需立即建立空目錄。

---

#### [差異 6] Orchestrator 直接存取 ConfigManager 私有屬性（程式碼品質問題）

**問題位置**：`tasks/spt/pipeline_orchestrator.py:67` 和 `tasks/spx/pipeline_orchestrator.py:63`

```python
# 現況 — 存取私有屬性，違反封裝原則
self.config = config_manager._config_toml.get('pipeline', {}).get('spt', {})
```

**參考文件建議的正確做法**：
```python
# 建議 — 透過公開 API
from accrual_bot.utils.config import ConfigManager
config = ConfigManager()
enabled_steps = config.get_list('pipeline.spt', 'enabled_po_steps')
```

**建議**：在 `ConfigManager` 中新增 `get_pipeline_config(entity)` 便利方法，讓 Orchestrator 不直接操作 `_config_toml`。

---

#### [差異 7] 根目錄存在未記錄的執行檔案（低優先）

| 檔案 | 說明 |
|------|------|
| `main_pipeline_spx_memo_validatation.py` | SPX 備忘錄驗證的獨立執行腳本，未在任何文件提及 |
| `validate_completeness.py` | 驗證工具腳本，未在任何文件提及 |

**建議**：在 README.md 或 CLAUDE.md 補充這些一次性/輔助腳本的說明，或移至 `scripts/` 子目錄統一管理。

---

#### [差異 8] `data/importers/` 模組未在參考文件中說明（低優先）

**存在但未記錄**：`accrual_bot/data/importers/`（`base_importer.py`、`google_sheets_importer.py`）

此模組已存在，但 Project_Design_Reference.md 中的目錄結構章節僅提及 `data/`「Importers, exporters, transformers」，無具體說明。

**建議**：更新文件或確認此模組是否仍被使用（可能已被 `core/datasources/` 取代）。

---

### 1.3 正向確認

以下項目**符合或超出**參考文件的設計要求：

| 項目 | 狀態 | 說明 |
|------|------|------|
| 四層架構邊界 | ✅ | UI/Tasks/Core/Utils 職責清晰 |
| BaseLoadingStep 模板方法 | ✅ | 593 行，實作完整 |
| BaseERMEvaluationStep 模板方法 | ✅ | 518 行，實作完整 |
| ConfigManager 執行緒安全 | ✅ | Double-Checked Locking 實作正確 |
| Pipeline Orchestrators | ✅ | SPT/SPX 均有完整實作 |
| Streamlit 雙層頁面架構 | ✅ | 5 頁工作流符合設計 |
| Checkpoint 機制 | ✅ | Parquet + Pickle fallback |
| async/await 非同步執行 | ✅ | 與文件一致 |
| common.py 共用步驟 | ✅ | 1254 行，涵蓋文件定義的所有共用步驟 |
| run_config.toml + StepByStepExecutor | ✅ | 超出文件描述的開發體驗提升 |

---

## 2. spe_bank_recon — 現況審查

### 2.1 架構整體評估

整體架構高度符合 `Project_Architecture_Reference.md`，三層框架（Core/Task/Utils）清晰，16 步驟 Pipeline 實作完整，Mixin 組合的 DuckDB Manager 架構精良。主要問題集中在**測試覆蓋度**與**文件/程式碼同步**。

**整體評分**：架構設計 ✅ 優秀，文件同步度 ⚠️ 部分過時，測試覆蓋 ❌ 嚴重不足

---

### 2.2 與參考文件的差異清單

#### [差異 1] 單元測試檔案缺失（高優先）

**CLAUDE.md 聲稱已建立 28 個單元測試**：
```
tests/utils/test_config_manager.py      # 7 個測試
tests/core/datasources/test_datasource_base.py  # 8 個測試
tests/utils/test_file_utils.py          # 13 個測試
```

**實際現況**：這三個測試檔案**完全不存在**。`tests/` 目錄中只有：
```
tests/
├── core/datasources/__init__.py    ← 空
├── utils/__init__.py               ← 空
├── verify_iteration1.py            ← 驗證腳本（非 unit tests）
├── verify_iteration2.py
├── verify_iteration3.py
├── test_iteration2_summary.md      ← 文件
└── test_iteration3_summary.md      ← 文件
```

`verify_iteration*.py` 是功能驗證腳本，不是標準化的 `unittest.TestCase` 或 `pytest` 測試，無法被測試框架自動收集執行。

**影響**：文件宣稱的 28 個測試和「Overall ≥ 80% 覆蓋率」目標完全未實現，CLAUDE.md 中的測試說明具有誤導性。

**建議**：
1. 依照 CLAUDE.md 描述的測試範本，建立三個測試檔案
2. 或將 CLAUDE.md 中的測試章節更新為正確現況
3. `run_all_tests.py` 現有指向這些不存在檔案，需同步修正

---

#### [差異 2] `new_main.py` 不存在（中優先）

**CLAUDE.md 說明**：
```bash
# Full workflow including accounting entries
python new_main.py
```

**實際現況**：根目錄只有 `main.py`，沒有 `new_main.py`。

**影響**：用戶依照文件操作會直接失敗。`mode='full_with_entry'` 的完整執行應透過：
```python
from src.tasks.bank_recon import BankReconTask
task = BankReconTask()
task.execute(mode='full_with_entry')
```

**建議**：從 CLAUDE.md 移除 `python new_main.py` 指令，或建立 `new_main.py` 作為 `full_with_entry` 模式的快速入口腳本。

---

#### [差異 3] `database/duckdb_manager.py` 為向後相容層（已正確處理）

**現況**：`src/utils/database/duckdb_manager.py` 是一個 299 行的向後相容 shim，內容開頭明確說明：

```
此模組已重構為獨立可移植模組 `src.utils.duckdb_manager`。
為保持向後相容性，此檔案從新模組重新導出所有公開接口。
此檔案將在未來版本移除。
```

**評估**：設計上是正確的，已有明確的棄用說明。但 Project_Architecture_Reference.md 的「可移植模組對照表」中未明確標示 `database/duckdb_manager` 是舊版、應使用 `duckdb_manager/` 新版。

**建議**：在參考文件中補充此說明，避免新開發者誤用舊版。

---

#### [差異 4] `generate_spe_bank_recon_wp_config.toml` 未完整記錄（低優先）

**現況**：`src/config/` 包含四個 TOML 配置檔案，但 CLAUDE.md 的「Key Configuration Sections」章節只詳細描述三個：
- `bank_recon_config.toml` ✅ 詳細說明
- `bank_recon_entry_monthly.toml` ✅ 說明
- `config.toml` ✅ 說明
- `generate_spe_bank_recon_wp_config.toml` ⚠️ 只在配置清單中列出，無說明

**建議**：補充 `generate_spe_bank_recon_wp_config.toml` 的用途說明（工作底稿格式化設定）。

---

#### [差異 5] `core/pipeline/steps/` 中的 common_steps 未在參考文件描述（低優先）

**實際存在但未記錄**：
- `src/core/pipeline/steps/common_steps.py`
- `src/core/pipeline/steps/step_utils.py`

這些是 Pipeline 框架層的通用步驟工具，Project_Architecture_Reference.md 的目錄結構中未提及。

**建議**：更新目錄結構說明，補充這兩個檔案的用途。

---

#### [差異 6] Orchestrator 行數遠超參考文件預期（觀察）

**實際**：`pipeline_orchestrator.py` = 1041 行
**參考文件描述**：未給出行數限制，但 1000 行以上的 Orchestrator 通常是需要關注的訊號

**原因**：`BankReconTask` 承擔了任務管理、Pipeline 組裝、6 種模式的分支邏輯、Checkpoint 整合、輸入驗證等多個職責，這是設計上的合理選擇（集中管理），但未來若需擴充至更多模式，建議考慮將 `build_pipeline()` 的各模式提取為獨立方法或 Strategy 物件。

---

### 2.3 正向確認

| 項目 | 狀態 | 說明 |
|------|------|------|
| 三層架構（Core/Task/Utils）| ✅ | 職責清晰，符合參考設計 |
| BaseBankProcessStep 模板方法 | ✅ | 消除 87.4% 重複，銀行步驟降至 15-20 行 |
| DuckDB Mixin 架構 | ✅ | CRUD/Table/DataCleaning/Transaction 分離 |
| Schema 遷移系統 | ✅ | schema_diff + strategies + migrator 完整 |
| MetadataBuilder Bronze/Silver | ✅ | 完整實作，含 Circuit Breaker |
| TTL+LRU 多層快取 | ✅ | MD5 key + TTL + LRU eviction |
| ConfigManager 執行緒安全 | ✅ | Double-Checked Locking 與 accrual_bot 一致 |
| 配置驅動銀行步驟 | ✅ | enabled_banks 動態添加步驟 |
| 6 種執行模式 | ✅ | full/full_with_entry/escrow/installment/daily_check/entry |
| Checkpoint 斷點續跑 | ✅ | Parquet + Pickle，結構符合文件 |
| Google Sheets 整合 | ✅ | 繼承 DataSource ABC，標準化 API |

---

## 3. 合併可行性分析

### 3.1 兩個專案的相似性矩陣

| 維度 | accrual_bot | spe_bank_recon | 相似度 |
|------|-------------|----------------|--------|
| Pipeline 框架 | Pipeline/PipelineStep/Context/Checkpoint | 完全相同結構 | ★★★★★ |
| ConfigManager | Thread-safe Singleton，TOML | 完全相同模式 | ★★★★★ |
| Logger | get_logger(), StructuredLogger | 完全相同框架 | ★★★★★ |
| DataSource 層 | Excel/CSV/Parquet/DuckDB | +Google Sheets | ★★★★☆ |
| 設計模式 | Template/Factory/Singleton/Strategy | 完全相同 | ★★★★★ |
| 執行模式 | 單一（按類型切換） | 6 種模式 | ★★☆☆☆ |
| 非同步模型 | **async/await** | **同步** | ★☆☆☆☆ |
| UI 層 | Streamlit（5 頁工作流）| 無 UI | ★☆☆☆☆ |
| 資料庫使用 | 輕度（DuckDB 作為資料源之一）| 重度（DuckDB 為核心中間層）| ★★☆☆☆ |
| 輸出目標 | Excel 工作底稿 | Excel + Google Sheets + DuckDB 累積表 | ★★☆☆☆ |
| 業務領域 | PO/PR 應計費用分類 | 銀行存款月結對帳 | ★☆☆☆☆ |

---

### 3.2 合併的潛在收益

#### 收益 A：消除框架層重複（最高價值）

兩個專案各自維護一份幾乎相同的 Pipeline 框架：

| 模組 | accrual_bot 行數 | spe_bank_recon 行數 | 重複度 |
|------|-----------------|---------------------|--------|
| `pipeline/base.py` | 466 | 394 | ~85% |
| `pipeline/context.py` | 343 | 218 | ~80% |
| `pipeline/pipeline.py` | 547 | 343 | ~75% |
| `utils/config/config_manager.py` | 745 | 381 | ~70% |
| `utils/logging/logger.py` | (未量) | (未量) | ~80% |
| `core/datasources/` | 8 檔案 | 7 檔案 | ~75% |

若將這些共享框架提取為獨立套件，估計可消除約 **2500-3000 行重複程式碼**。

#### 收益 B：accrual_bot 可使用 spe_bank_recon 的進階工具

| 工具 | 應用場景 |
|------|---------|
| `MetadataBuilder` Bronze/Silver | SPT/SPX 資料載入步驟的髒資料防護 |
| `DuckDB Manager`（完整 Mixin 版）| 若未來 accrual_bot 需要累積歷史資料 |
| TTL+LRU 多層快取 | DataSource 讀取效能提升 |

#### 收益 C：spe_bank_recon 可選用 Streamlit UI

若 spe_bank_recon 未來需要提供非技術人員操作介面，可沿用 accrual_bot 的 Streamlit 架構（雙層頁面、UnifiedPipelineService 模式）。

---

### 3.3 合併的風險與成本

#### 風險 1（高）：async vs sync 根本不相容

這是**最大的技術障礙**。

| 項目 | accrual_bot | spe_bank_recon |
|------|-------------|----------------|
| `PipelineStep.execute()` | `async def execute(...)` | `def execute(...)` |
| Pipeline 執行 | `asyncio.run()` | 同步呼叫 |
| DataSource.read() | `async def read()` | `def read()` |

**後果**：若強制合併為單一框架，必須選擇以下之一：
- **方案 A**：全面改為 async（spe_bank_recon 需大規模重寫，風險高）
- **方案 B**：全面改為 sync（accrual_bot 失去並發載入能力）
- **方案 C**：維持雙版本介面（框架複雜度倍增，不推薦）

**預估改寫成本**：若選方案 A（async 統一），spe_bank_recon 需修改所有 16 個步驟 + 8 個業務工具類，估計 **40-60 小時**工時。

#### 風險 2（中）：依賴套件衝突

| 套件 | accrual_bot | spe_bank_recon |
|------|-------------|----------------|
| Google Sheets | `google-api-python-client` | `gspread` 或類似 |
| Streamlit | ✅ 有 | ❌ 無 |
| DuckDB | 輕度使用 | 核心依賴 |
| 非同步相關 | `asyncio` | 無需要 |

合併為單一 `pyproject.toml` 會使所有用戶被迫安裝所有依賴，即使他們只用其中一個功能模組。

#### 風險 3（中）：開發節奏干擾

- 兩個專案目前**獨立 git 倉儲**，各自有清晰的歷史紀錄
- 合併後若一個專案的修改破壞另一個的測試，CI 流程將複雜化
- 目前 spe_bank_recon 在 `accrual_bot/` 的子目錄（`spe_bank_recon/`），已形成一種「monorepo 初始形態」，但 git 設定各自獨立

#### 風險 4（低）：業務語意混亂

兩個專案使用相同名稱但語意不同的概念：

| 術語 | accrual_bot | spe_bank_recon |
|------|-------------|----------------|
| `processing_date` | YYYYMM 格式（月份） | 日期範圍（start/end） |
| `entity_type` | SPT/SPX/MOB（業務實體） | 不使用此概念 |
| `processing_type` | PO/PR/PPE（文件類型）| full/escrow/entry（執行模式） |
| `auxiliary_data` | 參考表（DataFrame）| 銀行容器物件（non-DataFrame）|

合併後命名衝突需要統一規範，存在混淆風險。

---

### 3.4 三種合併策略比較

#### 策略一：完全合併（Monorepo + 共享程式碼）

**描述**：兩個專案合併為一個 Python 套件，業務任務各在子模組中。

```
financial_automation/
├── core/          ← 共享 Pipeline 框架（統一 async 或 sync）
├── utils/         ← 共享工具層
├── tasks/
│   ├── accrual/   ← 原 accrual_bot 任務
│   └── bank_recon/← 原 spe_bank_recon 任務
└── ui/            ← 共享 Streamlit UI（可選）
```

| 面向 | 評估 |
|------|------|
| 代碼重複消除 | ★★★★★ 最佳 |
| 實施成本 | ★☆☆☆☆ 最高（async 統一問題）|
| 風險 | ★☆☆☆☆ 最高（兩個專案同時受影響）|
| 維護彈性 | ★★★☆☆ 中（修改共享框架影響全部）|

**建議**：**不推薦**（至少短期內）

---

#### 策略二：共享框架套件（最佳平衡方案）

**描述**：提取共享核心為獨立的內部套件，兩個業務專案作為依賴者各自獨立。

```
# 新建內部套件
fp_pipeline_core/         ← "Financial Processing Pipeline Core"
├── pipeline/             ← Pipeline/PipelineStep/Context/Checkpoint
├── datasources/          ← DataSource 抽象層
└── utils/                ← ConfigManager/Logger/file_utils/DuckDBManager

# 兩個業務專案各自獨立
accrual_bot/              ← 依賴 fp_pipeline_core
  ├── 業務步驟（async 版）
  ├── Streamlit UI
  └── pyproject.toml: fp_pipeline_core>=1.0.0

spe_bank_recon/           ← 依賴 fp_pipeline_core
  ├── 業務步驟（sync 版）
  ├── MetadataBuilder（業務特有，不共享）
  └── pyproject.toml: fp_pipeline_core>=1.0.0
```

**注意**：此策略下 async vs sync 的問題透過框架提供**雙介面**或**兩個版本的 PipelineStep 基類**來解決，業務層各自選擇。

| 面向 | 評估 |
|------|------|
| 代碼重複消除 | ★★★★☆ 良好（消除框架層重複）|
| 實施成本 | ★★★☆☆ 中（需要新建套件，但不需要業務層改寫）|
| 風險 | ★★★★☆ 低（業務層獨立，框架層變更可控）|
| 維護彈性 | ★★★★☆ 高（業務層完全獨立）|

**建議**：**推薦作為中長期目標**

---

#### 策略三：維持現狀 + 文件同步（最小成本方案）

**描述**：不改變程式碼組織，僅確保文件準確、程式碼品質改善。

| 面向 | 評估 |
|------|------|
| 代碼重複消除 | ★★☆☆☆ 有限（手動搬移工具函數）|
| 實施成本 | ★★★★★ 最低 |
| 風險 | ★★★★★ 最低 |
| 維護彈性 | ★★★★☆ 高（維持各自獨立）|

**建議**：**推薦作為短期行動**

---

### 3.5 推薦決策路徑

```
現在（短期 - 1 個月）
  ↓
  策略三：維持現狀 + 修正文件和程式碼品質問題
  - 修正 spe_bank_recon CLAUDE.md 中不存在的 new_main.py 和測試檔案
  - 更新 accrual_bot CLAUDE.md 補充 PROCUREMENT / PPE_DESC / runner/ 模組
  - 修正 accrual_bot Orchestrator 存取 ConfigManager 私有屬性問題
  - 建立 spe_bank_recon 真正的 pytest 測試
  ↓
未來（中期 - 3~6 個月）
  ↓
  評估策略二的可行性
  - 確認兩個專案的 async/sync 需求是否有長期統一的可能
  - 評估 accrual_bot 是否需要 MetadataBuilder
  - 若 spe_bank_recon 需要 UI，評估 Streamlit 整合成本
  ↓
決策點：若有新的第三個財務自動化專案需要開發
  → 此時提取共享框架的收益最高（三個專案受益）
  → 適合推動策略二
```

---

## 4. 建議行動清單

### accrual_bot（按優先序）

| 優先 | 項目 | 類型 | 預估工時 |
|------|------|------|---------|
| P1 | 修正 Orchestrator 存取 `config_manager._config_toml` 私有屬性 | 程式碼品質 | 2h |
| P2 | 更新 CLAUDE.md：補充 PROCUREMENT、PPE_DESC 類型說明 | 文件 | 1h |
| P2 | 更新 CLAUDE.md：補充 `runner/` 模組和 `run_config.toml` 說明 | 文件 | 1h |
| P3 | 更新 Project_Design_Reference.md：四層配置體系改為四個配置檔案 | 文件 | 0.5h |
| P3 | 將根目錄零散腳本（`validate_completeness.py` 等）移至 `scripts/` | 重構 | 0.5h |
| P3 | 繼續推進步驟從 `core/pipeline/steps/` 遷移至 `tasks/*/steps/` | 重構 | 8-16h |

### spe_bank_recon（按優先序）

| 優先 | 項目 | 類型 | 預估工時 |
|------|------|------|---------|
| P1 | 建立真正的 pytest 測試（test_config_manager.py、test_datasource_base.py、test_file_utils.py）| 測試 | 4-6h |
| P1 | 修正 CLAUDE.md：移除 `python new_main.py` 指令或建立該檔案 | 文件 | 0.5h |
| P2 | 更新 CLAUDE.md 測試章節：移除不存在測試的錯誤描述 | 文件 | 0.5h |
| P2 | 更新 Project_Architecture_Reference.md：補充 `database/duckdb_manager.py` 為向後相容層的說明 | 文件 | 0.5h |
| P3 | 補充 `generate_spe_bank_recon_wp_config.toml` 的文件說明 | 文件 | 0.5h |

### 合併方向（中長期）

| 項目 | 說明 |
|------|------|
| 評估 async 統一可能性 | 確認 spe_bank_recon 是否有改為 async 的需求（效能、IO 密集度） |
| 評估 MetadataBuilder 移植 | accrual_bot 的資料載入步驟是否能受益於 Bronze/Silver 架構 |
| 監控第三個專案的出現 | 第三個財務自動化任務出現時，即為提取共享框架的最佳時機 |
| TTL+LRU 快取移植 | 將 spe_bank_recon 的進階快取機制整合至 accrual_bot DataSource |

---

## 附錄：核心差異一覽

| 面向 | accrual_bot | spe_bank_recon |
|------|-------------|----------------|
| 執行模型 | **非同步**（asyncio）| **同步** |
| Pipeline 建構 | PipelineBuilder 流式 API | Pipeline(config) + add_step() |
| 配置入口 | 4 個 TOML/INI 配置 | 3 個 TOML 配置 |
| 資料庫角色 | 資料源之一（可選）| 核心計算引擎（必要）|
| 步驟基類層數 | 2 層（PipelineStep → BaseLoadingStep/BaseERMEvaluationStep）| 2 層（PipelineStep → BaseBankProcessStep）|
| 進入點 | main_pipeline.py（讀 run_config.toml）+ Streamlit | main.py（硬編碼 or 程式呼叫）|
| 測試框架 | pytest + pytest-asyncio | unittest（verify scripts）|
| 業務規則複雜度 | 高（11+ 條件引擎、帳號預測）| 中（銀行計算邏輯複雜但條件數少）|
| 擴充方式 | 新增 entity 或 processing_type | 新增 bank 或 execution mode |
