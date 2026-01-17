# Accrual Bot Streamlit UI 架構文檔

> **版本**: 2.2.0
> **最後更新**: 2026-01-17
> **作者**: Architecture Review Team
> **狀態**: Production Ready

## 📋 變更日誌

### v2.2.0 (2026-01-17)
- ✅ 移除已棄用的 Template 系統（7 個檔案，~150 行代碼）
- ✅ 清理重複頁面檔案（5 個檔案，~400 行代碼）
- ✅ 添加日誌匯出功能到執行頁面
- ✅ 修復雙層 Pages 架構問題（Entry Point 改用 exec）
- ✅ 修復 ProcessingContext.auxiliary_data 屬性錯誤
- 📖 完善雙層 Pages 架構說明文檔
- 📊 更新檔案清單與行數統計（淨減少 ~558 行代碼）

### v2.1.0 (2026-01-17)
- 📖 添加擴充指南：新增 Pipeline 類型（Chapter 14）
- 📖 完善 UI 與後端串接文檔
- 📖 添加設計模式識別章節

### v2.0.0 (2026-01-16)
- 🎉 初始 UI 架構文檔完成
- 📖 16 章節，約 2,600 行完整文檔

---

## 目錄

1. [概述與設計目標](#1-概述與設計目標)
2. [架構總覽](#2-架構總覽)
3. [三層架構詳解](#3-三層架構詳解)
4. [狀態管理機制](#4-狀態管理機制)
5. [資料流向圖解](#5-資料流向圖解)
6. [UI 與後端 Pipeline 串接](#6-ui-與後端-pipeline-串接)
7. [配置驅動設計](#7-配置驅動設計)
8. [設計模式識別](#8-設計模式識別)
9. [五頁工作流程](#9-五頁工作流程)
10. [七大 UI 元件](#10-七大-ui-元件)
11. [服務層 API 參考](#11-服務層-api-參考)
12. [錯誤處理與重試機制](#12-錯誤處理與重試機制)
13. [Checkpoint 系統](#13-checkpoint-系統)
14. [擴充指南：新增 Pipeline 類型](#14-擴充指南新增-pipeline-類型)
15. [已知限制與改進建議](#15-已知限制與改進建議)
16. [附錄](#16-附錄)

---

## 1. 概述與設計目標

### 1.1 專案背景

Accrual Bot UI 是基於 Streamlit 構建的 Web 介面，為複雜的 PO/PR（Purchase Order/Purchase Request）月結處理系統提供用戶友善的操作界面。該系統處理三個業務實體（SPT、SPX、MOB）的財務對帳作業。

### 1.2 設計目標

| 目標 | 描述 | 實現方式 |
|------|------|----------|
| **易用性** | 引導式操作流程 | 5 頁工作流程 + 導航狀態控制 |
| **解耦** | UI 與 Pipeline 分離 | UnifiedPipelineService 服務層 |
| **可配置** | 配置驅動的 UI 內容 | ENTITY_CONFIG + paths.toml |
| **可監控** | 實時進度追蹤 | Progress callbacks + Logs |
| **可擴展** | 支援新實體類型 | Orchestrator 模式 |
| **穩定性** | 級聯狀態清除 | Session State 管理策略 |

### 1.3 技術棧

```
Frontend:     Streamlit 1.31+
Backend:      Python 3.10+, Pandas, Async/Await
Config:       TOML (paths.toml, stagging.toml), INI (config.ini)
Serialization: Parquet (Checkpoint), JSON (Metadata)
Export:       Excel (openpyxl, xlsxwriter), CSV
```

---

## 2. 架構總覽

### 2.1 系統架構圖

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Streamlit UI Layer                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
│  │ Page 1  │→│ Page 2  │→│ Page 3  │→│ Page 4  │→│ Page 5  │           │
│  │ 配置    │  │ 上傳    │  │ 執行    │  │ 結果    │  │Checkpoint│           │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘           │
│       │            │            │            │            │                 │
│  ┌────┴────────────┴────────────┴────────────┴────────────┴────┐           │
│  │                     Components Layer                         │           │
│  │  entity_selector | file_uploader | progress_tracker | ...   │           │
│  └─────────────────────────────┬───────────────────────────────┘           │
│                                │                                            │
├────────────────────────────────┼────────────────────────────────────────────┤
│                         Services Layer                                      │
│  ┌─────────────────────────────┴───────────────────────────────┐           │
│  │ UnifiedPipelineService | StreamlitPipelineRunner | FileHandler │        │
│  └─────────────────────────────┬───────────────────────────────┘           │
│                                │                                            │
├────────────────────────────────┼────────────────────────────────────────────┤
│                          Backend Layer                                      │
│  ┌─────────────────────────────┴───────────────────────────────┐           │
│  │          SPTPipelineOrchestrator | SPXPipelineOrchestrator   │           │
│  └─────────────────────────────┬───────────────────────────────┘           │
│                                │                                            │
│  ┌─────────────────────────────┴───────────────────────────────┐           │
│  │    Pipeline | PipelineStep | ProcessingContext | Checkpoint  │           │
│  └─────────────────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 目錄結構

```
accrual_bot/ui/
├── __init__.py                    # 模組進入點 (版本聲明)
├── app.py                         # Session State 初始化與導航狀態
├── config.py                      # UI 配置常數
│
├── models/
│   ├── __init__.py
│   └── state_models.py            # Dataclass 狀態模型
│
├── components/                    # 可重用 UI 元件
│   ├── __init__.py
│   ├── entity_selector.py         # Entity/Type/日期選擇
│   ├── step_preview.py            # 步驟預覽 (唯讀)
│   ├── file_uploader.py           # 動態檔案上傳
│   ├── progress_tracker.py        # 進度追蹤
│   └── data_preview.py            # 數據預覽 (含日誌匯出)
│
├── services/                      # 服務層
│   ├── __init__.py
│   ├── unified_pipeline_service.py # Pipeline 統一服務 (核心)
│   ├── pipeline_runner.py         # Pipeline 執行器
│   └── file_handler.py            # 檔案處理
│
├── pages/                         # Streamlit 頁面
│   ├── __init__.py
│   ├── 1_configuration.py
│   ├── 2_file_upload.py
│   ├── 3_execution.py
│   ├── 4_results.py
│   └── 5_checkpoint.py
│
└── utils/                         # 工具函數
    ├── __init__.py
    ├── async_bridge.py            # Sync/Async 橋接
    └── ui_helpers.py              # 格式化輔助
```

### 2.3 雙層 Pages 架構 🆕

為了解決 Streamlit 的 emoji 檔名限制，專案採用雙層 Pages 架構：

```
專案根目錄/
│
├── pages/                          # ← Streamlit 識別層（Emoji 檔名）
│   ├── 1_⚙️_配置.py                 # Entry Point (17 行)
│   ├── 2_📁_檔案上傳.py             # Entry Point (17 行)
│   ├── 3_▶️_執行.py                 # Entry Point (17 行)
│   ├── 4_📊_結果.py                 # Entry Point (17 行)
│   └── 5_💾_Checkpoint.py          # Entry Point (17 行)
│         ↓ exec()
│         ↓
└── accrual_bot/ui/pages/           # ← 實際實作層（數字檔名）
    ├── 1_configuration.py          # 真正的邏輯 (65 行)
    ├── 2_file_upload.py            # 真正的邏輯 (80 行)
    ├── 3_execution.py              # 真正的邏輯 (205 行)
    ├── 4_results.py                # 真正的邏輯 (149 行)
    └── 5_checkpoint.py             # 真正的邏輯 (142 行)
```

#### 為什麼需要兩組 Pages？

| 原因 | 說明 |
|------|------|
| **Streamlit 限制** | Multi-page 應用必須在 `pages/` 目錄下使用 emoji 或特殊字元檔名，Sidebar 才會自動顯示導航 |
| **跨平台相容性** | Emoji 檔名在不同 OS、文件系統、Git 上有編碼問題 |
| **版本控制** | Emoji 在 diff、merge 時難以閱讀 |
| **最佳實踐** | 業務邏輯應在標準命名的檔案中，方便測試和重用 |
| **解耦設計** | Entry Point 與業務邏輯分離，符合 SRP 原則 |

#### Entry Point 檔案範例

```python
# pages/1_⚙️_配置.py (Streamlit Entry Point)
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 直接執行實際的頁面檔案
actual_page = project_root / "accrual_bot" / "ui" / "pages" / "1_configuration.py"
exec(open(actual_page, encoding='utf-8').read())
```

#### 頁面導航路徑

所有 `st.switch_page()` **必須指向 emoji 版本**（Streamlit 識別的頁面）：

```python
# ✅ 正確
st.switch_page("pages/1_⚙️_配置.py")
st.switch_page("pages/3_▶️_執行.py")

# ❌ 錯誤
st.switch_page("pages/1_configuration.py")  # Streamlit 找不到此頁面
st.switch_page("accrual_bot/ui/pages/1_configuration.py")  # 不在 pages/ 目錄
```

---

## 3. 三層架構詳解

### 3.1 頁面層 (Pages Layer)

**職責**: 直接操作 Streamlit UI 元件與 Session State

| 頁面 | 檔案 | 職責 | 輸入 | 輸出 |
|------|------|------|------|------|
| 配置 | `1_configuration.py` | 收集 Entity/Type/日期 | 無 | `pipeline_config` |
| 上傳 | `2_file_upload.py` | 檔案上傳與驗證 | `pipeline_config` | `file_upload` |
| 執行 | `3_execution.py` | Pipeline 執行監控 | `config + upload` | `execution + result` |
| 結果 | `4_results.py` | 預覽與匯出 | `result` | CSV/Excel 下載 |
| Checkpoint | `5_checkpoint.py` | 管理已儲存狀態 | 無 | Checkpoint 操作 |

**導航控制**:

```python
# app.py - get_navigation_status()
def get_navigation_status() -> Dict[str, bool]:
    """判斷各頁面是否可訪問"""
    config = st.session_state.get('pipeline_config')
    upload = st.session_state.get('file_upload')
    execution = st.session_state.get('execution')

    return {
        'configuration': True,  # 始終可訪問
        'file_upload': bool(config and config.entity and config.processing_type),
        'execution': bool(upload and upload.required_files_complete),
        'results': execution and execution.status == ExecutionStatus.COMPLETED,
        'checkpoint': True,  # 始終可訪問
    }
```

### 3.2 元件層 (Components Layer)

**職責**: 提供可重用的 UI 元件，無業務邏輯

| 元件 | 檔案 | 功能 |
|------|------|------|
| Entity Selector | `entity_selector.py` | 按鈕式 Entity/Type 選擇 + 狀態清除 |
| Template Picker | `template_picker.py` | 範本選擇 (已棄用) |
| Step Preview | `step_preview.py` | 唯讀步驟清單展示 |
| File Uploader | `file_uploader.py` | 動態必填/選填檔案上傳 |
| Progress Tracker | `progress_tracker.py` | 進度條 + 步驟狀態表 |
| Data Preview | `data_preview.py` | DataFrame 預覽 + 統計 + 下載 |

### 3.3 服務層 (Services Layer)

**職責**: 封裝業務邏輯，解耦 UI 與 Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                    UnifiedPipelineService                     │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐ │
│  │ get_entities() │  │ get_steps()    │  │ build_pipeline()│ │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘ │
│          │                   │                   │           │
│          └───────────────────┼───────────────────┘           │
│                              ↓                               │
│                    _get_orchestrator()                       │
│                              ↓                               │
│         ┌────────────────────┴────────────────────┐          │
│         │ SPTPipelineOrchestrator                 │          │
│         │ SPXPipelineOrchestrator                 │          │
│         └─────────────────────────────────────────┘          │
└──────────────────────────────────────────────────────────────┘
```

---

## 4. 狀態管理機制

### 4.1 Session State 結構

```python
st.session_state = {
    # 配置狀態
    'pipeline_config': PipelineConfig(
        entity='SPX',
        processing_type='PO',
        processing_date=202512,
        template_name='',
        enabled_steps=['SPXDataLoading', 'ColumnAddition', ...]
    ),

    # 檔案上傳狀態
    'file_upload': FileUploadState(
        uploaded_files={'raw_po': UploadedFile, ...},
        file_paths={'raw_po': '/tmp/xxx/raw_po_file.csv', ...},
        validation_errors=[],
        required_files_complete=True
    ),

    # 執行狀態
    'execution': ExecutionState(
        status=ExecutionStatus.COMPLETED,
        current_step='',
        completed_steps=['SPXDataLoading', 'ColumnAddition', ...],
        failed_steps=[],
        step_results={...},
        logs=['[INFO] Loading data...', ...],
        error_message='',
        start_time=1705500000.0,
        end_time=1705500045.0
    ),

    # 結果狀態
    'result': ResultState(
        success=True,
        output_data=pd.DataFrame(...),  # 43225 rows x 85 columns
        auxiliary_data={
            'locker_non_discount': pd.DataFrame(...),
            'kiosk_data': pd.DataFrame(...),
        },
        statistics={'total_rows': 43225, 'accrual_count': 316},
        execution_time=45.23,
        checkpoint_path='./checkpoints/SPX_PO_202512_...'
    ),

    # 其他
    'temp_dir': '/tmp/accrual_bot_ui_xxx',
    'file_handler': FileHandler(...),
    'current_page': 'execution',
    'confirm_delete_all': False,
}
```

### 4.2 狀態模型定義

```python
# models/state_models.py

class ExecutionStatus(Enum):
    IDLE = "idle"           # 初始狀態
    RUNNING = "running"     # 執行中
    COMPLETED = "completed" # 完成
    FAILED = "failed"       # 失敗
    PAUSED = "paused"       # 暫停 (預留)

@dataclass
class PipelineConfig:
    entity: str = ""
    processing_type: str = ""
    processing_date: int = 0
    template_name: str = ""
    enabled_steps: List[str] = field(default_factory=list)

@dataclass
class FileUploadState:
    uploaded_files: Dict[str, Any] = field(default_factory=dict)
    file_paths: Dict[str, str] = field(default_factory=dict)
    validation_errors: List[str] = field(default_factory=list)
    required_files_complete: bool = False

@dataclass
class ExecutionState:
    status: ExecutionStatus = ExecutionStatus.IDLE
    current_step: str = ""
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    step_results: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)
    error_message: str = ""
    start_time: float = 0.0
    end_time: float = 0.0

@dataclass
class ResultState:
    success: bool = False
    output_data: Optional[pd.DataFrame] = None
    auxiliary_data: Dict[str, pd.DataFrame] = field(default_factory=dict)
    statistics: Dict[str, Any] = field(default_factory=dict)
    execution_time: float = 0.0
    checkpoint_path: str = ""
```

### 4.3 級聯狀態清除策略

為避免不一致狀態，當上游配置改變時，自動清除下游狀態：

```
Entity 改變
    ├── 清除: processing_type, enabled_steps, template_name
    ├── 清除: file_upload (全部)
    └── 清除: execution (全部)

Processing Type 改變
    ├── 清除: enabled_steps, template_name
    ├── 清除: file_upload (全部)
    └── 清除: execution (全部)

檔案上傳改變
    └── 重新驗證: required_files_complete

重新開始
    └── 清除: 所有狀態 (reset_session_state)
```

**實現代碼** (`entity_selector.py`):

```python
def render_entity_selector():
    # ... 渲染按鈕 ...

    if st.button(entity, ...):
        # Entity 改變 → 級聯清除
        st.session_state.pipeline_config.entity = entity
        st.session_state.pipeline_config.processing_type = ""
        st.session_state.pipeline_config.enabled_steps = []
        st.session_state.pipeline_config.template_name = ""

        # 清除檔案上傳狀態
        st.session_state.file_upload.file_paths = {}
        st.session_state.file_upload.uploaded_files = {}
        st.session_state.file_upload.validation_errors = []
        st.session_state.file_upload.required_files_complete = False

        # 清除執行狀態
        st.session_state.execution.status = ExecutionStatus.IDLE
        st.session_state.execution.current_step = ""
        st.session_state.execution.completed_steps = []
        st.session_state.execution.failed_steps = []
        st.session_state.execution.logs = []
        st.session_state.execution.error_message = ""

        st.rerun()
```

---

## 5. 資料流向圖解

### 5.1 完整資料流

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           Page 1: Configuration                             │
├────────────────────────────────────────────────────────────────────────────┤
│  render_entity_selector()     → entity                                      │
│  render_processing_type_selector() → proc_type                              │
│  render_date_selector()       → processing_date                             │
│  render_step_preview()        → enabled_steps (from orchestrator)           │
│                                                                             │
│  Output: st.session_state.pipeline_config                                   │
└─────────────────────────────────────┬──────────────────────────────────────┘
                                      │ (user clicks "Next")
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Page 2: File Upload                               │
├────────────────────────────────────────────────────────────────────────────┤
│  Input: pipeline_config                                                     │
│                                                                             │
│  get REQUIRED_FILES[(entity, proc_type)]  → ['raw_po']                      │
│  get OPTIONAL_FILES[(entity, proc_type)]  → ['previous', 'procurement', ...]│
│                                                                             │
│  for each file:                                                             │
│    st.file_uploader() → UploadedFile                                        │
│    file_handler.save_uploaded_file() → temp_path                            │
│    file_handler.validate_file() → errors[]                                  │
│    store in file_upload.file_paths                                          │
│                                                                             │
│  Output: st.session_state.file_upload                                       │
└─────────────────────────────────────┬──────────────────────────────────────┘
                                      │ (user clicks "Execute")
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Page 3: Execution                                 │
├────────────────────────────────────────────────────────────────────────────┤
│  Input: pipeline_config + file_upload                                       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ StreamlitPipelineRunner.execute()                                    │   │
│  │   │                                                                  │   │
│  │   ├── UnifiedPipelineService.build_pipeline()                        │   │
│  │   │     ├── _enrich_file_paths()  ← paths.toml params                │   │
│  │   │     ├── _get_orchestrator()   ← SPX/SPT Orchestrator             │   │
│  │   │     └── orchestrator.build_po_pipeline()                         │   │
│  │   │                                                                  │   │
│  │   ├── ProcessingContext(data=DataFrame(), ...)                       │   │
│  │   │     └── context.set_variable('file_paths', enriched_paths)       │   │
│  │   │                                                                  │   │
│  │   └── AsyncBridge.run_async(pipeline.execute(context))               │   │
│  │         │                                                            │   │
│  │         ├── Step 1: SPXDataLoading                                   │   │
│  │         │     ├── load raw_po → context.update_data()                │   │
│  │         │     └── load auxiliaries → context.add_auxiliary_data()    │   │
│  │         ├── Step 2: ColumnAddition                                   │   │
│  │         ├── Step 3: ...                                              │   │
│  │         └── Step N: SPXExport                                        │   │
│  │                                                                      │   │
│  │   Return: {'success': bool, 'context': ctx, 'error': str}            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Update: execution.status, execution.logs, execution.completed_steps        │
│  Copy: context.data → result.output_data                                    │
│  Copy: context.auxiliary_data → result.auxiliary_data                       │
│                                                                             │
│  Output: st.session_state.execution + st.session_state.result               │
└─────────────────────────────────────┬──────────────────────────────────────┘
                                      │ (auto redirect on success)
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Page 4: Results                                   │
├────────────────────────────────────────────────────────────────────────────┤
│  Input: result                                                              │
│                                                                             │
│  render_data_preview(result.output_data)                                    │
│    ├── Statistics: rows, columns, memory                                    │
│    ├── Column selector                                                      │
│    ├── Row slider (10-200)                                                  │
│    └── Download CSV button                                                  │
│                                                                             │
│  render_auxiliary_data_tabs(result.auxiliary_data)                          │
│    └── Tab per auxiliary DataFrame                                          │
│                                                                             │
│  Export Excel button → pd.ExcelWriter → {entity}_{type}_{date}_output.xlsx  │
│                                                                             │
│  Output: Downloaded files                                                   │
└────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 檔案路徑與參數整合

```
UI 提交的 file_paths (簡單字符串):
{
    'raw_po': '/tmp/accrual_bot_ui_xxx/raw_po_file.csv',
    'previous': '/tmp/accrual_bot_ui_xxx/previous_file.xlsx',
}
            │
            ▼
UnifiedPipelineService._enrich_file_paths()
            │
            ├── 讀取 paths.toml [spx.po.params]:
            │   {
            │       'raw_po': { encoding='utf-8', sep=',', dtype='str' },
            │       'previous': { sheet_name=0, header=0, dtype='str' },
            │   }
            │
            ▼
整合後的 file_paths (包含參數):
{
    'raw_po': {
        'path': '/tmp/accrual_bot_ui_xxx/raw_po_file.csv',
        'params': { 'encoding': 'utf-8', 'sep': ',', 'dtype': 'str' }
    },
    'previous': {
        'path': '/tmp/accrual_bot_ui_xxx/previous_file.xlsx',
        'params': { 'sheet_name': 0, 'header': 0, 'dtype': 'str' }
    },
}
            │
            ▼
傳遞給 Pipeline 步驟
            │
            ▼
DataSource.read(path, **params)
```

---

## 6. UI 與後端 Pipeline 串接

### 6.1 服務層橋接架構

```python
# UI 調用鏈
UI Page → UnifiedPipelineService → Orchestrator → Pipeline → Steps

# 具體流程
3_execution.py
    │
    ├── service = UnifiedPipelineService()
    ├── runner = StreamlitPipelineRunner(service)
    │
    └── runner.execute(entity, proc_type, file_paths, date)
            │
            ├── service.build_pipeline(...)
            │       │
            │       ├── _enrich_file_paths()  # 參數整合
            │       ├── _get_orchestrator(entity)
            │       │       └── SPXPipelineOrchestrator() or SPTPipelineOrchestrator()
            │       │
            │       └── orchestrator.build_po_pipeline(file_paths)
            │               │
            │               ├── 讀取 stagging.toml [pipeline.spx].enabled_po_steps
            │               ├── 創建 PipelineConfig
            │               ├── 實例化每個 PipelineStep
            │               └── 返回 Pipeline 物件
            │
            ├── ProcessingContext(data=pd.DataFrame(), ...)
            │
            └── AsyncBridge.run_async(pipeline.execute(context))
                    │
                    └── 返回執行結果
```

### 6.2 Orchestrator API

```python
class SPXPipelineOrchestrator:
    """SPX Pipeline 協調器"""

    def __init__(self):
        """初始化，從 config 讀取 [pipeline.spx] 配置"""

    def build_po_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX PO 處理 Pipeline

        預設步驟 (從 stagging.toml 讀取):
        1. SPXDataLoading
        2. ProductFilter
        3. ColumnAddition
        4. APInvoiceIntegration
        5. PreviousWorkpaperIntegration
        6. ProcurementIntegration
        7. DateLogic
        8. ClosingListIntegration
        9. StatusStage1
        10. SPXERMLogic
        11. DepositStatusUpdate
        12. ValidationDataProcessing
        13. DataReformatting
        14. SPXExport
        """

    def build_pr_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """構建 SPX PR 處理 Pipeline"""

    def build_ppe_pipeline(
        self,
        file_paths: Dict[str, Any],
        processing_date: int,  # YYYYMM (必需)
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """構建 SPX PPE (固定資產) 處理 Pipeline"""

    def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
        """獲取指定處理類型的啟用步驟清單"""
```

### 6.3 ProcessingContext 資料傳遞

```python
class ProcessingContext:
    """Pipeline 步驟間的數據傳遞容器"""

    # 主數據
    data: pd.DataFrame                    # 主處理 DataFrame

    # 輔助數據
    _auxiliary_data: Dict[str, pd.DataFrame]  # 參照數據

    # 共享變量
    _variables: Dict[str, Any]            # 跨步驟共享變量

    # 元數據
    metadata: ContextMetadata             # entity_type, processing_date, etc.

    # 關鍵方法
    def update_data(self, df: pd.DataFrame) -> None:
        """更新主數據"""

    def add_auxiliary_data(self, name: str, df: pd.DataFrame) -> None:
        """添加輔助數據"""

    def get_auxiliary_data(self, name: str) -> Optional[pd.DataFrame]:
        """獲取輔助數據"""

    def set_variable(self, key: str, value: Any) -> None:
        """設置共享變量"""

    def get_variable(self, key: str, default: Any = None) -> Any:
        """獲取共享變量"""
```

### 6.4 Async/Sync 橋接

由於 Streamlit 是同步框架，而 Pipeline 是異步的，需要橋接層：

```python
# utils/async_bridge.py

class AsyncBridge:
    """Sync/Async 橋接層"""

    @staticmethod
    def run_async(coro: Coroutine) -> Any:
        """
        在同步環境中執行異步協程

        實現方式：
        1. 創建新的 event loop
        2. 在新線程中運行 loop
        3. 等待協程完成
        4. 返回結果

        這避免了 "Cannot run the event loop while another loop is running" 錯誤
        """
        loop = asyncio.new_event_loop()

        def run_in_thread():
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_in_thread)
            return future.result()
```

---

## 7. 配置驅動設計

### 7.1 配置文件層級

```
┌─────────────────────────────────────────────────────────────────┐
│                      配置文件層級結構                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐     ┌─────────────────┐                    │
│  │  config/        │     │  ui/            │                    │
│  │  paths.toml     │     │  config.py      │                    │
│  │  stagging.toml  │     │                 │                    │
│  │  config.ini     │     │                 │                    │
│  └────────┬────────┘     └────────┬────────┘                    │
│           │                       │                              │
│           ▼                       ▼                              │
│  ┌────────────────────────────────────────────────────┐         │
│  │              ConfigManager (Singleton)              │         │
│  │  ┌──────────────┐  ┌──────────────┐               │         │
│  │  │ _config      │  │ _config_toml │               │         │
│  │  │ (INI)        │  │ (TOML)       │               │         │
│  │  └──────────────┘  └──────────────┘               │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 paths.toml - 檔案路徑與參數

```toml
# config/paths.toml

[base]
resources = "C:/SEA/Accrual/prpo_bot/resources/頂一下"
output = "./output"

# 變數替換支援:
# {YYYYMM}      - 處理日期 (202512)
# {PREV_YYYYMM} - 前一個月 (202511)
# {YYMM}        - 簡短格式 (2512)
# {resources}   - 資源根目錄

[spx.po]
raw_po = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv"
previous = "{resources}/{YYYYMM}/前期底稿/SPX/{PREV_YYYYMM}_PO_FN.xlsx"
procurement_po = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_PO_PQ.xlsx"
ap_invoice = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_AP_Invoice_Match_*.xlsx"
ops_validation = "{resources}/{YYYYMM}/Original Data/SPX智取櫃及繳費機驗收明細(For FN_{YYMM}).xlsx"

[spx.po.params]
# 檔案讀取參數
raw_po = { encoding = "utf-8", sep = ",", dtype = "str", keep_default_na = false, na_values = [""] }
previous = { sheet_name = 0, header = 0, dtype = "str" }
procurement_po = { dtype = "str" }
ap_invoice = {}
ops_validation = { sheet_name = "智取櫃驗收明細", header = 3, usecols = "A:AH", kiosk_sheet_name = "繳費機驗收明細", kiosk_usecols = "A:G" }
```

### 7.3 stagging.toml - Pipeline 步驟配置

```toml
# config/stagging.toml

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
]

enabled_pr_steps = [
    "SPTPRDataLoading",
    "ProductFilter",
    "ColumnAddition",
    # ... 其他步驟
]

[pipeline.spx]
enabled_po_steps = [
    "SPXDataLoading",
    "ProductFilter",
    "ColumnAddition",
    # ... 14 個步驟
]

enabled_pr_steps = [...]
enabled_ppe_steps = [...]
```

### 7.4 UI 配置 - config.py

```python
# ui/config.py

# Entity 配置
ENTITY_CONFIG: Dict[str, Dict] = {
    'SPT': {
        'display_name': 'SPT',
        'types': ['PO', 'PR'],
        'description': 'SPT Platform 採購/請購單處理',
        'icon': '🛒',
    },
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE'],  # SPX 特有 PPE 類型
        'description': 'SPX Platform 採購/請購單/固定資產處理',
        'icon': '📦',
    },
}

# 處理類型配置
PROCESSING_TYPE_CONFIG: Dict[str, Dict] = {
    'PO': {'display_name': 'PO (採購單)', 'icon': '📋'},
    'PR': {'display_name': 'PR (請購單)', 'icon': '📝'},
    'PPE': {'display_name': 'PPE (固定資產)', 'icon': '🏢'},
}

# 必填檔案配置
REQUIRED_FILES: Dict[Tuple[str, str], List[str]] = {
    ('SPT', 'PO'): ['raw_po'],
    ('SPT', 'PR'): ['raw_pr'],
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PR'): ['raw_pr'],
    ('SPX', 'PPE'): ['contract_filing_list'],
}

# 選填檔案配置
OPTIONAL_FILES: Dict[Tuple[str, str], List[str]] = {
    ('SPT', 'PO'): [
        'previous', 'procurement_po', 'ap_invoice',
        'previous_pr', 'procurement_pr',
        'media_finished', 'media_left', 'media_summary',
    ],
    ('SPX', 'PO'): [
        'previous', 'procurement_po', 'ap_invoice',
        'previous_pr', 'procurement_pr',
        'closing_list', 'ops_validation',
    ],
    # ... 其他配置
}

# 檔案標籤 (顯示名稱)
FILE_LABELS: Dict[str, str] = {
    'raw_po': 'PO 原始資料',
    'raw_pr': 'PR 原始資料',
    'previous': '前期底稿',
    'procurement_po': '採購系統 PO',
    'ap_invoice': 'AP Invoice',
    'ops_validation': 'OPS 驗收資料',
    # ... 其他標籤
}

# 支援的檔案格式
SUPPORTED_FILE_FORMATS: Dict[str, List[str]] = {
    'raw_po': ['.csv'],
    'raw_pr': ['.csv'],
    'previous': ['.xlsx', '.xls'],
    'procurement_po': ['.xlsx', '.xls'],
    # ... 其他格式
}
```

---

## 8. 設計模式識別

### 8.1 Facade 模式 - UnifiedPipelineService

```
┌─────────────────────────────────────────────────────┐
│              UnifiedPipelineService                  │
│  (Facade - 統一入口)                                 │
├─────────────────────────────────────────────────────┤
│  get_available_entities()                           │
│  get_entity_types()                                 │
│  get_enabled_steps()                                │
│  build_pipeline()                                   │
└──────────────────────┬──────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│SPTOrchestrator│ │SPXOrchestrator│ │ConfigManager │
└──────────────┘ └──────────────┘ └──────────────┘
```

**優點**:
- UI 層只需與一個服務交互
- 隱藏複雜的 Orchestrator 選擇邏輯
- 易於測試和模擬

### 8.2 Strategy 模式 - Orchestrator 選擇

```python
# 根據 entity 選擇不同的 Orchestrator 策略
def _get_orchestrator(self, entity: str):
    orchestrators = {
        'SPT': SPTPipelineOrchestrator,
        'SPX': SPXPipelineOrchestrator,
    }

    orchestrator_class = orchestrators.get(entity)
    if not orchestrator_class:
        raise ValueError(f"不支援的 entity: {entity}")

    return orchestrator_class()
```

### 8.3 Template Method 模式 - 基礎步驟類

```
┌────────────────────────────────────────┐
│          BaseLoadingStep               │
│  (Template Method - ~570 行共享邏輯)    │
├────────────────────────────────────────┤
│  # 具體方法 (不可覆寫)                  │
│  _normalize_file_paths()               │
│  _load_all_files_concurrent()          │
│  _validate_file_configs()              │
│                                        │
│  # 抽象方法 (必須覆寫)                  │
│  get_required_file_type() → str        │
│  _load_primary_file() → DataFrame      │
│  _load_reference_data() → int          │
└────────────────────────────────────────┘
        △
        │ 繼承
        │
┌───────┴───────┐
│ SPXDataLoading │
│ SPTDataLoading │
│ ...            │
└────────────────┘
```

### 8.4 Observer 模式 - 進度回調

```python
# 設置回調
runner.set_progress_callback(progress_callback)
runner.set_log_callback(log_callback)

# 執行時調用回調
for step_result in result['results']:
    self.progress_callback(step_name, idx, total, status)
    self.log_callback(f"[{idx}/{total}] 執行步驟: {step_name}")
```

### 8.5 Singleton 模式 - ConfigManager

```python
class ConfigManager:
    _instance = None
    _initialized = False
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:  # 線程安全
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not ConfigManager._initialized:
            with ConfigManager._lock:
                if not ConfigManager._initialized:
                    self._load_config()
                    ConfigManager._initialized = True
```

---

## 9. 五頁工作流程

### 9.1 Page 1: 配置頁面

**目的**: 收集基本配置資訊

**元件**:
- Entity Selector (按鈕式)
- Processing Type Selector (按鈕式)
- Date Selector (年月輸入)
- Step Preview (唯讀清單)

**流程**:
```
1. 顯示 Entity 按鈕 (SPT / SPX)
2. 用戶點選 Entity
   └─ 觸發狀態清除 + rerun
3. 顯示 Processing Type 按鈕 (PO / PR / PPE)
4. 用戶點選 Type
   └─ 觸發狀態清除 + rerun
5. 顯示日期選擇器
6. 用戶輸入年月 (YYYYMM)
7. 顯示步驟預覽
   └─ 從 orchestrator 讀取啟用步驟
8. 用戶點擊「下一步」
   └─ 跳轉到檔案上傳頁
```

### 9.2 Page 2: 檔案上傳頁面

**目的**: 上傳並驗證所需檔案

**元件**:
- File Uploader (動態生成)
- Validation Summary

**流程**:
```
1. 檢查導航狀態 (必須完成配置)
2. 初始化 FileHandler
3. 根據 REQUIRED_FILES 顯示必填檔案上傳區
4. 根據 OPTIONAL_FILES 顯示選填檔案上傳區
5. 用戶上傳檔案
   ├─ save_uploaded_file() → 儲存到暫存
   ├─ validate_file() → 驗證格式
   ├─ 成功 → 存儲路徑
   └─ 失敗 → 顯示錯誤
6. 更新驗證摘要
   └─ required_files_complete = all(required files uploaded)
7. 用戶點擊「開始執行」
   └─ 跳轉到執行頁
```

### 9.3 Page 3: 執行頁面

**目的**: 執行 Pipeline 並監控進度

**元件**:
- Progress Tracker (進度條 + 指標)
- Step Status Table
- Log Viewer
- Control Buttons (開始/停止)

**流程**:
```
1. 檢查導航狀態 (必須完成上傳)
2. 用戶點擊「開始執行」
3. 設置 status = RUNNING
4. 創建 runner 並設置回調
5. 調用 AsyncBridge.run_async(runner.execute(...))
   ├─ build_pipeline()
   ├─ create ProcessingContext
   └─ pipeline.execute(context)
       └─ 每個步驟執行後觸發回調
6. 實時更新:
   ├─ 進度條
   ├─ 步驟狀態表
   └─ 日誌區
7. 執行完成:
   ├─ 成功 → status = COMPLETED, 跳轉結果頁
   └─ 失敗 → status = FAILED, 顯示錯誤
```

### 9.4 Page 4: 結果頁面

**目的**: 預覽與匯出結果

**元件**:
- Execution Summary
- Data Preview (主數據)
- Auxiliary Data Tabs
- Export Buttons (CSV/Excel)

**流程**:
```
1. 檢查執行狀態 (必須 COMPLETED)
2. 顯示執行摘要:
   ├─ Entity / Type
   ├─ 執行時間
   └─ 輸出行數
3. 顯示主數據預覽:
   ├─ 統計資訊 (行數/欄數/記憶體)
   ├─ 欄位選擇器
   ├─ 行數 Slider
   └─ 數據表格
4. 顯示輔助數據 Tabs
5. 提供下載按鈕:
   ├─ CSV 下載
   └─ Excel 下載
6. 操作按鈕:
   ├─ 重新執行 → 返回配置頁
   └─ 管理 Checkpoint → 跳轉 Checkpoint 頁
```

### 9.5 Page 5: Checkpoint 管理頁面

**目的**: 檢視與管理已儲存的執行狀態

**元件**:
- Checkpoint List
- Sort Options
- Action Buttons

**流程**:
```
1. 掃描 ./checkpoints/ 目錄
2. 收集 .pkl 和 .json 檔案
3. 顯示排序選項 (最新/檔名/大小)
4. 顯示 Checkpoint 清單
   ├─ 檔名
   ├─ 大小
   ├─ 修改時間
   └─ 操作按鈕
5. 個別操作:
   ├─ 刪除 (已實作)
   └─ 載入 (TODO - 未實作)
6. 批次操作:
   ├─ 清空所有 (需確認)
   └─ 統計資訊
```

---

## 10. 七大 UI 元件

### 10.1 Entity Selector

```python
# components/entity_selector.py

def render_entity_selector() -> str:
    """
    渲染 Entity 選擇器

    Returns:
        選擇的 entity 名稱

    Side Effects:
        - 更新 st.session_state.pipeline_config.entity
        - Entity 改變時清除下游狀態
    """

def render_processing_type_selector(entity: str) -> str:
    """
    渲染 Processing Type 選擇器

    Args:
        entity: 已選擇的 entity

    Returns:
        選擇的 processing_type

    Side Effects:
        - 更新 st.session_state.pipeline_config.processing_type
        - Type 改變時清除下游狀態
    """

def render_date_selector() -> int:
    """
    渲染日期選擇器

    Returns:
        YYYYMM 格式的日期整數
    """
```

### 10.2 File Uploader

```python
# components/file_uploader.py

def render_file_uploader(
    entity: str,
    proc_type: str,
    file_handler: FileHandler
) -> Dict[str, str]:
    """
    渲染動態檔案上傳器

    Args:
        entity: Entity 名稱
        proc_type: Processing Type
        file_handler: FileHandler 實例

    Returns:
        檔案路徑字典 {'file_key': '/path/to/file'}

    Behavior:
        - 根據 REQUIRED_FILES 顯示必填區
        - 根據 OPTIONAL_FILES 顯示選填區
        - 自動驗證檔案格式
        - 更新 st.session_state.file_upload
    """
```

### 10.3 Step Preview

```python
# components/step_preview.py

def render_step_preview(entity: str, proc_type: str) -> List[str]:
    """
    渲染步驟預覽 (唯讀)

    Args:
        entity: Entity 名稱
        proc_type: Processing Type

    Returns:
        啟用的步驟清單

    Note:
        步驟來自 orchestrator.get_enabled_steps()
        無法在 UI 中修改
    """
```

### 10.4 Progress Tracker

```python
# components/progress_tracker.py

def render_progress_tracker(
    current_step: str,
    completed_steps: List[str],
    failed_steps: List[str],
    total_steps: int,
    start_time: float
) -> None:
    """
    渲染進度追蹤器

    Features:
        - 進度條 (百分比)
        - 統計指標 (已完成/失敗/耗時)
        - 預估剩餘時間
    """

def render_step_status_table(
    all_steps: List[str],
    completed_steps: List[str],
    failed_steps: List[str],
    current_step: str
) -> None:
    """
    渲染步驟狀態表格

    Columns:
        - 序號
        - 步驟名稱
        - 狀態 (待執行/執行中/完成/失敗)
    """
```

### 10.5 Data Preview

```python
# components/data_preview.py

def render_data_preview(
    data: pd.DataFrame,
    title: str = "數據預覽",
    max_rows: int = 100,
    show_stats: bool = True
) -> None:
    """
    渲染 DataFrame 預覽

    Features:
        - 統計資訊 (行數/欄數/記憶體)
        - 動態欄位選擇
        - 行數 Slider (10-max_rows)
        - CSV 下載按鈕
    """

def render_auxiliary_data_tabs(auxiliary_data: Dict[str, pd.DataFrame]) -> None:
    """
    渲染輔助數據 Tabs

    Args:
        auxiliary_data: {name: DataFrame} 字典
    """

def render_statistics_metrics(statistics: Dict[str, Any]) -> None:
    """
    渲染統計指標卡

    Layout: 最多 4 列
    """
```

### 10.6 Template Picker (已棄用)

```python
# components/template_picker.py

def render_template_picker(entity: str, proc_type: str) -> str:
    """
    渲染範本選擇器

    Status: DEPRECATED
    Reason: 已改用 orchestrator 配置驅動
    """
```

### 10.7 UI Helpers

```python
# utils/ui_helpers.py

def format_duration(seconds: float) -> str:
    """格式化時間長度 (e.g., '1分30秒')"""

def format_file_size(bytes: int) -> str:
    """格式化檔案大小 (e.g., '1.5 MB')"""

def get_status_icon(status: str) -> str:
    """獲取狀態圖示"""
    # 'pending'   → '⏳'
    # 'running'   → '🔄'
    # 'completed' → '✅'
    # 'failed'    → '❌'

def truncate_text(text: str, max_length: int = 50) -> str:
    """截斷過長文字"""
```

---

## 11. 服務層 API 參考

### 11.1 UnifiedPipelineService

```python
class UnifiedPipelineService:
    """統一的 Pipeline 服務層"""

    def __init__(self):
        """初始化服務"""

    # === 查詢 API ===

    def get_available_entities(self) -> List[str]:
        """
        獲取可用的 entity 清單

        Returns:
            ['SPT', 'SPX']
        """

    def get_entity_config(self, entity: str) -> Dict[str, Any]:
        """
        獲取 entity 設定

        Returns:
            {
                'display_name': 'SPX',
                'types': ['PO', 'PR', 'PPE'],
                'description': '...',
                'icon': '📦'
            }
        """

    def get_entity_types(self, entity: str) -> List[str]:
        """
        獲取 entity 支援的處理類型

        Returns:
            ['PO', 'PR'] or ['PO', 'PR', 'PPE']
        """

    def get_enabled_steps(self, entity: str, proc_type: str) -> List[str]:
        """
        獲取啟用的步驟清單

        Returns:
            ['SPXDataLoading', 'ColumnAddition', ...]
        """

    # === 構建 API ===

    def build_pipeline(
        self,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str],
        processing_date: Optional[int] = None
    ) -> Pipeline:
        """
        構建 Pipeline

        Args:
            entity: 'SPT' or 'SPX'
            proc_type: 'PO', 'PR', or 'PPE'
            file_paths: {file_key: path} 字典
            processing_date: YYYYMM (PPE 必填)

        Returns:
            Pipeline 物件

        Raises:
            ValueError: 不支援的 entity 或 proc_type
        """
```

### 11.2 StreamlitPipelineRunner

```python
class StreamlitPipelineRunner:
    """Pipeline 執行器"""

    def __init__(self, service: UnifiedPipelineService):
        """初始化執行器"""

    def set_progress_callback(
        self,
        callback: Callable[[str, int, int, str], None]
    ) -> None:
        """
        設置進度回調

        Callback signature:
            callback(step_name, current, total, status)
            - step_name: 步驟名稱
            - current: 當前步驟序號
            - total: 總步驟數
            - status: 'running', 'completed', 'failed'
        """

    def set_log_callback(
        self,
        callback: Callable[[str], None]
    ) -> None:
        """
        設置日誌回調

        Callback signature:
            callback(message)
        """

    async def execute(
        self,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str],
        processing_date: int,
        use_template: bool = False,
        template_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        執行 Pipeline

        Returns:
            {
                'success': bool,
                'context': ProcessingContext,
                'step_results': {step_name: result},
                'error': Optional[str],
                'execution_time': float,
                'pipeline_result': Dict
            }
        """
```

### 11.3 FileHandler

```python
class FileHandler:
    """檔案處理器"""

    def __init__(self, temp_dir: Optional[str] = None):
        """
        初始化處理器

        Args:
            temp_dir: 暫存目錄路徑，None 則自動創建
        """

    def save_uploaded_file(
        self,
        uploaded_file: UploadedFile,
        file_key: str
    ) -> str:
        """
        儲存上傳的檔案

        Args:
            uploaded_file: Streamlit UploadedFile 物件
            file_key: 檔案識別鍵

        Returns:
            儲存的檔案路徑
        """

    def validate_file(
        self,
        file_path: str,
        file_key: str
    ) -> List[str]:
        """
        驗證檔案

        Returns:
            錯誤訊息清單 (空列表表示通過)
        """

    def validate_all_files(
        self,
        file_paths: Dict[str, str]
    ) -> List[str]:
        """驗證所有檔案"""

    def cleanup(self) -> None:
        """清理暫存目錄"""
```

---

## 12. 錯誤處理與重試機制

### 12.1 Pipeline 層錯誤處理

```python
# PipelineStep.__call__() 中的重試邏輯

for attempt in range(self.retry_count + 1):
    try:
        if self.timeout:
            result = await asyncio.wait_for(
                self.execute(context),
                timeout=self.timeout
            )
        else:
            result = await self.execute(context)
        break  # 成功則退出

    except asyncio.TimeoutError as e:
        last_error = e
        self.logger.error(f"Step {self.name} timeout")

    except Exception as e:
        last_error = e
        if attempt < self.retry_count:
            # 指數退避重試
            self.logger.warning(f"Retrying... ({attempt + 1})")
            await asyncio.sleep(2 ** attempt)

# 所有重試失敗
if result is None:
    if self.required:
        await self.rollback(context, last_error)
        raise last_error
    else:
        result = StepResult(status=StepStatus.FAILED, error=last_error)
```

### 12.2 UI 層錯誤處理

```python
# 3_execution.py

try:
    result = AsyncBridge.run_async(runner.execute(...))

    if result['success']:
        execution.status = ExecutionStatus.COMPLETED
        st.success("✅ 執行完成")
        st.switch_page("pages/4_results.py")
    else:
        execution.status = ExecutionStatus.FAILED
        execution.error_message = result['error']
        st.error(f"❌ 執行失敗: {result['error']}")

except Exception as e:
    execution.status = ExecutionStatus.FAILED
    execution.error_message = str(e)
    st.error(f"❌ 系統錯誤: {e}")
    st.exception(e)  # 顯示完整堆疊追蹤
```

### 12.3 檔案驗證錯誤

```python
# file_handler.py

def validate_file(self, file_path: str, file_key: str) -> List[str]:
    errors = []

    # 檢查檔案存在
    if not os.path.exists(file_path):
        errors.append(f"檔案不存在: {file_path}")
        return errors

    # 檢查檔案大小
    if os.path.getsize(file_path) == 0:
        errors.append(f"檔案為空: {file_key}")
        return errors

    # 檢查格式可讀性
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.csv':
            pd.read_csv(file_path, nrows=1)
        elif ext in ['.xlsx', '.xls']:
            pd.read_excel(file_path, nrows=1)
    except Exception as e:
        errors.append(f"無法讀取 {file_key}: {e}")

    return errors
```

---

## 13. Checkpoint 系統

### 13.1 Checkpoint 結構

```
checkpoints/
├── SPX_PO_202512_after_SPXDataLoading/
│   ├── data.parquet              # 主數據
│   ├── auxiliary_data/
│   │   ├── previous.parquet
│   │   ├── ap_invoice.parquet
│   │   └── procurement.parquet
│   └── checkpoint_info.json      # 元數據
│
└── SPT_PR_202512_after_SPTERMLogic/
    ├── data.parquet
    └── checkpoint_info.json
```

### 13.2 checkpoint_info.json 結構

```json
{
    "entity_type": "SPX",
    "processing_type": "PO",
    "processing_date": 202512,
    "step_name": "SPXDataLoading",
    "created_at": "2026-01-17T10:30:00",
    "data_shape": [43225, 85],
    "auxiliary_data": ["previous", "ap_invoice", "procurement"],
    "variables": {
        "file_paths": {...}
    },
    "history": [
        {"step": "SPXDataLoading", "status": "success", "duration": 5.2}
    ]
}
```

### 13.3 CheckpointManager API

```python
class CheckpointManager:
    """Checkpoint 管理器"""

    def __init__(self, checkpoint_dir: str = "./checkpoints"):
        """初始化"""

    def save_checkpoint(
        self,
        context: ProcessingContext,
        step_name: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        儲存 Checkpoint

        Returns:
            Checkpoint 名稱 (e.g., "SPX_PO_202512_after_SPXDataLoading")
        """

    def load_checkpoint(self, checkpoint_name: str) -> ProcessingContext:
        """
        載入 Checkpoint

        Returns:
            還原的 ProcessingContext
        """

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """
        列出所有 Checkpoint

        Returns:
            [{'name': str, 'created_at': datetime, 'size': int}, ...]
        """

    def delete_checkpoint(self, checkpoint_name: str) -> bool:
        """刪除 Checkpoint"""
```

---

## 14. 擴充指南：新增 Pipeline 類型

本章節說明如何在現有架構下擴充系統，包含兩個主要場景：
1. **場景 A**：在現有 Entity 新增 Processing Type（例如在 SPX 新增 'INV' 發票處理）
2. **場景 B**：新增全新的 Entity（例如新增 'MOB' 實體）

### 14.1 擴充架構總覽

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         擴充時需要修改的檔案                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  場景 A: 新增 Processing Type                                                │
│  ─────────────────────────────                                              │
│  1. ui/config.py                    ← ENTITY_CONFIG['SPX']['types']         │
│  2. ui/config.py                    ← REQUIRED_FILES, OPTIONAL_FILES        │
│  3. config/paths.toml               ← [spx.inv] 檔案路徑與參數               │
│  4. config/stagging.toml            ← [pipeline.spx] enabled_inv_steps      │
│  5. tasks/spx/pipeline_orchestrator ← build_inv_pipeline(), _create_step()  │
│  6. ui/services/unified_pipeline_service ← build_pipeline() 分支            │
│                                                                              │
│  場景 B: 新增 Entity                                                         │
│  ──────────────────                                                         │
│  上述全部 + 新增:                                                            │
│  7. tasks/mob/pipeline_orchestrator.py  ← 新的 Orchestrator 類別            │
│  8. tasks/mob/steps/*.py                ← 實體特定步驟 (如需要)              │
│  9. ui/services/unified_pipeline_service ← _get_orchestrator() 註冊         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 14.2 場景 A：在現有 Entity 新增 Processing Type

以在 **SPX** 新增 **INV (Invoice 發票核對)** 處理類型為例。

#### Step 1: 更新 UI 配置 (`ui/config.py`)

```python
# ============================================================
# 1.1 在 ENTITY_CONFIG 新增支援的 type
# ============================================================
ENTITY_CONFIG: Dict[str, Dict] = {
    'SPX': {
        'display_name': 'SPX',
        'types': ['PO', 'PR', 'PPE', 'INV'],  # ← 新增 'INV'
        'description': 'SPX Platform for opened PR/PO',
        'icon': '📦',
    },
    # ...
}

# ============================================================
# 1.2 在 PROCESSING_TYPE_CONFIG 定義新類型的顯示資訊
# ============================================================
PROCESSING_TYPE_CONFIG: Dict[str, Dict] = {
    # ... 現有類型 ...
    'INV': {
        'display_name': '發票核對 (INV)',
        'description': 'Invoice Reconciliation 處理流程',
        'icon': '🧾',
    },
}

# ============================================================
# 1.3 定義必填檔案
# ============================================================
REQUIRED_FILES: Dict[Tuple[str, str], List[str]] = {
    # ... 現有配置 ...
    ('SPX', 'INV'): ['raw_invoice', 'ap_aging'],  # ← 新增
}

# ============================================================
# 1.4 定義選填檔案
# ============================================================
OPTIONAL_FILES: Dict[Tuple[str, str], List[str]] = {
    # ... 現有配置 ...
    ('SPX', 'INV'): ['previous_inv', 'vendor_master'],  # ← 新增
}

# ============================================================
# 1.5 新增檔案標籤
# ============================================================
FILE_LABELS: Dict[str, str] = {
    # ... 現有標籤 ...
    'raw_invoice': '發票原始資料 (必填)',
    'ap_aging': 'AP 帳齡報表 (必填)',
    'previous_inv': '前期發票底稿 (選填)',
    'vendor_master': '供應商主檔 (選填)',
}
```

#### Step 2: 新增檔案路徑與參數 (`config/paths.toml`)

```toml
# ============================================================
# 新增 [spx.inv] 區段
# ============================================================
[spx.inv]
raw_invoice = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_AP_Invoice_Detail.xlsx"
ap_aging = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_AP_Aging_Report.xlsx"
previous_inv = "{resources}/{YYYYMM}/前期底稿/SPX/{PREV_YYYYMM}_INV_FN.xlsx"
vendor_master = "{resources}/Master Data/Vendor_Master.xlsx"

# ============================================================
# 新增 [spx.inv.params] 區段 - 檔案讀取參數
# ============================================================
[spx.inv.params]
raw_invoice = { sheet_name = "Detail", header = 0, dtype = "str" }
ap_aging = { sheet_name = "Aging", header = 1, dtype = "str" }
previous_inv = { sheet_name = 0, header = 0, dtype = "str" }
vendor_master = { sheet_name = "Vendors", header = 0 }
```

#### Step 3: 定義啟用步驟 (`config/stagging.toml`)

```toml
[pipeline.spx]
# ... 現有配置 ...

# ============================================================
# 新增 enabled_inv_steps
# ============================================================
enabled_inv_steps = [
    "SPXINVDataLoading",
    "InvoiceValidation",
    "VendorMatching",
    "AgingAnalysis",
    "INVERMLogic",
    "INVStatusLabel",
    "SPXINVExport",
]
```

#### Step 4: 更新 Orchestrator (`tasks/spx/pipeline_orchestrator.py`)

```python
# ============================================================
# 4.1 導入新步驟類別 (如果是新建的步驟)
# ============================================================
from accrual_bot.tasks.spx.steps import (
    # ... 現有導入 ...
    SPXINVDataLoadingStep,
    InvoiceValidationStep,
    VendorMatchingStep,
    AgingAnalysisStep,
    INVERMLogicStep,
    INVStatusLabelStep,
    SPXINVExportStep,
)

class SPXPipelineOrchestrator:
    # ... 現有方法 ...

    # ============================================================
    # 4.2 新增 build_inv_pipeline 方法
    # ============================================================
    def build_inv_pipeline(
        self,
        file_paths: Dict[str, Any],
        processing_date: int,
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """
        構建 SPX INV (Invoice) 處理 pipeline

        Args:
            file_paths: 文件路徑配置
            processing_date: 處理日期 (YYYYMM)
            custom_steps: 自定義步驟（可選）

        Returns:
            Pipeline: 配置好的 pipeline
        """
        pipeline_config = PipelineConfig(
            name="SPX_INV_Processing",
            description="SPX Invoice reconciliation pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        # 獲取啟用的步驟列表
        enabled_steps = self.config.get('enabled_inv_steps', [])

        if not enabled_steps:
            # 默認步驟順序
            enabled_steps = [
                "SPXINVDataLoading",
                "InvoiceValidation",
                "VendorMatching",
                "AgingAnalysis",
                "INVERMLogic",
                "INVStatusLabel",
                "SPXINVExport",
            ]

        # 動態添加步驟
        for step_name in enabled_steps:
            step = self._create_step(step_name, file_paths, processing_type='INV')
            if step:
                pipeline.add_step(step)

        # 添加自定義步驟
        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    # ============================================================
    # 4.3 更新 _create_step 註冊新步驟
    # ============================================================
    def _create_step(
        self,
        step_name: str,
        file_paths: Dict[str, Any],
        processing_type: str = 'PO'
    ) -> Optional[PipelineStep]:
        step_registry = {
            # ... 現有步驟 ...

            # INV 專用步驟
            'SPXINVDataLoading': lambda: SPXINVDataLoadingStep(
                name="SPXINVDataLoading",
                file_paths=file_paths
            ),
            'InvoiceValidation': lambda: InvoiceValidationStep(
                name="InvoiceValidation"
            ),
            'VendorMatching': lambda: VendorMatchingStep(
                name="VendorMatching"
            ),
            'AgingAnalysis': lambda: AgingAnalysisStep(
                name="AgingAnalysis"
            ),
            'INVERMLogic': lambda: INVERMLogicStep(
                name="INVERMLogic"
            ),
            'INVStatusLabel': lambda: INVStatusLabelStep(
                name="INVStatusLabel"
            ),
            'SPXINVExport': lambda: SPXINVExportStep(
                name="SPXINVExport"
            ),
        }

        step_factory = step_registry.get(step_name)
        if step_factory:
            return step_factory()
        else:
            print(f"Warning: Unknown step '{step_name}' for SPX {processing_type}")
            return None

    # ============================================================
    # 4.4 更新 get_enabled_steps 支援新類型
    # ============================================================
    def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
        if processing_type == 'PO':
            return self.config.get('enabled_po_steps', [])
        elif processing_type == 'PR':
            return self.config.get('enabled_pr_steps', [])
        elif processing_type == 'PPE':
            return self.config.get('enabled_ppe_steps', [])
        elif processing_type == 'INV':  # ← 新增
            return self.config.get('enabled_inv_steps', [])
        else:
            return []
```

#### Step 5: 更新服務層 (`ui/services/unified_pipeline_service.py`)

```python
def build_pipeline(
    self,
    entity: str,
    proc_type: str,
    file_paths: Dict[str, str],
    processing_date: Optional[int] = None
) -> Pipeline:
    enriched_file_paths = self._enrich_file_paths(file_paths, entity, proc_type)
    orchestrator = self._get_orchestrator(entity)

    if proc_type == 'PO':
        return orchestrator.build_po_pipeline(enriched_file_paths)
    elif proc_type == 'PR':
        return orchestrator.build_pr_pipeline(enriched_file_paths)
    elif proc_type == 'PPE' and entity == 'SPX':
        if not processing_date:
            raise ValueError("PPE 處理需要提供 processing_date")
        return orchestrator.build_ppe_pipeline(enriched_file_paths, processing_date)
    # ============================================================
    # 新增 INV 類型處理
    # ============================================================
    elif proc_type == 'INV' and entity == 'SPX':
        if not processing_date:
            raise ValueError("INV 處理需要提供 processing_date")
        return orchestrator.build_inv_pipeline(enriched_file_paths, processing_date)
    else:
        raise ValueError(f"不支援的處理類型: {entity}/{proc_type}")
```

#### Step 6: (選填) 創建新的步驟類別

如果需要全新的業務邏輯，在 `tasks/spx/steps/` 目錄下創建新步驟：

```python
# tasks/spx/steps/inv_steps.py

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


class SPXINVDataLoadingStep(PipelineStep):
    """SPX Invoice 數據加載步驟"""

    def __init__(self, name: str, file_paths: dict):
        super().__init__(name=name, required=True)
        self.file_paths = file_paths

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據加載"""
        # 實作加載邏輯
        # ...
        return StepResult(
            step_name=self.name,
            status=StepStatus.SUCCESS,
            message="Invoice data loaded successfully"
        )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'raw_invoice' in self.file_paths


class InvoiceValidationStep(PipelineStep):
    """發票驗證步驟"""
    # ...
```

### 14.3 場景 B：新增全新 Entity

以新增 **MOB (Mobile)** 實體為例。

#### Step 1-5: 同場景 A

按照場景 A 的步驟 1-5 進行配置，但需要創建全新的配置區段。

#### Step 6: 創建新的 Orchestrator (`tasks/mob/pipeline_orchestrator.py`)

```python
"""
MOB Pipeline Orchestrator

Manages MOB-specific pipeline configuration and construction.
"""

from typing import List, Dict, Any, Optional
from accrual_bot.core.pipeline import Pipeline, PipelineConfig
from accrual_bot.core.pipeline.base import PipelineStep
from accrual_bot.utils.config import config_manager

# 導入 MOB 專用步驟
from accrual_bot.tasks.mob.steps import (
    MOBDataLoadingStep,
    MOBERMLogicStep,
    MOBExportStep,
)

# 導入共享步驟
from accrual_bot.core.pipeline.steps import (
    ProductFilterStep,
    ColumnAdditionStep,
    DateLogicStep,
)


class MOBPipelineOrchestrator:
    """
    MOB Pipeline 編排器

    功能:
    1. 根據配置動態創建 pipeline
    2. 支援 PO/PR 處理類型
    """

    def __init__(self):
        self.config = config_manager._config_toml.get('pipeline', {}).get('mob', {})
        self.entity_type = 'MOB'

    def build_po_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """構建 MOB PO 處理 pipeline"""
        pipeline_config = PipelineConfig(
            name="MOB_PO_Processing",
            description="MOB PO data processing pipeline",
            entity_type=self.entity_type,
            stop_on_error=True
        )

        pipeline = Pipeline(pipeline_config)

        enabled_steps = self.config.get('enabled_po_steps', [])

        if not enabled_steps:
            enabled_steps = [
                "MOBDataLoading",
                "ProductFilter",
                "ColumnAddition",
                "DateLogic",
                "MOBERMLogic",
                "MOBExport",
            ]

        for step_name in enabled_steps:
            step = self._create_step(step_name, file_paths, processing_type='PO')
            if step:
                pipeline.add_step(step)

        if custom_steps:
            for step in custom_steps:
                pipeline.add_step(step)

        return pipeline

    def build_pr_pipeline(
        self,
        file_paths: Dict[str, Any],
        custom_steps: Optional[List[PipelineStep]] = None
    ) -> Pipeline:
        """構建 MOB PR 處理 pipeline"""
        # 類似 build_po_pipeline 的實作
        pass

    def _create_step(
        self,
        step_name: str,
        file_paths: Dict[str, Any],
        processing_type: str = 'PO'
    ) -> Optional[PipelineStep]:
        """步驟工廠方法"""
        step_registry = {
            # MOB 專用步驟
            'MOBDataLoading': lambda: MOBDataLoadingStep(
                name="MOBDataLoading",
                file_paths=file_paths
            ),
            'MOBERMLogic': lambda: MOBERMLogicStep(
                name="MOBERMLogic"
            ),
            'MOBExport': lambda: MOBExportStep(
                name="MOBExport"
            ),

            # 共享步驟
            'ProductFilter': lambda: ProductFilterStep(
                name="ProductFilter",
                product_pattern='(?i)LG_MOB',
                required=True
            ),
            'ColumnAddition': lambda: ColumnAdditionStep(
                name="ColumnAddition"
            ),
            'DateLogic': lambda: DateLogicStep(
                name="DateLogic",
                required=True
            ),
        }

        step_factory = step_registry.get(step_name)
        if step_factory:
            return step_factory()
        else:
            print(f"Warning: Unknown step '{step_name}' for MOB {processing_type}")
            return None

    def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
        """獲取啟用的步驟列表"""
        if processing_type == 'PO':
            return self.config.get('enabled_po_steps', [])
        elif processing_type == 'PR':
            return self.config.get('enabled_pr_steps', [])
        return []
```

#### Step 7: 在服務層註冊新 Orchestrator

```python
# ui/services/unified_pipeline_service.py

from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.tasks.spx import SPXPipelineOrchestrator
from accrual_bot.tasks.mob import MOBPipelineOrchestrator  # ← 新增導入


class UnifiedPipelineService:
    # ...

    def _get_orchestrator(self, entity: str):
        """獲取對應的 orchestrator"""
        orchestrators = {
            'SPT': SPTPipelineOrchestrator,
            'SPX': SPXPipelineOrchestrator,
            'MOB': MOBPipelineOrchestrator,  # ← 新增註冊
        }

        orchestrator_class = orchestrators.get(entity)
        if not orchestrator_class:
            raise ValueError(f"不支援的 entity: {entity}")

        return orchestrator_class()
```

### 14.4 擴充檢查清單

使用以下檢查清單確保擴充完整：

#### 場景 A: 新增 Processing Type

| # | 檔案 | 修改項目 | 完成 |
|---|------|----------|------|
| 1 | `ui/config.py` | `ENTITY_CONFIG['entity']['types']` 新增類型 | ☐ |
| 2 | `ui/config.py` | `PROCESSING_TYPE_CONFIG` 新增類型定義 | ☐ |
| 3 | `ui/config.py` | `REQUIRED_FILES[(entity, type)]` 新增 | ☐ |
| 4 | `ui/config.py` | `OPTIONAL_FILES[(entity, type)]` 新增 | ☐ |
| 5 | `ui/config.py` | `FILE_LABELS` 新增檔案標籤 | ☐ |
| 6 | `config/paths.toml` | `[entity.type]` 新增檔案路徑 | ☐ |
| 7 | `config/paths.toml` | `[entity.type.params]` 新增讀取參數 | ☐ |
| 8 | `config/stagging.toml` | `enabled_xxx_steps` 新增步驟清單 | ☐ |
| 9 | `tasks/xxx/pipeline_orchestrator.py` | `build_xxx_pipeline()` 新增方法 | ☐ |
| 10 | `tasks/xxx/pipeline_orchestrator.py` | `_create_step()` 註冊新步驟 | ☐ |
| 11 | `tasks/xxx/pipeline_orchestrator.py` | `get_enabled_steps()` 支援新類型 | ☐ |
| 12 | `ui/services/unified_pipeline_service.py` | `build_pipeline()` 新增分支 | ☐ |
| 13 | `tasks/xxx/steps/` | 創建新步驟類別 (如需要) | ☐ |

#### 場景 B: 新增 Entity

上述全部，加上：

| # | 檔案 | 修改項目 | 完成 |
|---|------|----------|------|
| 14 | `ui/config.py` | `ENTITY_CONFIG` 新增 entity | ☐ |
| 15 | `tasks/xxx/__init__.py` | 創建模組並導出 Orchestrator | ☐ |
| 16 | `tasks/xxx/pipeline_orchestrator.py` | 創建 Orchestrator 類別 | ☐ |
| 17 | `tasks/xxx/steps/__init__.py` | 創建步驟模組 | ☐ |
| 18 | `ui/services/unified_pipeline_service.py` | `_get_orchestrator()` 註冊 | ☐ |

### 14.5 擴充流程圖

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         新增 Processing Type 流程                            │
└─────────────────────────────────────────────────────────────────────────────┘

User Request: "在 SPX 新增 INV 類型"
    │
    ▼
┌─────────────────────────────────────────────┐
│ Step 1: 更新 UI 配置                        │
│ • ENTITY_CONFIG['SPX']['types'] += ['INV'] │
│ • REQUIRED_FILES[('SPX', 'INV')] = [...]   │
│ • FILE_LABELS['raw_invoice'] = '...'       │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Step 2: 更新配置文件                        │
│ • paths.toml: [spx.inv], [spx.inv.params]  │
│ • stagging.toml: enabled_inv_steps         │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Step 3: 更新 Orchestrator                   │
│ • build_inv_pipeline() 方法                 │
│ • _create_step() 註冊新步驟                 │
│ • get_enabled_steps() 支援 'INV'           │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Step 4: 更新服務層                          │
│ • build_pipeline() 新增 elif 分支          │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Step 5: (選填) 創建新步驟類別               │
│ • tasks/spx/steps/inv_steps.py             │
│ • 實作 SPXINVDataLoadingStep 等            │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Step 6: 測試驗證                            │
│ • 啟動 UI，選擇 SPX → INV                  │
│ • 確認檔案上傳區顯示正確                    │
│ • 確認步驟預覽顯示正確                      │
│ • 執行 pipeline，確認無錯誤                 │
└─────────────────────────────────────────────┘
```

### 14.6 常見問題與解決方案

#### Q1: 新增的類型在 UI 沒有顯示？

**可能原因**: `ENTITY_CONFIG` 中的 `types` 列表沒有更新

**解決方案**:
```python
# ui/config.py
ENTITY_CONFIG = {
    'SPX': {
        'types': ['PO', 'PR', 'PPE', 'INV'],  # 確認已新增
        # ...
    },
}
```

#### Q2: 檔案上傳後參數沒有正確傳遞？

**可能原因**: `paths.toml` 中的 `[entity.type.params]` 區段配置錯誤

**解決方案**:
1. 確認區段名稱正確：`[spx.inv.params]`（全小寫）
2. 確認參數格式：
   ```toml
   raw_invoice = { sheet_name = "Detail", header = 0 }
   ```
3. 在 `_enrich_file_paths()` 中添加調試日誌確認讀取

#### Q3: get_enabled_steps() 返回空列表？

**可能原因**: `stagging.toml` 中的步驟清單名稱不匹配

**解決方案**:
```toml
# config/stagging.toml
[pipeline.spx]
enabled_inv_steps = [...]  # 確認名稱與 get_enabled_steps() 中一致
```

```python
# pipeline_orchestrator.py
def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
    if processing_type == 'INV':
        return self.config.get('enabled_inv_steps', [])  # 名稱要一致
```

#### Q4: Pipeline 執行時找不到步驟？

**可能原因**: `_create_step()` 中未註冊該步驟

**解決方案**:
```python
def _create_step(self, step_name, file_paths, processing_type):
    step_registry = {
        # ... 確認已註冊所有步驟 ...
        'SPXINVDataLoading': lambda: SPXINVDataLoadingStep(...),
    }
```

#### Q5: 如何複用現有步驟？

**解決方案**: 直接在 `_create_step()` 中引用共享步驟：

```python
from accrual_bot.core.pipeline.steps import ProductFilterStep, DateLogicStep

def _create_step(self, step_name, file_paths, processing_type):
    step_registry = {
        # 複用共享步驟
        'ProductFilter': lambda: ProductFilterStep(
            name="ProductFilter",
            product_pattern='(?i)LG_SPX',  # 可自定義參數
            required=True
        ),
        'DateLogic': lambda: DateLogicStep(
            name="DateLogic",
            required=True
        ),
        # INV 專用步驟
        'SPXINVDataLoading': lambda: SPXINVDataLoadingStep(...),
    }
```

### 14.7 擴充設計最佳實踐

1. **配置優先**: 優先通過配置文件控制行為，避免硬編碼

2. **步驟複用**: 盡可能複用 `core/pipeline/steps/` 中的共享步驟

3. **命名一致性**:
   - 步驟類別：`{Entity}{Type}xxxStep`（如 `SPXINVDataLoadingStep`）
   - 配置鍵：`enabled_{type}_steps`（如 `enabled_inv_steps`）
   - 方法名：`build_{type}_pipeline`（如 `build_inv_pipeline`）

4. **漸進式測試**:
   - 先測試配置頁面是否正確顯示新類型
   - 再測試檔案上傳是否正確
   - 最後測試完整 pipeline 執行

5. **文檔同步**: 更新 `CLAUDE.md` 和本文檔中的相關描述

---

## 15. 已知限制與改進建議

### 15.1 已知限制

| 類別 | 限制 | 影響 | 狀態 |
|------|------|------|------|
| **功能** | Checkpoint 載入未實作 | 無法從中間步驟繼續執行 | ⚠️ 待處理 |
| **功能** | Pipeline 無法取消 | 長時間執行無法中斷 | ⚠️ 待處理 |
| **效能** | 進度更新非實時 | 執行完成後才批量更新 | ⚠️ 待處理 |
| ~~**功能**~~ | ~~日誌無法匯出~~ | ~~調試不便~~ | ✅ **已修復** (2026-01-17) |
| ~~**架構**~~ | ~~重複頁面檔案~~ | ~~v1 和 v2 版本並存~~ | ✅ **已修復** (2026-01-17) |
| ~~**配置**~~ | ~~Template 系統已棄用~~ | ~~代碼冗餘~~ | ✅ **已移除** (2026-01-17) |

### 15.2 改進建議

#### ✅ 已完成 (2026-01-17)

<details>
<summary><b>1. 清理重複頁面</b> ✅ 完成</summary>

**問題**: `accrual_bot/ui/pages/` 同時存在兩組頁面檔案
- 數字版本: `1_configuration.py`, `2_file_upload.py` 等
- 模組化版本: `configuration_page.py`, `file_upload_page.py` 等

**解決方案**:
```bash
# 刪除重複的模組化版本
rm accrual_bot/ui/pages/configuration_page.py
rm accrual_bot/ui/pages/file_upload_page.py
rm accrual_bot/ui/pages/execution_page.py
rm accrual_bot/ui/pages/results_page.py
rm accrual_bot/ui/pages/checkpoint_page.py
```

**成果**:
- 刪除 5 個冗餘檔案
- 減少約 20KB 代碼
- 維護更簡單

</details>

<details>
<summary><b>2. 移除已棄用的 Template 系統</b> ✅ 完成</summary>

**移除的檔案**:
- `accrual_bot/ui/components/template_picker.py`

**修改的檔案** (7 個):
- `ui/components/__init__.py` - 移除導入
- `ui/pages/1_configuration.py` - 移除範本選擇
- `ui/services/unified_pipeline_service.py` - 刪除 `get_templates()`, `build_pipeline_from_template()` 方法
- `ui/services/pipeline_runner.py` - 移除 `use_template`, `template_name` 參數
- `ui/pages/3_execution.py` - 清理 execute 呼叫
- `ui/models/state_models.py` - 刪除 `template_name` 欄位
- `ui/components/entity_selector.py` - 移除重置邏輯

**成果**:
- 刪除約 150 行已棄用代碼
- API 介面更簡潔
- 減少使用者困惑

</details>

<details>
<summary><b>3. 添加日誌匯出功能</b> ✅ 完成</summary>

**位置**: `ui/pages/3_execution.py`

**實作**:
```python
col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("📝 執行日誌")
with col2:
    if execution.logs:
        log_content = "\n".join(execution.logs)
        st.download_button(
            label="📥 下載日誌",
            data=log_content,
            file_name=f"{entity}_{proc_type}_{date}_logs.txt",
            mime="text/plain"
        )
```

**成果**:
- 支援日誌匯出為 `.txt` 檔案
- 檔名格式: `SPX_PO_202512_logs.txt`
- 方便離線分析和問題排查

</details>

<details>
<summary><b>4. 修復雙層 Pages 架構</b> ✅ 完成</summary>

**問題**: 專案使用雙層 Pages 目錄，但 Entry Point 檔案導入方式錯誤

**架構說明**:
```
專案根目錄/pages/           ← Streamlit 識別層（emoji 檔名）
└─ 1_⚙️_配置.py              Entry Point (17 行)
   ↓ exec()
   └─ accrual_bot/ui/pages/  ← 實際實作層（數字檔名）
      └─ 1_configuration.py  真正的邏輯 (73 行)
```

**為什麼需要兩組**:
1. **Streamlit 限制**: Multi-page 需要 emoji 檔名才能在 sidebar 顯示
2. **最佳實踐**: 不在檔名使用 emoji（跨平台、版本控制問題）
3. **解耦設計**: 進入點與業務邏輯分離

**修復內容**:
- 修改 5 個 emoji Entry Point 檔案，改用 `exec()` 執行實際頁面
- 修正所有 `st.switch_page()` 路徑指向 emoji 版本
- 清理 `accrual_bot/ui/pages/__init__.py` 的錯誤導入

**成果**:
- 頁面導航正常運作
- 無 import 錯誤
- 保持代碼整潔

</details>

<details>
<summary><b>5. 修復 ProcessingContext.auxiliary_data 屬性</b> ✅ 完成</summary>

**問題**: `ProcessingContext` 將輔助數據存儲在私有屬性 `_auxiliary_data`，但 UI 層試圖直接訪問不存在的公開屬性 `auxiliary_data`

**修復**: 在 `core/pipeline/context.py` 添加 property：
```python
@property
def auxiliary_data(self) -> Dict[str, pd.DataFrame]:
    """獲取所有輔助數據"""
    return self._auxiliary_data.copy()

def set_auxiliary_data(self, name: str, data: pd.DataFrame):
    """設置輔助數據（add_auxiliary_data 的別名）"""
    self.add_auxiliary_data(name, data)
```

**成果**:
- UI 可正常訪問輔助數據
- 提供一致的 getter/setter 介面
- 保持向後兼容

</details>

#### High Priority

1. **實作 Checkpoint 載入功能**
   ```python
   # 5_checkpoint.py
   if st.button("載入"):
       context = checkpoint_manager.load_checkpoint(checkpoint_name)
       # 跳轉到執行頁，從下一個步驟繼續
   ```

#### Medium Priority

2. **實作 Pipeline 取消功能**
   ```python
   # 使用 asyncio.Task.cancel()
   if st.button("停止"):
       if hasattr(st.session_state, 'pipeline_task'):
           st.session_state.pipeline_task.cancel()
   ```

3. **修復 DataSourcePool 資源清理**
   ```python
   # 在 Pipeline 執行完成後確保正確關閉
   finally:
       await DataSourcePool.close_all()
   ```

#### Low Priority

4. **Session 持久化**
   - 瀏覽器刷新後保留狀態
   - 使用 `st.cache_data` 或外部儲存

5. **添加 UI 元件測試**
   - 使用 `streamlit.testing` 模組
   - Mock Session State

6. **修復 Pandas 警告**
   ```python
   # SettingWithCopyWarning
   df = df.copy()
   df.loc[mask, col] = value

   # 日期格式
   pd.to_datetime(df[col], format='%Y-%m-%d')
   ```

---

## 16. 附錄

### 16.1 檔案清單與行數

| 檔案 | 行數 | 職責 | 狀態 |
|------|------|------|------|
| **根目錄** | | | |
| `__init__.py` | 8 | 模組版本 | ✅ |
| `app.py` | 71 | Session State 初始化 | ✅ |
| `config.py` | 126 | UI 配置常數 | ✅ |
| **models/** | | | |
| `state_models.py` | 62 | 狀態 Dataclass | ✅ 已更新 |
| **components/** | | | |
| `entity_selector.py` | 177 | Entity/Type 選擇 | ✅ 已更新 |
| `step_preview.py` | 73 | 步驟預覽 | ✅ |
| `file_uploader.py` | 143 | 檔案上傳 | ✅ |
| `progress_tracker.py` | 111 | 進度追蹤 | ✅ |
| `data_preview.py` | 145 | 數據預覽 | ✅ |
| **services/** | | | |
| `unified_pipeline_service.py` | 210 | Pipeline 服務 (核心) | ✅ 已精簡 |
| `pipeline_runner.py` | 162 | Pipeline 執行器 | ✅ 已精簡 |
| `file_handler.py` | 157 | 檔案處理 | ✅ |
| **pages/** | | | |
| `1_configuration.py` | 65 | 配置頁 | ✅ 已精簡 |
| `2_file_upload.py` | 80 | 上傳頁 | ✅ 已更新 |
| `3_execution.py` | 205 | 執行頁 | ✅ 已更新 |
| `4_results.py` | 149 | 結果頁 | ✅ 已更新 |
| `5_checkpoint.py` | 142 | Checkpoint 頁 | ✅ |
| **utils/** | | | |
| `async_bridge.py` | 95 | Async 橋接 | ✅ |
| `ui_helpers.py` | 112 | 輔助函數 | ✅ |
| **Entry Points (根目錄 pages/)** | | | |
| `1_⚙️_配置.py` | 17 | Streamlit Entry Point | ✅ 已重構 |
| `2_📁_檔案上傳.py` | 17 | Streamlit Entry Point | ✅ 已重構 |
| `3_▶️_執行.py` | 17 | Streamlit Entry Point | ✅ 已重構 |
| `4_📊_結果.py` | 17 | Streamlit Entry Point | ✅ 已重構 |
| `5_💾_Checkpoint.py` | 17 | Streamlit Entry Point | ✅ 已重構 |
| **總計** | **~2,210** | | |

**變更摘要 (2026-01-17)**:
- ❌ 刪除 `template_picker.py` (93 行)
- ❌ 刪除 5 個重複頁面檔案 (~400 行)
- ➕ 添加 5 個 Entry Point 檔案 (85 行)
- ✂️ 精簡多個檔案的 template 相關代碼 (~150 行)
- **淨減少**: ~558 行代碼 (~22% 減少)

### 16.2 依賴關係圖

```
                    ┌─────────────────┐
                    │  state_models   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   config.py  │    │ ui_helpers   │    │ async_bridge │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       │    ┌──────────────┴──────────────┐    │
       │    │                             │    │
       ▼    ▼                             ▼    ▼
┌──────────────────────────────────────────────────────┐
│                      components/                      │
│  entity_selector | file_uploader | progress_tracker  │
│  step_preview | data_preview | template_picker       │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│                       services/                       │
│  unified_pipeline_service | pipeline_runner          │
│  file_handler                                        │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│                        pages/                         │
│  1_configuration | 2_file_upload | 3_execution       │
│  4_results | 5_checkpoint                            │
└──────────────────────────────────────────────────────┘
```

### 16.3 Session State 完整結構

```python
st.session_state = {
    # === 核心狀態 ===
    'pipeline_config': PipelineConfig,
    'file_upload': FileUploadState,
    'execution': ExecutionState,
    'result': ResultState,

    # === 服務實例 ===
    'file_handler': FileHandler,

    # === 暫存 ===
    'temp_dir': str,

    # === 導航 ===
    'current_page': str,

    # === Checkpoint 頁面 ===
    'confirm_delete_all': bool,

    # === 動態 UI 狀態 ===
    # (由 Streamlit 自動管理的 widget keys)
    'columns_數據預覽': List[str],
    'rows_數據預覽': int,
    'download_數據預覽': bool,
    # ...
}
```

### 16.4 配置文件快速參考

| 配置項 | 檔案 | 路徑 | 用途 |
|--------|------|------|------|
| Entity 列表 | `ui/config.py` | `ENTITY_CONFIG` | UI 顯示 |
| 處理類型 | `ui/config.py` | `ENTITY_CONFIG[entity]['types']` | UI 顯示 |
| 必填檔案 | `ui/config.py` | `REQUIRED_FILES` | 上傳驗證 |
| 選填檔案 | `ui/config.py` | `OPTIONAL_FILES` | 上傳提示 |
| 檔案標籤 | `ui/config.py` | `FILE_LABELS` | UI 顯示 |
| 檔案路徑 | `config/paths.toml` | `[entity.proc_type]` | 路徑模板 |
| 檔案參數 | `config/paths.toml` | `[entity.proc_type.params]` | 讀取參數 |
| 啟用步驟 | `config/stagging.toml` | `[pipeline.entity].enabled_*_steps` | Pipeline 構建 |

---

## 版本歷史

| 版本 | 日期 | 變更 |
|------|------|------|
| 2.1.0 | 2026-01-17 | 新增第 14 章：擴充指南 (新增 Pipeline 類型) |
| 2.0.0 | 2026-01-17 | 初始版本，完整架構文檔 |

---

> **文檔維護**: 當 UI 架構有重大變更時，請同步更新此文檔。
>
> **聯絡方式**: 如有問題或建議，請提交 Issue 或聯繫架構團隊。
