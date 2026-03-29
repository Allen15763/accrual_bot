# UI 模組深度研究報告 (accrual_bot/ui)

> 撰寫日期：2026-03-13
> 基準程式碼分支：`refactor/restructure`
> 模組路徑：`accrual_bot/ui/`（24 個 Python 檔案，共 2544 行）

---

## 目錄

1. [背景](#1-背景)
2. [用途](#2-用途)
3. [設計思路](#3-設計思路)
4. [各項知識點](#4-各項知識點)
   - 4.1 Session State 管理機制
   - 4.2 Dual-Layer Pages 架構
   - 4.3 Button-as-Radio 模式與 Cascade Reset
   - 4.4 Async/Sync 橋接機制（AsyncBridge）
   - 4.5 Tuple 鍵值設計（REQUIRED_FILES / OPTIONAL_FILES）
   - 4.6 _enrich_file_paths 路徑參數注入
   - 4.7 導航守衛（Navigation Guard）
   - 4.8 進度回報機制
   - 4.9 多格式匯出設計
   - 4.10 Checkpoint 管理頁設計
5. [應用範例](#5-應用範例)
6. [優缺分析](#6-優缺分析)
7. [已識別的設計問題與改進建議](#7-已識別的設計問題與改進建議)
8. [延伸議題](#8-延伸議題)
9. [其他](#9-其他)

---

## 1. 背景

Accrual Bot 是一個處理 PO/PR（採購單/請購單）月結應計（Accrual）資料的批次資料處理系統，主要用於 SPT、SPX 和 SCT 三個事業體的財務資料調節。原始系統僅支援 CLI（`main_pipeline.py`）執行，每次修改參數或切換 entity 都需要手動改程式碼或設定檔，對非技術人員極不友善。

2026 年 1 月起，系統新增了 Streamlit Web UI（`main_streamlit.py`），以視覺化操作介面取代命令列，讓財務人員可以自助完成整個 pipeline 的設定與執行。UI 層被設計為獨立模組 `accrual_bot/ui/`，與核心的 `accrual_bot/core/` 和 `accrual_bot/tasks/` 完全解耦，對外只透過 `UnifiedPipelineService` 這一個門面（Facade）溝通。

整個 UI 模組共 24 個 Python 檔案，約 2544 行，分為五個子模組：`models/`、`components/`、`services/`、`pages/`、`utils/`。

---

## 2. 用途

UI 模組提供一個五步驟的有導引工作流程：

```
Page 1: 配置   → 選擇 Entity (SPT/SPX/SCT)、Type (PO/PR/PPE/…)、處理日期
Page 2: 上傳   → 上傳所需/選填檔案，支援 CSV/XLSX/XLS
Page 3: 執行   → 啟動 pipeline，顯示步驟進度與日誌
Page 4: 結果   → 預覽輸出資料，下載 CSV / Excel
Page 5: Checkpoint → 管理已儲存的 pipeline 中斷點（列表、刪除；載入尚未完成）
```

**目標使用者**：財務/會計人員，不需要了解 Python 程式碼細節，只要透過瀏覽器即可完成月結應計資料的處理。

**技術目標**：
- 讓後端 pipeline 邏輯完全不感知 UI 層的存在
- 讓 UI 層可以在不修改後端的情況下切換 entity/type

---

## 3. 設計思路

### 3.1 整體分層原則

UI 模組的設計遵循「由外而內」的依賴方向：

```
pages/            ← 頁面（依賴 components、services、app）
components/       ← 可複用 UI 元件（依賴 services、config、models）
services/         ← 服務層（依賴 tasks 的 Orchestrators、core 的 Pipeline）
models/           ← 資料模型（無外部依賴）
utils/            ← 工具函數（無外部依賴）
config.py         ← 靜態常數（無外部依賴）
app.py            ← Session State 初始化（依賴 models）
```

pages 不直接呼叫 `tasks/` 或 `core/`，所有對後端的呼叫都必須經過 `services/` 層。這符合 Facade 設計模式的精神：`UnifiedPipelineService` 是 UI 通往整個後端世界的唯一入口。

### 3.2 無狀態渲染原則

Streamlit 的執行模型是每次使用者操作都重新執行整個頁面腳本，因此所有跨 rerun 需要保存的狀態必須存在 `st.session_state` 中。UI 模組透過四個 dataclass 模型（`PipelineConfig`、`FileUploadState`、`ExecutionState`、`ResultState`）結構化地管理所有狀態，而非在 session_state 中散落各種 key。

### 3.3 配置驅動而非硬編碼

entity 清單、各 entity 支援的處理類型、各組合需要的檔案，全部集中在 `ui/config.py` 的常數字典中。新增或移除一個處理類型，只需修改 config.py（以及對應的 orchestrator），不需動頁面元件或服務層。

### 3.4 Async 後端、Sync 前端的橋接策略

Streamlit 是同步框架，但 pipeline 使用 `async/await` 撰寫。`AsyncBridge` 透過建立獨立 thread + 新 event loop 的方式，將 async coroutine 包裝成同步呼叫，讓 Streamlit 可以直接使用。

---

## 4. 各項知識點

### 4.1 Session State 管理機制

**檔案：`accrual_bot/ui/app.py`（第 17–50 行）**

Streamlit 的 `st.session_state` 是一個 dict-like 物件，在同一個瀏覽器 session 中跨 rerun 保持資料。但直接在 session_state 中散落字串 key（如 `st.session_state['entity'] = 'SPT'`）容易導致型別不安全、key 拼錯難以追蹤的問題。

UI 模組採用「**結構化 dataclass 作為 session_state 的 Value**」策略：

```python
# app.py 第 17–41 行
def init_session_state():
    if 'pipeline_config' not in st.session_state:
        st.session_state.pipeline_config = PipelineConfig()
    if 'file_upload' not in st.session_state:
        st.session_state.file_upload = FileUploadState()
    if 'execution' not in st.session_state:
        st.session_state.execution = ExecutionState()
    if 'result' not in st.session_state:
        st.session_state.result = ResultState()
```

**為什麼用 `if key not in` 而非直接賦值？**
Streamlit rerun 時整個腳本重新執行，若直接賦值（`st.session_state.pipeline_config = PipelineConfig()`），每次 rerun 都會重置狀態，使用者的選擇會消失。`if 'key' not in` 的 idempotent 寫法確保只在首次進入時初始化。

**四個狀態 dataclass 的職責切割：**

| 狀態類別 | 檔案 | 代表的生命週期 |
|---------|------|--------------|
| `PipelineConfig` | `state_models.py` 第 22–28 行 | 使用者在 Page 1 的選擇 |
| `FileUploadState` | `state_models.py` 第 31–37 行 | 使用者在 Page 2 上傳的檔案 |
| `ExecutionState` | `state_models.py` 第 40–51 行 | Page 3 的執行過程與日誌 |
| `ResultState` | `state_models.py` 第 54–62 行 | Page 4 的輸出結果 |

**reset_session_state 的設計**（`app.py` 第 44–50 行）：
透過重新賦值（而非 `if key not in`），強制清空所有狀態，回到初始。這個函數在「重新配置」和「重新執行」按鈕被按下時呼叫：

```python
# app.py 第 44–50 行
def reset_session_state():
    st.session_state.pipeline_config = PipelineConfig()
    st.session_state.file_upload = FileUploadState()
    st.session_state.execution = ExecutionState()
    st.session_state.result = ResultState()
    st.session_state.temp_dir = None
```

注意：`reset_session_state` 不重置 `current_page`，也不重置 `file_handler`，這意味著暫存目錄不會被自動清理（詳見[問題 12](#問題12)）。

**get_navigation_status 的設計**（`app.py` 第 53–70 行）：
這個函數是 UI 的「守門員」，根據當前 session state 的狀態決定各頁面是否可進入：

```python
# app.py 第 64–70 行
return {
    'configuration': True,                        # 永遠可進入
    'file_upload': bool(config.entity and config.processing_type),  # 需選完 entity+type
    'execution': upload.required_files_complete,  # 需上傳完必填檔案
    'results': execution.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED],
    'checkpoint': True,                           # 永遠可進入
}
```

這個設計有一個重要的副作用：即使執行失敗（`FAILED`），Page 4（結果頁）也是可進入的，可以查看錯誤詳情。這是刻意的設計選擇，讓使用者即使在失敗情況下也能看到部分結果或完整的錯誤訊息。

---

### 4.2 Dual-Layer Pages 架構

**相關檔案：**
- `pages/1_⚙️_配置.py`（17 行，進入點）
- `accrual_bot/ui/pages/1_configuration.py`（80 行，業務邏輯）

**問題根源：**
Streamlit multi-page app 需要在 `pages/` 目錄下放置有 emoji 檔名的 Python 檔案才能出現在側邊欄導航。然而，emoji 字元在跨平台（尤其是 Windows）的 git 操作中容易造成問題，在 `sys.path` 中難以 import，且不符合 Python 模組命名規範（Python 模組名稱不能包含 emoji）。

**解決方案：兩層架構 + exec()**

```
pages/1_⚙️_配置.py       ← Streamlit 要求的 emoji 檔名（進入點，17 行）
    ↓ exec()
accrual_bot/ui/pages/1_configuration.py  ← 實際業務邏輯（標準 Python 檔名，80 行）
```

進入點的完整程式碼（`pages/1_⚙️_配置.py`）：

```python
# pages/1_⚙️_配置.py 第 1–17 行（全文）
"""
Configuration Page - Streamlit Entry Point
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

actual_page = project_root / "accrual_bot" / "ui" / "pages" / "1_configuration.py"
exec(open(actual_page, encoding='utf-8').read())
```

**為什麼用 `exec()` 而非 `import`？**

這是此架構最關鍵的技術選擇。`exec()` 讓目標程式碼在**當前命名空間**（`__main__` 的全域作用域）中執行，而 `import` 會建立新的模組命名空間。

Streamlit 的工作原理依賴於偵測在特定「魔法命名空間」中執行的 `st.xxx()` 呼叫。當 Streamlit 執行 `pages/1_⚙️_配置.py` 時，它在自己管理的命名空間中運行。如果用 `import`，`accrual_bot.ui.pages.1_configuration` 模組會在獨立的模組命名空間中執行，Streamlit 的 widget 追蹤機制（widget key registry）無法正確運作，會導致 widget 重複執行、session state 不一致等問題。

`exec()` 消除了命名空間邊界，讓被執行的程式碼「感覺」自己就是 `pages/1_⚙️_配置.py` 本身在執行，Streamlit context 完全保留。

**st.switch_page() 的路徑要求**

由於 Streamlit multi-page 系統識別的是 emoji 檔案，所有頁面跳轉必須使用 emoji 路徑：

```python
# 正確（使用 emoji 進入點路徑）
st.switch_page("pages/3_▶️_執行.py")

# 錯誤（Streamlit 找不到此頁面）
st.switch_page("pages/3_execution.py")
```

每個 `accrual_bot/ui/pages/*.py` 中的頁面跳轉都遵守這個規則。例如 `2_file_upload.py` 第 37 行：`st.switch_page("pages/1_⚙️_配置.py")`。

**架構的代價**：
每個 `accrual_bot/ui/pages/*.py` 頁面的頂部都有相同的 `sys.path.insert` 語句（例如 `3_execution.py` 第 12–14 行、`4_results.py` 第 12–14 行），這是 DRY 原則的違反，但由於 `exec()` 帶來的命名空間特性，目前沒有簡單的方法消除（詳見[問題 11](#問題11)）。

---

### 4.3 Button-as-Radio 模式與 Cascade Reset

**檔案：`accrual_bot/ui/components/entity_selector.py`（第 13–181 行）**

Streamlit 提供 `st.radio()` 元件，但其視覺設計固定（垂直排列或水平排列的文字選項），無法顯示大的圖示按鈕或自訂描述文字。因此 UI 採用「**Button-as-Radio**」模式：用一排 `st.button()` 模擬 radio 的單選行為，被選中的按鈕顯示為 `type="primary"`（藍色），未選中的為 `type="secondary"`（灰色）。

```python
# entity_selector.py 第 30–64 行
for idx, entity in enumerate(entities):
    config = ENTITY_CONFIG[entity]
    with cols[idx]:
        button_type = "primary" if selected_entity == entity else "secondary"
        if st.button(
            f"{config['icon']} {config['display_name']}",
            key=f"entity_{entity}",
            type=button_type,
            use_container_width=True
        ):
            st.session_state.pipeline_config.entity = entity
            # ... cascade reset ...
            st.rerun()
        st.caption(config['description'])
```

**button_type 的視覺回饋**：
`selected_entity == entity` 的比較發生在 render 時（每次 rerun），而按鈕的點擊則寫入 session_state 並觸發 `st.rerun()`。下一次 rerun 時，比較結果會不同，按鈕顏色隨之改變。這是 Streamlit 的典型「先寫狀態、再 rerun、再渲染」模式。

**Cascade Reset（層級重置）**：
Entity → ProcessingType → SourceType 是三層從屬關係。更改上層選擇時，必須清除所有下層狀態，否則會出現「顯示 SPT 的步驟，但 entity 已改為 SPX」的矛盾狀態。`render_entity_selector()` 在按鈕被按下後執行 10 行重置：

```python
# entity_selector.py 第 42–60 行
st.session_state.pipeline_config.entity = entity
st.session_state.pipeline_config.processing_type = ""
st.session_state.pipeline_config.procurement_source_type = ""
st.session_state.pipeline_config.enabled_steps = []
st.session_state.file_upload.file_paths = {}
st.session_state.file_upload.uploaded_files = {}
st.session_state.file_upload.validation_errors = []
st.session_state.file_upload.required_files_complete = False
from accrual_bot.ui.models.state_models import ExecutionStatus
st.session_state.execution.status = ExecutionStatus.IDLE
st.session_state.execution.current_step = ""
st.session_state.execution.completed_steps = []
st.session_state.execution.failed_steps = []
st.session_state.execution.logs = []
st.session_state.execution.error_message = ""
```

**重要問題**：這段完全相同的 10 行重置程式碼在 `entity_selector.py` 中出現三次：
- `render_entity_selector()` 第 42–60 行
- `render_processing_type_selector()` 第 104–121 行
- `render_procurement_source_type_selector()` 第 159–175 行

這是明顯的 DRY 違反，應該抽取為 `_reset_downstream_states()` helper function（詳見[問題 5](#問題5)）。

**另一個細節**：`from accrual_bot.ui.models.state_models import ExecutionStatus` 在函數體內被 import，而非在模組頂端。這是因為 entity_selector.py 在頂部只導入 `UnifiedPipelineService` 和 config，沒有導入 `state_models`。這個 inline import 是個「補救措施」，正常的做法是在模組頂部統一 import。

---

### 4.4 Async/Sync 橋接機制（AsyncBridge）

**檔案：`accrual_bot/ui/utils/async_bridge.py`（第 12–94 行）**

這是整個 UI 模組技術複雜度最高的部分。

**問題背景**：
Pipeline 後端使用 `async/await`（`pipeline.execute()` 是 coroutine），但 Streamlit 是完全同步的框架，沒有內建的 event loop。在 Streamlit 中直接呼叫 `asyncio.run()` 會遭遇「cannot run nested event loops」的錯誤，因為 Streamlit 的某些版本本身在特定環境下可能已有 event loop 運行中。

**解決方案：Thread + 獨立 Event Loop**

```python
# async_bridge.py 第 15–62 行
@staticmethod
def run_async(coro) -> Any:
    import threading
    import queue

    result_queue = queue.Queue()
    exception_queue = queue.Queue()

    def run_in_thread():
        try:
            new_loop = asyncio.new_event_loop()   # 建立全新的 event loop
            asyncio.set_event_loop(new_loop)       # 設定為當前 thread 的 loop
            try:
                result = new_loop.run_until_complete(coro)
                result_queue.put(result)
            finally:
                new_loop.close()                   # 確保 loop 被關閉
        except Exception as e:
            exception_queue.put(e)

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    thread.join(timeout=300)   # 最多等待 5 分鐘

    if not exception_queue.empty():
        raise exception_queue.get()
    if thread.is_alive():
        raise TimeoutError("Pipeline 執行超時（5分鐘）")
    if not result_queue.empty():
        return result_queue.get()
    else:
        raise RuntimeError("Pipeline 執行失敗：無返回結果")
```

**為什麼建立新的 thread？**

Python 的 `asyncio` event loop 是 per-thread 的。在新 thread 中建立新 loop，完全隔離於 Streamlit 主 thread 的任何 event loop，避免 loop 衝突。`daemon=True` 確保主程式退出時 thread 不會阻塞關閉。

**為什麼用 queue 而非 shared variable？**

`threading.Thread` 的子 thread 中的 exception 不會自動傳播到父 thread。使用兩個 `queue.Queue()`（`result_queue` 和 `exception_queue`）是標準的跨 thread 通訊模式：子 thread 將結果或異常 `put()` 進 queue，主 thread `get()` 取出並處理。

**5 分鐘超時的意義**：
`thread.join(timeout=300)` 在 300 秒後無論如何返回（不會真正中止子 thread）。如果 `thread.is_alive()` 仍為 True，拋出 `TimeoutError`。但這個超時是**單方面的**：Streamlit 主 thread 會繼續執行（顯示超時錯誤），但子 thread 中的 pipeline 仍在後台執行，直到 `daemon=True` 的 thread 被 GC 回收。這意味著「超時後的 pipeline」仍可能佔用 CPU 和記憶體（詳見[問題 1](#問題1)）。

**`run_in_thread` 方法**（第 64–94 行）是 `run_async` 的非阻塞版本，可以傳入 callback 和 error_callback，但目前 UI 沒有使用這個方法——所有執行都走 `run_async`（阻塞版本）。這是因為 Streamlit 的 UI 更新機制需要主 thread 主導，在子 thread 中呼叫 `st.xxx()` 是不支援的。

---

### 4.5 Tuple 鍵值設計（REQUIRED_FILES / OPTIONAL_FILES）

**檔案：`accrual_bot/ui/config.py`（第 93–160 行）**

UI 需要為不同的 entity/type 組合定義不同的必填/選填檔案清單。一個簡單的做法是巢狀 dict：`{'SPT': {'PO': ['raw_po']}}` 但這在有三層（entity/type/source_type）的情況下會變成三層巢狀，查詢時也需要多次 `.get()`。

改用 **Tuple 作為字典鍵**，利用 Python tuple 的可哈希性：

```python
# config.py 第 93–105 行
REQUIRED_FILES: Dict[Tuple, List[str]] = {
    # 2-tuple：標準處理類型
    ('SPT', 'PO'): ['raw_po'],
    ('SPT', 'PR'): ['raw_pr'],
    ('SPX', 'PO'): ['raw_po'],
    ('SPX', 'PPE'): ['contract_filing_list'],
    ('SPX', 'PPE_DESC'): ['workpaper', 'contract_periods'],
    # 3-tuple：PROCUREMENT 子類型
    ('SPT', 'PROCUREMENT', 'PO'): ['raw_po'],
    ('SPT', 'PROCUREMENT', 'PR'): ['raw_pr'],
    # TODO: ('SPT', 'PROCUREMENT', 'COMBINED') 尚未啟用
}
```

**為什麼可以混用 2-tuple 和 3-tuple？**
Python dict 的 key 只要求 hashable，不要求同型別或同長度。`('SPT', 'PO')` 和 `('SPT', 'PROCUREMENT', 'PO')` 是不同的 tuple，hash 值不同，可以共存於同一個 dict。

查詢時透過 `get_file_requirements()` 抽象化路由邏輯：

```python
# config.py 第 187–206 行
def get_file_requirements(entity, proc_type, source_type=""):
    if proc_type == 'PROCUREMENT' and source_type:
        key = (entity, proc_type, source_type)   # 3-tuple
    else:
        key = (entity, proc_type)                 # 2-tuple
    required = REQUIRED_FILES.get(key, [])
    optional = OPTIONAL_FILES.get(key, [])
    return required, optional
```

這樣呼叫者不需要知道 dict 內部使用幾元 tuple，只需傳入三個參數即可。`.get(key, [])` 的預設值 `[]` 確保未定義的組合返回空清單而非 KeyError。

**設計的侷限性**：
PROCUREMENT/COMBINED 目前被 TODO 註解掉，一旦啟用，就需要同時在 `REQUIRED_FILES`、`OPTIONAL_FILES`、`ENTITY_CONFIG`（其實不需要，因為 ENTITY_CONFIG 沒有列 source_type）等多處新增，分散的修改點仍是維護的痛點。

---

### 4.6 `_enrich_file_paths` 路徑參數注入

**檔案：`accrual_bot/ui/services/unified_pipeline_service.py`（第 149–213 行）**

**問題背景**：
Pipeline 的 DataLoading steps 在讀取檔案時，不只需要檔案路徑，還需要讀取參數（如 `encoding='utf-8'`、`sheet_name=0`、`header=3`、`usecols='A:AH'`）。這些參數存放在 `config/paths.toml` 的 `[entity.proc_type.params]` 區段。

UI 的 FileHandler 儲存的是純字串路徑（`{'raw_po': '/tmp/accrual_bot_ui_xxx/raw_po_file.csv'}`），但 pipeline 期望的格式可能是含 params 的字典（`{'raw_po': {'path': '...', 'params': {'encoding': 'utf-8'}}}`）。

**_enrich_file_paths 的職責**：
在 UI 的 `build_pipeline()` 呼叫 orchestrator 之前，將純路徑字串「注入」對應的讀取參數，轉換為 pipeline 期望的格式：

```python
# unified_pipeline_service.py 第 149–213 行
def _enrich_file_paths(self, file_paths, entity, proc_type, source_type=None):
    """
    Input:  {'ops_validation': '/path/to/file.xlsx'}
    Output: {'ops_validation': {'path': '/path/to/file.xlsx',
                                 'params': {'sheet_name': '智取櫃驗收明細',
                                            'header': 3, 'usecols': 'A:AH'}}}
    """
    try:
        config_manager = ConfigManager()
        params_config = config_manager.get_paths_config(
            entity.lower(), proc_type.lower(), 'params'
        )
        if params_config and isinstance(params_config, dict):
            enriched_paths = {}
            for file_key, file_path in file_paths.items():
                if file_key in params_config:
                    enriched_paths[file_key] = {
                        'path': file_path,
                        'params': params_config[file_key]
                    }
                elif source_type and f"{file_key}_{source_type.lower()}" in params_config:
                    # PROCUREMENT 後綴 fallback
                    suffixed_key = f"{file_key}_{source_type.lower()}"
                    enriched_paths[file_key] = {
                        'path': file_path,
                        'params': params_config[suffixed_key]
                    }
                else:
                    enriched_paths[file_key] = file_path   # 無 params，原樣保留
            return enriched_paths
    except Exception as e:
        print(f"[ERROR] Failed to enrich file_paths: {e}")
        traceback.print_exc()
    return file_paths   # exception fallback：回傳原始 file_paths
```

**兩層 fallback 的設計意圖**：
1. 首先嘗試精確匹配 `file_key`（如 `raw_po`）
2. 若無，且有 `source_type`，嘗試加後綴（如 `raw_po_po`）—— 這是 PROCUREMENT 模式的特殊需求，因為 paths.toml 中 PROCUREMENT 的參數可能按 source_type 區分命名
3. 若仍無 params，保留原始字串路徑（兼容沒有特殊 params 的檔案）
4. 整個函數用 try/except 包裹，任何例外都 fallback 到原始 file_paths，確保不因 paths.toml 讀取失敗而阻斷 pipeline 執行

**副作用**：
`print()` 而非使用 logging 框架（第 210 行）。在 production 環境中應改用 `get_logger()`。

---

### 4.7 導航守衛（Navigation Guard）

**相關檔案：`accrual_bot/ui/pages/2_file_upload.py`（第 33–38 行）、`3_execution.py`（第 36–41 行）、`4_results.py`（第 35–40 行）**

為了防止使用者跳過步驟（例如直接訪問 Page 3 但尚未上傳檔案），每個有前置條件的頁面都在頂部設置導航守衛：

```python
# 2_file_upload.py 第 33–38 行
nav_status = get_navigation_status()
if not nav_status['file_upload']:
    st.warning("⚠️ 請先完成配置頁設定")
    if st.button("前往配置頁"):
        st.switch_page("pages/1_⚙️_配置.py")
    st.stop()   # 關鍵：阻止頁面其餘部分執行
```

`st.stop()` 是 Streamlit 提供的「立即停止腳本執行」指令，效果等同於 `return` 但在腳本頂層作用域中有效。它確保條件不符時，使用者只看到警告訊息，而不是一個「功能部分缺失」的破碎頁面。

**守衛邏輯摘要**：

| 頁面 | 守衛條件 | 違反時跳轉 |
|------|---------|-----------|
| Page 2 | `entity` 且 `processing_type` 已設定 | Page 1 |
| Page 3 | `required_files_complete == True` | Page 2 |
| Page 4 | `execution.status` 為 COMPLETED 或 FAILED | Page 3 |
| Page 1, 5 | 無守衛（永遠可進入） | — |

---

### 4.8 進度回報機制

**相關檔案：`accrual_bot/ui/services/pipeline_runner.py`（第 131–161 行）、`accrual_bot/ui/components/progress_tracker.py`（第 13–71 行）**

這是 UI 中設計與現實差距最大的部分，需要深入理解。

**設計意圖**：
讓使用者在 pipeline 執行期間看到每個步驟的進度（正在執行第幾步，哪些步驟已完成）。

**實際行為（pipeline_runner.py 第 131–161 行）**：

```python
# pipeline_runner.py 第 143–160 行
async def _execute_with_progress(self, pipeline, context):
    total_steps = len(pipeline.steps)

    # 步驟 1：預先批量記錄所有步驟名稱到日誌
    # 這不是「即時進度」，而是在執行前一次性輸出所有步驟
    for idx, step in enumerate(pipeline.steps, start=1):
        self._log(f"[{idx}/{total_steps}] 執行步驟: {step.name}")

    # 步驟 2：一次性執行全部步驟（blocking call）
    result = await pipeline.execute(context)

    # 步驟 3：執行完成後，根據結果批量更新進度
    if self.progress_callback and 'results' in result:
        for idx, step_result in enumerate(result['results'], start=1):
            step_name = step_result.get('step', f'Step {idx}')
            is_success = step_result.get('status') == 'success'
            status = 'completed' if is_success else 'failed'
            self.progress_callback(step_name, idx, total_steps, status=status)
```

**問題的本質**：
`pipeline.execute(context)` 是一個將所有步驟作為整體執行的 coroutine，沒有 step-level 的 yield 或 callback hook。因此無法在中途回報進度。目前的「進度」資訊是在執行**完成後**才批量填入，而非即時的。

更嚴重的是：由於 `AsyncBridge.run_async()` 在 Streamlit 主 thread 中阻塞等待，Streamlit UI 在 `thread.join(timeout=300)` 期間是**凍結**的。即便 log_callback 在子 thread 中呼叫（往 `execution.logs` append），Streamlit UI 也不會因此刷新，使用者在整個 pipeline 執行期間看到的是靜止的頁面，直到執行完成（或超時）才會更新。

**progress_tracker 的預估時間計算**（`progress_tracker.py` 第 66–71 行）：

```python
if start_time and current_idx > 0 and current_idx < total_steps:
    elapsed = time.time() - start_time
    avg_time_per_step = elapsed / current_idx
    remaining_steps = total_steps - current_idx
    estimated_remaining = avg_time_per_step * remaining_steps
    st.caption(f"⏳ 預估剩餘時間: {format_duration(estimated_remaining)}")
```

這個算法基於「平均步驟時間」線性推算，是合理的近似值。但由於進度更新是批量的（執行完成後一次性），這個計算在執行過程中永遠不會更新（current_idx 不變）。只有在執行完成後刷新頁面才能看到最終結果。

---

### 4.9 多格式匯出設計

**相關檔案：`accrual_bot/ui/pages/4_results.py`（第 98–122 行）、`accrual_bot/ui/components/data_preview.py`（第 78–85 行）**

**CSV 下載的 BOM 編碼**：

```python
# 4_results.py 第 99 行
csv_data = result.output_data.to_csv(index=False).encode('utf-8-sig')
```

`utf-8-sig` 是帶有 BOM（Byte Order Mark，`\xEF\xBB\xBF`）的 UTF-8 編碼。Microsoft Excel 在開啟 CSV 時依賴 BOM 判斷編碼，若沒有 BOM，Excel 預設以系統語系（Windows 通常是 CP950 / Big5）解讀，導致中文字元亂碼。使用 `utf-8-sig` 確保下載的 CSV 在 Excel 中正確顯示中文。

**Excel 下載的 in-memory BytesIO**：

```python
# 4_results.py 第 110–122 行
from io import BytesIO
excel_buffer = BytesIO()
with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
    result.output_data.to_excel(writer, index=False, sheet_name='Output')
excel_buffer.seek(0)   # 重置讀取位置到檔案開頭

st.download_button(
    data=excel_buffer,
    file_name=f"{config.entity}_{config.processing_type}_{config.processing_date}_output.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ...
)
```

`BytesIO` 是記憶體中的檔案物件，避免在磁碟上寫入臨時檔案。`excel_buffer.seek(0)` 是必要的：`pd.ExcelWriter` 寫入後，指標位於檔案末尾，若不 seek(0)，`st.download_button` 讀取的將是空資料。

`engine='openpyxl'` 明確指定 Excel 引擎，避免在沒有 xlsxwriter 的環境下出現 engine-not-found 錯誤。

**data_preview 的 utf-8-sig 一致性**（`data_preview.py` 第 81 行）：

```python
data=data[selected_columns].to_csv(index=False).encode('utf-8-sig'),
```

預覽元件中的下載按鈕也使用 `utf-8-sig`，與結果頁保持一致。

**檔案命名規則**：
所有下載檔案都以 `{entity}_{processing_type}_{processing_date}` 為前綴（例如 `SPX_PO_202512_output.csv`），確保多次下載的檔案在同一目錄中不會互相覆蓋，同時易於辨識月份。

---

### 4.10 Checkpoint 管理頁設計

**檔案：`accrual_bot/ui/pages/5_checkpoint.py`（第 1–142 行）**

**目前功能（可用）**：
- 掃描 `checkpoints/` 目錄下的 `.pkl` 和 `.json` 檔案
- 顯示名稱、路徑、修改時間、大小
- 個別刪除
- 批次清空（帶二次確認）
- 統計總大小

**二次確認的實作模式**（第 109–126 行）：
這是一個值得注意的 UX 設計。Streamlit 按鈕沒有原生的 confirmation dialog，因此採用「**雙擊確認**」模式：

```python
# 5_checkpoint.py 第 109–126 行
if st.button("🗑️ 清空所有 Checkpoint"):
    if st.session_state.get('confirm_delete_all', False):
        # 第二次點擊：執行刪除
        deleted_count = 0
        for checkpoint in checkpoint_files:
            os.remove(checkpoint['path'])
            deleted_count += 1
        st.success(f"已刪除 {deleted_count} 個 checkpoint")
        st.session_state.confirm_delete_all = False
        st.rerun()
    else:
        # 第一次點擊：設定確認旗標，顯示警告
        st.session_state.confirm_delete_all = True
        st.warning("⚠️ 再次點擊確認刪除所有 checkpoint")
```

第一次點擊後，`confirm_delete_all = True` 寫入 session_state，頁面刷新顯示警告。第二次點擊時，條件成立，執行刪除。這個模式的唯一缺點是：如果使用者第一次點擊後離開頁面，`confirm_delete_all` 仍保留在 session_state 中，下次回到這頁時「第一次點擊」就會直接刪除（因為旗標已是 True）。

**載入功能的 TODO**（第 98–100 行）：

```python
if st.button("▶️ 載入", key=f"load_{idx}"):
    st.info("⚠️ 從 checkpoint 繼續執行的功能尚未實作")
    # TODO: 實作從 checkpoint 繼續執行的邏輯
```

後端的 `CheckpointManager` 已實作儲存和載入（`core/pipeline/checkpoint.py`），但 UI 層尚未串接。這個 gap 使 Page 5 的核心功能（從中斷點繼續）目前無法使用。

---

## 5. 應用範例

### 範例 1：新增一個新的 Entity Type 到 UI

假設要為 SPX 新增 `INV`（Invoice）處理類型：

**步驟 1**：在 `accrual_bot/ui/config.py` 新增設定

```python
# config.py — ENTITY_CONFIG
'SPX': {
    'types': ['PO', 'PR', 'PPE', 'PPE_DESC', 'INV'],  # 新增 'INV'
    ...
}

# config.py — PROCESSING_TYPE_CONFIG
'INV': {
    'display_name': '發票處理 (INV)',
    'description': 'Invoice 處理流程',
    'icon': '🧾',
}

# config.py — REQUIRED_FILES
('SPX', 'INV'): ['raw_invoice'],

# config.py — OPTIONAL_FILES
('SPX', 'INV'): ['previous_invoice'],

# config.py — FILE_LABELS
'raw_invoice': '發票原始資料 (必填)',
```

**步驟 2**：在 `config/paths.toml` 新增路徑設定

```toml
[spx.inv]
raw_invoice = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_invoice_*.csv"

[spx.inv.params]
raw_invoice = { encoding = "utf-8", sep = "," }
```

**步驟 3**：在 `config/stagging_spx.toml` 新增步驟設定

```toml
[pipeline.spx]
enabled_inv_steps = ["SPXInvDataLoading", "SPXInvProcessing", "SPXInvExport"]
```

**步驟 4**：在 `tasks/spx/pipeline_orchestrator.py` 新增 `build_inv_pipeline()` 方法

**步驟 5**：在 `ui/services/unified_pipeline_service.py` 的 `build_pipeline()` 新增 elif 分支

完成後，不需要修改任何頁面元件（`entity_selector.py`、`file_uploader.py` 等），UI 會自動根據更新的設定顯示新選項。

---

### 範例 2：理解頁面間的資料流

完整的一次執行週期中，各模組的作用：

```
1. Page 1（配置）
   render_entity_selector()
     → 寫入 st.session_state.pipeline_config.entity
   render_step_preview()
     → 呼叫 UnifiedPipelineService().get_enabled_steps()
     → 寫入 st.session_state.pipeline_config.enabled_steps

2. Page 2（上傳）
   FileHandler.save_uploaded_file()
     → 寫入 /tmp/accrual_bot_ui_xxx/raw_po_file.csv
   _render_validation_summary()
     → 寫入 st.session_state.file_upload.required_files_complete = True

3. Page 3（執行）
   AsyncBridge.run_async(runner.execute(...))
     → 新 thread + 新 event loop
     → UnifiedPipelineService.build_pipeline()
         → _enrich_file_paths()  ← 注入 params
         → SPXPipelineOrchestrator().build_po_pipeline() / SCTPipelineOrchestrator().build_po_pipeline()
         → Pipeline 物件
     → pipeline.execute(context)
     → 結果寫入 st.session_state.result

4. Page 4（結果）
   讀取 st.session_state.result.output_data
   render_data_preview() → 顯示 DataFrame
   st.download_button(..., data=df.to_csv().encode('utf-8-sig'))
```

---

## 6. 優缺分析

### 優點

**1. 關注點分離（Separation of Concerns）徹底**
`pages/` 只做頁面渲染，`components/` 只做 UI 元件，`services/` 只做業務協調，`models/` 只做資料結構。新增功能時，每個層級的修改範圍清晰。例如，修改進度條的顯示方式只需改 `progress_tracker.py`，完全不影響 `pipeline_runner.py` 或頁面。

**2. Session State 結構化管理**
使用 dataclass 而非散落的 string key，讓 IDE 可以進行型別推斷和自動補全（`st.session_state.pipeline_config.entity` vs `st.session_state['entity']`），大幅降低 key 拼錯的風險。

**3. 配置驅動擴充性**
新增 entity/type 的工作量集中在設定檔和 orchestrator，不需要修改頁面 template code，符合「開閉原則」（Open/Closed Principle）。

**4. Dual-Layer Pages 的聰明解法**
用 `exec()` 解決 Streamlit emoji 檔名限制，讓業務邏輯保持在標準 Python 檔名中，兼顧 Streamlit 要求和程式碼可維護性。

**5. 錯誤邊界完整**
`AsyncBridge`、`_enrich_file_paths`、`_execute_with_progress` 都有 try/except，確保後端崩潰時 UI 能顯示錯誤訊息而非直接 crash。`get()` 加預設值的使用（如 `REQUIRED_FILES.get(key, [])`）也確保未定義組合的優雅降級。

**6. 匯出的細節品質**
`utf-8-sig` 編碼確保 Excel 開啟 CSV 不亂碼，`BytesIO` 避免磁碟 I/O，`engine='openpyxl'` 明確指定避免環境依賴問題。

### 缺點

**1. 進度回報名不副實**
雖然 UI 有進度條和步驟狀態表，但由於 `AsyncBridge.run_async()` 阻塞 Streamlit 主 thread，整個執行期間 UI 是凍結的，使用者無法看到即時的步驟更新。

**2. Stop button 是個假按鈕**
`pages/3_execution.py` 第 63–67 行有停止按鈕，但 `AsyncBridge.run_async()` 是阻塞呼叫，在 pipeline 完成前 Streamlit 不會處理任何使用者輸入，stop_button 永遠不會被觸發。

**3. DRY 違反**
Cascade reset 邏輯重複三次（`entity_selector.py` 約 200 行中佔了 45 行），`sys.path.insert` 在五個頁面檔案中各重複一次。

**4. 未完成的功能**
Checkpoint 載入功能（Page 5）只有 TODO，但 UI 卻展示了這個頁面，可能讓使用者以為這是可用功能。`ExecutionStatus.PAUSED` 定義了但沒有任何程式碼設定它。`ResultState.checkpoint_path` 定義了但從未被設定。

**5. 服務層每次渲染重新實例化**
`entity_selector.py` 第 22 行 `service = UnifiedPipelineService()` 在 render 函數內部，每次 Streamlit rerun 都建立新實例。`step_preview.py` 第 30 行同樣。`UnifiedPipelineService` 是輕量物件（無狀態），重建成本低，但如果未來加入快取或連接池，這個模式會成為瓶頸。

**6. 安全性不完整**
`file_handler.py` 的 `_sanitize_filename` 只處理 `/`、`\` 和 `..`，Windows 的 `\x00`、`|`、`>`、`<`、`?`、`*` 等特殊字元未處理。在可公開部署的環境中這是安全隱患，但對內部工具的影響有限。

---

## 7. 已識別的設計問題與改進建議

### 問題 1：Stop button 沒有實際中斷邏輯 {#問題1}

**位置**：`pages/3_execution.py` 第 63–67 行、第 131 行

**問題描述**：
`stop_button = st.button("⏹️ 停止")` 的 `disabled` 條件設為 `execution.status != ExecutionStatus.RUNNING`，看起來邏輯正確。但 `AsyncBridge.run_async()` 在第 131 行是同步阻塞呼叫，在 `thread.join()` 期間 Streamlit 主 thread 被占用，無法響應任何使用者輸入，stop_button 的點擊永遠不會被處理。

**改進建議**：
需要將 pipeline 執行改為非阻塞模式。可考慮：

方案 A：使用 `asyncio.run_coroutine_threadsafe()` + Streamlit 的 `st.rerun()` loop 輪詢狀態，在執行期間定期 rerun 更新 UI。

方案 B：在 pipeline 步驟間插入 cancellation checkpoint（需要修改 `Pipeline.execute()` 支援 `threading.Event` 取消信號）。

方案 C：接受限制，將按鈕隱藏或標示為「執行中無法停止，請等待完成後重置」。

---

### 問題 2：進度無法真正即時 {#問題2}

**位置**：`services/pipeline_runner.py` 第 143–151 行

**問題描述**：
`pipeline.execute(context)` 是一次性執行所有步驟，中途無法插入 step-level callback。進度更新只在全部執行完成後才能發生（第 154–159 行）。

**改進建議**：
核心修改點在 `Pipeline.execute()` 需要支援 `on_step_complete` callback。例如：

```python
# 假設的改良版 Pipeline.execute()
async def execute(self, context, on_step_complete=None):
    for step in self.steps:
        result = await step.execute(context)
        if on_step_complete:
            on_step_complete(step.name, result.status)
```

然後在 `StreamlitPipelineRunner._execute_with_progress()` 中傳入 callback，配合 `asyncio.Queue` 或共享 state 讓主 thread 的輪詢機制讀取進度。

---

### 問題 3：Checkpoint 載入功能未完成 {#問題3}

**位置**：`pages/5_checkpoint.py` 第 98–100 行

**問題描述**：
「載入」按鈕直接顯示 TODO 訊息，功能完全未實作，但 UI 上沒有任何標示說明這是「尚未完成的功能」，可能誤導使用者。

**改進建議**：
短期：在按鈕上加上 `disabled=True` 並加上 `(Coming Soon)` 文字，或整個隱藏這個按鈕。
長期：實作串接 `CheckpointManager.load()` 的邏輯，將 checkpoint 中的 `ProcessingContext` 還原到 session_state，並讓使用者從特定步驟繼續執行。

---

### 問題 4：`ResultState.checkpoint_path` 是孤兒欄位 {#問題4}

**位置**：`models/state_models.py` 第 62 行

**問題描述**：
`checkpoint_path: Optional[str] = None` 定義在模型中，但代碼中沒有任何地方設定它（搜尋整個 `accrual_bot/ui/` 目錄無任何 `checkpoint_path =` 的賦值）。

**改進建議**：
如果 checkpoint 儲存路徑確實需要在 UI 中追蹤，應在 `pages/3_execution.py` 執行成功後設定：
`st.session_state.result.checkpoint_path = result.get('checkpoint_path')`
若確認不需要，應從 dataclass 中移除，避免「死程式碼」引起混淆。

---

### 問題 5：DRY 違反——Cascade Reset 邏輯重複三次 {#問題5}

**位置**：`components/entity_selector.py` 第 42–60 行、第 104–121 行、第 159–175 行

**問題描述**：
完全相同的 10 行重置邏輯出現三次。每次修改重置邏輯（例如新增一個 session state key 需要重置）都要記得改三個地方，極易遺漏。

**改進建議**：
抽取為 `_reset_downstream_states(level='entity')` helper 函數：

```python
def _reset_downstream_states():
    """清除 entity/type 改變時需要重置的所有後續狀態"""
    st.session_state.pipeline_config.processing_type = ""
    st.session_state.pipeline_config.procurement_source_type = ""
    st.session_state.pipeline_config.enabled_steps = []
    st.session_state.file_upload.file_paths = {}
    st.session_state.file_upload.uploaded_files = {}
    st.session_state.file_upload.validation_errors = []
    st.session_state.file_upload.required_files_complete = False
    st.session_state.execution.status = ExecutionStatus.IDLE
    st.session_state.execution.current_step = ""
    st.session_state.execution.completed_steps = []
    st.session_state.execution.failed_steps = []
    st.session_state.execution.logs = []
    st.session_state.execution.error_message = ""
```

---

### 問題 6：`UnifiedPipelineService` 每次渲染重新實例化 {#問題6}

**位置**：`components/entity_selector.py` 第 22 行、第 85 行；`components/step_preview.py` 第 30 行

**問題描述**：
`service = UnifiedPipelineService()` 在 render 函數的頂部。Streamlit 每次 rerun 都重新執行這些函數，每次都建立新的 `SPTPipelineOrchestrator()`、`SPXPipelineOrchestrator()` 和 `SCTPipelineOrchestrator()` 實例（因為 `_get_orchestrator()` 在每次呼叫時都是 `orchestrator_class()`）。

**改進建議**：
使用 `st.cache_resource` 裝飾器快取 service 實例（`@st.cache_resource` 是跨 session 的 singleton），或在 session_state 中初始化一次：

```python
# app.py 的 init_session_state() 中加入
if 'pipeline_service' not in st.session_state:
    st.session_state.pipeline_service = UnifiedPipelineService()
```

---

### 問題 7：`FileHandler.__del__` 是空實作 {#問題7}

**位置**：`services/file_handler.py` 第 152–156 行

**問題描述**：
`__del__` 方法存在但為空（只有 `pass`）。Python 的 `__del__` 本來就不可靠（GC 不保證呼叫時機），且即便實作了也不應在 `__del__` 中執行 I/O 操作（可能已無法安全執行）。更重要的是，`FileHandler` 被儲存在 `st.session_state.file_handler`，Streamlit session 過期時不會觸發 `__del__`，temp 目錄永遠不會被自動清理。

**改進建議**：
確保 `cleanup()` 在明確的生命週期點被呼叫。目前 `2_file_upload.py` 第 73–74 行在「重新配置」按鈕時有呼叫：

```python
if hasattr(file_handler, 'cleanup'):
    file_handler.cleanup()
```

但若使用者直接關閉瀏覽器，這段 cleanup 不會執行。可以考慮加入 Streamlit 的 `on_session_end` callback（若版本支援），或設定 OS 層的定期 temp 目錄清理 cron job。

---

### 問題 8：`_sanitize_filename` 不完整 {#問題8}

**位置**：`services/file_handler.py` 第 136–150 行

**問題描述**：
目前只替換 `/`、`\`、`..`，Windows 系統還有以下保留字元未處理：`\x00`（null byte）、`|`（pipe）、`>`（redirection）、`<`（redirection）、`?`（wildcard）、`*`（wildcard）、`"`（quote）、`:`（冒號，Windows 路徑分隔符）、以及保留名稱（`CON`、`NUL`、`PRN` 等）。

**改進建議**：
```python
import re

def _sanitize_filename(self, filename: str) -> str:
    # 移除路徑相關字元
    filename = filename.replace('/', '_').replace('\\', '_')
    # 移除路徑穿越
    filename = filename.replace('..', '_')
    # 移除 Windows 特殊字元
    filename = re.sub(r'[<>:"|?*\x00]', '_', filename)
    # 限制長度
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200 - len(ext)] + ext
    return filename
```

---

### 問題 9：`ExecutionStatus.PAUSED` 未使用 {#問題9}

**位置**：`models/state_models.py` 第 18 行

**問題描述**：
`PAUSED = "paused"` 定義在 Enum 中，但代碼中從未將狀態設為 `PAUSED`，`get_navigation_status()` 也不包含對 PAUSED 的處理。

**改進建議**：
若不計劃實作暫停功能，移除此值避免閱讀者誤解。若計劃實作，需要在 `get_navigation_status()` 中處理 PAUSED 狀態應如何影響各頁面的可進入性。

---

### 問題 10：`width="stretch"` 已棄用 {#問題10}

**位置**：`components/data_preview.py` 第 72–76 行

**問題描述**：
```python
st.dataframe(
    data[selected_columns].head(display_rows),
    width="stretch",   # ← 此參數在較新版 Streamlit 中已不支援
    height=400
)
```

Streamlit 1.22+ 中 `width` 參數已改為接受整數或 `None`，`"stretch"` 字串值會被忽略或拋出 warning。

**改進建議**：
改用 `use_container_width=True`：

```python
st.dataframe(
    data[selected_columns].head(display_rows),
    use_container_width=True,
    height=400
)
```

---

### 問題 11：各頁面重複的 `sys.path.insert` {#問題11}

**位置**：`pages/1_configuration.py` 第 12–14 行、`pages/2_file_upload.py` 第 12–14 行、`pages/3_execution.py` 第 12–14 行、`pages/4_results.py` 第 12–14 行、`pages/5_checkpoint.py` 第 12–14 行

**問題描述**：
五個頁面各自執行：
```python
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
```

由於 `exec()` 的特性，每個頁面在 exec 時都在獨立的檔案上下文中執行，`__file__` 指向不同的路徑，因此無法用單一 module-level 的 path setup 解決。

**改進建議**：
可以在進入點（`pages/1_⚙️_配置.py`）中設定 sys.path 後再 exec，由於 `exec()` 是在進入點的命名空間執行，sys.path 的修改對被 exec 的程式碼也有效。這樣 `accrual_bot/ui/pages/` 中的頁面檔案就不需要再 `sys.path.insert`。

---

### 問題 12：FileHandler 生命週期管理缺失 {#問題12}

**位置**：`pages/2_file_upload.py` 第 49–54 行

**問題描述**：
```python
if 'file_handler' not in st.session_state or st.session_state.temp_dir is None:
    file_handler = FileHandler()
    st.session_state.file_handler = file_handler
    st.session_state.temp_dir = file_handler.temp_dir
```

`FileHandler` 在 `tempfile.mkdtemp()` 建立的目錄（如 `/tmp/accrual_bot_ui_xxxxxx/`）中存放使用者上傳的檔案。這些 temp 目錄只在「重新配置」按鈕按下時被清理，在以下情況不會被清理：使用者直接關閉瀏覽器、Streamlit session 逾時（預設 1 小時）、伺服器重啟。

在長時間運行的 server 環境中，大量的 temp 目錄可能消耗磁碟空間。

**改進建議**：
1. 設定一個 Streamlit on_session_end hook（若版本支援），呼叫 `file_handler.cleanup()`
2. 或在 `checkpoints/` 目錄旁設定一個定時清理 cron job（清理超過 24 小時的 `/tmp/accrual_bot_ui_*` 目錄）
3. 或設定 `tmpdir` 在 `output/` 下而非 `/tmp/`，納入集中管理

---

## 8. 延伸議題

### 8.1 Streamlit 執行模型對 UI 設計的深遠影響

Streamlit 的「每次互動都重新執行整個腳本」模型，是理解所有設計選擇的基礎。與傳統 Web 框架（React、Vue）的 event-driven model 不同，Streamlit 是 **script-centric**：整個頁面腳本從頭到尾執行一遍，依序渲染 widgets，Streamlit 框架追蹤每個 widget 的值並在需要時插入上一次的值。

這個模型的隱含後果：
- **不能有任何 side effects 在 render 路徑上**：如果第 N 次 rerun 和第 N+1 次 rerun 之間呼叫了外部 API 或修改了全域狀態，行為可能不一致
- **所有可變狀態必須在 session_state 中**：函數內的局部變數每次 rerun 都重新初始化
- **`st.button()` 的點擊是一次性的**：按鈕按下觸發 rerun，但下一次 rerun 時按鈕狀態重置，必須在同一次 rerun 中處理點擊事件

這解釋了為什麼 `render_entity_selector()` 在按鈕按下後立即更新 session_state 再 `st.rerun()`，而不是等到頁面頂部讀取。

### 8.2 為何 Streamlit 不適合長時間 blocking 操作

Streamlit 的 server 是單 thread 的（每個連接一個 thread），在執行 `AsyncBridge.run_async()` 的 `thread.join()` 期間，這個 thread 被完全阻塞，無法響應任何使用者請求（包括同一個 session 的其他 widget 交互）。

這對 Accrual Bot 的用途來說影響尚可接受（財務人員通常等待批次完成），但如果多人同時使用，Streamlit 需要為每個連接分配一個 server thread，scaling 能力有限。

更適合長時間任務的架構是：Celery 或 Redis Queue + Streamlit 輪詢狀態。

### 8.3 Dual-Layer Pages 的替代方案

`exec()` 方案有其技術負債（調試困難、IDE 支援差）。以下是替代方案：

**方案 A：全部放在 emoji 檔名中**
直接將業務邏輯寫在 `pages/1_⚙️_配置.py` 中，放棄對業務邏輯使用標準命名。缺點是無法直接 `import` 頁面邏輯進行測試。

**方案 B：使用 Streamlit 的 `navigation()` API（Streamlit 1.36+）**
新版 Streamlit 支援 `st.navigation()` 可以在 `main_streamlit.py` 中定義頁面，不依賴 `pages/` 目錄的 emoji 命名。這個方案能完全消除雙層架構的需求，是長期的正確解法。

**方案 C：使用 `importlib` 動態 import**
```python
import importlib.util
spec = importlib.util.spec_from_file_location("page", actual_page)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
```
這比 `exec()` 更「Python 化」，但同樣有命名空間問題。

### 8.4 Dataclass 作為 Session State Value 的型別安全性

Python dataclass 預設不做型別強制（只有型別提示）。`PipelineConfig.processing_date: int = 0` 在 runtime 中仍可被設為字串，不會有任何錯誤。若需要嚴格型別保護，可以：

1. 使用 `pydantic.BaseModel` 取代 `@dataclass`（v2 支援在賦值時驗證型別）
2. 使用 `__post_init__` 加入手動驗證
3. 目前的做法是「信任使用者不會傳入錯誤型別」——對內部工具是可接受的

### 8.5 安全性考量（上傳檔案的潛在風險）

UI 接受使用者上傳的 CSV/XLSX 檔案，並透過 `pd.read_csv()` / `pd.read_excel()` 讀取。

已知 pandas 的安全考量：
- `pd.read_excel()` 在處理畸形的 XLSX 時可能觸發某些解析庫的漏洞（openpyxl 歷史上有 XXE 漏洞）
- 巨大的 CSV（數百 MB）可能導致記憶體耗盡（DoS）

`FileHandler.validate_file()` 只讀取前 5 行作為驗證，這對確認格式基本正確是足夠的，但無法防範 XLSX 解析漏洞或記憶體炸彈。

對於純內部工具（僅財務人員使用），這個風險層級是可接受的。如需公開部署，需要加入檔案大小上限（Streamlit 有 `server.maxUploadSize` 設定）和 openpyxl 版本鎖定。

---

## 9. 其他

### 9.1 模組版本與初始化

`accrual_bot/ui/__init__.py` 宣告了版本號 `__version__ = "0.1.0"`，表示 UI 模組仍在早期開發階段，API 可能仍有重大變更。

### 9.2 測試覆蓋率

根據 CLAUDE.md 的說明，UI 服務層有較高的測試覆蓋率：
- `ui/services/unified_pipeline_service.py`：94%
- `ui/models/state_models.py`：100%
- `ui/services/file_handler.py`：91%

但頁面層（`ui/pages/*.py`）和元件層（`ui/components/*.py`）目前沒有專屬的測試。頁面邏輯的測試通常需要 Streamlit 的 app testing API（`from streamlit.testing.v1 import AppTest`），或透過整合測試覆蓋。

### 9.3 PROCUREMENT/COMBINED 的 Feature Flag 模式

`config.py` 和 `stagging_spx.toml` 中多處有 TODO 的註解掉的設定，例如：

```python
# config.py 第 66–70 行（PROCUREMENT_SOURCE_TYPES）
# 'COMBINED': {  # TODO: 待測試完成後啟用
#     'display_name': 'PO + PR',
#     ...
# },
```

這是典型的「Feature Flag」模式的低技術版本——用 Python 代碼注釋來開關功能。更健全的做法是在 `config.py` 中定義布林旗標 `ENABLE_COMBINED_PROCUREMENT = False`，然後在需要的地方 `if ENABLE_COMBINED_PROCUREMENT:` 判斷，而非到處散落 TODO 注釋，讓功能邊界更清晰，也更容易統一開關。

### 9.4 ui_helpers.py 的設計哲學

`utils/ui_helpers.py` 中的所有函數都是純函數（pure functions）：沒有副作用、不依賴外部狀態、相同輸入永遠相同輸出。這讓它們非常易於測試，也可以在任何 context 中安全呼叫。

`format_duration()` 的分段顯示邏輯（第 29–50 行）值得注意——它根據時間長短選擇不同的表示方式（秒/分秒/時分），這是好的 UX 設計：執行 30 秒的 pipeline 顯示「30.0秒」，執行 5 分鐘的顯示「5分00秒」，而非一律顯示「300秒」。

### 9.5 `get_status_icon` 的擴充性設計

`utils/ui_helpers.py` 第 53–75 行的 `get_status_icon()` 使用字典 dispatch 而非 if/elif：

```python
status_icons = {
    'idle': '⏸️',
    'running': '▶️',
    'completed': '✅',
    'failed': '❌',
    'paused': '⏸️',
    'pending': '⏳',
    'success': '✅',
    'error': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
}
return status_icons.get(status.lower(), '❓')
```

字典 dispatch 比 if/elif 鏈更易於擴充（只需加一行 key/value），且 `.lower()` 使匹配大小寫不敏感，`.get(..., '❓')` 提供安全的預設值。這是一個小但設計良好的函數。

---

*本報告基於 2026-03-13 的 `refactor/restructure` 分支程式碼撰寫，共分析 24 個 Python 檔案。*
