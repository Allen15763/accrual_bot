# utils/logging 模組深度研究

> 研究對象：`accrual_bot/utils/logging/`（2 個 Python 檔案，共 586 行）
> 研究角度：軟體工程最佳實踐（可維護性、執行緒安全、延伸性、可測試性）
> 日期：2026-03-12

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

### 1.1 模組演進脈絡

`utils/logging` 是 Accrual Bot 在 Phase 1（2026-01）重構中從分散式 `print()`、裸 `logging.basicConfig()` 統一遷移而來的集中化日誌框架。原始問題：

| 問題 | 描述 |
|------|------|
| 多模組重複設定 | 各模組各自呼叫 `logging.basicConfig()`，導致 handler 重複添加 |
| 輸出格式不一致 | 不同模組格式各異，無法直接比對時間序列 |
| 無執行緒安全保證 | 多線程並發時 `_loggers` dict 存在競態條件 |
| 缺乏檔案持久化 | 處理結果只輸出到終端，問題排查困難 |

重構後以 **Singleton Logger Manager** 為核心，提供統一的命名空間 (`accrual_bot.*`)、彩色終端輸出、輪轉檔案處理器，並暴露語義化的 `StructuredLogger` 包裝層。

### 1.2 檔案概觀

```
accrual_bot/utils/logging/
├── __init__.py    (13 行)   — re-export facade
└── logger.py      (573 行)  — 全部實作
```

```
tests/unit/utils/logging/
└── test_logger.py (201 行)  — 單元測試
```

### 1.3 在整體架構中的位置

```
accrual_bot (package root)
└── utils/
    ├── config/config_manager.py  ← Logger 依賴（讀取日誌設定）
    └── logging/logger.py         ← Logger 本身
            ↓ get_logger()
    core/pipeline/base.py, pipeline.py, context.py, checkpoint.py
    core/datasources/base.py
    tasks/spt/*, tasks/spx/*
    data/importers/base_importer.py
    utils/helpers/file_utils.py
    runner/step_executor.py, config_loader.py
```

整個 pipeline 框架（22 個模組、60+ 次呼叫）皆透過 `get_logger(name)` 取得子記錄器，全部共用同一個 `Logger` singleton 所設定的 handler 樹。

---

## 2. 用途

### 2.1 五個公開介面

| 名稱 | 類型 | 用途 |
|------|------|------|
| `Logger` | Class | Singleton Manager，管理所有 logger 實例和 handler |
| `StructuredLogger` | Class | 語義化包裝器，提供帶 emoji 前綴的操作日誌方法 |
| `logger_manager` | Instance | `Logger()` 的模組級全域實例 |
| `get_logger(name)` | Function | 取得 `logging.Logger` 的便利函數（最常用） |
| `get_structured_logger(name)` | Function | 取得 `StructuredLogger` 的便利函數 |

### 2.2 典型使用模式

在整個 Accrual Bot 中，**所有 22 個使用模組**一致採用以下單行模式：

```python
from accrual_bot.utils.logging import get_logger

class MyStep(PipelineStep):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
```

這使每個模組都擁有 `accrual_bot.{ClassName}` 這個獨立命名的子記錄器，同時繼承根記錄器的所有 handler 設定。

### 2.3 `StructuredLogger` 的設計意圖 vs 實際使用

`StructuredLogger` 設計目的是語義化日誌，但**在整個 production 程式碼中，無任何模組實際呼叫 `get_structured_logger()`**，僅在測試檔案中驗證「不拋例外」。這反映出一個常見的軟體開發模式：提前設計的 API 介面尚未被採納。

---

## 3. 設計思路

### 3.1 雙鎖分層設計（Two-Lock Stratification）

`Logger` 同時維護兩把獨立的 `threading.Lock()`：

```python
class Logger:
    _lock = threading.Lock()         # ① Singleton 建立鎖（類別層級）
    _logger_lock = threading.Lock()  # ② Logger dict 操作鎖（類別層級）
```

**為什麼需要兩把鎖？**

- `_lock`：只用於 `__new__()` 內的 Double-Checked Locking，確保全域只建立一個 `Logger` 實例。一旦實例建立完成，此鎖就再也不會進入競爭。
- `_logger_lock`：用於保護所有對 `self._loggers` 和 `self._handlers` 的讀寫操作，包括 `get_logger()`、`add_custom_handler()`、`remove_handler()`、`set_level()` 等。在 singleton 建立後，這把鎖是長期活躍的。

這種分層設計避免了「用一把大鎖解決所有問題」的粗糙作法，相比 `ConfigManager` 只有一把 `_lock` 更為精細。

### 3.2 Python Logger 命名空間繼承（Logger Hierarchy Propagation）

本模組充分利用 Python `logging` 模組的**點分層次（dotted hierarchy）**設計：

```
                  logging root logger
                        │ (propagate=True by default)
            ┌───────────┴───────────┐
  [propagate=False]          其他第三方 logger
  accrual_bot  ←── root logger（本模組設定的命名空間根）
       │           handlers: [ConsoleHandler, FileHandler]
       │           level: INFO
       ├── accrual_bot.Pipeline
       ├── accrual_bot.ProcessingContext
       ├── accrual_bot.SPTDataLoadingStep
       └── ...（子記錄器，無 handler，level=DEBUG）
              └── 透過 propagate=True 傳給 accrual_bot
```

**關鍵設計決策**：
1. 根記錄器設定 `propagate = False`，切斷與 Python root logger 的連接，避免 basicConfig 重複輸出
2. 所有子記錄器**不添加 handler**，只設定 `level=DEBUG`，讓事件自動向上傳播到 `accrual_bot` 根
3. 這意味著所有日誌輸出行為（格式、目標）只需在一處（根記錄器）設定即可全域生效

### 3.3 ColoredFormatter 的 Record 保護策略

Python 的 `logging.LogRecord` 物件在**同一事件的所有 handler 之間共享**，不是每個 handler 各自持有一份複本。因此，若在 `format()` 中直接修改 `record.levelname`（添加 ANSI 碼），會導致同一事件傳到 FileHandler 時，檔案中也包含 ANSI 控制碼。

`ColoredFormatter` 的解法：

```python
def format(self, record: logging.LogRecord) -> str:
    if self.use_color:
        original_levelname = record.levelname   # ① 儲存原始值
        original_name = record.name

        color = self.COLORS.get(record.levelno, ColorCodes.RESET)
        record.levelname = f"{color}{original_levelname}{ColorCodes.RESET}"  # ② 暫時修改
        record.name = f"{ColorCodes.CYAN}{original_name}{ColorCodes.RESET}"

        result = super().format(record)          # ③ 格式化

        record.levelname = original_levelname    # ④ 立即恢復
        record.name = original_name
        return result
    else:
        return super().format(record)
```

此 save-modify-format-restore 模式是 Python logging 社群的標準做法，也是 `colorlog` 等第三方庫的核心邏輯。

### 3.4 Fallback 降級策略（Graceful Degradation）

`_setup_logging()` 的完整容錯流程：

```
_setup_logging()
    │
    ├─ try: config_manager.get('LOGGING', 'level', 'INFO')
    │       → _setup_root_logger(...)
    │           ├─ try: log_path = config_manager.get('PATHS', 'log_path')
    │           │       → _setup_file_handler(...)
    │           │           └─ try: RotatingFileHandler(...)
    │           │               └─ except: stderr.write(錯誤)
    │           └─ 控制台 handler 無論如何都會成功
    │
    └─ except: _setup_fallback_logger()
               → 最低限度的 INFO 控制台輸出
```

即使 ConfigManager 初始化失敗（罕見情況），logging 仍能正常工作，確保後續錯誤訊息可被記錄。

### 3.5 三種日誌格式的設計哲學

```python
SIMPLE_FORMAT  = '%(asctime)s %(levelname)s: %(message)s'
# → 用於降級（fallback）情境，最小化輸出

DETAILED_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(process)d-%(thread)d | %(message)s'
# → 用於控制台輸出，包含呼叫位置與 PID-TID，便於開發除錯與確認並發行為

FILE_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(module)s.%(funcName)s:%(lineno)d | %(process)d-%(thread)d | %(message)s'
# → 用於檔案輸出，額外記錄模組名，適合離線分析
```

三層設計的邏輯：終端關注即時可讀性（含 PID-TID 以確認並發），檔案關注完整可追溯性（加模組名），降級關注最低可用性。

> **2026-03-17 更新**：DETAILED_FORMAT 加入 `%(process)d-%(thread)d`，使控制台與檔案日誌均顯示執行緒資訊，方便確認並發載入是否正常運作。

---

## 4. 各項知識點

### 4.1 Python logging 模組架構全覽

Python `logging` 是一個多層次的事件傳遞系統：

```
Logger.info("msg")
    │
    ▼
Logger.isEnabledFor(INFO)?  → No → 丟棄
    │ Yes
    ▼
建立 LogRecord（含 name, level, msg, pathname, lineno, funcName 等）
    │
    ▼
Logger.handle(record)
    │
    ├─ 本 logger 的所有 handlers（若有）
    │
    └─ if propagate=True → 傳給 parent logger
           └─ parent.handlers（若有）
                └─ 繼續向上傳遞，直到 root 或 propagate=False
```

關鍵：**level 判斷在 logger 層做，filter/format 在 handler 層做**。子記錄器設 `DEBUG` 但父的 handler 設 `INFO`，實際效果是 INFO 以上才輸出。

### 4.2 `getattr(logging, level.upper(), logging.INFO)` 的安全解析

```python
log_level = getattr(logging, log_level.upper(), logging.INFO)
```

這是從字串轉換為 logging 整數常數的慣用模式：

| 字串輸入 | 等同於 |
|---------|--------|
| `"INFO"` | `logging.INFO` (20) |
| `"DEBUG"` | `logging.DEBUG` (10) |
| `"INVALID"` | `logging.INFO` (fallback) |

相比 `logging.getLevelName()` 或 `int(level)`，此方法更安全：無效字串直接使用預設值，不拋出例外。

### 4.3 `RotatingFileHandler` 的輪轉機制

```python
RotatingFileHandler(
    log_file_path,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5,
    encoding='utf-8'
)
```

輪轉行為：
- 當 `log_file_path` 達到 10MB → 重新命名為 `*.log.1`，舊的 `*.log.1` → `*.log.2`，最多保留 5 個備份
- 超過 5 個備份的最老檔案**自動刪除**

**本專案的特殊性**：由於日誌檔案名稱包含啟動時間戳記（`Accrual_bot_20260312_143022.log`），每次程式啟動都會建立新檔案，`maxBytes` 限制只在**單次執行**中才有意義。對於月度批次處理系統，10MB 的限制通常不會觸發，實際上 `backupCount` 的意義更多是「防止異常長時間執行撐爆磁碟」的保險機制。

### 4.4 UTC+8 時區感知 datetime

```python
tz_offset = timezone(timedelta(hours=8))
aware_datetime = datetime.now(tz_offset).strftime('%Y%m%d_%H%M%S')
```

相比舊版的 `datetime.now()` (naive datetime)：
- `datetime.now(tz)` 建立**時區感知（aware）**的 datetime 物件
- 在跨時區部署（如 CI/CD 在 UTC 伺服器、本地在 UTC+8）時，日誌時間戳記將始終對應台灣時間
- `timezone(timedelta(hours=8))` 是 Python 3.2+ 標準方式，無需第三方 `pytz` 或 `zoneinfo`

注意：`strftime()` 格式中缺乏時區後綴（如 `+0800`），若日誌檔案需在多時區環境分析，建議加上 `%z`。

### 4.5 `ColorCodes` 的設計選擇：Class vs Enum vs 模組常數

本模組使用 `class ColorCodes:` 存放 ANSI 碼，而非 `enum.Enum` 或模組級常數：

| 方式 | 優點 | 缺點 |
|------|------|------|
| `class ColorCodes:` 類別屬性 | 語意分組、存取簡潔（`ColorCodes.RED`） | 可被繼承和修改，值可被重新指派 |
| `enum.Enum` | 不可變、可迭代、有成員數量 | 需要 `.value` 存取字串值 |
| 模組級常數 | 最簡單 | 缺乏命名空間分組 |

對於「只是一組字串常數、不需要迭代或比較」的場景，`class` 方式在 Python 中是合理的（也是 `http.HTTPStatus` 等標準庫的做法）。若未來需要驗證或迭代顏色清單，改為 `StrEnum`（Python 3.11+）會更嚴謹。

### 4.6 `%(levelname)-8s` 對齊格式化

DETAILED_FORMAT 中的 `%(levelname)-8s` 是標準 Python 字串格式化：
- `-8s` = 左對齊，最小寬度 8 個字元
- 效果：`DEBUG   `, `INFO    `, `WARNING `, `ERROR   `, `CRITICAL` 全部等寬
- 使多行日誌的「消息本體」欄位（`%(message)s`）自動對齊，提升可讀性

### 4.7 `_setup_fallback_logger()` 中的鎖保護

```python
def _setup_fallback_logger(self) -> None:
    with Logger._logger_lock:
        if 'root' not in self._loggers:
            ...
```

`_setup_fallback_logger()` 在 `_setup_logging()` 的 except 塊中被呼叫，此時尚未進入任何鎖。因此在 fallback 方法內部需要自行加鎖保護 `_loggers` dict 的修改。這是一個正確且必要的防護。

然而，對比 `_setup_root_logger()` 卻**沒有**持有 `_logger_lock` 就直接修改 `_loggers` 和 `_handlers`：

```python
def _setup_root_logger(self, ...):
    if 'root' not in self._loggers:   # ← 無鎖保護，check-then-act 競態條件
        self._loggers['root'] = root_logger
    ...
    self._handlers['console'] = console_handler  # ← 無鎖
```

這兩個方法的一致性存在缺口：`_setup_fallback_logger` 加鎖，`_setup_root_logger` 不加鎖。

### 4.8 Handler 繼承 vs 直接添加

Python logging 的子記錄器有兩種使用 handler 的方式：

**方式一（本模組採用）**：子記錄器不添加 handler，依賴 propagate 傳給父

```python
# get_logger('SPTDataLoading') 會產生：
logger = logging.getLogger('accrual_bot.SPTDataLoading')
logger.setLevel(logging.DEBUG)  # 不添加任何 handler
# 所有訊息向上傳給 accrual_bot，由 accrual_bot 的 handler 輸出
```

**方式二**：每個子記錄器各自持有 handler

```python
logger.addHandler(my_handler)  # 每個子記錄器都有自己的輸出設定
```

方式一的優點：動態性強，只需在根記錄器改動 handler 就能全局生效；添加/移除 handler 無需遍歷所有子記錄器。
方式一的缺點：若需要讓特定模組輸出到獨立的日誌檔案，需要額外的特殊處理。

### 4.9 `add_custom_handler()` 的重複輸出問題 ✅ 已修復（2026-03-14）

> **此問題已修復。** 改為只將 handler 添加到 root logger，子記錄器透過 `propagate=True` 自動享用。

**歷史紀錄**：原實作遍歷所有 loggers 添加 handler，子記錄器的 `propagate=True` 導致同一條訊息同時被子與 root 的 handler 處理，輸出兩次。

**修復後的實作**：
```python
def add_custom_handler(self, name: str, handler: logging.Handler) -> None:
    with Logger._logger_lock:
        self._handlers[name] = handler
        # 只將處理器添加到 root logger
        # 子記錄器的 propagate=True 會自然將訊息傳至 root，避免重複輸出
        root = self._loggers.get('root')
        if root and handler not in root.handlers:
            root.addHandler(handler)
```

### 4.10 `get_logger()` 的死程式碼（Dead Code）

```python
def get_logger(self, name: Optional[str] = None) -> logging.Logger:
    with Logger._logger_lock:
        if name not in self._loggers:
            if name == 'root':
                if 'root' not in self._loggers:   # ← 永遠為 True（外層已確認）
                    self._loggers['root'] = logging.getLogger('accrual_bot')
                return self._loggers['root']
```

外層 `if name not in self._loggers:` 已確認 `name` 不在 dict 中。若 `name == 'root'`，則內層 `if 'root' not in self._loggers:` 必然為 `True`，此檢查是死程式碼（永遠成立的判斷）。這不影響正確性，但增加閱讀困惑。

### 4.11 duckdb_manager 的平行日誌系統

`accrual_bot/utils/duckdb_manager/utils/logging.py` 定義了一套完全獨立的日誌系統：

```python
@runtime_checkable
class LoggerProtocol(Protocol):  # 介面協議
    def debug(self, msg, *args, **kwargs): ...
    def info(self, msg, *args, **kwargs): ...
    ...

class NullLogger:  # 空實作
    def debug(self, msg, *args, **kwargs): pass
    ...
```

這套系統基於 **Protocol（結構型別）+ 依賴注入**，設計模式完全不同於主系統的 Singleton。目的是讓 `duckdb_manager` 作為獨立函式庫被外部使用，不硬依賴 `accrual_bot` 的日誌系統。

兩套並行日誌系統的架構影響：
- 優點：`duckdb_manager` 具有高度可攜性，可被其他專案使用
- 缺點：在同一個 `accrual_bot` 執行期間，若 DuckDB 操作的日誌需要與 pipeline 日誌合併分析，需要確保 `DuckDBManager` 注入了相同的 logger 實例

### 4.12 `_initialized` 標誌的非原子性問題 ✅ 已修復（2026-03-14）

> **此問題已修復。** `__init__` 的整個初始化區塊現已包裹在 `with Logger._lock:` 內。

**歷史紀錄**：`_initialized` 的讀取在 `__init__()` 無鎖進行，寫入在 `cleanup()` 的 `_logger_lock` 內，不同鎖域導致 TOCTOU（Time-of-check to time-of-use）競態條件。CPython GIL 下不常觸發，但理論上不安全。

**修復後的實作**：
```python
def __init__(self):
    with Logger._lock:        # ← 使用 _lock（非 _logger_lock，避免 _setup_fallback_logger 死鎖）
        if self._initialized:
            return
        self._loggers: Dict[str, logging.Logger] = {}
        self._handlers: Dict[str, logging.Handler] = {}
        self._setup_logging()
        self._initialized = True
```

**為何選 `_lock` 而非 `_logger_lock`**：`_setup_logging()` 失敗時會呼叫 `_setup_fallback_logger()`，後者內部已持有 `_logger_lock`；若 `__init__` 也持有 `_logger_lock`，會造成同執行緒死鎖（`threading.Lock` 不可重入）。`_lock` 只在 `__new__` 的創建區塊使用，無此風險。

---

## 5. 應用範例

### 5.1 最基本用法：取得命名子記錄器

```python
from accrual_bot.utils.logging import get_logger

class SPTDataLoadingStep:
    def __init__(self):
        # 取得 'accrual_bot.SPTDataLoadingStep' 子記錄器
        self.logger = get_logger(self.__class__.__name__)

    async def execute(self, context):
        self.logger.info(f"開始載入 SPT 資料，處理月份: {context.processing_date}")
        # INFO | accrual_bot.SPTDataLoadingStep | execute:42 | 開始載入 SPT 資料...
```

### 5.2 動態調整日誌級別（除錯用）

```python
from accrual_bot.utils.logging import Logger

# 全部記錄器切換到 DEBUG
Logger().set_level('DEBUG')

# 只對特定模組切換
Logger().set_level('DEBUG', 'SPXConditionEngine')

# 執行完畢後切回 INFO
Logger().set_level('INFO')
```

### 5.3 新增自定義 Handler（如 Streamlit 即時顯示）

```python
import logging
from accrual_bot.utils.logging import Logger

# 建立一個 in-memory handler，供 Streamlit UI 讀取
class InMemoryHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(self.format(record))

memory_handler = InMemoryHandler()
memory_handler.setLevel(logging.INFO)
memory_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

# 注意：因 add_custom_handler 設計問題，建議只直接添加到根記錄器
import logging as std_logging
std_logging.getLogger('accrual_bot').addHandler(memory_handler)
```

### 5.4 使用 StructuredLogger 的語義化方法

```python
from accrual_bot.utils.logging import get_structured_logger

sl = get_structured_logger('DataProcessor')

# ▶ 開始執行: 載入 PO 資料 | entity=SPT date=202502
sl.log_operation_start('載入 PO 資料', entity='SPT', date=202502)

# ✓ 成功: 載入 PO 資料 | rows=1234
sl.log_operation_end('載入 PO 資料', success=True, rows=1234)

# 📊 處理 PO 數據: 1,234 筆記錄 | 耗時 0.85s
sl.log_data_processing('PO', 1234, processing_time=0.85)

# ⏳ 進度: 45/100 (45.0%) | 正在處理分類
sl.log_progress(45, 100, operation='正在處理分類')

# ❌ [APInvoiceIntegration] 錯誤: ... (含 exc_info traceback)
try:
    raise ValueError("無效的發票格式")
except Exception as e:
    sl.log_error(e, context='APInvoiceIntegration')
```

### 5.5 查詢日誌系統狀態

```python
from accrual_bot.utils.logging import logger_manager

stats = logger_manager.get_log_stats()
# 返回：{
#   'loggers_count': 15,
#   'handlers_count': 2,
#   'logger_names': ['root', 'Pipeline', 'SPTDataLoadingStep', ...],
#   'handler_names': ['console', 'file']
# }
print(f"目前已建立 {stats['loggers_count']} 個記錄器")
```

### 5.6 測試隔離模式（參考測試檔案的 fixture）

```python
import pytest
from accrual_bot.utils.logging.logger import Logger

@pytest.fixture(autouse=True)
def reset_logger_singleton():
    """每個測試後重置 Logger 單例，避免測試間互相污染"""
    yield
    with Logger._lock:
        if Logger._instance is not None:
            try:
                Logger._instance.cleanup()
            except Exception:
                pass
            Logger._instance = None
            Logger._initialized = False
```

這個 fixture 演示了正確的測試隔離：先 `cleanup()` 釋放資源，再清除 `_instance` 和 `_initialized`，確保下一個測試獲得全新的 Logger 狀態。

### 5.7 Windows 終端彩色輸出啟用

```python
# ColoredFormatter._supports_color() 在 Windows 上的行為：
# 呼叫 Windows API 啟用 ANSI 處理模式

# 若需要強制禁用彩色（如寫入管道或 CI 環境）：
from accrual_bot.utils.logging.logger import ColoredFormatter
formatter = ColoredFormatter(
    fmt='%(levelname)s: %(message)s',
    use_color=False  # 明確禁用
)
```

### 5.8 取得子記錄器並確認繼承關係

```python
import logging
from accrual_bot.utils.logging import get_logger

# 取得兩個子記錄器
logger_a = get_logger('ModuleA')  # → logging.getLogger('accrual_bot.ModuleA')
logger_b = get_logger('ModuleB')  # → logging.getLogger('accrual_bot.ModuleB')

# 驗證命名空間繼承
assert logger_a.parent.name == 'accrual_bot'
assert logger_b.parent.name == 'accrual_bot'

# 驗證子記錄器無 handler（依賴繼承）
assert len(logger_a.handlers) == 0
assert logger_a.propagate == True  # 預設值，向上傳遞

# 根記錄器有 handler
root = get_logger()  # accrual_bot
assert len(root.handlers) >= 1
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ 優點 1：雙鎖設計，職責分離

`_lock`（Singleton 建立）與 `_logger_lock`（dict 操作）職責分離，避免了粗糙的「一鎖到底」設計。Singleton 建立後 `_lock` 進入冷卻，`_logger_lock` 接管所有日常操作，減少了不必要的競爭。

#### ✅ 優點 2：Record 保護的 Save-Restore 模式

`ColoredFormatter.format()` 正確實作 save-modify-format-restore 流程，解決了多 handler 環境下 ANSI 碼污染問題。同一 `LogRecord` 物件流過 console handler 後，file handler 仍能收到乾淨的原始字串。

#### ✅ 優點 3：三層格式化設計

SIMPLE、DETAILED、FILE 三種格式對應不同場景（降級、開發、生產排查），且格式選擇由 config_manager 驅動（`LOGGING.detailed`），可在不修改程式碼的情況下切換。

#### ✅ 優點 4：完整的 Fallback 容錯鏈

`_setup_logging()` → `_setup_root_logger()` → `_setup_file_handler()` 每層都有 try-except，且 fallback 使用 `sys.stderr.write()` 而非 `print()`，符合 Python 程式庫的最佳實踐（不污染 stdout）。

#### ✅ 優點 5：測試可重置性（Testability）

`cleanup()` 方法配合測試 fixture，提供完整的 Singleton 重置路徑，避免測試間狀態污染。這是本模組相比 `ConfigManager`（無 `reset_for_testing()`）更優秀的設計。

### 6.2 缺點與問題

#### ✅ 問題 1：`_supports_color()` 的 TypeError（已修復，2026-03-14）

**位置**：`logger.py` 第 71 行

`sys.stderr.write()` 只接受字串，原實作直接傳入 `Exception` 物件會拋出 `TypeError`，在 Windows 每次啟動時觸發。

**修復後**：
```python
except Exception as err:
    sys.stderr.write(f"{err}\n")   # ← 修復：f-string 確保輸出字串
    return False
```

#### 🟡 問題 2：`_setup_root_logger()` 缺乏鎖保護

**位置**：`logger.py` 第 172-205 行

`_setup_root_logger()` 直接修改 `self._loggers` 和 `self._handlers`，但不持有 `_logger_lock`。相比 `_setup_fallback_logger()` 的加鎖做法，這兩個方法的一致性有缺口。

**嚴重性**：🟡 Medium（理論上 `_setup_root_logger` 只在 `_initialized=False` 時呼叫，多線程同時觸發此路徑機率低）

#### ✅ 問題 3：`_initialized` 標誌的非原子性讀寫（已修復，2026-03-14）

**位置**：`__init__()` — 已在整個初始化區塊加 `with Logger._lock:`，`_initialized` 的讀寫均在同一把鎖保護下，TOCTOU 競態條件完全消除。詳見 [§4.12](#412-_initialized-標誌的非原子性問題-✅-已修復2026-03-14)。

#### ✅ 問題 4：`add_custom_handler()` 添加到所有子記錄器導致重複輸出（已修復，2026-03-14）

**位置**：`logger.py` 第 305–310 行 — 已改為只添加至 root logger，`propagate` 機制自然分發，同時加入冪等保護（`handler not in root.handlers`）。詳見 [§4.9](#49-add_custom_handler-的重複輸出問題-✅-已修復2026-03-14)。

#### 🟢 問題 5：`get_logger()` 中的死程式碼

**位置**：`logger.py` 第 285 行

```python
if name == 'root':
    if 'root' not in self._loggers:  # ← 此條件永遠為 True（外層已排除）
```

不影響正確性，但增加閱讀困惑，建議清理。

**嚴重性**：🟢 Low

#### 🟢 問題 6：`StructuredLogger.__init__` 型別標註不精確

**位置**：`logger.py` 第 421 行

```python
def __init__(self, logger_name: str = None):  # ← 允許 None 但標註為 str
```

應為 `Optional[str] = None`，目前型別標註與實際行為不符，靜態分析工具（mypy）會報警告。

**嚴重性**：🟢 Low

#### 🟢 問題 7：`StructuredLogger` 在 production 程式碼中零使用

`StructuredLogger` 被導出並記錄在 `__all__` 中，但整個 `accrual_bot` production 程式碼（22 個使用 logging 的模組）無任何一個實際使用 `get_structured_logger()`。這是「設計超前於需求」的典型案例，增加了 API 表面積但無實際貢獻。

**嚴重性**：🟢 Low（可作為技術債追蹤）

#### 🟢 問題 8：duckdb_manager 的平行日誌系統不整合

`utils/duckdb_manager/utils/logging.py` 擁有獨立的 `LoggerProtocol`、`NullLogger`、`get_logger()`，與主系統完全分離。雖然設計上有理由（可攜性），但在同一個 `accrual_bot` 執行期間，DuckDB 操作日誌和 pipeline 日誌的整合需要手動注入，增加使用複雜度。

**嚴重性**：🟢 Low（架構問題，非 Bug）

### 6.3 問題彙整表

| # | 位置 | 問題 | 嚴重性 | 修正建議 |
|---|------|------|--------|---------|
| 1 | logger.py:71 | ~~`sys.stderr.write(err)` TypeError~~ | ✅ 已修復（2026-03-14）| 改為 `f"{err}\n"` |
| 2 | logger.py:172 | `_setup_root_logger` 無鎖直寫 dict | 🟡 Medium | 加 `with Logger._logger_lock:` |
| 3 | __init__:141 | ~~`_initialized` 讀寫跨鎖域~~ | ✅ 已修復（2026-03-14）| `__init__` 加 `with Logger._lock:` |
| 4 | logger.py:305 | ~~`add_custom_handler` 重複添加~~ | ✅ 已修復（2026-03-14）| 只添加到 root logger + 冪等保護 |
| 5 | logger.py:285 | 死程式碼 inner check | 🟢 Low | 移除內層 if |
| 6 | logger.py:421 | 型別標註 `str` 非 `Optional[str]` | 🟢 Low | 修正型別標註 |
| 7 | 整體 | StructuredLogger 零 production 使用 | 🟢 Low | 確認需求後留或移除 |
| 8 | duckdb_manager | 平行日誌系統不整合 | 🟢 Low | 文件化注入方式 |

---

## 7. 延伸議題

### 7.1 ReadWriteLock（讀寫鎖）優化

目前所有操作（包括純讀取的 `get_logger()`）都使用互斥鎖 `_logger_lock`。在高並發 pipeline 中，`get_logger()` 的呼叫頻率遠高於寫入操作，可考慮引入讀寫鎖（RWLock）：

```python
import threading

class RWLock:
    def __init__(self):
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._readers = 0

    # 多個讀者可並發；寫者獨占
    def read_acquire(self): ...
    def write_acquire(self): ...
```

Python 3.x 標準庫未提供 RWLock，但 `threading.RLock`（可重入鎖）可用於避免同一線程的死鎖問題。

### 7.2 Structured Logging 的現代替代方案

目前的 `StructuredLogger` 以拼接字串的方式構造訊息（如 `f"key={value}"`），這種「偽結構化」日誌難以被 log aggregator（如 ELK Stack、Grafana Loki）解析。真正的結構化日誌方案：

**方案 A：使用 `extra` 參數**

```python
logger.info("處理資料", extra={"entity": "SPT", "rows": 1234, "duration": 0.85})
# LogRecord 附加屬性，可透過自定義 Formatter 輸出 JSON
```

**方案 B：JSON 格式 Formatter**

```python
import json
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function": record.funcName,
            "line": record.lineno,
        }
        if hasattr(record, 'entity'):
            log_obj['entity'] = record.entity
        return json.dumps(log_obj, ensure_ascii=False)
```

**方案 C：`structlog` 第三方庫**

```python
import structlog
log = structlog.get_logger()
log.info("資料處理完成", entity="SPT", rows=1234, duration_s=0.85)
# → {"event": "資料處理完成", "entity": "SPT", "rows": 1234, ...}
```

`structlog` 支援 context binding、processor chains、JSON/console 雙模式輸出。

### 7.3 `logging.config.dictConfig` 集中化設定

目前日誌設定分散在 `_setup_logging()`、`_setup_root_logger()`、`_setup_file_handler()` 三個方法中。Python 提供了 `logging.config.dictConfig()` 支援從 dict（可由 TOML 轉換）進行一次性設定：

```python
import logging.config

log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {"format": "%(asctime)s | %(levelname)-8s | ..."},
        "file": {"format": "%(asctime)s | ... | %(process)d-%(thread)d | ..."},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "detailed"},
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/accrual_bot.log",
            "maxBytes": 10485760,
            "backupCount": 5,
            "formatter": "file",
        },
    },
    "loggers": {
        "accrual_bot": {"level": "INFO", "handlers": ["console", "file"], "propagate": False},
    }
}

logging.config.dictConfig(log_config)
```

優點：設定完全聲明式，易於測試和文件化；支援從 YAML/TOML 載入；`fileConfig()` 也支援 INI 格式。

### 7.4 Log Aggregation 與可觀測性（Observability）

在雲端或分散式環境中，本機 RotatingFileHandler 不足夠。可考慮添加：

**OpenTelemetry Logging**：
```python
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
# 將日誌傳送到 Jaeger/Zipkin/OTLP
```

**CloudWatch/GCP Logging Handler**：
```python
# AWS
import boto3
from watchtower import CloudWatchLogHandler
handler = CloudWatchLogHandler(boto3_client=boto3.client('logs', region_name='ap-northeast-1'))
```

對於月度批次處理，雖然分散式可觀測性不是迫切需求，但加入 correlation ID（如 `processing_date` + `entity` 作為 trace context）可大幅提升多次執行日誌的關聯分析能力。

### 7.5 `loguru` — 現代 Python Logging 替代方案

[loguru](https://github.com/Delgan/loguru) 解決了標準 `logging` 模組的多個痛點：

| 功能 | 標準 logging | loguru |
|------|-------------|--------|
| 設定複雜度 | 需要 Logger + Handler + Formatter | 開箱即用，一行設定 |
| 結構化日誌 | 需要自定義 Formatter | 原生支援 |
| 彩色輸出 | 需要 ColoredFormatter | 內建，自動偵測 |
| 例外鏈記錄 | `exc_info=True` | `logger.exception()` 自動 |
| 跨 thread ID | `%(thread)d` | 原生支援 |
| `sink` 動態添加 | 複雜的 addHandler | `logger.add(sink, ...)` |

對於全新專案，`loguru` 是更簡潔的選擇；對於已有 `logging.Logger` 依賴的系統，可透過 `loguru` 的 `PropagateHandler` 橋接兩個系統。

### 7.6 測試覆蓋率的提升方向

目前測試著重 Singleton 和 get_logger，但以下場景尚未覆蓋：

| 未測試場景 | 重要性 | 建議測試方法 |
|-----------|--------|------------|
| `_supports_color()` 在 Windows 上的行為 | 🔴 High（含 Bug） | `mock ctypes.windll.kernel32` |
| `_setup_file_handler()` 成功路徑 | 🟡 Medium | `tmp_path` fixture |
| `add_custom_handler()` 的重複輸出問題 | 🟡 Medium | 捕捉 handler 輸出計數 |
| `set_level()` 對 propagate 行為的影響 | 🟡 Medium | `caplog` fixture |
| `cleanup()` 後重新初始化的完整性 | 🟡 Medium | cleanup → Logger() → 驗證 handler |
| `StructuredLogger` 全部 6 個方法 | 🟢 Low | mock logger 驗證訊息格式 |
| `get_logger()` 並發安全（100+ threads） | 🟢 Low | 同 ConfigManager 的壓力測試 |

---

## 8. 其他

### 8.1 完整 API 速查表

#### `Logger` 類別

| 方法 | 簽名 | 說明 |
|------|------|------|
| `__new__` | `() → Logger` | Double-Checked Locking Singleton |
| `__init__` | `() → None` | 初始化並呼叫 `_setup_logging()` |
| `get_logger` | `(name: Optional[str]) → logging.Logger` | 取得命名子記錄器（線程安全） |
| `add_custom_handler` | `(name: str, handler: Handler) → None` | 添加自定義 handler |
| `remove_handler` | `(name: str) → None` | 移除並關閉 handler |
| `set_level` | `(level: str, logger_name: Optional[str]) → None` | 設定記錄器級別 |
| `get_log_stats` | `() → Dict[str, Any]` | 取得日誌系統統計資訊 |
| `cleanup` | `() → None` | 釋放所有 handler 資源，重置狀態 |

#### `StructuredLogger` 類別

| 方法 | 主要參數 | 輸出格式 |
|------|---------|---------|
| `log_operation_start` | `operation, **kwargs` | `▶ 開始執行: {op} \| {key}={val}` |
| `log_operation_end` | `operation, success, **kwargs` | `✓/✗ 成功/失敗: {op} \| ...` |
| `log_data_processing` | `data_type, record_count, processing_time` | `📊 處理 {type}: {n:,} 筆 \| 耗時 {t}s` |
| `log_file_operation` | `operation, file_path, success` | `✓/✗ 檔案{op}: {path}` |
| `log_error` | `error, context, **kwargs` | `❌ [{ctx}] 錯誤: {msg}` (含 exc_info) |
| `log_progress` | `current, total, operation` | `⏳ 進度: {c}/{t} ({pct}%)` |

#### 模組級函式

| 函式 | 說明 |
|------|------|
| `get_logger(name)` | 取得 `logging.Logger` 的便利包裝 |
| `get_structured_logger(name)` | 建立新的 `StructuredLogger` 實例 |

### 8.2 日誌格式範例輸出

**DETAILED_FORMAT（控制台，含 PID-TID）：**
```
2026-03-17 10:12:45 | INFO     | accrual_bot.datasource.CSVSource   | read_csv_sync:67 | 12345-93908 | Reading CSV file: ...
2026-03-17 10:12:45 | INFO     | accrual_bot.datasource.ExcelSource | read_excel_sync:67 | 12345-19612 | Reading Excel file: ...
2026-03-17 10:12:45 | INFO     | accrual_bot.datasource.ExcelSource | read_excel_sync:67 | 12345-16188 | Reading Excel file: ...
2026-03-17 10:12:46 | INFO     | accrual_bot.datasource.CSVSource   | read_csv_sync:103 | 12345-93908 | Successfully read 47548 rows from CSV
```

> **2026-03-17 更新**：DETAILED_FORMAT 加入 `%(process)d-%(thread)d`，控制台輸出現可直接確認並發載入是否正常（不同 thread ID = 不同執行緒）。

**FILE_FORMAT（日誌檔案，額外含模組名）：**
```
2026-03-17 10:12:45 | INFO     | accrual_bot.Pipeline | pipeline.execute:201 | 12345-140234567890 | Pipeline 開始執行，共 15 步驟
```

### 8.3 設定驅動的行為矩陣

| ConfigManager key | 預設值 | 影響 |
|------------------|--------|------|
| `LOGGING.level` | `"INFO"` | 根記錄器和 console handler 的最低輸出級別 |
| `LOGGING.detailed` | `True` | 控制台使用 DETAILED_FORMAT（含位置與 PID-TID）還是 SIMPLE_FORMAT |
| `LOGGING.color` | `True` | 是否啟用 ANSI 彩色輸出 |
| `PATHS.log_path` | `None` | 若設定則建立 RotatingFileHandler；否則只有控制台 |

### 8.4 與 ConfigManager 的對比

| 面向 | ConfigManager | Logger |
|------|--------------|--------|
| Singleton 保護 | `_lock` × 1 | `_lock` + `_logger_lock` × 2（更完善） |
| 測試可重置 | 無 `reset_for_testing()` | 有 `cleanup()` 方法 |
| `_initialized` 鎖保護 | `__init__` 已加 `_lock`（2026-03-14 修復） | `__init__` 已加 `_lock`（2026-03-14 修復） |
| Fallback 機制 | 5 層路徑解析 | 2 層日誌降級 |
| 依賴關係 | 依賴 stdlib `logging`（避免循環） | 依賴 ConfigManager（單向） |

### 8.5 `utils/__init__.py` 的 import 順序保證

`utils/__init__.py` 的 import 順序是：

```python
from .config import *    # ① ConfigManager 初始化
from .logging import *   # ② Logger 初始化（此時 config_manager 已就緒）
from .helpers import *   # ③
```

由於 Logger 初始化時會呼叫 `config_manager.get()`，確保 config 先於 logging 初始化是必要的。這種依賴順序目前靠 import 順序隱性保證，未來若 `utils/__init__.py` 的 import 順序被調換（如自動 isort），可能導致 `config_manager` 尚未初始化時 Logger 就嘗試讀取設定，觸發 fallback 路徑。

建議在 Logger 的設計文件中明確標注此初始化順序依賴。
