# `accrual_bot/utils/config` 模組深度研究

> 撰寫日期：2026-03-12
> 研究對象：`accrual_bot/utils/config/` 目錄下所有原始碼（3 個 Python 檔案，共 997 行）

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

`utils/config` 模組是整個 accrual_bot 系統的配置管理核心。在系統重構前（2025 年以前），配置讀取分散在各個業務模組中——每個 step 檔案自行讀取 INI 或 TOML，沒有統一入口，導致三個主要問題：

1. **多執行緒競態（Race Condition）**：pipeline 以 `asyncio` 並發執行，當多個 Task 同時初始化 ConfigManager 時，舊版缺乏鎖保護，可能產生多個不同實例，各自讀取不同版本的配置
2. **路徑脆弱性**：dev/prod/PyInstaller 三種環境的相對路徑不同，各模組硬編寫路徑字串容易失效
3. **格式割裂**：舊 legacy 配置用 INI（`config.ini`），新業務規則用 TOML（`stagging.toml`）；兩套格式、兩套讀法、無統一抽象層

### 1.2 重構目標（Architecture Improvements Phase 1）

根據 `CLAUDE.md` 記載，**Phase 1（P0）** 的首要工作就是修復 ConfigManager 的執行緒安全問題，引入 **雙重檢查鎖定（Double-Checked Locking）** 模式。後續 Phase 7 進一步將 `stagging.toml`（1762 行）按實體拆分為三個 TOML 文件，並在 `_load_config()` 中加入 deep-merge 邏輯，實現「零改動現有存取點」的透明升級。

### 1.3 模組組成

| 檔案 | 行數 | 職責 |
|------|------|------|
| `__init__.py` | 29 | 公開 API 匯出 |
| `constants.py` | 114 | 編譯期靜態常數 |
| `config_manager.py` | 854 | 執行期動態配置（ConfigManager 主體） |

### 1.4 配置文件生態系

ConfigManager 管理的配置文件共 6 個：

```
accrual_bot/config/
├── config.ini           ← INI 格式，legacy；含 GENERAL / CREDENTIALS / FA_ACCOUNTS / SPT / SPX
├── stagging.toml        ← TOML；共用業務規則（~430 行）：general, date_patterns, category_patterns
├── stagging_spt.toml    ← TOML；SPT 專屬：[pipeline.spt], [spt], [spt_status_label_rules], ...
├── stagging_spx.toml    ← TOML；SPX 專屬：[pipeline.spx], [spx], [spx_column_defaults], ...
├── stagging_sct.toml    ← TOML；SCT 專屬：[pipeline.sct], [sct], [sct_erm_status_rules], ...（Phase 12 新增）
└── paths.toml           ← TOML；檔案路徑與讀取參數（由 get_paths_config() 存取）
```

另有 `run_config.toml`（專案根目錄），由 `main_pipeline.py` 獨立讀取，不歸 ConfigManager 管轄。

---

## 2. 用途

### 2.1 模組在系統中的定位

```
┌─────────────────────────────────────────────────┐
│           UI / Tasks / Core / Runner             │
│  任何需要讀取配置的模組                           │
│  from accrual_bot.utils.config import config_manager │
└───────────────┬─────────────────────────────────┘
                │  統一存取點（Facade）
┌───────────────▼─────────────────────────────────┐
│         ConfigManager（singleton）               │
│  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ _config      │  │ _config_toml             │  │
│  │ (ConfigParser│  │ (dict，stagging*.toml    │  │
│  │  INI)        │  │  deep-merged)            │  │
│  └──────────────┘  └──────────────────────────┘  │
│  ┌──────────────────────────────────────────────┐ │
│  │ _paths_toml (dict，paths.toml)               │ │
│  └──────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### 2.2 主要使用場景（依頻率排序）

| 場景 | API | 說明 |
|------|-----|------|
| 讀取 pipeline 啟用步驟 | `get_nested('pipeline', 'spt', 'enabled_po_steps')` | Orchestrator 最常用 |
| 讀取整個業務規則段落 | `get_all('spx')` / `get_all('spt')` | condition engine, evaluation step |
| 讀取巢狀 TOML 值 | `get_nested('previous_workpaper_integration', 'column_patterns')` | common steps |
| 讀取 FA 帳戶列表 | `get_nested('fa_accounts', 'spx')` | ERM evaluation base class |
| 讀取檔案路徑參數 | `get_paths_config('spx', 'po', 'params')` | 各 loading step |
| 讀取正規表達式樣式 | `get_regex_patterns()` | date parsing steps |
| 讀取 Google Sheets 憑證 | `get_credentials_config()` | GoogleSheetsSource |
| 讀取透視表配置 | `get_pivot_config('SPT', 'po')` | export steps |
| INI 鍵值讀取 | `get('GENERAL', 'pt_ym')` | legacy 相容 |
| dot-notation 讀取 | `get('pipeline.spt.enabled_po_steps')` | 彈性存取 |

### 2.3 constants.py 的定位

`constants.py` 存放**不依賴配置文件的靜態常數**，在 import 時就確定、永不改變：

- 檔案格式常數（`SUPPORTED_FILE_EXTENSIONS`）
- 業務狀態值（`STATUS_VALUES`）
- 欄位名稱（`COMMON_COLUMNS`）
- Google Sheets Sheet ID（`SPX_CONSTANTS['CLOSING_SHEET_ID']`）
- 效能參數（`PERFORMANCE_SETTINGS`, `CONCURRENT_SETTINGS`）

這些常數與 TOML/INI 配置的本質差別在於：它們**不需要跨環境差異化**，也**不需要讓業務人員在不懂 Python 的情況下修改**，因此不放入配置文件。

---

## 3. 設計思路

### 3.1 單例模式（Singleton Pattern）+ 雙重檢查鎖定

```python
class ConfigManager:
    _instance = None
    _initialized = False
    _lock = threading.Lock()  # 類級別，全域唯一

    def __new__(cls):
        if cls._instance is None:          # 第一次檢查（無鎖，效能快速路徑）
            with cls._lock:
                if cls._instance is None:  # 第二次檢查（有鎖，確保唯一性）
                    cls._instance = super().__new__(cls)
        return cls._instance
```

**為什麼需要雙重檢查？**

| 方案 | 問題 |
|------|------|
| 完全不加鎖 | 多執行緒可能各自建立實例 |
| `with cls._lock` 包住整個 `__new__` | 效能差：每次 `ConfigManager()` 都要競爭鎖 |
| 雙重檢查鎖定（DCL） | 只有第一次（`_instance is None`）才進入臨界區，後續無鎖 |

`_initialized` 標誌防止 `__init__` 在同一實例上執行多次（因為 `__new__` 返回已存在實例時，Python 仍然會呼叫 `__init__`）：

```python
def __init__(self):
    if self._initialized:    # 快速短路
        return
    ...
    self._initialized = True
```

### 3.2 Facade 模式（外觀模式）

ConfigManager 對外隱藏了三種底層存取機制的差異：

```
呼叫者 → config_manager.get_nested('pipeline', 'spt', ...) → _config_toml (dict)
呼叫者 → config_manager.get('GENERAL', 'pt_ym')            → _config_data (configparser)
呼叫者 → config_manager.get_paths_config('spx', 'po')      → _paths_toml (dict)
```

外部程式碼不需要知道資料存在 INI 還是 TOML，也不需要知道哪個 TOML 文件提供了哪個 section。

### 3.3 漸進式路徑解析策略（Graceful Degradation）

`_load_config()` 對 INI 文件的路徑搜尋採用「優先級降序，依序嘗試」策略：

```
Priority 1: 基於 __file__ 推算（最可靠，CI/開發環境）
Priority 2: cwd / accrual_bot / config / config.ini
Priority 3: cwd / config / config.ini
Priority 4: resolve_flexible_path()（爬父目錄找 accrual_bot 層）
Priority 5: ZIP 壓縮包中的 config.ini（離線/部署環境備用）
Priority 6: 預設硬編碼配置（最終防線）
```

這種策略確保系統在任何執行環境下都能啟動，不會因路徑問題崩潰。

### 3.4 Deep-Merge 策略（三 TOML 合一）

Phase 7 引入的設計：將三個 TOML 文件按順序合併到同一個 `_config_toml` dict：

```
stagging.toml     → 基礎 dict
    ↓ deep_merge
stagging_spt.toml → 新增 SPT-only sections
    ↓ deep_merge
stagging_spx.toml → 新增 SPX-only sections
    ↓
_config_toml      → 包含所有 sections，結構與舊版 stagging.toml 完全相同
```

```python
@staticmethod
def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = ConfigManager._deep_merge(result[key], value)
        else:
            result[key] = value
    return result
```

關鍵性質：三個 TOML 的頂層 section 不重疊（`[pipeline.spt]` 只在 `stagging_spt.toml`，`[spx]` 只在 `stagging_spx.toml`），所以 deep-merge 等價於「合併不同層級的字典」，**不存在 override 覆蓋衝突**。

### 3.5 循環導入防禦（Circular Import Guard）

ConfigManager 在初始化時需要日誌功能，但如果直接 `from accrual_bot.utils.logging import get_logger`，會導致循環導入（logging 模組可能間接 import config_manager）。解決方案：

```python
def _setup_simple_logger(self) -> None:
    # 使用 Python 內建 logging，不依賴自定義日誌系統
    self._simple_logger = logging.getLogger('config_manager')
    ...
```

再加三個私有方法 `_log_info`, `_log_warning`, `_log_error` 作為防禦性包裝，當 `_simple_logger` 尚未初始化時直接 `sys.stdout.write()`。

### 3.6 `__init__.py` 的 `from .constants import *`

`constants.py` 使用 `import *` 匯出，這是少數合理使用 `import *` 的場景——所有常數以模組名稱作為命名空間（`ENTITY_TYPES`, `STATUS_VALUES` 等），不會有命名衝突風險，且調用端不需要加 `constants.` 前綴。`__all__` 明確列出 28 個公開符號，確保 `import *` 行為可預期。

---

## 4. 各項知識點

### 4.1 雙重檢查鎖定（DCL）的 Python 細節

Python 的 DCL 與 Java/C++ 的實作有重要差異：

**Python 不需要 `volatile`（或 `AtomicReference`）**：Python 的 GIL（Global Interpreter Lock）保證了物件引用的設置是原子操作。因此 `cls._instance = super().__new__(cls)` 在 GIL 保護下不會出現「物件已分配但尚未完全初始化」的 Java 式問題。

```python
# ✓ Python 的 DCL 是安全的
if cls._instance is None:
    with cls._lock:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
```

但 `__init__` 的 `_initialized` 保護**沒有鎖**：

```python
def __init__(self):
    if self._initialized:  # ← 無鎖讀取 class attribute
        return
    ...
    self._initialized = True  # ← 無鎖寫入 instance attribute
```

在極端競爭下（兩執行緒同時通過 `__new__` 且同時讀到 `_initialized = False`），`_load_config()` 可能被呼叫兩次。由於 `_load_config()` 只做讀取操作（讀檔案、建 dict），重複執行不會損壞狀態，但會浪費 I/O。實務上 GIL 幾乎不讓這種情況發生。

### 4.2 `sys._MEIPASS`：PyInstaller 打包環境支援

```python
if getattr(sys, 'frozen', False):
    base_dir = sys._MEIPASS  # PyInstaller 解壓暫存目錄
    config_path = os.path.join(base_dir, 'config.ini')
```

PyInstaller 將程式打包為 EXE 時，資源文件被解壓到 `sys._MEIPASS`（通常是 `%TEMP%/_MEIXXXXX/`）。`getattr(sys, 'frozen', False)` 是標準檢測方式，因為 `sys.frozen` 只有在打包環境才存在。`get_resource_path()` 也使用相同邏輯。

### 4.3 `tomllib` vs `tomli`

```python
import tomllib  # Python 3.11+ 標準庫
```

`tomllib` 是 Python 3.11 加入的 TOML 讀取器（read-only）。本模組直接使用標準庫版本，無需安裝第三方 `tomli`。**只支援讀取，不支援寫入**（TOML 寫入需要 `tomli_w`，但本系統不需要在執行期寫回 TOML）。

使用時必須以 binary mode 開啟（`open(..., 'rb')`）：

```python
with open(get_toml_path('stagging.toml'), 'rb') as f:
    self._config_toml = tomllib.load(f)
```

這也是 TOML 字面量字串（`'...'`）中反斜線（`\d`, `\s`）能正確保留的原因——`tomllib` 的解析層面保留了 TOML 字面量語義。

### 4.4 `Path.cwd()` vs `Path(__file__).parent`

程式碼中混用了兩種路徑基準：

```python
Path(__file__).parent.parent.parent / 'config' / 'config.ini'  # 相對於 config_manager.py 本身
Path.cwd() / 'accrual_bot' / 'config' / 'config.ini'          # 相對於執行時工作目錄
```

兩者語意完全不同：

| 方法 | 語意 | 風險 |
|------|------|------|
| `Path(__file__).parent` | 靜態，取決於 .py 文件位置 | 幾乎不變，穩定 |
| `Path.cwd()` | 動態，取決於執行時 `os.chdir()` | 若有 cd 操作則失效 |

多路徑候選的設計就是為了覆蓋兩者可能失效的情形。

### 4.5 `configparser` INI 解析特性

```python
self._config.read(config_path, encoding='utf-8')
```

`configparser` 有幾個需要注意的特性，影響本模組的行為：

1. **鍵名自動小寫**：`configparser` 預設將所有 option keys 轉為小寫（`optionxform = str.lower`），所以 `GENERAL.pt_YM` 讀出後 key 為 `pt_ym`，這就是為何 `get_regex_patterns()` 寫 `general_section.get('pt_ym')` 而非 `'pt_YM'`

2. **DEFAULT section 穿透**：`configparser` 的 `[DEFAULT]` section 值會被所有 section 繼承，這裡未使用此特性

3. **字串型別**：所有 INI 值均為字串，故有 `get_int()`, `get_float()`, `get_boolean()` 等型別轉換方法

### 4.6 `get()` 的統一查詢行為（TOML 優先，INI fallback）✅ 已修復（2026-03-17）

> **此問題已修復。** `get()` 現在同時查詢 TOML 和 INI，且支援大小寫不敏感的 TOML 段落查詢。

**歷史紀錄**：原實作的 `get(section, key)` 只查詢 INI `_config_data`，TOML-only 的值（如 `closing_list_spreadsheet_id`）會返回 `None`。

**修復後的實作**：

```python
def _get_toml_section(self, section: str) -> Optional[dict]:
    """從 TOML 配置中取得段落，支援大小寫不敏感查詢"""
    result = self._config_toml.get(section)
    if result is not None:
        return result
    return self._config_toml.get(section.lower())

def get(self, section: str, key: str = None, fallback: Any = None) -> Any:
    # Case 1: dot-notation TOML 存取
    if key is None and '.' in section:
        parts = section.split('.')
        value = self._config_toml
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part, {})
            else:
                return fallback
        return value if value != {} else fallback

    # Case 2: 兩參數形式 get('SPX', 'key') — TOML 優先，INI fallback
    if key is not None:
        toml_section = self._get_toml_section(section)
        if isinstance(toml_section, dict) and key in toml_section:
            return toml_section[key]
        ini_value = self._config_data.get(section, {}).get(key)
        if ini_value is not None:
            return ini_value
        return fallback

    return self._config_data.get(section, {}).get(key, fallback)
```

**關鍵改進**：
- TOML 段落查詢支援大小寫不敏感（`'SPX'` 可匹配 `[spx]`）
- 兩參數形式 `get('SPX', 'key')` 先查 TOML 再 fallback 到 INI
- `get_list()`、`get_boolean()`、`get_section()`、`has_section()`、`has_option()`、`get_all()` 同步修復，均改為 TOML 優先 + INI fallback

### 4.7 `get_nested()` vs `get_all()` 的差異

```python
# get_nested：嚴格多鍵遞歸，找不到時回 fallback
get_nested('pipeline', 'spt', 'enabled_po_steps')
# → self._config_toml['pipeline']['spt']['enabled_po_steps']

# get_all：取整個 section（TOML 優先，INI fallback）
get_all('spx')
# → self._config_toml.get('spx')  （整個 spx dict）
```

兩者的共同限制：只查詢 `_config_toml`（`get_all()` 有 INI fallback，但 TOML 中若 key 存在且值為 `None` 則不 fallback）。`_paths_toml` 完全獨立，只能透過 `get_paths_config()` 存取。

### 4.8 `resolve_flexible_path()`：爬父目錄搜尋

```python
# 往上找到 accrual_bot 目錄
while ref_dir.name != 'accrual_bot' and ref_dir.parent != ref_dir:
    ref_dir = ref_dir.parent
```

這是路徑解析的「智能爬樹」策略：從 `__file__` 所在目錄開始，向上爬直到找到名為 `accrual_bot` 的目錄。`ref_dir.parent != ref_dir` 是到達根目錄的終止條件（在 POSIX 系統根目錄 `/` 的 `parent` 是自身）。

**潛在問題**：若目錄名稱不含 `accrual_bot` 字串（例如重命名了目錄），則此邏輯會爬到根目錄後停止，返回 `None`，觸發下一個候選路徑。

### 4.9 ZIP 備用讀取機制

```python
root_url = r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\accrual_bot.zip'  # 硬編碼！
ini_in_zip_path = 'accrual_bot/config/config.ini'
with zipfile.ZipFile(root_url, 'r') as zf:
    ini_bytes = zf.read(ini_in_zip_path)
    ini_string = ini_bytes.decode('utf-8')
    self._config.read_string(ini_string)
```

這是為特殊部署環境（如 Google Colab 或無法直接安裝的機器）設計的備用機制。`configparser.read_string()` 接受字串而非檔案路徑，可以直接解析從 ZIP 讀出的內容。

**設計缺陷**：`root_url` 是 Windows 的絕對路徑，在 macOS/Linux 或不同磁碟機字母下必然失敗。此備用路徑應改為讀自環境變數或 config。

### 4.10 `to_dict()` 的命名空間設計

```python
def to_dict(self) -> Dict:
    result = dict(self._config_data)    # INI 配置（頂層 key = section name）
    if self._config_toml:
        result['_toml'] = self._config_toml.copy()   # 加下底線避免衝突
    if self._paths_toml:
        result['_paths'] = self._paths_toml.copy()
    return result
```

用 `_toml` 和 `_paths` 前綴（加底線）作為命名空間，避免與 INI section 名稱衝突（INI section 名 `GENERAL` / `LOGGING` 等不以底線開頭）。這是一個簡單但有效的命名空間策略。

### 4.11 `_set_default_config()` 的轉義問題

```python
def _set_default_config(self) -> None:
    self._config_data = {
        'GENERAL': {
            'pt_ym': r'(\\d{4}\\/(0[1-9]|1[0-2])(\\s|$))',  # ← 雙重轉義！
```

此處使用的是 raw string `r'...'`，但裡面的 `\\d` 在 Python raw string 中代表兩個字符 `\` 和 `\`，即最終字串包含 `\\d`。然而正規表達式應只有一個反斜線（`\d`）。這是一個**潛在的回退配置錯誤**：只要 INI 文件讀取失敗且使用了此預設配置，日期格式比對將全部失敗。

對比 `stagging.toml` 的正確寫法：

```toml
pt_YM = '''(\d{4}\/(0[1-9]|1[0-2])(\s|$))'''  # TOML 字面量字串，\d 不被處理
```

### 4.12 `get_toml_path()` 的路徑重建邏輯

```python
def get_toml_path(file_name: str) -> Path:
    current_dir = Path(__file__).parent  # utils/config/
    parts = list(current_dir.parts)
    # 移除 ['utils', 'config'] 兩層
    index = parts.index('utils')
    if parts[index:index + 2] == ['utils', 'config']:
        del parts[index:index + 2]
    new_path = Path(parts[0]).joinpath(*parts[1:])  # .../accrual_bot/
    return new_path / 'config' / file_name
```

這個函數是**定義在 `_load_config()` 內部的閉包**（nested function），不可從外部呼叫。它的路徑重建邏輯：從 `utils/config/config_manager.py` 的路徑中剝掉 `utils/config` 兩層，得到 `accrual_bot/` 根目錄，再拼上 `config/` 和文件名。

這依賴模組在套件結構中的固定位置（`accrual_bot/utils/config/`），若未來重組目錄則需更新此邏輯。

---

## 5. 應用範例

### 5.1 基礎：讀取 TOML 配置段落

```python
from accrual_bot.utils.config import config_manager

# 讀取 SPT pipeline 啟用的步驟列表
enabled_steps = config_manager.get_nested('pipeline', 'spt', 'enabled_po_steps')
# → ['SPTDataLoading', 'ProductFilter', 'ColumnAddition', ...]

# 讀取整個 spx 業務規則字典
spx_config = config_manager.get_all('spx')
# → {'kiosk_suppliers': [...], 'locker_suppliers': [...], ...}

# 讀取 fa_accounts（共用基類 BaseERMEvaluationStep 使用）
fa_spx = config_manager.get_nested('fa_accounts', 'spx')
# → ['650005', '610104', ...]
```

### 5.2 進階：讀取 paths.toml 動態路徑

```python
# 讀取 SPX PO 原始資料的讀取參數
spx_po_params = config_manager.get_paths_config('spx', 'po', 'params')
# → {'raw_po': {'encoding': 'utf-8', 'sep': ',', 'dtype': 'str'},
#    'previous': {'sheet_name': 0, 'header': 0, 'dtype': 'str'}, ...}

# 讀取 SPX PO 的原始資料路徑模板
spx_po_paths = config_manager.get_paths_config('spx', 'po')
# → {'raw_po': '{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv', ...}
```

### 5.3 type-safe 讀取（INI 配置）

```python
# 讀取整數
max_workers = config_manager.get_int('CONCURRENT', 'max_workers', fallback=5)

# 讀取布林值（支援 'true', '1', 'yes', 'on'）
debug_mode = config_manager.get_boolean('LOGGING', 'debug', fallback=False)

# 讀取逗號分隔列表
fa_accounts = config_manager.get_list('FA_ACCOUNTS', 'spx')
# INI 中: spx = 650005, 610104, 630001
# → ['650005', '610104', '630001']
```

### 5.4 憑證路徑（彈性解析）

```python
credentials = config_manager.get_credentials_config()
# → {
#     'certificate_path': '/absolute/resolved/path/to/credentials.json',
#     'scopes': ['https://www.googleapis.com/auth/spreadsheets.readonly']
#   }

# 手動解析路徑
from accrual_bot.utils.config.config_manager import resolve_flexible_path
resolved = resolve_flexible_path('./secret/credentials.json', __file__)
```

### 5.5 dot-notation 語法（TOML）

```python
# 等效寫法：兩種都可以取得相同值
v1 = config_manager.get_nested('pipeline', 'spt', 'enabled_po_steps')
v2 = config_manager.get('pipeline.spt.enabled_po_steps')

# 但 get_nested 更清晰且有明確 fallback 語意
assert v1 == v2
```

### 5.6 透視表配置讀取

```python
# 獲取 SPT PO 透視表配置
pivot_config = config_manager.get_pivot_config('SPT', 'po')
# → {
#     'index': ['Department', 'Account Name', ...],
#     'sm_cr_pivot_cols': [...],
#     'ga_cr_pivot_cols': [...],
#     'pivot_value_col': 'Accr. Amount'
#   }
```

### 5.7 使用 constants 模組

```python
from accrual_bot.utils.config import STATUS_VALUES, COMMON_COLUMNS, ENTITY_TYPES

# 使用狀態值常數（避免魔術字串）
df.loc[mask, status_col] = STATUS_VALUES['COMPLETED']   # '已完成'
df.loc[err_mask, status_col] = STATUS_VALUES['FORMAT_ERROR']  # '格式錯誤'

# 使用欄位名稱常數
amount_col = COMMON_COLUMNS['ACCR_AMOUNT']  # 'Accr. Amount'

# 使用實體類型常數
entity = ENTITY_TYPES['SPX']  # 'SPXTW'
```

### 5.8 執行期動態設定配置（測試中使用）

```python
# 在測試中覆蓋某個配置值（不寫入檔案）
config_manager.set_config('GENERAL', 'pt_ym', r'(\d{4}\/test_pattern)')

# 重新載入配置（重新讀取所有 TOML/INI 文件）
config_manager.reload_config()

# 取得完整配置快照（用於診斷）
full_config = config_manager.to_dict()
print(full_config.keys())
# dict_keys(['GENERAL', 'LOGGING', 'CREDENTIALS', 'FA_ACCOUNTS', ..., '_toml', '_paths'])
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ 優 1：真正線程安全的單例（Double-Checked Locking）

透過類級別 `threading.Lock()` 與雙重 `_instance is None` 檢查，在 CPython GIL 環境下提供了可靠的執行緒安全性。100 執行緒並發存取測試通過（見 `test_config_manager.py`）。這是從「無鎖裸奔」升級到「正確 DCL」的關鍵改進。

#### ✅ 優 2：透明的多 TOML 聚合（Deep-Merge）

呼叫端完全不需要知道 `spt_account_prediction.rules` 來自 `stagging_spt.toml` 或 `spx_column_defaults` 來自 `stagging_spx.toml`。`_config_toml` 呈現統一視圖，35 個現有存取點零改動。這是「對擴充開放，對修改封閉」原則在配置管理層面的體現。

#### ✅ 優 3：漸進式路徑回退策略

5 層候選路徑 + ZIP 備用 + 硬編碼預設配置，確保系統在任何環境（dev / packaged / Colab）都不會因路徑問題啟動失敗。這在多環境部署系統中是關鍵的韌性設計。

#### ✅ 優 4：型別安全的存取 API

`get_int()`, `get_float()`, `get_boolean()`, `get_list()` 統一處理 INI 字串到 Python 型別的轉換，呼叫端不需要自行 `int(config.get(...))`，減少型別錯誤。

#### ✅ 優 5：循環導入防禦

`_setup_simple_logger()` 使用內建 `logging` 而非自定義 logger，`_log_*` 私有方法在 logger 未準備好時 fallback 到 `sys.stdout.write()`。這防止了 ConfigManager 與 logging 模組之間的循環導入死鎖。

### 6.2 缺點與風險

#### ❌ 缺 1：硬編碼 Windows 絕對路徑（嚴重）

**位置**：`config_manager.py` 第 305 行

```python
root_url = r'C:\SEA\Accrual\prpo_bot\prpo_bot_renew_v2\accrual_bot.zip'
```

這個路徑只在特定開發機上有效，在任何其他機器（包括 CI/CD、其他開發者機器、macOS/Linux）都會立即失敗。雖然這只是備用路徑，但原始碼中出現具名人員路徑是嚴重的維護性問題，也是安全合規的隱患（路徑洩露組織結構）。

**修正方向**：讀取環境變數 `ACCRUAL_BOT_ZIP_PATH`，或從 TOML 配置讀取此路徑。

#### ❌ 缺 2：`__init__` 的 `_initialized` 保護無鎖（低概率競態）

**位置**：`config_manager.py` 第 119–130 行

`__init__` 讀寫 `_initialized` 不在 `_lock` 保護下，理論上兩執行緒可能同時通過 `if self._initialized: return` 並各自呼叫 `_load_config()`，造成配置被重複載入（雖然結果相同）。

**修正方向**：

```python
def __init__(self):
    with self.__class__._lock:
        if self._initialized:
            return
        ...
        self._initialized = True
```

#### ❌ 缺 3：`reload_config()` 非執行緒安全（中等風險）

**位置**：第 846–850 行

```python
def reload_config(self) -> None:
    self._initialized = False   # ← 無鎖寫入
    self._load_config()
    self._initialized = True
```

在並發環境中，`_initialized = False` 寫入後到 `_load_config()` 完成之間，其他執行緒呼叫 `ConfigManager()` 會觸發新的 `__init__()` 執行，導致兩個 `_load_config()` 並發運行，最終寫入結果不確定。

**修正方向**：加鎖整個 reload 過程，或使用讀寫鎖（`threading.RLock`）。

#### ✅ 缺 4：`get()` API 語意不一致（已修復，2026-03-17）

> **此問題已修復。** `get(section, key)` 現在同時查詢 TOML（優先）和 INI（fallback），不再是「只走 INI」。`get('a.b.c')` dot-notation 仍走 TOML，語意差異仍在但實務影響已大幅降低——無論用哪種語法，TOML 值都能被正確讀取。

#### ❌ 缺 5：`_set_default_config()` 的正規表達式雙重轉義

**位置**：第 467–482 行

```python
'pt_ym': r'(\\d{4}\\/(0[1-9]|1[0-2])(\\s|$))',
```

raw string 中的 `\\d` 是兩個字元（反斜線＋反斜線），而非一個反斜線。這個 fallback 配置下的日期正則將永遠無法正確比對，但只在 INI 文件讀取失敗時才會使用，故平時不被注意。

#### ❌ 缺 6：`constants.py` 與 TOML 的重複定義

`REGEX_PATTERNS` 常數（定義在 `constants.py`）與 `[general]` section 中的 `pt_YM` 等正規表達式樣式（定義在 `stagging.toml`）存在語義重疊。業務程式碼可能不一致地使用兩者之一，造成難以察覺的行為差異。

此外，`SPX_CONSTANTS['CLOSING_SHEETS']` 中的年份列表（`['2023年_done', '2024年', '2025年']`）是硬編碼，每年需手動更新。

#### ❌ 缺 7：模組層級立即執行（側效應）

```python
# config_manager.py 底部
config_manager = ConfigManager()  # ← import 時立即執行！
```

這行程式碼在 `import accrual_bot.utils.config` 時立即觸發：文件 I/O（讀取 TOML/INI）、logging 初始化、磁碟掃描（路徑候選搜尋）。這些 I/O 操作在 import 時發生，不符合「import 應該是惰性且無副作用」的原則，且會在測試中造成不必要的環境依賴。

**修正方向**：採用懶加載（lazy initialization），第一次呼叫 `config_manager.get(...)` 時才初始化。

#### ✅ 缺 8：`get_all()` / `get_section()` 的 TOML/INI 查詢不對稱（已修復，2026-03-17）

> **此問題已修復。** `get_section()` 和 `get_all()` 現在都透過 `_get_toml_section()` 先查 TOML（大小寫不敏感），找不到時 fallback 到 INI。`has_section()` 和 `has_option()` 同步修復，行為一致。

---

## 7. 延伸議題

### 7.1 執行緒安全升級：讀寫鎖（RWLock）

當前的 `threading.Lock()` 是互斥鎖——即使只是讀取也需要排隊。對於讀取遠多於寫入的配置管理場景，可改用讀寫鎖：

```python
from threading import RLock

class ConfigManager:
    _rlock = RLock()    # 允許同一執行緒重入

    def get_nested(self, *keys, fallback=None):
        # 讀操作不需要鎖（dict 讀取是 GIL 保護的）
        ...

    def reload_config(self):
        with self._rlock:   # 寫操作需要互斥
            self._load_config()
```

事實上，CPython 的 dict 讀取由 GIL 保護，對純讀取操作加鎖是多餘的。真正需要鎖的只有寫入操作（`reload_config()` 和 `set_config()`）。

### 7.2 不可變配置快照（Immutable Configuration Snapshot）

目前 `_config_toml` 是普通可變 dict，`set_config()` 可在執行期修改配置，這可能造成不同 pipeline step 看到不同的配置狀態（特別是 `reload_config()` 中途）。

改進方向：提供 `freeze()` 方法，返回 `types.MappingProxyType` 包裝的不可變視圖：

```python
import types

def get_immutable_snapshot(self) -> types.MappingProxyType:
    return types.MappingProxyType(self._config_toml)
```

### 7.3 配置版本控制與熱更新

TOML 文件的 `stagging.toml` 可能在執行期被業務人員修改（如調整 pipeline 步驟）。目前 `reload_config()` 需要手動呼叫；可加入文件監看（file watching）機制：

```python
from watchdog.observers import Observer

def start_config_watch(self):
    """監看配置文件變更，自動觸發 reload"""
    observer = Observer()
    observer.schedule(ConfigChangeHandler(self), path=config_dir, recursive=False)
    observer.start()
```

這在 Streamlit UI 環境下特別有價值——業務人員可以修改 TOML 並即時看到效果。

### 7.4 Schema 驗證：TOML 配置結構保護

當前系統沒有對 TOML 配置進行結構驗證。若業務人員誤刪某個 section 或拼錯 key，pipeline 會在執行時才失敗（`get_nested()` 回傳 `None`，造成難以診斷的 NullPointerError）。

改進方向：使用 `pydantic` 定義 schema，在載入時驗證：

```python
from pydantic import BaseModel

class PipelineSPTConfig(BaseModel):
    enabled_po_steps: list[str]
    enabled_pr_steps: list[str]
    enabled_procurement_po_steps: list[str]

class StaggingConfig(BaseModel):
    pipeline: dict[str, PipelineSPTConfig | dict]
    ...

# 在 _load_config() 中驗證
StaggingConfig(**self._config_toml)  # 若格式錯誤立即 raise ValidationError
```

### 7.5 環境變數覆蓋機制（Twelve-Factor App）

參考 [12-Factor App](https://12factor.net/config) 原則，配置應可透過環境變數覆蓋，方便 CI/CD 注入：

```python
def get_nested(self, *keys, fallback=None):
    # 環境變數優先（例如 ACCRUAL_PIPELINE_SPT_ENABLED_PO_STEPS）
    env_key = 'ACCRUAL_' + '_'.join(keys).upper()
    env_val = os.environ.get(env_key)
    if env_val:
        return json.loads(env_val)

    # 再查 TOML
    ...
```

### 7.6 單元測試隔離問題

`ConfigManager` 是 import 時立即初始化的全局單例，在測試中難以重置或替換。目前 `tests/unit/utils/config/test_config_manager.py` 只測試單例唯一性和執行緒安全，沒有測試「配置值是否正確讀取」，因為重置單例很困難。

改進方向：提供 `reset_for_testing()` 類方法（僅在 `pytest` 環境可用）：

```python
@classmethod
def reset_for_testing(cls):
    """僅供測試使用：重置單例狀態"""
    with cls._lock:
        cls._instance = None
        cls._initialized = False
```

或改用依賴注入（DI），讓 pipeline 步驟接受 `config_manager` 作為建構參數，測試時傳入 mock。

---

## 8. 其他

### 8.1 模組結構總覽

```
accrual_bot/utils/config/
├── __init__.py          (29 行)  ← 公開 API：ConfigManager + config_manager + 所有常數
├── constants.py        (114 行)  ← 靜態常數（compile-time，不依賴配置文件）
└── config_manager.py   (854 行)  ← 主體：ConfigManager 類 + 2 個模組函數

配置文件（被管理，不屬於本模組）：
accrual_bot/config/
├── config.ini           ← INI；legacy；不會再新增 key
├── stagging.toml        ← TOML；共用業務規則
├── stagging_spt.toml    ← TOML；SPT 專屬
├── stagging_spx.toml    ← TOML；SPX 專屬
└── paths.toml           ← TOML；檔案路徑與讀取參數
```

### 8.2 ConfigManager 公開 API 彙整

| 方法 | 存取目標 | 說明 |
|------|---------|------|
| `get(section, key, fallback)` | TOML 優先，INI fallback / TOML dot-notation | 通用讀取（2026-03-17 修復：TOML 優先） |
| `get_int(section, key, fallback)` | TOML 優先，INI fallback | 型別轉換：整數 |
| `get_float(section, key, fallback)` | TOML 優先，INI fallback | 型別轉換：浮點數 |
| `get_boolean(section, key, fallback)` | TOML 優先，INI fallback | 型別轉換：布林值（支援 TOML 原生 bool） |
| `get_list(section, key, separator, fallback)` | TOML 優先，INI fallback | 型別轉換：列表（支援 TOML 原生陣列） |
| `get_section(section)` | TOML 優先，INI fallback | 取整個 section dict（2026-03-17 修復） |
| `get_all(section, subsection)` | TOML 優先，INI fallback | 取整個 section/subsection |
| `get_nested(*keys, fallback)` | TOML only | 多層巢狀鍵存取 |
| `get_paths_config(*keys)` | paths.toml only | paths.toml 專屬存取 |
| `get_path(section, key, fallback)` | INI / TOML | 回傳 Path 物件 |
| `get_fa_accounts(entity_type)` | INI (`FA_ACCOUNTS`) | FA 帳戶列表 |
| `get_pivot_config(entity_type, data_type)` | INI | 透視表配置 |
| `get_regex_patterns()` | INI (`GENERAL`) | 正規表達式 dict |
| `get_credentials_config()` | INI (`CREDENTIALS`) | 憑證配置（含路徑解析） |
| `get_resolved_path(section, key, fallback)` | INI | 路徑配置（彈性解析） |
| `has_section(section)` | TOML + INI | section 存在性檢查（2026-03-17 修復） |
| `has_option(section, key)` | TOML + INI | key 存在性檢查（2026-03-17 修復） |
| `set_config(section, key, value)` | INI | 執行期動態設定（不持久化） |
| `reload_config()` | 所有 | 重新載入所有配置文件 |
| `to_dict()` | 所有 | 完整配置快照 |

### 8.3 已知問題快速索引

| 編號 | 嚴重度 | 位置 | 問題描述 | 修正方向 |
|------|--------|------|---------|---------|
| BUG-1 | 🔴 高 | line 305 | 硬編碼 Windows 路徑 | 改用環境變數 |
| BUG-2 | 🟡 中 | line 846 | `reload_config()` 非執行緒安全 | 加 `_lock` |
| BUG-3 | 🟡 中 | line 469 | 預設 regex 雙重反斜線 | 移除多餘 `r'...'` 前綴 |
| DESIGN-1 | 🟡 中 | line 119 | `_initialized` 無鎖初始化競態 | `__init__` 加鎖 |
| DESIGN-2 | ✅ 已修復 | line 484 | ~~`get()` 雙重行為~~ | **已修復（2026-03-17）**：`get()` 統一為 TOML 優先 + INI fallback |
| DESIGN-3 | 🟢 低 | line 854 | import 時立即執行 I/O | 懶加載 |
| DESIGN-4 | 🟢 低 | constants.py | `REGEX_PATTERNS` 與 TOML 重複 | 選一為主，刪除另一 |
| DESIGN-5 | ✅ 已修復 | `get_section()` | ~~只查 INI，與 `get_all()` 行為不一致~~ | **已修復（2026-03-17）**：`get_section()` 改為 TOML 優先 + INI fallback |

### 8.4 constants.py 常數分類

| 常數名 | 類型 | 用途 |
|--------|------|------|
| `REF_PATH_MOB`, `REF_PATH_SPT` | `str` | Google Drive 參考路徑（已部分棄用） |
| `SUPPORTED_FILE_EXTENSIONS` | `list[str]` | 支援的輸入格式 |
| `EXCEL_EXTENSIONS`, `CSV_EXTENSIONS` | `list[str]` | 格式子集 |
| `ENTITY_TYPES` | `dict[str, str]` | 實體代碼對應 |
| `PROCESSING_MODES` | `dict[str, str]` | 8 種處理模式組合（部分未使用） |
| `COMMON_COLUMNS` | `dict[str, str]` | 標準欄位名稱 |
| `STATUS_VALUES` | `dict[str, str]` | 中英文狀態值對應 |
| `REGEX_PATTERNS` | `dict[str, str]` | 日期比對正則（與 TOML 重複） |
| `DEFAULT_DATE_RANGE` | `str` | 格式錯誤時的備用日期範圍 |
| `EXCEL_FORMAT` | `dict` | xlsxwriter 輸出設定 |
| `CONCURRENT_SETTINGS` | `dict` | 執行緒池大小與逾時設定 |
| `GOOGLE_SHEETS` | `dict` | Google API scope 與憑證文件名 |
| `SPX_CONSTANTS` | `dict` | SPX 專屬常數（Sheet ID、分類、供應商） |
| `PERFORMANCE_SETTINGS` | `dict` | 記憶體與分塊設定 |

### 8.5 測試覆蓋情況

> **2026-03-28 更新**：Phase 15 擴充 `test_config_manager.py`，覆蓋率提升至 ~69%。

| 測試案例 | 覆蓋內容 | 測試強度 |
|---------|---------|---------|
| `test_singleton_same_instance` | DCL 單例唯一性 | ✅ 充分 |
| `test_thread_safe_singleton` | 100 執行緒並發單例 | ✅ 充分 |
| `test_config_data_integrity` | `_config_toml` 存在且為 dict | 🟡 基本 |
| `test_concurrent_access_stress` | 1000 執行緒壓力測試 | ✅ 充分 |
| `test_get_*` | `get()`, `get_list()`, `get_dict()` 公開 API | ✅ 新增 |
| `test_reload_config` | `reload()` 重載邏輯 | ✅ 新增 |
| `test_load_config_paths` | `_load_config()` 多路徑解析 | ✅ 新增 |

**仍缺乏的測試**：
- `_set_default_config()` 的預設值正確性
- `get_credentials_config()` 的路徑解析
- PyInstaller 環境下的 `sys.frozen` 分支

整體測試覆蓋率：線程安全 ✅，功能正確性 ✅（主要 API 已覆蓋）
