# `accrual_bot/data/importers` 模組深度研究報告

> **研究日期**：2026-03-13
> **研究範圍**：`accrual_bot/data/__init__.py`、`accrual_bot/data/importers/__init__.py`、`accrual_bot/data/importers/base_importer.py`、`accrual_bot/data/importers/google_sheets_importer.py`（共 537 行）
> **研究方法**：逐行閱讀全部原始碼，交叉比對依賴模組（`constants.py`、`google_sheet_source.py`），反覆推敲設計意圖與潛在問題

---

## 目錄

1. [背景](#1-背景)
2. [用途](#2-用途)
3. [設計思路](#3-設計思路)
4. [各項知識點](#4-各項知識點)
5. [應用範例](#5-應用範例)
6. [優缺分析](#6-優缺分析)
7. [延伸議題](#7-延伸議題)
8. [其他觀察](#8-其他觀察)

---

## 1. 背景

### 1.1 模組定位與歷史脈絡

`accrual_bot/data/importers` 是整個 accrual_bot 系統的**歷史遺留資料導入層（Legacy Import Layer）**。從程式碼中可還原出其演化軌跡：

**第一代**（早期單體架構）：`data/importers/` 是系統唯一的資料存取入口，同時承擔 Excel/CSV 本地檔案讀取與 Google Sheets 雲端資料存取兩項職責。`GoogleSheetsImporter` 是直接對 Google API 的封裝，包含完整的業務邏輯（如 SPX 關單清單的讀取與欄位對應）。

**第二代**（架構重構後）：系統引入了 `accrual_bot/core/datasources/` 統一資料來源層，`GoogleSheetsSource` 成為新的標準介面，實作了 `DataSource` 抽象基類，支援 async/await、連線池、LRU 快取等現代特性。`data/importers/google_sheets_importer.py` 因此降格為**薄包裝層（Thin Wrapper）**，僅保留以向後兼容為目的。

**現況**：`BaseDataImporter`（本地檔案讀取）仍在各 pipeline 的 loading step 中被繼承使用，屬於**仍活躍的功能模組**；`GoogleSheetsImporter` 與 `AsyncGoogleSheetsImporter` 則已全面廢棄，僅因為舊呼叫點尚未遷移而存在。

### 1.2 在系統架構中的位置

```
┌──────────────────────────────────────────────────────┐
│                  Tasks Layer                          │
│   spt/steps/  spx/steps/  → 繼承 BaseLoadingStep      │
├──────────────────────────────────────────────────────┤
│                  Core Layer                           │
│   BaseLoadingStep → 繼承 BaseDataImporter ← [此模組]  │
│   datasources/GoogleSheetsSource（取代舊 Importer）   │
├──────────────────────────────────────────────────────┤
│                  Utils Layer                          │
│   helpers/file_utils, config/constants                │
└──────────────────────────────────────────────────────┘
```

`BaseDataImporter` 是 `BaseLoadingStep`（`core/pipeline/steps/base_loading.py`）的上游依賴，被所有 entity-specific 的資料載入步驟間接使用。

---

## 2. 用途

### 2.1 `BaseDataImporter`（`base_importer.py`，373 行）

本地檔案讀取的通用工具類別，涵蓋以下功能：

| 方法 | 功能 | 說明 |
|------|------|------|
| `import_file()` | 導入單檔 | 自動識別 Excel/CSV，計時並記錄 |
| `_import_excel()` | 內部 Excel 讀取 | 引擎降級策略（openpyxl → xlrd） |
| `_import_csv()` | 內部 CSV 讀取 | 編碼自動偵測（5 種編碼依序嘗試） |
| `import_multiple_files()` | 批量循序導入 | 單一失敗不中斷，繼續處理後續 |
| `concurrent_import_files()` | 批量並發導入 | ThreadPoolExecutor，可配置工作執行緒數與逾時 |
| `extract_date_and_month_from_filename()` | 從檔名提取日期 | 解析 YYYYMM 格式，回傳 `(int, int)` |
| `validate_dataframe()` | 驗證 DataFrame | 檢查空值、最少行數、必要欄位 |
| `get_import_statistics()` | 導入統計 | 計算檔案數、行數、欄位數、記憶體用量 |

### 2.2 `GoogleSheetsImporter`（`google_sheets_importer.py`，薄包裝層）

一個**已廢棄**的向後兼容包裝類別，繼承 `GoogleSheetsSource`（`core/datasources/`），於初始化時發出 `DeprecationWarning`。唯一保留的業務方法 `import_spx_closing_list()` 用於讀取 SPX 關單清單（但存在嚴重 Bug，詳見第 6 節）。

### 2.3 `AsyncGoogleSheetsImporter`

`GoogleSheetsImporter` 的子類別，名稱暗示非同步功能，但**完全不新增任何方法**，僅為舊版 API 提供另一個名稱。本質上是一個空殼類別（Shell Class）。

---

## 3. 設計思路

### 3.1 防禦性導入策略（Defensive Import）

`base_importer.py` 第 15–32 行展示了一種雙重導入保護機制：

```python
try:
    from ...utils import (
        get_logger, validate_file_path, is_excel_file, ...
    )
except ImportError:
    # 當相對導入失敗時（例如：直接執行此檔案），
    # 動態將 accrual_bot 根目錄加入 sys.path 後改用絕對導入
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from utils import (...)
```

這種設計允許 `base_importer.py` 在兩種情境下正常運作：
1. **作為套件使用**（正常情況）：相對導入 `from ...utils` 成功
2. **獨立執行**（debug/測試情境）：fallback 到 `sys.path` 修改後的絕對導入

**設計評估**：雖然這解決了開發便利性問題，但 `sys.path.insert(0, ...)` 是一種全域性的副作用操作，在複雜的多模組環境中可能引發難以追蹤的導入衝突（見[第 6 節優缺分析](#64-sys-path-副作用問題)）。

### 3.2 預設字串型別策略（Default `dtype=str`）

Excel 與 CSV 導入均預設 `dtype=str`：

```python
default_kwargs = {
    'engine': 'openpyxl',
    'dtype': str  # 預設為字符串類型，避免數據類型問題
}
```

這是一個在財務資料處理中相當常見且務實的決策：

- **避免自動型別推斷**：Pandas 讀取 CSV 時可能將 `000123` 變成 `123`，將日期字串轉換為 Timestamp，導致後續業務邏輯出錯
- **保留原始格式**：財務系統的 PO/PR 編號、會計科目代碼往往需要保持字串形式
- **延後型別轉換**：型別轉換由各 pipeline step 的業務邏輯明確處理，而非由 pandas 隱式完成

**代價**：所有欄位均為字串，後續需要數值計算時需要額外的 `pd.to_numeric()` 轉換步驟。

### 3.3 引擎降級策略（Engine Fallback）

`_import_excel()` 實作了 openpyxl → xlrd 的兩段式引擎降級：

```python
try:
    default_kwargs = {'engine': 'openpyxl', 'dtype': str}
    default_kwargs.update(kwargs)          # 用戶參數可覆蓋預設值
    return pd.read_excel(file_path, **default_kwargs)
except Exception as e:
    try:
        default_kwargs['engine'] = 'xlrd'  # 舊版 .xls 格式
        return pd.read_excel(file_path, **default_kwargs)
    except Exception:
        raise e                            # 重新拋出原始錯誤
```

設計意圖：`openpyxl` 是 pandas 對 `.xlsx` 的推薦引擎，但部分舊版 `.xls`（Excel 97-2003 格式）需要 `xlrd`。降級策略使導入器對兩種格式透明。

**注意**：若兩個引擎都失敗，`raise e` 重新拋出的是**第一次**（openpyxl）的錯誤，xlrd 的錯誤被靜默丟棄。這在診斷問題時可能造成誤導。

### 3.4 編碼自動偵測策略（Encoding Detection）

`_import_csv()` 實作了 5 種編碼的循序嘗試：

```python
try:
    return pd.read_csv(file_path, encoding=encoding, ...)  # 首先嘗試指定編碼（預設 utf-8）
except UnicodeDecodeError:
    for enc in ['utf-8-sig', 'big5', 'gbk', 'iso-8859-1']:
        try:
            default_kwargs['encoding'] = enc
            return pd.read_csv(file_path, **default_kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"無法使用任何編碼格式讀取檔案: {file_path}")
```

編碼順序的選擇反映了業務場景：
- `utf-8`：現代 CSV 預設
- `utf-8-sig`：Windows Excel 匯出 CSV 時常加入 BOM（Byte Order Mark）
- `big5`：台灣傳統中文編碼，財務系統常見
- `gbk`：中國大陸簡體中文
- `iso-8859-1`：Latin-1 安全回退（幾乎不會拋出 `UnicodeDecodeError`）

**精妙之處**：`keep_default_na=False` 配合 `na_values=['']` 的組合，只將空字串視為 NaN，防止 pandas 將 `'NA'`、`'N/A'`、`'None'` 等有效業務字串自動解讀為空值。

### 3.5 並發導入的設計取捨

`concurrent_import_files()` 使用 `ThreadPoolExecutor` 而非 `asyncio`：

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    future_to_file = {
        executor.submit(self._import_single_file_safe, path, name, config): name
        for path, name in valid_files
    }
    for future in concurrent.futures.as_completed(future_to_file, timeout=self.timeout):
        ...
```

**選擇 Thread 而非 async 的原因**：`pd.read_excel()` 和 `pd.read_csv()` 均為同步 I/O 操作，無法直接用 `await`。使用 `ThreadPoolExecutor` 可讓多個 I/O 密集型的檔案讀取真正並行，同時不需要改變 pandas API。

**`as_completed()` vs `futures` 順序迭代的差異**：`as_completed()` 在任意一個 future 完成時立即產出結果，無需等待所有 future。搭配 `timeout=300` 秒可防止掛起。

### 3.6 廢棄警告的實作方式

`GoogleSheetsImporter` 使用標準的 `warnings.warn()` 機制：

```python
def __init__(self, credentials_config: Dict[str, Any]):
    warnings.warn(
        "GoogleSheetsImporter 已廢棄，請改用 GoogleSheetsSource",
        DeprecationWarning,
        stacklevel=2,  # 警告指向呼叫方，而非此 __init__ 本身
    )
    super().__init__(credentials_config=credentials_config)
```

`stacklevel=2` 的重要性：若設為預設值 1，警告訊息會指向 `google_sheets_importer.py` 這一行，對使用者無意義；設為 2 則指向**呼叫 `GoogleSheetsImporter()` 的那行程式碼**，方便使用者定位需遷移的位置。

---

## 4. 各項知識點

### 4.1 `ThreadPoolExecutor` 與 GIL 的關係

Python 的 GIL（Global Interpreter Lock）限制了多執行緒在 CPU 密集型任務中的並行能力，但對 I/O 密集型任務（如磁碟讀取）**幾乎沒有影響**：在等待 I/O 回應時，執行緒會釋放 GIL，讓其他執行緒執行。因此 `ThreadPoolExecutor(max_workers=5)` 用於並發讀取多個檔案是合適的選擇。

若任務是 CPU 密集型（如大量資料轉換），應改用 `ProcessPoolExecutor`，但需注意 DataFrame 序列化的額外開銷。

### 4.2 `pd.read_csv()` 的 `na_values` 參數行為

pandas 預設的 `na_values` 集合包含約 20 個字串（`'NA'`、`'N/A'`、`'null'`、`'None'`、`'nan'` 等），這對財務資料而言常是陷阱。

`base_importer.py` 的設定：
```python
'keep_default_na': False,  # 關閉預設的 NA 字串辨識
'na_values': ['']          # 只有空字串才是 NaN
```

這確保了例如 PO 狀態欄位中的 `'N/A'` 字串不會被誤解為缺失值。

### 4.3 `Path.stem` vs `Path.name`

```python
file_name = Path(file_path).stem   # '/data/report_202501.xlsx' → 'report_202501'
# vs
file_name = Path(file_path).name   # '/data/report_202501.xlsx' → 'report_202501.xlsx'
```

`import_multiple_files()` 使用 `.stem`（不含副檔名）作為 `file_configs` 的 key，這意味著若目錄中有 `report.xlsx` 和 `report.csv`，兩者的 config 會發生衝突（後者覆蓋前者）。

### 4.4 `re.search()` vs `re.match()` 的差異

`extract_date_and_month_from_filename()` 使用 `re.search()`，它在字串的**任意位置**尋找匹配；`re.match()` 則只從字串**開頭**嘗試匹配。

對於檔名中嵌入的日期（如 `SPX_PO_202501_finalv2.xlsx`），`re.search()` 是正確選擇。

### 4.5 `df.memory_usage(deep=True)` 的效能影響

`get_import_statistics()` 中計算記憶體用量：
```python
'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
```

`deep=True` 會遞迴計算物件陣列（如字串）的實際記憶體用量，這比 `deep=False`（只計算固定大小的欄位頭）準確，但對有大量 object 欄位的 DataFrame 會**顯著增加計算時間**。由於 `BaseDataImporter` 預設 `dtype=str`，實際上每個 DataFrame 的每個欄位都是 object 型別，此操作的代價特別高。

### 4.6 `warnings.warn()` 與 Python 的警告過濾機制

`DeprecationWarning` 預設只在**測試環境**（`-Wd` 標誌或 pytest）中顯示，在普通 Python 腳本執行時**預設被靜默**。這意味著生產環境中呼叫 `GoogleSheetsImporter` 的開發人員不一定會看到廢棄警告，除非明確配置：

```python
import warnings
warnings.simplefilter('always', DeprecationWarning)
```

### 4.7 `concurrent.futures.as_completed()` 的 `timeout` 語義

```python
for future in concurrent.futures.as_completed(future_to_file, timeout=self.timeout):
```

此 `timeout` 是針對**整個 `as_completed()` 迭代過程**的總逾時，而非個別 future 的逾時。若在 `timeout` 秒後仍有未完成的 future，會拋出 `concurrent.futures.TimeoutError`。

### 4.8 `super().__init__()` 在多層繼承中的行為

`AsyncGoogleSheetsImporter` → `GoogleSheetsImporter` → `GoogleSheetsSource` → `DataSource`

呼叫 `AsyncGoogleSheetsImporter()` 時的初始化鏈：
1. `AsyncGoogleSheetsImporter.__init__`：發 DeprecationWarning，呼叫 `super().__init__(credentials_config)`
2. `GoogleSheetsImporter.__init__`：發 DeprecationWarning，呼叫 `super().__init__(credentials_config=credentials_config)`
3. `GoogleSheetsSource.__init__`：實際初始化連線

結果：使用 `AsyncGoogleSheetsImporter` 時會收到**兩次**廢棄警告（第 2 步是因為 `AsyncGoogleSheetsImporter` 呼叫了仍帶 warning 的 `GoogleSheetsImporter.__init__`）。

### 4.9 `GOOGLE_SHEETS` vs `SPX_CONSTANTS` — 常數分層設計

`constants.py` 定義了兩個相關的常數字典：

```python
GOOGLE_SHEETS = {
    'DEFAULT_SCOPES': ['...'],
    'CREDENTIALS_FILE': 'credentials.json'
}

SPX_CONSTANTS = {
    'CLOSING_SHEET_ID': '1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE',
    'CLOSING_SHEETS': ['2023年_done', '2024年', '2025年'],
    'CLOSING_RANGE': 'A:J',
    ...
}
```

`GOOGLE_SHEETS` 設計用途是存放**技術層面**的 Google API 設定；`SPX_CONSTANTS` 用於存放 SPX **業務層面**的試算表 ID 與工作表清單。兩者語意清晰分層，是合理設計。

---

## 5. 應用範例

### 5.1 單檔導入（最常見用法）

```python
from accrual_bot.data.importers import BaseDataImporter

importer = BaseDataImporter()

# 導入 Excel（自動識別格式，預設 dtype=str）
df = importer.import_file('/data/PO_202501.xlsx', sheet_name=0)

# 導入 CSV（指定初始編碼，失敗時自動嘗試其他編碼）
df = importer.import_file('/data/raw_data.csv', encoding='big5')

# 覆蓋預設 dtype（如需保留數值型別）
df = importer.import_file('/data/amounts.xlsx', dtype={'amount': float})
```

### 5.2 批量循序導入

```python
files = [
    '/data/raw_po_202501.xlsx',
    '/data/raw_po_202502.xlsx',
    '/data/raw_po_202503.xlsx',
]

# 為特定檔案指定個別設定
file_configs = {
    'raw_po_202502': {'sheet_name': 'Sheet2', 'header': 1}
}

results = importer.import_multiple_files(files, file_configs)
# results: {'raw_po_202501': df1, 'raw_po_202502': df2, 'raw_po_202503': df3}
```

### 5.3 並發導入（效能優化）

```python
# 並發導入（I/O 密集型，推薦大量檔案時使用）
results = importer.concurrent_import_files(files)

# 取得統計資訊
stats = importer.get_import_statistics(results)
print(f"總計 {stats['total_files']} 個檔案，{stats['total_rows']} 行")
for name, detail in stats['file_details'].items():
    print(f"  {name}: {detail['rows']} 行，{detail['memory_usage_mb']:.2f} MB")
```

### 5.4 從檔名提取日期

```python
# 支援格式：YYYYMM、YYYY-MM、YYYY_MM
date_int, month = importer.extract_date_and_month_from_filename('SPX_PO_202501_final.xlsx')
# → (202501, 1)

date_int, month = importer.extract_date_and_month_from_filename('report_2024-03.csv')
# → (202403, 3)

date_int, month = importer.extract_date_and_month_from_filename('no_date.xlsx')
# → (None, None)，並記錄 WARNING
```

### 5.5 DataFrame 驗證

```python
# 驗證必要欄位與最少行數
is_valid = importer.validate_dataframe(
    df,
    required_columns=['PO Line', 'Amount', 'Date'],
    min_rows=1
)

if not is_valid:
    # validate_dataframe 內部已記錄 WARNING，此處可直接處理
    raise ValueError("導入資料驗證失敗")
```

### 5.6 透過 `BaseLoadingStep` 間接使用（系統內部用法）

`BaseDataImporter` 在實際業務中通常不被直接實例化，而是透過繼承鏈使用：

```python
# core/pipeline/steps/base_loading.py
class BaseLoadingStep(PipelineStep, BaseDataImporter):
    """BaseLoadingStep 繼承 BaseDataImporter 取得檔案讀取能力"""
    ...

# tasks/spt/steps/spt_loading.py
class SPTDataLoadingStep(BaseLoadingStep):
    async def _load_primary_file(self, source, path: str) -> pd.DataFrame:
        df = self.import_file(path, sheet_name=0)  # 使用繼承來的方法
        return df  # 回傳 DataFrame（不再回傳 Tuple）
```

> **注意**：`_load_primary_file()` 的回傳型別已從 `Tuple[pd.DataFrame, int, int]` 簡化為 `pd.DataFrame`。
> 此外，載入步驟不再從檔名提取日期——`processing_date` 統一由 `context.metadata.processing_date` 提供
> （來源：UI 使用者選擇 / CLI `run_config.toml`）。`BaseLoadingStep._extract_date_from_filename()` 已標記為 deprecated，保留僅供向後兼容。

### 5.7 繼承 `BaseDataImporter` 建立自訂導入器

```python
from accrual_bot.data.importers import BaseDataImporter
import pandas as pd

class MyCustomImporter(BaseDataImporter):
    """自訂導入器：加入資料清洗邏輯"""

    def import_and_clean(self, file_path: str) -> pd.DataFrame:
        df = self.import_file(file_path)

        # 驗證結構
        if not self.validate_dataframe(df, required_columns=['Date', 'Amount']):
            return pd.DataFrame()

        # 提取日期
        from pathlib import Path
        date_int, _ = self.extract_date_and_month_from_filename(Path(file_path).name)

        df['file_date'] = date_int
        return df
```

### 5.8 ⚠️ Google Sheets 正確用法（避免廢棄 API）

```python
# ❌ 已廢棄（仍可用，但會發出 DeprecationWarning）
from accrual_bot.data.importers import GoogleSheetsImporter
importer = GoogleSheetsImporter({'certificate_path': 'credentials.json'})

# ✅ 推薦（直接使用 core/datasources 層）
from accrual_bot.core.datasources import GoogleSheetsSource
from accrual_bot.core.datasources.config import DataSourceConfig, DataSourceType

config = DataSourceConfig(
    source_type=DataSourceType.GOOGLE_SHEETS,
    connection_params={
        'credentials_path': 'credentials.json',
        'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/...',
    }
)
source = GoogleSheetsSource(config)
df = await source.read(sheet_name='Sheet1')
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ 健壯的錯誤恢復機制

多層 fallback（引擎降級、編碼嘗試）使導入器能應對真實業務環境中的各種檔案品質問題。批量操作中的 `continue` 設計確保單一失敗不中斷整批作業，符合 pipeline 系統對容錯性的要求。

#### ✅ 合理的預設值選擇

`dtype=str` + `keep_default_na=False` + `na_values=['']` 的組合，完整防止了 pandas 的隱式型別轉換和誤判 NA 值的問題，非常適合財務資料處理場景。這些預設值的選擇顯示作者有實際踩過這些坑的經驗。

#### ✅ 向後兼容層設計清晰

`google_sheets_importer.py` 以薄包裝層的形式保留舊 API，並在 `__init__` 就發出廢棄警告，引導使用者遷移。類別文件注釋中也提供了完整的遷移範例，是廢棄 API 的良好實踐。

#### ✅ 統計與可觀測性

`get_import_statistics()` 提供了導入作業的量化資訊（記憶體用量、行列數），有助於監控和診斷資料品質問題。

### 6.2 缺點與問題

#### ❌ 嚴重 Bug：`import_spx_closing_list()` 永遠回傳空 DataFrame

**位置**：`google_sheets_importer.py`，第 80–83 行

```python
# Bug：從 GOOGLE_SHEETS 讀取，但這些 key 實際在 SPX_CONSTANTS 裡
spreadsheet_id = GOOGLE_SHEETS.get('CLOSING_SHEET_ID', '')    # → '' (key 不存在)
sheet_names = GOOGLE_SHEETS.get('CLOSING_SHEETS', [])         # → [] (key 不存在)
cell_range = GOOGLE_SHEETS.get('CLOSING_RANGE', '')           # → '' (key 不存在)

# 因此接下來的條件判斷
if not spreadsheet_id or not sheet_names:
    self.logger.warning("SPX 關單清單設定不完整，請確認 GOOGLE_SHEETS 常數")
    return pd.DataFrame()  # ← 永遠走到這裡
```

**根因**：`constants.py` 中 SPX 關單相關設定存放在 `SPX_CONSTANTS`，而非 `GOOGLE_SHEETS`。應改為：

```python
from accrual_bot.utils.config.constants import GOOGLE_SHEETS, SPX_CONSTANTS
spreadsheet_id = SPX_CONSTANTS.get('CLOSING_SHEET_ID', '')
sheet_names = SPX_CONSTANTS.get('CLOSING_SHEETS', [])
cell_range = SPX_CONSTANTS.get('CLOSING_RANGE', '')
```

**影響範圍**：凡是呼叫 `GoogleSheetsImporter().import_spx_closing_list()` 的地方均受影響，但由於此類已廢棄，呼叫點應已遷移至其他方式。

#### ❌ `import numpy as np` 從未被使用

**位置**：`base_importer.py`，第 8 行

```python
import numpy as np  # ← 未被使用，應刪除
```

在整個 373 行的 `base_importer.py` 中，`np` 從未被引用。這是一個**未使用的導入（Unused Import）**，增加了不必要的記憶體佔用，且可能在無 `numpy` 環境中造成 `ImportError`。

#### ❌ `import re` 置於方法內部（違反 PEP 8）

**位置**：`base_importer.py`，第 263 行

```python
def extract_date_and_month_from_filename(self, filename: str):
    try:
        import re  # ← 應移至模組頂層
        ...
```

模組級別的 `import` 在模組首次載入時只執行一次，之後會被快取；方法內的 `import` 每次呼叫時都會執行（雖然 Python 有 `sys.modules` 快取使其開銷極小，但仍是不良實踐）。PEP 8 明確建議：「**Imports are always put at the top of the file**」。

#### ❌ 第三個日期正則模式永遠無法觸達

**位置**：`base_importer.py`，第 266–293 行

```python
# 第一輪：嘗試 r'(\d{6})'
pattern = r'(\d{6})'
match = re.search(pattern, filename)
if match: return ...  # 匹配任何 6 位數字

# fallback patterns:
patterns = [
    r'(\d{4})[-_](\d{1,2})',  # YYYY-MM 或 YYYY_MM → 功能不同，合理存在
    r'(\d{4})(\d{2})'         # YYYYMM → 與第一輪的 (\d{6}) 完全等效！永遠無法觸達
]
```

`r'(\d{4})(\d{2})'` 能匹配的所有字串，`r'(\d{6})'` 也能匹配，因此第一輪就已處理，第二個 fallback 永遠不會執行。這是冗餘程式碼（Dead Code）。

#### ❌ `AsyncGoogleSheetsImporter` 是空殼類別

```python
class AsyncGoogleSheetsImporter(GoogleSheetsImporter):
    """[已廢棄] 請直接使用 GoogleSheetsSource"""
    def __init__(self, credentials_config: Dict[str, Any]):
        warnings.warn("AsyncGoogleSheetsImporter 已廢棄...", DeprecationWarning, stacklevel=2)
        super().__init__(credentials_config)
```

此類別**完全不提供非同步功能**，名稱中的 `Async` 具有誤導性。更大的問題是：由於 `super().__init__()` 呼叫到同樣帶廢棄警告的 `GoogleSheetsImporter.__init__`，使用者會收到**兩次廢棄警告**，第一次是 `AsyncGoogleSheetsImporter` 的，第二次是 `GoogleSheetsImporter` 的，這是意料外的行為。

#### ❌ `validate_dataframe()` 回傳 bool 缺乏詳細資訊

```python
def validate_dataframe(self, df, required_columns=None, min_rows=0) -> bool:
    ...
    if missing_columns:
        self.logger.warning(f"DataFrame缺少必要列: {missing_columns}")
        return False  # ← 呼叫方只知道「失敗了」，不知道「缺了哪些欄位」
```

回傳 `bool` 使呼叫方無法在不重複計算的情況下得知具體失敗原因。更好的設計是回傳 `(bool, List[str])`（是否有效，缺少的欄位列表），或拋出包含詳細資訊的例外。

#### ❌ `CONCURRENT_SETTINGS` 為全域常數，無法逐實例覆蓋

```python
# constants.py
CONCURRENT_SETTINGS = {
    'MAX_WORKERS': 5,
    'TIMEOUT': 300
}

# base_importer.py
class BaseDataImporter:
    def __init__(self):
        self.max_workers = CONCURRENT_SETTINGS['MAX_WORKERS']  # 讀一次，之後無法覆蓋
        self.timeout = CONCURRENT_SETTINGS['TIMEOUT']
```

若不同的 pipeline 需要不同的並發設定（如低資源環境需要 `MAX_WORKERS=2`），目前唯一的方式是直接修改實例屬性 `importer.max_workers = 2`，缺乏正式的配置介面。

#### 6.4 `sys.path` 副作用問題

```python
except ImportError:
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))  # 全域副作用
```

`sys.path.insert(0, ...)` 將路徑插入**最高優先級**，可能導致在複雜專案中意外遮蓋（shadow）其他同名模組。雖然有 `if ... not in sys.path` 的檢查，但這只能防止重複插入，無法防止路徑優先級問題。

---

## 7. 延伸議題

### 7.1 `BaseDataImporter` 與 `core/datasources/` 的雙軌問題

系統目前存在兩套資料讀取機制：

| 機制 | 位置 | 狀態 | 特性 |
|------|------|------|------|
| `BaseDataImporter` | `data/importers/` | 活躍 | 同步，直接使用 pandas |
| `ExcelSource`/`CSVSource` | `core/datasources/` | 活躍（新架構） | async，DataSource 介面，LRU 快取，連線池 |

`BaseLoadingStep` 同時繼承兩者（多重繼承：`PipelineStep` + `BaseDataImporter`），在 pipeline 中混用兩套機制。長期來看應考慮將 `BaseDataImporter` 的功能逐步遷移至 `core/datasources/`，以統一資料存取層。

### 7.2 真正的非同步 CSV/Excel 讀取

`concurrent_import_files()` 使用 `ThreadPoolExecutor` 模擬並發，但 pandas 的 `read_csv()`/`read_excel()` 本質上是同步 I/O。更現代的做法：

```python
import asyncio
import aiofiles

async def read_csv_async(file_path: str) -> pd.DataFrame:
    """真正的非同步 CSV 讀取"""
    async with aiofiles.open(file_path, mode='rb') as f:
        content = await f.read()
    import io
    return pd.read_csv(io.BytesIO(content), dtype=str)
```

這消除了執行緒管理的開銷，更適合 `asyncio` 事件迴圈中的使用。

### 7.3 Polars 作為 pandas 的替代

對於大型 CSV/Excel 的讀取，`polars` 提供顯著的效能提升：

```python
import polars as pl

# polars 使用 Rust 實作，讀取速度通常比 pandas 快 5-10 倍
df_polars = pl.read_csv(file_path, infer_schema_length=0)  # infer_schema_length=0 等同於 dtype=str
df = df_polars.to_pandas()  # 轉回 pandas 以兼容現有程式碼
```

在大量並發讀取多個月份財務資料的場景中，效能差異尤為明顯。

### 7.4 使用 `chardet` 自動偵測編碼

現有的 5 種編碼嘗試是「猜測式」的，存在誤判風險（如 Big5 編碼的檔案被 Latin-1 成功讀取但字元錯誤）。`chardet` 函式庫透過統計分析方法偵測編碼：

```python
import chardet

def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        raw = f.read(10000)  # 讀前 10KB 做樣本分析
    result = chardet.detect(raw)
    return result.get('encoding', 'utf-8')

# 使用偵測結果
encoding = detect_encoding(file_path)
df = pd.read_csv(file_path, encoding=encoding)
```

### 7.5 是否應保留 `data/importers/` 層

從架構觀點分析，`data/` 這個頂層包的存在意義已被大幅削弱：

- **`BaseDataImporter`**：其核心能力（讀取 Excel/CSV）已被 `core/datasources/ExcelSource`、`CSVSource` 取代，差異在於後者是 async + DataSource 介面。若 `BaseLoadingStep` 改為完全使用 `DataSourceFactory`，`BaseDataImporter` 的獨立價值就消失了
- **`GoogleSheetsImporter`**：已完全廢棄，建議在下一個主版本（`v3.x`）中移除

**建議的最終架構**：

```
accrual_bot/
├── core/datasources/           # 唯一的資料存取層
│   ├── excel_source.py         # 取代 BaseDataImporter._import_excel()
│   ├── csv_source.py           # 取代 BaseDataImporter._import_csv()
│   └── google_sheet_source.py  # 取代 GoogleSheetsImporter
└── data/                       # 可整體移除
    └── importers/              # 歷史遺留，待下版本刪除
```

### 7.6 `DeprecationWarning` 對使用者的可見性問題

如第 4.6 節所述，`DeprecationWarning` 預設在生產環境中靜默。若要確保遷移通知被看到，可考慮改用 `FutureWarning`（在所有環境下預設顯示），或在首次使用時記錄至日誌系統：

```python
def __init__(self, credentials_config):
    self.logger.warning(
        "GoogleSheetsImporter 已廢棄（v2.0 將移除），請改用 GoogleSheetsSource"
    )
    # 同時保留 warnings.warn 供靜態分析工具使用
    warnings.warn("...", DeprecationWarning, stacklevel=2)
    super().__init__(credentials_config=credentials_config)
```

---

## 8. 其他觀察

### 8.1 模組文件與實際行為的不一致

`importers/__init__.py` 的文件字串寫道：「提供Google Sheets的讀取和並發處理功能」，但 `BaseDataImporter`（Excel/CSV 讀取）才是模組中**唯一仍活躍**的功能。Google Sheets 相關功能已廢棄，文件未反映此現況。

### 8.2 `data/__init__.py` 的萬用符號導入

```python
from .importers import *
```

`from ... import *` 是 Python 中通常應避免的用法（PEP 8：「Wildcard imports should be avoided」），原因是它使命名空間污染風險提高、靜態分析工具難以追蹤符號來源。雖然透過 `__all__` 限制了匯出範圍，但 `importers/__init__.py` 中已明確列出 `__all__`，直接在 `data/__init__.py` 中顯式導入更為清晰：

```python
# 更清晰的寫法
from .importers.base_importer import BaseDataImporter
from .importers.google_sheets_importer import GoogleSheetsImporter, AsyncGoogleSheetsImporter
```

### 8.3 `TIMEOUT` 常數的語義模糊

`CONCURRENT_SETTINGS['TIMEOUT'] = 300`（5 分鐘）作為 `as_completed()` 的總逾時，但沒有文件說明：若逾時發生，部分已完成的 future 結果是否被保留？

查看實作：

```python
for future in concurrent.futures.as_completed(future_to_file, timeout=self.timeout):
    file_name = future_to_file[future]
    try:
        df = future.result()
        if df is not None:
            results[file_name] = df
    except Exception as e:
        ...
```

`TimeoutError` 會在外層 `except Exception` 中被捕獲，導致所有**未完成的**檔案被靜默略過，`results` 中只包含 timeout 前已完成的檔案。這個行為應在文件中明確說明。

### 8.4 `concurrent_import_files` 的雙重驗證

```python
# 第一次驗證（過濾階段）
valid_files = [(path, stem) for path in file_paths if path and validate_file_path(path)]

# 第二次驗證（import_file 內部）
def import_file(self, file_path: str, ...):
    if not validate_file_path(file_path):
        raise ValueError(...)
```

每個檔案被 `validate_file_path()` 呼叫了兩次（過濾一次，再在 `import_file` 內部一次）。雖然 `validate_file_path` 的開銷不大，但這表示設計上有不必要的重複，且在過濾後路徑狀態改變（如檔案被刪除）的極端情況下仍能被第二次驗證捕捉，具有一定的防禦性。

### 8.5 `_import_single_file_safe` 的存在理由

```python
def _import_single_file_safe(self, file_path: str, file_name: str, config: Dict) -> Optional[pd.DataFrame]:
    try:
        return self.import_file(file_path, **config)
    except Exception as e:
        self.logger.error(f"導入檔案失敗: {file_name}, 錯誤: {str(e)}")
        return None
```

此方法的存在是因為 `ThreadPoolExecutor.submit()` 需要傳入一個可被序列化的函式，且並發場景中需要**不拋出例外**（拋出例外的 future 會在 `future.result()` 時重新拋出，可能中斷整個迭代）。透過 `_import_single_file_safe` 捕捉所有例外並回傳 `None`，使 `concurrent_import_files` 的外層邏輯只需處理 `None` 而不是例外，是合理的設計決策。

### 8.6 `google_sheets_importer.py` 的 `queries` 參數格式不一致

```python
# import_spx_closing_list 中的 queries 格式：
queries = [
    (spreadsheet_id, sheet_name, cell_range, True)  # 4-tuple，第4個是 bool
    for sheet_name in sheet_names
]
dfs = self.concurrent_get_data(queries)
```

查看 `GoogleSheetsSource.concurrent_get_data()` 的預期格式（來自 `google_sheet_source.py` 文件）：

```
queries = [(spreadsheet_id, sheet_name, cell_range, header_row), ...]
```

`True` 被傳入作為 `header_row`，在 Python 中 `True == 1`，這等同於 `header_row=1`（第 2 行作為表頭）。這個隱式的 `bool → int` 轉換是可以運作的，但可讀性差，應改為明確的整數 `1`。

---

## 總結

| 面向 | 評分 | 說明 |
|------|------|------|
| 功能完整性 | ★★★★☆ | `BaseDataImporter` 涵蓋常見檔案讀取需求 |
| 錯誤處理 | ★★★★☆ | 多層 fallback 設計健壯 |
| 程式碼品質 | ★★★☆☆ | 多處 bad practices（unused import、方法內 import、dead code） |
| 設計一致性 | ★★☆☆☆ | 雙軌資料存取架構（importer + datasources）增加維護複雜度 |
| 廢棄管理 | ★★★☆☆ | 廢棄通知機制完整，但存在嚴重 Bug（`import_spx_closing_list`） |
| 文件品質 | ★★★☆☆ | 主要方法有文件，但模組級文件未反映廢棄狀況 |
| 可測試性 | ★★★★☆ | 純函式風格、無複雜全域狀態，易於單元測試 |

**核心建議**：
1. **立即**：修正 `import_spx_closing_list()` 中 `GOOGLE_SHEETS` → `SPX_CONSTANTS` 的 Bug
2. **短期**：移除 `import numpy as np`，將 `import re` 移至模組頂層，刪除 dead code（第三個日期正則模式）
3. **中期**：為 `BaseDataImporter.__init__` 加入 `max_workers` 和 `timeout` 參數，取代全域常數讀取
4. **長期**：評估是否將 `BaseDataImporter` 整合進 `core/datasources/` 架構，統一資料存取層，最終廢棄並移除 `data/` 套件
