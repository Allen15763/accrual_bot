# `accrual_bot/utils/helpers/` — 深度研究文件

> **研究日期**：2026-03-12
> **研究範圍**：`accrual_bot/utils/helpers/` 下的全部原始碼（4 個 Python 檔案）
> **研究方法**：逐行閱讀全部原始碼、測試檔案、使用位置追蹤、常數定義對照

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

`utils/helpers/` 是 `accrual_bot` 工具層（Utils Layer）的第三個子模組，與 `config/`、`logging/` 並列，負責提供**跨切面（cross-cutting）的輔助功能**。

### 起源與演進

根據程式碼內部線索，可以推斷此模組的演進歷程：

| 階段 | 特徵 |
|------|------|
| 早期 | `data_utils.py` 混雜了業務邏輯（`classify_description`、`give_account_by_keyword`）與通用工具；Colab 環境殘留函數（`get_ref_on_colab`）提示此模組最初在 Google Colab 上開發 |
| 重構期 | `column_utils.py` 獨立抽出，提供正式的欄位名稱解析機制；`file_utils.py` 改以 `pathlib.Path` 為核心重寫；對外暴露統一的 `__all__` |
| 現狀 | 作為整個 `accrual_bot.utils` 命名空間的一部分，透過 `utils/__init__.py` 的 `from .helpers import *` 對外開放 |

### 模組結構一覽

```
accrual_bot/utils/helpers/
├── __init__.py         (91 行)   — 彙整三個子模組的公開 API
├── column_utils.py    (146 行)   — ColumnResolver 欄位名稱解析
├── data_utils.py      (841 行)   — 資料處理工具函數（含業務邏輯）
└── file_utils.py      (478 行)   — 檔案操作工具函數
```

---

## 2. 用途

### 2.1 `column_utils.py` — 欄位名稱解析

處理財務資料的痛點之一是**同一欄位有多種命名方式**。例如：
- `PO Line`（標準格式）
- `po_line`（snake_case）
- `PO_Line`（混合格式）

`ColumnResolver` 提供一個統一的解析機制，讓 pipeline steps 無論面對哪種命名方式都能找到對應欄位。

**實際使用位置**：

| 檔案 | 用途 |
|------|------|
| `core/pipeline/steps/common.py` | `PreviousWorkpaperIntegrationStep` 解析 PO/PR Line、Reviewer 欄位 |
| `tasks/spt/steps/spt_procurement_mapping.py` | 映射前期底稿欄位至當前資料 |
| `tasks/spt/steps/spt_procurement_validation.py` | 驗證前期底稿必要欄位是否存在 |

### 2.2 `data_utils.py` — 資料處理工具

提供兩類功能：

**通用工具層**（適合跨專案重用）：

| 函數 | 功能 |
|------|------|
| `clean_nan_values` | 將 NaN 清理為空字串 |
| `safe_string_operation` | 安全的 Series 字串操作（含 NaN 防護） |
| `format_numeric_with_thousands` | 數值千分位格式化 |
| `format_numeric_columns` | 批次格式化整數/浮點數列 |
| `parse_date_string` | 日期字串解析與格式轉換 |
| `extract_date_range_from_description` | 從描述文字提取日期範圍 |
| `convert_date_format_in_string` | 字串內日期格式轉換 |
| `extract_pattern_from_string` | 正規表達式提取 |
| `safe_numeric_operation` | 安全數值運算 |
| `create_mapping_dict` | 從 DataFrame 建立映射字典 |
| `apply_mapping_safely` | 安全套用映射字典 |
| `validate_dataframe_columns` | 驗證 DataFrame 欄位存在性 |
| `concat_dataframes_safely` | 安全合併多個 DataFrame |
| `parallel_apply` | 並行化 apply 操作 |
| `memory_efficient_operation` | 分塊處理大型 DataFrame |

**業務邏輯層**（accrual_bot 特定）：

| 函數 | 功能 |
|------|------|
| `classify_description` | 依 TOML 規則將品項描述分類為費用類別 |
| `give_account_by_keyword` | 依關鍵字規則預測會計科目代碼 |
| `extract_clean_description` | 從 Item Description 提取清洗後字串 |
| `clean_pr_data` | 清理 PR 資料欄位名稱與型別 |
| `clean_po_data` | 清理 PO 資料欄位名稱與型別 |
| `get_ref_on_colab` | Colab 環境讀取參照檔案（遺留函數） |

### 2.3 `file_utils.py` — 檔案操作工具

提供安全且一致的檔案系統操作：

| 函數 | 功能 |
|------|------|
| `get_resource_path` | 取得資源路徑（適配 PyInstaller 打包環境） |
| `validate_file_path` | 驗證路徑有效性（可選 existence check） |
| `validate_file_extension` | 驗證副檔名 |
| `get_file_extension` | 取得副檔名（小寫） |
| `is_excel_file` / `is_csv_file` | 類型判斷快捷函數 |
| `ensure_directory_exists` | 確保目錄存在（遞歸建立） |
| `get_safe_filename` | 移除不安全字元，限制檔名長度 |
| `get_unique_filename` | 衝突時自動加數字後綴 |
| `get_file_info` | 取得檔案詳細資訊字典 |
| `calculate_file_hash` | 計算檔案雜湊值（支援 md5/sha1/sha256） |
| `copy_file_safely` | 安全複製（可防覆蓋） |
| `move_file_safely` | 安全移動 |
| `cleanup_temp_files` | 清理指定時間內的臨時檔案 |
| `find_files_by_pattern` | 依 glob 模式搜尋檔案 |
| `load_toml` | 載入 TOML 配置（錯誤時返回空字典） |
| `get_directory_size` | 計算目錄總大小與檔案數 |

---

## 3. 設計思路

### 3.1 防禦性程式設計（Defensive Programming）

三個模組均大量採用防禦性設計，但策略不同：

**`file_utils.py`（最嚴謹）**：
```python
def validate_file_path(file_path: str, check_exists: bool = True) -> bool:
    if not file_path or not isinstance(file_path, str):
        logger.warning(f"無效的檔案路徑（非字串或空值）: {file_path!r}")
        return False
    try:
        path = Path(file_path)
        if not path.name:  # 路徑格式無效
            return False
        if check_exists and not path.exists():  # 分層驗證
            return False
        if check_exists and not path.is_file():  # 區分檔案與目錄
            return False
        return True
    except (OSError, ValueError) as e:
        logger.warning(...)
        return False
```

每一層驗證失敗都有獨立的日誌訊息，可追溯失敗原因。

**`data_utils.py`（靜默降級）**：
```python
def safe_string_operation(series, operation, pattern=None, replacement=None, **kwargs):
    try:
        str_series = series.astype(str).fillna('')
        # ...操作...
    except Exception:
        return series  # 靜默降級，返回原始值
```

幾乎所有操作都包在 `try/except Exception`，任何失敗都返回原始值而非拋出異常。

**策略差異的含義**：
- `file_utils.py` 的函數是「基礎設施層」，提供清晰的結果指示，適合讓呼叫端做決策
- `data_utils.py` 的函數是「資料轉換層」，重視不中斷流程，適合在 pipeline 中批次處理

### 3.2 Pattern Registry 設計（ColumnResolver）

`ColumnResolver` 採用**靜態的 Pattern Registry 模式**：

```python
class ColumnResolver:
    # 類別層級字典 — 全域共享
    COLUMN_PATTERNS: Dict[str, str] = {
        'po_line': r'(?i)^(po[_\s]?line)$',
        'pr_line': r'(?i)^(pr[_\s]?line)$',
        # ...共 9 個預定義模式
    }

    @classmethod
    def resolve(cls, df: pd.DataFrame, canonical_name: str) -> Optional[str]:
        # 1. 嘗試 Pattern Registry
        pattern = cls.COLUMN_PATTERNS.get(canonical_name)
        if pattern:
            matches = df.filter(regex=pattern).columns
            # ...

        # 2. Fallback: 大小寫不敏感直接比對
        canonical_normalized = canonical_name.lower().replace(' ', '_')
        for col in df.columns:
            col_normalized = str(col).lower().replace(' ', '_')
            if col_normalized == canonical_normalized:
                return col
        return None
```

**雙層查找策略**：
- 第一層：使用預定義正規表達式，適合複雜模式（`remarked[_\s]?by[_\s]?fn`）
- 第二層：空格→底線的 normalize 比對，適合未預定義的欄位

### 3.3 模組層級初始化（Module-Level Initialization）

`data_utils.py` 採用模組載入時執行的初始化策略：

```python
# 模組頂部 — 載入時立即執行
toml_path = None
ACCOUNT_RULES = load_config_from_toml(toml_path, "account_rules", output_format='list')
CATEGORY_PATTERNS_BY_DESC = load_config_from_toml(toml_path, "category_patterns_by_desc")
DATE_PATTERNS = load_config_from_toml(toml_path, "date_patterns")
COLAB_ZIP_PATH = load_config_from_toml(toml_path, "paths").get("colab_zip")
```

**設計意圖**：讓規則作為「常數」快取，避免每次呼叫 `classify_description()` 都重新讀取 TOML 文件。

**代價**：任何 `import accrual_bot.utils.helpers.data_utils` 都會觸發 TOML 文件讀取（後述問題分析）。

### 3.4 路徑解析策略

`load_config_from_toml` 採用以下路徑解析策略：

```python
current_dir = Path(__file__).parent  # utils/helpers/
parts = list(current_dir.parts)
# 移除 'utils' 和 'helpers' 兩個路徑分段
# 重新組合 → accrual_bot/accrual_bot/
config_dir = new_path / 'config'
```

這等價於：
```python
# 更簡潔的等價寫法（但程式碼未採用）
config_dir = Path(__file__).parents[2] / 'config'
```

### 3.5 PyInstaller 適配

`file_utils.py` 支援 PyInstaller 打包環境：

```python
def get_resource_path(relative_path: str) -> str:
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)
```

`sys._MEIPASS` 是 PyInstaller 注入的屬性，指向解壓縮後的臨時目錄。一般開發環境不存在此屬性，走 fallback 路徑。

---

## 4. 各項知識點

### 4.1 `df.filter(regex=pattern)` vs `df.columns.str.match()`

`ColumnResolver.resolve()` 使用 `df.filter(regex=pattern)` 而不是 `df.columns.str.match(pattern)`，差異如下：

| 方法 | 回傳 | 適用情境 |
|------|------|---------|
| `df.filter(regex=pattern)` | 子 DataFrame（帶匹配欄位） | 回傳欄位名稱時需再取 `.columns` |
| `df.columns[df.columns.str.match(pattern)]` | Index | 直接取得欄位名稱 |
| `df.filter(like=substring)` | 子 DataFrame | 只支援子字串搜尋 |

`filter(regex=...)` 是 pandas 的欄位/行篩選通用介面，這裡用它做 regex 篩選是慣用但略顯迂迴的寫法。

### 4.2 `re.IGNORECASE` 在 Pattern 中內嵌 vs 旗標

`ColumnResolver.COLUMN_PATTERNS` 使用內嵌 `(?i)` 旗標而非傳入 `re.IGNORECASE`：

```python
'po_line': r'(?i)^(po[_\s]?line)$',
# 等價於：re.compile(r'^(po[_\s]?line)$', re.IGNORECASE)
```

**選擇內嵌的優點**：
- 正則表達式本身即是自描述的（pattern 攜帶了旗標）
- 當 pattern 被存在字典中、透過 `df.filter(regex=...)` 呼叫時，不需要另外傳遞旗標

### 4.3 `iter(lambda: f.read(4096), b"")` — 哨兵 iter 模式

`calculate_file_hash` 使用 Python 鮮為人知的雙參數 `iter()` 語法：

```python
for chunk in iter(lambda: f.read(4096), b""):
    hash_obj.update(chunk)
```

等價於但更 Pythonic 的寫法：
```python
while True:
    chunk = f.read(4096)
    if chunk == b"":
        break
    hash_obj.update(chunk)
```

`iter(callable, sentinel)` 會持續呼叫 `callable()`，直到返回 `sentinel` 值（此處為 `b""`，即 EOF）。這是 Python 中讀取固定塊大小的慣用模式。

### 4.4 `shutil.copy2` vs `shutil.copy`

`copy_file_safely` 使用 `shutil.copy2`：

| 函數 | 複製內容 |
|------|---------|
| `shutil.copy` | 檔案內容 + 部分 metadata（權限位） |
| `shutil.copy2` | 檔案內容 + 完整 metadata（包括時間戳） |
| `shutil.copyfile` | 僅檔案內容 |
| `shutil.copytree` | 整個目錄樹 |

`shutil.copy2` 是複製檔案時最接近「真正複製」的函數，適合備份場景。

### 4.5 `pd.to_numeric(errors='coerce')` 搭配 `.map()` 的映射鏈

`apply_mapping_safely` 展示了 pandas 映射的標準模式：

```python
return series.map(mapping_dict).fillna(default_value)
```

`Series.map(dict)` 會對每個值在字典中查找，找不到的返回 `NaN`。搭配 `.fillna(default_value)` 即可實現「找不到就用預設值」的語義。

**注意**：`Series.map()` 和 `Series.apply()` 的差異：
- `map(dict)` — 值查找，僅接受字典/Series/函數
- `apply(func)` — 函數應用，接受任意 callable
- `map(func)` — 等同 `apply(func)` 但不接受額外參數

### 4.6 ThreadPoolExecutor 的分塊並行 vs GIL

`parallel_apply` 使用 `ThreadPoolExecutor` 對 DataFrame 分塊並行處理：

```python
chunk_size = max(1, len(data) // max_workers)
chunks = [data.iloc[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    results = list(executor.map(lambda chunk: chunk.apply(func, **kwargs), chunks))
```

**GIL 的影響**：
- Python GIL 限制 CPU-bound 任務的真正並行
- 但若 `func` 主要是 I/O 或涉及釋放 GIL 的 C 擴展（如 numpy 數值運算），Threading 仍有效
- 對 CPU-bound 純 Python 函數，`ProcessPoolExecutor` 才能真正並行

**潛在問題**：`max_workers = min(4, len(df) // 1000 + 1)` — 當 `len(df) == 0` 時 `max_workers = 1`，功能上無問題但 ThreadPool 仍會建立。

### 4.7 `lazy import`（延遲匯入）vs 頂層 import

`common.py` 中的 `ColumnResolver` 採用 lazy import 模式：

```python
# 方法內部才 import（出現 4 次）
def _process_mapping(self, ...):
    from accrual_bot.utils.helpers.column_utils import ColumnResolver
    df_key = ColumnResolver.resolve(df, key_col_canonical)
```

而 `spt_procurement_mapping.py` 採用頂層 import：
```python
from accrual_bot.utils.helpers.column_utils import ColumnResolver
```

兩者各有取捨：

| 方式 | 優點 | 缺點 |
|------|------|------|
| 頂層 import | 清晰、明確、IDE 支援好 | 存在循環 import 風險 |
| lazy import | 避免循環 import | 每次呼叫方法都觸發 import（CPython 有 cache，實際近乎零成本） |

`common.py` 使用 lazy import 可能是歷史上曾存在循環依賴問題的痕跡。

### 4.8 `_validate_date_format` 的私有函數測試問題

`tests/unit/utils/helpers/test_data_utils.py` 直接測試私有函數：

```python
from accrual_bot.utils.helpers.data_utils import (
    _validate_date_format,  # 私有函數（底線前綴）
    ...
)
```

**軟體工程觀點**：Python 的單底線慣例表示「模組內部使用」，測試私有函數有以下爭議：
- **正方**：確保輔助函數的邊界條件正確性，避免 `extract_date_range_from_description` 失效時難以定位問題
- **反方**：私有函數是實作細節，應透過公開介面測試；直接測試造成重構難度上升

### 4.9 `hashlib.new(algorithm)` 動態選擇演算法

```python
hash_obj = hashlib.new(algorithm)  # 支援 'md5', 'sha1', 'sha256'
```

`hashlib.new(name)` 是動態建立雜湊物件的通用介面，等價於：
```python
hashlib.md5()      # 固定使用 MD5
hashlib.sha256()   # 固定使用 SHA-256
```

使用 `hashlib.new` 的優點是支援傳入任意演算法名稱，但需注意 `hashlib.algorithms_guaranteed` 與 `hashlib.algorithms_available` 的差異（前者跨平台保證，後者取決於 OpenSSL 版本）。

### 4.10 `concurrent.futures.executor.map` 的例外處理

`parallel_apply` 使用 `executor.map()` 並設計了 fallback：

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    results = list(executor.map(lambda chunk: chunk.apply(func, **kwargs), chunks))
```

**`executor.map()` 的例外特性**：若任何 chunk 拋出例外，`list()` 在迭代到該項時才會重新拋出。函數的 `except Exception` 捕捉後回退串行處理：

```python
except Exception:
    if column:
        return df[column].apply(func, **kwargs)
    else:
        return df.apply(func, **kwargs)
```

這是「並行失敗降級串行」的標準安全模式。

### 4.11 `DATE_PATTERNS` 的雙來源問題

系統中存在**兩個** `DATE_PATTERNS`：

| 來源 | 內容 |
|------|------|
| `utils/config/constants.py::REGEX_PATTERNS` | 使用嚴格的月份驗證 `(0[1-9]\|1[0-2])` |
| TOML `[date_patterns]` (由 `load_config_from_toml` 載入) | 使用簡化的 `\d{2}` 或更寬鬆的模式 |

`data_utils.py` 載入模組時執行：
```python
DATE_PATTERNS = load_config_from_toml(toml_path, "date_patterns")
```

這讀取的是 `stagging.toml` 的 `[date_patterns]` section，而非 `constants.py` 的 `REGEX_PATTERNS`。兩組模式有細微差異，容易造成混淆。

### 4.12 `classify_description` 的一次掃描效率

```python
def classify_description(description: str) -> str:
    patterns = CATEGORY_PATTERNS_BY_DESC
    for label, pattern in patterns.items():
        if re.search(pattern, description):
            return label  # 立即返回第一個匹配
    return 'Miscellaneous'
```

**效率考量**：`patterns` 的**順序**決定了分類結果（先宣告的模式優先）。`CATEGORY_PATTERNS_BY_DESC` 來自 TOML 的 `[category_patterns_by_desc]` section，Python 3.7+ 中字典保持插入順序，TOML 也保持文件順序，故分類結果是確定性的。

`stagging.toml` 的注釋明確說明了這一點：`# 將服務費放在較後面，避免攔截掉更具體的項目`。

---

## 5. 應用範例

### 5.1 ColumnResolver — 欄位名稱解析

```python
from accrual_bot.utils.helpers.column_utils import ColumnResolver
import pandas as pd

# 情境：上傳的 Excel 欄位名稱不一致
df = pd.DataFrame({
    'PO Line': ['PO-001', 'PO-002'],    # 標準格式
    'Remarked by FN': ['Y', 'N'],        # 標準格式
})

# 解析預定義欄位
po_col = ColumnResolver.resolve(df, 'po_line')
# 返回: 'PO Line'

remark_col = ColumnResolver.resolve(df, 'remarked_by_fn')
# 返回: 'Remarked by FN'

# 安全存取欄位
if ColumnResolver.has_column(df, 'liability'):
    # 欄位存在才處理
    liability_col = ColumnResolver.resolve(df, 'liability')
    df[liability_col] = df[liability_col].fillna('')

# 強制解析（找不到則拋出 ValueError）
try:
    col = ColumnResolver.resolve_or_raise(df, 'pr_line')
except ValueError as e:
    print(f"必要欄位缺失: {e}")

# 批次解析
columns_map = ColumnResolver.resolve_multiple(df, ['po_line', 'pr_line', 'liability'])
# 返回: {'po_line': 'PO Line', 'pr_line': None, 'liability': None}

# 動態新增自訂模式
ColumnResolver.add_pattern('store_code', r'(?i)^(store[_\s]?code|門市代碼)$')
```

### 5.2 建立映射字典並安全套用

```python
from accrual_bot.utils.helpers.data_utils import create_mapping_dict, apply_mapping_safely
import pandas as pd

# 參照資料（科目代碼 → 科目名稱）
ref_df = pd.DataFrame({
    'account_code': ['520016', '520017', '520018'],
    'account_name': ['文具用品', '包材', '室內標示']
})

# 建立映射字典
account_map = create_mapping_dict(ref_df, 'account_code', 'account_name')
# 返回: {'520016': '文具用品', '520017': '包材', '520018': '室內標示'}

# 主資料
main_df = pd.DataFrame({
    'account': ['520016', '520019', '520017']
})

# 安全套用映射
main_df['account_name'] = apply_mapping_safely(
    main_df['account'],
    account_map,
    default_value='未知科目'
)
# 結果: ['文具用品', '未知科目', '包材']
```

### 5.3 日期範圍提取

```python
from accrual_bot.utils.helpers.data_utils import extract_date_range_from_description
import logging

logger = logging.getLogger('my_module')

# 從不同格式的描述提取日期範圍
desc1 = "門市租金 2024/01-2024/12 Monthly Rental"
result1 = extract_date_range_from_description(desc1, logger=logger)
# 返回: "202401,202412"

desc2 = "設備押金 2024/03/01 - 2024/03/31"
result2 = extract_date_range_from_description(desc2, logger=logger)
# 返回: "202403,202403"

desc3 = "Service Fee 2024/06"
result3 = extract_date_range_from_description(desc3, logger=logger)
# 返回: "202406,202406"

desc4 = "無日期描述"
result4 = extract_date_range_from_description(desc4, logger=logger)
# 返回: "100001,100002"（DEFAULT_DATE_RANGE）
```

**重要提醒**：必須傳入 `logger` 參數，否則當 `description` 為空時將引發 `AttributeError`（見問題分析）。

### 5.4 檔案安全操作

```python
from accrual_bot.utils.helpers.file_utils import (
    validate_file_path, ensure_directory_exists,
    copy_file_safely, get_unique_filename, calculate_file_hash
)

# 驗證上傳檔案
def process_uploaded_file(file_path: str) -> bool:
    if not validate_file_path(file_path, check_exists=True):
        return False

    # 確保輸出目錄存在
    ensure_directory_exists('/output/processed')

    # 取得不衝突的輸出路徑
    unique_path = get_unique_filename('/output/processed', 'report.xlsx')
    # 如果 report.xlsx 已存在，返回 report_1.xlsx

    # 複製而非移動（保留原始檔案）
    success = copy_file_safely(file_path, unique_path, overwrite=False)
    return success

# 計算檔案完整性雜湊
original_hash = calculate_file_hash('data/original.xlsx', algorithm='sha256')
copy_hash = calculate_file_hash('backup/original.xlsx', algorithm='sha256')
assert original_hash == copy_hash, "複製完整性驗證失敗"
```

### 5.5 分類描述應用

```python
from accrual_bot.utils.helpers.data_utils import classify_description, give_account_by_keyword
import pandas as pd

# 分類費用描述
descriptions = [
    '門市租金 Monthly Rental',
    'Cleaning fee 清潔費',
    '保險費 Insurance',
    '不明項目'
]

for desc in descriptions:
    category = classify_description(desc)
    print(f"{desc[:20]} → {category}")
# 輸出:
# 門市租金 Monthly Renta → Rental
# Cleaning fee 清潔費   → Service Fee  (先匹配到 service fee 之前的 cleaning)
# 保險費 Insurance       → Insurance
# 不明項目              → Miscellaneous

# 使用關鍵字預測會計科目
df = pd.DataFrame({'Item Description': ['門市裝修工程', '管理費', '郵資費']})
df = give_account_by_keyword(df, 'Item Description', export_keyword=True)
# 新增 'Predicted_Account' 和 'Matched_Keyword' 欄位
```

### 5.6 並行與分塊處理

```python
from accrual_bot.utils.helpers.data_utils import parallel_apply, memory_efficient_operation
import pandas as pd

# 大型 DataFrame 並行處理
large_df = pd.DataFrame({'description': ['...'] * 100000})

# 並行 apply（自動分塊）
result = parallel_apply(
    large_df,
    func=lambda x: classify_description(str(x)),
    column='description',
    max_workers=4
)

# 分塊操作（適合記憶體敏感場景）
def expensive_transform(df_chunk: pd.DataFrame) -> pd.DataFrame:
    # 某個昂貴的轉換操作
    return df_chunk.assign(processed=df_chunk['description'].str.upper())

result_df = memory_efficient_operation(
    large_df,
    operation=expensive_transform,
    chunk_size=5000
)
```

---

## 6. 優缺分析

### 6.1 優點

#### ✅ `column_utils.py` 設計精良

`ColumnResolver` 解決了財務資料處理的真實痛點。預定義 pattern registry + fallback 的雙層策略既靈活又安全，`add_pattern()` 提供了擴展機制而不需要修改核心程式碼。

100% 測試覆蓋（根據 CLAUDE.md）且測試品質良好：涵蓋 pattern 匹配、fallback、多重匹配、raise/no-raise 兩種變體。

#### ✅ `file_utils.py` 的日誌一致性

每個操作失敗都有對應的 `logger.warning()`/`logger.error()` 訊息，且訊息包含失敗路徑，方便問題追蹤。這是工具函數庫應有的設計。

#### ✅ 防止 import * 污染

`__init__.py` 明確定義 `__all__`，避免 `from helpers import *` 引入不必要的名稱（如私有函數 `_validate_date_format`、`extract_clean_description` 等未在 `__all__` 的函數）。

#### ✅ `concat_dataframes_safely` 的空值防護

```python
valid_dfs = [df for df in dfs if df is not None and not df.empty]
```

區分了 `None`（未載入）和 `df.empty`（空 DataFrame），避免 `pd.concat([])` 拋出 ValueError。

#### ✅ `get_unique_filename` 的上限防護

```python
if counter > 9999:
    timestamp = str(int(time.time()))
    new_filename = f"{name_part}_{timestamp}{ext_part}"
    return str(base_dir / new_filename)
```

防止無限迴圈（雖然 9999 個同名檔案是極端情況）。

### 6.2 缺點與問題

#### 🔴 P0 嚴重問題：`extract_date_range_from_description` — `None` logger 必然崩潰

**位置**：`data_utils.py:307`

```python
def extract_date_range_from_description(
    description: str,
    patterns: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None  # 預設 None
) -> str:
    try:
        if pd.isna(description) or not description or str(description).strip() == '':
            logger.warning("描述為空，返回預設日期範圍")  # ← AttributeError: NoneType
```

當 `description` 為空值且使用預設 `logger=None` 時，必然拋出 `AttributeError`。這是一個函數簽名與函數體邏輯不匹配的 bug。

**正確修復**：
```python
if logger:
    logger.warning("描述為空，返回預設日期範圍")
```
或改用 module-level logger：
```python
_logger = logging.getLogger(__name__)
# 函數內：_logger.warning(...) 而不是 logger.warning(...)
```

#### 🔴 P0 嚴重問題：`give_account_by_keyword` 參數無效

**位置**：`data_utils.py:659`

```python
def give_account_by_keyword(df, column_name, rules=None, export_keyword=False):
    # 步驟 1: 規則列表現在從函數參數傳入，不再硬編碼。
    rules = ACCOUNT_RULES  # ← 覆蓋了傳入的 rules 參數！
```

函數簽名接受 `rules` 參數，但函數體第一行就用模組常數覆蓋它。注釋說「從函數參數傳入，不再硬編碼」，實際上仍然是硬編碼。這是注釋與程式碼矛盾的典型問題。

**正確修復**：
```python
def give_account_by_keyword(df, column_name, rules=None, export_keyword=False):
    if rules is None:
        rules = ACCOUNT_RULES
```

#### 🟡 P1 問題：模組層級初始化造成測試困難

**位置**：`data_utils.py:101-104`

```python
ACCOUNT_RULES = load_config_from_toml(toml_path, "account_rules", output_format='list')
CATEGORY_PATTERNS_BY_DESC = load_config_from_toml(toml_path, "category_patterns_by_desc")
DATE_PATTERNS = load_config_from_toml(toml_path, "date_patterns")
COLAB_ZIP_PATH = load_config_from_toml(toml_path, "paths").get("colab_zip")
```

任何 `import data_utils` 都會觸發 TOML 讀取。在測試環境、CI/CD 環境或不同工作目錄下運行時，若找不到 TOML 文件，**整個模組載入就會失敗**，連不相關的測試（如 `test_clean_nan_values`）也會被連帶中斷。

**改善方向**：使用 `ConfigManager` singleton 而非直接讀取 TOML；或改為 lazy initialization（首次呼叫相關函數時才載入）。

#### 🟡 P1 問題：`load_config_from_toml` 路徑解析脆弱

**位置**：`data_utils.py:43-65`

```python
parts = list(current_dir.parts)
parts_to_remove = ['utils', 'helpers']
try:
    index = parts.index(parts_to_remove[0])
    if parts[index:index + len(parts_to_remove)] == parts_to_remove:
        del parts[index:index + len(parts_to_remove)]
        new_path = Path(parts[0]).joinpath(*parts[1:])
```

此路徑解析依賴目錄名稱 `'utils'` 和 `'helpers'` 的存在，且假設它們是連續的路徑段。若：
- 父目錄中恰好有另一個名為 `utils` 的目錄
- 模組被移動到不同位置
- 在符號連結環境下運行

行為都可能不符預期。

**更簡潔且穩健的等價寫法**：
```python
config_dir = Path(__file__).parents[2] / 'config'
```

`Path.parents[n]` 是向上 n 層的直接語義，比手動操作 `parts` 更清晰。

#### 🟡 P1 問題：`extract_clean_description` 重複定義

同名函數在兩個位置定義：
1. `accrual_bot/utils/helpers/data_utils.py:690` — 通用工具
2. `accrual_bot/tasks/spx/steps/spx_ppe_desc.py:35` — SPX PPE 專用（包含更詳細的注釋和完整的 docstring）

`spx_ppe_qty_validation.py` 導入的是 `data_utils.py` 版本：
```python
from accrual_bot.utils.helpers.data_utils import extract_clean_description
```

兩個版本的邏輯相似但可能存在差異，未來修改時容易只改一處而遺漏另一處。

#### 🟡 P1 問題：`ColumnResolver.add_pattern()` 全域狀態汙染

```python
@classmethod
def add_pattern(cls, canonical_name: str, pattern: str) -> None:
    cls.COLUMN_PATTERNS[canonical_name] = pattern
```

`COLUMN_PATTERNS` 是類別層級字典，`add_pattern()` 會永久修改全域狀態。測試檔案需要手動清理：

```python
def test_add_pattern_and_resolve(self):
    ColumnResolver.add_pattern('custom_field', r'(?i)^(custom[_\s]?field)$')
    # ...
    del ColumnResolver.COLUMN_PATTERNS['custom_field']  # 手動清理
```

**改善方向**：若需要動態 pattern，應考慮使用 instance 而非 classmethod，或提供 `remove_pattern()` 方法。

#### 🟢 P2 問題：`clean_pr_data` / `clean_po_data` 未在 `__all__` 中

這兩個函數被 `spt_steps.py` 直接導入，但未加入 `data_utils.__all__`，也沒有加入 `helpers/__init__.py` 的 `__all__`。這意味著它們是「半公開」狀態，無法透過 `from accrual_bot.utils.helpers import clean_pr_data` 使用。

#### 🟢 P2 問題：`safe_string_operation` 的 NaN 轉換邏輯

```python
str_series = series.astype(str).fillna('')
```

`astype(str)` 會將 `NaN` 轉換為字串 `'nan'`，而不是 `NaN`。因此後續的 `.fillna('')` 實際上沒有效果（因為 Series 中已沒有實際的 NaN 值，只有字串 `'nan'`）。

正確做法應為：
```python
str_series = series.fillna('').astype(str)
# 或
str_series = series.where(series.notna(), '').astype(str)
```

#### 🟢 P2 問題：`move_file_safely` 日誌缺失

相較於 `copy_file_safely` 的完整日誌，`move_file_safely` 完全沒有日誌輸出，失敗時難以追蹤：

```python
def move_file_safely(src_path, dst_path, overwrite=False):
    try:
        # ...
        shutil.move(str(src), str(dst))
        return True  # ← 無成功日誌
    except (OSError, shutil.Error):
        return False  # ← 無失敗日誌
```

#### 🟢 P2 問題：`get_ref_on_colab` — 歷史遺留函數

```python
def get_ref_on_colab(ref_data_path):
    """work for colab env"""
    def is_colab():
        try:
            import google.colab
            return True
        except Exception:
            return False

    if is_colab():
        import zipfile
        # ...讀取 ZIP 中的 Excel 文件
    else:
        return None
```

此函數為 Google Colab 開發環境的遺留產物，在生產環境中恆返回 `None`，被 `spt_loading.py` 和 `spx_loading.py` 呼叫後立即被 `if ref_ac is not None` 跳過。名稱暴露了特定執行環境（違反抽象原則），建議在適當時機移除。

---

## 7. 延伸議題

### 7.1 業務邏輯與工具函數的邊界問題

`data_utils.py` 同時包含：
- 通用工具（`create_mapping_dict`、`concat_dataframes_safely`）
- accrual_bot 特定業務邏輯（`classify_description`、`give_account_by_keyword`）

從乾淨架構（Clean Architecture）的角度，業務規則應在 Domain Layer，而工具函數應保持純粹（無業務知識）。`classify_description` 知道「Miscellaneous」分類、`give_account_by_keyword` 知道科目規則，這些屬於業務知識，不應出現在工具層。

**改善建議**：將業務相關函數移至 `tasks/common/` 或建立獨立的 `domain/` 模組。

### 7.2 `ColumnResolver` 的設定來源一致性問題

目前 `ColumnResolver.COLUMN_PATTERNS` 是 hardcoded 在 `column_utils.py`，而類似的欄位模式定義也出現在：

- `stagging.toml` 的 `[previous_workpaper_integration.column_patterns]`（外部 TOML 配置）

兩者都定義了 `po_line`、`pr_line`、`remarked_by_fn` 等模式，是同一概念的兩處定義。若模式需要調整，需要同步修改兩個地方。

理想做法：`ColumnResolver.COLUMN_PATTERNS` 在初始化時從 `config_manager` 載入，以 TOML 為單一來源。

### 7.3 `parallel_apply` 的適用性評估

`parallel_apply` 理論上是性能優化工具，但實際適用場景有限：

- **不適合**：純 pandas 操作（vectorized operations，如 `str.contains()`），這些本身已比 `apply()` 快得多
- **不適合**：CPU-bound Python 函數（受 GIL 限制，threading 無效）
- **適合**：I/O-bound 操作（如對每行進行 HTTP 請求、資料庫查詢）

對於 `classify_description` 這種正規表達式操作，使用 `parallel_apply` 的實際效益值得基準測試驗證，而不應直接假設有效。

### 7.4 檔案雜湊的使用場景擴展

`calculate_file_hash` 目前僅作為獨立函數存在，並未整合到更高層的工作流中。可以考慮：

1. **Checkpoint 完整性驗證**：儲存 checkpoint 時記錄雜湊，載入時驗證
2. **重複文件偵測**：上傳相同文件時給予警告
3. **輸出文件驗證**：確保輸出文件未被意外修改

### 7.5 `find_files_by_pattern` 與 `paths.toml` 的整合空間

`paths.toml` 中的路徑使用 glob 模式（如 `{YYYYMM}_purchase_order_*.csv`），而 `find_files_by_pattern` 提供 glob 搜尋能力。目前兩者是獨立的，若在 `DataLoader` 層整合，可以讓路徑解析和文件搜尋更統一。

### 7.6 型別標注的完整性

`file_utils.py` 有完整的型別標注（Python 3.9+ style），而 `data_utils.py` 部分函數缺乏標注（如 `give_account_by_keyword(df, column_name, rules=None, export_keyword=False)` 缺少 `pd.DataFrame` 和 `bool` 型別），降低了 IDE 的自動補全和靜態分析能力。

---

## 8. 其他

### 8.1 函數命名慣例觀察

模組中採用不同的命名前綴慣例：

| 前綴 | 語義 | 範例 |
|------|------|------|
| `validate_` | 驗證，返回 bool | `validate_file_path`, `validate_dataframe_columns` |
| `is_` | 類型判斷，返回 bool | `is_excel_file`, `is_csv_file` |
| `get_` | 取得值，可能為 None | `get_file_extension`, `get_file_info` |
| `ensure_` | 確保狀態，返回 bool | `ensure_directory_exists` |
| `safe_` | 安全操作，捕捉例外 | `safe_string_operation`, `copy_file_safely` |
| `clean_` | 清理資料 | `clean_nan_values`, `clean_pr_data` |
| `create_` | 建立新物件 | `create_mapping_dict` |
| `apply_` | 套用轉換 | `apply_mapping_safely` |
| `parse_` | 解析字串 | `parse_date_string` |
| `extract_` | 從字串提取 | `extract_date_range_from_description` |
| `convert_` | 格式轉換 | `convert_date_format_in_string` |
| `format_` | 格式化輸出 | `format_numeric_with_thousands` |
| `find_` | 搜尋集合 | `find_files_by_pattern` |
| `calculate_` | 計算 | `calculate_file_hash` |
| `classify_` | 分類 | `classify_description` |
| `give_` | 回填資料（特殊慣例） | `give_account_by_keyword` |

`give_` 前綴是此模組特有的慣例，語義上接近 `assign_` 或 `predict_`，略顯不正式。

### 8.2 `__init__.py` 中的缺漏

`helpers/__init__.py` 的 `__all__` 未包含：

```python
# 未匯出的函數（可直接導入但不在 __all__）
# data_utils.py
'classify_description'      # 在 spt_steps.py 直接從 data_utils 導入
'give_account_by_keyword'   # 在 spx_integration.py 直接從 data_utils 導入
'get_ref_on_colab'          # 在 spt_loading.py, spx_loading.py 直接從 helpers 導入（有！）
'extract_clean_description' # 在 spx_ppe_qty_validation.py 直接從 data_utils 導入
'clean_pr_data'             # 在 spt_steps.py 直接從 data_utils 導入
'clean_po_data'             # 在 spt_steps.py, spx_integration.py 直接從 data_utils 導入
```

注意：`get_ref_on_colab` 實際上**有**在 `helpers/__init__.py` 的 `__all__` 和 `utils/__init__.py` 的 `__all__` 中，且 `spt_loading.py` 使用 `from accrual_bot.utils.helpers import get_ref_on_colab`（透過 helpers 層）。其他函數則使用直接導入路徑繞過 helpers 的封裝。

### 8.3 測試覆蓋率分析

| 模組 | 測試函數數 | 覆蓋狀況 |
|------|-----------|---------|
| `column_utils.py` | 12 個測試方法 | 100%（CLAUDE.md 記載） |
| `data_utils.py` | 約 30 個測試方法 | 覆蓋主要公開函數；`classify_description`、`give_account_by_keyword`、`parallel_apply`、`memory_efficient_operation`、`extract_date_range_from_description` 無測試 |
| `file_utils.py` | 約 25 個測試方法 | 79%（CLAUDE.md 記載）；`get_resource_path`（PyInstaller 分支）、`get_directory_size`（邊界情況）有測試 |

**最大測試空白**：`extract_date_range_from_description` 完全沒有測試，而這個函數含有已知的 `NoneType` bug，且是 pipeline 中實際使用的業務邏輯。

### 8.4 與 `spe_bank_recon` 的關係

根據對 `metadata_builder` 的前期研究，`spe_bank_recon` 有自己的 `utils` 模組，與 `accrual_bot/utils/helpers/` 沒有直接複製關係。兩個專案共享相似的工具模式（防禦性設計、TOML 讀取），但 `helpers/` 是 accrual_bot 獨自演進的成果。

### 8.5 公開 API 摘要表

#### `column_utils.py::ColumnResolver`

| 方法 | 簽名 | 返回 | 說明 |
|------|------|------|------|
| `resolve` | `(df, canonical_name)` | `Optional[str]` | 解析欄位名稱，找不到返回 None |
| `resolve_or_raise` | `(df, canonical_name)` | `str` | 找不到拋出 ValueError |
| `has_column` | `(df, canonical_name)` | `bool` | 檢查欄位是否存在 |
| `resolve_multiple` | `(df, canonical_names)` | `Dict[str, Optional[str]]` | 批次解析 |
| `add_pattern` | `(canonical_name, pattern)` | `None` | 動態新增 pattern |

#### `file_utils.py` 主要函數

| 函數 | 返回型別 | 失敗行為 |
|------|---------|---------|
| `validate_file_path` | `bool` | 返回 False（帶日誌） |
| `ensure_directory_exists` | `bool` | 返回 False（帶日誌） |
| `copy_file_safely` | `bool` | 返回 False（帶日誌） |
| `calculate_file_hash` | `Optional[str]` | 返回 None |
| `get_file_info` | `Dict[str, Any]` | 返回空字典 |
| `load_toml` | `Dict[str, Any]` | 返回空字典（帶日誌） |
| `cleanup_temp_files` | `int` | 返回 0 |
| `find_files_by_pattern` | `List[str]` | 返回空列表 |
| `get_directory_size` | `Tuple[int, int]` | 返回 `(0, 0)` |

失敗時的返回型別保持一致（空值/空集合），使呼叫端可以安全地用 `if not result:` 判斷。
