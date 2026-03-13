# DuckDB Manager 模組深度研究文件

> **版本**: 基於 duckdb_manager v2.1.0
> **研究日期**: 2026-03-13
> **研究範圍**: `accrual_bot/utils/duckdb_manager/` 所有 15 個 Python 檔案

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

### 1.1 模組定位

`duckdb_manager` 是一個**自給自足的資料庫管理模組**，設計上可以「整個資料夾複製到其他專案直接使用」（可移植插件）。它封裝了 [DuckDB](https://duckdb.org/) 的底層 Python API，提供更高層次的操作介面。

DuckDB 是一個嵌入式 OLAP 資料庫引擎，定位類似 SQLite 但針對分析工作負載最佳化。在本專案（accrual_bot）中，DuckDB 主要用於：

- **Parquet 格式 Checkpoint 儲存**：`core/datasources/parquet_source.py` 使用 DuckDB 讀寫 Parquet 檔案
- **中間資料的結構化處理**：以 SQL 方式操作 DataFrame 數據
- **資料品質驗證**：在 pipeline 步驟間進行資料完整性檢查

### 1.2 版本演進

根據 `README.md` 和 `__init__.py` 的記錄：

| 版本 | 時間 | 重大變化 |
|------|------|---------|
| v1.0.0 | — | 初始版本，monolithic 設計 |
| v2.0.0 | 2026-02 | Mixin 架構重構，`manager.py` 從 979 行精簡至 231 行，加入 Schema 遷移 |
| v2.1.0 | 2026-02 | 新增 YAML 配置支援 |

v2.0 是關鍵版本——將單一巨型類別拆分為 Mixin 組合，這是一個典型的「分離關注點」重構案例。

---

## 2. 用途

### 2.1 功能全覽

模組提供以下六大功能群：

```
DuckDBManager
├── CRUD 操作（CRUDMixin）
│   ├── create_table_from_df  — DataFrame → DuckDB 表格
│   ├── insert_df_into_table  — 附加插入
│   ├── upsert_df_into_table  — 鍵值更新插入
│   ├── query_to_df           — SQL → DataFrame
│   ├── delete_data           — DELETE 語句
│   └── 便利方法（count_rows, query_single_value, ...）
│
├── 表格管理（TableManagementMixin）
│   ├── show_tables / list_tables_with_info
│   ├── describe_table / get_table_info / get_table_ddl
│   ├── drop_table / truncate_table
│   ├── backup_table          — 匯出 Parquet/CSV/JSON
│   └── clone_table_schema    — 複製結構不含資料
│
├── 資料清理（DataCleaningMixin）
│   ├── clean_numeric_column  — 移除千分位符號等
│   ├── alter_column_type     — 修改欄位型態（含驗證）
│   ├── clean_and_convert_column — 清理 + 轉型一站式
│   ├── preview_column_values — 資料預覽
│   └── add_column / rename_column / drop_column
│
├── 事務處理（TransactionMixin）
│   ├── execute_transaction   — 多步驟 SQL 事務
│   ├── validate_data_integrity — 完整性報告
│   ├── check_null_values     — NULL 統計
│   └── check_duplicates      — 重複記錄
│
├── Schema 遷移（migration 子套件）
│   ├── SchemaDiff            — 差異比對結果
│   ├── MigrationPlan         — 遷移計劃（操作清單 + 警告）
│   ├── MigrationStrategy     — 四種策略枚舉
│   └── SchemaMigrator        — 遷移執行器
│
└── 工具層（utils 子套件）
    ├── 日誌系統（可注入外部 logger）
    ├── 類型映射（Pandas dtype → DuckDB 類型）
    └── SQL 安全工具（防止 SQL 注入）
```

### 2.2 在 accrual_bot 中的實際使用情況

從 `tests/unit/utils/duckdb_manager/test_duckdb_manager.py` 和程式碼整體來看，模組主要被：

- `accrual_bot/utils/duckdb_manager/` — 模組本身（工具層）
- `core/datasources/duckdb_source.py` — 作為資料源的 DuckDB 介接層

---

## 3. 設計思路

### 3.1 核心架構：Mixin 組合模式

最關鍵的設計決策是**多重繼承 + Mixin 組合**：

```python
# manager.py
class DuckDBManager(
    CRUDMixin,
    TableManagementMixin,
    DataCleaningMixin,
    TransactionMixin
):
    # 僅負責: 初始化、連線管理、Context Manager
    ...
```

所有 Mixin 繼承自共同基類 `OperationMixin`：

```
                    OperationMixin
                   /    |    |    \
            CRUDMixin  TMMixin  DCMixin  TMixin
                   \    |    |    /
                    DuckDBManager
```

`OperationMixin` 作用如下：
1. **定義屬性類型提示**：`conn`, `config`, `logger` — 這些屬性由 `DuckDBManager.__init__` 提供，但 Mixin 需要知道它們的類型
2. **提供共用輔助方法**：`_table_exists()`, `_execute_sql()`, `_begin()`, `_commit()`, `_rollback()`, `_atomic()`

這種設計的關鍵是「**Mixin 本身不可實例化**」——它們只是能力的集合，必須透過 `DuckDBManager` 獲得真正的連線物件和配置。

### 3.2 多形配置解析（Polymorphic Config Resolution）

`_resolve_config()` 實現了一個輕量級的「多形輸入接受器」：

```python
def _resolve_config(self, config):
    if config is None:          → DuckDBConfig()          # 預設值
    if isinstance(config, DuckDBConfig):   → config        # 直接使用
    if isinstance(config, dict):           → DuckDBConfig.from_dict(config)
    if isinstance(config, (str, Path)):    → DuckDBConfig(db_path=str(config))
    raise TypeError(...)
```

這讓 API 使用者無需關心配置物件的建立方式，降低使用門檻。

### 3.3 可插拔日誌系統（Pluggable Logger）

`utils/logging.py` 定義了 `LoggerProtocol`（結構性子型別）：

```python
@runtime_checkable
class LoggerProtocol(Protocol):
    def debug(self, msg: str, ...) -> None: ...
    def info(self, msg: str, ...) -> None: ...
    def warning(self, msg: str, ...) -> None: ...
    def error(self, msg: str, ...) -> None: ...
```

`@runtime_checkable` 讓 `isinstance(obj, LoggerProtocol)` 在執行期可用。任何符合這四個方法簽章的物件都可作為日誌器，包括：

- Python 標準 `logging.Logger`
- `loguru.Logger`（第三方）
- 本模組的 `NullLogger`
- 任何自定義日誌類

`get_logger()` 的注入邏輯：
```python
def get_logger(name, level, external_logger=None):
    if external_logger is not None:
        return external_logger   # 優先使用外部注入
    # 否則建立內建日誌器
```

這在 `accrual_bot` 中很重要——可以將 DuckDB 的日誌整合進專案的統一日誌體系（`accrual_bot.utils.logging.logger.Logger`）。

### 3.4 事務原子性的兩種機制

模組提供了兩套事務處理機制，服務不同層次：

**外部 API（`execute_transaction`）**：
- 接受 SQL 字串列表
- 手動管理 BEGIN/COMMIT/ROLLBACK
- 失敗時拋出 `DuckDBTransactionError`

**內部 API（`_atomic()` context manager）**：
```python
@contextmanager
def _atomic(self):
    self._begin()
    try:
        yield
        self._commit()
    except Exception:
        self._rollback()
        raise
```
- 被 Mixin 內部方法使用（如 `create_table_from_df` 的 replace 邏輯）
- Python context manager 慣用法，確保異常時自動 rollback
- `_rollback()` 靜默處理「無活躍事務」的情況

### 3.5 Schema 遷移的三層設計

`migration` 子套件採用三層職責分離：

```
SchemaDiff（資料層）
   └── 純資料結構：記錄哪些欄位新增/移除/類型變更
   └── compare() 類別方法：執行比對邏輯

MigrationPlan + MigrationPlanner（計劃層）
   └── 根據 SchemaDiff + MigrationStrategy 生成 SQL 操作清單
   └── 計劃與執行分離，支援「乾跑」（dry_run）預覽

SchemaMigrator（執行層）
   └── 接受 DuckDBManager 實例，組合差異分析與計劃
   └── auto_migrate() 提供智慧自動化入口
```

四種策略的行為矩陣：

| 策略 | 新增欄位 | 移除欄位 | 類型變更 | 備份 | 實際執行 |
|------|---------|---------|---------|------|---------|
| `safe` | ✓ | ✗（警告） | ✗（警告） | ✗ | ✓ |
| `force` | ✓ | ✓ | ✓（警告） | ✗ | ✓ |
| `backup_first` | ✓ | ✓ | ✓（警告） | ✓ | ✓ |
| `dry_run` | 只顯示 | 只顯示 | 只顯示 | ✗ | ✗ |

### 3.6 向後相容的異常別名機制

`exceptions.py` 中一個精巧的工廠函數：

```python
def _deprecated_alias(old_name: str, new_class: type) -> type:
    class DeprecatedAlias(new_class):
        def __init__(self, *args, **kwargs):
            warnings.warn(
                f"'{old_name}' 已棄用，請使用 '{new_class.__name__}'",
                DeprecationWarning,
                stacklevel=2
            )
            super().__init__(*args, **kwargs)
    DeprecatedAlias.__name__ = old_name
    DeprecatedAlias.__qualname__ = old_name
    return DeprecatedAlias

ConnectionError = _deprecated_alias("ConnectionError", DuckDBConnectionError)
```

關鍵技術點：
- 動態建立繼承新類的子類別
- `stacklevel=2` 讓警告指向呼叫端（而非工廠函數內部）
- 手動設定 `__name__` 和 `__qualname__` 讓 repr 顯示正確
- 每個舊名稱都是獨立的動態類別，`isinstance` 仍能正常運作

---

## 4. 各項知識點

### 4.1 DuckDB 的 DataFrame 直接查詢特性

DuckDB 最強大的特性之一是可以在 SQL 中直接引用 Python 變數名稱：

```python
# crud.py:106
self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')
```

這裡 `df` 是 Python 本地變數（`pd.DataFrame`），DuckDB 會自動掃描呼叫幀（call frame）中的變數。這省去了必須先透過 `execute(query, params)` 傳入參數的步驟，但也意味著：

> **隱式依賴**：SQL 字串中的 `df` 必須在呼叫 `conn.sql()` 的函數作用域中存在，否則 DuckDB 會找不到此變數。

這是 DuckDB 的特有行為，在 SQLite / PostgreSQL 等其他資料庫的 Python 驅動程式中不存在。

### 4.2 類型映射策略（Pandas → DuckDB）

`utils/type_mapping.py` 的映射表有幾個重要細節：

**Nullable 整數型別（大寫 I）**：
```python
"Int64": "BIGINT",   # pandas 的可為 NULL 的整數（大寫 I）
"int64": "BIGINT",   # 標準 numpy 整數（小寫 i）
```

pandas 1.0+ 引入了 `Int64`（大寫，可為 NULL）和 `int64`（小寫，不可 NULL）兩套並行的整數類型。映射表分別處理了這兩種情況。

**複雜 datetime 格式的 fallback**：
```python
if "datetime64" in pandas_dtype:
    return "TIMESTAMP"
```

`datetime64[ns, UTC]`、`datetime64[ns, Asia/Taipei]` 等帶時區的格式無法精確匹配，使用字串包含判斷作為 fallback。

**預設值為 VARCHAR**：
任何無法識別的 dtype 都回傳 `VARCHAR`，這保證了相容性但可能造成類型資訊遺失。

### 4.3 `TRY_CAST`：DuckDB 的安全轉型函數

`data_cleaning.py` 使用了 DuckDB 特有的 `TRY_CAST`：

```sql
SELECT COUNT(*) as invalid_count
FROM "table"
WHERE "column" IS NOT NULL
AND TRY_CAST("column" AS BIGINT) IS NULL
```

`TRY_CAST` 在轉型失敗時回傳 `NULL`（而非報錯），這讓「哪些資料無法轉型」的查詢變得優雅。這是標準 SQL `CAST` 的安全版本，在 DuckDB、SQL Server、Snowflake 等系統中都有類似機制（不同名稱）。

### 4.4 SQL 識別符安全（Identifier Quoting）

`utils/query_builder.py` 的 `SafeSQL.quote_identifier()` 使用雙引號包裹識別符：

```python
def quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')  # 轉義內部雙引號
    return f'"{escaped}"'
```

SQL 標準中：
- **雙引號**用於識別符（表名、欄位名）：`"my table"`, `"column""with""quotes"`
- **單引號**用於字串字面值：`'value'`

`escape_string()` 對應字串值的安全處理（單引號加倍轉義）：
```python
def escape_string(value: str) -> str:
    return value.replace("'", "''")  # SQL 標準：單引號加倍
```

**`IDENTIFIER_PATTERN`** 用於識別「不需要引號」的安全識別符（只含字母、數字、底線）：
```python
IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
```

### 4.5 `TYPE_CHECKING` 防止循環導入

`operations/base.py` 和 `migration/migrator.py` 都使用了：

```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import duckdb
    from ..manager import DuckDBManager
```

`TYPE_CHECKING` 是 `False`（僅在型別檢查工具如 mypy 執行時為 `True`），因此這些導入只存在於靜態分析層面，執行期不會觸發。

原因：`OperationMixin` 需要引用 `DuckDBManager`（型別提示用），而 `DuckDBManager` 又繼承 `OperationMixin`，形成循環。`TYPE_CHECKING` 優雅地打破這個循環。

### 4.6 `SHOW TABLES` vs `information_schema`

`_table_exists()` 使用 DuckDB 的 `SHOW TABLES`：

```python
def _table_exists(self, table_name: str) -> bool:
    existing_tables = self.conn.sql("SHOW TABLES").df()
    return (
        table_name in existing_tables['name'].values
        if not existing_tables.empty else False
    )
```

DuckDB 也支援標準 SQL 的 `information_schema.tables`，但 `SHOW TABLES` 更簡潔。注意此查詢在每次需要判斷表格存在性時都會發出，在有大量表格的資料庫中，這可能成為效能瓶頸。

### 4.7 `logger.propagate = False` 的重要性

在 `get_logger()` 中設定 `logger.propagate = False`：

```python
logger.addHandler(handler)
logger.propagate = False  # 防止日誌傳播到父 logger
```

Python logging 的預設行為是將日誌記錄向上傳播到父 logger（直到根 logger）。若根 logger 也有 handler，同一條訊息會被輸出兩次。設定 `propagate = False` 避免了這種重複輸出，對獨立模組特別重要。

### 4.8 DuckDB 時區設定的雙重備援

`_setup_timezone()` 展示了有彈性的備援設計：

```python
def _setup_timezone(self):
    try:
        self.conn.sql(f"SET timezone='{self.config.timezone}'")  # DuckDB 原生設定
    except Exception:
        import os, time
        os.environ['TZ'] = self.config.timezone  # 回退到環境變數
        if hasattr(time, 'tzset'):  # tzset() 僅存在於 Unix
            try:
                time.tzset()
            except Exception:
                pass
```

DuckDB 內建時區設定只影響當前連線的時區解釋，不影響全域 Python 環境。環境變數方式則影響整個 process，有副作用。`hasattr(time, 'tzset')` 確保 Windows 相容性（Windows 上 `time` 模組無 `tzset`）。

---

## 5. 應用範例

### 5.1 基本使用流程

```python
import pandas as pd
from accrual_bot.utils.duckdb_manager import DuckDBManager, DuckDBConfig

# 方式一：記憶體資料庫（測試用）
with DuckDBManager() as db:
    df = pd.DataFrame({
        'order_id': ['PO-001', 'PO-002'],
        'amount': ['1,234,567', '2,345,678'],  # 含千分位的字串
        'date': pd.to_datetime(['2025-01', '2025-02'])
    })

    # 建立表格
    db.create_table_from_df('orders', df)

    # 查詢
    result = db.query_to_df("SELECT * FROM orders WHERE amount LIKE '%2%'")
    print(result)
```

### 5.2 整合專案日誌系統

```python
from accrual_bot.utils.logging import get_logger
from accrual_bot.utils.duckdb_manager import DuckDBManager, DuckDBConfig

# 使用 accrual_bot 的統一日誌系統
project_logger = get_logger('accrual_bot.database')

config = DuckDBConfig(
    db_path='./data/pipeline_results.duckdb',
    logger=project_logger,          # 注入外部 logger
    enable_query_logging=True,
    timezone='Asia/Taipei'
)

with DuckDBManager(config) as db:
    db.create_table_from_df('spt_results', spt_df, if_exists='replace')
```

### 5.3 資料清理流程

```python
with DuckDBManager('./pipeline_data.duckdb') as db:
    # 步驟一：預覽，了解資料格式
    preview = db.preview_column_values('spt_data', 'invoice_amount', limit=20)
    # 輸出可能顯示: '1,234,567', '$5,678', '9,012 元'

    # 步驟二：乾跑模式，確認清理範圍
    db.clean_numeric_column(
        'spt_data', 'invoice_amount',
        remove_chars=[',', '$', '元', ' '],
        preview_only=True   # 只預覽，不修改
    )

    # 步驟三：確認後一站式清理 + 轉型
    success = db.clean_and_convert_column(
        'spt_data', 'invoice_amount',
        target_type='BIGINT',
        remove_chars=[',', '$', '元', ' '],
        handle_empty_as_null=True
    )
    # 內部: UPDATE (清理) → UPDATE (空→NULL) → ALTER TYPE
    # 全部在單一 Transaction 中，失敗自動 rollback
```

### 5.4 Schema 遷移（應對月份資料結構變化）

accrual_bot 每月處理的 Excel 格式可能隨時間更動，Schema 遷移功能能安全地處理此情況：

```python
from accrual_bot.utils.duckdb_manager import DuckDBManager
from accrual_bot.utils.duckdb_manager.migration import SchemaMigrator

with DuckDBManager('./historical_data.duckdb') as db:
    migrator = SchemaMigrator(db)

    # 新月份的 DataFrame 可能多了幾個欄位
    new_month_df = load_current_month_data()

    # 先比對差異
    diff = migrator.compare_schema('spt_history', new_month_df)
    if diff.has_changes:
        print(diff.report())
        # Schema Diff for 'spt_history':
        #   Added columns:
        #     + commission_rate (DOUBLE)
        #     + media_code (VARCHAR)
        #   Summary: 2 added, 0 removed, 0 type changed
        #   Status: SAFE (can migrate without data loss)

    # 安全遷移（只允許新增欄位，不會遺失資料）
    result = migrator.migrate('spt_history', new_month_df, strategy='safe')

    if result['success']:
        db.insert_df_into_table('spt_history', new_month_df)
    else:
        print(f"遷移失敗: {result['errors']}")
```

### 5.5 完整性驗證

```python
with DuckDBManager('./output.duckdb') as db:
    integrity = db.validate_data_integrity(
        'spx_results',
        checks={
            'no_negative_amount': 'SELECT COUNT(*) FROM "{table_name}" WHERE amount < 0',
            'all_have_status': 'SELECT COUNT(*) FROM "{table_name}" WHERE status IS NULL',
            'unique_po_numbers': '''
                SELECT po_number, COUNT(*) as cnt
                FROM "{table_name}"
                GROUP BY po_number HAVING COUNT(*) > 1
            '''
        }
    )

    print(f"總筆數: {integrity['total_rows']}")
    print(f"重複行: {integrity['duplicate_rows']}")
    print(f"NULL 統計: {integrity['null_counts']}")
    print(f"自訂檢查: {integrity['custom_checks']}")
```

### 5.6 事務性批次更新

```python
with DuckDBManager('./data.duckdb') as db:
    # 多步驟操作，全部成功或全部回滾
    operations = [
        "UPDATE spt_results SET status = 'confirmed' WHERE status = 'pending'",
        "INSERT INTO audit_log (action, ts) VALUES ('monthly_confirm', NOW())",
        "DELETE FROM spt_results WHERE status = 'cancelled' AND date < '2024-01-01'"
    ]

    success = db.execute_transaction(operations)
    if not success:
        print("事務失敗，所有操作已回滾")
```

### 5.7 從 TOML 配置載入

```toml
# accrual_bot/config/config.toml（假設新增此 section）
[duckdb]
db_path = "./data/pipeline.duckdb"
timezone = "Asia/Taipei"
log_level = "INFO"
read_only = false
enable_query_logging = true
```

```python
from accrual_bot.utils.duckdb_manager import DuckDBConfig, DuckDBManager

config = DuckDBConfig.from_toml('./accrual_bot/config/config.toml', section='duckdb')
with DuckDBManager(config) as db:
    ...
```

---

## 6. 優缺分析

### 6.1 優點

#### 6.1.1 高度可移植性

模組設計為「複製資料夾即可使用」，所有依賴（除 duckdb/pandas）都是 Python 標準庫（`dataclasses`, `typing`, `logging`, `contextlib`, `pathlib`）。這讓它成為一個真正的可插拔工具。

#### 6.1.2 配置彈性（五種輸入格式）

`_resolve_config` + `DuckDBConfig` 的組合讓 API 對使用者極其友好：

```python
DuckDBManager()                           # 最簡
DuckDBManager("./data.duckdb")            # 快速
DuckDBManager({"db_path": "./d.duckdb"}) # 字典（適合程式生成配置）
DuckDBManager(DuckDBConfig(...))          # 完整控制
DuckDBConfig.from_toml(...)              # 外部化配置
```

#### 6.1.3 Mixin 架構的可擴展性

`OperationMixin` 基類讓新增功能非常直覺：

```python
class MyCustomMixin(OperationMixin):
    def my_special_query(self) -> pd.DataFrame:
        return self.conn.sql("SELECT ...").df()  # conn 由 DuckDBManager 提供

class ExtendedManager(DuckDBManager, MyCustomMixin):
    pass
```

不需要修改既有程式碼，符合開放/封閉原則（OCP）。

#### 6.1.4 日誌注入解耦

`LoggerProtocol` 的結構性子型別設計讓日誌系統完全解耦。這對 `accrual_bot` 特別重要——可以將 DuckDB 操作日誌無縫整合到 `StructuredLogger` 或任何符合協議的日誌物件中。

#### 6.1.5 Schema 遷移的策略模式

四種遷移策略（SAFE/FORCE/BACKUP_FIRST/DRY_RUN）採用策略模式，且支援字串輸入（`"safe"` → `MigrationStrategy.SAFE`），對使用者友好。`auto_migrate()` 的智慧判斷（只有純新增才能自動遷移）保護了資料安全。

#### 6.1.6 向後相容的棄用機制

`_deprecated_alias` 工廠函數提供了優雅的遷移路徑：舊程式碼繼續工作（並收到警告），新程式碼使用新名稱。這比直接刪除舊名稱或靜默保留舊名稱都更好。

#### 6.1.7 乾跑（Dry Run）模式

`backup_table` 的 `preview_only` 和 `migrate` 的 `dry_run` 策略都提供了「先預覽後執行」的安全操作模式，特別適合財務資料處理的謹慎場景。

---

### 6.2 缺點與問題

#### 6.2.1 **Bug：`validation_success` 結果從未使用**

`data_cleaning.py:262-264`：

```python
# 先驗證 (在事務外，只讀操作)
validation_success = self._validate_conversion(
    table_name, column_name, target_type
)
# ❌ validation_success 之後從未被讀取或判斷！
```

`_validate_conversion()` 的回傳值（`True`/`False`）存入 `validation_success`，但程式碼繼續往下執行，沒有 `if not validation_success: return False`。這意味著即使驗證失敗，`_atomic()` 事務仍會開始執行，直到事務內的第二次驗證（第 303-318 行）才真正阻止問題——但此時日誌輸出可能已誤導使用者。

#### 6.2.2 **SQL 安全工具使用不一致**

模組有完整的 `SafeSQL` 類，但部分地方仍使用手動字串拼接：

```python
# crud.py:177-179 — upsert_df_into_table 中的手動轉義
escaped_values = [str(v).replace("'", "''") for v in unique_values]
values_str = "', '".join(escaped_values)

# table_management.py:200 — backup_table 中的手動轉義
safe_path = backup_path.replace("'", "''")
```

這些手動轉義雖然功能上正確，但沒有使用已存在的 `SafeSQL.escape_string()`，造成兩個潛在問題：
1. 將來若 `SafeSQL` 修改轉義邏輯，這些地方會不同步
2. 審查程式碼的人需要額外確認手動轉義是否正確

#### 6.2.3 **`connection_timeout` 是無效配置**

`config.py:39`：
```python
connection_timeout: int = 30
```

`manager.py:197-200`：
```python
self.conn = duckdb.connect(
    self.config.db_path,
    read_only=self.config.read_only,
    # ❌ connection_timeout 完全沒有傳入！
)
```

DuckDB 的 `duckdb.connect()` 本身不支援 `timeout` 參數，因此此配置項無論設定什麼值都完全沒有效果。這是一個「死設定」，可能給使用者造成誤導（以為設定了逾時保護，但實際上沒有）。

#### 6.2.4 **N+1 查詢問題**

兩個方法存在 N+1 查詢（針對 N 個欄位/表格各發一次查詢）：

```python
# transaction.py:172-178 — check_null_values
for col_name in columns:
    count = self.conn.sql(
        f'SELECT COUNT(*) as count FROM "{table_name}" WHERE "{col_name}" IS NULL'
    ).df()  # N 個欄位 = N 次查詢

# table_management.py:259-267 — list_tables_with_info
for table_name in tables_df['name']:
    info = self.get_table_info(table_name)  # 每個表格 2 次查詢（COUNT + DESCRIBE）
```

更高效的作法是使用單一 SQL 查詢（如 `COUNT(CASE WHEN col IS NULL THEN 1 END)` 對所有欄位），但目前的設計在表格欄位數量或表格數量較多時，效能會線性下降。

#### 6.2.5 **`OperationMixin.logger` 型別標注不精確**

```python
# base.py:33
logger: any  # 可以是 logging.Logger 或任何符合 LoggerProtocol 的物件
```

`any`（小寫）不是有效的 Python 型別標注。應使用 `Any`（`from typing import Any`）或更精確的 `Union[logging.Logger, LoggerProtocol]`。這個錯誤雖然在執行期無影響，但對靜態分析工具（mypy、pyright）會造成困惑。

#### 6.2.6 **事務處理的重複 BEGIN/COMMIT 邏輯**

`execute_transaction()` 在 `transaction.py` 中直接使用字串呼叫 `BEGIN`/`COMMIT`/`ROLLBACK`，而沒有使用 `OperationMixin` 中已定義的 `_begin()`/`_commit()`/`_rollback()` 輔助方法：

```python
# transaction.py:37 — 應使用 self._begin() 但直接呼叫
self.conn.sql("BEGIN TRANSACTION")
# transaction.py:47 — 應使用 self._rollback()
self.conn.sql("ROLLBACK")
```

這造成 `_rollback()` 的靜默處理邏輯（`except Exception: pass`）在 `execute_transaction` 中不一致——此方法直接呼叫 `ROLLBACK` 而不用 try/except 包裹。

#### 6.2.7 **`MigrationStrategy.SAFE` 的邏輯複雜度**

`strategies.py:143-149` 中的 SAFE 策略後處理邏輯有冗餘條件：

```python
if strategy == MigrationStrategy.SAFE:
    if diff.removed_columns or diff.type_changed_columns:
        will_execute = will_execute and len(operations) > 0
        if diff.removed_columns or diff.type_changed_columns:  # 重複判斷！
            warnings.append(...)
```

外層 `if diff.removed_columns or diff.type_changed_columns:` 已存在，內層的相同判斷是無意義的重複。

---

## 7. 延伸議題

### 7.1 DuckDB vs SQLite：何時選哪個？

本模組選擇 DuckDB 而非 SQLite，值得思考背後的取捨：

| 面向 | DuckDB | SQLite |
|------|--------|--------|
| 優化方向 | OLAP（分析查詢） | OLTP（事務處理） |
| 欄位掃描 | 列式儲存，快 | 行式儲存，慢 |
| 並行讀取 | 支援多讀者 | WAL 模式支援 |
| Pandas 整合 | 原生掃描本地 df | 需 `pd.to_sql` |
| Parquet 支援 | 原生支援 | 不支援 |
| 部署大小 | 較大 | 極輕量 |

accrual_bot 的使用場景（大型 DataFrame 的分析處理、Parquet checkpoint）非常適合 DuckDB。

### 7.2 Thread Safety 考量

目前 `DuckDBManager` 沒有任何執行緒保護機制。`duckdb.connect()` 返回的連線物件不是執行緒安全的（每個執行緒應有獨立連線）。

`accrual_bot` 的 `ConfigManager` 使用了雙重檢查鎖（Double-Checked Locking）確保執行緒安全，但 `DuckDBManager` 沒有類似設計。若在非同步 Pipeline 環境中多個步驟共用同一個 `DuckDBManager` 實例，可能產生資料競態。

建議：在多執行緒場景下，每個執行緒應創建獨立的 `DuckDBManager` 實例（利用 `:memory:` 模式或檔案鎖）。

### 7.3 記憶體 vs 檔案資料庫的選擇策略

`:memory:` 模式的使用場景：
- 單次 Pipeline 執行中的臨時結構化查詢
- 測試（不需要持久化，清理簡單）
- 中間計算結果（避免磁碟 I/O 開銷）

檔案資料庫的使用場景：
- Checkpoint 儲存（需要跨 session 的持久化）
- 大型資料集（超過記憶體限制）
- 需要備份的歷史資料

### 7.4 Schema 遷移的局限性：無法處理重命名

`SchemaDiff` 目前偵測不到欄位重命名，只能識別「移除舊欄位 + 新增新欄位」：

```python
class ChangeType(Enum):
    ADDED = "added"
    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"
    RENAMED = "renamed"   # ← 定義了，但 compare() 中從未生成此類型！
```

`ChangeType.RENAMED` 雖在 `__str__` 中有處理邏輯，但 `SchemaDiff.compare()` 的實現完全沒有重命名偵測邏輯（這通常需要啟發式算法，如 Levenshtein 距離比較）。這是一個「預留接口但未完成」的功能。

### 7.5 可觀測性增強方向

目前模組的日誌主要是人類可讀的文字。若要增強可觀測性，可以考慮：

- **結構化日誌**：記錄每次操作的執行時間、影響行數、SQL 長度
- **指標收集**：操作頻率、錯誤率、查詢延遲
- **追蹤 ID**：關聯 Pipeline 執行 ID 與資料庫操作，方便問題追蹤

`accrual_bot` 的 `StructuredLogger` 已有 `log_operation_start`、`log_progress` 等結構化方法，是整合的良好起點。

### 7.6 配置驗證的完整性問題

`DuckDBConfig.__post_init__` 驗證了 `log_level`（必須是有效值）並確保父目錄存在，但沒有驗證：

- `timezone` 是否為有效時區字串（如 `"Invalid/Zone"` 不會在配置時報錯，而是在 `_setup_timezone()` 時才失敗）
- `connection_timeout` 是否為正整數（負值或零值是允許的但無意義）
- `db_path` 是否在唯讀模式下確實存在（唯讀打開不存在的檔案會在 `_connect()` 時報錯）

提前驗證（Fail Fast）能提供更友好的錯誤訊息。

### 7.7 模組獨立性與 accrual_bot 整合的張力

模組設計為「可移植插件」，但在 `accrual_bot` 中使用時，可以更深度整合：

- 使用 `accrual_bot.utils.config.config_manager` 讀取資料庫配置（而非獨立的 TOML 檔案）
- 使用 `accrual_bot.utils.logging.get_logger()` 注入統一日誌（已有此支援）
- 使用 `accrual_bot.utils.helpers.file_utils.ensure_directory_exists()` 取代 `DuckDBConfig.__post_init__` 中的 `path.parent.mkdir()`

目前模組保持完全獨立，這是個有意識的設計選擇，方便移植到 `spe_bank_recon` 等其他專案。

---

## 8. 其他觀察

### 8.1 模組歸屬與命名

模組路徑為 `accrual_bot/utils/duckdb_manager/`，但 `__init__.py` 中的模組文檔字串顯示 `from duckdb_manager import DuckDBManager`（沒有 `accrual_bot` 前綴），反映了其「可移植插件」的設計初衷——可以直接複製到其他專案使用，而導入路徑不變。

### 8.2 `__version__` 與 `__author__` 的意義

```python
__version__ = "2.1.0"
__author__ = "SPE Bank Recon Team"
```

這表示 `duckdb_manager` 起源於另一個專案（`spe_bank_recon`），被移植/共享到 `accrual_bot`。這也解釋了為何模組設計如此注重「可移植性」——它本來就要跨專案使用。

### 8.3 `ColoredFormatter` 的 Windows 相容性

`utils/logging.py` 中：

```python
@staticmethod
def _supports_color() -> bool:
    if sys.platform == "win32":
        try:
            import os
            return os.isatty(sys.stdout.fileno())
        except Exception:
            return False
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
```

這處理了 Windows 的 ANSI 顏色支援問題。在 Windows Terminal、Git Bash 等現代終端中，`isatty()` 會返回 `True`。但在 Windows 舊版 CMD 中，ANSI 碼不被支援，`try/except` 提供了保護。

`accrual_bot` 在 Windows/WSL 環境運行（從 CLAUDE.md 的指令格式判斷），此跨平台考慮是必要的。

### 8.4 `backup_table` 的時間戳記格式

```python
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
backup_path = f"{table_name}_backup_{timestamp}.{backup_format}"
```

備份文件名使用秒精度的時間戳記（如 `users_backup_20260313_143022.parquet`）。在快速連續備份的場景（如自動化測試）可能產生衝突，但對人工觸發的備份操作已足夠。

### 8.5 測試覆蓋現況

根據 `CLAUDE.md` 的覆蓋率報告：

- `utils/duckdb_manager/config.py`: **77%**
- `utils/duckdb_manager/manager.py`: **81%**
- `utils/duckdb_manager/operations/*`: **8-34%**（主要低覆蓋區域）

`operations/` 子模組的低覆蓋率（特別是 `data_cleaning.py` 和 `transaction.py` 的較複雜路徑）意味著上述發現的 `validation_success` bug 和 N+1 查詢問題很可能在測試中未被偵測到。

### 8.6 `MigrationPlanner.can_auto_migrate` 的保守策略

```python
@classmethod
def can_auto_migrate(cls, diff: SchemaDiff) -> bool:
    return diff.is_safe and len(diff.added_columns) > 0
```

這個方法只在「純新增欄位且至少有一個新欄位」時才允許自動遷移。`diff.is_safe` 的定義是「無移除、無類型變更」，這非常保守。

對於財務資料處理系統（accrual_bot）而言，這種保守策略是正確的工程決策——寧可讓使用者手動確認，也不自動執行任何可能影響既有資料的操作。

---

## 附錄：模組類別關係圖

```
DuckDBConfig (dataclass)
├── from_dict()
├── from_toml()
├── from_yaml()
├── from_path()
├── to_dict()
└── copy()

DuckDBManagerError (Exception)
├── DuckDBConnectionError
├── DuckDBTableError
│   ├── DuckDBTableExistsError
│   └── DuckDBTableNotFoundError
├── DuckDBQueryError
├── DuckDBDataValidationError
├── DuckDBTransactionError
├── DuckDBConfigurationError
└── DuckDBMigrationError

OperationMixin
├── _table_exists()
├── _execute_sql()
├── _execute_sql_no_return()
├── _begin() / _commit() / _rollback()
└── _atomic() [context manager]

CRUDMixin(OperationMixin)
TableManagementMixin(OperationMixin)
DataCleaningMixin(OperationMixin)
TransactionMixin(OperationMixin)

DuckDBManager(CRUDMixin, TableManagementMixin, DataCleaningMixin, TransactionMixin)
├── __init__(config)  → _resolve_config() → _connect() → _setup_timezone()
├── __enter__() / __exit__()  [Context Manager]
├── close()
└── 屬性: database_path, is_memory_db, is_connected

ChangeType (Enum): ADDED, REMOVED, TYPE_CHANGED, RENAMED
ColumnChange (dataclass): column_name, change_type, old_type, new_type, new_name
SchemaDiff (dataclass): table_name, changes, current_schema, target_schema
    └── compare() [classmethod]

MigrationStrategy (Enum): SAFE, FORCE, BACKUP_FIRST, DRY_RUN
MigrationPlan (dataclass): strategy, diff, operations, warnings, will_execute, backup_required
MigrationPlanner: create_plan(), can_auto_migrate()
SchemaMigrator: compare_schema(), create_migration_plan(), migrate(), auto_migrate()

LoggerProtocol (Protocol, @runtime_checkable)
NullLogger
ColoredFormatter(logging.Formatter)
get_logger() / setup_file_logger()

SafeSQL: quote_identifier(), escape_string(), quote_value(), quote_values(),
         is_safe_identifier(), escape_like_pattern(), build_in_clause(), build_where_equals()

PANDAS_TO_DUCKDB_MAPPING (dict)
get_duckdb_dtype(pandas_dtype: str) → str
```
