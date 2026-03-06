# DuckDB Manager 模組使用文件

> **版本**: 2.1.0
> **更新日期**: 2026-02
> **作者**: SPE Bank Recon Team

## 目錄

1. [概述](#概述)
2. [安裝與依賴](#安裝與依賴)
3. [快速開始](#快速開始)
4. [配置方式](#配置方式)
5. [核心功能](#核心功能)
   - [CRUD 操作](#crud-操作)
   - [表格管理](#表格管理)
   - [資料清理](#資料清理)
   - [事務處理](#事務處理)
6. [Schema 遷移](#schema-遷移)
7. [日誌系統](#日誌系統)
8. [SQL 安全工具](#sql-安全工具)
9. [異常處理](#異常處理)
10. [擴展指南](#擴展指南)
11. [API 參考](#api-參考)
12. [最佳實踐](#最佳實踐)

---

## 概述

DuckDB Manager 是一個高可用、可移植的 DuckDB 資料庫管理模組，設計為可獨立使用的插件。

### 主要特性

- **多種配置方式**: 支援字串路徑、字典、dataclass、TOML 檔案、YAML 檔案
- **可插拔日誌**: 支援外部日誌器注入或使用內建日誌
- **完整 CRUD**: 建立、讀取、更新、刪除操作
- **Schema 遷移**: 自動比對和遷移資料庫 Schema
- **資料清理**: 數字欄位清理、類型轉換
- **事務支援**: 多步驟事務處理與回滾
- **SQL 安全**: 防止 SQL 注入的工具函數

### 模組結構

```
duckdb_manager/
├── __init__.py              # 公開 API
├── config.py                # DuckDBConfig 配置類
├── exceptions.py            # 自定義異常
├── manager.py               # DuckDBManager 核心類
├── operations/              # 操作 Mixin
│   ├── crud.py              # CRUD 操作
│   ├── table_management.py  # 表格管理
│   ├── data_cleaning.py     # 資料清理
│   └── transaction.py       # 事務處理
├── migration/               # Schema 遷移
│   ├── schema_diff.py       # 差異比對
│   ├── strategies.py        # 遷移策略
│   └── migrator.py          # 遷移器
└── utils/                   # 工具模組
    ├── logging.py           # 日誌系統
    ├── type_mapping.py      # 類型映射
    └── query_builder.py     # SQL 安全工具
```

---

## 安裝與依賴

### 必要依賴

```bash
pip install duckdb pandas
```

### 可選依賴

```bash
pip install tomli   # Python 3.10 以下需要，用於 TOML 配置
pip install pyyaml  # 用於 YAML 配置
```

### 作為獨立模組使用

將整個 `duckdb_manager` 資料夾複製到您的專案中：

```bash
cp -r duckdb_manager /path/to/your/project/
```

然後直接導入：

```python
from duckdb_manager import DuckDBManager, DuckDBConfig
```

---

## 快速開始

### 最簡單的使用方式

```python
from duckdb_manager import DuckDBManager
import pandas as pd

# 建立記憶體資料庫
with DuckDBManager() as db:
    # 建立表格
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    db.create_table_from_df('users', df)

    # 查詢資料
    result = db.query_to_df('SELECT * FROM users WHERE age > 25')
    print(result)
```

### 使用檔案資料庫

```python
with DuckDBManager('./data/my_database.duckdb') as db:
    # 列出所有表格
    tables = db.show_tables()
    print(tables)

    # 取得表格資訊
    info = db.get_table_info('users')
    print(f"表格有 {info['row_count']} 筆資料")
```

---

## 配置方式

DuckDBManager 支援 5 種配置方式：

### 方式 1: 字串路徑 (最簡單)

```python
db = DuckDBManager('./data.duckdb')
db = DuckDBManager(':memory:')  # 記憶體模式
```

### 方式 2: 字典配置

```python
config = {
    'db_path': './data.duckdb',
    'timezone': 'Asia/Taipei',
    'log_level': 'DEBUG',
    'read_only': False,
}
db = DuckDBManager(config)
```

### 方式 3: DuckDBConfig 物件 (推薦)

```python
from duckdb_manager import DuckDBConfig

config = DuckDBConfig(
    db_path='./data.duckdb',
    timezone='Asia/Taipei',
    log_level='INFO',
    read_only=False,
    connection_timeout=30,
    enable_query_logging=True,
)
db = DuckDBManager(config)
```

### 方式 4: 從 TOML 檔案載入

```toml
# config.toml
[database]
db_path = "./data.duckdb"
timezone = "Asia/Taipei"
log_level = "INFO"
read_only = false
```

```python
config = DuckDBConfig.from_toml('config.toml', section='database')
db = DuckDBManager(config)
```

### 方式 5: 從 YAML 檔案載入

```yaml
# config.yaml
database:
  db_path: "./data.duckdb"
  timezone: "Asia/Taipei"
  log_level: "INFO"
  read_only: false
  connection_timeout: 30
  enable_query_logging: true
```

```python
config = DuckDBConfig.from_yaml('config.yaml', section='database')
db = DuckDBManager(config)
```

> **注意**: 需要安裝 PyYAML 套件: `pip install pyyaml`

### 配置參數說明

| 參數 | 類型 | 預設值 | 說明 |
|------|------|--------|------|
| `db_path` | str | `:memory:` | 資料庫路徑 |
| `timezone` | str | `Asia/Taipei` | 時區設定 |
| `read_only` | bool | `False` | 唯讀模式 |
| `connection_timeout` | int | `30` | 連線逾時秒數 |
| `log_level` | str | `INFO` | 日誌級別 |
| `enable_query_logging` | bool | `True` | 是否記錄 SQL 查詢 |
| `logger` | Logger | `None` | 外部日誌器 |

---

## 核心功能

### CRUD 操作

#### 建立表格

```python
import pandas as pd

df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'salary': [50000, 60000, 70000]
})

with DuckDBManager('./data.duckdb') as db:
    # 基本建立
    db.create_table_from_df('employees', df)

    # 如果存在則替換
    db.create_table_from_df('employees', df, if_exists='replace')

    # 如果存在則附加
    db.create_table_from_df('employees', df, if_exists='append')

    # 便捷方法：建立或替換
    db.create_or_replace_table('employees', df)
```

#### 插入資料

```python
new_employees = pd.DataFrame({
    'id': [4, 5],
    'name': ['David', 'Eve'],
    'salary': [55000, 65000]
})

with DuckDBManager('./data.duckdb') as db:
    # 插入到現有表格
    db.insert_df_into_table('employees', new_employees)

    # Upsert (更新或插入)
    db.upsert_df_into_table('employees', new_employees, key_columns=['id'])
```

#### 查詢資料

```python
with DuckDBManager('./data.duckdb') as db:
    # 返回 DataFrame
    result = db.query_to_df('SELECT * FROM employees WHERE salary > 55000')

    # 返回單一值
    count = db.query_single_value('SELECT COUNT(*) FROM employees')

    # 返回單一行
    row = db.query_single_row('SELECT * FROM employees WHERE id = 1')

    # 計算行數
    total = db.count_rows('employees')
    filtered = db.count_rows('employees', where='salary > 55000')
```

#### 刪除資料

```python
with DuckDBManager('./data.duckdb') as db:
    # 執行 DELETE 語句
    db.delete_data('DELETE FROM employees WHERE id = 5')
```

### 表格管理

```python
with DuckDBManager('./data.duckdb') as db:
    # 列出所有表格
    tables = db.show_tables()

    # 列出表格及詳細資訊
    tables_info = db.list_tables_with_info()
    # 返回: name, row_count, column_count

    # 檢查表格是否存在
    exists = db.table_exists('employees')

    # 描述表格結構
    schema = db.describe_table('employees')

    # 取得表格詳細資訊
    info = db.get_table_info('employees')
    print(f"表格: {info['table_name']}")
    print(f"行數: {info['row_count']}")
    print(f"欄位: {info['columns']}")

    # 取得 DDL 語句
    ddl = db.get_table_ddl('employees')
    print(ddl)
    # CREATE TABLE "employees" ("id" BIGINT, "name" VARCHAR, "salary" BIGINT)

    # 複製表格結構 (不含資料)
    db.clone_table_schema('employees', 'employees_backup')

    # 清空表格 (保留結構)
    db.truncate_table('employees')

    # 刪除表格
    db.drop_table('employees')
    db.drop_table('employees', if_exists=True)  # 不存在也不報錯

    # 備份表格
    db.backup_table('employees', backup_format='parquet')
    db.backup_table('employees', backup_format='csv', backup_path='./backup/emp.csv')
```

### 資料清理

```python
with DuckDBManager('./data.duckdb') as db:
    # 預覽欄位值
    preview = db.preview_column_values('employees', 'salary', limit=10, show_unique=True)

    # 清理數字欄位 (移除千分位符號等)
    db.clean_numeric_column(
        'employees',
        'salary',
        remove_chars=[',', '$', ' '],
        preview_only=False  # True 只預覽不執行
    )

    # 修改欄位類型
    db.alter_column_type(
        'employees',
        'salary',
        'DOUBLE',
        validate_conversion=True  # 先驗證資料是否可轉換
    )

    # 一站式清理並轉換
    db.clean_and_convert_column(
        'employees',
        'salary',
        target_type='BIGINT',
        remove_chars=[','],
        handle_empty_as_null=True
    )

    # 新增欄位
    db.add_column('employees', 'department', 'VARCHAR', default='Unknown')

    # 重新命名欄位
    db.rename_column('employees', 'salary', 'annual_salary')

    # 刪除欄位
    db.drop_column('employees', 'department')
```

### 事務處理

```python
with DuckDBManager('./data.duckdb') as db:
    # 執行多步驟事務
    operations = [
        "UPDATE employees SET salary = salary * 1.1 WHERE department = 'IT'",
        "INSERT INTO audit_log (action, timestamp) VALUES ('salary_update', NOW())",
        "DELETE FROM pending_updates WHERE processed = true"
    ]

    success = db.execute_transaction(operations)
    # 如果任一操作失敗，所有操作都會回滾

    # 驗證資料完整性
    result = db.validate_data_integrity(
        'employees',
        checks={
            'no_negative_salary': 'SELECT COUNT(*) FROM "{table_name}" WHERE salary < 0',
            'unique_ids': 'SELECT id, COUNT(*) FROM "{table_name}" GROUP BY id HAVING COUNT(*) > 1'
        }
    )
    print(f"總行數: {result['total_rows']}")
    print(f"NULL 計數: {result['null_counts']}")
    print(f"重複行數: {result['duplicate_rows']}")

    # 檢查 NULL 值
    null_counts = db.check_null_values('employees', columns=['name', 'salary'])

    # 檢查重複記錄
    duplicates = db.check_duplicates('employees', key_columns=['id'])
```

---

## Schema 遷移

Schema 遷移功能允許您在 DataFrame 結構變更時自動更新資料庫表格。

### 基本使用

```python
from duckdb_manager import DuckDBManager
from duckdb_manager.migration import SchemaMigrator, MigrationStrategy

with DuckDBManager('./data.duckdb') as db:
    migrator = SchemaMigrator(db)

    # 比對 Schema 差異
    new_df = pd.DataFrame({
        'id': [1],
        'name': ['Alice'],
        'email': ['alice@example.com'],  # 新欄位
        'age': [25]  # 新欄位
    })

    diff = migrator.compare_schema('users', new_df)

    if diff.has_changes:
        print(diff.report())
        # Schema Diff for 'users':
        #   Added columns:
        #     + email (VARCHAR)
        #     + age (BIGINT)
        #   Summary: 2 added, 0 removed, 0 type changed
        #   Status: SAFE (can migrate without data loss)
```

### 遷移策略

```python
# 安全模式 (預設) - 只執行不會造成資料遺失的操作
result = migrator.migrate('users', new_df, strategy='safe')

# 強制模式 - 執行所有變更 (可能造成資料遺失)
result = migrator.migrate('users', new_df, strategy='force')

# 先備份再遷移
result = migrator.migrate('users', new_df, strategy='backup_first', backup_format='parquet')

# 乾跑模式 - 只顯示將執行的操作
result = migrator.migrate('users', new_df, strategy='dry_run')
```

### 自動遷移

```python
# 自動判斷並執行遷移
result = migrator.auto_migrate('users', new_df, create_if_not_exists=True)

# result['action'] 可能的值:
# - 'created': 表格不存在，已建立
# - 'no_change': 無 Schema 變更
# - 'auto_migrated': 安全變更，已自動遷移
# - 'requires_review': 危險變更，需要手動確認
```

### 遷移結果

```python
result = migrator.migrate('users', new_df, strategy='safe')

print(result['success'])           # bool: 是否成功
print(result['plan'])              # MigrationPlan: 遷移計劃
print(result['backup_path'])       # str: 備份路徑 (如果有)
print(result['executed_operations']) # int: 執行的操作數
print(result['errors'])            # list: 錯誤訊息
```

---

## 日誌系統

### 使用內建日誌

```python
from duckdb_manager import DuckDBConfig

config = DuckDBConfig(
    db_path='./data.duckdb',
    log_level='DEBUG',  # DEBUG, INFO, WARNING, ERROR
    enable_query_logging=True  # 記錄所有 SQL 查詢
)
db = DuckDBManager(config)
```

### 注入外部日誌器

```python
import logging

# 使用您專案的日誌器
project_logger = logging.getLogger('my_project.database')

config = DuckDBConfig(
    db_path='./data.duckdb',
    logger=project_logger
)
db = DuckDBManager(config)
```

### 禁用日誌

```python
from duckdb_manager import DuckDBConfig
from duckdb_manager.utils import NullLogger

config = DuckDBConfig(
    db_path='./data.duckdb',
    logger=NullLogger()  # 不輸出任何日誌
)
db = DuckDBManager(config)
```

### 設定檔案日誌

```python
from duckdb_manager.utils.logging import setup_file_logger

# 建立帶檔案輸出的日誌器
file_logger = setup_file_logger(
    name='duckdb_manager',
    file_path='./logs/database.log',
    level='DEBUG',
    max_bytes=10*1024*1024,  # 10 MB
    backup_count=5
)

config = DuckDBConfig(db_path='./data.duckdb', logger=file_logger)
```

---

## SQL 安全工具

防止 SQL 注入攻擊的工具函數。

```python
from duckdb_manager.utils import SafeSQL, quote_identifier, escape_string

# 安全引用識別符 (表名、欄位名)
table = SafeSQL.quote_identifier('user table')  # '"user table"'
column = SafeSQL.quote_identifier('name"test')  # '"name""test"'

# 轉義字串值
value = SafeSQL.escape_string("O'Brien")  # "O''Brien"

# 生成安全的值
SafeSQL.quote_value("hello")  # "'hello'"
SafeSQL.quote_value(123)      # '123'
SafeSQL.quote_value(None)     # 'NULL'
SafeSQL.quote_value(True)     # 'TRUE'

# 生成 IN 子句
SafeSQL.build_in_clause('id', [1, 2, 3])
# '"id" IN (1, 2, 3)'

# 生成 WHERE 條件
SafeSQL.build_where_equals({'name': 'John', 'age': 30})
# '"name" = 'John' AND "age" = 30'

# 轉義 LIKE 模式
SafeSQL.escape_like_pattern('100%')  # '100\\%'

# 檢查識別符是否安全 (不需要引號)
SafeSQL.is_safe_identifier('users')      # True
SafeSQL.is_safe_identifier('user table') # False
```

---

## 異常處理

### 異常類層級

```
DuckDBManagerError (基類)
├── DuckDBConnectionError      # 連線失敗
├── DuckDBTableError           # 表格操作錯誤
│   ├── DuckDBTableExistsError   # 表格已存在
│   └── DuckDBTableNotFoundError # 表格不存在
├── DuckDBQueryError           # 查詢執行失敗
├── DuckDBDataValidationError  # 資料驗證失敗
├── DuckDBTransactionError     # 事務處理失敗
├── DuckDBConfigurationError   # 配置錯誤
└── DuckDBMigrationError       # Schema 遷移失敗
```

### 異常處理範例

```python
from duckdb_manager import (
    DuckDBManager,
    DuckDBConnectionError,
    DuckDBTableExistsError,
    DuckDBTableNotFoundError,
    DuckDBQueryError,
    DuckDBMigrationError,
)

try:
    with DuckDBManager('./data.duckdb') as db:
        db.create_table_from_df('users', df)

except DuckDBConnectionError as e:
    print(f"連線失敗: {e.db_path}")

except DuckDBTableExistsError as e:
    print(f"表格已存在: {e.table_name}")

except DuckDBTableNotFoundError as e:
    print(f"表格不存在: {e.table_name}")

except DuckDBQueryError as e:
    print(f"查詢失敗: {e.query[:100]}")
    print(f"原始錯誤: {e.original_error}")

except DuckDBMigrationError as e:
    print(f"遷移失敗: {e.table_name}")
```

### 向後相容

舊版異常名稱仍可使用，但會發出 DeprecationWarning：

```python
# 舊版 (已棄用，會發出警告)
from duckdb_manager import ConnectionError, TableExistsError

# 新版 (推薦)
from duckdb_manager import DuckDBConnectionError, DuckDBTableExistsError
```

---

## 擴展指南

### 新增自定義 Mixin

如果您需要擴展功能，可以建立新的 Mixin：

```python
# my_custom_mixin.py
from duckdb_manager.operations.base import OperationMixin

class MyCustomMixin(OperationMixin):
    """自定義操作 Mixin"""

    def my_custom_method(self, table_name: str) -> bool:
        """自定義方法"""
        self.logger.info(f"執行自定義操作: {table_name}")

        # 使用基類提供的方法
        if not self._table_exists(table_name):
            return False

        # 執行 SQL
        result = self.conn.sql(f'SELECT COUNT(*) FROM "{table_name}"').df()
        return True
```

### 建立自定義 Manager

```python
from duckdb_manager.manager import DuckDBManager
from duckdb_manager.operations import (
    CRUDMixin,
    TableManagementMixin,
)
from my_custom_mixin import MyCustomMixin

class MyDuckDBManager(
    CRUDMixin,
    TableManagementMixin,
    MyCustomMixin,  # 加入自定義 Mixin
):
    """擴展的 DuckDB 管理器"""

    def __init__(self, config=None):
        # 複用父類初始化邏輯
        super().__init__(config)

    def my_special_operation(self):
        """您的特殊操作"""
        pass
```

### 新增遷移策略

```python
from duckdb_manager.migration.strategies import MigrationStrategy, MigrationPlan

class CustomMigrationStrategy:
    """自定義遷移策略"""

    @classmethod
    def create_plan(cls, diff, options=None):
        # 實作您的遷移邏輯
        operations = []
        warnings = []

        # 根據 diff 生成操作
        for change in diff.added_columns:
            operations.append(f'ALTER TABLE ... ADD COLUMN ...')

        return MigrationPlan(
            strategy=MigrationStrategy.SAFE,
            diff=diff,
            operations=operations,
            warnings=warnings,
            will_execute=True
        )
```

---

## API 參考

### DuckDBManager 方法列表

#### CRUD 操作 (CRUDMixin)

| 方法 | 說明 |
|------|------|
| `create_table_from_df(table, df, if_exists)` | 從 DataFrame 建立表格 |
| `create_or_replace_table(table, df)` | 建立或替換表格 |
| `insert_df_into_table(table, df)` | 插入資料 |
| `upsert_df_into_table(table, df, keys)` | 更新或插入 |
| `query_to_df(query)` | 執行查詢返回 DataFrame |
| `query_single_value(query)` | 返回單一值 |
| `query_single_row(query)` | 返回單一行 |
| `count_rows(table, where)` | 計算行數 |
| `delete_data(query)` | 執行 DELETE |

#### 表格管理 (TableManagementMixin)

| 方法 | 說明 |
|------|------|
| `show_tables()` | 列出所有表格 |
| `list_tables_with_info()` | 列出表格及詳細資訊 |
| `table_exists(table)` | 檢查表格是否存在 |
| `describe_table(table)` | 描述表格結構 |
| `get_table_info(table)` | 取得表格詳細資訊 |
| `get_table_ddl(table)` | 取得 DDL 語句 |
| `clone_table_schema(source, target)` | 複製表格結構 |
| `truncate_table(table)` | 清空表格 |
| `drop_table(table, if_exists)` | 刪除表格 |
| `backup_table(table, format, path)` | 備份表格 |

#### 資料清理 (DataCleaningMixin)

| 方法 | 說明 |
|------|------|
| `preview_column_values(table, column)` | 預覽欄位值 |
| `clean_numeric_column(table, column, chars)` | 清理數字欄位 |
| `alter_column_type(table, column, type)` | 修改欄位類型 |
| `clean_and_convert_column(table, column, type)` | 清理並轉換 |
| `add_column(table, column, type, default)` | 新增欄位 |
| `rename_column(table, old, new)` | 重新命名欄位 |
| `drop_column(table, column)` | 刪除欄位 |

#### 事務處理 (TransactionMixin)

| 方法 | 說明 |
|------|------|
| `execute_transaction(operations)` | 執行事務 |
| `validate_data_integrity(table, checks)` | 驗證資料完整性 |
| `check_null_values(table, columns)` | 檢查 NULL 值 |
| `check_duplicates(table, keys)` | 檢查重複記錄 |

#### 屬性

| 屬性 | 說明 |
|------|------|
| `database_path` | 資料庫路徑 |
| `is_memory_db` | 是否為記憶體資料庫 |
| `is_connected` | 是否已連線 |

### SchemaMigrator 方法列表

| 方法 | 說明 |
|------|------|
| `compare_schema(table, df)` | 比對 Schema 差異 |
| `create_migration_plan(table, df, strategy)` | 建立遷移計劃 |
| `migrate(table, df, strategy, backup_format)` | 執行遷移 |
| `auto_migrate(table, df, create_if_not_exists)` | 自動遷移 |

---

## 最佳實踐

### 1. 使用 Context Manager

```python
# 推薦
with DuckDBManager('./data.duckdb') as db:
    db.query_to_df('SELECT * FROM users')
# 自動關閉連線

# 不推薦
db = DuckDBManager('./data.duckdb')
db.query_to_df('SELECT * FROM users')
db.close()  # 容易忘記
```

### 2. 使用配置物件

```python
# 推薦
config = DuckDBConfig(
    db_path='./data.duckdb',
    log_level='INFO',
    timezone='Asia/Taipei'
)
db = DuckDBManager(config)

# 可複用配置
new_config = config.copy(db_path='./other.duckdb')
```

### 3. 正確處理異常

```python
from duckdb_manager import DuckDBManager, DuckDBTableNotFoundError

with DuckDBManager('./data.duckdb') as db:
    try:
        result = db.query_to_df('SELECT * FROM maybe_not_exists')
    except DuckDBTableNotFoundError:
        result = pd.DataFrame()  # 返回空 DataFrame
```

### 4. 使用 SQL 安全工具

```python
from duckdb_manager.utils import SafeSQL

# 推薦
user_input = "O'Brien"
safe_value = SafeSQL.quote_value(user_input)
query = f"SELECT * FROM users WHERE name = {safe_value}"

# 不推薦 (SQL 注入風險)
query = f"SELECT * FROM users WHERE name = '{user_input}'"
```

### 5. Schema 遷移前先備份

```python
from duckdb_manager.migration import SchemaMigrator

with DuckDBManager('./data.duckdb') as db:
    migrator = SchemaMigrator(db)

    # 先檢查變更
    diff = migrator.compare_schema('users', new_df)
    print(diff.report())

    # 危險變更使用 backup_first
    if not diff.is_safe:
        result = migrator.migrate('users', new_df, strategy='backup_first')
    else:
        result = migrator.migrate('users', new_df, strategy='safe')
```

### 6. 批量操作使用事務

```python
with DuckDBManager('./data.duckdb') as db:
    operations = [
        "INSERT INTO users (name) VALUES ('Alice')",
        "INSERT INTO users (name) VALUES ('Bob')",
        "UPDATE user_count SET count = count + 2",
    ]

    # 全部成功或全部回滾
    db.execute_transaction(operations)
```

---

## 變更日誌

### v2.1.0 (2026-02)

**新功能**:
- 新增 YAML 配置支援: `DuckDBConfig.from_yaml(path, section)`
- 需要安裝 `pyyaml` 套件

### v2.0.0 (2026-02)

**重大變更**:
- 使用 Mixin 模式重構，manager.py 從 979 行精簡至 231 行
- 異常類加上 `DuckDB` 前綴，避免與內建異常衝突
- 新增 Schema 遷移功能

**新功能**:
- `migration` 模組: SchemaDiff, SchemaMigrator, MigrationStrategy
- `utils/query_builder.py`: SQL 安全工具
- 15+ 新便利方法

**向後相容**:
- 舊版異常名稱保留為別名 (會發出 DeprecationWarning)
- 所有現有 API 保持不變

### v1.0.0

- 初始版本
