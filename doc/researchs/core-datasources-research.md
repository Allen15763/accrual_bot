# Core DataSources 模組深度研究文件

> **適用版本**：Accrual Bot（截至 2026-03）
> **模組路徑**：`accrual_bot/core/datasources/`
> **文件目的**：從軟體工程最佳實踐角度，系統性地分析本模組的設計思路、知識點、應用方式與改進空間。

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

Accrual Bot 是一套用於 PO/PR 應計帳款處理的非同步資料管線系統，涵蓋 SPT、SPX 與 SCT 三個業務實體。在資料管線的各個步驟（Loading → Filtering → Integration → Export）中，系統需要從多種異質資料來源讀取資料，例如：

- 業務部門提供的 **Excel 檔案**（`.xlsx`）
- 系統導出的 **CSV 報表**（`.csv`）
- 管線 Checkpoint 儲存的 **Parquet 序列化檔案**（`.parquet`）
- 用於快速記憶體計算的 **DuckDB 資料庫**（`:memory:` 或 `.duckdb`）
- 透過 **Google Sheets** 管理的共用配置或數據

在模組重構前，各個步驟直接呼叫 `pandas.read_excel()`、`pandas.read_csv()` 等函數，導致：

- 重複的錯誤處理與日誌程式碼分散在各步驟中
- 無法共享快取，同一份檔案在管線中可能被重複讀取
- 缺乏統一的連接管理，資源（檔案控制代碼、執行緒）難以追蹤

`core/datasources/` 模組即為解決上述問題而設計的**統一資料存取抽象層（Unified Data Access Layer）**。

---

## 2. 用途

本模組的主要功能分為三個層次：

### 2.1 統一存取介面

無論底層資料格式為何，所有消費方程式碼均使用相同介面：

```python
df = await source.read()          # 讀取為 DataFrame
await source.write(df)            # 寫入
metadata = source.get_metadata()  # 取得元數據
```

### 2.2 透明快取

基類自動提供 TTL（5 分鐘）+ LRU（最多 10 項）快取，消費方無需自行管理：

```python
df = await source.read_with_cache()  # 自動命中或更新快取
```

### 2.3 非同步執行

所有 I/O 操作封裝為 `async/await` 形式，與管線的非同步架構整合：

```python
# 並發讀取多個資料來源
dfs = await asyncio.gather(source_a.read(), source_b.read(), source_c.read())
```

---

## 3. 設計思路

本模組綜合運用了多種軟體工程設計模式，各自解決不同層面的問題。

### 3.1 模組架構總覽

```
datasources/
├── __init__.py           # 公開 API，優雅處理可選依賴
├── base.py               # 抽象基類 + 快取邏輯（核心）
├── config.py             # 配置與型別定義（純資料）
├── factory.py            # 工廠 + 連接池（物件創建）
├── excel_source.py       # Excel 具體實作
├── csv_source.py         # CSV 具體實作
├── parquet_source.py     # Parquet 具體實作
├── duckdb_source.py      # DuckDB 具體實作（最複雜）
└── google_sheet_source.py # Google Sheets 具體實作（可選依賴）
```

### 3.2 核心設計模式

#### 模式 1：抽象工廠（Abstract Factory）

`DataSourceFactory` 將「建立哪種數據源」的決策集中管理，消費方只需提供 `DataSourceConfig`，不需要知道底層類別：

```
DataSourceFactory.create(config)
    ↓ 查 _sources registry
    → ExcelSource / CSVSource / ParquetSource / DuckDBSource / GoogleSheetsSource
```

**優點**：新增資料來源類型時，只需實作 `DataSource` 介面並在 `_sources` 中登記，其餘程式碼無需修改（開放封閉原則）。

#### 模式 2：模板方法（Template Method）

`DataSource` 基類定義演算法骨架（快取邏輯），子類只需實作 `read()` 抽象方法：

```
read_with_cache()  ← 基類定義（快取查找 → 過期判斷 → LRU 驅逐）
       ↓ 快取未命中時呼叫
    read()         ← 子類實作（真正讀取邏輯）
```

這確保所有資料來源的快取行為完全一致，子類不需要重複實作。

#### 模式 3：策略模式（Strategy）

`DuckDBSource` 在初始化時根據 `db_path` 是否為 `:memory:` 決定連接策略：

| 情境 | 策略 | 原因 |
|------|------|------|
| `db_path == ':memory:'` | 持久連接（執行緒本地） | 記憶體 DB 斷線即資料消失 |
| `db_path` 為檔案路徑 | 每次操作臨時連接 | 避免並發衝突，確保執行緒安全 |

#### 模式 4：物件池（Object Pool）

`DataSourcePool` 提供多數據源的統一管理容器，支援廣播式操作：

```python
pool = DataSourcePool()
pool.add_source('excel', excel_source)
pool.add_source('csv', csv_source)
results = await pool.execute_on_all('read')  # 並發讀取所有來源
```

---

## 4. 各項知識點

### 4.1 `DataSourceType` 枚舉（`base.py`、`config.py`）

```python
class DataSourceType(Enum):
    EXCEL = "excel"
    CSV = "csv"
    PARQUET = "parquet"
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    GOOGLE_SHEETS = "google_sheets"
    IN_MEMORY = "in_memory"
```

**注意**：`DataSourceType` 在 `base.py` 與 `config.py` 中**各定義了一次**，這是為了讓 `config.py` 可獨立導入（不依賴 `base.py`），但也造成了輕微的重複定義。實際使用時兩者等價。

---

### 4.2 `DataSourceConfig` 資料類（`config.py`）

```python
@dataclass
class DataSourceConfig:
    source_type: DataSourceType
    connection_params: Dict[str, Any]    # 依類型不同：file_path / db_path / sheet_id
    cache_enabled: bool = True
    cache_ttl_seconds: int = 300         # 5 分鐘
    cache_max_items: int = 10
    cache_eviction_policy: str = "lru"
    encoding: str = 'utf-8'
    chunk_size: Optional[int] = None
    lazy_load: bool = False
```

**設計重點**：使用 Python `dataclass` 而非一般 `class`，自動產生 `__init__`、`__repr__`、`__eq__`，配置物件可直接比較與打印。

各資料來源類型的 `connection_params` 必要鍵值一覽：

| 類型 | 必要鍵值 |
|------|---------|
| EXCEL | `file_path` |
| CSV | `file_path` |
| PARQUET | `file_path` |
| DUCKDB | `db_path` |
| POSTGRES | `host`, `port`, `database`, `user`, `password` |
| GOOGLE_SHEETS | `sheet_id`, `credentials` |
| IN_MEMORY | `dataframe` |

`validate()` 方法在 `DataSourceFactory.create()` 時自動呼叫，確保必要參數存在且檔案路徑有效。

---

### 4.3 TTL + LRU 快取（`base.py`，第 84–136 行）

快取機制由基類統一提供，不依賴外部函式庫（如 `functools.lru_cache`），採用手動管理的字典：

```python
self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
# 鍵值 = MD5(query + kwargs)
# 值 = (DataFrame 副本, 寫入時間)
```

**快取鍵值生成**（`_generate_cache_key`）：

```python
# 1. 排序 kwargs，確保鍵值順序無關
filtered_kwargs = {k: v for k, v in sorted(kwargs.items())
                   if k not in ('logger', 'log_level')}
# 2. JSON 序列化後取 MD5
key_json = json.dumps({'query': query, 'kwargs': filtered_kwargs},
                      sort_keys=True, default=str)
return hashlib.md5(key_json.encode('utf-8')).hexdigest()
```

**LRU 驅逐邏輯**：當快取超過 `cache_max_items` 時，找出 `timestamp` 最舊的條目刪除：

```python
if len(self._cache) > self._cache_max_size:
    oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
    del self._cache[oldest_key]
```

> ⚠️ 實作說明：此 LRU 為「近似 LRU」（驅逐寫入時間最舊者，非存取時間最舊者），嚴格的 LRU 應在每次存取時更新時間戳。當前實作對本系統已足夠，但在高頻讀取熱點資料的場景下效率略低。

---

### 4.4 執行緒池與 `run_in_executor`（`excel_source.py`、`csv_source.py`、`parquet_source.py`）

`pandas.read_excel()`、`pd.read_csv()` 等均為同步阻塞操作，不能直接在 `async` 函數中呼叫（會阻塞事件迴圈）。解法是使用執行緒池：

```python
# 類級別的執行緒池（所有實例共用，節省資源）
_executor = ThreadPoolExecutor(max_workers=4)

async def read(self, ...):
    def read_sync():           # 同步函數，包裝實際 I/O
        return pd.read_excel(...)

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(self._executor, read_sync)
```

**類級別 vs 實例級別執行緒池**：

| 方式 | 記憶體使用 | 適用場景 |
|------|-----------|---------|
| 類級別（本模組採用） | 低（共用） | 同類數據源並發數適中 |
| 實例級別 | 高（每實例獨立） | 需要獨立控制並發度 |

`cleanup_executor()` 類方法由 `DataSourceFactory._cleanup_all_executors()` 在程式退出時呼叫（透過 `atexit` 鉤子）。

---

### 4.5 DuckDB 雙連接策略（`duckdb_source.py`，第 67–135 行）

這是本模組中最複雜的設計決策，根源在於 DuckDB 的特性：

- **記憶體 DB**：連接關閉即資料消失，因此必須保持持久連接。但多執行緒共用同一個 `:memory:` 連接會有競爭問題，因此使用**執行緒本地儲存（`threading.local()`）**，每個執行緒持有自己的記憶體 DB 連接。
- **檔案 DB**：DuckDB 僅允許一個寫入連接，多個讀取連接。為避免連接洩漏與鎖定問題，每次操作開始時建立連接，結束時立即關閉。

```python
@contextmanager
def _connection(self):
    if self.is_memory_db:
        conn = self._get_memory_connection()  # 持久連接，不關閉
        try:
            yield conn
        except Exception as e:
            raise
        # ← 不呼叫 conn.close()
    else:
        conn = duckdb.connect(self.db_path, ...)  # 臨時連接
        try:
            yield conn
        finally:
            conn.close()  # ← 確保關閉
```

---

### 4.6 Transaction 支援（`duckdb_source.py`，第 136–171 行）

Phase 2 新增的原子操作支援，確保多步驟 SQL 的一致性：

```python
@contextmanager
def _transaction(self):
    with self._connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            yield conn
            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise
```

典型應用場景：先刪除舊資料再插入新資料，避免中間狀態被其他執行緒讀取。

---

### 4.7 指數退避重試（`duckdb_source.py`，第 173–199 行）

統一的重試裝飾器，避免 DuckDB 因短暫鎖定失敗：

```python
def _with_retry(self, operation: Callable, max_retries: int = 3) -> Any:
    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)  # 0.1s, 0.2s, 0.3s
                time.sleep(wait_time)
            else:
                raise
```

注意此為線性退避（`0.1 * attempt`），非真正的指數退避（`0.1 * 2^attempt`），對短時間重試足夠。

---

### 4.8 Parquet 列式讀取優化（`parquet_source.py`）

Parquet 格式天然支援**列式儲存**，本模組充分利用 PyArrow API 進行效能優化：

```python
# 只讀取需要的欄位（跳過不需要的列，I/O 大幅減少）
parquet_file.read(columns=['col1', 'col2'])

# 按 row group 讀取（適合大型檔案的分批處理）
for row_group_idx in row_group_indices:
    table = parquet_file.read_row_group(row_group_idx, columns=columns)
```

這是 Parquet 作為 Checkpoint 格式的核心優勢：恢復特定步驟時只需讀取需要的欄位，而非載入整個 DataFrame。

---

### 4.9 Google Sheets ZIP Fallback（`google_sheet_source.py`）

在無網路環境（如 Colab 離線、打包部署）或憑證檔無法直接存取時，系統嘗試從預設 ZIP 路徑解壓縮服務帳戶 JSON：

```python
zip_paths = [
    Path('/content/credentials.zip'),    # Colab 掛載路徑
    Path('./credentials.zip'),            # 本地相對路徑
    Path(home_dir / 'credentials.zip'),  # 家目錄
]
for zip_path in zip_paths:
    if zip_path.exists():
        # 解壓縮並載入憑證
        ...
```

這是針對特定部署環境的防禦性設計，確保在受限環境下也能取得外部資料。

---

### 4.10 可選依賴的優雅處理（`__init__.py`、`factory.py`）

`google-auth`、`gspread` 為選用依賴，安裝失敗不應讓整個模組崩潰：

```python
# __init__.py
try:
    from .google_sheet_source import GoogleSheetsSource, GoogleSheetsManager
except ImportError:
    GoogleSheetsSource = None  # type: ignore
    GoogleSheetsManager = None

# factory.py
try:
    from .google_sheet_source import GoogleSheetsSource
    _GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GoogleSheetsSource = None
    _GOOGLE_SHEETS_AVAILABLE = False

# 只在可用時加入 registry
_sources = {
    ...,
    **({DataSourceType.GOOGLE_SHEETS: GoogleSheetsSource} if _GOOGLE_SHEETS_AVAILABLE else {}),
}
```

`**{}` 展開空字典是 Python 的慣用技巧，條件式地加入字典條目。

---

## 5. 應用範例

### 5.1 基本讀取（最常見用法）

```python
from accrual_bot.core.datasources import DataSourceFactory, DataSourceConfig, DataSourceType

# 建立配置
config = DataSourceConfig(
    source_type=DataSourceType.EXCEL,
    connection_params={
        'file_path': 'resources/202503/Original Data/202503_PO.xlsx',
        'sheet_name': 0,
        'header': 0,
        'dtype': 'str'
    }
)

# 透過工廠建立（含配置驗證）
source = DataSourceFactory.create(config)

# 非同步讀取
df = await source.read()
```

### 5.2 自動型別偵測

```python
# 根據副檔名自動決定類型
source = DataSourceFactory.create_from_file('data/report.csv', encoding='utf-8-sig')
df = await source.read()
```

### 5.3 帶快取的讀取（避免重複 I/O）

```python
# 第一次：實際讀取檔案，結果存入快取
df1 = await source.read_with_cache()

# 5 分鐘內再次呼叫：直接從快取返回（無 I/O）
df2 = await source.read_with_cache()

# 手動清除快取（資料更新後）
source.clear_cache()
```

### 5.4 非同步上下文管理器（確保資源釋放）

```python
async with DataSourceFactory.create(config) as source:
    df = await source.read()
    await source.write(processed_df)
# 離開 with 塊自動呼叫 source.close()
```

### 5.5 DuckDB 記憶體資料庫（快速 SQL 計算）

```python
from accrual_bot.core.datasources import DuckDBSource

# 建立記憶體 DB
db = DuckDBSource.create_memory_db()

# 寫入 DataFrame
await db.write(df, table_name='po_data')

# 執行 SQL 聚合
result = await db.read(query="SELECT vendor_code, SUM(amount) FROM po_data GROUP BY vendor_code")

# 原子操作（避免中間狀態）
await db.write_atomic(new_df, table_name='po_data', mode='replace')
```

### 5.6 多數據源並發讀取（DataSourcePool）

```python
from accrual_bot.core.datasources import DataSourcePool, DataSourceFactory

pool = DataSourcePool()
pool.add_source('po', DataSourceFactory.create_from_file('po.xlsx'))
pool.add_source('pr', DataSourceFactory.create_from_file('pr.csv'))
pool.add_source('invoice', DataSourceFactory.create_from_file('invoice.xlsx'))

# 並發讀取所有來源
results = await pool.execute_on_all('read')
po_df = results['po']
pr_df = results['pr']

# 結束後關閉所有連接
await pool.close_all()
```

### 5.7 Parquet Checkpoint 讀寫（管線斷點恢復）

```python
from accrual_bot.core.datasources import ParquetSource, DataSourceConfig, DataSourceType

# 儲存 checkpoint
config = DataSourceConfig(
    source_type=DataSourceType.PARQUET,
    connection_params={
        'file_path': 'checkpoints/step_5_integration.parquet',
        'compression': 'snappy'
    }
)
source = ParquetSource(config)
await source.write(context.data)

# 恢復時只讀取需要的欄位（節省記憶體）
df = await source.read(columns=['po_number', 'vendor_code', 'amount', 'accrual_flag'])
```

### 5.8 自訂數據源（擴充）

```python
from accrual_bot.core.datasources import DataSource, DataSourceConfig, DataSourceType

class S3Source(DataSource):
    """Amazon S3 數據源（自訂擴充）"""

    async def read(self, query=None, **kwargs) -> pd.DataFrame:
        import boto3
        # ... S3 讀取邏輯 ...

    async def write(self, data, **kwargs) -> bool:
        # ... S3 寫入邏輯 ...

    def get_metadata(self):
        return {'bucket': self.config.connection_params['bucket']}

# 註冊到工廠
DataSourceFactory.register_source(DataSourceType.IN_MEMORY, S3Source)
```

---

## 6. 優缺分析

### 6.1 優點

#### 統一介面，降低認知負擔
消費方程式碼（pipeline steps）無需了解底層格式細節，`await source.read()` 即可。新開發者只需學習一套 API。

#### 快取透明化
TTL+LRU 快取直接整合在基類，各步驟自動受益，無需各自管理。在管線中多個步驟讀取同一份檔案時（如 `previous_workpaper.xlsx`），只有第一次讀取觸發 I/O。

#### 非同步優先，資源隔離
所有 I/O 封裝為 `async/await`，配合類級別執行緒池，避免阻塞事件迴圈，且同類資料來源共用執行緒池而非各自佔用。

#### DuckDB 連接生命週期管理精確
記憶體 DB 與檔案 DB 各有適合的連接策略，避免記憶體資料意外丟失（斷線即消失的陷阱）或檔案鎖定問題。

#### 擴充性強
透過 `DataSourceFactory.register_source()` 可在不修改核心程式碼的情況下新增資料來源類型（符合開放封閉原則）。

#### 優雅降級
`gspread` 等選用依賴不存在時，工廠自動排除 `GOOGLE_SHEETS` 類型，其他功能完全不受影響。

---

### 6.2 缺點與已知限制

#### LRU 快取為近似實作
快取驅逐基於**寫入時間**而非**最後存取時間**，理論上不是標準 LRU。對大多數場景影響不大，但高頻存取某幾個熱點資料時，非熱點資料可能被提前保留。

```python
# 目前：驅逐寫入最早的條目
oldest_key = min(self._cache, key=lambda k: self._cache[k][1])

# 標準 LRU 應：驅逐最後存取時間最早的條目（需在每次 read_with_cache 時更新時間戳）
```

#### 記憶體 DB 執行緒隔離導致資料隔離
每個執行緒持有獨立的 `:memory:` 連接，在多執行緒場景中無法共享記憶體資料庫狀態。若需要跨執行緒共享，應改用檔案 DB 或專用的共享記憶體機制。

#### `DataSourcePool.__del__` 中的 asyncio 反模式

```python
def __del__(self):
    loop = asyncio.new_event_loop()  # 在析構函數中建立新事件迴圈
    loop.run_until_complete(self.close_all())
    loop.close()
```

在析構函數（`__del__`）中呼叫 `asyncio.new_event_loop()` 是已知反模式：析構時機由 GC 決定，若 GC 在程式退出過程中執行，可能觸發 `RuntimeError: Event loop is closed`。建議改用 `atexit` 或明確呼叫 `await pool.close_all()`。

#### `remove_source` 中的 `asyncio.run()` 問題

```python
def remove_source(self, name):
    asyncio.run(source.close())  # 若已在事件迴圈中呼叫，會拋出 RuntimeError
```

`asyncio.run()` 不能在已有事件迴圈的環境（如 `async` 函數中）呼叫。在管線 step 的非同步上下文中使用 `pool.remove_source()` 可能失敗。

#### `DataSourceType` 重複定義
`base.py` 與 `config.py` 各自定義了 `DataSourceType`，雖然值相同，但嚴格來說是不同的型別物件。在使用 `isinstance` 或型別比較時需注意導入來源一致性。

#### Google Sheets 讀取為循序而非真正並發
`get_multiple_sheets_data()` 看似批量讀取，但實際為循序執行；只有 `concurrent_get_data()` 才使用 `ThreadPoolExecutor` 並發，兩者命名與行為差異不夠直覺。

---

## 7. 延伸議題

### 7.1 連接池真正實作

目前 `ConnectionPool` dataclass（`config.py`，第 162–171 行）僅定義了連接池**配置參數**（min_size、max_size 等），並無實際的連接池管理邏輯。對於 DuckDB 或 PostgreSQL 等支援多連接的資料庫，實作真正的連接池可顯著提升並發效能：

```python
# 概念性實作方向
class RealConnectionPool:
    def __init__(self, config: ConnectionPool):
        self._pool: asyncio.Queue[Connection] = asyncio.Queue(maxsize=config.max_size)
        # 預先建立 min_size 個連接 ...

    async def acquire(self) -> Connection:
        return await asyncio.wait_for(self._pool.get(), timeout=self.timeout)

    async def release(self, conn: Connection):
        await self._pool.put(conn)
```

### 7.2 觀察者模式整合（快取失效通知）

目前快取只能透過 TTL 或手動呼叫 `clear_cache()` 失效。在未來若需要跨數據源的快取一致性（例如寫入操作後自動使相關讀取快取失效），可引入觀察者模式：

```python
# 寫入後自動廣播快取失效事件
async def write(self, data, **kwargs):
    result = await super().write(data, **kwargs)
    self.clear_cache()           # 自身快取清除
    self.notify_observers()      # 通知依賴此數據源的觀察者
    return result
```

### 7.3 Streaming / 分批讀取 API 統一化

目前只有 `CSVSource` 提供 `read_in_chunks()` 方法，而 `ParquetSource` 的 `read_row_groups()` API 形式不同。未來可在基類統一串流介面：

```python
# 統一的分批讀取介面
async def read_stream(self, batch_size: int = 10000):
    """AsyncGenerator，逐批 yield DataFrame"""
    yield batch_df  # 由各子類實作
```

這對處理數百萬行資料的 CSV/Parquet 場景特別有價值，避免一次性載入耗盡記憶體。

### 7.4 Schema 驗證整合

目前讀取資料後無自動 schema 驗證。可在基類加入可選的 schema 驗證鉤子，與 `DataSourceConfig` 整合：

```python
config = DataSourceConfig(
    source_type=DataSourceType.EXCEL,
    connection_params={'file_path': 'po.xlsx'},
    expected_schema={
        'po_number': 'str',
        'amount': 'float64',
        'vendor_code': 'str'
    }
)
# 讀取後自動驗證欄位存在性與型別
```

### 7.5 指標收集（Metrics）

在生產環境中，了解各資料來源的讀取延遲、快取命中率、錯誤率等指標至關重要。可在基類的 `read_with_cache()` 中加入輕量指標收集：

```python
async def read_with_cache(self, ...):
    start = time.monotonic()
    cache_hit = cache_key in self._cache and not expired
    result = ...
    elapsed = time.monotonic() - start

    self._metrics.record(
        source_type=self.config.source_type.value,
        operation='read',
        duration_ms=elapsed * 1000,
        cache_hit=cache_hit
    )
    return result
```

### 7.6 與 Pydantic 整合

`DataSourceConfig` 目前使用 Python `dataclass`，驗證邏輯在 `validate()` 方法中手動實作。改用 Pydantic v2 可獲得：

- 自動型別強制轉換（`str` → `DataSourceType`）
- 更豐富的欄位驗證（正規表達式、範圍限制等）
- JSON Schema 自動產生（利於文件化與 API 整合）

```python
from pydantic import BaseModel, validator

class DataSourceConfig(BaseModel):
    source_type: DataSourceType
    connection_params: Dict[str, Any]
    cache_ttl_seconds: int = Field(default=300, ge=0, le=86400)

    @validator('connection_params')
    def validate_params(cls, v, values):
        # 自動型別驗證
        ...
```

---

## 8. 其他

### 8.1 模組依賴關係

```
config.py          （無外部依賴，純資料結構）
    ↑
base.py            （依賴 config.py、utils.logging）
    ↑
excel_source.py    （依賴 base.py、pandas、openpyxl）
csv_source.py      （依賴 base.py、pandas）
parquet_source.py  （依賴 base.py、pandas、pyarrow）
duckdb_source.py   （依賴 base.py、duckdb）
google_sheet_source.py  （依賴 base.py、gspread [選用]）
    ↑
factory.py         （依賴所有上述 source 類）
    ↑
__init__.py        （公開 API，處理選用依賴）
```

### 8.2 執行緒安全性一覽

| 元件 | 執行緒安全 | 說明 |
|------|-----------|------|
| `DataSource._cache` | ❌ 不保證 | 字典操作在 CPython 中部分原子，但複合操作（讀取+寫入）不安全 |
| `ExcelSource._executor` | ✅ | 類級別 ThreadPoolExecutor 本身執行緒安全 |
| `DuckDBSource`（記憶體 DB） | ✅ | `threading.local()` 確保每執行緒獨立連接 |
| `DuckDBSource`（檔案 DB） | ✅ | 每次操作新建連接，無共享狀態 |
| `DataSourceFactory._sources` | ✅（讀取） | 初始化後只讀，無競爭 |
| `ConfigManager` | ✅ | 雙重檢查鎖定保證單例安全 |

### 8.3 測試覆蓋率

根據專案測試文件，本模組的覆蓋率如下：

| 模組 | 覆蓋率 | 測試檔案 |
|------|-------|---------|
| `config.py` | 100% | `tests/unit/core/datasources/test_datasource_config.py` |
| `factory.py` | — | `tests/unit/core/datasources/test_datasource_factory.py` |
| `csv_source.py` | 77% | `tests/unit/core/datasources/test_csv_source.py` |
| `excel_source.py` | 80% | `tests/unit/core/datasources/test_excel_source.py` |
| `parquet_source.py` | 82% | `tests/unit/core/datasources/test_parquet_source.py` |
| `duckdb_source.py` | 15% | — （複雜度高，測試覆蓋尚不足） |

### 8.4 版本演進記錄

| 階段 | 內容 |
|------|------|
| Phase 1 | 基礎 DataSource ABC、ExcelSource、CSVSource、ParquetSource |
| Phase 2 | DuckDBSource 強化：Transaction 支援、記憶體/檔案雙策略連接管理、統一重試邏輯 |
| Phase 3 | GoogleSheetsSource：合併 `GoogleSheetsImporter`（accrual_bot）與 `GoogleSheetsManager`（spe_bank_recon），統一 DataSource 介面 |

### 8.5 快速查閱：從用途找 API

| 需要做什麼 | 使用方式 |
|-----------|---------|
| 讀取 Excel 檔案 | `ExcelSource.create_from_file(path)` + `.read()` |
| 自動偵測檔案類型 | `DataSourceFactory.create_from_file(path)` |
| 帶快取讀取 | `.read_with_cache()` |
| 強制重讀（清除快取） | `.clear_cache()` 後 `.read()` |
| 並發讀取多個來源 | `DataSourcePool` + `.execute_on_all('read')` |
| DuckDB SQL 計算 | `DuckDBSource.create_memory_db()` + `.write(df)` + `.read(query=sql)` |
| 原子寫入（避免中間狀態） | `.write_atomic(df, mode='replace')` |
| Parquet Checkpoint | `ParquetSource` + `.write(df)` / `.read(columns=[...])` |
| 非同步安全關閉 | `async with source:` 上下文管理器 |

---

*文件最後更新：2026-03-12*
*文件撰寫：依據 `accrual_bot/core/datasources/` 原始碼逐行分析*
