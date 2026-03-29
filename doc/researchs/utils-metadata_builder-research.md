# utils/metadata_builder 深度研究文件

> **⚠️ 注意：此模組已於 2026-03-27 提取為獨立 GitHub 套件**
> - **套件名稱**: `seafin-metadata-builder` v1.0.0
> - **GitHub**: https://github.com/Allen15763/seafin-metadata-builder
> - **安裝**: `pip install "seafin-metadata-builder @ git+https://github.com/Allen15763/seafin-metadata-builder.git@v1.0.0"`
> - **Import**: `from seafin_metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec, SourceSpec`
> - 本文件為歷史研究文件，保留作為架構與設計參考。文中路徑 `accrual_bot/utils/metadata_builder/` 已不存在於主專案中。

> 原始研究範圍：`accrual_bot/utils/metadata_builder/`（13 個 Python 檔案，含 README.md）
> 研究時間：2026-03-12
> 研究目的：軟體工程最佳實踐角度的深度分析

---

## 目錄

1. [背景](#一背景)
2. [用途](#二用途)
3. [設計思路](#三設計思路)
4. [各項知識點](#四各項知識點)
5. [應用範例](#五應用範例)
6. [優缺分析](#六優缺分析)
7. [延伸議題](#七延伸議題)
8. [其他](#八其他)

---

## 一、背景

### 1.1 模組起源

`utils/metadata_builder` 是一個**可移植的獨立插件（Portable Plugin）**，設計為「髒資料處理工具類」。根據 README.md 和程式碼中的 `__version__ = "1.0.0"` 標記，這是一個明確版本化的獨立模組。

從程式碼內部的 `from src.utils.metadata_builder import...` 路徑（舊路徑，非 `accrual_bot` 的正確路徑）可以推斷：此模組**原生於 `spe_bank_recon` 子專案**，後來被複製至 `accrual_bot` 中。目前兩個專案中存在幾乎完全相同的程式碼：

```
accrual_bot/utils/metadata_builder/      ← 本研究對象
spe_bank_recon/src/utils/metadata_builder/  ← 原始版本，實際使用中
```

### 1.2 在 accrual_bot 中的地位

**重要發現**：在 `accrual_bot` 的生產程式碼中，`MetadataBuilder`（髒資料處理器）**實際上並未被任何生產模組 import 使用**。grep 結果顯示：

| 位置 | 引用類型 |
|------|---------|
| `utils/__init__.py` | 僅重新匯出（re-export） |
| `core/pipeline/steps/__init__.py` | 僅重新匯出 |
| `tasks/spt/steps/*.py`（8 個檔案） | 引用的是 `StepMetadataBuilder`（不同類） |
| `tasks/spx/steps/*.py`（7 個檔案） | 引用的是 `StepMetadataBuilder`（不同類） |

> **命名陷阱**：`StepMetadataBuilder`（在 `core/pipeline/steps/common.py:1160`）是步驟執行統計構建器，與本模組的 `MetadataBuilder`（髒資料處理器）是**完全不同的類別**，僅名稱相近。

### 1.3 模組結構概覽

```
metadata_builder/                  (13 個 Python 檔案)
├── __init__.py                    # 插件入口，顯式 __all__，版本號
├── config.py                      # 3 個資料類（SourceSpec, ColumnSpec, SchemaConfig）
├── exceptions.py                  # 6 個自定義異常類
├── reader.py                      # SourceReader：多格式容錯讀取
├── builder.py                     # MetadataBuilder：Facade 主類
├── processors/
│   ├── __init__.py
│   ├── bronze.py                  # BronzeProcessor：原樣落地
│   └── silver.py                  # SilverProcessor：清洗轉換
├── transformers/
│   ├── __init__.py
│   ├── column_mapper.py           # ColumnMapper：精確/Regex 欄位映射
│   └── type_caster.py             # SafeTypeCaster：安全類型轉換
└── validation/
    ├── __init__.py
    └── circuit_breaker.py         # CircuitBreaker + CircuitBreakerResult
```

---

## 二、用途

### 2.1 解決的核心問題

財務類 Excel 來源資料具有高度不可控性：
- 欄位名稱含前後空白、特殊字元、大小寫不一致
- 日期格式不統一（`2025/12/01`、`20251201`、`2025-12-01`）
- 數字欄位混雜千分位逗號、貨幣符號（`$1,234`、`NT$500`）
- 偶爾有整欄為空的情況（表示該期無資料）

`MetadataBuilder` 提供一套**配置驅動的標準化流程**，將「不可控來源 → 可用 DataFrame」的轉換系統化。

### 2.2 公開 API 一覽

#### MetadataBuilder（主 Facade）

| 方法 | 層次 | 說明 |
|------|------|------|
| `extract(file_path, ...)` | Bronze | 讀取源檔案，全字串，可加 metadata 欄位 |
| `transform(df, schema_config)` | Silver | 欄位映射、類型轉換、Circuit Breaker |
| `build(file_path, schema_config)` | Bronze+Silver | 一次完成兩層 |
| `extract_and_preview(file_path, n_rows)` | Bronze | 預覽模式，不加 metadata |
| `get_excel_sheets(file_path)` | 工具 | 取得 Excel Sheet 列表 |

#### 配置類 API

| 類別 | 工廠方法 | 說明 |
|------|---------|------|
| `SourceSpec` | `from_dict()`, `to_dict()` | 源檔案讀取參數 |
| `ColumnSpec` | `from_dict()`, `is_regex` 屬性 | 單一欄位映射定義 |
| `SchemaConfig` | `from_dict()`, `from_yaml()`, `from_toml()`, `to_dict()` | 完整 Schema 定義 |

#### 子元件 API（可獨立使用）

| 子元件 | 關鍵方法 |
|--------|---------|
| `SourceReader` | `read()`, `read_excel()`, `read_csv()`, `read_parquet()`, `read_json()` |
| `BronzeProcessor` | `process()`, `get_metadata_columns()` |
| `SilverProcessor` | `process()`, `validate_only()` |
| `ColumnMapper` | `map_columns()`, `find_matching_column()`, `apply_defaults()` |
| `SafeTypeCaster` | `cast_columns()`, `get_cast_summary()` |
| `CircuitBreaker` | `check()`, `check_and_raise()`, `get_null_summary()` |

### 2.3 典型使用場景（spe_bank_recon 中的實際用法）

```python
# 最完整的使用方式（出自 README.md 完整範例）
from accrual_bot.utils.metadata_builder import MetadataBuilder, SchemaConfig

builder = MetadataBuilder(logger=logger)

# Step 1: Bronze - 原樣讀取
df_raw = builder.extract(
    './input/bank.xlsx',
    sheet_name=1,
    add_metadata=True
)

# Step 2: 從 YAML 載入 Schema（放寬空值容忍度）
schema = SchemaConfig.from_yaml('./schema_config.yaml', section='banks.cub')
schema.circuit_breaker_threshold = 0.5  # 空值較多時放寬

# Step 3: Silver - 清洗轉換
df_clean = builder.transform(df_raw, schema)

# Step 4: 存入 DuckDB（由呼叫者控制）
with DuckDBManager('./db/data.duckdb') as db:
    db.create_table_from_df('bronze_bank', df_raw, if_exists='replace')
    db.create_table_from_df('silver_bank', df_clean, if_exists='replace')
```

---

## 三、設計思路

### 3.1 Medallion 架構（資料湖分層理念）

本模組借用**資料工程（Data Engineering）**中的 Medallion Architecture（獎章架構）概念：

```
原始資料（Excel/CSV）
    ↓
  Bronze Layer（青銅層）：原樣落地
  - 不做任何業務轉換
  - 全部讀為 string（最大相容性）
  - 添加追溯 metadata（來源檔案、批次 ID、載入時間）
  - 標準化欄位名稱（去空白、特殊字元）
    ↓
  Silver Layer（白銀層）：清洗轉換
  - 欄位映射（精確/Regex）
  - 安全類型轉換（失敗 → NULL，不中斷）
  - 過濾無效空行
  - Circuit Breaker 驗證（NULL 比例檢查）
    ↓
  可用 DataFrame（由呼叫者決定下游行為）
```

這種分層的核心哲學是**「先保存，再清洗」**：Bronze 層確保原始資料不遺失，Silver 層才做業務層面的轉換。

### 3.2 Facade 模式（外觀設計模式）

`MetadataBuilder` 作為 **Facade（外觀）**，封裝了 5 個子元件的協作細節：

```
使用者 → MetadataBuilder（Facade）
                ├── SourceReader
                ├── BronzeProcessor
                └── SilverProcessor
                        ├── ColumnMapper
                        ├── SafeTypeCaster
                        └── CircuitBreaker（延遲建立）
```

**設計決策**：子元件可以通過建構子注入（DI）替換，使得各元件可獨立測試、替換：

```python
processor = SilverProcessor(
    column_mapper=custom_mapper,  # 可替換
    type_caster=custom_caster,    # 可替換
    circuit_breaker=custom_breaker  # 可替換，None 時延遲建立
)
```

### 3.3 配置驅動（Configuration-Driven Design）

Schema 定義完全外化為配置，支援三種來源：

| 配置來源 | 方法 | 適用場景 |
|---------|------|---------|
| Python 字典 | `SchemaConfig.from_dict(data)` | 程式碼內嵌、動態生成 |
| YAML 檔案 | `SchemaConfig.from_yaml(path, section)` | 人類可讀，支援嵌套路徑 |
| TOML 檔案 | `SchemaConfig.from_toml(path, section)` | 與專案現有配置系統整合 |

YAML/TOML 皆支援**點路徑（dot notation）** section 導航：
```python
SchemaConfig.from_yaml('schema.yaml', section='banks.cub')
# 等同於 yaml_data['banks']['cub']
```

### 3.4 防禦性設計（Defensive Design）

三層防禦機制防止資料品質問題進入下游：

1. **Bronze 層**：欄位名稱標準化、全字串讀取（避免 pandas 類型推斷產生 NaN）
2. **Silver 層**：`errors="coerce"`（轉換失敗 → NULL，不中斷流程）
3. **Circuit Breaker**：NULL 比例超過閾值時主動終止（防止品質過差的資料靜默流入）

### 3.5 工具類模式（Not Bound to Pipeline）

`MetadataBuilder` **刻意不繼承 `PipelineStep`**，設計為獨立工具類。這與 `accrual_bot` 中大量繼承 `PipelineStep` 的步驟不同：

```python
# ❌ 不這樣設計：
class MetadataBuilderStep(PipelineStep):
    async def execute(self, context) -> StepResult: ...

# ✅ 實際設計：普通工具類
builder = MetadataBuilder()
df = builder.build('./bank.xlsx', schema)
# 由呼叫者的 Step 決定如何使用 df
```

這種設計讓 `MetadataBuilder` 可以在任何地方使用，而非只限於 Pipeline 框架內。

---

## 四、各項知識點

### 4.1 `dtype='string'` vs `dtype=str`（Pandas StringDtype）

`SourceReader` 讀取所有欄位時使用 `dtype='string'`（注意是字串 `'string'`，不是 Python 的 `str`）：

```python
# reader.py 中的 read_excel()
if spec.read_as_string:
    read_kwargs["dtype"] = 'string'  # 使用 Pandas StringDtype
```

| 比較項目 | `dtype=str` | `dtype='string'` |
|---------|-------------|-----------------|
| pandas 類型 | `object`（Python str） | `StringDtype`（pd.NA 感知） |
| 空值表示 | `float('nan')` | `pd.NA` |
| `isna()` | 可能有邊界問題 | 完全感知 |
| 記憶體 | 非固定 | 略優化 |
| 引入版本 | 始終可用 | pandas 1.0+ |

使用 `StringDtype` 的主要優點是 NA 值以 `pd.NA` 表示，而非 `float('nan')`，使後續的 `isna()` 更加可靠。

### 4.2 `extract()` 中 `read_as_string=True` 的強制覆蓋

在 `MetadataBuilder.extract()` 中，無論 `SourceSpec` 的 `read_as_string` 設定為何，都會**強制**全字串讀取：

```python
# builder.py:119
spec = SourceSpec(
    ...
    read_as_string=True,  # 始終全字串讀取（即使 source_spec.read_as_string=False）
    ...
)
```

這是一個有意識的設計決策，確保 Bronze 層不受呼叫者的配置影響，始終保持「原始資料原樣落地」的語義。但這也意味著 `SourceSpec.read_as_string` 在通過 `MetadataBuilder.extract()` 使用時**實際上無效**，是一個可能造成混淆的行為。

### 4.3 Python 3.10 `match` 語句在類型路由中的應用

`SafeTypeCaster.cast_columns()` 使用 Python 3.10 引入的 `match` 語句進行類型路由：

```python
# type_caster.py:74
match dtype:
    case "VARCHAR" | "STRING" | "TEXT":
        continue  # 保持字串

    case "BIGINT" | "INTEGER" | "INT" | "INT64":
        df[spec.target] = self.cast_to_integer(df[spec.target])

    case "DOUBLE" | "FLOAT" | "DECIMAL" | "NUMERIC":
        df[spec.target] = self.cast_to_numeric(df[spec.target])
    ...
    case _:
        self.logger.warning(f"未知類型 {dtype}，保持原樣")
```

`match` 語句比 `if/elif` 鏈更清晰，且 `case A | B | C:` 的**或模式（OR pattern）**使多個別名的類型映射簡潔。注意 `case "BIGINT" | "INTEGER"` 匹配的是字面量字串，不需要建立映射字典。

### 4.4 Circuit Breaker 模式的跨域應用

Circuit Breaker（斷路器）模式原屬於**分散式系統容錯設計**（如 Netflix Hystrix），用於防止服務雪崩。本模組將此模式**創造性地應用於資料品質控制**：

| 比較維度 | 分散式系統 Circuit Breaker | 資料品質 Circuit Breaker |
|---------|--------------------------|------------------------|
| 觸發條件 | 服務呼叫失敗率超過閾值 | 欄位 NULL 比例超過閾值 |
| 觸發後行為 | 快速失敗，不再呼叫下游服務 | 拋出 `CircuitBreakerError`，阻止品質差的資料進入下游 |
| 可配置閾值 | 失敗率（如 50%） | NULL 比例（如 30%） |
| 結果物件 | 狀態枚舉 | `CircuitBreakerResult`（含詳細 null_ratios） |

`CircuitBreakerResult` 使用 `Literal["OK", "TRIPPED"]` 作為狀態類型，而非布林值，使狀態語義更明確：

```python
# circuit_breaker.py
@dataclass
class CircuitBreakerResult:
    status: Literal["OK", "TRIPPED"]  # 比 is_tripped: bool 更具可讀性

    @property
    def is_tripped(self) -> bool:
        return self.status == "TRIPPED"
```

### 4.5 Pandas 安全轉換的 `errors="coerce"` 模式

`SafeTypeCaster` 的各 `cast_to_*` 方法遵循相同的安全轉換模式：

```python
# 標準模式（以 cast_to_integer 為例）
def cast_to_integer(self, series: pd.Series) -> pd.Series:
    # 1. 字串清理：移除千分位、貨幣符號
    cleaned = series.astype('string').str.strip()
    cleaned = cleaned.str.replace(",", "", regex=False)  # 千分位
    cleaned = cleaned.str.replace("NT$", "", regex=False)  # 貨幣

    # 2. 空值標準化（統一為 np.nan）
    cleaned = cleaned.replace(["", "nan", "None", "N/A", "-"], np.nan)

    # 3. 安全轉換：無效值 → NaN（不拋出異常）
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")  # 大寫 I
```

注意 `.astype("Int64")`（大寫 `I`，Nullable Integer）與 `.astype("int64")`（小寫 `i`，不可空）的區別：

| 類型 | 空值支援 | 值域 |
|------|---------|------|
| `int64`（Python int） | 不支援（空值轉 float） | -9,223,372,036,854,775,808 ～ 9,223,372,036,854,775,807 |
| `Int64`（Nullable Integer） | 支援 `pd.NA` | 相同 |

### 4.6 `cast_to_date()` 的 `.dt.date` 截斷

```python
# type_caster.py:183
result = pd.to_datetime(cleaned, errors="coerce", dayfirst=False)
return result.dt.date  # ← 截斷為 datetime.date
```

`pd.to_datetime()` 返回 `datetime64[ns]`（帶時間），而 `.dt.date` 返回 Python `datetime.date` 物件的 Series。這確保 `DATE` 型別欄位不含時間部分。

但需注意：`.dt.date` 返回的 Series dtype 是 `object`（而非 `DatetimeTZDtype`），這在後續存入 DuckDB 時不會有問題（DuckDB 能自動識別），但在純 pandas 操作中可能意外：

```python
df['date'].dtype  # object（而非 datetime64[ns]）
```

### 4.7 `is_percent` 向量化布林遮罩的潛在問題

`cast_to_numeric()` 中的百分比處理邏輯：

```python
# type_caster.py:146-156
is_percent = cleaned.str.endswith("%")  # ← 返回 BooleanArray（可能含 pd.NA）
cleaned = cleaned.str.replace("%", "", regex=False)
cleaned = cleaned.replace(["", "nan", "None", "N/A", "-"], np.nan)

result = pd.to_numeric(cleaned, errors="coerce")

# 百分比轉換
result = result.where(~is_percent, result / 100)  # ← ~pd.NA 可能有問題
```

**潛在問題**：當 `cleaned` 含 `pd.NA`（空值）時，`cleaned.str.endswith("%")` 返回 `pd.NA`（不是 `False`）。`~pd.NA` 結果同為 `pd.NA`，而 `pd.Series.where(cond, other)` 在 `cond` 為 `pd.NA` 時行為為**保留原值**，這意味著 `pd.NA` 的儲存格不會進行 `/100` 轉換，邏輯正確。但若輸入是含 `None`（非 `pd.NA`）的 Series，行為可能不同。

### 4.8 `_normalize_column_name()` 的正則表達式清理鏈

```python
# bronze.py:120-134
def _normalize_column_name(self, name: str) -> str:
    name = name.strip()                           # 去前後空白
    name = re.sub(r"\s+", "_", name)              # 空白 → 底線
    name = re.sub(r"[^\w\u4e00-\u9fff]", "", name)  # 保留：word chars + 中文
    name = re.sub(r"_+", "_", name)               # 合併連續底線
    name = name.strip("_")                        # 去首尾底線
    return name if name else "unnamed"            # 空字串兜底
```

Unicode 範圍 `\u4e00-\u9fff` 涵蓋 CJK 統一漢字（基本區塊），正確保留中文欄位名稱。此清理鏈的順序很重要：先替換空白為底線，再移除特殊字元，最後合併連續底線。

### 4.9 `from_dict()` 的防禦性過濾

三個 dataclass 都實現了防禦性的 `from_dict()`：

```python
# config.py:62-64
@classmethod
def from_dict(cls, data: dict[str, Any]) -> "SourceSpec":
    valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
    filtered_data = {k: v for k, v in data.items() if k in valid_fields}
    return cls(**filtered_data)
```

利用 `__dataclass_fields__`（dataclass 自動生成的類屬性）來過濾未知鍵，避免 `TypeError: unexpected keyword argument`。這是一種「寬鬆輸入」的設計，對 YAML/TOML 配置來源特別重要，因為配置可能包含自定義注釋鍵。

### 4.10 `check_and_raise()` 的延遲 import

```python
# circuit_breaker.py:179
def check_and_raise(self, ...) -> CircuitBreakerResult:
    from ..exceptions import CircuitBreakerError  # ← 在方法內部 import
    result = self.check(df, column_specs, columns)
    if result.is_tripped:
        raise CircuitBreakerError(...)
    return result
```

將 `CircuitBreakerError` 的 import 放在方法內部，是一種**延遲 import** 技術。雖然此處不存在循環 import 問題（`circuit_breaker.py → exceptions.py` 無循環），但這種模式作為防禦性實踐，在修改 exceptions.py 時若意外引入循環依賴，此延遲 import 能阻止啟動時的 ImportError。

### 4.11 CSV 讀取的雙重容錯策略

```python
# reader.py:199-207
try:
    df = pd.read_csv(file_path, **read_kwargs)  # 優先嘗試 UTF-8
except UnicodeDecodeError:
    self.logger.warning(f"UTF-8 解碼失敗，嘗試 cp950 編碼: {file_path.name}")
    read_kwargs["encoding"] = "cp950"  # 自動 fallback 到 Big5
    df = pd.read_csv(file_path, **read_kwargs)
```

同時 `on_bad_lines="warn"` 允許格式不完整的 CSV 行以警告代替終止。這種「UTF-8 → cp950」的 fallback 策略針對台灣/中文環境的 Excel 匯出 CSV 特別實用，因為 Windows 預設以 Big5（cp950）編碼儲存 CSV。

### 4.12 `validate_only()` 的 Exception 吞噬問題

> ✅ **已修復（2026-03-14）**

**原始問題：**

```python
# 舊實作（已移除）
except Exception as e:
    cb_result = None  # ← 異常被吞噬

return {
    "valid": len(missing_required) == 0 and (cb_result is None or cb_result.is_ok),
    # cb_result is None → True → 驗證過程出錯反而被誤判為「通過」
}
```

**修復後（silver.py:164–204）：**

```python
cb_result = None
validation_error = None
try:
    df_mapped = self.column_mapper.map_columns(
        df, schema_config.columns, preserve_unmapped=True
    )
    df_casted = self.type_caster.cast_columns(df_mapped, schema_config.columns)
    breaker = self.circuit_breaker or CircuitBreaker(
        threshold=schema_config.circuit_breaker_threshold
    )
    cb_result = breaker.check(df_casted, schema_config.columns)
except Exception as e:
    validation_error = str(e)  # ← 保留錯誤訊息供呼叫者診斷

return {
    "valid": (
        len(missing_required) == 0
        and validation_error is None     # ← 驗證過程本身未出錯
        and cb_result is not None        # ← cb_result 必須被成功賦值
        and cb_result.is_ok
    ),
    "missing_required_columns": missing_required,
    "circuit_breaker_result": cb_result,
    "validation_error": validation_error,  # ← 新增：描述過程中的意外錯誤
}
```

**修復重點：** `cb_result is None` 的語意從「通過」改為「未知/失敗」，同時新增 `validation_error` 欄位讓呼叫者看到為何無法完成驗證。

---

## 五、應用範例

### 5.1 基本用法：一次完成 Bronze + Silver

```python
from accrual_bot.utils.metadata_builder import (
    MetadataBuilder, SchemaConfig, ColumnSpec
)

# 定義 Schema
schema = SchemaConfig(
    columns=[
        ColumnSpec(source='交易日期', target='date', dtype='DATE', required=True),
        ColumnSpec(source='金額',     target='amount', dtype='BIGINT'),
        ColumnSpec(source='.*備註.*', target='remarks', dtype='VARCHAR'),  # regex
    ],
    circuit_breaker_threshold=0.3,
    filter_empty_rows=True,
)

# 一次完成兩層處理
builder = MetadataBuilder()
df = builder.build(
    './input/bank_data.xlsx',
    schema,
    sheet_name='B2B',
    header_row=2,
)
print(f"處理結果: {df.shape}")
```

### 5.2 分步用法：Bronze 後人工檢查再 Silver

```python
from accrual_bot.utils.metadata_builder import MetadataBuilder, SchemaConfig

builder = MetadataBuilder()

# Step 1: Bronze - 原樣讀取並預覽結構
info = builder.extract_and_preview('./input/bank.xlsx', sheet_name=0, n_rows=5)
print("可用欄位:", info['columns'])
print("資料形狀:", info['shape'])

# Step 2: 確認欄位後進行完整 Bronze
df_raw = builder.extract(
    './input/bank.xlsx',
    sheet_name=0,
    header_row=3,     # 第 4 行才是 Header
    add_metadata=True,
    batch_id='2025-12-batch-001'
)
print("Bronze 欄位:", list(df_raw.columns))
# 包含 _source_file, _sheet_name, _batch_id, _ingested_at

# Step 3: Silver
schema = SchemaConfig(columns=[
    ColumnSpec(source='日期', target='date', dtype='DATE'),
    ColumnSpec(source='金額', target='amount', dtype='BIGINT'),
])
df_clean = builder.transform(df_raw, schema)
```

### 5.3 從 YAML 檔案讀取 Schema

```yaml
# schema_config.yaml
banks:
  cub:
    circuit_breaker_threshold: 0.5
    filter_empty_rows: true
    columns:
      - source: "交易日期"
        target: "date"
        dtype: "DATE"
        required: true
      - source: ".*金額.*"   # regex 匹配含「金額」的欄位
        target: "amount"
        dtype: "BIGINT"
      - source: "備忘欄"
        target: "remarks"
        dtype: "VARCHAR"
        default: ""          # 欄位不存在時填入空字串
```

```python
from accrual_bot.utils.metadata_builder import MetadataBuilder, SchemaConfig

schema = SchemaConfig.from_yaml('./schema_config.yaml', section='banks.cub')
builder = MetadataBuilder()
df = builder.build('./input/cub_statement.xlsx', schema)
```

### 5.4 在 Pipeline Step 中使用

```python
from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec
from accrual_bot.utils.logging import get_logger

class BankDataLoadingStep(PipelineStep):
    """載入銀行對帳資料的 Pipeline Step"""

    def __init__(self, schema_config: SchemaConfig, **kwargs):
        super().__init__(name="BankDataLoading", **kwargs)
        self.schema_config = schema_config
        self.logger = get_logger(self.__class__.__name__)

    async def execute(self, context: ProcessingContext) -> StepResult:
        file_path = context.get_variable('bank_file_path')

        builder = MetadataBuilder(logger=self.logger)

        try:
            df = builder.build(file_path, self.schema_config)
            context.update_data(df)
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                metadata={'rows': len(df), 'columns': list(df.columns)}
            )
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error_message=str(e)
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        return context.get_variable('bank_file_path') is not None
```

### 5.5 獨立使用 CircuitBreaker 進行資料品質報告

```python
from accrual_bot.utils.metadata_builder import CircuitBreaker, ColumnSpec
import pandas as pd

# 已有的 DataFrame 進行品質檢查
df = pd.read_excel('./data.xlsx')

breaker = CircuitBreaker(threshold=0.2)  # 20% NULL 容忍度

# 取得所有欄位的 NULL 統計
summary = breaker.get_null_summary(df)
print(summary[summary['exceeds_threshold']])  # 只看超標欄位

# 或整合 ColumnSpec 進行指定欄位檢查
specs = [
    ColumnSpec(source='date', target='date', dtype='DATE'),
    ColumnSpec(source='amount', target='amount', dtype='BIGINT'),
]
result = breaker.check(df, column_specs=specs)
if result.is_tripped:
    print(f"資料品質不合格: {result.message}")
    print(f"觸發欄位: {result.tripped_columns}")
    print(f"NULL 比例: {result.null_ratios}")
```

### 5.6 從 TOML 讀取 Schema（與 accrual_bot 配置整合）

```toml
# accrual_bot/config/stagging.toml

[bank_schema]
circuit_breaker_threshold = 0.3

[[bank_schema.columns]]
source = "交易日期"
target = "date"
dtype = "DATE"
required = true

[[bank_schema.columns]]
source = "金額"
target = "amount"
dtype = "BIGINT"
```

```python
from accrual_bot.utils.metadata_builder import SchemaConfig, MetadataBuilder

# 從現有的 TOML 配置讀取 Schema
schema = SchemaConfig.from_toml(
    'accrual_bot/config/stagging.toml',
    section='bank_schema'
)
builder = MetadataBuilder()
df = builder.build('./bank_data.xlsx', schema)
```

### 5.7 自定義 SourceSpec 覆蓋預設

```python
from accrual_bot.utils.metadata_builder import MetadataBuilder, SourceSpec, SchemaConfig

# 為 CSV 檔案建立專用 SourceSpec
csv_spec = SourceSpec(
    file_type='csv',
    encoding='cp950',      # Big5 編碼的 CSV
    delimiter='\t',        # 以 Tab 分隔
    header_row=0,
    read_as_string=True,
)

builder = MetadataBuilder(source_spec=csv_spec)
schema = SchemaConfig(columns=[...])
df = builder.build('./export.csv', schema)
```

### 5.8 獨立使用 BronzeProcessor 進行欄位名稱標準化

```python
from accrual_bot.utils.metadata_builder import BronzeProcessor
import pandas as pd

# 欄位名稱含特殊字元的 DataFrame
df = pd.DataFrame({
    '  交易 日期  ': ['2025/12/01'],
    '金額($)': [1000],
    'Product#Code': ['P001'],
})

processor = BronzeProcessor(normalize_columns=True, add_row_num=True)
df_bronze = processor.process(df, source_file='test.xlsx', add_metadata=False)
print(df_bronze.columns.tolist())
# ['_row_num', '交易_日期', '金額', 'ProductCode']
```

---

## 六、優缺分析

### 6.1 優點

#### ✅ STRENGTH-1：清晰的職責分離（SRP）

每個子元件有單一職責：

- `SourceReader`：只負責讀取
- `BronzeProcessor`：只負責標準化
- `ColumnMapper`：只負責欄位映射
- `SafeTypeCaster`：只負責類型轉換
- `CircuitBreaker`：只負責 NULL 比例檢查

這種設計使每個元件均可獨立測試，且替換其中一個不影響其他。

#### ✅ STRENGTH-2：可移植性（Portability）

模組自我描述為「可移植插件」，做到了：
- 無 accrual_bot 特有依賴（不引用 `config_manager`、`PipelineStep` 等）
- 使用標準 Python 庫 + pandas（最低依賴）
- PyYAML 採用 lazy import（可選依賴）

#### ✅ STRENGTH-3：失敗不中斷（Fail-Safe）的轉換策略

`SafeTypeCaster` 對所有類型轉換使用 `errors="coerce"`，轉換失敗變 NULL 而非拋出異常，確保一筆髒資料不會中斷整批處理。這對 Excel 資料尤其重要（混合了數值和文字的「備註格」很常見）。

#### ✅ STRENGTH-4：完整的異常層次（Exception Hierarchy）

6 個自定義異常形成清晰的繼承樹：

```
MetadataBuilderError（基礎）
├── SourceFileError（檔案讀取）
│   └── SheetNotFoundError（Sheet 不存在）
├── SchemaValidationError（Schema 驗證）
├── CircuitBreakerError（資料品質）
├── TypeCastingError（定義但未使用）
└── ColumnMappingError（Regex 語法錯誤，✅ 2026-03-14 啟用）
```

呼叫者可以按粒度捕獲：`except SourceFileError` 只處理檔案問題，`except MetadataBuilderError` 處理所有情況。

#### ✅ STRENGTH-5：`CircuitBreakerResult` 的資訊豐富度

`CircuitBreakerResult` 不只返回 bool，而是返回完整的診斷資訊：

```python
result.status         # "OK" | "TRIPPED"
result.null_ratios    # {'date': 0.05, 'amount': 0.42}（各欄位比例）
result.tripped_columns  # ['amount']（觸發欄位清單）
result.message        # 人類可讀的說明
```

這對除錯資料問題非常有價值。

### 6.2 缺點與設計問題

#### ~~🔴 DEFECT-1（嚴重）：`validate_only()` 的靜默通過問題~~ ✅ 已修復（2026-03-14）

**位置**：`silver.py` `validate_only()`

~~當 `column_mapper.map_columns()` 或 `type_caster.cast_columns()` 拋出異常時，`cb_result = None`，而 `valid` 的計算 `cb_result is None or cb_result.is_ok` 等於 `True`，導致**驗證失敗被誤報為通過**。~~

修復：`cb_result is None` 語意改為「失敗」；新增 `validation_error` 欄位記錄異常訊息；`valid` 條件改為明確要求 `cb_result is not None and cb_result.is_ok`。詳見 §4.12。

#### ~~🟡 DEFECT-2（中等）：`cast_failures` 計算的誤導性~~ ✅ 已修復（2026-03-14）

**位置**：`type_caster.py` `cast_columns()`

~~這個計數包含了**原本就是 NULL 的值**，不只是轉換失敗的值。名稱 `cast_failures` 暗示是「轉換失敗的筆數」，但實際是「轉換後的 NULL 總數」。~~

修復：在每個 `match case` 前記錄 `pre_null_count`，改用差值（`null_after - null_before`）計算，只統計轉換新引入的 NULL。

#### ~~🟡 DEFECT-3（中等）：`ColumnMappingError` 定義但從未拋出~~ ✅ 已修復（2026-03-14）

**位置**：`exceptions.py` `ColumnMappingError`、`column_mapper.py` `find_matching_column()`

~~`ColumnMappingError` 在 `column_mapper.py` 中被 import 但從未被 `raise`，成為死碼（dead code）。~~

修復：`find_matching_column()` 的 `re.error` 捕獲改為 `raise ColumnMappingError(source_pattern, columns, reason=f"Regex 語法錯誤: {e}")`；`exceptions.py` 加入可選 `reason` 參數，使錯誤訊息更清晰。

#### 🟡 DEFECT-4（中等）：`extract()` 靜默忽略 `read_as_string=False`

**位置**：`builder.py:119`

`MetadataBuilder.extract()` 無論 `SourceSpec.read_as_string` 設定為何，都強制設 `read_as_string=True`。雖然這是設計意圖，但沒有任何文件或警告告知呼叫者此設定被忽略，容易造成混淆。

#### 🟢 DEFECT-5（輕微）：測試覆蓋嚴重不足

**位置**：`tests/unit/utils/metadata_builder/test_metadata_builder.py`

現有 196 行測試**只測試 `config.py` 的三個 dataclass**（SourceSpec、ColumnSpec、SchemaConfig），而 `builder.py`、`reader.py`、`processors/`、`transformers/`、`validation/` 完全沒有單元測試。

根據 CLAUDE.md 的覆蓋率報告：
- `utils/metadata_builder/config.py`：81%（已測）
- `utils/metadata_builder/processors/*`：19-26%
- `utils/metadata_builder/processors/silver.py`：涵蓋的複雜邏輯（`validate_only`、`_filter_empty_rows`）無測試

#### 🟢 DEFECT-6（輕微）：兩個 `MetadataBuilder` 名稱衝突

| 類別 | 位置 | 用途 |
|------|------|------|
| `MetadataBuilder` | `utils/metadata_builder/builder.py` | 髒資料處理器 |
| `StepMetadataBuilder` | `core/pipeline/steps/common.py:1160` | 步驟執行統計構建器 |

兩者在不同命名空間，但名稱高度相似，搜尋 `metadata_builder` 會同時找到兩者，增加維護時的認知負擔。

#### 🟢 DEFECT-7（輕微）：程式碼複製問題（Code Duplication）

`accrual_bot/utils/metadata_builder/` 和 `spe_bank_recon/src/utils/metadata_builder/` 幾乎完全相同，但作為兩個獨立副本存在。若未來修正 bug（如 DEFECT-1），需要在兩處同步修改，容易發生遺漏。

#### 🟢 DEFECT-8（輕微）：README 中的舊路徑

**位置**：`accrual_bot/utils/metadata_builder/README.md`

README 中所有範例使用 `from src.utils.metadata_builder import...`（spe_bank_recon 的路徑），在 accrual_bot 中正確路徑應是 `from accrual_bot.utils.metadata_builder import...`。

---

## 七、延伸議題

### 7.1 打包為獨立 PyPI 套件

目前採用「原始碼複製」的可移植方式，同樣的程式碼維護在兩個專案中。更優的方案是抽取為獨立套件：

```
spe-metadata-builder/
├── pyproject.toml
├── src/
│   └── spe_metadata_builder/
│       └── ...（同現有結構）
└── tests/
```

```toml
# pyproject.toml
[project]
name = "spe-metadata-builder"
version = "1.0.0"
dependencies = ["pandas>=1.3.0"]

[project.optional-dependencies]
yaml = ["pyyaml>=6.0"]
```

這樣兩個專案可以共用同一套件，修復 bug 只需更新套件版本。

### 7.2 Gold Layer（黃金層）的延伸

現有的 Bronze/Silver 兩層對於簡單場景足夠，但資料湖架構通常有三層：

```
Bronze（原始）→ Silver（清洗）→ Gold（業務聚合）
```

Gold Layer 可以處理：
- 多來源資料的 join 操作
- 業務計算（如計算應計金額）
- 重複資料消除（upsert 邏輯）

目前 `MetadataBuilder` 的 `build()` 在 Silver 後停止，Gold Layer 完全由呼叫者實現，沒有標準化支援。

### 7.3 `SourceReader` 擴充：非同步讀取

目前所有讀取都是同步的（`pd.read_excel()`、`pd.read_csv()`），在 Pipeline 的 async 上下文中使用時，這些同步操作會阻塞事件循環。

可以考慮在 `MetadataBuilder.extract()` 層提供 async 版本：

```python
async def extract_async(self, file_path, ...) -> pd.DataFrame:
    """非同步讀取，在 thread pool 中執行"""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: self.extract(file_path, ...)
    )
```

### 7.4 Schema 版本管理與遷移

當來源 Excel 格式隨時間變化時（如供應商修改了欄位名稱），Schema 需要更新。可以考慮：

1. **Schema 版本化**：`SchemaConfig(version='2025-Q4')`
2. **多版本相容**：`ColumnSpec(source='金額|Amount', target='amount')` 利用現有 regex `|` 支援
3. **Schema 差異報告**：在 Bronze 階段對比實際欄位與 Schema 期望欄位，生成差異報告

### 7.5 類型推斷（Type Inference）自動化

目前 `dtype` 必須手動在 `ColumnSpec` 中指定。可以考慮加入自動類型推斷：

```python
# 概念性 API
df_raw = builder.extract('./bank.xlsx')
inferred_schema = SchemaConfig.infer_from_df(df_raw, sample_rows=100)
# 自動推斷：數字列 → BIGINT/DOUBLE，日期格式 → DATE，其他 → VARCHAR
```

這對初次接觸來源資料的場景（如 `extract_and_preview()` 的延伸）特別有用。

### 7.6 改善測試覆蓋的策略

現有測試嚴重偏重配置類，建議補充：

```python
# 需要補充的測試類型

# 1. SourceReader 讀取測試（使用 tmp_path fixture）
def test_read_excel_with_header_row(tmp_path):
    # 建立測試 Excel，驗證 header_row=2 正確讀取

# 2. BronzeProcessor 欄位標準化
def test_normalize_column_name_removes_special_chars():
    processor = BronzeProcessor()
    assert processor._normalize_column_name("金額($)") == "金額"

# 3. SafeTypeCaster 安全轉換
def test_cast_to_integer_with_comma_separator():
    caster = SafeTypeCaster()
    s = pd.Series(["1,234", "invalid", None])
    result = caster.cast_to_integer(s)
    assert result[0] == 1234
    assert pd.isna(result[1])
    assert pd.isna(result[2])

# 4. CircuitBreaker 觸發
def test_circuit_breaker_trips_when_null_ratio_exceeds_threshold():
    breaker = CircuitBreaker(threshold=0.3)
    df = pd.DataFrame({'col': [None] * 40 + [1] * 60})  # 40% NULL
    specs = [ColumnSpec(source='col', target='col')]
    result = breaker.check(df, specs)
    assert result.is_tripped
```

---

## 八、其他

### 8.1 完整公開類別與方法一覽

#### `SourceSpec`（dataclass）

| 屬性/方法 | 類型 | 預設值 | 說明 |
|----------|------|--------|------|
| `file_type` | `Literal["excel","csv","parquet","json"]` | `"excel"` | 檔案格式 |
| `encoding` | `str` | `"utf-8"` | 字元編碼 |
| `read_as_string` | `bool` | `True` | 全字串讀取 |
| `sheet_name` | `str \| int` | `0` | Excel Sheet |
| `header_row` | `int` | `0` | Header 所在行（0-indexed） |
| `skip_rows` | `int` | `0` | 跳過行數 |
| `delimiter` | `str` | `","` | CSV 分隔符 |
| `from_dict(data)` | classmethod | — | 從字典建立（過濾未知鍵） |
| `to_dict()` | method | — | 轉換為字典 |

#### `ColumnSpec`（dataclass）

| 屬性/方法 | 類型 | 預設值 | 說明 |
|----------|------|--------|------|
| `source` | `str` | — | 來源欄位名（支援 regex） |
| `target` | `str` | — | 目標欄位名 |
| `dtype` | `str` | `"VARCHAR"` | 目標類型（`__post_init__` 大寫化並驗證） |
| `required` | `bool` | `False` | 缺失時拋出 `SchemaValidationError` |
| `default` | `Any` | `None` | 欄位不存在時的填充值 |
| `date_format` | `str \| None` | `None` | 日期格式（DATE 類型使用） |
| `is_regex` | 屬性 | — | 判斷 source 是否含 regex 字元 |
| `from_dict(data)` | classmethod | — | 從字典建立 |

有效 dtype 清單：`VARCHAR`, `BIGINT`, `INTEGER`, `DOUBLE`, `FLOAT`, `DATE`, `DATETIME`, `TIMESTAMP`, `BOOLEAN`, `BOOL`

#### `SchemaConfig`（dataclass）

| 屬性/方法 | 類型 | 預設值 | 說明 |
|----------|------|--------|------|
| `columns` | `list[ColumnSpec]` | `[]` | 欄位定義清單 |
| `circuit_breaker_threshold` | `float` | `0.3` | NULL 比例閾值（0~1） |
| `filter_empty_rows` | `bool` | `True` | 過濾全空行 |
| `preserve_unmapped` | `bool` | `False` | 保留未映射欄位 |
| `required_columns` | 屬性 | — | 取得 required=True 的欄位 |
| `target_columns` | 屬性 | — | 取得所有目標欄位名 |
| `from_dict(data)` | classmethod | — | 支援 ColumnSpec 物件混入 |
| `from_yaml(path, section)` | classmethod | — | 支援點路徑導航 |
| `from_toml(path, section)` | classmethod | — | 使用 tomllib（Python 3.11+） |
| `to_dict()` | method | — | 序列化 |

#### `MetadataBuilder`

| 方法 | 回傳值 | 說明 |
|------|--------|------|
| `extract(file_path, sheet_name, header_row, add_metadata, batch_id, **kwargs)` | `pd.DataFrame` | Bronze 層 |
| `transform(df, schema_config, validate)` | `pd.DataFrame` | Silver 層 |
| `build(file_path, schema_config, ...)` | `pd.DataFrame` | Bronze + Silver |
| `extract_and_preview(file_path, n_rows)` | `dict` | 預覽用，不加 metadata |
| `get_excel_sheets(file_path)` | `list[str]` | Sheet 名稱列表 |

#### `CircuitBreaker`

| 方法 | 回傳值 | 說明 |
|------|--------|------|
| `check(df, column_specs, columns)` | `CircuitBreakerResult` | 檢查，不拋出異常 |
| `check_and_raise(df, column_specs, columns)` | `CircuitBreakerResult` | 觸發時拋出 `CircuitBreakerError` |
| `get_null_summary(df)` | `pd.DataFrame` | 完整 NULL 統計表格 |

### 8.2 Bronze 層 metadata 欄位說明

當 `add_metadata=True`（預設）時，Bronze 層會添加以下 4 個追溯欄位：

| 欄位名 | 類型 | 說明 | 範例值 |
|--------|------|------|--------|
| `_source_file` | str | 來源檔案名稱（只取 `Path.name`） | `bank_202512.xlsx` |
| `_sheet_name` | str | Sheet 名稱或索引的字串表示 | `B2B` 或 `0` |
| `_batch_id` | str | UUID 前 8 碼（可自訂） | `a3f7c2d1` |
| `_ingested_at` | str | 載入時間（ISO 8601 格式） | `2025-12-01T14:30:00.123456` |

若 `add_row_num=True`（BronzeProcessor 選項），另有：

| 欄位名 | 類型 | 說明 |
|--------|------|------|
| `_row_num` | int | 原始行號（從 1 開始） |

### 8.3 支援的類型轉換行為矩陣

| dtype | 空值輸入 | 逗號數字 `"1,234"` | 百分比 `"75%"` | 布林 `"是"/"否"` |
|-------|---------|------------------|---------------|----------------|
| `BIGINT` | `pd.NA` | `1234` | `pd.NA`（失敗） | `pd.NA`（失敗） |
| `DOUBLE` | `pd.NA` | `1234.0` | `0.75` | `pd.NA`（失敗） |
| `DATE` | `pd.NA` | `pd.NA`（失敗） | `pd.NA`（失敗） | `pd.NA`（失敗） |
| `BOOLEAN` | `pd.NA` | `pd.NA`（無匹配） | `pd.NA`（無匹配） | `True`/`False` |
| `VARCHAR` | 保持原值 | `"1,234"`（原樣） | `"75%"`（原樣） | `"是"`（原樣） |

Boolean true 值集合：`{"true", "yes", "1", "y", "t", "是", "有"}`
Boolean false 值集合：`{"false", "no", "0", "n", "f", "否", "無"}`

### 8.4 異常類結構與觸發條件

```
MetadataBuilderError
│
├── SourceFileError（file_path, message 屬性）
│   ├── 觸發：檔案不存在、讀取失敗
│   └── SheetNotFoundError（sheet_name 屬性）
│       └── 觸發：Excel Sheet 名稱不存在
│
├── SchemaValidationError（missing_columns 屬性）
│   └── 觸發：required=True 的欄位在來源中找不到
│
├── CircuitBreakerError（tripped_columns, null_ratios, threshold 屬性）
│   └── 觸發：任一欄位 NULL 比例超過 circuit_breaker_threshold
│
├── TypeCastingError（column_name, target_type, failed_count 屬性）
│   └── ⚠️ 定義但未使用（SafeTypeCaster 不拋出此異常）
│
└── ColumnMappingError（source_pattern, available_columns, reason 屬性）
    └── ✅ 已啟用（2026-03-14）：find_matching_column() Regex 語法錯誤時觸發
```

### 8.5 `MetadataBuilder` 與 `StepMetadataBuilder` 的概念辨析

這兩個類別名稱相近，但完全不同：

| 比較維度 | `MetadataBuilder` | `StepMetadataBuilder` |
|---------|------------------|----------------------|
| 所在位置 | `utils/metadata_builder/builder.py` | `core/pipeline/steps/common.py:1160` |
| 用途 | 處理髒資料（Excel/CSV → DataFrame） | 構建 Pipeline Step 的執行統計 metadata |
| 回傳值 | `pd.DataFrame` | `dict`（step 執行統計） |
| 輸入 | 檔案路徑 + SchemaConfig | 行數、時間、自定義 kv |
| 使用場景 | 資料載入 | Pipeline 步驟的結果報告 |
| 實際使用 | 僅在 spe_bank_recon | accrual_bot 生產程式碼中廣泛使用 |

### 8.6 模組版本與相容性要求

```python
__version__ = "1.0.0"  # utils/metadata_builder/__init__.py
```

| 依賴 | 最低版本 | 用途 |
|------|---------|------|
| Python | 3.10+ | `match` 語句（`type_caster.py`）、`tomllib`（3.11+ 內建） |
| pandas | 1.3+ | `StringDtype`（`dtype='string'`）、`on_bad_lines` 參數 |
| numpy | 任意 | `np.nan`、`pd.to_numeric` 底層 |
| pyyaml | 6.0+（可選） | `SchemaConfig.from_yaml()` |

> 若 Python 版本為 3.10（非 3.11），`tomllib` 未內建，需安裝 `tomli` 並在 `config.py` 中添加相容性 import。現有程式碼直接 `import tomllib`，在 3.10 環境下會 ImportError。

---

*文件版本：1.1.0 | 最後更新：2026-03-14（修復 DEFECT-1/2/3）*
