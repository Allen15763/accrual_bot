# SPT PROCUREMENT 任務實作文檔

> **版本**: 2.0.0
> **最後更新**: 2026-01-18
> **作者**: Development Team
> **狀態**: Production Ready

## 📋 變更日誌

### v2.0.0 (2026-01-18)
- ✅ 實作 COMBINED 合併處理模式
  - 新增 `CombinedProcurementDataLoadingStep`
  - 新增 `CombinedProcurementProcessingStep`
  - 新增 `CombinedProcurementExportStep`
- ✅ 新增前期底稿格式驗證
  - 新增 `ProcurementPreviousValidationStep`
  - 驗證 Excel 格式、sheets 存在性、必要欄位
  - 支援嚴格/寬鬆模式
- ✅ 更新 pipeline_orchestrator 支援三種模式
- ✅ 更新配置檔案 (stagging.toml)
- ✅ 完整語法檢查通過

### v1.0.0 (2026-01-17)
- ✅ 新增 SPT PROCUREMENT 處理類型
- ✅ 實作配置驅動的狀態判斷系統（6 種條件類型）
- ✅ 實作配置驅動的前期底稿映射
- ✅ 支援 PO、PR 單獨處理模式
- ✅ 完成 UI 配置整合

---

## 目錄

1. [概述與需求](#1-概述與需求)
2. [架構設計](#2-架構設計)
3. [配置檔案](#3-配置檔案)
4. [Pipeline 步驟](#4-pipeline-步驟)
5. [Orchestrator 整合](#5-orchestrator-整合)
6. [UI 配置](#6-ui-配置)
7. [使用方式](#7-使用方式)
8. [測試驗證](#8-測試驗證)
9. [已知限制](#9-已知限制)
10. [附錄](#10-附錄)

---

## 1. 概述與需求

### 1.1 業務需求

SPT PROCUREMENT 是專為採購人員設計的處理流程，用於審核採購單（PO）和請購單（PR）的狀態。

**主要功能**:
1. 讀取原始 PO/PR 資料
2. 整合採購前期底稿資料
3. 根據業務規則自動判斷狀態
4. 支援單一類型或合併處理

### 1.2 處理流程

```
原始資料 (raw_po/raw_pr) ─┐
                          ├─> 資料載入 -> 欄位初始化 -> 前期映射 ->
採購前期底稿            ─┘    日期邏輯 -> 狀態判斷 -> 匯出結果
```

### 1.3 狀態判斷邏輯（優先順序）

| 優先順序 | 條件 | 狀態值 |
|---------|------|--------|
| 1 | Item Description 含 "Affiliate" (不分大小寫) | `Affiliate` |
| 2 | Item Description 含 "Employee Bonus" (不分大小寫) | `FN Payroll` |
| 3 | Item Description 同時含 "SVP" 和 ("租金" 或 "rent") (不分大小寫) | `Svp Rental` |
| 4 | 前期 remarked_procurement = "已完成" | `前期已完成` |
| 5 | 前期 remarked_procurement ≠ "已完成" 且 ERM 在摘要日期範圍內 且 ERM ≤ 結帳月 | `已完成` |
| 6 | ERM 在摘要日期範圍內 且 ERM > 結帳月 | `未完成` |

### 1.4 使用者工作流程

**單一類型處理**:
- 給予 `raw_po` + `procurement_previous` → 產出 PO 檔案
- 給予 `raw_pr` + `procurement_previous` → 產出 PR 檔案

**合併處理** (v2.0.0 新增):
- 給予 `raw_po` + `raw_pr` + `procurement_previous` → 產出單一 Excel（含 PO、PR 兩個 sheets）
- 自動驗證前期底稿格式（Excel、sheets、必要欄位）
- 分別處理 PO 和 PR 資料，最後合併輸出

---

## 2. 架構設計

### 2.1 設計原則

**完全配置驅動** (Configuration-Driven):
- 所有條件規則定義在 `stagging.toml`
- 新增條件無需修改程式碼
- 映射規則透過配置管理

**模組化設計**:
- 每個步驟職責單一
- 可獨立測試
- 易於維護

**可擴充性**:
- 支援新增條件類型
- 支援新增映射欄位
- 支援新增處理模式

### 2.2 步驟架構

```
SPTProcurementDataLoadingStep (PO)        SPTProcurementPRDataLoadingStep (PR)
    ├─ 載入 raw_po                             ├─ 載入 raw_pr
    └─ 載入 procurement_previous (PO sheet)    └─ 載入 procurement_previous (PR sheet)
                            ↓
                 ColumnInitializationStep
                    ├─ 初始化 PO狀態/PR狀態 欄位
                            ↓
              ProcurementPreviousMappingStep
                    ├─ 使用 ColumnResolver 解析欄位
                    ├─ 映射 prev_remarked_procurement
                            ↓
                      DateLogicStep (複用)
                    ├─ 解析 Item Description 日期範圍
                    ├─ 轉換 Expected Received Month 格式
                            ↓
           SPTProcurementStatusEvaluationStep
                    ├─ 載入配置驅動條件規則
                    ├─ 按優先順序評估條件
                    ├─ 應用狀態值
                            ↓
                    SPTProcurementExport
                    └─ 匯出 Excel 結果
```

### 2.3 條件評估引擎

**支援的條件類型**:

| 條件類型 | 說明 | 範例 |
|---------|------|------|
| `contains` | 欄位包含正則模式 | `(?i)affiliate` |
| `equals` | 欄位等於指定值 | `"已完成"` |
| `not_equals` | 欄位不等於指定值 | `"已完成"` |
| `erm_in_range` | ERM 在 Item Description 日期範圍內 | - |
| `erm_le_closing` | ERM ≤ 結帳月 | - |
| `erm_gt_closing` | ERM > 結帳月 | - |

**條件組合邏輯**:
- `combine = "and"`: 所有 checks 皆需滿足
- `combine = "or"`: 任一 check 滿足即可

---

## 3. 配置檔案

### 3.1 paths.toml - 檔案路徑配置

```toml
# =============================================================================
# SPT PROCUREMENT 路徑配置
# =============================================================================
[spt.procurement]
raw_po = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_order_*.csv"
raw_pr = "{resources}/{YYYYMM}/Original Data/{YYYYMM}_purchase_request_*.csv"
procurement_previous = "{resources}/{YYYYMM}/採購底稿/{PREV_YYYYMM}_採購底稿.xlsx"

[spt.procurement.params]
raw_po = { encoding = "utf-8", sep = ",", dtype = "str", keep_default_na = false, na_values = [""] }
raw_pr = { encoding = "utf-8", sep = ",", dtype = "str", keep_default_na = false, na_values = [""] }
procurement_previous = { dtype = "str" }
```

**路徑變數**:
- `{resources}`: 資源根目錄
- `{YYYYMM}`: 結帳月份（例如 202512）
- `{PREV_YYYYMM}`: 前期月份（例如 202511）

### 3.2 stagging.toml - Pipeline 步驟配置

```toml
[pipeline.spt]
enabled_procurement_po_steps = [
    "SPTProcurementDataLoading",
    "ColumnInitialization",
    "ProcurementPreviousMapping",
    "DateLogic",
    "SPTProcurementStatusEvaluation",
    "SPTProcurementExport",
]

enabled_procurement_pr_steps = [
    "SPTProcurementPRDataLoading",
    "ColumnInitialization",
    "ProcurementPreviousMapping",
    "DateLogic",
    "SPTProcurementStatusEvaluation",
    "SPTProcurementExport",
]
```

### 3.3 stagging.toml - 映射配置

```toml
# =============================================================================
# 採購前期底稿映射配置
# =============================================================================
[spt_procurement_previous_mapping]

# 欄位名稱正則模式
[spt_procurement_previous_mapping.column_patterns]
po_line = '(?i)^(po[_\s]?line)$'
pr_line = '(?i)^(pr[_\s]?line)$'
remarked_by_procurement = '(?i)^(remarked?[_\s]?by[_\s]?(procurement|pq))$'

# PO 映射配置
[[spt_procurement_previous_mapping.po_mappings.fields]]
source = "remarked_by_procurement"
target = "prev_remarked_procurement"
fill_na = true
description = "前期採購備註"

# PR 映射配置
[[spt_procurement_previous_mapping.pr_mappings.fields]]
source = "remarked_by_procurement"
target = "prev_remarked_procurement"
fill_na = true
description = "前期採購備註"
```

**映射參數**:
- `source`: 來源欄位（使用 canonical name）
- `target`: 目標欄位名稱
- `fill_na`: 是否僅填充空值（true）或允許覆蓋（false）
- `description`: 欄位說明

### 3.4 stagging.toml - 狀態判斷規則

```toml
# =============================================================================
# 採購狀態判斷規則 - 完全配置驅動
# =============================================================================

[[spt_procurement_status_rules.conditions]]
priority = 1
status_value = "Affiliate"
note = "Item Description 包含 Affiliate"
[[spt_procurement_status_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern = "(?i)affiliate"

[[spt_procurement_status_rules.conditions]]
priority = 2
status_value = "FN Payroll"
note = "Item Description 包含 Employee Bonus"
[[spt_procurement_status_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern = "(?i)employee\\s*bonus"

[[spt_procurement_status_rules.conditions]]
priority = 3
status_value = "Svp Rental"
note = "Item Description 同時包含 SVP 和 租金/rent"
combine = "and"
[[spt_procurement_status_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern = "(?i)svp"
[[spt_procurement_status_rules.conditions.checks]]
field = "Item Description"
type = "contains"
pattern = "(?i)租金|rent"

[[spt_procurement_status_rules.conditions]]
priority = 4
status_value = "前期已完成"
note = "前期採購備註為已完成"
[[spt_procurement_status_rules.conditions.checks]]
field = "prev_remarked_procurement"
type = "equals"
value = "已完成"

[[spt_procurement_status_rules.conditions]]
priority = 5
status_value = "已完成"
note = "前期非已完成，且 ERM 在範圍內且 ≤ 結帳月"
combine = "and"
[[spt_procurement_status_rules.conditions.checks]]
field = "prev_remarked_procurement"
type = "not_equals"
value = "已完成"
[[spt_procurement_status_rules.conditions.checks]]
type = "erm_in_range"
[[spt_procurement_status_rules.conditions.checks]]
type = "erm_le_closing"

[[spt_procurement_status_rules.conditions]]
priority = 6
status_value = "未完成"
note = "ERM 在範圍內且 > 結帳月"
combine = "and"
[[spt_procurement_status_rules.conditions.checks]]
type = "erm_in_range"
[[spt_procurement_status_rules.conditions.checks]]
type = "erm_gt_closing"
```

**規則結構**:
- `priority`: 優先順序（數字越小優先級越高）
- `status_value`: 狀態值
- `note`: 規則說明
- `combine`: 條件組合方式（"and" 或 "or"）
- `checks`: 檢查條件陣列

---

## 4. Pipeline 步驟

### 4.1 SPTProcurementDataLoadingStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_procurement_loading.py`

**功能**:
- 繼承自 `BaseLoadingStep`
- 載入 PO 原始資料
- 載入採購前期底稿（PO sheet）

**關鍵實作**:
```python
class SPTProcurementDataLoadingStep(BaseLoadingStep):
    def get_required_file_type(self) -> str:
        return 'raw_po'

    async def _load_primary_file(self, source, path: str) -> pd.DataFrame:
        df = await source.read()
        return df

    async def _load_reference_data(self, context: ProcessingContext) -> int:
        if 'procurement_previous' in self.file_paths:
            params = self.file_paths['procurement_previous'].get('params', {}).copy()
            params['sheet_name'] = 'PO'  # 指定 PO sheet
            procurement_prev = await self._load_reference_file_with_params(
                'procurement_previous', params
            )
            context.set_auxiliary_data('procurement_previous', procurement_prev)
            return 1
        return 0
```

### 4.2 SPTProcurementPRDataLoadingStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_procurement_loading.py`

**功能**:
- 繼承自 `BaseLoadingStep`
- 載入 PR 原始資料
- 載入採購前期底稿（PR sheet）

**與 PO 的差異**:
- `get_required_file_type()` 返回 `'raw_pr'`
- 載入前期底稿時指定 `sheet_name = 'PR'`

### 4.3 ColumnInitializationStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_column_initialization.py`

**功能**:
- 初始化狀態欄位（PO狀態/PR狀態）
- 確保欄位存在於 DataFrame 中

**為何需要此步驟**:
- `SPTProcurementStatusEvaluationStep` 需要狀態欄位存在
- 避免 KeyError

**關鍵實作**:
```python
class ColumnInitializationStep(PipelineStep):
    def __init__(self, status_column: str = "PO狀態", **kwargs):
        super().__init__(name="ColumnInitialization", **kwargs)
        self.status_column = status_column

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()

        if self.status_column not in df.columns:
            df[self.status_column] = pd.NA
            self.logger.info(f"Created empty column: {self.status_column}")

        context.update_data(df)
        return StepResult(...)
```

### 4.4 ProcurementPreviousMappingStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_procurement_mapping.py`

**功能**:
- 配置驅動的前期底稿映射
- 使用 `ColumnResolver` 解析欄位名稱
- 自動判斷 PO/PR 處理類型

**關鍵實作**:
```python
class ProcurementPreviousMappingStep(PipelineStep):
    def _load_mapping_config(self) -> None:
        config = config_manager._config_toml.get('spt_procurement_previous_mapping', {})
        self.po_mappings = config.get('po_mappings', {}).get('fields', [])
        self.pr_mappings = config.get('pr_mappings', {}).get('fields', [])

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()
        prev_df = context.get_auxiliary_data('procurement_previous')

        # 判斷處理類型
        is_po = 'PO#' in df.columns
        key_type = 'po' if is_po else 'pr'
        mappings = self.po_mappings if is_po else self.pr_mappings

        # 應用配置驅動映射
        df = self._apply_field_mappings(df, prev_df, mappings, key_type)

        context.update_data(df)
        return StepResult(...)

    def _apply_field_mappings(self, df, source_df, mappings, key_type):
        # 使用 ColumnResolver 解析鍵值欄位
        key_col_canonical = f'{key_type}_line'
        df_key = ColumnResolver.resolve(df, key_col_canonical)
        source_key = ColumnResolver.resolve(source_df, key_col_canonical)

        for mapping in mappings:
            source_col = ColumnResolver.resolve(source_df, mapping['source'])
            mapping_dict = create_mapping_dict(source_df, source_key, source_col)
            df[mapping['target']] = df[df_key].map(mapping_dict).fillna(pd.NA)

        return df
```

**設計模式**: 參考 `PreviousWorkpaperIntegrationStep` 的配置驅動設計

### 4.5 SPTProcurementStatusEvaluationStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_procurement_evaluation.py`

**功能**:
- 完全配置驅動的狀態判斷
- 支援 6 種條件類型
- 優先順序處理
- 條件組合邏輯（and/or）

**關鍵實作**:
```python
class SPTProcurementStatusEvaluationStep(PipelineStep):
    def _load_conditions_from_config(self):
        config = config_manager._config_toml.get('spt_procurement_status_rules', {})
        self.conditions = config.get('conditions', [])
        # 按 priority 排序
        self.conditions = sorted(self.conditions, key=lambda x: x.get('priority', 999))

    async def execute(self, context: ProcessingContext) -> StepResult:
        df = context.data.copy()
        file_date = context.get_variable('file_date')

        # 準備 ERM 相關欄位
        erm_data = self._prepare_erm_data(df)

        # 按優先順序應用每個條件
        for condition in self.conditions:
            df = self._apply_condition(df, condition, erm_data, file_date)

        context.update_data(df)
        return StepResult(...)

    def _evaluate_check(self, df, check, erm_data, file_date) -> pd.Series:
        check_type = check.get('type')

        if check_type == 'contains':
            return df[field].str.contains(pattern, na=False, regex=True)
        elif check_type == 'equals':
            return df[field] == value
        elif check_type == 'not_equals':
            return df[field] != value
        elif check_type == 'erm_in_range':
            return erm_data['erm'].between(erm_data['ym_start'], erm_data['ym_end'])
        elif check_type == 'erm_le_closing':
            return erm_data['erm'] <= file_date
        elif check_type == 'erm_gt_closing':
            return erm_data['erm'] > file_date

        return None
```

**擴充性**: 新增條件類型只需：
1. 在 `_evaluate_check()` 新增 elif 分支
2. 在 `stagging.toml` 新增條件規則

---

## 5. Orchestrator 整合

### 5.1 SPTPipelineOrchestrator 修改

**檔案**: `accrual_bot/tasks/spt/pipeline_orchestrator.py`

**新增方法**:

```python
def build_procurement_pipeline(
    self,
    file_paths: Dict[str, Any],
    source_type: str = 'PO',
    custom_steps: Optional[List[PipelineStep]] = None
) -> Pipeline:
    """
    構建 SPT 採購處理 pipeline

    Args:
        file_paths: 檔案路徑配置
        source_type: 處理類型 ('PO', 'PR', 'COMBINED')

    Returns:
        Pipeline: 配置好的 pipeline
    """
    pipeline_config = PipelineConfig(
        name=f"SPT_PROCUREMENT_{source_type}_Processing",
        description=f"SPT Procurement {source_type} processing pipeline",
        entity_type=self.entity_type,
        stop_on_error=True
    )

    pipeline = Pipeline(pipeline_config)

    if source_type == 'COMBINED':
        raise NotImplementedError("COMBINED mode not yet implemented")

    # 載入啟用的步驟
    if source_type == 'PO':
        enabled_steps = self.config.get('enabled_procurement_po_steps', [])
    else:  # PR
        enabled_steps = self.config.get('enabled_procurement_pr_steps', [])

    for step_name in enabled_steps:
        step = self._create_step(step_name, file_paths,
                                processing_type='PROCUREMENT',
                                source_type=source_type)
        if step:
            pipeline.add_step(step)

    return pipeline
```

### 5.2 步驟註冊

**更新 `_create_step()` 方法**:

```python
def _create_step(
    self,
    step_name: str,
    file_paths: Dict[str, Any],
    processing_type: str = 'PO',
    source_type: str = None
) -> Optional[PipelineStep]:
    step_registry = {
        # ... 現有步驟 ...

        # PROCUREMENT 步驟
        'SPTProcurementDataLoading': lambda: SPTProcurementDataLoadingStep(
            name="SPTProcurementDataLoading",
            file_paths=file_paths
        ),
        'SPTProcurementPRDataLoading': lambda: SPTProcurementPRDataLoadingStep(
            name="SPTProcurementPRDataLoading",
            file_paths=file_paths
        ),
        'ColumnInitialization': lambda: ColumnInitializationStep(
            name="ColumnInitialization",
            status_column="PR狀態" if source_type == 'PR' else "PO狀態"
        ),
        'ProcurementPreviousMapping': lambda: ProcurementPreviousMappingStep(
            name="ProcurementPreviousMapping"
        ),
        'SPTProcurementStatusEvaluation': lambda: SPTProcurementStatusEvaluationStep(
            name="SPTProcurementStatusEvaluation",
            status_column="PR狀態" if source_type == 'PR' else "PO狀態"
        ),
        'SPTProcurementExport': lambda: SPXPRExportStep(
            name="SPTProcurementExport",
            output_dir="output",
            sheet_name=source_type if source_type else "PO",
            include_index=False,
            required=True,
            retry_count=0
        ),
    }

    step_factory = step_registry.get(step_name)
    return step_factory() if step_factory else None
```

### 5.3 啟用步驟查詢

**更新 `get_enabled_steps()` 方法**:

```python
def get_enabled_steps(self, processing_type: str = 'PO') -> List[str]:
    if processing_type == 'PO':
        return self.config.get('enabled_po_steps', [])
    elif processing_type == 'PR':
        return self.config.get('enabled_pr_steps', [])
    elif processing_type == 'PROCUREMENT':
        return self.config.get('enabled_procurement_po_steps', [])
    else:
        return []
```

---

## 6. UI 配置

### 6.1 ui/config.py 修改

**新增 PROCUREMENT 到 SPT types**:

```python
ENTITY_CONFIG: Dict[str, Dict] = {
    'SPT': {
        'display_name': 'SPT',
        'types': ['PO', 'PR', 'PROCUREMENT'],  # 新增 PROCUREMENT
        'description': 'SPT Platform for opened PR/PO',
        'icon': '🛒',
    },
    # ...
}
```

**新增處理類型配置**:

```python
PROCESSING_TYPE_CONFIG: Dict[str, Dict] = {
    # ... 現有配置 ...
    'PROCUREMENT': {
        'display_name': '採購審核 (PROCUREMENT)',
        'description': '採購人員專用處理流程，支援 PO/PR 單獨或合併處理',
        'icon': '📋',
    },
}
```

**新增檔案標籤**:

```python
FILE_LABELS: Dict[str, str] = {
    # ... 現有標籤 ...
    'procurement_previous': '採購前期底稿 (選填)',
}
```

**配置必填/選填檔案**:

```python
# 必要檔案 (彈性檢查 - 至少需要 raw_po 或 raw_pr)
REQUIRED_FILES: Dict[Tuple[str, str], List[str]] = {
    # ... 現有配置 ...
    ('SPT', 'PROCUREMENT'): [],
}

# 選填檔案
OPTIONAL_FILES: Dict[Tuple[str, str], List[str]] = {
    # ... 現有配置 ...
    ('SPT', 'PROCUREMENT'): ['raw_po', 'raw_pr', 'procurement_previous'],
}
```

### 6.2 unified_pipeline_service.py 修改

**新增 PROCUREMENT 路由**:

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
    elif proc_type == 'PROCUREMENT' and entity == 'SPT':
        # 判斷處理模式
        has_po = 'raw_po' in file_paths
        has_pr = 'raw_pr' in file_paths

        if has_po and has_pr:
            source_type = 'COMBINED'
        elif has_po:
            source_type = 'PO'
        elif has_pr:
            source_type = 'PR'
        else:
            raise ValueError("PROCUREMENT 需要至少提供 raw_po 或 raw_pr")

        return orchestrator.build_procurement_pipeline(
            enriched_file_paths,
            source_type=source_type
        )
    elif proc_type == 'PPE' and entity == 'SPX':
        if not processing_date:
            raise ValueError("PPE 處理需要提供 processing_date")
        return orchestrator.build_ppe_pipeline(enriched_file_paths, processing_date)
    else:
        raise ValueError(f"不支援的處理類型: {entity}/{proc_type}")
```

---

## 7. 使用方式

### 7.1 透過 UI 使用

**步驟 1: 配置**
1. 開啟 Streamlit UI: `streamlit run main_streamlit.py`
2. 進入「1️⃣ 配置」頁面
3. 選擇 Entity: `SPT`
4. 選擇 Processing Type: `PROCUREMENT`
5. 輸入處理日期 (YYYYMM)

**步驟 2: 上傳檔案**
1. 進入「2️⃣ 檔案上傳」頁面
2. 上傳 `raw_po` 或 `raw_pr` (至少一個)
3. 上傳 `procurement_previous` (選填但建議)

**步驟 3: 執行**
1. 進入「3️⃣ 執行」頁面
2. 點擊「開始執行」按鈕
3. 查看即時日誌與進度

**步驟 4: 查看結果**
1. 進入「4️⃣ 結果」頁面
2. 預覽處理後的資料
3. 下載 CSV 或 Excel 檔案

### 7.2 透過程式碼使用

**範例: PO 處理**

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext

# 建立 orchestrator
orchestrator = SPTPipelineOrchestrator()

# 定義檔案路徑
file_paths = {
    'raw_po': '/path/to/202512_purchase_order.csv',
    'procurement_previous': '/path/to/202511_採購底稿.xlsx'
}

# 建構 pipeline
pipeline = orchestrator.build_procurement_pipeline(
    file_paths=file_paths,
    source_type='PO'
)

# 建立 context
context = ProcessingContext()

# 執行 pipeline
result = await pipeline.execute(context)

# 查看結果
if result.success:
    print(f"Pipeline 執行成功: {result.message}")
    print(f"處理了 {len(context.data)} 筆資料")
else:
    print(f"Pipeline 執行失敗: {result.error}")
```

**範例: PR 處理**

```python
file_paths = {
    'raw_pr': '/path/to/202512_purchase_request.csv',
    'procurement_previous': '/path/to/202511_採購底稿.xlsx'
}

pipeline = orchestrator.build_procurement_pipeline(
    file_paths=file_paths,
    source_type='PR'
)

# 執行流程同上...
```

### 7.3 透過 UnifiedPipelineService 使用

```python
from accrual_bot.ui.services import UnifiedPipelineService

service = UnifiedPipelineService()

# 查詢可用類型
types = service.get_entity_types('SPT')
print(types)  # ['PO', 'PR', 'PROCUREMENT']

# 查詢啟用的步驟
steps = service.get_enabled_steps('SPT', 'PROCUREMENT')
print(steps)
# ['SPTProcurementDataLoading', 'ColumnInitialization', ...]

# 建構 pipeline
file_paths = {
    'raw_po': '/path/to/po.csv',
    'procurement_previous': '/path/to/previous.xlsx'
}

pipeline = service.build_pipeline(
    entity='SPT',
    proc_type='PROCUREMENT',
    file_paths=file_paths
)

# 執行 pipeline...
```

---

## 8. 測試驗證

### 8.1 語法檢查

```bash
# 檢查 Python 語法
python -m py_compile accrual_bot/tasks/spt/steps/spt_procurement_loading.py
python -m py_compile accrual_bot/tasks/spt/steps/spt_procurement_mapping.py
python -m py_compile accrual_bot/tasks/spt/steps/spt_procurement_evaluation.py
python -m py_compile accrual_bot/tasks/spt/steps/spt_column_initialization.py
python -m py_compile accrual_bot/tasks/spt/pipeline_orchestrator.py
python -m py_compile accrual_bot/ui/services/unified_pipeline_service.py
```

### 8.2 匯入測試

```python
# 測試新步驟匯入
from accrual_bot.tasks.spt.steps import (
    SPTProcurementDataLoadingStep,
    SPTProcurementPRDataLoadingStep,
    ProcurementPreviousMappingStep,
    SPTProcurementStatusEvaluationStep,
    ColumnInitializationStep,
)

print("✓ All imports successful")
```

### 8.3 配置載入測試

```python
from accrual_bot.utils.config import config_manager

# 測試條件規則載入
config = config_manager._config_toml.get('spt_procurement_status_rules', {})
conditions = config.get('conditions', [])
print(f"✓ Loaded {len(conditions)} conditions")  # 應為 6

# 測試映射配置載入
mapping_config = config_manager._config_toml.get('spt_procurement_previous_mapping', {})
po_mappings = mapping_config.get('po_mappings', {}).get('fields', [])
pr_mappings = mapping_config.get('pr_mappings', {}).get('fields', [])
print(f"✓ PO mappings: {len(po_mappings)}, PR mappings: {len(pr_mappings)}")
```

### 8.4 Pipeline 建構測試

```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator

orchestrator = SPTPipelineOrchestrator()

# 測試 PO 模式
file_paths = {'raw_po': '/tmp/test.csv', 'procurement_previous': '/tmp/test.xlsx'}
pipeline_po = orchestrator.build_procurement_pipeline(file_paths, source_type='PO')
print(f"✓ PO Pipeline: {len(pipeline_po.steps)} steps")

# 測試 PR 模式
file_paths = {'raw_pr': '/tmp/test.csv', 'procurement_previous': '/tmp/test.xlsx'}
pipeline_pr = orchestrator.build_procurement_pipeline(file_paths, source_type='PR')
print(f"✓ PR Pipeline: {len(pipeline_pr.steps)} steps")

# 測試 COMBINED 模式 (應拋出 NotImplementedError)
try:
    file_paths = {'raw_po': '/tmp/po.csv', 'raw_pr': '/tmp/pr.csv'}
    pipeline = orchestrator.build_procurement_pipeline(file_paths, source_type='COMBINED')
except NotImplementedError as e:
    print(f"✓ Expected error: {e}")
```

### 8.5 UI 配置測試

```python
from accrual_bot.ui.config import (
    ENTITY_CONFIG,
    PROCESSING_TYPE_CONFIG,
    REQUIRED_FILES,
    OPTIONAL_FILES,
    FILE_LABELS
)

# 驗證配置
assert 'PROCUREMENT' in ENTITY_CONFIG['SPT']['types']
assert 'PROCUREMENT' in PROCESSING_TYPE_CONFIG
assert ('SPT', 'PROCUREMENT') in REQUIRED_FILES
assert ('SPT', 'PROCUREMENT') in OPTIONAL_FILES
assert 'procurement_previous' in FILE_LABELS

print("✓ All UI config checks passed")
```

### 8.6 整合測試建議

**測試案例 1: PO 完整流程**
```python
import asyncio
from accrual_bot.tasks.spt import SPTPipelineOrchestrator
from accrual_bot.core.pipeline.context import ProcessingContext

async def test_po_pipeline():
    orchestrator = SPTPipelineOrchestrator()

    file_paths = {
        'raw_po': '/path/to/real/202512_purchase_order.csv',
        'procurement_previous': '/path/to/real/202511_採購底稿.xlsx'
    }

    pipeline = orchestrator.build_procurement_pipeline(file_paths, source_type='PO')
    context = ProcessingContext()

    result = await pipeline.execute(context)

    assert result.success, f"Pipeline failed: {result.error}"
    assert 'PO狀態' in context.data.columns
    assert context.data['PO狀態'].notna().sum() > 0

    print(f"✓ PO Pipeline: Processed {len(context.data)} rows")
    print(f"  Status distribution: {context.data['PO狀態'].value_counts().to_dict()}")

asyncio.run(test_po_pipeline())
```

**測試案例 2: PR 完整流程**
```python
async def test_pr_pipeline():
    orchestrator = SPTPipelineOrchestrator()

    file_paths = {
        'raw_pr': '/path/to/real/202512_purchase_request.csv',
        'procurement_previous': '/path/to/real/202511_採購底稿.xlsx'
    }

    pipeline = orchestrator.build_procurement_pipeline(file_paths, source_type='PR')
    context = ProcessingContext()

    result = await pipeline.execute(context)

    assert result.success
    assert 'PR狀態' in context.data.columns

    print(f"✓ PR Pipeline: Processed {len(context.data)} rows")

asyncio.run(test_pr_pipeline())
```

---

## 9. 已知限制

### 9.1 COMBINED 模式未實作

**現狀**:
- 同時上傳 `raw_po` 和 `raw_pr` 會拋出 `NotImplementedError`
- 無法產生包含兩個 sheets 的合併 Excel 檔案

**影響**:
- 使用者需要分別處理 PO 和 PR
- 無法一次性輸出合併檔案

**解決方案** (未來實作):
1. 建立 `CombinedProcurementProcessingStep` 類別
2. 在該步驟中依序執行 PO 和 PR 處理
3. 合併結果為單一 Excel 檔案（兩個 sheets）
4. 更新 `build_procurement_pipeline()` 移除 NotImplementedError

### 9.2 前期底稿格式限制

**要求**:
- 必須是 Excel 檔案 (.xlsx)
- 必須包含 `PO` 和 `PR` 兩個 sheets
- 必須包含以下欄位:
  - `PO Line` 或 `PO_Line` (PO sheet)
  - `PR Line` 或 `PR_Line` (PR sheet)
  - `Remarked by Procurement` 或類似名稱

**建議**:
- 提供範本檔案
- 新增檔案格式驗證步驟

### 9.3 DateLogicStep 依賴

**依賴**:
- PROCUREMENT 處理依賴 `DateLogicStep` 輸出以下欄位:
  - `YMs of Item Description`: 日期範圍（格式: YYYYMM-YYYYMM）
  - `Expected Received Month_轉換格式`: ERM 轉換後格式（Int32）

**風險**:
- 若 `DateLogicStep` 未啟用或執行失敗，狀態判斷會出錯

**緩解措施**:
- 在配置中確保 `DateLogic` 在 `SPTProcurementStatusEvaluation` 之前執行
- 新增輸入驗證檢查必要欄位

### 9.4 錯誤處理

**現狀**:
- 條件評估失敗時會記錄警告但繼續執行
- 映射失敗時可能導致空值

**建議**:
- 新增嚴格模式開關
- 提供詳細的錯誤報告

---

## 10. 附錄

### 10.1 檔案清單

| 操作 | 檔案路徑 | 行數 | 狀態 |
|------|----------|------|------|
| 修改 | `accrual_bot/config/paths.toml` | +12 | ✅ |
| 修改 | `accrual_bot/config/stagging.toml` | +120 | ✅ |
| 新增 | `accrual_bot/tasks/spt/steps/spt_procurement_loading.py` | 134 | ✅ |
| 新增 | `accrual_bot/tasks/spt/steps/spt_procurement_mapping.py` | 203 | ✅ |
| 新增 | `accrual_bot/tasks/spt/steps/spt_procurement_evaluation.py` | 302 | ✅ |
| 新增 | `accrual_bot/tasks/spt/steps/spt_column_initialization.py` | 78 | ✅ |
| 修改 | `accrual_bot/tasks/spt/steps/__init__.py` | +9 | ✅ |
| 修改 | `accrual_bot/tasks/spt/pipeline_orchestrator.py` | +90 | ✅ |
| 修改 | `accrual_bot/ui/config.py` | +15 | ✅ |
| 修改 | `accrual_bot/ui/services/unified_pipeline_service.py` | +20 | ✅ |

**統計**:
- 新增檔案: 4 個
- 修改檔案: 6 個
- 新增程式碼: ~717 行
- 修改程式碼: ~266 行
- 總計: ~983 行

### 10.2 核心類別與方法

**新增類別**:
1. `SPTProcurementDataLoadingStep`
2. `SPTProcurementPRDataLoadingStep`
3. `ProcurementPreviousMappingStep`
4. `SPTProcurementStatusEvaluationStep`
5. `ColumnInitializationStep`

**新增方法**:
1. `SPTPipelineOrchestrator.build_procurement_pipeline()`
2. `SPTProcurementStatusEvaluationStep._load_conditions_from_config()`
3. `SPTProcurementStatusEvaluationStep._prepare_erm_data()`
4. `SPTProcurementStatusEvaluationStep._apply_condition()`
5. `SPTProcurementStatusEvaluationStep._evaluate_check()`
6. `ProcurementPreviousMappingStep._load_mapping_config()`
7. `ProcurementPreviousMappingStep._apply_field_mappings()`
8. `ProcurementPreviousMappingStep._apply_single_mapping()`

### 10.3 配置區段

**paths.toml**:
- `[spt.procurement]`
- `[spt.procurement.params]`

**stagging.toml**:
- `[pipeline.spt]` (新增 `enabled_procurement_po_steps`, `enabled_procurement_pr_steps`)
- `[spt_procurement_previous_mapping]`
- `[spt_procurement_previous_mapping.column_patterns]`
- `[[spt_procurement_previous_mapping.po_mappings.fields]]`
- `[[spt_procurement_previous_mapping.pr_mappings.fields]]`
- `[[spt_procurement_status_rules.conditions]]` (6 個條件)

### 10.4 依賴關係

**外部依賴**:
- `pandas`: DataFrame 操作
- `asyncio`: 異步執行

**內部依賴**:
- `accrual_bot.core.pipeline.base`: PipelineStep, StepResult, StepStatus
- `accrual_bot.core.pipeline.context`: ProcessingContext
- `accrual_bot.core.pipeline.steps.base_loading`: BaseLoadingStep
- `accrual_bot.core.pipeline.steps.common`: DateLogicStep
- `accrual_bot.utils.config`: config_manager
- `accrual_bot.utils.helpers.data_utils`: create_mapping_dict
- `accrual_bot.utils.helpers.column_utils`: ColumnResolver
- `accrual_bot.tasks.spx.steps`: SPXPRExportStep

### 10.5 設計模式

**1. Template Method Pattern**:
- `BaseLoadingStep`: 定義載入流程框架
- `SPTProcurementDataLoadingStep`: 實作 PO 專屬邏輯
- `SPTProcurementPRDataLoadingStep`: 實作 PR 專屬邏輯

**2. Strategy Pattern**:
- `_evaluate_check()`: 根據條件類型選擇評估策略
- 條件組合 (and/or): 根據 combine 參數選擇組合策略

**3. Factory Pattern**:
- `_create_step()`: 根據步驟名稱建立步驟實例

**4. Configuration-Driven Design**:
- 所有業務規則定義在 TOML 配置檔
- 支援無程式碼修改的擴充

### 10.6 參考資料

**相關文檔**:
- [UI Architecture](UI_Architecture.md)
- [Task Pipeline Structure Unit Test Plan](Task%20Pipeline%20Structure%20Unit%20Test%20Plan.md)
- [CLAUDE.md](../CLAUDE.md)

**程式碼參考**:
- `accrual_bot/core/pipeline/steps/base_loading.py`: 載入步驟範本
- `accrual_bot/core/pipeline/steps/spx_integration.py`: PreviousWorkpaperIntegrationStep
- `accrual_bot/tasks/spx/pipeline_orchestrator.py`: SPX Orchestrator 範例

**配置參考**:
- `config/paths.toml`: 檔案路徑配置
- `config/stagging.toml`: Pipeline 與業務規則配置

---

## 結語

SPT PROCUREMENT 任務已成功實作，提供了完全配置驅動的靈活架構。透過配置檔案即可新增條件規則與映射欄位，無需修改程式碼。目前支援 PO 和 PR 單獨處理模式，COMBINED 合併模式可於未來根據需求實作。

**核心優勢**:
- ✅ 完全配置驅動
- ✅ 高度可擴充
- ✅ 易於維護
- ✅ 符合專案架構規範

**後續建議**:
- 實作 COMBINED 模式
- 新增單元測試
- 新增整合測試
- 提供前期底稿範本

---

**文檔版本**: 2.0.0
**最後更新**: 2026-01-18
**維護者**: Development Team


## 11. v2.0.0 新功能詳解

### 11.1 COMBINED 模式概述

v2.0.0 實作了完整的 COMBINED 合併處理模式，允許同時處理 PO 和 PR 資料並輸出到單一 Excel 檔案的兩個 sheets。

**處理流程**:
```
原始資料:
  - raw_po.csv
  - raw_pr.csv  
  - procurement_previous.xlsx (含 PO 和 PR sheets)
       ↓
CombinedProcurementDataLoadingStep
  - 載入 PO 資料 → auxiliary_data["po_data"]
  - 載入 PR 資料 → auxiliary_data["pr_data"]
  - 載入前期 PO → auxiliary_data["procurement_previous_po"]
  - 載入前期 PR → auxiliary_data["procurement_previous_pr"]
       ↓
ProcurementPreviousValidationStep
  - 驗證 Excel 格式
  - 驗證 PO/PR sheets 存在
  - 驗證必要欄位（使用 ColumnResolver）
       ↓
CombinedProcurementProcessingStep
  - 處理 PO: 欄位初始化 → 前期映射 → 日期邏輯 → 狀態判斷
  - 處理 PR: 欄位初始化 → 前期映射 → 日期邏輯 → 狀態判斷
  - PO 結果 → auxiliary_data["po_result"]
  - PR 結果 → auxiliary_data["pr_result"]
       ↓
CombinedProcurementExportStep
  - 從 auxiliary_data 讀取 po_result 和 pr_result
  - 輸出到 {YYYYMM}_PROCUREMENT_COMBINED.xlsx
    - PO sheet
    - PR sheet
```

### 11.2 新增步驟詳解

#### 11.2.1 ProcurementPreviousValidationStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_procurement_validation.py`

**功能**:
- 驗證前期底稿為 Excel 檔案 (.xlsx 或 .xls)
- 驗證包含 PO 和 PR 兩個 sheets
- 使用 ColumnResolver 驗證必要欄位存在
- 支援嚴格/寬鬆模式

**關鍵特性**:
```python
class ProcurementPreviousValidationStep(PipelineStep):
    def __init__(self, strict_mode: bool = False, **kwargs):
        # strict_mode=False: 驗證失敗返回 SKIPPED（不中斷）
        # strict_mode=True:  驗證失敗返回 FAILED（中斷）
```

**驗證項目**:
1. 檔案格式: .xlsx 或 .xls
2. PO sheet 存在性
3. PR sheet 存在性
4. PO sheet 必要欄位: `po_line`, `remarked_by_procurement`
5. PR sheet 必要欄位: `pr_line`, `remarked_by_procurement`

**驗證報告範例**:
```
============================================================
Procurement Previous Workpaper Validation Report
============================================================
File Format: ✓ Valid
PO Sheet:    ✓ Exists
PR Sheet:    ✓ Exists
PO Columns:  ✓ Valid
PR Columns:  ✓ Valid
============================================================
```

#### 11.2.2 CombinedProcurementDataLoadingStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_combined_procurement_loading.py`

**功能**:
- 同時載入 raw_po 和 raw_pr 資料
- 載入前期底稿的 PO 和 PR sheets
- 將資料存儲到 auxiliary_data

**資料存儲結構**:
```python
context.set_auxiliary_data("po_data", po_df)                     # PO 原始資料
context.set_auxiliary_data("pr_data", pr_df)                     # PR 原始資料
context.set_auxiliary_data("procurement_previous_po", prev_po)   # 前期 PO
context.set_auxiliary_data("procurement_previous_pr", prev_pr)   # 前期 PR
context.set_variable("file_date", file_date)                     # 結帳月份
context.set_variable("procurement_previous_path", file_path)     # 檔案路徑
```

**載入摘要範例**:
```
============================================================
Combined Procurement Data Loading Summary
============================================================
File Date: 202512

PO Data:     ✓ Loaded
  - Rows: 1234
PR Data:     ✓ Loaded
  - Rows: 567

PO Previous: ✓ Loaded
PR Previous: ✓ Loaded
============================================================
```

#### 11.2.3 CombinedProcurementProcessingStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_combined_procurement_processing.py`

**功能**:
- 使用獨立 sub-context 分別處理 PO 和 PR
- 複用現有處理步驟（ColumnInitializationStep、ProcurementPreviousMappingStep 等）
- 避免 PO 和 PR 處理互相干擾

**處理架構**:
```python
async def _process_po_data(self, parent_context, po_data):
    # 創建獨立 sub-context
    sub_context = ProcessingContext()
    sub_context.update_data(po_data.copy())
    
    # 複製必要變數和前期資料
    sub_context.set_variable("file_date", parent_context.get_variable("file_date"))
    sub_context.set_auxiliary_data("procurement_previous", 
                                    parent_context.get_auxiliary_data("procurement_previous_po"))
    
    # 執行處理步驟
    steps = [
        ColumnInitializationStep(status_column="PO狀態"),
        ProcurementPreviousMappingStep(),
        DateLogicStep(),
        SPTProcurementStatusEvaluationStep(status_column="PO狀態"),
    ]
    
    for step in steps:
        result = await step.execute(sub_context)
        if result.status == StepStatus.FAILED:
            return None
    
    return sub_context.data
```

**處理摘要範例**:
```
============================================================
Combined Procurement Processing Summary
============================================================
PO Processing: ✓ Success
  - Final rows: 1234
  - Status distribution:
    - Affiliate: 10
    - FN Payroll: 5
    - 已完成: 800
    - 未完成: 419
PR Processing: ✓ Success
  - Final rows: 567
  - Status distribution:
    - 已完成: 400
    - 未完成: 167
============================================================
```

#### 11.2.4 CombinedProcurementExportStep

**檔案**: `accrual_bot/tasks/spt/steps/spt_combined_procurement_export.py`

**功能**:
- 從 auxiliary_data 讀取 po_result 和 pr_result
- 使用 openpyxl 輸出到單一 Excel 檔案
- 支援重試機制（預設 3 次）

**輸出結構**:
```
{YYYYMM}_PROCUREMENT_COMBINED.xlsx
├── PO (sheet)
│   ├── PO#, PO Line, Item Description, PO狀態, ...
└── PR (sheet)
    ├── PR#, PR Line, Item Description, PR狀態, ...
```

**匯出摘要範例**:
```
============================================================
Combined Procurement Export Summary
============================================================
Output Path: output/202512_PROCUREMENT_COMBINED.xlsx
File Size:   245.67 KB

PO Sheet:    ✓ Exported
  - Rows: 1234
PR Sheet:    ✓ Exported
  - Rows: 567
============================================================
```

### 11.3 配置更新

#### 11.3.1 stagging.toml

新增 COMBINED 模式的步驟配置:

```toml
[pipeline.spt]
enabled_procurement_combined_steps = [
    "CombinedProcurementDataLoading",
    "ProcurementPreviousValidation",
    "CombinedProcurementProcessing",
    "CombinedProcurementExport",
]
```

#### 11.3.2 pipeline_orchestrator.py

移除 NotImplementedError，實作 COMBINED 模式:

```python
if source_type == "COMBINED":
    enabled_steps = self.config.get("enabled_procurement_combined_steps", [])
    
    for step_name in enabled_steps:
        step = self._create_step(step_name, file_paths, 
                                processing_type="PROCUREMENT",
                                source_type="COMBINED")
        if step:
            pipeline.add_step(step)
    
    return pipeline
```

### 11.4 設計優勢

**1. 不影響現有功能**:
- 單一模式（PO、PR）的步驟完全不變
- COMBINED 模式使用獨立的步驟類別
- 向後相容性 100%

**2. 模組化設計**:
- 每個步驟職責單一
- 可獨立測試
- 易於維護和擴充

**3. 錯誤隔離**:
- PO 和 PR 使用獨立 sub-context
- 一方失敗不影響另一方
- 詳細的錯誤報告

**4. 靈活的驗證**:
- 支援嚴格/寬鬆模式
- 使用 ColumnResolver 靈活匹配欄位名稱
- 詳細的驗證報告

### 11.5 使用範例

**透過 UI 使用**:
1. 選擇 Entity: SPT
2. 選擇 Processing Type: PROCUREMENT
3. 上傳 raw_po.csv
4. 上傳 raw_pr.csv
5. 上傳 procurement_previous.xlsx（含 PO 和 PR sheets）
6. 執行 → 自動判斷為 COMBINED 模式
7. 下載 {YYYYMM}_PROCUREMENT_COMBINED.xlsx

**透過程式碼使用**:
```python
from accrual_bot.tasks.spt import SPTPipelineOrchestrator

orchestrator = SPTPipelineOrchestrator()

file_paths = {
    "raw_po": "/path/to/202512_purchase_order.csv",
    "raw_pr": "/path/to/202512_purchase_request.csv",
    "procurement_previous": "/path/to/202511_採購底稿.xlsx"
}

# 自動判斷為 COMBINED 模式
pipeline = orchestrator.build_procurement_pipeline(
    file_paths=file_paths,
    source_type="COMBINED"
)

context = ProcessingContext()
result = await pipeline.execute(context)

# 檢查結果
po_result = context.get_auxiliary_data("po_result")
pr_result = context.get_auxiliary_data("pr_result")
```

### 11.6 測試驗證

**語法檢查**: ✓ 通過
```bash
python3 -m py_compile accrual_bot/tasks/spt/steps/spt_procurement_validation.py
python3 -m py_compile accrual_bot/tasks/spt/steps/spt_combined_procurement_loading.py
python3 -m py_compile accrual_bot/tasks/spt/steps/spt_combined_procurement_processing.py
python3 -m py_compile accrual_bot/tasks/spt/steps/spt_combined_procurement_export.py
python3 -m py_compile accrual_bot/tasks/spt/pipeline_orchestrator.py
```

**建議測試案例**:
1. COMBINED 模式基本流程測試
2. 前期底稿格式驗證測試
3. PO/PR 分別處理正確性測試
4. 輸出檔案格式驗證
5. 錯誤處理測試（缺少檔案、格式錯誤等）

---


