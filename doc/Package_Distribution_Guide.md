# Accrual Bot — pip install 套件化分發指南

> **文件日期**：2026-03-29
> **適用版本**：v2.0.0+
> **GitHub**：`git@github.com:Allen15763/accrual_bot.git`

---

## 目錄

1. [架構概覽](#1-架構概覽)
2. [使用者安裝流程](#2-使用者安裝流程)
3. [腳本說明](#3-腳本說明)
4. [CLI 入口](#4-cli-入口)
5. [工作區結構](#5-工作區結構)
6. [配置覆蓋機制](#6-配置覆蓋機制)
7. [Credentials 管理](#7-credentials-管理)
8. [Streamlit Pages 解決方案](#8-streamlit-pages-解決方案)
9. [打包機制](#9-打包機制)
10. [開發者注意事項](#10-開發者注意事項)
11. [已知限制](#11-已知限制)
12. [故障排除](#12-故障排除)

---

## 1. 架構概覽

### 分發模型變遷

| | 舊方案 | 新方案 |
|---|--------|--------|
| **流程** | 複製 code 到 Embedded Python → 上傳 Google Drive ~500MB → 使用者下載 → bat 啟動 | 使用者雙擊 `install.bat` → pip 從 GitHub 安裝 → `run.bat` 啟動 |
| **更新** | 重新打包上傳，使用者重新下載 | 雙擊 `update.bat`（幾秒完成） |
| **體積** | ~500MB（含所有依賴） | ~50MB（首次下載依賴，之後增量更新） |

### 元件關係

```
scripts/install.bat ──→ pip install "accrual-bot @ git+..." ──→ accrual-bot init
                                                                      │
scripts/run.bat ────→ accrual-bot (cli.py:cmd_ui) ──→ streamlit run  │
                           │                                          │
                     設定 ACCRUAL_BOT_WORKSPACE                       │
                     複製 credentials.json                            │
                                                                      ▼
                                                               workspace/
                                                               ├── config/
                                                               ├── secret/
                                                               ├── output/
                                                               └── app/ (Streamlit pages)
```

---

## 2. 使用者安裝流程

### 前置條件

- Windows 10/11
- 網路連線（存取 GitHub 和 PyPI）
- Git for Windows（pip clone 需要）
- G: drive 掛載（Google 共用雲端硬碟，credentials 和部分資料來源）

### 步驟

1. 取得三個腳本（`install.bat`、`run.bat`、`update.bat`），放到安裝目錄（如 `D:\AccrualBot\`）
2. 雙擊 `install.bat`
3. 編輯 `workspace\config\paths.local.toml`，設定本機 resources 路徑
4. 雙擊 `run.bat` 啟動 Streamlit UI

### install.bat 的兩條路線

```
偵測系統 Python
├── 有 Python ≥ 3.11 → 建立 venv → pip install → accrual-bot init
└── 無 Python         → 下載 Embedded Python 3.11.9 (amd64)
                         → 修改 python311._pth（取消 #import site、加 Lib\site-packages）
                         → get-pip.py 安裝 pip
                         → pip install → accrual-bot init
```

---

## 3. 腳本說明

### `scripts/install.bat`

| 環境變數 | 用途 |
|----------|------|
| `PYTHONNOUSERSITE=1` | 禁止讀取使用者 site-packages（隔離環境） |
| `PYTHONDONTWRITEBYTECODE=1` | 不產生 .pyc |
| `ACCRUAL_BOT_WORKSPACE` | 指向 `%INSTALL_DIR%workspace` |

**Embedded Python 關鍵限制**：
- 無 `venv`、`ensurepip`、`tkinter` 模組
- `python311._pth` 控制 `sys.path`，必須取消 `#import site` 註解
- 直接安裝到 Embedded Python 的 `Lib/site-packages`（不建 venv）

### `scripts/run.bat`

- 設定 `ACCRUAL_BOT_WORKSPACE` 環境變數
- **自動從 G: drive 複製 credentials.json** 到 `workspace\secret\`（每次啟動同步）
- 自動偵測 venv 或 Embedded Python 安裝

### `scripts/update.bat`

- `pip install --upgrade --force-reinstall --no-deps --no-cache-dir`
- `--force-reinstall`：繞過版本號比對（git URL 安裝時版本號不變）
- `--no-deps`：只重裝 accrual-bot，不動 pandas/streamlit 等依賴
- `--no-cache-dir`：重新 git clone（不用舊快取）

---

## 4. CLI 入口

**檔案**：`accrual_bot/cli.py`

**Entry point**（`pyproject.toml`）：
```toml
[project.scripts]
accrual-bot = "accrual_bot.cli:main"
```

| 命令 | 功能 |
|------|------|
| `accrual-bot` | 預設執行 `ui`，啟動 Streamlit |
| `accrual-bot init` | 初始化工作區 |
| `accrual-bot version` | 顯示版本號 |

---

## 5. 工作區結構

`accrual-bot init` 建立的目錄：

```
workspace/
├── config/
│   ├── paths.local.toml      ← 使用者必須編輯（設定 resources 路徑）
│   └── run_config.toml       ← 執行配置（processing_date 等）
├── secret/
│   └── credentials.json      ← run.bat 自動從 G: drive 複製
├── output/                   ← Pipeline 輸出
├── logs/                     ← 日誌
└── app/
    ├── main_streamlit.py     ← Streamlit 入口（thin wrapper）
    └── pages/
        ├── 1_⚙️_配置.py
        ├── 2_📁_檔案上傳.py
        ├── 3_▶️_執行.py
        ├── 4_📊_結果.py
        └── 5_💾_Checkpoint.py
```

---

## 6. 配置覆蓋機制

### 優先順序

```
ACCRUAL_BOT_WORKSPACE/config/paths.local.toml   ← 最高（pip install 使用者）
accrual_bot/config/paths.local.toml              ← 開發者本機覆蓋（gitignored）
accrual_bot/config/paths.toml                    ← 共享預設值（git tracked）
```

### paths.toml 不應修改

`paths.toml` 包含開發者的硬編碼路徑（`C:/SEA/...`、`G:/共用雲端硬碟/...`）。這些路徑只有開發者會用到。

pip install 使用者透過 `paths.local.toml` 設定自己的路徑：

```toml
# workspace/config/paths.local.toml
[base]
resources = "D:/MyLocalData/accrual"

[spx.ppe]
contract_filing_list = "G:/共用雲端硬碟/.../SPX租金合約歸檔清單及匯款狀態_marge1.xlsx"
```

### config_loader.py 的 get_config_dir()

```python
def get_config_dir() -> Path:
    workspace = os.environ.get("ACCRUAL_BOT_WORKSPACE")
    if workspace:
        ws_config = Path(workspace) / "config"
        if ws_config.is_dir():
            return ws_config  # pip install 使用者的工作區
    return Path(__file__).parent.parent / "config"  # 開發者的套件內 config
```

---

## 7. Credentials 管理

### 查找優先順序（`google_sheet_source.py:_resolve_credentials()`）

```
1. ACCRUAL_BOT_CREDENTIALS 環境變數     ← 直接指定路徑
2. ACCRUAL_BOT_WORKSPACE/secret/credentials.json  ← 工作區目錄
3. ./secret/credentials.json             ← 相對路徑 fallback（開發環境）
```

### 為何用 auto-copy 而非 env var

`chcp 65001`（UTF-8 codepage）下，batch 的 `set` 命令中的中文字元 byte 可能被 cmd.exe 誤解析，導致 Python 端 `os.environ.get()` 取得殘缺字串。

**解法**：`run.bat` 用 `copy` 命令（不受 codepage 影響）將 credentials 複製到 workspace/secret/，由 `_resolve_credentials()` 的路徑 2 命中。

---

## 8. Streamlit Pages 解決方案

### 問題

Streamlit 要求 `pages/` 目錄在 entry point（`main_streamlit.py`）旁邊，且檔名含 emoji。pip install 後，套件程式碼在 `site-packages/` 裡，無法滿足此要求。

### 解法

`accrual-bot init` 在工作區產生 thin wrapper：

**`app/main_streamlit.py`**：從 `accrual_bot.ui._streamlit_app` 載入實際邏輯。

**`app/pages/1_⚙️_配置.py`**（每個 page stub）：
```python
from importlib.resources import files as _files
_src = _files("accrual_bot.ui.pages").joinpath("1_configuration.py")
exec(compile(_src.read_text(encoding="utf-8"), str(_src), "exec"))
```

### 兩版 main_streamlit.py

| | 根目錄版（開發用） | `_streamlit_app/` 版（pip install 用） |
|---|---|---|
| 路徑處理 | `sys.path.insert(0, project_root)` | 不需要（已 pip install） |
| 版本顯示 | hard-coded | `from accrual_bot import __version__` |

---

## 9. 打包機制

### pyproject.toml 關鍵設定

```toml
[project.scripts]
accrual-bot = "accrual_bot.cli:main"

[tool.setuptools.packages.find]
include = ["accrual_bot*"]          # 所有子套件

[tool.setuptools.package-data]
"accrual_bot.config" = ["*.toml", "*.ini", "*.xlsx", "*.toml.example"]
"accrual_bot.ui._streamlit_app" = ["**/*.py"]
```

### MANIFEST.in

```
recursive-include accrual_bot/config *.toml *.ini *.xlsx *.example
recursive-include accrual_bot/ui/_streamlit_app *.py
```

控制 `python -m build` 產生的 sdist 包含哪些非 Python 檔案。

### .gitattributes

```
scripts/*.bat text eol=crlf
*.sh text eol=lf
```

確保 checkout 後 bat 檔是 CRLF（Windows cmd.exe 要求）、sh 檔是 LF（bash 要求）。

### ref*.xlsx 追蹤

`.gitignore` 有全域 `*.xlsx` 規則，但 ref 檔需要隨套件打包：
```gitignore
!accrual_bot/config/ref*.xlsx
```

---

## 10. 開發者注意事項

### 本地開發不受影響

所有 packaging 改動都是**純增量**：

| 改動 | 向後相容 | 說明 |
|------|----------|------|
| config_manager ZIP fallback → importlib | 是 | ZIP 路徑本來就不存在（歷史殘留） |
| google_sheet_source credentials 解析 | 是 | 原始 `resolve_flexible_path` 仍是最終 fallback |
| config_loader WORKSPACE env var | 是 | env var 未設定時 fallback 回原路徑 |
| constants.py REF_PATH 清空 | 是 | 無 production code 使用這兩個常數 |
| file_utils resolve_config_ref_path | 是 | 原始路徑存在時直接返回 |
| paths.toml | 未修改 | 開發者路徑保持原樣 |

### 開發模式安裝

```bash
pip install -e ".[dev]"    # editable install + dev dependencies
accrual-bot version        # 驗證 entry point
```

---

## 11. 已知限制

| 限制 | 說明 | 影響 |
|------|------|------|
| `chcp 65001` + 中文 env var | batch `set` 中的中文路徑可能被 cmd.exe 損壞 | 不影響（改用 auto-copy） |
| Embedded Python 無 venv | 直接安裝到 Embedded Python 目錄 | `PYTHONNOUSERSITE=1` 確保隔離 |
| pip 版本號比對 | git URL 安裝時版本號不變，需 `--force-reinstall` | update.bat 已處理 |
| `.gitattributes` renormalize | 首次加入後需 `git add --renormalize .` | 一次性操作，已完成 |

---

## 12. 故障排除

### install.bat 報 `was unexpected at this time`
batch 的 `if/for` 巢狀超過 2 層會崩。腳本已改用 `goto` 標籤避免。

### update.bat 沒拿到新 code
確認 `--force-reinstall --no-deps --no-cache-dir` 三個 flag 都有。

### Google Sheets 連線失敗
1. 確認 `run.bat` 裡 `CRED_SRC` 路徑指向正確的 `credentials.json`
2. 確認 G: drive 已掛載
3. 或手動複製 credentials.json 到 `workspace\secret\`

### Streamlit 頁面顯示空白
確認 `workspace\app\pages\` 下有 5 個 emoji 檔名的 `.py` 檔。如果遺失，重跑 `accrual-bot init`。
