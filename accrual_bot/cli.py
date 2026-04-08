"""
Accrual Bot CLI 入口

提供命令列介面：
- accrual-bot         啟動 Streamlit UI（預設）
- accrual-bot init    初始化工作區
- accrual-bot version 顯示版本
"""

import argparse
import io
import os
import shutil
import subprocess
import sys
from pathlib import Path

WORKSPACE_ENV = "ACCRUAL_BOT_WORKSPACE"

# Streamlit page stubs 映射：emoji 檔名 → 套件內實際模組名
_PAGE_STUBS = {
    "1_⚙️_配置.py": "1_configuration.py",
    "2_📁_檔案上傳.py": "2_file_upload.py",
    "3_▶️_執行.py": "3_execution.py",
    "4_📊_結果.py": "4_results.py",
    "5_💾_Checkpoint.py": "5_checkpoint.py",
}


def _safe_print(msg: str):
    """印出訊息，處理 Windows cp950 無法編碼 emoji 的問題"""
    try:
        print(msg)
    except UnicodeEncodeError:
        # 移除無法編碼的字元（emoji），保留中文
        sys.stdout.buffer.write((msg + "\n").encode("utf-8", errors="replace"))


def get_workspace() -> Path:
    """取得工作區路徑（環境變數優先，預設 ~/accrual-bot）"""
    ws = os.environ.get(WORKSPACE_ENV)
    return Path(ws) if ws else Path.home() / "accrual-bot"


def cmd_init(args=None):
    """初始化工作區目錄結構和配置範本"""
    ws = get_workspace()

    # 建立目錄
    for d in ["config", "secret", "output", "logs"]:
        (ws / d).mkdir(parents=True, exist_ok=True)

    # 從套件內複製配置範本
    from importlib.resources import files as pkg_files

    pkg_config = pkg_files("accrual_bot.config")

    # 複製範本配置
    templates = {
        "paths.local.toml.example": "paths.local.toml",
        "run_config.toml": "run_config.toml",
    }
    for src_name, dst_name in templates.items():
        target = ws / "config" / dst_name
        if not target.exists():
            src = pkg_config.joinpath(src_name)
            if src.is_file():
                shutil.copy(str(src), str(target))
                _safe_print(f"  建立 {target}")

    # 產生 Streamlit app scaffold
    _generate_streamlit_app(ws)

    _safe_print(f"\n工作區已初始化：{ws}")
    _safe_print(f"   config/   — 編輯 paths.local.toml 設定本機路徑")
    _safe_print(f"   secret/   — 放入 credentials.json")
    _safe_print(f"   output/   — Pipeline 輸出目錄")
    _safe_print(f"   logs/     — 日誌目錄")


def _generate_streamlit_app(ws: Path):
    """產生 Streamlit 入口和 emoji pages 到工作區"""
    app_dir = ws / "app"
    pages_dir = app_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    # 產生 main_streamlit.py
    main_app = app_dir / "main_streamlit.py"
    if not main_app.exists():
        main_app.write_text(
            '"""Accrual Bot Streamlit UI — 由 accrual-bot init 產生"""\n'
            "from importlib.resources import files\n"
            'page = files("accrual_bot.ui._streamlit_app").joinpath("main_streamlit.py")\n'
            'exec(compile(page.read_text(encoding="utf-8"), str(page), "exec"))\n',
            encoding="utf-8",
        )
        _safe_print(f"  建立 {main_app}")

    # 產生 page stubs
    for emoji_name, module_name in _PAGE_STUBS.items():
        stub_path = pages_dir / emoji_name
        if not stub_path.exists():
            stub_path.write_text(
                f'"""Page stub — 由 accrual-bot init 產生，載入套件內的 {module_name}"""\n'
                f"from importlib.resources import files\n"
                f'page = files("accrual_bot.ui.pages").joinpath("{module_name}")\n'
                f'exec(compile(page.read_text(encoding="utf-8"), str(page), "exec"))\n',
                encoding="utf-8",
            )
            _safe_print(f"  建立 {stub_path}")


def cmd_ui(args=None):
    """啟動 Streamlit UI"""
    try:
        import streamlit  # noqa: F401
    except ImportError:
        _safe_print("錯誤：需要安裝 UI 依賴。請執行：pip install 'accrual-bot[ui]'")
        sys.exit(1)

    ws = get_workspace()
    app_entry = ws / "app" / "main_streamlit.py"

    # 若工作區尚未初始化，自動執行 init
    if not app_entry.exists():
        _safe_print("工作區尚未初始化，正在執行 init...")
        cmd_init()

    os.environ[WORKSPACE_ENV] = str(ws)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(app_entry),
            "--server.headless=true",
        ]
    )


def cmd_version(args=None):
    """顯示版本資訊"""
    from accrual_bot import __version__

    _safe_print(f"accrual-bot v{__version__}")


def main():
    """CLI 主進入點"""
    parser = argparse.ArgumentParser(
        prog="accrual-bot",
        description="Accrual Bot - PO/PR 自動化處理系統",
    )
    subparsers = parser.add_subparsers(dest="command")

    # init
    subparsers.add_parser("init", help="初始化工作區")

    # ui（預設）
    subparsers.add_parser("ui", help="啟動 Streamlit UI")

    # version
    subparsers.add_parser("version", help="顯示版本")

    args = parser.parse_args()

    commands = {
        "init": cmd_init,
        "ui": cmd_ui,
        "version": cmd_version,
        None: cmd_ui,  # 預設啟動 UI
    }

    handler = commands.get(args.command, cmd_ui)
    handler(args)


if __name__ == "__main__":
    main()
