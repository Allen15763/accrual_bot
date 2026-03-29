"""
Accrual Bot Streamlit UI - Main Entry Point

啟動 Streamlit UI 的主進入點。

Usage:
    streamlit run main_streamlit.py
"""

import streamlit as st
import sys
from pathlib import Path

# 加入專案根目錄到 path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state, get_navigation_status
from accrual_bot.ui.config import PAGE_CONFIG
from accrual_bot.ui.utils.ui_helpers import format_date

# 頁面設定
st.set_page_config(**PAGE_CONFIG)

# 初始化 session state
init_session_state()

# ===== Sidebar =====
with st.sidebar:
    st.title("📊 Accrual Bot")
    st.markdown("---")

    # 當前配置摘要
    st.header("當前配置")

    config = st.session_state.pipeline_config

    if config.entity:
        st.success(f"**實體:** {config.entity}")
    else:
        st.info("**實體:** 未選擇")

    if config.processing_type:
        st.success(f"**類型:** {config.processing_type}")
    else:
        st.info("**類型:** 未選擇")

    if config.processing_date > 0:
        st.success(f"**日期:** {format_date(config.processing_date)}")
    else:
        st.info("**日期:** 未設定")

    if config.enabled_steps:
        st.success(f"**步驟數:** {len(config.enabled_steps)}")
    else:
        st.info("**步驟數:** -")

    st.markdown("---")

    # 導航狀態
    st.header("導航")
    nav_status = get_navigation_status()

    pages = [
        ("⚙️ 配置", "configuration", "pages/1_⚙️_配置.py"),
        ("📁 檔案上傳", "file_upload", "pages/2_📁_檔案上傳.py"),
        ("▶️ 執行", "execution", "pages/3_▶️_執行.py"),
        ("📊 結果", "results", "pages/4_📊_結果.py"),
        ("💾 Checkpoint", "checkpoint", "pages/5_💾_Checkpoint.py"),
    ]

    for icon_name, key, page_path in pages:
        enabled = nav_status.get(key, False)
        button_type = "primary" if enabled else "secondary"

        if st.button(
            icon_name,
            key=f"nav_{key}",
            disabled=not enabled,
            use_container_width=True,
            type=button_type if enabled else "secondary"
        ):
            st.switch_page(page_path)

    st.markdown("---")

    # 操作按鈕
    st.header("操作")

    if st.button("🔄 重置所有設定", use_container_width=True):
        from accrual_bot.ui.app import reset_session_state
        reset_session_state()
        st.rerun()

    # 版本資訊
    st.markdown("---")
    st.caption("Accrual Bot UI v0.1.0")
    st.caption("Powered by Streamlit")

# ===== Main Content =====
st.title("Dev Accrual Bot")

st.markdown("""
## 📊 PO/PR/etc 處理系統

取代未結機器人，用於配置和執行 PO (Purchase Order)、PR (Purchase Request) 和 其他任務 的自動化處理流程。

### 🚀 使用步驟

1. **⚙️ 配置** - 選擇處理實體、類型和日期
2. **📁 檔案上傳** - 上傳所需的數據檔案
3. **▶️ 執行** - 監控 pipeline 執行進度
4. **📊 結果** - 查看處理結果並匯出
5. ~~**💾 Checkpoint** - 管理執行中斷點 (可選)~~

### 📋 支援的實體

- **SPT**
  - 支援 PO 和 PR 處理

- **SPX**
  - 支援 PO、PR 和 PPE 處理

### 💡 快速開始

點擊左側 Sidebar 的 **「⚙️ 配置」** 按鈕開始設定您的第一個處理流程。

---

### 📖 使用說明

- 所有配置都會儲存在當前 session 中
- 檔案上傳後會暫存於臨時目錄
- Pipeline 執行時會即時顯示進度和日誌
- 執行結果可匯出為 CSV 或 Excel 格式
- 結束任務請依序關閉該瀏覽器分頁與終端(黑色的執行介面)
- ~~Checkpoint 功能允許從中斷點繼續執行（節省時間)~~

### ⚠️ 注意事項

- 請確保上傳的檔案格式正確（CSV 或 Excel）
- 必填檔案必須全部上傳才能開始執行
- 執行過程中請勿關閉瀏覽器視窗
- MOB 支援尚在開發中，暫時不可用

---

""")

# 快速操作按鈕
st.markdown("### 🎯 快速操作")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🆕 開始新的處理", type="primary", use_container_width=True):
        from accrual_bot.ui.app import reset_session_state
        reset_session_state()
        st.switch_page("pages/1_⚙️_配置.py")

with col2:
    if nav_status.get('execution', False):
        if st.button("▶️ 繼續執行", use_container_width=True):
            st.switch_page("pages/3_▶️_執行.py")
    else:
        st.button("▶️ 繼續執行", disabled=True, use_container_width=True)

with col3:
    if nav_status.get('results', False):
        if st.button("📊 查看結果", use_container_width=True):
            st.switch_page("pages/4_📊_結果.py")
    else:
        st.button("📊 查看結果", disabled=True, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>Accrual Bot UI | Developed with FBA using Streamlit</p>
</div>
""", unsafe_allow_html=True)
