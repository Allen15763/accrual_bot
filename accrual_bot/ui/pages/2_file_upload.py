"""
File Upload Page

檔案上傳頁面。
"""

import streamlit as st
import sys
from pathlib import Path

# 加入專案根目錄到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state, get_navigation_status
from accrual_bot.ui.components import render_file_uploader
from accrual_bot.ui.services import FileHandler

# 初始化 session state
init_session_state()

# 頁面設定
st.set_page_config(
    page_title="檔案上傳 | Accrual Bot",
    page_icon="📁",
    layout="wide"
)

st.title("📁 檔案上傳")
st.markdown("---")

# 檢查導航狀態
nav_status = get_navigation_status()
if not nav_status['file_upload']:
    st.warning("⚠️ 請先完成配置頁設定")
    if st.button("前往配置頁"):
        st.switch_page("pages/1_⚙️_配置.py")
    st.stop()

# 獲取配置
config = st.session_state.pipeline_config
entity = config.entity
proc_type = config.processing_type

# 顯示當前配置
st.info(f"📊 當前配置: **{entity} / {proc_type}** | 日期: **{config.processing_date}**")

# 初始化 FileHandler
if 'file_handler' not in st.session_state or st.session_state.temp_dir is None:
    file_handler = FileHandler()
    st.session_state.file_handler = file_handler
    st.session_state.temp_dir = file_handler.temp_dir
else:
    file_handler = st.session_state.file_handler

# 渲染檔案上傳器
file_paths = render_file_uploader(entity, proc_type, file_handler)

# 前往執行頁按鈕
st.markdown("---")
upload_state = st.session_state.file_upload

if upload_state.required_files_complete:
    st.success("✅ 所有必填檔案已上傳！可以開始執行。")

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("▶️ 開始執行", type="primary", use_container_width=True):
            st.switch_page("pages/3_▶️_執行.py")
    with col2:
        if st.button("🔄 重新配置", use_container_width=True):
            # 清理暫存檔案
            if hasattr(file_handler, 'cleanup'):
                file_handler.cleanup()
            from accrual_bot.ui.app import reset_session_state
            reset_session_state()
            st.switch_page("pages/1_⚙️_配置.py")
else:
    st.warning("⚠️ 請上傳所有必填檔案後才能開始執行")
