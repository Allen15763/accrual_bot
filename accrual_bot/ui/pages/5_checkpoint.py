"""
Checkpoint Management Page

Checkpoint 管理頁面。
"""

import streamlit as st
import sys
from pathlib import Path
import os
from datetime import datetime

# 加入專案根目錄到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state

# 初始化 session state
init_session_state()

# 頁面設定
st.set_page_config(
    page_title="Checkpoint 管理 | Accrual Bot",
    page_icon="💾",
    layout="wide"
)

st.title("💾 Checkpoint 管理")
st.markdown("---")

st.info("💡 Checkpoint 功能允許您從中斷點繼續執行 pipeline，節省重複處理的時間。")

# Checkpoint 目錄
checkpoint_dir = os.path.join(os.getcwd(), "checkpoints")

if not os.path.exists(checkpoint_dir):
    st.warning("📂 Checkpoint 目錄不存在")
    st.caption(f"目錄路徑: {checkpoint_dir}")
    st.stop()

# 掃描 checkpoint 檔案
checkpoint_files = []
for root, dirs, files in os.walk(checkpoint_dir):
    for file in files:
        if file.endswith('.pkl') or file.endswith('.json'):
            file_path = os.path.join(root, file)
            stat = os.stat(file_path)
            checkpoint_files.append({
                'name': file,
                'path': file_path,
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime),
                'size_mb': stat.st_size / (1024 * 1024)
            })

if not checkpoint_files:
    st.info("📭 目前沒有已儲存的 checkpoint")
    st.stop()

# 顯示 checkpoint 清單
st.subheader(f"📋 已儲存的 Checkpoint ({len(checkpoint_files)})")

# 排序選項
sort_by = st.radio(
    "排序方式",
    options=['最新', '檔名', '大小'],
    horizontal=True
)

if sort_by == '最新':
    checkpoint_files.sort(key=lambda x: x['modified'], reverse=True)
elif sort_by == '檔名':
    checkpoint_files.sort(key=lambda x: x['name'])
elif sort_by == '大小':
    checkpoint_files.sort(key=lambda x: x['size'], reverse=True)

# 顯示表格
for idx, checkpoint in enumerate(checkpoint_files):
    with st.expander(f"📄 {checkpoint['name']}", expanded=False):
        col1, col2 = st.columns([3, 1])

        with col1:
            st.markdown(f"**檔案路徑:** `{checkpoint['path']}`")
            st.markdown(f"**修改時間:** {checkpoint['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
            st.markdown(f"**檔案大小:** {checkpoint['size_mb']:.2f} MB")

        with col2:
            # 操作按鈕
            if st.button("🗑️ 刪除", key=f"delete_{idx}"):
                try:
                    os.remove(checkpoint['path'])
                    st.success(f"已刪除: {checkpoint['name']}")
                    st.rerun()
                except Exception as e:
                    st.error(f"刪除失敗: {str(e)}")

            if st.button("▶️ 載入", key=f"load_{idx}"):
                st.info("⚠️ 從 checkpoint 繼續執行的功能尚未實作")
                # TODO: 實作從 checkpoint 繼續執行的邏輯

# 批次操作
st.markdown("---")
st.subheader("🔧 批次操作")

col1, col2 = st.columns(2)

with col1:
    if st.button("🗑️ 清空所有 Checkpoint", type="secondary", use_container_width=True):
        if st.session_state.get('confirm_delete_all', False):
            # 執行刪除
            deleted_count = 0
            for checkpoint in checkpoint_files:
                try:
                    os.remove(checkpoint['path'])
                    deleted_count += 1
                except Exception as e:
                    st.error(f"刪除 {checkpoint['name']} 失敗: {str(e)}")

            st.success(f"已刪除 {deleted_count} 個 checkpoint")
            st.session_state.confirm_delete_all = False
            st.rerun()
        else:
            # 請求確認
            st.session_state.confirm_delete_all = True
            st.warning("⚠️ 再次點擊確認刪除所有 checkpoint")

with col2:
    if st.button("📊 查看統計", use_container_width=True):
        total_size = sum(c['size'] for c in checkpoint_files)
        total_size_mb = total_size / (1024 * 1024)

        st.metric("總檔案數", len(checkpoint_files))
        st.metric("總大小", f"{total_size_mb:.2f} MB")

        # 最舊和最新的 checkpoint
        oldest = min(checkpoint_files, key=lambda x: x['modified'])
        newest = max(checkpoint_files, key=lambda x: x['modified'])

        st.markdown(f"**最舊:** {oldest['name']} ({oldest['modified'].strftime('%Y-%m-%d')})")
        st.markdown(f"**最新:** {newest['name']} ({newest['modified'].strftime('%Y-%m-%d')})")
