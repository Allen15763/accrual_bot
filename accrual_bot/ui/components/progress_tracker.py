"""
Progress Tracker Component

進度追蹤元件。
"""

import streamlit as st
from typing import List
from accrual_bot.ui.utils.ui_helpers import get_status_icon, format_duration
import time


def render_progress_tracker(
    current_step: str,
    completed_steps: List[str],
    failed_steps: List[str],
    total_steps: int,
    start_time: float = None
):
    """
    渲染進度追蹤器

    Args:
        current_step: 當前執行的步驟
        completed_steps: 已完成的步驟清單
        failed_steps: 失敗的步驟清單
        total_steps: 總步驟數
        start_time: 開始時間 (timestamp)
    """
    st.subheader("⏱️ 執行進度")

    # 計算進度
    current_idx = len(completed_steps) + len(failed_steps)
    if current_step and current_step not in completed_steps and current_step not in failed_steps:
        current_idx += 1

    progress_percentage = (current_idx / total_steps) if total_steps > 0 else 0

    # 進度條
    st.progress(progress_percentage)

    # 統計資訊
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("總步驟", total_steps)

    with col2:
        st.metric("已完成", len(completed_steps), delta=None)

    with col3:
        st.metric("失敗", len(failed_steps), delta=None if len(failed_steps) == 0 else f"-{len(failed_steps)}")

    with col4:
        if start_time:
            elapsed = time.time() - start_time
            st.metric("已耗時", format_duration(elapsed))
        else:
            st.metric("已耗時", "-")

    # 當前步驟資訊
    if current_step:
        st.info(f"▶️ 正在執行: **{current_step}**")

    # 預估剩餘時間
    if start_time and current_idx > 0 and current_idx < total_steps:
        elapsed = time.time() - start_time
        avg_time_per_step = elapsed / current_idx
        remaining_steps = total_steps - current_idx
        estimated_remaining = avg_time_per_step * remaining_steps
        st.caption(f"⏳ 預估剩餘時間: {format_duration(estimated_remaining)}")


def render_step_status_table(
    all_steps: List[str],
    completed_steps: List[str],
    failed_steps: List[str],
    current_step: str
):
    """
    渲染步驟狀態表格

    Args:
        all_steps: 所有步驟清單
        completed_steps: 已完成的步驟清單
        failed_steps: 失敗的步驟清單
        current_step: 當前步驟
    """
    st.subheader("📋 步驟詳情")

    # 建立表格數據
    table_data = []
    for idx, step in enumerate(all_steps, start=1):
        if step in completed_steps:
            status = f"{get_status_icon('completed')} 完成"
        elif step in failed_steps:
            status = f"{get_status_icon('failed')} 失敗"
        elif step == current_step:
            status = f"{get_status_icon('running')} 執行中"
        else:
            status = f"{get_status_icon('pending')} 待執行"

        table_data.append({
            "序號": idx,
            "步驟名稱": step,
            "狀態": status
        })

    # 顯示表格
    st.table(table_data)
