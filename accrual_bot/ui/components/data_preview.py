"""
Data Preview Component

數據預覽元件。
"""

import streamlit as st
import pandas as pd
from typing import Optional, List


def render_data_preview(
    data: pd.DataFrame,
    title: str = "數據預覽",
    max_rows: int = 100,
    show_stats: bool = True
):
    """
    渲染數據預覽

    Args:
        data: DataFrame 數據
        title: 標題
        max_rows: 最大顯示行數
        show_stats: 是否顯示統計資訊
    """
    if data is None or data.empty:
        st.warning("無數據可顯示")
        return

    st.subheader(title)

    # 統計資訊
    if show_stats:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("總行數", len(data))
        with col2:
            st.metric("欄位數", len(data.columns))
        with col3:
            memory_mb = data.memory_usage(deep=True).sum() / (1024 ** 2)
            st.metric("記憶體", f"{memory_mb:.2f} MB")

    # 欄位選擇器
    all_columns = data.columns.tolist()
    selected_columns = st.multiselect(
        "選擇要顯示的欄位",
        options=all_columns,
        default=all_columns[:10] if len(all_columns) > 10 else all_columns,
        key=f"columns_{title}"
    )

    if not selected_columns:
        st.warning("請至少選擇一個欄位")
        return

    # 行數限制
    # 如果數據少於 10 行，直接顯示全部，不需要 slider
    if len(data) <= 10:
        display_rows = len(data)
    else:
        display_rows = st.slider(
            "顯示行數",
            min_value=10,
            max_value=min(max_rows, len(data)),
            value=min(50, len(data)),
            step=10,
            key=f"rows_{title}"
        )

    # 顯示數據
    st.dataframe(
        data[selected_columns].head(display_rows),
        width="stretch",
        height=400
    )

    # 下載按鈕
    st.download_button(
        label="📥 下載 CSV",
        data=data[selected_columns].to_csv(index=False).encode('utf-8-sig'),
        file_name=f"{title}.csv",
        mime="text/csv",
        key=f"download_{title}"
    )


def render_auxiliary_data_tabs(auxiliary_data: dict):
    """
    渲染輔助數據 tabs

    Args:
        auxiliary_data: 輔助數據字典
    """
    if not auxiliary_data:
        st.info("無輔助數據")
        return

    st.subheader("📂 輔助數據")

    # 建立 tabs
    tab_names = list(auxiliary_data.keys())
    tabs = st.tabs(tab_names)

    for idx, (data_name, data_df) in enumerate(auxiliary_data.items()):
        with tabs[idx]:
            if isinstance(data_df, pd.DataFrame):
                render_data_preview(
                    data=data_df,
                    title=data_name,
                    show_stats=True
                )
            else:
                st.write(data_df)


def render_statistics_metrics(statistics: dict):
    """
    渲染統計指標

    Args:
        statistics: 統計資訊字典
    """
    if not statistics:
        return

    st.subheader("📊 統計資訊")

    # 動態建立 columns
    num_metrics = len(statistics)
    cols = st.columns(min(num_metrics, 4))

    for idx, (key, value) in enumerate(statistics.items()):
        col_idx = idx % 4
        with cols[col_idx]:
            # 格式化值
            if isinstance(value, float):
                formatted_value = f"{value:.2f}"
            elif isinstance(value, int):
                formatted_value = f"{value:,}"
            else:
                formatted_value = str(value)

            st.metric(key, formatted_value)
