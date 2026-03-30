"""
Results Page

執行結果預覽與匯出頁面。
"""

import streamlit as st
import sys
from pathlib import Path
import pandas as pd

# 加入專案根目錄到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state, get_navigation_status
from accrual_bot.ui.components import render_data_preview, render_auxiliary_data_tabs, render_statistics_metrics
from accrual_bot.ui.utils.ui_helpers import format_duration
from accrual_bot.ui.models.state_models import ExecutionStatus

# 初始化 session state
init_session_state()

# 頁面設定
st.set_page_config(
    page_title="執行結果 | Accrual Bot",
    page_icon="📊",
    layout="wide"
)

st.title("📊 執行結果")
st.markdown("---")

# 檢查導航狀態
nav_status = get_navigation_status()
if not nav_status['results']:
    st.warning("⚠️ 尚未執行或執行未完成")
    if st.button("前往執行頁"):
        st.switch_page("pages/3_▶️_執行.py")
    st.stop()

# 獲取結果
config = st.session_state.pipeline_config
execution = st.session_state.execution
result = st.session_state.result

# 成功/失敗 banner
if execution.status == ExecutionStatus.COMPLETED:
    st.success("✅ Pipeline 執行成功！")
elif execution.status == ExecutionStatus.FAILED:
    st.error("❌ Pipeline 執行失敗")
    if execution.error_message:
        with st.expander("錯誤詳情", expanded=True):
            st.code(execution.error_message)

# 統計 metrics
st.markdown("---")
st.subheader("📈 執行統計")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("處理實體", config.entity)

with col2:
    st.metric("處理類型", config.processing_type)

with col3:
    if execution.start_time and execution.end_time:
        duration = execution.end_time - execution.start_time
        st.metric("執行時間", format_duration(duration))
    else:
        st.metric("執行時間", "-")

with col4:
    if result.output_data is not None and isinstance(result.output_data, pd.DataFrame):
        st.metric("輸出行數", len(result.output_data))
    else:
        st.metric("輸出行數", "-")

# 主數據預覽
st.markdown("---")
if result.output_data is not None and isinstance(result.output_data, pd.DataFrame):

    # 差異分析專用：Tabs 佈局（差異明細 + 分析摘要）
    if config.processing_type == 'VARIANCE':
        summary = st.session_state.get('variance_executive_summary', '')
        insights = st.session_state.get('variance_top_5_insight', '')

        tab_detail, tab_analysis = st.tabs(["📋 差異明細", "📝 分析摘要"])

        with tab_detail:
            render_data_preview(
                data=result.output_data,
                title="差異明細表",
                max_rows=200,
                show_stats=True
            )

        with tab_analysis:
            if summary:
                st.subheader("📝 Executive Summary")
                st.info(summary, icon="📊")

            if insights:
                st.markdown("---")
                st.subheader("🔍 Top 5 Insights")
                st.markdown(insights)

            if not summary and not insights:
                st.info("本次分析未產生摘要內容")
    else:
        render_data_preview(
            data=result.output_data,
            title="主要輸出數據",
            max_rows=200,
            show_stats=True
        )

    # Excel 下載按鈕
    st.markdown("---")
    st.subheader("💾 匯出數據")

    if config.processing_type == 'VARIANCE':
        # 差異分析：提供完整報告下載（多 Sheet Excel）
        export_path = st.session_state.get('variance_export_path')
        if export_path:
            try:
                with open(export_path, 'rb') as f:
                    excel_data = f.read()
                st.download_button(
                    label="📥 下載完整差異分析報告 (Excel)",
                    data=excel_data,
                    file_name=Path(export_path).name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            except FileNotFoundError:
                st.warning("匯出檔案不存在，請重新執行")
        else:
            # fallback: 只下載差異明細表
            from io import BytesIO
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                result.output_data.to_excel(writer, index=False, sheet_name='差異明細')
            excel_buffer.seek(0)
            st.download_button(
                label="📥 下載差異明細表 (Excel)",
                data=excel_buffer,
                file_name=f"SCT_差異分析_{config.processing_date}_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:
        col1, col2 = st.columns(2)

        with col1:
            # CSV 下載
            csv_data = result.output_data.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 下載 CSV",
                data=csv_data,
                file_name=f"{config.entity}_{config.processing_type}_{config.processing_date}_output.csv",
                mime="text/csv",
                use_container_width=True
            )

        with col2:
            # Excel 下載
            from io import BytesIO
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                result.output_data.to_excel(writer, index=False, sheet_name='Output')
            excel_buffer.seek(0)

            st.download_button(
                label="📥 下載 Excel",
                data=excel_buffer,
                file_name=f"{config.entity}_{config.processing_type}_{config.processing_date}_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
else:
    st.warning("無輸出數據")

# 輔助數據
if result.auxiliary_data:
    st.markdown("---")
    render_auxiliary_data_tabs(result.auxiliary_data)

# 統計資訊
if result.statistics:
    st.markdown("---")
    render_statistics_metrics(result.statistics)

# 操作按鈕
st.markdown("---")
col1, col2 = st.columns([1, 4])

with col1:
    if st.button("🔄 重新執行", type="primary", use_container_width=True):
        from accrual_bot.ui.app import reset_session_state
        reset_session_state()
        st.switch_page("pages/1_⚙️_配置.py")

with col2:
    if st.button("📋 查看 Checkpoint", use_container_width=True):
        st.switch_page("pages/5_💾_Checkpoint.py")
