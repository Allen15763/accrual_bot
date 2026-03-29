"""
Configuration Page

Pipeline 配置頁面：選擇 entity、processing type、日期、範本。
"""

import streamlit as st
import sys
from pathlib import Path

# 加入專案根目錄到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state
from accrual_bot.ui.components import (
    render_entity_selector,
    render_processing_type_selector,
    render_procurement_source_type_selector,
    render_date_selector,
    render_step_preview,
)

# 初始化 session state
init_session_state()

# 頁面設定
st.set_page_config(
    page_title="配置 | Accrual Bot",
    page_icon="⚙️",
    layout="wide"
)

st.title("⚙️ Pipeline 配置")
st.markdown("---")

# 第一步：選擇 Entity
entity = render_entity_selector()

if entity:
    st.markdown("---")
    # 第二步：選擇 Processing Type
    proc_type = render_processing_type_selector(entity)

    if proc_type:
        # 如果是 PROCUREMENT，顯示子類型選擇器
        source_type = ""
        if proc_type == 'PROCUREMENT':
            st.markdown("---")
            source_type = render_procurement_source_type_selector()
            if not source_type:
                st.info("📌 請選擇處理來源類型 (PO / PR)")
                st.stop()

        st.markdown("---")
        # 第三步：選擇日期
        processing_date = render_date_selector()

        if processing_date > 0:
            st.markdown("---")
            # 第四步：預覽步驟
            enabled_steps = render_step_preview(entity, proc_type)

            # 配置完成提示
            if enabled_steps:
                st.markdown("---")
                st.success("✅ 配置完成！請前往「檔案上傳」頁面上傳所需檔案。")

                # 顯示配置摘要
                config_summary = {
                    "entity": entity,
                    "processing_type": proc_type,
                    "processing_date": processing_date,
                    "total_steps": len(enabled_steps),
                }
                if source_type:
                    config_summary["procurement_source_type"] = source_type

                with st.expander("📝 配置摘要", expanded=False):
                    st.json(config_summary)
