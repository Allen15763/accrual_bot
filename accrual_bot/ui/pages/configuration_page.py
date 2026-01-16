"""
Configuration Page Module

Pipeline 配置頁面：選擇 entity、processing type、日期、範本。
"""

import streamlit as st
from accrual_bot.ui.app import init_session_state
from accrual_bot.ui.components import (
    render_entity_selector,
    render_processing_type_selector,
    render_date_selector,
    render_step_preview,
)


def render():
    """渲染配置頁面"""
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
            st.markdown("---")
            # 第三步：選擇日期
            processing_date = render_date_selector()

            if processing_date > 0:
                st.markdown("---")
                # 第四步：Pipeline 配置說明
                st.info(f"""
                📋 **Pipeline 配置來源**

                - 使用 `{entity} Orchestrator` 的預設配置
                - 步驟順序由 `config/stagging.toml` 決定
                - 配置區段: `[pipeline.{entity.lower()}]`
                """)

                st.markdown("---")
                # 第五步：預覽步驟
                enabled_steps = render_step_preview(entity, proc_type)

                # 配置完成提示
                if enabled_steps:
                    st.markdown("---")
                    st.success("✅ 配置完成！請前往「檔案上傳」頁面上傳所需檔案。")

                    # 顯示配置摘要
                    with st.expander("📝 配置摘要", expanded=False):
                        st.json({
                            "entity": entity,
                            "processing_type": proc_type,
                            "processing_date": processing_date,
                            "pipeline_source": f"{entity} Orchestrator (config/stagging.toml)",
                            "total_steps": len(enabled_steps),
                        })
