"""
Template Picker Component

範本選擇元件。
"""

import streamlit as st
from accrual_bot.ui.services.unified_pipeline_service import UnifiedPipelineService


def render_template_picker(entity: str, proc_type: str) -> str:
    """
    渲染範本選擇器

    Args:
        entity: Entity 名稱
        proc_type: Processing type

    Returns:
        選擇的範本名稱
    """
    if not entity or not proc_type:
        st.info("請先完成平台和處理類型選擇")
        return ""

    st.subheader("📋 選擇 Pipeline 範本")

    service = UnifiedPipelineService()

    try:
        templates_data = service.get_templates(entity, proc_type)
        recommended = templates_data['recommended']
        all_templates = templates_data['all']

        if not all_templates:
            st.warning("此組合沒有可用範本，將使用預設配置")
            return ""

        # 顯示推薦範本
        if recommended:
            st.info(f"💡 推薦範本: **{recommended}**")

        # 範本選擇下拉選單
        template_names = [t['name'] for t in all_templates]

        # 預設選擇推薦範本
        default_idx = 0
        if recommended and recommended in template_names:
            default_idx = template_names.index(recommended)

        selected_template = st.selectbox(
            "選擇範本",
            options=template_names,
            index=default_idx,
            format_func=lambda x: _format_template_name(x, all_templates),
            key="template_selector"
        )

        # 顯示範本詳細資訊
        if selected_template:
            template_info = next((t for t in all_templates if t['name'] == selected_template), None)
            if template_info:
                with st.expander("📖 範本說明", expanded=False):
                    st.write(template_info.get('description', '無說明'))

        st.session_state.pipeline_config.template_name = selected_template
        return selected_template

    except Exception as e:
        st.error(f"載入範本失敗: {str(e)}")
        return ""


def _format_template_name(name: str, templates: list) -> str:
    """
    格式化範本名稱以顯示

    Args:
        name: 範本名稱
        templates: 範本清單

    Returns:
        格式化的名稱
    """
    template = next((t for t in templates if t['name'] == name), None)
    if template and 'description' in template:
        # 取描述的前 30 個字元
        desc = template['description']
        if len(desc) > 30:
            desc = desc[:30] + "..."
        return f"{name} - {desc}"
    return name
