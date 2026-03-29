"""
Step Preview Component

步驟預覽元件（唯讀，不支援編輯）。
"""

import streamlit as st
from typing import List
from accrual_bot.ui.services.unified_pipeline_service import UnifiedPipelineService
from accrual_bot.ui.utils.ui_helpers import get_status_icon


def render_step_preview(entity: str, proc_type: str) -> List[str]:
    """
    渲染步驟預覽（唯讀）

    Args:
        entity: Entity 名稱
        proc_type: Processing type

    Returns:
        啟用的步驟清單
    """
    if not entity or not proc_type:
        st.info("請先完成實體和處理類型選擇")
        return []

    st.subheader("🔄 Pipeline 步驟預覽")

    service = UnifiedPipelineService()

    try:
        # 獲取 source_type (僅 PROCUREMENT 使用)
        source_type = None
        if proc_type == 'PROCUREMENT':
            source_type = st.session_state.pipeline_config.procurement_source_type

        enabled_steps = service.get_enabled_steps(entity, proc_type, source_type=source_type)

        if not enabled_steps:
            st.warning("此組合沒有已啟用的步驟")
            return []

        st.info(f"📊 共 **{len(enabled_steps)}** 個步驟 (順序固定，由配置檔決定)")

        # 儲存到 session state
        st.session_state.pipeline_config.enabled_steps = enabled_steps

        # 顯示步驟清單
        with st.container():
            for idx, step_name in enumerate(enabled_steps, start=1):
                # 顯示步驟編號、名稱和狀態
                col1, col2, col3 = st.columns([1, 8, 1])

                with col1:
                    st.markdown(f"**{idx}**")

                with col2:
                    st.markdown(f"`{step_name}`")

                with col3:
                    # 顯示待執行圖示
                    st.markdown(get_status_icon('pending'))

        # 展開詳細資訊
        with st.expander("ℹ️ 步驟說明", expanded=False):
            st.markdown("""
            - 步驟順序由 `config/stagging.toml` 配置檔決定
            - 無法在 UI 中調整步驟順序或新增/移除步驟
            - 如需修改，請編輯配置檔後重新啟動
            """)

        return enabled_steps

    except Exception as e:
        st.error(f"載入步驟清單失敗: {str(e)}")
        return []
