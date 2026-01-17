"""
Entity Selector Component

Entity、Processing Type 和日期選擇元件。
"""

import streamlit as st
from datetime import datetime
from accrual_bot.ui.services.unified_pipeline_service import UnifiedPipelineService
from accrual_bot.ui.config import ENTITY_CONFIG, PROCESSING_TYPE_CONFIG


def render_entity_selector() -> str:
    """
    渲染 Entity 選擇器

    Returns:
        選擇的 entity
    """
    st.subheader("📊 選擇處理平台")

    service = UnifiedPipelineService()
    entities = service.get_available_entities()

    # 使用 columns 排列 entity 選項
    cols = st.columns(len(entities))

    selected_entity = st.session_state.pipeline_config.entity

    for idx, entity in enumerate(entities):
        config = ENTITY_CONFIG[entity]
        with cols[idx]:
            # 建立按鈕式選擇
            button_type = "primary" if selected_entity == entity else "secondary"
            if st.button(
                f"{config['icon']} {config['display_name']}",
                key=f"entity_{entity}",
                type=button_type,
                use_container_width=True
            ):
                # Entity 改變時，清除所有後續狀態
                st.session_state.pipeline_config.entity = entity
                st.session_state.pipeline_config.processing_type = ""  # 重置 type
                st.session_state.pipeline_config.enabled_steps = []  # 重置步驟

                # 清除檔案上傳狀態
                st.session_state.file_upload.file_paths = {}
                st.session_state.file_upload.uploaded_files = {}
                st.session_state.file_upload.validation_errors = []
                st.session_state.file_upload.required_files_complete = False

                # 清除執行狀態
                from accrual_bot.ui.models.state_models import ExecutionStatus
                st.session_state.execution.status = ExecutionStatus.IDLE
                st.session_state.execution.current_step = ""
                st.session_state.execution.completed_steps = []
                st.session_state.execution.failed_steps = []
                st.session_state.execution.logs = []
                st.session_state.execution.error_message = ""

                st.rerun()

            st.caption(config['description'])

    return st.session_state.pipeline_config.entity


def render_processing_type_selector(entity: str) -> str:
    """
    渲染 Processing Type 選擇器

    Args:
        entity: 已選擇的 entity

    Returns:
        選擇的 processing type
    """
    if not entity:
        st.info("請先選擇處理平台")
        return ""

    st.subheader("📝 選擇處理類型")

    service = UnifiedPipelineService()
    types = service.get_entity_types(entity)

    # 使用 columns 排列 type 選項
    cols = st.columns(len(types))

    selected_type = st.session_state.pipeline_config.processing_type

    for idx, proc_type in enumerate(types):
        type_config = PROCESSING_TYPE_CONFIG[proc_type]
        with cols[idx]:
            button_type = "primary" if selected_type == proc_type else "secondary"
            if st.button(
                f"{type_config['icon']} {type_config['display_name']}",
                key=f"type_{proc_type}",
                type=button_type,
                use_container_width=True
            ):
                # Processing type 改變時，清除檔案上傳和執行狀態
                st.session_state.pipeline_config.processing_type = proc_type
                st.session_state.pipeline_config.enabled_steps = []  # 重置步驟

                # 清除檔案上傳狀態
                st.session_state.file_upload.file_paths = {}
                st.session_state.file_upload.uploaded_files = {}
                st.session_state.file_upload.validation_errors = []
                st.session_state.file_upload.required_files_complete = False

                # 清除執行狀態
                from accrual_bot.ui.models.state_models import ExecutionStatus
                st.session_state.execution.status = ExecutionStatus.IDLE
                st.session_state.execution.current_step = ""
                st.session_state.execution.completed_steps = []
                st.session_state.execution.failed_steps = []
                st.session_state.execution.logs = []
                st.session_state.execution.error_message = ""

                st.rerun()

            st.caption(type_config['description'])

    return st.session_state.pipeline_config.processing_type


def render_date_selector() -> int:
    """
    渲染日期選擇器

    Returns:
        選擇的日期 (YYYYMM 格式)
    """
    st.subheader("📅 選擇處理日期")

    col1, col2 = st.columns(2)

    # 預設為當前日期
    current_date = datetime.now()
    default_year = current_date.year
    default_month = current_date.month

    # 從 session state 獲取已選日期
    current_date_int = st.session_state.pipeline_config.processing_date
    if current_date_int > 0:
        default_year = current_date_int // 100
        default_month = current_date_int % 100

    with col1:
        year = st.number_input(
            "年份",
            min_value=2020,
            max_value=2030,
            value=default_year,
            step=1,
            key="date_year"
        )

    with col2:
        month = st.number_input(
            "月份",
            min_value=1,
            max_value=12,
            value=default_month,
            step=1,
            key="date_month"
        )

    # 計算 YYYYMM
    processing_date = year * 100 + month
    st.session_state.pipeline_config.processing_date = processing_date

    st.info(f"處理日期: **{year}年{month:02d}月** (格式: {processing_date})")

    return processing_date
