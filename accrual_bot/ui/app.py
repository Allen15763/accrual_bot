"""
Streamlit UI Application

主應用程式進入點，負責初始化 session state。
"""

import streamlit as st
from accrual_bot.ui.models.state_models import (
    PipelineConfig,
    FileUploadState,
    ExecutionState,
    ResultState,
    ExecutionStatus,
)


def init_session_state():
    """初始化所有 session state"""

    # Pipeline 配置狀態
    if 'pipeline_config' not in st.session_state:
        st.session_state.pipeline_config = PipelineConfig()

    # 檔案上傳狀態
    if 'file_upload' not in st.session_state:
        st.session_state.file_upload = FileUploadState()

    # 執行狀態
    if 'execution' not in st.session_state:
        st.session_state.execution = ExecutionState()

    # 結果狀態
    if 'result' not in st.session_state:
        st.session_state.result = ResultState()

    # 其他狀態
    if 'temp_dir' not in st.session_state:
        st.session_state.temp_dir = None

    if 'current_page' not in st.session_state:
        st.session_state.current_page = '1_configuration'


def reset_session_state():
    """重置 session state"""
    st.session_state.pipeline_config = PipelineConfig()
    st.session_state.file_upload = FileUploadState()
    st.session_state.execution = ExecutionState()
    st.session_state.result = ResultState()
    st.session_state.temp_dir = None


def get_navigation_status() -> dict:
    """
    獲取導航狀態，用於判斷各頁面是否可進入

    Returns:
        各頁面的可用狀態
    """
    config = st.session_state.pipeline_config
    upload = st.session_state.file_upload
    execution = st.session_state.execution

    return {
        'configuration': True,  # 總是可進入
        'file_upload': bool(config.entity and config.processing_type),
        'execution': upload.required_files_complete,
        'results': execution.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED],
        'checkpoint': True,  # 總是可進入
    }
