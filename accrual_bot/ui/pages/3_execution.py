"""
Execution Page

Pipeline 執行監控頁面。
"""

import streamlit as st
import sys
from pathlib import Path
import time

# 加入專案根目錄到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from accrual_bot.ui.app import init_session_state, get_navigation_status
from accrual_bot.ui.components import render_progress_tracker, render_step_status_table
from accrual_bot.ui.services import UnifiedPipelineService, StreamlitPipelineRunner
from accrual_bot.ui.utils import AsyncBridge
from accrual_bot.ui.models.state_models import ExecutionStatus

# 初始化 session state
init_session_state()

# 頁面設定
st.set_page_config(
    page_title="執行監控 | Accrual Bot",
    page_icon="▶️",
    layout="wide"
)

st.title("▶️ Pipeline 執行監控")
st.markdown("---")

# 檢查導航狀態
nav_status = get_navigation_status()
if not nav_status['execution']:
    st.warning("⚠️ 請先完成檔案上傳")
    if st.button("前往檔案上傳頁"):
        st.switch_page("pages/2_📁_檔案上傳.py")
    st.stop()

# 獲取配置
config = st.session_state.pipeline_config
execution = st.session_state.execution
upload = st.session_state.file_upload

# 顯示當前配置
st.info(f"📊 配置: **{config.entity} / {config.processing_type}** | 日期: **{config.processing_date}**")

# 執行控制按鈕
col1, col2, col3 = st.columns([1, 1, 3])

with col1:
    start_button = st.button(
        "▶️ 開始執行",
        disabled=execution.status == ExecutionStatus.RUNNING,
        type="primary",
        use_container_width=True
    )

with col2:
    stop_button = st.button(
        "⏹️ 停止",
        disabled=execution.status != ExecutionStatus.RUNNING,
        use_container_width=True
    )

with col3:
    if st.button("🔄 重置", use_container_width=True):
        from accrual_bot.ui.app import reset_session_state
        reset_session_state()
        st.switch_page("pages/1_⚙️_配置.py")

st.markdown("---")

# 開始執行
if start_button and execution.status != ExecutionStatus.RUNNING:
    execution.status = ExecutionStatus.RUNNING
    execution.start_time = time.time()
    execution.logs = []
    execution.completed_steps = []
    execution.failed_steps = []
    execution.error_message = ""

    # 執行 pipeline
    try:
        service = UnifiedPipelineService()
        runner = StreamlitPipelineRunner(service)

        # 設定回調
        def log_callback(message: str):
            execution.logs.append(message)

        def progress_callback(step_name: str, current: int, total: int, status: str = 'running'):
            """進度回調：更新執行狀態"""
            execution.current_step = step_name if status == 'running' else ""

            if status == 'completed':
                if step_name not in execution.completed_steps:
                    execution.completed_steps.append(step_name)
                # 從 failed_steps 移除（如果存在）
                if step_name in execution.failed_steps:
                    execution.failed_steps.remove(step_name)

            elif status == 'failed':
                if step_name not in execution.failed_steps:
                    execution.failed_steps.append(step_name)
                # 從 completed_steps 移除（如果存在）
                if step_name in execution.completed_steps:
                    execution.completed_steps.remove(step_name)

        runner.set_log_callback(log_callback)
        runner.set_progress_callback(progress_callback)

        # 執行
        st.info("⏳ 正在執行 pipeline...")

        # 準備參數
        execute_params = {
            'entity': config.entity,
            'proc_type': config.processing_type,
            'file_paths': upload.file_paths,
            'processing_date': config.processing_date,
        }

        # 如果是 PROCUREMENT，傳入 source_type
        if config.processing_type == 'PROCUREMENT':
            execute_params['source_type'] = config.procurement_source_type

        result = AsyncBridge.run_async(runner.execute(**execute_params))

        execution.end_time = time.time()

        if result['success']:
            execution.status = ExecutionStatus.COMPLETED
            st.session_state.result.success = True
            st.session_state.result.output_data = result['context'].data
            st.session_state.result.auxiliary_data = result['context'].auxiliary_data
            st.session_state.result.execution_time = result['execution_time']
            st.success("✅ 執行成功！")
            time.sleep(1)
            st.switch_page("pages/4_📊_結果.py")
        else:
            execution.status = ExecutionStatus.FAILED
            execution.error_message = result['error']
            st.error(f"❌ 執行失敗: {result['error']}")

    except Exception as e:
        execution.status = ExecutionStatus.FAILED
        execution.error_message = str(e)
        execution.end_time = time.time()
        st.error(f"❌ 執行失敗: {str(e)}")

# 顯示進度
if execution.status != ExecutionStatus.IDLE:
    enabled_steps = config.enabled_steps

    render_progress_tracker(
        current_step=execution.current_step,
        completed_steps=execution.completed_steps,
        failed_steps=execution.failed_steps,
        total_steps=len(enabled_steps),
        start_time=execution.start_time
    )

    st.markdown("---")

    # 步驟狀態表格
    render_step_status_table(
        all_steps=enabled_steps,
        completed_steps=execution.completed_steps,
        failed_steps=execution.failed_steps,
        current_step=execution.current_step
    )

    st.markdown("---")

    # 日誌 viewer
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("📝 執行日誌")
    with col2:
        if execution.logs:
            # 日誌匯出按鈕
            log_content = "\n".join(execution.logs)
            st.download_button(
                label="📥 下載日誌",
                data=log_content,
                file_name=f"{config.entity}_{config.processing_type}_{config.processing_date}_logs.txt",
                mime="text/plain",
                use_container_width=True
            )

    if execution.logs:
        log_container = st.container(height=300)
        with log_container:
            for log in execution.logs:
                st.text(log)
    else:
        st.info("尚無日誌")

    # 錯誤訊息
    if execution.error_message:
        st.markdown("---")
        st.subheader("❌ 錯誤訊息")
        st.error(execution.error_message)
