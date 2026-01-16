"""
Execution Page Module

Pipeline 執行監控頁面。
"""

import streamlit as st
import time
from accrual_bot.ui.app import init_session_state, get_navigation_status
from accrual_bot.ui.components import render_progress_tracker, render_step_status_table
from accrual_bot.ui.services import UnifiedPipelineService, StreamlitPipelineRunner
from accrual_bot.ui.utils import AsyncBridge
from accrual_bot.ui.models.state_models import ExecutionStatus


def render():
    """渲染執行監控頁面"""
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

            runner.set_log_callback(log_callback)

            # 執行
            progress_placeholder = st.empty()
            progress_placeholder.info("⏳ 正在執行 pipeline，請稍候...")

            try:
                result = AsyncBridge.run_async(
                    runner.execute(
                        entity=config.entity,
                        proc_type=config.processing_type,
                        file_paths=upload.file_paths,
                        processing_date=config.processing_date,
                        use_template=False,  # 不使用範本，直接用 orchestrator
                        template_name=None
                    )
                )
            except Exception as exec_error:
                progress_placeholder.empty()
                raise exec_error

            execution.end_time = time.time()
            progress_placeholder.empty()

            if result['success']:
                execution.status = ExecutionStatus.COMPLETED
                st.session_state.result.success = True
                st.session_state.result.output_data = result['context'].data

                # 正確訪問 auxiliary_data
                aux_data_dict = {}
                for name in result['context'].list_auxiliary_data():
                    aux_data_dict[name] = result['context'].get_auxiliary_data(name)
                st.session_state.result.auxiliary_data = aux_data_dict

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

            # 顯示詳細錯誤
            st.error(f"❌ 執行失敗")
            with st.expander("錯誤詳情", expanded=True):
                st.code(str(e))

            # 如果有日誌，也顯示
            if execution.logs:
                with st.expander("執行日誌", expanded=False):
                    for log in execution.logs:
                        st.text(log)

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
        st.subheader("📝 執行日誌")
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
