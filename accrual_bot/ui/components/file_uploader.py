"""
File Uploader Component

動態檔案上傳元件。
"""

import streamlit as st
from typing import Dict, List
from accrual_bot.ui.config import REQUIRED_FILES, OPTIONAL_FILES, FILE_LABELS, SUPPORTED_FILE_FORMATS
from accrual_bot.ui.services.file_handler import FileHandler


def render_file_uploader(entity: str, proc_type: str, file_handler: FileHandler) -> Dict[str, str]:
    """
    渲染動態檔案上傳器

    Args:
        entity: Entity 名稱
        proc_type: Processing type
        file_handler: FileHandler 實例

    Returns:
        檔案路徑字典
    """
    if not entity or not proc_type:
        st.info("請先完成配置頁設定")
        return {}

    st.subheader("📁 檔案上傳")

    # 獲取 source_type (僅 PROCUREMENT 使用)
    source_type = ""
    if proc_type == 'PROCUREMENT':
        source_type = st.session_state.pipeline_config.procurement_source_type
        if not source_type:
            st.warning("⚠️ 請先在配置頁選擇處理來源類型 (PO / PR)")
            return {}

    # 使用 helper 函數獲取檔案需求
    from accrual_bot.ui.config import get_file_requirements
    required_files, optional_files = get_file_requirements(entity, proc_type, source_type)

    if not required_files:
        st.warning("此組合沒有定義檔案需求")
        return {}

    # 必填檔案區
    st.markdown("### 必填檔案 ⭐")
    _render_file_section(required_files, file_handler, required=True)

    # 選填檔案區
    if optional_files:
        st.markdown("### 選填檔案")
        with st.expander("📂 展開選填檔案上傳", expanded=False):
            _render_file_section(optional_files, file_handler, required=False)

    # 驗證摘要
    _render_validation_summary(required_files)

    return st.session_state.file_upload.file_paths


def _render_file_section(file_keys: List[str], file_handler: FileHandler, required: bool):
    """
    渲染檔案上傳區段

    Args:
        file_keys: 檔案 key 清單
        file_handler: FileHandler 實例
        required: 是否必填
    """
    for file_key in file_keys:
        label = FILE_LABELS.get(file_key, file_key)

        uploaded_file = st.file_uploader(
            label,
            type=[fmt.strip('.') for fmt in SUPPORTED_FILE_FORMATS],
            key=f"uploader_{file_key}",
            help=f"{'必填' if required else '選填'} - 支援格式: {', '.join(SUPPORTED_FILE_FORMATS)}"
        )

        if uploaded_file:
            # 儲存檔案
            try:
                file_path = file_handler.save_uploaded_file(uploaded_file, file_key)

                # 驗證檔案
                errors = file_handler.validate_file(file_path, file_key)

                if errors:
                    st.error(f"❌ {errors[0]}")
                    # 從 session state 移除
                    if file_key in st.session_state.file_upload.file_paths:
                        del st.session_state.file_upload.file_paths[file_key]
                else:
                    st.success(f"✅ 檔案已上傳: {uploaded_file.name}")
                    # 儲存到 session state
                    st.session_state.file_upload.file_paths[file_key] = file_path
                    st.session_state.file_upload.uploaded_files[file_key] = uploaded_file

                    # 顯示檔案資訊
                    file_info = file_handler.get_file_info(file_path)
                    st.caption(f"大小: {file_info['size_mb']:.2f} MB")

            except Exception as e:
                st.error(f"❌ 上傳失敗: {str(e)}")
        else:
            # 清除已上傳的檔案
            if file_key in st.session_state.file_upload.file_paths:
                del st.session_state.file_upload.file_paths[file_key]
            if file_key in st.session_state.file_upload.uploaded_files:
                del st.session_state.file_upload.uploaded_files[file_key]


def _render_validation_summary(required_files: List[str]):
    """
    渲染驗證摘要

    Args:
        required_files: 必填檔案清單
    """
    st.markdown("---")
    st.markdown("### 📊 上傳狀態")

    uploaded_file_paths = st.session_state.file_upload.file_paths
    uploaded_required = [f for f in required_files if f in uploaded_file_paths]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("必填檔案", f"{len(uploaded_required)}/{len(required_files)}")

    with col2:
        total_uploaded = len(uploaded_file_paths)
        st.metric("總上傳檔案", total_uploaded)

    with col3:
        all_required_uploaded = len(uploaded_required) == len(required_files)
        status = "✅ 完整" if all_required_uploaded else "⚠️ 未完整"
        st.metric("狀態", status)

    # 更新 session state
    st.session_state.file_upload.required_files_complete = all_required_uploaded

    # 顯示缺少的必填檔案
    if not all_required_uploaded:
        missing = [f for f in required_files if f not in uploaded_file_paths]
        st.warning(f"⚠️ 缺少必填檔案: {', '.join([FILE_LABELS.get(f, f) for f in missing])}")
    else:
        st.success("✅ 所有必填檔案已上傳完成！")
