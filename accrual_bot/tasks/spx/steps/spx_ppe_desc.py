"""
SPX PPE_DESC Pipeline Steps

後處理步驟：從 PO/PR 底稿的 Item Description 欄位提取摘要資訊，
並對應年限表計算合約剩餘月數。

資料流：
    PPEDescDataLoadingStep → DescriptionExtractionStep →
    ContractPeriodMappingStep → PPEDescExportStep
"""

import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import StepMetadataBuilder
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


# =============================================================================
# 模組級業務邏輯函式（可獨立測試）
# =============================================================================


def extract_clean_description(desc: str) -> str:
    """
    從 Item Description 提取清洗後的摘要字串（SPX_ 前綴格式）

    處理規則優先順序：
    1. 門市裝修工程（含地址和期數）
    2. 有地址但無期數的工程項目
    3. 通用清理規則

    Args:
        desc: 原始 Item Description 字串

    Returns:
        清洗後的摘要，以 SPX_ 開頭
    """
    desc = desc.strip()

    # --- 規則一：門市裝修工程 (有地址和期數) ---
    pattern1 = r'(門市裝修工程-.*?\(.*?\))\s*SPX\s*store decoration\s*(.*?)\s*#'
    match1 = re.search(pattern1, desc, re.IGNORECASE)
    if match1:
        description_part = match1.group(1).strip()
        payment_term = match1.group(2).strip()
        return f"SPX_{description_part}_{payment_term}"

    # --- 規則二：有地址但沒有期數的工程項目 ---
    pattern2 = r'SVP_?(?:SPX)?\s*(.*?)(?:\(|（)([^)）]+)(?:\)|）)'
    match2 = re.search(pattern2, desc)
    if match2:
        project_name = match2.group(1).strip()
        address = match2.group(2).strip()
        # 移除工程名稱中夾雜的英文
        project_name = re.sub(r'[a-zA-Z\s-]+$', '', project_name).strip()
        return f"SPX_{project_name}({address})"

    # --- 通用規則 ---
    core_content = desc

    # 移除結尾的 #... 標籤
    core_content = re.sub(r'\s*#.*$', '', core_content).strip()

    # 移除結尾的英文描述
    core_content = re.sub(
        r'\s*payment machine.*$', '', core_content, flags=re.IGNORECASE
    )
    core_content = re.sub(
        r'_SPX N-SOC.*$', '', core_content, flags=re.IGNORECASE
    )

    # 移除前面的日期和公司前綴
    core_content = re.sub(
        r'^(\d{4}/\d{2}/\d{2})\s*-\s*(\d{4}/\d{2}/\d{2})', '', core_content
    )
    core_content = re.sub(r'^\d{4}/\d{2}\s*_?SVP_?(?:SPX)?\s*', '', core_content)

    # IT 等沒有 SVP 的特殊項目，單獨移除日期
    core_content = re.sub(r'^\d{4}/\d{2}\s*', '', core_content)

    # 清理多餘空白
    core_content = re.sub(r'\s+', ' ', core_content).strip()

    # 加上 SPX_ 前綴
    if core_content.upper().startswith('SPX '):
        return re.sub(r'^SPX\s', 'SPX_', core_content, flags=re.IGNORECASE)
    return f"SPX_{core_content}"


def extract_locker_info(text: str) -> Optional[str]:
    """
    從含「智取櫃」的字串中擷取 locker 後的資訊

    Args:
        text: 輸入字串

    Returns:
        擷取的智取櫃資訊，或 None
    """
    if not isinstance(text, str):
        return None

    pattern = r'門市智取櫃工程SPX locker\s?(.*)'
    match = re.search(pattern, text)
    if match:
        return match.group(1).strip()
    return None


def extract_address_from_dataframe(
    df: pd.DataFrame, column_name: str
) -> pd.DataFrame:
    """
    從指定欄位中擷取括號內的台灣地址

    Args:
        df: 包含地址資料的 DataFrame
        column_name: 包含原始地址字串的欄位名稱

    Returns:
        新增 'extracted_address' 欄位的 DataFrame
    """
    df_copy = df.copy()
    regex_pattern = r'\(((?:.{2,3}[縣市])?.{1,3}[區鄉鎮市].*?)\)'
    extracted = df_copy[column_name].str.extract(regex_pattern, expand=False)
    df_copy['extracted_address'] = extracted.str.strip()
    return df_copy


def _hd_locker_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    處理 HD 智取櫃特殊情況，標記 HD 主櫃、安裝運費、HD 櫃

    Args:
        df: 含 item_description 和 locker_type 欄位的 DataFrame

    Returns:
        更新 locker_type 後的 DataFrame
    """
    df_copy = df.copy()

    # 取得說明欄位名稱（大小寫不同）
    desc_col = (
        'Item Description' if 'Item Description' in df_copy.columns
        else 'item_description'
    )

    mask_1 = df_copy[desc_col].str.contains(
        '(?i)SPX HD locker 控制主櫃', na=False
    )
    mask_2 = df_copy[desc_col].str.contains(
        '(?i)SPX HD locker 安裝運費', na=False
    )
    mask_3 = df_copy[desc_col].str.contains(
        '(?i)SPX HD locker', na=False
    )
    non_state = df_copy['locker_type'].isna()

    conditions = [
        non_state & mask_1,
        non_state & mask_2,
        non_state & mask_3,
    ]
    result = ['HD主櫃', 'HD安裝運費', 'HD櫃']
    df_copy['locker_type'] = np.select(
        conditions, result, default=df_copy['locker_type']
    )
    return df_copy


def _process_description(df: pd.DataFrame) -> pd.DataFrame:
    """
    整合所有說明欄位提取邏輯

    依序執行：
    1. 提取清洗後摘要 (new_extracted_result)
    2. 移除「第n期款項」(new_extracted_result_without_第n期款項)
    3. 標記智取櫃型號 (locker_type)
    4. 提取地址 (extracted_address)
    5. 處理 HD 智取櫃 (locker_type 更新)

    Args:
        df: 含 Item Description 或 item_description 欄位的 DataFrame

    Returns:
        新增多個欄位的 DataFrame
    """
    # 判斷欄位名稱
    has_upper = 'Item Description' in df.columns
    desc_col = 'Item Description' if has_upper else 'item_description'
    result_col = 'New_Extracted_Result' if has_upper else 'new_extracted_result'
    result_clean_col = (
        'New_Extracted_Result_without_第n期款項' if has_upper
        else 'new_extracted_result_without_第n期款項'
    )

    df_copy = df.copy()

    # 1. 提取清洗後摘要
    df_copy[result_col] = df_copy[desc_col].apply(extract_clean_description)

    # 2. 移除「第n期款項」
    df_copy[result_clean_col] = (
        df_copy[result_col]
        .str.replace(r'第[一|二|三]期款項', '', regex=True)
        .str.strip('_')
    )

    # 3. 標記智取櫃型號
    mask = df_copy[result_clean_col].str.contains('智取櫃', na=False)
    df_copy.loc[mask, 'locker_type'] = (
        df_copy.loc[mask, result_clean_col]
        .apply(extract_locker_info)
        .str.replace('主機', '主櫃')
    )

    # 4. 提取地址
    df_copy = extract_address_from_dataframe(df_copy, desc_col)

    # 5. HD 智取櫃處理
    df_copy = _hd_locker_info(df_copy)

    return df_copy


def _process_contract_period(
    df: pd.DataFrame, df_dep: pd.DataFrame
) -> pd.DataFrame:
    """
    根據地址對應年限表的 months_diff

    策略：先用完整地址比對，再用截短至「號」的地址 fallback

    Args:
        df: 含 extracted_address 欄位的底稿 DataFrame
        df_dep: 年限表 DataFrame（需含 address, truncated_address, months_diff 欄位）

    Returns:
        新增 months_diff 欄位的 DataFrame
    """
    df_copy = df.copy()

    # 完整地址對應
    full_address_map = (
        df_dep.drop_duplicates(subset=['address'])
        .set_index('address')['months_diff']
    )

    # 截短地址對應（fallback）
    truncated_address_map = (
        df_dep.drop_duplicates(subset=['truncated_address'])
        .set_index('truncated_address')['months_diff']
    )

    df_copy['months_diff'] = df_copy['extracted_address'].map(full_address_map)
    fallback = df_copy['extracted_address'].map(truncated_address_map)
    df_copy['months_diff'] = df_copy['months_diff'].fillna(fallback)

    return df_copy


# =============================================================================
# Pipeline Steps
# =============================================================================


class PPEDescDataLoadingStep(PipelineStep):
    """
    PPE_DESC 資料載入步驟

    從單一 Excel 檔的不同 sheet 載入 PO 和 PR 底稿，
    並載入年限表作為參考資料。

    輸入檔案：
    - workpaper: PO/PR 底稿 Excel（sheet: PO_{YYYYMM}, PR_{YYYYMM}）
    - contract_periods: 年限表 Excel

    輸出：
    - context.data = PO DataFrame
    - auxiliary_data['pr_data'] = PR DataFrame
    - auxiliary_data['contract_periods'] = 年限表 DataFrame
    """

    def __init__(
        self,
        name: str = "PPEDescDataLoading",
        file_paths: Optional[Dict[str, Any]] = None,
        processing_date: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(
            name, description="載入 PO/PR 底稿和年限表", **kwargs
        )
        self.file_paths = file_paths or {}
        self.processing_date = processing_date

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行資料載入"""
        start_time = time.time()

        try:
            processing_date = (
                self.processing_date or context.metadata.processing_date
            )
            po_sheet = f"PO_{processing_date}"
            pr_sheet = f"PR_{processing_date}"

            # 解析 workpaper 路徑
            wp_config = self.file_paths.get('workpaper', {})
            wp_path = (
                wp_config.get('path') if isinstance(wp_config, dict)
                else wp_config
            )
            if not wp_path:
                raise ValueError("未提供底稿檔案路徑 (workpaper)")

            # 解析年限表路徑
            cp_config = self.file_paths.get('contract_periods', {})
            cp_path = (
                cp_config.get('path') if isinstance(cp_config, dict)
                else cp_config
            )
            if not cp_path:
                raise ValueError("未提供年限表檔案路徑 (contract_periods)")

            self.logger.info(f"載入底稿: {wp_path}")
            self.logger.info(f"PO sheet: {po_sheet}, PR sheet: {pr_sheet}")
            self.logger.info(f"載入年限表: {cp_path}")

            # 讀取 PO sheet
            df_po = pd.read_excel(wp_path, sheet_name=po_sheet, dtype=str)
            self.logger.info(f"PO 資料: {len(df_po)} 筆")

            # 讀取 PR sheet
            df_pr = pd.read_excel(wp_path, sheet_name=pr_sheet, dtype=str)
            self.logger.info(f"PR 資料: {len(df_pr)} 筆")

            # 讀取年限表
            df_dep = pd.read_excel(cp_path)
            self.logger.info(f"年限表: {len(df_dep)} 筆")

            # 存入 context
            context.update_data(df_po)
            context.add_auxiliary_data('pr_data', df_pr)
            context.add_auxiliary_data('contract_periods', df_dep)

            duration = time.time() - start_time

            metadata = (
                StepMetadataBuilder()
                .set_row_counts(0, len(df_po))
                .add_custom('po_rows', len(df_po))
                .add_custom('pr_rows', len(df_pr))
                .add_custom('contract_period_rows', len(df_dep))
                .build()
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_po,
                message=(
                    f"成功載入 PO {len(df_po)} 筆, "
                    f"PR {len(df_pr)} 筆, "
                    f"年限表 {len(df_dep)} 筆"
                ),
                duration=duration,
                metadata=metadata,
            )

        except Exception as e:
            self.logger.error(f"資料載入失敗: {e}", exc_info=True)
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=f"載入失敗: {e}",
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入：檢查檔案路徑是否存在"""
        wp_config = self.file_paths.get('workpaper', {})
        wp_path = (
            wp_config.get('path') if isinstance(wp_config, dict)
            else wp_config
        )
        if not wp_path:
            self.logger.error("缺少 workpaper 檔案路徑")
            return False
        return True


class DescriptionExtractionStep(PipelineStep):
    """
    說明欄位提取步驟

    對 PO 和 PR 底稿執行摘要提取：
    - 清洗 Item Description → SPX_ 格式摘要
    - 提取智取櫃型號
    - 提取台灣地址
    - 處理 HD 智取櫃特殊情況
    """

    def __init__(self, name: str = "DescriptionExtraction", **kwargs):
        super().__init__(
            name, description="從品項說明提取摘要、地址、智取櫃型號", **kwargs
        )

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行說明欄位提取"""
        start_time = time.time()

        try:
            # 處理 PO
            df_po = context.data.copy()
            df_po = _process_description(df_po)
            context.update_data(df_po)
            self.logger.info(f"PO 說明欄位提取完成: {len(df_po)} 筆")

            # 處理 PR
            df_pr = context.get_auxiliary_data('pr_data')
            if df_pr is not None and not df_pr.empty:
                df_pr = _process_description(df_pr.copy())
                context.set_auxiliary_data('pr_data', df_pr)
                self.logger.info(f"PR 說明欄位提取完成: {len(df_pr)} 筆")

            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_po,
                message=f"PO {len(df_po)} 筆, PR {len(df_pr) if df_pr is not None else 0} 筆提取完成",
                duration=duration,
                metadata={
                    'po_rows': len(df_po),
                    'pr_rows': len(df_pr) if df_pr is not None else 0,
                },
            )

        except Exception as e:
            self.logger.error(f"說明欄位提取失敗: {e}", exc_info=True)
            context.add_error(f"DescriptionExtraction failed: {e}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e),
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入：需要主 DataFrame 且含說明欄位"""
        if context.data is None or context.data.empty:
            self.logger.error("無 PO 資料")
            return False

        desc_col = (
            'Item Description' if 'Item Description' in context.data.columns
            else 'item_description'
        )
        if desc_col not in context.data.columns:
            self.logger.error(f"缺少欄位: {desc_col}")
            return False
        return True


class ContractPeriodMappingStep(PipelineStep):
    """
    年限對應步驟

    根據提取的地址對應年限表的 months_diff：
    - 先用完整地址比對
    - 再用截短至「號」的地址 fallback
    """

    def __init__(self, name: str = "ContractPeriodMapping", **kwargs):
        super().__init__(
            name, description="對應年限表計算合約剩餘月數", **kwargs
        )

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行年限對應"""
        start_time = time.time()

        try:
            df_dep = context.get_auxiliary_data('contract_periods')
            if df_dep is None or df_dep.empty:
                raise ValueError("年限表資料為空")

            # 處理 PO
            df_po = _process_contract_period(context.data.copy(), df_dep)
            po_matched = df_po['months_diff'].notna().sum()
            context.update_data(df_po)
            self.logger.info(
                f"PO 年限對應完成: {po_matched}/{len(df_po)} 筆匹配"
            )

            # 處理 PR
            pr_matched = 0
            df_pr = context.get_auxiliary_data('pr_data')
            if df_pr is not None and not df_pr.empty:
                df_pr = _process_contract_period(df_pr.copy(), df_dep)
                pr_matched = df_pr['months_diff'].notna().sum()
                context.set_auxiliary_data('pr_data', df_pr)
                self.logger.info(
                    f"PR 年限對應完成: {pr_matched}/{len(df_pr)} 筆匹配"
                )

            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df_po,
                message=(
                    f"PO 匹配 {po_matched}/{len(df_po)}, "
                    f"PR 匹配 {pr_matched}/{len(df_pr) if df_pr is not None else 0}"
                ),
                duration=duration,
                metadata={
                    'po_matched': int(po_matched),
                    'po_total': len(df_po),
                    'pr_matched': int(pr_matched),
                    'pr_total': len(df_pr) if df_pr is not None else 0,
                },
            )

        except Exception as e:
            self.logger.error(f"年限對應失敗: {e}", exc_info=True)
            context.add_error(f"ContractPeriodMapping failed: {e}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e),
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入：需要主 DataFrame 和年限表"""
        if context.data is None or context.data.empty:
            self.logger.error("無 PO 資料")
            return False
        if not context.has_auxiliary_data('contract_periods'):
            self.logger.error("缺少年限表資料")
            return False
        return True


class PPEDescExportStep(PipelineStep):
    """
    PPE_DESC 匯出步驟

    匯出 3-sheet Excel：PO、PR、年限表
    """

    def __init__(
        self,
        name: str = "PPEDescExport",
        output_dir: str = "output",
        **kwargs,
    ):
        super().__init__(
            name, description="匯出 PPE_DESC 結果 Excel (3 sheets)", **kwargs
        )
        self.output_dir = output_dir

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行匯出"""
        start_time = time.time()

        try:
            df_po = context.data.copy()
            df_pr = context.get_auxiliary_data('pr_data')
            df_dep = context.get_auxiliary_data('contract_periods')

            # 清理 <NA>
            df_po = df_po.replace('<NA>', pd.NA)
            if df_pr is not None:
                df_pr = df_pr.replace('<NA>', pd.NA)

            # 生成檔名
            processing_date = context.metadata.processing_date
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"SPX_PPE_DESC_{processing_date}_{timestamp}.xlsx"

            # 建立輸出目錄
            os.makedirs(self.output_dir, exist_ok=True)
            output_path = os.path.join(self.output_dir, filename)

            # 確保檔名唯一
            counter = 1
            while os.path.exists(output_path):
                filename = (
                    f"SPX_PPE_DESC_{processing_date}_{timestamp}_{counter}.xlsx"
                )
                output_path = os.path.join(self.output_dir, filename)
                counter += 1

            # 匯出 Excel
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df_po.to_excel(writer, sheet_name='PO', index=False)
                if df_pr is not None:
                    df_pr.to_excel(writer, sheet_name='PR', index=False)
                if df_dep is not None:
                    df_dep.to_excel(writer, sheet_name='年限表', index=False)

            self.logger.info(f"匯出完成: {output_path}")

            # 儲存路徑到 context
            context.set_variable('export_output_path', str(output_path))

            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"匯出至 {output_path}",
                duration=duration,
                metadata={
                    'output_path': output_path,
                    'po_rows': len(df_po),
                    'pr_rows': len(df_pr) if df_pr is not None else 0,
                },
            )

        except Exception as e:
            self.logger.error(f"匯出失敗: {e}", exc_info=True)
            context.add_error(f"PPEDescExport failed: {e}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e),
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("無資料可匯出")
            return False
        return True
