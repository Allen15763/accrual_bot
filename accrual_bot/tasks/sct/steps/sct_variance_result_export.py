"""
SCT 差異分析 - 結果解析與匯出步驟

負責：
1. 解析 Dify API 回應，提取差異明細表、executive summary、top 5 insights
2. 組裝多 Sheet Excel 匯出檔（差異明細 + 分析摘要）

API 回應路徑從 TOML 配置讀取，方便 API 格式微調時不需改程式碼。
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger


class SCTVarianceResultExportStep(PipelineStep):
    """
    SCT 差異分析結果解析與 Excel 匯出步驟

    從 context.variable['api_response'] 提取：
    - result_df → context.data（主要 DataFrame，差異明細表）
    - executive_summary → context.variable
    - top_5_insight → context.variable

    匯出 Excel：
    - Sheet "差異明細" = result_df
    - Sheet "分析摘要" = executive_summary + top_5_insight 文字
    """

    def __init__(self, name: str = "SCTVarianceResultExport", **kwargs):
        super().__init__(name=name, **kwargs)
        self.logger = get_logger(__name__)
        self.config = config_manager._config_toml.get('sct', {}).get('variance', {})

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        解析 API 回應並匯出 Excel
        """
        try:
            api_response = context.get_variable('api_response')
            if not api_response:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="無 API 回應可解析",
                )

            # 從配置讀取回應路徑
            result_path = self.config.get(
                'api_response_result_path', 'data.outputs.result_df'
            )
            summary_path = self.config.get(
                'api_response_summary_path', 'data.outputs.executive_summary'
            )
            insight_path = self.config.get(
                'api_response_insight_path', 'data.outputs.top_5_insight'
            )

            # 檢查 API 回應狀態（防禦性檢查，正常情況 API call 步驟已攔截）
            api_status = api_response.get('data', {}).get('status', 'unknown')
            if api_status != 'succeeded':
                error_info = (
                    api_response.get('error', '')
                    or api_response.get('data', {}).get('error', '')
                    or '無詳細錯誤'
                )
                msg = (
                    f"API 回應狀態非 succeeded ({api_status})，無法解析結果。"
                    f"\n  錯誤: {error_info}"
                )
                self.logger.error(msg)
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=msg,
                )

            # 提取各項結果
            result_raw = self._extract_by_path(api_response, result_path)
            executive_summary = self._extract_by_path(api_response, summary_path) or ""
            top_5_insight = self._extract_by_path(api_response, insight_path) or ""

            # 解析 result_df
            result_df = self._parse_result_df(result_raw)
            if result_df is None:
                # 詳細列出失敗原因
                raw_type = type(result_raw).__name__
                raw_preview = str(result_raw)[:200] if result_raw else "None"
                msg = (
                    f"無法解析差異明細表（result_df）"
                    f"\n  路徑: {result_path}"
                    f"\n  取得類型: {raw_type}"
                    f"\n  內容預覽: {raw_preview}"
                )
                self.logger.error(msg)
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=msg,
                )

            self.logger.info(f"差異明細表解析成功: {result_df.shape}")

            # 欄位排序（從配置讀取）
            output_columns = self.config.get('result_output_columns', [])
            if output_columns:
                available = [c for c in output_columns if c in result_df.columns]
                # 配置欄位排在前面，其餘欄位依原順序附加在後
                remaining = [c for c in result_df.columns if c not in available]
                result_df = result_df[available + remaining]
                if remaining:
                    self.logger.debug(f"額外欄位（未在排序配置中）: {remaining}")

            # 儲存到 context
            context.data = result_df
            context.set_variable('executive_summary', executive_summary)
            context.set_variable('top_5_insight', top_5_insight)

            # 匯出 Excel
            export_path = self._export_excel(
                result_df, executive_summary, top_5_insight, context
            )
            if export_path:
                context.set_variable('export_path', export_path)
                self.logger.info(f"Excel 匯出成功: {export_path}")

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"結果解析完成: {result_df.shape}, Excel 已匯出",
                metadata={
                    'result_rows': len(result_df),
                    'has_summary': bool(executive_summary),
                    'has_insights': bool(top_5_insight),
                    'export_path': export_path or "",
                },
            )

        except Exception as e:
            self.logger.error(f"結果解析/匯出失敗: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"結果解析/匯出失敗: {e}",
            )

    @staticmethod
    def _extract_by_path(data: Dict[str, Any], path: str) -> Any:
        """
        根據點分隔路徑從巢狀字典中提取值

        例如 "data.outputs.result_df" → data['data']['outputs']['result_df']
        """
        keys = path.split('.')
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def _parse_result_df(self, raw: Any) -> Optional[pd.DataFrame]:
        """
        解析 result_df 原始資料為 DataFrame

        支援：
        - JSON 字串 → pd.read_json
        - list of dict → pd.DataFrame
        - 已是 DataFrame → 直接回傳
        """
        if raw is None:
            return None

        if isinstance(raw, pd.DataFrame):
            return raw

        if isinstance(raw, list):
            return pd.DataFrame(raw)

        if isinstance(raw, dict):
            return pd.DataFrame(raw)

        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return pd.DataFrame(parsed)
                elif isinstance(parsed, dict):
                    return pd.DataFrame(parsed)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"JSON 解析失敗，嘗試 pd.read_json: {e}")
                try:
                    from io import StringIO
                    return pd.read_json(StringIO(raw))
                except Exception:
                    pass

        self.logger.error(f"無法解析 result_df，類型: {type(raw)}")
        return None

    def _export_excel(
        self,
        result_df: pd.DataFrame,
        executive_summary: str,
        top_5_insight: str,
        context: ProcessingContext,
    ) -> Optional[str]:
        """
        匯出多 Sheet Excel 檔案

        Sheet 1: 差異明細（result_df）
        Sheet 2: 分析摘要（executive_summary + top_5_insight 文字）
        """
        try:
            export_config = self.config.get('export', {})
            detail_sheet = export_config.get('detail_sheet_name', '差異明細')
            summary_sheet = export_config.get('summary_sheet_name', '分析摘要')

            # 決定輸出路徑
            output_dir = self._get_output_dir()
            os.makedirs(output_dir, exist_ok=True)

            processing_date = getattr(context.metadata, 'processing_date', None) or ''
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"SCT_差異分析報表_{processing_date}_{timestamp}.xlsx"
            export_path = os.path.join(output_dir, filename)

            with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
                # Sheet 1: 差異明細
                result_df.to_excel(writer, sheet_name=detail_sheet, index=False)

                # Sheet 2: 分析摘要
                summary_data = []
                if executive_summary:
                    summary_data.append({
                        '類型': 'Executive Summary',
                        '內容': executive_summary,
                    })
                if top_5_insight:
                    summary_data.append({
                        '類型': 'Top 5 Insights',
                        '內容': top_5_insight,
                    })

                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    summary_df.to_excel(
                        writer, sheet_name=summary_sheet, index=False
                    )

            return export_path

        except Exception as e:
            self.logger.warning(f"Excel 匯出失敗: {e}")
            return None

    @staticmethod
    def _get_output_dir() -> str:
        """取得輸出目錄路徑"""
        # 優先使用 ACCRUAL_BOT_WORKSPACE
        workspace = os.environ.get("ACCRUAL_BOT_WORKSPACE")
        if workspace:
            return os.path.join(workspace, "output")

        # fallback: 從 paths.toml 取得
        output_dir = config_manager._config_toml.get('base', {}).get('output', './output')
        return output_dir

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證 API 回應已存在"""
        if not context.get_variable('api_response'):
            self.logger.error("無 API 回應")
            return False
        return True
