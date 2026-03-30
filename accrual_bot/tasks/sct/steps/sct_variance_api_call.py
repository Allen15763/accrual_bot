"""
SCT 差異分析 - API 呼叫步驟

將預處理後的當期與前期底稿傳送至 Dify Workflow API，
取得差異分析結果。API 端點、欄位名稱、超時等均從 TOML 配置讀取。
"""

from typing import Any, Dict

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.utils.api import DifyClient, DifyAPIError
from accrual_bot.utils.config import config_manager
from accrual_bot.utils.logging import get_logger


class SCTVarianceAPICallStep(PipelineStep):
    """
    SCT 差異分析 API 呼叫步驟

    從 context 取得預處理後的當期/前期 DataFrame，
    轉為 JSON 後呼叫 Dify Workflow API。
    原始 API 回應儲存至 context.variable['api_response']。
    """

    def __init__(self, name: str = "SCTVarianceAPICall", **kwargs):
        super().__init__(name=name, **kwargs)
        self.logger = get_logger(__name__)
        self.config = config_manager._config_toml.get('sct', {}).get('variance', {})

    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        呼叫 Dify API 進行差異分析
        """
        try:
            current_df = context.data
            previous_df = context.get_auxiliary_data('previous_worksheet')

            if current_df is None or previous_df is None:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="當期或前期底稿未載入，無法呼叫 API",
                )

            # 從配置讀取 API 參數
            api_url = self.config.get('api_url')
            if not api_url:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="未設定 API URL（sct.variance.api_url）",
                )

            api_timeout = self.config.get('api_timeout', 300)
            api_max_retries = self.config.get('api_max_retries', 2)

            # 從配置讀取 payload 欄位名稱
            prev_key = self.config.get('api_request_prev_key', 'prev_wp')
            curr_key = self.config.get('api_request_curr_key', 'curr_wp')

            # 組裝 payload
            payload = {
                prev_key: previous_df.to_json(),
                curr_key: current_df.to_json(),
            }

            self.logger.info(
                f"呼叫 Dify API: {api_url} "
                f"(當期 {len(current_df)} 筆, 前期 {len(previous_df)} 筆)"
            )

            # 呼叫 API
            client = DifyClient()
            response = await client.run_workflow(
                url=api_url,
                inputs=payload,
                timeout=api_timeout,
                max_retries=api_max_retries,
            )

            # 從回應中取得基本統計資訊
            data_block = response.get('data', {})
            elapsed_ms = data_block.get('elapsed_time_ms', 0)
            status = data_block.get('status', 'unknown')

            # 檢查 Dify workflow 執行狀態
            if status != 'succeeded':
                # 收集可用的錯誤資訊（error 可能在頂層或 data 層）
                error_info = response.get('error', '') or data_block.get('error', '')
                outputs = data_block.get('outputs', {})
                detail_parts = [f"Dify workflow 狀態異常: {status}"]
                if error_info:
                    detail_parts.append(f"錯誤訊息: {error_info}")
                if elapsed_ms:
                    detail_parts.append(f"耗時: {elapsed_ms}ms")
                if outputs:
                    # 顯示 outputs 的 key 方便除錯
                    detail_parts.append(f"回應 outputs keys: {list(outputs.keys())}")
                else:
                    detail_parts.append("回應無 outputs 資料")

                detail_msg = "\n  ".join(detail_parts)
                self.logger.error(detail_msg)

                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=detail_msg,
                    metadata={
                        'api_status': status,
                        'elapsed_time_ms': elapsed_ms,
                        'error': error_info,
                    },
                )

            # 成功 — 儲存原始回應
            context.set_variable('api_response', response)

            self.logger.info(
                f"API 回應成功: status={status}, elapsed={elapsed_ms}ms"
            )

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"API 呼叫成功 (耗時 {elapsed_ms}ms)",
                metadata={
                    'api_status': status,
                    'elapsed_time_ms': elapsed_ms,
                },
            )

        except DifyAPIError as e:
            self.logger.error(f"Dify API 錯誤: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"API 呼叫失敗: {e}",
                metadata={
                    'status_code': e.status_code,
                },
            )
        except Exception as e:
            self.logger.error(f"API 呼叫步驟失敗: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"API 呼叫步驟失敗: {e}",
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證 context 中有預處理後的資料"""
        if context.data is None or context.data.empty:
            self.logger.error("當期底稿為空")
            return False
        if context.get_auxiliary_data('previous_worksheet') is None:
            self.logger.error("前期底稿未載入")
            return False
        return True
