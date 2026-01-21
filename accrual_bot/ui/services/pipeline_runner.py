"""
Streamlit Pipeline Runner

封裝 pipeline 執行邏輯，處理 async 執行與進度回報。
"""

import time
import traceback
from typing import Dict, Any, Optional, Callable
import pandas as pd

from accrual_bot.core.pipeline import ProcessingContext, Pipeline
from accrual_bot.ui.services.unified_pipeline_service import UnifiedPipelineService
from accrual_bot.ui.models.state_models import ExecutionStatus


class StreamlitPipelineRunner:
    """封裝 pipeline 執行邏輯"""

    def __init__(self, service: UnifiedPipelineService):
        """
        初始化 runner

        Args:
            service: UnifiedPipelineService 實例
        """
        self.service = service
        self.progress_callback: Optional[Callable] = None
        self.log_callback: Optional[Callable] = None

    def set_progress_callback(self, callback: Callable[[str, int, int], None]):
        """
        設定進度回調函數

        Args:
            callback: 回調函數，參數為 (step_name, current, total)
        """
        self.progress_callback = callback

    def set_log_callback(self, callback: Callable[[str], None]):
        """
        設定日誌回調函數

        Args:
            callback: 回調函數，參數為 log_message
        """
        self.log_callback = callback

    async def execute(
        self,
        entity: str,
        proc_type: str,
        file_paths: Dict[str, str],
        processing_date: int,
        source_type: str = None
    ) -> Dict[str, Any]:
        """
        執行 pipeline 並返回結果

        Args:
            entity: Entity 名稱
            proc_type: 處理類型
            file_paths: 檔案路徑字典
            processing_date: 處理日期 (YYYYMM)
            source_type: 子類型 (僅 PROCUREMENT 使用)

        Returns:
            執行結果字典，包含:
                - success: 是否成功
                - context: ProcessingContext
                - step_results: 各步驟結果
                - error: 錯誤訊息 (如果失敗)
                - execution_time: 執行時間
        """
        start_time = time.time()

        try:
            # 建立 pipeline（忽略範本，直接使用 orchestrator）
            self._log("正在建立 pipeline...")
            self._log(f"使用 {entity} orchestrator 配置...")

            pipeline = self.service.build_pipeline(
                entity=entity,
                proc_type=proc_type,
                file_paths=file_paths,
                processing_date=processing_date,
                source_type=source_type
            )

            self._log(f"Pipeline 建立完成，共 {len(pipeline.steps)} 個步驟")

            # 建立 ProcessingContext
            context = ProcessingContext(
                data=pd.DataFrame(),
                entity_type=entity,
                processing_date=processing_date,
                processing_type=proc_type
            )
            context.set_variable('file_paths', file_paths)

            # 執行 pipeline
            self._log("開始執行 pipeline...")
            result = await self._execute_with_progress(pipeline, context)

            execution_time = time.time() - start_time
            self._log(f"Pipeline 執行完成，耗時 {execution_time:.2f} 秒")

            # pipeline.execute() 返回 dict: {'success': bool, 'results': list, ...}
            return {
                'success': result.get('success', False),
                'context': context,
                'step_results': {r.get('step', ''): r for r in result.get('results', [])},
                'error': result.get('error') if not result.get('success') else None,
                'execution_time': execution_time,
                'pipeline_result': result
            }

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"執行失敗: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg)

            return {
                'success': False,
                'context': None,
                'step_results': {},
                'error': error_msg,
                'execution_time': execution_time
            }

    async def _execute_with_progress(self, pipeline: Pipeline, context: ProcessingContext):
        """
        執行 pipeline 並回報進度

        Args:
            pipeline: Pipeline 物件
            context: ProcessingContext

        Returns:
            執行結果
        """
        total_steps = len(pipeline.steps)

        # 預先記錄所有步驟（讓 UI 知道即將執行的步驟）
        for idx, step in enumerate(pipeline.steps, start=1):
            self._log(f"[{idx}/{total_steps}] 執行步驟: {step.name}")

        # 使用 Pipeline 原生的 execute() 方法
        # 注意：這會一次性執行所有步驟，無法實時回調每個步驟的進度
        # 但可以避免 event loop 衝突
        result = await pipeline.execute(context)

        # 執行完成後，根據結果更新進度
        if self.progress_callback and 'results' in result:
            for idx, step_result in enumerate(result['results'], start=1):
                step_name = step_result.get('step', f'Step {idx}')
                is_success = step_result.get('status') == 'success'
                status = 'completed' if is_success else 'failed'
                self.progress_callback(step_name, idx, total_steps, status=status)

        return result

    def _log(self, message: str):
        """記錄日誌"""
        if self.log_callback:
            self.log_callback(message)
