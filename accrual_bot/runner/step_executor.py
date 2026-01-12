"""
Step-by-Step Executor - 逐步執行 Pipeline

提供互動式逐步執行功能，每個步驟執行後暫停等待用戶確認
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from accrual_bot.core.pipeline import Pipeline, ProcessingContext
from accrual_bot.core.pipeline.base import StepResult, StepStatus
from accrual_bot.core.pipeline.checkpoint import CheckpointManager
from accrual_bot.utils.logging import get_logger

logger = get_logger(__name__)


class StepByStepExecutor:
    """
    逐步執行器

    每個步驟執行後暫停，顯示結果並等待用戶確認
    支援繼續、跳過、中止操作
    """

    def __init__(
        self,
        pipeline: Pipeline,
        context: ProcessingContext,
        save_checkpoints: bool = True,
        checkpoint_dir: str = "./checkpoints"
    ):
        """
        初始化逐步執行器

        Args:
            pipeline: 要執行的 Pipeline
            context: 處理上下文
            save_checkpoints: 是否儲存 checkpoint
            checkpoint_dir: checkpoint 儲存目錄
        """
        self.pipeline = pipeline
        self.context = context
        self.save_checkpoints = save_checkpoints
        self.checkpoint_manager = CheckpointManager(checkpoint_dir) if save_checkpoints else None

        self.results: List[StepResult] = []
        self.start_time: Optional[datetime] = None
        self.aborted: bool = False

    async def run(self) -> Dict[str, Any]:
        """
        執行 Pipeline (逐步模式)

        Returns:
            Dict[str, Any]: 執行結果
        """
        self.start_time = datetime.now()
        self.results = []
        self.aborted = False

        total_steps = len(self.pipeline.steps)

        self._print_header()
        logger.info(f"開始逐步執行 Pipeline: {self.pipeline.config.name}")

        for i, step in enumerate(self.pipeline.steps):
            if self.aborted:
                break

            # 顯示步驟資訊
            self._print_step_header(i + 1, total_steps, step.name)

            # 詢問用戶操作
            action = self._prompt_action(step.name)

            if action == "skip":
                result = StepResult(
                    step_name=step.name,
                    status=StepStatus.SKIPPED,
                    message="User skipped"
                )
                self.results.append(result)
                self._print_step_result(result)
                continue

            elif action == "abort":
                self.aborted = True
                logger.info("用戶中止執行")
                break

            # 執行步驟
            step_start = time.time()
            try:
                result = await step.execute(self.context)
                result.duration = time.time() - step_start
            except Exception as e:
                result = StepResult(
                    step_name=step.name,
                    status=StepStatus.FAILED,
                    error=e,
                    message=str(e),
                    duration=time.time() - step_start
                )
                logger.error(f"步驟執行失敗: {e}")

            self.results.append(result)

            # 顯示結果
            self._print_step_result(result)

            # 儲存 checkpoint
            if self.save_checkpoints and result.is_success:
                self._save_checkpoint(step.name)

            # 如果失敗，詢問是否繼續
            if result.is_failed:
                if not self._confirm_continue_on_error(step.name, result):
                    self.aborted = True
                    break

        return self._build_execution_result()

    def _print_header(self):
        """顯示執行標題"""
        print("\n" + "=" * 60)
        print(f"Pipeline: {self.pipeline.config.name}")
        print(f"Entity: {self.context.metadata.entity_type}")
        print(f"Processing Date: {self.context.metadata.processing_date}")
        print(f"Total Steps: {len(self.pipeline.steps)}")
        print("=" * 60)
        print("\n逐步執行模式已啟用")
        print("指令: [Enter]=繼續 | [s]=跳過 | [q]=中止\n")

    def _print_step_header(self, current: int, total: int, step_name: str):
        """顯示步驟標題"""
        print("\n" + "-" * 60)
        print(f"步驟 {current}/{total}: {step_name}")
        print("-" * 60)

    def _print_step_result(self, result: StepResult):
        """顯示步驟結果"""
        status_icon = {
            StepStatus.SUCCESS: "[OK]",
            StepStatus.FAILED: "[X]",
            StepStatus.SKIPPED: "[SKIP]",
        }.get(result.status, "[?]")

        print(f"\n{status_icon} Status: {result.status.value}")
        if result.duration > 0:
            print(f"    Duration: {result.duration:.2f}s")
        if result.message:
            print(f"    Message: {result.message}")

        # 顯示 context 摘要
        if result.is_success and self.context.data is not None:
            print(f"    Data rows: {len(self.context.data)}")

    def _prompt_action(self, step_name: str) -> str:
        """
        提示用戶選擇操作

        Returns:
            str: 'continue', 'skip', 或 'abort'
        """
        while True:
            try:
                response = input(f"\n執行 '{step_name}'? [Enter/s/q]: ").strip().lower()
                if response == "" or response == "c":
                    return "continue"
                elif response == "s":
                    return "skip"
                elif response == "q":
                    return "abort"
                else:
                    print("無效輸入，請重試 (Enter=繼續, s=跳過, q=中止)")
            except EOFError:
                # 非互動模式，自動繼續
                return "continue"

    def _confirm_continue_on_error(self, step_name: str, result: StepResult) -> bool:
        """步驟失敗時詢問是否繼續"""
        print(f"\n警告: 步驟 '{step_name}' 執行失敗")
        if result.error:
            print(f"錯誤: {result.error}")
        try:
            response = input("是否繼續執行下一步? [y/n]: ").strip().lower()
            return response == "y"
        except EOFError:
            return False

    def _save_checkpoint(self, step_name: str):
        """儲存 checkpoint"""
        if self.checkpoint_manager is None:
            return

        try:
            checkpoint_name = (
                f"{self.context.entity_type}_"
                f"{self.context.processing_type}_"
                f"{self.context.processing_date}_"
                f"after_{step_name}"
            )
            self.checkpoint_manager.save(self.context, checkpoint_name)
            logger.debug(f"Checkpoint saved: {checkpoint_name}")
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _build_execution_result(self) -> Dict[str, Any]:
        """建立執行結果"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0

        success_count = sum(1 for r in self.results if r.is_success)
        failed_count = sum(1 for r in self.results if r.is_failed)
        skipped_count = sum(1 for r in self.results if r.status == StepStatus.SKIPPED)

        success = failed_count == 0 and not self.aborted

        # 顯示最終摘要
        print("\n" + "=" * 60)
        print("執行完成" if not self.aborted else "執行中止")
        print("=" * 60)
        print(f"成功: {success_count} | 失敗: {failed_count} | 跳過: {skipped_count}")
        print(f"總耗時: {duration:.2f}s")
        print("=" * 60 + "\n")

        return {
            "pipeline": self.pipeline.config.name,
            "success": success,
            "aborted": self.aborted,
            "start_time": self.start_time,
            "end_time": end_time,
            "duration": duration,
            "total_steps": len(self.pipeline.steps),
            "executed_steps": len(self.results),
            "successful_steps": success_count,
            "failed_steps": failed_count,
            "skipped_steps": skipped_count,
            "results": [r.to_dict() for r in self.results],
            "context": self.context,
            "errors": self.context.errors,
            "warnings": self.context.warnings,
        }
