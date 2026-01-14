"""
Pipeline 步驟基類
定義所有處理步驟的抽象接口
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Any, Dict, List, Union, Callable, TypeVar, Generic
from dataclasses import dataclass, field
import asyncio
from accrual_bot.utils.logging import get_logger
from datetime import datetime
import pandas as pd

from .context import ProcessingContext


class StepStatus(Enum):
    """步驟執行狀態"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"


@dataclass
class StepResult:
    """步驟執行結果"""
    step_name: str
    status: StepStatus
    data: Optional[pd.DataFrame] = None
    error: Optional[Exception] = None
    message: Optional[str] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_success(self) -> bool:
        return self.status == StepStatus.SUCCESS
    
    @property
    def is_failed(self) -> bool:
        return self.status == StepStatus.FAILED
    
    @property
    def is_skipped(self) -> bool:
        return self.status == StepStatus.SKIPPED
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'step_name': self.step_name,
            'status': self.status.value,
            'message': self.message,
            'duration': self.duration,
            'metadata': self.metadata,
            'error': str(self.error) if self.error else None
        }


T = TypeVar('T')


class PipelineStep(ABC, Generic[T]):
    """
    Pipeline 步驟基類
    所有處理步驟必須繼承此類
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 required: bool = True,
                 retry_count: int = 0,
                 timeout: Optional[float] = None):
        """
        初始化步驟
        
        Args:
            name: 步驟名稱
            description: 步驟描述
            required: 是否必需（失敗時是否停止Pipeline）
            retry_count: 重試次數
            timeout: 超時時間（秒）
        """
        self.name = name
        self.description = description
        self.required = required
        self.retry_count = retry_count
        self.timeout = timeout
        self.logger = get_logger(f"pipeline.{name}")
        self._prerequisites = []
        self._post_actions = []
    
    @abstractmethod
    async def execute(self, context: 'ProcessingContext') -> StepResult:
        """
        執行步驟邏輯
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        pass
    
    @abstractmethod
    async def validate_input(self, context: 'ProcessingContext') -> bool:
        """
        驗證輸入是否符合要求
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 是否通過驗證
        """
        pass
    
    async def rollback(self, context: 'ProcessingContext', error: Exception):
        """
        回滾操作（可選實現）
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(f"Rollback not implemented for {self.name}")
    
    async def __call__(self, context: 'ProcessingContext') -> StepResult:
        """
        使步驟可調用，包含完整的執行流程
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        start_time = datetime.now()
        
        try:
            # 執行前置檢查
            if not await self.validate_input(context):
                if self.required:
                    raise ValueError(f"Input validation failed for step {self.name}")
                else:
                    self.logger.warning(f"Skipping step {self.name} due to validation failure")
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.SKIPPED,
                        message="Input validation failed"
                    )
            
            # 執行前置動作
            for action in self._prerequisites:
                await action(context)
            
            # 執行主邏輯（支援重試）
            result = None
            last_error = None
            
            for attempt in range(self.retry_count + 1):
                try:
                    if self.timeout:
                        result = await asyncio.wait_for(
                            self.execute(context),
                            timeout=self.timeout
                        )
                    else:
                        result = await self.execute(context)
                    
                    # 執行成功，跳出重試循環
                    break
                    
                except asyncio.TimeoutError as e:
                    last_error = e
                    self.logger.error(f"Step {self.name} timeout after {self.timeout}s")
                    
                except Exception as e:
                    last_error = e
                    if attempt < self.retry_count:
                        self.logger.warning(f"Step {self.name} failed, retrying... ({attempt + 1}/{self.retry_count})")
                        await asyncio.sleep(2 ** attempt)  # 指數退避
                    else:
                        self.logger.error(f"Step {self.name} failed after {self.retry_count + 1} attempts")
            
            # 如果所有重試都失敗
            if result is None:
                if self.required:
                    await self.rollback(context, last_error)
                    raise last_error
                else:
                    result = StepResult(
                        step_name=self.name,
                        status=StepStatus.FAILED,
                        error=last_error,
                        message=str(last_error)
                    )
            
            # 執行後置動作
            for action in self._post_actions:
                await action(context)
            
            # 計算執行時間
            duration = (datetime.now() - start_time).total_seconds()
            result.duration = duration
            
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e),
                duration=duration
            )
    
    def add_prerequisite(self, action: Callable):
        """添加前置動作"""
        self._prerequisites.append(action)
        return self
    
    def add_post_action(self, action: Callable):
        """添加後置動作"""
        self._post_actions.append(action)
        return self
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"


class ConditionalStep(PipelineStep):
    """
    條件步驟：根據條件決定是否執行
    """
    
    def __init__(self,
                 name: str,
                 condition: Callable[['ProcessingContext'], bool],
                 true_step: PipelineStep,
                 false_step: Optional[PipelineStep] = None,
                 **kwargs):
        """
        初始化條件步驟
        
        Args:
            name: 步驟名稱
            condition: 條件函數
            true_step: 條件為真時執行的步驟
            false_step: 條件為假時執行的步驟（可選）
        """
        super().__init__(name, **kwargs)
        self.condition = condition
        self.true_step = true_step
        self.false_step = false_step
    
    async def execute(self, context: 'ProcessingContext') -> StepResult:
        """執行條件步驟"""
        try:
            # 評估條件
            condition_result = self.condition(context)
            
            # 根據條件執行相應步驟
            if condition_result:
                self.logger.info(f"Condition met, executing {self.true_step.name}")
                return await self.true_step(context)
            elif self.false_step:
                self.logger.info(f"Condition not met, executing {self.false_step.name}")
                return await self.false_step(context)
            else:
                self.logger.info("Condition not met, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message="Condition not met, no false step defined"
                )
                
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Conditional step failed: {str(e)}"
            )
    
    async def validate_input(self, context: 'ProcessingContext') -> bool:
        """驗證輸入"""
        return True  # 條件步驟本身不驗證，由子步驟驗證


class ParallelStep(PipelineStep):
    """
    並行步驟：同時執行多個步驟
    """
    
    def __init__(self,
                 name: str,
                 steps: List[PipelineStep],
                 fail_fast: bool = False,
                 **kwargs):
        """
        初始化並行步驟
        
        Args:
            name: 步驟名稱
            steps: 要並行執行的步驟列表
            fail_fast: 是否快速失敗（任一步驟失敗即停止）
        """
        super().__init__(name, **kwargs)
        self.steps = steps
        self.fail_fast = fail_fast
    
    async def execute(self, context: 'ProcessingContext') -> StepResult:
        """並行執行所有步驟"""
        try:
            # 創建所有任務
            tasks = [step(context) for step in self.steps]
            
            if self.fail_fast:
                # 快速失敗模式：任何步驟失敗立即返回
                results = []
                for task in asyncio.as_completed(tasks):
                    result = await task
                    if result.is_failed:
                        # 取消其他任務
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        return StepResult(
                            step_name=self.name,
                            status=StepStatus.FAILED,
                            message=f"Step {result.step_name} failed",
                            metadata={'failed_step': result.step_name}
                        )
                    results.append(result)
            else:
                # 等待所有任務完成
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 檢查結果
            failed_steps = []
            success_steps = []
            
            for result in results:
                if isinstance(result, Exception):
                    failed_steps.append(str(result))
                elif isinstance(result, StepResult):
                    if result.is_failed:
                        failed_steps.append(result.step_name)
                    else:
                        success_steps.append(result.step_name)
            
            if failed_steps:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=f"Failed steps: {', '.join(failed_steps)}",
                    metadata={
                        'failed': failed_steps,
                        'succeeded': success_steps
                    }
                )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"All {len(self.steps)} steps completed successfully",
                metadata={'completed_steps': success_steps}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Parallel execution failed: {str(e)}"
            )
    
    async def validate_input(self, context: 'ProcessingContext') -> bool:
        """驗證輸入"""
        return len(self.steps) > 0


class SequentialStep(PipelineStep):
    """
    順序步驟：依序執行多個步驟
    """
    
    def __init__(self,
                 name: str,
                 steps: List[PipelineStep],
                 stop_on_failure: bool = True,
                 **kwargs):
        """
        初始化順序步驟
        
        Args:
            name: 步驟名稱
            steps: 要順序執行的步驟列表
            stop_on_failure: 失敗時是否停止
        """
        super().__init__(name, **kwargs)
        self.steps = steps
        self.stop_on_failure = stop_on_failure
    
    async def execute(self, context: 'ProcessingContext') -> StepResult:
        """順序執行所有步驟"""
        completed_steps = []
        
        try:
            for step in self.steps:
                self.logger.info(f"Executing step {step.name}")
                result = await step(context)
                
                completed_steps.append(result)
                
                if result.is_failed and self.stop_on_failure:
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.FAILED,
                        message=f"Step {step.name} failed",
                        metadata={
                            'failed_at': step.name,
                            'completed': [s.step_name for s in completed_steps]
                        }
                    )
            
            # 檢查是否有失敗的步驟
            failed_steps = [s.step_name for s in completed_steps if s.is_failed]
            
            if failed_steps:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=f"Some steps failed: {', '.join(failed_steps)}",
                    metadata={
                        'failed': failed_steps,
                        'total': len(self.steps)
                    }
                )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"All {len(self.steps)} steps completed successfully",
                metadata={'completed_steps': [s.step_name for s in completed_steps]}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Sequential execution failed: {str(e)}",
                metadata={'completed_steps': [s.step_name for s in completed_steps]}
            )
    
    async def validate_input(self, context: 'ProcessingContext') -> bool:
        """驗證輸入"""
        return len(self.steps) > 0
