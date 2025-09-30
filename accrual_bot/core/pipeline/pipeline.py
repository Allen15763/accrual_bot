"""
Pipeline 主類
管理和執行步驟序列
"""

from typing import List, Optional, Dict, Any, Union
from dataclasses import dataclass, field
import asyncio
import logging
from datetime import datetime
import pandas as pd

from .base import PipelineStep, StepResult, StepStatus, SequentialStep
from .context import ProcessingContext


@dataclass
class PipelineConfig:
    """Pipeline配置"""
    name: str
    description: str = ""
    entity_type: str = "MOB"
    stop_on_error: bool = True
    parallel_execution: bool = False
    max_concurrent_steps: int = 5
    enable_cache: bool = False
    log_level: str = "INFO"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'description': self.description,
            'entity_type': self.entity_type,
            'stop_on_error': self.stop_on_error,
            'parallel_execution': self.parallel_execution,
            'max_concurrent_steps': self.max_concurrent_steps,
            'enable_cache': self.enable_cache,
            'log_level': self.log_level
        }


class Pipeline:
    """
    Pipeline 主類
    組合和管理處理步驟
    """
    
    def __init__(self, config: PipelineConfig):
        """
        初始化Pipeline
        
        Args:
            config: Pipeline配置
        """
        self.config = config
        self.steps: List[PipelineStep] = []
        self.logger = logging.getLogger(f"Pipeline.{config.name}")
        
        # 設置日誌級別
        self.logger.setLevel(getattr(logging, config.log_level))
        
        # 執行統計
        self._execution_count = 0
        self._last_execution = None
        self._execution_history = []
    
    def add_step(self, step: PipelineStep) -> 'Pipeline':
        """
        添加處理步驟
        
        Args:
            step: 處理步驟
            
        Returns:
            Pipeline: 自身（用於鏈式調用）
        """
        self.steps.append(step)
        self.logger.debug(f"Added step: {step.name}")
        return self
    
    def add_steps(self, steps: List[PipelineStep]) -> 'Pipeline':
        """
        批量添加處理步驟
        
        Args:
            steps: 處理步驟列表
            
        Returns:
            Pipeline: 自身
        """
        for step in steps:
            self.add_step(step)
        return self
    
    def remove_step(self, step_name: str) -> bool:
        """
        移除指定步驟
        
        Args:
            step_name: 步驟名稱
            
        Returns:
            bool: 是否成功移除
        """
        initial_count = len(self.steps)
        self.steps = [s for s in self.steps if s.name != step_name]
        return len(self.steps) < initial_count
    
    def get_step(self, step_name: str) -> Optional[PipelineStep]:
        """
        獲取指定步驟
        
        Args:
            step_name: 步驟名稱
            
        Returns:
            Optional[PipelineStep]: 步驟或None
        """
        for step in self.steps:
            if step.name == step_name:
                return step
        return None
    
    def clear_steps(self):
        """清空所有步驟"""
        self.steps.clear()
        self.logger.debug("Cleared all steps")
    
    async def execute(self, context: ProcessingContext) -> Dict[str, Any]:
        """
        執行Pipeline
        
        Args:
            context: 處理上下文
            
        Returns:
            Dict[str, Any]: 執行結果
        """
        start_time = datetime.now()
        self._execution_count += 1
        
        self.logger.info(f"Starting pipeline execution #{self._execution_count}")
        self.logger.info(f"Context: {context}")
        
        results = []
        failed = False
        
        try:
            if self.config.parallel_execution:
                # 並行執行模式
                results = await self._execute_parallel(context)
            else:
                # 順序執行模式
                results = await self._execute_sequential(context)
            
            # 檢查結果
            failed_steps = [r for r in results if r.status == StepStatus.FAILED]
            success_steps = [r for r in results if r.status == StepStatus.SUCCESS]
            skipped_steps = [r for r in results if r.status == StepStatus.SKIPPED]
            
            if failed_steps:
                failed = True
                self.logger.error(f"Pipeline failed with {len(failed_steps)} failed steps")
            else:
                self.logger.info("Pipeline completed successfully")
            
            # 構建執行結果
            execution_result = {
                'pipeline': self.config.name,
                'success': not failed,
                'start_time': start_time,
                'end_time': datetime.now(),
                'duration': (datetime.now() - start_time).total_seconds(),
                'total_steps': len(self.steps),
                'executed_steps': len(results),
                'successful_steps': len(success_steps),
                'failed_steps': len(failed_steps),
                'skipped_steps': len(skipped_steps),
                'results': [r.to_dict() for r in results],
                'context_summary': context.to_dict(),
                'errors': context.errors,
                'warnings': context.warnings
            }
            
            # 記錄執行歷史
            self._last_execution = execution_result
            self._execution_history.append({
                'timestamp': start_time,
                'success': not failed,
                'duration': execution_result['duration']
            })
            
            return execution_result
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return {
                'pipeline': self.config.name,
                'success': False,
                'error': str(e),
                'start_time': start_time,
                'end_time': datetime.now(),
                'duration': (datetime.now() - start_time).total_seconds()
            }
    
    async def _execute_sequential(self, context: ProcessingContext) -> List[StepResult]:
        """
        順序執行步驟
        
        Args:
            context: 處理上下文
            
        Returns:
            List[StepResult]: 執行結果列表
        """
        results = []
        
        for i, step in enumerate(self.steps, 1):
            self.logger.info(f"Executing step {i}/{len(self.steps)}: {step.name}")
            
            # 執行步驟
            result = await step(context)
            results.append(result)
            
            # 記錄到上下文歷史
            context.add_history(step.name, result.status.value)
            
            # 檢查是否需要停止
            if result.status == StepStatus.FAILED and self.config.stop_on_error:
                self.logger.error(f"Stopping pipeline due to failed step: {step.name}")
                break
        
        return results
    
    async def _execute_parallel(self, context: ProcessingContext) -> List[StepResult]:
        """
        並行執行步驟
        
        Args:
            context: 處理上下文
            
        Returns:
            List[StepResult]: 執行結果列表
        """
        results = []
        semaphore = asyncio.Semaphore(self.config.max_concurrent_steps)
        
        async def execute_with_semaphore(step: PipelineStep) -> StepResult:
            async with semaphore:
                return await step(context)
        
        # 創建所有任務
        tasks = [execute_with_semaphore(step) for step in self.steps]
        
        if self.config.stop_on_error:
            # 快速失敗模式
            for task in asyncio.as_completed(tasks):
                result = await task
                results.append(result)
                
                if result.status == StepStatus.FAILED:
                    # 取消其他任務
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
        else:
            # 等待所有任務完成
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 處理異常
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
                    processed_results.append(
                        StepResult(
                            step_name="Unknown",
                            status=StepStatus.FAILED,
                            error=result,
                            message=str(result)
                        )
                    )
                else:
                    processed_results.append(result)
            
            results = processed_results
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        獲取執行統計
        
        Returns:
            Dict[str, Any]: 統計信息
        """
        return {
            'total_executions': self._execution_count,
            'last_execution': self._last_execution,
            'history': self._execution_history,
            'total_steps': len(self.steps),
            'step_names': [s.name for s in self.steps]
        }
    
    def clone(self) -> 'Pipeline':
        """
        克隆Pipeline
        
        Returns:
            Pipeline: 新的Pipeline實例
        """
        new_pipeline = Pipeline(self.config)
        new_pipeline.steps = self.steps.copy()
        return new_pipeline
    
    def __repr__(self) -> str:
        return f"Pipeline(name={self.config.name}, steps={len(self.steps)})"


class PipelineBuilder:
    """
    Pipeline構建器
    提供流式API來構建Pipeline
    """
    
    def __init__(self, name: str, entity_type: str = "MOB"):
        """
        初始化構建器
        
        Args:
            name: Pipeline名稱
            entity_type: 實體類型
        """
        self.config = PipelineConfig(name=name, entity_type=entity_type)
        self.steps = []
    
    def with_description(self, description: str) -> 'PipelineBuilder':
        """設置描述"""
        self.config.description = description
        return self
    
    def with_stop_on_error(self, stop: bool = True) -> 'PipelineBuilder':
        """設置是否遇錯停止"""
        self.config.stop_on_error = stop
        return self
    
    def with_parallel_execution(self, parallel: bool = True) -> 'PipelineBuilder':
        """設置是否並行執行"""
        self.config.parallel_execution = parallel
        return self
    
    def with_max_concurrent(self, max_concurrent: int) -> 'PipelineBuilder':
        """設置最大併發數"""
        self.config.max_concurrent_steps = max_concurrent
        return self
    
    def add_step(self, step: PipelineStep) -> 'PipelineBuilder':
        """添加步驟"""
        self.steps.append(step)
        return self
    
    def add_steps(self, *steps: PipelineStep) -> 'PipelineBuilder':
        """添加多個步驟"""
        self.steps.extend(steps)
        return self
    
    def build(self) -> Pipeline:
        """
        構建Pipeline
        
        Returns:
            Pipeline: 構建好的Pipeline
        """
        pipeline = Pipeline(self.config)
        pipeline.add_steps(self.steps)
        return pipeline


class PipelineExecutor:
    """
    Pipeline執行器
    管理多個Pipeline的執行
    """
    
    def __init__(self):
        """初始化執行器"""
        self.pipelines: Dict[str, Pipeline] = {}
        self.logger = logging.getLogger("PipelineExecutor")
        self._running_pipelines = set()
    
    def register_pipeline(self, pipeline: Pipeline):
        """
        註冊Pipeline
        
        Args:
            pipeline: Pipeline實例
        """
        self.pipelines[pipeline.config.name] = pipeline
        self.logger.info(f"Registered pipeline: {pipeline.config.name}")
    
    def unregister_pipeline(self, name: str) -> bool:
        """
        取消註冊Pipeline
        
        Args:
            name: Pipeline名稱
            
        Returns:
            bool: 是否成功
        """
        if name in self.pipelines:
            del self.pipelines[name]
            self.logger.info(f"Unregistered pipeline: {name}")
            return True
        return False
    
    def get_pipeline(self, name: str) -> Optional[Pipeline]:
        """
        獲取Pipeline
        
        Args:
            name: Pipeline名稱
            
        Returns:
            Optional[Pipeline]: Pipeline或None
        """
        return self.pipelines.get(name)
    
    async def execute_pipeline(self,
                               name: str,
                               data: pd.DataFrame,
                               processing_date: int,
                               **kwargs) -> Dict[str, Any]:
        """
        執行指定Pipeline
        
        Args:
            name: Pipeline名稱
            data: 輸入數據
            processing_date: 處理日期
            **kwargs: 其他參數
            
        Returns:
            Dict[str, Any]: 執行結果
        """
        pipeline = self.pipelines.get(name)
        if not pipeline:
            raise ValueError(f"Pipeline {name} not found")
        
        # 檢查是否正在運行
        if name in self._running_pipelines:
            self.logger.warning(f"Pipeline {name} is already running")
            return {'success': False, 'error': 'Pipeline already running'}
        
        try:
            self._running_pipelines.add(name)
            
            # 創建處理上下文
            context = ProcessingContext(
                data=data,
                entity_type=pipeline.config.entity_type,
                processing_date=processing_date,
                processing_type=kwargs.get('processing_type', 'PO')
            )
            
            # 添加輔助數據（如果有）
            auxiliary_data = kwargs.get('auxiliary_data', {})
            for aux_name, aux_data in auxiliary_data.items():
                context.add_auxiliary_data(aux_name, aux_data)
            
            # 執行Pipeline
            result = await pipeline.execute(context)
            
            # 如果成功，返回處理後的數據
            if result['success']:
                result['output_data'] = context.data
            
            return result
            
        finally:
            self._running_pipelines.discard(name)
    
    async def execute_multiple(self,
                               pipelines: List[str],
                               data: pd.DataFrame,
                               processing_date: int,
                               **kwargs) -> Dict[str, Dict[str, Any]]:
        """
        執行多個Pipeline
        
        Args:
            pipelines: Pipeline名稱列表
            data: 輸入數據
            processing_date: 處理日期
            **kwargs: 其他參數
            
        Returns:
            Dict[str, Dict[str, Any]]: 執行結果字典
        """
        tasks = []
        for pipeline_name in pipelines:
            task = self.execute_pipeline(
                pipeline_name,
                data.copy(),  # 每個Pipeline使用數據副本
                processing_date,
                **kwargs
            )
            tasks.append((pipeline_name, task))
        
        results = {}
        for pipeline_name, task in tasks:
            try:
                result = await task
                results[pipeline_name] = result
            except Exception as e:
                results[pipeline_name] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results
    
    def list_pipelines(self) -> List[str]:
        """
        列出所有已註冊的Pipeline
        
        Returns:
            List[str]: Pipeline名稱列表
        """
        return list(self.pipelines.keys())
    
    def get_pipeline_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        獲取Pipeline信息
        
        Args:
            name: Pipeline名稱
            
        Returns:
            Optional[Dict[str, Any]]: Pipeline信息或None
        """
        pipeline = self.pipelines.get(name)
        if pipeline:
            return {
                'config': pipeline.config.to_dict(),
                'steps': [s.name for s in pipeline.steps],
                'statistics': pipeline.get_statistics()
            }
        return None
