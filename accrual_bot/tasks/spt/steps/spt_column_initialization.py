"""
Column Initialization Step

欄位初始化步驟 - 確保狀態欄位存在
"""

import pandas as pd
import time
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


class ColumnInitializationStep(PipelineStep):
    """
    欄位初始化步驟

    功能:
    - 初始化狀態欄位 (PO狀態/PR狀態)
    - 確保在狀態評估前建立空白欄位
    """

    def __init__(self, status_column: str = "PO狀態", **kwargs):
        super().__init__(**kwargs)
        self.status_column = status_column

    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行欄位初始化"""
        start_time = time.time()
        start_datetime = datetime.now()

        try:
            df = context.data.copy()

            if self._is_pr(context):
                self.status_column = "PR狀態"
                df['PR Line'] = df['PR#'].fillna('') + '-' + df['Line#'].fillna('')
                df['Supplier'] = df['PR Supplier']
            else:
                df['PO Line'] = df['PO#'].fillna('') + '-' + df['Line#'].fillna('')
                df['Supplier'] = df['PO Supplier']
                pass

            # 初始化狀態欄位
            if self.status_column not in df.columns:
                df[self.status_column] = pd.NA
                self.logger.info(f"Created empty status column: {self.status_column}")
                created = True
            else:
                self.logger.info(f"Status column '{self.status_column}' already exists")
                created = False

            df['Remarked by Procurement'] = pd.NA
            context.update_data(df)
            duration = time.time() - start_time

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Initialized {self.status_column} column",
                duration=duration,
                metadata={
                    'status_column': self.status_column,
                    'created': created
                }
            )

        except Exception as e:
            self.logger.error(f"Column initialization failed: {str(e)}", exc_info=True)
            context.add_error(f"Column initialization failed: {str(e)}")
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                duration=duration,
                message=str(e)
            )

    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for column initialization")
            return False
        return True
    
    def _is_pr(self, context) -> bool:
        var = context.metadata.processing_type
        if var == 'PR':
            return True
        else:
            return False
        
