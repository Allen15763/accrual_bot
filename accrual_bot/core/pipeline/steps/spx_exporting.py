import os
import time
import pandas as pd

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext


class SPXExportStep(PipelineStep):
    """
    SPX 導出步驟
    
    功能: 將處理完成的數據導出到 Excel
    
    輸入: Processed DataFrame
    輸出: Excel file path
    """
    
    def __init__(self, 
                 name: str = "SPXExport",
                 output_dir: str = "output",
                 **kwargs):
        super().__init__(name, description="Export SPX processed data", **kwargs)
        self.output_dir = output_dir
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行導出"""
        start_time = time.time()
        try:
            df = context.data.copy()
            
            # 清理 <NA> 值
            df_export = df.replace('<NA>', pd.NA)
            
            # 生成文件名
            processing_date = context.metadata.processing_date
            entity_type = context.metadata.entity_type
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}.xlsx"
            
            # 創建輸出目錄
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            
            output_path = os.path.join(self.output_dir, filename)
            
            # 確保文件名唯一
            counter = 1
            while os.path.exists(output_path):
                filename = f"{entity_type}_PO_{processing_date}_processed_{timestamp}_{counter}.xlsx"
                output_path = os.path.join(self.output_dir, filename)
                counter += 1
            
            # 導出 Excel
            df_export.to_excel(output_path, index=False)
            
            self.logger.info(f"Data exported to: {output_path}")
            duration = time.time() - start_time
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {output_path}",
                duration=duration,
                metadata={
                    'output_path': output_path,
                    'rows_exported': len(df_export),
                    'columns_exported': len(df_export.columns)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Export failed: {str(e)}", exc_info=True)
            context.add_error(f"Export failed: {str(e)}")
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
            self.logger.error("No data to export")
            return False
        
        return True