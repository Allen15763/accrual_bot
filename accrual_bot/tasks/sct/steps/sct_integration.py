"""
數據整合與清理

- 前期底稿、採購底稿與摘要期間解析(DateLogicStep)為通用步驟放在common
"""
import time
import pandas as pd
from datetime import datetime

from accrual_bot.core.pipeline.base import PipelineStep, StepResult, StepStatus
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.steps.common import (
    StepMetadataBuilder, 
    create_error_metadata
)


class APInvoiceIntegrationStep(PipelineStep):
    """
    AP Invoice 整合步驟
    
    功能:
    從 AP Invoice 數據中提取 VOUCHER_NUMBER 並填入 PO 數據
    排除月份 m 之後的期間
    
    輸入: DataFrame + AP Invoice auxiliary data
    輸出: DataFrame with VOUCHER_NUMBER column
    """
    
    def __init__(self, name: str = "APInvoiceIntegration", **kwargs):
        super().__init__(name, description="Integrate AP Invoice VOUCHER_NUMBER", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行 AP Invoice 整合"""
        start_time = time.time()
        start_datetime = datetime.now()
        try:
            df = context.data.copy()
            input_count = len(df)
            df_ap = context.get_auxiliary_data('ap_invoice')
            yyyymm = context.get_variable('processing_date')
            
            if df_ap is None or df_ap.empty:
                self.logger.warning("No AP Invoice data available, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    data=df,
                    message="No AP Invoice data"
                )
            
            self.logger.info("Processing AP Invoice integration...")
            
            # 移除缺少 'PO Number' 的行
            df_ap = df_ap.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # 創建組合鍵
            df_ap['po_line'] = (
                df_ap['Company'].astype('string') + '-' + 
                df_ap['PO Number'].astype('string') + '-' + 
                df_ap['PO_LINE_NUMBER'].astype('string')
            )
            
            # 轉換 Period 為 yyyymm 格式
            df_ap['period'] = (
                pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
                .dt.strftime('%Y%m')
                .fillna('0')
                .astype('Int32')
            )
            
            df_ap['match_type'] = df_ap['Match Type'].fillna('system_filled')
            df_ap['voucher_number'] = df_ap['VOUCHER_NUMBER'].fillna('system_filled')
            
            # 只保留期間在 yyyymm 之前的 AP 發票
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # 合併到主 DataFrame
            df = df.merge(
                df_ap[['po_line', 'voucher_number']], 
                left_on='PO Line', 
                right_on='po_line', 
                how='left'
            )
            
            df.drop(columns=['po_line'], inplace=True)
            
            context.update_data(df)
            
            matched_count = df['voucher_number'].notna().sum()
            output_count = len(df)
            
            duration = time.time() - start_time
            end_datetime = datetime.now()
            self.logger.info(f"AP Invoice integration completed: {matched_count} records matched")
            
            metadata = (StepMetadataBuilder()
                        .set_row_counts(input_count, output_count)
                        .set_process_counts(processed=output_count)
                        .set_time_info(start_datetime, end_datetime)
                        .add_custom('matched_records', int(matched_count))
                        .add_custom('total_records', len(df))
                        .build())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Integrated GL DATE for {matched_count} records",
                duration=duration,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"AP Invoice integration failed: {str(e)}", exc_info=True)
            context.add_error(f"AP Invoice integration failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data for AP Invoice integration")
            return False
        
        if 'PO Line' not in context.data.columns:
            self.logger.error("Missing 'PO Line' column")
            return False
        
        return True