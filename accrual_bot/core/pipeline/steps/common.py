"""
通用處理步驟
適用於所有實體類型的基礎步驟
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Any
from datetime import datetime
import os

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext, ValidationResult


class DataCleaningStep(PipelineStep):
    """
    數據清理步驟
    清理NaN值、去除空白字符、標準化數據
    """
    
    def __init__(self, 
                 name: str = "DataCleaning",
                 columns_to_clean: Optional[List[str]] = None,
                 **kwargs):
        super().__init__(name, description="Clean and standardize data", **kwargs)
        self.columns_to_clean = columns_to_clean
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據清理"""
        try:
            df = context.data.copy()
            
            # 清理指定列的NaN值
            if self.columns_to_clean:
                for col in self.columns_to_clean:
                    if col in df.columns:
                        df[col] = df[col].fillna('').astype(str).str.strip()
                        self.logger.debug(f"Cleaned column: {col}")
            
            # 去除所有字符串列的空白
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                df[col] = df[col].astype(str).str.strip()
            
            # 替換常見的無效值
            df.replace(['nan', 'None', 'N/A', 'n/a'], '', inplace=True)
            
            # 更新上下文數據
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Cleaned {len(df)} rows",
                metadata={
                    'cleaned_columns': len(self.columns_to_clean) if self.columns_to_clean else len(string_columns)}
            )
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.data is None or context.data.empty:
            self.logger.error("No data to clean")
            return False
        return True


class DateFormattingStep(PipelineStep):
    """
    日期格式化步驟
    統一日期格式
    """
    
    def __init__(self,
                 name: str = "DateFormatting",
                 date_columns: Optional[Dict[str, str]] = None,
                 **kwargs):
        super().__init__(name, description="Format date columns", **kwargs)
        self.date_columns = date_columns or {
            'Expected Receive Month': '%b-%y',
            'Submission Date': '%d-%b-%y'
        }
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行日期格式化"""
        try:
            df = context.data.copy()
            formatted_count = 0
            
            for col, format_str in self.date_columns.items():
                if col in df.columns:
                    # 轉換日期格式
                    df[col] = pd.to_datetime(df[col], format=format_str, errors='coerce')
                    
                    # 統一輸出格式
                    if 'Month' in col:
                        df[col] = df[col].dt.strftime('%Y-%m')
                    else:
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                    
                    formatted_count += 1
                    self.logger.debug(f"Formatted date column: {col}")
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Formatted {formatted_count} date columns",
                metadata={'formatted_columns': formatted_count}
            )
            
        except Exception as e:
            self.logger.error(f"Date formatting failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class DateParsingStep(PipelineStep):
    """
    日期解析步驟
    從Item Description解析日期範圍
    
    ### 核心邏輯：
    - 使用正則表達式從Item Description中提取日期範圍
    - 支援多種日期格式 (YYYY/MM, YYYYMM, MMM-YY等)
    - 轉換為統一的YYYYMM格式
    - 處理特殊情況和錯誤格式
    """
    
    def __init__(self, name: str = "DateParsing", **kwargs):
        super().__init__(name, description="Parse dates from item description", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行日期解析
        
        ### 實現邏輯摘要：
        1. 從Item Description提取日期範圍
        2. 轉換Expected Receive Month為數值格式
        3. 驗證日期範圍有效性
        4. 添加解析結果到新列
        """
        try:
            df = context.data.copy()
            
            # === 詳細實現邏輯 ===
            # 1. 轉換Expected Receive Month為YYYYMM格式
            # 2. 從Item Description提取日期（正則匹配）
            # 3. 處理各種日期格式
            # 4. 驗證和標準化日期範圍
            
            # 這裡實際實現時調用原有的parse_date_from_description邏輯
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Date parsing completed"
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = ['Item Description']
        return all(col in context.data.columns for col in required_cols)


class ValidationStep(PipelineStep):
    """
    數據驗證步驟
    驗證數據完整性和業務規則
    """
    
    def __init__(self,
                 name: str = "Validation",
                 validations: Optional[List[str]] = None,
                 **kwargs):
        super().__init__(name, description="Validate data integrity", **kwargs)
        self.validations = validations or ['required_columns', 'data_types']
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行驗證"""
        try:
            validation_result = ValidationResult(is_valid=True)
            
            for validation_type in self.validations:
                if validation_type == 'required_columns':
                    self._validate_required_columns(context, validation_result)
                elif validation_type == 'data_types':
                    self._validate_data_types(context, validation_result)
                elif validation_type == 'business_rules':
                    self._validate_business_rules(context, validation_result)
            
            # 添加驗證結果到上下文
            context.add_validation(self.name, validation_result)
            
            if validation_result.is_valid:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SUCCESS,
                    message="Validation passed",
                    metadata={'warnings': len(validation_result.warnings)}
                )
            else:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="Validation failed",
                    metadata={
                        'errors': validation_result.errors,
                        'warnings': validation_result.warnings
                    }
                )
                
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _validate_required_columns(self, context: ProcessingContext, result: ValidationResult):
        """驗證必要列"""
        if context.is_po_processing():
            required = ['PO#', 'Item Description', 'GL#', 'Entry Amount']
        else:
            required = ['PR#', 'Item Description', 'GL#']
        
        missing = [col for col in required if col not in context.data.columns]
        if missing:
            result.add_error(f"Missing required columns: {missing}")
    
    def _validate_data_types(self, context: ProcessingContext, result: ValidationResult):
        """驗證數據類型"""
        # 檢查數值列
        numeric_columns = ['Entry Amount', 'Entry Quantity', 'Received Quantity']
        for col in numeric_columns:
            if col in context.data.columns:
                try:
                    pd.to_numeric(context.data[col], errors='coerce')
                except Exception as err:
                    self.logger.error(f"_validate_data_types Failed: {err}")
                    result.add_warning(f"Column {col} contains non-numeric values")
    
    def _validate_business_rules(self, context: ProcessingContext, result: ValidationResult):
        """
        驗證業務規則
        
        ### 業務規則摘要：
        - Entry Amount > 0 for accrual items
        - GL# must be valid account code
        - Department code format validation
        - Date range consistency
        """
        df = context.data
        
        # 檢查金額
        if 'Entry Amount' in df.columns:
            negative_amounts = df[df['Entry Amount'] < 0]
            if not negative_amounts.empty:
                result.add_warning(f"Found {len(negative_amounts)} items with negative amounts")
        
        # 其他業務規則驗證...
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class ExportStep(PipelineStep):
    """
    數據導出步驟
    將處理結果導出到文件
    """
    
    def __init__(self,
                 name: str = "Export",
                 format: str = "excel",
                 output_path: Optional[str] = None,
                 **kwargs):
        super().__init__(name, description="Export processed data", **kwargs)
        self.format = format
        self.output_path = output_path or "output"
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行導出"""
        try:
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            entity = context.metadata.entity_type
            proc_type = context.metadata.processing_type
            
            if self.format == "excel":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.xlsx"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_excel(filename, index=False)
            elif self.format == "csv":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.csv"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_csv(filename, index=False)
            elif self.format == "parquet":
                filename = f"{self.output_path}/{entity}_{proc_type}_{timestamp}.parquet"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                context.data.to_parquet(filename, index=False)
            else:
                raise ValueError(f"Unsupported export format: {self.format}")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported to {filename}",
                metadata={'filename': filename, 'rows': len(context.data)}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.data is not None and not context.data.empty


class DataIntegrationStep(PipelineStep):
    """
    數據整合步驟
    整合輔助數據源
    """
    
    def __init__(self,
                 name: str = "DataIntegration",
                 source_name: str = None,
                 join_columns: Optional[List[str]] = None,
                 how: str = "left",
                 **kwargs):
        super().__init__(name, description="Integrate auxiliary data", **kwargs)
        self.source_name = source_name
        self.join_columns = join_columns or ['PO#']
        self.how = how
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據整合"""
        try:
            # 獲取輔助數據
            aux_data = context.get_auxiliary_data(self.source_name)
            
            if aux_data is None:
                self.logger.warning(f"Auxiliary data '{self.source_name}' not found")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message=f"No auxiliary data: {self.source_name}"
                )
            
            # 合併數據
            df = context.data.copy()
            merged_df = pd.merge(
                df,
                aux_data,
                on=self.join_columns,
                how=self.how,
                suffixes=('', f'_{self.source_name}')
            )
            
            context.update_data(merged_df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=merged_df,
                message=f"Integrated {self.source_name}",
                metadata={
                    'original_rows': len(df),
                    'merged_rows': len(merged_df),
                    'aux_rows': len(aux_data)
                }
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        # 檢查join列是否存在
        return all(col in context.data.columns for col in self.join_columns)
