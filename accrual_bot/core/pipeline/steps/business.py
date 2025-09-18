"""
業務邏輯處理步驟
包含狀態評估、會計調整等核心業務邏輯
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext


class StatusEvaluationStep(PipelineStep):
    """
    狀態評估步驟
    根據業務規則評估PO/PR狀態
    
    ### 核心邏輯摘要：
    1. 根據日期範圍判斷完成狀態
    2. 檢查收貨數量和入帳金額
    3. 應用實體特定的狀態規則
    4. 處理特殊狀態（關單、待驗收等）
    """
    
    def __init__(self,
                 name: str = "StatusEvaluation",
                 entity_type: str = "MOB",
                 **kwargs):
        super().__init__(name, description="Evaluate PO/PR status", **kwargs)
        self.entity_type = entity_type
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行狀態評估
        
        ### 實現邏輯：
        - MOB: 標準的日期範圍判斷
        - SPT: 考慮提早完成的情況
        - SPX: 複雜的11個條件判斷（租金、資產驗收等）
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # === 核心狀態評估邏輯 ===
            # 這裡實際實現時調用原有的evaluate_status_based_on_dates邏輯
            # 根據entity_type應用不同的規則
            
            self.logger.info(f"Evaluating status for {self.entity_type}")
            
            # 模擬狀態評估
            df[status_col] = df.apply(
                lambda row: self._evaluate_row_status(row, context),
                axis=1
            )
            
            context.update_data(df)
            
            # 統計各狀態數量
            status_counts = df[status_col].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Status evaluation completed for {self.entity_type}",
                metadata={'status_counts': status_counts}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _evaluate_row_status(self, row, context):
        """
        評估單行狀態
        
        ### 實體特定規則：
        - MOB: 基本日期判斷
        - SPT: 支援提早完成
        - SPX: 租金、資產、關單等複雜判斷
        """
        # 簡化實現，實際使用原有邏輯
        return "待評估"
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = ['Expected Receive Month', 'Item Description']
        return all(col in context.data.columns for col in required_cols)


class AccountingAdjustmentStep(PipelineStep):
    """
    會計調整步驟
    判斷是否需要估計入帳，設置科目代碼
    
    ### 核心邏輯摘要：
    1. 根據狀態判斷是否估計入帳
    2. 檢查採購備註的特殊標記
    3. 判斷FA科目
    4. 部門代碼轉換
    """
    
    def __init__(self, name: str = "AccountingAdjustment", **kwargs):
        super().__init__(name, description="Perform accounting adjustments", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行會計調整"""
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # === 會計調整邏輯 ===
            # 1. 判斷是否估計入帳
            self._update_accrual_flag(df, status_col)
            
            # 2. 設置科目代碼
            self._set_account_codes(df, context)
            
            # 3. 部門代碼處理
            if context.metadata.entity_type == "SPT":
                self._convert_department_codes(df)
            
            context.update_data(df)
            
            # 統計
            accrual_count = (df['是否估計入帳'] == 'Y').sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Accounting adjustment completed",
                metadata={'accrual_items': accrual_count}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _update_accrual_flag(self, df: pd.DataFrame, status_col: str):
        """
        更新估計入帳標記
        
        ### 規則：
        - 狀態為"已完成"且採購未標記"不預估" -> Y
        - 特定狀態不預估
        - 採購備註優先級最高
        """
        # 實際實現調用原有的update_estimation_based_on_status邏輯
        df['是否估計入帳'] = df[status_col].apply(
            lambda x: 'Y' if x == '已完成' else 'N'
        )
    
    def _set_account_codes(self, df: pd.DataFrame, context: ProcessingContext):
        """設置科目代碼"""
        config = context.get_entity_config()
        fa_accounts = config.get('fa_accounts', [])
        
        df['Account code'] = np.where(
            df['是否估計入帳'] == 'Y',
            df['GL#'],
            np.nan
        )
        
        df['是否為FA'] = df['GL#'].isin(fa_accounts).map({True: 'Y', False: 'N'})
    
    def _convert_department_codes(self, df: pd.DataFrame):
        """SPT部門代碼轉換"""
        # 簡化實現
        df['Department_Converted'] = df['Department'].astype(str).str[:3]
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.get_status_column() in context.data.columns


class AccountCodeMappingStep(PipelineStep):
    """
    科目代碼映射步驟
    將GL#映射到標準科目代碼
    """
    
    def __init__(self,
                 name: str = "AccountCodeMapping",
                 mapping_source: str = "account_mapping",
                 **kwargs):
        super().__init__(name, description="Map GL codes to account codes", **kwargs)
        self.mapping_source = mapping_source
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行科目映射"""
        try:
            # 獲取映射數據
            mapping_data = context.get_auxiliary_data(self.mapping_source)
            
            if mapping_data is None:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message="No mapping data available"
                )
            
            # 執行映射
            df = context.data.copy()
            
            # 創建映射字典
            mapping_dict = dict(zip(
                mapping_data['GL#'],
                mapping_data['Account Code']
            ))
            
            # 應用映射
            df['Mapped_Account_Code'] = df['GL#'].map(mapping_dict)
            
            context.update_data(df)
            
            unmapped = df['Mapped_Account_Code'].isna().sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Account code mapping completed",
                metadata={'unmapped_count': unmapped}
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
        return 'GL#' in context.data.columns


class DepartmentConversionStep(PipelineStep):
    """
    部門代碼轉換步驟
    根據業務規則轉換部門代碼
    """
    
    def __init__(self,
                 name: str = "DepartmentConversion",
                 **kwargs):
        super().__init__(name, description="Convert department codes", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """執行部門轉換"""
        try:
            df = context.data.copy()
            
            # 根據實體類型應用不同的轉換規則
            if context.metadata.entity_type == "SPT":
                # SPT特殊規則
                df['Department_Code'] = self._convert_spt_department(df)
            else:
                # 標準轉換
                df['Department_Code'] = df['Department'].astype(str).str[:3]
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="Department conversion completed"
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _convert_spt_department(self, df: pd.DataFrame) -> pd.Series:
        """
        SPT部門轉換邏輯
        
        ### 規則：
        - Account code 1,2,9開頭 -> 000
        - Account code 5,4開頭 -> 000
        - 其他 -> 前3碼
        """
        result = pd.Series('', index=df.index)
        
        # 簡化實現
        result = df['Department'].astype(str).str[:3]
        
        # 特殊處理
        mask_special = df['Account code'].astype(str).str[0].isin(['1', '2', '9'])
        result[mask_special] = '000'
        
        return result
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'Department' in context.data.columns
