"""
MOB實體特定處理步驟
包含MOB特有的業務邏輯
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext


class MOBStatusStep(PipelineStep):
    """
    MOB狀態判斷步驟
    實現MOB特定的狀態評估邏輯
    """
    
    def __init__(self, name: str = "MOBStatus", **kwargs):
        super().__init__(name, description="MOB-specific status evaluation", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行MOB狀態評估
        
        MOB狀態規則：
        1. 基本日期範圍判斷
        2. 檢查收貨數量
        3. 檢查已關單狀態
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # 獲取處理日期
            processing_date = context.metadata.processing_date
            
            # 應用MOB狀態規則
            df[status_col] = df.apply(
                lambda row: self._evaluate_mob_status(row, processing_date),
                axis=1
            )
            
            # 特殊處理：已關單的狀態覆蓋
            if 'Status' in df.columns:
                closed_mask = df['Status'].str.contains('Closed', case=False, na=False)
                df.loc[closed_mask, status_col] = '已關單'
            
            context.update_data(df)
            
            # 統計
            status_counts = df[status_col].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"MOB status evaluation completed",
                metadata={'status_counts': status_counts}
            )
            
        except Exception as e:
            self.logger.error(f"MOB status evaluation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _evaluate_mob_status(self, row: pd.Series, processing_date: int) -> str:
        """
        評估單行MOB狀態
        
        Args:
            row: 數據行
            processing_date: 處理日期 (YYYYMM)
            
        Returns:
            str: 狀態值
        """
        # 獲取Expected Receive Month
        expected_month = row.get('Expected Receive Month', '')
        
        # 轉換為YYYYMM格式比較
        try:
            if pd.notna(expected_month) and expected_month != '':
                # 假設格式已經標準化為YYYY-MM
                expected_yyyymm = int(expected_month.replace('-', ''))
                
                if expected_yyyymm <= processing_date:
                    # 檢查收貨數量
                    received_qty = row.get('Received Quantity', 0)
                    entry_qty = row.get('Entry Quantity', 0)
                    
                    if pd.notna(received_qty) and received_qty > 0:
                        if received_qty >= entry_qty:
                            return '已完成'
                        else:
                            return '部分收貨'
                    else:
                        return '已完成'  # MOB預設為已完成
                else:
                    return '未到期'
            else:
                return '資訊不足'
                
        except Exception:
            return '資訊不足'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.metadata.entity_type != "MOB":
            self.logger.error("This step is only for MOB entity")
            return False
        
        required_cols = ['Expected Receive Month']
        return all(col in context.data.columns for col in required_cols)


class MOBAccrualStep(PipelineStep):
    """
    MOB預估入帳步驟
    處理MOB特定的預估邏輯
    """
    
    def __init__(self, name: str = "MOBAccrual", **kwargs):
        super().__init__(name, description="MOB accrual processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行MOB預估入帳處理
        
        MOB預估規則：
        1. 狀態為"已完成"且無採購備註"不預估"
        2. FA科目特殊處理
        3. 租金項目識別
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            
            # 初始化預估欄位
            df['是否估計入帳'] = 'N'
            
            # 基本規則：已完成狀態
            completed_mask = df[status_col] == '已完成'
            df.loc[completed_mask, '是否估計入帳'] = 'Y'
            
            # 檢查採購備註
            if '採購備註' in df.columns:
                no_accrual_mask = df['採購備註'].str.contains(
                    '不預估|不估計|勿估', case=False, na=False
                )
                df.loc[no_accrual_mask, '是否估計入帳'] = 'N'
            
            # FA科目標記
            config = context.get_entity_config()
            fa_accounts = config.get('fa_accounts', ['151101', '151201'])
            df['是否為FA'] = df['GL#'].isin(fa_accounts).map({True: 'Y', False: 'N'})
            
            # 租金識別
            rent_account = config.get('rent_account', '622101')
            rent_mask = (
                (df['GL#'] == rent_account) |
                df['Item Description'].str.contains('租金|Rent', case=False, na=False)
            )
            df.loc[rent_mask, '租金標記'] = 'Y'
            
            # 設置Account code
            accrual_mask = df['是否估計入帳'] == 'Y'
            df.loc[accrual_mask, 'Account code'] = df.loc[accrual_mask, 'GL#']
            
            context.update_data(df)
            
            # 統計
            accrual_count = (df['是否估計入帳'] == 'Y').sum()
            fa_count = (df['是否為FA'] == 'Y').sum()
            rent_count = df['租金標記'].notna().sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="MOB accrual processing completed",
                metadata={
                    'accrual_items': accrual_count,
                    'fa_items': fa_count,
                    'rent_items': rent_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"MOB accrual processing failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = [context.get_status_column(), 'GL#']
        return all(col in context.data.columns for col in required_cols)


class MOBDepartmentStep(PipelineStep):
    """
    MOB部門處理步驟
    處理MOB的部門代碼轉換
    """
    
    def __init__(self, name: str = "MOBDepartment", **kwargs):
        super().__init__(name, description="MOB department processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行MOB部門處理
        
        MOB部門規則：
        1. 標準3碼部門代碼
        2. 特殊部門映射
        """
        try:
            df = context.data.copy()
            
            # 基本部門代碼處理
            df['Department_Code'] = df['Department'].astype(str).str[:3]
            
            # 特殊部門映射
            special_dept_mapping = {
                'MOB_HQ': '000',
                'MOB_OPS': '100',
                'MOB_IT': '200'
            }
            
            for key, value in special_dept_mapping.items():
                mask = df['Department'].str.contains(key, case=False, na=False)
                df.loc[mask, 'Department_Code'] = value
            
            # 空值處理
            df['Department_Code'].fillna('999', inplace=True)
            
            context.update_data(df)
            
            # 統計
            dept_counts = df['Department_Code'].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="MOB department processing completed",
                metadata={'department_distribution': dept_counts}
            )
            
        except Exception as e:
            self.logger.error(f"MOB department processing failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'Department' in context.data.columns


class MOBValidationStep(PipelineStep):
    """
    MOB驗證步驟
    執行MOB特定的業務規則驗證
    """
    
    def __init__(self, name: str = "MOBValidation", **kwargs):
        super().__init__(name, description="MOB-specific validation", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行MOB驗證
        
        MOB驗證規則：
        1. Entry Amount必須為正數（預估項）
        2. GL#必須在允許範圍內
        3. 部門代碼格式驗證
        """
        try:
            df = context.data
            errors = []
            warnings = []
            
            # 驗證1：金額檢查
            accrual_mask = df['是否估計入帳'] == 'Y'
            negative_amounts = df[accrual_mask & (df['Entry Amount'] < 0)]
            if not negative_amounts.empty:
                errors.append(f"Found {len(negative_amounts)} accrual items with negative amounts")
            
            # 驗證2：GL#範圍檢查
            valid_gl_prefixes = ['1', '2', '5', '6', '7', '8', '9']
            invalid_gl = df[~df['GL#'].astype(str).str[0].isin(valid_gl_prefixes)]
            if not invalid_gl.empty:
                warnings.append(f"Found {len(invalid_gl)} items with unusual GL codes")
            
            # 驗證3：部門代碼格式
            if 'Department_Code' in df.columns:
                invalid_dept = df[~df['Department_Code'].str.match(r'^\d{3}$', na=False)]
                if not invalid_dept.empty:
                    warnings.append(f"Found {len(invalid_dept)} items with invalid department codes")
            
            # 記錄驗證結果
            for error in errors:
                context.add_error(f"[MOB Validation] {error}")
            for warning in warnings:
                context.add_warning(f"[MOB Validation] {warning}")
            
            if errors:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message="MOB validation failed",
                    metadata={'errors': errors, 'warnings': warnings}
                )
            else:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SUCCESS,
                    message="MOB validation passed",
                    metadata={'warnings': warnings}
                )
                
        except Exception as e:
            self.logger.error(f"MOB validation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = ['是否估計入帳', 'Entry Amount', 'GL#']
        return all(col in context.data.columns for col in required_cols)
