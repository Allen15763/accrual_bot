"""
SPX實體特定處理步驟
包含SPX特有的複雜業務邏輯
"""

import re
from typing import Dict
import pandas as pd
import numpy as np

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext
from accrual_bot.core.pipeline import PipelineBuilder, Pipeline

from accrual_bot.core.pipeline.steps.common import (ProductFilterStep,
                                                    PreviousWorkpaperIntegrationStep,
                                                    ProcurementIntegrationStep,
                                                    DateLogicStep)
from accrual_bot.core.pipeline.steps.spx_integration import (ColumnAdditionStep, 
                                                             ClosingListIntegrationStep, 
                                                             PRDataReformattingStep)
from accrual_bot.core.pipeline.steps.spx_loading import SPXPRDataLoadingStep
from accrual_bot.core.pipeline.steps.spx_evaluation import StatusStage1Step
from accrual_bot.core.pipeline.steps.spx_pr_evaluation import SPXPRERMLogicStep
from accrual_bot.core.pipeline.steps.spx_exporting import SPXPRExportStep


class SPXDepositCheckStep(PipelineStep):
    """
    SPX押金檢查步驟
    識別和處理押金相關項目
    """
    
    def __init__(self,
                 name: str = "SPXDepositCheck",
                 deposit_keywords: str = '押金|保證金|Deposit|找零金|保證|擔保',
                 **kwargs):
        super().__init__(name, description="SPX deposit identification", **kwargs)
        self.deposit_keywords = deposit_keywords
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行押金檢查
        
        SPX押金規則：
        1. 關鍵字識別
        2. GL#檢查（通常為199999）
        3. 押金項目不預估
        """
        try:
            df = context.data.copy()
            
            # 押金識別
            deposit_mask = (
                df['Item Description'].str.contains(
                    self.deposit_keywords, case=False, na=False
                ) |
                (df['GL#'] == '199999')
            )
            
            df['押金標記'] = np.where(deposit_mask, 'Y', None)
            
            # 押金項目特殊處理
            df.loc[deposit_mask, '押金類型'] = df.loc[deposit_mask, 'Item Description'].apply(
                self._classify_deposit_type
            )
            
            # 押金不預估
            if '是否估計入帳' in df.columns:
                df.loc[deposit_mask, '是否估計入帳'] = 'N'
                df.loc[deposit_mask, 'Accrual Note'] = '押金項目-不預估'
            
            context.update_data(df)
            
            # 統計
            deposit_count = deposit_mask.sum()
            deposit_types = df[deposit_mask]['押金類型'].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX deposit check completed",
                metadata={
                    'deposit_items': deposit_count,
                    'deposit_types': deposit_types
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPX deposit check failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _classify_deposit_type(self, description: str) -> str:
        """分類押金類型"""
        desc_lower = str(description).lower()
        
        if '找零金' in desc_lower:
            return '找零金'
        elif '租賃' in desc_lower or 'lease' in desc_lower:
            return '租賃押金'
        elif '保證金' in desc_lower:
            return '保證金'
        elif 'deposit' in desc_lower:
            return '一般押金'
        else:
            return '其他押金'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'Item Description' in context.data.columns


class SPXClosingListIntegrationStep(PipelineStep):
    """
    SPX關單清單整合步驟
    整合關單清單數據
    """
    
    def __init__(self, name: str = "SPXClosingList", **kwargs):
        super().__init__(name, description="SPX closing list integration", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行關單清單整合
        
        SPX關單處理：
        1. 整合closing_list輔助數據
        2. 標記已關單項目
        3. 更新關單狀態
        """
        try:
            df = context.data.copy()
            
            # 獲取關單清單
            closing_list = context.get_auxiliary_data('closing_list')
            
            if closing_list is not None:
                # 合併關單資訊
                id_col = context.get_id_column()
                
                # 創建關單標記
                closed_ids = set(closing_list[id_col].unique())
                df['關單標記'] = df[id_col].isin(closed_ids).map({True: 'Y', False: None})
                
                # 合併關單詳細資訊
                if 'Closing_Date' in closing_list.columns:
                    closing_info = closing_list[[id_col, 'Closing_Date']].drop_duplicates()
                    df = pd.merge(
                        df, 
                        closing_info,
                        on=id_col,
                        how='left',
                        suffixes=('', '_closing')
                    )
                
                # 更新狀態
                status_col = context.get_status_column()
                df.loc[df['關單標記'] == 'Y', status_col] = '已關單'
                
                self.logger.info(f"Integrated {len(closed_ids)} closed items")
            else:
                self.logger.warning("No closing list data available")
                df['關單標記'] = None
            
            context.update_data(df)
            
            # 統計
            closed_count = (df['關單標記'] == 'Y').sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX closing list integration completed",
                metadata={'closed_items': closed_count}
            )
            
        except Exception as e:
            self.logger.error(f"SPX closing list integration failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.get_id_column() in context.data.columns


class SPXRentProcessingStep(PipelineStep):
    """
    SPX租金處理步驟
    處理各種租金相關項目
    """
    
    def __init__(self, name: str = "SPXRentProcessing", **kwargs):
        super().__init__(name, description="SPX rent processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行租金處理
        
        SPX租金規則：
        1. 倉庫租金識別
        2. 辦公室租金識別
        3. 設備租賃識別
        4. 租金期間解析
        """
        try:
            df = context.data.copy()
            config = context.get_entity_config()
            
            # 租金科目
            rent_account = config.get('rent_account', '622101')
            
            # 租金識別
            rent_mask = (
                (df['GL#'] == rent_account) |
                df['Item Description'].str.contains(
                    '租金|Rent|租賃|Lease|月租', case=False, na=False
                )
            )
            
            df['租金標記'] = np.where(rent_mask, 'Y', None)
            
            # 租金分類
            df.loc[rent_mask, '租金類型'] = df.loc[rent_mask, 'Item Description'].apply(
                self._classify_rent_type
            )
            
            # 租金期間解析
            df.loc[rent_mask, 'Rent_Period'] = df.loc[rent_mask, 'Item Description'].apply(
                self._extract_rent_period
            )
            
            # OPS租金特殊處理
            ops_rent = config.get('ops_rent', 'ShopeeOPS07')
            ops_mask = rent_mask & df['Department'].str.contains(ops_rent, case=False, na=False)
            df.loc[ops_mask, '租金類型'] = 'OPS倉庫租金'
            
            # 租金預估處理
            processing_date = context.metadata.processing_date
            df.loc[rent_mask, '租金預估狀態'] = df.loc[rent_mask].apply(
                lambda row: self._evaluate_rent_accrual(row, processing_date),
                axis=1
            )
            
            context.update_data(df)
            
            # 統計
            rent_count = rent_mask.sum()
            rent_types = df[rent_mask]['租金類型'].value_counts().to_dict()
            ops_rent_count = ops_mask.sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX rent processing completed",
                metadata={
                    'rent_items': rent_count,
                    'rent_types': rent_types,
                    'ops_rent_items': ops_rent_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPX rent processing failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _classify_rent_type(self, description: str) -> str:
        """分類租金類型"""
        desc_lower = str(description).lower()
        
        if '倉庫' in desc_lower or 'warehouse' in desc_lower:
            return '倉庫租金'
        elif '辦公室' in desc_lower or 'office' in desc_lower:
            return '辦公室租金'
        elif 'kiosk' in desc_lower or '機台' in desc_lower:
            return 'Kiosk租金'
        elif 'locker' in desc_lower or '櫃' in desc_lower:
            return 'Locker租金'
        elif '車' in desc_lower or 'vehicle' in desc_lower:
            return '車輛租金'
        else:
            return '其他租金'
    
    def _extract_rent_period(self, description: str) -> str:
        """提取租金期間"""
        # 簡化實現，實際應使用正則表達式提取
        
        # 嘗試匹配YYYY/MM格式
        pattern = r'(\d{4})[/\-](\d{1,2})'
        match = re.search(pattern, str(description))
        
        if match:
            year, month = match.groups()
            return f"{year}{month.zfill(2)}"
        
        return ''
    
    def _evaluate_rent_accrual(self, row: pd.Series, processing_date: int) -> str:
        """評估租金預估狀態"""
        rent_period = row.get('Rent_Period', '')
        
        if rent_period:
            try:
                rent_yyyymm = int(rent_period)
                if rent_yyyymm <= processing_date:
                    return '當期租金-預估'
                else:
                    return '預付租金-不預估'
            except Exception as err:
                self.logger.error(f"SPX evaluate_rent_accrual failed: {str(err)}")
                pass
        
        return '待判斷'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'Item Description' in context.data.columns and 'GL#' in context.data.columns


class SPXAssetValidationStep(PipelineStep):
    """
    SPX資產驗收步驟
    處理Kiosk、Locker等資產驗收邏輯
    """
    
    def __init__(self, name: str = "SPXAssetValidation", **kwargs):
        super().__init__(name, description="SPX asset validation processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行資產驗收處理
        
        SPX資產規則：
        1. Kiosk驗收狀態
        2. Locker驗收狀態
        3. 其他設備驗收
        """
        try:
            df = context.data.copy()
            config = context.get_entity_config()
            
            # 獲取供應商列表
            kiosk_suppliers = config.get('kiosk_suppliers', [
                '益欣資訊股份有限公司',
                '振樺電子股份有限公司'
            ])
            
            locker_suppliers = config.get('locker_suppliers', [
                '掌櫃智能股份有限公司',
                '台灣宅配通股份有限公司'
            ])
            
            # Kiosk識別
            kiosk_mask = (
                df['Supplier'].isin(kiosk_suppliers) |
                df['Item Description'].str.contains('Kiosk|機台', case=False, na=False)
            )
            df.loc[kiosk_mask, '資產類型'] = 'Kiosk'
            
            # Locker識別
            locker_mask = (
                df['Supplier'].isin(locker_suppliers) |
                df['Item Description'].str.contains('Locker|智能櫃', case=False, na=False)
            )
            df.loc[locker_mask, '資產類型'] = 'Locker'
            
            # 驗收狀態判斷
            asset_mask = kiosk_mask | locker_mask
            
            if asset_mask.any():
                # 檢查驗收相關欄位
                df.loc[asset_mask, '驗收狀態'] = df.loc[asset_mask].apply(
                    self._evaluate_validation_status,
                    axis=1
                )
                
                # 根據驗收狀態更新預估
                pending_validation = df['驗收狀態'] == '待驗收'
                df.loc[pending_validation, '是否估計入帳'] = 'N'
                df.loc[pending_validation, 'Accrual Note'] = '資產待驗收-不預估'
                
                validated = df['驗收狀態'] == '已驗收'
                df.loc[validated, '是否估計入帳'] = 'Y'
                df.loc[validated, 'Accrual Note'] = '資產已驗收-預估'
            
            context.update_data(df)
            
            # 統計
            kiosk_count = kiosk_mask.sum()
            locker_count = locker_mask.sum()
            pending_count = (df['驗收狀態'] == '待驗收').sum()
            validated_count = (df['驗收狀態'] == '已驗收').sum()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX asset validation completed",
                metadata={
                    'kiosk_items': kiosk_count,
                    'locker_items': locker_count,
                    'pending_validation': pending_count,
                    'validated': validated_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPX asset validation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _evaluate_validation_status(self, row: pd.Series) -> str:
        """評估驗收狀態"""
        # 檢查收貨數量
        received_qty = row.get('Received Quantity', 0)
        entry_qty = row.get('Entry Quantity', 0)
        
        # 檢查狀態欄位
        status = str(row.get('Status', '')).lower()
        
        # 判斷邏輯
        if 'validated' in status or '驗收' in status:
            return '已驗收'
        elif pd.notna(received_qty) and received_qty >= entry_qty:
            return '已收貨待驗收'
        elif pd.notna(received_qty) and received_qty > 0:
            return '部分收貨'
        else:
            return '待驗收'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        required_cols = ['Supplier', 'Item Description']
        return all(col in context.data.columns for col in required_cols)


class SPXComplexStatusStep(PipelineStep):
    """
    SPX複雜狀態判斷步驟
    實現SPX的11個狀態判斷條件
    """
    
    def __init__(self, name: str = "SPXComplexStatus", **kwargs):
        super().__init__(name, description="SPX complex status evaluation", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行SPX複雜狀態判斷
        
        SPX 11個條件判斷：
        1. 已關單
        2. 押金類（不預估）
        3. OPS倉租（特定規則）
        4-11. 其他複雜條件
        """
        try:
            df = context.data.copy()
            status_col = context.get_status_column()
            processing_date = context.metadata.processing_date
            
            # 初始化狀態
            df[status_col] = '待評估'
            df['狀態條件'] = ''
            
            # 條件1：已關單
            if '關單標記' in df.columns:
                closed_mask = df['關單標記'] == 'Y'
                df.loc[closed_mask, status_col] = '已關單'
                df.loc[closed_mask, '狀態條件'] = '條件1-已關單'
            
            # 條件2：押金類
            if '押金標記' in df.columns:
                deposit_mask = df['押金標記'] == 'Y'
                df.loc[deposit_mask, status_col] = '押金項目'
                df.loc[deposit_mask, '狀態條件'] = '條件2-押金'
            
            # 條件3：OPS倉租
            if '租金類型' in df.columns:
                ops_rent_mask = df['租金類型'] == 'OPS倉庫租金'
                df.loc[ops_rent_mask, status_col] = 'OPS倉租'
                df.loc[ops_rent_mask, '狀態條件'] = '條件3-OPS倉租'
            
            # 條件4：Kiosk/Locker待驗收
            if '驗收狀態' in df.columns:
                pending_mask = df['驗收狀態'] == '待驗收'
                df.loc[pending_mask, status_col] = '待驗收'
                df.loc[pending_mask, '狀態條件'] = '條件4-待驗收'
                
                validated_mask = df['驗收狀態'] == '已驗收'
                df.loc[validated_mask, status_col] = '已驗收完成'
                df.loc[validated_mask, '狀態條件'] = '條件4-已驗收'
            
            # 條件5-11：其他業務規則
            # 應用標準日期判斷（未被前述條件覆蓋的項目）
            standard_mask = df['狀態條件'] == ''
            df.loc[standard_mask, status_col] = df.loc[standard_mask].apply(
                lambda row: self._evaluate_standard_status(row, processing_date),
                axis=1
            )
            df.loc[standard_mask, '狀態條件'] = '標準判斷'
            
            context.update_data(df)
            
            # 統計各條件分布
            condition_counts = df['狀態條件'].value_counts().to_dict()
            status_counts = df[status_col].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX complex status evaluation completed",
                metadata={
                    'condition_distribution': condition_counts,
                    'status_distribution': status_counts
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPX complex status evaluation failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _evaluate_standard_status(self, row: pd.Series, processing_date: int) -> str:
        """標準狀態評估"""
        expected_month = row.get('Expected Receive Month', '')
        
        try:
            if pd.notna(expected_month) and expected_month != '':
                expected_yyyymm = int(expected_month.replace('-', ''))
                
                if expected_yyyymm <= processing_date:
                    received_qty = row.get('Received Quantity', 0)
                    entry_qty = row.get('Entry Quantity', 0)
                    
                    if pd.notna(received_qty) and received_qty >= entry_qty:
                        return '已完成'
                    elif pd.notna(received_qty) and received_qty > 0:
                        return '部分收貨'
                    else:
                        return '待收貨'
                else:
                    return '未到期'
            else:
                return '資訊不足'
                
        except Exception:
            return '資訊不足'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        if context.metadata.entity_type != "SPX":
            self.logger.error("This step is only for SPX entity")
            return False
        return True


class SPXPPEProcessingStep(PipelineStep):
    """
    SPX PPE處理步驟
    處理固定資產相關邏輯
    """
    
    def __init__(self, name: str = "SPXPPEProcessing", **kwargs):
        super().__init__(name, description="SPX PPE processing", **kwargs)
    
    async def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行PPE處理
        
        PPE處理規則：
        1. 識別FA科目
        2. 檢查資本化門檻
        3. 設置折舊相關資訊
        """
        try:
            df = context.data.copy()
            config = context.get_entity_config()
            
            # FA科目列表
            fa_accounts = config.get('fa_accounts', ['151101', '151201', '199999'])
            
            # 識別FA項目
            fa_mask = df['GL#'].isin(fa_accounts)
            df['是否為FA'] = fa_mask.map({True: 'Y', False: 'N'})
            
            # 資本化門檻檢查（假設門檻為10000）
            capital_threshold = 10000
            
            if 'Entry Amount' in df.columns:
                # FA且金額超過門檻
                capitalize_mask = fa_mask & (df['Entry Amount'] >= capital_threshold)
                df.loc[capitalize_mask, '資本化標記'] = 'Y'
                
                # FA但金額未達門檻
                expense_mask = fa_mask & (df['Entry Amount'] < capital_threshold)
                df.loc[expense_mask, '資本化標記'] = 'N'
                df.loc[expense_mask, 'PPE Note'] = '金額未達資本化門檻-費用化'
            
            # PPE分類
            df.loc[fa_mask, 'PPE類別'] = df.loc[fa_mask].apply(
                self._classify_ppe_type,
                axis=1
            )
            
            # FA項目預估處理
            df.loc[fa_mask, 'FA預估狀態'] = df.loc[fa_mask].apply(
                self._evaluate_fa_accrual,
                axis=1
            )
            
            context.update_data(df)
            
            # 統計
            fa_count = fa_mask.sum()
            capitalize_count = (df['資本化標記'] == 'Y').sum()
            expense_count = (df['資本化標記'] == 'N').sum()
            ppe_types = df[fa_mask]['PPE類別'].value_counts().to_dict()
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message="SPX PPE processing completed",
                metadata={
                    'fa_items': fa_count,
                    'capitalized': capitalize_count,
                    'expensed': expense_count,
                    'ppe_types': ppe_types
                }
            )
            
        except Exception as e:
            self.logger.error(f"SPX PPE processing failed: {str(e)}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _classify_ppe_type(self, row: pd.Series) -> str:
        """分類PPE類型"""
        gl_code = str(row.get('GL#', ''))
        description = str(row.get('Item Description', '')).lower()
        
        if gl_code == '151101':
            if 'computer' in description or '電腦' in description:
                return 'IT設備'
            elif 'furniture' in description or '家具' in description:
                return '辦公家具'
            else:
                return '一般設備'
        elif gl_code == '151201':
            return '租賃改良'
        elif gl_code == '199999':
            return '押金及預付'
        else:
            return '其他FA'
    
    def _evaluate_fa_accrual(self, row: pd.Series) -> str:
        """評估FA預估狀態"""
        status_col = 'PO狀態' if 'PO狀態' in row else 'PR狀態'
        status = row.get(status_col, '')
        
        if status in ['已完成', '已驗收完成']:
            if row.get('資本化標記') == 'Y':
                return '資本化-預估'
            else:
                return '費用化-預估'
        elif status == '待驗收':
            return '待驗收-不預估'
        else:
            return '未完成-不預估'
    
    async def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return 'GL#' in context.data.columns


def create_spx_pr_complete_pipeline(file_paths: Dict[str, str]) -> Pipeline:
    """
    創建完整的 SPX PR 處理 Pipeline
    
    這是將原始 SPXPRProcessor.process() 方法完全重構後的版本
    
    Args:
        file_paths: 文件路徑字典，包含:
            - raw_po: 原始 PR 文件
            - previous_pr: 前期底稿 (PR)
            - procurement_pr: 採購底稿 (PR)
            
    Returns:
        Pipeline: 完整配置的 SPX PR Pipeline
    """
    
    # 創建 Pipeline Builder
    builder = PipelineBuilder("SPX_PR_Complete", "SPX")
    
    # 配置 Pipeline
    pipeline = (builder
                .with_description("Complete SPX PR processing pipeline - refactored from process()")
                .with_stop_on_error(False)  # 不要遇錯即停，收集所有錯誤
                #  ========== 階段 1: 數據載入 ==========
                .add_step(
                    SPXPRDataLoadingStep(
                        name="PR_Load_All_Data",
                        file_paths=file_paths,
                        required=True,
                        retry_count=2,  # 載入失敗重試2次
                        timeout=300.0   # 5分鐘超時
                    )
                )
        
                # ========== 階段 2: 數據準備與整合 ==========
                .add_step(
                    ProductFilterStep(
                        name="PR_Filter_Products",
                        product_pattern='(?i)LG_SPX',
                        required=True
                    )
                )
                .add_step(ColumnAdditionStep(name="PR_Add_Columns", required=True))
                .add_step(PreviousWorkpaperIntegrationStep(name="PR_Integrate_Previous_WP", required=True))
                .add_step(ProcurementIntegrationStep(name="PR_Integrate_Procurement", required=True))
                
                # # ========== 階段3: 業務邏輯 ==========
                .add_step(DateLogicStep(name="PR_Process_Dates", required=True))
                .add_step(ClosingListIntegrationStep(name="PR_Integrate_Closing_List", required=True))
                .add_step(StatusStage1Step(name="PR_Evaluate_Status_Stage1", required=True))
                .add_step(SPXPRERMLogicStep(name="PR_Apply_ERM_Logic", required=True, retry_count=0))
                
                # # ========== 階段4: 後處理 ==========
                .add_step(PRDataReformattingStep(name="PR_Reformat_Data", required=True))
                # ========== 階段 5: 導出結果 ========== (新增！)
                .add_step(
                    SPXPRExportStep(
                        name="PR_Export_Results",
                        output_dir="output",              # 輸出目錄
                        sheet_name="PR",                  # Sheet 名稱
                        include_index=False,              # 不包含索引
                        required=True,                    # 必需步驟
                        retry_count=0                     # 失敗重試0次
                    )
                )
                )

    return pipeline.build()