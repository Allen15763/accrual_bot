"""
SPT PO專用處理器

繼承BasePOProcessor，實現SPT特有的PO處理邏輯
"""

from typing import Optional, Dict, Any
import pandas as pd

from .po_processor import BasePOProcessor
try:
    from ...core.models.data_models import ProcessingResult
    from ...utils.logging import Logger
except ImportError:
    from core.models.data_models import ProcessingResult
    from utils.logging import Logger


class SptPOProcessor(BasePOProcessor):
    """SPT PO專用處理器"""
    
    def __init__(self, entity_type: str = "SPT"):
        super().__init__(entity_type)
        self.logger = Logger().get_logger(f"{__name__}.SptPOProcessor")
    
    def process(self, raw_data_file: str, filename: str, 
                previous_workpaper: Optional[str] = None,
                procurement_file: Optional[str] = None,
                closing_list: Optional[str] = None,
                **kwargs) -> ProcessingResult:
        """
        SPT PO處理主流程
        
        可以在這裡添加SPT特有的邏輯，不會影響MOB
        """
        self.logger.info(f"開始SPT PO處理: {filename}")
        
        try:
            # 1. SPT特有的前置處理
            df = self._spt_preprocess_data(raw_data_file)
            
            # 2. 調用父類的通用處理邏輯
            df = self._apply_common_processing(df, filename, previous_workpaper, 
                                             procurement_file, closing_list)
            
            # 3. SPT特有的後置處理
            df = self._spt_postprocess_data(df)
            
            # 4. 匯出結果
            output_path = self._export_results(df, filename)
            
            self.logger.info(f"SPT PO處理完成: {output_path}")
            return ProcessingResult(
                success=True,
                output_path=output_path,
                message=f"SPT PO處理成功: {filename}",
                metadata={"entity_type": "SPT", "record_count": len(df)}
            )
            
        except Exception as e:
            self.logger.error(f"SPT PO處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"SPT PO處理失敗: {str(e)}"
            )
    
    def _spt_preprocess_data(self, raw_data_file: str) -> pd.DataFrame:
        """SPT特有的前置數據處理"""
        self.logger.debug("執行SPT前置處理")
        
        # 載入數據
        df = self.import_raw_data(raw_data_file)
        
        # SPT特有的前置處理邏輯
        df = self._apply_spt_data_cleaning(df)
        df = self._apply_spt_validation_rules(df)
        
        return df
    
    def _spt_postprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """SPT特有的後置數據處理"""
        self.logger.debug("執行SPT後置處理")
        
        # SPT特有的後置處理邏輯
        df = self._apply_spt_business_rules(df)
        df = self._apply_spt_formatting(df)
        
        return df
    
    def _apply_spt_data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """SPT特有的數據清理"""
        # 範例：SPT特有的數據清理邏輯
        
        # SPT特有：處理特殊的科目代碼格式
        if '科目代碼' in df.columns:
            df['科目代碼'] = df['科目代碼'].astype(str).str.zfill(6)  # SPT要求6位數
        
        return df
    
    def _apply_spt_validation_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """SPT特有的驗證規則"""
        # 範例：SPT特有的驗證邏輯
        
        # SPT特有：金額範圍驗證（與MOB不同）
        if '金額' in df.columns:
            # SPT規定：單筆PO金額不能超過2000萬
            df.loc[df['金額'] > 20000000, '備註'] = df['備註'].fillna('') + ' [SPT金額超限]'
        
        return df
    
    def _apply_spt_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """SPT特有的業務規則"""
        # 範例：SPT特有的業務邏輯
        
        # SPT特有：專案代碼處理
        if '專案代碼' in df.columns:
            # SPT特殊規則：某些專案需要特殊標記
            high_priority_projects = ['PROJ001', 'PROJ002']
            mask = df['專案代碼'].isin(high_priority_projects)
            df.loc[mask, '優先級'] = 'SPT_HIGH_PRIORITY'
        
        return df
    
    def _apply_spt_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """SPT特有的格式化"""
        # SPT特有的欄位順序
        spt_column_order = [
            '公司', '採購單號', '供應商代碼', '供應商名稱', 
            '科目代碼', '專案代碼', '金額', '幣別', '優先級', '狀態', '備註'
        ]
        
        # 重新排列欄位（只排列存在的欄位）
        existing_columns = [col for col in spt_column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in spt_column_order]
        df = df[existing_columns + other_columns]
        
        return df
    
    def _apply_common_processing(self, df: pd.DataFrame, filename: str,
                                previous_workpaper: Optional[str] = None,
                                procurement_file: Optional[str] = None,
                                closing_list: Optional[str] = None) -> pd.DataFrame:
        """調用父類的通用處理邏輯"""
        
        # 呼叫父類的方法來執行通用邏輯
        df = self.add_cols(df)
        
        if previous_workpaper:
            df = self.import_and_merge_previous_data(df, previous_workpaper)
        
        if procurement_file:
            df = self.import_and_merge_procurement_data(df, procurement_file)
        
        if closing_list:
            df = self.merge_closing_list(df, closing_list)
        
        # 執行通用的ERM邏輯
        df = self.erm(df)
        
        # 執行通用的狀態判斷
        df = self.give_status_stage_1(df)
        
        # 執行通用的格式化
        df = self.reformate(df)
        
        return df
