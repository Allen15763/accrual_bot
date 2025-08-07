"""
MOB PO專用處理器

繼承BasePOProcessor，實現MOB特有的PO處理邏輯
"""

from typing import Optional, List
from datetime import datetime
import pandas as pd
import numpy as np

from .po_processor import BasePOProcessor
try:
    from ...core.models.data_models import ProcessingResult
    from ...utils.logging import Logger
except ImportError:
    from core.models.data_models import ProcessingResult
    from utils.logging import Logger


class MobPOProcessor(BasePOProcessor):
    """MOB PO專用處理器"""
    
    def __init__(self, entity_type: str = "MOB"):
        super().__init__(entity_type)
        self.logger = Logger().get_logger(f"{__name__}.MobPOProcessor")
    
    def process(self, raw_data_file: str, filename: str, 
                previous_workpaper: Optional[str] = None,
                procurement_file: Optional[str] = None,
                closing_list: Optional[str] = None,
                **kwargs) -> ProcessingResult:
        """
        MOB PO處理主流程
        
        可以在這裡添加MOB特有的邏輯，不會影響SPT
        """
        self.logger.info(f"開始MOB PO處理: {filename}")
        
        try:
            start_time = datetime.now()
            # 1. 讀取原始PO數據 and MOB特有的前置處理 TEMP
            df = self._mob_preprocess_data(raw_data_file)
            if df is None or df.empty:
                raise ValueError(f"無法讀取或空的原始數據檔案: {raw_data_file}")
            
            # 2. 調用父類的通用處理邏輯
            df = self._apply_common_processing(df, filename, previous_workpaper, 
                                               procurement_file, closing_list)
            
            # 3. MOB特有的後置處理 TEMP
            df = self._mob_postprocess_data(df)
            df = df.replace('<NA>', np.nan)
            
            # 4. 匯出結果
            output_path = self._save_output(df, filename)
            
            self.logger.info(f"MOB PO處理完成: {output_path}")
            end_time = datetime.now()

            return ProcessingResult(
                success=True,
                message=f"MOB PO處理成功: {filename}",
                processed_data=df,
                total_records=len(df),
                processed_records=len(df),
                start_time=start_time,
                end_time=end_time,
                output_files=[output_path] if output_path else []
            )
            
        except Exception as e:
            self.logger.error(f"MOB PO處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"MOB PO處理失敗: {str(e)}"
            )
    
    def _mob_preprocess_data(self, raw_data_file: str) -> pd.DataFrame:
        """MOB特有的前置數據處理"""
        self.logger.debug("執行MOB前置處理")
        
        # 載入數據
        df = self._read_raw_data(raw_data_file)
        
        # MOB特有的前置處理邏輯; TEMP example
        # df = self._apply_mob_data_cleaning(df)
        # df = self._apply_mob_validation_rules(df)
        
        return df
    
    def _mob_postprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的後置數據處理"""
        self.logger.debug("執行MOB後置處理")
        
        # MOB特有的後置處理邏輯; TEMP example
        # df = self._apply_mob_business_rules(df)
        # df = self._apply_mob_formatting(df)
        
        return df
    
    def _apply_mob_data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的數據清理"""
        # 範例：MOB特有的數據清理邏輯
        # 例如：特定欄位的處理、特殊字符清理等
        
        # MOB特有：處理特殊的供應商代碼格式
        if '供應商代碼' in df.columns:
            df['供應商代碼'] = df['供應商代碼'].astype(str).str.upper().str.strip()
        
        return df
    
    def _apply_mob_validation_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的驗證規則"""
        # 範例：MOB特有的驗證邏輯
        
        # MOB特有：金額範圍驗證
        if '金額' in df.columns:
            # MOB規定：單筆PO金額不能超過1000萬
            df.loc[df['金額'] > 10000000, '備註'] = df['備註'].fillna('') + ' [金額超限]'
        
        return df
    
    def _apply_mob_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的業務規則"""
        # 範例：MOB特有的業務邏輯
        
        # MOB特有：部門代碼處理
        if '部門' in df.columns:
            # MOB特殊規則：某些部門需要特殊標記
            special_depts = ['IT', 'HR']
            mask = df['部門'].isin(special_depts)
            df.loc[mask, '特殊標記'] = 'MOB_SPECIAL'
        
        return df
    
    def _apply_mob_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的格式化"""
        # 範例：MOB特有的格式化邏輯
        
        # MOB特有的欄位順序
        mob_column_order = [
            '公司', '採購單號', '供應商代碼', '供應商名稱', 
            '部門', '金額', '幣別', '狀態', '特殊標記', '備註'
        ]
        
        # 重新排列欄位（只排列存在的欄位）
        existing_columns = [col for col in mob_column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in mob_column_order]
        df = df[existing_columns + other_columns]
        
        return df
    
    def _apply_common_processing(self, df: pd.DataFrame, filename: str,
                                 previous_workpaper: Optional[str] = None,
                                 procurement_file: Optional[str] = None,
                                 closing_list_file: Optional[str] = None) -> pd.DataFrame:
        """調用父類的通用處理邏輯"""
        
        # 呼叫父類的方法來執行通用邏輯
        # 但是先要確保數據格式符合父類期望

        # 2. 提取月份資訊
        month = self._extract_month_from_filename(filename)
        
        # 3. 添加基本欄位
        df, previous_month = self.add_basic_columns(df, month)
        
        # 4. 處理前期底稿（如果提供）
        if previous_workpaper:
            previous_wp_df = self._read_workpaper(previous_workpaper)
            if previous_wp_df is not None:
                df = self.process_previous_workpaper(df, previous_wp_df, month)
        
        # 5. 處理採購底稿（如果提供）
        if procurement_file:
            procurement_df = self._read_workpaper(procurement_file)
            if procurement_df is not None:
                df = self.process_procurement_workpaper(df, procurement_df)
        
        # 6. 處理關單清單（如果提供）
        if closing_list_file:
            closing_list = self._read_closing_list(closing_list_file)
            if closing_list:
                df = self.process_closing_list(df, closing_list)
        
        # 7. 應用日期邏輯
        df = self.apply_date_logic(df)
        
        # 8. 應用ERM邏輯
        file_date = self._convert_month_to_file_date(month)
        df = self.apply_erm_logic(df, file_date, None, None)  # 參考數據可以後續添加
        
        # 9. 最終格式化
        df = self.finalize_data_format(df)

        return df

    def _read_raw_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取原始數據檔案"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
            else:
                raise ValueError(f"不支援的檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取原始數據: {len(df)} 行")
            return df
            
        except Exception as e:
            self.logger.error(f"讀取原始數據失敗: {e}")
            return None
    
    def _read_workpaper(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取底稿檔案"""
        try:
            if file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            else:
                raise ValueError(f"不支援的底稿檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取底稿檔案: {len(df)} 行")
            return df
            
        except Exception as e:
            self.logger.error(f"讀取底稿檔案失敗: {e}")
            return None
    
    def _read_closing_list(self, file_path: str) -> List[str]:
        """讀取關單清單"""
        try:
            if file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, dtype=str)
                # 假設關單清單在第一欄
                closing_list = df.iloc[:, 0].dropna().astype(str).tolist()
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str)
                closing_list = df.iloc[:, 0].dropna().astype(str).tolist()
            else:
                raise ValueError(f"不支援的關單清單檔案格式: {file_path}")
            
            self.logger.info(f"成功讀取關單清單: {len(closing_list)} 項")
            return closing_list
            
        except Exception as e:
            self.logger.error(f"讀取關單清單失敗: {e}")
            return []
    
    def _extract_month_from_filename(self, filename: str) -> int:
        """從檔案名稱提取月份"""
        import re
        
        try:
            # 嘗試從檔案名稱中提取YYYYMM格式的日期
            match = re.search(r'(\d{6})', filename)
            if match:
                date_str = match.group(1)
                month = int(date_str[4:6])  # 取月份部分
                return month
            
            # 如果沒找到，嘗試其他格式
            match = re.search(r'(\d{4})(\d{2})', filename)
            if match:
                month = int(match.group(2))
                return month
            
            # 預設返回當前月份
            from datetime import datetime
            return datetime.now().month
            
        except Exception as e:
            self.logger.warning(f"無法從檔案名稱提取月份: {e}")
            return datetime.now().month
    
    def _convert_month_to_file_date(self, month: int) -> int:
        """將月份轉換為檔案日期格式"""
        from datetime import datetime
        current_year = datetime.now().year
        return current_year * 100 + month
    
    def _save_output(self, df: pd.DataFrame, original_filename: str) -> Optional[str]:
        """保存輸出檔案"""
        try:
            import os
            
            # 生成輸出檔案名稱
            base_name = os.path.splitext(original_filename)[0]
            output_filename = f"{base_name}_processed_{self.entity_type}.xlsx"
            
            # 創建輸出目錄
            output_dir = "output"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            output_path = os.path.join(output_dir, output_filename)
            
            # 保存Excel檔案
            df.to_excel(output_path, index=False)
            
            self.logger.info(f"輸出檔案已保存: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存輸出檔案失敗: {e}")
            return None
