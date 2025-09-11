"""
MOB PR專用處理器

繼承BasePRProcessor，實現MOB特有的PR處理邏輯
"""

from typing import Optional, Tuple, List
import os
from datetime import datetime
import pandas as pd
import numpy as np

from .pr_processor import BasePRProcessor
try:
    from ...core.models.data_models import ProcessingResult
    from ...utils.logging import Logger
    from ...data.exporters import ExcelExporter, CSVExporter
    from ...data.importers import ExcelImporter
except ImportError:
    from core.models.data_models import ProcessingResult
    from utils.logging import Logger
    from data.exporters import ExcelExporter, CSVExporter
    from data.importers import ExcelImporter


class MobPRProcessor(BasePRProcessor):
    """MOB PR專用處理器"""
    
    def __init__(self, entity_type: str = "MOB"):
        super().__init__(entity_type)
        self.logger = Logger().get_logger(f"{__name__}.MobPRProcessor")
        self.data_importer = ExcelImporter()
    
    def process(self, raw_data_file: str, filename: str,
                previous_workpaper: Optional[str] = None,
                procurement_file: Optional[str] = None,
                closing_list: Optional[str] = None,
                **kwargs) -> ProcessingResult:
        """
        MOB PR處理主流程
        
        可以在這裡添加MOB特有的邏輯，不會影響SPT
        """
        self.logger.info(f"開始MOB PR處理: {filename}")
        
        try:
            start_time = datetime.now()
            # 1. MOB特有的前置處理
            df, yyyymm = self._mob_preprocess_data(raw_data_file)
            if df is None or df.empty:
                raise ValueError(f"無法讀取或空的原始數據檔案: {raw_data_file}")
            
            # 2. 調用父類的通用處理邏輯
            df = self._apply_common_processing(df, filename, previous_workpaper, 
                                               procurement_file, closing_list,
                                               yyyymm)
            
            # 3. MOB特有的後置處理
            df = self._mob_postprocess_data(df)
            
            # 4. 匯出結果
            output_path = self._save_output(df, filename)
            
            self.logger.info(f"MOB PO處理完成: {output_path}")
            end_time = datetime.now()

            return ProcessingResult(
                success=True,
                message=f"MOB PR處理成功: {filename}",
                processed_data=df,
                total_records=len(df),
                processed_records=len(df),
                start_time=start_time,
                end_time=end_time,
                output_files=[output_path] if output_path else []
            )
            
        except Exception as e:
            self.logger.error(f"MOB PR處理失敗: {e}")
            return ProcessingResult(
                success=False,
                message=f"MOB PR處理失敗: {str(e)}"
            )
    
    def _mob_preprocess_data(self, raw_data_file: str) -> Tuple[pd.DataFrame, str]:
        """MOB特有的前置數據處理"""
        self.logger.debug("執行MOB PR前置處理")
        
        # 載入數據
        df, yyyymm = self.import_raw_data(raw_data_file)
        
        # MOB特有的前置處理邏輯
        # df = self._apply_mob_pr_data_cleaning(df)
        # df = self._apply_mob_pr_validation_rules(df)
        
        return df, yyyymm
    
    def _mob_postprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB特有的後置數據處理"""
        self.logger.debug("執行MOB PR後置處理")
        
        # MOB特有的後置處理邏輯
        # df = self._apply_mob_pr_business_rules(df)
        # df = self._apply_mob_pr_formatting(df)
        
        return df
    
    def _apply_mob_pr_data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB PR特有的數據清理"""
        # MOB特有：處理PR特殊格式
        if '請購單號' in df.columns:
            df['請購單號'] = df['請購單號'].astype(str).str.upper().str.strip()
        
        return df
    
    def _apply_mob_pr_validation_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB PR特有的驗證規則"""
        # MOB特有：PR金額驗證（可能與PO不同）
        if '金額' in df.columns:
            # MOB規定：單筆PR金額不能超過500萬
            df.loc[df['金額'] > 5000000, '備註'] = df['備註'].fillna('') + ' [PR金額超限]'
        
        return df
    
    def _apply_mob_pr_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB PR特有的業務規則"""
        # MOB特有：PR審批流程標記
        if '申請人' in df.columns:
            # MOB特殊規則：某些申請人需要額外審核
            special_requesters = ['manager01', 'director02']
            mask = df['申請人'].isin(special_requesters)
            df.loc[mask, '審核標記'] = 'MOB_SPECIAL_REVIEW'
        
        return df
    
    def _apply_mob_pr_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOB PR特有的格式化"""
        # MOB特有的欄位順序
        mob_pr_column_order = [
            '公司', '請購單號', '申請人', '部門', 
            '金額', '幣別', '狀態', '審核標記', '備註'
        ]
        
        # 重新排列欄位
        existing_columns = [col for col in mob_pr_column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in mob_pr_column_order]
        df = df[existing_columns + other_columns]
        
        return df
    
    def _apply_common_processing(self, df: pd.DataFrame, filename: str,
                                 previous_workpaper: Optional[str] = None,
                                 procurement_file: Optional[str] = None,
                                 closing_list: Optional[str] = None,
                                 yyyymm: str = None) -> pd.DataFrame:
        """調用父類的通用處理邏輯"""

        # 導入參考數據
        ref_ac, ref_liability = self.import_reference_data()
        
        # 添加必要列
        df = self.add_basic_columns(df)
        
        # 處理採購底稿
        if procurement_file:
            df_procu = self.import_procurement(procurement_file)
            df = self.process_with_procurement(df, df_procu)
        
        # 處理關單清單
        if closing_list:
            mapping_list = self.import_closing_list(closing_list)
            df = self.process_with_closing_list(df, mapping_list)
        
        # 處理前期底稿
        if previous_workpaper:
            previous_wp = self.import_previous_wp(previous_workpaper)
            df = self.process_previous_workpaper(df, previous_wp)
        
        # 處理特殊情況
        df = self.process_special_cases(df)
        
        # 設置檔案日期
        df['檔案日期'] = yyyymm
        
        # 解析日期並評估狀態
        df = self.parse_date_from_description(df)
        df = self.evaluate_status_based_on_dates_integrated(df, 'PR狀態')
        
        # 更新估計入帳標識
        df = self.update_estimation_based_on_status(df, 'PR狀態')
        
        # 判斷科目代碼 and 判斷其他欄位
        df = self.set_accounting_fields(df, ref_ac, ref_liability)
        
        # 格式化數據
        df = self.finalize_data_format(df)
        df = df.replace('<NA>', np.nan)

        return df

    def import_raw_data(self, url: str) -> Tuple[pd.DataFrame, int]:
        """導入PR數據
        
        Args:
            url: 文件路徑
            
        Returns:
            Tuple[pd.DataFrame, int]: 數據框和年月值
        """
        try:
            name = os.path.basename(url)

            if name.lower().endswith('.csv'):
                df = self.data_importer.import_file(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = self.data_importer.import_file(url, dtype=str)
                
            df.encoding = 'big5'
            
            # 數據轉換
            df['Line#'] = round(df['Line#'].astype(float), 0).astype(int).astype(str)
            df['GL#'] = np.where(df['GL#'] == 'N.A.', '666666', df['GL#'])
            
            # 從文件名獲取年月
            try:
                ym = int(name[0:6])
            except ValueError:
                self.logger.warning(f"無法從文件名 {name} 獲取年月值，使用默認值0")
                ym = 0
                
            self.logger.info(f"完成導入PR數據與基本填充處理, 形狀: {df.shape}")
            return df, ym
            
        except Exception as e:
            self.logger.error(f"導入數據文件 {name} 時出錯: {str(e)}", exc_info=True)
            raise

    def import_procurement(self, url: str) -> pd.DataFrame:
        """導入採購底稿
        
        Args:
            url: 文件路徑
            
        Returns:
            pd.DataFrame: 採購底稿數據
        """
        try:
            self.logger.info(f"正在導入採購底稿(PR): {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, header=0, dtype=str)
                
            df.encoding = 'big5'
            df['PR Line'] = df['PR#'].astype(str) + "-" + df['Line#'].astype(str)
            
            self.logger.info(f"成功導入採購底稿(PR), 形狀: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"導入採購底稿時出錯: {str(e)}", exc_info=True)
            raise

    def import_closing_list(self, url: str) -> List[str]:
        """導入關單清單
        
        Args:
            url: 文件路徑
            
        Returns:
            List[str]: 關單清單項目
        """
        try:
            self.logger.info(f"正在導入關單清單: {url}")
            
            if url.lower().endswith('.csv'):
                df = pd.read_csv(url, header=0, dtype=str, encoding='utf-8-sig')
            else:
                df = pd.read_excel(url, dtype=str)
                
            df.encoding = 'big5'
            
            # 2022/3/10 確認只會是A欄 PO# or PR#
            mapping_list = df.iloc[:, 0].tolist()
            unique_list = list(set(mapping_list))
            
            self.logger.info(f"成功導入關單清單, 項目數: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"導入關單清單時出錯: {str(e)}", exc_info=True)
            raise

    def process_with_closing_list(self, df: pd.DataFrame, mapping_list: List[str]) -> pd.DataFrame:
        """處理關單清單
        
        Args:
            df: PR數據框
            mapping_list: 關單清單
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            # 設置在關單清單中的PR狀態
            df['PR狀態'] = np.where(df['PR#'].isin(mapping_list), "待關單", df['PR狀態'])
            
            # 設置在關單清單中的PR不估計入帳
            df['是否估計入帳'] = np.where(df['PR#'].isin(mapping_list), "N", df['是否估計入帳'])
            
            self.logger.info(f"成功處理關單清單，找到 {df['PR#'].isin(mapping_list).sum()} 個在關單清單中的PR")
            return df
            
        except Exception as e:
            self.logger.error(f"處理關單清單時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理關單清單時出錯")

    def import_previous_wp(self, url: str) -> pd.DataFrame:
        """導入前期底稿
        
        Args:
            url: 文件路徑
            
        Returns:
            pd.DataFrame: 前期底稿數據
        """
        try:
            self.logger.info(f"正在導入前期底稿: {url}")
            
            y = pd.read_excel(url, dtype=str)
            y['Line#'] = round(y['Line#'].astype(float), 0).astype(int).astype(str)
            
            if 'PO#' in y.columns:
                y['PO Line'] = y['PO#'].astype(str) + "-" + y['Line#'].astype(str)
            else:
                y['PR Line'] = y['PR#'].astype(str) + "-" + y['Line#'].astype(str)
                
            self.logger.info(f"成功導入前期底稿, 形狀: {y.shape}")
            return y
            
        except Exception as e:
            self.logger.error(f"導入前期底稿時出錯: {str(e)}", exc_info=True)
            raise
        
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