"""
SPX PR處理器
直接繼承自BasePRProcessor，實現SPX特有的PR處理邏輯
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

try:
    from .pr_processor import BasePRProcessor
    from ...utils.logging import get_logger
    # from ...utils.config import get_config_manager
except ImportError:
    # 如果相對導入失敗，使用絕對導入
    import sys
    from pathlib import Path
    
    # 添加accrual_bot目錄到sys.path
    current_dir = Path(__file__).parent.parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from core.processors.pr_processor import BasePRProcessor
    from utils.logging import get_logger
    # from utils.config import get_config_manager


class SpxPRProcessor(BasePRProcessor):
    """SPX PR處理器，直接繼承自BasePRProcessor"""
    
    def __init__(self):
        """初始化SPX PR處理器"""
        super().__init__("SPX")
        self.logger = get_logger(self.__class__.__name__)
        # self.config_manager = get_config_manager()
        
        # SPX特有配置
        self.dept_accounts = ['650005', '610104', '630001', '650003', '600301', 
                              '610110', '610105', '600310', '620003', '610311']
    
    def filter_spx_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """過濾SPX產品代碼
        
        Args:
            df: 原始PR數據
            
        Returns:
            pd.DataFrame: 過濾後的DataFrame
        """
        try:
            filtered_df = df.loc[df['Product Code'].str.contains('(?i)LG_SPX'), :].reset_index(drop=True)
            self.logger.info(f"SPX產品過濾: {len(df)} -> {len(filtered_df)} 筆記錄")
            return filtered_df
        except Exception as e:
            self.logger.error(f"過濾SPX產品代碼時出錯: {str(e)}", exc_info=True)
            return df
    
    def add_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加SPX PR特有的必要列
        
        Args:
            df: 原始PR數據
            
        Returns:
            pd.DataFrame: 添加了必要列的數據框
        """
        try:
            # 生成PR Line
            df['PR Line'] = df['PR#'] + "-" + df['Line#']
            
            # 添加標記和備註
            df['Remarked by Procurement'] = np.nan
            df['Noted by Procurement'] = np.nan
            df['Remarked by FN'] = np.nan
            df['Noted by FN'] = np.nan
            
            # 添加計算欄位
            df['是否估計入帳'] = np.nan
            df['是否為FA'] = np.nan
            df['是否為S&M'] = self.determine_sm_status(df)
            
            # 添加會計相關欄位
            df['Account code'] = np.nan
            df['Account Name'] = np.nan
            df['Product code_c'] = np.nan
            df['Region_c'] = np.nan
            df['Dep.'] = np.nan
            df['Currency_c'] = np.nan
            df['Accr. Amount'] = np.nan
            df['Liability'] = np.nan
            
            # 添加審核相關欄位
            df['PR Product Code Check'] = np.nan
            df['Question from Reviewer'] = np.nan
            df['Check by AP'] = np.nan
            df['memo'] = np.nan
            
            # 添加上月備註欄位
            df['Remarked by 上月 FN'] = np.nan
            df['PR狀態'] = np.nan
            
            self.logger.info("成功添加SPX PR必要列")
            return df
            
        except Exception as e:
            self.logger.error(f"添加列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加列時出錯")
    
    def determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """確定是否為S&M - SPX邏輯
        
        Args:
            df: PR數據框
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        return np.where(df['GL#'].str.startswith('65'), "Y", "N")
    
    def process_with_procurement(self, df: pd.DataFrame, df_procu: pd.DataFrame) -> pd.DataFrame:
        """處理採購底稿 - SPX特有邏輯
        
        Args:
            df: PR數據框
            df_procu: 採購底稿數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            # 獲取採購底稿中的Remarked by Procurement
            map_dict = self.get_mapping_dict(df_procu, 'PR Line', 'Remarked by Procurement')
            df['Remarked by Procurement'] = df['PR Line'].map(map_dict)
            
            # 獲取採購底稿中的Noted by Procurement
            map_dict = self.get_mapping_dict(df_procu, 'PR Line', 'Noted by Procurement')
            df['Noted by Procurement'] = df['PR Line'].map(map_dict)
            
            # 設置FN備註
            df['Remarked by FN'] = df['Remarked by Procurement']
            
            # 尋找不在採購底稿中的PR
            pr_list = df['PR Line'].tolist()
            procurement_list = df_procu['PR Line'].tolist()
            outer_list = [pr for pr in pr_list if pr not in procurement_list]
            
            # 標記錯誤
            mask_payroll = df['PR Line'].isin(outer_list) & df['EBS Task'].str.contains("(?i)Payroll")
            
            df.loc[mask_payroll, 'PR狀態'] = "not in Procurement/Payroll"
            
            self.logger.info(f"成功處理採購底稿，找到 {len(outer_list)} 個不在採購底稿中的PR")
            return df
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def process_with_previous(self, df: pd.DataFrame, previous_wp: pd.DataFrame) -> pd.DataFrame:
        """處理前期底稿 - SPX特有邏輯
        
        Args:
            df: PR數據框
            previous_wp: 前期底稿數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            # 重命名前期底稿中的列
            previous_wp = previous_wp.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                    'Remarked by Procurement': 'Remark by PR Team_l'
                }
            )
            # 獲取前期FN備註
            map_dict = self.get_mapping_dict(previous_wp, 'PR Line', 'Remarked by FN_l')
            df['Remarked by 上月 FN'] = df['PR Line'].map(map_dict)
            
            self.logger.info("成功處理前期底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")

        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")
        
    def update_estimation_based_on_status(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """根據狀態更新估計入帳標識 - SPX特有邏輯
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態')
            
        Returns:
            pd.DataFrame: 更新了估計入帳的DataFrame
        """
        try:
            # 已完成狀態設為Y，未完成設為N
            mask_completed = df[status_col] == '已完成'
            mask_incomplete = df[status_col] == '未完成'
            
            df.loc[mask_completed, '是否估計入帳'] = 'Y'
            df.loc[mask_incomplete, '是否估計入帳'] = 'N'
            
            # 處理SPX特殊狀態
            if status_col == 'PR狀態':
                df.loc[df[status_col] == '待關單', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == 'Payroll', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '不預估', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '摘要內有押金/保證金/Deposit/找零金', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '關單/待刪', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '授扣GL調整', '是否估計入帳'] = 'N'
            
            # 根據其他條件更新
            not_accrued = ['不預估', '未完成', 'Payroll', '待關單', '未完成ERM', '格式錯誤', 'error(Description Period is out of ERM)']
            mask_procurement_completed = ((df['是否估計入帳'].isna() | (df['是否估計入帳'] == 'nan')) & 
                                          (df['Remarked by Procurement'] == '已完成') &
                                          (~df[status_col].isin(not_accrued)))
            df.loc[mask_procurement_completed, '是否估計入帳'] = 'Y'
            
            return df
        except Exception as e:
            self.logger.error(f"根據狀態更新估計入帳時出錯: {str(e)}", exc_info=True)
            raise ValueError("根據狀態更新估計入帳時出錯")

    def judge_cols(self, df: pd.DataFrame, ref_ac: pd.DataFrame, ref_liability: pd.DataFrame) -> pd.DataFrame:
        """判斷各種欄位值 - SPX特有邏輯
        
        Args:
            df: 要處理的DataFrame
            ref_ac: 科目參考數據
            ref_liability: 負債參考數據
            
        Returns:
            pd.DataFrame: 更新了各種欄位的DataFrame
        """
        try:
            # 設置科目名稱
            df['Account Name'] = pd.merge(
                df, ref_ac, how='left', 
                left_on='Account code', right_on='Account'
            ).loc[:, 'Account Desc']
            
            # 設置產品代碼 - SPX固定使用 "LG_SPX_OWN"
            need_book = (df['是否估計入帳'].eq('Y')) & (df['Product code_c'].isna())
            df['Product code_c'] = np.where(need_book, 'LG_SPX_OWN', pd.NA)
            
            # 設置Region - SPX固定使用 "TW"
            df['Region_c'] = np.where(df['是否估計入帳'] == 'Y', 'TW', pd.NA)
            
            # 設置部門 - SPX特有邏輯
            target_accounts = self.dept_accounts

            # 條件1: 是否估計入帳為Y 且 Account code在清單中
            # 條件2: 是否估計入帳為Y (但不滿足條件1)
            conditions = [
                (df['是否估計入帳'] == 'Y') & (df['Account code'].astype(str).isin([str(acc) for acc in target_accounts])), 
                (df['是否估計入帳'] == 'Y')                                    
            ]

            # 對應條件1: 取Department欄位前3個字元
            # 對應條件2: 給予'000'
            choices = [
                df['Department'].str[:3], 
                '000'            
            ]
            df['Dep.'] = np.select(conditions, choices, default=pd.NA)
            
            # 設置幣別
            df['Currency_c'] = np.where(df['是否估計入帳'] == 'Y', df['Currency'], pd.NA)
            
            # 設置應計金額
            df['Accr. Amount'] = np.where(df['是否估計入帳'] == 'Y', df['Entry Amount'], pd.NA)
            
            # 設置負債科目
            df['Liability'] = pd.merge(
                df, ref_liability, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Liability_y']
            
            # 設置產品代碼檢查
            df['PR Product Code Check'] = 'NA'
            
            return df
        except Exception as e:
            self.logger.error(f"判斷欄位值時出錯: {str(e)}", exc_info=True)
            raise ValueError("判斷欄位值時出錯")

    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據 - SPX特有格式
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 格式化後的數據框
        """
        try:
            # 格式化數值列
            df = self.format_numeric_columns_safely(
                df, 
                ['Line#'],
                ['Unit Price', 'Entry Amount', 'Accr. Amount']
            )
            
            # 格式化日期
            df = self.reformat_dates(df)
            
            # 移除臨時計算列
            temp_columns = ['Expected Received Month_轉換格式', 'YMs of Item Description', '檔案日期']
            for col in temp_columns:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PR狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self.clean_nan_values(df, columns_to_clean)
            
            # SPX特有的欄位重新排列
            # 重新排列上月備註欄位位置
            if 'Remarked by FN' in df.columns and 'Remarked by 上月 FN' in df.columns:
                fn_index = df.columns.get_loc('Remarked by FN') + 1
                last_month_col = df.pop('Remarked by 上月 FN')
                df.insert(fn_index, 'Remarked by 上月 FN', last_month_col)

            # 重新排列PR狀態欄位位置
            if 'Remarked by 上月 FN' in df.columns and 'PR狀態' in df.columns:
                fn_pr_index = df.columns.get_loc('Remarked by 上月 FN') + 1
                status_col = df.pop('PR狀態')
                df.insert(fn_pr_index, 'PR狀態', status_col)
            
            self.logger.info("成功格式化SPX PR數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")

    def process(self, fileUrl: str, file_name: str, 
                fileUrl_p: str = None,
                fileUrl_previwp: str = None) -> None:
        """處理SPX PR數據的主流程
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            fileUrl_previwp: 前期底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理SPX PR數據: {file_name}")
            
            # 導入原始數據
            df, date = self.importer.import_rawdata(fileUrl, file_name)
            
            # 導入參考數據 - SPX使用SPT的ref檔案參照會計科目和負債科目
            ref_ac, ref_liability = self.importer.import_reference_data('SPT')

            # 過濾SPX產品代碼
            df = self.filter_spx_product_code(df)
            
            # 添加必要列
            df = self.add_cols(df)
            
            # 處理採購底稿
            if fileUrl_p:
                try:
                    df_procu = self.importer.import_procurement(fileUrl_p)
                    if df_procu is not None and not df_procu.empty:
                        df = self.process_with_procurement(df, df_procu)
                        self.logger.info("成功處理採購底稿")
                    else:
                        self.logger.warning("採購底稿數據為空或無效")
                except Exception as e:
                    self.logger.warning(f"處理採購底稿時出錯: {str(e)}")
            
            # 處理前期底稿
            if fileUrl_previwp:
                try:
                    previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                    if previous_wp is not None and not previous_wp.empty:
                        df = self.process_with_previous(df, previous_wp)
                        self.logger.info("成功處理前期底稿")
                    else:
                        self.logger.warning("前期底稿數據為空或無效")
                except Exception as e:
                    self.logger.warning(f"處理前期底稿時出錯: {str(e)}")
            
            # 設置檔案日期
            df['檔案日期'] = date
            
            # 解析日期並評估狀態
            df = self.parse_date_from_description(df)
            df = self.evaluate_status_based_on_dates_integrated(df, 'PR狀態')
            
            # 更新估計入帳標識
            df = self.give_status_stage_1(df)
            df = self.update_estimation_based_on_status(df, 'PR狀態')
            
            # 判斷科目代碼
            df = self.judge_ac_code(df)
            
            # 判斷其他欄位
            df = self.judge_cols(df, ref_ac, ref_liability)
            
            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PR')
            
            self.logger.info(f"成功完成SPX PR數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理SPX PR數據時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"處理SPX PR數據時出錯: {str(e)}")
