import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from typing_extensions import override

from base_processor import BaseDataProcessor
from utils import Utils, Logger


class BasePRProcessor(BaseDataProcessor):
    """PR處理器基類，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化PR處理器
        
        Args:
            entity_type: 實體類型，'MOB'或'SPT'
        """
        super().__init__(entity_type)
        self.logger = Logger().get_logger(__name__)
    
    def add_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加必要列
        
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
            df['是否為S&M'] = self._determine_sm_status(df)
            
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
            df['Memo'] = np.nan
            
            # 添加上月備註欄位
            df['Remarked by 上月 FN'] = np.nan
            df['PR狀態'] = np.nan
            
            self.logger.info("成功添加必要列")
            return df
            
        except Exception as e:
            self.logger.error(f"添加列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加列時出錯")
    
    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """確定是否為S&M
        
        Args:
            df: PR數據框
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        if self.entity_type == 'MOB':
            return np.where(df['GL#'].str.startswith('65'), "Y", "N")
        else:  # SPT
            return np.where((df['GL#'] == '650003') | (df['GL#'] == '450014'), "Y", "N")
    
    def process_with_procurement(self, df: pd.DataFrame, df_procu: pd.DataFrame) -> pd.DataFrame:
        """處理採購底稿
        
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
            # mask_other = df['PR Line'].isin(outer_list) & (df['PR狀態'].isna() | (df['PR狀態'] == 'nan'))
            
            df.loc[mask_payroll, 'PR狀態'] = "not in Procurement/Payroll"
            # df.loc[mask_other, 'PR狀態'] = "Error" # not in procurement
            
            self.logger.info(f"成功處理採購底稿，找到 {len(outer_list)} 個不在採購底稿中的PR")
            return df
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def process_with_previous(self, df: pd.DataFrame, previous_wp: pd.DataFrame) -> pd.DataFrame:
        """處理前期底稿
        
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
    
    def process_special_cases(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理特殊情況
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            # 處理Payroll
            mask_payroll = (df['PR狀態'].isna() | (df['PR狀態'] == 'nan')) & df['EBS Task'].str.contains("(?i)Payroll")
            df.loc[mask_payroll, 'PR狀態'] = "Payroll"
            df.loc[df['EBS Task'].str.contains("(?i)Payroll"), '是否估計入帳'] = "N"
            
            # PR Task = Payroll --> Remarked by FN=Payroll
            df.loc[df['EBS Task'].str.contains("(?i)Payroll"), 'Remarked by FN'] = 'Payroll'
            
            # 處理分潤合作
            mask_profit = (df['PR狀態'].isna() | (df['PR狀態'] == 'nan')) & df['Item Description'].str.contains("蝦幣兌換|分潤合作")
            df.loc[mask_profit, 'PR狀態'] = "不預估"
            df.loc[df['PR狀態'] == '不預估', '是否估計入帳'] = "N"
            
            self.logger.info("成功處理特殊情況")
            return df
            
        except Exception as e:
            self.logger.error(f"處理特殊情況時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理特殊情況時出錯")
    
    def process_spt_specific(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理SPT特有邏輯(僅當entity_type為SPT時調用)
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        if self.entity_type != 'SPT':
            return df
            
        try:
            # Project含SPX, Remarked by FN=SPX
            df.loc[df['Product Code'].str.contains('(?i)SPX'), 'Remarked by FN'] = 'SPX'
            df.loc[df['Remarked by FN'] == 'SPX', '是否估計入帳'] = "N"
            
            # 處理分潤
            self._update_commission_data(df)
            
            self.logger.info("成功處理SPT特有邏輯")
            return df
            
        except Exception as e:
            self.logger.error(f"處理SPT特有邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理SPT特有邏輯時出錯")
    
    def _update_commission_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """更新分潤數據
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 更新後的數據框
        """
        try:
            def update_remark(df, type_=True):
                if type_:
                    keywords = '(?i)Affiliate commission|(?i)Shopee commission|蝦皮分潤計劃會員分潤金'
                    k1, k2 = 'Affiliate分潤合作', '品牌加碼'
                    gl_value = '650022'
                    product_code = 'EC_SPE_COM'
                else:
                    keywords = '(?i)AMS commission'
                    k1, k2 = 'Affiliate分潤合作', '品牌加碼'
                    gl_value = '650019'
                    product_code = 'EC_AMS_COST'
                
                # 條件
                condition = df['Item Description'].str.contains(keywords) | (
                    df['Item Description'].str.contains(k1) &
                    (~df['Item Description'].str.contains(k2)) if type_ else
                    df['Item Description'].str.contains(k2)
                )
                
                # 更新 Remarked by FN, GL#, Product code
                df.loc[condition, 'Remarked by FN'] = '分潤'
                df.loc[condition, 'GL#'] = gl_value
                df.loc[condition, 'Account code'] = gl_value
                df.loc[condition, 'Product code_c'] = product_code
                
                return df
            
            # 分兩種情況更新分潤數據
            df = update_remark(df)
            df = update_remark(df, type_=False)
            
            # 設置分潤估計入帳
            df.loc[(((df['GL#'] == '650022') | (df['GL#'] == '650019')) &
                   (df['Remarked by FN'] == '分潤') &
                   (df['PR狀態'].str.contains('已完成'))), '是否估計入帳'] = "Y"
            
            return df
            
        except Exception as e:
            self.logger.error(f"更新分潤數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("更新分潤數據時出錯")
    
    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 格式化後的數據框
        """
        try:
            # 處理數值格式
            int_cols = ['Line#']
            float_cols = ['Unit Price', 'Entry Amount', 'Accr. Amount']
            
            # 格式化GL#
            df['GL#'] = df['GL#'].fillna('666666').apply(lambda x: format(int(float(x)), ','))
            
            # 格式化數值列
            df = self._format_numeric_columns(df, int_cols, float_cols)
            
            # 格式化日期
            df = self._reformat_dates(df)
            
            # 移除臨時計算列
            if 'Expected Received Month_轉換格式' in df.columns:
                df.drop(columns=['Expected Received Month_轉換格式'], axis='columns', inplace=True)
            
            if 'YMs of Item Description' in df.columns:
                df.drop(columns=['YMs of Item Description'], axis='columns', inplace=True)
            
            if '檔案日期' in df.columns:
                df.drop(columns=['檔案日期'], axis='columns', inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PR狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self._clean_nan_values(df, columns_to_clean)
            
            # 重新排列上月備註欄位位置
            num = df.columns.get_loc('Remarked by FN') + 1
            last_col = df.pop(df.columns[df.columns.get_loc('Remarked by 上月 FN')])
            df.insert(num, 'Remarked by 上月 FN', last_col)

            num = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_col = df.pop(df.columns[df.columns.get_loc('PR狀態')])
            df.insert(num, 'PR狀態', last_col)
            
            self.logger.info("成功格式化數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")
    
    def process(self, fileUrl: str, file_name: str, 
                fileUrl_p: str = None, fileUrl_c: str = None, 
                fileUrl_previwp: str = None) -> None:
        """處理PR數據的主流程
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            fileUrl_previwp: 前期底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理PR數據: {file_name}")
            
            # 導入原始數據
            df, date = self.importer.import_rawdata(fileUrl, file_name)
            
            # 導入參考數據
            ref_key = 'SPT' if self.entity_type == 'SPT' else 'MOB'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)
            
            # 添加必要列
            df = self.add_cols(df)
            
            # 處理採購底稿
            if fileUrl_p:
                df_procu = self.importer.import_procurement(fileUrl_p)
                df = self.process_with_procurement(df, df_procu)
            
            # 處理關單清單
            if fileUrl_c:
                mapping_list = self.importer.import_closing_list(fileUrl_c)
                df = self.process_with_closing_list(df, mapping_list)
            
            # 處理前期底稿
            if fileUrl_previwp:
                previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                df = self.process_with_previous(df, previous_wp)
            
            # 處理特殊情況
            df = self.process_special_cases(df)
            
            # 設置檔案日期
            df['檔案日期'] = date
            
            # 解析日期並評估狀態
            df = self.parse_date_from_description(df)
            df = self.evaluate_status_based_on_dates(df, 'PR狀態')
            
            # 更新估計入帳標識
            df = self.update_estimation_based_on_status(df, 'PR狀態')
            
            # 判斷科目代碼
            df = self.judge_ac_code(df)
            
            # 處理SPT特有邏輯
            df = self.process_spt_specific(df)
            
            # 判斷其他欄位
            df = self.judge_cols(df, ref_ac, ref_liability)
            
            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PR')
            
            self.logger.info(f"成功完成PR數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理PR數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PR數據時出錯")


class SpxPRProcessor(BaseDataProcessor):

    def __init__(self, entity_type: str = "SPX"):
        """
        初始化PR處理器
        
        Args:
            entity_type: 實體類型，'SPX'
        """
        super().__init__(entity_type)
        self.logger = Logger().get_logger(__name__)
    
    @override
    def add_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加必要列
        
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
            df['是否為S&M'] = self._determine_sm_status(df)
            
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
            
            self.logger.info("成功添加必要列")
            return df
            
        except Exception as e:
            self.logger.error(f"添加列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加列時出錯")
    
    def fillter_spx_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[df['Product Code'].str.contains('(?i)LG_SPX'), :].reset_index(drop=True)
    
    @override
    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """確定是否為S&M
        
        Args:
            df: PR數據框
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        return np.where(df['GL#'].str.startswith('65'), "Y", "N")
    
    @override
    def process_with_procurement(self, df: pd.DataFrame, df_procu: pd.DataFrame) -> pd.DataFrame:
        """處理採購底稿
        
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
            # mask_other = df['PR Line'].isin(outer_list) & (df['PR狀態'].isna() | (df['PR狀態'] == 'nan'))
            
            df.loc[mask_payroll, 'PR狀態'] = "not in Procurement/Payroll"
            # df.loc[mask_other, 'PR狀態'] = "Error" # not in procurement
            
            self.logger.info(f"成功處理採購底稿，找到 {len(outer_list)} 個不在採購底稿中的PR")
            return df
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    @override
    def process_with_previous(self, df: pd.DataFrame, previous_wp: pd.DataFrame) -> pd.DataFrame:
        """處理前期底稿
        
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

    def give_status_stage_1(self, df):
        try:
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是 FA 相關科目
            cond1 = df['Item Description'].str.contains('(?i)押金|保證金|Deposit|找零金|定存', na=False)
            is_fa = df['GL#'] == self.config.get('FA_ACCOUNTS', 'spx')
            cond2 = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA，避免前端選錯加強過濾
            df.loc[cond1 & ~is_fa & ~cond2, 'PR狀態'] = '摘要內有押金/保證金/Deposit/找零金'

            # 條件2：該筆資料在前期底稿中有"關單"或"待刪"字
            cond2 = df['Remarked by 上月 FN'].fillna('system_filled').str.contains('關單|待刪')
            df.loc[cond2, 'PR狀態'] = '關單/待刪'

            # 條件3：該筆資料supplier是"台電"、"台水"、"北水"
            cond3 = df['PR Supplier'].fillna('system_filled').str.contains('台灣電力|自來水')
            df.loc[cond3, 'PR狀態'] = '授扣GL調整'

            self.logger.info("成功給予第一階段狀態")
            return df

        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")
        
    @override
    def update_estimation_based_on_status(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """根據狀態更新估計入帳標識
        
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
            
            # 處理特殊狀態
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

    @override
    def judge_cols(self, df: pd.DataFrame, ref_ac: pd.DataFrame, ref_liability: pd.DataFrame) -> pd.DataFrame:
        """判斷各種欄位值
        
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
            
            # 設置產品代碼; hard code: "LG_SPX_OWN"
            need_book = (df['是否估計入帳'].eq('Y')) & (df['Product code_c'].isna())
            df['Product code_c'] = np.where(need_book, 'LG_SPX_OWN', pd.NA)
            
            # 設置Region，一律使用"TW"
            df['Region_c'] = np.where(df['是否估計入帳'] == 'Y', 'TW', pd.NA)
            
            # 設置部門;
            target_accounts = [item.strip().strip("'") for item in self.config.get('SPX', 'exp_accounts').split(',')]

            # 條件1: 是否估計入帳為Y 且 Account code在清單中
            # 條件2: 是否估計入帳為Y (但不滿足條件1)
            conditions = [
                (df['是否估計入帳'] == 'Y') & (df['Account code'].isin(target_accounts)), 
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

    @override
    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據
        
        Args:
            df: PR數據框
            
        Returns:
            pd.DataFrame: 格式化後的數據框
        """
        try:
            # 處理數值格式
            int_cols = ['Line#']
            float_cols = ['Unit Price', 'Entry Amount', 'Accr. Amount']
            
            # 格式化GL#
            df['GL#'] = df['GL#'].fillna('666666').apply(lambda x: format(int(float(x)), ','))
            
            # 格式化數值列
            df = self._format_numeric_columns(df, int_cols, float_cols)
            
            # 格式化日期
            df = self._reformat_dates(df)
            
            # 移除臨時計算列
            if 'Expected Received Month_轉換格式' in df.columns:
                df.drop(columns=['Expected Received Month_轉換格式'], axis='columns', inplace=True)
            
            if 'YMs of Item Description' in df.columns:
                df.drop(columns=['YMs of Item Description'], axis='columns', inplace=True)
            
            if '檔案日期' in df.columns:
                df.drop(columns=['檔案日期'], axis='columns', inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PR狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self._clean_nan_values(df, columns_to_clean)
            
            # 重新排列上月備註欄位位置
            num = df.columns.get_loc('Remarked by FN') + 1
            last_col = df.pop(df.columns[df.columns.get_loc('Remarked by 上月 FN')])
            df.insert(num, 'Remarked by 上月 FN', last_col)

            num = df.columns.get_loc('Remarked by 上月 FN') + 1
            last_col = df.pop(df.columns[df.columns.get_loc('PR狀態')])
            df.insert(num, 'PR狀態', last_col)
            
            self.logger.info("成功格式化數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")

    def process(self, fileUrl: str, file_name: str, 
                fileUrl_p: str = None,
                fileUrl_previwp: str = None) -> None:
        """處理PR數據的主流程
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            fileUrl_previwp: 前期底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理PR數據: {file_name}")
            
            # 導入原始數據
            df, date = self.importer.import_rawdata(fileUrl, file_name)
            
            # 導入參考數據
            ref_key = 'SPT'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)

            df = self.fillter_spx_product_code(df).reset_index(drop=True)
            
            # 添加必要列
            df = self.add_cols(df)
            
            # 處理採購底稿
            if fileUrl_p:
                df_procu = self.importer.import_procurement(fileUrl_p)
                df = self.process_with_procurement(df, df_procu)
            
            # 處理前期底稿
            if fileUrl_previwp:
                previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                df = self.process_with_previous(df, previous_wp)
            
            # 設置檔案日期
            df['檔案日期'] = date
            
            # 解析日期並評估狀態
            df = self.parse_date_from_description(df)
            df = self.evaluate_status_based_on_dates(df, 'PR狀態')
            
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
            
            self.logger.info(f"成功完成PR數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理PR數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PR數據時出錯")
