import os
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any

from base_processor import BaseDataProcessor
from utils import Utils, Logger, AsyncDataImporter, AsyncGoogleSheetsBase


class BasePOProcessor(BaseDataProcessor):
    """PO處理器基類，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化PO處理器
        
        Args:
            entity_type: 實體類型，'MOB'或'SPT'
        """
        super().__init__(entity_type)
        self.logger = Logger().get_logger(__name__)
    
    def add_cols(self, df: pd.DataFrame, m: int) -> Tuple[pd.DataFrame, int]:
        """添加必要列
        
        Args:
            df: 原始PO數據
            m: 月份
            
        Returns:
            Tuple[pd.DataFrame, int]: 添加了必要列的DataFrame和更新的月份
        """
        try:
            # 添加狀態欄位
            df['是否結案'] = np.where(df['Closed For Invoice'] == '0', "未結案", '結案')
            df['結案是否有差異數量'] = np.where(
                df.是否結案 == '結案',
                df['Entry Quantity'].astype(float) - df['Billed Quantity'].astype(float),
                '未結案'
            )
            df['Check with Entry Invoice'] = np.where(
                df['Entry Billed Amount'].astype(float) > 0,
                df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float),
                '未入帳'
            )
            
            # 生成PR Line和PO Line
            df['PR Line'] = df['PR#'] + '-' + df['Line#']
            df['PO Line'] = df['PO#'] + '-' + df['Line#']
            
            # 添加標記和備註
            df['Remarked by Procurement'] = np.nan
            df['Noted by Procurement'] = np.nan
            df['Remarked by FN'] = np.nan
            df['Noted by FN'] = np.nan
            
            # 計算上月
            def month_c(m):
                if m == 1:
                    return 12
                else:
                    return m - 1
            
            m = month_c(m)
            df[f'Remarked by {m}月 Procurement'] = np.nan
            
            # 添加計算欄位
            df['是否估計入帳'] = np.nan
            df['是否為FA'] = self._determine_fa_status(df)
            df['是否為S&M'] = self._determine_sm_status(df)
            
            # 添加會計相關欄位
            df['Account code'] = np.nan
            df['Account Name'] = np.nan
            df['Product code'] = np.nan
            df['Region_c'] = np.nan
            df['Dep.'] = np.nan
            df['Currency_c'] = np.nan
            df['Accr. Amount'] = np.nan
            df['Liability'] = np.nan
            df['是否有預付'] = np.nan
            
            # 添加審核相關欄位
            df['PR Product Code Check'] = np.nan
            df['Question from Reviewer'] = np.nan
            df['Check by AP'] = np.nan
            
            # 添加狀態和上月備註
            df['PO狀態'] = np.nan
            df['Remarked by 上月 FN'] = np.nan
            
            self.logger.info("成功添加必要列")
            return df, m
            
        except Exception as e:
            self.logger.error(f"添加列時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加列時出錯")
    
    def _determine_fa_status(self, df: pd.DataFrame) -> pd.Series:
        """確定是否為FA
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為FA的結果
        """
        if self.entity_type == 'MOB':
            return np.where(df['GL#'].isin(self.fa_accounts), 'Y', '')
        else:  # SPT
            return np.where(df['GL#'].isin(self.fa_accounts), 'Y', '')
    
    def _determine_sm_status(self, df: pd.DataFrame) -> pd.Series:
        """確定是否為S&M
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.Series: 是否為S&M的結果
        """
        if self.entity_type == 'MOB':
            return np.where(df['GL#'].str.startswith('65'), "Y", "N")
        else:  # SPT
            return np.where((df['GL#'] == '650003') | (df['GL#'] == '450014'), "Y", "N")
    
    def judge_closing(self, df: pd.DataFrame, mapping_list: List[str]) -> pd.DataFrame:
        """處理關單清單
        
        Args:
            df: PO DataFrame
            mapping_list: 關單清單
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 設置在關單清單中的PO狀態
            df['PO狀態'] = np.where(df['PO#'].isin(mapping_list), "待關單", df['PO狀態'])
            
            # 設置在關單清單中的PO不估計入帳
            df['是否估計入帳'] = np.where(df['PO#'].isin(mapping_list), "N", df['是否估計入帳'])
            
            self.logger.info(f"成功處理關單清單，找到 {df['PO#'].isin(mapping_list).sum()} 個在關單清單中的PO")
            return df
            
        except Exception as e:
            self.logger.error(f"處理關單清單時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理關單清單時出錯")
    
    def judge_previous(self, df: pd.DataFrame, previous_wp: pd.DataFrame, m: int) -> pd.DataFrame:
        """處理前期底稿
        
        Args:
            df: PO DataFrame
            previous_wp: 前期底稿DataFrame
            m: 月份
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
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
            map_dict = self.get_mapping_dict(previous_wp, 'PO Line', 'Remarked by FN_l')
            df['Remarked by 上月 FN'] = df['PO Line'].map(map_dict)
            
            # 獲取前期採購備註
            df[f'Remarked by {m}月 Procurement'] = \
                df['PO Line'].map(self.get_mapping_dict(previous_wp, 'PO Line', 'Remark by PR Team_l'))
            
            self.logger.info("成功處理前期底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def judge_procurement(self, df: pd.DataFrame, df_procu: pd.DataFrame) -> pd.DataFrame:
        """處理採購底稿
        
        Args:
            df: PO DataFrame
            df_procu: 採購底稿DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 重命名採購底稿中的列
            df_procu = df_procu.rename(
                columns={
                    'Remarked by Procurement': 'Remark by PR Team',
                    'Noted by Procurement': 'Noted by PR'
                }
            )
            
            # 獲取採購底稿中的備註
            map_dict = self.get_mapping_dict(df_procu, 'PO Line', 'Remark by PR Team')
            df['Remarked by Procurement'] = df['PO Line'].map(map_dict)
            
            map_dict = self.get_mapping_dict(df_procu, 'PO Line', 'Noted by PR')
            df['Noted by Procurement'] = df['PO Line'].map(map_dict)
            
            # 使用PR Line查找
            map_dict = self.get_mapping_dict(df_procu, 'PR Line', 'Remark by PR Team')
            df['Remarked by Procurement'] = \
                (df.apply(lambda x: map_dict.get(x['PR Line'], None) 
                          if x['Remarked by Procurement'] is np.nan else x['Remarked by Procurement'], axis=1))

            df['Remarked by FN'] = df['Remarked by Procurement']
            
            # 標記不在採購底稿中的PO
            pr_list_po = df_procu['PO Line'].tolist()
            pr_list_pr = df_procu['PR Line'].tolist()
            
            df['PO狀態'] = np.where(
                (~df['PO Line'].isin(pr_list_po)) & (~df['PR Line'].isin(pr_list_pr)),
                "Not In Procurement WP",
                df['PO狀態']
            )
            
            self.logger.info("成功處理採購底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理採購底稿時出錯")
    
    def get_logic_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理日期邏輯
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 處理分潤合作
            mask_profit_sharing = (((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) & 
                                   df['Item Description'].str.contains('分潤合作'))
            df.loc[mask_profit_sharing, 'PO狀態'] = '分潤'
            
            if self.entity_type == 'SPT':
                df['是否估計入帳'] = np.where(df.PO狀態.eq('分潤'), df['是否估計入帳'] == '分潤', df['是否估計入帳'])
            
            # 處理已入帳
            mask_posted = ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) & (df['PO Entry full invoiced status'] == '1')
            df.loc[mask_posted, 'PO狀態'] = '已入帳'
            df.loc[df['PO狀態'] == '已入帳', '是否估計入帳'] = "N"
            
            # 解析日期
            df = self.parse_date_from_description(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"處理日期邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理日期邏輯時出錯")
    
    def erm(self, df: pd.DataFrame, ym: int, ref_a: pd.DataFrame, ref_b: pd.DataFrame) -> pd.DataFrame:
        """處理ERM邏輯
        
        Args:
            df: PO DataFrame
            ym: 年月
            ref_a: 科目參考數據
            ref_b: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 設置檔案日期
            df['檔案日期'] = ym
            
            # 定義ERM狀態條件
            # 這裡簡化了原始代碼中的複雜條件邏輯，使其更易於閱讀和維護
            # 條件1：已完成
            condition_completed = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件2：全付完，未關單
            condition_paid_not_closed = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件3：已完成但有未付款部分
            condition_completed_with_unpaid = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) != 0)
            )
            
            # 條件4：需檢查收貨
            condition_check_receipt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 條件5：未完成(MOBTW)
            condition_incomplete_mobtw = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] > df['檔案日期']) &
                (df['Company'] == 'MOBTW')
            )
            
            # 條件6：未完成(SPTTW，未收貨)
            condition_incomplete_spt_no_receipt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] > df['檔案日期']) &
                (df['Received Quantity'].astype(float) == 0) &
                (df['Company'] == 'SPTTW')
            )
            
            # 條件7：提早完成
            condition_early_completion = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] > df['檔案日期']) &
                (df['Received Quantity'].astype(float) != 0) &
                (df['Company'] == 'SPTTW')
            )
            
            # 條件8：範圍錯誤(MOBTW)
            condition_range_error_mobtw = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['Company'] == 'MOBTW') &
                (df['YMs of Item Description'] != '100001,100002')
            )
            
            # 條件9：已完成ERM(SPTTW)
            condition_completed_erm_spt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['Company'] == 'SPTTW') &
                (df['YMs of Item Description'] != '100001,100002') &
                (df['Received Quantity'].astype(float) != 0) &
                (df['Entry Quantity'] == df['Received Quantity'])
            )
            
            # 條件10：部分完成ERM(SPTTW)
            condition_partially_completed_erm_spt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['Company'] == 'SPTTW') &
                (df['YMs of Item Description'] != '100001,100002') &
                (df['Received Quantity'].astype(float) != 0) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 條件11：未完成ERM(SPTTW)
            condition_incomplete_erm_spt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['Company'] == 'SPTTW') &
                (df['YMs of Item Description'] != '100001,100002') &
                (df['Received Quantity'].astype(float) == 0)
            )
            
            # 組合所有條件
            conditions = [
                condition_completed,
                condition_paid_not_closed,
                condition_completed_with_unpaid,
                condition_check_receipt,
                condition_incomplete_mobtw,
                condition_incomplete_spt_no_receipt,
                condition_early_completion,
                condition_range_error_mobtw,
                condition_completed_erm_spt,
                condition_partially_completed_erm_spt,
                condition_incomplete_erm_spt
            ]
            
            # 對應的結果
            results = [
                '已完成',
                '全付完，未關單?',
                '已完成',
                'Check收貨',
                '未完成',
                '未完成',
                '提早完成?',
                'error(Description Period is out of ERM)',
                '已完成ERM',
                '部分完成ERM',
                '未完成ERM'
            ]
            
            # 應用條件
            df['PO狀態'] = np.select(conditions, results, default=df['PO狀態'])
            
            # 處理格式錯誤
            mask_format_error = (((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) & 
                                 (df['YMs of Item Description'] == '100001,100002'))
            df.loc[mask_format_error, 'PO狀態'] = '格式錯誤，退單'
            
            # 根據PO狀態設置估計入帳
            # 條件1: 已完成
            mask_completed = (df['PO狀態'] == '已完成') & (df['Remarked by Procurement'] != '已入帳')
            df.loc[mask_completed, '是否估計入帳'] = 'Y'
            
            # 條件2: error(Description Period is out of ERM)或全付完，未關單?
            mask_design_formula = (df['PO狀態'] == 'error(Description Period is out of ERM)') | (df['PO狀態'] == '全付完，未關單?')
            df.loc[mask_design_formula, '是否估計入帳'] = '設計公式'
            
            # 條件3: 未完成
            mask_incomplete = df['PO狀態'] == '未完成'
            df.loc[mask_incomplete, '是否估計入帳'] = 'N'
            
            # 條件4: ERM相關狀態
            df.loc[df.PO狀態.eq('已完成ERM'), '是否估計入帳'] = 'Y'
            df.loc[df.PO狀態.eq('未完成ERM'), '是否估計入帳'] = 'N'
            
            # 條件5: 採購已完成但狀態未設置
            mask_procurement_completed = (
                (df['是否估計入帳'] == 'nan') &
                (df['Remarked by Procurement'] == '已完成') &
                (df['PO狀態'] != '格式錯誤，退單') &
                (df['PO狀態'] != '全付完，未關單?') &
                (df['PO狀態'] != '提早完成?') &
                (df['PO狀態'] != 'Check收貨') &
                (df['PO狀態'] != 'error(Description Period is out of ERM)') &
                (df['PO狀態'] != '已完成ERM') &
                (df['PO狀態'] != '部分完成ERM') &
                (df['PO狀態'] != '未完成ERM')
            )
            df.loc[mask_procurement_completed, '是否估計入帳'] = 'Y'
            
            # 處理設計公式狀態
            # error(Description Period is out of ERM)且Remarked by Procurement為已完成且Check with Entry Invoice為未結案
            mask_design_formula_1 = (
                (df['是否估計入帳'] == '設計公式') &
                (df['PO狀態'] == 'error(Description Period is out of ERM)') &
                (df['Check with Entry Invoice'] == '未結案') &
                (df['Remarked by Procurement'] == '已完成')
            )
            df.loc[mask_design_formula_1, '是否估計入帳'] = 'Y'
            
            # error(Description Period is out of ERM)且Remarked by Procurement為未完成且Check with Entry Invoice為未結案
            mask_design_formula_2 = (
                (df['是否估計入帳'] == '設計公式') &
                (df['PO狀態'] == 'error(Description Period is out of ERM)') &
                (df['Check with Entry Invoice'] == '未結案') &
                (df['Remarked by Procurement'] == '未完成')
            )
            df.loc[mask_design_formula_2, '是否估計入帳'] = 'N'
            
            # error(Description Period is out of ERM)且Remarked by Procurement為已完成且Check with Entry Invoice不是未結案
            mask_design_formula_3 = (
                (df['是否估計入帳'] == '設計公式') &
                (df['PO狀態'] == 'error(Description Period is out of ERM)') &
                (df['Check with Entry Invoice'] != '未結案') &
                (df['Remarked by Procurement'] == '已完成')
            )
            df.loc[mask_design_formula_3, '是否估計入帳'] = 'N'
            
            # 採購備註為error，不估計
            df.loc[df['Remarked by Procurement'] == 'error', '是否估計入帳'] = 'N'
            
            # 設置Account code
            df.loc[df['是否估計入帳'] == 'Y', 'Account code'] = df.loc[df['是否估計入帳'] == 'Y', 'GL#']
            
            # 設置Account Name
            df['Account Name'] = pd.merge(
                df, ref_a, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Account Desc']
            
            # 設置Product code
            df.loc[df['是否估計入帳'] == 'Y', 'Product code'] = df.loc[df['是否估計入帳'] == 'Y', 'Product Code']
            
            # 設置Region_c
            if self.entity_type == 'MOB':
                df.loc[df['是否估計入帳'] == 'Y', 'Region_c'] = df.loc[df['是否估計入帳'] == 'Y', 'Region']
            else:  # SPT
                if 'Account code' in df.columns:
                    mask_income_expense = df['Account code'].str.match(r'^[4-6]')
                    df.loc[(df['是否估計入帳'] == 'Y') & mask_income_expense, 'Region_c'] = \
                        df.loc[(df['是否估計入帳'] == 'Y') & mask_income_expense, 'Region']
                    df.loc[(df['是否估計入帳'] == 'Y') & ~mask_income_expense.fillna(False), 'Region_c'] = '000'
            
            # 設置Dep.
            if self.entity_type == 'MOB':
                df.loc[df['是否估計入帳'] == 'Y', 'Dep.'] = df.loc[df['是否估計入帳'] == 'Y', 'Department'].str[:3]
            else:  # SPT
                # if 'Account code' in df.columns:
                #     mask_expense = df['Account code'].str.startswith('6')
                #     df.loc[(df['是否估計入帳'] == 'Y') & mask_expense, 'Dep.'] = 'A09'
                #     df.loc[(df['是否估計入帳'] == 'Y') & ~mask_expense, 'Dep.'] = '000'
                df['Dep.'] = self.convert_dep_code(df)
            
            # 設置Currency_c
            df.loc[df['是否估計入帳'] == 'Y', 'Currency_c'] = df.loc[df['是否估計入帳'] == 'Y', 'Currency']
            
            # 設置Accr. Amount
            df.loc[df['是否估計入帳'] == 'Y', 'Accr. Amount'] = (
                df.loc[df['是否估計入帳'] == 'Y', 'Entry Amount'].astype(float) - 
                df.loc[df['是否估計入帳'] == 'Y', 'Entry Billed Amount'].astype(float)
            )
            
            # 設置Liability
            df['Liability'] = pd.merge(
                df, ref_b, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Liability_y']
            
            # 設置是否有預付
            df.loc[df['是否估計入帳'] == 'Y', '是否有預付'] = df.loc[df['是否估計入帳'] == 'Y', 'Entry Prepay Amount']
            
            # 設置PR Product Code Check
            if 'Product code' in df.columns and 'Project' in df.columns:
                mask_product_code = df['Product code'].notnull()
                try:
                    product_match = df.loc[mask_product_code, 'Project'].str.findall(r'^(\w+(?:))').apply(
                        lambda x: x[0] if len(x) > 0 else ''
                    ) == df.loc[mask_product_code, 'Product code']
                    
                    df.loc[mask_product_code, 'PR Product Code Check'] = np.where(
                        product_match, 'good', 'bad'
                    )
                except Exception as e:
                    self.logger.error(f"設置PR Product Code Check時出錯: {str(e)}", exc_info=True)
                    raise ValueError("設置PR Product Code Check時出錯")
            
            self.logger.info("成功處理ERM邏輯")
            return df
            
        except Exception as e:
            self.logger.error(f"處理ERM邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理ERM邏輯時出錯")
    
    def process_spt_specific(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理SPT特有邏輯(僅當entity_type為SPT時調用)
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
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
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 更新後的DataFrame
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
                df.loc[condition, 'Product code'] = product_code
                
                return df
            
            # 分兩種情況更新分潤數據
            df = update_remark(df)
            df = update_remark(df, type_=False)
            
            # 設置分潤估計入帳
            df.loc[(df['GL#'] == '650022') | (df['GL#'] == '650019'), '是否估計入帳'] = "Y"
            
            return df
            
        except Exception as e:
            self.logger.error(f"更新分潤數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("更新分潤數據時出錯")
    
    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            # 處理數值格式
            int_cols = ['Line#', 'GL#']
            float_cols = [
                'Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                'Entry Billed Amount', 'Entry Prepay Amount', 
                'PO Entry full invoiced status', 'Accr. Amount'
            ]
            
            # 格式化數值列
            df = self._format_numeric_columns(df, int_cols, float_cols)
            
            # 格式化日期
            df = self._reformat_dates(df)
            
            # 移除臨時計算列
            if '檔案日期' in df.columns:
                df.drop(columns=['檔案日期'], axis='columns', inplace=True)
                
            if 'Expected Received Month_轉換格式' in df.columns:
                df.drop(columns=['Expected Received Month_轉換格式'], axis='columns', inplace=True)
                
            if 'YMs of Item Description' in df.columns:
                df.drop(columns=['YMs of Item Description'], axis='columns', inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PO狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self._clean_nan_values(df, columns_to_clean)
            
            # 處理特殊值
            if self.entity_type == 'SPT':
                df['是否估計入帳'] = df['是否估計入帳'].str.replace('分潤', '').replace("0.0", "")
                if 'PR Product Code Check' in df.columns:
                    df.drop(columns='PR Product Code Check', inplace=True)
            
            # 重新排列上月備註欄位位置
            num = df.columns.get_loc('Remarked by FN') + 1
            last_col = df.pop(df.columns[-1])
            df.insert(num, last_col.name, last_col)  # Move Remarked by 上月 FN
            
            # 重新排列PO狀態欄位位置
            num = df.columns.get_loc('Remarked by FN') + 2
            last_col = df.pop(df.columns[df.columns.get_loc('PO狀態')])
            df.insert(num, 'PO狀態', last_col)  # Move PO狀態
            
            self.logger.info("成功格式化數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")
    
    def process(self, fileUrl: str, file_name: str, 
                fileUrl_previwp: str = None, fileUrl_p: str = None, 
                fileUrl_c: str = None) -> None:
        """處理PO數據的主流程（使用並發導入）
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理PO數據: {file_name}")
            
            # 準備文件信息字典
            file_info = {
                'raw_po': fileUrl,
                'previous': fileUrl_previwp if fileUrl_previwp else None,
                'procurement_po': fileUrl_p if fileUrl_p else None,
                'closing_po': fileUrl_c if fileUrl_c else None
            }
            
            # 並發導入所有文件
            import_results = self.concurrent_import_files(file_info)
            
            # 提取結果 - 正確處理原始PO數據
            if 'raw_po' in import_results:
                raw_po_result = import_results['raw_po']
                # 檢查raw_po_result是否為元組且包含3個元素
                if isinstance(raw_po_result, tuple) and len(raw_po_result) == 3:
                    df, date, m = raw_po_result
                else:
                    self.logger.error("原始PO數據格式不正確")
                    raise ValueError("原始PO數據格式不正確")
            else:
                self.logger.error("無法導入原始PO數據")
                raise ValueError("無法導入原始PO數據")
            
            # 導入參考數據
            ref_key = 'SPT' if self.entity_type == 'SPT' else 'MOB'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理關單清單 - 安全性檢查
            if 'closing_po' in import_results:
                closing_result = import_results['closing_po']
                # 檢查是否為None或空列表
                if self._is_valid_data(closing_result):
                    mapping_list = closing_result
                    df = self.judge_closing(df, mapping_list)
                    self.logger.info("成功處理關單清單")
                else:
                    self.logger.warning("關單清單為空或無效")
            
            # 處理前期底稿 - 安全性檢查
            if 'previous' in import_results:
                previous_result = import_results['previous']
                # 檢查是否為None或空DataFrame
                if self._is_valid_data(previous_result):
                    previous_wp = previous_result
                    df = self.judge_previous(df, previous_wp, m)
                    self.logger.info("成功處理前期底稿")
                else:
                    self.logger.warning("前期底稿為空或無效")
            
            # 處理採購底稿 - 安全性檢查
            if 'procurement_po' in import_results:
                procurement_result = import_results['procurement_po']
                # 檢查是否為None或空DataFrame
                if self._is_valid_data(procurement_result):
                    procurement = procurement_result
                    df = self.judge_procurement(df, procurement)
                    self.logger.info("成功處理採購底稿")
                else:
                    self.logger.warning("採購底稿為空或無效")
            
            # 處理日期邏輯
            df = self.get_logic_date(df)
            
            # 處理ERM邏輯
            df = self.erm(df, date, ref_ac, ref_liability)
            
            # 處理SPT特有邏輯
            df = self.process_spt_specific(df)
            
            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PO')
            
            self.logger.info(f"成功完成PO數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理PO數據時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"處理PO數據時出錯: {str(e)}")
            
    def _is_valid_data(self, data) -> bool:
        """檢查數據是否有效
        
        Args:
            data: 要檢查的數據
            
        Returns:
            bool: 數據是否有效
        """
        try:
            if data is None:
                return False
            
            # 檢查 DataFrame
            if isinstance(data, pd.DataFrame):
                return not data.empty
            
            # 檢查 Series
            if isinstance(data, pd.Series):
                return not data.empty
            
            # 檢查列表或元組
            if isinstance(data, (list, tuple)):
                return len(data) > 0
            
            # 檢查字典
            if isinstance(data, dict):
                return len(data) > 0
            
            # 其他情況，假設非None即有效
            return True
            
        except Exception as e:
            self.logger.error(f"檢查數據有效性時出錯: {str(e)}", exc_info=True)
            return False

class SpxPOProcessor(BasePOProcessor):
    """SPX處理器，繼承自BaseDataProcessor"""
    
    def __init__(self, entity_type: str = "SPX"):
        """
        初始化PO處理器
        
        Args:
            entity_type: 實體類型，'SPX'
        """
        super().__init__(entity_type)
        self.logger = Logger().get_logger(self.__class__.__name__)

    def fillter_spx_product_code(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[df['Product Code'].str.contains('(?i)LG_SPX_OWN'), :].reset_index(drop=True)

    def add_cols(self, df: pd.DataFrame, m: int) -> Tuple[pd.DataFrame, int]:
        # 先執行父類別的邏輯
        df, m = super().add_cols(df, m)
        # 再額外新增特定欄位
        df['memo'] = np.nan
        df['GL DATE'] = np.nan

        df['Remarked by Procurement PR'] = np.nan
        df['Noted by Procurement PR'] = np.nan
        df['Remarked by 上月 FN PR'] = np.nan
        return df, m

    def get_logic_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理日期邏輯
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 解析日期
            df = self.parse_date_from_description(df)
            return df
            
        except Exception as e:
            self.logger.error(f"處理日期邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理日期邏輯時出錯")
        
    def judge_previous(self, 
                       df: pd.DataFrame, 
                       previous_wp: pd.DataFrame, 
                       m: int, 
                       previous_wp_pr: pd.DataFrame) -> pd.DataFrame:
        df = super().judge_previous(df, previous_wp, m)

        """處理PR前期底稿
        
        Args:
            df: PO DataFrame
            previous_wp_pr: PR前期底稿DataFrame
            m: 月份
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 重命名前期底稿中的列
            previous_wp_pr = previous_wp_pr.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                }
            )

            # 獲取前期FN備註
            map_dict = self.get_mapping_dict(previous_wp_pr, 'PR Line', 'Remarked by FN_l')
            df['Remarked by 上月 FN PR'] = df['PR Line'].map(map_dict)
            
            self.logger.info("成功處理PR前期底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理PR前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PR前期底稿時出錯")
        
    def judge_procurement(self, 
                          df: pd.DataFrame, 
                          procurement: pd.DataFrame, 
                          procurement_pr: pd.DataFrame) -> pd.DataFrame:
        df = super().judge_procurement(df, procurement)
        # 移除SPT模組給的狀態
        df.loc[df['PO狀態'] == 'Not In Procurement WP', 'PO狀態'] = pd.NA

        try:
            # 重命名PR採購底稿中的列
            procurement_pr = procurement_pr.rename(
                columns={
                    'Remarked by Procurement': 'Remark by PR Team',
                    'Noted by Procurement': 'Noted by PR'
                }
            )
            
            # 獲取PR採購底稿中的備註
            map_dict = self.get_mapping_dict(procurement_pr, 'PR Line', 'Remark by PR Team')
            df['Remarked by Procurement PR'] = df['PR Line'].map(map_dict)
            
            map_dict = self.get_mapping_dict(procurement_pr, 'PR Line', 'Noted by PR')
            df['Noted by Procurement PR'] = df['PR Line'].map(map_dict)
            
            self.logger.info("成功處理PR採購底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理PR採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PR採購底稿時出錯")

    # def get_closing_note(self) -> pd.DataFrame:
    #     config = {'certificate_path': self.config.get('CREDENTIALS', 'certificate_path'),
    #               'scopes': self.config.get_list('CREDENTIALS', 'scopes')}
    #     return self.importer.import_spx_closing_list(config)

    def get_closing_note(self) -> pd.DataFrame:
        """獲取關單數據 - 優化版本支持並發處理
        
        Returns:
            pd.DataFrame: 關單數據框
        """
        try:
            config = {'certificate_path': self.config.get('CREDENTIALS', 'certificate_path'),
                      'scopes': self.config.get_list('CREDENTIALS', 'scopes')}
            
            # 創建並發處理器
            async_sheets = AsyncGoogleSheetsBase(config)
            
            # 準備查詢
            queries = [
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2023年_done', 'A:J', True),
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2024年', 'A:J', True),
                ('1wuwyyNtU6dhK7JF2AFJfUJC0ChNScrDza6UQtfE7sCE', '2025年', 'A:J', True)
            ]
            
            # 並發執行查詢
            dfs = async_sheets.concurrent_get_data(queries)
            
            # 合併結果
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # 處理數據
            combined_df.dropna(subset=['Date'], inplace=True)
            combined_df.rename(columns={'Date': 'date', 'Type': 'type', 'PO Number': 'po_no', 
                                        'Requester': 'requester', 'Supplier': 'supplier',
                                        'Line Number / ALL': 'line_no', 'Reason': 'reason', 
                                        'New PR Number': 'new_pr_no', 'Remark': 'remark', 
                                        'Done(V)': 'done_by_fn'}, inplace=True)
            combined_df = combined_df.query("date!=''").reset_index(drop=True)
            
            self.logger.info("成功獲取關單數據(使用並發處理)")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"獲取關單數據時出錯: {str(e)}", exc_info=True)
            raise
    
    def is_closed_spx(self, df: pd.DataFrame) -> pd.Series:
        """
        [0]有新的PR編號，但FN未上系統關單的
        [1]有新的PR編號，但FN已經上系統關單的
        """
        return (((~df['new_pr_no'].isna()) & (df['new_pr_no'] != '')) & (df['done_by_fn'].isna())), \
            (((~df['new_pr_no'].isna()) & (df['new_pr_no'] != '')) & (~df['done_by_fn'].isna()))

    def get_period_from_ap_invoice(self, df, df_ap, yyyymm: int) -> pd.DataFrame:
        """
        Fill in the AP invoice period into the PO data (excluding periods after month m).
        """
        try:
            # Drop rows with missing 'PO Number' and reset index
            df_ap = df_ap.dropna(subset=['PO Number']).reset_index(drop=True)
            
            # Create a combined key for matching
            df_ap['po_line'] = df_ap['Company'] + '-' + df_ap['PO Number'] + '-' + df_ap['PO_LINE_NUMBER']
            
            # Convert 'Period' to datetime then format as integer yyyymm
            df_ap['period'] = (
                pd.to_datetime(df_ap['Period'], format='%b-%y', errors='coerce')
                .dt.strftime('%Y%m')
                .fillna(0)
                .astype('int32')
            )
            
            # Keep only AP invoices for periods up to m and for each po_line keep the latest period
            df_ap = (
                df_ap.loc[df_ap['period'] <= yyyymm, :]
                .sort_values(by=['po_line', 'period'])
                .drop_duplicates(subset='po_line', keep='last')
                .reset_index(drop=True)
            )
            
            # Merge the period info into df based on matching 'PO Line'
            df = df.merge(df_ap[['po_line', 'period']], left_on='PO Line', right_on='po_line', how='left')
            df['GL DATE'] = df['period']
            df.drop(columns=['po_line', 'period'], inplace=True)
            
            # For each PO#, fill missing GL DATE with the maximum GL DATE available in that group
            # df['GL DATE'] = df['GL DATE'].fillna(df.groupby('PO#')['GL DATE'].transform('max'))
            """
            # 檢查有無PO#含有兩個以上GL DATE
            fillna_dict = df.groupby('PO#')['GL DATE'].max().to_dict()
            df['GL DATE'] = df.apply(lambda x: 
            fillna_dict[x['PO#']] if np.isnan(x['GL DATE']) and x['PO#'] in fillna_dict else x['GL DATE'], axis=1)
            """
            self.logger.info("成功添加GL DATE")
            return df
        except Exception as e:
            self.logger.error(f"添加GL DATE時出錯: {str(e)}", exc_info=True)
            raise ValueError("添加GL DATE時出錯")
    
    def convert_date_format_in_remark(self, series) -> pd.Series:
        return series.str.replace(r'(\d{4})/(\d{2})', r'\1\2', regex=True)
    
    def extact_fa_remark(self, series) -> pd.Series:
        return series.str.extract(r'(\d{6}入FA)', expand=False)
        
    def give_status_stage_1(self, df: pd.DataFrame, df_spx_closing: pd.DataFrame) -> pd.DataFrame:
        try:
            # 依據已關單條件取得對應的 PO#
            c1, c2 = self.is_closed_spx(df_spx_closing)
            to_be_close = df_spx_closing.loc[c1, 'po_no'].unique()
            closed = df_spx_closing.loc[c2, 'po_no'].unique()

            # 處理特殊品項與供應商條件
            bao = ['Cost of Logistics and Warehouse - Water', 'Cost of Logistics and Warehouse - Electricity']
            
            # 定義「上月FN」備註關單條件
            remarked_close_by_fn_last_month = (
                df['Remarked by 上月 FN'].str.contains('刪|關', na=False) | 
                df['Remarked by 上月 FN PR'].str.contains('刪|關', na=False)
            )
            
            # 統一轉換日期格式
            df['Remarked by 上月 FN'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN'])
            df['Remarked by 上月 FN PR'] = self.convert_date_format_in_remark(df['Remarked by 上月 FN PR'])
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金，且不是 FA 相關科目
            cond1 = df['Item Description'].str.contains('(?i)押金|保證金|Deposit|找零金', na=False)
            is_fa = df['GL#'] == self.config.get('FA_ACCOUNTS', 'spx')
            cond2 = df['Item Description'].str.contains('(?i)繳費機訂金', na=False)  # 繳費機訂金屬FA，避免前端選錯加強過濾
            df.loc[cond1 & ~is_fa & ~cond2, 'PO狀態'] = '摘要內有押金/保證金/Deposit/找零金'
            
            # 條件2：供應商與類別對應，做 GL 調整
            cond2 = (df['PO Supplier'] == 'TW_寶倉物流股份有限公司') & (df['Category'].isin(bao))
            df.loc[cond2, 'PO狀態'] = 'GL調整'
            
            # 條件3：該 PO# 在待關單清單中
            cond3 = df['PO#'].isin(to_be_close)
            df.loc[cond3, 'PO狀態'] = '待關單'
            
            # 條件4：該 PO# 在已關單清單中
            cond4 = df['PO#'].isin(closed)
            df.loc[cond4, 'PO狀態'] = '已關單'
            
            # 條件5：上月 FN 備註含有「刪」或「關」
            cond5 = remarked_close_by_fn_last_month
            df.loc[cond5, 'PO狀態'] = '參照上月關單'
            
            # 條件6：若「Remarked by 上月 FN」含有「入FA」，則提取該數字，並更新狀態
            cond6 = ((df['Remarked by 上月 FN'].str.contains('入FA', na=False)) & 
                     (~df['Remarked by 上月 FN'].str.contains('部分完成', na=False)))
            if cond6.any():
                extracted_fn = self.extact_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, 'PO狀態'] = extracted_fn
            
            # 條件7：若「Remarked by 上月 FN PR」含有「入FA」，則提取該數字，並更新狀態
            cond7 = ((df['Remarked by 上月 FN PR'].str.contains('入FA', na=False)) & 
                     (~df['Remarked by 上月 FN PR'].str.contains('部分完成', na=False)))
            if cond7.any():
                extracted_pr = self.extact_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                df.loc[cond7, 'PO狀態'] = extracted_pr
            
            self.logger.info("成功給予第一階段狀態")
            return df
        
        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")

    def erm(self, df: pd.DataFrame, ym: int, ref_a: pd.DataFrame, ref_b: pd.DataFrame) -> pd.DataFrame:
        """處理ERM邏輯
        
        Args:
            df: PO DataFrame
            ym: 年月
            ref_a: 科目參考數據
            ref_b: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 設置檔案日期
            df['檔案日期'] = ym
            
            # 定義ERM狀態條件
            is_fa = df['GL#'] == self.config.get('FA_ACCOUNTS', 'spx')
            # 條件1：已入帳
            df.loc[df['Remarked by 上月 FN'].str.contains('(?i)已入帳', na=False), 'PO狀態'] = '已入帳'
            condition_booked = (
                (~df['GL DATE'].isna()) & 
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both')) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Billed Quantity'] != '0') &
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成|已入帳', na=False))) &
                (~is_fa)
            )

            # 條件2：已完成
            condition_completed = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                (~df['Remarked by 上月 FN PR'].str.contains('(?i)未完成', na=False)) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件3：全付完，未關單
            condition_paid_not_closed = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) == 0)
            )
            
            # 條件4：已完成但有未付款部分
            condition_completed_with_unpaid = (
                ((df['Remarked by Procurement'].str.contains('(?i)已完成|rent', na=False)) | 
                 (df['Remarked by 上月 FN'].str.contains('(?i)已完成', na=False))) &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] == df['Received Quantity']) &
                (df['Entry Billed Amount'].astype(float) != 0) &
                (df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float) != 0)
            )
            
            # 條件5：需檢查收貨
            condition_check_receipt = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] <= df['檔案日期']) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 條件6：未完成
            condition_incomplete = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) &
                (df['Expected Received Month_轉換格式'] > df['檔案日期'])
            )
            
            # 條件7：範圍錯誤_租金
            condition_range_error_lease = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') & 
                (df['Item Description'].str.contains('(?i)租金', na=False))
            )

            # 條件8：範圍錯誤_薪資
            condition_range_error_salary = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') & 
                (df['Item Description'].str.contains('(?i)派遣|Salary|Agency Fee', na=False))
            )

            # 條件9：範圍錯誤
            condition_range_error = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002')
            )
            
            # 條件10：部分完成ERM
            condition_partially_completed_erm = (
                (df['Remarked by Procurement'] != 'error') &
                ((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) &
                (df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                ) == False) &
                (df['YMs of Item Description'] != '100001,100002') &
                (df['Received Quantity'].astype(float) != 0) &
                (df['Entry Quantity'] != df['Received Quantity'])
            )
            
            # 組合所有條件
            conditions = [
                condition_booked,
                condition_completed,
                condition_paid_not_closed,
                condition_completed_with_unpaid,
                condition_check_receipt,
                condition_incomplete,
                condition_range_error_lease,
                condition_range_error_salary,
                condition_range_error,
                condition_partially_completed_erm

            ]
            
            # 對應的結果
            results = [
                '已入帳',
                '已完成',
                '全付完，未關單?',
                '已完成',
                'Check收貨',
                '未完成',
                'error(Description Period is out of ERM)_租金',
                'error(Description Period is out of ERM)_薪資',
                'error(Description Period is out of ERM)',
                '部分完成ERM',
            ]
            
            # 應用條件
            df['PO狀態'] = np.select(conditions, results, default=df['PO狀態'])
            
            # 處理格式錯誤
            # TODO; 上面的條件目前尚可，次期由此更新
            mask_format_error = (((df['PO狀態'].isna()) | (df['PO狀態'] == 'nan')) & 
                                 (df['YMs of Item Description'] == '100001,100002'))
            df.loc[mask_format_error, 'PO狀態'] = '格式錯誤，退單'
            
            # 根據PO狀態設置估計入帳
            # 條件: 已完成 ->入帳, 其餘N
            mask_completed = (df['PO狀態'] == '已完成')
            df.loc[mask_completed, '是否估計入帳'] = 'Y'
            df.loc[~mask_completed, '是否估計入帳'] = 'N'
            
            need_to_accrual = df['是否估計入帳'] == 'Y'
            # 設置Account code
            df.loc[need_to_accrual, 'Account code'] = df.loc[need_to_accrual, 'GL#']
            
            # 設置Account Name
            df['Account Name'] = pd.merge(
                df, ref_a, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Account Desc']
            
            # 設置Product code
            df.loc[need_to_accrual, 'Product code'] = "LG_SPX_OWN"
            
            # 設置Region_c
            df.loc[need_to_accrual, 'Region_c'] = "TW"
            
            # 設置Dep.
            necessary_columns_for_dept = ['650005', '610104', '630001', '650003', '600301', 
                                          '610110', '610105', '600310', '620003', '610311']
            isin_dept_account = df['Account code'].isin(necessary_columns_for_dept)
            df.loc[need_to_accrual & isin_dept_account, 'Dep.'] = \
                df.loc[need_to_accrual & isin_dept_account, 'Department'].str[:3]
            df.loc[need_to_accrual & ~isin_dept_account, 'Dep.'] = '000'
            
            # 設置Currency_c
            df.loc[need_to_accrual, 'Currency_c'] = df.loc[need_to_accrual, 'Currency']
            
            # 設置Accr. Amount
            df['temp_amount'] = (df['Unit Price'].astype(float) * 
                                 (df['Entry Quantity'].astype(float) - df['Billed Quantity'].astype(float))
                                 )
            df.loc[need_to_accrual, 'Accr. Amount'] = df.loc[need_to_accrual, 'temp_amount']
            df.drop('temp_amount', axis=1, inplace=True)
            
            # 設置是否有預付
            is_prepayment = df['Entry Prepay Amount'] != '0'
            df.loc[need_to_accrual & is_prepayment, '是否有預付'] = 'Y'

            # 設置Liability
            df['Liability'] = pd.merge(
                df, ref_b, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Liability_y']
            df.loc[need_to_accrual & is_prepayment, 'Liability'] = '111112'
            
            # 設置PR Product Code Check
            if 'Product code' in df.columns and 'Project' in df.columns:
                mask_product_code = df['Product code'].notnull()
                try:
                    product_match = df.loc[mask_product_code, 'Project'].str.findall(r'^(\w+(?:))').apply(
                        lambda x: x[0] if len(x) > 0 else ''
                    ) == df.loc[mask_product_code, 'Product code']
                    
                    df.loc[mask_product_code, 'PR Product Code Check'] = np.where(
                        product_match, 'good', 'bad'
                    )
                except Exception as e:
                    self.logger.error(f"設置PR Product Code Check時出錯: {str(e)}", exc_info=True)
                    raise ValueError("設置PR Product Code Check時出錯")
            
            self.logger.info("成功處理ERM邏輯")
            return df
            
        except Exception as e:
            self.logger.error(f"處理ERM邏輯時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理ERM邏輯時出錯")
    
    def reformate(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化最終數據
        
        Args:
            df: PO DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
        """
        try:
            # 處理數值格式
            int_cols = ['Line#', 'GL#']
            float_cols = [
                'Unit Price', 'Entry Amount', 'Entry Invoiced Amount', 
                'Entry Billed Amount', 'Entry Prepay Amount', 
                'PO Entry full invoiced status', 'Accr. Amount'
            ]
            
            # 格式化數值列
            df = self._format_numeric_columns(df, int_cols, float_cols)
            
            # 格式化日期
            df = self._reformat_dates(df)
            
            # 移除臨時計算列
            if '檔案日期' in df.columns:
                df.drop(columns=['檔案日期'], axis='columns', inplace=True)
                
            if 'Expected Received Month_轉換格式' in df.columns:
                df.drop(columns=['Expected Received Month_轉換格式'], axis='columns', inplace=True)
                
            if 'YMs of Item Description' in df.columns:
                df.drop(columns=['YMs of Item Description'], axis='columns', inplace=True)
            
            # 清理nan值
            columns_to_clean = [
                '是否估計入帳', 'PR Product Code Check', 'PO狀態',
                'Accr. Amount', '是否為FA', 'Region_c', 'Dep.'
            ]
            df = self._clean_nan_values(df, columns_to_clean)
            
            # 重新排列上月備註欄位位置
            num = df.columns.get_loc('Remarked by FN') + 1
            last_col = df.pop('Remarked by 上月 FN')
            df.insert(num, last_col.name, last_col)  # Move Remarked by 上月 FN

            num += 1
            last_col = df.pop('Remarked by 上月 FN PR')
            df.insert(num, last_col.name, last_col)  # Move Remarked by 上月 FN
            
            # 重新排列PO狀態欄位位置; 放在"是否估計入帳"之前
            num = df.columns.get_loc('是否估計入帳')
            last_col = df.pop(df.columns[df.columns.get_loc('PO狀態')])
            df.insert(num, last_col.name, last_col)  # Move PO狀態

            # 重新排列PR欄位位置
            num = df.columns.get_loc('Noted by Procurement') + 1
            last_col = df.pop(df.columns[df.columns.get_loc('Remarked by Procurement PR')])
            df.insert(num, last_col.name, last_col)

            num += 1
            last_col = df.pop(df.columns[df.columns.get_loc('Noted by Procurement PR')])
            df.insert(num, last_col.name, last_col)
            
            self.logger.info("成功格式化數據")
            return df
            
        except Exception as e:
            self.logger.error(f"格式化數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("格式化數據時出錯")
        
    def process(self, fileUrl: str, file_name: str, 
                fileUrl_previwp: str = None, fileUrl_p: str = None, 
                fileUrl_ap: str = None, fileUrl_previwp_pr: str = None,
                fileUrl_p_pr: str = None) -> None:
        """處理PO數據的主流程（使用並發導入）
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            fileUrl_ap: AP invoice文件路徑
            fileUrl_previwp_pr: 前期PR底稿文件路徑
            fileUrl_p_pr: 採購PR底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理SPX PO數據: {file_name}")
            
            # 準備文件信息字典 - SPX特有的文件結構
            file_info = {
                'raw_po': fileUrl,
                'previous': fileUrl_previwp if fileUrl_previwp else None,
                'procurement_po': fileUrl_p if fileUrl_p else None,
                'ap_invoice': fileUrl_ap if fileUrl_ap else None,
                'previous_pr': fileUrl_previwp_pr if fileUrl_previwp_pr else None,
                'procurement_pr': fileUrl_p_pr if fileUrl_p_pr else None
            }
            
            # 創建並發導入器
            async_importer = AsyncDataImporter()
            
            # 準備並發導入任務
            file_types = []
            file_paths = []
            file_names = {}
            
            for file_type, file_path in file_info.items():
                if file_path:
                    file_types.append(file_type)
                    file_paths.append(file_path)
                    file_names[file_type] = os.path.basename(file_path)
            
            # 並發導入所有文件
            import_results = async_importer.concurrent_read_files(
                file_types, 
                file_paths, 
                file_names=file_names,
                config={'certificate_path': self.config.get('CREDENTIALS', 'certificate_path'),
                        'scopes': self.config.get_list('CREDENTIALS', 'scopes')},
                ap_columns=self.config.get_list('SPX', 'ap_columns')
            )
            
            # 檢測並處理原始PO數據
            if 'raw_po' in import_results:
                raw_po_result = import_results['raw_po']
                if isinstance(raw_po_result, tuple) and len(raw_po_result) == 3:
                    df, date, m = raw_po_result
                else:
                    self.logger.error("原始PO數據格式不正確")
                    raise ValueError("原始PO數據格式不正確")
            else:
                self.logger.error("無法導入原始PO數據")
                raise ValueError("無法導入原始PO數據")
            
            # 導入參考數據 - 用SPT的ref檔案參照會計科目和負債科目
            ref_key = 'SPT'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)

            # 過濾SPX產品代碼
            df = self.fillter_spx_product_code(df)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理AP invoice - SPX特有邏輯
            if 'ap_invoice' in import_results:
                ap_invoice_result = import_results['ap_invoice']
                if self._is_valid_data(ap_invoice_result):
                    df_ap = ap_invoice_result
                    df = self.get_period_from_ap_invoice(df, df_ap, date)
                    self.logger.info("成功處理AP發票數據")
                else:
                    self.logger.warning("AP發票數據為空或無效")
            
            # 處理前期底稿(PO和PR)
            previous_wp = None
            previous_wp_pr = None
            
            if 'previous' in import_results:
                previous_result = import_results['previous']
                if self._is_valid_data(previous_result):
                    previous_wp = previous_result
            
            if 'previous_pr' in import_results:
                previous_pr_result = import_results['previous_pr']
                if self._is_valid_data(previous_pr_result):
                    previous_wp_pr = previous_pr_result
            
            if previous_wp is not None and previous_wp_pr is not None:
                df = self.judge_previous(df, previous_wp, m, previous_wp_pr)
                self.logger.info("成功處理前期底稿(PO和PR)")
            else:
                self.logger.warning("前期底稿(PO或PR)為空或無效")
            
            # 處理採購底稿(PO和PR)
            procurement = None
            procurement_pr = None
            
            if 'procurement_po' in import_results:
                procurement_result = import_results['procurement_po']
                if self._is_valid_data(procurement_result):
                    procurement = procurement_result
            
            if 'procurement_pr' in import_results:
                procurement_pr_result = import_results['procurement_pr']
                if self._is_valid_data(procurement_pr_result):
                    procurement_pr = procurement_pr_result
            
            if procurement is not None and procurement_pr is not None:
                df = self.judge_procurement(df, procurement, procurement_pr)
                self.logger.info("成功處理採購底稿(PO和PR)")
            else:
                self.logger.warning("採購底稿(PO或PR)為空或無效")
            
            # 處理日期邏輯
            df = self.get_logic_date(df)
            
            # 獲取關單數據
            df_spx_closing = self.get_closing_note()
            df = self.give_status_stage_1(df, df_spx_closing)
            df = self.erm(df, date, ref_ac, ref_liability)

            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PO')
            
            self.logger.info(f"成功完成SPX PO數據處理: {file_name}")
            
        except Exception as e:
            self.logger.error(f"處理SPX PO數據時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"處理SPX PO數據時出錯: {str(e)}")

    def _is_valid_data(self, data) -> bool:
        """檢查數據是否有效
        
        Args:
            data: 要檢查的數據
            
        Returns:
            bool: 數據是否有效
        """
        try:
            if data is None:
                return False
            
            # 檢查 DataFrame
            if isinstance(data, pd.DataFrame):
                return not data.empty
            
            # 檢查 Series
            if isinstance(data, pd.Series):
                return not data.empty
            
            # 檢查列表或元組
            if isinstance(data, (list, tuple)):
                return len(data) > 0
            
            # 檢查字典
            if isinstance(data, dict):
                return len(data) > 0
            
            # 其他情況，假設非None即有效
            return True
            
        except Exception as e:
            self.logger.error(f"檢查數據有效性時出錯: {str(e)}", exc_info=True)
            return False
        
    def concurrent_spx_process(self, file_paths: Dict[str, str]) -> None:
        """SPX PO 並發處理主流程
        
        Args:
            file_paths: 文件路徑字典，包含所有需要處理的文件路徑
                {
                    'po_file': PO原始數據文件路徑,
                    'po_file_name': PO原始數據文件名,
                    'previous_wp': 前期底稿文件路徑,
                    'procurement': 採購底稿文件路徑,
                    'ap_invoice': AP發票文件路徑,
                    'previous_wp_pr': 前期PR底稿文件路徑,
                    'procurement_pr': 採購PR底稿文件路徑
                }
        
        Returns:
            None
        """
        try:
            self.logger.info(f"開始並發處理SPX PO數據: {file_paths.get('po_file_name', 'Unknown')}")
            
            # 調用process方法處理
            self.process(
                fileUrl=file_paths.get('po_file', ''),
                file_name=file_paths.get('po_file_name', ''),
                fileUrl_previwp=file_paths.get('previous_wp', None),
                fileUrl_p=file_paths.get('procurement', None),
                fileUrl_ap=file_paths.get('ap_invoice', None),
                fileUrl_previwp_pr=file_paths.get('previous_wp_pr', None),
                fileUrl_p_pr=file_paths.get('procurement_pr', None)
            )
            
            self.logger.info("成功完成SPX PO並發處理")
            
        except Exception as e:
            self.logger.error(f"SPX PO並發處理時出錯: {str(e)}", exc_info=True)
            raise ValueError(f"SPX PO並發處理時出錯: {str(e)}")