import re
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime

from base_processor import BaseDataProcessor
from utils import Utils, Logger


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
            Tuple[pd.DataFrame, int]: 添加了必要列的數據框和更新的月份
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
            df: PO數據框
            
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
            df: PO數據框
            
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
            df: PO數據框
            mapping_list: 關單清單
            
        Returns:
            pd.DataFrame: 處理後的數據框
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
            df: PO數據框
            previous_wp: 前期底稿數據框
            m: 月份
            
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
            df['前期FN備註'] = pd.merge(
                df, previous_wp, how='left', on='PO Line'
            ).loc[:, ['Remarked by FN_l']]
            
            df['Remarked by 上月 FN'] = df['前期FN備註']
            
            # 獲取前期採購備註
            previous_wp_fv = pd.merge(
                df, previous_wp, how='inner', on='PO Line'
            ).loc[:, ['PO Line', 'Remark by PR Team_l']]
            
            df[f'Remarked by {m}月 Procurement'] = pd.merge(
                df, previous_wp_fv, on='PO Line', how='left'
            ).loc[:, 'Remark by PR Team_l']
            
            df.drop('前期FN備註', axis=1, inplace=True)
            
            self.logger.info("成功處理前期底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理前期底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理前期底稿時出錯")
    
    def judge_procurement(self, df: pd.DataFrame, df_procu: pd.DataFrame) -> pd.DataFrame:
        """處理採購底稿
        
        Args:
            df: PO數據框
            df_procu: 採購底稿數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
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
            df_procu_fv = pd.merge(
                df, df_procu, how='inner', on='PO Line'
            ).loc[:, ['PO Line', 'Remark by PR Team', 'Noted by PR']]
            
            df['Remarked by Procurement'] = pd.merge(
                df, df_procu_fv, how='left', on='PO Line'
            ).loc[:, 'Remark by PR Team']
            
            df['Noted by Procurement'] = pd.merge(
                df, df_procu_fv, how='left', on='PO Line'
            ).loc[:, 'Noted by PR']
            
            # 將PO Line轉成PR Line去查找
            df_procu['PR Line'] = df_procu['PO Line'].str.replace('PO', 'PR')
            
            # 使用PR Line查找
            df_procu_fv = pd.merge(
                df, df_procu, how='left', on='PR Line'
            ).loc[:, ['PO Line_x', 'Remark by PR Team']]
            
            df_m = df.loc[:, ['PO Line', 'Remarked by Procurement']]
            df_m = df_m.combine_first(df_procu_fv)
            
            df['Remarked by Procurement'] = df_m['Remarked by Procurement']
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
            df: PO數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
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
            df: PO數據框
            ym: 年月
            ref_a: 科目參考數據
            ref_b: 負債參考數據
            
        Returns:
            pd.DataFrame: 處理後的數據框
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
            df: PO數據框
            
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
            df: PO數據框
            
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
            df: PO數據框
            
        Returns:
            pd.DataFrame: 格式化後的數據框
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
        """處理PO數據的主流程
        
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
            
            # 導入原始數據
            df, date, m = self.importer.import_rawdata_POonly(fileUrl, file_name)
            
            # 導入參考數據
            ref_key = 'SPT' if self.entity_type == 'SPT' else 'MOB'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理關單清單
            if fileUrl_c:
                mapping_list = self.importer.import_closing_list_PO(fileUrl_c)
                df = self.judge_closing(df, mapping_list)
            
            # 處理前期底稿
            if fileUrl_previwp:
                previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                df = self.judge_previous(df, previous_wp, m)
            
            # 處理採購底稿
            if fileUrl_p:
                procurement = self.importer.import_procurement_PO(fileUrl_p)
                df = self.judge_procurement(df, procurement)
            
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
            raise ValueError("處理PO數據時出錯")

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

    # def _determine_fa_status(self, df: pd.DataFrame) -> pd.Series:
    #     base_status = super()._determine_fa_status(df) # 應該可以直接使用SPT的，只要在config上新增entity內容即可。
    #     # 根據子類需求進一步處理 base_status
    #     # 例如：將某些情況強制設為 'N' 或其他值
    #     # base_status = np.where(某條件, 'N', base_status)
    #     return base_status

    def get_logic_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理日期邏輯
        
        Args:
            df: PO數據框
            
        Returns:
            pd.DataFrame: 處理後的數據框
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
            df: PO數據框
            previous_wp_pr: PR前期底稿數據框
            m: 月份
            
        Returns:
            pd.DataFrame: 處理後的數據框
        """
        try:
            # 重命名前期底稿中的列
            previous_wp_pr = previous_wp_pr.rename(
                columns={
                    'Remarked by FN': 'Remarked by FN_l',
                }
            )
            
            # 獲取前期FN備註
            df['前期FN備註'] = pd.merge(
                df, previous_wp_pr, how='left', on='PR Line'
            ).loc[:, ['Remarked by FN_l']]
            
            df['Remarked by 上月 FN PR'] = df['前期FN備註']
            
            df.drop('前期FN備註', axis=1, inplace=True)
            
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
            df_procu_fv = pd.merge(
                df, procurement_pr, how='inner', on='PR Line'
            ).loc[:, ['PR Line', 'Remark by PR Team', 'Noted by PR']]
            
            df['Remarked by Procurement PR'] = pd.merge(
                df, df_procu_fv, how='left', on='PR Line'
            ).loc[:, 'Remark by PR Team']
            
            df['Noted by Procurement PR'] = pd.merge(
                df, df_procu_fv, how='left', on='PR Line'
            ).loc[:, 'Noted by PR']
            
            self.logger.info("成功處理PR採購底稿")
            return df
            
        except Exception as e:
            self.logger.error(f"處理PR採購底稿時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PR採購底稿時出錯")

    def get_closing_note(self) -> pd.DataFrame:
        config = {'certificate_path': self.config.get('CREDENTIALS', 'certificate_path'),
                  'scopes': self.config.get_list('CREDENTIALS', 'scopes')}
        return self.importer.import_spx_closing_list(config)
    
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
            df_ap['po_line'] = df_ap['Company'] + '-' + df_ap['PO Number'] + '-' + df_ap['Line']
            
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
            df['GL DATE'] = df['GL DATE'].fillna(df.groupby('PO#')['GL DATE'].transform('max'))
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
            
            # 條件1：摘要中有押金/保證金/Deposit/找零金
            cond1 = df['Item Description'].str.contains('(?i)押金|保證金|Deposit|找零金', na=False)
            df.loc[cond1, 'PO狀態'] = '摘要內有押金/保證金/Deposit/找零金'
            
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
            cond6 = df['Remarked by 上月 FN'].str.contains('入FA', na=False)
            if cond6.any():
                extracted_fn = self.extact_fa_remark(df.loc[cond6, 'Remarked by 上月 FN'])
                df.loc[cond6, 'PO狀態'] = extracted_fn
            
            # 條件7：若「Remarked by 上月 FN PR」含有「入FA」，則提取該數字，並更新狀態
            cond7 = df['Remarked by 上月 FN PR'].str.contains('入FA', na=False)
            if cond7.any():
                extracted_pr = self.extact_fa_remark(df.loc[cond7, 'Remarked by 上月 FN PR'])
                df.loc[cond7, 'PO狀態'] = extracted_pr
            
            self.logger.info("成功給予第一階段狀態")
            return df
        
        except Exception as e:
            self.logger.error(f"給予第一階段狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("給予第一階段狀態時出錯")

    def process(self, 
                fileUrl: str, file_name: str, 
                fileUrl_previwp: str = None, 
                fileUrl_p: str = None, 
                fileUrl_ap: str = None,
                fileUrl_previwp_pr: str = None,
                fileUrl_p_pr: str = None) -> None:
        """處理PO數據的主流程
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            fileUrl_ap: AP文件路徑
            fileUrl_previwp_pr: PR前期底稿文件路徑
            fileUrl_p_pr: PR採購底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"開始處理PO數據: {file_name}")
            
            # 導入原始數據
            df, date, m = self.importer.import_rawdata_POonly(fileUrl, file_name)
            
            # 導入參考數據; 用SPT的ref檔案參照會計科目和負債科目
            ref_key = 'SPT'
            ref_ac, ref_liability = self.importer.import_reference_data(ref_key)

            df = self.fillter_spx_product_code(df)
            
            # 添加必要列
            df, m = self.add_cols(df, m)
            
            # 處理AP invoice
            if fileUrl_ap:
                df_ap = self.importer.import_ap_invoice(fileUrl_ap, self.config.get_list('SPX', 'ap_columns'))
                df = self.get_period_from_ap_invoice(df, df_ap, date)
            
            # 處理前期底稿
            if fileUrl_previwp:
                previous_wp = self.importer.import_previous_wp(fileUrl_previwp)
                previous_wp_pr = self.importer.import_previous_wp(fileUrl_previwp_pr)
                df = self.judge_previous(df, previous_wp, m, previous_wp_pr)
            
            # 處理採購底稿
            if fileUrl_p:
                procurement = self.importer.import_procurement_PO(fileUrl_p)
                procurement_pr = self.importer.import_procurement(fileUrl_p_pr)
                df = self.judge_procurement(df, procurement, procurement_pr)
            
            # 處理日期邏輯
            df = self.get_logic_date(df)
            
            # 處理
            # TODO
            """
            1.get AP invoice
            2.get GL date from AP invoice
            3.give status, 前期入FA, 摘要內有押金\保證金\Deposit\找零金
            4.give status, GL調整
            5.give status, 關單
            """
            df_spx_closing = self.get_closing_note()
            self.give_status_stage_1(df, df_spx_closing)

            # 格式化數據
            df = self.reformate(df)
            
            # 導出文件
            self.export_file(df, date, 'PO')
            
            self.logger.info(f"成功完成PO數據處理: {file_name}")

        except Exception as e:
            self.logger.error(f"處理PO數據時出錯: {str(e)}", exc_info=True)
            raise ValueError("處理PO數據時出錯")