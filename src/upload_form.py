import os
import sys
import logging
import configparser
import traceback
import warnings
from typing import List, Tuple, Iterable, Union, Dict, Any, Optional

import pandas as pd
from pandas import DataFrame, Series
import numpy as np

from utils import Logger, ConfigManager


# 忽略警告
warnings.filterwarnings(action='ignore')


class UploadFormProcessor:
    """上傳表單處理基類，提供共同的處理方法"""
    
    def __init__(self, currency: str):
        """
        初始化上傳表單處理器
        
        Args:
            currency: 貨幣類型
        """
        self.currency = currency
        self.logger = Logger().get_logger(__name__)
        self.config = ConfigManager()
        
        # 從配置中加載設置
        self.pr_pivot_index = self.config.get('MOB', 'pr_pivot_index')
        self.po_pivot_index = self.config.get('MOB', 'po_pivot_index')
        self.sm_cr_pivot_cols = self.config.get('MOB', 'sm_cr_pivot_cols')
        self.ga_cr_pivot_cols = self.config.get('MOB', 'ga_cr_pivot_cols')
        self.pivot_value_col = self.config.get('MOB', 'pivot_value_col')
    
    def update_configs(self, entity: str = 'MOB'):
        """
        更新配置設置
        
        Args:
            entity: 實體類型，'MOB'或'SPT'
        """
        try:
            self.pr_pivot_index = self.config.get(entity, 'pr_pivot_index')
            self.po_pivot_index = self.config.get(entity, 'po_pivot_index')
            self.sm_cr_pivot_cols = self.config.get(entity, 'sm_cr_pivot_cols')
            self.ga_cr_pivot_cols = self.config.get(entity, 'ga_cr_pivot_cols')
            self.pivot_value_col = self.config.get(entity, 'pivot_value_col')
            
            self.logger.info(f"已更新 {entity} 配置設置")
        except Exception as e:
            self.logger.error(f"更新配置設置時出錯: {str(e)}", exc_info=True)
    
    def get_df(self, path: str, sheet: str) -> DataFrame:
        """
        獲取數據框
        
        Args:
            path: 文件路徑
            sheet: 工作表名
            
        Returns:
            DataFrame: 數據框
        """
        try:
            df = pd.read_excel(path, sheet_name=sheet, dtype=str)
            
            # 統一供應商欄位名稱
            if 'PR Supplier' in df.columns:
                df_reformated = df.rename(columns={'PR Supplier': 'Supplier'})
            else:
                df_reformated = df.rename(columns={'PO Supplier': 'Supplier'})
                
            return df_reformated
            
        except Exception as e:
            self.logger.error(f"獲取數據框時出錯: {str(e)}", exc_info=True)
            raise
    
    def reformate_dtypes(self, df: DataFrame, int_cols: List[str]) -> DataFrame:
        """
        重新格式化數據類型
        
        Args:
            df: 數據框
            int_cols: 整數列名列表
            
        Returns:
            DataFrame: 格式化後的數據框
        """
        try:
            df_c = df.copy()
            for i in int_cols:
                df_c[i] = df_c[i].astype(float)
                
            return df_c
            
        except Exception as e:
            self.logger.error(f"重新格式化數據類型時出錯: {str(e)}", exc_info=True)
            return df
    
    def is_book_equals_to_y(self, df: DataFrame) -> Series:
        """
        檢查是否估計入帳
        
        Args:
            df: 數據框
            
        Returns:
            Series: 布爾序列
        """
        return df.是否估計入帳 == 'Y'
    
    def is_fa_euqals_to_na(self, df: DataFrame, is_pr: bool = True) -> Series:
        """
        檢查是否為FA
        
        Args:
            df: 數據框
            is_pr: 是否為PR
            
        Returns:
            Series: 布爾序列
        """
        try:
            if is_pr:
                return (df.是否為FA_variable.isna()) | (df.是否為FA_variable == 'N')
            else:
                return df.是否為FA_variable.apply(lambda x: x in [np.nan, 'N'])
                
        except Exception as e:
            self.logger.error(f"檢查是否為FA時出錯: {str(e)}", exc_info=True)
            return pd.Series([True] * len(df))
    
    def is_sm_expense(self, df: DataFrame, sm: str = 'Y') -> Series:
        """
        檢查是否為S&M費用
        
        Args:
            df: 數據框
            sm: 'Y'或'N'
            
        Returns:
            Series: 布爾序列
        """
        if sm == 'Y':
            return df['是否為S&M_variable'] == 'Y'
        else:
            return df['是否為S&M_variable'] == 'N'
    
    def is_twd(self, df: DataFrame, currency: str) -> Series:
        """
        檢查是否為指定幣別
        
        Args:
            df: 數據框
            currency: 幣別
            
        Returns:
            Series: 布爾序列
        """
        return df.Currency_c_variable == currency
    
    def make_pivot(self, df: DataFrame, index: Union[List[str], str], 
                   values: Union[List[str], str], columns=None, 
                   aggfunc: str = 'sum') -> DataFrame:
        """
        建立樞紐表
        
        Args:
            df: 數據框
            index: 索引欄位
            values: 值欄位
            columns: 列欄位
            aggfunc: 聚合函數
            
        Returns:
            DataFrame: 樞紐表
        """
        try:
            pivot = df.pivot_table(
                index=index,
                values=values,
                columns=columns,
                aggfunc=aggfunc
            ).rename_axis(None, axis=1).reset_index()
            
            return pivot
            
        except Exception as e:
            self.logger.error(f"建立樞紐表時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def get_item_description_dr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = False) -> Series:
        """
        獲取借方項目描述
        
        Args:
            df: 數據框
            period: 期間，格式為 "2024/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost is False:
                if sm:
                    constant = ' '.join([period, f'Accrual S&M from {is_pr}'])
                    s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + '_' + df['Item Description']
                    return s
                else:
                    constant = ' '.join([period, f'Accrual G&A from {is_pr}'])
                    s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + '_' + df['Item Description']
                    return s
            else:
                constant = ' '.join([period, f'Accrual Cost from {is_pr}'])
                s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + '_' + df['Item Description']
                return s
                
        except Exception as e:
            self.logger.error(f"獲取借方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([''] * len(df))
    
    def get_item_description_cr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = True) -> Series:
        """
        獲取貸方項目描述
        
        Args:
            df: 數據框
            period: 期間，格式為 "2024/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost:
                constant = ' '.join([period, f'Accrual Cost from {is_pr}'])
                s = constant + '_' + df['Account Name_variable']
                return s
            else:
                if sm:
                    constant = ' '.join([period, f'Accrual S&M from {is_pr}'])
                    s = constant + '_' + df['Product Code']
                    return s
                else:
                    constant = ' '.join([period, f'Accrual G&A from {is_pr}'])
                    s = constant + '_' + df['Account Name_variable']
                    return s
                    
        except Exception as e:
            self.logger.error(f"獲取貸方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([''] * len(df))
    
    def extract_cost_items(self, df: DataFrame, extract: bool = True) -> DataFrame:
        """
        提取成本項目
        
        Args:
            df: 數據框
            extract: 是提取還是排除成本項目
            
        Returns:
            DataFrame: 處理後的數據框
        """
        try:
            if extract:
                df_filtered = \
                    df.loc[df['Account code_variable'].fillna('na').str.contains('^(4|5)'), :].reset_index(drop=True)
                return df_filtered
            else:
                df_filtered = \
                    df.loc[~df['Account code_variable'].fillna('na').str.contains('^(4|5)'), :].reset_index(drop=True)
                return df_filtered
                
        except Exception as e:
            self.logger.error(f"提取成本項目時出錯: {str(e)}", exc_info=True)
            return df
    
    def split_config_value(self, config_value: str) -> List[str]:
        """
        分割配置值
        
        Args:
            config_value: 配置字符串
            
        Returns:
            List[str]: 分割後的列表
        """
        return config_value.strip("'").split("', '")
    
    def strip_config_value(self, config_value: str) -> str:
        """
        去除配置值的引號
        
        Args:
            config_value: 配置字符串
            
        Returns:
            str: 處理後的字符串
        """
        return config_value.strip("'")


class MOBUpload(UploadFormProcessor):
    """MOBTW上傳表單處理器"""
    
    def __init__(self, currency: str, df_pr: DataFrame = None, df_po: DataFrame = None):
        """
        初始化MOBTW上傳表單處理器
        
        Args:
            currency: 貨幣類型
            df_pr: PR數據框
            df_po: PO數據框
        """
        super().__init__(currency)
        self.df_pr = df_pr
        self.df_po = df_po
        self.update_configs(entity='MOB')
    
    def get_filtered_df(self, df: DataFrame, is_pr: bool, sm: str, extract: bool) -> DataFrame:
        """
        獲取過濾後的數據框
        
        Args:
            df: 數據框
            is_pr: 是否為PR
            sm: 'Y'或'N'
            extract: 是提取還是排除成本項目
            
        Returns:
            DataFrame: 過濾後的數據框
        """
        try:
            filtered_df = df.loc[
                (self.is_book_equals_to_y(df)) &
                (self.is_fa_euqals_to_na(df, is_pr=is_pr)) &
                (self.is_sm_expense(df, sm=sm)) &
                (self.is_twd(df, currency=self.currency)), :
            ]
            
            return self.extract_cost_items(filtered_df, extract=extract)
            
        except Exception as e:
            self.logger.error(f"獲取過濾後的數據框時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def handle_non_cost(self, df_non_cost: DataFrame, period: str, sm: bool, pr: bool):
        """
        處理非成本項目
        
        Args:
            df_non_cost: 非成本數據框
            period: 期間
            sm: 是否為S&M
            pr: 是否為PR
            
        Returns:
            None
        """
        try:
            if df_non_cost.empty:
                self.logger.warning("沒有符合條件的非成本項目")
                self.flatten_df_dr = pd.DataFrame()
                self.flatten_df_cr = pd.DataFrame()
                return
                
            if sm is True and pr is True:
                # PR S&M 非成本
                flatten_df_dr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.pr_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.sm_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col)
                )
                
                if all([not flatten_df_dr.empty, not flatten_df_cr.empty]):
                    self.logger.info(
                        f"PR S&M NC: {flatten_df_dr[self.strip_config_value(self.pivot_value_col)].sum()}, "
                        f"{flatten_df_cr[self.strip_config_value(self.pivot_value_col)].sum()}"
                    )
                
                self.flatten_df_dr = flatten_df_dr.assign(
                    desc=self.get_item_description_dr(flatten_df_dr, period, sm=sm, is_pr='PR', is_cost=False)
                )
                self.flatten_df_cr = flatten_df_cr.assign(
                    desc=self.get_item_description_cr(flatten_df_cr, period, sm=sm, is_pr='PR', is_cost=False)
                )
                
            elif sm is True and pr is not True:
                # PO S&M 非成本
                flatten_df_dr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.po_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.sm_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col)
                )
                
                if all([not flatten_df_dr.empty, not flatten_df_cr.empty]):
                    self.logger.info(
                        f"PO S&M NC: {flatten_df_dr[self.strip_config_value(self.pivot_value_col)].sum()}, "
                        f"{flatten_df_cr[self.strip_config_value(self.pivot_value_col)].sum()}"
                    )
                
                self.flatten_df_dr = flatten_df_dr.assign(
                    desc=self.get_item_description_dr(flatten_df_dr, period, sm=sm, is_pr='PO', is_cost=False)
                )
                self.flatten_df_cr = flatten_df_cr.assign(
                    desc=self.get_item_description_cr(flatten_df_cr, period, sm=sm, is_pr='PO', is_cost=False)
                )
                
            elif sm is not True and pr is True:
                # PR G&A 非成本
                flatten_df_dr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.pr_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.ga_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col)
                )
                
                if all([not flatten_df_dr.empty, not flatten_df_cr.empty]):
                    self.logger.info(
                        f"PR G&A NC: {flatten_df_dr[self.strip_config_value(self.pivot_value_col)].sum()}, "
                        f"{flatten_df_cr[self.strip_config_value(self.pivot_value_col)].sum()}"
                    )
                
                self.flatten_df_dr = flatten_df_dr.assign(
                    desc=self.get_item_description_dr(flatten_df_dr, period, sm=sm, is_pr='PR', is_cost=False)
                )
                self.flatten_df_cr = flatten_df_cr.assign(
                    desc=self.get_item_description_cr(flatten_df_cr, period, sm=sm, is_pr='PR', is_cost=False)
                )
                
            elif sm is not True and pr is not True:
                # PO G&A 非成本
                flatten_df_dr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.po_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr = self.make_pivot(
                    df_non_cost,
                    self.split_config_value(self.ga_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col)
                )
                
                if all([not flatten_df_dr.empty, not flatten_df_cr.empty]):
                    self.logger.info(
                        f"PO G&A NC: {flatten_df_dr[self.strip_config_value(self.pivot_value_col)].sum()}, "
                        f"{flatten_df_cr[self.strip_config_value(self.pivot_value_col)].sum()}"
                    )
                
                self.flatten_df_dr = flatten_df_dr.assign(
                    desc=self.get_item_description_dr(flatten_df_dr, period, sm=sm, is_pr='PO', is_cost=False)
                )
                self.flatten_df_cr = flatten_df_cr.assign(
                    desc=self.get_item_description_cr(flatten_df_cr, period, sm=sm, is_pr='PO', is_cost=False)
                )
            else:
                self.logger.error("處理非成本項目時出錯：無效的組合")
                raise ValueError("無效的組合")
                
        except Exception as e:
            self.logger.error(f"處理非成本項目時出錯: {str(e)}", exc_info=True)
            self.flatten_df_dr = pd.DataFrame()
            self.flatten_df_cr = pd.DataFrame()
    
    def handle_pr_sm_nc(self, period: str):
        """
        處理PR S&M非成本項目
        
        Args:
            period: 期間，格式為 "2024/01"
            
        Returns:
            None
        """
        try:
            self.logger.info(f"處理PR S&M非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='Y', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=True, pr=True)
        except Exception as e:
            self.logger.error(f"處理PR S&M非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_pr_ga_nc(self, period: str):
        """
        處理PR G&A非成本項目
        
        Args:
            period: 期間，格式為 "2024/01"
            
        Returns:
            None
        """
        try:
            self.logger.info(f"處理PR G&A非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='N', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=False, pr=True)
        except Exception as e:
            self.logger.error(f"處理PR G&A非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_po_sm_nc(self, period: str):
        """
        處理PO S&M非成本項目
        
        Args:
            period: 期間，格式為 "2024/01"
            
        Returns:
            None
        """
        try:
            self.logger.info(f"處理PO S&M非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_po, is_pr=False, sm='Y', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=True, pr=False)
        except Exception as e:
            self.logger.error(f"處理PO S&M非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_po_ga_nc(self, period: str):
        """
        處理PO G&A非成本項目
        
        Args:
            period: 期間，格式為 "2024/01"
            
        Returns:
            None
        """
        try:
            self.logger.info(f"處理PO G&A非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_po, is_pr=False, sm='N', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=False, pr=False)
        except Exception as e:
            self.logger.error(f"處理PO G&A非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_cost(self, period: str):
        """
        處理成本項目
        
        Args:
            period: 期間，格式為 "2024/01"
            
        Returns:
            None
        """
        try:
            self.logger.info(f"處理成本項目: {period}")
            
            # 處理PR G&A成本
            df_pr_ga_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='N', extract=True)
            if not df_pr_ga_cost.empty:
                flatten_df_dr_ga = self.make_pivot(
                    df_pr_ga_cost,
                    self.split_config_value(self.pr_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr_ga = self.make_pivot(
                    df_pr_ga_cost,
                    self.split_config_value(self.ga_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col),
                )
                
                self.flatten_df_pr_dr_cost = flatten_df_dr_ga.assign(
                    desc=self.get_item_description_dr(flatten_df_dr_ga, period, sm=False, is_cost=True)
                )
                self.flatten_df_pr_cr_cost = flatten_df_cr_ga.assign(
                    desc=self.get_item_description_cr(flatten_df_cr_ga, period, sm=False, is_cost=True)
                )
                
                self.logger.info("成功處理PR G&A成本項目")
            else:
                self.logger.info("PR G&A成本項目為空")
                self.flatten_df_pr_dr_cost, self.flatten_df_pr_cr_cost = None, None
            
            # 處理PO G&A成本
            df_po_ga_cost = self.get_filtered_df(self.df_po, is_pr=False, sm='N', extract=True)
            if not df_po_ga_cost.empty:
                flatten_df_dr_ga = self.make_pivot(
                    df_po_ga_cost,
                    self.split_config_value(self.po_pivot_index),
                    self.strip_config_value(self.pivot_value_col),
                )
                flatten_df_cr_ga = self.make_pivot(
                    df_po_ga_cost,
                    self.split_config_value(self.ga_cr_pivot_cols),
                    self.strip_config_value(self.pivot_value_col),
                )
                
                self.flatten_df_po_dr_cost = flatten_df_dr_ga.assign(
                    desc=self.get_item_description_dr(flatten_df_dr_ga, period, sm=False, is_pr='PO', is_cost=True)
                )
                self.flatten_df_po_cr_cost = flatten_df_cr_ga.assign(
                    desc=self.get_item_description_cr(flatten_df_cr_ga, period, sm=False, is_pr='PO', is_cost=True)
                )
                
                self.logger.info("成功處理PO G&A成本項目")
            else:
                self.logger.info("PO G&A成本項目為空")
                self.flatten_df_po_dr_cost, self.flatten_df_po_cr_cost = None, None
                
        except Exception as e:
            self.logger.error(f"處理成本項目時出錯: {str(e)}", exc_info=True)
            self.flatten_df_pr_dr_cost, self.flatten_df_pr_cr_cost = None, None
            self.flatten_df_po_dr_cost, self.flatten_df_po_cr_cost = None, None
    
    def __call__(self, *args: DataFrame, **kwargs):
        """
        設置數據框
        
        Args:
            *args: 位置參數
            **kwargs: 關鍵字參數
            
        Returns:
            None
        """
        try:
            if len(args) == 2:
                self.df_pr = args[0]
                self.df_po = args[1]
            elif len(kwargs) == 1:
                if 'pr' in kwargs:
                    self.df_pr = kwargs['pr']
                elif 'po' in kwargs:
                    self.df_po = kwargs['po']
            else:
                pass
        except Exception as e:
            self.logger.error(f"設置數據框時出錯: {str(e)}", exc_info=True)


class SPTUpload(MOBUpload):
    """SPTTW上傳表單處理器"""
    
    def __init__(self, currency: str, df_pr: DataFrame = None, df_po: DataFrame = None):
        """
        初始化SPTTW上傳表單處理器
        
        Args:
            currency: 貨幣類型
            df_pr: PR數據框
            df_po: PO數據框
        """
        super().__init__(currency)
        self.df_pr = df_pr
        self.df_po = df_po
        self.update_configs(entity='SPT')
    
    def get_item_description_dr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = False) -> Series:
        """
        獲取借方項目描述(SPTTW特有邏輯)
        
        Args:
            df: 數據框
            period: 期間，格式為 "2024/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost is False:
                if sm:
                    constant = ' '.join([period, f'Accrual S&M from {is_pr}'])
                    s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + ' : ' + df['Project']
                    return s
                else:
                    constant = ' '.join([period, f'Accrual G&A from {is_pr}'])
                    s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + ' : ' + df['Project']
                    return s
            else:
                constant = ' '.join([period, f'Accrual Cost from {is_pr}'])
                s = constant + '_' + df[''.join([is_pr, '#'])] + '_' + df['Supplier'] + ' : ' + df['Project']
                return s
                
        except Exception as e:
            self.logger.error(f"獲取借方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([''] * len(df))
    
    def get_item_description_cr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = True) -> Series:
        """
        獲取貸方項目描述(SPTTW特有邏輯)
        
        Args:
            df: 數據框
            period: 期間，格式為 "2024/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost:
                constant = ' '.join([period, f'Accrual Cost from {is_pr}'])
                s = constant + '_' + df['Account Name_variable']
                return s
            else:
                if sm:
                    constant = ' '.join([period, f'Accrual S&M from {is_pr}'])
                    s = constant + '_' + df['Account Name_variable'] + '_' + df['Supplier']
                    return s
                else:
                    constant = ' '.join([period, f'Accrual G&A from {is_pr}'])
                    s = constant + '_' + df['Account Name_variable']
                    return s
                    
        except Exception as e:
            self.logger.error(f"獲取貸方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([''] * len(df))
    
    def extract_cost_items(self, df: DataFrame, extract: bool = True) -> DataFrame:
        """
        提取成本項目(SPTTW特有邏輯)
        
        Args:
            df: 數據框
            extract: 是提取還是排除成本項目
            
        Returns:
            DataFrame: 處理後的數據框
        """
        if extract:
            """SPT沒有Cost entry，返回空DataFrame"""
            return pd.DataFrame()
        else:
            return df


def get_aggregation_twd(path: str, period: str, is_mob: bool = True) -> Tuple:
    """
    獲取台幣聚合數據
    
    Args:
        path: 文件路徑
        period: 期間，格式為 "2024/01"
        is_mob: 是否為MOB
        
    Returns:
        Tuple: 聚合結果元組
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"獲取台幣聚合數據: {path}, {period}, is_mob={is_mob}")
        
        if is_mob:
            # 處理MOB數據
            mob = MOBUpload('TWD')
            
            # 讀取PR數據
            df_pr = mob.get_df(path=path, sheet='PR')
            df_pr = df_pr.iloc[:, 0:df_pr.columns.get_loc('Question from Reviewer_variable')]
            df_pr = mob.reformate_dtypes(df_pr, ['Entry Amount', 'Accr. Amount_variable'])
            mob(pr=df_pr)
            
            # 處理PR G&A非成本
            mob.handle_pr_ga_nc(period)
            df_pr_ga_nc_dr, df_pr_ga_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理PR S&M非成本
            mob.handle_pr_sm_nc(period)
            df_pr_sm_nc_dr, df_pr_sm_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 讀取PO數據
            df_po = mob.get_df(path=path, sheet='PO')
            df_po = df_po.iloc[:, 0:df_po.columns.get_loc('Question from Reviewer_variable')]
            df_po = mob.reformate_dtypes(df_po, ['Entry Amount', 'Accr. Amount_variable'])
            mob(po=df_po)
            
            # 處理PO G&A非成本
            mob.handle_po_ga_nc(period)
            df_po_ga_nc_dr, df_po_ga_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理PO S&M非成本
            mob.handle_po_sm_nc(period)
            df_po_sm_nc_dr, df_po_sm_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理成本項目
            mob.handle_cost(period)
            df_pr_cost_dr, df_pr_cost_cr = mob.flatten_df_pr_dr_cost, mob.flatten_df_pr_cr_cost
            df_po_cost_dr, df_po_cost_cr = mob.flatten_df_po_dr_cost, mob.flatten_df_po_cr_cost
            
            logger.info("成功獲取MOB台幣聚合數據")
            return (df_pr_ga_nc_dr, df_pr_ga_nc_cr, df_pr_sm_nc_dr, df_pr_sm_nc_cr,
                    df_po_ga_nc_dr, df_po_ga_nc_cr, df_po_sm_nc_dr, df_po_sm_nc_cr,
                    df_pr_cost_dr, df_pr_cost_cr, df_po_cost_dr, df_po_cost_cr)
        else:
            # 處理SPT數據
            logger.info('處理SPT數據')
            spt = SPTUpload('TWD')
            
            # 讀取PR數據
            df_pr = spt.get_df(path=path, sheet='PR')
            df_pr = df_pr.iloc[:, 0:df_pr.columns.get_loc('Question from Reviewer_variable')]
            df_pr = spt.reformate_dtypes(df_pr, ['Entry Amount', 'Accr. Amount_variable'])
            spt(pr=df_pr)
            
            # 處理PR G&A非成本
            spt.handle_pr_ga_nc(period)
            df_pr_ga_nc_dr, df_pr_ga_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理PR S&M非成本
            spt.handle_pr_sm_nc(period)
            df_pr_sm_nc_dr, df_pr_sm_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 讀取PO數據
            df_po = spt.get_df(path=path, sheet='PO')
            df_po = df_po.iloc[:, 0:df_po.columns.get_loc('Question from Reviewer_variable')]
            df_po = spt.reformate_dtypes(df_po, ['Entry Amount', 'Accr. Amount_variable'])
            spt(po=df_po)
            
            # 處理PO G&A非成本
            spt.handle_po_ga_nc(period)
            df_po_ga_nc_dr, df_po_ga_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理PO S&M非成本
            spt.handle_po_sm_nc(period)
            df_po_sm_nc_dr, df_po_sm_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理成本項目
            spt.handle_cost(period)
            df_pr_cost_dr, df_pr_cost_cr = spt.flatten_df_pr_dr_cost, spt.flatten_df_pr_cr_cost
            df_po_cost_dr, df_po_cost_cr = spt.flatten_df_po_dr_cost, spt.flatten_df_po_cr_cost
            
            logger.info("成功獲取SPT台幣聚合數據")
            return (df_pr_ga_nc_dr, df_pr_ga_nc_cr, df_pr_sm_nc_dr, df_pr_sm_nc_cr,
                    df_po_ga_nc_dr, df_po_ga_nc_cr, df_po_sm_nc_dr, df_po_sm_nc_cr,
                    df_pr_cost_dr, df_pr_cost_cr, df_po_cost_dr, df_po_cost_cr)
            
    except Exception as e:
        logger.error(f"獲取台幣聚合數據時出錯: {str(e)}", exc_info=True)
        # 返回12個None組成的元組
        return tuple([None] * 12)


def get_aggregation_foreign(path: str, period: str, is_mob: bool = True, currency: str = 'USD') -> Tuple:
    """
    獲取外幣聚合數據
    
    Args:
        path: 文件路徑
        period: 期間，格式為 "2024/01"
        is_mob: 是否為MOB
        currency: 幣別，默認USD
        
    Returns:
        Tuple: 聚合結果元組
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"獲取外幣聚合數據: {path}, {period}, is_mob={is_mob}, currency={currency}")
        
        if is_mob:
            # 處理MOB數據
            mob = MOBUpload(currency)
            
            # 讀取PR數據
            df_pr = mob.get_df(path=path, sheet='PR')
            df_pr = df_pr.iloc[:, 0:df_pr.columns.get_loc('Question from Reviewer_variable')]
            df_pr = mob.reformate_dtypes(df_pr, ['Entry Amount', 'Accr. Amount_variable'])
            mob(pr=df_pr)
            
            # 處理PR G&A非成本
            mob.handle_pr_ga_nc(period)
            df_pr_ga_nc_dr, df_pr_ga_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理PR S&M非成本
            mob.handle_pr_sm_nc(period)
            df_pr_sm_nc_dr, df_pr_sm_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 讀取PO數據
            df_po = mob.get_df(path=path, sheet='PO')
            df_po = df_po.iloc[:, 0:df_po.columns.get_loc('Question from Reviewer_variable')]
            df_po = mob.reformate_dtypes(df_po, ['Entry Amount', 'Accr. Amount_variable'])
            mob(po=df_po)
            
            # 處理PO G&A非成本
            mob.handle_po_ga_nc(period)
            df_po_ga_nc_dr, df_po_ga_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理PO S&M非成本
            mob.handle_po_sm_nc(period)
            df_po_sm_nc_dr, df_po_sm_nc_cr = mob.flatten_df_dr, mob.flatten_df_cr
            
            # 處理成本項目
            mob.handle_cost(period)
            df_pr_cost_dr, df_pr_cost_cr = mob.flatten_df_pr_dr_cost, mob.flatten_df_pr_cr_cost
            df_po_cost_dr, df_po_cost_cr = mob.flatten_df_po_dr_cost, mob.flatten_df_po_cr_cost
            
            logger.info(f"成功獲取MOB {currency} 聚合數據")
            return (df_pr_ga_nc_dr, df_pr_ga_nc_cr, df_pr_sm_nc_dr, df_pr_sm_nc_cr,
                    df_po_ga_nc_dr, df_po_ga_nc_cr, df_po_sm_nc_dr, df_po_sm_nc_cr,
                    df_pr_cost_dr, df_pr_cost_cr, df_po_cost_dr, df_po_cost_cr)
        else:
            # 處理SPT數據
            logger.info(f'處理SPT {currency} 數據')
            spt = SPTUpload(currency)
            
            # 讀取PR數據
            df_pr = spt.get_df(path=path, sheet='PR')
            df_pr = df_pr.iloc[:, 0:df_pr.columns.get_loc('Question from Reviewer_variable')]
            df_pr = spt.reformate_dtypes(df_pr, ['Entry Amount', 'Accr. Amount_variable'])
            spt(pr=df_pr)
            
            # 處理PR G&A非成本
            spt.handle_pr_ga_nc(period)
            df_pr_ga_nc_dr, df_pr_ga_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理PR S&M非成本
            spt.handle_pr_sm_nc(period)
            df_pr_sm_nc_dr, df_pr_sm_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 讀取PO數據
            df_po = spt.get_df(path=path, sheet='PO')
            df_po = df_po.iloc[:, 0:df_po.columns.get_loc('Question from Reviewer_variable')]
            df_po = spt.reformate_dtypes(df_po, ['Entry Amount', 'Accr. Amount_variable'])
            spt(po=df_po)
            
            # 處理PO G&A非成本
            spt.handle_po_ga_nc(period)
            df_po_ga_nc_dr, df_po_ga_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理PO S&M非成本
            spt.handle_po_sm_nc(period)
            df_po_sm_nc_dr, df_po_sm_nc_cr = spt.flatten_df_dr, spt.flatten_df_cr
            
            # 處理成本項目
            spt.handle_cost(period)
            df_pr_cost_dr, df_pr_cost_cr = spt.flatten_df_pr_dr_cost, spt.flatten_df_pr_cr_cost
            df_po_cost_dr, df_po_cost_cr = spt.flatten_df_po_dr_cost, spt.flatten_df_po_cr_cost
            
            logger.info(f"成功獲取SPT {currency} 聚合數據")
            return (df_pr_ga_nc_dr, df_pr_ga_nc_cr, df_pr_sm_nc_dr, df_pr_sm_nc_cr,
                    df_po_ga_nc_dr, df_po_ga_nc_cr, df_po_sm_nc_dr, df_po_sm_nc_cr,
                    df_pr_cost_dr, df_pr_cost_cr, df_po_cost_dr, df_po_cost_cr)
            
    except Exception as e:
        logger.error(f"獲取外幣聚合數據時出錯: {str(e)}", exc_info=True)
        # 返回12個None組成的元組
        return tuple([None] * 12)


def create_form(df_dr: DataFrame, df_cr: DataFrame,
                entity: str, period: str, accounting_date: str, 
                category: str, usr: str, currency: str) -> Union[DataFrame, None]:
    """
    創建上傳表單
    
    Args:
        df_dr: 借方數據框
        df_cr: 貸方數據框
        entity: 實體名稱
        period: 期間
        accounting_date: 會計日期
        category: 類別
        usr: 使用者
        currency: 幣別
        
    Returns:
        Union[DataFrame, None]: 上傳表單或None
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"創建上傳表單: {entity}, {period}, {currency}")
        
        # 檢查輸入數據框是否有效
        if is_NA_df(df_dr) or is_NA_df(df_cr):
            logger.warning("輸入數據框為空或None")
            return None
        
        # 預處理數據框
        df_dr = pre_defined(df_dr, entity, period, accounting_date, category, usr, currency)
        """
        pre_defined(df_dr)要有之前
        ['Account code_variable', 'Account Name_variable', 'Supplier', 'PR#',
       'Project', 'Product code._variable', 'Region_c_variable',
       'Dep._variable', 'Accr. Amount_variable', 'desc']
        
        之後應該要有
        ['Account code_variable', 'Account Name_variable', 'Supplier', 'PR#',
       'Project', 'Product code._variable', 'Region_c_variable',
       'Dep._variable', 'Accr. Amount_variable', 'desc', 'Period',
       'Accounting Date', 'Category', 'Batch Name', 'Journal Name',
       'Journal Description', 'Currency', 'COMPANY', 'RELATED PARTY',
       'RESERVED', 'Conversion Type']
        
        之前只有['PR#', 'Supplier', 'Project', 'Accr. Amount_variable', 'desc']

        之後有
        ['PR#', 'Supplier', 'Project', 'Accr. Amount_variable', 'desc', 'Period',
       'Accounting Date', 'Category', 'Batch Name', 'Journal Name',
       'Journal Description', 'Currency', 'COMPANY', 'RELATED PARTY',
       'RESERVED', 'Conversion Type']

       get_aggregation_twd有問題
        """
        df_dr = rename_cols(df_dr).pipe(relocate_orders)
        
        df_cr = pre_defined(df_cr, entity, period, accounting_date, category, usr, currency)
        df_cr = rename_cols(df_cr, dr=False).pipe(relocate_orders, dr=False)
        
        # 合併結果
        result = pd.concat([df_dr, df_cr], ignore_index=True)
        
        logger.info(f"成功創建上傳表單, 形狀: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"創建上傳表單時出錯: {str(e)}", exc_info=True)
        return None


def is_NA_df(df: Any) -> bool:
    """
    檢查數據框是否為空或None
    
    Args:
        df: 要檢查的對象
        
    Returns:
        bool: 是否為空或None
    """
    try:
        return any([getattr(df, 'shape', (0,))[0] == 0, df is None])
    except AttributeError:
        return df is None
    except Exception:
        return True


def detect_prpo(word: str) -> str:
    """
    檢測PR或PO
    
    Args:
        word: 輸入字符串
        
    Returns:
        str: 'PR'或'PO'
    """
    if 'PO' in word:
        return 'PO'
    else:
        return "PR"


def rename_cols(df: DataFrame, dr: bool = True) -> DataFrame:
    """
    重命名列
    
    Args:
        df: 數據框
        dr: 是否為借方
        
    Returns:
        DataFrame: 重命名後的數據框
    """
    if dr:
        df = df.rename(columns={
            "Account code_variable": 'ACCOUNT',
            'Product code._variable': 'PRODUCT',
            'Region_c_variable': 'REGION',
            'Dep._variable': 'DEPARTMENT',
            'Accr. Amount_variable': 'Debit',
            'desc': 'Line Description',
        })
        return df
    else:
        df = df.rename(columns={
            "Liability_variable": 'ACCOUNT',
            'Accr. Amount_variable': 'Credit',
            'desc': 'Line Description',
        })
        return df


def relocate_orders(df: DataFrame, dr: bool = True) -> DataFrame:
    """
    重新排列列順序
    
    Args:
        df: 數據框
        dr: 是否為借方
        
    Returns:
        DataFrame: 重新排列後的數據框
    """
    if dr:
        df_re = pd.DataFrame()
        df_re = df_re.assign(**{
            'Period': df['Period'],
            'Accounting Date': df['Accounting Date'],
            'Category': df['Category'],
            'Batch Name': df['Batch Name'],
            'Journal Name': df['Journal Name'],
            'Journal Description': df['Journal Description'],
            'Currency': df['Currency'],
            'COMPANY': df['COMPANY'],
            'ACCOUNT': df['ACCOUNT'],
            'PRODUCT': df['PRODUCT'],
            'REGION': df['REGION'],
            'DEPARTMENT': df['DEPARTMENT'],
            'RELATED PARTY': df['RELATED PARTY'],
            'RESERVED': df['RESERVED'],
            'Debit': df['Debit'],
            'Credit': None,
            'Line Description': df['Line Description'],
            'Conversion Type': df['Conversion Type'],
            'Conversion Date': None,
            'Line DFF': None,
        })
        return df_re
    else:
        df_re = pd.DataFrame()
        df_re = df_re.assign(**{
            'Period': df['Period'],
            'Accounting Date': df['Accounting Date'],
            'Category': df['Category'],
            'Batch Name': df['Batch Name'],
            'Journal Name': df['Journal Name'],
            'Journal Description': df['Journal Description'],
            'Currency': df['Currency'],
            'COMPANY': df['COMPANY'],
            'ACCOUNT': df['ACCOUNT'],
            'PRODUCT': '000',
            'REGION': '000',
            'DEPARTMENT': '000',
            'RELATED PARTY': '000',
            'RESERVED': df['RESERVED'],
            'Debit': None,
            'Credit': df['Credit'],
            'Line Description': df['Line Description'],
            'Conversion Type': df['Conversion Type'],
            'Conversion Date': None,
            'Line DFF': None,
        })
        return df_re


def pre_defined(df: DataFrame, entity: str, period: str, 
                accounting_date: str, category: str, 
                usr: str, currency: str) -> DataFrame:
    """
    預定義數據框
    
    Args:
        df: 數據框
        entity: 實體名稱
        period: 期間
        accounting_date: 會計日期
        category: 類別
        usr: 使用者
        currency: 幣別
        
    Returns:
        DataFrame: 預定義後的數據框
    """
    logger = Logger().get_logger(__name__)
    
    try:
        data_type = detect_prpo(df.desc.iloc[0])
        
        if currency == 'TWD':
            df['Period'] = period
            df['Accounting Date'] = accounting_date
            df['Category'] = category
            df['Batch Name'] = entity + '-' + usr + '-' + df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0]) + f"{data_type}"
            df['Journal Name'] = df['Batch Name']
            df['Journal Description'] = df.desc.apply(lambda x: x.split(f"{data_type}_", 1)[0]) + f"{data_type}"
            df['Currency'] = currency
            df['COMPANY'] = entity
            df['RELATED PARTY'] = '000'
            df['RESERVED'] = '0'
            df['Conversion Type'] = 'Corporate'
        else:
            df['Period'] = period
            df['Accounting Date'] = accounting_date
            df['Category'] = category
            df['Batch Name'] = entity + '-' + usr + '-' + df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0]) + f"{data_type}" + '-' + currency
            df['Journal Name'] = df['Batch Name']
            df['Journal Description'] = df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0]) + f"{data_type}" + '-' + currency
            df['Currency'] = currency
            df['COMPANY'] = entity
            df['RELATED PARTY'] = '000'
            df['RESERVED'] = '0'
            df['Conversion Type'] = 'Corporate'
            
        return df
        
    except Exception as e:
        logger.error(f"預定義數據框時出錯: {str(e)}", exc_info=True)
        return df


def get_entries(dfs: Iterable, entity: str, period: str, 
                ac_date: str, cate: str, accountant: str, 
                currency: str) -> DataFrame:
    """
    獲取所有條目
    
    Args:
        dfs: 數據框元組
        entity: 實體名稱
        period: 期間
        ac_date: 會計日期
        cate: 類別
        accountant: 會計人員
        currency: 幣別
        
    Returns:
        DataFrame: 所有條目數據框
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"獲取所有條目: {entity}, {period}, {currency}")
        
        # 創建所有可能的表單
        concat_dfs = [
            create_form(dfs[a], dfs[b], entity, period, ac_date, cate, accountant, currency)
            for a, b in zip(range(0, 12, 2), range(1, 12, 2))
        ]
        
        # 過濾掉None值
        concat_dfs = [df for df in concat_dfs if df is not None]
        
        if not concat_dfs:
            logger.warning("沒有有效的表單數據")
            return pd.DataFrame()
            
        # 合併結果
        result = pd.concat(concat_dfs, ignore_index=True)
        
        logger.info(f"成功獲取所有條目, 形狀: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"獲取所有條目時出錯: {str(e)}", exc_info=True)
        return pd.DataFrame()


# 測試代碼，僅在直接運行時執行
if __name__ == "__main__":
    """測試上傳表單功能"""
    # path = r'C:\SEA\MOB PRPO re\頂一下\202501\SPT\BACA看過\SPT_202501_POPR_wp_v06 2nd reviewed by Rebecca.xlsm'
    path = r'C:\SEA\MOB PRPO re\頂一下\202501\MOBA\BACA看過\MOBTW_202501_Purchase Order_WP.xlsm'
    entity, period, ac_date, cate, accountant, currency = \
        'MOBTW', 'JAN-25', '2025/01/23', '01 SEA Accrual Expense', 'Rebecca', 'HKD'
    
    # 獲取聚合數據
    # dfs = get_aggregation_twd(path, 
    #                           '2025/01', 
    #                           is_mob=True)
    dfs = get_aggregation_foreign(path, 
                                  '2025/01', 
                                  is_mob=True, 
                                  currency=currency)
    
    # 生成上傳表單
    result = get_entries(dfs, entity, period, ac_date, cate, accountant, currency)
    
    # 保存結果
    result.to_excel(r'MOB_test_{period}_{currency}_.xlsx'.format(period=period[:3], currency=currency), index=False)
    
    print("上傳表單生成完成")