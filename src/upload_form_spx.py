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


class SPXUploadFormProcessor:
    """SPX Upload Form處理基類，提供專屬的處理方法"""
    
    def __init__(self, currency: str = 'TWD'):
        """
        初始化SPX Upload Form處理器
        
        Args:
            currency: 貨幣類型，默認TWD
        """
        self.currency = currency
        self.logger = Logger().get_logger(__name__)
        self.config = ConfigManager()
        
        # 從配置中加載SPX設置
        self.pr_ga_pivot_index = self.config.get('SPX', 'pr_ga_pivot_index')
        self.pr_sm_pivot_index = self.config.get('SPX', 'pr_sm_pivot_index')
        self.po_pivot_index = self.config.get('SPX', 'po_pivot_index')
        self.sm_cr_pivot_cols = self.config.get('SPX', 'sm_cr_pivot_cols')
        self.ga_cr_pivot_cols = self.config.get('SPX', 'ga_cr_pivot_cols')
        self.pivot_value_col = self.config.get('SPX', 'pivot_value_col')
        self.ap_columns = self.config.get('SPX', 'ap_columns')
        
        self.logger.info(f"SPX Upload Form處理器初始化完成，幣別: {currency}")
    
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
            
            # 統一供應商欄位名稱（SPX特有邏輯）
            if 'PR Supplier' in df.columns:
                df_reformated = df.rename(columns={'PR Supplier': 'Supplier'})
            elif 'PO Supplier' in df.columns:
                df_reformated = df.rename(columns={'PO Supplier': 'Supplier'})
            else:
                df_reformated = df.copy()
                
            self.logger.debug(f"成功讀取數據框，形狀: {df_reformated.shape}")
            return df_reformated
            
        except Exception as e:
            self.logger.error(f"獲取數據框時出錯: {str(e)}", exc_info=True)
            raise
    
    def reformate_dtypes(self, df: DataFrame, amount_cols: List[str]) -> DataFrame:
        """
        重新格式化數據類型
        
        Args:
            df: 數據框
            amount_cols: 金額列名列表
            
        Returns:
            DataFrame: 格式化後的數據框
        """
        try:
            df_c = df.copy()
            for col in amount_cols:
                if col in df_c.columns:
                    # 處理逗號分隔的數字格式
                    df_c[col] = df_c[col].astype(str).str.replace(',', '')
                    df_c[col] = pd.to_numeric(df_c[col], errors='coerce').fillna(0)
                    
            self.logger.debug(f"成功格式化數據類型，處理了 {len(amount_cols)} 個金額欄位")
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
        return df['是否估計入帳'] == 'Y'
    
    def is_fa_equals_to_na(self, df: DataFrame) -> Series:
        """
        檢查是否為FA（SPX邏輯）
        
        Args:
            df: 數據框
            
        Returns:
            Series: 布爾序列
        """
        try:
            if '是否為FA' in df.columns:
                return (df['是否為FA'].isna()) | (df['是否為FA'] == 'N')
            else:
                # 如果沒有FA欄位，默認為非FA
                return pd.Series([True] * len(df), index=df.index)
                
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
        sm_col = '是否為S&M' if '是否為S&M' in df.columns else '是否為S&M_variable'
        return df[sm_col] == sm
    
    def is_target_currency(self, df: DataFrame, currency: str) -> Series:
        """
        檢查是否為指定幣別
        
        Args:
            df: 數據框
            currency: 幣別
            
        Returns:
            Series: 布爾序列
        """
        currency_col = 'Currency' if 'Currency' in df.columns else 'Currency_c_variable'
        return df[currency_col] == currency
    
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
            if df.empty:
                self.logger.warning("輸入數據框為空，返回空樞紐表")
                return pd.DataFrame()
                
            pivot = df.pivot_table(
                index=index,
                values=values,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=0
            ).rename_axis(None, axis=1).reset_index()
            
            self.logger.debug(f"成功建立樞紐表，形狀: {pivot.shape}")
            return pivot
            
        except Exception as e:
            self.logger.error(f"建立樞紐表時出錯: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def get_item_description_dr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = False) -> Series:
        """
        獲取借方項目描述（SPX專屬格式）
        
        Args:
            df: 數據框
            period: 期間，格式為 "2025/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost:
                constant = f'{period} Accrual SPX Cost from {is_pr}'
                if f'{is_pr}#' in df.columns and 'Supplier' in df.columns:
                    desc = constant + '_' + df[f'{is_pr}#'] + '_' + df['Supplier']
                else:
                    desc = constant + '_SPX'
            else:
                if sm:
                    constant = f'{period} Accrual SPX S&M from {is_pr}'
                else:
                    constant = f'{period} Accrual SPX G&A from {is_pr}'
                    
                if f'{is_pr}#' in df.columns and 'Supplier' in df.columns:
                    desc = constant + '_' + df['Supplier'] + '_' + df[f'{is_pr}#']
                else:
                    desc = constant + '_SPX'
                    
            return desc
                
        except Exception as e:
            self.logger.error(f"獲取借方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([f'{period} SPX Accrual'] * len(df))
    
    def get_item_description_cr(self, df: DataFrame, period: str, 
                                sm: bool = True, is_pr: str = 'PR', 
                                is_cost: bool = True) -> Series:
        """
        獲取貸方項目描述（SPX專屬格式）
        
        Args:
            df: 數據框
            period: 期間，格式為 "2025/01"
            sm: 是否為S&M
            is_pr: 'PR'或'PO'
            is_cost: 是否為成本
            
        Returns:
            Series: 項目描述序列
        """
        try:
            if is_cost:
                constant = f'{period} Accrual SPX Cost from {is_pr}'
                if 'Account Name' in df.columns:
                    desc = constant + '_' + df['Account Name']
                else:
                    desc = constant + '_SPX'
            else:
                if sm:
                    constant = f'{period} Accrual SPX S&M from {is_pr}'
                    if 'Account Name' in df.columns:
                        desc = constant + '_' + df['Account Name']
                    else:
                        desc = constant + '_SPX'
                else:
                    constant = f'{period} Accrual SPX G&A from {is_pr}'
                    if 'Account Name' in df.columns:
                        desc = constant + '_' + df['Account Name']
                    else:
                        desc = constant + '_SPX'
                        
            return desc
                    
        except Exception as e:
            self.logger.error(f"獲取貸方項目描述時出錯: {str(e)}", exc_info=True)
            return pd.Series([f'{period} SPX Accrual'] * len(df))
    
    def extract_cost_items(self, df: DataFrame, extract: bool = True) -> DataFrame:
        """
        提取成本項目（SPX邏輯）
        
        Args:
            df: 數據框
            extract: 是提取還是排除成本項目
            
        Returns:
            DataFrame: 處理後的數據框
        """
        try:
            account_col = 'Account code' if 'Account code' in df.columns else 'Account code_variable'
            
            if account_col not in df.columns:
                self.logger.warning("找不到科目代碼欄位，返回原數據框")
                return df if not extract else pd.DataFrame()
                
            if extract:
                # SPX成本科目以199999為主
                df_filtered = df.loc[
                    df[account_col].fillna('na').str.contains('^(199)', regex=True), :
                ].reset_index(drop=True)
                self.logger.debug(f"提取成本項目，從 {len(df)} 行提取到 {len(df_filtered)} 行")
                return df_filtered
            else:
                # 排除成本項目
                df_filtered = df.loc[
                    ~df[account_col].fillna('na').str.contains('^(199)', regex=True), :
                ].reset_index(drop=True)
                self.logger.debug(f"排除成本項目，從 {len(df)} 行保留 {len(df_filtered)} 行")
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
        try:
            # 處理不同格式的配置值
            if config_value.startswith("'") and config_value.endswith("'"):
                # 去除外層引號後分割
                inner = config_value.strip("'")
                if "', '" in inner:
                    return inner.split("', '")
                else:
                    return [inner]
            else:
                # 直接以逗號分割並去除空格
                return [item.strip().strip("'") for item in config_value.split(',')]
        except Exception as e:
            self.logger.error(f"分割配置值時出錯: {str(e)}")
            return [config_value]
    
    def strip_config_value(self, config_value: str) -> str:
        """
        去除配置值的引號
        
        Args:
            config_value: 配置字符串
            
        Returns:
            str: 處理後的字符串
        """
        return config_value.strip("'")


class SPXUpload(SPXUploadFormProcessor):
    """SPX Upload Form處理器"""
    
    def __init__(self, currency: str = 'TWD', df_pr: DataFrame = None, df_po: DataFrame = None):
        """
        初始化SPX Upload Form處理器
        
        Args:
            currency: 貨幣類型，默認TWD
            df_pr: PR數據框
            df_po: PO數據框
        """
        super().__init__(currency)
        self.df_pr = df_pr
        self.df_po = df_po
        
        # 初始化結果變數
        self.flatten_df_dr = pd.DataFrame()
        self.flatten_df_cr = pd.DataFrame()
        self.flatten_df_pr_dr_cost = None
        self.flatten_df_pr_cr_cost = None
        self.flatten_df_po_dr_cost = None
        self.flatten_df_po_cr_cost = None
    
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
            if df is None or df.empty:
                self.logger.warning("輸入數據框為空")
                return pd.DataFrame()
                
            if extract:
                """成本/非成本項目篩選
                extract=True:
                    是否估計入帳=Y
                    是否為FA=Y
                    是否為S&M=N
                    幣別
                extract=False:
                    是否估計入帳=Y
                    是否為FA=N
                    是否為S&M=N or Y
                    幣別
                """
                filtered_df = df.loc[
                    (self.is_book_equals_to_y(df)) &
                    (self.is_sm_expense(df, sm=sm)) &
                    (self.is_target_currency(df, currency=self.currency)), :
                ]
                
                result = self.extract_cost_items(filtered_df, extract=extract)
                self.logger.debug(f"過濾後數據框形狀: {result.shape}")
                return result
            else:
                filtered_df = df.loc[
                    (self.is_book_equals_to_y(df)) &
                    (self.is_fa_equals_to_na(df)) &
                    (self.is_sm_expense(df, sm=sm)) &
                    (self.is_target_currency(df, currency=self.currency)), :
                ]
                
                result = self.extract_cost_items(filtered_df, extract=extract)
                self.logger.debug(f"過濾後數據框形狀: {result.shape}")
                return result
            
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
                
            # 確定使用的pivot配置
            if pr:
                if sm:
                    # PR S&M
                    dr_index = self.split_config_value(self.pr_sm_pivot_index)
                    cr_index = self.split_config_value(self.sm_cr_pivot_cols)
                else:
                    # PR G&A
                    dr_index = self.split_config_value(self.pr_ga_pivot_index)
                    cr_index = self.split_config_value(self.ga_cr_pivot_cols)
            else:
                # PO (統一使用po_pivot_index)
                dr_index = self.split_config_value(self.po_pivot_index)
                if sm:
                    cr_index = self.split_config_value(self.sm_cr_pivot_cols)
                else:
                    cr_index = self.split_config_value(self.ga_cr_pivot_cols)
            
            # 建立樞紐表
            value_col = self.strip_config_value(self.pivot_value_col)
            
            flatten_df_dr = self.make_pivot(
                df_non_cost,
                dr_index,
                value_col
            )
            flatten_df_cr = self.make_pivot(
                df_non_cost,
                cr_index,
                value_col
            )
            
            # 檢查樞紐表是否有效
            if not flatten_df_dr.empty and not flatten_df_cr.empty:
                dr_sum = flatten_df_dr[value_col].sum() if value_col in flatten_df_dr.columns else 0
                cr_sum = flatten_df_cr[value_col].sum() if value_col in flatten_df_cr.columns else 0
                
                type_str = f"{'PR' if pr else 'PO'} {'S&M' if sm else 'G&A'} NC"
                self.logger.info(f"{type_str}: DR={dr_sum:,.0f}, CR={cr_sum:,.0f}")
                
                # 添加描述
                self.flatten_df_dr = flatten_df_dr.assign(
                    desc=self.get_item_description_dr(
                        flatten_df_dr, period, sm=sm, 
                        is_pr='PR' if pr else 'PO', is_cost=False
                    )
                )
                self.flatten_df_cr = flatten_df_cr.assign(
                    desc=self.get_item_description_cr(
                        flatten_df_cr, period, sm=sm, 
                        is_pr='PR' if pr else 'PO', is_cost=False
                    )
                )
            else:
                self.logger.warning("樞紐表為空")
                self.flatten_df_dr = pd.DataFrame()
                self.flatten_df_cr = pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"處理非成本項目時出錯: {str(e)}", exc_info=True)
            self.flatten_df_dr = pd.DataFrame()
            self.flatten_df_cr = pd.DataFrame()
    
    def handle_pr_sm_nc(self, period: str):
        """處理PR S&M非成本項目"""
        try:
            self.logger.info(f"處理PR S&M非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='Y', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=True, pr=True)
        except Exception as e:
            self.logger.error(f"處理PR S&M非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_pr_ga_nc(self, period: str):
        """處理PR G&A非成本項目"""
        try:
            self.logger.info(f"處理PR G&A非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='N', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=False, pr=True)
        except Exception as e:
            self.logger.error(f"處理PR G&A非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_po_sm_nc(self, period: str):
        """處理PO S&M非成本項目"""
        try:
            self.logger.info(f"處理PO S&M非成本項目: {period}")
            df_non_cost = self.get_filtered_df(self.df_po, is_pr=False, sm='Y', extract=False)
            self.handle_non_cost(df_non_cost, period, sm=True, pr=False)
        except Exception as e:
            self.logger.error(f"處理PO S&M非成本項目時出錯: {str(e)}", exc_info=True)
    
    def handle_po_ga_nc(self, period: str):
        """處理PO G&A非成本項目"""
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
            period: 期間，格式為 "2025/01"
        """
        try:
            self.logger.info(f"處理成本項目: {period}")
            value_col = self.strip_config_value(self.pivot_value_col)
            
            # 處理PR G&A成本
            if self.df_pr is not None:
                df_pr_ga_cost = self.get_filtered_df(self.df_pr, is_pr=True, sm='N', extract=True)
                if not df_pr_ga_cost.empty:
                    flatten_df_dr_ga = self.make_pivot(
                        df_pr_ga_cost,
                        self.split_config_value(self.pr_ga_pivot_index),
                        value_col
                    )
                    flatten_df_cr_ga = self.make_pivot(
                        df_pr_ga_cost,
                        self.split_config_value(self.ga_cr_pivot_cols),
                        value_col
                    )
                    
                    self.flatten_df_pr_dr_cost = flatten_df_dr_ga.assign(
                        desc=self.get_item_description_dr(flatten_df_dr_ga, period, sm=False, is_pr='PR', is_cost=True)
                    )
                    self.flatten_df_pr_cr_cost = flatten_df_cr_ga.assign(
                        desc=self.get_item_description_cr(flatten_df_cr_ga, period, sm=False, is_pr='PR', is_cost=True)
                    )
                    
                    self.logger.info("成功處理PR G&A成本項目")
                    try:
                        df_pr_dr_cost, df_pr_cr_cost = self.flatten_df_pr_dr_cost.copy(), self.flatten_df_pr_cr_cost.copy()
                        if not df_pr_dr_cost.empty and not df_pr_cr_cost.empty:
                            dr_sum = df_pr_dr_cost[value_col].sum() if value_col in df_pr_dr_cost.columns else 0
                            cr_sum = df_pr_cr_cost[value_col].sum() if value_col in df_pr_cr_cost.columns else 0
                            self.logger.info(f"PR Cost: DR={dr_sum:,.0f}, CR={cr_sum:,.0f}")
                    except Exception as e:
                        self.logger.error(f"統計PR成本項目時出錯: {str(e)}", exc_info=True)
                else:
                    self.logger.info("PR G&A成本項目為空")
                    self.flatten_df_pr_dr_cost, self.flatten_df_pr_cr_cost = None, None
            
            # 處理PO G&A成本
            if self.df_po is not None:
                df_po_ga_cost = self.get_filtered_df(self.df_po, is_pr=False, sm='N', extract=True)
                if not df_po_ga_cost.empty:
                    flatten_df_dr_ga = self.make_pivot(
                        df_po_ga_cost,
                        self.split_config_value(self.po_pivot_index),
                        value_col
                    )
                    flatten_df_cr_ga = self.make_pivot(
                        df_po_ga_cost,
                        self.split_config_value(self.ga_cr_pivot_cols),
                        value_col
                    )
                    
                    self.flatten_df_po_dr_cost = flatten_df_dr_ga.assign(
                        desc=self.get_item_description_dr(flatten_df_dr_ga, period, sm=False, is_pr='PO', is_cost=True)
                    )
                    self.flatten_df_po_cr_cost = flatten_df_cr_ga.assign(
                        desc=self.get_item_description_cr(flatten_df_cr_ga, period, sm=False, is_pr='PO', is_cost=True)
                    )
                    
                    self.logger.info("成功處理PO G&A成本項目")
                    try:
                        df_pr_dr_cost, df_pr_cr_cost = self.flatten_df_po_dr_cost.copy(), self.flatten_df_po_cr_cost.copy()
                        if not df_pr_dr_cost.empty and not df_pr_cr_cost.empty:
                            dr_sum = df_pr_dr_cost[value_col].sum() if value_col in df_pr_dr_cost.columns else 0
                            cr_sum = df_pr_cr_cost[value_col].sum() if value_col in df_pr_cr_cost.columns else 0
                            self.logger.info(f"PO Cost: DR={dr_sum:,.0f}, CR={cr_sum:,.0f}")
                    except Exception as e:
                        self.logger.error(f"統計PO成本項目時出錯: {str(e)}", exc_info=True)
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


def get_aggregation_spx(path: str, period: str, currency: str = 'TWD') -> Tuple:
    """
    獲取SPX聚合數據
    
    Args:
        path: 文件路徑
        period: 期間，格式為 "2025/01"
        currency: 幣別，默認TWD
        
    Returns:
        Tuple: 聚合結果元組
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"獲取SPX聚合數據: {path}, {period}, currency={currency}")
        
        # 初始化SPX處理器
        spx = SPXUpload(currency)
        
        # 讀取PR數據
        try:
            df_pr = spx.get_df(path=path, sheet='PR')
            if 'Question from Reviewer_variable' in df_pr.columns:
                df_pr = df_pr.iloc[:, 0:df_pr.columns.get_loc('Question from Reviewer_variable')]
            df_pr = spx.reformate_dtypes(df_pr, ['Entry Amount', 'Accr. Amount'])
            spx(pr=df_pr)
        except Exception as e:
            logger.warning(f"讀取PR數據時出錯: {str(e)}")
            df_pr = pd.DataFrame()
            spx.df_pr = df_pr
        
        # 處理PR G&A非成本
        spx.handle_pr_ga_nc(period)
        df_pr_ga_nc_dr, df_pr_ga_nc_cr = spx.flatten_df_dr, spx.flatten_df_cr
        
        # 處理PR S&M非成本
        spx.handle_pr_sm_nc(period)
        df_pr_sm_nc_dr, df_pr_sm_nc_cr = spx.flatten_df_dr, spx.flatten_df_cr
        
        # 讀取PO數據
        try:
            df_po = spx.get_df(path=path, sheet='PO')
            if 'Question from Reviewer_variable' in df_po.columns:
                df_po = df_po.iloc[:, 0:df_po.columns.get_loc('Question from Reviewer_variable')]
            df_po = spx.reformate_dtypes(df_po, ['Entry Amount', 'Accr. Amount'])
            spx(po=df_po)
        except Exception as e:
            logger.warning(f"讀取PO數據時出錯: {str(e)}")
            df_po = pd.DataFrame()
            spx.df_po = df_po
        
        # 處理PO G&A非成本
        spx.handle_po_ga_nc(period)
        df_po_ga_nc_dr, df_po_ga_nc_cr = spx.flatten_df_dr, spx.flatten_df_cr
        
        # 處理PO S&M非成本
        spx.handle_po_sm_nc(period)
        df_po_sm_nc_dr, df_po_sm_nc_cr = spx.flatten_df_dr, spx.flatten_df_cr
        
        # 處理成本項目
        spx.handle_cost(period)
        df_pr_cost_dr, df_pr_cost_cr = spx.flatten_df_pr_dr_cost, spx.flatten_df_pr_cr_cost
        df_po_cost_dr, df_po_cost_cr = spx.flatten_df_po_dr_cost, spx.flatten_df_po_cr_cost
        
        logger.info(f"成功獲取SPX {currency} 聚合數據")
        return (df_pr_ga_nc_dr, df_pr_ga_nc_cr, df_pr_sm_nc_dr, df_pr_sm_nc_cr,
                df_po_ga_nc_dr, df_po_ga_nc_cr, df_po_sm_nc_dr, df_po_sm_nc_cr,
                df_pr_cost_dr, df_pr_cost_cr, df_po_cost_dr, df_po_cost_cr)
        
    except Exception as e:
        logger.error(f"獲取SPX聚合數據時出錯: {str(e)}", exc_info=True)
        # 返回12個None組成的元組
        return tuple([None] * 12)


def create_spx_form(df_dr: DataFrame, df_cr: DataFrame,
                    entity: str, period: str, accounting_date: str, 
                    category: str, usr: str, currency: str) -> Union[DataFrame, None]:
    """
    創建SPX Upload Form
    
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
        Union[DataFrame, None]: Upload Form或None
    """
    logger = Logger().get_logger(__name__)
    
    try:
        logger.info(f"創建SPX Upload Form: {entity}, {period}, {currency}")
        
        # 檢查輸入數據框是否有效
        if is_NA_df(df_dr) or is_NA_df(df_cr):
            logger.warning("輸入數據框為空或None")
            return None
        
        # 預處理數據框
        df_dr = pre_defined_spx(df_dr, entity, period, accounting_date, category, usr, currency)
        df_dr = rename_cols_spx(df_dr).pipe(relocate_orders_spx)
        
        df_cr = pre_defined_spx(df_cr, entity, period, accounting_date, category, usr, currency)
        df_cr = rename_cols_spx(df_cr, dr=False).pipe(relocate_orders_spx, dr=False)
        
        # 合併結果
        result = pd.concat([df_dr, df_cr], ignore_index=True)
        
        logger.info(f"成功創建SPX Upload Form, 形狀: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"創建SPX Upload Form時出錯: {str(e)}", exc_info=True)
        return None


def is_NA_df(df: Any) -> bool:
    """檢查數據框是否為空或None"""
    try:
        return any([getattr(df, 'shape', (0,))[0] == 0, df is None])
    except AttributeError:
        return df is None
    except Exception:
        return True


def detect_prpo(word: str) -> str:
    """檢測PR或PO"""
    if 'PO' in word:
        return 'PO'
    else:
        return "PR"


def rename_cols_spx(df: DataFrame, dr: bool = True) -> DataFrame:
    """重命名列（SPX專屬）"""
    if dr:
        rename_map = {
            "Account code": 'ACCOUNT',
            'Product code': 'PRODUCT',
            'Region': 'REGION',
            'Dep.': 'DEPARTMENT',
            'Accr. Amount': 'Debit',
            'desc': 'Line Description',
        }
        # 支援變數名稱
        rename_map.update({
            "Account code_variable": 'ACCOUNT',
            'Product code._variable': 'PRODUCT',
            'Region_c_variable': 'REGION',
            'Dep._variable': 'DEPARTMENT',
            'Accr. Amount_variable': 'Debit',
        })
        df = df.rename(columns=rename_map)
        return df
    else:
        rename_map = {
            "Liability": 'ACCOUNT',
            'Accr. Amount': 'Credit',
            'desc': 'Line Description',
        }
        # 支援變數名稱
        rename_map.update({
            "Liability_variable": 'ACCOUNT',
            'Accr. Amount_variable': 'Credit',
        })
        df = df.rename(columns=rename_map)
        return df


def relocate_orders_spx(df: DataFrame, dr: bool = True) -> DataFrame:
    """重新排列列順序（SPX專屬）"""
    base_columns = [
        'Period', 'Accounting Date', 'Category', 'Batch Name', 'Journal Name',
        'Journal Description', 'Currency', 'COMPANY', 'ACCOUNT', 'PRODUCT',
        'REGION', 'DEPARTMENT', 'RELATED PARTY', 'RESERVED'
    ]
    
    if dr:
        columns_order = base_columns + ['Debit', 'Credit', 'Line Description', 
                                       'Conversion Type', 'Conversion Date', 'Line DFF']
        df_re = pd.DataFrame()
        df_re = df_re.assign(**{
            col: df.get(col, None) for col in base_columns
        })
        df_re['Debit'] = df.get('Debit', None)
        df_re['Credit'] = None
    else:
        columns_order = base_columns + ['Debit', 'Credit', 'Line Description', 
                                       'Conversion Type', 'Conversion Date', 'Line DFF']
        df_re = pd.DataFrame()
        df_re = df_re.assign(**{
            col: df.get(col, '000' if col in ['PRODUCT', 'REGION', 'DEPARTMENT', 'RELATED PARTY'] else None) 
            for col in base_columns
        })
        df_re['PRODUCT'] = '000'
        df_re['REGION'] = '000'
        df_re['DEPARTMENT'] = '000'
        df_re['RELATED PARTY'] = '000'
        df_re['Debit'] = None
        df_re['Credit'] = df.get('Credit', None)
    
    # 添加剩餘欄位
    df_re['Line Description'] = df.get('Line Description', '')
    df_re['Conversion Type'] = df.get('Conversion Type', 'Corporate')
    df_re['Conversion Date'] = None
    df_re['Line DFF'] = None
    
    return df_re


def pre_defined_spx(df: DataFrame, entity: str, period: str, 
                    accounting_date: str, category: str, 
                    usr: str, currency: str) -> DataFrame:
    """預定義數據框（SPX專屬）"""
    logger = Logger().get_logger(__name__)
    
    try:
        data_type = detect_prpo(df.desc.iloc[0])
        
        df['Period'] = period
        df['Accounting Date'] = accounting_date
        df['Category'] = category
        
        if currency == 'TWD':
            df['Batch Name'] = entity + '-' + usr + '-' + df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0] if f"{data_type}_" in x else x.split(' ')[0]
            ) + f"{data_type}"
            df['Journal Description'] = df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0] if f"{data_type}_" in x else x.split(' ')[0]
            ) + f"{data_type}"
        else:
            df['Batch Name'] = entity + '-' + usr + '-' + df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0] if f"{data_type}_" in x else x.split(' ')[0]
            ) + f"{data_type}" + '-' + currency
            df['Journal Description'] = df.desc.apply(
                lambda x: x.split(f"{data_type}_", 1)[0] if f"{data_type}_" in x else x.split(' ')[0]
            ) + f"{data_type}" + '-' + currency
            
        df['Journal Name'] = df['Batch Name']
        df['Currency'] = currency
        df['COMPANY'] = entity
        df['RELATED PARTY'] = '000'
        df['RESERVED'] = '0'
        df['Conversion Type'] = 'Corporate'
            
        return df
        
    except Exception as e:
        logger.error(f"預定義SPX數據框時出錯: {str(e)}", exc_info=True)
        return df


def get_spx_entries(dfs: Iterable, entity: str, period: str, 
                    ac_date: str, cate: str, accountant: str, 
                    currency: str) -> DataFrame:
    """
    獲取SPX所有條目
    
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
        logger.info(f"獲取SPX所有條目: {entity}, {period}, {currency}")
        
        # 創建所有可能的表單
        concat_dfs = [
            create_spx_form(dfs[a], dfs[b], entity, period, ac_date, cate, accountant, currency)
            for a, b in zip(range(0, 12, 2), range(1, 12, 2))
        ]
        
        # 過濾掉None值
        concat_dfs = [df for df in concat_dfs if df is not None]
        
        if not concat_dfs:
            logger.warning("沒有有效的SPX表單數據")
            return pd.DataFrame()
            
        # 合併結果
        result = pd.concat(concat_dfs, ignore_index=True)
        
        logger.info(f"成功獲取SPX所有條目, 形狀: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"獲取SPX所有條目時出錯: {str(e)}", exc_info=True)
        return pd.DataFrame()


# 測試代碼，僅在直接運行時執行
if __name__ == "__main__":
    """測試SPX Upload Form功能"""
    
    # Debug模式設定
    DEBUG_MODE = True
    
    if DEBUG_MODE:
        print("=== SPX Upload Form 測試模式 ===")
        
        # 測試檔案路徑（請根據實際情況修改）
        test_path = r'C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\UploadForm檢視用\SPTTW-202504-Purchase Order-Accrued-SPX_Review.1.xlsx'
        
        # 測試參數
        entity = 'SPXTW'
        period = 'APR-25'
        ac_date = '2025/04/30'
        cate = '01 SEA Accrual Expense'
        accountant = 'Allen'
        currency = 'TWD'
        period_format = '2025/04'
        
        try:
            print(f"測試檔案: {test_path}")
            print(f"實體: {entity}, 期間: {period}, 幣別: {currency}")
            
            # 檢查檔案是否存在
            if not os.path.exists(test_path):
                print(f"錯誤: 測試檔案不存在 - {test_path}")
                print("請修改test_path變數為有效的檔案路徑")
            else:
                # 獲取聚合數據
                print("正在獲取SPX聚合數據...")
                dfs = get_aggregation_spx(test_path, period_format, currency)
                
                # 生成Upload Form
                print("正在生成SPX Upload Form...")
                result = get_spx_entries(dfs, entity, period, ac_date, cate, accountant, currency)
                
                if not result.empty:
                    # 保存結果
                    output_file = f'Upload Form-{entity}-{period}-{currency}.xlsx'
                    result.to_excel(output_file, index=False)
                    print(f"成功生成 SPX Upload Form: {output_file}")
                    print(f"總行數: {len(result)}")
                    print(f"總金額: {result['Debit'].fillna(0).astype(float).sum() + result['Credit'].fillna(0).astype(float).sum():,.0f}")
                else:
                    print("警告: 生成的Upload Form為空")
                    
        except Exception as e:
            print(f"測試時發生錯誤: {str(e)}")
            print("請檢查檔案路徑和格式是否正確")
            
        print("=== 測試完成 ===")
    else:
        print("SPX Upload Form 模組已載入")
        print("使用 get_aggregation_spx() 和 get_spx_entries() 函數來處理數據")
