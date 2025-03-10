import logging
import pandas as pd
import numpy as np
from typing import Tuple

from utils import Logger


class HRISDuplicateChecker:
    """HRIS重複項檢查器，用於檢查PR、PO和AP中的重複項"""
    
    def __init__(self):
        """初始化重複項檢查器"""
        self.logger = Logger().get_logger(__name__)
    
    def check_duplicates_in_po(self, df_pr: pd.DataFrame, df_po: pd.DataFrame) -> pd.DataFrame:
        """檢查PO中的重複項
        
        Args:
            df_pr: PR數據框
            df_po: PO數據框
            
        Returns:
            pd.DataFrame: 更新了dup標記的PR數據框
        """
        try:
            self.logger.info("檢查PO中的重複項")
            
            # 找出PR Line在PO中出現多次的情況
            duplicate_pr_lines = df_po.groupby('PR Line')['PO Supplier'].count().loc[lambda x: x > 1].index
            
            # 標記重複項
            df_pr['dup'] = np.where(df_pr['PR Line'].isin(duplicate_pr_lines), 'dup', pd.NA)
            
            self.logger.info(f"發現 {len(duplicate_pr_lines)} 個重複的PR Line")
            return df_pr
            
        except Exception as e:
            self.logger.error(f"檢查PO中的重複項時出錯: {str(e)}", exc_info=True)
            raise
    
    def check_duplicates_in_ap(self, df_pr: pd.DataFrame, df_po: pd.DataFrame, df_ap: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """檢查AP中的重複項
        
        Args:
            df_pr: PR數據框
            df_po: PO數據框
            df_ap: AP數據框
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 更新了ap_invoice標記的PR和PO數據框
        """
        try:
            self.logger.info("檢查AP中的重複項")
            
            # 只保留有Receipt Number的AP記錄
            df_ap = df_ap.loc[~df_ap['Receipt Number'].isna(), :]
            
            # 找出公司+PO Number在AP中出現的情況
            ap_po_key = df_ap.assign(
                key=df_ap['Company'] + '-' + df_ap['PO Number']
            ).groupby('key').Company.count().loc[lambda x: x >= 1].index
            
            # 找出公司+PR Number在AP中出現多次的情況
            ap_pr_key = df_ap.assign(
                key=df_ap['Company'] + '-' + df_ap['PO Number'].str.replace('PO', 'PR')
            ).groupby('key').Company.count().loc[lambda x: x > 1].index
            
            # 標記重複項
            df_pr['ap_invoice'] = np.where(df_pr['PR Line'].isin(ap_pr_key), 'AP_dup', pd.NA)
            df_po['ap_invoice'] = np.where(df_po['PO#'].isin(ap_po_key), 'AP_dup_or_1', pd.NA)
            
            self.logger.info(f"發現 {len(ap_pr_key)} 個重複的PR Key和 {len(ap_po_key)} 個重複的PO Key")
            return df_pr, df_po
            
        except Exception as e:
            self.logger.error(f"檢查AP中的重複項時出錯: {str(e)}", exc_info=True)
            raise
    
    def relocate_columns(self, df_pr: pd.DataFrame, df_po: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """重新定位列
        
        Args:
            df_pr: PR數據框
            df_po: PO數據框
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 重新定位了列的PR和PO數據框
        """
        try:
            self.logger.info("重新定位列")
            
            # 提取列
            col_pr_d = df_pr.pop('dup')
            col_pr_a = df_pr.pop('ap_invoice')
            col_po_a = df_po.pop('ap_invoice')
            
            # 重新插入列
            df_pr.insert(
                loc=df_pr.columns.get_loc('PR狀態') + 1,
                column='dup',
                value=col_pr_d
            )
            
            df_pr.insert(
                loc=df_pr.columns.get_loc('dup') + 1,
                column='ap_invoice',
                value=col_pr_a
            )
            
            df_po.insert(
                loc=df_po.columns.get_loc('PO狀態') + 1,
                column='ap_invoice',
                value=col_po_a
            )
            
            self.logger.info("成功重新定位列")
            return df_pr, df_po
            
        except Exception as e:
            self.logger.error(f"重新定位列時出錯: {str(e)}", exc_info=True)
            raise
    
    def save_files(self, df_pr: pd.DataFrame, df_po: pd.DataFrame) -> None:
        """保存文件
        
        Args:
            df_pr: PR數據框
            df_po: PO數據框
            
        Returns:
            None
        """
        try:
            self.logger.info("正在保存文件")
            
            try:
                df_pr.to_excel('PR Compare Result_hris.xlsx', index=False, encoding='utf-8-sig', engine='xlsxwriter')
                df_po.to_excel('PO Compare Result_hris.xlsx', index=False, encoding='utf-8-sig', engine='xlsxwriter')
                self.logger.info("成功保存帶編碼的文件")
            except TypeError:
                df_pr.to_excel('PR Compare Result_hris.xlsx', index=False, engine='xlsxwriter')
                df_po.to_excel('PO Compare Result_hris.xlsx', index=False, engine='xlsxwriter')
                self.logger.info("成功保存無編碼的文件")
            except Exception as err:
                self.logger.error(f"保存文件時發生未預期的錯誤: {str(err)}", exc_info=True)
                raise
                
        except Exception as e:
            self.logger.error(f"保存文件時出錯: {str(e)}", exc_info=True)
            raise


# 兼容舊版API的函數
def dup_inPO(df_pr, df_po):
    """檢查PO中的重複項"""
    return HRISDuplicateChecker().check_duplicates_in_po(df_pr, df_po)

def dup_inAP(df_pr, df_po, df_ap):
    """檢查AP中的重複項"""
    return HRISDuplicateChecker().check_duplicates_in_ap(df_pr, df_po, df_ap)

def relocate_cols(df_pr, df_po):
    """重新定位列"""
    return HRISDuplicateChecker().relocate_columns(df_pr, df_po)

def saving_files(df_pr, df_po):
    """保存文件"""
    HRISDuplicateChecker().save_files(df_pr, df_po)


# 自定義異常類
class UnexpError(Exception):
    def __init__(self, message):
        super().__init__(message)
