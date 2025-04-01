import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional, Union, Any
from datetime import datetime

# 導入優化後的工具類
from utils import Utils, Logger, ConfigManager, DataImporter


class BaseDataProcessor:
    """處理數據的基類，抽象出PR和PO處理器的共同功能"""
    
    def __init__(self, entity_type: str = "MOB"):
        """
        初始化數據處理器
        
        Args:
            entity_type: 實體類型，'MOB'或'SPT'
        """
        self.entity_type = entity_type
        self.logger = Logger().get_logger(__name__)
        self.config = ConfigManager()
        self.importer = DataImporter()
        
        # 從配置中加載固定值
        self.fa_accounts = self._get_fa_accounts()
    
    def _get_fa_accounts(self) -> List[str]:
        """從配置獲取FA帳戶列表"""
        entity_key = self.entity_type.lower()
        fa_accounts = self.config.get_list('FA_ACCOUNTS', entity_key)
        return fa_accounts
    
    def _clean_nan_values(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """清理DataFrame中的nan值
        
        Args:
            df: 要處理的DataFrame
            columns: 要清理nan值的列名列表
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        for col in columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('nan', '')
        return df
    
    def _reformat_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化日期字段
        
        Args:
            df: 要處理的DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 格式化提交日期
            if 'Submission Date' in df.columns:
                df['Submission Date'] = pd.to_datetime(
                    df['Submission Date'], 
                    errors='coerce', 
                    format='%d-%b-%y'
                ).astype(str)
            
            # 格式化預期接收月份
            if 'Expected Receive Month' in df.columns:
                df['Expected Receive Month'] = pd.to_datetime(
                    df['Expected Receive Month'], 
                    errors='coerce', 
                    format='%b-%y'
                ).astype(str)
            
            # 格式化創建日期
            create_date_col = 'PR Create Date' if 'PR Create Date' in df.columns else 'PO Create Date'
            if create_date_col in df.columns:
                df[create_date_col] = pd.to_datetime(df[create_date_col]).apply(
                    lambda x: datetime.strftime(x, '%Y/%m/%d') if pd.notna(x) else ''
                )
            
            return df
        except Exception as e:
            self.logger.error(f"格式化日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("日期格式化時出錯")
    
    def _format_numeric_columns(self, df: pd.DataFrame, int_cols: List[str], float_cols: List[str]) -> pd.DataFrame:
        """格式化數值列，包括千分位
        
        Args:
            df: 要處理的DataFrame
            int_cols: 整數列名列表
            float_cols: 浮點數列名列表
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 處理整數列
            for col in int_cols:
                if col in df.columns:
                    df[col] = df[col].fillna('0')
                    df[col] = round(df[col].astype(float), 0).astype(int)
                    df[col] = df[col].apply(lambda x: format(int(x), ','))
            
            # 處理浮點數列
            for col in float_cols:
                if col in df.columns:
                    df[col] = df[col].fillna('0')
                    df[col] = df[col].astype(float)
                    df[col] = df[col].apply(lambda x: format(float(x), ','))
            
            return df
        except Exception as e:
            self.logger.error(f"格式化數值列時出錯: {str(e)}", exc_info=True)
            raise ValueError("數值格式化時出錯")
    
    def parse_date_from_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """從描述欄位解析日期範圍
        
        Args:
            df: 包含Item Description的DataFrame
            
        Returns:
            pd.DataFrame: 添加了解析結果的DataFrame
        """
        try:
            # 定義正規表達式模式
            pt_YM = self.config.get('GENERAL', 'pt_YM')  # YYYY/MM
            pt_YMD = self.config.get('GENERAL', 'pt_YMD')  # YYYY/MM/DD
            pt_YMtoYM = self.config.get('GENERAL', 'pt_YMtoYM')  # YYYY/MM-YYYY/MM
            pt_YMDtoYMD = self.config.get('GENERAL', 'pt_YMDtoYMD')  # YYYY/MM/DD-YYYY/MM/DD
            pt_YMYMD = f'({pt_YM}|{pt_YMD})'  # 將YM&YMD的正規表示式彙總
            
            # 將Expected Receive Month轉換為數值格式以便比較
            df['Expected Received Month_轉換格式'] = pd.to_datetime(
                df['Expected Receive Month'], 
                format='%b-%y'
            ).dt.strftime('%Y%m').fillna(0).astype('int32')
            
            # 解析Item Description中的日期範圍
            col_desc = 'Item Description'
            conditions = [
                # 如果匹配單一日期格式（年月或年月日）
                (df[col_desc].str.match(pat=pt_YMYMD)),
                # 如果匹配年月-年月範圍格式
                (df[col_desc].str.match(pat=pt_YMtoYM)),
                # 如果匹配年月日-年月日範圍格式
                (df[col_desc].str.match(pat=pt_YMDtoYMD))
            ]
            
            choices = [
                # 單一日期處理：使用相同的日期作為開始和結束
                (df[col_desc].str[:7] + ',' + df[col_desc].str[:7]).str.replace('/', '', regex=False),
                # 年月-年月處理：提取開始和結束日期
                (df[col_desc].str[:7] + ',' + df[col_desc].str[8:15]).str.replace('/', '', regex=False),
                # 年月日-年月日處理：提取開始和結束日期（只取年月部分）
                (df[col_desc].str[:7] + ',' + df[col_desc].str[11:18]).str.replace('/', '', regex=False)
            ]
            
            # 使用numpy.select進行條件選擇
            df['YMs of Item Description'] = np.select(conditions, choices, default='100001,100002')
            
            return df
        except Exception as e:
            self.logger.error(f"解析描述中的日期時出錯: {str(e)}", exc_info=True)
            raise ValueError("解析日期時出錯")
    
    def evaluate_status_based_on_dates(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """根據日期範圍評估狀態
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態' 或 'PO狀態')
            
        Returns:
            pd.DataFrame: 更新了狀態的DataFrame
        """
        try:
            # 確保檔案日期和已知狀態存在
            if 'YMs of Item Description' not in df.columns or 'Expected Received Month_轉換格式' not in df.columns:
                df = self.parse_date_from_description(df)
            
            # 定義條件邏輯
            na_mask = df[status_col].isna() | (df[status_col] == 'nan') | (df[status_col] == '')
            conditions = [
                # 條件1：格式錯誤
                (df['YMs of Item Description'] == '100001,100002') & na_mask,
                
                # 條件2：已完成（日期在範圍內且預期接收月已過）
                ((df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) & (df['Expected Received Month_轉換格式'] <= df['檔案日期'])) & na_mask,
                
                # 條件3：未完成（日期在範圍內但預期接收月尚未到）
                ((df['Expected Received Month_轉換格式'].between(
                    df['YMs of Item Description'].str[:6].astype('int32'),
                    df['YMs of Item Description'].str[7:].astype('int32'),
                    inclusive='both'
                )) & (df['Expected Received Month_轉換格式'] > df['檔案日期'])) & na_mask
            ]
            
            choices = ['格式錯誤', '已完成', '未完成']
            
            # 只更新空值
            # mask = df[status_col].isna() | (df[status_col] == 'nan') | (df[status_col] == '')
            df.loc[:, status_col] = np.select(conditions, choices, default=df.loc[:, status_col])
            df.loc[:, status_col] = df.loc[:, status_col].fillna('error(Description Period is out of ERM)')
            
            return df
        except Exception as e:
            self.logger.error(f"根據日期評估狀態時出錯: {str(e)}", exc_info=True)
            raise ValueError("根據日期評估狀態時出錯")
    
    def update_estimation_based_on_status(self, df: pd.DataFrame, status_col: str) -> pd.DataFrame:
        """根據狀態更新估計入帳標識
        
        Args:
            df: 要處理的DataFrame
            status_col: 狀態列名 ('PR狀態' 或 'PO狀態')
            
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
            if status_col == 'PO狀態':
                df.loc[df[status_col] == '待關單', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '已入帳', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '已完成ERM', '是否估計入帳'] = 'Y'
                df.loc[df[status_col] == '未完成ERM', '是否估計入帳'] = 'N'
            elif status_col == 'PR狀態':
                df.loc[df[status_col] == '待關單', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == 'Payroll', '是否估計入帳'] = 'N'
                df.loc[df[status_col] == '不預估', '是否估計入帳'] = 'N'
            
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
    
    def judge_ac_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """判斷科目代碼
        
        Args:
            df: 要處理的DataFrame
            
        Returns:
            pd.DataFrame: 更新了科目代碼的DataFrame
        """
        try:
            # 設置Account code
            df['Account code'] = np.where(df['是否估計入帳'] == 'Y', df['GL#'], np.nan)
            
            # 設置是否為FA
            df['是否為FA'] = np.where(df['GL#'].isin(self.fa_accounts), 'Y', pd.NA)
            
            return df
        except Exception as e:
            self.logger.error(f"判斷科目代碼時出錯: {str(e)}", exc_info=True)
            raise ValueError("判斷科目代碼時出錯")
        
    def convert_dep_code(self, df: pd.DataFrame) -> pd.Series:
        # Create a new column or series for the result
        df_ = df.copy()
        df_['result'] = ''
        
        # Condition 1: 是否估計入帳 is "Y" AND Account code starts with 1, 2, or 9
        condition1 = (df_['是否估計入帳'] == 'Y') & (
            df_['Account code'].str[0].isin(['1', '2', '9'])
        )
        
        # Condition 2: 是否估計入帳 is "Y" AND Account code does NOT start with 5 or 4
        condition2 = (df_['是否估計入帳'] == 'Y') & (
            ~df_['Account code'].str[0].isin(['5', '4'])
        )
        
        # Condition 3: 是否估計入帳 is "Y" AND Account code starts with 5 or 4
        condition3 = (df_['是否估計入帳'] == 'Y') & (
            df_['Account code'].str[0].isin(['5', '4'])
        )
        
        # Apply the conditions in the same order as the original formula
        df_.loc[condition1, 'result'] = '000'
        df_.loc[condition2 & ~condition1, 'result'] = df_.loc[condition2 & ~condition1, 'Department'].str[:3]
        df_.loc[condition3, 'result'] = '000'
        
        return df_['result'].values
    
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
            
            # 設置產品代碼
            df['Product code'] = np.where(df['是否估計入帳'] == 'Y', df['Product Code'], pd.NA)
            need_book = (df['是否估計入帳'].eq('Y')) & (df['Product code_c'].isna())
            df['Product code_c'] = np.where(need_book, df['Product Code'], pd.NA)
            
            # 設置會計代碼
            is_income_expense = False
            if 'Account code' in df.columns:
                is_income_expense = df['Account code'].str.match(r'^[4-6]')
            
            df['Region_c'] = np.where(
                (df['是否估計入帳'] == 'Y') & is_income_expense,
                df['Region'],
                np.where(df['是否估計入帳'] == 'Y', '000', pd.NA)
            )
            
            # 設置部門; SPT篩選費用類科目, MOB擷取原始字段
            if self.entity_type == 'SPT':
                df['Dep.'] = self.convert_dep_code(df)
            else:
                df['Dep.'] = np.where(df['是否估計入帳'].eq("Y"), df['Department'].str[:3], pd.NA)
            
            # 設置幣別
            df['Currency_c'] = np.where(df['是否估計入帳'] == 'Y', df['Currency'], pd.NA)
            
            # 設置應計金額
            if 'PO#' in df.columns:  # 判斷是否為PO數據
                df['Accr. Amount'] = np.where(
                    df['是否估計入帳'] == 'Y',
                    df['Entry Amount'].astype(float) - df['Entry Billed Amount'].astype(float),
                    pd.NA
                )
            else:  # PR數據
                df['Accr. Amount'] = np.where(df['是否估計入帳'] == 'Y', df['Entry Amount'], pd.NA)
            
            # 設置負債科目
            df['Liability'] = pd.merge(
                df, ref_liability, how='left',
                left_on='Account code', right_on='Account'
            ).loc[:, 'Liability_y']
            
            # 設置產品代碼檢查
            if 'Product code' in df.columns and 'Project' in df.columns:
                product_match = False
                try:
                    product_match = df['Project'].str.findall(r'^(\w+(?:))').apply(
                        lambda x: x[0] if len(x) > 0 else ''
                    ) == df['Product code']
                except Exception as e:
                    self.logger.error(f"設置產品代碼檢查時出錯: {str(e)}", exc_info=True)
                    raise ValueError("設置產品代碼檢查時出錯")
                
                df['PR Product Code Check'] = np.where(
                    df['Product code'].notnull(),
                    np.where(product_match, 'good', 'bad'),
                    pd.NA
                )
            
            return df.drop('Product code', axis=1)
        except Exception as e:
            self.logger.error(f"判斷欄位值時出錯: {str(e)}", exc_info=True)
            raise ValueError("判斷欄位值時出錯")
    
    def export_file(self, df: pd.DataFrame, date: int, file_prefix: str) -> None:
        """導出文件
        
        Args:
            df: 要導出的DataFrame
            date: 日期值
            file_prefix: 文件前綴
            
        Returns:
            None
        """
        try:
            df = df.replace('<NA>', np.nan)
            file_name = f"{date}-{file_prefix} Compare Result.xlsx"
            self.logger.info(f"正在導出文件: {file_name}")
            
            try:
                df.to_excel(file_name, index=False, encoding='utf-8-sig', engine='xlsxwriter')
                self.logger.info(f"成功導出文件: {file_name}")
            except Exception as e:
                df.to_excel(file_name, index=False, engine='xlsxwriter')
                self.logger.info(f"成功導出文件(無encoding): {file_name}")
                
        except Exception as e:
            self.logger.error(f"導出文件時出錯: {str(e)}", exc_info=True)
            raise ValueError("導出文件時出錯")
        
    def get_mapping_dict(self, df: pd.DataFrame, key_col: str, column: str) -> Dict[str, Any]:
        """獲取映射字典"""
        mask = (~df[column].isna())
        necessary_columns = [key_col, column]
        return df.loc[mask, necessary_columns].set_index(key_col).to_dict()[column]
