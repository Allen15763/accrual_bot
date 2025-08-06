"""
數據轉換器

提供完整的數據轉換功能，整合日期和格式轉換
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime

from .date_transformer import DateTransformer
from .format_transformer import FormatTransformer
from ...core.models.data_models import POData, PRData, EntityType, ProcessingType
from ...utils.logging import Logger


class DataTransformer:
    """數據轉換器主類別"""
    
    def __init__(self, entity_type: EntityType = EntityType.MOB):
        self.entity_type = entity_type
        self.logger = Logger().get_logger(__name__)
        
        # 初始化子轉換器
        self.date_transformer = DateTransformer()
        self.format_transformer = FormatTransformer()
        
        # 業務規則配置
        self.business_rules = self._get_business_rules()
    
    def _get_business_rules(self) -> Dict[str, Any]:
        """獲取業務規則配置"""
        base_rules = {
            'po_rules': {
                'required_fields': ['PO#', 'Line#', 'Account'],
                'numeric_fields': ['Entry Quantity', 'Billed Quantity', 'Entry Amount', 'Entry Billed Amount'],
                'date_fields': ['PO Date', 'Receipt Date'],
                'validation_rules': {
                    'Entry Quantity': {'min': 0},
                    'Entry Amount': {'min': 0},
                    'Account': {'length': 4, 'type': 'numeric_string'}
                }
            },
            'pr_rules': {
                'required_fields': ['PR#', 'Line#', 'Account'],
                'numeric_fields': ['Quantity', 'Amount'],
                'date_fields': ['PR Date'],
                'validation_rules': {
                    'Quantity': {'min': 0},
                    'Amount': {'min': 0},
                    'Account': {'length': 4, 'type': 'numeric_string'}
                }
            }
        }
        
        # 根據實體類型調整規則
        if self.entity_type == EntityType.SPX:
            base_rules['spx_specific'] = {
                'enable_closing_list': True,
                'require_gl_adjustment': True
            }
        
        return base_rules
    
    def transform_po_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """轉換PO數據
        
        Args:
            df: 原始PO數據DataFrame
            
        Returns:
            pd.DataFrame: 轉換後的DataFrame
        """
        try:
            self.logger.info(f"開始轉換PO數據，共 {len(df)} 筆記錄")
            df_transformed = df.copy()
            
            # 1. 清理基本欄位
            text_columns = ['PO#', 'Line#', 'PR#', 'Account', 'Department']
            for col in text_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.format_transformer.clean_text_data
                    )
            
            # 2. 標準化會計科目
            if 'Account' in df_transformed.columns:
                df_transformed['Account'] = df_transformed['Account'].apply(
                    self.format_transformer.normalize_account_code
                )
            
            # 3. 標準化部門名稱
            if 'Department' in df_transformed.columns:
                df_transformed['Department'] = df_transformed['Department'].apply(
                    self.format_transformer.standardize_department_name
                )
            
            # 4. 處理數值欄位
            numeric_columns = self.business_rules['po_rules']['numeric_fields']
            for col in numeric_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.format_transformer.clean_numeric_string
                    ).fillna(0)
            
            # 5. 處理日期欄位
            date_columns = self.business_rules['po_rules']['date_fields']
            for col in date_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.date_transformer.parse_date_string
                    )
            
            # 6. 應用業務邏輯
            df_transformed = self._apply_po_business_logic(df_transformed)
            
            self.logger.info(f"PO數據轉換完成，處理了 {len(df_transformed)} 筆記錄")
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"PO數據轉換失敗: {e}")
            raise
    
    def transform_pr_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """轉換PR數據
        
        Args:
            df: 原始PR數據DataFrame
            
        Returns:
            pd.DataFrame: 轉換後的DataFrame
        """
        try:
            self.logger.info(f"開始轉換PR數據，共 {len(df)} 筆記錄")
            df_transformed = df.copy()
            
            # 1. 清理基本欄位
            text_columns = ['PR#', 'Line#', 'Account', 'Department']
            for col in text_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.format_transformer.clean_text_data
                    )
            
            # 2. 標準化會計科目
            if 'Account' in df_transformed.columns:
                df_transformed['Account'] = df_transformed['Account'].apply(
                    self.format_transformer.normalize_account_code
                )
            
            # 3. 標準化部門名稱
            if 'Department' in df_transformed.columns:
                df_transformed['Department'] = df_transformed['Department'].apply(
                    self.format_transformer.standardize_department_name
                )
            
            # 4. 處理數值欄位
            numeric_columns = self.business_rules['pr_rules']['numeric_fields']
            for col in numeric_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.format_transformer.clean_numeric_string
                    ).fillna(0)
            
            # 5. 處理日期欄位
            date_columns = self.business_rules['pr_rules']['date_fields']
            for col in date_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(
                        self.date_transformer.parse_date_string
                    )
            
            # 6. 應用業務邏輯
            df_transformed = self._apply_pr_business_logic(df_transformed)
            
            self.logger.info(f"PR數據轉換完成，處理了 {len(df_transformed)} 筆記錄")
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"PR數據轉換失敗: {e}")
            raise
    
    def _apply_po_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用PO特有的業務邏輯
        
        Args:
            df: PO數據DataFrame
            
        Returns:
            pd.DataFrame: 應用業務邏輯後的DataFrame
        """
        df_result = df.copy()
        
        try:
            # 1. 添加狀態欄位
            if 'Closed For Invoice' in df_result.columns:
                df_result['是否結案'] = np.where(
                    df_result['Closed For Invoice'].astype(str) == '0', 
                    "未結案", 
                    "結案"
                )
            
            # 2. 計算結案差異數量
            if all(col in df_result.columns for col in ['是否結案', 'Entry Quantity', 'Billed Quantity']):
                df_result['結案是否有差異數量'] = np.where(
                    df_result['是否結案'] == '結案',
                    df_result['Entry Quantity'].astype(float) - df_result['Billed Quantity'].astype(float),
                    '未結案'
                )
            
            # 3. 檢查發票入帳
            if all(col in df_result.columns for col in ['Entry Billed Amount', 'Entry Amount']):
                df_result['Check with Entry Invoice'] = np.where(
                    df_result['Entry Billed Amount'].astype(float) > 0,
                    df_result['Entry Amount'].astype(float) - df_result['Entry Billed Amount'].astype(float),
                    '未入帳'
                )
            
            # 4. 生成組合欄位
            if all(col in df_result.columns for col in ['PR#', 'Line#']):
                df_result['PR Line'] = df_result['PR#'].astype(str) + '-' + df_result['Line#'].astype(str)
            
            if all(col in df_result.columns for col in ['PO#', 'Line#']):
                df_result['PO Line'] = df_result['PO#'].astype(str) + '-' + df_result['Line#'].astype(str)
            
            # 5. ERM邏輯處理
            df_result = self._apply_erm_logic(df_result)
            
        except Exception as e:
            self.logger.warning(f"應用PO業務邏輯時發生錯誤: {e}")
        
        return df_result
    
    def _apply_pr_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用PR特有的業務邏輯
        
        Args:
            df: PR數據DataFrame
            
        Returns:
            pd.DataFrame: 應用業務邏輯後的DataFrame
        """
        df_result = df.copy()
        
        try:
            # 1. 生成組合欄位
            if all(col in df_result.columns for col in ['PR#', 'Line#']):
                df_result['PR Line'] = df_result['PR#'].astype(str) + '-' + df_result['Line#'].astype(str)
            
            # 2. 處理狀態欄位
            if 'Status' in df_result.columns:
                df_result['狀態'] = df_result['Status'].apply(
                    self.format_transformer.clean_text_data
                )
            
        except Exception as e:
            self.logger.warning(f"應用PR業務邏輯時發生錯誤: {e}")
        
        return df_result
    
    def _apply_erm_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用ERM邏輯
        
        Args:
            df: 數據DataFrame
            
        Returns:
            pd.DataFrame: 應用ERM邏輯後的DataFrame
        """
        try:
            # 這裡實現ERM特有的業務邏輯
            # 根據實體類型和會計科目設定相關欄位
            
            if 'Account' in df.columns:
                # 判斷是否為FA帳戶
                fa_accounts = self._get_fa_accounts()
                df['是否FA帳戶'] = df['Account'].apply(
                    lambda x: '是' if str(x) in fa_accounts else '否'
                )
            
            # 根據實體類型應用特殊邏輯
            if self.entity_type == EntityType.SPX:
                df = self._apply_spx_specific_logic(df)
            
        except Exception as e:
            self.logger.warning(f"應用ERM邏輯時發生錯誤: {e}")
        
        return df
    
    def _apply_spx_specific_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """應用SPX特有邏輯"""
        # SPX特有的業務邏輯處理
        # 例如：關單清單處理、GL調整等
        return df
    
    def _get_fa_accounts(self) -> List[str]:
        """獲取FA帳戶列表"""
        # 根據實體類型返回對應的FA帳戶
        fa_mapping = {
            EntityType.MOB: ["1410", "1411", "1420", "1610", "1640", "1650"],
            EntityType.SPT: ["1410", "1420", "1610", "1640", "1650"],
            EntityType.SPX: ["1410", "1420", "1610", "1640", "1650"]
        }
        return fa_mapping.get(self.entity_type, [])
    
    def apply_business_rules(self, df: pd.DataFrame, 
                           processing_type: ProcessingType,
                           custom_rules: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """應用業務規則
        
        Args:
            df: 數據DataFrame
            processing_type: 處理類型
            custom_rules: 自定義規則
            
        Returns:
            pd.DataFrame: 應用規則後的DataFrame
        """
        if processing_type == ProcessingType.PO:
            return self.transform_po_data(df)
        elif processing_type == ProcessingType.PR:
            return self.transform_pr_data(df)
        else:
            self.logger.warning(f"未知的處理類型: {processing_type}")
            return df
    
    def validate_transformed_data(self, df: pd.DataFrame, 
                                processing_type: ProcessingType) -> Dict[str, Any]:
        """驗證轉換後的數據
        
        Args:
            df: 轉換後的DataFrame
            processing_type: 處理類型
            
        Returns:
            Dict[str, Any]: 驗證結果
        """
        rules = self.business_rules[f"{processing_type.value.lower()}_rules"]
        
        validation_result = {
            'total_records': len(df),
            'valid_records': 0,
            'errors': [],
            'warnings': []
        }
        
        # 檢查必填欄位
        for field in rules['required_fields']:
            if field not in df.columns:
                validation_result['errors'].append(f"缺少必填欄位: {field}")
            else:
                null_count = df[field].isna().sum()
                if null_count > 0:
                    validation_result['warnings'].append(f"欄位 {field} 有 {null_count} 筆空值")
        
        # 檢查數值欄位
        numeric_validation = self.format_transformer.validate_numeric_columns(
            df, rules['numeric_fields']
        )
        
        for col, stats in numeric_validation.items():
            if stats['invalid_count'] > 0:
                validation_result['warnings'].append(
                    f"數值欄位 {col} 有 {stats['invalid_count']} 筆無效值"
                )
        
        validation_result['valid_records'] = len(df) - len(validation_result['errors'])
        
        return validation_result


# 便捷函數
def transform_po_data(df: pd.DataFrame, entity_type: EntityType = EntityType.MOB) -> pd.DataFrame:
    """轉換PO數據的便捷函數"""
    transformer = DataTransformer(entity_type)
    return transformer.transform_po_data(df)


def transform_pr_data(df: pd.DataFrame, entity_type: EntityType = EntityType.MOB) -> pd.DataFrame:
    """轉換PR數據的便捷函數"""
    transformer = DataTransformer(entity_type)
    return transformer.transform_pr_data(df)


def apply_business_rules(df: pd.DataFrame, 
                        processing_type: ProcessingType,
                        entity_type: EntityType = EntityType.MOB) -> pd.DataFrame:
    """應用業務規則的便捷函數"""
    transformer = DataTransformer(entity_type)
    return transformer.apply_business_rules(df, processing_type)
