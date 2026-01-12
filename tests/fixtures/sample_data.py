"""測試數據生成器"""
import pandas as pd
from typing import Dict


def create_minimal_loading_df() -> pd.DataFrame:
    """為 BaseLoadingStep 創建最小化測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Item Description': ['Item A 2025/10-2025/11', 'Item B 2025/11', 'Item C'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003'],
        'Entry Quantity': [10, 20, 30],
        'Billed Quantity': [5, 15, 25],
        'Received Quantity': [8, 18, 28],
        'Unit Price': [100.0, 200.0, 300.0],
        'Entry Amount': [1000, 4000, 9000],
        'Entry Billed Amount': [500, 3000, 7500],
        'Currency': ['TWD', 'TWD', 'USD']
    })


def create_minimal_erm_df() -> pd.DataFrame:
    """為 BaseERMEvaluationStep 創建最小化測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002'],
        'Expected Received Month_轉換格式': [202512, 202512, 202601],
        'YMs of Item Description': ['2025/10-2025/11', '2025/11-2025/12', '格式錯誤'],
        'Entry Quantity': [100, 200, 150],
        'Received Quantity': [80, 150, 100],
        'Billed Quantity': [60, 120, 80],
        'Entry Amount': [10000, 20000, 15000],
        'Entry Billed Amount': [6000, 14400, 12000],
        'Entry Prepay Amount': ['0', '0', '0'],
        'Item Description': ['Test Item 1', 'Test Item 2', 'Test Item 3'],
        'Remarked by Procurement': [pd.NA, 'Remark', pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA],
        'Unit Price': [100.0, 100.0, 100.0],
        'Currency': ['TWD', 'TWD', 'USD'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003'],
        'PO狀態': [pd.NA, pd.NA, pd.NA],
        'Account code': ['100000', '100001', '100002'],
        'Department': ['001', '002', '003']
    })


def create_reference_account_df() -> pd.DataFrame:
    """參考科目 DataFrame"""
    return pd.DataFrame({
        'Account': ['100000', '100001', '100002', '199999'],
        'Account Desc': ['Cash', 'Receivables', 'Inventory', 'FA']
    })


def create_reference_liability_df() -> pd.DataFrame:
    """負債科目 DataFrame"""
    return pd.DataFrame({
        'Account': ['100000', '100001', '100002'],
        'Liability': ['111111', '111112', '111113']
    })


def create_complex_erm_scenario_df() -> pd.DataFrame:
    """複雜 ERM 場景測試數據"""
    return pd.DataFrame({
        'GL#': ['100000', '100001', '100002', '100003'],
        'Expected Received Month_轉換格式': [202512, 202512, 202512, 202512],
        'YMs of Item Description': ['2025/10-2025/11', '2025/11-2025/12', '格式錯誤', '2025/12'],
        'PO狀態': ['已完成', pd.NA, pd.NA, '未完成'],
        'Entry Quantity': [100, 100, 100, 100],
        'Billed Quantity': [100, 50, 100, 75],
        'Received Quantity': [100, 80, 100, 80],
        'Unit Price': [100.0, 100.0, 100.0, 100.0],
        'Entry Amount': [10000, 10000, 10000, 10000],
        'Entry Billed Amount': [10000, 5000, 10000, 7500],
        'Entry Prepay Amount': ['0', '0', '0', '0'],
        'Item Description': ['Item 1', 'Item 2', 'Item 3', 'Item 4'],
        'Remarked by Procurement': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Currency': ['TWD', 'TWD', 'TWD', 'TWD'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003', 'PROD004'],
        'Account code': ['100000', '100001', '100002', '100003'],
        'Department': ['001', '002', '003', '004']
    })
