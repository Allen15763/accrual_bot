"""基於真實資料結構的綜合測試資料產生器"""
import pandas as pd
import numpy as np
from typing import Optional


def create_spx_po_df(n_rows: int = 10) -> pd.DataFrame:
    """產生 SPX PO 資料（對應 purchase_order CSV 欄位結構）"""
    return pd.DataFrame({
        'PO#': [f'PO{1000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'GL#': [f'{100000+i}' for i in range(n_rows)],
        'Product Code': [f'PROD{i:03d}' for i in range(n_rows)],
        'Item Description': [f'SPX Item {i} 2025/10-2025/11' for i in range(n_rows)],
        'Supplier Name': ['Test Supplier'] * n_rows,
        'Entry Quantity': np.random.randint(10, 500, n_rows).astype(str),
        'Received Quantity': np.random.randint(0, 400, n_rows).astype(str),
        'Billed Quantity': np.random.randint(0, 400, n_rows).astype(str),
        'Unit Price': np.random.uniform(100, 10000, n_rows).round(2).astype(str),
        'Entry Amount': np.random.uniform(1000, 100000, n_rows).round(2).astype(str),
        'Entry Billed Amount': np.random.uniform(0, 100000, n_rows).round(2).astype(str),
        'Entry Prepay Amount': ['0'] * n_rows,
        'Currency': ['TWD'] * n_rows,
        'Expected Received Date': ['2025/12/31'] * n_rows,
        'Project Number': [f'PRJ{i:03d}' for i in range(n_rows)],
        'Remarks': [pd.NA] * n_rows,
    })


def create_spx_pr_df(n_rows: int = 10) -> pd.DataFrame:
    """產生 SPX PR 資料（對應 purchase_request CSV 欄位結構）"""
    return pd.DataFrame({
        'PR#': [f'PR{2000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'GL#': [f'{200000+i}' for i in range(n_rows)],
        'Product Code': [f'PROD{i:03d}' for i in range(n_rows)],
        'Item Description': [f'SPX PR Item {i}' for i in range(n_rows)],
        'Entry Quantity': np.random.randint(1, 100, n_rows).astype(str),
        'Billed Quantity': np.random.randint(0, 100, n_rows).astype(str),
        'Unit Price': np.random.uniform(50, 5000, n_rows).round(2).astype(str),
        'Entry Amount': np.random.uniform(100, 50000, n_rows).round(2).astype(str),
        'Entry Billed Amount': np.random.uniform(0, 50000, n_rows).round(2).astype(str),
        'Currency': ['TWD'] * n_rows,
    })


def create_spt_po_df(n_rows: int = 10) -> pd.DataFrame:
    """產生 SPT PO 資料（對應 PO_PQ Excel 欄位結構）"""
    return pd.DataFrame({
        'PO#': [f'PO{3000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'GL#': [f'{300000+i}' for i in range(n_rows)],
        'Product Code': [f'SPT{i:03d}' for i in range(n_rows)],
        'Item Description': [f'SPT Item {i} 2025/11' for i in range(n_rows)],
        'Supplier Name': ['SPT Supplier'] * n_rows,
        'Entry Quantity': np.random.randint(10, 300, n_rows).astype(str),
        'Received Quantity': np.random.randint(0, 300, n_rows).astype(str),
        'Billed Quantity': np.random.randint(0, 300, n_rows).astype(str),
        'Unit Price': np.random.uniform(100, 5000, n_rows).round(2).astype(str),
        'Entry Amount': np.random.uniform(1000, 50000, n_rows).round(2).astype(str),
        'Entry Billed Amount': np.random.uniform(0, 50000, n_rows).round(2).astype(str),
        'Entry Prepay Amount': ['0'] * n_rows,
        'Currency': ['TWD'] * n_rows,
        'Expected Received Date': ['2025/11/30'] * n_rows,
        'Department': [f'{i:03d}' for i in range(n_rows)],
    })


def create_spt_pr_df(n_rows: int = 10) -> pd.DataFrame:
    """產生 SPT PR 資料"""
    return pd.DataFrame({
        'PR#': [f'PR{4000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'GL#': [f'{400000+i}' for i in range(n_rows)],
        'Product Code': [f'SPT{i:03d}' for i in range(n_rows)],
        'Item Description': [f'SPT PR Item {i}' for i in range(n_rows)],
        'Entry Quantity': np.random.randint(1, 100, n_rows).astype(str),
        'Billed Quantity': np.random.randint(0, 100, n_rows).astype(str),
        'Unit Price': np.random.uniform(50, 5000, n_rows).round(2).astype(str),
        'Entry Amount': np.random.uniform(100, 50000, n_rows).round(2).astype(str),
        'Entry Billed Amount': np.random.uniform(0, 50000, n_rows).round(2).astype(str),
        'Currency': ['TWD'] * n_rows,
    })


def create_previous_workpaper_df(
    entity: str = 'SPX', proc_type: str = 'PO', n_rows: int = 5
) -> pd.DataFrame:
    """產生前期底稿資料"""
    id_col = 'PO#' if proc_type == 'PO' else 'PR#'
    status_col = 'PO狀態' if proc_type == 'PO' else 'PR狀態'
    prefix = 'PO' if proc_type == 'PO' else 'PR'

    return pd.DataFrame({
        id_col: [f'{prefix}{5000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'GL#': [f'{500000+i}' for i in range(n_rows)],
        status_col: ['已完成', '未完成', '已入帳', pd.NA, '已完成'][:n_rows],
        'Remarked by Procurement': [pd.NA] * n_rows,
        'Remarked by 上月 FN': [pd.NA] * n_rows,
        'Account code': [f'{500000+i}' for i in range(n_rows)],
        'Account Name': [f'Account {i}' for i in range(n_rows)],
        'Reviewer': ['Reviewer A'] * n_rows,
    })


def create_ap_invoice_df(n_rows: int = 5) -> pd.DataFrame:
    """產生 AP Invoice 資料"""
    return pd.DataFrame({
        'PO Number': [f'PO{6000+i}' for i in range(n_rows)],
        'PO Line': [str(i+1) for i in range(n_rows)],
        'Invoice Number': [f'INV{i:05d}' for i in range(n_rows)],
        'Invoice Amount': np.random.uniform(1000, 50000, n_rows).round(2),
        'Invoice Date': ['2025/12/15'] * n_rows,
        'Matched': ['Y'] * n_rows,
    })


def create_closing_list_df(n_rows: int = 3) -> pd.DataFrame:
    """產生 SPX 關單清單資料"""
    return pd.DataFrame({
        'PO#': [f'PO{7000+i}' for i in range(n_rows)],
        'Line#': [str(i+1) for i in range(n_rows)],
        'Status': ['待關單', '已關單', '待關單'][:n_rows],
        'Remarks': ['Close reason'] * n_rows,
    })


def create_ppe_contract_df(n_rows: int = 5) -> pd.DataFrame:
    """產生 PPE 合約檔案清單資料"""
    return pd.DataFrame({
        'Contract No': [f'CTR{i:04d}' for i in range(n_rows)],
        'PO#': [f'PO{8000+i}' for i in range(n_rows)],
        'Contract Start Date': ['2024/01/01'] * n_rows,
        'Contract End Date': ['2025/12/31'] * n_rows,
        'Monthly Amount': np.random.uniform(5000, 50000, n_rows).round(2),
        'Asset Type': ['Locker', 'Kiosk', 'Locker', 'Kiosk', 'Locker'][:n_rows],
    })


def create_erm_scenario(scenario: str) -> pd.DataFrame:
    """
    建立特定 ERM 評估情境

    Args:
        scenario: 情境名稱
            - 'all_completed': 所有項目已完成
            - 'partial_receipt': 部分收貨
            - 'date_mismatch': 日期格式錯誤
            - 'mixed_currency': 混合幣別
            - 'fa_accounts': 固定資產科目
            - 'prepayment': 預付款情境
    """
    base = {
        'GL#': ['100000', '100001', '100002', '100003'],
        'Expected Received Month_轉換格式': [202512, 202512, 202512, 202512],
        'Entry Quantity': [100, 200, 150, 50],
        'Received Quantity': [100, 200, 150, 50],
        'Billed Quantity': [100, 200, 150, 50],
        'Unit Price': [100.0, 200.0, 150.0, 500.0],
        'Entry Amount': [10000, 40000, 22500, 25000],
        'Entry Billed Amount': [10000, 40000, 22500, 25000],
        'Entry Prepay Amount': ['0', '0', '0', '0'],
        'Item Description': ['Item 1', 'Item 2', 'Item 3', 'Item 4'],
        'YMs of Item Description': ['2025/10-2025/11', '2025/11', '2025/12', '2025/10'],
        'Remarked by Procurement': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Remarked by 上月 FN': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Currency': ['TWD', 'TWD', 'TWD', 'TWD'],
        'Product Code': ['PROD001', 'PROD002', 'PROD003', 'PROD004'],
        'PO狀態': [pd.NA, pd.NA, pd.NA, pd.NA],
        'Account code': ['100000', '100001', '100002', '100003'],
        'Department': ['001', '002', '003', '004'],
    }

    df = pd.DataFrame(base)

    if scenario == 'all_completed':
        # 全部已完成：Entry Qty == Billed Qty == Received Qty
        pass  # base data already set this way

    elif scenario == 'partial_receipt':
        df['Received Quantity'] = [80, 150, 100, 30]
        df['Billed Quantity'] = [60, 120, 80, 20]
        df['Entry Billed Amount'] = [6000, 24000, 12000, 10000]

    elif scenario == 'date_mismatch':
        df['YMs of Item Description'] = ['格式錯誤', '格式錯誤', '2025/12', '2025/10']

    elif scenario == 'mixed_currency':
        df['Currency'] = ['TWD', 'USD', 'TWD', 'JPY']

    elif scenario == 'fa_accounts':
        df['GL#'] = ['199999', '151101', '151201', '100000']
        df['Account code'] = ['199999', '151101', '151201', '100000']

    elif scenario == 'prepayment':
        df['Entry Prepay Amount'] = ['5000', '10000', '0', '0']

    return df
