import pandas as pd
from pathlib import Path
from typing import List, Optional, Literal


def create_pivot_summary(
    df: pd.DataFrame,
    product_col: str,
    currency_col: str,
    amount_col: str,
    column_indices: List[int]
) -> pd.DataFrame:
    """
    建立產品代碼與幣別的 pivot table 摘要
    
    Parameters:
    -----------
    df : pd.DataFrame
        來源資料框
    product_col : str
        產品代碼欄位名稱
    currency_col : str
        幣別欄位名稱
    amount_col : str
        金額欄位名稱
    column_indices : List[int]
        要保留的欄位索引
        
    Returns:
    --------
    pd.DataFrame
        處理後的 pivot table
    """
    return (
        df[[product_col, currency_col, amount_col]]
        .assign(amt=lambda row: row[amount_col].fillna('0').astype('Float64'))
        .pivot_table(
            index=[product_col],
            columns=[currency_col],
            values='amt',
            aggfunc=['sum', 'count'],
            margins_name='Total',
            margins=True
        )
        .iloc[:, column_indices]
    )


def load_and_process_data(
    file_path: str,
    file_type: Literal['parquet', 'xlsx', 'csv'],
    columns: List[str],
    column_indices: List[int],
    filter_condition: Optional[str] = None
) -> pd.DataFrame:
    """
    載入並處理資料檔案
    
    Parameters:
    -----------
    file_path : str
        檔案路徑
    file_type : Literal['parquet', 'xlsx', 'csv']
        檔案類型
    columns : List[str]
        要讀取的欄位清單 [產品代碼, 幣別, 金額]
    column_indices : List[int]
        pivot table 要保留的欄位索引
    filter_condition : Optional[str]
        篩選條件，如 'SPX' 或 '~SPX'
        
    Returns:
    --------
    pd.DataFrame
        處理後的資料框
    """
    # 讀取檔案
    if file_type == 'parquet':
        df = pd.read_parquet(file_path)[columns]
    elif file_type == 'xlsx':
        df = pd.read_excel(file_path)[columns]
    else:  # csv
        df = pd.read_csv(file_path)[columns]
    
    # 應用篩選條件
    if filter_condition:
        product_col = columns[0]
        if filter_condition.startswith('~'):
            # 排除條件
            pattern = filter_condition[1:]
            df = df.loc[~df[product_col].str.contains(pattern, na=False)]
        else:
            # 包含條件
            df = df.loc[df[product_col].str.contains(filter_condition, na=False)]
    
    # 建立 pivot table
    return create_pivot_summary(
        df=df,
        product_col=columns[0],
        currency_col=columns[1],
        amount_col=columns[2],
        column_indices=column_indices
    )


def generate_data_shape_summary(
    config: dict,
    output_file: str = 'DataShape_Summary.xlsx'
) -> None:
    """
    產生資料完整性摘要報表
    
    Parameters:
    -----------
    config : dict
        資料來源配置字典
    output_file : str
        輸出檔案名稱
    """
    results = {}
    
    for sheet_name, params in config.items():
        print(f"處理 {sheet_name}...")
        results[sheet_name] = load_and_process_data(**params)
    
    # 寫入 Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in results.items():
            df.to_excel(writer, sheet_name=sheet_name)
    
    print(f"✓ 報表已產生: {output_file}")


# 使用範例
if __name__ == '__main__':
    # 定義欄位名稱
    COL_ORIGINAL = ['Product Code', 'Currency', 'Entry Amount']
    COL_STANDARDIZED = ['product_code', 'currency', 'entry_amount']
    
    # 配置所有資料來源
    data_config = {
        'raw_po': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\accrual_bot\checkpoints\SPX_PO_202601_after_SPXDataLoading\data.parquet',
            'file_type': 'parquet',
            'columns': COL_ORIGINAL,
            'column_indices': [0, 1, 3, 4],
        },
        'spx_po': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\accrual_bot\checkpoints\SPX_PO_202601_after_DataReformatting\data.parquet',
            'file_type': 'parquet',
            'columns': COL_STANDARDIZED,
            'column_indices': [0, 2],
        },
        'spt_po': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\accrual_bot\checkpoints\SPT_PO_202601_after_SPTPostProcessing\data.parquet',
            'file_type': 'parquet',
            'columns': COL_STANDARDIZED,
            'column_indices': [0, 1, 3, 4],
        },
        'raw_pr': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\resources\頂一下\202601\Original Data\202601_purchase_request_20260202_100413.csv',
            'file_type': 'csv',
            'columns': COL_ORIGINAL,
            'column_indices': [0, 1, 3, 4],
        },
        'spx_pr': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\resources\頂一下\202601\Original Data\202601_purchase_request_20260202_100413.csv',
            'file_type': 'csv',
            'columns': COL_ORIGINAL,
            'column_indices': [0, 2],
            'filter_condition': 'SPX',
        },
        'spt_pr': {
            'file_path': r'C:\SEA\Accrual\prpo_bot\resources\頂一下\202601\Original Data\202601_purchase_request_20260202_100413.csv',
            'file_type': 'csv',
            'columns': COL_ORIGINAL,
            'column_indices': [0, 1, 3, 4],
            'filter_condition': '~SPX',
        },
    }
    
    # 執行
    generate_data_shape_summary(data_config)