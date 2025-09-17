"""
DataSources 使用範例
展示如何在實際專案中使用數據源模組
"""

import asyncio
import pandas as pd
from pathlib import Path
import sys

# 添加專案路徑
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from core.datasources import (
    DataSourceFactory, 
    DataSourceConfig, 
    DataSourceType,
    DuckDBSource,
    ExcelSource
)


async def example_1_basic_usage():
    """範例1: 基本使用 - 讀取現有的PR/PO檔案"""
    print("\n=== 範例1: 基本使用 ===")
    
    # 從Excel檔案讀取
    po_file = r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\202503_purchase_order_20250204_151523.csv"
    
    # 方法1: 使用工廠自動判斷類型
    source = DataSourceFactory.create_from_file(po_file)
    
    # 讀取數據
    df = await source.read()
    print(f"讀取到 {len(df)} 筆PO資料")
    print(f"欄位: {df.columns.tolist()[:5]}...")  # 顯示前5個欄位
    
    # 篩選數據（使用pandas query語法）
    filtered_df = await source.read(query="Amount > 10000")
    print(f"金額大於10000的記錄: {len(filtered_df)} 筆")
    
    return df


async def example_2_excel_to_duckdb():
    """範例2: 將Excel資料遷移到DuckDB以提升查詢效能"""
    print("\n=== 範例2: Excel遷移到DuckDB ===")
    
    # 假設有多個Excel檔案需要整合
    excel_files = {
        'po_data': r"C:\SEA\Accrual\prpo_bot\resources\test_po.xlsx",
        'procurement': r"C:\SEA\Accrual\prpo_bot\resources\test_procurement.xlsx"
    }
    
    # 創建DuckDB數據庫
    db_source = DuckDBSource.create_file_db('prpo_data.db')
    
    # 模擬數據（實際使用時替換為真實檔案）
    test_po = pd.DataFrame({
        'PO_number': [f'PO{i:04d}' for i in range(1, 101)],
        'Amount': [i * 1000 for i in range(1, 101)],
        'Status': ['Pending'] * 50 + ['Approved'] * 50
    })
    
    # 寫入DuckDB
    await db_source.write(test_po, table_name='purchase_orders')
    
    # 使用SQL查詢
    result = await db_source.read("""
        SELECT Status, 
               COUNT(*) as count,
               SUM(Amount) as total_amount
        FROM purchase_orders
        GROUP BY Status
    """)
    
    print("統計結果:")
    print(result)
    
    # 創建索引以加速查詢
    await db_source.create_index('purchase_orders', 'idx_po_num', ['PO_number'])
    
    await db_source.close()
    return result


async def example_3_pipeline_integration():
    """範例3: 與Pipeline整合使用"""
    print("\n=== 範例3: Pipeline整合 ===")
    
    # 配置多個數據源
    configs = {
        'main_data': DataSourceConfig(
            source_type=DataSourceType.DUCKDB,
            connection_params={
                'db_path': ':memory:',
                'table_name': 'po_raw'
            }
        ),
        'procurement': DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={
                'file_path': 'procurement_workpaper.parquet'
            },
            cache_enabled=True  # 啟用快取
        ),
        'accounting': DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={
                'file_path': 'accounting_workpaper.parquet'
            }
        )
    }
    
    # 創建數據源
    sources = {}
    for name, config in configs.items():
        if name == 'main_data':
            sources[name] = DataSourceFactory.create(config)
        # 其他數據源需要實際檔案，這裡跳過
    
    # 準備測試數據
    test_data = pd.DataFrame({
        'PO_number': ['PO001', 'PO002', 'PO003'],
        'GL': ['622101', '199999', '511101'],
        'Amount': [10000, 20000, 15000],
        'Expected Receive Month': ['Jan-25', 'Feb-25', 'Mar-25']
    })
    
    # 寫入主數據源
    await sources['main_data'].write(test_data, table_name='po_raw')
    
    # 從主數據源讀取並處理
    po_data = await sources['main_data'].read("SELECT * FROM po_raw")
    
    print(f"處理 {len(po_data)} 筆資料")
    print("資料預覽:")
    print(po_data)
    
    # 清理
    await sources['main_data'].close()
    
    return po_data


async def example_4_spx_specific():
    """範例4: SPX實體特定的數據處理"""
    print("\n=== 範例4: SPX特定處理 ===")
    
    # 創建DuckDB用於SPX數據
    db = DuckDBSource.create_memory_db()
    
    # 模擬SPX PO數據
    spx_po_data = pd.DataFrame({
        'PO_number': [f'SPX-PO{i:04d}' for i in range(1, 21)],
        'PO Supplier': ['益欣資訊股份有限公司'] * 10 + ['掌櫃智能股份有限公司'] * 10,
        'Item Description': ['繳費機訂金'] * 5 + ['智取櫃設備'] * 5 + ['智能櫃維護'] * 9 + ['門市租金'] * 1,
        'GL': ['199999'] * 5 + ['151101'] * 15,
        'Amount': [50000] * 10 + [30000] * 10,
        'Company': ['SPXTW'] * 20,
        'PO狀態': [''] * 20
    })
    
    # 寫入數據
    await db.write(spx_po_data, table_name='spx_po')
    
    # SPX特定查詢：找出需要驗收的資產
    asset_query = """
        SELECT * FROM spx_po
        WHERE "PO Supplier" IN ('益欣資訊股份有限公司', '掌櫃智能股份有限公司')
          AND "GL" != '199999'
    """
    
    assets_to_validate = await db.read(asset_query)
    print(f"需要驗收的資產: {len(assets_to_validate)} 筆")
    
    # 租金相關查詢
    rent_query = """
        SELECT * FROM spx_po
        WHERE "GL" = '622101'
           OR "Item Description" LIKE '%租金%'
    """
    
    rent_items = await db.read(rent_query)
    print(f"租金相關項目: {len(rent_items)} 筆")
    
    await db.close()
    return assets_to_validate


async def example_5_performance_comparison():
    """範例5: 性能比較 - CSV vs Parquet vs DuckDB"""
    print("\n=== 範例5: 性能比較 ===")
    
    import time
    import numpy as np
    
    # 創建較大的測試數據集
    num_rows = 10000
    test_data = pd.DataFrame({
        'ID': range(num_rows),
        'Amount': np.random.uniform(1000, 100000, num_rows),
        'Category': np.random.choice(['A', 'B', 'C', 'D'], num_rows),
        'Date': pd.date_range('2024-01-01', periods=num_rows, freq='H')
    })
    
    results = {}
    
    # CSV測試
    csv_file = Path(r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_purchase_request.csv")
    csv_source = DataSourceFactory.create_from_file(str(csv_file))
    
    start = time.time()
    await csv_source.write(test_data)
    csv_write_time = time.time() - start
    
    start = time.time()
    csv_data = await csv_source.read()
    csv_read_time = time.time() - start
    
    results['CSV'] = {'write': csv_write_time, 'read': csv_read_time}
    
    # Parquet測試
    parquet_file = Path('perf_test.parquet')
    parquet_source = DataSourceFactory.create_from_file(str(parquet_file))
    
    start = time.time()
    await parquet_source.write(test_data)
    parquet_write_time = time.time() - start
    
    start = time.time()
    parquet_data = await parquet_source.read()
    parquet_read_time = time.time() - start
    
    results['Parquet'] = {'write': parquet_write_time, 'read': parquet_read_time}
    
    # DuckDB測試
    db_source = DuckDBSource.create_memory_db()
    
    start = time.time()
    await db_source.write(test_data, table_name='perf_test')
    db_write_time = time.time() - start
    
    start = time.time()
    db_data = await db_source.read("SELECT * FROM perf_test")
    db_read_time = time.time() - start
    
    # 測試查詢性能
    start = time.time()
    db_filtered = await db_source.read(
        "SELECT * FROM perf_test WHERE Amount > 50000 AND Category = 'A'"
    )
    db_query_time = time.time() - start
    
    results['DuckDB'] = {
        'write': db_write_time, 
        'read': db_read_time,
        'query': db_query_time
    }
    
    # 顯示結果
    print(f"\n資料集大小: {num_rows} 行")
    print("\n性能比較結果:")
    print("-" * 50)
    for source, times in results.items():
        print(f"\n{source}:")
        for operation, time_val in times.items():
            print(f"  {operation:10}: {time_val:.4f} 秒")
    
    # 清理
    # csv_file.unlink(missing_ok=True)
    parquet_file.unlink(missing_ok=True)
    await db_source.close()
    
    return results


async def main():
    """執行所有範例"""
    print("=" * 60)
    print("DataSources 模組使用範例")
    print("=" * 60)
    
    # 執行範例
    await example_1_basic_usage()
    await example_2_excel_to_duckdb()
    await example_3_pipeline_integration()
    await example_4_spx_specific()
    await example_5_performance_comparison()
    
    print("\n" + "=" * 60)
    print("所有範例執行完成！")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
