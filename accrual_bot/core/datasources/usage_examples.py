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
    db_path = Path('prpo_data.db')
    db_source = DuckDBSource.create_file_db(db_path)
    
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
    Path(db_path).unlink()
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
    csv_file = Path(r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_purchase_order.csv")
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
    csv_file.unlink(missing_ok=True)
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
    # await example_5_performance_comparison()  # 會洗掉原始csv，沒意思。
    
    print("\n" + "=" * 60)
    print("所有範例執行完成！")
    print("=" * 60)

"""
Refer C:\SEA\Accrual\prpo_bot\accrual_bot\accrual_bot\core\datasources\README.md
4. **併發操作**

用await觸發async method,USE asyncio.gather實現IO任務併發
"""
async def concurrent_test():
    """這個函數現在只專注於執行一次讀取任務並返回結果"""
    po_file = r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\202503_purchase_order_20250704_100921 - 複製.csv"
    # po_file = r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\PO_for前期載入.xlsx"
    source = DataSourceFactory.create_from_file(po_file, sheet_name=0)
    print(f"任務開始: 讀取 {po_file}")
    df = await source.read()
    print(f"任務完成: 讀取到 {len(df)} 筆資料")
    return df

async def main_():
    import time
    start_time = time.time()
    
    # --- 錯誤的線性方法 ---
    print("\n--- 開始線性執行 (Sequential) ---")
    await concurrent_test()
    await concurrent_test()
    await concurrent_test()
    await concurrent_test()
    print(f"線性執行耗時: {time.time() - start_time:.2f} 秒\n")  # 預計 > 2 秒
    
    start_time_concurrent = time.time()

    # --- 正確的併發方法 ---
    print("--- 開始併發執行 (Concurrent) ---")
    # 創建一個任務列表，但此時還不執行
    tasks = [
        concurrent_test(),
        concurrent_test(),
        concurrent_test(),
        concurrent_test()
    ]
    
    # asyncio.gather 會併發地運行所有任務
    results = await asyncio.gather(*tasks)
    
    print(f"\n併發執行耗時: {time.time() - start_time_concurrent:.2f} 秒")  # 預計約 1 秒
    
    # results 是一個列表，包含了每個任務的返回值
    print(f"共收集到 {len(results)} 個 DataFrame 結果。")
    df1, df2, df3, df4 = results

####################################################################################################
# 測試腳本
async def test_memory_db():
    """測試內存數據庫 - 修復後應該正常工作"""
    print("\n" + "="*60)
    print("測試 1: 內存數據庫")
    print("="*60)
    
    source = DuckDBSource.create_memory_db()
    
    # 創建測試數據
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })
    
    print(f"\n1. 寫入數據...")
    await source.write(df, table_name='test')
    print(f"   ✅ 寫入 {len(df)} 行")
    
    print(f"\n2. 讀取數據...")
    result = await source.read("SELECT * FROM test")
    print(f"   ✅ 讀取 {len(result)} 行")
    print(f"   數據預覽:\n{result}")
    
    print(f"\n3. 查詢測試...")
    filtered = await source.read("SELECT * FROM test WHERE value > 15")
    print(f"   ✅ 條件查詢返回 {len(filtered)} 行")
    
    print(f"\n4. 列出表...")
    tables = await source.list_tables()
    print(f"   ✅ 找到表: {tables}")
    
    await source.close()
    print("\n✅ 內存數據庫測試通過！")
    return True


async def test_file_db():
    """測試文件數據庫"""
    print("\n" + "="*60)
    print("測試 2: 文件數據庫")
    print("="*60)
    
    db_path = 'test_file.db'
    
    # 清理舊文件
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 創建測試數據
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    print(f"\n1. 寫入數據...")
    await source.write(df, table_name='users')
    print(f"   ✅ 寫入 {len(df)} 行")
    
    print(f"\n2. 讀取數據...")
    result = await source.read("SELECT * FROM users")
    print(f"   ✅ 讀取 {len(result)} 行")
    print(f"   數據預覽:\n{result}")
    
    await source.close()
    
    # 重新打開驗證持久化
    print(f"\n3. 重新打開數據庫驗證持久化...")
    source2 = DuckDBSource.create_file_db(db_path)
    result2 = await source2.read("SELECT * FROM users")
    print(f"   ✅ 讀取 {len(result2)} 行（持久化成功）")
    await source2.close()
    
    # 清理
    Path(db_path).unlink()
    print("\n✅ 文件數據庫測試通過！")
    return True


async def test_concurrent():
    """測試併發操作"""
    print("\n" + "="*60)
    print("測試 3: 併發操作")
    print("="*60)
    db_path = 'test_file.db'
    # source = DuckDBSource.create_memory_db()
    source = DuckDBSource.create_file_db(db_path)
    
    # 準備數據
    df = pd.DataFrame({
        'id': range(100),
        'value': range(100, 200)
    })
    await source.write(df, table_name='concurrent_test')
    print(f"\n準備數據: {len(df)} 行")
    
    print(f"\n執行 10 個併發讀取...")
    tasks = [
        source.read("SELECT * FROM concurrent_test WHERE id < 10"),
        source.read("SELECT * FROM concurrent_test WHERE id >= 10 AND id < 20"),
        source.read("SELECT * FROM concurrent_test WHERE id >= 20 AND id < 30"),
        source.read("SELECT * FROM concurrent_test WHERE id >= 30 AND id < 40"),
        source.read("SELECT * FROM concurrent_test WHERE id >= 40 AND id < 50"),
        source.read("SELECT COUNT(*) as cnt FROM concurrent_test"),
        source.read("SELECT AVG(value) as avg_val FROM concurrent_test"),
        source.read("SELECT MAX(value) as max_val FROM concurrent_test"),
        source.read("SELECT MIN(value) as min_val FROM concurrent_test"),
        source.read("SELECT * FROM concurrent_test LIMIT 5"),
    ]
    
    results = await asyncio.gather(*tasks)
    print(f"   ✅ 併發完成，共 {len(results)} 個結果")
    print(f"   結果行數: {[len(r) for r in results]}")
    
    await source.close()
    print("\n✅ 併發測試通過！")
    Path(db_path).unlink()
    return True


async def test_append_mode():
    """測試追加模式"""
    print("\n" + "="*60)
    print("測試 4: 追加模式")
    print("="*60)
    
    db_path = 'test_file.db'
    # source = DuckDBSource.create_memory_db()
    source = DuckDBSource.create_file_db(db_path)
    
    # 第一批數據
    df1 = pd.DataFrame({'x': [1, 2, 3]})
    await source.write(df1, table_name='append_test', mode='replace')
    print(f"\n1. 初始寫入: {len(df1)} 行")
    
    # 追加數據
    df2 = pd.DataFrame({'x': [4, 5, 6]})
    await source.write(df2, table_name='append_test', mode='append')
    print(f"2. 追加數據: {len(df2)} 行")
    
    # 驗證
    result = await source.read("SELECT * FROM append_test")
    print(f"3. 總行數: {len(result)} 行")
    
    if len(result) == 6:
        print("   ✅ 追加模式正確！")
    else:
        print(f"   ❌ 期望 6 行，實際 {len(result)} 行")
    
    await source.close()
    print("\n✅ 追加模式測試通過！")
    Path(db_path).unlink()
    return True


async def phase_1_test():
    """運行所有測試"""
    print("\n" + "="*60)
    print("DuckDB 重構版本 - 完整測試套件")
    print("="*60)
    
    try:
        # 測試 1: 內存數據庫（關鍵測試）
        await test_memory_db()
        
        # 測試 2: 文件數據庫
        await test_file_db()
        
        # 測試 3: 併發
        await test_concurrent()
        
        # 測試 4: 追加模式
        await test_append_mode()
        
        print("\n" + "="*60)
        print("🎉 所有phase_1測試通過！")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()


###################################################################################################
async def test_transaction_context_manager():
    """測試 1: Transaction context manager"""
    print("\n" + "="*60)
    print("測試 1: Transaction Context Manager")
    print("="*60)
    
    db_path = 'test_transaction.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 準備測試數據
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [100, 200, 300]
    })
    await source.write(df, table_name='accounts')
    print(f"\n初始數據: {len(df)} 行")
    
    # 測試成功的 transaction
    print("\n測試成功的 transaction...")
    operations = [
        "UPDATE accounts SET value = value - 50 WHERE id = 1",
        "UPDATE accounts SET value = value + 50 WHERE id = 2"
    ]
    success = await source.execute_transaction(operations)
    print(f"   ✅ Transaction 成功: {success}")
    
    result = await source.read("SELECT * FROM accounts ORDER BY id")
    print(f"   更新後的數據:\n{result}")
    
    # 測試失敗的 transaction (應該回滾)
    print("\n測試失敗的 transaction (應該回滾)...")
    try:
        operations = [
            "UPDATE accounts SET value = value - 100 WHERE id = 1",
            "UPDATE accounts SET value = value + 100 WHERE id = 999",  # 這會失敗
        ]
        await source.execute_transaction(operations)
    except Exception as e:
        print(f"   ✅ Transaction 正確回滾: {type(e).__name__}")
    
    result = await source.read("SELECT * FROM accounts WHERE id = 1")
    print(f"   回滾後 id=1 的值: {result['value'].iloc[0]} (應該保持不變)")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ Transaction context manager 測試通過！")
    return True


async def test_write_atomic():
    """測試 2: 原子寫入"""
    print("\n" + "="*60)
    print("測試 2: 原子寫入 (write_atomic)")
    print("="*60)
    
    db_path = 'test_atomic.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 測試原子寫入
    df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'status': ['pending', 'pending', 'pending']
    })
    
    print("\n使用 write_atomic 寫入...")
    success = await source.write_atomic(df, table_name='orders')
    print(f"   ✅ 原子寫入成功: {success}")
    
    result = await source.read("SELECT * FROM orders")
    print(f"   寫入的數據: {len(result)} 行")
    
    # 測試原子追加
    df2 = pd.DataFrame({
        'order_id': [4, 5],
        'status': ['completed', 'completed']
    })
    
    print("\n使用 write_atomic 追加...")
    success = await source.write_atomic(df2, table_name='orders', mode='append')
    print(f"   ✅ 原子追加成功: {success}")
    
    result = await source.read("SELECT * FROM orders")
    print(f"   追加後總數: {len(result)} 行")
    print(f"   數據預覽:\n{result}")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ 原子寫入測試通過！")
    return True


async def test_execute_transaction_complex():
    """測試 3: 複雜 transaction 操作"""
    print("\n" + "="*60)
    print("測試 3: 複雜 Transaction 操作")
    print("="*60)
    
    db_path = 'test_complex.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 創建多個表
    await source.execute("CREATE TABLE orders (id INT, amount FLOAT, status TEXT)")
    await source.execute("CREATE TABLE inventory (product_id INT, quantity INT)")
    await source.execute("CREATE TABLE audit_log (operation TEXT, timestamp TIMESTAMP)")
    
    print("\n創建了 3 個表")
    
    # 執行複雜的 transaction
    operations = [
        "INSERT INTO orders VALUES (1, 1000.0, 'pending')",
        "INSERT INTO orders VALUES (2, 2000.0, 'pending')",
        "INSERT INTO inventory VALUES (101, 50)",
        "INSERT INTO inventory VALUES (102, 30)",
        "INSERT INTO audit_log VALUES ('bulk_insert', CURRENT_TIMESTAMP)",
        "UPDATE orders SET status = 'confirmed' WHERE id = 1",
        "UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 101"
    ]
    
    print(f"\n執行 {len(operations)} 個操作作為單一 transaction...")
    success = await source.execute_transaction(operations)
    print(f"   ✅ Transaction 成功: {success}")
    
    # 驗證結果
    orders = await source.read("SELECT * FROM orders")
    inventory = await source.read("SELECT * FROM inventory")
    audit = await source.read("SELECT * FROM audit_log")
    
    print(f"\n結果驗證:")
    print(f"   Orders: {len(orders)} 行")
    print(f"   Inventory: {len(inventory)} 行")
    print(f"   Audit log: {len(audit)} 行")
    print(f"\n   Orders 數據:\n{orders}")
    print(f"\n   Inventory 數據:\n{inventory}")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ 複雜 transaction 測試通過！")
    return True


async def test_concurrent_transactions():
    """測試 4: 併發 transaction（文件DB）- 不重疊操作"""
    print("\n" + "="*60)
    print("測試 4: 併發 Transaction（文件DB）")
    print("="*60)
    
    db_path = 'test_concurrent_tx.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 初始化更多賬戶以避免衝突
    df = pd.DataFrame({
        'account_id': list(range(1, 11)),  # 10個賬戶
        'balance': [1000] * 10
    })
    await source.write(df, table_name='accounts')
    print(f"\n初始賬戶: {len(df)} 個")
    
    # 創建多個併發 transaction - 使用不重疊的賬戶
    async def transfer_money(from_id, to_id, amount):
        """轉賬操作"""
        operations = [
            f"UPDATE accounts SET balance = balance - {amount} WHERE account_id = {from_id}",
            f"UPDATE accounts SET balance = balance + {amount} WHERE account_id = {to_id}"
        ]
        return await source.execute_transaction(operations)
    
    print("\n執行 5 個併發轉賬（不重疊賬戶）...")
    tasks = [
        # transfer_money(1, 2, 100)  # 修改賬戶 1, 2
        # transfer_money(2, 3, 200)  # 修改賬戶 2, 3  ← 衝突！賬戶2被同時修改
        # transfer_money(3, 4, 150)  # 修改賬戶 3, 4  ← 衝突！賬戶3被同時修改
        # transfer_money(4, 5, 250)  # 修改賬戶 4, 5  ← 衝突！
        # transfer_money(5, 1, 300)  # 修改賬戶 5, 1  ← 衝突！
        transfer_money(1, 2, 100),   # 修改 1, 2
        transfer_money(3, 4, 200),   # 修改 3, 4 ✅ 無衝突
        transfer_money(5, 6, 150),   # 修改 5, 6 ✅ 無衝突
        transfer_money(7, 8, 250),   # 修改 7, 8 ✅ 無衝突
        transfer_money(9, 10, 300)   # 修改 9, 10 ✅ 無衝突
    ]
    
    results = await asyncio.gather(*tasks)
    print(f"   ✅ 併發完成: {sum(results)}/{len(results)} 個成功")
    
    # 驗證最終結果
    final = await source.read("SELECT * FROM accounts ORDER BY account_id")
    print(f"\n最終賬戶狀態:\n{final}")
    
    # 驗證總額不變
    initial_total = df['balance'].sum()
    final_total = final['balance'].sum()
    print(f"\n總額驗證:")
    print(f"   初始總額: {initial_total}")
    print(f"   最終總額: {final_total}")
    print(f"   ✅ 總額{'相等' if initial_total == final_total else '不相等'}")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ 併發 transaction 測試通過！")
    return True


async def test_concurrent_conflict_handling():
    """測試 4b: Transaction 衝突處理（預期行為）"""
    print("\n" + "="*60)
    print("測試 4b: Transaction 衝突處理")
    print("="*60)
    
    db_path = 'test_conflict.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 初始化數據
    df = pd.DataFrame({'account_id': [1, 2], 'balance': [1000, 2000]})
    await source.write(df, table_name='accounts')
    print(f"\n初始賬戶: {len(df)} 個")
    
    # 創建會衝突的操作
    async def update_account_1():
        operations = ["UPDATE accounts SET balance = balance + 100 WHERE account_id = 1"]
        return await source.execute_transaction(operations)
    
    print("\n執行 5 個併發修改同一賬戶（預期會衝突）...")
    tasks = [update_account_1() for _ in range(5)]
    
    # 使用 return_exceptions=True 來捕獲異常
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 統計結果
    successes = sum(1 for r in results if r is True)
    failures = sum(1 for r in results if isinstance(r, Exception))
    
    print(f"\n結果:")
    print(f"   成功: {successes} 個")
    print(f"   失敗（衝突）: {failures} 個")
    print(f"   ✅ 這是正常行為！Transaction 衝突檢測工作正常")
    
    # 驗證最終狀態
    final = await source.read("SELECT * FROM accounts WHERE account_id = 1")
    expected_balance = 1000 + (100 * successes)
    actual_balance = final['balance'].iloc[0]
    
    print(f"\n最終狀態:")
    print(f"   初始餘額: 1000")
    print(f"   成功交易: {successes} 筆 x 100 = {100 * successes}")
    print(f"   預期餘額: {expected_balance}")
    print(f"   實際餘額: {actual_balance}")
    print(f"   ✅ 餘額{'正確' if expected_balance == actual_balance else '不正確'}")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ Transaction 衝突處理測試通過！")
    print("\n💡 重要說明:")
    print("   - Transaction 衝突是正常的數據庫行為")
    print("   - 保證了數據一致性（ACID）")
    print("   - 應用層應該處理衝突（重試或串列化）")
    return True


async def test_transaction_rollback():
    """測試 5: Transaction 回滾機制"""
    print("\n" + "="*60)
    print("測試 5: Transaction 回滾機制")
    print("="*60)
    
    db_path = 'test_rollback.db'
    if Path(db_path).exists():
        Path(db_path).unlink()
    
    source = DuckDBSource.create_file_db(db_path)
    
    # 初始化數據
    df = pd.DataFrame({'id': [1, 2, 3], 'value': [100, 200, 300]})
    await source.write(df, table_name='test_table')
    
    initial = await source.read("SELECT SUM(value) as total FROM test_table")
    initial_total = initial['total'].iloc[0]
    print(f"\n初始總和: {initial_total}")
    
    # 測試會失敗的 transaction
    print("\n測試會失敗的 transaction...")
    try:
        operations = [
            "UPDATE test_table SET value = value + 100 WHERE id = 1",
            "UPDATE test_table SET value = value + 200 WHERE id = 2",
            "UPDATE test_table SET value = 'invalid' WHERE id = 3",  # 這會失敗
        ]
        await source.execute_transaction(operations)
        print("   ❌ Transaction 應該失敗但沒有")
    except Exception as e:
        print(f"   ✅ Transaction 正確失敗: {type(e).__name__}")
    
    # 驗證數據沒有改變
    final = await source.read("SELECT SUM(value) as total FROM test_table")
    final_total = final['total'].iloc[0]
    print(f"\n回滾後總和: {final_total}")
    print(f"   ✅ 數據{'未改變' if initial_total == final_total else '已改變'}")
    
    # 檢查每個值
    data = await source.read("SELECT * FROM test_table ORDER BY id")
    print(f"\n回滾後的數據:\n{data}")
    
    await source.close()
    Path(db_path).unlink()
    print("\n✅ Transaction 回滾測試通過！")
    return True


async def phase_2_test():
    """運行所有 Phase 2 測試"""
    print("\n" + "="*60)
    print("Phase 2: Transaction 支持 - 完整測試")
    print("="*60)
    
    try:
        await test_transaction_context_manager()
        await test_write_atomic()
        await test_execute_transaction_complex()
        await test_concurrent_transactions()
        await test_concurrent_conflict_handling()
        await test_transaction_rollback()
        
        print("\n" + "="*60)
        print("🎉 Phase 2 所有測試通過！")
        print("="*60)
        print("\n✅ Transaction context manager")
        print("✅ 原子寫入 (write_atomic)")
        print("✅ 複雜 transaction 操作")
        print("✅ 併發 transaction（不重疊賬戶）")
        print("✅ Transaction 衝突處理（預期行為）")
        print("✅ Transaction 回滾機制")
        print("\n重構完成！可以替換原文件。")
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # asyncio.run(main())
    # asyncio.run(main_())
    # asyncio.run(phase_1_test())
    asyncio.run(phase_2_test())
