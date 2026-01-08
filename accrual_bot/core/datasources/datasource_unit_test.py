"""
數據源模組測試腳本
用於測試各種數據源的功能
"""

import asyncio
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
import time
import gc

# 添加專案路徑
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from core.datasources import (
    DataSourceFactory, DataSourceConfig, DataSourceType,
    ExcelSource, CSVSource, ParquetSource, DuckDBSource
)

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataSourceTest")


async def test_excel_source():
    """測試Excel數據源"""
    logger.info("=== 測試Excel數據源 ===")
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'PO#': ['PO001', 'PO002', 'PO003'],
        'Amount': [1000, 2000, 3000],
        'Date': pd.date_range('2025-01-01', periods=3)
    })
    
    # 測試文件路徑
    test_file = Path('test_data.xlsx')
    
    try:
        # 創建Excel數據源
        config = DataSourceConfig(
            source_type=DataSourceType.EXCEL,
            connection_params={'file_path': str(test_file)}
        )
        
        # 先寫入測試數據
        test_data.to_excel(test_file, index=False)
        
        source = DataSourceFactory.create(config)
        
        # 測試讀取
        df = await source.read()
        logger.info(f"讀取到 {len(df)} 行數據")
        logger.info(f"列: {df.columns.tolist()}")
        
        # 測試寫入
        new_data = pd.DataFrame({
            'PO#': ['PO004'],
            'Amount': [4000],
            'Date': [pd.Timestamp('2025-01-04')]
        })
        
        success = await source.write(new_data, sheet_name='NewSheet')
        logger.info(f"寫入新工作表: {'成功' if success else '失敗'}")
        
        # 獲取元數據
        metadata = source.get_metadata()
        logger.info(f"Excel元數據: {metadata}")
        
        # 清理
        await source.close()
        # 新的線程再要操作時，舊的線程卻要移除Path('test_data.xlsx')，因為ExcelSource使用pd.ExcelFile沒有用with正確關閉，會導致錯誤。
        # ERROR - Excel數據源異常: [WinError 32] 程序無法存取檔案，因為檔案正由另一個程序使用。: 'test_data.xlsx'
        test_file.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        logger.error(f"Excel測試失敗: {str(e)}")
        test_file.unlink(missing_ok=True)
        return False


async def test_csv_source():
    """測試CSV數據源"""
    logger.info("=== 測試CSV數據源 ===")
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'PR#': ['PR001', 'PR002', 'PR003'],
        'Description': ['Item 1', 'Item 2', 'Item 3'],
        'Quantity': [10, 20, 30]
    })
    
    # 測試文件路徑
    test_file = Path('test_data.csv')
    
    try:
        # 先創建測試文件
        test_data.to_csv(test_file, index=False)
        
        # 創建CSV數據源
        source = CSVSource.create_from_file(str(test_file))
        
        # 測試讀取
        df = await source.read()
        logger.info(f"CSV讀取: {len(df)} 行")
        
        # 測試查詢
        filtered = await source.read(query="Quantity > 15")
        logger.info(f"篩選後: {len(filtered)} 行")
        
        # 測試追加
        new_data = pd.DataFrame({
            'PR#': ['PR004'],
            'Description': ['Item 4'],
            'Quantity': [40]
        })
        
        success = await source.append_data(new_data)
        logger.info(f"追加數據: {'成功' if success else '失敗'}")
        
        # 驗證追加結果
        df_after = await source.read()
        logger.info(f"追加後總行數: {len(df_after)}")
        
        # 清理
        await source.close()
        test_file.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        logger.error(f"CSV測試失敗: {str(e)}")
        test_file.unlink(missing_ok=True)
        return False


async def test_parquet_source():
    """測試Parquet數據源"""
    logger.info("=== 測試Parquet數據源 ===")
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'ID': range(1000),
        'Value': np.random.randn(1000),
        'Category': np.random.choice(['A', 'B', 'C'], 1000)
    })
    
    # 測試文件路徑
    test_file = Path('test_data.parquet')
    
    try:
        # 創建Parquet數據源
        source = ParquetSource.create_from_file(str(test_file))
        
        # 測試寫入
        success = await source.write(test_data)
        logger.info(f"Parquet寫入: {'成功' if success else '失敗'}")
        
        # 測試讀取
        df = await source.read()
        logger.info(f"Parquet讀取: {len(df)} 行")
        
        # 測試列篩選
        df_subset = await source.read(columns=['ID', 'Value'])
        logger.info(f"列篩選後: {df_subset.columns.tolist()}")
        
        # 獲取元數據
        metadata = source.get_metadata()
        logger.info(f"Parquet元數據: 行數={metadata.get('num_rows')}, "
                    f"列數={metadata.get('num_columns')}")
        
        # 獲取schema
        schema = await source.get_schema()
        logger.info(f"Schema: {schema}")
        
        # 清理
        await source.close()
        test_file.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        logger.error(f"Parquet測試失敗: {str(e)}")
        test_file.unlink(missing_ok=True)
        return False


async def test_duckdb_source():
    """測試DuckDB數據源"""
    logger.info("=== 測試DuckDB數據源 ===")
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'order_id': range(1, 101),
        'customer': [f'Customer_{i%10}' for i in range(100)],
        'amount': np.random.uniform(100, 1000, 100),
        'date': pd.date_range('2025-01-01', periods=100)
    })
    
    try:
        # 創建內存數據庫
        source = DuckDBSource.create_memory_db()
        
        # 寫入數據
        success = await source.write(test_data, table_name='orders')
        logger.info(f"DuckDB寫入: {'成功' if success else '失敗'}")
        
        # SQL查詢
        result = await source.read("SELECT * FROM orders WHERE amount > 500")
        logger.info(f"查詢結果: {len(result)} 行")
        
        # 聚合查詢
        agg_result = await source.read("""
            SELECT customer, 
                   COUNT(*) as order_count,
                   AVG(amount) as avg_amount
            FROM orders 
            GROUP BY customer
        """)
        logger.info(f"聚合結果: {len(agg_result)} 個客戶")
        
        # 創建新表
        await source.create_table('summary', {
            'customer': 'VARCHAR',
            'total_amount': 'DOUBLE'
        })
        
        # 插入聚合數據
        await source.execute("""
            INSERT INTO summary
            SELECT customer, SUM(amount) as total_amount
            FROM orders
            GROUP BY customer
        """)
        
        # 驗證
        summary = await source.read("SELECT * FROM summary")
        logger.info(f"Summary表: {len(summary)} 行")
        
        # 列出所有表
        tables = await source.list_tables()
        logger.info(f"所有表: {tables}")
        
        # 獲取表信息
        table_info = await source.get_table_info('orders')
        logger.info(f"Orders表信息: 行數={table_info.get('row_count')}")
        
        # 關閉連接
        await source.close()
        
        return True
        
    except Exception as e:
        logger.error(f"DuckDB測試失敗: {str(e)}")
        return False


async def safe_file_cleanup_async(file_path: Path, max_retries: int = 5, 
                                  initial_delay: float = 0.5) -> bool:
    """
    異步版本的安全檔案清理，處理Windows文件鎖定問題
    
    Args:
        file_path: 要刪除的檔案路徑
        max_retries: 最大重試次數
        initial_delay: 初始重試間隔（秒）
        
    Returns:
        bool: 是否成功刪除
    """
    if not file_path.exists():
        return True
        
    for attempt in range(max_retries):
        try:
            # 強制垃圾回收，幫助釋放資源
            gc.collect()
            
            # 嘗試刪除檔案
            file_path.unlink()
            return True
            
        except (PermissionError, OSError) as e:
            if attempt < max_retries - 1:
                # 計算延遲時間（指數退避）
                delay = initial_delay * (2 ** attempt)
                logging.getLogger(__name__).warning(
                    f"刪除檔案失敗 (嘗試 {attempt + 1}/{max_retries}): {e}"
                    f"，等待 {delay} 秒後重試..."
                )
                await asyncio.sleep(delay)
                
                # 每次重試前都進行垃圾回收
                gc.collect()
            else:
                # 最後一次嘗試失敗
                logging.getLogger(__name__).error(
                    f"無法刪除檔案 {file_path} (已重試 {max_retries} 次): {e}"
                )
                return False
                
    return False

async def test_data_migration():
    """測試數據遷移（從Excel到DuckDB）- 修復版本"""
    logger.info("=== 測試數據遷移 ===")
    
    # 準備測試數據
    test_data = pd.DataFrame({
        'PO#': [f'PO{i:04d}' for i in range(1, 21)],
        'Supplier': [f'Supplier_{i%5}' for i in range(20)],
        'Amount': np.random.uniform(1000, 10000, 20),
        'Status': np.random.choice(['Pending', 'Approved', 'Completed'], 20)
    })
    
    excel_file = Path('migration_test.xlsx')
    db_file = Path('migration_test.db')
    
    # 確保開始前檔案已清理
    await safe_file_cleanup_async(excel_file)
    await safe_file_cleanup_async(db_file)
    
    excel_source = None
    db_source = None
    
    try:
        # 1. 保存到Excel
        test_data.to_excel(excel_file, index=False)
        logger.info("創建源Excel文件")
        
        # 2. 從Excel讀取
        excel_source = ExcelSource.create_from_file(str(excel_file))
        df = await excel_source.read()
        logger.info(f"從Excel讀取: {len(df)} 行")
        
        # 3. 寫入DuckDB
        db_source = DuckDBSource.create_file_db(str(db_file))
        success = await db_source.write(df, table_name='purchase_orders')
        logger.info(f"寫入DuckDB: {'成功' if success else '失敗'}")
        
        # 4. 驗證遷移結果
        migrated_data = await db_source.read("SELECT * FROM purchase_orders")
        logger.info(f"遷移驗證: 原始={len(test_data)}行, 遷移後={len(migrated_data)}行")
        
        # 5. 測試查詢性能
        # Excel查詢（需要讀取全部數據）
        start = time.time()
        excel_df = await excel_source.read()
        excel_filtered = excel_df[excel_df['Amount'] > 5000]
        excel_time = time.time() - start
        
        # DuckDB查詢（直接SQL篩選）
        start = time.time()
        db_filtered = await db_source.read("SELECT * FROM purchase_orders WHERE Amount > 5000")
        db_time = time.time() - start
        
        logger.info(f"查詢性能: Excel={excel_time:.4f}秒, DuckDB={db_time:.4f}秒")
        
        # 成功標記
        migration_success = True
        
    except Exception as e:
        logger.error(f"數據遷移測試失敗: {str(e)}")
        migration_success = False
    
    finally:
        # 確保資源完全釋放
        logger.info("開始清理資源...")
        
        try:
            # 關閉 Excel 源
            if excel_source:
                await excel_source.close()
                excel_source = None
                logger.info("Excel 源已關閉")
                
            # 關閉 DuckDB 源 - 這是關鍵步驟
            if db_source:
                await db_source.close()
                db_source = None
                logger.info("DuckDB 源已關閉")
                
            # 額外等待，讓系統完全釋放資源
            logger.info("等待系統完全釋放檔案鎖定...")
            await asyncio.sleep(2.0)
            
            # 強制垃圾回收
            import gc
            gc.collect()
            await asyncio.sleep(0.5)
            
            # 使用安全清理函數刪除檔案
            logger.info("開始清理測試檔案...")
            
            excel_cleanup = await safe_file_cleanup_async(excel_file)
            logger.info(f"Excel 檔案清理: {'成功' if excel_cleanup else '失敗'}")
            
            db_cleanup = await safe_file_cleanup_async(db_file)
            logger.info(f"DuckDB 檔案清理: {'成功' if db_cleanup else '失敗'}")
            
            if not db_cleanup:
                logger.warning(f"DuckDB 檔案 {db_file} 無法刪除，可能被系統鎖定")
                # 在某些情況下，我們可以選擇重命名檔案而不是刪除
                try:
                    backup_name = db_file.with_suffix('.db.bak')
                    if backup_name.exists():
                        backup_name.unlink()
                    db_file.rename(backup_name)
                    logger.info(f"已將 {db_file} 重命名為 {backup_name}")
                except Exception as rename_e:
                    logger.warning(f"重命名也失敗: {rename_e}")
            
        except Exception as cleanup_e:
            logger.error(f"清理過程中發生錯誤: {cleanup_e}")
    
    return migration_success

async def test_concurrent_operations():
    """測試改進的併發操作（避免死鎖）"""
    logger.info("=== 測試併發操作（改進版）===")
    
    db_source = None
    
    try:
        # 創建DuckDB內存數據庫 
        """RAM DB在不同線程是獨立的，不能用memory做併發測試"""
        # db_source = DuckDBSource.create_memory_db()
        # 創建文件數據庫（支持併發訪問）
        db_file = Path('concurrent_test.db')
        db_source = DuckDBSource.create_file_db(str(db_file))
        
        # 創建測試數據
        test_data = pd.DataFrame({
            'id': range(100),
            'value': np.random.randn(100)
        })
        
        # 先創建所有表（避免併發創建表的問題）
        for i in range(5):
            table_data = test_data.copy()
            table_data['batch'] = i
            await db_source.write(table_data, table_name=f'table_{i}')
            logger.info(f"創建表 table_{i}")
        
        # 併發讀取（這是安全的）
        read_tasks = []
        for i in range(5):
            task = db_source.read(f"SELECT COUNT(*) as cnt FROM table_{i}")
            read_tasks.append(task)
        
        # 使用gather執行併發讀取
        results = await asyncio.gather(*read_tasks, return_exceptions=True)
        
        # 檢查結果
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Table_{i} 讀取失敗: {result}")
            else:
                count = result['cnt'].iloc[0] if not result.empty else 0
                logger.info(f"Table_{i}: {count} 行")
                success_count += 1
        
        logger.info(f"併發讀取成功率: {success_count}/5")
        
        # 測試併發查詢
        query_tasks = []
        for i in range(3):
            query = f"""
                SELECT 
                    batch,
                    COUNT(*) as count,
                    AVG(value) as avg_value
                FROM table_{i}
                GROUP BY batch
            """
            query_tasks.append(db_source.read(query))
        
        # 執行併發查詢
        query_results = await asyncio.gather(*query_tasks, return_exceptions=True)
        
        query_success = sum(1 for r in query_results if not isinstance(r, Exception))
        logger.info(f"併發查詢成功: {query_success}/3")
        
        # 關閉連接
        await db_source.close()
        await safe_file_cleanup_async(db_file)
        
        return success_count == 5 and query_success == 3
        
    except Exception as e:
        logger.error(f"併發測試失敗: {str(e)}")
        if db_source:
            await db_source.close()
        await safe_file_cleanup_async(db_file)
        return False


async def test_thread_safety():
    """測試線程安全性"""
    logger.info("=== 測試線程安全性 ===")
    
    db_path = Path('thread_test.db')
    
    try:
        # 創建文件數據庫（測試多線程訪問同一文件）
        source = DuckDBSource.create_file_db(str(db_path))
        
        # 創建測試表
        test_data = pd.DataFrame({
            'id': range(100),
            'value': np.random.randn(100)
        })
        await source.write(test_data, table_name='test_table')
        
        # 併發執行多個操作
        tasks = []
        
        # 混合讀寫操作
        for i in range(10):
            if i % 2 == 0:
                # 讀操作
                task = source.read("SELECT COUNT(*) FROM test_table")
            else:
                # 寫操作（追加數據）
                new_data = pd.DataFrame({
                    'id': [100 + i],
                    'value': [np.random.randn()]
                })
                task = source.write(new_data, table_name=f'test_{i}')
            tasks.append(task)
        
        # 執行所有任務
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 統計成功的操作
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        logger.info(f"線程安全測試: 成功={success_count}, 失敗={error_count}")
        
        # 清理
        await source.close()
        # db_path.unlink(missing_ok=True)
        await safe_file_cleanup_async(db_path)
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"線程安全測試失敗: {str(e)}")
        # db_path.unlink(missing_ok=True)
        await safe_file_cleanup_async(db_path)
        return False


async def test_deadlock_prevention():
    """測試死鎖預防機制"""
    logger.info("=== 測試死鎖預防 ===")
    
    try:
        # 測試1: DuckDB多線程訪問
        logger.info("測試1: DuckDB多線程訪問")
        db_path = 'deadlock_test.duckdb'
        
        # db_source = DuckDBSource.create_memory_db()
        db_source = DuckDBSource.create_file_db(db_path)
        
        # 創建基礎數據
        base_data = pd.DataFrame({
            'id': range(1000),
            'value': np.random.randn(1000)
        })
        await db_source.write(base_data, table_name='base_table')
        
        # 高併發讀寫測試
        tasks = []
        for i in range(20):  # 20個併發操作
            if i % 3 == 0:
                # 複雜查詢
                task = db_source.read("""
                    SELECT 
                        id % 10 as group_id,
                        COUNT(*) as count,
                        AVG(value) as avg_value,
                        MIN(value) as min_value,
                        MAX(value) as max_value
                    FROM base_table
                    GROUP BY id % 10
                    ORDER BY group_id
                """)
            elif i % 3 == 1:
                # 簡單查詢
                task = db_source.read(f"SELECT * FROM base_table WHERE id = {i}")
            else:
                # 寫入新表
                new_data = pd.DataFrame({
                    'id': [i],
                    'value': [np.random.randn()]
                })
                task = db_source.write(new_data, table_name=f'concurrent_table_{i}')
            tasks.append(task)
        
        # 執行併發任務
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 檢查結果
        exceptions = [r for r in results if isinstance(r, Exception)]
        if exceptions:
            for e in exceptions[:3]:  # 顯示前3個錯誤
                logger.error(f"併發錯誤: {e}")
        
        test1_success = len(exceptions) == 0
        logger.info(f"DuckDB併發測試: {'✅ 通過' if test1_success else '❌ 失敗'} ({len(exceptions)} 錯誤)")
        
        await db_source.close()
        Path(db_path).unlink()
        
        # 測試2: 多數據源併發
        logger.info("\n測試2: 多數據源併發操作")
        sources = []
        
        # 創建多個數據源
        csv_file = Path('test_concurrent.csv')
        excel_file = Path('test_concurrent.xlsx')
        parquet_file = Path('test_concurrent.parquet')
        
        # 準備測試數據
        test_df = pd.DataFrame({
            'id': range(100),
            'value': np.random.randn(100)
        })
        
        # 先創建檔案
        test_df.to_csv(csv_file, index=False)
        test_df.to_excel(excel_file, index=False)
        test_df.to_parquet(parquet_file, index=False)
        
        # 創建數據源
        csv_source = CSVSource.create_from_file(str(csv_file))
        excel_source = ExcelSource.create_from_file(str(excel_file))
        parquet_source = ParquetSource.create_from_file(str(parquet_file))
        
        sources = [csv_source, excel_source, parquet_source]
        
        # 併發讀取所有數據源
        read_tasks = [source.read() for source in sources for _ in range(3)]  # 每個源讀3次
        read_results = await asyncio.gather(*read_tasks, return_exceptions=True)
        
        read_errors = sum(1 for r in read_results if isinstance(r, Exception))
        test2_success = read_errors == 0
        logger.info(f"多數據源併發測試: {'✅ 通過' if test2_success else '❌ 失敗'} ({read_errors} 錯誤)")
        
        # 清理
        for source in sources:
            await source.close()
        
        csv_file.unlink(missing_ok=True)
        excel_file.unlink(missing_ok=True)
        parquet_file.unlink(missing_ok=True)
        
        # 測試3: 極端併發壓力測試
        logger.info("\n測試3: 極端併發壓力測試")
        # stress_db = DuckDBSource.create_memory_db()
        stress_db = DuckDBSource.create_file_db(db_path)
        
        # 創建測試表
        await stress_db.create_table('stress_test', {
            'id': 'INTEGER',
            'thread_id': 'INTEGER',
            'timestamp': 'TIMESTAMP',
            'data': 'VARCHAR'
        })
        
        # 50個併發寫入
        import threading
        write_tasks = []
        for i in range(50):
            thread_id = threading.current_thread().ident
            data = pd.DataFrame({
                'id': [i],
                'thread_id': [thread_id],
                'timestamp': [pd.Timestamp.now()],
                'data': [f'Test data {i}']
            })
            task = stress_db.write(data, table_name='stress_test', mode='append')
            write_tasks.append(task)
        
        write_results = await asyncio.gather(*write_tasks, return_exceptions=True)
        write_errors = sum(1 for r in write_results if isinstance(r, Exception))
        
        # 驗證寫入結果
        count_result = await stress_db.read("SELECT COUNT(*) as cnt FROM stress_test")
        actual_count = count_result['cnt'].iloc[0] if not count_result.empty else 0
        
        test3_success = write_errors == 0 and actual_count == 50
        logger.info(f"壓力測試: {'✅ 通過' if test3_success else '❌ 失敗'} ")
        logger.info(f"  - 寫入錯誤: {write_errors}")
        logger.info(f"  - 實際寫入: {actual_count}/50")
        
        await stress_db.close()
        
        # 總結
        all_success = test1_success and test2_success and test3_success
        logger.info(f"\n死鎖預防測試總結: {'✅ 全部通過' if all_success else '❌ 有測試失敗'}")
        Path(db_path).unlink()
        
        return all_success
        
    except Exception as e:
        logger.error(f"死鎖預防測試異常: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def test_resource_cleanup():
    """測試資源清理機制"""
    logger.info("=== 測試資源清理 ===")
    
    try:
        # 測試線程池清理
        logger.info("測試線程池清理機制")
        
        # 創建多個數據源
        sources = []
        for i in range(5):
            csv_file = Path(f'cleanup_test_{i}.csv')
            pd.DataFrame({'data': [i]}).to_csv(csv_file, index=False)
            source = CSVSource.create_from_file(str(csv_file))
            sources.append((source, csv_file))
        
        # 執行操作
        for source, _ in sources:
            await source.read()
        
        # 關閉所有數據源
        for source, file_path in sources:
            await source.close()
            file_path.unlink(missing_ok=True)
        
        # 手動觸發清理
        DataSourceFactory._cleanup_all_executors()
        
        logger.info("✅ 資源清理測試通過")
        return True
        
    except Exception as e:
        logger.error(f"資源清理測試失敗: {str(e)}")
        return False


async def main():
    """運行所有測試"""
    logger.info("開始數據源模組測試")
    logger.info("=" * 60)
    
    test_results = []
    
    # 運行各項測試
    tests = [
        ("Excel數據源", test_excel_source),
        ("CSV數據源", test_csv_source),
        ("Parquet數據源", test_parquet_source),
        ("DuckDB數據源", test_duckdb_source),
        ("數據遷移", test_data_migration),
        ("併發操作", test_concurrent_operations),
        ("線程安全性", test_thread_safety),
        ("死鎖預防", test_deadlock_prevention),
        ("資源清理", test_resource_cleanup),
    ]
    
    for test_name, test_func in tests:
        try:
            logger.info(f"\n開始測試: {test_name}")
            result = await test_func()
            test_results.append((test_name, result))
        except Exception as e:
            logger.error(f"{test_name}異常: {str(e)}")
            test_results.append((test_name, False))
        
        logger.info("")  # 空行分隔
    
    # 總結測試結果
    logger.info("=" * 60)
    logger.info("測試結果總結:")
    passed = sum(1 for _, result in test_results if result)
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 通過" if result else "❌ 失敗"
        logger.info(f"{test_name:20}: {status}")
    
    logger.info(f"\n總計: {passed}/{total} 測試通過")
    logger.info(f"成功率: {(passed/total*100):.1f}%")
    
    if passed == total:
        logger.info("\n🎉 所有測試通過！數據源模組運行正常。")
    else:
        logger.warning(f"\n⚠️ 有 {total-passed} 項測試失敗，請檢查錯誤訊息。")
    
    # 清理全局線程池
    logger.info("\n清理資源...")
    DataSourceFactory._cleanup_all_executors()
    
    return passed == total


if __name__ == "__main__":
    # 運行測試
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
