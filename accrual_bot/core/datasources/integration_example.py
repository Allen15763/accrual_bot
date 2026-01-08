"""
DataSources 與現有處理器的整合範例
展示如何將新的數據源模組整合到現有的PRPO處理流程
"""

import asyncio
import pandas as pd
from pathlib import Path
import sys
from typing import Optional, Dict, Any

# 添加專案路徑
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from core.datasources import (
    DataSource,
    DataSourceFactory,
    DataSourceConfig,
    DataSourceType,
    DuckDBSource
)
from core.processors.base_processor import BaseDataProcessor


class EnhancedDataProcessor(BaseDataProcessor):
    """
    增強版數據處理器
    整合新的數據源架構，同時保持向後兼容
    """
    
    def __init__(self, entity_type: str = "MOB"):
        super().__init__(entity_type)
        self.data_sources: Dict[str, DataSource] = {}
        
    async def setup_data_sources(self, 
                                 raw_data_path: str,
                                 previous_wp_path: Optional[str] = None,
                                 procurement_path: Optional[str] = None):
        """
        設置數據源
        
        Args:
            raw_data_path: 原始數據路徑
            previous_wp_path: 前期底稿路徑
            procurement_path: 採購底稿路徑
        """
        # 主要數據源
        self.data_sources['main'] = DataSourceFactory.create_from_file(raw_data_path)
        
        # 前期底稿
        if previous_wp_path and Path(previous_wp_path).exists():
            self.data_sources['previous'] = DataSourceFactory.create_from_file(previous_wp_path)
        
        # 採購底稿
        if procurement_path and Path(procurement_path).exists():
            self.data_sources['procurement'] = DataSourceFactory.create_from_file(procurement_path)
        
        # 創建DuckDB作為工作數據庫
        self.data_sources['workdb'] = DuckDBSource.create_memory_db()
    
    async def process_with_datasources(self, processing_date: int) -> pd.DataFrame:
        """
        使用新的數據源架構處理數據
        
        Args:
            processing_date: 處理日期（例如：202503）
            
        Returns:
            pd.DataFrame: 處理後的數據
        """
        # 1. 從主數據源讀取
        self.logger.info("讀取主要數據...")
        main_df = await self.data_sources['main'].read()
        
        # 2. 寫入工作數據庫
        work_db = self.data_sources['workdb']
        await work_db.write(main_df, table_name='raw_data')
        
        # 3. 如果有前期底稿，合併數據
        if 'previous' in self.data_sources:
            self.logger.info("合併前期底稿...")
            previous_df = await self.data_sources['previous'].read()
            await work_db.write(previous_df, table_name='previous_wp')
            
            # 使用SQL進行合併
            merged_df = await work_db.read("""
                SELECT 
                    r.*,
                    p."Remarked by 上月 FN" as prev_fn_remark
                FROM raw_data r
                LEFT JOIN previous_wp p
                    ON r."PO_number" = p."PO_number"
            """)
        else:
            merged_df = main_df
        
        # 4. 應用現有的處理邏輯
        self.logger.info("應用處理邏輯...")
        
        # 清理NaN值
        merged_df = self.clean_nan_values(merged_df, ['Item Description', 'GL#'])
        
        # 格式化日期
        merged_df = self.reformat_dates(merged_df)
        
        # 解析日期範圍
        merged_df = self.parse_date_from_description(merged_df)
        
        # 評估狀態
        status_col = 'PO狀態' if 'PO_number' in merged_df.columns else 'PR狀態'
        if status_col == 'PO狀態':
            merged_df['PO狀態'] = None
        else:
            merged_df['PR狀態']
        merged_df['檔案日期'] = processing_date
        
        merged_df = self.evaluate_status_based_on_dates(merged_df, status_col)
        
        # 更新估計入帳
        merged_df = self.update_estimation_based_on_status(merged_df, status_col)
        
        # 判斷科目代碼
        merged_df = self.judge_ac_code(merged_df)
        
        return merged_df
    
    async def migrate_to_duckdb(self, output_db_path: str = 'prpo_data.db'):
        """
        將處理結果遷移到DuckDB永久存儲
        
        Args:
            output_db_path: 輸出數據庫路徑
        """
        # 創建文件數據庫
        file_db = DuckDBSource.create_file_db(output_db_path)
        
        # 從工作數據庫複製數據
        work_db = self.data_sources['workdb']
        
        # 獲取所有表
        tables = await work_db.list_tables()
        
        for table in tables:
            self.logger.info(f"遷移表 {table}...")
            data = await work_db.read(f"SELECT * FROM {table}")
            await file_db.write(data, table_name=table)
        
        # 創建索引
        await file_db.create_index('raw_data', 'idx_po', ['PO_number'])
        
        await file_db.close()
        self.logger.info(f"數據已遷移到 {output_db_path}")
    
    async def cleanup(self):
        """清理數據源連接"""
        for name, source in self.data_sources.items():
            try:
                await source.close()
                self.logger.info(f"關閉數據源: {name}")
            except Exception as err:
                self.logger.error(f'{err}')


class ProcessorAdapter:
    """
    適配器：讓現有代碼使用新的數據源架構
    """
    
    def __init__(self, entity_type: str):
        self.entity_type = entity_type
        self.processor = EnhancedDataProcessor(entity_type)
    
    def process_po_mode_1(self, raw_file: str, raw_filename: str,
                          previous_wp: str, procurement: str) -> pd.DataFrame:
        """
        兼容原有的同步接口
        
        Args:
            raw_file: 原始檔案路徑
            raw_filename: 原始檔案名稱
            previous_wp: 前期底稿路徑
            procurement: 採購底稿路徑
            
        Returns:
            pd.DataFrame: 處理結果
        """
        # 將同步調用轉換為異步
        async def async_process():
            # 設置數據源
            await self.processor.setup_data_sources(
                raw_file,
                previous_wp,
                procurement
            )
            
            # 從檔名提取日期
            import re
            match = re.search(r'(\d{6})', raw_filename)
            processing_date = int(match.group(1)) if match else 202503
            
            # 處理數據
            result = await self.processor.process_with_datasources(processing_date)
            
            # 清理
            await self.processor.cleanup()
            
            return result
        
        # 運行異步函數
        return asyncio.run(async_process())


async def demonstration():
    """
    完整的整合示範
    """
    print("=" * 60)
    print("數據源架構整合示範")
    print("=" * 60)
    
    # 模擬數據準備
    test_po_data = pd.DataFrame({
        'PO_number': [f'PO{i:04d}' for i in range(1, 11)],
        'Item Description': ['Office Supplies'] * 5 + ['IT Equipment'] * 5,
        'GL#': ['511101'] * 5 + ['151101'] * 5,
        'Expected Receive Month': ['Jan-25'] * 3 + ['Feb-25'] * 3 + ['Mar-25'] * 4,
        'Entry Quantity': [10] * 10,
        'Received Quantity': [10] * 7 + [0] * 3,
        'Entry Amount': [1000 * i for i in range(1, 11)],
        'Entry Billed Amount': [1000 * i for i in range(1, 8)] + [0] * 3,
        'Company': ['MOBTW'] * 10,
        'Department': ['IT'] * 10,
        'Submission Date': ['01-Jan-25'] * 10,
        'PO Create Date': ['2025-01-01'] * 10,
        'Remarked by Procurement': [''] * 10
    })
    
    # 保存測試數據
    test_file = Path('test_po_data.csv')
    test_po_data.to_csv(test_file, index=False)
    
    try:
        # 創建增強版處理器
        processor = EnhancedDataProcessor("MOB")
        
        # 設置數據源
        await processor.setup_data_sources(str(test_file))
        
        # 處理數據
        print("\n處理數據中...")
        result = await processor.process_with_datasources(202501)
        
        print(f"\n處理完成，結果包含 {len(result)} 筆記錄")
        print("\n關鍵欄位:")
        display_cols = ['PO#', 'PO狀態', '是否估計入帳', 'Account code']
        if all(col in result.columns for col in display_cols):
            print(result[display_cols].head())
        
        # 展示DuckDB的查詢能力
        work_db = processor.data_sources['workdb']
        
        # 將結果寫入工作數據庫
        await work_db.write(result, table_name='processed_result')
        
        # 執行分析查詢
        print("\n執行分析查詢...")
        
        # 狀態統計
        status_stats = await work_db.read("""
            SELECT 
                "PO狀態" as status,
                COUNT(*) as count,
                SUM("Entry Amount") as total_amount
            FROM processed_result
            GROUP BY "PO狀態"
        """)
        
        print("\n狀態統計:")
        print(status_stats)
        
        # 估計入帳統計
        accrual_stats = await work_db.read("""
            SELECT 
                "是否估計入帳" as accrual_flag,
                COUNT(*) as count
            FROM processed_result
            GROUP BY "是否估計入帳"
        """)
        
        print("\n估計入帳統計:")
        print(accrual_stats)
        
        # 遷移到永久存儲
        print("\n遷移數據到永久存儲...")
        await processor.migrate_to_duckdb('demo_output.db')
        
        # 清理
        await processor.cleanup()
        
    finally:
        # 清理測試文件
        test_file.unlink(missing_ok=True)
        Path('demo_output.db').unlink(missing_ok=True)
    
    print("\n示範完成！")


def test_backward_compatibility():
    """
    測試向後兼容性 ERROR TBC
    """
    print("\n" + "=" * 60)
    print("測試向後兼容性")
    print("=" * 60)
    
    # 準備測試數據
    test_data = pd.DataFrame({
        'PO_number': ['PO001', 'PO002'],
        'Amount': [1000, 2000],
        'Item Description': ['Test Item 1', 'Test Item 2'],
        'GL#': ['511101', '622101'],
        'Expected Receive Month': ['Jan-25', 'Feb-25'],
        'Entry Quantity': [10, 20],
        'Received Quantity': [10, 20],
        'Entry Amount': [1000, 2000],
        'Entry Billed Amount': [0, 0],
        'Company': ['MOBTW', 'MOBTW'],
        'Department': ['IT', 'OPS'],
        'Submission Date': ['01-Jan-25', '01-Feb-25'],
        'PO Create Date': ['2025-01-01', '2025-02-01']
    })
    
    test_file = Path('backward_compat_test.csv')
    test_data.to_csv(test_file, index=False)
    
    try:
        # 使用適配器（模擬原有調用方式）
        adapter = ProcessorAdapter("MOB")
        result = adapter.process_po_mode_1(
            str(test_file),
            'backward_compat_test_202501.csv',
            '',  # 無前期底稿
            ''   # 無採購底稿
        )
        
        print(f"向後兼容測試成功！處理了 {len(result)} 筆記錄")
        
        if 'PO狀態' in result.columns:
            print("狀態欄位存在: ✓")
        
        if '是否估計入帳' in result.columns:
            print("估計入帳欄位存在: ✓")
        
        print("\n向後兼容性測試通過！")
        
    finally:
        test_file.unlink(missing_ok=True)


if __name__ == "__main__":
    # 運行示範
    asyncio.run(demonstration())
    
    # 測試向後兼容性
    test_backward_compatibility()
