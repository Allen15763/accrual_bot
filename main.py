import sys
import os
import time
from pathlib import Path
from datetime import datetime

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# 設置logging（在其他導入之前）
from accrual_bot import get_logger
logger = get_logger('test_script')

# TEST NEW MODULE
import asyncio
from accrual_bot.core.datasources import (
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

async def run_async_function(is_confurrent=True) -> list:
    if is_confurrent:
        tasks = [
            example_1_basic_usage(),
            example_1_basic_usage()
        ]
        results = await asyncio.gather(*tasks)
        return results
    
    else:
        return await example_1_basic_usage()
    

def test_mob_entity():
    """測試MOB實體功能"""
    logger.info("=== 測試MOB實體 ===")
    
    try:
        # from core.entities import create_entity
        # from core.models.data_models import EntityType
        # # 創建MOB實體
        # mob_entity = create_entity(EntityType.MOB)

        from accrual_bot import create_entity_by_name
        mob_entity = create_entity_by_name('MOB')
        
        # 測試資料路徑 (請根據實際情況調整)
        test_files = {
            'raw_data': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\202503_purchase_order_20250204_151523.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\PO_for前期載入.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\採購底稿PO.xlsx",
            # 'closing_list': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\mob_closing.xlsx"
        }
        
        logger.info("MOB實體創建成功")
        logger.info(f"實體名稱: {mob_entity.get_entity_name()}")
        logger.info(f"實體描述: {mob_entity.get_entity_description()}")

        # 如何使用mob_entity開始處理底稿流程 --> 部分測試
        """
        process_po_mode_2  ok
        """
        # TODO
        # mob_entity.process_po_mode_2(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        #     test_files['procurement']
        # )
        # mob_entity.process_po_mode_3(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        # )
        # mob_entity.process_po_mode_4(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        # )

        """
        PR test, all pass
        """
        test_files = {
            'raw_data': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\202503_purchase_request_20250204_151339.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\PR_for前期載入.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\MOBA\raw\採購底稿PR.xlsx",
        }
        # mob_entity.process_pr_mode_1(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        #     test_files['procurement']
        # )
        # mob_entity.process_pr_mode_2(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        # )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ MOB實體測試失敗: {str(e)}")
        return False

def test_spt_entity():
    """測試SPT實體功能"""
    logger.info("=== 測試SPT實體 ===")
    
    try:
        from accrual_bot import create_entity_by_name
        spt_entity = create_entity_by_name('SPT')

        # from core.entities import create_entity
        # from core.models.data_models import EntityType
        # spt_entity = create_entity(EntityType.SPT)
        
        logger.info("SPT實體創建成功")
        logger.info(f"實體名稱: {spt_entity.get_entity_name()}")
        logger.info(f"實體描述: {spt_entity.get_entity_description()}")

        # 測試資料路徑 (請根據實際情況調整)
        test_files = {
            'raw_data': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\202503_purchase_order_20250704_100921.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\PO_for前期載入.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\採購底稿PO_未結PO 202503 SPTTW.xlsx",
            # 'closing_list': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\mob_closing.xlsx"
        }
        """
        SPT PO mode 2 ok pass
        """
        # spt_entity.process_po_mode_2(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        #     test_files['procurement']
        # )

        # 測試資料路徑 (請根據實際情況調整)
        test_files = {
            'raw_data': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\202503_purchase_request_20250704_100735.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\PR_for前期載入.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\未結PO 202503 SPTTW.xlsx",
            # 'closing_list': r"C:\SEA\Accrual\prpo_bot\resources\頂一下\202503\SPT\raw\mob_closing.xlsx"
        }

        """
        SPT PR mode 1 ok pass
        """
        # spt_entity.process_pr_mode_1(
        #     test_files['raw_data'],
        #     os.path.basename(test_files['raw_data']),
        #     test_files['previous_wp'],
        #     test_files['procurement']
        # )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SPT實體測試失敗: {str(e)}")
        return False

def test_spx_po_processing():
    """測試SPX PO處理功能"""
    logger.info("=== 測試SPX PO處理 ===")
    
    try:
        from accrual_bot import create_entity
        from accrual_bot import EntityType
        
        spx_entity = create_entity(EntityType.SPX)
        
        # SPX測試資料路徑 (請根據實際情況調整)
        test_files = {
            # 'po_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_purchase_order.csv",
            # 'po_file_name': "202504_purchase_order.csv",
            # 'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PO_FN.xlsx",
            # 'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PO_PQ.xlsx",
            # 'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\AP_Invoice_Match_Monitoring_Ext_202504.xlsx",
            # 'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PR_FN.xlsx",
            # 'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PR_PQ.xlsx",
            # 'ops_validation': r"C:\Users\lia\Downloads\SPX智取櫃及繳費機驗收明細(For FN)_2507.xlsx"

            # 'po_file': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_purchase_order.csv",
            # 'po_file_name': "202507_purchase_order.csv",
            # 'previous_wp': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202506_PO_FN.xlsx",
            # 'procurement': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_PO_PQ.xlsx",
            # 'ap_invoice': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\AP_Invoice_Match_Monitoring_Ext_202507.xlsx",
            # 'previous_wp_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202506_PR_FN.xlsx",
            # 'procurement_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_PR_PQ.xlsx",
            # 'ops_validation': r"C:\Users\lia\Downloads\SPX智取櫃及繳費機驗收明細(For FN)_2507.xlsx"

            'po_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_purchase_order.csv",
            'po_file_name': "202508_purchase_order.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202507_PO_FN.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_PO_PQ.xlsx",
            'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\AP_Invoice_Match_Monitoring_Ext (NEW).xlsx",
            'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202507_PR_FN.xlsx",
            'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_PR_PQ.xlsx",
            'ops_validation': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\SPX智取櫃及繳費機驗收明細(For FN)_2508_修復.xlsx"

            # 'po_file': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_purchase_order.csv",
            # 'po_file_name': "202507_purchase_order.csv",
            # 'previous_wp': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202506_PO_FN.xlsx",
            # 'procurement': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_PO_PQ.xlsx",
            # 'ap_invoice': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\AP_Invoice_Match_Monitoring_Ext_202507.xlsx",
            # 'previous_wp_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202506_PR_FN.xlsx",
            # 'procurement_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_PR_PQ.xlsx",
            # 'ops_validation': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202507\SPX智取櫃及繳費機驗收明細(For FN)_2507.xlsx"
        }
        
        # 檢查檔案是否存在
        missing_files = []
        for file_type, file_path in test_files.items():
            if not os.path.exists(file_path) and file_type != 'po_file_name':
                missing_files.append(f"{file_type}: {file_path}")
        
        if missing_files:
            logger.warning("⚠️ 以下測試檔案不存在:")
            for missing in missing_files:
                logger.warning(f"  - {missing}")
            logger.warning("請調整test_files中的路徑或準備測試數據")
            return False
        
        logger.info("開始SPX PO模式1測試...")
        start_time = time.time()
        
        # 測試模式1（完整流程）
        result = spx_entity.mode_1(
            test_files['po_file'],
            test_files['po_file_name'],
            test_files['previous_wp'],
            test_files['procurement'],
            test_files['ap_invoice'],
            test_files['previous_wp_pr'],
            test_files['procurement_pr'],
            test_files['ops_validation']
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"處理完成，耗時: {processing_time:.2f} 秒")
        logger.info(f"處理結果: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SPX PO處理測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_spx_pr_processing():
    """測試SPX PR處理功能"""
    logger.info("=== 測試SPX PR處理 ===")
    
    try:
        from accrual_bot import create_entity
        from accrual_bot import EntityType
        
        spx_entity = create_entity(EntityType.SPX)
        
        test_files = {
            # 'pr_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_purchase_request_20250206_101058_移除缺失資料.xlsx",
            # 'pr_file_name': "202505_purchase_request_20250206_101058.xlsx",
            # 'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_PR_PQ.xlsx",
            # 'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202504_PR_FN.xlsx"

            # 'pr_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_purchase_request.xlsx",
            # 'pr_file_name': "202508_purchase_request.xlsx",
            # 'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202508_PR_PQ.xlsx",
            # 'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202508\SPX未結For 機器人\202507_PR_FN.xlsx"

            'pr_file': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_purchase_request.csv",
            'pr_file_name': "202507_purchase_request.csv",
            'procurement_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202507_PR_PQ.xlsx",
            'previous_wp_pr': r"G:\.shortcut-targets-by-id\1am-NBNd2ffKOuVY0b81CBrFd4dxss3QP\Financial Data - LG\02_Accounts Payable\SPX_Closing Data\2025(新路徑-已完成)\202507\未結PRPO\For robot\202506_PR_FN.xlsx"
        }
        
        # 檢查檔案是否存在
        missing_files = []
        for file_type, file_path in test_files.items():
            if not os.path.exists(file_path) and file_type != 'pr_file_name':
                missing_files.append(f"{file_type}: {file_path}")
        
        if missing_files:
            logger.warning("⚠️ 以下測試檔案不存在:")
            for missing in missing_files:
                logger.warning(f"  - {missing}")
            return False
        
        logger.info("開始SPX PR模式1測試...")
        start_time = time.time()
        
        spx_entity.process_pr(
            test_files['pr_file'],
            test_files['pr_file_name'],
            test_files['previous_wp_pr'],
            test_files['procurement_pr']
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"處理完成，耗時: {processing_time:.2f} 秒")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SPX PR處理測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def ppe_test():
    """PPE底稿測試 - 優化版本"""
    logger.info("=== 測試PPE處理器（優化版本）===")
    
    try:
        from accrual_bot import create_entity
        from accrual_bot import EntityType
        
        # 創建SPX實體
        spx_entity = create_entity(EntityType.SPX)
        
        # 測試檔案路徑
        test_file_url = r'G:\共用雲端硬碟\INT_TWN_SEA_FN_Shared_Resources\00_Temp_Internal_share\SPX\租金\SPX租金合約歸檔清單及匯款狀態_marge1.xlsx'
        
        # 方法1: 向後相容的呼叫方式
        logger.info("\n測試方法1: 向後相容的呼叫方式")
        result1 = spx_entity.process_ppe_working_paper(
            contract_filing_list_url=test_file_url, 
            current_month=202508
        )
        
        if hasattr(result1, 'success'):
            logger.info(f"處理狀態: {'成功' if result1.success else '失敗'}")
            logger.info(f"訊息: {result1.message}")
            logger.info(f"總記錄數: {result1.total_records}")
            logger.info(f"處理時間: {result1.processing_time:.2f} 秒" if result1.processing_time else "處理時間: N/A")
            
            if result1.errors:
                logger.error("錯誤列表:")
                for error in result1.errors:
                    logger.error(f"  - {error}")
            
            if result1.warnings:
                logger.warning("警告列表:")
                for warning in result1.warnings:
                    logger.warning(f"  - {warning}")
            
            if result1.metadata:
                logger.info("元數據:")
                for key, value in result1.metadata.items():
                    logger.info(f"  - {key}: {value}")
        
        return result1.success
        
    except FileNotFoundError as e:
        logger.error(f"❌ 測試檔案不存在: {str(e)}")
        logger.warning("請確認檔案路徑是否正確或準備測試數據")
        return False
    except Exception as e:
        logger.error(f"❌ PPE測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    """地址模糊比對 - 待實現"""

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    # 執行完整測試
    
    # 測試PPE優化版本
    # logger.info("\n" + "=" * 60)
    # logger.info("開始測試PPE處理器優化版本")
    # logger.info("=" * 60)
    # ppe_test_result = ppe_test()
    # if ppe_test_result:
    #     logger.info("✅ PPE處理器測試通過")
    # else:
    #     logger.warning("⚠️ PPE處理器測試失敗或檔案不存在")
    
    
    # 測試特定檔案
    test_spx_po_processing()
    # test_spx_pr_processing()

    # test_mob_entity()
    # test_spt_entity()

    asyncio.run(run_async_function())
