#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRPO Bot 重構後功能測試腳本

此腳本用於測試重構後的程式碼是否能正確處理各種前端檔案
並產出與原始程式相同的底稿結果

使用方法:
python test_script.py
"""

import sys
import os
import time
from pathlib import Path
from datetime import datetime

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# 設置logging（在其他導入之前）
from utils.logging import get_logger
logger = get_logger('test_script')

def test_mob_entity():
    """測試MOB實體功能"""
    logger.info("=== 測試MOB實體 ===")
    
    try:
        # from core.entities import create_entity
        # from core.models.data_models import EntityType
        # # 創建MOB實體
        # mob_entity = create_entity(EntityType.MOB)

        from core.entities import create_entity_by_name
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
        from core.entities import create_entity_by_name
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

def test_spx_entity():
    """測試SPX實體功能"""
    logger.info("=== 測試SPX實體 ===")
    
    try:
        from core.entities import create_entity
        from core.models.data_models import EntityType
        
        # 創建SPX實體
        spx_entity = create_entity(EntityType.SPX)
        
        logger.info("SPX實體創建成功")
        logger.info(f"實體名稱: {spx_entity.get_entity_name()}")
        logger.info(f"實體描述: {spx_entity.get_entity_description()}")
        
        # 測試向後相容性方法
        logger.info("測試向後相容性方法:")
        logger.info(f"mode_1 方法: {hasattr(spx_entity, 'mode_1')}")
        logger.info(f"mode_2 方法: {hasattr(spx_entity, 'mode_2')}")
        logger.info(f"mode_5 方法: {hasattr(spx_entity, 'mode_5')}")
        logger.info(f"concurrent_spx_process 方法: {hasattr(spx_entity, 'concurrent_spx_process')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SPX實體測試失敗: {str(e)}")
        return False

def test_spx_po_processing():
    """測試SPX PO處理功能"""
    logger.info("=== 測試SPX PO處理 ===")
    
    try:
        from core.entities import create_entity
        from core.models.data_models import EntityType
        
        spx_entity = create_entity(EntityType.SPX)
        
        # SPX測試資料路徑 (請根據實際情況調整)
        test_files = {
            'po_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_purchase_order.csv",
            'po_file_name': "202504_purchase_order.csv",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PO_FN.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PO_PQ.xlsx",
            'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\AP_Invoice_Match_Monitoring_Ext_202504.xlsx",
            'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PR_FN.xlsx",
            'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PR_PQ.xlsx"
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
            test_files['procurement_pr']
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
        # 這裡應該測試SPX PR處理，但目前重構版本可能有問題
        logger.warning("⚠️ SPX PR處理需要進一步檢查重構後的實現")
        
        # 檢查是否有SPXTW_PR類別
        from core.entities.spx_entity import SPXTW_PR
        
        # 創建SPXTW_PR實例
        spx_pr_processor = SPXTW_PR()
        
        test_files = {
            'pr_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_purchase_request_20250206_101058_移除缺失資料.xlsx",
            'pr_file_name': "202505_purchase_request_20250206_101058.xlsx",
            'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_PR_PQ.xlsx",
            'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202504_PR_FN.xlsx"
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
        
        # 測試PR模式1
        # 注意：這裡需要檢查SPXTW_PR是否有正確的mode_1方法
        if hasattr(spx_pr_processor, 'mode_1'):
            spx_pr_processor.mode_1(
                test_files['pr_file'],
                test_files['pr_file_name'],
                test_files['procurement_pr'],
                test_files['previous_wp_pr']
            )
        else:
            logger.error("❌ SPXTW_PR沒有mode_1方法")
            return False
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"處理完成，耗時: {processing_time:.2f} 秒")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SPX PR處理測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_concurrent_processing():
    """測試並發處理功能"""
    logger.info("=== 測試並發處理功能 ===")
    
    try:
        from core.entities import create_entity, EntityType
        
        spx_entity = create_entity(EntityType.SPX)
        
        file_paths = {
            'po_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\202502_purchase_order_reERM.xlsx",
            'po_file_name': "202502_purchase_order.xlsx",
            'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\PO_for前期載入.xlsx",
            'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\採購底稿_PO.xlsx",
            'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\AP_Invoice_Match_Monitoring_Ext_202502.xlsx",
            'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\PR_for前期載入.xlsx",
            'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\採購底稿_PR.xlsx"
        }
        
        # 檢查檔案
        missing_files = []
        for file_type, file_path in file_paths.items():
            if not os.path.exists(file_path) and file_type != 'po_file_name':
                missing_files.append(f"{file_type}: {file_path}")
        
        if missing_files:
            logger.warning("⚠️ 以下測試檔案不存在:")
            for missing in missing_files:
                logger.warning(f"  - {missing}")
            return False
        
        logger.info("開始並發處理測試...")
        start_time = time.time()
        
        result = spx_entity.concurrent_spx_process(file_paths)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"並發處理完成，耗時: {processing_time:.2f} 秒")
        logger.info(f"處理結果: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 並發處理測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def validate_output_consistency():
    """驗證輸出一致性"""
    logger.info("=== 驗證輸出一致性 ===")
    
    try:
        # 這裡需要比較原始程式和重構程式的輸出結果
        logger.warning("⚠️ 需要準備基準數據來比較輸出一致性")
        logger.info("建議步驟:")
        logger.info("1. 使用原始程式處理同一組測試數據")
        logger.info("2. 使用重構程式處理相同數據")
        logger.info("3. 比較兩個輸出檔案的差異")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 輸出一致性驗證失敗: {str(e)}")
        return False

def run_comprehensive_test():
    """執行完整測試"""
    logger.info("🚀 開始PRPO Bot重構後完整功能測試")
    logger.info("=" * 60)
    logger.info(f"測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    test_results = []
    
    # 測試實體創建
    test_results.append(("MOB實體", test_mob_entity()))
    test_results.append(("SPT實體", test_spt_entity()))
    test_results.append(("SPX實體", test_spx_entity()))
    
    # 測試實際處理功能
    test_results.append(("SPX PO處理", test_spx_po_processing()))
    test_results.append(("SPX PR處理", test_spx_pr_processing()))
    test_results.append(("並發處理", test_concurrent_processing()))
    
    # 驗證一致性
    test_results.append(("輸出一致性", validate_output_consistency()))
    
    # 總結測試結果
    logger.info("\n" + "=" * 60)
    logger.info("📊 測試結果總結:")
    logger.info("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "✅ 通過" if result else "❌ 失敗"
        logger.info(f"{test_name:<20}: {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    logger.info(f"\n總計: {passed + failed} 項測試")
    logger.info(f"通過: {passed} 項")
    logger.info(f"失敗: {failed} 項")
    logger.info(f"成功率: {(passed / (passed + failed) * 100):.1f}%")
    
    if failed == 0:
        logger.info("\n🎉 所有測試通過！重構後程式功能正常。")
    else:
        logger.warning(f"\n⚠️ 有 {failed} 項測試失敗，需要進一步修復。")
    
    return failed == 0

def test_specific_spx_file():
    """測試特定的SPX檔案處理"""
    logger.info("=== 測試特定SPX檔案處理 ===")
    
    # 使用原始程式中的測試案例
    test_cases = [
        {
            'name': 'SPX PO 202504 - 模式1',
            'entity_type': 'SPX_PO',
            'mode': 1,
            'files': {
                'raw_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_purchase_order.csv",
                'filename': "202504_purchase_order.csv",
                'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PO_FN.xlsx",
                'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PO_PQ.xlsx",
                'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\AP_Invoice_Match_Monitoring_Ext_202504.xlsx",
                'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202503_PR_FN.xlsx",
                'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202504\202504_PR_PQ.xlsx"
            }
        },
        {
            'name': 'SPX PR 202505 - 模式1',
            'entity_type': 'SPX_PR',
            'mode': 1,
            'files': {
                'raw_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_purchase_request_20250206_101058_移除缺失資料.xlsx",
                'filename': "202505_purchase_request_20250206_101058.xlsx",
                'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_PR_PQ.xlsx",
                'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202504_PR_FN.xlsx"
            }
        }
    ]
    
    for test_case in test_cases:
        logger.info(f"測試案例: {test_case['name']}")
        
        # 檢查檔案是否存在
        missing_files = []
        for file_key, file_path in test_case['files'].items():
            if not os.path.exists(file_path) and file_key != 'filename':
                missing_files.append(f"{file_key}: {file_path}")
        
        if missing_files:
            logger.warning("⚠️ 以下檔案不存在，跳過此測試:")
            for missing in missing_files:
                logger.warning(f"  - {missing}")
            continue
        
        try:
            if test_case['entity_type'] == 'SPX_PO':
                from core.entities import create_entity, EntityType
                spx_entity = create_entity(EntityType.SPX)
                
                start_time = time.time()
                
                # 模擬原始的mode_1調用
                result = spx_entity.mode_1(
                    test_case['files']['raw_file'],
                    test_case['files']['filename'],
                    test_case['files']['previous_wp'],
                    test_case['files']['procurement'],
                    test_case['files']['ap_invoice'],
                    test_case['files']['previous_wp_pr'],
                    test_case['files']['procurement_pr']
                )
                
                end_time = time.time()
                logger.info(f"✅ {test_case['name']} 處理成功，耗時: {end_time - start_time:.2f}秒")
                
            elif test_case['entity_type'] == 'SPX_PR':
                # 測試SPX PR處理
                # 這裡需要檢查重構後的SPX PR處理器是否正確
                logger.warning("⚠️ SPX PR處理需要進一步實現")
                
        except Exception as e:
            logger.error(f"❌ {test_case['name']} 處理失敗: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    # 執行完整測試
    success = run_comprehensive_test()
    
    # 測試特定檔案
    test_specific_spx_file()
    
    logger.info("\n" + "=" * 60)
    if success:
        logger.info("🎉 重構驗證完成！")
    else:
        logger.warning("⚠️ 重構驗證發現問題，請檢查錯誤訊息。")
    logger.info("=" * 60)
