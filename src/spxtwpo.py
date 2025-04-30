import pandas as pd
import numpy as np
from typing import Tuple, Optional, List, Dict, Any

from base_po_processor import SpxPOProcessor
from utils import Logger


class SPXTW_PO(SpxPOProcessor):
    """SPXTW公司PO處理器"""
    
    def __init__(self):
        """初始化SPXTW PO處理器"""
        super().__init__("SPX")
        self.logger = Logger().get_logger(self.__class__.__name__)
    
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str, fileUrl_ap: str, fileUrl_previwp_pr: str, fileUrl_p_pr: str):
        """模式1：處理原始數據+前期底稿+採購底稿+關單清單
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            fileUrl_ap: AP invoice文件路徑

            fileUrl_previwp_pr: 前期PR底稿文件路徑
            fileUrl_p_pr: 採購PR底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"SPXTW PO 模式1: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp, fileUrl_p, fileUrl_ap, fileUrl_previwp_pr, fileUrl_p_pr)
        except Exception as e:
            self.logger.error(f"SPXTW PO 模式1處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_2(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str):
        """模式2：處理原始數據+前期底稿+採購底稿
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"SPXTW PO 模式2: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp, fileUrl_p)
        except Exception as e:
            self.logger.error(f"SPXTW PO 模式2處理時出錯: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    test_processor = SPXTW_PO()
    import time
    start_time = time.time()
    print('Start time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
    file_name = "202503_purchase_order.csv"
    file_path = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\202503_purchase_order.csv"
    file_path_p = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\202503_PO_PQ.xlsx"
    file_path_p_pr = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\202503_PR_PQ.xlsx"
    file_path_previwp = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\202502_PO_FN.xlsx"
    file_path_previwp_pr = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\202502_PR_FN.xlsx"
    file_path_ap = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\sec_test\AP_Invoice_Match_Monitoring_Ext_202503.xlsx"

    # 採購用: PO + 自己的底稿 + (關單)OPTIONAL; test NA     4/5差關單 採購在SPT作業 這邊無須採購路徑
    # test_processor.mode_5(file_path, file_name, file_path_p)

    # 會計用: PO + 自己的底稿 + 採購; test  pass
    test_processor.mode_1(file_path, file_name, file_path_previwp, file_path_p, file_path_ap, 
                          file_path_previwp_pr, file_path_p_pr)
    end_time = time.time()
    print('End time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
    print('Total time:', end_time - start_time, 'seconds')
    
    # # 使用 concurrent_spx_process 方法
    # import time
    # start_time = time.time()
    # print('Start time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
    # file_paths = {
    #     'po_file': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\202502_purchase_order_reERM.xlsx",
    #     'po_file_name': "202502_purchase_order.xlsx",
    #     'previous_wp': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\PO_for前期載入.xlsx",
    #     'procurement': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\採購底稿_PO.xlsx",
    #     'ap_invoice': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\AP_Invoice_Match_Monitoring_Ext_202502.xlsx",
    #     'previous_wp_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\PR_for前期載入.xlsx",
    #     'procurement_pr': r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_test\採購底稿_PR.xlsx"
    # }
    # test_processor.concurrent_spx_process(file_paths)
    # end_time = time.time()
    # print('End time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
    # print('Total time:', end_time - start_time, 'seconds')