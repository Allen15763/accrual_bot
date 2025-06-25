import pandas as pd
import numpy as np
from typing import Tuple, Optional, List, Dict, Any

from base_pr_processor import SpxPRProcessor
from utils import Logger


class SPXTW_PR(SpxPRProcessor):
    """SPXTW公司PR處理器"""
    
    def __init__(self):
        """初始化SPXTW PR處理器"""
        super().__init__("SPX")
        self.logger = Logger().get_logger(self.__class__.__name__)
    
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_p_pr: str, fileUrl_previwp_pr: str):
        """模式1：處理原始數據+前期底稿+採購底稿
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_previwp_pr: 前期PR底稿文件路徑
            fileUrl_p_pr: 採購PR底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"SPXTW PR 模式1: {file_name}")
            self.process(fileUrl, file_name, fileUrl_p_pr, fileUrl_previwp_pr)
        except Exception as e:
            self.logger.error(f"SPXTW PR 模式1處理時出錯: {str(e)}", exc_info=True)
            raise
    

if __name__ == "__main__":
    test_processor = SPXTW_PR()
    import time
    start_time = time.time()
    print('Start time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
    file_name = "202505_purchase_request_20250206_101058.xlsx"
    file_path = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_purchase_request_20250206_101058_移除缺失資料.xlsx"
    file_path_p_pr = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202505_PR_PQ.xlsx"
    file_path_previwp_pr = r"C:\SEA\Accrual\prpo_bot\resources\SPX未結模組\raw_202505\For robot\202504_PR_FN.xlsx"

    # 會計用: PR + 自己的底稿 + 採購; test  pass
    test_processor.mode_1(file_path, file_name, file_path_p_pr, file_path_previwp_pr)
    end_time = time.time()
    print('End time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
    print('Total time:', end_time - start_time, 'seconds')
