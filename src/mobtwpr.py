import pandas as pd
import numpy as np
from typing import Tuple, Optional, List, Dict, Any

from .base_pr_processor import BasePRProcessor
from .utils import Logger


class MOBTW_PR(BasePRProcessor):
    """MOBTW公司PR處理器"""
    
    def __init__(self):
        """初始化MOBTW PR處理器"""
        super().__init__("MOB")
        self.logger = Logger().get_logger(__name__)
    
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_p: str, fileUrl_c: str):
        """模式1：處理原始數據+採購底稿+關單清單
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PR 模式1: {file_name}")
            self.process(fileUrl, file_name, fileUrl_p, fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PR 模式1處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_2(self, fileUrl: str, file_name: str, fileUrl_c: str):
        """模式2：處理原始數據+關單清單
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PR 模式2: {file_name}")
            self.process(fileUrl, file_name, fileUrl_c=fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PR 模式2處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_3(self, fileUrl: str, file_name: str, fileUrl_p: str):
        """模式3：處理原始數據+採購底稿
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PR 模式3: {file_name}")
            self.process(fileUrl, file_name, fileUrl_p=fileUrl_p)
        except Exception as e:
            self.logger.error(f"MOBTW PR 模式3處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_4(self, fileUrl: str, file_name: str):
        """模式4：只處理原始數據
        
        Args:
            fileUrl: PR原始數據文件路徑
            file_name: PR原始數據文件名
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PR 模式4: {file_name}")
            self.process(fileUrl, file_name)
        except Exception as e:
            self.logger.error(f"MOBTW PR 模式4處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_5(self, *args):
        """模式5：處理原始數據，並根據參數數量選擇不同處理方式
        
        Args:
            *args: 可變參數，根據數量決定處理方式
            
        Returns:
            None
        """
        try:
            if len(args) == 5:
                fileUrl, file_name, fileUrl_p, fileUrl_c, fileUrl_previwp = args
                self.logger.info(f"MOBTW PR 模式5(5參數): {file_name}")
                self.process(fileUrl, file_name, fileUrl_p, fileUrl_c, fileUrl_previwp)
            elif len(args) == 4:
                fileUrl, file_name, fileUrl_p, fileUrl_previwp = args
                self.logger.info(f"MOBTW PR 模式5(4參數): {file_name}")
                self.process(fileUrl, file_name, fileUrl_p=fileUrl_p, fileUrl_previwp=fileUrl_previwp)
            elif len(args) == 3:
                fileUrl, file_name, fileUrl_previwp = args
                self.logger.info(f"MOBTW PR 模式5(3參數): {file_name}")
                self.process(fileUrl, file_name, fileUrl_previwp=fileUrl_previwp)
            else:
                self.logger.error("不支持的參數數量")
                raise NotImplementedError("沒有關單組合:fileUrl, file_name, fileUrl_c, fileUrl_previwp, go ERROR.")
        except Exception as e:
            self.logger.error(f"MOBTW PR 模式5處理時出錯: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    test_processor = MOBTW_PR()
    file_name = "202501_purchase_request_20252201_173708.csv"
    file_path = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\202501_purchase_request_20252201_173708.csv"
    file_path_p = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\未結PO 202501 MOBTW.xlsx"
    file_path_previwp = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\PR_for前期載入.xlsx"

    # 採購用: PR + 自己的底稿 + (關單)OPTIONAL; tess pass
    # test_processor.mode_3(file_path, file_name, file_path_p)

    # 會計用: PR + 採購; test pass
    # test_processor.mode_5(file_path, file_name, file_path_p)

    # 會計用: PR + 自己的底稿; test NA

    # 會計用: PR + 自己的底稿 + 採購; test pass
    # test_processor.mode_5(file_path, file_name, file_path_p, file_path_previwp)