import pandas as pd
import numpy as np
from typing import Tuple, Optional, List, Dict, Any

from base_po_processor import BasePOProcessor
from utils import Logger


class MOBTW_PO(BasePOProcessor):
    """MOBTW公司PO處理器"""
    
    def __init__(self):
        """初始化MOBTW PO處理器"""
        super().__init__("MOB")
        self.logger = Logger().get_logger(__name__)
    
    def mode_1(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_p: str, fileUrl_c: str):
        """模式1：處理原始數據+前期底稿+採購底稿+關單清單
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_p: 採購底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式1: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp, fileUrl_p, fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式1處理時出錯: {str(e)}", exc_info=True)
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
            self.logger.info(f"MOBTW PO 模式2: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp, fileUrl_p)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式2處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_3(self, fileUrl: str, file_name: str, fileUrl_previwp: str, 
               fileUrl_c: str):
        """模式3：處理原始數據+前期底稿+關單清單
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式3: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp, fileUrl_c=fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式3處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_4(self, fileUrl: str, file_name: str, fileUrl_p: str, 
               fileUrl_c: str):
        """模式4：處理原始數據+採購底稿+關單清單
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式4: {file_name}")
            self.process(fileUrl, file_name, fileUrl_p=fileUrl_p, fileUrl_c=fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式4處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_5(self, fileUrl: str, file_name: str, fileUrl_p: str):
        """模式5：處理原始數據+採購底稿
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_p: 採購底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式5: {file_name}")
            self.process(fileUrl, file_name, fileUrl_p=fileUrl_p)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式5處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_6(self, fileUrl: str, file_name: str, fileUrl_previwp: str):
        """模式6：處理原始數據+前期底稿
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_previwp: 前期底稿文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式6: {file_name}")
            self.process(fileUrl, file_name, fileUrl_previwp)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式6處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_7(self, fileUrl: str, file_name: str, fileUrl_c: str):
        """模式7：處理原始數據+關單清單
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            fileUrl_c: 關單清單文件路徑
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式7: {file_name}")
            self.process(fileUrl, file_name, fileUrl_c=fileUrl_c)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式7處理時出錯: {str(e)}", exc_info=True)
            raise
    
    def mode_8(self, fileUrl: str, file_name: str):
        """模式8：只處理原始數據
        
        Args:
            fileUrl: PO原始數據文件路徑
            file_name: PO原始數據文件名
            
        Returns:
            None
        """
        try:
            self.logger.info(f"MOBTW PO 模式8: {file_name}")
            self.process(fileUrl, file_name)
        except Exception as e:
            self.logger.error(f"MOBTW PO 模式8處理時出錯: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    test_processor = MOBTW_PO()
    file_name = "202501_purchase_order_20252201_173718.csv"
    file_path = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\202501_purchase_order_20252201_173718.csv"
    file_path_p = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\採購底稿PO-未結PO 202501 MOBTW.xlsx"
    file_path_previwp = r"C:\SEA\MOB PRPO re\頂一下\202501\MOBA\raw\PO_for前期載入.xlsx"

    # 採購用: PO + 自己的底稿 + (關單)OPTIONAL; tess pass
    test_processor.mode_5(file_path, file_name, file_path_p)

    # 會計用: PO + 採購; test 沒再用的模式  不做
    # test_processor.mode_5(file_path, file_name, file_path_p)

    # 會計用: PO + 自己的底稿 + 採購; test  pass
    # test_processor.mode_2(file_path, file_name, file_path_previwp, file_path_p)