"""
Pipeline 主執行入口
統一的Pipeline執行介面
"""

import asyncio
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging
from datetime import datetime


from accrual_bot.core.pipeline.pipeline import PipelineExecutor
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
from accrual_bot.core.pipeline.entity_strategies import EntityStrategyFactory, AdaptivePipelineManager
from accrual_bot.core.pipeline.templates import PipelineTemplateManager


# DataSourceFactory - 簡化版本
class DataSourceFactory:
    def create_source(self, source_type, **kwargs):
        # 簡化實作，直接返回一個簡單的讀取器
        return SimpleDataSource(**kwargs)

class SimpleDataSource:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    
    async def read_data(self, **kwargs):
        import pandas as pd
        file_path = self.kwargs.get('file_path') or self.kwargs.get('db_path')
        if file_path and file_path.endswith('.xlsx'):
            return pd.read_excel(file_path)
        elif file_path and file_path.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path and file_path.endswith('.parquet'):
            return pd.read_parquet(file_path)
        else:
            return pd.DataFrame()


class AccrualPipelineManager:
    """
    主Pipeline管理器
    提供統一的執行介面
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化管理器
        
        Args:
            config_path: 配置文件路徑
        """
        # 初始化配置管理器
        self.config_manager = PipelineConfigManager(config_path)
        
        # 初始化策略工廠
        EntityStrategyFactory.initialize(self.config_manager)
        
        # 初始化模板管理器
        self.template_manager = PipelineTemplateManager()
        
        # 初始化Pipeline執行器
        self.executor = PipelineExecutor()
        
        # 初始化自適應管理器
        self.adaptive_manager = AdaptivePipelineManager(self.config_manager)
        
        # 設置日誌
        self.logger = logging.getLogger("AccrualPipelineManager")
        
        # 數據源工廠
        self.datasource_factory = DataSourceFactory()
    
    async def process_from_db(self,
                              entity_type: str,
                              processing_date: int,
                              processing_type: str = "PO",
                              mode: int = 1,
                              **kwargs) -> Dict[str, Any]:
        """
        從數據庫處理數據
        
        Args:
            entity_type: 實體類型 (MOB/SPT/SPX)
            processing_date: 處理日期 (YYYYMM)
            processing_type: 處理類型 (PO/PR)
            mode: 處理模式 (1-5)
            **kwargs: 其他參數
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        try:
            self.logger.info(f"Starting {entity_type} {processing_type} processing for {processing_date}")
            
            # 1. 從DuckDB獲取主數據
            main_data = await self._load_main_data_from_db(
                entity_type, 
                processing_date, 
                processing_type
            )
            
            # 2. 創建處理上下文
            context = ProcessingContext(
                data=main_data,
                entity_type=entity_type,
                processing_date=processing_date,
                processing_type=processing_type
            )
            
            # 3. 載入輔助數據
            await self._load_auxiliary_data(context, **kwargs)
            
            # 4. 創建Pipeline
            pipeline = self._create_pipeline(entity_type, processing_type, mode)
            
            # 5. 執行Pipeline
            result = await pipeline.execute(context)
            
            # 6. 保存結果
            if result['success'] and kwargs.get('save_results', True):
                await self._save_results(context, result)
            
            self.logger.info(f"Processing completed: {result['success']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    async def process_from_files(self,
                                 data_path: str,
                                 entity_type: str,
                                 processing_date: int,
                                 processing_type: str = "PO",
                                 mode: Optional[int] = None,
                                 **kwargs) -> Dict[str, Any]:
        """
        從文件處理數據
        
        Args:
            data_path: 數據文件路徑
            entity_type: 實體類型
            processing_date: 處理日期
            processing_type: 處理類型
            mode: 處理模式（可選，None則自動選擇）
            **kwargs: 其他參數
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        try:
            # 1. 載入主數據
            main_data = await self._load_data_from_file(data_path)
            
            # 2. 創建處理上下文
            context = ProcessingContext(
                data=main_data,
                entity_type=entity_type,
                processing_date=processing_date,
                processing_type=processing_type
            )
            
            # 3. 載入輔助數據文件
            if 'auxiliary_files' in kwargs:
                for name, path in kwargs['auxiliary_files'].items():
                    aux_data = await self._load_data_from_file(path)
                    context.add_auxiliary_data(name, aux_data)
            
            # 4. 創建Pipeline（自適應或指定模式）
            if mode is None:
                # 自適應選擇
                pipeline = await self.adaptive_manager.create_adaptive_pipeline(context)
                self.logger.info(f"Auto-selected mode: {context.get_variable('selected_mode')}")
            else:
                # 指定模式
                pipeline = self._create_pipeline(entity_type, processing_type, mode)
            
            # 5. 執行Pipeline
            result = await pipeline.execute(context)
            
            return result
            
        except Exception as e:
            self.logger.error(f"File processing failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    async def process_with_template(self,
                                    template_name: str,
                                    data: pd.DataFrame,
                                    entity_type: str,
                                    processing_date: int,
                                    **kwargs) -> Dict[str, Any]:
        """
        使用模板處理數據
        
        Args:
            template_name: 模板名稱
            data: 輸入數據
            entity_type: 實體類型
            processing_date: 處理日期
            **kwargs: 模板參數
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        try:
            # 1. 創建上下文
            context = ProcessingContext(
                data=data,
                entity_type=entity_type,
                processing_date=processing_date,
                processing_type=kwargs.get('processing_type', 'PO')
            )
            
            # 2. 從模板創建Pipeline
            pipeline = self.template_manager.create_from_template(
                template_name,
                entity_type,
                **kwargs
            )
            
            # 3. 註冊Pipeline
            self.executor.register_pipeline(pipeline)
            
            # 4. 執行
            result = await self.executor.execute_pipeline(
                pipeline.config.name,
                data,
                processing_date,
                **kwargs
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Template processing failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def _create_pipeline(self, entity_type: str, processing_type: str, mode: int):
        """創建Pipeline"""
        return EntityStrategyFactory.create_pipeline(entity_type, processing_type, mode)
    
    async def _load_main_data_from_db(self, 
                                      entity_type: str,
                                      processing_date: int,
                                      processing_type: str) -> pd.DataFrame:
        """從數據庫載入主數據"""
        # 創建DuckDB數據源
        db_source = self.datasource_factory.create_source(
            'duckdb',
            db_path=f'accrual_bot/test_data/{entity_type.lower()}_prpo.db'
        )
        
        # 構建查詢
        table_name = f"{processing_type.lower()}_{processing_date}"
        query = f"SELECT * FROM {table_name} WHERE entity_type = '{entity_type}'"
        
        # 執行查詢
        data = await db_source.read_data(query=query)
        return data
    
    async def _load_data_from_file(self, file_path: str) -> pd.DataFrame:
        """從文件載入數據"""
        path = Path(file_path)
        
        # 根據文件類型選擇數據源
        if path.suffix == '.xlsx':
            source = self.datasource_factory.create_source('excel', file_path=file_path)
        elif path.suffix == '.csv':
            source = self.datasource_factory.create_source('csv', file_path=file_path)
        elif path.suffix == '.parquet':
            source = self.datasource_factory.create_source('parquet', file_path=file_path)
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")
        
        return await source.read_data()
    
    async def _load_auxiliary_data(self, context: ProcessingContext, **kwargs):
        """載入輔助數據"""
        # 採購底稿
        if kwargs.get('load_procurement', False):
            procurement_data = await self._load_data_from_file(
                'accrual_bot/test_data/procurement_workpaper.parquet'
            )
            context.add_auxiliary_data('procurement', procurement_data)
        
        # 會計底稿
        if kwargs.get('load_accounting', False):
            accounting_data = await self._load_data_from_file(
                'accrual_bot/test_data/accounting_workpaper.parquet'
            )
            context.add_auxiliary_data('accounting', accounting_data)
        
        # 上期底稿
        if kwargs.get('load_previous', False):
            previous_data = await self._load_data_from_file(
                f'accrual_bot/test_data/previous_{context.metadata.processing_date - 1}.parquet'
            )
            context.add_auxiliary_data('previous_workpaper', previous_data)
        
        # SPX關單清單
        if context.metadata.entity_type == "SPX" and kwargs.get('load_closing', False):
            closing_data = await self._load_data_from_file(
                'accrual_bot/test_data/spx_closing_list.xlsx'
            )
            context.add_auxiliary_data('closing_list', closing_data)
    
    async def _save_results(self, context: ProcessingContext, result: Dict[str, Any]):
        """保存處理結果"""
        # 生成輸出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        entity = context.metadata.entity_type
        proc_type = context.metadata.processing_type
        date = context.metadata.processing_date
        
        output_path = f"output/{entity}/{proc_type}_{date}_{timestamp}.parquet"
        
        # 創建輸出目錄
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 保存數據
        context.data.to_parquet(output_path, index=False)
        
        # 保存元數據
        metadata_path = output_path.replace('.parquet', '_metadata.json')
        import json
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump({
                'result': result,
                'context': context.to_dict(),
                'errors': context.errors,
                'warnings': context.warnings
            }, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to {output_path}")


async def main():
    """主函數示例"""
    # 初始化管理器
    manager = AccrualPipelineManager()
    
    # 示例1：從數據庫處理MOB PO數據
    result1 = await manager.process_from_db(
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO",
        mode=1,
        load_previous=True,
        save_results=True
    )
    print(f"MOB PO Processing: {result1['success']}")
    
    # 示例2：從文件處理SPT數據（自適應模式）
    result2 = await manager.process_from_files(
        data_path="accrual_bot/test_data/SPT_PO_202410.xlsx",
        entity_type="SPT",
        processing_date=202410,
        processing_type="PO",
        mode=None,  # 自動選擇
        auxiliary_files={
            'procurement': 'accrual_bot/test_data/procurement.xlsx',
            'previous_workpaper': 'accrual_bot/test_data/previous_202409.xlsx'
        }
    )
    print(f"SPT PO Processing (adaptive): {result2['success']}")
    
    # 示例3：使用模板處理SPX數據
    spx_data = pd.read_excel("accrual_bot/test_data/SPX_PO_202410.xlsx")
    result3 = await manager.process_with_template(
        template_name="SPX_Special",
        data=spx_data,
        entity_type="SPX",
        processing_date=202410,
        processing_type="PO",
        require_validation=True,
        export_format="excel"
    )
    print(f"SPX Special Processing: {result3['success']}")


if __name__ == "__main__":
    # 設置日誌
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 執行主函數
    asyncio.run(main())
