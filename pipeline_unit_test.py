#!/usr/bin/env python
"""
Pipeline 系統測試腳本
從專案根目錄執行：python test_pipeline_system.py
"""

import sys
import os
from pathlib import Path
import asyncio
import logging

# 設置路徑和日誌
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def print_header(title):
    """打印標題"""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def test_imports():
    """測試模組導入"""
    print_header("測試模組導入")
    
    modules_to_test = [
        ("Pipeline基礎", "accrual_bot.core.pipeline.base"),
        ("Pipeline主類", "accrual_bot.core.pipeline.pipeline"),
        ("處理上下文", "accrual_bot.core.pipeline.context"),
        ("配置管理器", "accrual_bot.core.pipeline.config_manager"),
        ("實體策略", "accrual_bot.core.pipeline.entity_strategies"),
        ("Pipeline模板", "accrual_bot.core.pipeline.templates"),
    ]
    
    all_success = True
    for name, module in modules_to_test:
        try:
            __import__(module)
            print(f"✅ {name}: {module}")
        except ImportError as e:
            print(f"❌ {name}: {e}")
            all_success = False
    
    return all_success

def generate_test_data():
    """生成測試資料"""
    print_header("生成測試資料")
    
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import random
    
    # 創建測試目錄
    test_dir = Path("accrual_bot/test_pipeline_data")
    test_dir.mkdir(exist_ok=True)
    
    # 生成MOB測試資料
    mob_data = []
    for i in range(30):
        mob_data.append({
            'PO#': f'PO-MOB-{str(i+1).zfill(5)}',
            'Supplier': f'供應商{random.choice(["A", "B", "C", "D"])}',
            'Department': random.choice(['MOB_HQ', 'MOB_OPS', 'MOB001']),
            'GL#': random.choice(['151101', '622101', '711101']),
            'Item Description': f'採購項目 {datetime(2024, 10, 1).strftime("%Y/%m")}',
            'Entry Amount': round(random.uniform(5000, 50000), 2),
            'Entry Quantity': random.randint(1, 50),
            'Received Quantity': random.randint(0, 50),
            'PO Create Date': '2024-09-15',
            'Expected Receive Month': 'Oct-24',
            'Submission Date': '15-Sep-24',
            'Status': random.choice(['Open', 'Closed', '']),
            '採購備註': random.choice(['', '已確認'])
        })
    
    mob_df = pd.DataFrame(mob_data)
    mob_file = test_dir / "mob_test.xlsx"
    mob_df.to_excel(mob_file, index=False)
    print(f"✅ MOB測試資料: {mob_file} ({len(mob_df)} 筆)")
    
    # 生成SPX測試資料（含特殊項目）
    spx_data = []
    for i in range(40):
        if i % 10 == 0:  # 押金項目
            spx_data.append({
                'PO#': f'PO-SPX-{str(i+1).zfill(5)}',
                'Supplier': '房東公司',
                'Department': 'SPX001',
                'GL#': '199999',
                'Item Description': '租賃押金',
                'Entry Amount': 100000,
                'Entry Quantity': 1,
                'Received Quantity': 1,
                'PO Create Date': '2024-09-01',
                'Expected Receive Month': 'Sep-24',
                'Submission Date': '01-Sep-24',
                'Status': 'Closed',
                '採購備註': ''
            })
        elif i % 10 == 1:  # Kiosk
            spx_data.append({
                'PO#': f'PO-SPX-{str(i+1).zfill(5)}',
                'Supplier': '益欣資訊股份有限公司',
                'Department': 'ShopeeOPS07',
                'GL#': '151101',
                'Item Description': 'Kiosk機台採購',
                'Entry Amount': 150000,
                'Entry Quantity': 1,
                'Received Quantity': 0,
                'PO Create Date': '2024-09-10',
                'Expected Receive Month': 'Oct-24',
                'Submission Date': '10-Sep-24',
                'Status': 'Open',
                '採購備註': '待驗收'
            })
        else:  # 一般項目
            spx_data.append({
                'PO#': f'PO-SPX-{str(i+1).zfill(5)}',
                'Supplier': f'供應商{random.randint(1, 5)}',
                'Department': 'SPX_HQ',
                'GL#': random.choice(['622101', '711101']),
                'Item Description': '一般採購',
                'Entry Amount': round(random.uniform(10000, 80000), 2),
                'Entry Quantity': random.randint(1, 20),
                'Received Quantity': random.randint(0, 20),
                'PO Create Date': '2024-09-20',
                'Expected Receive Month': 'Oct-24',
                'Submission Date': '20-Sep-24',
                'Status': random.choice(['Open', '']),
                '採購備註': ''
            })
    
    spx_df = pd.DataFrame(spx_data)
    spx_file = test_dir / "spx_test.xlsx"
    spx_df.to_excel(spx_file, index=False)
    print(f"✅ SPX測試資料: {spx_file} ({len(spx_df)} 筆)")
    
    return str(mob_file), str(spx_file)

async def test_basic_pipeline():
    """測試基本Pipeline功能"""
    print_header("測試基本Pipeline功能")
    
    from accrual_bot.core.pipeline.pipeline import Pipeline, PipelineBuilder
    from accrual_bot.core.pipeline.context import ProcessingContext
    from accrual_bot.core.pipeline.steps.common import DataCleaningStep, ValidationStep
    
    import pandas as pd
    
    # 載入測試資料
    test_data = pd.read_excel("accrual_bot/test_pipeline_data/mob_test.xlsx")
    print(f"載入測試資料: {len(test_data)} 筆")
    
    # 創建Pipeline
    builder = PipelineBuilder("BasicTestPipeline", "MOB")
    builder.add_step(DataCleaningStep(name="Clean"))
    builder.add_step(ValidationStep(name="Validate", validations=['required_columns']))
    
    pipeline = builder.build()
    print(f"創建Pipeline: {pipeline.config.name}")
    
    # 創建上下文
    context = ProcessingContext(
        data=test_data,
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO"
    )
    
    # 執行Pipeline
    result = await pipeline.execute(context)
    
    if result['success']:
        print("✅ Pipeline執行成功")
        print(f"   執行時間: {result['duration']:.2f}秒")
        print(f"   成功步驟: {result['successful_steps']}")
    else:
        print(f"❌ Pipeline執行失敗: {result.get('error')}")
    
    return result['success']

async def test_entity_strategy():
    """測試實體策略"""
    print_header("測試實體策略")
    
    from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
    from accrual_bot.core.pipeline.entity_strategies import EntityStrategyFactory
    import pandas as pd
    
    # 初始化配置
    config_manager = PipelineConfigManager()
    EntityStrategyFactory.initialize(config_manager)
    
    # 測試MOB策略
    print("\n測試MOB策略...")
    mob_pipeline = EntityStrategyFactory.create_pipeline("MOB", "PO", mode=2)
    print(f"✅ MOB Pipeline創建成功: {mob_pipeline.config.name}")
    
    # 測試SPX策略
    print("\n測試SPX策略...")
    spx_pipeline = EntityStrategyFactory.create_pipeline("SPX", "PO", mode=5)
    print(f"✅ SPX Pipeline創建成功: {spx_pipeline.config.name}")
    
    return True

async def test_template():
    """測試Pipeline模板"""
    print_header("測試Pipeline模板")
    
    from accrual_bot.core.pipeline.templates import PipelineTemplateManager
    import pandas as pd
    
    # 創建模板管理器
    template_manager = PipelineTemplateManager()
    
    # 列出可用模板
    templates = template_manager.list_templates()
    print(f"可用模板數量: {len(templates)}")
    for tmpl in templates:
        print(f"  - {tmpl['name']}: {tmpl['description']}")
    
    # 測試創建Pipeline
    pipeline = template_manager.create_from_template(
        "Standard_PO",
        "MOB",
        export=False
    )
    print(f"\n✅ 從模板創建Pipeline成功: {pipeline.config.name}")
    
    return True

async def main():
    """主測試函數"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║          Accrual Bot Pipeline System v2.0                   ║
║                    完整系統測試                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    all_pass = True
    
    # 1. 測試導入
    if not test_imports():
        print("\n⚠️ 部分模組導入失敗")
        all_pass = False
    
    # 2. 生成測試資料
    try:
        mob_file, spx_file = generate_test_data()
    except Exception as e:
        print(f"\n❌ 生成測試資料失敗: {e}")
        all_pass = False
    
    # 3. 測試基本Pipeline
    try:
        if not await test_basic_pipeline():
            all_pass = False
    except Exception as e:
        print(f"\n❌ 基本Pipeline測試失敗: {e}")
        import traceback
        traceback.print_exc()
        all_pass = False
    
    # 4. 測試實體策略
    try:
        if not await test_entity_strategy():
            all_pass = False
    except Exception as e:
        print(f"\n❌ 實體策略測試失敗: {e}")
        import traceback
        traceback.print_exc()
        all_pass = False
    
    # 5. 測試模板
    try:
        if not await test_template():
            all_pass = False
    except Exception as e:
        print(f"\n❌ 模板測試失敗: {e}")
        import traceback
        traceback.print_exc()
        all_pass = False
    
    # 總結
    print_header("測試總結")
    
    if all_pass:
        print("✅ 所有測試通過！Pipeline系統運作正常。")
        print("\n建議下一步：")
        print("1. 查看 accrual_bot/test_pipeline_data/ 目錄中的測試資料")
        print("2. 執行 python accrual_bot/pipeline_examples.py 查看更多範例")
        print("3. 使用實際資料測試完整流程")
        return 0
    else:
        print("⚠️ 部分測試失敗，請檢查錯誤信息")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

    # # 創建驗證腳本；確認配置不同了
    # def verify_config_consistency():
    #     # 比較兩個配置管理器的輸出
    #     from accrual_bot.utils import config_manager as old_cm
    #     from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
    #     new_cm = PipelineConfigManager()
        
    #     # 驗證FA帳戶
    #     for entity in ['MOB', 'SPT', 'SPX']:
    #         old_fa = old_cm.get_fa_accounts(entity.lower())
    #         new_fa = new_cm.get_entity_config(entity).fa_accounts
    #         assert old_fa == new_fa, f"{entity} FA帳戶不一致"

    # verify_config_consistency()
