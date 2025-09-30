"""
快速測試腳本
一鍵測試Pipeline系統
"""

import asyncio
import sys
import os
from pathlib import Path

# # 添加專案根目錄到 Python 路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# project_root = Path(__file__).parent.parent
# if str(project_root) not in sys.path:
#     sys.path.insert(0, str(project_root))

# # 切換到正確的工作目錄
# os.chdir(project_root)


async def quick_test():
    """快速測試主要功能"""
    print("\n" + "=" * 60)
    print(" " * 20 + "Pipeline 系統快速測試")
    print("=" * 60)
    
    # 1. 生成測試資料
    print("\n[1/3] 生成測試資料...")
    from accrual_bot.test_data_generator import TestDataGenerator
    
    generator = TestDataGenerator(output_dir="accrual_bot/test_data")
    test_data_dir = generator.generate_all_test_data()
    print(f"✅ 測試資料已生成至: {test_data_dir}")
    
    # 2. 測試基本Pipeline
    print("\n[2/3] 測試基本Pipeline功能...")
    # from accrual_bot.main_pipeline import AccrualPipelineManager
    from pipeline_main import AccrualPipelineManager
    
    manager = AccrualPipelineManager()
    
    # 測試MOB基本處理
    result = await manager.process_from_files(
        data_path="accrual_bot/test_data/sample_mob_po.xlsx",
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO",
        mode=2  # 基本模式
    )
    
    if result['success']:
        print("✅ MOB Pipeline測試成功")
        print(f"   - 處理時間: {result.get('duration', 0):.2f}秒")
        print(f"   - 執行步驟: {result.get('executed_steps', 0)}")
    else:
        print(f"❌ MOB Pipeline測試失敗: {result.get('error', 'Unknown error')}")
        return False
    
    # 3. 測試SPX特殊處理
    print("\n[3/3] 測試SPX特殊處理...")
    import pandas as pd
    
    spx_data = pd.read_excel("accrual_bot/test_data/sample_spx_po.xlsx")
    
    """
    # 預設模板C:\SEA\Accrual\prpo_bot\accrual_bot\accrual_bot\core\pipeline\templates.py, 
        步驟請參考SPXSpecialTemplate
    """
    result = await manager.process_with_template(
        template_name="SPX_Special",  
        data=spx_data,
        entity_type="SPX",
        processing_date=202410
    )
    
    if result['success']:
        print("✅ SPX特殊處理測試成功")
    else:
        print("❌ SPX特殊處理測試失敗")
        return False
    
    print("\n" + "=" * 60)
    print("✅ 所有測試通過！系統運作正常。")
    print("=" * 60)
    
    print("\n📚 下一步：")
    print("1. 執行 `python accrual_bot/examples.py` 查看完整範例")
    print("2. 執行 `python accrual_bot/examples.py 1` 執行特定範例")
    print("3. 查看 accrual_bot/test_data/ 目錄了解測試資料結構")
    print("4. 修改 accrual_bot/main.py 來處理您的實際資料")
    
    return True


def main():
    """主函數"""
    print("""
╔══════════════════════════════════════════════════════════╗
║           Accrual Bot Pipeline System v2.0              ║
║                   快速測試工具                           ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        # 執行測試
        success = asyncio.run(quick_test())
        
        if success:
            print("\n✨ 測試完成！系統已準備就緒。")
            return 0
        else:
            print("\n⚠️ 測試未完全通過，請檢查錯誤信息。")
            return 1
            
    except Exception as e:
        print(f"\n❌ 測試過程中發生錯誤：{str(e)}")
        import traceback
        traceback.print_exc()
        print("\n請確認：")
        print("1. Python版本 >= 3.8")
        print("2. 已安裝所需套件 (pandas, numpy, openpyxl等)")
        print("3. 檔案權限正確")
        return 2


if __name__ == "__main__":
    sys.exit(main())
