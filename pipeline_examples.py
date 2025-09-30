"""
Pipeline使用範例
展示各種Pipeline的使用方法
自動生成測試資料，無需手動準備
"""

import asyncio
import pandas as pd
from datetime import datetime
import logging
import sys
import os
from pathlib import Path

# 確保在正確的目錄
script_dir = Path(__file__).parent
os.chdir(script_dir)

# 添加專案路徑
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from accrual_bot.test_data_generator import TestDataGenerator
from pipeline_main import AccrualPipelineManager
from accrual_bot.core.pipeline.context import ProcessingContext
from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
from accrual_bot.core.pipeline.entity_strategies import EntityStrategyFactory
from accrual_bot.core.pipeline.templates import PipelineTemplateManager


# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/examples_pipeline.log'
)


async def setup_test_data():
    """
    設置測試資料
    在執行範例前自動生成所需的測試資料
    """
    print("\n" + "=" * 60)
    print("準備測試環境...")
    print("=" * 60)
    
    # 生成測試資料
    generator = TestDataGenerator(output_dir="accrual_bot/test_data")
    test_data_dir = generator.generate_all_test_data()
    
    print("\n✅ 測試資料準備完成")
    print(f"📁 資料位置: {test_data_dir}")
    
    return test_data_dir


async def example_1_basic_processing():
    """
    範例1：基本處理流程
    最簡單的使用方式
    """
    print("\n" + "=" * 50)
    print("範例1：基本MOB PO處理")
    print("=" * 50)
    
    # 創建管理器
    manager = AccrualPipelineManager()
    
    # 載入測試資料
    data = pd.read_excel("accrual_bot/test_data/sample_mob_po.xlsx")
    print(f"載入資料: {len(data)} 筆 MOB PO 記錄")
    
    # 創建處理上下文
    context = ProcessingContext(
        data=data,
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO"
    )
    
    # 使用模式2（基本處理）創建Pipeline
    pipeline = manager._create_pipeline("MOB", "PO", mode=2)
    
    # 執行Pipeline
    result = await pipeline.execute(context)
    
    # 顯示結果
    print(f"處理結果：{'成功' if result['success'] else '失敗'}")
    print(f"執行時間：{result['duration']:.2f}秒")
    print(f"處理行數：{len(context.data)}")
    print(f"成功步驟：{result.get('successful_steps', 0)}")
    print(f"失敗步驟：{result.get('failed_steps', 0)}")
    
    # 顯示詳細步驟結果
    if 'results' in result and result['results']:
        print("\n步驟執行詳情：")
        for step_result in result['results'][:5]:  # 只顯示前5個
            status_emoji = "✅" if step_result['status'] == 'success' else "❌"
            print(f"  {status_emoji} {step_result['step_name']}: {step_result['status']}")
    
    return result


async def example_2_with_auxiliary_data():
    """
    範例2：含輔助資料的處理
    整合採購底稿和上期底稿
    """
    print("\n" + "=" * 50)
    print("範例2：SPT PO處理（含輔助資料）")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 從文件處理，包含輔助資料
    result = await manager.process_from_files(
        data_path="accrual_bot/test_data/sample_spt_po.xlsx",
        entity_type="SPT",
        processing_date=202410,
        processing_type="PO",
        mode=1,  # 完整處理模式
        auxiliary_files={
            'procurement': 'accrual_bot/test_data/procurement.xlsx',
            'previous_workpaper': 'accrual_bot/test_data/previous_workpaper.xlsx'
        }
    )
    
    print(f"處理結果：{'成功' if result['success'] else '失敗'}")
    print(f"執行步驟數：{result.get('executed_steps', 0)}")
    print(f"處理時間：{result.get('duration', 0):.2f}秒")
    
    # 顯示各步驟結果
    if 'results' in result and result['results']:
        print("\n步驟執行詳情：")
        success_count = sum(1 for r in result['results'] if r['status'] == 'success')
        failed_count = sum(1 for r in result['results'] if r['status'] == 'failed')
        skipped_count = sum(1 for r in result['results'] if r['status'] == 'skipped')
        
        print(f"  成功: {success_count} | 失敗: {failed_count} | 跳過: {skipped_count}")
        
        # 顯示失敗的步驟
        if failed_count > 0:
            print("\n  失敗步驟：")
            for step_result in result['results']:
                if step_result['status'] == 'failed':
                    print(f"    ❌ {step_result['step_name']}: {step_result.get('message', '')}")
    
    return result


async def example_3_spx_special_processing():
    """
    範例3：SPX特殊處理
    包含押金、租金、資產驗收等複雜邏輯
    """
    print("\n" + "=" * 50)
    print("範例3：SPX特殊處理流程")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 載入SPX測試資料
    data = pd.read_excel("accrual_bot/test_data/sample_spx_po.xlsx")
    print(f"載入資料: {len(data)} 筆 SPX PO 記錄")
    
    # 檢查特殊項目
    if 'Item Description' in data.columns:
        deposit_count = data['Item Description'].str.contains('押金|保證金|Deposit', na=False).sum()
        rent_count = data['Item Description'].str.contains('租金|Rent', na=False).sum()
        kiosk_count = data['Item Description'].str.contains('Kiosk', na=False).sum()
        locker_count = data['Item Description'].str.contains('Locker', na=False).sum()
        
        print("\n特殊項目分布：")
        print(f"  - 押金項目: {deposit_count}")
        print(f"  - 租金項目: {rent_count}")
        print(f"  - Kiosk設備: {kiosk_count}")
        print(f"  - Locker設備: {locker_count}")
    
    # 使用SPX特殊模板
    result = await manager.process_with_template(
        template_name="SPX_Special",
        data=data,
        entity_type="SPX",
        processing_date=202410,
        processing_type="PO",
        deposit_keywords='押金|保證金|Deposit',
        require_validation=True,
        export_format="excel",
        output_path="output/spx"
    )
    
    print(f"\n處理結果：{'成功' if result['success'] else '失敗'}")
    
    # 顯示特殊處理統計
    if result['success']:
        if 'output_data' in result:
            output_data = result['output_data']
            print("\n處理後統計：")
            
            # 統計各種標記
            if '押金標記' in output_data.columns:
                deposit_marked = (output_data['押金標記'] == 'Y').sum()
                print(f"  - 識別為押金: {deposit_marked}")
            
            if '租金標記' in output_data.columns:
                rent_marked = (output_data['租金標記'] == 'Y').sum()
                print(f"  - 識別為租金: {rent_marked}")
            
            if '驗收狀態' in output_data.columns:
                validation_status = output_data['驗收狀態'].value_counts()
                print("  - 驗收狀態分布:")
                for status, count in validation_status.items():
                    print(f"    • {status}: {count}")
    
    return result


async def example_4_adaptive_mode():
    """
    範例4：自適應模式選擇
    系統自動根據數據特徵選擇最佳處理模式
    """
    print("\n" + "=" * 50)
    print("範例4：自適應模式處理")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 測試不同大小的資料集
    test_cases = [
        ("accrual_bot/test_data/sample_mob_po.xlsx", "MOB", 50),   # 小資料集
        ("accrual_bot/test_data/mob_po_202410.xlsx", "MOB", 100),  # 中等資料集
        ("accrual_bot/test_data/sample_data.xlsx", "MOB", 200)      # 大資料集
    ]
    
    for data_path, entity_type, expected_rows in test_cases:
        print(f"\n測試資料: {Path(data_path).name} ({expected_rows}行)")
        
        # 不指定mode，讓系統自動選擇
        result = await manager.process_from_files(
            data_path=data_path,
            entity_type=entity_type,
            processing_date=202410,
            processing_type="PO",
            mode=None,  # 自動選擇模式
            auxiliary_files={
                'procurement': 'accrual_bot/test_data/procurement.xlsx'
            }
        )
        
        print(f"  處理結果：{'成功' if result['success'] else '失敗'}")
        
        # 從context中獲取選擇的模式
        # 注意：這需要在實際執行中從context獲取
        mode_names = {
            1: "完整處理",
            2: "基本處理", 
            3: "PR處理",
            4: "快速處理",
            5: "SPX特殊"
        }
        
        # 這裡簡單根據資料大小推測
        if expected_rows < 100:
            selected_mode = 4  # 快速處理
        elif expected_rows < 150:
            selected_mode = 2  # 基本處理
        else:
            selected_mode = 1  # 完整處理
            
        print(f"  推測選擇模式：Mode {selected_mode} ({mode_names.get(selected_mode, '未知')})")
    
    return result


async def example_5_pr_processing():
    """
    範例5：PR處理
    簡化的PR處理流程
    """
    print("\n" + "=" * 50)
    print("範例5：PR處理流程")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 測試三個實體的PR處理
    entities = ["MOB", "SPT", "SPX"]
    
    for entity in entities:
        print(f"\n處理 {entity} PR資料...")
        
        # PR處理使用模式3
        result = await manager.process_from_files(
            data_path=f"accrual_bot/test_data/sample_{entity.lower()}_pr.xlsx",
            entity_type=entity,
            processing_date=202410,
            processing_type="PR",
            mode=3,
            save_results=False  # PR通常不需要保存結果
        )
        
        print(f"  {entity} PR處理：{'成功' if result['success'] else '失敗'}")
        
        if result['success'] and 'output_data' in result:
            pr_data = result['output_data']
            print(f"  處理項目數：{len(pr_data)}")
            
            # 顯示狀態分布
            if 'PR狀態' in pr_data.columns:
                status_counts = pr_data['PR狀態'].value_counts()
                print("  狀態分布：")
                for status, count in status_counts.items()[:3]:
                    print(f"    • {status}: {count}")
    
    return result


async def example_6_custom_pipeline():
    """
    範例6：自定義Pipeline
    根據特定需求組合步驟
    """
    print("\n" + "=" * 50)
    print("範例6：自定義Pipeline - 數據品質檢查")
    print("=" * 50)
    
    from accrual_bot.core.pipeline.pipeline import PipelineBuilder
    from accrual_bot.core.pipeline.steps import (
        DataCleaningStep,
        DateFormattingStep,
        ValidationStep,
        MOBValidationStep
    )
    
    # 創建自定義Pipeline - 專注於數據品質
    builder = PipelineBuilder("Custom_Quality_Check", "MOB")
    builder.with_stop_on_error(False)  # 不因錯誤停止，收集所有問題
    
    # 只添加清理和驗證步驟
    builder.add_steps(
        DataCleaningStep(
            name="InitialClean",
            columns_to_clean=['Item Description', 'GL#', 'Department']
        ),
        DateFormattingStep(
            name="DateFormat",
            date_columns={
                'Expected Receive Month': '%b-%y',
                'Submission Date': '%d-%b-%y'
            }
        ),
        ValidationStep(
            name="BasicValidation",
            validations=['required_columns', 'data_types']
        ),
        MOBValidationStep(
            name="MOBSpecificValidation"
        )
    )
    
    pipeline = builder.build()
    
    # 載入測試資料
    data = pd.read_excel("accrual_bot/test_data/sample_mob_po.xlsx")
    print(f"載入資料: {len(data)} 筆記錄")
    
    context = ProcessingContext(
        data=data,
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO"
    )
    
    # 執行自定義Pipeline
    result = await pipeline.execute(context)
    
    print(f"\n品質檢查結果：{'通過' if result['success'] else '發現問題'}")
    print(f"驗證錯誤數：{len(context.errors)}")
    print(f"驗證警告數：{len(context.warnings)}")
    
    # 顯示發現的問題
    if context.errors:
        print("\n發現的錯誤：")
        for i, error in enumerate(context.errors[:3], 1):
            print(f"  {i}. {error}")
        if len(context.errors) > 3:
            print(f"  ... 還有 {len(context.errors)-3} 個錯誤")
    
    if context.warnings:
        print("\n發現的警告：")
        for i, warning in enumerate(context.warnings[:3], 1):
            print(f"  {i}. {warning}")
        if len(context.warnings) > 3:
            print(f"  ... 還有 {len(context.warnings)-3} 個警告")
    
    return result


async def example_7_batch_processing():
    """
    範例7：批次處理
    處理多個實體的數據
    """
    print("\n" + "=" * 50)
    print("範例7：批次處理多個實體")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 定義批次任務
    tasks = [
        {"entity": "MOB", "file": "accrual_bot/test_data/mob_po_202410.xlsx", "name": "MOB十月PO"},
        {"entity": "SPT", "file": "accrual_bot/test_data/spt_po_202410.xlsx", "name": "SPT十月PO"},
        {"entity": "SPX", "file": "accrual_bot/test_data/spx_po_202410.xlsx", "name": "SPX十月PO"}
    ]
    
    print(f"準備處理 {len(tasks)} 個批次任務...")
    
    results = {}
    start_time = datetime.now()
    
    # 並行處理所有任務
    async def process_entity(task):
        print(f"  開始處理 {task['name']}...")
        result = await manager.process_from_files(
            data_path=task["file"],
            entity_type=task["entity"],
            processing_date=202410,
            processing_type="PO",
            mode=2  # 使用基本模式加快處理
        )
        return task["entity"], result
    
    # 執行批次處理
    batch_tasks = [process_entity(task) for task in tasks]
    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
    
    # 整理結果
    for item in batch_results:
        if isinstance(item, Exception):
            print(f"  ❌ 處理失敗：{str(item)}")
        else:
            entity, result = item
            results[entity] = result
            status = "✅ 成功" if result['success'] else "❌ 失敗"
            duration = result.get('duration', 0)
            print(f"  {status} {entity}: 耗時 {duration:.2f}秒")
    
    # 總結
    total_time = (datetime.now() - start_time).total_seconds()
    success_count = sum(1 for r in results.values() if r['success'])
    
    print("\n批次處理完成統計：")
    print(f"  - 成功: {success_count}/{len(results)}")
    print(f"  - 總耗時: {total_time:.2f}秒")
    print(f"  - 平均耗時: {total_time/len(results):.2f}秒/任務")
    
    return results


async def example_8_error_handling():
    """
    範例8：錯誤處理和恢復
    展示Pipeline的錯誤處理機制
    """
    print("\n" + "=" * 50)
    print("範例8：錯誤處理示例")
    print("=" * 50)
    
    from accrual_bot.core.pipeline.pipeline import PipelineBuilder
    from accrual_bot.core.pipeline.steps import DataCleaningStep, ValidationStep
    
    # 載入有問題的測試資料
    bad_data = pd.read_excel("accrual_bot/test_data/bad_data.xlsx")
    print(f"載入問題資料: {len(bad_data)} 筆記錄")
    
    # 顯示資料問題
    print("\n資料問題預覽：")
    print(f"  - 空值PO#: {bad_data['PO#'].isna().sum()}")
    invalid_amt = (bad_data['Entry Amount']
                   .apply(
                       lambda x: not str(x).replace('.', '').replace('-', '').isdigit() if pd.notna(x) else True
    ).sum()
    )
    print(f"  - 無效金額: {invalid_amt}")
    print(f"  - 空值GL#: {bad_data['GL#'].isna().sum()}")
    
    # 創建包含錯誤處理的Pipeline
    builder = PipelineBuilder("Error_Handling_Demo", "MOB")
    builder.with_stop_on_error(False)  # 不因錯誤停止，繼續執行
    
    builder.add_steps(
        DataCleaningStep(
            name="CleanWithErrors",
            required=False  # 非必需步驟
        ),
        ValidationStep(
            name="StrictValidation",
            validations=['required_columns', 'data_types'],
            required=False,  # 失敗不停止Pipeline
            retry_count=2    # 重試2次
        )
    )
    
    pipeline = builder.build()
    
    context = ProcessingContext(
        data=bad_data,
        entity_type="MOB",
        processing_date=202410,
        processing_type="PO"
    )
    
    # 執行並處理錯誤
    result = await pipeline.execute(context)
    
    print(f"\n執行結果：{'完成' if result else '失敗'}")
    print(f"Pipeline狀態：{'成功' if result.get('success') else '部分失敗'}")
    
    # 詳細錯誤報告
    if context.errors or context.warnings:
        print("\n錯誤處理報告：")
        print(f"  錯誤數量: {len(context.errors)}")
        print(f"  警告數量: {len(context.warnings)}")
        
        if context.errors:
            print("\n  主要錯誤：")
            for i, error in enumerate(context.errors[:3], 1):
                print(f"    {i}. {error[:100]}...")  # 截斷長錯誤信息
        
        if context.warnings:
            print("\n  主要警告：")
            for i, warning in enumerate(context.warnings[:3], 1):
                print(f"    {i}. {warning[:100]}...")
    
    # 顯示恢復情況
    if 'results' in result:
        recovered = (sum(1 for r in result['results'] 
                     if r.get('status') == 'success' and r.get('metadata', {}).get('retry_count', 0) > 0))
        if recovered > 0:
            print(f"\n  ✓ {recovered} 個步驟通過重試恢復")
    
    return result


async def example_9_performance_test():
    """
    範例9：效能測試
    測試不同規模資料的處理效能
    """
    print("\n" + "=" * 50)
    print("範例9：效能測試")
    print("=" * 50)
    
    manager = AccrualPipelineManager()
    
    # 生成不同規模的測試資料
    generator = TestDataGenerator(output_dir="accrual_bot/test_data")
    
    test_sizes = [10, 50, 100, 500]
    performance_results = []
    
    print("測試不同規模資料的處理效能...")
    
    for size in test_sizes:
        # 生成測試資料
        test_data = generator.generate_mob_po_data(size)
        
        # 測試處理時間
        start_time = datetime.now()
        
        context = ProcessingContext(
            data=test_data,
            entity_type="MOB",
            processing_date=202410,
            processing_type="PO"
        )
        
        pipeline = manager._create_pipeline("MOB", "PO", mode=4)  # 快速模式
        result = await pipeline.execute(context)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        performance_results.append({
            'size': size,
            'duration': duration,
            'success': result['success']
        })
        
        print(f"  {size:4d} 筆資料: {duration:.3f}秒 ({'成功' if result['success'] else '失敗'})")
    
    # 分析效能
    print("\n效能分析：")
    if len(performance_results) > 1:
        # 計算平均處理速度
        total_rows = sum(r['size'] for r in performance_results)
        total_time = sum(r['duration'] for r in performance_results)
        avg_speed = total_rows / total_time if total_time > 0 else 0
        
        print(f"  平均處理速度: {avg_speed:.1f} 筆/秒")
        
        # 檢查是否線性增長
        if len(performance_results) >= 2:
            time_ratio = performance_results[-1]['duration'] / performance_results[0]['duration']
            size_ratio = performance_results[-1]['size'] / performance_results[0]['size']
            efficiency = size_ratio / time_ratio if time_ratio > 0 else 0
            
            print(f"  擴展效率: {efficiency:.2f} (1.0 = 完美線性)")
    
    return performance_results


async def run_all_examples():
    """執行所有範例"""
    print("\n" + "#" * 60)
    print("#" + " " * 18 + "Pipeline 使用範例" + " " * 21 + "#")
    print("#" * 60)
    
    # 設置測試資料
    test_data_dir = await setup_test_data()
    
    # 所有範例函數
    examples = [
        ("基本處理", example_1_basic_processing),
        ("輔助資料整合", example_2_with_auxiliary_data),
        ("SPX特殊處理", example_3_spx_special_processing),
        ("自適應模式", example_4_adaptive_mode),
        ("PR處理", example_5_pr_processing),
        ("自定義Pipeline", example_6_custom_pipeline),
        ("批次處理", example_7_batch_processing),
        ("錯誤處理", example_8_error_handling),
        ("效能測試", example_9_performance_test)
    ]
    
    # 執行統計
    results_summary = {
        'total': len(examples),
        'success': 0,
        'failed': 0,
        'errors': []
    }
    
    # 執行所有範例
    for i, (name, example_func) in enumerate(examples, 1):
        try:
            print(f"\n{'='*60}")
            print(f"執行範例 {i}/{len(examples)}: {name}")
            print('=' * 60)
            
            await example_func()
            results_summary['success'] += 1
            
        except Exception as e:
            results_summary['failed'] += 1
            error_msg = f"範例{i} ({name}) 執行失敗：{str(e)}"
            results_summary['errors'].append(error_msg)
            print(f"\n❌ {error_msg}")
            
            # 繼續執行下一個範例
            continue
    
    # 顯示總結
    print("\n" + "#" * 60)
    print("#" + " " * 22 + "執行總結" + " " * 23 + "#")
    print("#" * 60)
    
    print("\n📊 執行統計：")
    print(f"  總範例數: {results_summary['total']}")
    print(f"  ✅ 成功: {results_summary['success']}")
    print(f"  ❌ 失敗: {results_summary['failed']}")
    
    if results_summary['errors']:
        print("\n❌ 錯誤列表：")
        for error in results_summary['errors']:
            print(f"  - {error}")
    else:
        print("\n✅ 所有範例執行成功！")
    
    print(f"\n📁 測試資料位置: {test_data_dir}")
    print("💡 提示: 您可以單獨執行任何範例函數來深入了解特定功能")
    
    print("\n" + "#" * 60)


async def run_single_example(example_number: int):
    """
    執行單個範例
    
    Args:
        example_number: 範例編號 (1-9)
    """
    # 設置測試資料
    await setup_test_data()
    
    examples = [
        example_1_basic_processing,
        example_2_with_auxiliary_data,
        example_3_spx_special_processing,
        example_4_adaptive_mode,
        example_5_pr_processing,
        example_6_custom_pipeline,
        example_7_batch_processing,
        example_8_error_handling,
        example_9_performance_test
    ]
    
    if 1 <= example_number <= len(examples):
        print(f"\n執行範例 {example_number}...")
        await examples[example_number - 1]()
    else:
        print(f"❌ 無效的範例編號。請選擇 1-{len(examples)} 之間的數字。")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 如果提供了參數，執行指定的範例
        try:
            example_num = int(sys.argv[1])
            asyncio.run(run_single_example(example_num))
        except ValueError:
            print("❌ 請提供有效的範例編號 (1-9)")
            print("用法: python examples.py [範例編號]")
            print("或直接執行 python examples.py 來運行所有範例")
    else:
        # 執行所有範例
        asyncio.run(run_all_examples())
