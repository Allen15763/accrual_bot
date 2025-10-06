"""
快速驗證腳本 - 階段一與階段二最小化修改

用途：快速驗證修改是否成功且不影響現有功能
執行方式：python quick_verification.py
"""

import sys
from pathlib import Path

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


def test_imports():
    """測試1: 驗證import是否成功"""
    print("\n" + "=" * 80)
    print("測試1: 驗證import")
    print("=" * 80)
    
    try:
        # 測試common.py的import
        from accrual_bot.core.pipeline.steps.common import (
            DataCleaningStep,
            DateParsingStep,
            ValidationStep
        )
        print("✓ common.py import成功")
        
        # 測試business.py的import
        from accrual_bot.core.pipeline.steps.business import (
            StatusEvaluationStep,
            AccountingAdjustmentStep
        )
        print("✓ business.py import成功")
        
        # 測試mob_steps.py的import
        from accrual_bot.core.pipeline.steps.mob_steps import (
            MOBStatusStep,
            MOBAccrualStep,
            MOBValidationStep
        )
        print("✓ mob_steps.py import成功")
        
        return True
    
    except Exception as e:
        print(f"✗ Import失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_date_parsing_step_regex_patterns():
    """測試2: 驗證DateParsingStep.regex_patterns屬性"""
    print("\n" + "=" * 80)
    print("測試2: 驗證DateParsingStep.regex_patterns")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.steps.common import DateParsingStep
        
        # 創建實例
        step = DateParsingStep()
        
        # 檢查是否有regex_patterns屬性
        if hasattr(step, 'regex_patterns'):
            print("✓ DateParsingStep有regex_patterns屬性")
            
            # 檢查regex_patterns是否為字典
            if isinstance(step.regex_patterns, dict):
                print("✓ regex_patterns是字典類型")
                
                # 檢查必要的鍵
                required_keys = ['pt_YM', 'pt_YMD', 'pt_YMtoYM', 'pt_YMDtoYMD']
                missing_keys = [k for k in required_keys if k not in step.regex_patterns]
                
                if not missing_keys:
                    print(f"✓ regex_patterns包含所有必要鍵: {required_keys}")
                    
                    # 顯示模式內容（截斷）
                    print("\n正則模式內容:")
                    for key, pattern in step.regex_patterns.items():
                        pattern_preview = pattern[:50] + "..." if len(pattern) > 50 else pattern
                        print(f"  {key}: {pattern_preview}")
                    
                    return True
                else:
                    print(f"✗ regex_patterns缺少鍵: {missing_keys}")
                    return False
            else:
                print(f"✗ regex_patterns不是字典: {type(step.regex_patterns)}")
                return False
        else:
            print("✗ DateParsingStep沒有regex_patterns屬性")
            return False
    
    except Exception as e:
        print(f"✗ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_config_manager_available():
    """測試3: 驗證config_manager可用"""
    print("\n" + "=" * 80)
    print("測試3: 驗證config_manager可用")
    print("=" * 80)
    
    try:
        from accrual_bot.utils.config import config_manager
        
        print("✓ config_manager可以導入")
        
        # 測試獲取正則模式
        patterns = config_manager.get_regex_patterns()
        if patterns:
            print(f"✓ config_manager.get_regex_patterns()返回{len(patterns)}個模式")
        
        # 測試獲取FA帳戶
        for entity in ['mob', 'spt', 'spx']:
            fa_accounts = config_manager.get_fa_accounts(entity)
            print(f"✓ {entity.upper()} FA帳戶: {fa_accounts}")
        
        return True
    
    except Exception as e:
        print(f"✗ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_status_values_available():
    """測試4: 驗證STATUS_VALUES可用"""
    print("\n" + "=" * 80)
    print("測試4: 驗證STATUS_VALUES可用")
    print("=" * 80)
    
    try:
        from accrual_bot.utils.config import STATUS_VALUES
        
        print("✓ STATUS_VALUES可以導入")
        
        # 檢查是否為字典
        if isinstance(STATUS_VALUES, dict):
            print(f"✓ STATUS_VALUES是字典，包含{len(STATUS_VALUES)}個狀態")
            
            # 顯示部分內容
            print("\n狀態值示例:")
            for i, (key, value) in enumerate(list(STATUS_VALUES.items())[:5]):
                print(f"  {key}: {value}")
            
            if len(STATUS_VALUES) > 5:
                print(f"  ... 共{len(STATUS_VALUES)}個狀態")
            
            return True
        else:
            print(f"✗ STATUS_VALUES不是字典: {type(STATUS_VALUES)}")
            return False
    
    except Exception as e:
        print(f"✗ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_class_signatures_unchanged():
    """測試5: 驗證類簽名未改變"""
    print("\n" + "=" * 80)
    print("測試5: 驗證類簽名未改變")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.steps.common import ValidationStep
        import inspect
        
        # 獲取ValidationStep.__init__的簽名
        sig = inspect.signature(ValidationStep.__init__)
        params = list(sig.parameters.keys())
        
        print(f"ValidationStep.__init__參數: {params}")
        
        # 檢查關鍵參數
        if 'validations' in params:
            print("✓ ValidationStep保留validations參數（關鍵！）")
            return True
        else:
            print("✗ ValidationStep缺少validations參數")
            return False
    
    except Exception as e:
        print(f"✗ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_step_instantiation():
    """測試6: 驗證步驟可以正常實例化"""
    print("\n" + "=" * 80)
    print("測試6: 驗證步驟實例化")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.steps.common import (
            DataCleaningStep,
            DateParsingStep,
            ValidationStep
        )
        from accrual_bot.core.pipeline.steps.business import StatusEvaluationStep
        from accrual_bot.core.pipeline.steps.mob_steps import MOBStatusStep
        
        # 測試實例化
        steps = [
            ('DataCleaningStep', DataCleaningStep()),
            ('DateParsingStep', DateParsingStep()),
            ('ValidationStep', ValidationStep(validations=['required_columns'])),
            ('StatusEvaluationStep', StatusEvaluationStep()),
            ('MOBStatusStep', MOBStatusStep())
        ]
        
        for name, step in steps:
            print(f"✓ {name}實例化成功")
        
        return True
    
    except Exception as e:
        print(f"✗ 實例化失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函數"""
    print("\n")
    print("*" * 80)
    print("階段一與階段二最小化修改 - 快速驗證")
    print("*" * 80)
    
    results = []
    
    # 執行所有測試
    tests = [
        ("Import驗證", test_imports),
        ("DateParsingStep.regex_patterns", test_date_parsing_step_regex_patterns),
        ("config_manager可用性", test_config_manager_available),
        ("STATUS_VALUES可用性", test_status_values_available),
        ("類簽名不變", test_class_signatures_unchanged),
        ("步驟實例化", test_step_instantiation)
    ]
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"\n✗ 測試 '{test_name}' 發生異常: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # 輸出總結
    print("\n" + "=" * 80)
    print("驗證結果總結")
    print("=" * 80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ 通過" if success else "✗ 失敗"
        print(f"{status}: {test_name}")
    
    print(f"\n總計: {passed}/{total} 測試通過")
    
    if passed == total:
        print("\n" + "=" * 80)
        print("✓ 所有驗證通過！修改成功且不影響現有功能。")
        print("=" * 80)
        print("\n下一步：")
        print("1. 運行原有pipeline測試: python pipeline_examples.py")
        print("2. 運行正則模式測試: python regex_pattern_integration_test.py")
        print("3. 運行配置驗證測試: python config_validation_test.py")
        return 0
    else:
        print("\n" + "=" * 80)
        print(f"✗ {total - passed} 個驗證失敗！請檢查修改。")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
