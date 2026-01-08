"""
配置遷移測試腳本

用途：測試Pipeline模組使用原配置管理器後的功能正常性
確保配置遷移不影響現有功能

執行方式：
    python config_migration_test.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


def test_config_loading():
    """測試配置載入"""
    print("\n" + "=" * 80)
    print("測試1：配置載入")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
        config_manager = PipelineConfigManager()
        print("✓ PipelineConfigManager 初始化成功")
        
        # 測試列出實體類型
        entities = config_manager.list_entity_types()
        print(f"✓ 支援的實體類型: {entities}")
        
        # 測試列出處理模式
        modes = config_manager.list_processing_modes()
        print(f"✓ 可用的處理模式: {len(modes)} 種")
        
        return True
    
    except Exception as e:
        print(f"✗ 配置載入失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_entity_config_access():
    """測試實體配置訪問"""
    print("\n" + "=" * 80)
    print("測試2：實體配置訪問")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
        config_manager = PipelineConfigManager()
        
        for entity in ['MOB', 'SPT', 'SPX']:
            print(f"\n測試 {entity} 配置...")
            
            # 獲取實體配置
            entity_config = config_manager.get_entity_config(entity)
            print(f"  ✓ 實體類型: {entity_config.entity_type}")
            print(f"  ✓ FA帳戶: {entity_config.fa_accounts}")
            print(f"  ✓ 租金帳戶: {entity_config.rent_account}")
            print(f"  ✓ OPS: {entity_config.ops_rent}")
            
            if entity == 'SPX':
                print(f"  ✓ Kiosk供應商: {len(entity_config.kiosk_suppliers)} 個")
                print(f"  ✓ Locker供應商: {len(entity_config.locker_suppliers)} 個")
                print(f"  ✓ 特殊規則: {list(entity_config.special_rules.keys())}")
        
        print("\n✓ 所有實體配置訪問正常")
        return True
    
    except Exception as e:
        print(f"✗ 實體配置訪問失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_regex_patterns():
    """測試正則模式訪問"""
    print("\n" + "=" * 80)
    print("測試3：正則模式訪問")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
        config_manager = PipelineConfigManager()
        
        patterns = config_manager.get_regex_patterns()
        print(f"✓ 獲取到 {len(patterns)} 個正則模式")
        
        for key, pattern in patterns.items():
            print(f"  {key}: {pattern[:50]}..." if len(pattern) > 50 else f"  {key}: {pattern}")
        
        # 驗證必要模式存在
        required_patterns = ['pt_YM', 'pt_YMD', 'pt_YMtoYM', 'pt_YMDtoYMD']
        for pattern_key in required_patterns:
            if pattern_key in patterns and patterns[pattern_key]:
                print(f"✓ 必要模式 {pattern_key} 存在")
            else:
                print(f"✗ 必要模式 {pattern_key} 缺失")
                return False
        
        print("\n✓ 正則模式訪問正常")
        return True
    
    except Exception as e:
        print(f"✗ 正則模式訪問失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_pivot_config():
    """測試Pivot配置訪問"""
    print("\n" + "=" * 80)
    print("測試4：Pivot配置訪問")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
        config_manager = PipelineConfigManager()
        
        test_cases = [
            ('MOB', 'po'),
            ('SPT', 'pr'),
            ('SPX', 'po')
        ]
        
        for entity, data_type in test_cases:
            pivot_config = config_manager.get_pivot_config(entity, data_type)
            
            print(f"\n{entity} {data_type.upper()} Pivot配置:")
            print(f"  Index欄位: {len(pivot_config.get('index', []))} 個")
            print(f"  SM CR Pivot欄位: {pivot_config.get('sm_cr_pivot_cols', [])}")
            print(f"  Value欄位: {pivot_config.get('pivot_value_col', '')}")
        
        print("\n✓ Pivot配置訪問正常")
        return True
    
    except Exception as e:
        print(f"✗ Pivot配置訪問失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_config_validation():
    """測試配置驗證功能"""
    print("\n" + "=" * 80)
    print("測試5：配置驗證功能")
    print("=" * 80)
    
    try:
        from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
        
        config_manager = PipelineConfigManager()
        
        # 執行配置驗證
        validation_result = config_manager.validate_config()
        
        print(f"驗證結果:")
        print(f"  有效: {validation_result['valid']}")
        print(f"  錯誤: {len(validation_result['errors'])} 個")
        print(f"  警告: {len(validation_result['warnings'])} 個")
        
        if validation_result['errors']:
            print("\n錯誤詳情:")
            for error in validation_result['errors']:
                print(f"  ✗ {error}")
        
        if validation_result['warnings']:
            print("\n警告詳情:")
            for warning in validation_result['warnings']:
                print(f"  ⚠ {warning}")
        
        if validation_result['valid']:
            print("\n✓ 配置驗證通過")
            return True
        else:
            print("\n✗ 配置驗證失敗")
            return False
    
    except Exception as e:
        print(f"✗ 配置驗證功能測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_backward_compatibility():
    """測試向後兼容性"""
    print("\n" + "=" * 80)
    print("測試6：向後兼容性")
    print("=" * 80)
    
    try:
        # 測試舊的導入方式是否仍然有效
        from accrual_bot.core.pipeline import PipelineConfigManager, EntityConfig, ProcessingMode
        
        print("✓ 舊的導入路徑仍然有效")
        
        # 測試配置管理器接口
        config_manager = PipelineConfigManager()
        
        # 測試各種方法
        methods_to_test = [
            ('get_entity_config', ('MOB',)),
            ('get_processing_mode', (1,)),
            ('get_regex_patterns', ()),
            ('get_fa_accounts', ('MOB',)),
            ('list_entity_types', ()),
            ('list_processing_modes', ())
        ]
        
        for method_name, args in methods_to_test:
            method = getattr(config_manager, method_name)
            result = method(*args)
            print(f"✓ 方法 {method_name}{args} 可用")
        
        print("\n✓ 向後兼容性測試通過")
        return True
    
    except Exception as e:
        print(f"✗ 向後兼容性測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函數"""
    print("\n")
    print("*" * 80)
    print("配置遷移測試")
    print("*" * 80)
    
    tests = [
        ("配置載入", test_config_loading),
        ("實體配置訪問", test_entity_config_access),
        ("正則模式訪問", test_regex_patterns),
        ("Pivot配置訪問", test_pivot_config),
        ("配置驗證功能", test_config_validation),
        ("向後兼容性", test_backward_compatibility)
    ]
    
    results = []
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
    print("測試結果總結")
    print("=" * 80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ 通過" if success else "✗ 失敗"
        print(f"{status}: {test_name}")
    
    print(f"\n總計: {passed}/{total} 測試通過")
    
    if passed == total:
        print("\n✓ 所有測試通過！配置遷移成功。")
        return 0
    else:
        print(f"\n✗ {total - passed} 個測試失敗！需要檢查配置遷移。")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
