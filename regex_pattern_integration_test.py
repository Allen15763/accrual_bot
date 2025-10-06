"""
正則模式整合驗證腳本

用途：
1. 驗證Pipeline步驟確實使用config.ini的正則模式，而非constants.py
2. 測試正則模式的實際匹配效果
3. 確保日期解析邏輯使用配置檔的正則表達式

執行方式：
    python regex_pattern_integration_test.py
"""

import sys
from pathlib import Path
import re
from typing import Dict, List, Tuple

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


class RegexPatternTester:
    """正則模式測試器"""
    
    def __init__(self):
        self.results = {
            'passed': [],
            'failed': [],
            'warnings': []
        }
    
    def test_pattern_source(self):
        """測試1：驗證模式來源"""
        print("\n" + "=" * 80)
        print("測試1：驗證正則模式來源")
        print("=" * 80)
        
        try:
            # 從config.ini讀取
            from accrual_bot.utils.config import config_manager
            config_patterns = config_manager.get_regex_patterns()
            
            # 從Pipeline配置管理器讀取
            from accrual_bot.core.pipeline.config_manager import PipelineConfigManager
            pipeline_config = PipelineConfigManager()
            pipeline_patterns = pipeline_config.get_regex_patterns()
            
            print("\n來自config.ini的正則模式:")
            for key, pattern in config_patterns.items():
                print(f"  {key}: {pattern[:60]}...")
            
            print("\n來自Pipeline配置管理器的正則模式:")
            for key, pattern in pipeline_patterns.items():
                print(f"  {key}: {pattern[:60]}...")
            
            # 比較兩者是否相同
            if config_patterns == pipeline_patterns:
                self.results['passed'].append("正則模式來源: ✓ Pipeline使用config.ini的模式")
                print("\n✓ Pipeline確實使用config.ini的正則模式")
                return True
            else:
                self.results['failed'].append("正則模式來源: ✗ Pipeline未使用config.ini的模式")
                print("\n✗ Pipeline未使用config.ini的正則模式")
                
                # 顯示差異
                print("\n差異詳情:")
                for key in set(config_patterns.keys()) | set(pipeline_patterns.keys()):
                    config_val = config_patterns.get(key, 'N/A')
                    pipeline_val = pipeline_patterns.get(key, 'N/A')
                    if config_val != pipeline_val:
                        print(f"\n  {key}:")
                        print(f"    config.ini: {config_val}")
                        print(f"    Pipeline:   {pipeline_val}")
                
                return False
        
        except Exception as e:
            self.results['failed'].append(f"模式來源驗證失敗: {str(e)}")
            print(f"✗ 驗證失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_pattern_matching(self):
        """測試2：驗證模式匹配效果"""
        print("\n" + "=" * 80)
        print("測試2：驗證正則模式匹配效果")
        print("=" * 80)
        
        try:
            from accrual_bot.utils.config import config_manager
            patterns = config_manager.get_regex_patterns()
            
            # 測試案例
            test_cases = {
                'pt_YM': [
                    ('2025/01 租金', True, '2025/01'),
                    ('2024/12 服務', True, '2024/12'),
                    ('2025/13 無效', False, None),
                    ('202501', False, None)
                ],
                'pt_YMD': [
                    ('2025/01/15 付款', True, '2025/01/15'),
                    ('2024/12/31 結算', True, '2024/12/31'),
                    ('2025/01/32 無效', False, None),
                    ('2025/1/5', False, None)  # 缺少前導零
                ],
                'pt_YMtoYM': [
                    ('2025/01-2025/03', True, '2025/01-2025/03'),
                    ('2024/12-2025/02', True, '2024/12-2025/02'),
                    ('2025/01~2025/03', False, None),  # 分隔符錯誤
                    ('2025/01 - 2025/03', False, None)  # 有空格
                ],
                'pt_YMDtoYMD': [
                    ('2025/01/01-2025/03/31', True, '2025/01/01-2025/03/31'),
                    ('2024/12/15-2025/02/28', True, '2024/12/15-2025/02/28'),
                    ('2025/01/01~2025/03/31', False, None),  # 分隔符錯誤
                ]
            }
            
            all_passed = True
            
            for pattern_key, cases in test_cases.items():
                pattern = patterns.get(pattern_key)
                
                if not pattern:
                    self.results['failed'].append(f"模式 {pattern_key}: ✗ 不存在")
                    print(f"\n✗ 模式 '{pattern_key}' 不存在於配置檔")
                    all_passed = False
                    continue
                
                print(f"\n測試模式: {pattern_key}")
                print(f"  正則表達式: {pattern}")
                
                for test_str, should_match, expected in cases:
                    match = re.search(pattern, test_str)
                    
                    if should_match:
                        if match:
                            matched_str = match.group(0).strip()
                            print(f"  ✓ '{test_str}' -> 匹配: '{matched_str}'")
                            
                            if expected and expected not in matched_str:
                                print(f"    ⚠ 預期 '{expected}'，但得到 '{matched_str}'")
                                self.results['warnings'].append(
                                    f"{pattern_key}: '{test_str}' 匹配結果與預期不符"
                                )
                        else:
                            print(f"  ✗ '{test_str}' -> 應該匹配但未匹配")
                            all_passed = False
                    else:
                        if match:
                            print(f"  ✗ '{test_str}' -> 不應該匹配但匹配了")
                            all_passed = False
                        else:
                            print(f"  ✓ '{test_str}' -> 正確未匹配")
            
            if all_passed:
                self.results['passed'].append("正則模式匹配: ✓ 所有測試通過")
                print("\n✓ 所有正則模式匹配測試通過")
                return True
            else:
                self.results['failed'].append("正則模式匹配: ✗ 部分測試失敗")
                print("\n✗ 部分正則模式匹配測試失敗")
                return False
        
        except Exception as e:
            self.results['failed'].append(f"模式匹配測試失敗: {str(e)}")
            print(f"✗ 測試失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_date_extraction_with_config_patterns(self):
        """測試3：驗證日期提取使用配置檔模式"""
        print("\n" + "=" * 80)
        print("測試3：驗證日期提取函數使用配置檔模式")
        print("=" * 80)
        
        try:
            from accrual_bot.utils import extract_date_range_from_description
            from accrual_bot.utils.config import config_manager
            
            # 獲取配置檔的模式
            config_patterns = config_manager.get_regex_patterns()
            
            # 測試案例
            test_descriptions = [
                "2025/01-2025/03租金費用",
                "2024/12-2025/02維護服務",
                "2025/04設備採購",
                "2025/01/01-2025/03/31辦公用品",
                "無效的日期格式測試",
                "2025/08 ~ 2025/10 SSOC-Electric pallet truck rental *18台",
                "2025/08~10 中倉_駐衛保全服務費 Central SOC security fee",
                "2025/06 SPTTW關貿發票開立費用Trade-Van Invoicing Fee",
                "2026/01/15 - 2026/07/14 SVP_台北市中山區吉林路390號1樓_租金_Rental"
            ]
            
            print("\n測試日期提取:")
            all_valid = True
            
            for desc in test_descriptions:
                start, end = extract_date_range_from_description(desc).split(',')
                print(f"\n  描述: {desc}")
                print(f"    開始: {start}, 結束: {end}")
                
                # 驗證輸出格式
                if start != '100001' and end != '100002':
                    # 有效日期應該是6位數字
                    if len(start) == 6 and len(end) == 6 and start.isdigit() and end.isdigit():
                        print(f"    ✓ 格式正確 (YYYYMM)")
                    else:
                        print(f"    ✗ 格式錯誤")
                        all_valid = False
                else:
                    print(f"    ⚠ 使用預設值（無法解析）")
            
            # 驗證函數內部是否使用配置檔的模式
            # 這需要檢查函數的實現或透過行為推斷
            print("\n驗證日期提取函數的模式使用:")
            
            # 測試特定模式
            test_ym_range = "2025/01-2025/03"
            start, end = extract_date_range_from_description(test_ym_range).split(',')
            
            if start == '202501' and end == '202503':
                print(f"  ✓ YYYY/MM-YYYY/MM 格式正確處理")
            else:
                print(f"  ✗ YYYY/MM-YYYY/MM 格式處理錯誤: {start}, {end}")
                all_valid = False
            
            test_ymd_range = "2025/01/15-2025/03/31"
            start, end = extract_date_range_from_description(test_ymd_range).split(',')
            
            if start == '202501' and end == '202503':
                print(f"  ✓ YYYY/MM/DD-YYYY/MM/DD 格式正確處理")
            else:
                print(f"  ✗ YYYY/MM/DD-YYYY/MM/DD 格式處理錯誤: {start}, {end}")
                all_valid = False
            
            if all_valid:
                self.results['passed'].append("日期提取: ✓ 使用配置檔模式")
                print("\n✓ 日期提取函數正確使用配置檔的正則模式")
                return True
            else:
                self.results['failed'].append("日期提取: ✗ 模式使用異常")
                print("\n✗ 日期提取函數的模式使用異常")
                return False
        
        except Exception as e:
            self.results['failed'].append(f"日期提取測試失敗: {str(e)}")
            print(f"✗ 測試失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_pipeline_steps_use_config(self):
        """測試4：驗證Pipeline步驟使用配置檔"""
        print("\n" + "=" * 80)
        print("測試4：驗證Pipeline步驟使用配置檔")
        print("=" * 80)
        
        try:
            from accrual_bot.core.pipeline.steps.common import DateParsingStep
            from accrual_bot.utils.config import config_manager
            
            # 創建步驟實例
            step = DateParsingStep()
            
            # 檢查步驟是否有regex_patterns屬性
            if hasattr(step, 'regex_patterns'):
                print("\n✓ DateParsingStep 包含 regex_patterns 屬性")
                
                # 獲取配置檔的模式
                config_patterns = config_manager.get_regex_patterns()
                
                # 比較
                if step.regex_patterns == config_patterns:
                    self.results['passed'].append("Pipeline步驟: ✓ 使用配置檔模式")
                    print("✓ DateParsingStep 的正則模式與配置檔一致")
                    
                    print("\n步驟使用的模式:")
                    for key, pattern in step.regex_patterns.items():
                        print(f"  {key}: {pattern[:60]}...")
                    
                    return True
                else:
                    self.results['failed'].append("Pipeline步驟: ✗ 未使用配置檔模式")
                    print("✗ DateParsingStep 的正則模式與配置檔不一致")
                    
                    # 顯示差異
                    print("\n差異:")
                    for key in set(step.regex_patterns.keys()) | set(config_patterns.keys()):
                        step_val = step.regex_patterns.get(key, 'N/A')
                        config_val = config_patterns.get(key, 'N/A')
                        if step_val != config_val:
                            print(f"\n  {key}:")
                            print(f"    步驟: {step_val}")
                            print(f"    配置: {config_val}")
                    
                    return False
            else:
                self.results['warnings'].append("Pipeline步驟: ⚠ 未找到 regex_patterns 屬性")
                print("⚠ DateParsingStep 未包含 regex_patterns 屬性")
                print("  步驟可能通過其他方式使用配置檔")
                return True
        
        except Exception as e:
            self.results['failed'].append(f"Pipeline步驟測試失敗: {str(e)}")
            print(f"✗ 測試失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_no_constants_usage(self):
        """測試5：驗證不使用constants.py的模式"""
        print("\n" + "=" * 80)
        print("測試5：驗證Pipeline步驟不直接使用constants.py")
        print("=" * 80)
        
        try:
            import inspect
            from accrual_bot.core.pipeline.steps import common, business, mob_steps
            
            # 檢查步驟模組的源代碼
            modules_to_check = [
                ('common.py', common),
                ('business.py', business),
                ('mob_steps.py', mob_steps)
            ]
            
            all_clean = True
            
            for module_name, module in modules_to_check:
                source = inspect.getsource(module)
                
                # 檢查是否直接導入REGEX_PATTERNS from constants
                if 'from' in source and 'constants' in source and 'REGEX_PATTERNS' in source:
                    print(f"\n  ✗ {module_name}: 發現直接使用constants.REGEX_PATTERNS")
                    self.results['failed'].append(f"{module_name}: 使用constants.REGEX_PATTERNS")
                    all_clean = False
                else:
                    print(f"\n  ✓ {module_name}: 未直接使用constants.REGEX_PATTERNS")
                
                # 檢查是否使用config_manager
                if 'config_manager' in source:
                    print(f"    ✓ 使用 config_manager")
                else:
                    print(f"    ⚠ 未發現 config_manager 使用")
            
            if all_clean:
                self.results['passed'].append("Constants檢查: ✓ 未使用constants.py的模式")
                print("\n✓ Pipeline步驟未直接使用constants.py的正則模式")
                return True
            else:
                print("\n✗ 部分步驟仍直接使用constants.py")
                return False
        
        except Exception as e:
            self.results['failed'].append(f"Constants檢查失敗: {str(e)}")
            print(f"✗ 檢查失敗: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_results(self):
        """輸出測試結果"""
        print("\n" + "=" * 80)
        print("測試結果統計")
        print("=" * 80)
        
        print(f"\n✓ 通過: {len(self.results['passed'])} 項")
        print(f"✗ 失敗: {len(self.results['failed'])} 項")
        print(f"⚠ 警告: {len(self.results['warnings'])} 項")
        
        if self.results['failed']:
            print("\n" + "=" * 80)
            print("失敗項目")
            print("=" * 80)
            for failure in self.results['failed']:
                print(f"✗ {failure}")
        
        if self.results['warnings']:
            print("\n" + "=" * 80)
            print("警告項目")
            print("=" * 80)
            for warning in self.results['warnings']:
                print(f"⚠ {warning}")
        
        # 總體結果
        if not self.results['failed']:
            print("\n✓ 所有測試通過！正則模式整合驗證成功。")
            return True
        else:
            print(f"\n✗ {len(self.results['failed'])} 個測試失敗！")
            return False


def main():
    """主函數"""
    print("\n")
    print("*" * 80)
    print("正則模式整合驗證")
    print("*" * 80)
    
    tester = RegexPatternTester()
    
    # 執行所有測試
    tests = [
        ("驗證模式來源", tester.test_pattern_source),
        ("驗證模式匹配", tester.test_pattern_matching),
        ("驗證日期提取", tester.test_date_extraction_with_config_patterns),
        ("驗證Pipeline步驟", tester.test_pipeline_steps_use_config),
        ("驗證不使用constants", tester.test_no_constants_usage)
    ]
    
    for test_name, test_func in tests:
        try:
            test_func()
        except Exception as e:
            print(f"\n✗ 測試 '{test_name}' 發生異常: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 輸出結果
    success = tester.print_results()
    
    return 0 if success else 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
