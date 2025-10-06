"""
配置一致性驗證腳本

用途：驗證Pipeline配置管理器與原架構配置管理器的一致性
確保重構後的配置讀取行為與原架構完全相同

執行方式：
    python config_validation_test.py
"""

import sys
from pathlib import Path
from typing import Dict, Any, List

# 添加模組路徑
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from accrual_bot.utils import config_manager as original_config
from accrual_bot.core.pipeline.config_manager import PipelineConfigManager


class ConfigValidator:
    """配置驗證器"""
    
    def __init__(self):
        self.original_config = original_config
        self.pipeline_config = PipelineConfigManager()
        self.results = {
            'passed': [],
            'failed': [],
            'warnings': []
        }
    
    def validate_all(self) -> Dict[str, Any]:
        """執行所有驗證"""
        print("=" * 80)
        print("配置一致性驗證")
        print("=" * 80)
        print()
        
        # 驗證FA帳戶
        self.validate_fa_and_locker_accounts()
        
        # 驗證正則模式
        self.validate_regex_patterns()
        
        # 驗證實體配置
        self.validate_entity_configs()
        
        # 驗證Pivot配置
        self.validate_pivot_configs()
        
        # 輸出結果
        self.print_results()
        
        return self.results
    
    def validate_fa_and_locker_accounts(self):
        """驗證FA帳戶跟Locker配置"""
        print("✓ 驗證FA帳戶跟Locker配置...")
        
        for entity in ['MOB', 'SPT', 'SPX']:
            try:
                # 從原配置讀取
                original_fa = self.original_config.get_fa_accounts(entity.lower())
                original_locker = 'discount,PO單號,廠商,門市,地址,A,B,C,D,E,F,G,H,I,J,K,DA,XA,XB,XC,XD,XE,XF,超出櫃體安裝費,超出櫃體運費,裝運費,進場月份,驗收月份,是否申請產編貼紙,運費PO單號,尾款月份'
                
                # 從Pipeline配置讀取
                pipeline_fa = self.pipeline_config.get_fa_accounts(entity)
                pipeline_locker = self.pipeline_config.config_manager._config_data.get("SPX").get("locker_columns")
                
                # 比較
                if all([set(original_fa) == set(pipeline_fa), original_locker == pipeline_locker]):
                    self.results['passed'].append(
                        f"FA帳戶 - {entity}: ✓ 一致 ({len(original_fa)} 個帳戶)"
                    )
                    print(f"  ✓ {entity}: {original_fa}")
                else:
                    self.results['failed'].append(
                        f"FA帳戶 - {entity}: ✗ 不一致\n"
                        f"    原配置: {original_fa}\n"
                        f"    Pipeline: {pipeline_fa}"
                    )
                    print(f"  ✗ {entity}: 不一致!")
                    print(f"    原配置: {original_fa}")
                    print(f"    Pipeline: {pipeline_fa}")
            
            except Exception as e:
                self.results['failed'].append(
                    f"FA帳戶 - {entity}: ✗ 驗證失敗 - {str(e)}"
                )
                print(f"  ✗ {entity}: 驗證失敗 - {str(e)}")
        
        print()
    
    def validate_regex_patterns(self):
        """驗證正則表達式模式"""
        print("✓ 驗證正則表達式模式...")
        
        try:
            # 從原配置讀取
            original_patterns = self.original_config.get_regex_patterns()
            
            # 從Pipeline配置讀取
            pipeline_patterns = self.pipeline_config.get_regex_patterns()
            
            # 比較每個模式
            pattern_keys = ['pt_YM', 'pt_YMD', 'pt_YMtoYM', 'pt_YMDtoYMD']
            all_match = True
            
            for key in pattern_keys:
                original_pattern = original_patterns.get(key, '')
                pipeline_pattern = pipeline_patterns.get(key, '')
                
                if original_pattern == pipeline_pattern:
                    self.results['passed'].append(f"正則模式 - {key}: ✓ 一致")
                    print(f"  ✓ {key}: 一致")
                else:
                    all_match = False
                    self.results['failed'].append(
                        f"正則模式 - {key}: ✗ 不一致\n"
                        f"    原配置: {original_pattern}\n"
                        f"    Pipeline: {pipeline_pattern}"
                    )
                    print(f"  ✗ {key}: 不一致!")
                    print(f"    原配置: {original_pattern}")
                    print(f"    Pipeline: {pipeline_pattern}")
            
            if all_match:
                print("  ✓ 所有正則模式一致")
        
        except Exception as e:
            self.results['failed'].append(f"正則模式驗證失敗: {str(e)}")
            print(f"  ✗ 驗證失敗: {str(e)}")
        
        print()
    
    def validate_entity_configs(self):
        """驗證實體配置"""
        print("✓ 驗證實體配置...")
        
        for entity in ['MOB', 'SPT', 'SPX']:
            try:
                # 獲取Pipeline實體配置
                entity_config = self.pipeline_config.get_entity_config(entity)
                
                # 驗證必要字段
                checks = []
                
                # 檢查FA帳戶
                if entity_config.fa_accounts:
                    checks.append("FA帳戶: ✓")
                else:
                    checks.append("FA帳戶: ✗ (為空)")
                    self.results['warnings'].append(f"{entity}: FA帳戶列表為空")
                
                # 檢查租金帳戶
                if entity_config.rent_account:
                    checks.append(f"租金帳戶: ✓ ({entity_config.rent_account})")
                else:
                    checks.append("租金帳戶: ✗ (為空)")
                    self.results['warnings'].append(f"{entity}: 租金帳戶為空")
                
                # 檢查OPS
                if entity_config.ops_rent:
                    checks.append(f"OPS: ✓ ({entity_config.ops_rent})")
                else:
                    checks.append("OPS: ✗ (為空)")
                    self.results['warnings'].append(f"{entity}: OPS配置為空")
                
                # SPX特殊檢查
                if entity == 'SPX':
                    if entity_config.kiosk_suppliers:
                        checks.append(f"Kiosk供應商: ✓ ({len(entity_config.kiosk_suppliers)})")
                    if entity_config.locker_suppliers:
                        checks.append(f"Locker供應商: ✓ ({len(entity_config.locker_suppliers)})")
                    if entity_config.special_rules:
                        checks.append(f"特殊規則: ✓ ({len(entity_config.special_rules)})")
                
                print(f"  {entity}:")
                for check in checks:
                    print(f"    {check}")
                
                self.results['passed'].append(f"實體配置 - {entity}: ✓ 已載入")
            
            except Exception as e:
                self.results['failed'].append(f"實體配置 - {entity}: ✗ {str(e)}")
                print(f"  ✗ {entity}: {str(e)}")
        
        print()
    
    def validate_pivot_configs(self):
        """驗證Pivot配置"""
        print("✓ 驗證Pivot配置...")
        
        for entity in ['MOB', 'SPT', 'SPX']:
            for data_type in ['pr', 'po']:
                try:
                    # 從原配置讀取
                    original_pivot = self.original_config.get_pivot_config(entity, data_type)
                    
                    # 從Pipeline配置讀取
                    pipeline_pivot = self.pipeline_config.get_pivot_config(entity, data_type)
                    
                    # 比較index
                    if original_pivot.get('index') == pipeline_pivot.get('index'):
                        self.results['passed'].append(
                            f"Pivot配置 - {entity} {data_type.upper()}: ✓ 一致"
                        )
                        print(f"  ✓ {entity} {data_type.upper()}: 一致")
                    else:
                        self.results['failed'].append(
                            f"Pivot配置 - {entity} {data_type.upper()}: ✗ 不一致"
                        )
                        print(f"  ✗ {entity} {data_type.upper()}: 不一致")
                
                except Exception as e:
                    self.results['warnings'].append(
                        f"Pivot配置 - {entity} {data_type.upper()}: 無法驗證 - {str(e)}"
                    )
                    print(f"  ⚠ {entity} {data_type.upper()}: 無法驗證")
        
        print()
    
    def print_results(self):
        """輸出驗證結果"""
        print("=" * 80)
        print("驗證結果統計")
        print("=" * 80)
        print()
        
        print(f"✓ 通過: {len(self.results['passed'])} 項")
        print(f"✗ 失敗: {len(self.results['failed'])} 項")
        print(f"⚠ 警告: {len(self.results['warnings'])} 項")
        print()
        
        if self.results['failed']:
            print("=" * 80)
            print("失敗項目詳情")
            print("=" * 80)
            for failure in self.results['failed']:
                print(f"\n{failure}")
            print()
        
        if self.results['warnings']:
            print("=" * 80)
            print("警告項目")
            print("=" * 80)
            for warning in self.results['warnings']:
                print(f"⚠ {warning}")
            print()
        
        # 總體結果
        if not self.results['failed']:
            print("✓ 配置驗證通過！所有配置一致。")
            return True
        else:
            print("✗ 配置驗證失敗！存在不一致的配置項。")
            return False


def main():
    """主函數"""
    validator = ConfigValidator()
    success = validator.validate_all()
    
    # 返回退出碼
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
