#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
專案完整性檢查腳本

檢查整個專案中的日誌使用是否一致，避免潛在的重複問題
"""

import os
import sys
from pathlib import Path
import re
from typing import List, Dict, Tuple

def scan_python_files(root_path: str) -> List[Path]:
    """掃描所有Python文件"""
    python_files = []
    for root, dirs, files in os.walk(root_path):
        # 跳過 __pycache__ 和 .git 目錄
        dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git']]
        
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)
    
    return python_files

def check_logging_imports(files: List[Path]) -> Dict[str, List[str]]:
    """檢查日誌相關的導入"""
    issues = {
        'direct_logging_imports': [],
        'basicconfig_usage': [],
        'multiple_logger_creations': [],
        'print_statements': []
    }
    
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                
                # 檢查直接導入 logging
                if re.search(r'^import logging$|^from logging import', content, re.MULTILINE):
                    if 'utils/logging' not in str(file_path):  # 允許在我們的日誌模組中使用
                        issues['direct_logging_imports'].append(str(file_path))
                
                # 檢查 basicConfig 的使用
                if 'logging.basicConfig' in content:
                    issues['basicconfig_usage'].append(str(file_path))
                
                # 檢查多個 Logger 實例創建
                logger_creations = re.findall(r'logging\.getLogger|Logger\(\)', content)
                if len(logger_creations) > 1:
                    issues['multiple_logger_creations'].append(f"{file_path}: {len(logger_creations)} 個創建")
                
                # 檢查 print 語句（在非測試文件中）
                if 'test' not in str(file_path).lower():
                    print_matches = re.findall(r'^\s*print\s*\(', content, re.MULTILINE)
                    if print_matches:
                        issues['print_statements'].append(f"{file_path}: {len(print_matches)} 個print語句")
                        
        except Exception as e:
            print(f"警告: 無法讀取文件 {file_path}: {e}")
    
    return issues

def check_logger_usage_patterns(files: List[Path]) -> Dict[str, List[str]]:
    """檢查日誌使用模式"""
    patterns = {
        'good_patterns': [],
        'inconsistent_patterns': [],
        'missing_logger_init': []
    }
    
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # 檢查好的模式
                if 'from utils.logging import get_logger' in content:
                    patterns['good_patterns'].append(str(file_path))
                elif 'get_logger(' in content:
                    patterns['good_patterns'].append(str(file_path))
                
                # 檢查是否有日誌調用但沒有初始化
                has_log_calls = any(pattern in content for pattern in 
                                    ['logger.info', 'logger.error', 'logger.warning', 'logger.debug'])
                has_logger_init = any(pattern in content for pattern in 
                                     ['get_logger', 'Logger()', 'logger ='])
                
                if has_log_calls and not has_logger_init:
                    patterns['missing_logger_init'].append(str(file_path))
                    
        except Exception as e:
            print(f"警告: 無法檢查文件 {file_path}: {e}")
    
    return patterns

def generate_report(issues: Dict[str, List[str]], patterns: Dict[str, List[str]]) -> str:
    """生成檢查報告"""
    report = []
    report.append("=" * 60)
    report.append("📋 專案日誌完整性檢查報告")
    report.append("=" * 60)
    
    # 問題檢查
    report.append("\n🔍 潛在問題檢查:")
    report.append("-" * 40)
    
    if issues['direct_logging_imports']:
        report.append("⚠️ 直接導入 logging 模組的文件:")
        for file in issues['direct_logging_imports']:
            report.append(f"  - {file}")
        report.append("  建議: 使用專案的 utils.logging 系統")
    else:
        report.append("✅ 沒有發現直接導入 logging 的問題")
    
    if issues['basicconfig_usage']:
        report.append("\n❌ 使用 logging.basicConfig 的文件:")
        for file in issues['basicconfig_usage']:
            report.append(f"  - {file}")
        report.append("  建議: 移除 basicConfig 調用，使用統一的日誌系統")
    else:
        report.append("\n✅ 沒有發現 basicConfig 使用問題")
    
    if issues['multiple_logger_creations']:
        report.append("\n⚠️ 多次創建 Logger 的文件:")
        for file in issues['multiple_logger_creations']:
            report.append(f"  - {file}")
        report.append("  建議: 每個模組只創建一個 logger 實例")
    else:
        report.append("\n✅ 沒有發現多重 Logger 創建問題")
    
    if issues['print_statements']:
        report.append("\n⚠️ 使用 print 語句的文件 (非測試文件):")
        for file in issues['print_statements'][:5]:  # 只顯示前5個
            report.append(f"  - {file}")
        if len(issues['print_statements']) > 5:
            report.append(f"  ... 和其他 {len(issues['print_statements']) - 5} 個文件")
        report.append("  建議: 將 print 替換為 logger 調用")
    else:
        report.append("\n✅ 沒有發現不當的 print 使用")
    
    # 好的模式
    report.append(f"\n✅ 正確使用日誌系統的文件: {len(patterns['good_patterns'])} 個")
    
    if patterns['missing_logger_init']:
        report.append("\n⚠️ 有日誌調用但缺少 logger 初始化的文件:")
        for file in patterns['missing_logger_init']:
            report.append(f"  - {file}")
    
    # 統計摘要
    report.append("\n" + "=" * 60)
    report.append("📊 檢查摘要:")
    report.append("=" * 60)
    
    total_issues = sum(len(files) for files in issues.values())
    report.append(f"總計發現問題: {total_issues} 個")
    report.append(f"正確使用日誌的文件: {len(patterns['good_patterns'])} 個")
    
    if total_issues == 0:
        report.append("\n🎉 恭喜！沒有發現日誌使用問題，專案日誌系統很健康！")
    else:
        report.append(f"\n⚠️ 建議修復上述問題以避免日誌重複或其他問題")
    
    return "\n".join(report)

def main():
    """主要檢查函數"""
    print("🚀 開始專案完整性檢查...")
    
    # 獲取專案根目錄
    current_dir = Path(__file__).parent
    
    # 掃描所有Python文件
    print("📁 掃描Python文件...")
    python_files = scan_python_files(str(current_dir))
    print(f"找到 {len(python_files)} 個Python文件")
    
    # 執行檢查
    print("🔍 檢查日誌導入和使用...")
    issues = check_logging_imports(python_files)
    
    print("🔍 檢查日誌使用模式...")
    patterns = check_logger_usage_patterns(python_files)
    
    # 生成報告
    print("📝 生成檢查報告...")
    report = generate_report(issues, patterns)
    
    # 輸出報告
    print(report)
    
    # 保存報告到文件
    report_file = current_dir / "07.專案完整性檢查報告.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 專案日誌完整性檢查報告\n\n")
        f.write(report)
    
    print(f"\n📄 報告已保存到: {report_file}")
    
    # 返回是否有嚴重問題
    serious_issues = (len(issues['basicconfig_usage']) + 
                     len(issues['multiple_logger_creations']))
    
    if serious_issues == 0:
        print("\n✅ 專案日誌系統檢查通過！")
        return True
    else:
        print(f"\n⚠️ 發現 {serious_issues} 個需要關注的問題")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
