"""
SPX PPE_DESC 模組級純函式單元測試

測試 spx_ppe_desc 模組中的獨立業務邏輯函式：
- extract_clean_description: 摘要提取
- extract_locker_info: 智取櫃資訊提取
- extract_address_from_dataframe: 台灣地址提取
- _hd_locker_info: HD 智取櫃標記
- _process_description: 說明欄位整合處理
- _process_contract_period: 年限對應
"""

import pytest
import numpy as np
import pandas as pd

from accrual_bot.tasks.spx.steps.spx_ppe_desc import (
    extract_clean_description,
    extract_locker_info,
    extract_address_from_dataframe,
    _hd_locker_info,
    _process_description,
    _process_contract_period,
)


# =============================================================================
# extract_clean_description
# =============================================================================


@pytest.mark.unit
class TestExtractCleanDescription:
    """extract_clean_description 測試"""

    def test_store_decoration_pattern(self):
        """Test 1: 門市裝修工程模式正確提取"""
        desc = (
            '門市裝修工程-台北信義店(台北市信義區松壽路10號) '
            'SPX store decoration 第一期款項 #tag123'
        )
        result = extract_clean_description(desc)
        assert result.startswith('SPX_門市裝修工程-')
        assert '台北市信義區' in result
        assert '第一期款項' in result

    def test_address_pattern(self):
        """Test 2: SVP 地址模式正確提取"""
        desc = 'SVP_SPX 門市智取櫃工程(台北市中山區南京東路100號)'
        result = extract_clean_description(desc)
        assert result.startswith('SPX_')
        assert '台北市中山區' in result

    def test_generic_cleanup(self):
        """Test 3: 通用清理規則"""
        desc = '2025/01 SVP_SPX 網路設備採購 #PO12345'
        result = extract_clean_description(desc)
        assert result.startswith('SPX_')
        assert '#PO12345' not in result
        assert '2025/01' not in result


# =============================================================================
# extract_locker_info
# =============================================================================


@pytest.mark.unit
class TestExtractLockerInfo:
    """extract_locker_info 測試"""

    def test_valid_locker_text(self):
        """Test 4: 正確提取智取櫃資訊"""
        text = '門市智取櫃工程SPX locker ML-300'
        result = extract_locker_info(text)
        assert result == 'ML-300'

    def test_non_string_returns_none(self):
        """Test 5: 非字串輸入回傳 None"""
        assert extract_locker_info(123) is None
        assert extract_locker_info(None) is None

    def test_no_match_returns_none(self):
        """Test 6: 無匹配回傳 None"""
        assert extract_locker_info('辦公設備採購') is None


# =============================================================================
# extract_address_from_dataframe
# =============================================================================


@pytest.mark.unit
class TestExtractAddressFromDataframe:
    """extract_address_from_dataframe 測試"""

    def test_extracts_taiwan_address(self):
        """Test 7: 正確提取括號內台灣地址"""
        df = pd.DataFrame({
            'desc': ['門市裝修(台北市信義區松壽路10號)'],
        })
        result = extract_address_from_dataframe(df, 'desc')
        assert 'extracted_address' in result.columns
        assert '台北市信義區松壽路10號' in result['extracted_address'].iloc[0]

    def test_no_address_returns_nan(self):
        """Test 8: 無地址回傳 NaN"""
        df = pd.DataFrame({'desc': ['普通文字沒有地址']})
        result = extract_address_from_dataframe(df, 'desc')
        assert pd.isna(result['extracted_address'].iloc[0])


# =============================================================================
# _hd_locker_info
# =============================================================================


@pytest.mark.unit
class TestHdLockerInfo:
    """_hd_locker_info 測試"""

    def _make_df(self, descriptions):
        """建立含必要欄位的 DataFrame"""
        return pd.DataFrame({
            'Item Description': descriptions,
            'locker_type': [pd.NA] * len(descriptions),
        })

    def test_marks_hd_main_locker(self):
        """Test 9: 標記 HD主櫃"""
        df = self._make_df(['SPX HD locker 控制主櫃 ABC'])
        result = _hd_locker_info(df)
        assert result['locker_type'].iloc[0] == 'HD主櫃'

    def test_marks_hd_installation_fee(self):
        """Test 10: 標記 HD安裝運費"""
        df = self._make_df(['SPX HD locker 安裝運費'])
        result = _hd_locker_info(df)
        assert result['locker_type'].iloc[0] == 'HD安裝運費'

    def test_marks_hd_locker(self):
        """Test 11: 標記 HD櫃（一般 HD locker）"""
        df = self._make_df(['SPX HD locker 設備'])
        result = _hd_locker_info(df)
        assert result['locker_type'].iloc[0] == 'HD櫃'


# =============================================================================
# _process_description
# =============================================================================


@pytest.mark.unit
class TestProcessDescription:
    """_process_description 測試"""

    def test_adds_expected_columns(self):
        """Test 12: 處理後新增預期欄位"""
        df = pd.DataFrame({
            'Item Description': [
                '2025/01 SVP_SPX 辦公用品 #tag1',
                '門市智取櫃工程SPX locker ML-300 (台北市中山區南京東路100號)',
            ],
        })
        result = _process_description(df)

        expected_cols = [
            'New_Extracted_Result',
            'New_Extracted_Result_without_第n期款項',
            'extracted_address',
        ]
        for col in expected_cols:
            assert col in result.columns, f"缺少欄位: {col}"


# =============================================================================
# _process_contract_period
# =============================================================================


@pytest.mark.unit
class TestProcessContractPeriod:
    """_process_contract_period 測試"""

    def _make_dep(self):
        """建立年限表 DataFrame"""
        return pd.DataFrame({
            'address': [
                '台北市信義區松壽路10號',
                '新北市板橋區中山路50號',
            ],
            'truncated_address': [
                '台北市信義區松壽路10號',
                '新北市板橋區中山路50號',
            ],
            'months_diff': [36, 24],
        })

    def test_maps_full_address(self):
        """Test 13: 完整地址對應成功"""
        df = pd.DataFrame({
            'extracted_address': ['台北市信義區松壽路10號'],
        })
        result = _process_contract_period(df, self._make_dep())
        assert result['months_diff'].iloc[0] == 36

    def test_falls_back_to_truncated_address(self):
        """Test 14: 完整地址無匹配時使用截短地址 fallback"""
        dep = pd.DataFrame({
            'address': ['台北市信義區松壽路10號1樓'],  # 完整地址不同
            'truncated_address': ['台北市信義區松壽路10號'],  # 截短地址匹配
            'months_diff': [48],
        })
        df = pd.DataFrame({
            'extracted_address': ['台北市信義區松壽路10號'],
        })
        result = _process_contract_period(df, dep)
        assert result['months_diff'].iloc[0] == 48

    def test_no_match_leaves_nan(self):
        """Test 15: 無匹配時保持 NaN"""
        df = pd.DataFrame({
            'extracted_address': ['高雄市前鎮區中華路99號'],
        })
        result = _process_contract_period(df, self._make_dep())
        assert pd.isna(result['months_diff'].iloc[0])
