"""ConfigManager 線程安全測試"""
import pytest
import threading
from accrual_bot.utils.config import ConfigManager


@pytest.mark.unit
class TestConfigManagerThreadSafety:
    """ConfigManager 線程安全測試"""

    def test_singleton_same_instance(self):
        """測試單例模式返回相同實例"""
        instance1 = ConfigManager()
        instance2 = ConfigManager()

        assert instance1 is instance2
        assert id(instance1) == id(instance2)

    def test_thread_safe_singleton(self):
        """測試多線程環境下的單例安全性"""
        results = []

        def get_instance():
            instance = ConfigManager()
            results.append(id(instance))

        # 創建100個線程同時獲取實例
        threads = [threading.Thread(target=get_instance) for _ in range(100)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 所有線程應該得到相同的實例
        unique_ids = set(results)
        assert len(unique_ids) == 1, f"Expected 1 unique instance, got {len(unique_ids)}"

    def test_config_data_integrity(self):
        """測試配置數據完整性"""
        instance = ConfigManager()

        # 驗證配置數據存在
        assert hasattr(instance, '_config_toml')
        assert isinstance(instance._config_toml, dict)

    @pytest.mark.slow
    def test_concurrent_access_stress(self):
        """壓力測試：大量並發訪問"""
        results = []
        errors = []

        def access_config():
            try:
                instance = ConfigManager()
                config = instance._config_toml
                results.append(id(instance))
            except Exception as e:
                errors.append(e)

        # 創建1000個線程
        threads = [threading.Thread(target=access_config) for _ in range(1000)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 不應該有任何錯誤
        assert len(errors) == 0
        # 所有線程得到相同實例
        assert len(set(results)) == 1


@pytest.mark.unit
class TestConfigManagerGet:
    """測試 ConfigManager.get() 方法"""

    def test_get_dot_notation(self):
        """測試 dot-notation 語法取值"""
        cm = ConfigManager()
        # 設定已知的 TOML 資料
        original = cm._config_toml.copy()
        cm._config_toml = {'pipeline': {'spt': {'name': 'test_value'}}}

        result = cm.get('pipeline.spt.name')
        assert result == 'test_value'

        # 還原
        cm._config_toml = original

    def test_get_dot_notation_missing_key(self):
        """測試 dot-notation 找不到鍵時返回 fallback"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'pipeline': {'spt': {}}}

        result = cm.get('pipeline.spt.nonexistent', fallback='default_val')
        assert result == 'default_val'

        cm._config_toml = original

    def test_get_two_args_toml_priority(self):
        """測試兩參數形式：TOML 優先於 INI"""
        cm = ConfigManager()
        original_toml = cm._config_toml.copy()
        original_data = cm._config_data.copy()

        cm._config_toml = {'test_section': {'key1': 'toml_value'}}
        cm._config_data = {'test_section': {'key1': 'ini_value'}}

        result = cm.get('test_section', 'key1')
        assert result == 'toml_value'

        cm._config_toml = original_toml
        cm._config_data = original_data

    def test_get_fallback_to_ini(self):
        """測試 TOML 找不到時 fallback 到 INI"""
        cm = ConfigManager()
        original_toml = cm._config_toml.copy()
        original_data = cm._config_data.copy()

        cm._config_toml = {}
        cm._config_data = {'MY_SECTION': {'my_key': 'ini_value'}}

        result = cm.get('MY_SECTION', 'my_key')
        assert result == 'ini_value'

        cm._config_toml = original_toml
        cm._config_data = original_data

    def test_get_returns_fallback_when_missing(self):
        """測試都找不到時返回 fallback"""
        cm = ConfigManager()
        original_toml = cm._config_toml.copy()
        original_data = cm._config_data.copy()

        cm._config_toml = {}
        cm._config_data = {}

        result = cm.get('nonexistent', 'key', fallback='my_fallback')
        assert result == 'my_fallback'

        cm._config_toml = original_toml
        cm._config_data = original_data


@pytest.mark.unit
class TestConfigManagerGetList:
    """測試 ConfigManager.get_list() 方法"""

    def test_get_list_toml_array(self):
        """測試 TOML 原生陣列直接回傳"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'section': {'items': ['a', 'b', 'c']}}

        result = cm.get_list('section', 'items')
        assert result == ['a', 'b', 'c']

        cm._config_toml = original

    def test_get_list_ini_comma_separated(self):
        """測試 INI 逗號分隔字串自動拆分"""
        cm = ConfigManager()
        original_toml = cm._config_toml.copy()
        original_data = cm._config_data.copy()

        cm._config_toml = {}
        cm._config_data = {'section': {'items': 'x, y, z'}}

        result = cm.get_list('section', 'items')
        assert result == ['x', 'y', 'z']

        cm._config_toml = original_toml
        cm._config_data = original_data

    def test_get_list_missing_returns_fallback(self):
        """測試找不到時返回 fallback"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {}

        result = cm.get_list('nonexistent', 'key', fallback=['default'])
        assert result == ['default']

        cm._config_toml = original

    def test_get_list_none_value_returns_fallback(self):
        """測試值為 None 時返回 fallback"""
        cm = ConfigManager()
        result = cm.get_list('nonexistent_section_xyz', 'nonexistent_key_xyz', fallback=['fb'])
        assert result == ['fb']


@pytest.mark.unit
class TestConfigManagerGetTyped:
    """測試 get_int / get_float / get_boolean 方法"""

    def test_get_int_valid(self):
        """測試取得整數值"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'sec': {'num': 42}}

        assert cm.get_int('sec', 'num') == 42

        cm._config_toml = original

    def test_get_int_invalid_returns_fallback(self):
        """測試無法轉換整數時返回 fallback"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'sec': {'num': 'not_a_number'}}

        assert cm.get_int('sec', 'num', fallback=99) == 99

        cm._config_toml = original

    def test_get_float_valid(self):
        """測試取得浮點數值"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'sec': {'val': 3.14}}

        assert cm.get_float('sec', 'val') == pytest.approx(3.14)

        cm._config_toml = original

    def test_get_boolean_true_values(self):
        """測試布林值 True 的各種表示"""
        cm = ConfigManager()
        original = cm._config_toml.copy()

        # TOML 原生布林
        cm._config_toml = {'sec': {'flag': True}}
        assert cm.get_boolean('sec', 'flag') is True

        # 字串形式
        cm._config_toml = {'sec': {'flag': 'yes'}}
        assert cm.get_boolean('sec', 'flag') is True

        cm._config_toml = original

    def test_get_boolean_false_values(self):
        """測試布林值 False 的各種表示"""
        cm = ConfigManager()
        original = cm._config_toml.copy()

        cm._config_toml = {'sec': {'flag': False}}
        assert cm.get_boolean('sec', 'flag') is False

        cm._config_toml = {'sec': {'flag': 'no'}}
        assert cm.get_boolean('sec', 'flag') is False

        cm._config_toml = original

    def test_get_boolean_missing_returns_fallback(self):
        """測試找不到布林值時返回 fallback"""
        cm = ConfigManager()
        assert cm.get_boolean('nonexistent_xyz', 'key_xyz', fallback=True) is True


@pytest.mark.unit
class TestConfigManagerSections:
    """測試 get_section / has_section / has_option / get_all 方法"""

    def test_get_section_toml(self):
        """測試從 TOML 取得段落"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'my_sec': {'k1': 'v1', 'k2': 'v2'}}

        result = cm.get_section('my_sec')
        assert result == {'k1': 'v1', 'k2': 'v2'}

        cm._config_toml = original

    def test_has_section_case_insensitive(self):
        """測試段落名稱大小寫不敏感查詢"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'spx': {'key': 'val'}}

        assert cm.has_section('SPX') is True
        assert cm.has_section('spx') is True
        assert cm.has_section('nonexistent_xyz') is False

        cm._config_toml = original

    def test_has_option(self):
        """測試檢查配置選項是否存在"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'sec': {'key1': 'val1'}}

        assert cm.has_option('sec', 'key1') is True
        assert cm.has_option('sec', 'missing_key') is False

        cm._config_toml = original

    def test_get_all_with_subsection(self):
        """測試 get_all 含子段落"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'parent': {'child': {'nested_key': 'nested_val'}}}

        result = cm.get_all('parent', 'child')
        assert result == {'nested_key': 'nested_val'}

        cm._config_toml = original

    def test_get_all_missing_subsection(self):
        """測試 get_all 子段落不存在時返回空字典"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'parent': {'other': 'val'}}

        result = cm.get_all('parent', 'nonexistent')
        assert result == {}

        cm._config_toml = original


@pytest.mark.unit
class TestConfigManagerUtility:
    """測試 set_config / to_dict / get_nested / deep_merge / get_paths_config"""

    def test_set_config(self):
        """測試運行時設定配置值"""
        cm = ConfigManager()
        original = cm._config_data.copy()

        cm.set_config('NEW_SEC', 'new_key', 'new_value')
        assert cm._config_data['NEW_SEC']['new_key'] == 'new_value'

        cm._config_data = original

    def test_to_dict(self):
        """測試 to_dict 返回完整配置副本"""
        cm = ConfigManager()
        result = cm.to_dict()

        assert isinstance(result, dict)
        # 應包含 INI 的資料
        # 如果有 TOML 資料，應有 _toml 鍵
        if cm._config_toml:
            assert '_toml' in result

    def test_get_nested(self):
        """測試多層巢狀取值"""
        cm = ConfigManager()
        original = cm._config_toml.copy()
        cm._config_toml = {'level1': {'level2': {'level3': 'deep_value'}}}

        result = cm.get_nested('level1', 'level2', 'level3')
        assert result == 'deep_value'

        cm._config_toml = original

    def test_get_nested_missing_returns_fallback(self):
        """測試巢狀取值找不到時返回 fallback"""
        cm = ConfigManager()
        result = cm.get_nested('nonexistent', 'path', fallback='default')
        assert result == 'default'

    def test_deep_merge(self):
        """測試遞歸合併字典，override 優先"""
        base = {'a': 1, 'b': {'c': 2, 'd': 3}}
        override = {'b': {'c': 99, 'e': 5}, 'f': 6}

        result = ConfigManager._deep_merge(base, override)

        assert result['a'] == 1
        assert result['b']['c'] == 99  # override 優先
        assert result['b']['d'] == 3   # base 保留
        assert result['b']['e'] == 5   # 新增
        assert result['f'] == 6        # 新增

    def test_get_paths_config(self):
        """測試 get_paths_config 取得 paths.toml 配置"""
        cm = ConfigManager()
        original = cm._paths_toml.copy()
        cm._paths_toml = {'spt': {'procurement': {'params': {'skip_rows': 1}}}}

        result = cm.get_paths_config('spt', 'procurement', 'params')
        assert result == {'skip_rows': 1}

        cm._paths_toml = original

    def test_get_paths_config_missing(self):
        """測試 get_paths_config 找不到時返回 None"""
        cm = ConfigManager()
        result = cm.get_paths_config('nonexistent_xyz', 'path_xyz')
        assert result is None
