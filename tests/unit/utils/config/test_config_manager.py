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
