"""Logger 模組單元測試"""
import logging
import threading
import pytest
from unittest.mock import patch, MagicMock

from accrual_bot.utils.logging.logger import (
    Logger,
    StructuredLogger,
    ColoredFormatter,
    ColorCodes,
    get_logger,
    get_structured_logger,
)


@pytest.fixture(autouse=True)
def reset_logger_singleton():
    """每個測試前後重置 Logger 單例狀態，避免測試互相影響"""
    yield
    # 重置單例
    with Logger._lock:
        if Logger._instance is not None:
            try:
                Logger._instance.cleanup()
            except Exception:
                pass
            Logger._instance = None
            Logger._initialized = False


@pytest.mark.unit
class TestLoggerSingleton:
    """測試 Logger 單例模式"""

    def test_singleton_returns_same_instance(self):
        """多次建立 Logger 應返回同一實例"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger1 = Logger()
            logger2 = Logger()
            assert logger1 is logger2

    def test_singleton_thread_safety(self):
        """多執行緒下建立 Logger 應返回同一實例"""
        instances = []

        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'

            def create_logger():
                instances.append(Logger())

            threads = [threading.Thread(target=create_logger) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # 所有實例應該是同一個物件
        assert all(inst is instances[0] for inst in instances)


@pytest.mark.unit
class TestLoggerGetLogger:
    """測試 Logger.get_logger 方法"""

    def test_get_root_logger(self):
        """取得 root logger"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            root = logger_instance.get_logger()
            assert root is not None
            assert isinstance(root, logging.Logger)

    def test_get_named_logger(self):
        """取得命名 logger，應為 accrual_bot 的子記錄器"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            named = logger_instance.get_logger('test_module')
            assert named.name == 'accrual_bot.test_module'

    def test_get_logger_caches_instance(self):
        """同一名稱多次呼叫應返回同一 logger"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            logger_a = logger_instance.get_logger('cached')
            logger_b = logger_instance.get_logger('cached')
            assert logger_a is logger_b


@pytest.mark.unit
class TestLoggerSetLevel:
    """測試 Logger.set_level 方法"""

    def test_set_level_for_specific_logger(self):
        """設定特定 logger 的級別"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            logger_instance.get_logger('level_test')
            logger_instance.set_level('DEBUG', 'level_test')
            assert logger_instance._loggers['level_test'].level == logging.DEBUG

    def test_set_level_for_all_loggers(self):
        """不指定名稱時應設定所有 logger 的級別"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            logger_instance.get_logger('a')
            logger_instance.get_logger('b')
            logger_instance.set_level('WARNING')
            for lgr in logger_instance._loggers.values():
                assert lgr.level == logging.WARNING


@pytest.mark.unit
class TestLoggerGetLogStats:
    """測試 Logger.get_log_stats 方法"""

    def test_returns_stats_dict(self):
        """應返回包含 loggers_count 和 handlers_count 的字典"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            stats = logger_instance.get_log_stats()
            assert 'loggers_count' in stats
            assert 'handlers_count' in stats
            assert 'logger_names' in stats
            assert 'handler_names' in stats


@pytest.mark.unit
class TestLoggerCleanup:
    """測試 Logger.cleanup 方法"""

    def test_cleanup_clears_state(self):
        """cleanup 後應清空所有 loggers 和 handlers"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            logger_instance.get_logger('cleanup_test')
            logger_instance.cleanup()
            assert len(logger_instance._loggers) == 0
            assert len(logger_instance._handlers) == 0
            assert logger_instance._initialized is False


@pytest.mark.unit
class TestColoredFormatter:
    """測試 ColoredFormatter"""

    def test_format_without_color(self):
        """不使用顏色時應正常格式化"""
        formatter = ColoredFormatter(fmt='%(levelname)s: %(message)s', use_color=False)
        record = logging.LogRecord(
            name='test', level=logging.INFO, pathname='', lineno=0,
            msg='hello', args=(), exc_info=None
        )
        result = formatter.format(record)
        assert 'INFO' in result
        assert 'hello' in result


@pytest.mark.unit
class TestStructuredLogger:
    """測試 StructuredLogger"""

    def test_log_operation_start(self):
        """log_operation_start 不應拋出異常"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            sl = StructuredLogger('structured_test')
            # 不應拋出異常
            sl.log_operation_start('test_op', key='value')

    def test_log_progress(self):
        """log_progress 不應拋出異常"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            sl = StructuredLogger('progress_test')
            sl.log_progress(50, 100, operation='processing')


@pytest.mark.unit
class TestGetLoggerConvenienceFunction:
    """測試 get_logger 便利函數"""

    def test_get_logger_returns_logging_logger(self):
        """get_logger 應返回 logging.Logger 實例"""
        result = get_logger('convenience_test')
        assert isinstance(result, logging.Logger)

    def test_get_structured_logger_returns_instance(self):
        """get_structured_logger 應返回 StructuredLogger 實例"""
        result = get_structured_logger('structured_convenience')
        assert isinstance(result, StructuredLogger)
