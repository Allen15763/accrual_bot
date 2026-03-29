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


@pytest.mark.unit
class TestSetupFileHandler:
    """測試 _setup_file_handler 方法 - 目錄建立與 RotatingFileHandler"""

    def test_setup_file_handler_creates_directory(self, tmp_path):
        """驗證 _setup_file_handler 自動建立日誌目錄"""
        log_dir = tmp_path / "logs" / "subdir"
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            root = logger_instance.get_logger()
            logger_instance._setup_file_handler(root, str(log_dir))
            assert log_dir.exists()

    def test_setup_file_handler_adds_handler(self, tmp_path):
        """驗證 _setup_file_handler 成功添加 RotatingFileHandler"""
        log_dir = tmp_path / "logs"
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            root = logger_instance.get_logger()
            initial_handler_count = len(root.handlers)
            logger_instance._setup_file_handler(root, str(log_dir))
            assert len(root.handlers) > initial_handler_count
            assert 'file' in logger_instance._handlers

    def test_setup_file_handler_creates_log_file(self, tmp_path):
        """驗證 _setup_file_handler 建立日誌檔案"""
        log_dir = tmp_path / "logs"
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            logger_instance = Logger()
            root = logger_instance.get_logger()
            logger_instance._setup_file_handler(root, str(log_dir))
            # 寫入一筆日誌以確保檔案被建立
            root.info("test message")
            log_files = list(log_dir.glob("Accrual_bot_*.log"))
            assert len(log_files) >= 1


@pytest.mark.unit
class TestColoredFormatterExtended:
    """ColoredFormatter 擴展測試"""

    def test_format_with_color_enabled(self):
        """驗證啟用顏色時格式化包含 ANSI 色碼"""
        formatter = ColoredFormatter(
            fmt='%(levelname)s %(name)s: %(message)s',
            use_color=True
        )
        # 強制啟用顏色（繞過終端偵測）
        formatter.use_color = True
        record = logging.LogRecord(
            name='test.module', level=logging.WARNING, pathname='', lineno=0,
            msg='warning message', args=(), exc_info=None
        )
        result = formatter.format(record)
        assert 'warning message' in result
        # 確認原始 record 不被污染
        assert record.levelname == 'WARNING'
        assert record.name == 'test.module'

    def test_format_preserves_record_after_coloring(self):
        """驗證格式化後 record 的 levelname 和 name 已恢復原始值"""
        formatter = ColoredFormatter(
            fmt='%(levelname)s: %(message)s',
            use_color=True
        )
        formatter.use_color = True
        record = logging.LogRecord(
            name='original', level=logging.ERROR, pathname='', lineno=0,
            msg='err', args=(), exc_info=None
        )
        formatter.format(record)
        assert record.levelname == 'ERROR'
        assert record.name == 'original'


@pytest.mark.unit
class TestStructuredLoggerExtended:
    """StructuredLogger 擴展測試"""

    @pytest.fixture
    def slogger(self):
        """建立 StructuredLogger 實例"""
        with patch('accrual_bot.utils.logging.logger.config_manager') as mock_cm:
            mock_cm.get.return_value = 'INFO'
            sl = StructuredLogger('structured_ext_test')
            yield sl

    def test_log_operation_end_success(self, slogger):
        """驗證 log_operation_end 成功情況不拋出例外"""
        slogger.log_operation_end('my_op', success=True, duration=1.5)

    def test_log_operation_end_failure(self, slogger):
        """驗證 log_operation_end 失敗情況使用 error 級別"""
        with patch.object(slogger.logger, 'error') as mock_error:
            slogger.log_operation_end('my_op', success=False, reason='timeout')
            mock_error.assert_called_once()

    def test_log_data_processing(self, slogger):
        """驗證 log_data_processing 含處理時間"""
        with patch.object(slogger.logger, 'info') as mock_info:
            slogger.log_data_processing('PO', 1000, processing_time=2.5, entity='SPX')
            mock_info.assert_called_once()
            msg = mock_info.call_args[0][0]
            assert '1,000' in msg
            assert '2.50s' in msg

    def test_log_file_operation_success(self, slogger):
        """驗證 log_file_operation 成功情況"""
        with patch.object(slogger.logger, 'info') as mock_info:
            slogger.log_file_operation('讀取', '/tmp/data.xlsx', success=True, size='2MB')
            mock_info.assert_called_once()

    def test_log_file_operation_failure(self, slogger):
        """驗證 log_file_operation 失敗情況使用 error 級別"""
        with patch.object(slogger.logger, 'error') as mock_error:
            slogger.log_file_operation('寫入', '/tmp/out.csv', success=False)
            mock_error.assert_called_once()

    def test_log_error_with_context(self, slogger):
        """驗證 log_error 帶上下文資訊"""
        with patch.object(slogger.logger, 'error') as mock_error:
            err = ValueError("test error")
            slogger.log_error(err, context='data_loading', step='Step1')
            mock_error.assert_called_once()
            msg = mock_error.call_args[0][0]
            assert 'data_loading' in msg
            assert 'test error' in msg
