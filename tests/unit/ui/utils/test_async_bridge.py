"""
AsyncBridge 單元測試

測試在同步環境中執行 async coroutines 的橋接功能。
"""

import asyncio
import threading

import pytest

from accrual_bot.ui.utils.async_bridge import AsyncBridge


# --- 測試用的 async 輔助函式 ---

async def async_return_value(value):
    """回傳指定值的 coroutine"""
    return value


async def async_add(a, b):
    """簡單加法 coroutine"""
    return a + b


async def async_raise(exc):
    """拋出指定例外的 coroutine"""
    raise exc


async def async_sleep_and_return(seconds, value):
    """等待後回傳值的 coroutine"""
    await asyncio.sleep(seconds)
    return value


async def async_none():
    """回傳 None 的 coroutine"""
    return None


@pytest.mark.unit
class TestAsyncBridgeRunAsync:
    """測試 AsyncBridge.run_async() 方法"""

    def test_run_async_returns_value(self):
        """驗證 run_async 能正確回傳 coroutine 的結果"""
        result = AsyncBridge.run_async(async_return_value(42))
        assert result == 42

    def test_run_async_returns_string(self):
        """驗證 run_async 能正確回傳字串結果"""
        result = AsyncBridge.run_async(async_return_value("hello"))
        assert result == "hello"

    def test_run_async_returns_complex_object(self):
        """驗證 run_async 能回傳複雜物件（如 dict）"""
        expected = {"key": "value", "count": 10}
        result = AsyncBridge.run_async(async_return_value(expected))
        assert result == expected

    def test_run_async_with_computation(self):
        """驗證 run_async 能執行帶有運算邏輯的 coroutine"""
        result = AsyncBridge.run_async(async_add(3, 7))
        assert result == 10

    def test_run_async_propagates_exception(self):
        """驗證 run_async 會將 coroutine 中的例外正確傳播"""
        with pytest.raises(ValueError, match="test error"):
            AsyncBridge.run_async(async_raise(ValueError("test error")))

    def test_run_async_propagates_runtime_error(self):
        """驗證 run_async 能傳播 RuntimeError"""
        with pytest.raises(RuntimeError, match="something broke"):
            AsyncBridge.run_async(async_raise(RuntimeError("something broke")))

    def test_run_async_returns_none(self):
        """驗證 coroutine 回傳 None 時，run_async 也回傳 None"""
        # 注意：原始實作在 result_queue 為空時會拋出 RuntimeError，
        # 但 None 會被放入 queue，所以應正常回傳
        result = AsyncBridge.run_async(async_none())
        assert result is None

    def test_run_async_with_short_async_sleep(self):
        """驗證 run_async 能處理包含 await 的 coroutine"""
        result = AsyncBridge.run_async(async_sleep_and_return(0.05, "done"))
        assert result == "done"


@pytest.mark.unit
class TestAsyncBridgeRunInThread:
    """測試 AsyncBridge.run_in_thread() 方法"""

    def test_run_in_thread_returns_thread(self):
        """驗證 run_in_thread 回傳 threading.Thread 物件"""
        thread = AsyncBridge.run_in_thread(async_return_value(1))
        assert isinstance(thread, threading.Thread)
        thread.join(timeout=5)

    def test_run_in_thread_callback_invoked(self):
        """驗證成功完成時會呼叫 callback"""
        results = []

        def on_success(value):
            results.append(value)

        thread = AsyncBridge.run_in_thread(
            async_return_value(99),
            callback=on_success
        )
        thread.join(timeout=5)

        assert results == [99]

    def test_run_in_thread_error_callback_invoked(self):
        """驗證發生錯誤時會呼叫 error_callback"""
        errors = []

        def on_error(exc):
            errors.append(exc)

        thread = AsyncBridge.run_in_thread(
            async_raise(ValueError("bg error")),
            error_callback=on_error
        )
        thread.join(timeout=5)

        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        assert str(errors[0]) == "bg error"

    def test_run_in_thread_no_callback(self):
        """驗證不提供 callback 時不會出錯（結果被丟棄）"""
        thread = AsyncBridge.run_in_thread(async_return_value("ignored"))
        thread.join(timeout=5)
        # 只要不拋出例外就算通過
        assert not thread.is_alive()

    def test_run_in_thread_no_error_callback_raises(self):
        """驗證沒有 error_callback 且 coroutine 拋出例外時，例外會在背景執行緒中傳播"""
        # 沒有 error_callback 時，worker 中的 raise 會在背景執行緒中發生，
        # 不會影響主執行緒，但執行緒會結束
        thread = AsyncBridge.run_in_thread(
            async_raise(RuntimeError("unhandled"))
        )
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_run_in_thread_is_daemon(self):
        """驗證 run_in_thread 建立的執行緒為 daemon 執行緒"""
        thread = AsyncBridge.run_in_thread(async_return_value(1))
        assert thread.daemon is True
        thread.join(timeout=5)
