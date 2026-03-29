"""
Async Bridge

在 sync Streamlit 環境中執行 async coroutines。
"""

import asyncio
import threading
from typing import Callable, Any, Optional


class AsyncBridge:
    """在 sync Streamlit 中執行 async coroutines"""

    @staticmethod
    def run_async(coro, timeout: int = 600) -> Any:
        """
        同步執行 async coroutine

        Args:
            coro: Async coroutine 物件
            timeout: 超時秒數（預設 600 秒 = 10 分鐘）

        Returns:
            Coroutine 的執行結果
        """
        import threading
        import queue

        # 使用新線程執行，避免 event loop 衝突
        result_queue = queue.Queue()
        exception_queue = queue.Queue()

        def run_in_thread():
            try:
                # 在新線程中創建新的 event loop
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    result = new_loop.run_until_complete(coro)
                    result_queue.put(result)
                finally:
                    new_loop.close()
            except Exception as e:
                exception_queue.put(e)

        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        # 檢查是否有異常
        if not exception_queue.empty():
            raise exception_queue.get()

        # 先檢查是否有結果（修復 race condition：thread 在 timeout 邊界完成時，
        # is_alive() 可能短暫仍為 True，但 result 已在 queue 中）
        if not result_queue.empty():
            return result_queue.get()

        # 最後才判斷超時
        if thread.is_alive():
            raise TimeoutError(f"Pipeline 執行超時（{timeout // 60}分鐘）")

        raise RuntimeError("Pipeline 執行失敗：無返回結果")

    @staticmethod
    def run_in_thread(
        coro,
        callback: Optional[Callable[[Any], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None
    ) -> threading.Thread:
        """
        在背景執行緒中執行 async coroutine

        Args:
            coro: Async coroutine 物件
            callback: 成功完成時的回調函數
            error_callback: 發生錯誤時的回調函數

        Returns:
            執行中的 Thread 物件
        """
        def worker():
            try:
                result = AsyncBridge.run_async(coro)
                if callback:
                    callback(result)
            except Exception as e:
                if error_callback:
                    error_callback(e)
                else:
                    raise

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        return thread
