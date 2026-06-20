from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class EventQueue:
    """Keeps synchronous HTTP replies off the lark-oapi WebSocket event loop."""

    def __init__(
        self,
        handler: Callable[[Any], None],
        worker_count: int = 2,
        max_size: int = 1000,
    ):
        self.handler = handler
        self.worker_count = worker_count
        self.queue: queue.Queue[Any | None] = queue.Queue(maxsize=max_size)
        self.workers: list[threading.Thread] = []
        self._accepting = True

    def start(self) -> None:
        for index in range(self.worker_count):
            worker = threading.Thread(
                target=self._worker,
                name=f"event-worker-{index + 1}",
                daemon=False,
            )
            worker.start()
            self.workers.append(worker)

    def submit(self, event: Any) -> None:
        if not self._accepting:
            logger.warning("服务正在停止，忽略新飞书事件")
            return
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            logger.error("飞书消息处理队列已满，拒绝新事件")
            raise RuntimeError("event queue is full")

    def shutdown(self) -> None:
        self._accepting = False
        self.queue.join()
        for _ in self.workers:
            self.queue.put(None)
        for worker in self.workers:
            worker.join()

    def _worker(self) -> None:
        while True:
            event = self.queue.get()
            try:
                if event is None:
                    return
                self.handler(event)
            except Exception:
                logger.exception("飞书消息处理 Worker 异常")
            finally:
                self.queue.task_done()
