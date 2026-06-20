import threading
import time

from feishu_agent_bot.event_queue import EventQueue


def test_submit_does_not_wait_for_slow_handler():
    started = threading.Event()
    release = threading.Event()
    handled = []

    def handler(event):
        started.set()
        release.wait(timeout=2)
        handled.append(event)

    events = EventQueue(handler, worker_count=1, max_size=10)
    events.start()
    started_at = time.monotonic()
    events.submit("message")
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.1
    assert started.wait(timeout=1)
    release.set()
    events.shutdown()
    assert handled == ["message"]
