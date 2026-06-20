import pytest

from feishu_agent_bot.instance_lock import InstanceAlreadyRunning, InstanceLock


def test_second_instance_is_rejected(tmp_path):
    first = InstanceLock(tmp_path / "bot.lock")
    second = InstanceLock(tmp_path / "bot.lock")
    first.acquire()
    try:
        with pytest.raises(InstanceAlreadyRunning):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()
