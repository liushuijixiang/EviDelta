from types import SimpleNamespace

import pytest

from feishu_agent_bot.feishu_client import (
    FeishuMessenger,
    FileTooLargeError,
    split_message,
)


def test_message_splitting_prefers_newline():
    assert split_message("12345\n67890", 6) == ["12345", "67890"]


def test_message_splitting_hard_boundary():
    assert split_message("abcdefgh", 3) == ["abc", "def", "gh"]


def test_report_file_upload_and_send(tmp_path):
    path = tmp_path / "report.md"
    path.write_text("# report", encoding="utf-8")
    calls = []

    class Response:
        code = 0
        msg = ""

        def __init__(self, file_key=None):
            self.data = SimpleNamespace(file_key=file_key)

        def success(self):
            return True

    class FileAPI:
        def create(self, request):
            calls.append(("upload", request.body.file_name))
            return Response("file-key")

    class MessageAPI:
        def create(self, request):
            calls.append(("send", request.body.msg_type, request.body.content))
            return Response()

    client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(file=FileAPI(), message=MessageAPI())
        )
    )
    FeishuMessenger(client).send_file_to_chat("chat-id", path)
    assert calls[0] == ("upload", "report.md")
    assert calls[1][0:2] == ("send", "file")
    assert "file-key" in calls[1][2]


def test_report_file_is_rejected_before_upload_when_too_large(tmp_path):
    path = tmp_path / "large.pdf"
    path.write_bytes(b"1234")
    client = SimpleNamespace()

    with pytest.raises(FileTooLargeError, match="超过飞书发送限制"):
        FeishuMessenger(client, max_file_bytes=3).send_file_to_chat(
            "chat-id", path
        )
