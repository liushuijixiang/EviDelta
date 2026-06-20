from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Callable

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    CreateFileRequest,
    CreateFileRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

logger = logging.getLogger(__name__)

RETRYABLE_CODES = {99991400, 99991401, 99991402, 99991663}


class ArtifactDeliveryError(RuntimeError):
    pass


class FileTooLargeError(ArtifactDeliveryError):
    pass


def split_message(text: str, max_length: int) -> list[str]:
    if len(text) <= max_length:
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining)
            break
        boundary = remaining.rfind("\n", 0, max_length + 1)
        if boundary <= 0:
            boundary = max_length
        chunks.append(remaining[:boundary])
        remaining = remaining[boundary:].lstrip("\n")
    return chunks


class FeishuMessenger:
    def __init__(
        self,
        client,
        max_length: int = 4000,
        max_file_bytes: int = 30 * 1024 * 1024,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.client = client
        self.max_length = max_length
        self.max_file_bytes = max_file_bytes
        self.sleep = sleep

    def reply_text(self, message_id: str, text: str) -> None:
        for chunk in split_message(text, self.max_length):
            request = (
                ReplyMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    ReplyMessageRequestBody.builder()
                    .msg_type("text")
                    .content(json.dumps({"text": chunk}, ensure_ascii=False))
                    .build()
                )
                .build()
            )
            self._execute(lambda: self.client.im.v1.message.reply(request))

    def send_text_to_chat(self, chat_id: str, text: str) -> None:
        for chunk in split_message(text, self.max_length):
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("text")
                    .content(json.dumps({"text": chunk}, ensure_ascii=False))
                    .build()
                )
                .build()
            )
            self._execute(lambda: self.client.im.v1.message.create(request))

    def send_file_to_chat(self, chat_id: str, path: str | Path) -> None:
        file_path = Path(path)
        try:
            byte_size = file_path.stat().st_size
        except OSError as exc:
            raise ArtifactDeliveryError(
                f"无法读取待发送文件 {file_path.name}：{exc}"
            ) from exc
        if byte_size > self.max_file_bytes:
            raise FileTooLargeError(
                f"文件 {file_path.name} 大小为 {byte_size} 字节，超过飞书发送限制 "
                f"{self.max_file_bytes} 字节"
            )
        try:
            with file_path.open("rb") as file:
                upload_request = (
                    CreateFileRequest.builder()
                    .request_body(
                        CreateFileRequestBody.builder()
                        .file_type("stream")
                        .file_name(file_path.name)
                        .file(file)
                        .build()
                    )
                    .build()
                )
                response = self._execute(
                    lambda: self.client.im.v1.file.create(upload_request)
                )
            file_key = response.data.file_key
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("file")
                    .content(json.dumps({"file_key": file_key}))
                    .build()
                )
                .build()
            )
            self._execute(lambda: self.client.im.v1.message.create(request))
        except ArtifactDeliveryError:
            raise
        except Exception as exc:
            raise ArtifactDeliveryError(
                f"飞书文件 {file_path.name} 上传或发送失败：{exc}"
            ) from exc

    def _execute(self, operation: Callable):
        for attempt in range(3):
            try:
                response = operation()
            except Exception:
                if attempt == 2:
                    raise
                logger.warning("飞书消息请求发生网络异常，准备重试", exc_info=True)
                self.sleep(2**attempt)
                continue
            if response.success():
                logger.debug("飞书消息 API 请求成功")
                return response
            code = getattr(response, "code", None)
            request_id = response.get_log_id() if hasattr(response, "get_log_id") else ""
            logger.error(
                "飞书消息发送失败 code=%s request_id=%s msg=%s",
                code,
                request_id,
                getattr(response, "msg", ""),
            )
            if code not in RETRYABLE_CODES or attempt == 2:
                raise RuntimeError(f"飞书消息发送失败，错误码 {code}")
            self.sleep(2**attempt)
        raise RuntimeError("飞书消息发送失败")


def build_api_client(app_id: str, app_secret: str):
    return (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.ERROR)
        .build()
    )
