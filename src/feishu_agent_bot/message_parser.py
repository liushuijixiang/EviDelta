from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable


class MessageParseError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedMessage:
    message_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    sender_type: str
    message_type: str
    text: str


def parse_text_content(content: str) -> str:
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, TypeError) as exc:
        raise MessageParseError("文本消息 content 不是合法 JSON") from exc
    text = payload.get("text")
    if not isinstance(text, str):
        raise MessageParseError("文本消息 content 缺少 text 字段")
    return text


def clean_group_mentions(text: str, mentions: Iterable[Any] | None) -> str:
    cleaned = text
    for mention in mentions or []:
        key = getattr(mention, "key", None)
        name = getattr(mention, "name", None)
        if key:
            cleaned = cleaned.replace(key, " ")
        if name:
            cleaned = cleaned.replace(f"@{name}", " ")
    cleaned = re.sub(r"@_user_\d+", " ", cleaned)
    return " ".join(cleaned.split())


def parse_event(event: Any) -> ParsedMessage:
    data = getattr(event, "event", None)
    message = getattr(data, "message", None)
    sender = getattr(data, "sender", None)
    sender_id_obj = getattr(sender, "sender_id", None)
    sender_id = (
        getattr(sender_id_obj, "open_id", None)
        or getattr(sender_id_obj, "user_id", None)
        or getattr(sender_id_obj, "union_id", None)
    )
    required = {
        "message_id": getattr(message, "message_id", None),
        "chat_id": getattr(message, "chat_id", None),
        "chat_type": getattr(message, "chat_type", None),
        "sender_id": sender_id,
        "message_type": getattr(message, "message_type", None),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise MessageParseError("事件缺少字段: " + ", ".join(missing))
    message_type = required["message_type"]
    text = ""
    if message_type == "text":
        text = parse_text_content(getattr(message, "content", ""))
        if required["chat_type"] == "group":
            text = clean_group_mentions(text, getattr(message, "mentions", None))
    return ParsedMessage(
        message_id=str(required["message_id"]),
        chat_id=str(required["chat_id"]),
        chat_type=str(required["chat_type"]),
        sender_id=str(required["sender_id"]),
        sender_type=str(getattr(sender, "sender_type", "")),
        message_type=str(message_type),
        text=text.strip(),
    )
