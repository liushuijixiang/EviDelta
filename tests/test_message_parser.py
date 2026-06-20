from types import SimpleNamespace

import pytest

from feishu_agent_bot.message_parser import (
    MessageParseError,
    clean_group_mentions,
    parse_text_content,
)


def test_parse_text_content():
    assert parse_text_content('{"text":"hello"}') == "hello"


def test_parse_text_content_invalid_json():
    with pytest.raises(MessageParseError):
        parse_text_content("{broken")


def test_clean_group_mentions():
    mention = SimpleNamespace(key="@_user_1", name="Bot")
    assert clean_group_mentions("@_user_1  调研 @Bot 市场", [mention]) == "调研 市场"
