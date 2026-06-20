from feishu_agent_bot.command_router import parse_command


def test_command_parsing():
    command = parse_command("/research  新能源汽车 ")
    assert command.name == "/research"
    assert command.argument == "新能源汽车"


def test_plain_text():
    assert parse_command("hello").name == "text"


def test_command_parsing_accepts_newline_and_fullwidth_slash():
    command = parse_command("／research\n新能源汽车")
    assert command.name == "/research"
    assert command.argument == "新能源汽车"


def test_command_parsing_removes_bot_suffix():
    command = parse_command("/status@research_bot job-id")
    assert command.name == "/status"
    assert command.argument == "job-id"


def test_unknown_plain_text_stays_plain_text():
    command = parse_command("请帮我调研新能源汽车")
    assert command.name == "text"
