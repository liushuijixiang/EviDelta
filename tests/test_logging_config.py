import logging

from feishu_agent_bot.logging_config import ContextFormatter, SensitiveDataFilter


def test_sensitive_connection_values_are_redacted():
    record = logging.LogRecord(
        "Lark",
        logging.INFO,
        "",
        0,
        (
            "connected wss://example/ws?access_key=secret"
            "&ticket=private&token=value"
        ),
        (),
        None,
    )

    assert SensitiveDataFilter().filter(record)
    message = record.getMessage()
    assert "secret" not in message
    assert "private" not in message
    assert "value" not in message
    assert message.count("[REDACTED]") == 3


def test_context_formatter_appends_structured_fields():
    record = logging.LogRecord(
        "feishu_agent_bot.temporal.activities",
        logging.INFO,
        "",
        0,
        "Temporal Activity completed",
        (),
        None,
    )
    record.job_id = "job-1"
    record.workflow_id = "research-job-1"
    record.activity = "search_sources_activity"
    record.attempt = 2
    record.stage = "searching"
    record.duration_ms = 12
    record.result = "success"

    message = ContextFormatter("%(levelname)s %(message)s").format(record)

    assert message.startswith("INFO Temporal Activity completed")
    assert "job_id=job-1" in message
    assert "workflow_id=research-job-1" in message
    assert "activity=search_sources_activity" in message
    assert "attempt=2" in message
    assert "stage=searching" in message
    assert "duration_ms=12" in message
    assert "result=success" in message
