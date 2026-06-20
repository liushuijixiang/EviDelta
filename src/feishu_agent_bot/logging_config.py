import logging
import re

CONTEXT_FIELDS = (
    "job_id",
    "workflow_id",
    "run_id",
    "activity",
    "attempt",
    "stage",
    "duration_ms",
    "result",
    "error_type",
    "requester_id",
    "action",
)


class SensitiveDataFilter(logging.Filter):
    _query_secret = re.compile(
        r"(?i)(access_key|ticket|token|app_secret)=([^&\s]+)"
    )
    _json_secret = re.compile(
        r'(?i)("?(?:access_token|tenant_access_token|app_secret)"?\s*:\s*")([^"]+)'
    )

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
            message = self._query_secret.sub(r"\1=[REDACTED]", message)
            message = self._json_secret.sub(r'\1[REDACTED]', message)
            record.msg = message
            record.args = ()
        except Exception:
            # During interpreter shutdown third-party SDK transports may still
            # emit logs after module globals are partially torn down.
            pass
        return True


class ContextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        fields = []
        for name in CONTEXT_FIELDS:
            value = getattr(record, name, None)
            if value is not None:
                fields.append(f"{name}={value}")
        if fields:
            message = f"{message} {' '.join(fields)}"
        return message


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sensitive_filter = SensitiveDataFilter()
    formatter = ContextFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(sensitive_filter)
        handler.setFormatter(formatter)

    sdk_logger = logging.getLogger("Lark")
    for handler in sdk_logger.handlers:
        handler.addFilter(sensitive_filter)
        handler.setFormatter(formatter)
