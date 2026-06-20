from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    pass


def _normalize_proxy_environment() -> None:
    for name in (
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ):
        value = os.getenv(name)
        if value and value.startswith("socks://"):
            os.environ[name] = "socks5://" + value[len("socks://") :]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip("'\"")


def _positive_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} 必须是整数") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{name} 必须大于 0")
    return parsed


def _nonnegative_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} 必须是整数") from exc
    if parsed < 0:
        raise ConfigurationError(f"{name} 必须大于或等于 0")
    return parsed


def _optional_positive_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} 必须是整数") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{name} 必须大于 0")
    return parsed


def _nonnegative_float(name: str, default: float) -> float:
    value = os.getenv(name, str(default))
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} 必须是数字") from exc
    if parsed < 0:
        raise ConfigurationError(f"{name} 不能小于 0")
    return parsed


def _positive_float(name: str, default: float) -> float:
    parsed = _nonnegative_float(name, default)
    if parsed <= 0:
        raise ConfigurationError(f"{name} 必须大于 0")
    return parsed


@dataclass(frozen=True)
class Settings:
    app_id: str
    app_secret: str
    database_path: Path
    worker_count: int = 2
    queue_size: int = 100
    log_level: str = "INFO"
    mock_stage_delay_seconds: float = 1.0
    message_max_length: int = 4000
    feishu_file_max_bytes: int = 30 * 1024 * 1024
    execution_backend: str = "temporal"
    llm_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = ""
    llm_model: str = ""
    llm_timeout_seconds: float = 90
    llm_max_retries: int = 2
    llm_max_tokens: int | None = None
    search_provider: str = "ddgs"
    serper_api_key: str = ""
    serper_url: str = "https://google.serper.dev/search"
    serper_country: str = "cn"
    serper_locale: str = "zh-cn"
    max_search_queries: int = 6
    max_results_per_query: int = 5
    max_fetched_pages: int = 0
    max_page_bytes: int = 3_000_000
    max_concurrent_downloads: int = 4
    max_concurrent_parsers: int = 3
    max_concurrent_ocr_jobs: int = 1
    max_concurrent_profile_jobs: int = 4
    max_concurrent_analysis_tools: int = 4
    max_concurrent_charts: int = 2
    max_charts_per_report: int = 12
    max_tables_per_report: int = 30
    fetch_timeout_seconds: float = 20
    max_pdf_bytes: int = 50_000_000
    max_csv_bytes: int = 50_000_000
    max_excel_bytes: int = 50_000_000
    max_json_bytes: int = 20_000_000
    max_docx_bytes: int = 30_000_000
    asset_download_timeout_seconds: float = 60
    asset_max_redirects: int = 5
    archive_max_entries: int = 10_000
    archive_max_uncompressed_bytes: int = 500_000_000
    archive_max_compression_ratio: float = 500.0
    pdf_ocr_enabled: bool = True
    pdf_ocr_languages: str = "chi_sim+eng"
    pdf_min_text_chars_per_page: int = 50
    pdf_max_pages: int = 500
    pdf_parse_timeout_seconds: int = 300
    csv_max_rows: int = 1_000_000
    csv_preview_rows: int = 100
    csv_chunk_size: int = 50_000
    pdf_enabled: bool = True
    latex_engine: str = "xelatex"
    latexmk_path: str = "latexmk"
    pdf_compile_timeout_seconds: int = 180
    pdf_max_output_bytes: int = 100_000_000
    pdf_template: str = "business_report"
    max_report_claims: int = 20
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "research-agent"
    temporal_connect_timeout_seconds: float = 10
    temporal_research_workflow_prefix: str = "research"
    temporal_activity_max_attempts: int = 3
    temporal_heartbeat_timeout_seconds: float = 60
    temporal_schedule_catchup_window_seconds: float = 3600
    monitor_default_mode: str = "safe"
    monitor_default_notify_level: str = "medium"
    monitor_default_timezone: str = "Asia/Shanghai"
    monitor_default_daily_time: str = "09:00"
    monitor_default_catchup_window_hours: int = 6
    monitor_min_interval_minutes: int = 30
    monitor_max_search_queries: int = 4
    monitor_max_results_per_query: int = 5
    monitor_max_fetched_pages: int = 0
    monitor_max_watch_targets: int = 10
    monitor_max_llm_calls: int = 25
    monitor_max_new_events: int = 20
    monitor_max_auto_patch_sections: int = 3
    monitor_max_consecutive_failures: int = 3
    monitor_lookback_days: int = 7
    monitor_patch_expiry_days: int = 30
    professional_artifact_concurrency: int = 2
    professional_pdf_timeout_seconds: int = 120
    professional_default_deliverables: str = "pdf,xlsx"

    @classmethod
    def from_env(
        cls, env_file: str | Path = ".env", require_credentials: bool = True
    ) -> "Settings":
        _load_env_file(Path(env_file))
        _normalize_proxy_environment()
        app_id = os.getenv("FEISHU_APP_ID", "").strip()
        app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
        placeholders = {
            "",
            "cli_xxxxxxxxxxxxx",
            "replace_with_rotated_secret",
        }
        if require_credentials and (
            app_id in placeholders or app_secret in placeholders
        ):
            raise ConfigurationError(
                "缺少有效的 FEISHU_APP_ID 或 FEISHU_APP_SECRET，请检查 .env"
            )
        return cls(
            app_id=app_id,
            app_secret=app_secret,
            database_path=Path(
                os.getenv("DATABASE_PATH", "data/feishu_agent_bot.db")
            ).expanduser(),
            worker_count=_positive_int("WORKER_COUNT", 2),
            queue_size=_positive_int("QUEUE_SIZE", 100),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            mock_stage_delay_seconds=_nonnegative_float(
                "MOCK_STAGE_DELAY_SECONDS", 1.0
            ),
            message_max_length=_positive_int("MESSAGE_MAX_LENGTH", 4000),
            feishu_file_max_bytes=_positive_int(
                "FEISHU_FILE_MAX_BYTES", 30 * 1024 * 1024
            ),
            execution_backend=os.getenv(
                "EXECUTION_BACKEND", "temporal"
            ).strip().lower(),
            llm_base_url=os.getenv(
                "LLM_BASE_URL", "https://api.openai.com/v1"
            ).strip(),
            llm_api_key=os.getenv("LLM_API_KEY", "").strip(),
            llm_model=os.getenv("LLM_MODEL", "").strip(),
            llm_timeout_seconds=_nonnegative_float("LLM_TIMEOUT_SECONDS", 90),
            llm_max_retries=_positive_int("LLM_MAX_RETRIES", 2),
            llm_max_tokens=_optional_positive_int("LLM_MAX_TOKENS"),
            search_provider=os.getenv("SEARCH_PROVIDER", "ddgs").strip().lower(),
            serper_api_key=os.getenv("SERPER_API_KEY", "").strip(),
            serper_url=os.getenv(
                "SERPER_URL", "https://google.serper.dev/search"
            ).strip(),
            serper_country=os.getenv("SERPER_COUNTRY", "cn").strip(),
            serper_locale=os.getenv("SERPER_LOCALE", "zh-cn").strip(),
            max_search_queries=_positive_int("MAX_SEARCH_QUERIES", 6),
            max_results_per_query=_positive_int("MAX_RESULTS_PER_QUERY", 5),
            max_fetched_pages=_nonnegative_int("MAX_FETCHED_PAGES", 0),
            max_page_bytes=_positive_int("MAX_PAGE_BYTES", 3_000_000),
            max_concurrent_downloads=_positive_int(
                "MAX_CONCURRENT_DOWNLOADS", 4
            ),
            max_concurrent_parsers=_positive_int("MAX_CONCURRENT_PARSERS", 3),
            max_concurrent_ocr_jobs=_positive_int(
                "MAX_CONCURRENT_OCR_JOBS", 1
            ),
            max_concurrent_profile_jobs=_positive_int(
                "MAX_CONCURRENT_PROFILE_JOBS", 4
            ),
            max_concurrent_analysis_tools=_positive_int(
                "MAX_CONCURRENT_ANALYSIS_TOOLS", 4
            ),
            max_concurrent_charts=_positive_int("MAX_CONCURRENT_CHARTS", 2),
            max_charts_per_report=_positive_int("MAX_CHARTS_PER_REPORT", 12),
            max_tables_per_report=_positive_int("MAX_TABLES_PER_REPORT", 30),
            fetch_timeout_seconds=_nonnegative_float("FETCH_TIMEOUT_SECONDS", 20),
            max_pdf_bytes=_positive_int("MAX_PDF_BYTES", 50_000_000),
            max_csv_bytes=_positive_int("MAX_CSV_BYTES", 50_000_000),
            max_excel_bytes=_positive_int("MAX_EXCEL_BYTES", 50_000_000),
            max_json_bytes=_positive_int("MAX_JSON_BYTES", 20_000_000),
            max_docx_bytes=_positive_int("MAX_DOCX_BYTES", 30_000_000),
            asset_download_timeout_seconds=_nonnegative_float(
                "ASSET_DOWNLOAD_TIMEOUT_SECONDS", 60
            ),
            asset_max_redirects=_positive_int("ASSET_MAX_REDIRECTS", 5),
            archive_max_entries=_positive_int("ARCHIVE_MAX_ENTRIES", 10_000),
            archive_max_uncompressed_bytes=_positive_int(
                "ARCHIVE_MAX_UNCOMPRESSED_BYTES", 500_000_000
            ),
            archive_max_compression_ratio=_positive_float(
                "ARCHIVE_MAX_COMPRESSION_RATIO", 500.0
            ),
            pdf_ocr_enabled=os.getenv("PDF_OCR_ENABLED", "true").strip().lower()
            not in {"0", "false", "no"},
            pdf_ocr_languages=os.getenv(
                "PDF_OCR_LANGUAGES", "chi_sim+eng"
            ).strip(),
            pdf_min_text_chars_per_page=_positive_int(
                "PDF_MIN_TEXT_CHARS_PER_PAGE", 50
            ),
            pdf_max_pages=_positive_int("PDF_MAX_PAGES", 500),
            pdf_parse_timeout_seconds=_positive_int(
                "PDF_PARSE_TIMEOUT_SECONDS", 300
            ),
            csv_max_rows=_positive_int("CSV_MAX_ROWS", 1_000_000),
            csv_preview_rows=_positive_int("CSV_PREVIEW_ROWS", 100),
            csv_chunk_size=_positive_int("CSV_CHUNK_SIZE", 50_000),
            pdf_enabled=os.getenv("PDF_ENABLED", "true").strip().lower()
            not in {"0", "false", "no"},
            latex_engine=os.getenv("LATEX_ENGINE", "xelatex").strip(),
            latexmk_path=os.getenv("LATEXMK_PATH", "latexmk").strip(),
            pdf_compile_timeout_seconds=_positive_int(
                "PDF_COMPILE_TIMEOUT_SECONDS", 180
            ),
            pdf_max_output_bytes=_positive_int(
                "PDF_MAX_OUTPUT_BYTES", 100_000_000
            ),
            pdf_template=os.getenv(
                "PDF_TEMPLATE", "business_report"
            ).strip(),
            max_report_claims=_positive_int("MAX_REPORT_CLAIMS", 20),
            temporal_address=os.getenv(
                "TEMPORAL_ADDRESS", "localhost:7233"
            ).strip(),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default").strip(),
            temporal_task_queue=os.getenv(
                "TEMPORAL_TASK_QUEUE", "research-agent"
            ).strip(),
            temporal_connect_timeout_seconds=_nonnegative_float(
                "TEMPORAL_CONNECT_TIMEOUT_SECONDS", 10
            ),
            temporal_research_workflow_prefix=os.getenv(
                "TEMPORAL_RESEARCH_WORKFLOW_PREFIX", "research"
            ).strip(),
            temporal_activity_max_attempts=_positive_int(
                "TEMPORAL_ACTIVITY_MAX_ATTEMPTS", 3
            ),
            temporal_heartbeat_timeout_seconds=_nonnegative_float(
                "TEMPORAL_HEARTBEAT_TIMEOUT_SECONDS", 60
            ),
            temporal_schedule_catchup_window_seconds=_nonnegative_float(
                "TEMPORAL_SCHEDULE_CATCHUP_WINDOW_SECONDS", 3600
            ),
            monitor_default_mode=os.getenv("MONITOR_DEFAULT_MODE", "safe").strip(),
            monitor_default_notify_level=os.getenv(
                "MONITOR_DEFAULT_NOTIFY_LEVEL", "medium"
            ).strip(),
            monitor_default_timezone=os.getenv(
                "MONITOR_DEFAULT_TIMEZONE", "Asia/Shanghai"
            ).strip(),
            monitor_default_daily_time=os.getenv(
                "MONITOR_DEFAULT_DAILY_TIME", "09:00"
            ).strip(),
            monitor_default_catchup_window_hours=_positive_int(
                "MONITOR_DEFAULT_CATCHUP_WINDOW_HOURS", 6
            ),
            monitor_min_interval_minutes=_positive_int(
                "MONITOR_MIN_INTERVAL_MINUTES", 30
            ),
            monitor_max_search_queries=_positive_int(
                "MONITOR_MAX_SEARCH_QUERIES", 4
            ),
            monitor_max_results_per_query=_positive_int(
                "MONITOR_MAX_RESULTS_PER_QUERY", 5
            ),
            monitor_max_fetched_pages=_nonnegative_int(
                "MONITOR_MAX_FETCHED_PAGES", 0
            ),
            monitor_max_watch_targets=_positive_int(
                "MONITOR_MAX_WATCH_TARGETS", 10
            ),
            monitor_max_llm_calls=_positive_int("MONITOR_MAX_LLM_CALLS", 25),
            monitor_max_new_events=_positive_int("MONITOR_MAX_NEW_EVENTS", 20),
            monitor_max_auto_patch_sections=_positive_int(
                "MONITOR_MAX_AUTO_PATCH_SECTIONS", 3
            ),
            monitor_max_consecutive_failures=_positive_int(
                "MONITOR_MAX_CONSECUTIVE_FAILURES", 3
            ),
            monitor_lookback_days=_positive_int("MONITOR_LOOKBACK_DAYS", 7),
            monitor_patch_expiry_days=_positive_int(
                "MONITOR_PATCH_EXPIRY_DAYS", 30
            ),
            professional_artifact_concurrency=_positive_int(
                "PROFESSIONAL_ARTIFACT_CONCURRENCY", 2
            ),
            professional_pdf_timeout_seconds=_positive_int(
                "PROFESSIONAL_PDF_TIMEOUT_SECONDS", 120
            ),
            professional_default_deliverables=os.getenv(
                "PROFESSIONAL_DEFAULT_DELIVERABLES", "pdf,xlsx"
            ).strip(),
        )
