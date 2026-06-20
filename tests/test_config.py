import pytest

from feishu_agent_bot.config import ConfigurationError, Settings


def test_missing_configuration(monkeypatch, tmp_path):
    monkeypatch.delenv("FEISHU_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_APP_SECRET", raising=False)
    with pytest.raises(ConfigurationError):
        Settings.from_env(tmp_path / "missing.env")


def test_placeholder_configuration_is_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("FEISHU_APP_ID", "cli_xxxxxxxxxxxxx")
    monkeypatch.setenv("FEISHU_APP_SECRET", "replace_with_rotated_secret")
    with pytest.raises(ConfigurationError):
        Settings.from_env(tmp_path / "missing.env")


def test_missing_llm_configuration_does_not_break_settings(
    monkeypatch, tmp_path
):
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.llm_api_key == ""
    assert settings.llm_model == ""
    assert settings.llm_max_tokens is None
    assert settings.max_fetched_pages == 0


def test_max_fetched_pages_zero_means_unlimited(monkeypatch, tmp_path):
    monkeypatch.setenv("MAX_FETCHED_PAGES", "0")
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.max_fetched_pages == 0

    monkeypatch.setenv("MAX_FETCHED_PAGES", "-1")
    with pytest.raises(ConfigurationError):
        Settings.from_env(tmp_path / "missing.env", require_credentials=False)


def test_llm_max_tokens_is_optional_but_can_be_set(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_MAX_TOKENS", "8192")
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.llm_max_tokens == 8192


def test_feishu_file_size_limit_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("FEISHU_FILE_MAX_BYTES", "123456")

    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )

    assert settings.feishu_file_max_bytes == 123456


def test_alpha_concurrency_and_report_limits_configuration(monkeypatch, tmp_path):
    values = {
        "MAX_CONCURRENT_DOWNLOADS": "5",
        "MAX_CONCURRENT_PARSERS": "4",
        "MAX_CONCURRENT_OCR_JOBS": "2",
        "MAX_CONCURRENT_ANALYSIS_TOOLS": "3",
        "MAX_CONCURRENT_CHARTS": "6",
        "MAX_CHARTS_PER_REPORT": "11",
        "MAX_TABLES_PER_REPORT": "22",
        "ARCHIVE_MAX_ENTRIES": "9000",
        "ARCHIVE_MAX_UNCOMPRESSED_BYTES": "400000000",
        "ARCHIVE_MAX_COMPRESSION_RATIO": "250.5",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )

    assert settings.max_concurrent_downloads == 5
    assert settings.max_concurrent_parsers == 4
    assert settings.max_concurrent_ocr_jobs == 2
    assert settings.max_concurrent_analysis_tools == 3
    assert settings.max_concurrent_charts == 6
    assert settings.max_charts_per_report == 11
    assert settings.max_tables_per_report == 22
    assert settings.archive_max_entries == 9000
    assert settings.archive_max_uncompressed_bytes == 400000000
    assert settings.archive_max_compression_ratio == 250.5


def test_socks_proxy_environment_is_normalized(monkeypatch, tmp_path):
    monkeypatch.setenv("ALL_PROXY", "socks://127.0.0.1:7897/")
    Settings.from_env(tmp_path / "missing.env", require_credentials=False)
    assert __import__("os").environ["ALL_PROXY"] == "socks5://127.0.0.1:7897/"


def test_serper_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("SEARCH_PROVIDER", "serper")
    monkeypatch.setenv("SERPER_API_KEY", "search-key")
    monkeypatch.setenv("SERPER_COUNTRY", "us")
    monkeypatch.setenv("SERPER_LOCALE", "en")
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.search_provider == "serper"
    assert settings.serper_api_key == "search-key"
    assert settings.serper_country == "us"
    assert settings.serper_locale == "en"


def test_schedule_catchup_window_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("TEMPORAL_SCHEDULE_CATCHUP_WINDOW_SECONDS", "1800")
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.temporal_schedule_catchup_window_seconds == 1800


def test_monitor_budget_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("MONITOR_DEFAULT_MODE", "observe")
    monkeypatch.setenv("MONITOR_DEFAULT_NOTIFY_LEVEL", "high")
    monkeypatch.setenv("MONITOR_MAX_SEARCH_QUERIES", "2")
    monkeypatch.setenv("MONITOR_MAX_RESULTS_PER_QUERY", "3")
    monkeypatch.setenv("MONITOR_MAX_FETCHED_PAGES", "4")
    monkeypatch.setenv("MONITOR_MAX_WATCH_TARGETS", "5")
    monkeypatch.setenv("MONITOR_MAX_LLM_CALLS", "6")
    monkeypatch.setenv("MONITOR_MAX_NEW_EVENTS", "7")
    monkeypatch.setenv("MONITOR_MAX_AUTO_PATCH_SECTIONS", "2")
    monkeypatch.setenv("MONITOR_MAX_CONSECUTIVE_FAILURES", "8")
    monkeypatch.setenv("MONITOR_LOOKBACK_DAYS", "9")

    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )

    assert settings.monitor_default_mode == "observe"
    assert settings.monitor_default_notify_level == "high"
    assert settings.monitor_max_search_queries == 2
    assert settings.monitor_max_results_per_query == 3
    assert settings.monitor_max_fetched_pages == 4
    assert settings.monitor_max_watch_targets == 5
    assert settings.monitor_max_llm_calls == 6
    assert settings.monitor_max_new_events == 7
    assert settings.monitor_max_auto_patch_sections == 2
    assert settings.monitor_max_consecutive_failures == 8
    assert settings.monitor_lookback_days == 9


def test_monitor_max_fetched_pages_zero_means_unlimited(monkeypatch, tmp_path):
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.monitor_max_fetched_pages == 0

    monkeypatch.setenv("MONITOR_MAX_FETCHED_PAGES", "0")
    settings = Settings.from_env(
        tmp_path / "missing.env", require_credentials=False
    )
    assert settings.monitor_max_fetched_pages == 0

    monkeypatch.setenv("MONITOR_MAX_FETCHED_PAGES", "-1")
    with pytest.raises(ConfigurationError):
        Settings.from_env(tmp_path / "missing.env", require_credentials=False)
