from __future__ import annotations

from types import SimpleNamespace

from feishu_agent_bot.config import Settings
from feishu_agent_bot.main import build_research_backend


def test_backend_factory_can_disable_llm_provider_retries(
    repository, monkeypatch
):
    for name in (
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(
        app_id="cli_real",
        app_secret="secret",
        database_path=repository.database_path,
        llm_api_key="key",
        llm_model="model",
        search_provider="ddgs",
        llm_max_retries=2,
        pdf_ocr_enabled=False,
        pdf_ocr_languages="eng",
        pdf_min_text_chars_per_page=77,
        pdf_max_pages=12,
        pdf_parse_timeout_seconds=34,
        archive_max_entries=123,
        archive_max_uncompressed_bytes=456_000_000,
        archive_max_compression_ratio=78.5,
    )

    backend = build_research_backend(
        settings, repository, llm_max_retries=0
    )

    assert backend.planner.llm.max_retries == 0
    pdf_parser = backend.parser_registry.parser_for_asset(
        SimpleNamespace(
            file_type="pdf", detected_mime_type="application/pdf"
        )
    )
    assert pdf_parser is not None
    assert pdf_parser.ocr_enabled is False
    assert pdf_parser.ocr_languages == "eng"
    assert pdf_parser.min_text_chars_per_page == 77
    assert pdf_parser.max_pages == 12
    assert pdf_parser.timeout_seconds == 34
    assert backend.asset_downloader.archive_max_entries == 123
    assert backend.asset_downloader.archive_max_uncompressed_bytes == 456_000_000
    assert backend.asset_downloader.archive_max_compression_ratio == 78.5
