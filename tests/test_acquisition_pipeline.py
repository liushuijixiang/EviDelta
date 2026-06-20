from __future__ import annotations

from io import BytesIO
import zipfile

import httpx
import pytest
from openpyxl import Workbook

from feishu_agent_bot.acquisition import (
    AssetDownloader,
    AssetStore,
    SourceAsset,
    UnsafeArchiveError,
    validate_office_archive,
)
from feishu_agent_bot.acquisition.downloader import (
    AssetTooLargeError,
    UnsupportedAssetTypeError,
)
from feishu_agent_bot.agent.planner import ResearchPlanner
from feishu_agent_bot.llm.mock import MockLLM
from feishu_agent_bot.llm.schemas import ResearchPlan
from feishu_agent_bot.parsers import ParserRegistry
from feishu_agent_bot.parsers.base import ParsedAsset, ParsedTable, TextBlock
from feishu_agent_bot.datasets import DatasetProfiler, TabularDataset


def test_planner_adds_file_discovery_queries_within_budget():
    llm = MockLLM(
        {
            ResearchPlan: ResearchPlan(
                objective="目标",
                research_questions=["问题"],
                search_queries=["新能源汽车 竞品"],
                comparison_dimensions=["价格"],
                expected_entities=["A"],
                acceptance_criteria=["有证据"],
            )
        }
    )
    plan = ResearchPlanner(llm, max_search_queries=4).create_plan(
        "新能源汽车充电设备行业主要竞品"
    )

    assert len(plan.search_queries) == 4
    assert any("filetype:pdf" in query for query in plan.search_queries)
    assert any("filetype:xlsx" in query for query in plan.search_queries)


def test_asset_downloader_streams_detects_and_stores_asset(tmp_path):
    def handler(request):
        if str(request.url).endswith("/start"):
            return httpx.Response(
                302,
                request=request,
                headers={"location": "https://example.com/prices.csv"},
            )
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/csv"},
            content=b"company,price\nA,99\n",
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    downloaded = downloader.download(job_id="J1", url="https://example.com/start")

    assert downloaded.http_status == 200
    assert downloaded.asset.file_type == "csv"
    assert downloaded.asset.generated_filename.endswith(".csv")
    assert downloaded.asset.raw_object_path.is_file()
    assert downloaded.asset.original_filename == "prices.csv"


def test_asset_downloader_revalidates_redirect_targets(tmp_path):
    def handler(request):
        return httpx.Response(
            302,
            request=request,
            headers={"location": "http://127.0.0.1/private.csv"},
        )

    checked_urls = []

    def validator(url):
        checked_urls.append(url)
        if "127.0.0.1" in url:
            raise ValueError("private redirect")
        return url

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=validator,
    )

    with pytest.raises(ValueError, match="private redirect"):
        downloader.download(job_id="J1", url="https://example.com/start")

    assert checked_urls == [
        "https://example.com/start",
        "http://127.0.0.1/private.csv",
    ]
    assert not list((tmp_path / "J1" / "assets").glob(".download-*.tmp"))


def test_asset_downloader_size_limit_cleans_temporary_file(tmp_path):
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/csv"},
            content=b"company,price\nA,99\n",
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        max_bytes_by_type={"csv": 8},
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    with pytest.raises(AssetTooLargeError):
        downloader.download(job_id="J1", url="https://example.com/prices.csv")

    asset_dir = tmp_path / "J1" / "assets"
    assert asset_dir.is_dir()
    assert list(asset_dir.iterdir()) == []


def test_asset_downloader_rejects_unknown_binary_and_leaves_no_file(tmp_path):
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "application/octet-stream"},
            content=b"\x00\x01\x02\x03binary",
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    with pytest.raises(UnsupportedAssetTypeError):
        downloader.download(job_id="J1", url="https://example.com/blob.bin")

    asset_dir = tmp_path / "J1" / "assets"
    assert asset_dir.is_dir()
    assert list(asset_dir.iterdir()) == []


def test_asset_downloader_deduplicates_same_hash_with_generated_names(tmp_path):
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/csv"},
            content=b"company,price\nA,99\n",
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    first = downloader.download(job_id="J1", url="https://example.com/a.csv")
    second = downloader.download(job_id="J1", url="https://example.com/b.csv")

    assert first.asset.asset_id == second.asset.asset_id
    assert first.asset.raw_object_path == second.asset.raw_object_path
    assert first.asset.generated_filename == second.asset.generated_filename
    assert sorted(path.name for path in (tmp_path / "J1" / "assets").iterdir()) == [
        first.asset.generated_filename
    ]
    assert first.asset.original_filename == "a.csv"
    assert second.asset.original_filename == "b.csv"


def test_asset_downloader_accepts_structurally_safe_xlsx(tmp_path):
    workbook = Workbook()
    workbook.active.append(["company", "price"])
    workbook.active.append(["A", 99])
    payload = BytesIO()
    workbook.save(payload)
    workbook.close()

    def handler(request):
        return httpx.Response(
            200,
            request=request,
            headers={
                "content-type": (
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                )
            },
            content=payload.getvalue(),
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    downloaded = downloader.download(
        job_id="J1", url="https://example.com/prices.xlsx"
    )

    assert downloaded.asset.file_type == "excel"
    assert downloaded.asset.raw_object_path.is_file()


def test_asset_downloader_rejects_high_ratio_office_archive_and_cleans_file(
    tmp_path,
):
    payload = BytesIO()
    with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("xl/worksheets/sheet1.xml", b"0" * 11_000_000)

    def handler(request):
        return httpx.Response(
            200,
            request=request,
            headers={
                "content-type": (
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                )
            },
            content=payload.getvalue(),
        )

    downloader = AssetDownloader(
        AssetStore(tmp_path),
        archive_max_uncompressed_bytes=20_000_000,
        archive_max_compression_ratio=10,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )

    with pytest.raises(UnsafeArchiveError, match="compression ratio"):
        downloader.download(
            job_id="J1", url="https://example.com/prices.xlsx"
        )

    assert list((tmp_path / "J1" / "assets").iterdir()) == []


def test_office_archive_rejects_path_traversal(tmp_path):
    path = tmp_path / "unsafe.docx"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("word/document.xml", "<document/>")
        archive.writestr("../outside.xml", "<outside/>")

    with pytest.raises(UnsafeArchiveError, match="unsafe path"):
        validate_office_archive(path, "docx")


def test_repository_persists_source_asset(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m1", "topic")
    path = tmp_path / "asset.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    asset = SourceAsset(
        asset_id="asset-1",
        job_id=job.job_id,
        source_id=None,
        original_url="https://example.com/a.csv",
        canonical_url="https://example.com/a.csv",
        generated_filename="asset-1.csv",
        original_filename="a.csv",
        declared_mime_type="text/csv",
        detected_mime_type="text/csv",
        file_extension=".csv",
        byte_size=8,
        sha256="abc",
        retrieved_at="2026-06-19T00:00:00+00:00",
        raw_object_path=path,
        source_type="csv",
    )

    repository.save_source_asset(asset)
    rows = repository.list_source_assets(job.job_id)

    assert rows[0]["asset_id"] == "asset-1"
    assert rows[0]["sha256"] == "abc"
    assert rows[0]["parse_status"] == "downloaded"


def test_parser_registry_supports_mock_parser_for_asset(tmp_path):
    class MockParser:
        name = "mock"
        version = "1"
        supported_file_types = {"text"}
        supported_mime_types = {"text/plain"}

        def can_parse(self, asset):
            return asset.detected_mime_type == "text/plain"

        def parse(self, path, *, asset_id):
            return ParsedAsset(
                asset_id=asset_id,
                file_type="text",
                text_blocks=[TextBlock(f"{asset_id}-B001", path.read_text())],
            )

    path = tmp_path / "asset.txt"
    path.write_text("hello", encoding="utf-8")
    asset = SourceAsset(
        asset_id="A1",
        job_id="J1",
        source_id=None,
        original_url="https://example.com/a.txt",
        canonical_url="https://example.com/a.txt",
        generated_filename="A1.txt",
        original_filename="a.txt",
        declared_mime_type="text/plain",
        detected_mime_type="text/plain",
        file_extension=".txt",
        byte_size=5,
        sha256="hash",
        retrieved_at="2026-06-19T00:00:00+00:00",
        raw_object_path=path,
    )

    parsed = ParserRegistry([MockParser()]).parse_asset(asset)

    assert parsed.text_blocks[0].text == "hello"


def test_default_parser_registry_selects_pdf_parser_for_asset(tmp_path):
    import fitz
    from feishu_agent_bot.parsers import default_parser_registry

    pdf_path = tmp_path / "asset.pdf"
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), "asset pdf text")
    document.save(pdf_path)
    document.close()
    asset = SourceAsset(
        asset_id="PDF-A1",
        job_id="J1",
        source_id=None,
        original_url="https://example.com/a.pdf",
        canonical_url="https://example.com/a.pdf",
        generated_filename="PDF-A1.pdf",
        original_filename="a.pdf",
        declared_mime_type="application/pdf",
        detected_mime_type="application/pdf",
        file_extension=".pdf",
        byte_size=pdf_path.stat().st_size,
        sha256="hash",
        retrieved_at="2026-06-19T00:00:00+00:00",
        raw_object_path=pdf_path,
    )

    parser = default_parser_registry().parser_for_asset(asset)
    parsed = default_parser_registry().parse_asset(asset)

    assert parser.name == "pdf"
    assert parsed.extraction_method == "pymupdf"
    assert "asset pdf text" in parsed.text_blocks[0].text


def test_default_parser_registry_applies_pdf_settings(tmp_path):
    import fitz
    from feishu_agent_bot.parsers import default_parser_registry

    pdf_path = tmp_path / "configured.pdf"
    document = fitz.open()
    document.new_page()
    document.save(pdf_path)
    document.close()
    asset = SourceAsset(
        asset_id="PDF-CONFIG",
        job_id="J1",
        source_id=None,
        original_url="https://example.com/configured.pdf",
        canonical_url="https://example.com/configured.pdf",
        generated_filename="PDF-CONFIG.pdf",
        original_filename="configured.pdf",
        declared_mime_type="application/pdf",
        detected_mime_type="application/pdf",
        file_extension=".pdf",
        byte_size=pdf_path.stat().st_size,
        sha256="hash",
        retrieved_at="2026-06-19T00:00:00+00:00",
        raw_object_path=pdf_path,
    )
    registry = default_parser_registry(
        pdf_ocr_enabled=False,
        pdf_ocr_languages="eng",
        pdf_min_text_chars_per_page=123,
        pdf_max_pages=7,
        pdf_parse_timeout_seconds=9,
    )

    parser = registry.parser_for_asset(asset)

    assert parser is not None
    assert parser.name == "pdf"
    assert parser.ocr_enabled is False
    assert parser.ocr_languages == "eng"
    assert parser.min_text_chars_per_page == 123
    assert parser.max_pages == 7
    assert parser.timeout_seconds == 9


def test_repository_persists_parsed_asset_and_dataset(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m2", "topic")
    path = tmp_path / "asset.csv"
    path.write_text("company,price\nA,99\n", encoding="utf-8")
    asset = SourceAsset(
        asset_id="asset-2",
        job_id=job.job_id,
        source_id=None,
        original_url="https://example.com/a.csv",
        canonical_url="https://example.com/a.csv",
        generated_filename="asset-2.csv",
        original_filename="a.csv",
        declared_mime_type="text/csv",
        detected_mime_type="text/csv",
        file_extension=".csv",
        byte_size=19,
        sha256="def",
        retrieved_at="2026-06-19T00:00:00+00:00",
        raw_object_path=path,
        source_type="csv",
    )
    repository.save_source_asset(asset)
    parsed = ParsedAsset(
        asset_id=asset.asset_id,
        file_type="csv",
        text_blocks=[
            TextBlock(
                block_id="B1",
                text="价格表说明",
                page_number=2,
                section="价格",
                bbox=(1.0, 2.0, 30.0, 40.0),
                source_locator="asset-2.csv#note-1",
            )
        ],
        tables=[
            ParsedTable(
                table_id="T1",
                columns=["company", "price"],
                rows=[{"company": "A", "price": "99"}],
                source_locator="asset-2.csv#rows",
                extraction_method="csv_chunked",
                metadata={"encoding": "utf-8"},
            )
        ],
    )

    repository.save_parsed_asset(job.job_id, parsed, parser_name="csv")
    block = repository.list_parsed_text_blocks(job.job_id)[0]
    assert block["section_title"] == "价格"
    assert block["bbox"] == (1.0, 2.0, 30.0, 40.0)
    assert block["source_locator"] == "asset-2.csv#note-1"
    table = repository.list_parsed_tables(job.job_id)[0]
    assert table["source_locator"] == "asset-2.csv#rows"
    assert table["extraction_method"] == "csv_chunked"
    assert table["metadata"] == {"encoding": "utf-8"}
    dataset = TabularDataset(
        dataset_id="D1",
        job_id=job.job_id,
        asset_id=asset.asset_id,
        table_id=table["table_id"],
        name="prices",
        columns=table["columns"],
        rows=table["rows"],
        lineage={"source_locator": "asset-2.csv#rows"},
    )
    profile = DatasetProfiler().profile(dataset)
    repository.save_tabular_dataset(dataset, profile)

    datasets = repository.list_tabular_datasets(job.job_id)
    assert datasets[0]["dataset_id"] == "D1"
    assert datasets[0]["row_count"] == 1
    assert datasets[0]["profile"]["numeric_columns"] == ["price"]
    assert datasets[0]["schema"][1]["name"] == "price"
    assert datasets[0]["schema"][1]["inferred_type"] == "integer"
