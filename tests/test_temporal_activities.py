from __future__ import annotations

import sqlite3
import json
import hashlib
from dataclasses import replace
import logging
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import httpx
import pytest
from temporalio.exceptions import CancelledError
from temporalio.testing import ActivityEnvironment

from feishu_agent_bot.agent.report_generator import ReportGenerator
from feishu_agent_bot.agent.report_validator import (
    ReportValidationError as AgentReportValidationError,
)
from feishu_agent_bot.acquisition import DownloadedAsset, SourceAsset
from feishu_agent_bot.artifacts import ArtifactStore
from feishu_agent_bot.datasets import DatasetProfiler, TabularDataset
from feishu_agent_bot.event_handler import EventHandler
from feishu_agent_bot.llm.schemas import (
    ClaimItem,
    EvidenceItem,
    FetchResult,
    ResearchPlan,
    SearchResult,
)
from feishu_agent_bot.llm.openai_compatible import LLMError
from feishu_agent_bot.research.parser import ContentExtractor
from feishu_agent_bot.parsers.base import ParsedAsset, ParsedTable, TextBlock
from feishu_agent_bot.reporting.models import BuiltArtifact
from feishu_agent_bot.reporting.artifacts import _ir_to_dict
from feishu_agent_bot.temporal.activities import ResearchActivities
import feishu_agent_bot.temporal.activities as activities_module
from feishu_agent_bot.temporal.exceptions import (
    AuthenticationError,
    AuthorizationError,
    InvalidModelOutputError,
    ProviderServerError,
    RateLimitError,
    ReportValidationError,
)


def create_job(repository):
    job = repository.create_job(
        "u1", "c1", "m1", "测试主题", execution_backend="temporal"
    )
    repository.start_job(job.job_id)
    return repository.get_job(job.job_id)


def plan():
    return ResearchPlan(
        objective="识别竞品",
        research_questions=["竞品是谁"],
        search_queries=["测试竞品"],
        comparison_dimensions=["产品"],
        expected_entities=["示例公司"],
        acceptance_criteria=["有引用"],
    )


class CountingSearchProvider:
    def __init__(self):
        self.calls = 0
        self.limits = []

    def search(self, query, limit):
        self.calls += 1
        self.limits.append(limit)
        return [
            SearchResult(
                title="官方资料",
                url="https://example.com/report",
                snippet="资料",
                query=query,
                rank=1,
            )
        ][:limit]


class MultiSearchProvider:
    def __init__(self, count=3):
        self.count = count
        self.calls = 0
        self.limits = []

    def search(self, query, limit):
        self.calls += 1
        self.limits.append(limit)
        return [
            SearchResult(
                title=f"资料 {index}",
                url=f"https://example.com/report-{index}",
                snippet="资料",
                query=query,
                rank=index,
            )
            for index in range(1, self.count + 1)
        ][:limit]


class FailingSearchProvider:
    def __init__(self, exc):
        self.exc = exc

    def search(self, query, limit):
        raise self.exc


class CountingFetcher:
    def __init__(self):
        self.calls = 0

    def fetch(self, url):
        self.calls += 1
        return FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            content=(
                b"<html><title>Official</title><main><p>"
                b"This page contains enough English context for parsing, "
                b"deduplication, and validation in the research workflow. "
                b"It describes product portfolio, deployment scenarios, "
                b"service capability, operations, customer groups, and "
                b"competitive positioning in sufficient detail."
                + b"</p><p>\xe8\xaf\xa5\xe4\xba\xa7\xe5\x93\x81\xe6\x94\xaf\xe6\x8c\x81\xe9\xab\x98\xe5\x8a\x9f\xe7\x8e\x87\xe5\xbf\xab\xe5\x85\x85\xe3\x80\x82</p></main></html>"
            ),
        )


class UniqueCountingFetcher(CountingFetcher):
    def fetch(self, url):
        fetched = super().fetch(url)
        unique_context = (
            f"<p>This fetched page is uniquely identified by {url}. "
            f"It contains source-specific context number {self.calls}.</p>"
        ).encode()
        return FetchResult(
            requested_url=fetched.requested_url,
            final_url=fetched.final_url,
            status_code=fetched.status_code,
            content_type=fetched.content_type,
            content=fetched.content.replace(b"</main>", unique_context + b"</main>"),
        )


class UnsupportedContentFetcher:
    def __init__(self):
        self.calls = 0

    def fetch(self, url):
        self.calls += 1
        raise ValueError("不支持的内容类型: application/pdf")


class FakeAssetDownloader:
    def __init__(self, path):
        self.path = path
        self.calls = []

    def download(
        self,
        *,
        job_id,
        url,
        source_id=None,
        snapshot_id=None,
        published_at=None,
    ):
        self.calls.append((job_id, url, source_id))
        asset = SourceAsset(
            asset_id="asset-pdf-1",
            job_id=job_id,
            source_id=source_id,
            snapshot_id=snapshot_id,
            original_url=url,
            canonical_url=url,
            generated_filename="asset-pdf-1.pdf",
            original_filename="report.pdf",
            declared_mime_type="application/pdf",
            detected_mime_type="application/pdf",
            file_extension=".pdf",
            byte_size=123,
            sha256="abc123",
            retrieved_at="2026-06-19T00:00:00+00:00",
            published_at=published_at,
            raw_object_path=self.path,
            source_type="pdf",
        )
        return DownloadedAsset(asset=asset, headers={}, http_status=200)


class FakePdfParser:
    name = "pdf"
    version = "test"
    supported_file_types = {"pdf"}
    supported_mime_types = {"application/pdf"}

    def can_parse(self, asset):
        return asset.file_type == "pdf"

    def parse(self, path, *, asset_id):
        return ParsedAsset(
            asset_id=asset_id,
            file_type="pdf",
            title="PDF 行业报告",
            text_blocks=[
                TextBlock(
                    f"{asset_id}-B001",
                    "这份PDF报告列出价格数据。该产品支持高功率快充。",
                    page_number=1,
                    source_locator="report.pdf#page=1",
                )
            ],
            tables=[
                ParsedTable(
                    f"{asset_id}-T001",
                    columns=["company", "price"],
                    rows=[{"company": "示例公司", "price": "99"}],
                    caption="价格表",
                    source_locator="report.pdf#table=1",
                    extraction_method="pdf_table",
                )
            ],
            extraction_method="pdf",
        )


class FakeParserRegistry:
    def __init__(self):
        self.parser = FakePdfParser()

    def parser_for_asset(self, asset):
        return self.parser if self.parser.can_parse(asset) else None

    def parse_asset(self, asset):
        return self.parser.parse(asset.raw_object_path, asset_id=asset.asset_id)


class CountingEvidenceExtractor:
    def __init__(self):
        self.calls = 0

    def extract(self, topic, page):
        self.calls += 1
        return [
            EvidenceItem(
                entity="示例公司",
                attribute="产品能力",
                value="支持高功率快充",
                exact_quote="该产品支持高功率快充。",
                evidence_type="product_feature",
                confidence_band="high",
            )
        ]


class CancelledEvidenceExtractor:
    def extract(self, topic, page):
        raise CancelledError()


class StaticClaimSynthesizer:
    def synthesize(self, evidence, language="zh"):
        return [
            ClaimItem(
                statement="示例公司的产品支持高功率快充。",
                claim_type="product_comparison",
                supporting_evidence_ids=["E-001"],
                confidence_band="medium",
                reasoning_summary="来源正文直接陈述。",
            )
        ]


class EmptyClaimSynthesizer:
    def __init__(self):
        self.calls = 0

    def synthesize(self, evidence, language="zh"):
        self.calls += 1
        return []


def backend(tmp_path, **overrides):
    values = {
        "planner": None,
        "search_provider": CountingSearchProvider(),
        "fetcher": CountingFetcher(),
        "content_extractor": ContentExtractor(),
        "evidence_extractor": CountingEvidenceExtractor(),
        "claim_synthesizer": None,
        "report_generator": ReportGenerator(),
        "report_validator": None,
        "artifact_store": ArtifactStore(tmp_path / "artifacts"),
        "limits": SimpleNamespace(max_results_per_query=5, max_fetched_pages=15),
        "api_key": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FailingPlanner:
    def create_plan(self, topic):
        raise LLMError("bad json")


class FailingReportValidator:
    def validate(self, **kwargs):
        raise AgentReportValidationError("报告包含不存在的 evidence 引用")


class PassingReportValidator:
    def validate(self, **kwargs):
        return None


class FakeMonitorScheduler:
    def __init__(self, fail: Exception | None = None):
        self.fail = fail
        self.calls = []

    def schedule_id(self, job_id):
        return f"monitor-{job_id}"

    def parse(self, tokens):
        self.calls.append(("parse", tuple(tokens)))
        return SimpleNamespace(
            kind=tokens[0],
            value=tokens[1] if tokens[0] != "weekly" else f"{tokens[1]} {tokens[2]}",
            timezone=tokens[-1] if "/" in tokens[-1] else "UTC",
            display=" ".join(tokens),
        )

    def create(self, job_id, parsed):
        self.calls.append(("create", job_id, parsed.display))
        if self.fail:
            raise self.fail
        return {"next_action_time": "2026-06-18T01:00:00+00:00"}

    def pause(self, schedule_id):
        self.calls.append(("pause", schedule_id))
        if self.fail:
            raise self.fail

    def delete(self, schedule_id):
        self.calls.append(("delete", schedule_id))


class RecordingMessenger:
    def __init__(self):
        self.calls = []

    def send_text_to_chat(self, chat_id, text):
        self.calls.append((chat_id, text))


def add_fetched_source(repository, job_id):
    return repository.add_source(
        job_id=job_id,
        url="https://example.com/report",
        canonical_url="https://example.com/report",
        title="官方资料",
        publisher=None,
        published_at=None,
        search_query="测试竞品",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="该产品支持高功率快充。这里还有足够的正文用于报告生成。",
        status="fetched",
    )


def add_claim(repository, job_id):
    repository.add_claim(
        job_id,
        ClaimItem(
            statement="示例公司的产品支持高功率快充。",
            claim_type="product_comparison",
            supporting_evidence_ids=["E-001"],
            confidence_band="medium",
            reasoning_summary="来源正文直接陈述。",
        ),
    )


def add_monitoring_config(repository, job, *, mode="safe", notify_level="medium"):
    return repository.create_monitoring_config(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_id=f"monitor-{job.job_id}",
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode=mode,
        notify_level=notify_level,
    )


def mark_incremental_search(repository, job_id, run_id, source_id, url):
    repository.add_monitoring_source_snapshot(
        job_id=job_id,
        source_id=source_id,
        run_id=run_id,
        url=url,
        content_hash=None,
        retrieval_method="incremental_search",
        status="searched",
    )


def test_start_monitoring_cycle_resolves_job_from_monitor_id(repository, tmp_path):
    job = create_job(repository)
    config = add_monitoring_config(repository, job)
    base_report_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report_v1.md"),
        str(tmp_path / "report_v1.json"),
    )
    activities = ResearchActivities(repository=repository, backend=backend(tmp_path))

    context = ActivityEnvironment().run(
        activities.start_monitoring_cycle, config.monitor_id, "monitor-cycle"
    )

    assert context == {
        "run_id": context["run_id"],
        "job_id": job.job_id,
        "monitor_id": config.monitor_id,
    }
    run = repository.get_running_monitoring_run(job.job_id)
    assert run["run_id"] == context["run_id"]
    assert run["job_id"] == job.job_id
    assert run["monitor_id"] == config.monitor_id
    assert run["base_report_version_id"] == base_report_id
    assert run["cutoff_from"]
    assert run["cutoff_to"]


def test_monitoring_context_and_delta_plan_are_small_and_persisted(
    repository, tmp_path
):
    job = create_job(repository)
    config = add_monitoring_config(repository, job)
    repository.save_research_plan(
        job.job_id,
        ResearchPlan(
            objective="识别竞品",
            research_questions=["竞品是谁"],
            search_queries=["q1", "q2", "q3", "q4", "q5"],
            comparison_dimensions=["产品"],
            expected_entities=["示例公司"],
            acceptance_criteria=["有引用"],
        ),
    )
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/pricing",
        canonical_url="https://example.com/pricing",
        title="Pricing",
        publisher=None,
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="该产品支持高功率快充。这里还有足够正文用于上下文测试。",
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report_v1.md"),
        str(tmp_path / "report_v1.json"),
    )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_search_queries=4,
                monitor_max_results_per_query=3,
                monitor_max_fetched_pages=6,
                monitor_max_watch_targets=5,
                monitor_lookback_days=7,
            ),
        ),
    )
    start = ActivityEnvironment().run(
        activities.start_monitoring_cycle, config.monitor_id, "monitor-cycle"
    )

    context = ActivityEnvironment().run(
        activities.load_monitor_context, job.job_id
    )
    delta_plan = ActivityEnvironment().run(
        activities.create_delta_plan, job.job_id, context
    )

    run = repository.get_running_monitoring_run(job.job_id)
    assert context["run_id"] == start["run_id"]
    assert context["base_report_version_id"]
    assert context["active_claim_ids"] == ["C-001"]
    assert context["competitor_entity_ids"] == ["示例公司"]
    assert context["watch_target_ids"]
    assert "raw_text" not in json.dumps(context, ensure_ascii=False)
    assert len(delta_plan["search_queries"]) == 3
    assert len(delta_plan["search_queries"]) < 5
    assert delta_plan["watch_target_ids"] == context["watch_target_ids"]
    assert delta_plan["max_search_requests"] == 4
    assert "price_change" in delta_plan["target_event_types"]
    assert run["context_json"]
    assert run["delta_plan_json"]
    assert json.loads(run["delta_plan_json"])["max_pages"] == 6


def test_search_activity_is_idempotent(repository, tmp_path):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    search = CountingSearchProvider()
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path, search_provider=search)
    )

    env = ActivityEnvironment()
    env.run(activities.search_sources, job.job_id)
    env.run(activities.search_sources, job.job_id)

    assert search.calls == 1
    assert len(repository.list_sources(job.job_id)) == 1


def test_monitoring_search_respects_monitor_budget(repository, tmp_path):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    repository.save_research_plan(
        job.job_id,
        ResearchPlan(
            objective="识别竞品",
            research_questions=["竞品是谁"],
            search_queries=["q1", "q2", "q3"],
            comparison_dimensions=["产品"],
            expected_entities=["示例公司"],
            acceptance_criteria=["有引用"],
        ),
    )
    search = CountingSearchProvider()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            search_provider=search,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_search_queries=2,
                monitor_max_results_per_query=3,
                monitor_max_fetched_pages=10,
                monitor_max_new_events=20,
            ),
        ),
    )

    added = ActivityEnvironment().run(
        activities.search_monitoring_sources, job.job_id
    )

    assert added == 1
    assert search.calls == 2
    assert search.limits == [3, 3]
    run = repository.get_latest_monitoring_run(job.job_id)
    assert run["run_id"] == run_id
    assert run["search_request_count"] == 2
    assert run["new_source_count"] == 1
    snapshots = repository.list_source_snapshots(job.job_id, run_id=run_id)
    assert any(
        snapshot["retrieval_method"] == "incremental_search"
        and snapshot["status"] == "searched"
        for snapshot in snapshots
    )


def test_monitoring_search_zero_fetched_pages_means_unlimited(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    repository.save_research_plan(
        job.job_id,
        ResearchPlan(
            objective="识别竞品",
            research_questions=["竞品是谁"],
            search_queries=["q1"],
            comparison_dimensions=["产品"],
            expected_entities=["示例公司"],
            acceptance_criteria=["有引用"],
        ),
    )
    search = MultiSearchProvider(count=3)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            search_provider=search,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_search_queries=1,
                monitor_max_results_per_query=5,
                monitor_max_fetched_pages=0,
                monitor_max_new_events=20,
            ),
        ),
    )

    added = ActivityEnvironment().run(
        activities.search_monitoring_sources, job.job_id
    )

    assert added == 3
    assert search.calls == 1
    snapshots = repository.list_source_snapshots(job.job_id, run_id=run_id)
    assert sum(
        1
        for snapshot in snapshots
        if snapshot["retrieval_method"] == "incremental_search"
        and snapshot["status"] == "searched"
    ) == 3


def test_monitoring_search_resumes_after_query_checkpoint(repository, tmp_path):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    repository.update_monitoring_run_stats(run_id, search_request_count=1)
    repository.save_research_plan(
        job.job_id,
        ResearchPlan(
            objective="识别竞品",
            research_questions=["竞品是谁"],
            search_queries=["q1", "q2"],
            comparison_dimensions=["产品"],
            expected_entities=["示例公司"],
            acceptance_criteria=["有引用"],
        ),
    )
    search = CountingSearchProvider()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, search_provider=search),
    )
    env = ActivityEnvironment()
    env.info = replace(
        env.info,
        heartbeat_details=[
            {"phase": "search", "item_id": "q1", "index": 0}
        ],
    )

    assert env.run(activities.search_monitoring_sources, job.job_id) == 1
    assert search.calls == 1
    assert repository.get_latest_monitoring_run(job.job_id)[
        "search_request_count"
    ] == 2


def test_recheck_monitored_sources_creates_and_uses_watch_targets(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    for index, (url, title) in enumerate(
        [
            ("https://example.com/pricing", "Pricing"),
            ("https://example.com/releases", "Release Notes"),
            ("https://example.com/legal", "Legal"),
        ],
        start=1,
    ):
        repository.add_source(
            job_id=job.job_id,
            url=url,
            canonical_url=url,
            title=title,
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index,
            http_status=200,
            content_type="text/html",
            content_hash="old",
            raw_text="old body",
            status="fetched",
        )
    fetcher = CountingFetcher()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=fetcher,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_watch_targets=2,
                monitor_max_new_events=20,
            ),
        ),
    )

    changed = ActivityEnvironment().run(
        activities.recheck_monitored_sources, job.job_id
    )

    targets = repository.list_monitoring_watch_targets(job.job_id)
    run = repository.get_latest_monitoring_run(job.job_id)
    with repository._connect() as connection:
        snapshots = connection.execute(
            """
            SELECT retrieval_method FROM monitoring_source_snapshots
            WHERE job_id = ? ORDER BY observed_at, source_id
            """,
            (job.job_id,),
        ).fetchall()
    assert changed == 2
    assert fetcher.calls == 2
    assert run["run_id"] == run_id
    assert run["fetched_page_count"] == 2
    assert {target["target_type"] for target in targets} == {
        "pricing_page",
        "release_notes",
    }
    assert [row["retrieval_method"] for row in snapshots] == [
        "watch_target_refetch:pricing_page",
        "watch_target_refetch:release_notes",
    ]


def test_recheck_monitored_sources_resumes_after_target_checkpoint(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    for index, suffix in enumerate(("pricing", "releases"), start=1):
        source_id = repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/{suffix}",
            canonical_url=f"https://example.com/{suffix}",
            title=suffix,
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index,
            http_status=200,
            content_type="text/html",
            content_hash="old",
            raw_text="old body",
            status="fetched",
        )
        repository.upsert_monitoring_watch_target(
            job_id=job.job_id,
            source_id=source_id,
            target_type="pricing_page" if index == 1 else "release_notes",
            url=f"https://example.com/{suffix}",
            canonical_url=f"https://example.com/{suffix}",
        )
    repository.add_monitoring_source_snapshot(
        job_id=job.job_id,
        source_id="S-001",
        run_id=run_id,
        url="https://example.com/pricing",
        content_hash="new",
        raw_text="changed body",
        retrieval_method="watch_target_refetch:pricing_page",
        status="fetched",
    )
    repository.add_change_event(
        job_id=job.job_id,
        source_id="S-001",
        event_type="page_changed",
        severity="medium",
        summary="pricing changed",
        old_value="old",
        new_value="new",
        run_id=run_id,
        event_fingerprint=f"{job.job_id}:page_changed:S-001:new",
    )
    fetcher = CountingFetcher()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, fetcher=fetcher),
    )
    env = ActivityEnvironment()
    env.info = replace(
        env.info,
        heartbeat_details=[
            {"phase": "recheck", "item_id": "S-001", "index": 0}
        ],
    )

    assert env.run(activities.recheck_monitored_sources, job.job_id) == 2
    assert fetcher.calls == 1
    assert len(repository.list_source_snapshots(job.job_id, run_id=run_id)) == 2


def test_monitoring_evidence_respects_fetch_and_llm_budget(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    for index in range(3):
        source_id = repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/report-{index}",
            canonical_url=f"https://example.com/report-{index}",
            title=f"资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
        mark_incremental_search(
            repository,
            job.job_id,
            run_id,
            source_id,
            f"https://example.com/report-{index}",
        )
    fetcher = CountingFetcher()
    evidence_extractor = CountingEvidenceExtractor()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=fetcher,
            evidence_extractor=evidence_extractor,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_fetched_pages=1,
                monitor_max_llm_calls=1,
                monitor_max_new_events=20,
            ),
        ),
    )

    extracted = ActivityEnvironment().run(
        activities.extract_monitoring_evidence, job.job_id
    )

    assert extracted == 1
    assert fetcher.calls == 1
    assert evidence_extractor.calls == 1
    run = repository.get_latest_monitoring_run(job.job_id)
    assert run["run_id"] == run_id
    assert run["fetched_page_count"] == 1
    assert run["llm_call_count"] == 1
    assert run["new_evidence_count"] == 1
    evidence = repository.list_evidence(job.job_id)
    assert evidence[0].snapshot_id
    assert repository.get_latest_source_snapshot(
        job.job_id, evidence[0].source_id
    )["snapshot_id"] == evidence[0].snapshot_id


def test_monitoring_evidence_zero_fetched_pages_means_unlimited(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    for index in range(3):
        source_id = repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/new-report-{index}",
            canonical_url=f"https://example.com/new-report-{index}",
            title=f"新增资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
            error_message=None,
        )
        mark_incremental_search(
            repository,
            job.job_id,
            run_id,
            source_id,
            f"https://example.com/new-report-{index}",
        )
    fetcher = UniqueCountingFetcher()
    evidence_extractor = CountingEvidenceExtractor()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=fetcher,
            evidence_extractor=evidence_extractor,
            limits=SimpleNamespace(
                max_results_per_query=5,
                max_fetched_pages=15,
                monitor_max_fetched_pages=0,
                monitor_max_llm_calls=10,
                monitor_max_new_events=20,
            ),
        ),
    )

    extracted = ActivityEnvironment().run(
        activities.extract_monitoring_evidence, job.job_id
    )

    assert extracted == 3
    assert fetcher.calls == 3
    assert evidence_extractor.calls == 3
    run = repository.get_latest_monitoring_run(job.job_id)
    assert run["fetched_page_count"] == 3
    assert run["new_evidence_count"] == 3


def test_monitoring_evidence_reuses_saved_snapshot_result_without_llm_cost(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    add_fetched_source(repository, job.job_id)
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/new-report",
        canonical_url="https://example.com/new-report",
        title="新增资料",
        publisher=None,
        published_at=None,
        search_query="q",
        search_rank=2,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    mark_incremental_search(
        repository, job.job_id, run_id, source_id, "https://example.com/new-report"
    )
    extractor = CountingEvidenceExtractor()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, evidence_extractor=extractor),
    )

    first = ActivityEnvironment().run(
        activities.extract_monitoring_evidence, job.job_id
    )
    second = ActivityEnvironment().run(
        activities.extract_monitoring_evidence, job.job_id
    )

    assert first == 1
    assert second == 0
    assert extractor.calls == 1
    run = repository.get_latest_monitoring_run(job.job_id)
    assert run["new_evidence_count"] == 1
    assert run["llm_call_count"] == 1


def test_monitoring_change_detection_creates_evidence_backed_event(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/new-report",
        canonical_url="https://example.com/new-report",
        title="新增资料",
        publisher=None,
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    mark_incremental_search(
        repository, job.job_id, run_id, source_id, "https://example.com/new-report"
    )
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path)
    )
    ActivityEnvironment().run(
        activities.extract_monitoring_evidence, job.job_id
    )

    heartbeats = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details)
    detected = env.run(activities.detect_monitoring_changes, job.job_id)

    assert detected == 1
    events = repository.list_change_events(job.job_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "feature_added"
    assert events[0]["entity"] == "示例公司"
    links = repository.list_change_event_evidence(events[0]["event_id"])
    assert [(link["evidence_id"], link["relation"]) for link in links] == [
        ("E-001", "support")
    ]
    assert heartbeats == [
        (
            {
                "phase": "change_detection",
                "item_id": "E-001",
                "index": 0,
            },
        )
    ]


def test_monitoring_change_detection_deduplicates_event_and_adds_evidence(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    quote = "该产品支持高功率快充。"
    for index in range(2):
        source_id = repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/report-{index}",
            canonical_url=f"https://example.com/report-{index}",
            title=f"资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=200,
            content_type="text/html",
            content_hash=f"hash-{index}",
            raw_text=quote,
            status="fetched",
        )
        snapshot_id = repository.add_monitoring_source_snapshot(
            job_id=job.job_id,
            source_id=source_id,
            run_id=run_id,
            url=f"https://example.com/report-{index}",
            content_hash=f"hash-{index}",
            raw_text=quote,
            retrieval_method="incremental_fetch",
            status="fetched",
        )
        repository.add_evidence(
            job.job_id,
            source_id,
            EvidenceItem(
                entity="示例公司",
                attribute="产品能力",
                value="支持高功率快充",
                exact_quote=quote,
                evidence_type="product_feature",
                confidence_band="high",
            ),
            snapshot_id=snapshot_id,
        )
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path)
    )

    assert ActivityEnvironment().run(
        activities.detect_monitoring_changes, job.job_id
    ) == 1

    events = repository.list_change_events(job.job_id)
    assert len(events) == 1
    links = repository.list_change_event_evidence(events[0]["event_id"])
    assert {link["evidence_id"] for link in links} == {"E-001", "E-002"}


def test_activity_success_logs_operational_fields(
    repository, tmp_path, caplog
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path)
    )

    with caplog.at_level(logging.INFO, logger="feishu_agent_bot.temporal.activities"):
        ActivityEnvironment().run(activities.search_sources, job.job_id)

    started = next(
        record
        for record in caplog.records
        if record.getMessage() == "Temporal Activity started"
    )
    completed = next(
        record
        for record in caplog.records
        if record.getMessage() == "Temporal Activity completed"
    )
    assert started.job_id == job.job_id
    assert started.workflow_id == "test"
    assert started.run_id == "test-run"
    assert started.activity == "search_sources_activity"
    assert started.attempt == 1
    assert started.stage == "searching"
    assert completed.job_id == job.job_id
    assert completed.duration_ms >= 0
    assert completed.result == "success"
    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "测试主题" not in messages


def test_activity_failure_logs_error_type_and_result(
    repository, tmp_path, caplog
):
    job = create_job(repository)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, planner=FailingPlanner()),
    )

    with caplog.at_level(logging.WARNING, logger="feishu_agent_bot.temporal.activities"):
        with pytest.raises(InvalidModelOutputError):
            ActivityEnvironment().run(activities.create_plan, job.job_id)

    failed = next(
        record
        for record in caplog.records
        if record.getMessage() == "Temporal Activity failed"
    )
    assert failed.job_id == job.job_id
    assert failed.workflow_id == "test"
    assert failed.run_id == "test-run"
    assert failed.activity == "create_plan_activity"
    assert failed.stage == "planning"
    assert failed.error_type == "InvalidModelOutputError"
    assert failed.result == "error"
    assert failed.duration_ms >= 0


def test_fetch_activity_heartbeats_and_skips_completed_sources(
    repository, tmp_path
):
    job = create_job(repository)
    repository.add_source(
        job_id=job.job_id,
        url="https://example.com/report",
        canonical_url="https://example.com/report",
        title="官方资料",
        publisher=None,
        published_at=None,
        search_query="测试竞品",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    fetcher = CountingFetcher()
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path, fetcher=fetcher)
    )
    heartbeats = []
    env = ActivityEnvironment()

    def record_heartbeat(*details):
        heartbeats.append(details)
        assert repository.list_sources(job.job_id, "fetched")[0]["source_id"] == "S-001"

    env.on_heartbeat = record_heartbeat

    assert env.run(activities.fetch_sources, job.job_id) == ["S-001"]
    assert env.run(activities.fetch_sources, job.job_id) == ["S-001"]

    assert fetcher.calls == 1
    assert heartbeats == [({"last_source_id": "S-001"},)]
    assert repository.get_job(job.job_id).last_heartbeat_at is not None
    assert repository.list_sources(job.job_id, "fetched")[0]["source_id"] == "S-001"


def test_fetch_activity_resumes_after_heartbeat_checkpoint(
    repository, tmp_path
):
    job = create_job(repository)
    for idx in range(2):
        repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/report-{idx}",
            canonical_url=f"https://example.com/report-{idx}",
            title=f"官方资料 {idx}",
            publisher=None,
            published_at=None,
            search_query="测试竞品",
            search_rank=idx + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
    fetcher = CountingFetcher()
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path, fetcher=fetcher)
    )
    env = ActivityEnvironment()
    env.info = replace(
        env.info, heartbeat_details=[{"last_source_id": "S-001"}]
    )

    assert env.run(activities.fetch_sources, job.job_id) == ["S-002"]

    assert fetcher.calls == 1
    assert repository.list_sources(job.job_id, "searched")[0]["source_id"] == "S-001"
    assert repository.list_sources(job.job_id, "fetched")[0]["source_id"] == "S-002"


def test_fetch_activity_uses_bounded_concurrency_and_serial_dedup_commit(
    repository, tmp_path
):
    job = create_job(repository)
    lock = threading.Lock()
    active = 0
    peak = 0

    class SlowFetcher:
        def fetch(self, url):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.04)
            with lock:
                active -= 1
            suffix = url.rsplit("-", 1)[-1]
            body = (
                "This source contains enough unique research content for "
                "parallel acquisition, deterministic deduplication, and "
                "heartbeat validation. "
                + suffix * 220
            )
            return FetchResult(
                requested_url=url,
                final_url=url,
                status_code=200,
                content_type="text/html",
                content=f"<html><main>{body}</main></html>".encode(),
            )

    for index in range(4):
        repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/report-{index}",
            canonical_url=f"https://example.com/report-{index}",
            title=f"资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, fetcher=SlowFetcher()),
        max_concurrent_downloads=2,
    )
    heartbeats = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details)

    fetched = env.run(activities.fetch_sources, job.job_id)

    assert peak == 2
    assert fetched == ["S-001", "S-002", "S-003", "S-004"]
    assert heartbeats == [
        ({"last_source_id": f"S-{index:03d}"},) for index in range(1, 5)
    ]


def test_fetch_activity_accepts_only_first_concurrent_duplicate(
    repository, tmp_path
):
    job = create_job(repository)
    for index in range(2):
        repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/duplicate-{index}",
            canonical_url=f"https://example.com/duplicate-{index}",
            title=f"重复资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, fetcher=CountingFetcher()),
        max_concurrent_downloads=2,
    )

    fetched = ActivityEnvironment().run(activities.fetch_sources, job.job_id)

    assert fetched == ["S-001"]
    failed = repository.list_sources(job.job_id, "failed")
    assert failed[0]["source_id"] == "S-002"
    assert failed[0]["error_message"] == "重复内容，已跳过"


def test_fetch_pipeline_heartbeats_without_advancing_checkpoint_while_waiting(
    repository, tmp_path, monkeypatch
):
    job = create_job(repository)
    repository.add_source(
        job_id=job.job_id,
        url="https://example.com/slow",
        canonical_url="https://example.com/slow",
        title="慢速资料",
        publisher=None,
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )

    class SlowFetcher(CountingFetcher):
        def fetch(self, url):
            time.sleep(0.04)
            return super().fetch(url)

    monkeypatch.setattr(
        activities_module, "SOURCE_PIPELINE_HEARTBEAT_SECONDS", 0.01
    )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, fetcher=SlowFetcher()),
    )
    heartbeats = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])

    env.run(activities.fetch_sources, job.job_id)

    progress = [item for item in heartbeats if "in_progress_source_ids" in item]
    assert progress
    assert all(item["last_source_id"] is None for item in progress)
    assert heartbeats[-1] == {"last_source_id": "S-001"}


def test_file_pipeline_limits_download_parser_and_ocr_concurrency(
    repository, tmp_path
):
    job = create_job(repository)
    lock = threading.Lock()
    download_active = parser_active = pdf_active = 0
    download_peak = parser_peak = pdf_peak = 0
    asset_path = tmp_path / "asset.bin"
    asset_path.write_bytes(b"fixture")

    class SlowDownloader:
        def download(self, *, job_id, url, source_id=None, **kwargs):
            nonlocal download_active, download_peak
            with lock:
                download_active += 1
                download_peak = max(download_peak, download_active)
            time.sleep(0.04)
            with lock:
                download_active -= 1
            number = int(source_id.split("-")[-1])
            is_pdf = number % 2 == 1
            extension = ".pdf" if is_pdf else ".csv"
            mime = "application/pdf" if is_pdf else "text/csv"
            asset = SourceAsset(
                asset_id=f"asset-{source_id}",
                job_id=job_id,
                source_id=source_id,
                original_url=url,
                canonical_url=url,
                generated_filename=f"asset-{source_id}{extension}",
                original_filename=f"source{extension}",
                declared_mime_type=mime,
                detected_mime_type=mime,
                file_extension=extension,
                byte_size=7,
                sha256=f"hash-{source_id}",
                retrieved_at="2026-06-19T00:00:00+00:00",
                raw_object_path=asset_path,
                source_type="pdf" if is_pdf else "csv",
            )
            return DownloadedAsset(asset=asset, headers={}, http_status=200)

    class SlowParserRegistry:
        parser = SimpleNamespace(name="mixed", version="test")

        def parser_for_asset(self, asset):
            return self.parser

        def parse_asset(self, asset):
            nonlocal parser_active, parser_peak, pdf_active, pdf_peak
            is_pdf = asset.file_type == "pdf"
            with lock:
                parser_active += 1
                parser_peak = max(parser_peak, parser_active)
                if is_pdf:
                    pdf_active += 1
                    pdf_peak = max(pdf_peak, pdf_active)
            time.sleep(0.04)
            with lock:
                parser_active -= 1
                if is_pdf:
                    pdf_active -= 1
            return ParsedAsset(
                asset_id=asset.asset_id,
                file_type=asset.file_type,
                text_blocks=[
                    TextBlock(
                        f"{asset.asset_id}-B1",
                        f"Unique parsed content for {asset.asset_id}. " * 10,
                    )
                ],
            )

    for index in range(4):
        repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/file-{index}",
            canonical_url=f"https://example.com/file-{index}",
            title=f"文件 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=UnsupportedContentFetcher(),
            asset_downloader=SlowDownloader(),
            parser_registry=SlowParserRegistry(),
        ),
        max_concurrent_downloads=2,
        max_concurrent_parsers=2,
        max_concurrent_ocr_jobs=1,
    )

    fetched = ActivityEnvironment().run(activities.fetch_sources, job.job_id)

    assert fetched == ["S-001", "S-002", "S-003", "S-004"]
    assert download_peak == 2
    assert parser_peak == 2
    assert pdf_peak == 1


def test_fetch_activity_downloads_and_parses_file_assets_after_page_fetch_fails(
    repository, tmp_path
):
    job = create_job(repository)
    asset_path = tmp_path / "report.pdf"
    asset_path.write_bytes(b"%PDF-1.4\n")
    repository.add_source(
        job_id=job.job_id,
        url="https://example.com/report.pdf",
        canonical_url="https://example.com/report.pdf",
        title="搜索结果 PDF",
        publisher=None,
        published_at=None,
        search_query="测试竞品 filetype:pdf",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    fetcher = UnsupportedContentFetcher()
    downloader = FakeAssetDownloader(asset_path)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=fetcher,
            asset_downloader=downloader,
            parser_registry=FakeParserRegistry(),
            dataset_profiler=DatasetProfiler(),
        ),
    )

    env = ActivityEnvironment()
    assert env.run(activities.fetch_sources, job.job_id) == [
        "S-001"
    ]
    assert env.run(activities.profile_datasets, job.job_id) == 1

    source = repository.list_sources(job.job_id, "fetched")[0]
    assets = repository.list_source_assets(job.job_id)
    tables = repository.list_parsed_tables(job.job_id)
    datasets = repository.list_tabular_datasets(job.job_id)

    assert fetcher.calls == 1
    assert downloader.calls == [
        (job.job_id, "https://example.com/report.pdf", "S-001")
    ]
    assert source["content_type"] == "application/pdf"
    assert source["title"] == "PDF 行业报告"
    assert "支持高功率快充" in source["raw_text"]
    assert "价格表" in source["raw_text"]
    assert assets[0]["parse_status"] == "parsed"
    assert assets[0]["parser_name"] == "pdf"
    assert tables[0]["caption"] == "价格表"
    assert datasets[0]["row_count"] == 1
    assert datasets[0]["profile"]["numeric_columns"] == ["price"]


def test_split_asset_activities_resume_from_persisted_checkpoints(
    repository, tmp_path
):
    job = create_job(repository)
    asset_path = tmp_path / "split-report.pdf"
    asset_path.write_bytes(b"%PDF-1.4\n")
    repository.add_source(
        job_id=job.job_id,
        url="https://example.com/report.pdf",
        canonical_url="https://example.com/report.pdf",
        title="搜索结果 PDF",
        publisher=None,
        published_at=None,
        search_query="测试竞品 filetype:pdf",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    downloader = FakeAssetDownloader(asset_path)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=UnsupportedContentFetcher(),
            asset_downloader=downloader,
            parser_registry=FakeParserRegistry(),
            dataset_profiler=DatasetProfiler(),
        ),
    )
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: None

    discovery = env.run(activities.discover_file_assets, job.job_id)
    assert discovery["asset_source_ids"] == ["S-001"]
    assert env.run(activities.download_assets, job.job_id) == 1
    assert env.run(activities.detect_asset_types, job.job_id) == 1
    assert env.run(activities.parse_assets, job.job_id) == 1
    assert env.run(activities.normalize_datasets, job.job_id) == 1
    assert env.run(activities.profile_datasets, job.job_id) == 1

    assert env.run(activities.discover_file_assets, job.job_id) == {
        "web_sources": 0,
        "asset_source_ids": [],
    }
    assert env.run(activities.download_assets, job.job_id) == 1
    assert env.run(activities.parse_assets, job.job_id) == 0
    assert env.run(activities.normalize_datasets, job.job_id) == 0
    assert len(downloader.calls) == 1
    assert len(repository.list_source_assets(job.job_id)) == 1
    assert len(repository.list_parsed_tables(job.job_id)) == 1
    assert len(repository.list_tabular_datasets(job.job_id)) == 1


def test_split_asset_download_skips_sources_already_fetched_as_web(
    repository, tmp_path
):
    job = create_job(repository)
    repository.add_source(
        job_id=job.job_id,
        url="https://example.com/page",
        canonical_url="https://example.com/page",
        title="网页资料",
        publisher=None,
        published_at=None,
        search_query="测试竞品",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    fetcher = CountingFetcher()
    downloader = FakeAssetDownloader(tmp_path / "unused.pdf")
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=fetcher,
            asset_downloader=downloader,
            parser_registry=FakeParserRegistry(),
            dataset_profiler=DatasetProfiler(),
        ),
    )

    env = ActivityEnvironment()
    discovery = env.run(activities.discover_file_assets, job.job_id)
    downloaded = env.run(activities.download_assets, job.job_id)

    assert discovery == {"web_sources": 1, "asset_source_ids": []}
    assert downloaded == 0
    assert fetcher.calls == 1
    assert downloader.calls == []
    assert len(repository.list_sources(job.job_id, "fetched")) == 1
    assert repository.list_source_assets(job.job_id) == []


def test_profile_datasets_runs_concurrently_and_resumes_from_checkpoints(
    repository, tmp_path
):
    job = create_job(repository)

    class CheckpointProfiler:
        version = "test-1"

        def __init__(self):
            self.lock = threading.Lock()
            self.calls = {"D1": 0, "D2": 0, "D3": 0}
            self.current = 0
            self.peak = 0

        def profile(self, dataset):
            with self.lock:
                self.calls[dataset.dataset_id] += 1
                attempt = self.calls[dataset.dataset_id]
                self.current += 1
                self.peak = max(self.peak, self.current)
            time.sleep(0.05)
            with self.lock:
                self.current -= 1
            if dataset.dataset_id == "D2" and attempt == 1:
                raise RuntimeError("profile interrupted")
            return DatasetProfiler().profile(dataset)

    datasets = []
    for index in range(1, 4):
        dataset = TabularDataset(
            dataset_id=f"D{index}",
            job_id=job.job_id,
            asset_id=f"A{index}",
            table_id=f"T{index}",
            name=f"dataset-{index}",
            columns=["company", "price"],
            rows=[{"company": "A", "price": str(index * 10)}],
        )
        repository.save_tabular_dataset(dataset)
        datasets.append(dataset)

    profiler = CheckpointProfiler()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, dataset_profiler=profiler),
        max_concurrent_profile_jobs=2,
    )
    env = ActivityEnvironment()
    heartbeats = []
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])

    with pytest.raises(RuntimeError, match="profile interrupted"):
        env.run(activities.profile_datasets, job.job_id)

    assert profiler.peak == 2
    assert len(repository.list_dataset_profiles(job.job_id)) == 2
    assert len([item for item in heartbeats if item.get("dataset_id")]) == 2

    heartbeats.clear()
    assert env.run(activities.profile_datasets, job.job_id) == 1
    assert profiler.calls == {"D1": 1, "D2": 2, "D3": 1}
    assert len(repository.list_dataset_profiles(job.job_id)) == 3
    assert heartbeats[-1]["dataset_id"] == "D2"
    assert heartbeats[-1]["reused"] is False

    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            "UPDATE tabular_datasets SET profile_json = '{}' WHERE dataset_id = 'D1'"
        )
    heartbeats.clear()
    assert env.run(activities.profile_datasets, job.job_id) == 1
    assert profiler.calls == {"D1": 1, "D2": 2, "D3": 1}
    assert heartbeats[-1]["dataset_id"] == "D1"
    assert heartbeats[-1]["reused"] is True

    heartbeats.clear()
    assert env.run(activities.profile_datasets, job.job_id) == 0
    assert heartbeats == [
        {"phase": "dataset_profile", "completed": 0, "pending": []}
    ]


def test_monitoring_file_asset_drives_incremental_analysis_and_same_hash_skips(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job)
    historical_dataset = TabularDataset(
        dataset_id=f"{job.job_id}:historical-competitors",
        job_id=job.job_id,
        asset_id="historical-asset",
        table_id="historical-table",
        name="历史竞品矩阵",
        columns=["company", "feature"],
        rows=[{"company": "Legacy Co", "feature": "历史能力"}],
        lineage={"source_locator": "historical.xlsx#Sheet1!A1:B2"},
    )
    repository.save_tabular_dataset(
        historical_dataset,
        DatasetProfiler().profile(historical_dataset),
    )
    first_run_id = repository.start_monitoring_run(
        job.job_id, "monitor-file-cycle-1"
    )
    asset_path = tmp_path / "pricing-report.pdf"
    asset_path.write_bytes(b"%PDF-1.4\n")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/pricing-report.pdf",
        canonical_url="https://example.com/pricing-report.pdf",
        title="Pricing PDF",
        publisher=None,
        published_at=None,
        search_query="pricing filetype:pdf",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
        error_message=None,
    )
    mark_incremental_search(
        repository,
        job.job_id,
        first_run_id,
        source_id,
        "https://example.com/pricing-report.pdf",
    )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=UnsupportedContentFetcher(),
            asset_downloader=FakeAssetDownloader(asset_path),
            parser_registry=FakeParserRegistry(),
            dataset_profiler=DatasetProfiler(),
        ),
    )
    env = ActivityEnvironment()

    assert env.run(activities.extract_monitoring_evidence, job.job_id) == 1
    assert env.run(activities.profile_datasets, job.job_id) == 1
    affected = repository.list_monitoring_run_dataset_ids(
        job.job_id, first_run_id
    )
    assert affected == [f"{job.job_id}:asset-pdf-1-T001"]
    assert env.run(activities.run_incremental_analysis, job.job_id) > 0
    runs = repository.list_analysis_runs(job.job_id)
    assert runs[-1]["reason"].startswith("monitoring incremental update")
    assert "pricing_and_packaging" in runs[-1]["selected_skills"]
    incremental_results = [
        result
        for result in repository.list_analysis_results(job.job_id)
        if result["run_id"] == runs[-1]["analysis_run_id"]
    ]
    assert incremental_results
    assert all(
        set(result["input_dataset_ids"]).issubset(set(affected))
        for result in incremental_results
    )
    assert all(
        historical_dataset.dataset_id not in result["input_dataset_ids"]
        for result in incremental_results
    )

    repository.complete_monitoring_run(
        first_run_id, job.job_id, decision="completed"
    )
    second_run_id = repository.start_monitoring_run(
        job.job_id, "monitor-file-cycle-2"
    )
    repository.upsert_monitoring_watch_target(
        job_id=job.job_id,
        source_id=source_id,
        target_type="pricing_page",
        url="https://example.com/pricing-report.pdf",
        canonical_url="https://example.com/pricing-report.pdf",
    )

    assert env.run(activities.recheck_monitored_sources, job.job_id) == 0
    assert repository.list_monitoring_run_dataset_ids(
        job.job_id, second_run_id
    ) == []
    assert env.run(activities.run_incremental_analysis, job.job_id) == 0
    assert len(repository.list_analysis_runs(job.job_id)) == len(runs)


def test_extract_evidence_reraises_temporal_cancellation(
    repository, tmp_path
):
    job = create_job(repository)
    add_fetched_source(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path, evidence_extractor=CancelledEvidenceExtractor()
        ),
    )

    with pytest.raises(CancelledError):
        ActivityEnvironment().run(activities.extract_evidence, job.job_id)

    assert repository.list_evidence(job.job_id) == []


def test_extract_evidence_skips_sources_already_extracted(
    repository, tmp_path
):
    job = create_job(repository)
    add_fetched_source(repository, job.job_id)
    extractor = CountingEvidenceExtractor()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, evidence_extractor=extractor),
    )
    env = ActivityEnvironment()
    heartbeats = []

    def record_heartbeat(*details):
        heartbeats.append(details)
        assert repository.source_has_evidence(job.job_id, "S-001")

    env.on_heartbeat = record_heartbeat

    env.run(activities.extract_evidence, job.job_id)
    env.run(activities.extract_evidence, job.job_id)

    assert extractor.calls == 1
    assert heartbeats == [
        ({"last_source_id": "S-001"},),
        ({"last_source_id": "S-001"},),
    ]
    assert repository.get_job(job.job_id).last_heartbeat_at is not None
    assert len(repository.list_evidence(job.job_id)) == 1


def test_extract_evidence_resumes_after_heartbeat_checkpoint(
    repository, tmp_path
):
    job = create_job(repository)
    first_source_id = add_fetched_source(repository, job.job_id)
    second_source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/report-2",
        canonical_url="https://example.com/report-2",
        title="第二份官方资料",
        publisher=None,
        published_at=None,
        search_query="测试竞品",
        search_rank=2,
        http_status=200,
        content_type="text/html",
        content_hash="hash-2",
        raw_text="该产品支持高功率快充。第二份资料也有足够正文。",
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        first_source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    extractor = CountingEvidenceExtractor()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, evidence_extractor=extractor),
    )
    env = ActivityEnvironment()
    env.info = replace(
        env.info, heartbeat_details=[{"last_source_id": first_source_id}]
    )

    assert env.run(activities.extract_evidence, job.job_id) == job.job_id

    assert second_source_id == "S-002"
    assert extractor.calls == 1
    assert repository.source_has_evidence(job.job_id, "S-001")
    assert repository.source_has_evidence(job.job_id, "S-002")


def test_generate_report_retry_does_not_create_v2(repository, tmp_path):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    item = EvidenceItem(
        entity="示例公司",
        attribute="产品能力",
        value="支持高功率快充",
        exact_quote="该产品支持高功率快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )
    assert repository.add_evidence(job.job_id, source_id, item)
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path)
    )
    env = ActivityEnvironment()

    first = env.run(activities.generate_report, job.job_id)
    second = env.run(activities.generate_report, job.job_id)

    assert first == second
    with sqlite3.connect(repository.database_path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM report_versions WHERE job_id = ?",
            (job.job_id,),
        ).fetchone()[0]
    assert count == 1


def test_generate_report_uses_persisted_research_language(repository, tmp_path):
    job = repository.create_job(
        "u1",
        "c1",
        "m-language",
        "Competitive topic",
        execution_backend="temporal",
        research_options={"language": "en"},
    )
    repository.start_job(job.job_id)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    assert repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="Example",
            attribute="capability",
            value="fast charging",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository, backend=backend(tmp_path)
    )

    ActivityEnvironment().run(activities.generate_report, job.job_id)

    report = repository.get_latest_report(job.job_id, status=None)
    payload = json.loads(
        Path(report["report_json_path"]).read_text(encoding="utf-8")
    )
    assert payload["language"] == "en"
    assert payload["sections"][0]["title"] == "Competitors and Key Participants"


def test_run_professional_analysis_uses_persisted_tabular_datasets(
    repository, tmp_path
):
    job = create_job(repository)
    dataset = TabularDataset(
        dataset_id="D1",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="competitor pricing",
        columns=["company", "price"],
        rows=[{"company": "A", "price": "99"}],
        lineage={"source_locator": "prices.csv#rows"},
    )
    repository.save_tabular_dataset(dataset, DatasetProfiler().profile(dataset))
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, dataset_profiler=DatasetProfiler()),
    )

    env = ActivityEnvironment()
    heartbeats = []
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])
    assert env.run(activities.run_professional_analysis, job.job_id) == job.job_id
    first_heartbeat_count = len(heartbeats)
    heartbeats.clear()
    assert env.run(activities.run_professional_analysis, job.job_id) == job.job_id

    runs = repository.list_analysis_runs(job.job_id)
    results = repository.list_analysis_results(job.job_id)
    assert len(runs) == 1
    assert first_heartbeat_count == len(results) + 1
    assert heartbeats == [{"phase": "analysis_planning", "completed_tasks": 0}]
    assert all(item["idempotency_key"] for item in results)
    assert "competitor_matrix" in runs[0]["selected_tools"]
    assert "pricing_normalizer" in runs[0]["selected_tools"]
    assert any(item["tool_name"] == "data_quality_summarizer" for item in results)
    assert any(item.get("tables") for item in results)

    changed = TabularDataset(
        dataset_id="D1",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="competitor pricing",
        columns=["company", "price"],
        rows=[{"company": "A", "price": "199"}],
        lineage={"source_locator": "prices.csv#rows"},
    )
    repository.save_tabular_dataset(changed, DatasetProfiler().profile(changed))
    heartbeats.clear()
    assert env.run(activities.run_professional_analysis, job.job_id) == job.job_id
    assert len(repository.list_analysis_runs(job.job_id)) == 2
    assert len(heartbeats) > 1


def test_split_analysis_activities_reuse_tool_and_skill_results(
    repository, tmp_path
):
    job = repository.create_job(
        "u1", "c1", "m-split-analysis", "竞品价格分析",
        execution_backend="temporal",
    )
    repository.start_job(job.job_id)
    dataset = TabularDataset(
        dataset_id="D-SPLIT",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="competitor pricing",
        columns=["company", "price"],
        rows=[{"company": "A", "price": "99"}],
    )
    repository.save_tabular_dataset(dataset, DatasetProfiler().profile(dataset))
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, dataset_profiler=DatasetProfiler()),
    )
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: None

    analysis_plan = env.run(activities.select_analysis_skills, job.job_id)
    assert "data_quality_summarizer" in analysis_plan["selected_tools"]
    assert "competitor_benchmark" in analysis_plan["selected_skills"]
    assert env.run(
        activities.execute_analysis_tools, job.job_id, analysis_plan
    ) == job.job_id
    assert env.run(
        activities.generate_business_analysis, job.job_id, analysis_plan
    ) == job.job_id
    first_runs = repository.list_analysis_runs(job.job_id)
    first_results = repository.list_analysis_results(job.job_id)

    env.run(activities.execute_analysis_tools, job.job_id, analysis_plan)
    env.run(activities.generate_business_analysis, job.job_id, analysis_plan)

    assert len(repository.list_analysis_runs(job.job_id)) == len(first_runs) == 2
    assert len(repository.list_analysis_results(job.job_id)) == len(first_results)
    assert all(item["idempotency_key"] for item in first_results)


def test_run_professional_analysis_honors_research_include_exclude_options(
    repository, tmp_path
):
    job = repository.create_job(
        "u1",
        "c1",
        "m-options",
        "竞品价格市场",
        execution_backend="temporal",
        research_options={
            "include": ["pricing,market_position"],
            "exclude": ["competitor"],
            "deliverables": ["pdf", "xlsx"],
            "depth": "professional",
        },
    )
    repository.start_job(job.job_id)
    dataset = TabularDataset(
        dataset_id="D_OPTIONS",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="competitor pricing market",
        columns=["company", "price", "market_share"],
        rows=[{"company": "A", "price": "99", "market_share": "10"}],
        lineage={"source_locator": "options.csv#rows"},
    )
    repository.save_tabular_dataset(dataset, DatasetProfiler().profile(dataset))
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, dataset_profiler=DatasetProfiler()),
    )

    env = ActivityEnvironment()
    assert env.run(activities.run_professional_analysis, job.job_id) == job.job_id

    run = repository.list_analysis_runs(job.job_id)[0]
    assert "pricing_and_packaging" in run["selected_skills"]
    assert "market_positioning" in run["selected_skills"]
    assert "competitor_benchmark" not in run["selected_skills"]
    assert "competitor_matrix" not in run["selected_tools"]
    skipped = {
        item["skill_name"]: item
        for item in run["analysis_plan"]["skipped_skills"]
    }
    assert skipped["competitor_benchmark"]["reason"] == "用户显式排除该分析"


def test_run_professional_analysis_honors_depth_defaults(repository, tmp_path):
    quick_job = repository.create_job(
        "u1",
        "c1",
        "m-depth-quick",
        "竞品价格市场商业模式趋势",
        execution_backend="temporal",
        research_options={"depth": "quick"},
    )
    standard_job = repository.create_job(
        "u1",
        "c1",
        "m-depth-standard",
        "竞品价格市场商业模式趋势",
        execution_backend="temporal",
        research_options={"depth": "standard"},
    )
    for job in (quick_job, standard_job):
        repository.start_job(job.job_id)
        dataset = TabularDataset(
            dataset_id=f"{job.job_id}:D",
            job_id=job.job_id,
            asset_id="A1",
            table_id="T1",
            name="competitor pricing market business trend",
            columns=["company", "price", "market_share", "revenue", "month"],
            rows=[
                {
                    "company": "A",
                    "price": "99",
                    "market_share": "10",
                    "revenue": "100",
                    "month": "2026-01",
                },
                {
                    "company": "B",
                    "price": "129",
                    "market_share": "12",
                    "revenue": "120",
                    "month": "2026-02",
                },
            ],
        )
        repository.save_tabular_dataset(dataset, DatasetProfiler().profile(dataset))

    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, dataset_profiler=DatasetProfiler()),
    )
    env = ActivityEnvironment()

    assert env.run(activities.run_professional_analysis, quick_job.job_id) == quick_job.job_id
    assert env.run(activities.run_professional_analysis, standard_job.job_id) == standard_job.job_id

    quick_run = repository.list_analysis_runs(quick_job.job_id)[0]
    standard_run = repository.list_analysis_runs(standard_job.job_id)[0]
    assert quick_run["selected_tools"] == ["data_quality_summarizer"]
    assert quick_run["selected_skills"] == []
    assert "pricing_and_packaging" in standard_run["selected_skills"]
    assert "market_positioning" in standard_run["selected_skills"]
    assert "business_model" not in standard_run["selected_skills"]
    assert "trend_and_change" not in standard_run["selected_skills"]


def test_depth_limits_search_and_fetch_scope(repository, tmp_path):
    class UniqueSearchProvider(CountingSearchProvider):
        def search(self, query, limit):
            self.calls += 1
            self.limits.append(limit)
            return [
                SearchResult(
                    title=f"资料 {query}",
                    url=f"https://example.com/{query}",
                    snippet="资料",
                    query=query,
                    rank=1,
                )
            ]

    job = repository.create_job(
        "u1",
        "c1",
        "m-depth-search",
        "快速搜索",
        execution_backend="temporal",
        research_options={"depth": "quick"},
    )
    repository.start_job(job.job_id)
    repository.save_research_plan(
        job.job_id,
        ResearchPlan(
            objective="快速搜索",
            research_questions=["问题"],
            search_queries=["q1", "q2", "q3", "q4", "q5"],
            comparison_dimensions=["产品"],
            expected_entities=["示例公司"],
            acceptance_criteria=["有引用"],
        ),
    )
    search_provider = UniqueSearchProvider()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            search_provider=search_provider,
            fetcher=CountingFetcher(),
            limits=SimpleNamespace(
                max_search_queries=6,
                max_results_per_query=5,
                max_fetched_pages=15,
            ),
        ),
    )
    env = ActivityEnvironment()

    assert env.run(activities.search_sources, job.job_id) == job.job_id
    assert search_provider.calls == 2
    assert search_provider.limits == [3, 3]
    assert len(repository.list_sources(job.job_id, "searched")) == 2
    fetched = env.run(activities.fetch_sources, job.job_id)
    assert len(fetched) <= 5


def test_default_fetch_limit_is_unlimited(repository, tmp_path):
    job = create_job(repository)
    for index in range(8):
        repository.add_source(
            job_id=job.job_id,
            url=f"https://example.com/unlimited-{index}",
            canonical_url=f"https://example.com/unlimited-{index}",
            title=f"资料 {index}",
            publisher=None,
            published_at=None,
            search_query="q",
            search_rank=index + 1,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="searched",
        )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            fetcher=UniqueCountingFetcher(),
            limits=SimpleNamespace(
                max_search_queries=6,
                max_results_per_query=5,
                max_fetched_pages=0,
            ),
        ),
        max_concurrent_downloads=3,
    )

    fetched = ActivityEnvironment().run(activities.fetch_sources, job.job_id)

    assert len(fetched) == 8


def test_notification_failure_keeps_completed_report(repository, tmp_path):
    class FailingMessenger:
        def send_text_to_chat(self, chat_id, text):
            raise RuntimeError("send failed")

    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    item = EvidenceItem(
        entity="示例公司",
        attribute="产品能力",
        value="支持高功率快充",
        exact_quote="该产品支持高功率快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )
    repository.add_evidence(job.job_id, source_id, item)
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        messenger=FailingMessenger(),
    )
    env = ActivityEnvironment()
    env.info = replace(env.info, attempt=3)
    env.run(activities.generate_report, job.job_id)
    env.run(activities.validate_report, job.job_id)
    env.run(activities.generate_professional_artifacts, job.job_id)
    env.run(activities.complete_job, job.job_id)

    result = env.run(activities.notify_completion, job.job_id)

    updated = repository.get_job(job.job_id)
    assert result.startswith("failed:")
    assert updated.status == "completed"
    assert updated.notification_status == "failed"
    assert repository.get_latest_report(job.job_id) is not None


def test_professional_artifact_generation_retries_failed_pdf(
    repository, tmp_path
):
    class RecoveringArtifactBuilder:
        def __init__(self):
            self.calls = 0

        def build(self, ir, deliverables, *, heartbeat=None):
            self.calls += 1
            if heartbeat:
                heartbeat("fake_build")
            output_dir = tmp_path / "recovering-artifacts"
            output_dir.mkdir(parents=True, exist_ok=True)
            json_path = output_dir / "report_ir.json"
            xlsx_path = output_dir / "report.xlsx"
            pdf_path = output_dir / "report.pdf"
            json_path.write_text(
                json.dumps(_ir_to_dict(ir), ensure_ascii=False), encoding="utf-8"
            )
            xlsx_path.write_bytes(b"xlsx")
            artifacts = [
                BuiltArtifact("json", json_path, "json-hash"),
                BuiltArtifact("xlsx", xlsx_path, "xlsx-hash"),
            ]
            if self.calls == 1:
                artifacts.append(
                    BuiltArtifact(
                        "pdf",
                        pdf_path,
                        "",
                        status="failed",
                        error_message="temporary compiler failure",
                    )
                )
            else:
                pdf_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
                artifacts.append(BuiltArtifact("pdf", pdf_path, "pdf-hash"))
            return artifacts

    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
    )
    recovering_builder = RecoveringArtifactBuilder()
    activities.professional_artifact_builder = recovering_builder
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)
    report = repository.get_latest_report(job.job_id, status=None)
    repository.publish_report_version(report["report_version_id"])

    env.run(activities.generate_professional_artifacts, job.job_id)
    first = repository.list_report_artifacts(
        job.job_id, report_version_id=report["report_version_id"]
    )
    failed_pdf = next(item for item in first if item["artifact_type"] == "pdf")
    assert failed_pdf["status"] == "failed"

    env.run(activities.generate_professional_artifacts, job.job_id)
    second = repository.list_report_artifacts(
        job.job_id, report_version_id=report["report_version_id"]
    )
    ready_pdf = next(item for item in second if item["artifact_type"] == "pdf")
    assert recovering_builder.calls == 2
    assert ready_pdf["artifact_id"] == failed_pdf["artifact_id"]
    assert ready_pdf["status"] == "ready"
    assert len([item for item in second if item["artifact_type"] == "pdf"]) == 1

    env.run(activities.generate_professional_artifacts, job.job_id)
    assert recovering_builder.calls == 2


def test_completion_notifies_pdf_failure_and_still_delivers_xlsx(
    repository, tmp_path
):
    class RecordingMessenger:
        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append(("text", chat_id, text))

        def send_file_to_chat(self, chat_id, path):
            self.calls.append(("file", chat_id, path))

    messenger = RecordingMessenger()
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        messenger=messenger,
    )
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)
    env.run(activities.validate_report, job.job_id)
    report = repository.get_latest_report(job.job_id)
    output_dir = tmp_path / "failed-pdf"
    output_dir.mkdir()
    xlsx_path = output_dir / "report.xlsx"
    xlsx_path.write_bytes(b"xlsx")
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=report["report_version_id"],
        artifact_type="xlsx",
        artifact_path=str(xlsx_path),
        content_hash="xlsx-hash",
        status="ready",
    )
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=report["report_version_id"],
        artifact_type="pdf",
        artifact_path=str(output_dir / "report.pdf"),
        content_hash=None,
        status="failed",
        error_message=(
            "PDF compilation failed\n"
            "! Missing $ inserted.\n"
            "Output written on report.pdf."
        ),
    )
    env.run(activities.complete_job, job.job_id)

    result = env.run(activities.notify_completion, job.job_id)

    assert result == "sent"
    assert [call[0] for call in messenger.calls] == ["text", "file"]
    message = messenger.calls[0][2]
    assert (
        "- PDF 报告（生成失败：LaTeX 引用或正文包含未转义特殊字符）"
        in message
    )
    assert "- Excel 数据与分析工作簿" in message
    assert messenger.calls[1][2] == str(xlsx_path)


def test_split_professional_artifact_activities_checkpoint_each_stage(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        pdf_enabled=False,
    )
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: None
    env.run(activities.generate_report, job.job_id)
    report = repository.get_latest_report(job.job_id, status=None)
    repository.publish_report_version(report["report_version_id"])

    assert env.run(activities.build_report_ir, job.job_id) == report["report_version_id"]
    assert env.run(activities.render_charts, job.job_id) == 1
    assert env.run(activities.render_latex, job.job_id) == "ready"
    assert env.run(activities.compile_pdf, job.job_id) == "unavailable"
    assert env.run(activities.render_excel, job.job_id) == "ready"
    assert env.run(activities.validate_artifacts, job.job_id) == "ready"
    assert env.run(activities.validate_artifacts, job.job_id) == "ready"

    artifacts = repository.list_report_artifacts(
        job.job_id, report_version_id=report["report_version_id"]
    )
    by_type = {item["artifact_type"]: item for item in artifacts}
    assert by_type["json"]["status"] == "ready"
    assert by_type["xlsx"]["status"] == "ready"
    assert by_type["pdf"]["status"] == "unavailable"
    assert by_type["chart_png"]["status"] == "ready"
    assert by_type["chart_svg"]["status"] == "ready"
    assert by_type["chart_data_csv"]["status"] == "ready"
    assert by_type["chart_metadata_json"]["status"] == "ready"
    assert by_type["manifest"]["status"] == "ready"
    assert by_type["artifact_manifest"]["status"] == "ready"
    assert len(artifacts) == len({item["artifact_type"] for item in artifacts})


def test_notification_success_sends_summary_and_report_file(
    repository, tmp_path
):
    class RecordingMessenger:
        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append(("text", chat_id, text))

        def send_file_to_chat(self, chat_id, path):
            self.calls.append(("file", chat_id, path))

    messenger = RecordingMessenger()
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    item = EvidenceItem(
        entity="示例公司",
        attribute="产品能力",
        value="支持高功率快充",
        exact_quote="该产品支持高功率快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )
    repository.add_evidence(job.job_id, source_id, item)
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        messenger=messenger,
    )
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)
    env.run(activities.validate_report, job.job_id)
    env.run(activities.generate_professional_artifacts, job.job_id)
    env.run(activities.complete_job, job.job_id)

    result = env.run(activities.notify_completion, job.job_id)

    updated = repository.get_job(job.job_id)
    report = repository.get_latest_report(job.job_id)
    assert result == "sent"
    assert updated.notification_status == "sent"
    assert messenger.calls[0][0:2] == ("text", job.chat_id)
    completion_message = messenger.calls[0][2]
    assert job.job_id in completion_message
    assert completion_message.startswith("调研报告已完成")
    assert "报告版本：v1" in completion_message
    assert "来源数量：1" in completion_message
    assert "文件资料数量：" in completion_message
    assert "数据集数量：" in completion_message
    assert "证据数量：1" in completion_message
    assert "分析方法：" in completion_message
    assert "主要结论：" in completion_message
    assert "数据限制：" in completion_message
    assert "交付文件：" in completion_message
    assert "- PDF 报告" in completion_message
    assert "- Excel 数据与分析工作簿" in completion_message
    artifacts = repository.list_report_artifacts(
        job.job_id,
        report_version_id=report["report_version_id"],
        ready_only=True,
    )
    assert artifacts
    assert messenger.calls[1][0:2] == ("file", job.chat_id)
    assert messenger.calls[1][2].endswith(("report.pdf", "report.xlsx"))


def test_notification_completion_is_idempotent(repository, tmp_path):
    class RecordingMessenger:
        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append(("text", chat_id, text))

        def send_file_to_chat(self, chat_id, path):
            self.calls.append(("file", chat_id, path))

    messenger = RecordingMessenger()
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        messenger=messenger,
    )
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)
    env.run(activities.validate_report, job.job_id)
    env.run(activities.generate_professional_artifacts, job.job_id)
    env.run(activities.complete_job, job.job_id)

    first = env.run(activities.notify_completion, job.job_id)
    second = env.run(activities.notify_completion, job.job_id)

    assert first == "sent"
    assert second == "skipped_duplicate"
    assert [call[0] for call in messenger.calls] == ["text", "file", "file"]
    assert any(call[2].endswith("report.pdf") for call in messenger.calls if call[0] == "file")
    assert any(call[2].endswith("report.xlsx") for call in messenger.calls if call[0] == "file")
    deliveries = repository.list_artifact_deliveries(job.job_id)
    assert len(deliveries) == 2
    assert {delivery["status"] for delivery in deliveries} == {"sent"}


def test_notification_rejects_oversized_artifacts_with_clear_notice(
    repository, tmp_path
):
    class SizeLimitedMessenger:
        max_file_bytes = 1

        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append(("text", chat_id, text))

        def send_file_to_chat(self, chat_id, path):
            self.calls.append(("file", chat_id, path))

    messenger = SizeLimitedMessenger()
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
        messenger=messenger,
    )
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)
    env.run(activities.validate_report, job.job_id)
    env.run(activities.generate_professional_artifacts, job.job_id)
    env.run(activities.complete_job, job.job_id)

    result = env.run(activities.notify_completion, job.job_id)

    assert result == "sent"
    assert [call[0] for call in messenger.calls] == ["text"]
    assert "超过飞书限制 1 字节" in messenger.calls[0][2]
    deliveries = repository.list_artifact_deliveries(job.job_id)
    assert len(deliveries) == 2
    assert {delivery["status"] for delivery in deliveries} == {"rejected"}


def test_project_workflow_status_sends_stage_progress_notification(
    repository, tmp_path
):
    class RecordingMessenger:
        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append((chat_id, text))

    messenger = RecordingMessenger()
    job = create_job(repository)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    ActivityEnvironment().run(
        activities.project_workflow_status, job.job_id, "planning", 10, False
    )
    ActivityEnvironment().run(
        activities.project_workflow_status, job.job_id, "planning", 10, False
    )

    assert len(messenger.calls) == 1
    assert messenger.calls[0][0] == job.chat_id
    assert "调研进度" in messenger.calls[0][1]
    assert "规划调研问题" in messenger.calls[0][1]


def test_notify_validation_failed_reports_auto_retry_choice(
    repository, tmp_path
):
    class RecordingMessenger:
        def __init__(self):
            self.calls = []

        def send_text_to_chat(self, chat_id, text):
            self.calls.append((chat_id, text))

    messenger = RecordingMessenger()
    job = create_job(repository)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    result = ActivityEnvironment().run(
        activities.notify_validation_failed,
        job.job_id,
        "报告缺少引用",
        True,
        1,
    )
    manual_result = ActivityEnvironment().run(
        activities.notify_validation_failed,
        job.job_id,
        "报告缺少引用",
        False,
        2,
    )

    assert result == "sent"
    assert manual_result == "sent"
    assert "自动回退并重新生成报告" in messenger.calls[0][1]
    assert "未自动重复研究" in messenger.calls[1][1]
    assert f"/research {job.topic}" in messenger.calls[1][1]


def test_reset_report_generation_clears_claims_and_reports(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
    )
    ActivityEnvironment().run(activities.generate_report, job.job_id)

    result = ActivityEnvironment().run(
        activities.reset_report_generation, job.job_id
    )

    assert result == job.job_id
    assert repository.list_claims(job.job_id) == []
    assert repository.get_latest_report(job.job_id) is None


def test_register_monitoring_schedule_creates_config_idempotently(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_monitor_registration_request(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )
    scheduler = FakeMonitorScheduler()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        monitor_scheduler=scheduler,
    )
    env = ActivityEnvironment()

    first = env.run(activities.register_monitoring_schedule, job.job_id)
    second = env.run(activities.register_monitoring_schedule, job.job_id)

    config = repository.get_monitoring_config(job.job_id)
    request = repository.get_monitor_registration_request(job.job_id)
    assert first == "registered"
    assert second == "registered"
    assert config.schedule_id == f"monitor-{job.job_id}"
    assert config.mode == "safe"
    assert request["status"] == "registered"
    assert repository.get_job(job.job_id).monitor_registration_status == "registered"
    assert scheduler.calls.count(("create", job.job_id, "daily 09:00 Asia/Shanghai")) == 1


def test_register_monitoring_schedule_failure_marks_job_and_notifies(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_monitor_registration_request(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_kind="every",
        schedule_value="6h",
        timezone="UTC",
        mode="safe",
        notify_level="high",
    )
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
        monitor_scheduler=FakeMonitorScheduler(RuntimeError("schedule failed")),
    )

    result = ActivityEnvironment().run(
        activities.register_monitoring_schedule, job.job_id
    )

    job_after = repository.get_job(job.job_id)
    request = repository.get_monitor_registration_request(job.job_id)
    assert result.startswith("failed:")
    assert job_after.monitor_registration_status == "monitor_registration_failed"
    assert "schedule failed" in job_after.monitor_registration_error
    assert request["status"] == "monitor_registration_failed"
    assert "报告已完成，但周期监测注册失败" in messenger.calls[0][1]


def test_register_monitoring_schedule_deletes_created_schedule_on_db_failure(
    repository, tmp_path, monkeypatch
):
    job = create_job(repository)
    repository.save_monitor_registration_request(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )
    scheduler = FakeMonitorScheduler()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        monitor_scheduler=scheduler,
    )

    def fail_create_config(**_kwargs):
        raise RuntimeError("sqlite failed")

    monkeypatch.setattr(repository, "create_monitoring_config", fail_create_config)

    result = ActivityEnvironment().run(
        activities.register_monitoring_schedule, job.job_id
    )

    assert result.startswith("failed:")
    assert ("delete", f"monitor-{job.job_id}") in scheduler.calls


def test_update_monitoring_report_publishes_version_only_after_validation(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=StaticClaimSynthesizer(),
            report_validator=PassingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    event_id = repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="页面变化",
        old_value="old",
        new_value="new",
        evidence_ids=["E-001"],
    )

    decision = env.run(activities.update_monitoring_report, job.job_id)

    assert decision.startswith("auto_patch")
    assert repository.get_report_version(job.job_id, 2) is None
    assert repository.get_latest_report(job.job_id)["version"] == 1
    assert repository.list_change_events(job.job_id, "open")
    patch = repository.list_report_patches(
        job_id=job.job_id, approval_status="not_required"
    )[0]
    draft_json = json.loads(
        Path(patch["patch_json"]["report_json_path"]).read_text(encoding="utf-8")
    )
    draft_markdown = Path(patch["patch_json"]["report_path"]).read_text(
        encoding="utf-8"
    )
    impacts = repository.list_claim_impacts(job.job_id, [event_id])
    assert "## 竞品对比" in draft_markdown
    assert "### 本轮监测更新" in draft_markdown
    assert "页面变化" in draft_markdown
    assert "引用证据：E-001" in draft_markdown
    assert "## 风险\n- 暂无充分证据。" in draft_markdown
    assert "## 来源清单\n### S-001 官方资料" in draft_markdown
    assert draft_json["sections"]
    assert draft_json["monitoring_revision"]["status"] == "auto_patch"
    assert draft_json["monitoring_revision"]["decision"] == "auto_patch"
    assert draft_json["monitoring_revision"]["base_version"] == 1
    assert draft_json["monitoring_revision"]["impacted_section_ids"] == [
        "product_comparison"
    ]
    assert draft_json["claim_impacts"][0]["claim_id"] == "C-001"
    assert patch["report_version_id"] is None
    assert patch["report_revision_id"] is None
    assert patch["approval_status"] == "not_required"
    assert patch["patch_json"]["change_event_ids"] == [event_id]
    assert patch["patch_json"]["section_patches"][0]["section_id"] == (
        "product_comparison"
    )
    assert patch["patch_json"]["section_patches"][0]["operation"] == "append"
    assert patch["patch_json"]["section_patches"][0]["evidence_ids"] == ["E-001"]
    assert draft_json["monitoring_revision"]["section_patches"][0][
        "section_id"
    ] == "product_comparison"
    assert patch["patch_json"]["claim_revisions"][0]["original_claim_id"] == "C-001"
    assert impacts[0]["section_id"] == "product_comparison"

    validated = env.run(
        activities.validate_monitoring_report, job.job_id, decision
    )

    assert validated == decision
    report = repository.get_latest_report(job.job_id)
    assert report["version"] == 2
    assert report["status"] == "published"
    assert repository.list_change_events(job.job_id, "open") == []
    revision = repository.get_report_revision(report["report_version_id"])
    assert revision["status"] == "published"
    published_patch = repository.get_report_patch(patch["patch_id"])
    assert published_patch["approval_status"] == "published"
    assert published_patch["report_revision_id"] == revision["revision_id"]
    assert repository.list_claim_revisions(
        job.job_id, report["report_version_id"]
    )[0]["status"] == "active"
    resolved = repository.list_change_events(job.job_id)
    assert any(
        event["event_id"] == event_id and event["status"] == "applied"
        for event in resolved
    )
    assert any(event["event_type"] == "report_updated" for event in resolved)


def test_monitoring_auto_patch_professional_artifacts_target_published_v2(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=StaticClaimSynthesizer(),
            report_validator=PassingReportValidator(),
        ),
        pdf_enabled=False,
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    env.run(activities.build_report_ir, job.job_id)
    env.run(activities.render_latex, job.job_id)
    v1_artifacts = repository.list_report_artifacts(
        job.job_id, report_version_id=initial_report_id
    )
    assert any(
        item["artifact_path"].endswith("/professional/v1/report_ir.json")
        for item in v1_artifacts
    )

    repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="页面变化",
        old_value="old",
        new_value="new",
        evidence_ids=["E-001"],
    )
    decision = env.run(activities.update_monitoring_report, job.job_id)
    validated = env.run(activities.validate_monitoring_report, job.job_id, decision)
    assert validated.startswith("auto_patch")
    report = repository.get_latest_report(job.job_id)
    assert report["version"] == 2

    assert env.run(activities.build_report_ir, job.job_id) == report["report_version_id"]
    assert env.run(activities.render_latex, job.job_id) == "ready"
    assert env.run(activities.compile_pdf, job.job_id) == "unavailable"
    assert env.run(activities.render_excel, job.job_id) == "ready"
    assert env.run(activities.validate_artifacts, job.job_id) == "ready"

    v2_artifacts = repository.list_report_artifacts(
        job.job_id, report_version_id=report["report_version_id"]
    )
    v2_by_type = {item["artifact_type"]: item for item in v2_artifacts}
    assert v2_by_type["json"]["artifact_path"].endswith(
        "/professional/v2/report_ir.json"
    )
    assert v2_by_type["latex_source"]["artifact_path"].endswith(
        "/professional/v2/report.tex"
    )
    assert v2_by_type["xlsx"]["artifact_path"].endswith(
        "/professional/v2/report.xlsx"
    )
    assert v2_by_type["pdf"]["artifact_path"].endswith(
        "/professional/v2/report.pdf"
    )
    assert all(
        "/professional/v1/" in item["artifact_path"] for item in v1_artifacts
    )
    assert all(
        "/professional/v2/" in item["artifact_path"] for item in v2_artifacts
    )


def test_dynamic_price_change_end_to_end_is_incremental_and_idempotent(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, mode="safe")
    repository.save_research_plan(job.job_id, plan())
    old_quote = "该产品公开价格为 100 元。"
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/pricing",
        canonical_url="https://example.com/pricing",
        title="官方定价",
        publisher="示例公司",
        published_at="2026-06-01T00:00:00+00:00",
        search_query="示例公司 价格",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="price-100",
        raw_text=old_quote,
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品价格",
            value="100 元",
            exact_quote=old_quote,
            evidence_type="price",
            observed_at="2026-06-01T00:00:00+00:00",
            confidence_band="high",
        ),
    )
    repository.add_claim(
        job.job_id,
        ClaimItem(
            statement="示例公司的产品公开价格为 100 元。",
            claim_type="product_comparison",
            supporting_evidence_ids=["E-001"],
            confidence_band="high",
            reasoning_summary="官方定价页直接陈述。",
        ),
    )
    claim_synthesizer = EmptyClaimSynthesizer()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=claim_synthesizer,
            report_validator=PassingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    v1 = repository.get_latest_report(job.job_id)
    v1_path = Path(v1["report_path"])
    v1_hash = hashlib.sha256(v1_path.read_bytes()).hexdigest()

    run_id = repository.start_monitoring_run(job.job_id, "monitor-price-cycle")
    new_quote = "该产品公开价格已调整为 90 元。"
    snapshot_id = repository.add_monitoring_source_snapshot(
        job_id=job.job_id,
        source_id=source_id,
        run_id=run_id,
        url="https://example.com/pricing",
        content_hash="price-90",
        raw_text=new_quote,
        published_at="2026-06-18T00:00:00+00:00",
        retrieval_method="watch_target",
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品价格",
            value="90 元",
            exact_quote=new_quote,
            evidence_type="price",
            observed_at="2026-06-18T00:00:00+00:00",
            confidence_band="high",
        ),
        snapshot_id=snapshot_id,
    )

    assert env.run(activities.detect_monitoring_changes, job.job_id) == 1
    event = repository.list_evidence_backed_change_events(job.job_id, "open")[0]
    assert event["event_type"] == "price_change"
    assert json.loads(event["new_value_json"])["value"] == "90 元"

    decision = env.run(activities.update_monitoring_report, job.job_id)
    assert decision.startswith("auto_patch")
    assert repository.get_report_version(job.job_id, 2) is None
    retry_decision = env.run(activities.update_monitoring_report, job.job_id)
    assert retry_decision.startswith("auto_patch")
    assert "复用已生成 patch" in retry_decision
    assert len(repository.list_report_patches(job_id=job.job_id)) == 1
    assert claim_synthesizer.calls == 1
    assert repository.get_latest_monitoring_run(job.job_id)["llm_call_count"] == 1
    env.run(activities.validate_monitoring_report, job.job_id, decision)

    v2 = repository.get_latest_report(job.job_id)
    assert v2["version"] == 2
    assert v2["parent_report_version_id"] == v1["report_version_id"]
    assert hashlib.sha256(v1_path.read_bytes()).hexdigest() == v1_hash
    v2_markdown = Path(v2["report_path"]).read_text(encoding="utf-8")
    assert "90 元" in v2_markdown
    assert "## 风险\n- 暂无充分证据。" in v2_markdown
    impacts = repository.list_claim_impacts(job.job_id, [event["event_id"]])
    assert impacts[0]["claim_id"] == "C-001"
    assert impacts[0]["section_id"] == "product_comparison"
    active_claim = repository.list_active_claims(job.job_id)[0]
    assert "90 元" in active_claim.statement
    assert "100 元" not in active_claim.statement
    assert "E-002" in active_claim.supporting_evidence_ids

    repeated = env.run(activities.update_monitoring_report, job.job_id)
    assert repeated.startswith("no_change")
    assert repository.get_report_version(job.job_id, 3) is None


def test_major_target_customer_shift_requires_approval_before_v2(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, mode="safe")
    repository.save_research_plan(job.job_id, plan())
    old_quote = "该产品当前主要面向学生用户。"
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/positioning",
        canonical_url="https://example.com/positioning",
        title="初始定位",
        publisher="示例公司",
        published_at="2026-05-01T00:00:00+00:00",
        search_query="示例公司 目标客户",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="student-market",
        raw_text=old_quote,
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="目标客户",
            value="学生用户",
            exact_quote=old_quote,
            evidence_type="fact",
            observed_at="2026-05-01T00:00:00+00:00",
            confidence_band="high",
        ),
    )
    repository.add_claim(
        job.job_id,
        ClaimItem(
            statement="示例公司的目标客户主要是学生用户。",
            claim_type="market_position",
            supporting_evidence_ids=["E-001"],
            confidence_band="high",
            reasoning_summary="初始资料直接陈述。",
        ),
    )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=EmptyClaimSynthesizer(),
            report_validator=PassingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    run_id = repository.start_monitoring_run(
        job.job_id, "monitor-target-customer-cycle"
    )

    for index in range(2):
        quote = f"来源 {index + 1}：该公司已将目标客户转向企业市场。"
        new_source_id = repository.add_source(
            job_id=job.job_id,
            url=f"https://news.example.com/enterprise-{index}",
            canonical_url=f"https://news.example.com/enterprise-{index}",
            title=f"企业市场转型 {index + 1}",
            publisher=f"独立来源 {index + 1}",
            published_at="2026-06-18T00:00:00+00:00",
            search_query="示例公司 企业客户",
            search_rank=index + 1,
            http_status=200,
            content_type="text/html",
            content_hash=f"enterprise-{index}",
            raw_text=quote,
            status="fetched",
        )
        snapshot_id = repository.add_monitoring_source_snapshot(
            job_id=job.job_id,
            source_id=new_source_id,
            run_id=run_id,
            url=f"https://news.example.com/enterprise-{index}",
            content_hash=f"enterprise-{index}",
            raw_text=quote,
            published_at="2026-06-18T00:00:00+00:00",
            retrieval_method="incremental_fetch",
            status="fetched",
        )
        repository.add_evidence(
            job.job_id,
            new_source_id,
            EvidenceItem(
                entity="示例公司",
                attribute="目标客户",
                value="企业市场",
                exact_quote=quote,
                evidence_type="fact",
                observed_at="2026-06-18T00:00:00+00:00",
                confidence_band="high",
            ),
            snapshot_id=snapshot_id,
        )

    assert env.run(activities.detect_monitoring_changes, job.job_id) == 1
    event = repository.list_evidence_backed_change_events(job.job_id, "open")[0]
    assert event["event_type"] == "target_customer_shift"
    assert set(event["evidence_ids"]) == {"E-002", "E-003"}

    decision = env.run(activities.update_monitoring_report, job.job_id)
    validated = env.run(
        activities.validate_monitoring_report, job.job_id, decision
    )

    assert decision.startswith("review_required")
    assert validated == decision
    assert repository.get_report_version(job.job_id, 2) is None
    patch = repository.list_report_patches(
        job_id=job.job_id, approval_status="pending"
    )[0]
    assert patch["validation_status"] == "passed"
    assert patch["patch_json"]["impacted_claim_ids"] == ["C-001"]
    section_evidence_ids = patch["patch_json"]["section_patches"][0]["evidence_ids"]
    assert "E-002" in section_evidence_ids
    assert "E-003" in section_evidence_ids
    assert set(
        patch["patch_json"]["section_patches"][0]["new_content_blocks"][0][
            "evidence_ids"
        ]
    ) == {"E-002", "E-003"}
    assert "企业市场" in patch["patch_json"]["claim_revisions"][0]["statement"]

    handler = EventHandler(repository, SimpleNamespace(), RecordingMessenger())
    response = handler._update_approve(patch["patch_id"], job.creator_id)

    assert "报告更新已发布" in response
    v2 = repository.get_latest_report(job.job_id)
    assert v2["version"] == 2
    assert v2["parent_report_version_id"] == initial_report_id
    assert "企业市场" in repository.list_active_claims(job.job_id)[0].statement
    assert repository.list_change_events(job.job_id, "open") == []


def test_monitoring_extract_ignores_initial_research_backlog(repository, tmp_path):
    job = create_job(repository)
    add_monitoring_config(repository, job, mode="safe")
    repository.save_research_plan(job.job_id, plan())
    backlog_source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/backlog",
        canonical_url="https://example.com/backlog",
        title="首轮已搜索但未抓取来源",
        publisher="示例公司",
        published_at=None,
        search_query="首轮查询",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    run_id = repository.start_monitoring_run(
        job.job_id,
        "monitor-backlog-cycle",
    )
    current_source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/current",
        canonical_url="https://example.com/current",
        title="本轮新增来源",
        publisher="示例公司",
        published_at=None,
        search_query="本轮查询",
        search_rank=1,
        http_status=None,
        content_type=None,
        content_hash=None,
        raw_text="",
        status="searched",
    )
    repository.add_monitoring_source_snapshot(
        job_id=job.job_id,
        source_id=current_source_id,
        run_id=run_id,
        url="https://example.com/current",
        content_hash=None,
        retrieval_method="incremental_search",
        status="searched",
    )
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=PassingReportValidator()),
    )

    ActivityEnvironment().run(activities.extract_monitoring_evidence, job.job_id)

    sources = {
        source["source_id"]: source for source in repository.list_sources(job.job_id)
    }
    backlog = sources[backlog_source_id]
    current = sources[current_source_id]
    assert backlog["status"] == "searched"
    assert current["status"] == "fetched"


def test_observe_mode_records_impacts_without_creating_report_version(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, mode="observe")
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=StaticClaimSynthesizer(),
            report_validator=PassingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    event_id = repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="页面变化",
        evidence_ids=["E-001"],
    )

    decision = env.run(activities.update_monitoring_report, job.job_id)
    validated = env.run(
        activities.validate_monitoring_report, job.job_id, decision
    )

    assert decision.startswith("evidence_only")
    assert validated == decision
    assert repository.get_latest_report(job.job_id)["version"] == 1
    assert repository.get_latest_report(job.job_id, status=None)["version"] == 1
    assert repository.list_claim_impacts(job.job_id, [event_id])
    assert repository.list_change_events(job.job_id, "open") == []


def test_safe_high_impact_creates_review_required_draft_not_published(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, mode="safe")
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=StaticClaimSynthesizer(),
            report_validator=PassingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    event_id = repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="high",
        summary="高影响页面变化",
        evidence_ids=["E-001"],
    )

    decision = env.run(activities.update_monitoring_report, job.job_id)
    validated = env.run(
        activities.validate_monitoring_report, job.job_id, decision
    )

    latest_published = repository.get_latest_report(job.job_id)
    latest_any = repository.get_latest_report(job.job_id, status=None)
    assert decision.startswith("review_required")
    assert validated == decision
    assert latest_published["version"] == 1
    assert latest_any["version"] == 1
    assert repository.get_report_version(job.job_id, 2) is None
    review_patch = repository.list_report_patches(
        job_id=job.job_id, approval_status="pending"
    )[0]
    assert review_patch["approval_status"] == "pending"
    assert review_patch["report_version_id"] is None
    assert review_patch["patch_json"]["target_version"] == 2
    assert Path(review_patch["patch_json"]["report_path"]).exists()
    assert repository.list_change_events(job.job_id, "open")[0]["event_id"] == event_id


def test_monitoring_validate_failure_keeps_change_events_open(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            claim_synthesizer=StaticClaimSynthesizer(),
            report_validator=FailingReportValidator(),
        ),
    )
    env = ActivityEnvironment()
    initial_report_id = env.run(activities.generate_report, job.job_id)
    repository.publish_report_version(initial_report_id)
    event_id = repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="页面变化",
        old_value="old",
        new_value="new",
        evidence_ids=["E-001"],
    )
    decision = env.run(activities.update_monitoring_report, job.job_id)

    with pytest.raises(ReportValidationError):
        env.run(activities.validate_monitoring_report, job.job_id, decision)

    assert repository.list_change_events(job.job_id, "open")[0]["event_id"] == event_id
    latest_any = repository.get_latest_report(job.job_id, status=None)
    assert latest_any["version"] == 1
    assert latest_any["status"] == "published"
    assert repository.get_report_version(job.job_id, 2) is None


def test_monitoring_notification_levels_filter_decisions(repository, tmp_path):
    job = create_job(repository)
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )
    env = ActivityEnvironment()

    add_monitoring_config(repository, job, notify_level="high")
    assert (
        env.run(
            activities.notify_monitoring_cycle,
            job.job_id,
            "no_change: 当前报告 v1 无新增变化",
            None,
        )
        == "skipped"
    )
    assert (
        env.run(
            activities.notify_monitoring_cycle,
            job.job_id,
            "auto_patch: 生成报告 v2",
            None,
        )
        == "skipped"
    )
    assert (
        env.run(
            activities.notify_monitoring_cycle,
            job.job_id,
            "review_required: 生成待审批 patch，目标 v2",
            None,
        )
        == "sent"
    )

    repository.upsert_monitoring_schedule(
        job_id=job.job_id,
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        notify_level="all",
    )
    assert (
        env.run(
            activities.notify_monitoring_cycle,
            job.job_id,
            "no_change: 当前报告 v1 无新增变化",
            None,
        )
        == "sent"
    )
    assert (
        env.run(
            activities.notify_monitoring_cycle,
            job.job_id,
            "no_change: 当前报告 v1 无新增变化",
            None,
        )
        == "skipped_duplicate"
    )

    assert len(messenger.calls) == 2
    assert "发现重大变化，需要确认报告更新" in messenger.calls[0][1]
    assert "Patch ID：" in messenger.calls[0][1]
    assert "监测完成：未发现需要更新报告的重要变化" in messenger.calls[1][1]
    assert "下次监测时间：" in messenger.calls[1][1]


def test_monitoring_review_required_notification_includes_patch_details(
    repository, tmp_path
):
    job = create_job(repository)
    config = add_monitoring_config(repository, job, notify_level="high")
    repository.update_monitoring_next_run(job.job_id, "2026-06-19T01:00:00+00:00")
    base_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report-v1.md"),
        str(tmp_path / "report-v1.json"),
    )
    draft_id = repository.add_report_version(
        job.job_id,
        2,
        str(tmp_path / "report-v2.md"),
        str(tmp_path / "report-v2.json"),
        status="draft",
        parent_report_version_id=base_id,
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="business_model_change",
        severity="high",
        summary="重大变化",
    )
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=draft_id,
        base_report_version_id=base_id,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="需要审批",
        status="draft",
        patch_json={
            "revision_type": "partial",
            "base_report_version_id": base_id,
            "decision": "review_required",
            "impacted_section_ids": ["product_comparison"],
            "impacted_claim_ids": ["C-001"],
            "change_event_ids": [event_id],
            "section_patches": [
                {
                    "section_id": "product_comparison",
                    "operation": "append",
                    "new_content_blocks": [],
                    "revised_claim_ids": ["C-001"],
                    "evidence_ids": ["E-001"],
                    "change_reason": "需要审批",
                }
            ],
        },
    )
    patch = repository.get_report_patch_by_revision_id(revision_id)
    repository.replace_claim_impacts(
        job.job_id,
        [event_id],
        [
            {
                "event_id": event_id,
                "claim_id": "C-001",
                "section_id": "product_comparison",
                "impact_type": "contradicts",
                "severity": "high",
                "impact_level": "high",
                "proposed_confidence_band": "conflicting",
                "requires_review": True,
                "rationale": "证据冲突",
            }
        ],
    )
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    result = ActivityEnvironment().run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "review_required: 生成待审批 patch，目标 v2",
        None,
    )

    assert result == "sent"
    message = messenger.calls[0][1]
    assert config.chat_id == messenger.calls[0][0]
    assert "发现重大变化，需要确认报告更新" in message
    assert f"Patch ID：{patch['patch_id']}" in message
    assert "受到影响的 Claim：C-001" in message
    assert "证据：E-001" in message
    assert "影响等级：high" in message
    assert "置信度：conflicting" in message
    assert f"/update approve {patch['patch_id']}" in message


def test_monitoring_review_required_validation_failure_notification(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="high")
    base_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report-v1.md"),
        str(tmp_path / "report-v1.json"),
    )
    draft_id = repository.add_report_version(
        job.job_id,
        2,
        str(tmp_path / "report-v2.md"),
        str(tmp_path / "report-v2.json"),
        status="draft",
        parent_report_version_id=base_id,
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="business_model_change",
        severity="high",
        summary="重大变化",
    )
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=draft_id,
        base_report_version_id=base_id,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="需要审批",
        status="draft",
        patch_json={
            "revision_type": "partial",
            "base_report_version_id": base_id,
            "decision": "review_required",
            "impacted_section_ids": ["product_comparison"],
            "impacted_claim_ids": ["C-001"],
            "change_event_ids": [event_id],
            "claim_revisions": [{"statement": "建议结论"}],
        },
    )
    patch = repository.get_report_patch_by_revision_id(revision_id)
    repository.mark_report_patch_validation(patch["patch_id"], "failed")
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    result = ActivityEnvironment().run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "review_required: 生成待审批 patch，目标 v2；待审批 patch 校验未通过：核心事实未在同一行附带证据引用",
        None,
    )

    assert result == "sent"
    message = messenger.calls[0][1]
    assert "报告更新校验失败" in message
    assert f"Patch ID：{patch['patch_id']}" in message
    assert "核心事实未在同一行附带证据引用" in message
    assert "不会自动发布" in message
    assert "/monitor run" in message
    assert "/research" in message
    assert "/update approve" not in message


def test_monitoring_auto_patch_notification_includes_report_details(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="medium")
    repository.update_monitoring_next_run(job.job_id, "2026-06-19T01:00:00+00:00")
    base_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report-v1.md"),
        str(tmp_path / "report-v1.json"),
    )
    report_path = tmp_path / "report-v2.md"
    report_json_path = tmp_path / "report-v2.json"
    report_id = repository.add_report_version(
        job.job_id,
        2,
        str(report_path),
        str(report_json_path),
        status="published",
        parent_report_version_id=base_id,
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="new_evidence",
        severity="medium",
        summary="新增证据",
        status="applied",
    )
    repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=report_id,
        base_report_version_id=base_id,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="自动更新",
        status="published",
        patch_json={
            "revision_type": "partial",
            "base_report_version_id": base_id,
            "decision": "auto_patch",
            "impacted_section_ids": ["product_comparison"],
            "impacted_claim_ids": ["C-001"],
            "change_event_ids": [event_id],
            "section_patches": [
                {
                    "section_id": "product_comparison",
                    "operation": "append",
                    "new_content_blocks": [],
                    "revised_claim_ids": ["C-001"],
                    "evidence_ids": ["E-001"],
                    "change_reason": "自动更新",
                }
            ],
        },
    )
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    result = ActivityEnvironment().run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "auto_patch: 生成报告 v2",
        None,
    )

    assert result == "sent"
    message = messenger.calls[0][1]
    assert "动态报告已更新" in message
    assert "报告版本：v1 → v2" in message
    assert "影响的原结论：C-001" in message
    assert "修改章节：product_comparison" in message
    assert "新增证据：E-001" in message
    assert "下一次监测时间：2026-06-19 09:00:00 Asia/Shanghai" in message
    assert f"报告路径：{report_path}" in message


def test_monitoring_no_change_notification_uses_config_timezone(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="all")
    repository.update_monitoring_next_run(job.job_id, "2026-06-19T01:00:00+00:00")
    run_id = repository.start_monitoring_run(
        job.job_id,
        "monitor-timezone",
        cutoff_from="2026-06-19T00:00:00+00:00",
        cutoff_to="2026-06-19T00:30:00+00:00",
    )
    repository.complete_monitoring_run(
        run_id, job.job_id, decision="no_change: 当前报告 v1 无新增变化"
    )
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )

    result = ActivityEnvironment().run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "no_change: 当前报告 v1 无新增变化",
        None,
    )

    assert result == "sent"
    message = messenger.calls[0][1]
    assert (
        "监测区间：2026-06-19 08:00:00 Asia/Shanghai → "
        "2026-06-19 08:30:00 Asia/Shanghai"
    ) in message
    assert "下次监测时间：2026-06-19 09:00:00 Asia/Shanghai" in message


def test_monitoring_update_notification_delivers_versioned_files(
    repository, tmp_path
):
    class FileMessenger(RecordingMessenger):
        def send_file_to_chat(self, chat_id, path):
            self.calls.append((chat_id, str(path)))

    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="medium")
    report_id = repository.add_report_version(
        job.job_id,
        2,
        str(tmp_path / "report-v2.md"),
        str(tmp_path / "report-v2.json"),
        status="published",
    )
    pdf_path = tmp_path / "professional" / "v2" / "report.pdf"
    pdf_path.parent.mkdir(parents=True)
    pdf_path.write_bytes(b"%PDF-1.4\n")
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=report_id,
        artifact_type="pdf",
        artifact_path=str(pdf_path),
        content_hash="pdf-hash",
        status="ready",
    )
    run_id = repository.start_monitoring_run(job.job_id, "monitor-files")
    repository.update_monitoring_run_stats(
        run_id, result_report_version_id=report_id
    )
    repository.complete_monitoring_run(
        run_id, job.job_id, decision="auto_patch: 生成报告 v2"
    )
    messenger = FileMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path),
        messenger=messenger,
    )
    env = ActivityEnvironment()

    assert env.run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "auto_patch: 生成报告 v2",
        None,
    ) == "sent"
    assert env.run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "auto_patch: 生成报告 v2",
        None,
    ) == "skipped_duplicate"
    assert messenger.calls[-1] == (job.chat_id, str(pdf_path))
    assert sum(1 for call in messenger.calls if call == (job.chat_id, str(pdf_path))) == 1


def test_complete_monitoring_cycle_pauses_schedule_after_failure_threshold(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="high")
    scheduler = FakeMonitorScheduler()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            limits=SimpleNamespace(monitor_max_consecutive_failures=1),
        ),
        monitor_scheduler=scheduler,
    )
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")

    result = ActivityEnvironment().run(
        activities.complete_monitoring_cycle,
        job.job_id,
        run_id,
        "failed",
        "provider timeout",
    )

    config = repository.get_monitoring_config(job.job_id)
    assert result == job.job_id
    assert config.consecutive_failure_count == 1
    assert config.status == "paused"
    assert ("pause", config.schedule_id) in scheduler.calls


def test_notify_monitoring_cycle_reports_failure_threshold_pause(
    repository, tmp_path
):
    job = create_job(repository)
    add_monitoring_config(repository, job, notify_level="high")
    scheduler = FakeMonitorScheduler()
    messenger = RecordingMessenger()
    activities = ResearchActivities(
        repository=repository,
        backend=backend(
            tmp_path,
            limits=SimpleNamespace(monitor_max_consecutive_failures=1),
        ),
        messenger=messenger,
        monitor_scheduler=scheduler,
    )
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")
    ActivityEnvironment().run(
        activities.complete_monitoring_cycle,
        job.job_id,
        run_id,
        "failed",
        "provider timeout",
    )

    result = ActivityEnvironment().run(
        activities.notify_monitoring_cycle,
        job.job_id,
        "failed",
        "provider timeout",
    )

    assert result == "sent"
    assert messenger.calls[0][0] == job.chat_id
    assert "周期监测执行失败" in messenger.calls[0][1]
    assert "错误摘要：provider timeout" in messenger.calls[0][1]
    assert "是否已暂停：是" in messenger.calls[0][1]
    assert "连续失败：1/1" in messenger.calls[0][1]
    assert "Schedule 状态：paused" in messenger.calls[0][1]


def test_validate_report_maps_business_validation_error(
    repository, tmp_path
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    source_id = add_fetched_source(repository, job.job_id)
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    add_claim(repository, job.job_id)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, report_validator=FailingReportValidator()),
    )
    env = ActivityEnvironment()
    env.run(activities.generate_report, job.job_id)

    with pytest.raises(ReportValidationError):
        env.run(activities.validate_report, job.job_id)

    draft = repository.get_latest_report(job.job_id, status=None)
    assert draft["status"] == "draft"
    assert "报告包含不存在" in draft["validation_error"]


def test_planning_llm_error_is_classified_as_invalid_model_output(
    repository, tmp_path
):
    job = create_job(repository)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, planner=FailingPlanner()),
    )

    with pytest.raises(InvalidModelOutputError):
        ActivityEnvironment().run(activities.create_plan, job.job_id)


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (401, AuthenticationError),
        (403, AuthorizationError),
        (429, RateLimitError),
        (502, ProviderServerError),
    ],
)
def test_search_provider_http_errors_are_classified(
    repository, tmp_path, status_code, expected
):
    job = create_job(repository)
    repository.save_research_plan(job.job_id, plan())
    response = httpx.Response(
        status_code,
        request=httpx.Request("POST", "https://search.example/api"),
    )
    exc = httpx.HTTPStatusError("provider failed", request=response.request, response=response)
    activities = ResearchActivities(
        repository=repository,
        backend=backend(tmp_path, search_provider=FailingSearchProvider(exc)),
    )

    with pytest.raises(expected):
        ActivityEnvironment().run(activities.search_sources, job.job_id)
