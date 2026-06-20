from __future__ import annotations

import logging
from dataclasses import dataclass

from .base import AgentCancelled, AgentResult
from .claim_synthesizer import ClaimSynthesizer
from .evidence_extractor import EvidenceExtractor
from .planner import ResearchPlanner
from .report_generator import ReportGenerator
from .report_validator import ReportValidator
from ..acquisition import AssetDownloader
from ..artifacts import ArtifactStore
from ..datasets import DatasetProfiler
from ..llm.schemas import ExtractedPage
from ..models import Job
from ..parsers import ParserRegistry
from ..repository import Repository
from ..research.deduplication import (
    content_hash,
    deduplicate_search_results,
    normalized_text_hash,
)
from ..research.fetcher import WebFetcher
from ..research.parser import ContentExtractor
from ..research.search import SearchProvider
from ..research.url_safety import canonicalize_url

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResearchLimits:
    max_results_per_query: int = 5
    max_fetched_pages: int = 0
    monitor_max_search_queries: int = 4
    monitor_max_results_per_query: int = 5
    monitor_max_fetched_pages: int = 0
    monitor_max_watch_targets: int = 10
    monitor_max_llm_calls: int = 25
    monitor_max_new_events: int = 20
    monitor_max_auto_patch_sections: int = 3
    monitor_max_consecutive_failures: int = 3


class UnavailableResearchBackend:
    def __init__(self, reason: str):
        self.reason = reason

    def run(self, job, progress_callback, cancellation_check) -> AgentResult:
        raise RuntimeError(self.reason)


class ResearchAgentBackend:
    def __init__(
        self,
        *,
        repository: Repository,
        planner: ResearchPlanner,
        search_provider: SearchProvider,
        fetcher: WebFetcher,
        content_extractor: ContentExtractor,
        evidence_extractor: EvidenceExtractor,
        claim_synthesizer: ClaimSynthesizer,
        report_generator: ReportGenerator,
        report_validator: ReportValidator,
        artifact_store: ArtifactStore,
        limits: ResearchLimits,
        api_key: str = "",
        asset_downloader: AssetDownloader | None = None,
        parser_registry: ParserRegistry | None = None,
        dataset_profiler: DatasetProfiler | None = None,
    ):
        self.repository = repository
        self.planner = planner
        self.search_provider = search_provider
        self.fetcher = fetcher
        self.content_extractor = content_extractor
        self.evidence_extractor = evidence_extractor
        self.claim_synthesizer = claim_synthesizer
        self.report_generator = report_generator
        self.report_validator = report_validator
        self.artifact_store = artifact_store
        self.limits = limits
        self.api_key = api_key
        self.asset_downloader = asset_downloader
        self.parser_registry = parser_registry
        self.dataset_profiler = dataset_profiler or DatasetProfiler()

    def run(self, job, progress_callback, cancellation_check) -> AgentResult:
        def stage(name: str, progress: int) -> None:
            if cancellation_check():
                raise AgentCancelled()
            logger.info(
                "研究阶段 job_id=%s stage=%s progress=%s",
                job.job_id,
                name,
                progress,
            )
            progress_callback(name, progress)

        stage("planning", 5)
        plan = self.planner.create_plan(job.topic)
        self.repository.save_research_plan(job.job_id, plan)

        stage("searching", 15)
        search_results = []
        for query in plan.search_queries:
            if cancellation_check():
                raise AgentCancelled()
            try:
                search_results.extend(
                    self.search_provider.search(
                        query, self.limits.max_results_per_query
                    )
                )
            except Exception:
                logger.exception(
                    "搜索查询失败 job_id=%s query=%s", job.job_id, query
                )
        search_results = deduplicate_search_results(search_results)
        if not search_results:
            raise RuntimeError("搜索未返回任何有效结果")

        stage("fetching", 30)
        normalized_hashes: set[str] = set()
        fetched_count = 0
        for result in search_results:
            if (
                self.limits.max_fetched_pages > 0
                and fetched_count >= self.limits.max_fetched_pages
            ):
                break
            if cancellation_check():
                raise AgentCancelled()
            canonical = canonicalize_url(result.url)
            try:
                fetched = self.fetcher.fetch(canonical)
                page = self.content_extractor.extract(
                    fetched.content, fetched.final_url, fetched.content_type
                )
                if len(page.text) < 200:
                    raise ValueError("页面有效正文不足 200 字符")
                digest = content_hash(page.text)
                near_digest = normalized_text_hash(page.text)
                if self.repository.content_hash_exists(job.job_id, digest):
                    continue
                if near_digest in normalized_hashes:
                    continue
                normalized_hashes.add(near_digest)
                self.repository.add_source(
                    job_id=job.job_id,
                    url=result.url,
                    canonical_url=canonicalize_url(fetched.final_url),
                    title=page.title,
                    publisher=page.publisher,
                    published_at=page.published_at,
                    search_query=result.query,
                    search_rank=result.rank,
                    http_status=fetched.status_code,
                    content_type=fetched.content_type,
                    content_hash=digest,
                    raw_text=page.text,
                    status="fetched",
                )
                fetched_count += 1
            except Exception as exc:
                logger.warning(
                    "单页面抓取失败 job_id=%s url=%s error=%s",
                    job.job_id,
                    canonical,
                    exc,
                )
                self.repository.add_source(
                    job_id=job.job_id,
                    url=result.url,
                    canonical_url=canonical,
                    title=result.title,
                    publisher=None,
                    published_at=None,
                    search_query=result.query,
                    search_rank=result.rank,
                    http_status=None,
                    content_type=None,
                    content_hash=None,
                    raw_text="",
                    status="failed",
                    error_message=str(exc)[:1000],
                )
        sources = self.repository.list_sources(job.job_id, "fetched")
        if not sources:
            raise RuntimeError("没有成功抓取到可用网页")

        stage("extracting_evidence", 50)
        for source in sources:
            if cancellation_check():
                raise AgentCancelled()
            page = ExtractedPage(
                title=source["title"],
                text=source["raw_text"],
                publisher=source["publisher"],
                published_at=source["published_at"],
            )
            try:
                items = self.evidence_extractor.extract(job.topic, page)
                for item in items:
                    self.repository.add_evidence(
                        job.job_id, source["source_id"], item
                    )
            except Exception:
                logger.exception(
                    "来源证据提取失败 job_id=%s source_id=%s",
                    job.job_id,
                    source["source_id"],
                )
        evidence = self.repository.list_evidence(job.job_id)
        if not evidence:
            raise RuntimeError("有效来源中未提取到可验证原文证据")

        stage("synthesizing_claims", 70)
        options = self.repository.get_research_options(job.job_id)
        for item in self.claim_synthesizer.synthesize(
            evidence, language=options.get("language") or "zh"
        ):
            self.repository.add_claim(job.job_id, item)
        claims = self.repository.list_claims(job.job_id)

        stage("generating_report", 85)
        version = self.repository.next_report_version(job.job_id)
        markdown, report = self.report_generator.generate(
            topic=job.topic,
            plan=plan,
            sources=sources,
            evidence=evidence,
            claims=claims,
            language=options.get("language") or "zh",
        )
        report_path, json_path = self.artifact_store.write_report(
            job.job_id, version, markdown, report
        )

        stage("validating_report", 95)
        self.report_validator.validate(
            markdown=markdown,
            report_path=report_path,
            sources=sources,
            evidence=evidence,
            claims=claims,
            api_key=self.api_key,
        )
        self.repository.add_report_version(
            job.job_id, version, str(report_path), str(json_path)
        )
        stage("completed", 100)
        key_claims = tuple(
            claim.statement
            for claim in claims
            if claim.claim_type != "uncertainty"
        )[:3]
        summary = (
            f"来源数量：{len(sources)}\n证据数量：{len(evidence)}\n"
            f"关键结论数量：{len(claims)}\n报告版本：v{version}\n"
            f"报告路径：{report_path}"
        )
        return AgentResult(
            summary=summary,
            report_path=str(report_path),
            report_json_path=str(json_path),
            source_count=len(sources),
            evidence_count=len(evidence),
            claim_count=len(claims),
            report_version=version,
            key_claims=key_claims,
        )
