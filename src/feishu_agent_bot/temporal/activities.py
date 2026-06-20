from __future__ import annotations

import hashlib
import json
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import replace
from functools import wraps
import logging
from pathlib import Path
import re
from threading import BoundedSemaphore
from time import monotonic
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from temporalio import activity
from temporalio.exceptions import CancelledError as TemporalCancelledError

from ..agent.report_generator import SECTION_TITLES
from ..acquisition import DownloadedAsset, SourceAsset
from ..agent.research_agent import ResearchAgentBackend
from ..agent.report_validator import ReportValidationError as AgentReportValidationError
from ..analysis import AnalysisExecutor
from ..datasets import TabularDataset
from ..datasets.models import DatasetProfile
from ..llm.openai_compatible import LLMError
from ..llm.schemas import ExtractedPage
from ..monitoring import (
    ChangeDetector,
    ImpactAnalyzer,
    MonitoringPatchValidationError,
    MonitoringPatchValidator,
    ReportPatcher,
    UpdateDecider,
)
from ..repository import Repository, utc_now
from ..reporting import ProfessionalArtifactBuilder
from ..reporting.artifacts import _ir_to_dict
from ..reporting.artifact_validator import ArtifactValidator
from ..reporting.ir_builder import ReportIRBuilder
from ..reporting.models import BuiltArtifact
from ..research.deduplication import (
    content_hash,
    deduplicate_search_results,
    normalized_text_hash,
)
from ..research.url_safety import canonicalize_url
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    InvalidModelOutputError,
    ProviderServerError,
    RateLimitError,
    ReportValidationError,
    TransientNetworkError,
)
from .models import CompleteJobResult

logger = logging.getLogger(__name__)
SOURCE_PIPELINE_HEARTBEAT_SECONDS = 10

STAGE_BY_ACTIVITY = {
    "initialize_job_activity": "initialize",
    "project_workflow_status_activity": "projection",
    "create_plan_activity": "planning",
    "search_sources_activity": "searching",
    "fetch_sources_activity": "fetching",
    "discover_file_assets_activity": "discovering_assets",
    "download_assets_activity": "downloading_assets",
    "detect_asset_types_activity": "detecting_asset_types",
    "parse_assets_activity": "parsing_assets",
    "normalize_datasets_activity": "normalizing_datasets",
    "profile_datasets_activity": "profiling_datasets",
    "extract_evidence_activity": "extracting_evidence",
    "synthesize_claims_activity": "synthesizing_claims",
    "run_professional_analysis_activity": "analyzing",
    "select_analysis_skills_activity": "selecting_analysis",
    "execute_analysis_tools_activity": "executing_analysis_tools",
    "generate_business_analysis_activity": "generating_business_analysis",
    "generate_report_activity": "generating_report",
    "validate_report_activity": "validating_report",
    "generate_professional_artifacts_activity": "generating_artifacts",
    "build_report_ir_activity": "building_report_ir",
    "render_charts_activity": "rendering_charts",
    "render_latex_activity": "rendering_latex",
    "compile_pdf_activity": "compiling_pdf",
    "render_excel_activity": "rendering_excel",
    "validate_artifacts_activity": "validating_artifacts",
    "notify_validation_failed_activity": "validation_failed_notice",
    "reset_report_generation_activity": "reset_report_generation",
    "complete_job_activity": "completing",
    "notify_completion_activity": "notifying",
    "deliver_artifacts_activity": "notifying",
    "register_monitoring_schedule_activity": "monitor_registration",
    "mark_job_failed_activity": "failed_projection",
    "mark_job_cancelled_activity": "cancelled_projection",
    "start_monitoring_cycle_activity": "monitoring_start",
    "load_monitor_context_activity": "monitoring_context",
    "create_delta_plan_activity": "monitoring_delta_plan",
    "recheck_monitored_sources_activity": "monitoring_recheck_sources",
    "search_monitoring_sources_activity": "monitoring_search_sources",
    "extract_monitoring_evidence_activity": "monitoring_extract_evidence",
    "detect_monitoring_changes_activity": "monitoring_detect_changes",
    "run_incremental_analysis_activity": "monitoring_incremental_analysis",
    "update_monitoring_report_activity": "monitoring_update_report",
    "validate_monitoring_report_activity": "monitoring_validate_report",
    "complete_monitoring_cycle_activity": "monitoring_complete",
    "notify_monitoring_cycle_activity": "monitoring_notify",
}

PROGRESS_STAGE_LABELS = {
    "planning": "规划调研问题",
    "searching": "搜索资料",
    "fetching": "抓取网页/文件",
    "profiling_datasets": "分析数据集质量",
    "extracting_evidence": "提取证据",
    "synthesizing_claims": "综合结论",
    "analyzing": "执行专业分析",
    "generating_report": "生成报告",
    "validating_report": "校验报告",
    "generating_artifacts": "生成专业交付文件",
}


def _activity_log(activity_name: str):
    def decorate(func):
        @wraps(func)
        def wrapper(self, job_id: str, *args, **kwargs):
            info = activity.info()
            stage = STAGE_BY_ACTIVITY.get(activity_name, activity_name)
            start = monotonic()
            logger.info(
                "Temporal Activity started",
                extra={
                    "job_id": job_id,
                    "workflow_id": info.workflow_id,
                    "run_id": info.workflow_run_id,
                    "activity": activity_name,
                    "attempt": info.attempt,
                    "stage": stage,
                },
            )
            try:
                result = func(self, job_id, *args, **kwargs)
            except Exception as exc:
                logger.warning(
                    "Temporal Activity failed",
                    extra={
                        "job_id": job_id,
                        "workflow_id": info.workflow_id,
                        "run_id": info.workflow_run_id,
                        "activity": activity_name,
                        "attempt": info.attempt,
                        "stage": stage,
                        "duration_ms": int((monotonic() - start) * 1000),
                        "error_type": exc.__class__.__name__,
                        "result": "error",
                    },
                )
                raise
            logger.info(
                "Temporal Activity completed",
                extra={
                    "job_id": job_id,
                    "workflow_id": info.workflow_id,
                    "run_id": info.workflow_run_id,
                    "activity": activity_name,
                    "attempt": info.attempt,
                    "stage": stage,
                    "duration_ms": int((monotonic() - start) * 1000),
                    "result": "success",
                },
            )
            return result

        return wrapper

    return decorate


def _last_heartbeat_source_id() -> str | None:
    details = activity.info().heartbeat_details
    if not details:
        return None
    latest = details[-1]
    if isinstance(latest, dict):
        value = latest.get("last_source_id")
        return str(value) if value else None
    return None


def _after_heartbeat_checkpoint(source_id: str, checkpoint: str | None) -> bool:
    return checkpoint is None or source_id > checkpoint


def _record_source_heartbeat(
    repository: Repository, job_id: str, source_id: str
) -> None:
    activity.heartbeat({"last_source_id": source_id})
    repository.update_workflow_projection(job_id, heartbeat=True)


def _monitoring_heartbeat_checkpoint() -> tuple[str | None, int]:
    details = activity.info().heartbeat_details
    if not details:
        return None, -1
    latest = details[-1]
    if not isinstance(latest, dict):
        return None, -1
    phase = latest.get("phase")
    index = latest.get("index", -1)
    return (str(phase) if phase else None, int(index))


def _record_monitoring_heartbeat(
    repository: Repository,
    job_id: str,
    *,
    phase: str,
    item_id: str | None,
    index: int,
) -> None:
    activity.heartbeat(
        {"phase": phase, "item_id": item_id, "index": index}
    )
    repository.update_workflow_projection(job_id, heartbeat=True)


class ResearchActivities:
    def __init__(
        self,
        *,
        repository: Repository,
        backend: ResearchAgentBackend,
        messenger=None,
        monitor_scheduler=None,
        pdf_enabled: bool = True,
        latex_engine: str = "xelatex",
        latexmk_path: str = "latexmk",
        pdf_timeout_seconds: int = 180,
        pdf_max_output_bytes: int = 100_000_000,
        pdf_template: str = "business_report",
        max_analysis_concurrency: int = 4,
        artifact_concurrency: int = 2,
        max_charts_per_report: int = 12,
        max_tables_per_report: int = 30,
        max_concurrent_downloads: int = 4,
        max_concurrent_parsers: int = 3,
        max_concurrent_ocr_jobs: int = 1,
        max_concurrent_profile_jobs: int = 4,
        max_concurrent_charts: int = 2,
    ):
        if min(
            max_concurrent_downloads,
            max_concurrent_parsers,
            max_concurrent_ocr_jobs,
            max_concurrent_profile_jobs,
        ) < 1:
            raise ValueError("source pipeline concurrency limits must be positive")
        self.repository = repository
        self.backend = backend
        self.messenger = messenger
        self.monitor_scheduler = monitor_scheduler
        self.change_detector = ChangeDetector()
        self.impact_analyzer = ImpactAnalyzer()
        self.patch_validator = MonitoringPatchValidator()
        self.report_patcher = ReportPatcher()
        self.update_decider = UpdateDecider()
        self.analysis_executor = AnalysisExecutor(
            max_concurrency=max_analysis_concurrency
        )
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_concurrent_parsers = max_concurrent_parsers
        self.max_concurrent_profile_jobs = max_concurrent_profile_jobs
        self.download_semaphore = BoundedSemaphore(max_concurrent_downloads)
        self.parser_semaphore = BoundedSemaphore(max_concurrent_parsers)
        self.ocr_semaphore = BoundedSemaphore(max_concurrent_ocr_jobs)
        self.ir_builder = ReportIRBuilder(
            max_charts=max_charts_per_report,
            max_tables=max_tables_per_report,
        )
        self.professional_artifact_builder = ProfessionalArtifactBuilder(
            backend.artifact_store.root,
            pdf_enabled=pdf_enabled,
            latex_engine=latex_engine,
            latexmk_path=latexmk_path,
            pdf_timeout_seconds=pdf_timeout_seconds,
            pdf_max_output_bytes=pdf_max_output_bytes,
            pdf_template=pdf_template,
            max_concurrency=artifact_concurrency,
            max_chart_concurrency=max_concurrent_charts,
            max_charts=max_charts_per_report,
            max_tables=max_tables_per_report,
        )

    @_activity_log("initialize_job_activity")
    @activity.defn(name="initialize_job_activity")
    def initialize_job(self, job_id: str) -> str:
        info = activity.info()
        self.repository.bind_temporal_workflow(
            job_id, info.workflow_id, info.workflow_run_id
        )
        job = self.repository.get_job(job_id)
        if not job:
            raise ValueError(f"job not found: {job_id}")
        if job.status == "queued":
            self.repository.start_job(job_id)
        self.repository.update_workflow_projection(
            job_id, workflow_status="running", stage="initialize", progress=1
        )
        return job_id

    @_activity_log("project_workflow_status_activity")
    @activity.defn(name="project_workflow_status_activity")
    def project_workflow_status(
        self, job_id: str, stage: str, progress: int, paused: bool
    ) -> None:
        job = self.repository.get_job(job_id)
        previous_stage = job.stage if job else None
        self.repository.update_workflow_projection(
            job_id,
            workflow_status="paused" if paused else "running",
            paused=paused,
            stage=stage,
            progress=progress,
        )
        if (
            self.messenger
            and job
            and previous_stage != stage
            and stage in PROGRESS_STAGE_LABELS
        ):
            try:
                self.messenger.send_text_to_chat(
                    job.chat_id,
                    "调研进度\n"
                    f"任务 ID：{job_id}\n"
                    f"阶段：{PROGRESS_STAGE_LABELS[stage]}\n"
                    f"进度：{progress}%",
                )
            except Exception:
                logger.warning(
                    "阶段进度通知失败 job_id=%s stage=%s", job_id, stage,
                    exc_info=True,
                )

    @_activity_log("create_plan_activity")
    @activity.defn(name="create_plan_activity")
    def create_plan(self, job_id: str) -> str:
        if self.repository.get_research_plan(job_id):
            return job_id
        job = self._job(job_id)
        try:
            plan = self.backend.planner.create_plan(job.topic)
        except Exception as exc:
            raise _classify_external_error(exc) from exc
        self.repository.save_research_plan(job_id, plan)
        return job_id

    @_activity_log("search_sources_activity")
    @activity.defn(name="search_sources_activity")
    def search_sources(self, job_id: str) -> str:
        if self.repository.list_sources(job_id):
            return job_id
        plan = self.repository.get_research_plan(job_id)
        if not plan:
            return 0
        search_query_limit, result_limit, _fetch_limit = self._depth_limits(job_id)
        results = []
        for query in plan.search_queries[:search_query_limit]:
            try:
                results.extend(
                    self.backend.search_provider.search(
                        query, result_limit
                    )
                )
            except Exception as exc:
                raise _classify_external_error(exc) from exc
        for result in deduplicate_search_results(results):
            self.repository.add_source(
                job_id=job_id,
                url=result.url,
                canonical_url=canonicalize_url(result.url),
                title=result.title,
                publisher=None,
                published_at=None,
                search_query=result.query,
                search_rank=result.rank,
                http_status=None,
                content_type=None,
                content_hash=None,
                raw_text="",
                status="searched",
                error_message=None,
            )
        if not self.repository.list_sources(job_id):
            raise RuntimeError("搜索未返回任何有效结果")
        return job_id

    @_activity_log("fetch_sources_activity")
    @activity.defn(name="fetch_sources_activity")
    def fetch_sources(self, job_id: str) -> list[str]:
        normalized_hashes = {
            normalized_text_hash(source["raw_text"])
            for source in self.repository.list_sources(job_id, "fetched")
        }
        fetched = [
            source["source_id"]
            for source in self.repository.list_sources(job_id, "fetched")
        ]
        _query_limit, _result_limit, fetch_limit = self._depth_limits(job_id)
        checkpoint = _last_heartbeat_source_id()
        pending = [
            source
            for source in self.repository.list_sources(job_id, "searched")
            if _after_heartbeat_checkpoint(source["source_id"], checkpoint)
        ]
        cursor = 0
        while cursor < len(pending) and (
            fetch_limit <= 0 or len(fetched) < fetch_limit
        ):
            remaining = (
                len(pending) - cursor
                if fetch_limit <= 0
                else fetch_limit - len(fetched)
            )
            batch_size = min(
                self.max_concurrent_downloads,
                remaining,
                len(pending) - cursor,
            )
            batch = pending[cursor : cursor + batch_size]
            cursor += batch_size
            web_results = self._bounded_map(
                self._fetch_web_candidate,
                batch,
                self.max_concurrent_downloads,
                heartbeat=lambda: self._record_pipeline_heartbeat(
                    job_id, checkpoint, batch
                ),
            )
            failed_web = [
                result for result in web_results if result["status"] == "failed"
            ]
            downloaded = self._bounded_map(
                lambda result: self._download_asset_candidate(job_id, result),
                failed_web,
                self.max_concurrent_downloads,
                heartbeat=lambda: self._record_pipeline_heartbeat(
                    job_id, checkpoint, batch
                ),
            )
            parsed = self._bounded_map(
                self._parse_asset_candidate,
                downloaded,
                self.max_concurrent_parsers,
                heartbeat=lambda: self._record_pipeline_heartbeat(
                    job_id, checkpoint, batch
                ),
            )
            asset_by_source = {
                item["source"]["source_id"]: item for item in parsed
            }

            for result in web_results:
                source = result["source"]
                if result["status"] == "fetched":
                    accepted = self._commit_web_candidate(
                        job_id, result, normalized_hashes
                    )
                else:
                    accepted = self._commit_asset_candidate(
                        job_id,
                        asset_by_source[source["source_id"]],
                        normalized_hashes,
                    )
                if accepted:
                    fetched.append(source["source_id"])
                _record_source_heartbeat(
                    self.repository, job_id, source["source_id"]
                )
                checkpoint = source["source_id"]
        if not fetched:
            raise RuntimeError("没有成功抓取到可用网页或文件")
        return fetched

    @_activity_log("discover_file_assets_activity")
    @activity.defn(name="discover_file_assets_activity")
    def discover_file_assets(self, job_id: str) -> dict:
        normalized_hashes = {
            normalized_text_hash(source["raw_text"])
            for source in self.repository.list_sources(job_id, "fetched")
        }
        fetched = len(self.repository.list_sources(job_id, "fetched"))
        _query_limit, _result_limit, fetch_limit = self._depth_limits(job_id)
        sources = self.repository.list_sources(job_id, "searched")
        if fetch_limit > 0:
            sources = sources[: max(0, fetch_limit - fetched)]
        web_count = 0
        asset_source_ids: list[str] = []
        for cursor in range(0, len(sources), self.max_concurrent_downloads):
            batch = sources[cursor : cursor + self.max_concurrent_downloads]
            results = self._bounded_map(
                self._fetch_web_candidate,
                batch,
                self.max_concurrent_downloads,
                heartbeat=lambda: self._record_pipeline_heartbeat(job_id, None, batch),
            )
            for result in results:
                source = result["source"]
                if result["status"] == "fetched" and self._commit_web_candidate(
                    job_id, result, normalized_hashes
                ):
                    web_count += 1
                elif result["status"] == "failed":
                    asset_source_ids.append(source["source_id"])
                _record_source_heartbeat(self.repository, job_id, source["source_id"])
        return {"web_sources": web_count, "asset_source_ids": asset_source_ids}

    @_activity_log("download_assets_activity")
    @activity.defn(name="download_assets_activity")
    def download_assets(self, job_id: str) -> int:
        existing_source_ids = {
            item.get("source_id")
            for item in self.repository.list_source_assets(job_id)
            if item.get("source_id") and item.get("raw_object_path")
        }
        fetched_source_ids = {
            source["source_id"]
            for source in self.repository.list_sources(job_id, "fetched")
        }
        pending = [
            source
            for source in self.repository.list_sources(job_id, "searched")
            if source["source_id"] not in existing_source_ids
            and source["source_id"] not in fetched_source_ids
        ]
        _query_limit, _result_limit, fetch_limit = self._depth_limits(job_id)
        remaining = max(
            0, fetch_limit - len(self.repository.list_sources(job_id, "fetched"))
        ) if fetch_limit > 0 else len(pending)
        pending = pending[:remaining]
        candidates = [
            {
                "status": "failed",
                "source": source,
                "canonical": canonicalize_url(source["url"]),
                "page_error": "网页正文不可用，按文件下载",
            }
            for source in pending
        ]
        results = self._bounded_map(
            lambda item: self._download_asset_candidate(job_id, item),
            candidates,
            self.max_concurrent_downloads,
            heartbeat=lambda: self._record_pipeline_heartbeat(job_id, None, pending),
        )
        downloaded = 0
        for result in results:
            source_id = result["source"]["source_id"]
            if result.get("downloaded") is not None:
                self.repository.save_source_asset(result["downloaded"].asset)
                downloaded += 1
            else:
                self._mark_source_acquisition_failed(job_id, result)
            _record_source_heartbeat(self.repository, job_id, source_id)
        return downloaded + len(existing_source_ids)

    @_activity_log("detect_asset_types_activity")
    @activity.defn(name="detect_asset_types_activity")
    def detect_asset_types(self, job_id: str) -> int:
        detected = 0
        for item in self.repository.list_source_assets(job_id):
            asset = self._source_asset_from_row(item)
            if asset.file_type == "unknown":
                self.repository.update_source_asset_parse_status(
                    asset.asset_id,
                    parse_status="failed",
                    error_message="unsupported or unknown asset type",
                )
            else:
                detected += 1
            _record_source_heartbeat(
                self.repository, job_id, asset.source_id or asset.asset_id
            )
        return detected

    @_activity_log("parse_assets_activity")
    @activity.defn(name="parse_assets_activity")
    def parse_assets(self, job_id: str) -> int:
        source_by_id = {
            source["source_id"]: source
            for source in self.repository.list_sources(job_id)
        }
        pending = [
            self._source_asset_from_row(item)
            for item in self.repository.list_source_assets(job_id)
            if item.get("parse_status") == "downloaded"
        ]
        candidates = [
            {
                "status": "failed",
                "source": source_by_id[asset.source_id],
                "canonical": asset.canonical_url,
                "page_error": "网页正文不可用，按文件解析",
                "downloaded": DownloadedAsset(asset, {}, 200),
            }
            for asset in pending
            if asset.source_id in source_by_id
        ]
        parsed_results = self._bounded_map(
            self._parse_asset_candidate,
            candidates,
            self.max_concurrent_parsers,
            heartbeat=lambda: activity.heartbeat(
                {"in_progress_asset_ids": [asset.asset_id for asset in pending]}
            ),
        )
        normalized_hashes = {
            normalized_text_hash(source["raw_text"])
            for source in self.repository.list_sources(job_id, "fetched")
        }
        parsed_count = 0
        for result in parsed_results:
            if self._commit_asset_candidate(
                job_id, result, normalized_hashes, persist_datasets=False
            ):
                parsed_count += 1
            asset = result["downloaded"].asset
            _record_source_heartbeat(
                self.repository, job_id, asset.source_id or asset.asset_id
            )
        if not self.repository.list_sources(job_id, "fetched"):
            raise RuntimeError("没有成功抓取到可用网页或文件")
        return parsed_count

    @_activity_log("normalize_datasets_activity")
    @activity.defn(name="normalize_datasets_activity")
    def normalize_datasets(self, job_id: str) -> int:
        existing = {
            item["dataset_id"]
            for item in self.repository.list_tabular_datasets(job_id)
        }
        created = 0
        for table in self.repository.list_parsed_tables(job_id):
            dataset_id = f"{job_id}:{table['table_id']}"
            if dataset_id not in existing:
                asset_id = str(table["parsed_asset_id"]).split(":", 1)[0]
                dataset = TabularDataset(
                    dataset_id=dataset_id,
                    job_id=job_id,
                    asset_id=asset_id,
                    table_id=table["table_id"],
                    name=table.get("caption") or table["table_id"],
                    columns=table["columns"],
                    rows=table["rows"],
                    lineage={
                        "asset_id": asset_id,
                        "table_id": table["table_id"],
                        "source_locator": table.get("source_locator"),
                        "extraction_method": table.get("extraction_method"),
                        "table_metadata": table.get("metadata") or {},
                        "normalized_path": (table.get("metadata") or {}).get(
                            "normalized_path",
                            f"datasets/{job_id}/{table['table_id']}.json",
                        ),
                    },
                )
                self.repository.save_tabular_dataset(dataset)
                existing.add(dataset_id)
                created += 1
            activity.heartbeat({"table_id": table["table_id"]})
            self.repository.update_workflow_projection(job_id, heartbeat=True)
        return created

    @staticmethod
    def _bounded_map(
        function,
        items: list,
        max_workers: int,
        *,
        heartbeat=None,
    ) -> list:
        if not items:
            return []
        if heartbeat is None and (max_workers == 1 or len(items) == 1):
            return [function(item) for item in items]
        with ThreadPoolExecutor(
            max_workers=min(max_workers, len(items)),
            thread_name_prefix="source",
        ) as executor:
            futures = [executor.submit(function, item) for item in items]
            pending = set(futures)
            while pending:
                completed, pending = wait(
                    pending,
                    timeout=SOURCE_PIPELINE_HEARTBEAT_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                if not completed and heartbeat:
                    heartbeat()
            return [future.result() for future in futures]

    def _record_pipeline_heartbeat(
        self, job_id: str, checkpoint: str | None, batch: list[dict]
    ) -> None:
        activity.heartbeat(
            {
                "last_source_id": checkpoint,
                "in_progress_source_ids": [
                    source["source_id"] for source in batch
                ],
            }
        )
        self.repository.update_workflow_projection(job_id, heartbeat=True)

    def _fetch_web_candidate(self, source: dict) -> dict:
        canonical = canonicalize_url(source["url"])
        try:
            with self.download_semaphore:
                fetched_page = self.backend.fetcher.fetch(canonical)
            page = self.backend.content_extractor.extract(
                fetched_page.content,
                fetched_page.final_url,
                fetched_page.content_type,
            )
            if len(page.text) < 200:
                raise ValueError("页面有效正文不足 200 字符")
            return {
                "status": "fetched",
                "source": source,
                "canonical": canonical,
                "fetched_page": fetched_page,
                "page": page,
            }
        except TemporalCancelledError:
            raise
        except Exception as exc:
            return {
                "status": "failed",
                "source": source,
                "canonical": canonical,
                "page_error": exc,
            }

    def _download_asset_candidate(self, job_id: str, web_result: dict) -> dict:
        source = web_result["source"]
        downloader = getattr(self.backend, "asset_downloader", None)
        if downloader is None:
            return {
                **web_result,
                "asset_error": RuntimeError("未配置文件资产下载管线"),
            }
        try:
            with self.download_semaphore:
                downloaded = downloader.download(
                    job_id=job_id,
                    url=source["url"],
                    source_id=source["source_id"],
                    published_at=source.get("published_at"),
                )
            return {**web_result, "downloaded": downloaded}
        except TemporalCancelledError:
            raise
        except Exception as exc:
            return {**web_result, "asset_error": exc}

    def _parse_asset_candidate(self, candidate: dict) -> dict:
        if "downloaded" not in candidate:
            return candidate
        parser_registry = getattr(self.backend, "parser_registry", None)
        if parser_registry is None:
            return {
                **candidate,
                "asset_error": RuntimeError("未配置文件资产解析管线"),
            }
        downloaded = candidate["downloaded"]
        asset = downloaded.asset
        parser = parser_registry.parser_for_asset(asset)
        if parser is None:
            return {
                **candidate,
                "asset_error": RuntimeError(f"没有可用解析器: {asset.file_type}"),
            }
        try:
            with self.parser_semaphore:
                if asset.file_type == "pdf":
                    with self.ocr_semaphore:
                        parsed = parser_registry.parse_asset(asset)
                else:
                    parsed = parser_registry.parse_asset(asset)
        except Exception as exc:
            return {
                **candidate,
                "parser": parser,
                "asset_error": exc,
            }
        raw_text = self._parsed_asset_text(parsed)
        if not raw_text.strip():
            return {
                **candidate,
                "parser": parser,
                "asset_error": RuntimeError("文件解析后没有可用文本或表格"),
            }
        return {
            **candidate,
            "parser": parser,
            "parsed": parsed,
            "raw_text": raw_text,
        }

    def _commit_web_candidate(
        self, job_id: str, candidate: dict, normalized_hashes: set[str]
    ) -> bool:
        source = candidate["source"]
        page = candidate["page"]
        fetched_page = candidate["fetched_page"]
        digest = content_hash(page.text)
        near_digest = normalized_text_hash(page.text)
        duplicate_error = None
        if self.repository.content_hash_exists(job_id, digest):
            duplicate_error = "重复内容，已跳过"
        elif near_digest in normalized_hashes:
            duplicate_error = "近重复内容，已跳过"
        if duplicate_error:
            self.repository.update_source_result(
                job_id=job_id,
                source_id=source["source_id"],
                canonical_url=candidate["canonical"],
                title=source["title"],
                publisher=None,
                published_at=None,
                http_status=fetched_page.status_code,
                content_type=fetched_page.content_type,
                content_hash=None,
                raw_text="",
                status="failed",
                error_message=duplicate_error,
            )
            return False
        normalized_hashes.add(near_digest)
        self.repository.update_source_result(
            job_id=job_id,
            source_id=source["source_id"],
            canonical_url=canonicalize_url(fetched_page.final_url),
            title=page.title,
            publisher=page.publisher,
            published_at=page.published_at,
            http_status=fetched_page.status_code,
            content_type=fetched_page.content_type,
            content_hash=digest,
            raw_text=page.text,
            status="fetched",
        )
        return True

    def _commit_asset_candidate(
        self,
        job_id: str,
        candidate: dict,
        normalized_hashes: set[str],
        *,
        persist_datasets: bool = True,
    ) -> bool:
        source = candidate["source"]
        canonical = candidate["canonical"]
        downloaded = candidate.get("downloaded")
        if downloaded is None:
            self._mark_source_acquisition_failed(job_id, candidate)
            return False
        asset = downloaded.asset
        self.repository.save_source_asset(asset)
        parser = candidate.get("parser")
        if "parsed" not in candidate:
            error = candidate.get("asset_error") or "未知解析错误"
            self.repository.update_source_asset_parse_status(
                asset.asset_id,
                parse_status="failed",
                parser_name=getattr(parser, "name", None),
                parser_version=getattr(parser, "version", None),
                error_message=str(error),
            )
            self._mark_source_acquisition_failed(job_id, candidate)
            return False

        parsed = candidate["parsed"]
        raw_text = candidate["raw_text"]
        self.repository.save_parsed_asset(
            job_id, parsed, parser_name=parser.name
        )
        if persist_datasets:
            self._persist_parsed_tables(job_id, asset.asset_id, parsed.tables)
        digest = content_hash(raw_text)
        near_digest = normalized_text_hash(raw_text)
        duplicate_error = None
        if self.repository.content_hash_exists(job_id, digest):
            duplicate_error = "重复文件内容，已跳过"
        elif near_digest in normalized_hashes:
            duplicate_error = "近重复文件内容，已跳过"
        self.repository.update_source_asset_parse_status(
            asset.asset_id,
            parse_status="parsed",
            parser_name=parser.name,
            parser_version=getattr(parser, "version", None),
            error_message="; ".join(parsed.warnings)[:1000]
            if parsed.warnings
            else None,
        )
        if duplicate_error:
            self.repository.update_source_result(
                job_id=job_id,
                source_id=source["source_id"],
                canonical_url=asset.canonical_url,
                title=parsed.title or source["title"],
                publisher=None,
                published_at=asset.published_at,
                http_status=downloaded.http_status,
                content_type=asset.detected_mime_type or asset.declared_mime_type,
                content_hash=None,
                raw_text="",
                status="failed",
                error_message=duplicate_error,
            )
            return False

        normalized_hashes.add(near_digest)
        self.repository.update_source_result(
            job_id=job_id,
            source_id=source["source_id"],
            canonical_url=asset.canonical_url,
            title=parsed.title or source["title"],
            publisher=None,
            published_at=asset.published_at,
            http_status=downloaded.http_status,
            content_type=asset.detected_mime_type or asset.declared_mime_type,
            content_hash=digest,
            raw_text=raw_text,
            status="fetched",
        )
        return True

    @staticmethod
    def _source_asset_from_row(item: dict) -> SourceAsset:
        raw_path = item.get("raw_object_path") or item.get("local_path")
        return SourceAsset(
            asset_id=item["asset_id"],
            job_id=item["job_id"],
            source_id=item.get("source_id"),
            snapshot_id=item.get("snapshot_id"),
            original_url=item.get("original_url") or item.get("url") or "",
            canonical_url=item.get("canonical_url") or item.get("url") or "",
            generated_filename=item.get("generated_filename") or item.get("file_name") or "",
            original_filename=item.get("original_filename"),
            declared_mime_type=item.get("declared_mime_type") or item.get("content_type"),
            detected_mime_type=item.get("detected_mime_type"),
            file_extension=item.get("file_extension"),
            byte_size=int(item.get("byte_size") or item.get("size_bytes") or 0),
            sha256=item.get("sha256") or item.get("content_hash") or "",
            retrieved_at=item.get("retrieved_at") or utc_now(),
            raw_object_path=Path(raw_path) if raw_path else None,
            source_type=item.get("source_type") or item.get("file_type") or "download",
            published_at=item.get("published_at"),
            parse_status=item.get("parse_status") or item.get("status") or "downloaded",
            parser_name=item.get("parser_name"),
            parser_version=item.get("parser_version"),
            detection_confidence=item.get("detection_confidence"),
            detection_method=item.get("detection_method"),
            error_message=item.get("error_message"),
        )

    def _mark_source_acquisition_failed(self, job_id: str, candidate: dict) -> None:
        source = candidate["source"]
        page_error = candidate.get("page_error") or "未知网页抓取错误"
        asset_error = candidate.get("asset_error") or "未知文件解析错误"
        logger.warning(
            "Temporal 来源抓取失败 job_id=%s source_id=%s "
            "page_error=%s asset_error=%s",
            job_id,
            source["source_id"],
            page_error,
            asset_error,
        )
        self.repository.update_source_result(
            job_id=job_id,
            source_id=source["source_id"],
            canonical_url=candidate["canonical"],
            title=source["title"],
            publisher=None,
            published_at=None,
            http_status=None,
            content_type=None,
            content_hash=None,
            raw_text="",
            status="failed",
            error_message=(
                f"网页抓取失败：{page_error}; 文件解析失败：{asset_error}"
            )[:1000],
        )

    def _download_parse_monitoring_asset(
        self,
        *,
        job_id: str,
        source: dict,
        run_id: str | None,
        retrieval_method: str,
    ) -> bool:
        downloader = getattr(self.backend, "asset_downloader", None)
        parser_registry = getattr(self.backend, "parser_registry", None)
        if downloader is None or parser_registry is None:
            raise RuntimeError("未配置文件资产下载/解析管线")

        downloaded = downloader.download(
            job_id=job_id,
            url=source["url"],
            source_id=source["source_id"],
            published_at=source.get("published_at"),
        )
        asset = downloaded.asset
        previous_assets = self.repository.list_source_assets(job_id)
        unchanged = any(
            item.get("source_id") == source["source_id"]
            and item.get("sha256") == asset.sha256
            for item in previous_assets
        )
        if unchanged:
            self.repository.add_monitoring_source_snapshot(
                job_id=job_id,
                source_id=source["source_id"],
                run_id=run_id,
                url=asset.canonical_url,
                http_status=downloaded.http_status,
                content_type=asset.detected_mime_type
                or asset.declared_mime_type,
                content_hash=source.get("content_hash") or asset.sha256,
                raw_object_path=(
                    str(asset.raw_object_path) if asset.raw_object_path else None
                ),
                retrieval_method=retrieval_method,
                status="fetched",
            )
            return False

        self.repository.save_source_asset(asset)
        parser = parser_registry.parser_for_asset(asset)
        if parser is None:
            raise RuntimeError(f"没有可用解析器: {asset.file_type}")
        parsed = parser_registry.parse_asset(asset)
        self.repository.save_parsed_asset(job_id, parsed, parser_name=parser.name)
        self._persist_parsed_tables(job_id, asset.asset_id, parsed.tables)
        raw_text = self._parsed_asset_text(parsed)
        if not raw_text.strip():
            raise RuntimeError("文件解析后没有可用文本或表格")
        digest = content_hash(raw_text)
        snapshot_id = self.repository.add_monitoring_source_snapshot(
            job_id=job_id,
            source_id=source["source_id"],
            run_id=run_id,
            url=asset.canonical_url,
            http_status=downloaded.http_status,
            content_type=asset.detected_mime_type or asset.declared_mime_type,
            content_hash=digest,
            raw_text=raw_text,
            raw_object_path=(
                str(asset.raw_object_path) if asset.raw_object_path else None
            ),
            published_at=asset.published_at,
            retrieval_method=retrieval_method,
            status="fetched",
        )
        self.repository.save_source_asset(replace(asset, snapshot_id=snapshot_id))
        self.repository.update_source_asset_parse_status(
            asset.asset_id,
            parse_status="parsed",
            parser_name=parser.name,
            parser_version=getattr(parser, "version", None),
            error_message=(
                "; ".join(parsed.warnings)[:1000] if parsed.warnings else None
            ),
        )
        self.repository.update_source_result(
            job_id=job_id,
            source_id=source["source_id"],
            canonical_url=asset.canonical_url,
            title=parsed.title or source["title"],
            publisher=source.get("publisher"),
            published_at=asset.published_at,
            http_status=downloaded.http_status,
            content_type=asset.detected_mime_type or asset.declared_mime_type,
            content_hash=digest,
            raw_text=raw_text,
            status="fetched",
        )
        return True

    def _persist_parsed_tables(self, job_id: str, asset_id: str, tables) -> None:
        for table in tables:
            dataset = TabularDataset(
                dataset_id=f"{job_id}:{table.table_id}",
                job_id=job_id,
                asset_id=asset_id,
                table_id=table.table_id,
                name=table.caption or table.table_id,
                columns=table.columns,
                rows=table.rows,
                lineage={
                    "asset_id": asset_id,
                    "table_id": table.table_id,
                    "source_locator": table.source_locator,
                    "extraction_method": table.extraction_method,
                    "table_metadata": table.metadata,
                    "normalized_path": table.metadata.get(
                        "normalized_path",
                        f"datasets/{job_id}/{table.table_id}.json",
                    ),
                },
            )
            self.repository.save_tabular_dataset(dataset)

    @_activity_log("profile_datasets_activity")
    @activity.defn(name="profile_datasets_activity")
    def profile_datasets(self, job_id: str) -> int:
        profiler = getattr(self.backend, "dataset_profiler", None)
        if profiler is None:
            raise RuntimeError("未配置数据集 Profile 组件")
        profiler_version = getattr(profiler, "version", "1.0")
        pending = [
            item
            for item in self.repository.list_tabular_datasets(job_id)
            if not (item.get("profile") or {})
        ]
        if not pending:
            activity.heartbeat(
                {"phase": "dataset_profile", "completed": 0, "pending": []}
            )
            self.repository.update_workflow_projection(job_id, heartbeat=True)
            return 0

        def build_dataset(item: dict) -> TabularDataset:
            return TabularDataset(
                dataset_id=item["dataset_id"],
                job_id=job_id,
                asset_id=item["asset_id"],
                table_id=item["table_id"],
                name=item.get("dataset_name") or item.get("name") or item["table_id"],
                columns=item["columns"],
                rows=item["rows"],
                lineage=item.get("lineage") or {},
            )

        def create_profile(item: dict):
            dataset = build_dataset(item)
            dataset_hash = item.get("dataset_hash") or self.repository._tabular_dataset_hash(
                dataset.columns, dataset.rows
            )
            cached = self.repository.get_cached_dataset_profile(
                dataset.dataset_id, dataset_hash, profiler_version
            )
            return dataset, cached or profiler.profile(dataset), cached is not None

        completed = 0
        errors: list[BaseException] = []
        with ThreadPoolExecutor(
            max_workers=min(self.max_concurrent_profile_jobs, len(pending)),
            thread_name_prefix="dataset-profile",
        ) as executor:
            futures = {
                executor.submit(create_profile, item): item["dataset_id"]
                for item in pending
            }
            outstanding = set(futures)
            while outstanding:
                done, outstanding = wait(
                    outstanding,
                    timeout=SOURCE_PIPELINE_HEARTBEAT_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    activity.heartbeat(
                        {
                            "phase": "dataset_profile",
                            "completed": completed,
                            "pending": sorted(futures[item] for item in outstanding),
                        }
                    )
                    self.repository.update_workflow_projection(
                        job_id, heartbeat=True
                    )
                    continue
                for future in sorted(done, key=lambda item: futures[item]):
                    try:
                        dataset, profile, reused = future.result()
                    except BaseException as exc:
                        errors.append(exc)
                        continue
                    self.repository.save_dataset_profile(
                        dataset,
                        profile,
                        profiler_version=profiler_version,
                    )
                    completed += 1
                    activity.heartbeat(
                        {
                            "phase": "dataset_profile",
                            "completed": completed,
                            "dataset_id": dataset.dataset_id,
                            "reused": reused,
                            "pending": sorted(
                                futures[item] for item in outstanding
                            ),
                        }
                    )
                    self.repository.update_workflow_projection(
                        job_id, heartbeat=True
                    )
        if errors:
            raise errors[0]
        return completed

    @staticmethod
    def _parsed_asset_text(parsed) -> str:
        parts: list[str] = []
        if parsed.title:
            parts.append(f"标题：{parsed.title}")
        for block in parsed.text_blocks:
            text = block.text.strip()
            if not text:
                continue
            locator = f"（{block.source_locator}）" if block.source_locator else ""
            parts.append(f"{locator}\n{text}".strip())
        for table in parsed.tables:
            parts.append(
                "表格："
                f"{table.caption or table.table_id}，"
                f"{len(table.rows)} 行 x {len(table.columns)} 列，"
                f"字段：{', '.join(table.columns)}"
            )
            for index, row in enumerate(table.rows[:20], start=1):
                parts.append(
                    f"表格 {table.table_id} 第 {index} 行："
                    + json.dumps(row, ensure_ascii=False, default=str)
                )
        return "\n\n".join(parts).strip()

    @_activity_log("extract_evidence_activity")
    @activity.defn(name="extract_evidence_activity")
    def extract_evidence(self, job_id: str) -> str:
        job = self._job(job_id)
        checkpoint = _last_heartbeat_source_id()
        for source in self.repository.list_sources(job_id, "fetched"):
            if not _after_heartbeat_checkpoint(source["source_id"], checkpoint):
                continue
            if self.repository.source_has_evidence(job_id, source["source_id"]):
                _record_source_heartbeat(
                    self.repository, job_id, source["source_id"]
                )
                continue
            page = ExtractedPage(
                title=source["title"],
                text=source["raw_text"],
                publisher=source["publisher"],
                published_at=source["published_at"],
            )
            try:
                for item in self.backend.evidence_extractor.extract(
                    job.topic, page
                ):
                    self.repository.add_evidence(
                        job_id, source["source_id"], item
                    )
                _record_source_heartbeat(
                    self.repository, job_id, source["source_id"]
                )
            except TemporalCancelledError:
                raise
            except Exception as exc:
                classified = _classify_external_error(exc)
                if isinstance(classified, InvalidModelOutputError):
                    raise classified from exc
                logger.exception(
                    "Temporal 证据提取失败 job_id=%s source_id=%s",
                    job_id,
                    source["source_id"],
                )
                _record_source_heartbeat(
                    self.repository, job_id, source["source_id"]
                )
        if not self.repository.list_evidence(job_id):
            raise RuntimeError("有效来源中未提取到可验证原文证据")
        return job_id

    @_activity_log("synthesize_claims_activity")
    @activity.defn(name="synthesize_claims_activity")
    def synthesize_claims(self, job_id: str) -> str:
        if self.repository.list_claims(job_id):
            return job_id
        evidence = self.repository.list_evidence(job_id)
        options = self.repository.get_research_options(job_id)
        try:
            items = self.backend.claim_synthesizer.synthesize(
                evidence, language=options.get("language") or "zh"
            )
        except Exception as exc:
            raise _classify_external_error(exc) from exc
        for item in items:
            self.repository.add_claim(job_id, item)
        return job_id

    @_activity_log("run_professional_analysis_activity")
    @activity.defn(name="run_professional_analysis_activity")
    def run_professional_analysis(self, job_id: str) -> str:
        options = self.repository.get_research_options(job_id)
        selected_tools, selected_skills, include = self._analysis_depth_selection(options)
        self._run_professional_analysis_tasks(
            job_id,
            selected_tools=selected_tools,
            selected_skills=selected_skills,
            include=include,
            exclude=options.get("exclude") or None,
            reason="deterministic keyword and dataset rules",
        )
        return job_id

    @_activity_log("select_analysis_skills_activity")
    @activity.defn(name="select_analysis_skills_activity")
    def select_analysis_skills(self, job_id: str) -> dict:
        job = self._job(job_id)
        datasets, profiles = self._load_analysis_datasets(job_id)
        executor = getattr(self.backend, "analysis_executor", None) or self.analysis_executor
        options = self.repository.get_research_options(job_id)
        selected_tools, selected_skills, include = self._analysis_depth_selection(options)
        quality_reports = [
            self.backend.dataset_profiler.quality_report(profile)
            for profile in profiles
        ]
        plan = executor.selector.build_plan(
            topic=job.topic,
            datasets=datasets,
            profiles=profiles,
            quality_reports=quality_reports,
            evidence=self.repository.list_evidence(job_id),
            claims=self.repository.list_active_claims(job_id),
            include=include,
            exclude=options.get("exclude") or None,
        )
        if selected_tools is not None:
            plan = plan.model_copy(update={"selected_tools": selected_tools})
        if selected_skills is not None:
            plan = plan.model_copy(update={"selected_skills": selected_skills})
        return plan.model_dump(mode="json")

    @_activity_log("execute_analysis_tools_activity")
    @activity.defn(name="execute_analysis_tools_activity")
    def execute_analysis_tools(self, job_id: str, analysis_plan: dict) -> str:
        self._run_professional_analysis_tasks(
            job_id,
            selected_tools=list(analysis_plan.get("selected_tools") or []),
            selected_skills=[],
            reason="selected deterministic analysis tools",
        )
        return job_id

    @_activity_log("generate_business_analysis_activity")
    @activity.defn(name="generate_business_analysis_activity")
    def generate_business_analysis(self, job_id: str, analysis_plan: dict) -> str:
        self._run_professional_analysis_tasks(
            job_id,
            selected_tools=[],
            selected_skills=list(analysis_plan.get("selected_skills") or []),
            reason="selected business analysis skills",
        )
        return job_id

    def _run_professional_analysis_tasks(
        self,
        job_id: str,
        *,
        selected_tools: list[str] | None,
        selected_skills: list[str] | None,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        reason: str,
    ) -> None:
        job = self._job(job_id)
        datasets, profiles = self._load_analysis_datasets(job_id)
        executor = getattr(self.backend, "analysis_executor", None) or self.analysis_executor
        completed = 0

        def on_result(result) -> None:
            nonlocal completed
            self.repository.save_analysis_result(job_id, result)
            completed += 1
            activity.heartbeat(
                {
                    "phase": "analysis",
                    "completed_tasks": completed,
                    "task": result.skill_name or result.tool_name,
                    "result_id": result.result_id,
                }
            )
            self.repository.update_workflow_projection(job_id, heartbeat=True)

        activity.heartbeat({"phase": "analysis_planning", "completed_tasks": 0})
        started_run_id = None

        def on_run_started(run) -> None:
            nonlocal started_run_id
            started_run_id = run.run_id
            self.repository.start_analysis_run(run)

        try:
            run, results = executor.run(
                job_id=job_id,
                topic=job.topic,
                datasets=datasets,
                profiles=profiles,
                selected_tools=selected_tools,
                selected_skills=selected_skills,
                include=include,
                exclude=exclude,
                reason=reason,
                load_cached_result=lambda key: (
                    self.repository.get_analysis_result_by_idempotency_key(
                        job_id, key
                    )
                ),
                on_run_started=on_run_started,
                on_result=on_result,
            )
        except Exception:
            if started_run_id:
                self.repository.fail_analysis_run(started_run_id)
            raise
        self.repository.complete_analysis_run(run.run_id)
        logger.info(
            "专业分析完成 job_id=%s run_id=%s tools=%s results=%s",
            job_id,
            run.run_id,
            ",".join(run.selected_tools),
            len(results),
        )

    def _load_analysis_datasets(
        self, job_id: str
    ) -> tuple[list[TabularDataset], list[DatasetProfile]]:
        datasets: list[TabularDataset] = []
        profiles: list[DatasetProfile] = []
        for item in self.repository.list_tabular_datasets(job_id):
            dataset = TabularDataset(
                dataset_id=item["dataset_id"],
                job_id=job_id,
                asset_id=item["asset_id"],
                table_id=item["table_id"],
                name=item.get("dataset_name") or item.get("name") or item["table_id"],
                columns=item["columns"],
                rows=item["rows"],
                lineage=item.get("lineage") or {},
            )
            datasets.append(dataset)
            profile_payload = item.get("profile") or {}
            if profile_payload:
                profiles.append(DatasetProfile(**profile_payload))
            else:
                profiles.append(self.backend.dataset_profiler.profile(dataset))
        return datasets, profiles

    @_activity_log("generate_report_activity")
    @activity.defn(name="generate_report_activity")
    def generate_report(self, job_id: str) -> str:
        existing = self.repository.get_latest_report(job_id, status=None)
        if existing:
            return existing["report_version_id"]
        job = self._job(job_id)
        plan = self.repository.get_research_plan(job_id)
        if not plan:
            raise ValueError("missing research plan")
        sources = self.repository.list_sources(job_id, "fetched")
        evidence = self.repository.list_evidence(job_id)
        claims = self.repository.list_active_claims(job_id)
        options = self.repository.get_research_options(job_id)
        version = 1
        markdown, report = self.backend.report_generator.generate(
            topic=job.topic,
            plan=plan,
            sources=sources,
            evidence=evidence,
            claims=claims,
            language=options.get("language") or "zh",
        )
        report_path, json_path = self.backend.artifact_store.write_report(
            job_id, version, markdown, report
        )
        return self.repository.add_report_version(
            job_id, version, str(report_path), str(json_path), status="draft"
        )

    @_activity_log("validate_report_activity")
    @activity.defn(name="validate_report_activity")
    def validate_report(self, job_id: str) -> str:
        report = self.repository.get_latest_report(job_id, status=None)
        if not report:
            raise ValueError("missing report")
        markdown_path = Path(report["report_path"])
        try:
            self.backend.report_validator.validate(
                markdown=markdown_path.read_text(encoding="utf-8"),
                report_path=markdown_path,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claims=self.repository.list_claims(job_id),
                api_key=self.backend.api_key,
            )
        except AgentReportValidationError as exc:
            self.repository.mark_report_validation_failed(
                report["report_version_id"], str(exc)
            )
            raise ReportValidationError(str(exc)) from exc
        self.repository.publish_report_version(report["report_version_id"])
        return report["report_version_id"]

    @_activity_log("generate_professional_artifacts_activity")
    @activity.defn(name="generate_professional_artifacts_activity")
    def generate_professional_artifacts(self, job_id: str) -> str:
        self._generate_professional_artifacts_for_job(job_id)
        return job_id

    def _generate_professional_artifacts_for_job(self, job_id: str) -> None:
        report, deliverables, ir = self._professional_artifact_context(job_id)
        existing_artifacts = self.repository.list_report_artifacts(
            job_id,
            report_version_id=report["report_version_id"],
        )
        required_types = {"json", *deliverables}
        if self._ready_professional_artifacts_are_current(
            existing_artifacts, required_types, ir
        ):
            return

        def heartbeat(phase: str) -> None:
            activity.heartbeat(
                {
                    "phase": phase,
                    "report_version_id": report["report_version_id"],
                }
            )
            self.repository.update_workflow_projection(job_id, heartbeat=True)

        artifacts = self.professional_artifact_builder.build(
            ir,
            deliverables,
            heartbeat=heartbeat,
        )
        self._save_professional_artifacts(job_id, report, artifacts)

    def _ready_professional_artifacts_are_current(
        self,
        artifacts: list[dict],
        required_types: set[str],
        ir,
    ) -> bool:
        ready_by_type = {
            artifact["artifact_type"]: artifact
            for artifact in artifacts
            if artifact["status"] == "ready"
        }
        if not required_types.issubset(set(ready_by_type)):
            return False
        json_artifact = ready_by_type.get("json")
        if not json_artifact:
            return False
        report_ir_path = Path(json_artifact["artifact_path"])
        if not report_ir_path.is_file():
            return False
        validator = ArtifactValidator()
        try:
            validator.validate_report_ir_json(report_ir_path)
        except Exception:
            return False
        try:
            stored_ir = json.loads(report_ir_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return False
        if stored_ir != _ir_to_dict(ir):
            return False
        for artifact_type in required_types - {"json"}:
            artifact = ready_by_type.get(artifact_type)
            if not artifact or not Path(artifact["artifact_path"]).is_file():
                return False
        return True

    @_activity_log("build_report_ir_activity")
    @activity.defn(name="build_report_ir_activity")
    def build_report_ir(self, job_id: str) -> str:
        report, _deliverables, ir = self._professional_artifact_context(job_id)
        json_artifact, _same = self.professional_artifact_builder.write_report_ir(ir)
        artifacts = [json_artifact, *self.professional_artifact_builder.export_datasets(ir)]
        self._save_professional_artifacts(job_id, report, artifacts)
        return report["report_version_id"]

    @_activity_log("render_charts_activity")
    @activity.defn(name="render_charts_activity")
    def render_charts(self, job_id: str) -> int:
        report, _deliverables, ir = self._professional_artifact_context(job_id)

        def completed(chart_id: str) -> None:
            activity.heartbeat(
                {"chart_id": chart_id, "report_version_id": report["report_version_id"]}
            )
            self.repository.update_workflow_projection(job_id, heartbeat=True)

        artifacts = self.professional_artifact_builder.render_charts(
            ir, on_chart_completed=completed
        )
        self._save_professional_artifacts(job_id, report, artifacts)
        return len(artifacts) // 4

    @_activity_log("render_latex_activity")
    @activity.defn(name="render_latex_activity")
    def render_latex(self, job_id: str) -> str:
        report, deliverables, ir = self._professional_artifact_context(job_id)
        if "pdf" not in deliverables:
            return "skipped"
        self._save_professional_artifacts(
            job_id, report, self.professional_artifact_builder.render_latex(ir)
        )
        return "ready"

    @_activity_log("compile_pdf_activity")
    @activity.defn(name="compile_pdf_activity")
    def compile_pdf(self, job_id: str) -> str:
        report, deliverables, ir = self._professional_artifact_context(job_id)
        if "pdf" not in deliverables:
            return "skipped"
        heartbeat_details = {
            "phase": "compile_pdf",
            "report_version_id": report["report_version_id"],
        }
        activity.heartbeat(heartbeat_details)
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="pdf") as executor:
            future = executor.submit(
                self.professional_artifact_builder.compile_pdf, ir
            )
            while True:
                completed, _pending = wait(
                    [future], timeout=SOURCE_PIPELINE_HEARTBEAT_SECONDS
                )
                if completed:
                    artifact = future.result()
                    break
                activity.heartbeat(heartbeat_details)
                self.repository.update_workflow_projection(job_id, heartbeat=True)
        self._save_professional_artifacts(job_id, report, [artifact])
        return artifact.status

    @_activity_log("render_excel_activity")
    @activity.defn(name="render_excel_activity")
    def render_excel(self, job_id: str) -> str:
        report, deliverables, ir = self._professional_artifact_context(job_id)
        if "xlsx" not in deliverables:
            return "skipped"
        artifact = self.professional_artifact_builder.render_xlsx(ir)
        self._save_professional_artifacts(job_id, report, [artifact])
        return artifact.status

    @_activity_log("validate_artifacts_activity")
    @activity.defn(name="validate_artifacts_activity")
    def validate_artifacts(self, job_id: str) -> str:
        report, deliverables, ir = self._professional_artifact_context(job_id)
        rows = self.repository.list_report_artifacts(
            job_id, report_version_id=report["report_version_id"]
        )
        by_type = {row["artifact_type"]: row for row in rows}
        required_ready = {"json"}
        if "xlsx" in deliverables:
            required_ready.add("xlsx")
        missing = sorted(
            kind
            for kind in required_ready
            if kind not in by_type or by_type[kind]["status"] != "ready"
        )
        if missing:
            raise RuntimeError(
                "required professional artifacts are not ready: " + ", ".join(missing)
            )
        if "pdf" in deliverables and "pdf" not in by_type:
            raise RuntimeError("PDF artifact status was not recorded")
        built = [
            BuiltArtifact(
                row["artifact_type"],
                Path(row["artifact_path"]),
                row.get("content_hash") or "",
                status=row["status"],
                error_message=row.get("error_message"),
            )
            for row in rows
            if row["artifact_type"] not in {"manifest", "artifact_manifest"}
        ]
        manifests = self.professional_artifact_builder.write_manifest(ir, built)
        self._save_professional_artifacts(job_id, report, manifests)
        return "ready"

    def _professional_artifact_context(self, job_id: str):
        job = self._job(job_id)
        report = self.repository.get_latest_report(job_id, status="published")
        if not report:
            raise ValueError("missing published report")
        options = self.repository.get_research_options(job_id)
        deliverables = options.get("deliverables") or ["pdf", "xlsx"]
        deliverables = [
            item for item in deliverables if item in {"pdf", "xlsx", "json"}
        ] or ["pdf", "xlsx"]
        ir = self.ir_builder.from_report_json(
            job_id=job_id,
            topic=job.topic,
            report_json_path=report["report_json_path"],
            analysis_results=self.repository.list_latest_analysis_results(job_id),
            datasets=self.repository.list_tabular_datasets(job_id),
            sources=self.repository.list_sources(job_id, "fetched"),
            claims=self.repository.list_claims(job_id),
            evidence=self.repository.list_evidence(job_id),
            change_events=self.repository.list_change_events(job_id),
            analysis_runs=self.repository.list_analysis_runs(job_id),
            report_id=report["report_version_id"],
            version=int(report["version"]),
            parent_version_id=report.get("parent_report_version_id"),
        )
        return report, deliverables, ir

    def _save_professional_artifacts(
        self, job_id: str, report: dict, artifacts: list[BuiltArtifact]
    ) -> None:
        for item in artifacts:
            self.repository.add_report_artifact(
                job_id=job_id,
                report_version_id=report["report_version_id"],
                artifact_type=item.artifact_type,
                artifact_path=str(item.path),
                content_hash=item.content_hash or None,
                status=item.status,
                error_message=item.error_message,
            )

    @_activity_log("notify_validation_failed_activity")
    @activity.defn(name="notify_validation_failed_activity")
    def notify_validation_failed(
        self, job_id: str, error: str, auto_retry: bool, attempt: int
    ) -> str:
        if not self.messenger:
            return "skipped"
        job = self._job(job_id)
        if auto_retry:
            message = (
                "报告校验未通过，系统将自动回退并重新生成报告。\n\n"
                f"任务 ID：{job_id}\n主题：{job.topic}\n"
                f"第 {attempt} 次校验错误：{error}"
            )
        else:
            message = (
                "报告校验未通过，未自动重复研究。\n\n"
                f"任务 ID：{job_id}\n主题：{job.topic}\n"
                f"错误：{error}\n"
                f"可重新发送 /research {job.topic} 发起新的调研。"
            )
        try:
            self.messenger.send_text_to_chat(job.chat_id, message)
            return "sent"
        except Exception:
            logger.warning("报告校验失败通知发送失败 job_id=%s", job_id, exc_info=True)
            return "failed"

    @_activity_log("reset_report_generation_activity")
    @activity.defn(name="reset_report_generation_activity")
    def reset_report_generation(self, job_id: str) -> str:
        self.repository.clear_claims_and_reports(job_id)
        return job_id

    @_activity_log("complete_job_activity")
    @activity.defn(name="complete_job_activity")
    def complete_job(self, job_id: str) -> CompleteJobResult:
        report = self.repository.get_latest_report(job_id)
        sources = self.repository.list_sources(job_id, "fetched")
        evidence = self.repository.list_evidence(job_id)
        claims = self.repository.list_active_claims(job_id)
        summary = (
            f"来源数量：{len(sources)}\n证据数量：{len(evidence)}\n"
            f"关键结论数量：{len(claims)}\n报告版本：v{report['version']}\n"
            f"报告路径：{report['report_path']}"
        )
        self.repository.complete_job(job_id, summary)
        return CompleteJobResult(
            summary=summary,
            report_version_id=report["report_version_id"],
            report_path=report["report_path"],
        )

    @_activity_log("notify_completion_activity")
    @activity.defn(name="notify_completion_activity")
    def notify_completion(self, job_id: str) -> str:
        if not self.messenger:
            self.repository.update_workflow_projection(
                job_id, notification_status="skipped"
            )
            return "skipped"
        job = self._job(job_id)
        report = self.repository.get_latest_report(job_id)
        report_version_id = report["report_version_id"] if report else "no-report"
        message = self._completion_message(job_id, job.topic, report)
        try:
            text_sent = self._send_deduped_notification(
                job_id=job_id,
                monitor_run_id=None,
                notification_type="research_completion",
                dedup_key=f"research_completion:text:{job_id}:{report_version_id}",
                chat_id=job.chat_id,
                send=lambda: self.messenger.send_text_to_chat(job.chat_id, message),
            )
            file_sent = "skipped"
            if report and hasattr(self.messenger, "send_file_to_chat"):
                artifacts = self.repository.list_report_artifacts(
                    job_id,
                    report_version_id=report["report_version_id"],
                    ready_only=True,
                )
                preferred = [
                    artifact
                    for artifact in artifacts
                    if artifact["artifact_type"] in {"pdf", "xlsx"}
                ]
                if not preferred:
                    preferred = [
                        {
                            "artifact_id": f"markdown:{report_version_id}",
                            "artifact_type": "markdown",
                            "artifact_path": report["report_path"],
                        }
                    ]
                file_results = []
                for artifact in preferred:
                    if artifact["artifact_type"] == "markdown":
                        result = self._send_deduped_notification(
                            job_id=job_id,
                            monitor_run_id=None,
                            notification_type="research_completion_file",
                            dedup_key=(
                                "research_completion:file:"
                                f"{job_id}:{artifact['artifact_id']}"
                            ),
                            chat_id=job.chat_id,
                            send=lambda artifact=artifact: self.messenger.send_file_to_chat(
                                job.chat_id, artifact["artifact_path"]
                            ),
                        )
                    else:
                        result = self._send_artifact_deduped(
                            artifact=artifact,
                            job_id=job_id,
                            chat_id=job.chat_id,
                        )
                    file_results.append(result)
                file_sent = "sent" if "sent" in file_results else file_results[-1]
            self.repository.update_workflow_projection(
                job_id, notification_status="sent"
            )
            if text_sent == "skipped_duplicate" and file_sent in {
                "skipped",
                "skipped_duplicate",
            }:
                return "skipped_duplicate"
            return "sent"
        except Exception as exc:
            attempt = activity.info().attempt
            logger.warning(
                "完成通知发送失败 job_id=%s attempt=%s error=%s",
                job_id,
                attempt,
                exc,
            )
            if attempt < 3:
                raise
            self.repository.update_workflow_projection(
                job_id, notification_status="failed"
            )
            return f"failed: {str(exc)[:200]}"

    @_activity_log("deliver_artifacts_activity")
    @activity.defn(name="deliver_artifacts_activity")
    def deliver_artifacts(self, job_id: str) -> str:
        return self.notify_completion(job_id)

    def _completion_message(
        self, job_id: str, topic: str, report: dict | None
    ) -> str:
        sources = self.repository.list_sources(job_id, "fetched")
        assets = self.repository.list_source_assets(job_id)
        datasets = self.repository.list_tabular_datasets(job_id)
        evidence = self.repository.list_evidence(job_id)
        claims = self.repository.list_active_claims(job_id)
        runs = self.repository.list_analysis_runs(job_id)
        results = self.repository.list_latest_analysis_results(job_id)
        artifacts = (
            self.repository.list_report_artifacts(
                job_id,
                report_version_id=report["report_version_id"],
            )
            if report
            else []
        )
        options = self.repository.get_research_options(job_id)
        requested_deliverables = {
            str(item)
            for item in (options.get("deliverables") or ["pdf", "xlsx"])
            if item in {"pdf", "xlsx", "json"}
        }

        latest_run = runs[-1] if runs else {}
        methods = latest_run.get("selected_skills") or latest_run.get(
            "selected_tools", []
        )
        method_text = "、".join(str(item) for item in methods[:8]) or "基础证据综合"

        limitations = list(
            latest_run.get("analysis_plan", {}).get("limitations", [])
        )
        for result in results:
            limitations.extend(result.get("limitations") or [])
        limitation_text = "；".join(
            str(item)[:160] for item in dict.fromkeys(limitations) if item
        ) or "未发现需要额外说明的数据限制"

        conclusions = [claim.statement.strip() for claim in claims if claim.statement]
        conclusion_text = (
            "\n".join(f"- {item[:180]}" for item in conclusions[:3])
            or "- 暂无可提炼的主要结论"
        )

        artifact_labels = {
            "pdf": "PDF 报告",
            "xlsx": "Excel 数据与分析工作簿",
            "json": "JSON 报告数据",
        }
        artifacts_by_type = {}
        for artifact in artifacts:
            artifacts_by_type[artifact["artifact_type"]] = artifact
        delivery_lines = []
        for kind in ("pdf", "xlsx", "json"):
            artifact = artifacts_by_type.get(kind)
            if not artifact:
                if kind in requested_deliverables:
                    delivery_lines.append(
                        f"- {artifact_labels[kind]}（未生成：未记录产物状态）"
                    )
                continue
            if artifact["status"] != "ready":
                if kind in requested_deliverables:
                    delivery_lines.append(
                        f"- {artifact_labels[kind]}（"
                        f"{self._artifact_failure_summary(artifact)}）"
                    )
                continue
            issue = self._artifact_delivery_issue(artifact["artifact_path"])
            suffix = f"（未发送：{issue}）" if issue else ""
            delivery_lines.append(f"- {artifact_labels[kind]}{suffix}")
        delivery_text = "\n".join(delivery_lines) or "- Markdown 报告"

        version = f"v{report['version']}" if report else "未生成"
        return (
            "调研报告已完成\n\n"
            f"任务 ID：{job_id}\n"
            f"主题：{topic}\n"
            f"报告版本：{version}\n"
            f"来源数量：{len(sources)}\n"
            f"文件资料数量：{len(assets)}\n"
            f"数据集数量：{len(datasets)}\n"
            f"证据数量：{len(evidence)}\n"
            f"分析方法：{method_text}\n"
            f"主要结论：\n{conclusion_text}\n"
            f"数据限制：{limitation_text}\n\n"
            f"交付文件：\n{delivery_text}"
        )

    @staticmethod
    def _artifact_failure_summary(artifact: dict) -> str:
        status = str(artifact.get("status") or "failed")
        error = str(artifact.get("error_message") or "")
        if status == "unavailable":
            missing = re.search(r"\bmissing\s+([^\r\n]+)", error, re.IGNORECASE)
            if missing:
                return f"不可用：缺少系统依赖 {missing.group(1)[:120]}"
            if "disabled" in error.lower():
                return "不可用：该产物已被配置关闭"
            return "不可用：当前环境不支持生成"
        if "timed out" in error.lower():
            return "生成失败：编译超时"
        if "Missing $ inserted" in error:
            return "生成失败：LaTeX 引用或正文包含未转义特殊字符"
        if "font" in error.lower() and "not found" in error.lower():
            return "生成失败：缺少所需字体"
        return "生成失败：请查看任务日志后重试"

    def _artifact_delivery_issue(self, artifact_path: str | Path) -> str | None:
        path = Path(artifact_path)
        if not path.is_file():
            return "本地文件缺失"
        max_bytes = getattr(self.messenger, "max_file_bytes", None)
        if max_bytes and path.stat().st_size > max_bytes:
            return (
                f"文件大小 {path.stat().st_size} 字节超过飞书限制 "
                f"{max_bytes} 字节"
            )
        return None

    def _send_artifact_deduped(
        self,
        *,
        artifact: dict,
        job_id: str,
        chat_id: str,
    ) -> str:
        dedup_key = f"artifact:{chat_id}:{artifact['artifact_id']}"
        should_send, delivery_id = self.repository.try_start_artifact_delivery(
            artifact_id=artifact["artifact_id"],
            job_id=job_id,
            chat_id=chat_id,
            dedup_key=dedup_key,
        )
        if not should_send:
            return "skipped_duplicate"
        issue = self._artifact_delivery_issue(artifact["artifact_path"])
        if issue:
            self.repository.mark_artifact_delivery_failed(
                delivery_id, issue, retryable=False
            )
            logger.warning(
                "交付文件不可发送 job_id=%s artifact_id=%s reason=%s",
                job_id,
                artifact["artifact_id"],
                issue,
            )
            return "skipped_invalid"
        try:
            self.messenger.send_file_to_chat(chat_id, artifact["artifact_path"])
        except Exception as exc:
            self.repository.mark_artifact_delivery_failed(
                delivery_id, str(exc), retryable=True
            )
            raise
        self.repository.mark_artifact_delivery_sent(delivery_id)
        return "sent"

    def _send_deduped_notification(
        self,
        *,
        job_id: str,
        monitor_run_id: str | None,
        notification_type: str,
        dedup_key: str,
        chat_id: str,
        send,
    ) -> str:
        should_send, notification_id = self.repository.try_start_notification_delivery(
            job_id=job_id,
            monitor_run_id=monitor_run_id,
            notification_type=notification_type,
            dedup_key=dedup_key,
            chat_id=chat_id,
        )
        if not should_send:
            return "skipped_duplicate"
        try:
            send()
        except Exception as exc:
            self.repository.mark_notification_delivery_failed(
                notification_id, str(exc)
            )
            raise
        self.repository.mark_notification_delivery_sent(notification_id)
        return "sent"

    @_activity_log("register_monitoring_schedule_activity")
    @activity.defn(name="register_monitoring_schedule_activity")
    def register_monitoring_schedule(self, job_id: str) -> str:
        request = self.repository.get_monitor_registration_request(job_id)
        if not request:
            return "skipped"
        existing = self.repository.get_monitoring_config(job_id)
        if existing and existing.status != "deleted":
            self.repository.mark_monitor_registration(job_id, "registered")
            return "registered"
        if not self.monitor_scheduler:
            return self._mark_monitor_registration_failed(
                job_id, "Temporal monitor scheduler unavailable"
            )
        job = self._job(job_id)
        try:
            parsed = self.monitor_scheduler.parse(
                self._monitor_request_tokens(request)
            )
            schedule_id = self.monitor_scheduler.schedule_id(job_id)
            created_schedule = False
            try:
                self.monitor_scheduler.create(job_id, parsed)
                created_schedule = True
            except ValueError as exc:
                if "已经存在" not in str(exc):
                    raise
            try:
                self.repository.create_monitoring_config(
                    job_id=job_id,
                    creator_id=job.creator_id,
                    chat_id=job.chat_id,
                    schedule_id=schedule_id,
                    schedule_kind=parsed.kind,
                    schedule_value=parsed.value,
                    timezone=parsed.timezone,
                    mode=request["mode"],
                    notify_level=request["notify_level"],
                    catchup_window_seconds=(
                        int(
                            getattr(
                                getattr(
                                    self.monitor_scheduler, "settings", None
                                ),
                                "monitor_default_catchup_window_hours",
                                6,
                            )
                        )
                        * 3600
                    ),
                )
            except Exception:
                if created_schedule:
                    try:
                        self.monitor_scheduler.delete(schedule_id)
                    except Exception:
                        logger.exception(
                            "自动监测注册写入 SQLite 失败后清理 Schedule 失败 job_id=%s schedule_id=%s",
                            job_id,
                            schedule_id,
                        )
                raise
            self.repository.mark_monitor_registration(job_id, "registered")
            return "registered"
        except Exception as exc:
            error = str(exc)[:300]
            result = self._mark_monitor_registration_failed(job_id, error)
            if self.messenger:
                try:
                    self.messenger.send_text_to_chat(
                        job.chat_id,
                        "报告已完成，但周期监测注册失败。\n\n"
                        f"任务 ID：{job_id}\n主题：{job.topic}\n错误：{error}\n"
                        "报告已保留，可稍后使用 /monitor create 手动创建监测计划。",
                    )
                except Exception:
                    logger.warning(
                        "监测自动注册失败通知发送失败 job_id=%s",
                        job_id,
                        exc_info=True,
                    )
            return result

    def _mark_monitor_registration_failed(self, job_id: str, error: str) -> str:
        self.repository.mark_monitor_registration(
            job_id, "monitor_registration_failed", error
        )
        return f"failed: {error}"

    @staticmethod
    def _monitor_request_tokens(request: dict) -> list[str]:
        kind = request["schedule_kind"]
        value = request["schedule_value"]
        timezone = request["timezone"]
        if kind == "every":
            return ["every", value]
        if kind == "weekly":
            weekday, time_value = value.split(" ", 1)
            return ["weekly", weekday, time_value, timezone]
        return [kind, value, timezone]

    @_activity_log("mark_job_failed_activity")
    @activity.defn(name="mark_job_failed_activity")
    def mark_job_failed(self, job_id: str, error: str) -> None:
        self.repository.fail_job(job_id, error)

    @_activity_log("mark_job_cancelled_activity")
    @activity.defn(name="mark_job_cancelled_activity")
    def mark_job_cancelled(self, job_id: str) -> None:
        self.repository.mark_cancelled(job_id)

    @_activity_log("start_monitoring_cycle_activity")
    @activity.defn(name="start_monitoring_cycle_activity")
    def start_monitoring_cycle(self, monitor_id: str, workflow_id: str) -> dict:
        config = self.repository.get_monitoring_config_by_monitor_id(monitor_id)
        if not config:
            config = self.repository.get_monitoring_config(monitor_id)
        if not config:
            raise ValueError("monitoring config not found")
        job_id = config.job_id
        base_report = self.repository.get_latest_report(job_id)
        cutoff_to = utc_now()
        cutoff_from = config.last_success_at or self._initial_monitor_cutoff(
            base_report, cutoff_to
        )
        run_id = self.repository.start_monitoring_run(
            job_id,
            workflow_id,
            cutoff_from=cutoff_from,
            cutoff_to=cutoff_to,
            base_report_version_id=(
                base_report["report_version_id"] if base_report else None
            ),
        )
        return {"run_id": run_id, "job_id": job_id, "monitor_id": config.monitor_id}

    @_activity_log("load_monitor_context_activity")
    @activity.defn(name="load_monitor_context_activity")
    def load_monitor_context(self, job_id: str) -> dict:
        running = self.repository.get_running_monitoring_run(job_id)
        config = self.repository.get_monitoring_config(job_id)
        base_report = self.repository.get_latest_report(job_id)
        claims = self.repository.list_active_claims(job_id)
        evidence = self.repository.list_evidence(job_id)
        watch_targets = self._ensure_watch_targets(
            job_id, self._limit("monitor_max_watch_targets", 10)
        )
        entities = sorted(
            {
                item.entity
                for item in evidence
                if item.entity and len(item.entity) <= 80
            }
        )
        context = {
            "monitor_id": config.monitor_id if config else None,
            "job_id": job_id,
            "run_id": running["run_id"] if running else None,
            "base_report_version_id": (
                base_report["report_version_id"] if base_report else None
            ),
            "active_claim_ids": [claim.claim_id for claim in claims],
            "competitor_entity_ids": entities[:20],
            "watch_target_ids": [
                target["watch_target_id"] for target in watch_targets
            ],
            "last_successful_run_at": config.last_success_at if config else None,
            "cutoff_from": running.get("cutoff_from") if running else None,
            "cutoff_to": running.get("cutoff_to") if running else None,
            "mode": config.mode if config else "safe",
            "schedule_kind": config.schedule_kind if config else None,
            "schedule_value": config.schedule_value if config else None,
        }
        if running:
            self.repository.update_monitoring_run_plan(
                running["run_id"], context=context, stage="context_loaded"
            )
        return context

    @_activity_log("create_delta_plan_activity")
    @activity.defn(name="create_delta_plan_activity")
    def create_delta_plan(self, job_id: str, context: dict) -> dict:
        plan = self.repository.get_research_plan(job_id)
        watch_targets = self.repository.list_monitoring_watch_targets(job_id)
        max_queries = self._limit("monitor_max_search_queries", 4)
        max_pages = self._limit("monitor_max_fetched_pages", 0)
        max_results = self._limit("monitor_max_results_per_query", 5)
        if plan:
            base_queries = plan.search_queries
        else:
            base_queries = [self._job(job_id).topic]
        query_limit = min(max_queries, max(1, (len(base_queries) + 1) // 2))
        queries = []
        cutoff_hint = self._cutoff_query_hint(context.get("cutoff_from"))
        entities = context.get("competitor_entity_ids") or []
        watch_types = sorted({target["target_type"] for target in watch_targets})
        for query in base_queries[:query_limit]:
            parts = [query]
            if entities:
                parts.append(" OR ".join(entities[:3]))
            if watch_types:
                parts.append(" ".join(self._query_terms_for_watch_types(watch_types)))
            if cutoff_hint:
                parts.append(cutoff_hint)
            queries.append(" ".join(part for part in parts if part).strip())
        delta_plan = {
            "search_queries": list(dict.fromkeys(queries))[:max_queries],
            "watch_target_ids": [
                target["watch_target_id"] for target in watch_targets
            ][: self._limit("monitor_max_watch_targets", 10)],
            "target_event_types": self._target_event_types(watch_types),
            "cutoff_from": context.get("cutoff_from"),
            "cutoff_to": context.get("cutoff_to"),
            "max_search_requests": max_queries,
            "max_results_per_query": max_results,
            "max_pages": max_pages,
        }
        running = self.repository.get_running_monitoring_run(job_id)
        if running:
            self.repository.update_monitoring_run_plan(
                running["run_id"],
                delta_plan=delta_plan,
                stage="delta_plan_created",
                search_request_count=len(delta_plan["search_queries"]),
                fetched_page_count=len(delta_plan["watch_target_ids"]),
            )
        return delta_plan

    @_activity_log("recheck_monitored_sources_activity")
    @activity.defn(name="recheck_monitored_sources_activity")
    def recheck_monitored_sources(
        self, job_id: str, delta_plan: dict | None = None
    ) -> int:
        running = self.repository.get_running_monitoring_run(job_id)
        run_id = running["run_id"] if running else None
        max_watch_targets = self._limit("monitor_max_watch_targets", 10)
        targets = self._ensure_watch_targets(job_id, max_watch_targets)
        if delta_plan and delta_plan.get("watch_target_ids"):
            allowed = set(delta_plan["watch_target_ids"])
            targets = [
                target
                for target in targets
                if target.get("watch_target_id") in allowed
            ]
        checkpoint_phase, checkpoint_index = _monitoring_heartbeat_checkpoint()
        for index, target in enumerate(targets[:max_watch_targets]):
            if checkpoint_phase == "recheck" and index <= checkpoint_index:
                continue
            canonical = canonicalize_url(target["canonical_url"] or target["url"])
            try:
                fetched_page = self.backend.fetcher.fetch(canonical)
                page = self.backend.content_extractor.extract(
                    fetched_page.content,
                    fetched_page.final_url,
                    fetched_page.content_type,
                )
                if len(page.text) < 200:
                    raise ValueError("页面有效正文不足 200 字符")
                digest = content_hash(page.text)
                has_baseline = bool(target["content_hash"])
                content_changed = has_baseline and digest != target["content_hash"]
                self.repository.add_monitoring_source_snapshot(
                    job_id=job_id,
                    source_id=target["source_id"],
                    run_id=run_id,
                    url=fetched_page.final_url,
                    http_status=fetched_page.status_code,
                    content_type=fetched_page.content_type,
                    content_hash=digest,
                    raw_text=page.text if content_changed else None,
                    published_at=page.published_at,
                    retrieval_method=f"watch_target_refetch:{target['target_type']}",
                    status="fetched",
                )
                if not has_baseline or content_changed:
                    self.repository.update_source_result(
                        job_id=job_id,
                        source_id=target["source_id"],
                        canonical_url=canonicalize_url(fetched_page.final_url),
                        title=page.title,
                        publisher=page.publisher,
                        published_at=page.published_at,
                        http_status=fetched_page.status_code,
                        content_type=fetched_page.content_type,
                        content_hash=digest,
                        raw_text=page.text,
                        status="fetched",
                    )
            except TemporalCancelledError:
                raise
            except Exception as exc:
                try:
                    self._download_parse_monitoring_asset(
                        job_id=job_id,
                        source=target,
                        run_id=run_id,
                        retrieval_method=(
                            f"watch_target_refetch:{target['target_type']}:asset"
                        ),
                    )
                except TemporalCancelledError:
                    raise
                except Exception as asset_exc:
                    self.repository.add_monitoring_source_snapshot(
                        job_id=job_id,
                        source_id=target["source_id"],
                        run_id=run_id,
                        url=target["url"],
                        content_hash=None,
                        retrieval_method=(
                            f"watch_target_refetch:{target['target_type']}"
                        ),
                        status="failed",
                        error_message=(
                            f"网页抓取失败：{exc}; 文件解析失败：{asset_exc}"
                        )[:1000],
                    )
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="recheck",
                item_id=target["source_id"],
                index=index,
            )
        run_snapshots = (
            self.repository.list_source_snapshots(job_id, run_id=run_id)
            if run_id
            else []
        )
        recheck_snapshots = [
            snapshot
            for snapshot in run_snapshots
            if str(snapshot.get("retrieval_method") or "").startswith(
                "watch_target_refetch:"
            )
        ]
        changed = sum(
            1
            for snapshot in recheck_snapshots
            if snapshot.get("status") == "fetched" and snapshot.get("raw_text")
        )
        checked = sum(
            1 for snapshot in recheck_snapshots if snapshot.get("status") == "fetched"
        )
        if running:
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="recheck_sources",
                changed_source_count=changed,
                fetched_page_count=checked,
            )
        return changed

    def _ensure_watch_targets(
        self, job_id: str, max_watch_targets: int
    ) -> list[dict]:
        existing = self.repository.list_monitoring_watch_targets(job_id)
        if existing:
            return existing
        created = 0
        for source in self.repository.list_sources(job_id, "fetched"):
            if created >= max_watch_targets:
                break
            target_type = self._classify_watch_target(source)
            if not target_type:
                continue
            self.repository.upsert_monitoring_watch_target(
                job_id=job_id,
                source_id=source["source_id"],
                target_type=target_type,
                url=source["url"],
                canonical_url=source["canonical_url"],
            )
            created += 1
        return self.repository.list_monitoring_watch_targets(job_id)

    @staticmethod
    def _classify_watch_target(source: dict) -> str | None:
        text = f"{source.get('url', '')} {source.get('title', '')}".lower()
        path = urlparse(source.get("canonical_url") or source.get("url") or "").path
        if "pricing" in text or "price" in text or "定价" in text or "价格" in text:
            return "pricing_page"
        if "release" in text or "changelog" in text or "更新日志" in text:
            return "release_notes"
        if "news" in text or "blog" in text or "新闻" in text:
            return "official_news"
        if "product" in text or "产品" in text:
            return "product_page"
        if "app store" in text or "apps.apple.com" in text:
            return "app_store_page"
        if "review" in text or "评价" in text:
            return "public_review_page"
        if path in {"", "/"}:
            return "official_homepage"
        return None

    @_activity_log("search_monitoring_sources_activity")
    @activity.defn(name="search_monitoring_sources_activity")
    def search_monitoring_sources(
        self, job_id: str, delta_plan: dict | None = None
    ) -> int:
        plan = self.repository.get_research_plan(job_id)
        if not plan and not delta_plan:
            return 0
        running = self.repository.get_running_monitoring_run(job_id)
        run_id = running["run_id"] if running else None
        queries = (
            list(delta_plan.get("search_queries") or [])
            if delta_plan
            else plan.search_queries
        )
        max_queries = int(
            delta_plan.get("max_search_requests")
            if delta_plan and delta_plan.get("max_search_requests")
            else self._limit("monitor_max_search_queries", 4)
        )
        max_results = int(
            delta_plan.get("max_results_per_query")
            if delta_plan and delta_plan.get("max_results_per_query")
            else self._limit("monitor_max_results_per_query", 5)
        )
        existing = {
            source["canonical_url"] for source in self.repository.list_sources(job_id)
        }
        added = 0
        search_requests = 0
        checkpoint_phase, checkpoint_index = _monitoring_heartbeat_checkpoint()
        for index, query in enumerate(queries[:max_queries]):
            if checkpoint_phase == "search" and index <= checkpoint_index:
                continue
            try:
                search_requests += 1
                results = self.backend.search_provider.search(query, max_results)
            except Exception as exc:
                raise _classify_external_error(exc) from exc
            for result in deduplicate_search_results(results):
                max_added = self._limit("monitor_max_fetched_pages", 0)
                if max_added > 0 and added >= max_added:
                    break
                canonical = canonicalize_url(result.url)
                if canonical in existing:
                    continue
                source_id = self.repository.add_source(
                    job_id=job_id,
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
                    status="searched",
                    error_message=None,
                )
                if run_id:
                    self.repository.add_monitoring_source_snapshot(
                        job_id=job_id,
                        source_id=source_id,
                        run_id=run_id,
                        url=result.url,
                        content_hash=None,
                        retrieval_method="incremental_search",
                        status="searched",
                    )
                existing.add(canonical)
                added += 1
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="search",
                item_id=query,
                index=index,
            )
        if running:
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="search_sources",
                new_source_count=added,
                search_request_count=(
                    int(running.get("search_request_count") or 0)
                    + search_requests
                ),
            )
        return added

    @_activity_log("extract_monitoring_evidence_activity")
    @activity.defn(name="extract_monitoring_evidence_activity")
    def extract_monitoring_evidence(self, job_id: str) -> int:
        job = self._job(job_id)
        extracted = 0
        running = self.repository.get_running_monitoring_run(job_id)
        run_id = running["run_id"] if running else None
        max_fetch = self._limit("monitor_max_fetched_pages", 0)
        max_llm_calls = self._limit("monitor_max_llm_calls", 25)
        checkpoint_phase, checkpoint_index = _monitoring_heartbeat_checkpoint()
        existing_run_snapshots = (
            self.repository.list_source_snapshots(job_id, run_id=run_id)
            if run_id
            else []
        )
        current_search_source_ids = {
            snapshot["source_id"]
            for snapshot in existing_run_snapshots
            if snapshot.get("retrieval_method") == "incremental_search"
            and snapshot.get("status") == "searched"
        }
        fetched_this_run = sum(
            1
            for snapshot in existing_run_snapshots
            if snapshot.get("retrieval_method") == "incremental_fetch"
            and snapshot.get("status") == "fetched"
        )
        for index, source in enumerate(self.repository.list_sources(job_id)):
            if source["status"] != "searched":
                continue
            if run_id and source["source_id"] not in current_search_source_ids:
                continue
            if checkpoint_phase == "evidence":
                continue
            if checkpoint_phase == "fetch" and index <= checkpoint_index:
                continue
            if max_fetch > 0 and fetched_this_run >= max_fetch:
                break
            canonical = canonicalize_url(source["url"])
            try:
                fetched_page = self.backend.fetcher.fetch(canonical)
                page = self.backend.content_extractor.extract(
                    fetched_page.content,
                    fetched_page.final_url,
                    fetched_page.content_type,
                )
                if len(page.text) < 200:
                    raise ValueError("页面有效正文不足 200 字符")
                digest = content_hash(page.text)
                self.repository.update_source_result(
                    job_id=job_id,
                    source_id=source["source_id"],
                    canonical_url=canonicalize_url(fetched_page.final_url),
                    title=page.title,
                    publisher=page.publisher,
                    published_at=page.published_at,
                    http_status=fetched_page.status_code,
                    content_type=fetched_page.content_type,
                    content_hash=digest,
                    raw_text=page.text,
                    status="fetched",
                )
                self.repository.add_monitoring_source_snapshot(
                    job_id=job_id,
                    source_id=source["source_id"],
                    run_id=run_id,
                    url=fetched_page.final_url,
                    http_status=fetched_page.status_code,
                    content_type=fetched_page.content_type,
                    content_hash=digest,
                    raw_text=page.text,
                    published_at=page.published_at,
                    retrieval_method="incremental_fetch",
                    status="fetched",
                )
                fetched_this_run += 1
            except TemporalCancelledError:
                raise
            except Exception as exc:
                try:
                    changed = self._download_parse_monitoring_asset(
                        job_id=job_id,
                        source=source,
                        run_id=run_id,
                        retrieval_method="incremental_fetch:asset",
                    )
                    if changed:
                        fetched_this_run += 1
                except TemporalCancelledError:
                    raise
                except Exception as asset_exc:
                    self.repository.update_source_result(
                        job_id=job_id,
                        source_id=source["source_id"],
                        canonical_url=canonical,
                        title=source["title"],
                        publisher=None,
                        published_at=None,
                        http_status=None,
                        content_type=None,
                        content_hash=None,
                        raw_text="",
                        status="failed",
                        error_message=(
                            f"网页抓取失败：{exc}; 文件解析失败：{asset_exc}"
                        )[:1000],
                    )
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="fetch",
                item_id=source["source_id"],
                index=index,
            )
        llm_calls = int(running.get("llm_call_count") or 0) if running else 0
        snapshots_by_source = {}
        if run_id:
            for snapshot in self.repository.list_source_snapshots(
                job_id, run_id=run_id, status="fetched"
            ):
                if snapshot.get("raw_text"):
                    snapshots_by_source[snapshot["source_id"]] = snapshot
        sources_by_id = {
            source["source_id"]: source
            for source in self.repository.list_sources(job_id, "fetched")
        }
        candidates = [
            (sources_by_id[source_id], snapshot)
            for source_id, snapshot in sorted(snapshots_by_source.items())
            if source_id in sources_by_id
        ]
        for index, (source, snapshot) in enumerate(candidates):
            if checkpoint_phase == "evidence" and index <= checkpoint_index:
                continue
            if llm_calls >= max_llm_calls:
                break
            if self.repository.snapshot_has_evidence(
                job_id, snapshot["snapshot_id"]
            ):
                _record_monitoring_heartbeat(
                    self.repository,
                    job_id,
                    phase="evidence",
                    item_id=snapshot["snapshot_id"],
                    index=index,
                )
                continue
            page = ExtractedPage(
                title=source["title"],
                text=snapshot["raw_text"],
                publisher=source["publisher"],
                published_at=(
                    snapshot.get("published_at") or source["published_at"]
                ),
            )
            try:
                llm_calls += 1
                if running:
                    self.repository.update_monitoring_run_stats(
                        running["run_id"], llm_call_count=llm_calls
                    )
                for item in self.backend.evidence_extractor.extract(job.topic, page):
                    stored = self.repository.add_evidence(
                        job_id,
                        source["source_id"],
                        item,
                        snapshot_id=(
                            snapshot["snapshot_id"] if snapshot else None
                        ),
                    )
                    if stored:
                        extracted += 1
                _record_monitoring_heartbeat(
                    self.repository,
                    job_id,
                    phase="evidence",
                    item_id=snapshot["snapshot_id"],
                    index=index,
                )
            except TemporalCancelledError:
                raise
            except Exception as exc:
                classified = _classify_external_error(exc)
                if isinstance(classified, InvalidModelOutputError):
                    raise classified from exc
                logger.warning(
                    "监测证据提取失败 job_id=%s source_id=%s error=%s",
                    job_id,
                    source["source_id"],
                    exc,
                )
                _record_monitoring_heartbeat(
                    self.repository,
                    job_id,
                    phase="evidence",
                    item_id=snapshot["snapshot_id"],
                    index=index,
                )
        if running:
            current_snapshots = self.repository.list_source_snapshots(
                job_id, run_id=running["run_id"]
            )
            fetched_page_count = sum(
                1
                for snapshot in current_snapshots
                if snapshot.get("status") == "fetched"
            )
            run_evidence_count = self.repository.count_monitoring_run_evidence(
                job_id, running["run_id"]
            )
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="extract_evidence",
                new_evidence_count=run_evidence_count,
                fetched_page_count=fetched_page_count,
                llm_call_count=llm_calls,
            )
        return extracted

    @_activity_log("detect_monitoring_changes_activity")
    @activity.defn(name="detect_monitoring_changes_activity")
    def detect_monitoring_changes(self, job_id: str) -> int:
        running = self.repository.get_running_monitoring_run(job_id)
        if not running:
            return 0
        run_id = running["run_id"]
        snapshots = self.repository.list_source_snapshots(
            job_id, run_id=run_id, status="fetched"
        )
        snapshot_ids = {snapshot["snapshot_id"] for snapshot in snapshots}
        all_evidence = self.repository.list_evidence(job_id)
        current_evidence = [
            item
            for item in all_evidence
            if item.snapshot_id in snapshot_ids
        ]
        evidence_positions = {
            item.evidence_id: index for index, item in enumerate(all_evidence)
        }
        max_new_events = self._limit("monitor_max_new_events", 20)
        checkpoint_phase, checkpoint_index = _monitoring_heartbeat_checkpoint()
        detected_event_ids = {
            event["event_id"]
            for event in self.repository.list_change_events(job_id)
            if event.get("run_id") == run_id
        }
        for index, item in enumerate(current_evidence):
            if (
                checkpoint_phase == "change_detection"
                and index <= checkpoint_index
            ):
                continue
            if len(detected_event_ids) >= max_new_events:
                break
            candidate = self.change_detector.detect_evidence(
                item, all_evidence[: evidence_positions[item.evidence_id]]
            )
            event_id = self.repository.add_change_event(
                job_id=job_id,
                run_id=run_id,
                source_id=item.source_id,
                entity=candidate.entity,
                event_type=candidate.event_type,
                severity=candidate.materiality_level,
                summary=candidate.summary,
                old_value_json=candidate.old_value,
                new_value_json=candidate.new_value,
                effective_at=(
                    candidate.effective_at.isoformat()
                    if candidate.effective_at
                    else item.observed_at
                ),
                novelty_level=candidate.novelty_level,
                materiality_level=candidate.materiality_level,
                confidence_band=candidate.confidence_band,
                event_fingerprint=self.change_detector.fingerprint(candidate),
                evidence_ids=candidate.supporting_evidence_ids,
            )
            for evidence_id in candidate.contradicting_evidence_ids:
                self.repository.link_change_event_evidence(
                    change_event_id=event_id,
                    evidence_id=evidence_id,
                    relation="contradict",
                )
            detected_event_ids.add(event_id)
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="change_detection",
                item_id=item.evidence_id,
                index=index,
            )
        self.repository.update_monitoring_run_stats(
            run_id,
            stage="detect_changes",
            change_event_count=len(detected_event_ids),
        )
        return len(detected_event_ids)

    @_activity_log("run_incremental_analysis_activity")
    @activity.defn(name="run_incremental_analysis_activity")
    def run_incremental_analysis(self, job_id: str) -> int:
        running = self.repository.get_running_monitoring_run(job_id)
        if not running:
            return 0
        affected_ids = self.repository.list_monitoring_run_dataset_ids(
            job_id, running["run_id"]
        )
        if not affected_ids:
            return 0
        datasets, profiles = self._load_analysis_datasets(job_id)
        affected = [
            dataset for dataset in datasets if dataset.dataset_id in affected_ids
        ]
        affected_profiles = [
            profile for profile in profiles if profile.dataset_id in affected_ids
        ]
        executor = getattr(self.backend, "analysis_executor", None)
        if executor is None:
            executor = self.analysis_executor
        options = self.repository.get_research_options(job_id)
        selected_tools, selected_skills, include = self._analysis_depth_selection(options)
        plan = executor.selector.build_plan(
            topic=self._job(job_id).topic,
            datasets=affected,
            profiles=affected_profiles,
            include=include,
            exclude=options.get("exclude") or None,
        )
        tools, skills = plan.selected_tools, plan.selected_skills
        if selected_tools is not None:
            tools = selected_tools
        if selected_skills is not None:
            skills = selected_skills
        for prior in self.repository.list_latest_analysis_results(job_id):
            if set(prior.get("input_dataset_ids") or []) & set(affected_ids):
                if prior.get("skill_name"):
                    skills.append(prior["skill_name"])
                elif prior.get("tool_name"):
                    tools.append(prior["tool_name"])
        tools = list(dict.fromkeys(["data_quality_summarizer", *tools]))
        skills = list(dict.fromkeys(skills))
        completed = 0

        def on_result(result) -> None:
            nonlocal completed
            self.repository.save_analysis_result(job_id, result)
            completed += 1
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="incremental_analysis",
                item_id=result.result_id,
                index=completed,
            )

        _record_monitoring_heartbeat(
            self.repository,
            job_id,
            phase="incremental_analysis",
            item_id=None,
            index=0,
        )
        started_run_id = None

        def on_run_started(run) -> None:
            nonlocal started_run_id
            started_run_id = run.run_id
            self.repository.start_analysis_run(run)

        try:
            run, results = executor.run(
                job_id=job_id,
                topic=self._job(job_id).topic,
                datasets=affected,
                profiles=affected_profiles,
                selected_tools=tools,
                selected_skills=skills,
                reason=(
                    "monitoring incremental update for datasets: "
                    + ", ".join(sorted(affected_ids))
                ),
                load_cached_result=lambda key: (
                    self.repository.get_analysis_result_by_idempotency_key(
                        job_id, key
                    )
                ),
                on_run_started=on_run_started,
                on_result=on_result,
            )
        except Exception:
            if started_run_id:
                self.repository.fail_analysis_run(started_run_id)
            raise
        self.repository.complete_analysis_run(run.run_id)
        self.repository.update_monitoring_run_stats(
            running["run_id"], stage="incremental_analysis"
        )
        logger.info(
            "监测增量专业分析完成 job_id=%s run_id=%s datasets=%s tools=%s skills=%s",
            job_id,
            run.run_id,
            ",".join(sorted(affected_ids)),
            ",".join(tools),
            ",".join(skills),
        )
        return len(results)

    @_activity_log("update_monitoring_report_activity")
    @activity.defn(name="update_monitoring_report_activity")
    def update_monitoring_report(self, job_id: str) -> str:
        open_events = self.repository.list_evidence_backed_change_events(
            job_id, "open"
        )
        if not open_events:
            report = self.repository.get_latest_report(job_id)
            return f"no_change: 当前报告 v{report['version']} 无新增变化"
        config = self.repository.get_monitoring_config(job_id)
        base_report = self.repository.get_latest_report(job_id)
        evidence = self.repository.list_evidence(job_id)
        running = self.repository.get_running_monitoring_run(job_id)
        if (
            running
            and running.get("stage") == "patch_created"
            and running.get("draft_patch_id")
        ):
            existing_patch = self.repository.get_report_patch(
                running["draft_patch_id"]
            )
            if existing_patch and existing_patch["approval_status"] in {
                "pending",
                "not_required",
            }:
                target_version = existing_patch.get("version") or "?"
                return (
                    f"{existing_patch['decision']}: 复用已生成 patch，"
                    f"目标 v{target_version}"
                )
        synthesis_already_saved = bool(
            running
            and running.get("stage")
            in {"claims_synthesized", "decision", "patch_created"}
        )
        if not synthesis_already_saved:
            llm_calls = int(running.get("llm_call_count") or 0) if running else 0
            max_llm_calls = self._limit("monitor_max_llm_calls", 25)
            if running and llm_calls >= max_llm_calls:
                raise RuntimeError(
                    f"监测 LLM 调用达到预算上限 {max_llm_calls}"
                )
            try:
                options = self.repository.get_research_options(job_id)
                for item in self.backend.claim_synthesizer.synthesize(
                    evidence, language=options.get("language") or "zh"
                ):
                    self.repository.add_claim(job_id, item)
            except Exception as exc:
                raise _classify_external_error(exc) from exc
            if running:
                self.repository.update_monitoring_run_stats(
                    running["run_id"],
                    stage="claims_synthesized",
                    llm_call_count=llm_calls + 1,
                )
        job = self._job(job_id)
        plan = self.repository.get_research_plan(job_id)
        if not plan:
            raise ValueError("missing research plan")
        sources = self.repository.list_sources(job_id, "fetched")
        claims = self.repository.list_active_claims(job_id)
        impact_candidates = self.impact_analyzer.analyze(
            open_events, claims, evidence
        )
        for index, candidate in enumerate(impact_candidates):
            _record_monitoring_heartbeat(
                self.repository,
                job_id,
                phase="impact_analysis",
                item_id=candidate.claim_id or candidate.change_event_id,
                index=index,
            )
        impacts = [
            row
            for candidate in impact_candidates
            for row in candidate.to_repository_rows()
        ]
        event_ids = [event["event_id"] for event in open_events]
        self.repository.replace_claim_impacts(job_id, event_ids, impacts)
        decision = self.update_decider.decide(
            mode=config.mode if config else "safe",
            events=open_events,
            impacts=impact_candidates,
            max_auto_patch_sections=self._limit("monitor_max_auto_patch_sections", 3),
        )
        decision_action = decision.action
        impacted_claim_ids = decision.affected_claim_ids
        impacted_section_ids = decision.affected_section_ids
        revision_summary = (
            f"处理 {len(open_events)} 条变化事件，影响 "
            f"{len(impacted_section_ids)} 个章节、{len(impacted_claim_ids)} 条 claim。"
            f"决策理由：{decision.reason}"
        )
        running = self.repository.get_running_monitoring_run(job_id)
        if running:
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="decision",
                change_event_count=len(open_events),
                affected_claim_count=len(impacted_claim_ids),
            )
        if decision_action == "evidence_only":
            return (
                f"evidence_only: 记录变化 {len(open_events)} 条，"
                f"影响章节 {len(impacted_section_ids)} 个、claims {len(impacted_claim_ids)} 条"
            )
        if not base_report:
            raise ReportValidationError("监测更新缺少已发布基线报告")
        patch_id = (
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    ":".join(
                        [
                            running["run_id"],
                            base_report["report_version_id"],
                            decision_action,
                            *sorted(event_ids),
                        ]
                    ),
                )
            )
            if running
            else str(uuid.uuid4())
        )
        version = self.repository.next_report_version(job_id)
        base_markdown = Path(base_report["report_path"]).read_text(encoding="utf-8")
        base_report_json = json.loads(
            Path(base_report["report_json_path"]).read_text(encoding="utf-8")
        )
        monitoring_revision = {
            "revision_type": "partial",
            "status": "draft" if decision_action == "review_required" else "auto_patch",
            "base_report_version_id": base_report["report_version_id"],
            "base_version": base_report["version"],
            "target_version": version,
            "change_event_ids": event_ids,
            "impacted_section_ids": impacted_section_ids,
            "impacted_claim_ids": impacted_claim_ids,
            "summary": revision_summary,
            "decision": decision_action,
        }
        claims_by_id = {claim.claim_id: claim for claim in claims}
        patch_evidence_ids = self._patch_evidence_ids(
            impacted_claim_ids, claims_by_id, evidence, open_events
        )
        patch_json = self.report_patcher.build_patch(
            base_report_version_id=(
                base_report["report_version_id"] if base_report else None
            ),
            affected_section_ids=impacted_section_ids,
            change_events=open_events,
            claim_impacts=impacts,
            evidence_ids=patch_evidence_ids,
            summary=revision_summary,
            decision=decision_action,
        )
        patch_json["base_report_hash"] = hashlib.sha256(
            base_markdown.encode("utf-8")
        ).hexdigest()
        patch_json["base_report_json_hash"] = hashlib.sha256(
            Path(base_report["report_json_path"]).read_bytes()
        ).hexdigest()
        self.report_patcher.validate_patch(
            patch_json,
            allowed_section_ids=impacted_section_ids,
            known_evidence_ids={item.evidence_id for item in evidence},
        )
        markdown, report = self.report_patcher.apply_patch(
            base_markdown=base_markdown,
            base_report=base_report_json,
            patch_json=patch_json,
            monitoring_revision=monitoring_revision,
            evidence=[item.model_dump() for item in evidence],
            claims=[item.model_dump() for item in claims],
            sources=sources,
            change_events=open_events,
            claim_impacts=impacts,
        )
        claim_revision_payloads = []
        events_by_id = {event["event_id"]: event for event in open_events}
        for claim_id in impacted_claim_ids:
            claim = claims_by_id.get(claim_id)
            if not claim:
                continue
            claim_impacts = [
                impact for impact in impacts if impact.get("claim_id") == claim_id
            ]
            related_events = [
                events_by_id[impact["event_id"]]
                for impact in claim_impacts
                if impact.get("event_id") in events_by_id
            ]
            summaries = list(
                dict.fromkeys(
                    event["summary"]
                    for event in related_events
                    if event.get("summary")
                )
            )
            replaces_conclusion = any(
                impact.get("impact_type") in {"contradicts", "supersedes"}
                for impact in claim_impacts
            ) or any(
                event.get("old_value_json") or event.get("old_value")
                for event in related_events
            )
            revised_statement = claim.statement
            if summaries:
                revised_statement = (
                    "；".join(summaries)
                    if replaces_conclusion
                    else f"{claim.statement} 更新：{'；'.join(summaries)}"
                )
            proposed_confidences = [
                impact.get("proposed_confidence_band")
                for impact in claim_impacts
                if impact.get("proposed_confidence_band")
            ]
            revised_confidence = (
                "conflicting"
                if "conflicting" in proposed_confidences
                else proposed_confidences[-1]
                if proposed_confidences
                else claim.confidence_band
            )
            supporting_ids = list(
                dict.fromkeys(
                    claim.supporting_evidence_ids
                    + [
                        evidence_id
                        for event in related_events
                        for evidence_id in event.get(
                            "supporting_evidence_ids",
                            event.get("evidence_ids", []),
                        )
                    ]
                )
            )
            contradicting_ids = list(
                dict.fromkeys(
                    claim.contradicting_evidence_ids
                    + [
                        evidence_id
                        for event in related_events
                        for evidence_id in event.get(
                            "contradicting_evidence_ids", []
                        )
                    ]
                )
            )
            claim_revision_payloads.append(
                {
                    "original_claim_id": claim.claim_id,
                    "statement": revised_statement,
                    "confidence_band": revised_confidence,
                    "reason": revision_summary,
                    "supporting_evidence_ids": supporting_ids,
                    "contradicting_evidence_ids": contradicting_ids,
                }
            )
        patch_json["claim_revisions"] = claim_revision_payloads
        report_path, json_path = self.backend.artifact_store.write_pending_report(
            job_id, patch_id, markdown, report
        )
        patch_json["report_path"] = str(report_path)
        patch_json["report_json_path"] = str(json_path)
        patch_json["target_version"] = version
        revision_id = self.repository.add_pending_report_patch(
            patch_id=patch_id,
            job_id=job_id,
            monitor_run_id=running["run_id"] if running else None,
            base_report_version_id=base_report["report_version_id"],
            patch_json=patch_json,
            change_summary=revision_summary,
            decision=decision_action,
            approval_status=(
                "pending"
                if decision_action == "review_required"
                else "not_required"
            ),
        )
        if running:
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="patch_created",
                result_report_version_id=None,
                draft_patch_id=revision_id,
            )
        if decision_action == "review_required":
            return (
                f"review_required: 生成待审批 patch，目标 v{version}，处理变化 {len(open_events)} 条，"
                f"影响章节 {len(impacted_section_ids)} 个、claims {len(impacted_claim_ids)} 条"
            )
        return (
            f"auto_patch: 生成报告 v{version}，处理变化 {len(open_events)} 条，"
            f"影响章节 {len(impacted_section_ids)} 个、claims {len(impacted_claim_ids)} 条"
        )

    @_activity_log("validate_monitoring_report_activity")
    @activity.defn(name="validate_monitoring_report_activity")
    def validate_monitoring_report(self, job_id: str, decision: str) -> str:
        if decision.startswith("no_change"):
            return decision
        if decision.startswith("evidence_only"):
            open_events = self.repository.list_change_events(job_id, "open")
            self.repository.mark_change_events_applied(
                job_id, [event["event_id"] for event in open_events]
            )
            return decision
        if decision.startswith("review_required"):
            patches = self.repository.list_report_patches(
                job_id=job_id, approval_status="pending"
            )
            if patches:
                try:
                    self._validate_pending_monitor_patch(job_id, patches[0])
                    self.repository.mark_report_patch_validation(
                        patches[0]["patch_id"], "passed"
                    )
                except ReportValidationError as exc:
                    self.repository.mark_report_patch_validation(
                        patches[0]["patch_id"], "failed"
                    )
                    return f"{decision}；待审批 patch 校验未通过：{str(exc)[:200]}"
            return decision
        patches = self.repository.list_report_patches(
            job_id=job_id, approval_status="not_required"
        )
        if not patches:
            raise ReportValidationError("监测自动更新缺少未发布 patch")
        patch = patches[0]
        try:
            self._validate_pending_monitor_patch(job_id, patch)
            published = self.repository.publish_pending_report_patch(
                patch["patch_id"]
            )
        except ReportValidationError:
            self._delete_unpublished_patch_artifacts(patch["patch_id"])
            raise
        except ValueError as exc:
            self._delete_unpublished_patch_artifacts(patch["patch_id"])
            raise ReportValidationError(str(exc)) from exc
        report_version_id = published["report_version_id"]
        report = self.repository.get_latest_report(job_id)
        running = self.repository.get_latest_monitoring_run(job_id)
        if running:
            self.repository.update_monitoring_run_stats(
                running["run_id"],
                stage="published",
                result_report_version_id=report_version_id,
            )
        open_events = self.repository.list_change_events(job_id, "open")
        self.repository.mark_change_events_applied(
            job_id, [event["event_id"] for event in open_events]
        )
        version = report["version"] if report else "?"
        path = report["report_path"] if report else ""
        self.repository.add_change_event(
            job_id=job_id,
            event_type="report_updated",
            severity="high",
            summary=f"监测发布报告 v{version}",
            new_value=str(path),
            status="applied",
        )
        return decision

    def _delete_unpublished_patch_artifacts(self, patch_id: str) -> None:
        try:
            paths = self.repository.delete_unpublished_report_patch(patch_id)
        except Exception:
            logger.exception(
                "删除失败的未发布监测 patch 失败 patch_id=%s",
                patch_id,
            )
            return
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                logger.warning(
                    "删除失败的未发布监测 artifact 失败 path=%s",
                    path,
                    exc_info=True,
                )

    def _delete_draft_report_artifacts(self, report_version_id: str) -> None:
        try:
            paths = self.repository.delete_draft_report_version(report_version_id)
        except Exception:
            logger.warning(
                "删除失败的监测 draft 版本失败 report_version_id=%s",
                report_version_id,
                exc_info=True,
            )
            return
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                logger.warning(
                    "删除失败的监测 draft artifact 失败 path=%s",
                    path,
                    exc_info=True,
                )

    def _validate_report_record(
        self, job_id: str, report: dict, *, publish: bool
    ) -> str:
        markdown_path = Path(report["report_path"])
        try:
            self.backend.report_validator.validate(
                markdown=markdown_path.read_text(encoding="utf-8"),
                report_path=markdown_path,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claims=self.repository.list_active_claims(job_id),
                api_key=self.backend.api_key,
                snapshots=self.repository.list_source_snapshots(job_id),
            )
        except AgentReportValidationError as exc:
            self.repository.mark_report_validation_failed(
                report["report_version_id"], str(exc)
            )
            raise ReportValidationError(str(exc)) from exc
        if publish:
            self.repository.publish_report_version(report["report_version_id"])
        return report["report_version_id"]

    def _validate_monitoring_patch_record(self, job_id: str, report: dict) -> None:
        patch = self.repository.get_report_patch_by_report_version_id(
            report["report_version_id"]
        )
        if not patch:
            raise ReportValidationError("监测报告缺少 report patch")
        base_report = (
            self.repository.get_report_version_by_id(
                patch["base_report_version_id"]
            )
            if patch.get("base_report_version_id")
            else None
        )
        try:
            self.patch_validator.validate(
                patch=patch,
                report=report,
                base_report=base_report,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claim_revisions=self.repository.list_claim_revisions(
                    job_id, report["report_version_id"]
                ),
                change_events=self.repository.list_change_events(job_id),
                markdown_path=Path(report["report_path"]),
                json_path=Path(report["report_json_path"]),
                snapshots=self.repository.list_source_snapshots(job_id),
            )
        except MonitoringPatchValidationError as exc:
            self.repository.mark_report_validation_failed(
                report["report_version_id"], str(exc)
            )
            raise ReportValidationError(str(exc)) from exc

    def _validate_pending_monitor_patch(self, job_id: str, patch: dict) -> None:
        base_report = (
            self.repository.get_report_version_by_id(
                patch["base_report_version_id"]
            )
            if patch.get("base_report_version_id")
            else None
        )
        patch_json = patch["patch_json"]
        report = {
            "report_version_id": None,
            "parent_report_version_id": patch["base_report_version_id"],
            "report_path": patch_json.get("report_path", ""),
            "report_json_path": patch_json.get("report_json_path", ""),
        }
        try:
            self.patch_validator.validate(
                patch=patch,
                report=report,
                base_report=base_report,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claim_revisions=patch_json.get("claim_revisions", []),
                change_events=self.repository.list_change_events(job_id),
                markdown_path=Path(report["report_path"]),
                json_path=Path(report["report_json_path"]),
                snapshots=self.repository.list_source_snapshots(job_id),
            )
            markdown_path = Path(report["report_path"])
            self.backend.report_validator.validate(
                markdown=markdown_path.read_text(encoding="utf-8"),
                report_path=markdown_path,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claims=self.repository.list_active_claims(job_id),
                api_key=self.backend.api_key,
                snapshots=self.repository.list_source_snapshots(job_id),
            )
        except MonitoringPatchValidationError as exc:
            raise ReportValidationError(str(exc)) from exc
        except AgentReportValidationError as exc:
            raise ReportValidationError(str(exc)) from exc

    def _analyze_claim_impacts(
        self,
        events: list[dict],
        claims,
        evidence,
    ) -> list[dict]:
        return [
            row
            for candidate in self.impact_analyzer.analyze(events, claims, evidence)
            for row in candidate.to_repository_rows()
        ]

    @staticmethod
    def _monitor_update_action(
        *,
        mode: str,
        impacts: list[dict],
        impacted_section_ids: list[str],
        max_auto_patch_sections: int = 3,
    ) -> str:
        if mode == "observe":
            return "evidence_only"
        high_severities = {"high", "critical"}
        if any(item["severity"] in high_severities for item in impacts):
            return "review_required"
        if len(impacted_section_ids) > max_auto_patch_sections:
            return "review_required"
        return "auto_patch"

    def _initial_monitor_cutoff(
        self, base_report: dict | None, cutoff_to: str
    ) -> str:
        now = self._parse_utc(cutoff_to) or datetime.now(timezone.utc)
        lookback_start = now - timedelta(
            days=self._limit("monitor_lookback_days", 7)
        )
        report_time = None
        if base_report:
            report_time = self._parse_utc(
                base_report.get("published_at") or base_report.get("created_at")
            )
        if report_time and report_time > lookback_start:
            return report_time.isoformat(timespec="seconds")
        return lookback_start.isoformat(timespec="seconds")

    @staticmethod
    def _parse_utc(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _cutoff_query_hint(cutoff_from: str | None) -> str:
        parsed = ResearchActivities._parse_utc(cutoff_from)
        if not parsed:
            return ""
        return f"after:{parsed.date().isoformat()}"

    @staticmethod
    def _query_terms_for_watch_types(watch_types: list[str]) -> list[str]:
        terms_by_type = {
            "official_news": "news",
            "product_page": "product",
            "pricing_page": "pricing price 定价",
            "release_notes": "release changelog 更新日志",
            "app_store_page": "app store reviews",
            "public_review_page": "reviews 用户评价",
            "official_homepage": "official",
        }
        terms = []
        for watch_type in watch_types:
            value = terms_by_type.get(watch_type)
            if value:
                terms.append(value)
        return terms[:4]

    @staticmethod
    def _target_event_types(watch_types: list[str]) -> list[str]:
        event_types = {"new_evidence", "feature_added", "product_launch"}
        if "pricing_page" in watch_types:
            event_types.add("price_change")
        if "release_notes" in watch_types:
            event_types.update({"feature_added", "feature_removed"})
        if "public_review_page" in watch_types or "app_store_page" in watch_types:
            event_types.add("sentiment_shift")
        if "official_news" in watch_types:
            event_types.add("company_statement")
        return sorted(event_types)

    def _limit(self, name: str, default: int) -> int:
        return int(getattr(self.backend.limits, name, default))

    def _depth_limits(self, job_id: str) -> tuple[int, int, int]:
        options = self.repository.get_research_options(job_id)
        depth = str(options.get("depth") or "standard")
        max_queries = self._limit("max_search_queries", 6)
        max_results = self._limit("max_results_per_query", 5)
        max_pages = self._limit("max_fetched_pages", 0)
        if depth == "quick":
            return min(max_queries, 2), min(max_results, 3), max_pages
        if depth == "professional":
            return max_queries, max_results, max_pages
        return min(max_queries, 4), max_results, max_pages

    @staticmethod
    def _analysis_depth_selection(
        options: dict,
    ) -> tuple[list[str] | None, list[str] | None, list[str] | None]:
        depth = str(options.get("depth") or "standard")
        include = options.get("include") or None
        if depth == "quick":
            return ["data_quality_summarizer"], [], include
        if depth == "standard" and not include:
            return None, None, ["competitor,pricing,market_position"]
        return None, None, include

    def _pause_monitoring_if_failure_threshold_reached(
        self, job_id: str, error_message: str | None
    ) -> bool:
        if not error_message:
            return False
        config = self.repository.get_monitoring_config(job_id)
        if not config or config.status != "active":
            return False
        threshold = self._limit("monitor_max_consecutive_failures", 3)
        if threshold <= 0 or config.consecutive_failure_count < threshold:
            return False
        if not self.monitor_scheduler:
            logger.warning(
                "连续失败已达阈值但未配置监测调度器 job_id=%s count=%s threshold=%s",
                job_id,
                config.consecutive_failure_count,
                threshold,
            )
            return False
        try:
            self.monitor_scheduler.pause(config.schedule_id)
            self.repository.set_monitoring_status(job_id, "paused")
            logger.warning(
                "连续失败已达阈值，已暂停监测 Schedule job_id=%s schedule_id=%s count=%s threshold=%s",
                job_id,
                config.schedule_id,
                config.consecutive_failure_count,
                threshold,
            )
            return True
        except Exception:
            logger.warning(
                "连续失败已达阈值但暂停监测 Schedule 失败 job_id=%s schedule_id=%s",
                job_id,
                config.schedule_id,
                exc_info=True,
            )
            return False

    @staticmethod
    def _patch_evidence_ids(
        impacted_claim_ids: list[str],
        claims_by_id: dict,
        evidence,
        change_events: list[dict] | None = None,
    ) -> list[str]:
        evidence_ids: list[str] = []
        for event in change_events or []:
            for key in ("supporting_evidence_ids", "evidence_ids"):
                for evidence_id in event.get(key, []) or []:
                    if evidence_id not in evidence_ids:
                        evidence_ids.append(evidence_id)
        for claim_id in impacted_claim_ids:
            claim = claims_by_id.get(claim_id)
            if not claim:
                continue
            for evidence_id in (
                claim.supporting_evidence_ids + claim.contradicting_evidence_ids
            ):
                if evidence_id not in evidence_ids:
                    evidence_ids.append(evidence_id)
        if evidence_ids:
            return evidence_ids
        return [item.evidence_id for item in evidence[:10]]

    @staticmethod
    def _fallback_section_for_event(event: dict) -> str:
        return ImpactAnalyzer.fallback_section_for_event(event)

    @staticmethod
    def _format_monitoring_revision_markdown(
        *,
        base_version: int | None,
        target_version: int,
        impacted_section_ids: list[str],
        impacted_claim_ids: list[str],
        change_event_count: int,
        summary: str,
    ) -> str:
        base_label = f"v{base_version}" if base_version else "无已发布基线"
        section_lines = [
            f"- `{section_id}` {SECTION_TITLES.get(section_id, section_id)}"
            for section_id in impacted_section_ids
        ] or ["- 无直接命中章节，已放入不确定性复核。"]
        claim_lines = [
            f"- `{claim_id}`" for claim_id in impacted_claim_ids
        ] or ["- 无直接命中既有 claim，将作为章节级新增信息处理。"]
        return (
            "\n## 本轮局部修订范围\n"
            f"- 基线报告：{base_label}\n"
            f"- 目标报告：v{target_version} pending patch\n"
            f"- 变化事件数量：{change_event_count}\n"
            f"- 修订摘要：{summary}\n"
            "### 受影响章节\n"
            + "\n".join(section_lines)
            + "\n### 受影响 Claims\n"
            + "\n".join(claim_lines)
            + "\n"
        )

    @_activity_log("complete_monitoring_cycle_activity")
    @activity.defn(name="complete_monitoring_cycle_activity")
    def complete_monitoring_cycle(
        self,
        job_id: str,
        run_id: str,
        decision: str,
        error_message: str | None,
    ) -> str:
        self.repository.complete_monitoring_run(
            run_id, job_id, decision=decision, error_message=error_message
        )
        self._pause_monitoring_if_failure_threshold_reached(job_id, error_message)
        return job_id

    @_activity_log("notify_monitoring_cycle_activity")
    @activity.defn(name="notify_monitoring_cycle_activity")
    def notify_monitoring_cycle(
        self, job_id: str, decision: str, error_message: str | None
    ) -> str:
        if not self.messenger:
            return "skipped"
        config = self.repository.get_monitoring_config(job_id)
        if not config:
            return "skipped"
        if not self._should_send_monitor_notification(
            config.notify_level, decision, error_message
        ):
            return "skipped"
        job = self._job(job_id)
        run = self.repository.get_latest_monitoring_run(job_id)
        notification_type = "monitor_failed" if error_message else "monitor_result"
        dedup_key = (
            f"{notification_type}:{run['run_id'] if run else job_id}:{decision}"
        )
        should_send, notification_id = self.repository.try_start_notification_delivery(
            job_id=job_id,
            monitor_run_id=run["run_id"] if run else None,
            notification_type=notification_type,
            dedup_key=dedup_key,
            chat_id=config.chat_id,
        )
        if not should_send:
            return "skipped_duplicate"
        if error_message:
            message = self._format_monitor_failure_notification(
                job_id, config, run, error_message
            )
        else:
            message = self._format_monitor_result_notification(
                job_id, config, run, decision
            )
        try:
            self.messenger.send_text_to_chat(config.chat_id, message)
            if (
                not error_message
                and run
                and run.get("result_report_version_id")
                and hasattr(self.messenger, "send_file_to_chat")
            ):
                artifacts = self.repository.list_report_artifacts(
                    job_id,
                    report_version_id=run["result_report_version_id"],
                    ready_only=True,
                )
                for artifact in artifacts:
                    if artifact["artifact_type"] not in {"pdf", "xlsx"}:
                        continue
                    self._send_artifact_deduped(
                        artifact=artifact,
                        job_id=job_id,
                        chat_id=config.chat_id,
                    )
            self.repository.mark_notification_delivery_sent(notification_id)
            return "sent"
        except Exception as exc:
            self.repository.mark_notification_delivery_failed(
                notification_id, str(exc)
            )
            logger.warning("监测通知发送失败 job_id=%s", job_id, exc_info=True)
            return "failed"

    def _format_monitor_failure_notification(
        self,
        job_id: str,
        config,
        run: dict | None,
        error_message: str,
    ) -> str:
        threshold = self._limit("monitor_max_consecutive_failures", 3)
        is_paused = config.status == "paused"
        suggested = "检查错误后使用 /monitor resume 恢复监测" if is_paused else "检查 Provider、网络或模型配置后等待下次调度"
        lines = [
            "周期监测执行失败",
            "",
            f"任务 ID：{job_id}",
            f"失败阶段：{run.get('stage') if run else '未知'}",
            f"错误摘要：{error_message}",
            f"Schedule 状态：{config.status}",
            f"是否已暂停：{'是' if is_paused else '否'}",
            f"建议操作：{suggested}",
        ]
        if threshold > 0 and config.consecutive_failure_count >= threshold:
            lines.append(
                f"连续失败：{config.consecutive_failure_count}/{threshold}"
            )
        return "\n".join(lines)

    def _format_monitor_result_notification(
        self,
        job_id: str,
        config,
        run: dict | None,
        decision: str,
    ) -> str:
        current_report = self.repository.get_latest_report(job_id)
        job = self._job(job_id)
        current_version = f"v{current_report['version']}" if current_report else "无"
        interval = self._monitor_run_interval(run, config.timezone)
        if decision.startswith("no_change"):
            return "\n".join(
                [
                    "监测完成：未发现需要更新报告的重要变化",
                    "",
                    f"任务 ID：{job_id}",
                    f"监测区间：{interval}",
                    f"检查来源数：{run.get('fetched_page_count', 0) if run else 0}",
                    f"新来源数：{run.get('new_source_count', 0) if run else 0}",
                    f"当前报告版本：{current_version}",
                    f"下次监测时间：{self._display_monitor_time(config.next_run_at, config.timezone)}",
                ]
            )
        patch = self._latest_monitor_patch(job_id)
        patch_json = patch["patch_json"] if patch else {}
        impacted_claim_ids = patch_json.get("impacted_claim_ids", [])
        impacted_sections = patch_json.get("impacted_section_ids", [])
        change_event_ids = patch_json.get("change_event_ids", [])
        impacts = self.repository.list_claim_impacts(job_id, change_event_ids)
        evidence_ids = self._patch_evidence_ids_from_json(patch_json)
        claims_by_id = {
            claim.claim_id: claim for claim in self.repository.list_claims(job_id)
        }
        current_conclusions = [
            claims_by_id[claim_id].statement
            for claim_id in impacted_claim_ids
            if claim_id in claims_by_id
        ]
        suggested_conclusions = [
            revision.get("statement", "")
            for revision in patch_json.get("claim_revisions", [])
            if revision.get("statement")
        ]
        current_conclusion_text = "；".join(
            current_conclusions or impacted_claim_ids
        ) or "无"
        suggested_conclusion_text = "；".join(suggested_conclusions)
        if decision.startswith("review_required"):
            patch_id = patch["patch_id"] if patch else "未知"
            validation_failed = (
                "校验未通过" in decision
                or (patch and patch.get("validation_status") == "failed")
            )
            if validation_failed:
                validation_error = (
                    decision.split("校验未通过：", 1)[1]
                    if "校验未通过：" in decision
                    else "待审批 patch 校验未通过"
                )
                return "\n".join(
                    [
                        "报告更新校验失败",
                        "",
                        f"任务 ID：{job_id}",
                        f"Patch ID：{patch_id}",
                        f"错误：{validation_error[:300]}",
                        f"受到影响的 Claim：{', '.join(impacted_claim_ids) or '无'}",
                        f"受影响章节：{', '.join(impacted_sections) or '无'}",
                        f"证据：{', '.join(evidence_ids) or '无'}",
                        "",
                        "该更新不会自动发布，也不应批准。",
                        f"可重新运行监测：/monitor run {job_id}",
                        f"或重新发起研究：/research {job.topic}",
                        f"拒绝该 patch：/update reject {patch_id} 校验失败",
                    ]
                )
            return "\n".join(
                [
                    "发现重大变化，需要确认报告更新",
                    "",
                    f"任务 ID：{job_id}",
                    f"Patch ID：{patch_id}",
                    f"变化摘要：{patch.get('change_summary', decision) if patch else decision}",
                    f"受到影响的 Claim：{', '.join(impacted_claim_ids) or '无'}",
                    f"当前结论：{current_conclusion_text}",
                    f"建议结论：{suggested_conclusion_text or '请查看待审批 patch 详情'}",
                    f"证据：{', '.join(evidence_ids) or '无'}",
                    f"影响等级：{self._highest_impact_level(impacts)}",
                    f"置信度：{self._impact_confidence(impacts)}",
                    f"受影响章节：{', '.join(impacted_sections) or '无'}",
                    "",
                    f"批准：/update approve {patch_id}",
                    "",
                    f"拒绝：/update reject {patch_id} 原因",
                ]
            )
        report = current_report
        previous = (
            f"v{report['version'] - 1}"
            if report and isinstance(report.get("version"), int) and report["version"] > 1
            else "旧版本"
        )
        return "\n".join(
            [
                "动态报告已更新",
                "",
                f"任务 ID：{job_id}",
                f"报告版本：{previous} → {current_version}",
                f"发现变化：{len(change_event_ids)} 条",
                f"影响的原结论：{current_conclusion_text}",
                f"修改章节：{', '.join(impacted_sections) or '无'}",
                f"新增证据：{', '.join(evidence_ids) or '无'}",
                f"更新原因：{patch.get('change_summary', decision) if patch else decision}",
                f"报告路径：{report['report_path'] if report else '无'}",
                f"下一次监测时间：{self._display_monitor_time(config.next_run_at, config.timezone)}",
            ]
        )

    def _monitor_run_interval(
        self, run: dict | None, timezone_name: str | None = None
    ) -> str:
        if not run:
            return "未知"
        start = self._display_monitor_time(
            run.get("cutoff_from"), timezone_name, empty="首次"
        )
        end = self._display_monitor_time(
            run.get("cutoff_to"), timezone_name, empty="未知"
        )
        return f"{start} → {end}"

    @staticmethod
    def _display_monitor_time(
        value: str | None, timezone_name: str | None, *, empty: str = "暂不可用"
    ) -> str:
        if not value:
            return empty
        zone_name = timezone_name or "Asia/Shanghai"
        try:
            zone = ZoneInfo(zone_name)
        except ZoneInfoNotFoundError:
            zone_name = "Asia/Shanghai"
            zone = ZoneInfo(zone_name)
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return f"{value} ({zone_name})"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(zone).strftime(f"%Y-%m-%d %H:%M:%S {zone_name}")

    def _latest_monitor_patch(self, job_id: str) -> dict | None:
        patches = self.repository.list_report_patches(job_id=job_id)
        return patches[0] if patches else None

    @staticmethod
    def _patch_evidence_ids_from_json(patch_json: dict) -> list[str]:
        evidence_ids: list[str] = []
        for section_patch in patch_json.get("section_patches", []):
            for evidence_id in section_patch.get("evidence_ids", []):
                if evidence_id not in evidence_ids:
                    evidence_ids.append(evidence_id)
        return evidence_ids

    @staticmethod
    def _highest_impact_level(impacts: list[dict]) -> str:
        order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        if not impacts:
            return "未知"
        return max(
            (impact.get("impact_level") or impact.get("severity") or "low" for impact in impacts),
            key=lambda value: order.get(value, 0),
        )

    @staticmethod
    def _impact_confidence(impacts: list[dict]) -> str:
        values = [
            impact.get("proposed_confidence_band")
            or impact.get("old_confidence_band")
            for impact in impacts
            if impact.get("proposed_confidence_band")
            or impact.get("old_confidence_band")
        ]
        return ", ".join(dict.fromkeys(values)) if values else "未知"

    @staticmethod
    def _should_send_monitor_notification(
        notify_level: str, decision: str, error_message: str | None
    ) -> bool:
        if error_message:
            return True
        if notify_level == "all":
            return True
        if notify_level == "medium":
            return not decision.startswith("no_change")
        if notify_level == "high":
            return decision.startswith("review_required")
        return not decision.startswith("no_change")

    def _job(self, job_id: str):
        job = self.repository.get_job(job_id)
        if not job:
            raise ValueError(f"job not found: {job_id}")
        return job


def _classify_external_error(exc: Exception) -> Exception:
    if isinstance(exc, LLMError):
        return InvalidModelOutputError(str(exc))
    if isinstance(exc, httpx.TimeoutException):
        return TransientNetworkError(str(exc))
    if isinstance(exc, httpx.NetworkError):
        return TransientNetworkError(str(exc))
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        if status_code == 401:
            return AuthenticationError("provider authentication failed")
        if status_code == 403:
            return AuthorizationError("provider authorization failed")
        if status_code == 429:
            return RateLimitError("provider rate limited")
        if 500 <= status_code <= 599:
            return ProviderServerError(f"provider server error {status_code}")
    return exc
