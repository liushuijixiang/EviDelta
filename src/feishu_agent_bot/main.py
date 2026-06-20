from __future__ import annotations

import logging
import signal
import sys
import threading

import lark_oapi as lark

from .agent import (
    ResearchAgentBackend,
    ResearchLimits,
    UnavailableResearchBackend,
)
from .agent.claim_synthesizer import ClaimSynthesizer
from .agent.evidence_extractor import EvidenceExtractor
from .agent.planner import ResearchPlanner
from .agent.report_generator import ReportGenerator
from .agent.report_validator import ReportValidator
from .artifacts import ArtifactStore
from .config import ConfigurationError, Settings
from .event_queue import EventQueue
from .event_handler import EventHandler
from .execution import LocalExecutor, TemporalExecutor
from .feishu_client import FeishuMessenger, build_api_client
from .instance_lock import InstanceAlreadyRunning, InstanceLock
from .job_queue import JobQueue
from .logging_config import configure_logging
from .llm.openai_compatible import LLMError, OpenAICompatibleLLM
from .repository import Repository
from .acquisition import AssetDownloader, AssetStore
from .parsers import default_parser_registry
from .research.fetcher import WebFetcher
from .research.parser import ContentExtractor
from .research.search import DDGSSearchProvider, SerperSearchProvider
from .temporal.monitoring import MonitoringScheduler

logger = logging.getLogger(__name__)


def build_research_backend(
    settings: Settings,
    repository: Repository,
    *,
    llm_max_retries: int | None = None,
):
    if not settings.llm_api_key or not settings.llm_model:
        logger.warning(
            "未配置 LLM_API_KEY/LLM_MODEL，/ping 等网关命令可用，"
            "/research 将明确失败"
        )
        return UnavailableResearchBackend(
            "真实调研需要配置 LLM_API_KEY 和 LLM_MODEL"
        )
    if settings.search_provider == "ddgs":
        search_provider = DDGSSearchProvider()
    elif settings.search_provider == "serper":
        if not settings.serper_api_key:
            return UnavailableResearchBackend(
                "SEARCH_PROVIDER=serper 需要配置 SERPER_API_KEY"
            )
        search_provider = SerperSearchProvider(
            api_key=settings.serper_api_key,
            url=settings.serper_url,
            country=settings.serper_country,
            locale=settings.serper_locale,
        )
    else:
        return UnavailableResearchBackend(
            f"暂不支持 SEARCH_PROVIDER={settings.search_provider}"
        )
    try:
        llm = OpenAICompatibleLLM(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model=settings.llm_model,
            timeout_seconds=settings.llm_timeout_seconds,
            max_retries=(
                settings.llm_max_retries
                if llm_max_retries is None
                else llm_max_retries
            ),
            max_tokens=settings.llm_max_tokens,
        )
    except LLMError as exc:
        return UnavailableResearchBackend(str(exc))
    return ResearchAgentBackend(
        repository=repository,
        planner=ResearchPlanner(llm, settings.max_search_queries),
        search_provider=search_provider,
        fetcher=WebFetcher(
            timeout_seconds=settings.fetch_timeout_seconds,
            max_page_bytes=settings.max_page_bytes,
        ),
        content_extractor=ContentExtractor(),
        evidence_extractor=EvidenceExtractor(llm),
        claim_synthesizer=ClaimSynthesizer(llm, settings.max_report_claims),
        report_generator=ReportGenerator(llm),
        report_validator=ReportValidator(),
        artifact_store=ArtifactStore(
            settings.database_path.parent / "artifacts"
        ),
        asset_downloader=AssetDownloader(
            AssetStore(settings.database_path.parent / "artifacts"),
            timeout_seconds=settings.asset_download_timeout_seconds,
            max_redirects=settings.asset_max_redirects,
            max_bytes_by_type={
                "pdf": settings.max_pdf_bytes,
                "csv": settings.max_csv_bytes,
                "excel": settings.max_excel_bytes,
                "json": settings.max_json_bytes,
                "docx": settings.max_docx_bytes,
                "html": settings.max_page_bytes,
            },
            archive_max_entries=settings.archive_max_entries,
            archive_max_uncompressed_bytes=(
                settings.archive_max_uncompressed_bytes
            ),
            archive_max_compression_ratio=(
                settings.archive_max_compression_ratio
            ),
        ),
        parser_registry=default_parser_registry(
            csv_max_rows=settings.csv_max_rows,
            csv_preview_rows=settings.csv_preview_rows,
            csv_chunk_size=settings.csv_chunk_size,
            pdf_ocr_enabled=settings.pdf_ocr_enabled,
            pdf_ocr_languages=settings.pdf_ocr_languages,
            pdf_min_text_chars_per_page=(
                settings.pdf_min_text_chars_per_page
            ),
            pdf_max_pages=settings.pdf_max_pages,
            pdf_parse_timeout_seconds=settings.pdf_parse_timeout_seconds,
        ),
        limits=ResearchLimits(
            max_results_per_query=settings.max_results_per_query,
            max_fetched_pages=settings.max_fetched_pages,
            monitor_max_search_queries=settings.monitor_max_search_queries,
            monitor_max_results_per_query=settings.monitor_max_results_per_query,
            monitor_max_fetched_pages=settings.monitor_max_fetched_pages,
            monitor_max_watch_targets=settings.monitor_max_watch_targets,
            monitor_max_llm_calls=settings.monitor_max_llm_calls,
            monitor_max_new_events=settings.monitor_max_new_events,
            monitor_max_auto_patch_sections=(
                settings.monitor_max_auto_patch_sections
            ),
            monitor_max_consecutive_failures=(
                settings.monitor_max_consecutive_failures
            ),
        ),
        api_key=settings.llm_api_key,
    )


def run(settings: Settings) -> None:
    configure_logging(settings.log_level)
    instance_lock = InstanceLock(
        settings.database_path.with_suffix(settings.database_path.suffix + ".lock")
    )
    instance_lock.acquire()
    logger.info("已获取单实例锁 lock_path=%s", instance_lock.path)
    repository = Repository(settings.database_path)
    try:
        repository.initialize()
        health = repository.health()
        logger.info(
            "SQLite 健康检查完成 database_path=%s journal_mode=%s "
            "job_count=%s message_count=%s schema_version=%s",
            health["database_path"],
            health["journal_mode"],
            health["job_count"],
            health["message_count"],
            health["schema_version"],
        )
        api_client = build_api_client(settings.app_id, settings.app_secret)
        messenger = FeishuMessenger(
            api_client,
            settings.message_max_length,
            settings.feishu_file_max_bytes,
        )
        jobs = None
        if settings.execution_backend == "local":
            backend = build_research_backend(settings, repository)
            jobs = JobQueue(
                repository,
                backend,
                messenger,
                settings.queue_size,
                settings.worker_count,
            )
            jobs.start()
            executor = LocalExecutor(repository, jobs)
            monitor_scheduler = None
            recovered = executor.recover()
            logger.info("SQLite 初始化完成，本地恢复任务数=%s", recovered)
        elif settings.execution_backend == "temporal":
            executor = TemporalExecutor(repository, settings)
            monitor_scheduler = MonitoringScheduler(settings)
            reconciled = executor.recover()
            logger.info(
                "SQLite 初始化完成，网关使用 Temporal backend address=%s "
                "task_queue=%s reconciled_jobs=%s",
                settings.temporal_address,
                settings.temporal_task_queue,
                reconciled,
            )
        else:
            raise ConfigurationError(
                "EXECUTION_BACKEND 仅支持 temporal 或 local"
            )

        handler = EventHandler(
            repository,
            executor,
            messenger,
            monitor_scheduler=monitor_scheduler,
            monitor_patch_expiry_days=settings.monitor_patch_expiry_days,
        )
        events = EventQueue(handler.handle)
        events.start()
        dispatcher = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(events.submit)
            .build()
        )
        ws_client = lark.ws.Client(
            settings.app_id,
            settings.app_secret,
            log_level=lark.LogLevel.INFO,
            event_handler=dispatcher,
        )
        ws_client.on_reconnecting = lambda: logger.warning("飞书长连接已断开，正在重连")
        ws_client.on_reconnected = lambda: logger.info("飞书长连接重连成功")
        stopping = threading.Event()

        def request_stop(signum, _frame):
            if not stopping.is_set():
                logger.info("收到信号 %s，开始优雅退出", signum)
                stopping.set()
                # lark-oapi start() owns its event loop and exits when interrupted.
                raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        try:
            logger.info("正在建立飞书长连接")
            ws_client.start()
        except KeyboardInterrupt:
            pass
        finally:
            events.shutdown()
            if jobs is not None:
                jobs.shutdown()
            repository.close()
            logger.info("服务已关闭")
    finally:
        instance_lock.release()


def main() -> int:
    try:
        settings = Settings.from_env()
    except ConfigurationError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    try:
        run(settings)
    except InstanceAlreadyRunning as exc:
        print(f"启动失败：{exc}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
