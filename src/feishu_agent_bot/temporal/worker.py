from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import logging
import signal

from temporalio.worker import Worker

from ..config import ConfigurationError, Settings
from ..feishu_client import FeishuMessenger, build_api_client
from ..logging_config import configure_logging
from ..main import build_research_backend
from ..repository import Repository
from .activities import ResearchActivities
from .client import connect_temporal
from .exceptions import TemporalUnavailable
from .monitoring import MonitoringScheduler
from .workflows import MonitoringCycleWorkflow, ResearchWorkflow

logger = logging.getLogger(__name__)


async def run_worker(settings: Settings) -> None:
    configure_logging(settings.log_level)
    repository = Repository(settings.database_path)
    repository.initialize()
    # Temporal owns external-call retries. Keep provider retries disabled here to
    # avoid provider retry N times multiplied by Activity retry N times.
    backend = build_research_backend(settings, repository, llm_max_retries=0)
    api_client = build_api_client(settings.app_id, settings.app_secret)
    messenger = FeishuMessenger(
        api_client,
        settings.message_max_length,
        settings.feishu_file_max_bytes,
    )
    monitor_scheduler = MonitoringScheduler(settings)
    activities = ResearchActivities(
        repository=repository,
        backend=backend,
        messenger=messenger,
        monitor_scheduler=monitor_scheduler,
        pdf_enabled=settings.pdf_enabled,
        latex_engine=settings.latex_engine,
        latexmk_path=settings.latexmk_path,
        pdf_timeout_seconds=settings.pdf_compile_timeout_seconds,
        pdf_max_output_bytes=settings.pdf_max_output_bytes,
        pdf_template=settings.pdf_template,
        max_analysis_concurrency=settings.max_concurrent_analysis_tools,
        artifact_concurrency=settings.professional_artifact_concurrency,
        max_charts_per_report=settings.max_charts_per_report,
        max_tables_per_report=settings.max_tables_per_report,
        max_concurrent_downloads=settings.max_concurrent_downloads,
        max_concurrent_parsers=settings.max_concurrent_parsers,
        max_concurrent_ocr_jobs=settings.max_concurrent_ocr_jobs,
        max_concurrent_profile_jobs=settings.max_concurrent_profile_jobs,
        max_concurrent_charts=settings.max_concurrent_charts,
    )
    client = await connect_temporal(settings)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)
    with ThreadPoolExecutor(max_workers=settings.worker_count) as executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[ResearchWorkflow, MonitoringCycleWorkflow],
            activities=[
                activities.initialize_job,
                activities.project_workflow_status,
                activities.create_plan,
                activities.search_sources,
                activities.fetch_sources,
                activities.discover_file_assets,
                activities.download_assets,
                activities.detect_asset_types,
                activities.parse_assets,
                activities.normalize_datasets,
                activities.profile_datasets,
                activities.extract_evidence,
                activities.synthesize_claims,
                activities.run_professional_analysis,
                activities.select_analysis_skills,
                activities.execute_analysis_tools,
                activities.generate_business_analysis,
                activities.generate_report,
                activities.validate_report,
                activities.generate_professional_artifacts,
                activities.build_report_ir,
                activities.render_charts,
                activities.render_latex,
                activities.compile_pdf,
                activities.render_excel,
                activities.validate_artifacts,
                activities.notify_validation_failed,
                activities.reset_report_generation,
                activities.complete_job,
                activities.notify_completion,
                activities.deliver_artifacts,
                activities.register_monitoring_schedule,
                activities.mark_job_failed,
                activities.mark_job_cancelled,
                activities.start_monitoring_cycle,
                activities.load_monitor_context,
                activities.create_delta_plan,
                activities.recheck_monitored_sources,
                activities.search_monitoring_sources,
                activities.extract_monitoring_evidence,
                activities.detect_monitoring_changes,
                activities.run_incremental_analysis,
                activities.update_monitoring_report,
                activities.validate_monitoring_report,
                activities.complete_monitoring_cycle,
            activities.notify_monitoring_cycle,
            ],
            activity_executor=executor,
            graceful_shutdown_timeout=timedelta(seconds=10),
        )
        logger.info(
            "Temporal Worker 启动 address=%s namespace=%s task_queue=%s",
            settings.temporal_address,
            settings.temporal_namespace,
            settings.temporal_task_queue,
        )
        async with worker:
            await stop_event.wait()
    repository.close()


def main() -> int:
    try:
        settings = Settings.from_env()
    except ConfigurationError as exc:
        print(f"配置错误：{exc}")
        return 2
    try:
        asyncio.run(run_worker(settings))
        return 0
    except TemporalUnavailable as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
