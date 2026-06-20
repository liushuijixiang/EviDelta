from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ActivityError, CancelledError, RetryState
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from feishu_agent_bot.temporal.exceptions import (
    AuthenticationError,
    ReportValidationError,
)
from feishu_agent_bot.temporal.models import (
    CompleteJobResult,
    MonitoringCycleInput,
    ResearchWorkflowInput,
)
from feishu_agent_bot.temporal.workflows import (
    MonitoringCycleWorkflow,
    ResearchWorkflow,
)


class RecordingActivities:
    def __init__(
        self,
        block_initialize: bool = False,
        notify_failures: int = 0,
        planning_failures: int = 0,
        planning_exception: type[Exception] = RuntimeError,
        validation_failures: int = 0,
        validation_exception: type[Exception] = ReportValidationError,
        block_monitor_recheck: bool = False,
    ):
        self.calls = []
        self.initialize_started = threading.Event()
        self.release_initialize = threading.Event()
        self.monitor_recheck_started = threading.Event()
        self.release_monitor_recheck = threading.Event()
        self.block_initialize = block_initialize
        self.block_monitor_recheck = block_monitor_recheck
        self.notify_failures = notify_failures
        self.notify_attempts = 0
        self.planning_failures = planning_failures
        self.planning_exception = planning_exception
        self.planning_attempts = 0
        self.validation_failures = validation_failures
        self.validation_exception = validation_exception
        self.validation_attempts = 0

    def _record(self, name, *args):
        self.calls.append((name, args))

    @activity.defn(name="initialize_job_activity")
    def initialize_job(self, job_id: str) -> str:
        self._record("initialize", job_id)
        self.initialize_started.set()
        if self.block_initialize:
            assert self.release_initialize.wait(timeout=10)
        return job_id

    @activity.defn(name="project_workflow_status_activity")
    def project_workflow_status(
        self, job_id: str, stage: str, progress: int, paused: bool
    ) -> None:
        self._record("project", job_id, stage, progress, paused)

    @activity.defn(name="create_plan_activity")
    def create_plan(self, job_id: str) -> str:
        self.planning_attempts += 1
        self._record("planning", job_id)
        if self.planning_attempts <= self.planning_failures:
            raise self.planning_exception("planning failed")
        return job_id

    @activity.defn(name="search_sources_activity")
    def search_sources(self, job_id: str) -> str:
        self._record("searching", job_id)
        return job_id

    @activity.defn(name="fetch_sources_activity")
    def fetch_sources(self, job_id: str) -> list[str]:
        self._record("fetching", job_id)
        return ["S-001"]

    @activity.defn(name="discover_file_assets_activity")
    def discover_file_assets(self, job_id: str) -> dict:
        self._record("fetching", job_id)
        return {"web_sources": 1, "asset_source_ids": []}

    @activity.defn(name="download_assets_activity")
    def download_assets(self, job_id: str) -> int:
        self._record("download_assets", job_id)
        return 0

    @activity.defn(name="detect_asset_types_activity")
    def detect_asset_types(self, job_id: str) -> int:
        self._record("detect_asset_types", job_id)
        return 0

    @activity.defn(name="parse_assets_activity")
    def parse_assets(self, job_id: str) -> int:
        self._record("parse_assets", job_id)
        return 0

    @activity.defn(name="normalize_datasets_activity")
    def normalize_datasets(self, job_id: str) -> int:
        self._record("normalize_datasets", job_id)
        return 0

    @activity.defn(name="extract_evidence_activity")
    def extract_evidence(self, job_id: str) -> str:
        self._record("extracting_evidence", job_id)
        return job_id

    @activity.defn(name="profile_datasets_activity")
    def profile_datasets(self, job_id: str) -> int:
        self._record("profiling_datasets", job_id)
        return 1

    @activity.defn(name="synthesize_claims_activity")
    def synthesize_claims(self, job_id: str) -> str:
        self._record("synthesizing_claims", job_id)
        return job_id

    @activity.defn(name="run_professional_analysis_activity")
    def run_professional_analysis(self, job_id: str) -> str:
        self._record("analyzing", job_id)
        return job_id

    @activity.defn(name="select_analysis_skills_activity")
    def select_analysis_skills(self, job_id: str) -> dict:
        self._record("analyzing", job_id)
        return {"selected_tools": ["data_quality_summarizer"], "selected_skills": []}

    @activity.defn(name="execute_analysis_tools_activity")
    def execute_analysis_tools(self, job_id: str, analysis_plan: dict) -> str:
        self._record("execute_analysis_tools", job_id, analysis_plan)
        return job_id

    @activity.defn(name="generate_business_analysis_activity")
    def generate_business_analysis(self, job_id: str, analysis_plan: dict) -> str:
        self._record("generate_business_analysis", job_id, analysis_plan)
        return job_id

    @activity.defn(name="generate_report_activity")
    def generate_report(self, job_id: str) -> str:
        self._record("generating_report", job_id)
        return "report-version-1"

    @activity.defn(name="validate_report_activity")
    def validate_report(self, job_id: str) -> str:
        self.validation_attempts += 1
        self._record("validating_report", job_id)
        if self.validation_attempts <= self.validation_failures:
            raise self.validation_exception("report validation failed")
        return "report-version-1"

    @activity.defn(name="generate_professional_artifacts_activity")
    def generate_professional_artifacts(self, job_id: str) -> str:
        self._record("generating_artifacts", job_id)
        return job_id

    @activity.defn(name="build_report_ir_activity")
    def build_report_ir(self, job_id: str) -> str:
        self._record("generating_artifacts", job_id)
        return "report-version-1"

    @activity.defn(name="render_charts_activity")
    def render_charts(self, job_id: str) -> int:
        self._record("render_charts", job_id)
        return 1

    @activity.defn(name="render_latex_activity")
    def render_latex(self, job_id: str) -> str:
        self._record("render_latex", job_id)
        return "ready"

    @activity.defn(name="compile_pdf_activity")
    def compile_pdf(self, job_id: str) -> str:
        self._record("compile_pdf", job_id)
        return "ready"

    @activity.defn(name="render_excel_activity")
    def render_excel(self, job_id: str) -> str:
        self._record("render_excel", job_id)
        return "ready"

    @activity.defn(name="validate_artifacts_activity")
    def validate_artifacts(self, job_id: str) -> str:
        self._record("validate_artifacts", job_id)
        return "ready"

    @activity.defn(name="notify_validation_failed_activity")
    def notify_validation_failed(
        self, job_id: str, error: str, auto_retry: bool, attempt: int
    ) -> str:
        self._record("validation_failed_notice", job_id, error, auto_retry, attempt)
        return "sent"

    @activity.defn(name="reset_report_generation_activity")
    def reset_report_generation(self, job_id: str) -> str:
        self._record("reset_report_generation", job_id)
        return job_id

    @activity.defn(name="complete_job_activity")
    def complete_job(self, job_id: str) -> CompleteJobResult:
        self._record("complete", job_id)
        return CompleteJobResult(
            summary="summary",
            report_version_id="report-version-1",
            report_path="data/artifacts/report_v1.md",
        )

    @activity.defn(name="notify_completion_activity")
    def notify_completion(self, job_id: str) -> str:
        self.notify_attempts += 1
        self._record("notify", job_id)
        if self.notify_attempts <= self.notify_failures:
            raise RuntimeError("temporary notify failure")
        return "sent"

    @activity.defn(name="deliver_artifacts_activity")
    def deliver_artifacts(self, job_id: str) -> str:
        return self.notify_completion(job_id)

    @activity.defn(name="register_monitoring_schedule_activity")
    def register_monitoring_schedule(self, job_id: str) -> str:
        self._record("monitor_registration", job_id)
        return "skipped"

    @activity.defn(name="mark_job_failed_activity")
    def mark_job_failed(self, job_id: str, error: str) -> None:
        self._record("failed", job_id, error)

    @activity.defn(name="mark_job_cancelled_activity")
    def mark_job_cancelled(self, job_id: str) -> None:
        self._record("cancelled", job_id)

    @activity.defn(name="start_monitoring_cycle_activity")
    def start_monitoring_cycle(self, monitor_id: str, workflow_id: str) -> dict:
        self._record("monitor_start", monitor_id, workflow_id)
        return {
            "run_id": "monitor-run-1",
            "job_id": "job-monitor",
            "monitor_id": monitor_id,
        }

    @activity.defn(name="load_monitor_context_activity")
    def load_monitor_context(self, job_id: str) -> dict:
        self._record("monitor_context", job_id)
        return {
            "monitor_id": "monitor-job-monitor",
            "job_id": job_id,
            "run_id": "monitor-run-1",
            "base_report_version_id": "rv-1",
            "active_claim_ids": ["C-001"],
            "competitor_entity_ids": ["示例公司"],
            "watch_target_ids": ["wt-1"],
            "cutoff_from": "2026-06-17T00:00:00+00:00",
            "cutoff_to": "2026-06-18T00:00:00+00:00",
        }

    @activity.defn(name="create_delta_plan_activity")
    def create_delta_plan(self, job_id: str, context: dict) -> dict:
        self._record("monitor_delta_plan", job_id, context["base_report_version_id"])
        return {
            "search_queries": ["示例公司 news after:2026-06-17"],
            "watch_target_ids": ["wt-1"],
            "target_event_types": ["feature_added"],
            "cutoff_from": context["cutoff_from"],
            "cutoff_to": context["cutoff_to"],
            "max_search_requests": 1,
            "max_results_per_query": 3,
            "max_pages": 2,
        }

    @activity.defn(name="recheck_monitored_sources_activity")
    def recheck_monitored_sources(self, job_id: str, delta_plan: dict) -> int:
        self._record("monitor_recheck", job_id, delta_plan["watch_target_ids"])
        self.monitor_recheck_started.set()
        if self.block_monitor_recheck:
            assert self.release_monitor_recheck.wait(timeout=10)
        return 1

    @activity.defn(name="search_monitoring_sources_activity")
    def search_monitoring_sources(self, job_id: str, delta_plan: dict) -> int:
        self._record("monitor_search", job_id, delta_plan["search_queries"])
        return 1

    @activity.defn(name="extract_monitoring_evidence_activity")
    def extract_monitoring_evidence(self, job_id: str) -> int:
        self._record("monitor_evidence", job_id)
        return 2

    @activity.defn(name="detect_monitoring_changes_activity")
    def detect_monitoring_changes(self, job_id: str) -> int:
        self._record("monitor_changes", job_id)
        return 2

    @activity.defn(name="run_incremental_analysis_activity")
    def run_incremental_analysis(self, job_id: str) -> int:
        self._record("monitor_analysis", job_id)
        return 3

    @activity.defn(name="update_monitoring_report_activity")
    def update_monitoring_report(self, job_id: str) -> str:
        self._record("monitor_report", job_id)
        return "auto_patch: 生成报告 v2"

    @activity.defn(name="validate_monitoring_report_activity")
    def validate_monitoring_report(self, job_id: str, decision: str) -> str:
        self._record("monitor_validate", job_id, decision)
        return decision

    @activity.defn(name="complete_monitoring_cycle_activity")
    def complete_monitoring_cycle(
        self,
        job_id: str,
        run_id: str,
        decision: str,
        error_message: str | None,
    ) -> str:
        self._record("monitor_complete", job_id, run_id, decision, error_message)
        return job_id

    @activity.defn(name="notify_monitoring_cycle_activity")
    def notify_monitoring_cycle(
        self, job_id: str, decision: str, error_message: str | None
    ) -> str:
        self._record("monitor_notify", job_id, decision, error_message)
        return "sent"


def test_research_workflow_completes_in_stage_order():
    asyncio.run(_workflow_completes_in_stage_order())


def test_research_workflow_pause_blocks_next_stage_until_resume():
    asyncio.run(_workflow_pause_blocks_next_stage_until_resume())


def test_research_workflow_cancel_projects_cancelled_state():
    asyncio.run(_workflow_cancel_projects_cancelled_state())


def test_research_workflow_detects_activity_cancelled_error():
    exc = ActivityError(
        "Activity cancelled",
        scheduled_event_id=1,
        started_event_id=2,
        identity="test-worker",
        activity_type="create_plan_activity",
        activity_id="activity-1",
        retry_state=RetryState.CANCEL_REQUESTED,
    )
    exc.__cause__ = CancelledError()

    assert ResearchWorkflow()._is_cancelled_exception(exc) is True


def test_research_workflow_retries_notification_then_completes():
    asyncio.run(_workflow_retries_notification_then_completes())


def test_research_workflow_retries_temporary_planning_failure():
    asyncio.run(_workflow_retries_temporary_planning_failure())


def test_research_workflow_does_not_retry_authentication_error():
    asyncio.run(_workflow_does_not_retry_authentication_error())


def test_research_workflow_does_not_retry_report_validation_error():
    asyncio.run(_workflow_does_not_retry_report_validation_error())


def test_research_workflow_auto_retries_report_validation_error():
    asyncio.run(_workflow_auto_retries_report_validation_error())


def test_monitoring_cycle_workflow_runs_finite_cycle():
    asyncio.run(_monitoring_cycle_workflow_runs_finite_cycle())


async def _workflow_completes_in_stage_order():
    activities = RecordingActivities()
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                await _run_workflow(env, activities)
    finally:
        await env.shutdown()


async def _run_workflow(env, activities):
    handle = await env.client.start_workflow(
        ResearchWorkflow.run,
        ResearchWorkflowInput(
            job_id="job-1",
            topic="测试主题",
            creator_id="u1",
            chat_id="c1",
            source_message_id="m1",
        ),
        id="research-job-1",
        task_queue="test-research",
    )
    result = await handle.result()
    status = await handle.query(ResearchWorkflow.get_status)

    assert result.job_id == "job-1"
    assert result.report_version_id == "report-version-1"
    assert result.report_path == "data/artifacts/report_v1.md"
    assert result.status == "completed"
    assert status.status == "completed"
    assert status.current_stage == "completed"
    assert status.progress == 100
    names = [name for name, _ in activities.calls if name != "project"]
    assert names[:6] == [
        "initialize",
        "planning",
        "searching",
        "fetching",
        "download_assets",
        "detect_asset_types",
    ]
    for required in [
        "parse_assets",
        "normalize_datasets",
        "extracting_evidence",
        "profiling_datasets",
        "synthesizing_claims",
        "analyzing",
        "execute_analysis_tools",
        "generate_business_analysis",
        "generating_report",
        "validating_report",
        "generating_artifacts",
        "render_charts",
        "render_latex",
        "compile_pdf",
        "render_excel",
        "validate_artifacts",
        "complete",
        "monitor_registration",
        "notify",
    ]:
        assert required in names
    assert names.index("download_assets") < names.index("detect_asset_types")
    assert names.index("detect_asset_types") < names.index("parse_assets")
    assert names.index("parse_assets") < names.index("normalize_datasets")
    assert names.index("normalize_datasets") < names.index("extracting_evidence")
    assert names.index("analyzing") < names.index("execute_analysis_tools")
    assert names.index("analyzing") < names.index("generate_business_analysis")
    assert names.index("execute_analysis_tools") < names.index("generating_report")
    assert names.index("generate_business_analysis") < names.index("generating_report")
    assert names.index("generating_artifacts") < names.index("render_charts")
    assert names.index("generating_artifacts") < names.index("render_latex")
    assert names.index("render_charts") < names.index("compile_pdf")
    assert names.index("render_latex") < names.index("compile_pdf")
    assert names.index("render_charts") < names.index("render_excel")
    assert names.index("render_latex") < names.index("render_excel")
    assert names.index("compile_pdf") < names.index("validate_artifacts")
    assert names.index("render_excel") < names.index("validate_artifacts")
    assert names.index("validate_artifacts") < names.index("complete")
    assert all(
        len(str(args)) < 200 for name, args in activities.calls if name != "project"
    )


async def _workflow_pause_blocks_next_stage_until_resume():
    activities = RecordingActivities(block_initialize=True)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-pause",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-pause",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-pause",
                    task_queue="test-research-pause",
                )
                assert await asyncio.to_thread(
                    activities.initialize_started.wait, 5
                )
                await handle.signal(ResearchWorkflow.pause)
                activities.release_initialize.set()
                for _ in range(20):
                    status = await handle.query(ResearchWorkflow.get_status)
                    if status.paused:
                        break
                    await asyncio.sleep(0.05)
                assert status.paused is True
                assert "planning" not in [
                    name for name, _ in activities.calls
                ]
                await handle.signal(ResearchWorkflow.resume)
                result = await handle.result()
    finally:
        await env.shutdown()

    assert result.status == "completed"
    assert "planning" in [name for name, _ in activities.calls]


async def _workflow_cancel_projects_cancelled_state():
    activities = RecordingActivities(block_initialize=True)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-cancel",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-cancel",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-cancel",
                    task_queue="test-research-cancel",
                )
                assert await asyncio.to_thread(
                    activities.initialize_started.wait, 5
                )
                await handle.signal(ResearchWorkflow.pause)
                activities.release_initialize.set()
                for _ in range(20):
                    status = await handle.query(ResearchWorkflow.get_status)
                    if status.paused:
                        break
                    await asyncio.sleep(0.05)
                await handle.cancel()
                try:
                    await handle.result()
                except WorkflowFailureError:
                    pass
                status = await handle.query(ResearchWorkflow.get_status)
    finally:
        await env.shutdown()

    assert ("cancelled", ("job-cancel",)) in activities.calls
    assert "complete" not in [name for name, _ in activities.calls]
    assert status.status == "cancelled"
    assert status.current_stage == "cancelled"
    assert status.progress == 100


async def _workflow_retries_notification_then_completes():
    activities = RecordingActivities(notify_failures=2)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-notify-retry",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-notify-retry",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-notify-retry",
                    task_queue="test-research-notify-retry",
                )
                result = await handle.result()
    finally:
        await env.shutdown()

    assert result.status == "completed"
    assert activities.notify_attempts == 3
    assert "failed" not in [name for name, _ in activities.calls]


async def _workflow_retries_temporary_planning_failure():
    activities = RecordingActivities(planning_failures=1)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-planning-retry",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-planning-retry",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-planning-retry",
                    task_queue="test-research-planning-retry",
                )
                result = await handle.result()
    finally:
        await env.shutdown()

    assert result.status == "completed"
    assert activities.planning_attempts == 2
    assert [name for name, _ in activities.calls].count("planning") == 2
    assert "failed" not in [name for name, _ in activities.calls]


async def _workflow_does_not_retry_authentication_error():
    activities = RecordingActivities(
        planning_failures=1,
        planning_exception=AuthenticationError,
    )
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-planning-auth",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-planning-auth",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-planning-auth",
                    task_queue="test-research-planning-auth",
                )
                try:
                    await handle.result()
                except WorkflowFailureError:
                    pass
    finally:
        await env.shutdown()

    assert activities.planning_attempts == 1
    assert [name for name, _ in activities.calls].count("planning") == 1
    assert [name for name, _ in activities.calls].count("failed") == 1
    assert "searching" not in [name for name, _ in activities.calls]


async def _workflow_does_not_retry_report_validation_error():
    activities = RecordingActivities(validation_failures=1)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-report-validation",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-report-validation",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                        auto_retry_validation=False,
                    ),
                    id="research-job-report-validation",
                    task_queue="test-research-report-validation",
                )
                try:
                    await handle.result()
                except WorkflowFailureError:
                    pass
    finally:
        await env.shutdown()

    assert activities.validation_attempts == 1
    assert [name for name, _ in activities.calls].count("validating_report") == 1
    assert [name for name, _ in activities.calls].count("failed") == 1
    assert [name for name, _ in activities.calls].count("validation_failed_notice") == 1
    assert "reset_report_generation" not in [name for name, _ in activities.calls]
    assert "complete" not in [name for name, _ in activities.calls]


async def _workflow_auto_retries_report_validation_error():
    activities = RecordingActivities(validation_failures=1)
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-research-report-validation-retry",
                workflows=[ResearchWorkflow],
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
                    activities.complete_job,
                    activities.notify_completion,
                    activities.deliver_artifacts,
                    activities.register_monitoring_schedule,
                    activities.notify_validation_failed,
                    activities.reset_report_generation,
                    activities.mark_job_failed,
                    activities.mark_job_cancelled,
                ],
                activity_executor=executor,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ResearchWorkflow.run,
                    ResearchWorkflowInput(
                        job_id="job-report-validation-retry",
                        topic="测试主题",
                        creator_id="u1",
                        chat_id="c1",
                        source_message_id="m1",
                    ),
                    id="research-job-report-validation-retry",
                    task_queue="test-research-report-validation-retry",
                )
                result = await handle.result()
    finally:
        await env.shutdown()

    assert result.status == "completed"
    assert activities.validation_attempts == 2
    names = [name for name, _ in activities.calls]
    assert names.count("validating_report") == 2
    assert names.count("validation_failed_notice") == 1
    assert names.count("reset_report_generation") == 1
    assert names.count("complete") == 1
    assert "failed" not in names


async def _monitoring_cycle_workflow_runs_finite_cycle():
    activities = RecordingActivities()
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            worker = Worker(
                env.client,
                task_queue="test-monitoring-cycle",
                workflows=[MonitoringCycleWorkflow],
                activities=[
                    activities.start_monitoring_cycle,
                    activities.load_monitor_context,
                    activities.create_delta_plan,
                    activities.recheck_monitored_sources,
                    activities.search_monitoring_sources,
                    activities.extract_monitoring_evidence,
                    activities.profile_datasets,
                    activities.detect_monitoring_changes,
                    activities.run_incremental_analysis,
                    activities.update_monitoring_report,
                    activities.validate_monitoring_report,
                    activities.generate_professional_artifacts,
                    activities.build_report_ir,
                    activities.render_charts,
                    activities.render_latex,
                    activities.compile_pdf,
                    activities.render_excel,
                    activities.validate_artifacts,
                    activities.complete_monitoring_cycle,
                    activities.notify_monitoring_cycle,
                ],
                activity_executor=executor,
            )
            async with worker:
                result = await env.client.execute_workflow(
                    MonitoringCycleWorkflow.run,
                    MonitoringCycleInput(
                        monitor_id="monitor-job-monitor",
                    ),
                    id="monitor-job-monitor-cycle",
                    task_queue="test-monitoring-cycle",
                )
    finally:
        await env.shutdown()

    assert result.status == "completed"
    assert result.decision == "auto_patch: 生成报告 v2"
    names = [name for name, _ in activities.calls]
    assert names[:12] == [
        "monitor_start",
        "monitor_context",
        "monitor_delta_plan",
        "monitor_recheck",
        "monitor_search",
        "monitor_evidence",
        "profiling_datasets",
        "monitor_changes",
        "monitor_analysis",
        "monitor_report",
        "monitor_validate",
        "generating_artifacts",
    ]
    for required in [
        "render_charts",
        "render_latex",
        "compile_pdf",
        "render_excel",
        "validate_artifacts",
        "monitor_complete",
        "monitor_notify",
    ]:
        assert required in names
    assert names.index("generating_artifacts") < names.index("render_charts")
    assert names.index("generating_artifacts") < names.index("render_latex")
    assert names.index("render_charts") < names.index("compile_pdf")
    assert names.index("render_latex") < names.index("compile_pdf")
    assert names.index("render_charts") < names.index("render_excel")
    assert names.index("render_latex") < names.index("render_excel")
    assert names.index("compile_pdf") < names.index("validate_artifacts")
    assert names.index("render_excel") < names.index("validate_artifacts")
    assert names.index("validate_artifacts") < names.index("monitor_complete")
    assert names.index("monitor_complete") < names.index("monitor_notify")
