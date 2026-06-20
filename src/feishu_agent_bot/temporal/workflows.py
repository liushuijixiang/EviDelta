from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import CancelledError as TemporalCancelledError

from .models import (
    MonitoringCycleInput,
    MonitoringCycleResult,
    ResearchWorkflowInput,
    ResearchWorkflowResult,
    ResearchWorkflowStatus,
)
from .retry_policies import activity_retry

MIN_HEARTBEAT_TIMEOUT_SECONDS = 300
MAX_VALIDATION_ATTEMPTS = 3


@workflow.defn
class ResearchWorkflow:
    def __init__(self) -> None:
        self.input: ResearchWorkflowInput | None = None
        self.paused = False
        self.current_stage = "initialize"
        self.progress = 0
        self.status = "running"
        self.last_error_summary: str | None = None
        self.report_version_id: str | None = None

    @workflow.run
    async def run(self, data: ResearchWorkflowInput) -> ResearchWorkflowResult:
        self.input = data
        try:
            await self._activity("initialize_job_activity", data.job_id, 30, 3)
            await self._pause_gate()
            await self._stage("planning", 5)
            await self._activity("create_plan_activity", data.job_id, 180, 2)
            await self._pause_gate()
            await self._stage("searching", 15)
            await self._activity("search_sources_activity", data.job_id, 180, 3)
            await self._pause_gate()
            await self._stage("fetching", 30)
            await self._acquire_sources(data.job_id)
            await self._pause_gate()
            await self._stage("extracting_evidence", 50)
            await self._activity(
                "extract_evidence_activity",
                data.job_id,
                1800,
                2,
                heartbeat=True,
            )
            if workflow.patched("profile-datasets-activity-v1"):
                await self._pause_gate()
                await self._stage("profiling_datasets", 60)
                await self._activity(
                    "profile_datasets_activity",
                    data.job_id,
                    600,
                    2,
                    heartbeat=True,
                )
            await self._pause_gate()
            await self._synthesize_report_until_valid(data)
            await self._pause_gate()
            await self._stage("generating_artifacts", 97)
            await self._generate_professional_artifacts(data.job_id)
            completed = await self._activity(
                "complete_job_activity", data.job_id, 30, 3
            )
            await self._activity(
                "register_monitoring_schedule_activity", data.job_id, 60, 1
            )
            await self._stage("notifying", 99)
            if workflow.patched("deliver-artifacts-activity-v1"):
                await self._activity("deliver_artifacts_activity", data.job_id, 60, 3)
            else:
                await self._activity("notify_completion_activity", data.job_id, 60, 3)
            self.status = "completed"
            await self._stage("completed", 100)
            report_path = (
                completed.get("report_path")
                if isinstance(completed, dict)
                else completed.report_path
            )
            return ResearchWorkflowResult(
                job_id=data.job_id,
                report_version_id=self.report_version_id or "",
                report_path=report_path,
                status="completed",
            )
        except asyncio.CancelledError:
            await self._mark_cancelled(data.job_id)
            raise
        except Exception as exc:
            if self._is_cancelled_exception(exc):
                await self._mark_cancelled(data.job_id)
                raise asyncio.CancelledError() from exc
            self.status = "failed"
            self.last_error_summary = str(exc)[:300]
            await workflow.execute_activity(
                "mark_job_failed_activity",
                args=[data.job_id, self.last_error_summary],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=activity_retry(1),
            )
            raise

    async def _synthesize_report_until_valid(
        self, data: ResearchWorkflowInput
    ) -> None:
        validation_attempt = 1
        while True:
            await self._stage("synthesizing_claims", 70)
            await self._activity("synthesize_claims_activity", data.job_id, 300, 2)
            await self._pause_gate()
            await self._stage("analyzing", 78)
            await self._run_professional_analysis(data.job_id)
            await self._pause_gate()
            await self._stage("generating_report", 85)
            report_version_id = await self._activity(
                "generate_report_activity", data.job_id, 300, 2
            )
            self.report_version_id = str(report_version_id)
            await self._pause_gate()
            await self._stage("validating_report", 95)
            try:
                await self._activity("validate_report_activity", data.job_id, 120, 1)
                return
            except Exception as exc:
                if not self._is_report_validation_exception(exc):
                    raise
                error = str(exc)[:300]
                auto_retry = (
                    data.auto_retry_validation
                    and validation_attempt < MAX_VALIDATION_ATTEMPTS
                )
                await workflow.execute_activity(
                    "notify_validation_failed_activity",
                    args=[data.job_id, error, auto_retry, validation_attempt],
                    start_to_close_timeout=timedelta(seconds=60),
                    retry_policy=activity_retry(3),
                )
                if not auto_retry:
                    raise
                await workflow.execute_activity(
                    "reset_report_generation_activity",
                    args=[data.job_id],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=activity_retry(3),
                )
                validation_attempt += 1
                await self._pause_gate()

    async def _stage(self, stage: str, progress: int) -> None:
        self.current_stage = stage
        self.progress = progress
        workflow.logger.info(
            "ResearchWorkflow stage changed job_id=%s workflow_id=%s "
            "stage=%s progress=%s paused=%s",
            self.input.job_id,
            workflow.info().workflow_id,
            stage,
            progress,
            self.paused,
        )
        await workflow.execute_activity(
            "project_workflow_status_activity",
            args=[self.input.job_id, stage, progress, self.paused],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=activity_retry(3),
        )

    async def _activity(
        self,
        name: str,
        job_id: str,
        timeout_seconds: int,
        max_attempts: int,
        *,
        heartbeat: bool = False,
    ):
        heartbeat_timeout = None
        if heartbeat:
            heartbeat_timeout = timedelta(
                seconds=max(
                    self.input.heartbeat_timeout_seconds,
                    MIN_HEARTBEAT_TIMEOUT_SECONDS,
                )
            )
        return await workflow.execute_activity(
            name,
            args=[job_id],
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=activity_retry(max_attempts),
        )

    async def _generate_professional_artifacts(self, job_id: str) -> None:
        if not workflow.patched("split-professional-artifacts-v1"):
            await self._activity(
                "generate_professional_artifacts_activity",
                job_id,
                600,
                2,
                heartbeat=True,
            )
            return
        await self._activity("build_report_ir_activity", job_id, 120, 2)
        await asyncio.gather(
            self._activity(
                "render_charts_activity", job_id, 600, 2, heartbeat=True
            ),
            self._activity("render_latex_activity", job_id, 120, 2),
        )
        await asyncio.gather(
            self._activity(
                "compile_pdf_activity", job_id, 900, 2, heartbeat=True
            ),
            self._activity("render_excel_activity", job_id, 300, 2),
        )
        await self._activity("validate_artifacts_activity", job_id, 120, 2)

    async def _acquire_sources(self, job_id: str) -> None:
        if not workflow.patched("split-source-assets-v1"):
            await self._activity(
                "fetch_sources_activity", job_id, 1200, 3, heartbeat=True
            )
            return
        await self._activity(
            "discover_file_assets_activity", job_id, 1200, 3, heartbeat=True
        )
        await self._activity(
            "download_assets_activity", job_id, 1200, 3, heartbeat=True
        )
        await self._activity(
            "detect_asset_types_activity", job_id, 120, 2, heartbeat=True
        )
        await self._activity(
            "parse_assets_activity", job_id, 1800, 2, heartbeat=True
        )
        await self._activity(
            "normalize_datasets_activity", job_id, 600, 2, heartbeat=True
        )

    async def _run_professional_analysis(self, job_id: str) -> None:
        if not workflow.patched("split-professional-analysis-v1"):
            await self._activity(
                "run_professional_analysis_activity",
                job_id,
                600,
                2,
                heartbeat=True,
            )
            return
        analysis_plan = await self._activity(
            "select_analysis_skills_activity", job_id, 120, 2
        )
        heartbeat_timeout = timedelta(
            seconds=max(
                self.input.heartbeat_timeout_seconds,
                MIN_HEARTBEAT_TIMEOUT_SECONDS,
            )
        )
        await asyncio.gather(
            workflow.execute_activity(
                "execute_analysis_tools_activity",
                args=[job_id, analysis_plan],
                start_to_close_timeout=timedelta(seconds=600),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            ),
            workflow.execute_activity(
                "generate_business_analysis_activity",
                args=[job_id, analysis_plan],
                start_to_close_timeout=timedelta(seconds=600),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            ),
        )

    async def _pause_gate(self) -> None:
        await workflow.wait_condition(lambda: not self.paused)

    async def _mark_cancelled(self, job_id: str) -> None:
        self.status = "cancelled"
        self.current_stage = "cancelled"
        self.progress = 100
        await asyncio.shield(
            workflow.execute_activity(
                "mark_job_cancelled_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=activity_retry(3),
            )
        )

    def _is_cancelled_exception(self, exc: BaseException) -> bool:
        current: BaseException | None = exc
        while current is not None:
            if isinstance(current, (asyncio.CancelledError, TemporalCancelledError)):
                return True
            current = (
                getattr(current, "cause", None)
                or getattr(current, "__cause__", None)
            )
        return False

    def _is_report_validation_exception(self, exc: BaseException) -> bool:
        current: BaseException | None = exc
        while current is not None:
            if current.__class__.__name__ == "ReportValidationError" or (
                current.__class__.__name__ == "ApplicationError"
                and "ReportValidationError" in str(current)
            ):
                return True
            current = (
                getattr(current, "cause", None)
                or getattr(current, "__cause__", None)
            )
        return False

    @workflow.signal
    async def pause(self) -> None:
        self.paused = True
        workflow.logger.info(
            "ResearchWorkflow paused job_id=%s workflow_id=%s stage=%s",
            self.input.job_id if self.input else "",
            workflow.info().workflow_id,
            self.current_stage,
        )

    @workflow.signal
    async def resume(self) -> None:
        self.paused = False
        workflow.logger.info(
            "ResearchWorkflow resumed job_id=%s workflow_id=%s stage=%s",
            self.input.job_id if self.input else "",
            workflow.info().workflow_id,
            self.current_stage,
        )

    @workflow.query
    def get_status(self) -> ResearchWorkflowStatus:
        workflow_id = workflow.info().workflow_id
        return ResearchWorkflowStatus(
            job_id=self.input.job_id if self.input else "",
            workflow_id=workflow_id,
            status=self.status,
            current_stage=self.current_stage,
            progress=self.progress,
            paused=self.paused,
            last_error_summary=self.last_error_summary,
            report_version_id=self.report_version_id,
        )


@workflow.defn
class MonitoringCycleWorkflow:
    @workflow.run
    async def run(self, data: MonitoringCycleInput) -> MonitoringCycleResult:
        workflow_id = workflow.info().workflow_id
        start_context = await workflow.execute_activity(
            "start_monitoring_cycle_activity",
            args=[data.monitor_id, workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=activity_retry(3),
        )
        if isinstance(start_context, dict):
            run_id = str(start_context["run_id"])
            job_id = str(start_context["job_id"])
        else:
            run_id = str(start_context)
            if not data.job_id:
                raise ValueError("monitoring start did not resolve job_id")
            job_id = data.job_id
        try:
            heartbeat_timeout = timedelta(
                seconds=max(
                    MIN_HEARTBEAT_TIMEOUT_SECONDS,
                    data.heartbeat_timeout_seconds,
                )
            )
            context = await workflow.execute_activity(
                "load_monitor_context_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=activity_retry(3),
            )
            delta_plan = await workflow.execute_activity(
                "create_delta_plan_activity",
                args=[job_id, context],
                start_to_close_timeout=timedelta(seconds=180),
                retry_policy=activity_retry(2),
            )
            await workflow.execute_activity(
                "recheck_monitored_sources_activity",
                args=[job_id, delta_plan],
                start_to_close_timeout=timedelta(seconds=1200),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(3),
            )
            await workflow.execute_activity(
                "search_monitoring_sources_activity",
                args=[job_id, delta_plan],
                start_to_close_timeout=timedelta(seconds=300),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(3),
            )
            await workflow.execute_activity(
                "extract_monitoring_evidence_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=1800),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            )
            if workflow.patched("monitor-profile-datasets-activity-v1"):
                await workflow.execute_activity(
                    "profile_datasets_activity",
                    args=[job_id],
                    start_to_close_timeout=timedelta(seconds=600),
                    heartbeat_timeout=heartbeat_timeout,
                    retry_policy=activity_retry(2),
                )
            await workflow.execute_activity(
                "detect_monitoring_changes_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=600),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            )
            await workflow.execute_activity(
                "run_incremental_analysis_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=300),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            )
            decision = await workflow.execute_activity(
                "update_monitoring_report_activity",
                args=[job_id],
                start_to_close_timeout=timedelta(seconds=300),
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=activity_retry(2),
            )
            decision = await workflow.execute_activity(
                "validate_monitoring_report_activity",
                args=[job_id, decision],
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=activity_retry(1),
            )
            if (
                decision.startswith("auto_patch")
                and workflow.patched("monitor-split-professional-artifacts-v1")
            ):
                await workflow.execute_activity(
                    "build_report_ir_activity",
                    args=[job_id],
                    start_to_close_timeout=timedelta(seconds=120),
                    retry_policy=activity_retry(2),
                )
                await asyncio.gather(
                    workflow.execute_activity(
                        "render_charts_activity",
                        args=[job_id],
                        start_to_close_timeout=timedelta(seconds=600),
                        heartbeat_timeout=heartbeat_timeout,
                        retry_policy=activity_retry(2),
                    ),
                    workflow.execute_activity(
                        "render_latex_activity",
                        args=[job_id],
                        start_to_close_timeout=timedelta(seconds=120),
                        retry_policy=activity_retry(2),
                    ),
                )
                await asyncio.gather(
                    workflow.execute_activity(
                        "compile_pdf_activity",
                        args=[job_id],
                        start_to_close_timeout=timedelta(seconds=900),
                        heartbeat_timeout=heartbeat_timeout,
                        retry_policy=activity_retry(2),
                    ),
                    workflow.execute_activity(
                        "render_excel_activity",
                        args=[job_id],
                        start_to_close_timeout=timedelta(seconds=300),
                        retry_policy=activity_retry(2),
                    ),
                )
                await workflow.execute_activity(
                    "validate_artifacts_activity",
                    args=[job_id],
                    start_to_close_timeout=timedelta(seconds=120),
                    retry_policy=activity_retry(2),
                )
            elif decision.startswith("auto_patch"):
                await workflow.execute_activity(
                    "generate_professional_artifacts_activity",
                    args=[job_id],
                    start_to_close_timeout=timedelta(seconds=600),
                    heartbeat_timeout=heartbeat_timeout,
                    retry_policy=activity_retry(2),
                )
            await workflow.execute_activity(
                "complete_monitoring_cycle_activity",
                args=[job_id, run_id, decision, None],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=activity_retry(3),
            )
            await workflow.execute_activity(
                "notify_monitoring_cycle_activity",
                args=[job_id, decision, None],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=activity_retry(3),
            )
            return MonitoringCycleResult(
                job_id=job_id, status="completed", decision=decision
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                workflow.execute_activity(
                    "complete_monitoring_cycle_activity",
                    args=[job_id, run_id, "cancelled", None],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=activity_retry(3),
                )
            )
            raise
        except Exception as exc:
            error = str(exc)[:300]
            await workflow.execute_activity(
                "complete_monitoring_cycle_activity",
                args=[job_id, run_id, "failed", error],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=activity_retry(3),
            )
            await workflow.execute_activity(
                "notify_monitoring_cycle_activity",
                args=[job_id, "failed", error],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=activity_retry(3),
            )
            raise
