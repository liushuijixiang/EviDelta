from __future__ import annotations

import asyncio
import logging

from temporalio.client import (
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowQueryRejectedError,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from .base import ExecutionStatus, SubmitResult
from ..config import Settings
from ..models import Job
from ..repository import Repository
from ..temporal.client import connect_temporal
from ..temporal.exceptions import TemporalUnavailable
from ..temporal.models import ResearchWorkflowInput
from ..temporal.workflows import ResearchWorkflow

logger = logging.getLogger(__name__)


class TemporalExecutor:
    backend_name = "temporal"

    def __init__(self, repository: Repository, settings: Settings):
        self.repository = repository
        self.settings = settings

    def submit(self, job: Job) -> SubmitResult:
        workflow_id = self.workflow_id_for(job.job_id)
        try:
            run_id = asyncio.run(self._start_workflow(job, workflow_id))
        except TemporalUnavailable as exc:
            self.repository.fail_job(job.job_id, str(exc))
            return SubmitResult(False, str(exc), workflow_id)
        except WorkflowAlreadyStartedError:
            self.repository.bind_temporal_workflow(job.job_id, workflow_id)
            return SubmitResult(True, "任务已存在，已关联现有 Workflow", workflow_id)
        self.repository.bind_temporal_workflow(job.job_id, workflow_id, run_id)
        return SubmitResult(True, "任务已接收", workflow_id)

    async def _start_workflow(self, job: Job, workflow_id: str) -> str | None:
        client = await connect_temporal(self.settings)
        handle = await client.start_workflow(
            ResearchWorkflow.run,
            ResearchWorkflowInput(
                job_id=job.job_id,
                topic=job.topic,
                creator_id=job.creator_id,
                chat_id=job.chat_id,
                source_message_id=job.source_message_id,
                heartbeat_timeout_seconds=self._effective_heartbeat_timeout(),
                auto_retry_validation=(
                    "[no-auto-retry-validation]" not in job.source_message_id
                ),
            ),
            id=workflow_id,
            task_queue=self.settings.temporal_task_queue,
        )
        return getattr(handle, "first_execution_run_id", None)

    def status(self, job_id: str) -> ExecutionStatus | None:
        job = self.repository.get_job(job_id)
        if not job:
            return None
        if not job.temporal_workflow_id:
            return self._sqlite_status(job)
        try:
            workflow_status = asyncio.run(
                self._query_workflow(job.temporal_workflow_id)
            )
            if workflow_status.status == "failed" and job.status not in {
                "completed",
                "failed",
                "cancelled",
            }:
                self.repository.fail_job(
                    job.job_id,
                    workflow_status.last_error_summary or "Workflow failed",
                )
            elif workflow_status.status == "cancelled" and job.status not in {
                "completed",
                "failed",
                "cancelled",
            }:
                self.repository.mark_cancelled(job.job_id)
            return ExecutionStatus(
                job_id=job.job_id,
                status=workflow_status.status,
                stage=workflow_status.current_stage,
                progress=workflow_status.progress,
                paused=workflow_status.paused,
                workflow_id=workflow_status.workflow_id,
                error=workflow_status.last_error_summary,
            )
        except WorkflowQueryRejectedError as exc:
            logger.info(
                "Temporal Workflow 已关闭，使用 SQLite 投影状态 "
                "job_id=%s workflow_id=%s workflow_status=%s",
                job_id,
                job.temporal_workflow_id,
                exc.status,
            )
            try:
                temporal_status = asyncio.run(
                    self._describe_workflow(job.temporal_workflow_id)
                )
                return self._project_closed_status(job, temporal_status)
            except RPCError as describe_exc:
                if self._is_not_found(describe_exc):
                    return self._mark_workflow_missing(job)
                return self._unavailable_sqlite_status(job)
            except Exception:
                return self._unavailable_sqlite_status(job)
        except RPCError as exc:
            if self._is_not_found(exc):
                return self._mark_workflow_missing(job)
            logger.warning(
                "Temporal 状态查询 RPC 失败 job_id=%s workflow_id=%s error=%s",
                job_id,
                job.temporal_workflow_id,
                exc,
            )
            return self._unavailable_sqlite_status(job)
        except Exception as exc:
            logger.warning(
                "Temporal 状态查询失败 job_id=%s workflow_id=%s error=%s",
                job_id,
                job.temporal_workflow_id,
                exc,
            )
            return self._unavailable_sqlite_status(job)

    async def _query_workflow(self, workflow_id: str):
        client = await connect_temporal(self.settings)
        handle = client.get_workflow_handle(workflow_id)
        return await handle.query(ResearchWorkflow.get_status)

    async def _describe_workflow(self, workflow_id: str) -> WorkflowExecutionStatus:
        client = await connect_temporal(self.settings)
        description = await client.get_workflow_handle(workflow_id).describe()
        return description.status

    def cancel(self, job_id: str, requester_id: str) -> str:
        job, result = self._authorize_job_action(job_id, requester_id)
        if result:
            logger.info(
                "用户取消任务审计",
                extra={
                    "job_id": job_id,
                    "requester_id": requester_id,
                    "action": "cancel",
                    "result": result,
                },
            )
            return result
        assert job is not None
        if job.temporal_workflow_id:
            try:
                asyncio.run(self._cancel_workflow(job.temporal_workflow_id))
            except Exception:
                logger.warning(
                    "Temporal Workflow 取消请求失败 job_id=%s workflow_id=%s",
                    job_id,
                    job.temporal_workflow_id,
                    exc_info=True,
                )
                return "temporal_unavailable"
        result = self.repository.cancel_job(job_id, requester_id)
        logger.info(
            "用户取消任务审计",
            extra={
                "job_id": job_id,
                "requester_id": requester_id,
                "workflow_id": job.temporal_workflow_id,
                "action": "cancel",
                "result": result,
            },
        )
        return result

    async def _cancel_workflow(self, workflow_id: str) -> None:
        client = await connect_temporal(self.settings)
        handle = client.get_workflow_handle(workflow_id)
        try:
            await handle.cancel()
        except WorkflowFailureError:
            pass

    def pause(self, job_id: str, requester_id: str) -> str:
        return self._signal_pause_state(job_id, requester_id, True)

    def resume(self, job_id: str, requester_id: str) -> str:
        return self._signal_pause_state(job_id, requester_id, False)

    def _signal_pause_state(
        self, job_id: str, requester_id: str, paused: bool
    ) -> str:
        job, result = self._authorize_job_action(job_id, requester_id)
        if result:
            logger.info(
                "用户暂停状态审计",
                extra={
                    "job_id": job_id,
                    "requester_id": requester_id,
                    "action": "pause" if paused else "resume",
                    "result": result,
                },
            )
            return result
        assert job is not None
        if job.temporal_workflow_id:
            try:
                asyncio.run(
                    self._signal_workflow(
                        job.temporal_workflow_id, "pause" if paused else "resume"
                    )
                )
            except Exception:
                logger.warning(
                    "Temporal Workflow 暂停状态 signal 失败 "
                    "job_id=%s workflow_id=%s paused=%s",
                    job_id,
                    job.temporal_workflow_id,
                    paused,
                    exc_info=True,
                )
                return "temporal_unavailable"
        result = (
            self.repository.pause_job(job_id, requester_id)
            if paused
            else self.repository.resume_job(job_id, requester_id)
        )
        logger.info(
            "用户暂停状态审计",
            extra={
                "job_id": job_id,
                "requester_id": requester_id,
                "workflow_id": job.temporal_workflow_id,
                "action": "pause" if paused else "resume",
                "result": result,
            },
        )
        return result

    async def _signal_workflow(self, workflow_id: str, signal: str) -> None:
        client = await connect_temporal(self.settings)
        handle = client.get_workflow_handle(workflow_id)
        await handle.signal(signal)

    def recover(self) -> int:
        jobs = self.repository.list_active_temporal_jobs()
        if not jobs:
            return 0
        try:
            states = asyncio.run(self._inspect_active_workflows(jobs))
        except Exception as exc:
            logger.warning("Temporal 启动状态核对失败，保留 SQLite 投影 error=%s", exc)
            return 0
        reconciled = 0
        for job, state_kind, state in states:
            if state_kind == "unavailable":
                continue
            if state_kind == "missing":
                self._mark_workflow_missing(job)
            elif state_kind == "running":
                self.repository.update_workflow_projection(
                    job.job_id,
                    workflow_status="paused" if state.paused else "running",
                    paused=state.paused,
                    stage=state.current_stage,
                    progress=state.progress,
                )
            else:
                self._project_closed_status(job, state)
            reconciled += 1
        return reconciled

    async def _inspect_active_workflows(
        self, jobs: list[Job]
    ) -> list[tuple[Job, str, object]]:
        client = await connect_temporal(self.settings)
        states: list[tuple[Job, str, object]] = []
        for job in jobs:
            assert job.temporal_workflow_id is not None
            handle = client.get_workflow_handle(job.temporal_workflow_id)
            try:
                description = await handle.describe()
                if description.status in {
                    WorkflowExecutionStatus.RUNNING,
                    WorkflowExecutionStatus.CONTINUED_AS_NEW,
                }:
                    try:
                        current = await handle.query(ResearchWorkflow.get_status)
                        states.append((job, "running", current))
                    except WorkflowQueryRejectedError:
                        description = await handle.describe()
                        states.append((job, "closed", description.status))
                else:
                    states.append((job, "closed", description.status))
            except RPCError as exc:
                states.append(
                    (job, "missing" if self._is_not_found(exc) else "unavailable", exc)
                )
            except Exception as exc:
                states.append((job, "unavailable", exc))
        return states

    def shutdown(self) -> None:
        return None

    def workflow_id_for(self, job_id: str) -> str:
        return f"{self.settings.temporal_research_workflow_prefix}-{job_id}"

    def _effective_heartbeat_timeout(self) -> float:
        return max(
            self.settings.temporal_heartbeat_timeout_seconds,
            self.settings.llm_timeout_seconds + 30,
            self.settings.fetch_timeout_seconds + 30,
        )

    def _authorize_job_action(
        self, job_id: str, requester_id: str
    ) -> tuple[Job | None, str | None]:
        job = self.repository.get_job(job_id)
        if not job:
            return None, "not_found"
        if job.creator_id != requester_id:
            return job, "forbidden"
        if job.status in {"completed", "failed", "cancelled"}:
            return job, "terminal"
        return job, None

    @staticmethod
    def _sqlite_status(job: Job) -> ExecutionStatus:
        return ExecutionStatus(
            job_id=job.job_id,
            status=job.status,
            stage=job.stage,
            progress=job.progress,
            paused=job.paused,
            workflow_id=job.temporal_workflow_id,
            error=job.error_message,
        )

    def _project_closed_status(
        self, job: Job, temporal_status: WorkflowExecutionStatus
    ) -> ExecutionStatus:
        if temporal_status == WorkflowExecutionStatus.COMPLETED:
            if job.status not in {"completed", "failed", "cancelled"}:
                self.repository.complete_job(
                    job.job_id,
                    job.result_summary or "Temporal Workflow 已完成",
                )
            workflow_status = "completed"
        elif temporal_status == WorkflowExecutionStatus.CANCELED:
            if job.status not in {"completed", "failed", "cancelled"}:
                self.repository.mark_cancelled(job.job_id)
            workflow_status = "cancelled"
        elif temporal_status in {
            WorkflowExecutionStatus.FAILED,
            WorkflowExecutionStatus.TIMED_OUT,
            WorkflowExecutionStatus.TERMINATED,
        }:
            workflow_status = temporal_status.name.lower()
            if job.status not in {"completed", "failed", "cancelled"}:
                self.repository.fail_job(
                    job.job_id,
                    f"Temporal Workflow {workflow_status}",
                )
        else:
            return self._sqlite_status(job)
        self.repository.update_workflow_projection(
            job.job_id,
            workflow_status=workflow_status,
            paused=False,
        )
        return self._sqlite_status(self.repository.get_job(job.job_id) or job)

    def _mark_workflow_missing(self, job: Job) -> ExecutionStatus:
        error = "Temporal Workflow 不存在，无法继续执行"
        if job.status not in {"completed", "failed", "cancelled"}:
            self.repository.fail_job(job.job_id, error)
        self.repository.update_workflow_projection(
            job.job_id,
            workflow_status="not_found",
            paused=False,
        )
        logger.error(
            "Temporal Workflow 不存在，已终止陈旧任务投影 job_id=%s workflow_id=%s",
            job.job_id,
            job.temporal_workflow_id,
        )
        return self._sqlite_status(self.repository.get_job(job.job_id) or job)

    def _unavailable_sqlite_status(self, job: Job) -> ExecutionStatus:
        status = self._sqlite_status(job)
        return ExecutionStatus(**{**status.__dict__, "realtime_unavailable": True})

    @staticmethod
    def _is_not_found(exc: RPCError) -> bool:
        return exc.status == RPCStatusCode.NOT_FOUND
