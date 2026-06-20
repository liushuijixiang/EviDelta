from __future__ import annotations

import logging

from temporalio.client import WorkflowExecutionStatus, WorkflowQueryRejectedError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from feishu_agent_bot.config import Settings
from feishu_agent_bot.event_handler import EventHandler
from feishu_agent_bot.execution.temporal_executor import TemporalExecutor
from feishu_agent_bot.temporal.exceptions import TemporalUnavailable
from feishu_agent_bot.temporal.models import ResearchWorkflowStatus

from conftest import FakeMessenger
from test_event_handler import valid_event


def settings(tmp_path):
    return Settings(
        app_id="cli_real",
        app_secret="secret",
        database_path=tmp_path / "test.db",
        execution_backend="temporal",
    )


def test_temporal_workflow_id_is_stable(repository, tmp_path):
    executor = TemporalExecutor(repository, settings(tmp_path))
    assert executor.workflow_id_for("job-1") == "research-job-1"


def test_temporal_unavailable_marks_research_failed(
    repository, tmp_path, monkeypatch
):
    async def fail(_settings):
        raise TemporalUnavailable("Temporal 不可用：无法连接 localhost:7233")

    monkeypatch.setattr(
        "feishu_agent_bot.execution.temporal_executor.connect_temporal",
        fail,
    )
    executor = TemporalExecutor(repository, settings(tmp_path))
    messenger = FakeMessenger()
    handler = EventHandler(repository, executor, messenger)

    handler.handle(
        valid_event(message_id="m1", text="/research 测试主题 --depth standard")
    )

    assert "Temporal 不可用" in messenger.replies[0][1]
    jobs = repository.list_recoverable_jobs()
    assert jobs == []


def test_temporal_submit_handles_existing_workflow(repository, tmp_path, monkeypatch):
    async def already_started(self, job, workflow_id):
        raise WorkflowAlreadyStartedError(workflow_id, "ResearchWorkflow")

    monkeypatch.setattr(TemporalExecutor, "_start_workflow", already_started)
    executor = TemporalExecutor(repository, settings(tmp_path))
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )

    result = executor.submit(job)

    updated = repository.get_job(job.job_id)
    assert result.accepted is True
    assert result.workflow_id == f"research-{job.job_id}"
    assert updated.temporal_workflow_id == result.workflow_id
    assert updated.execution_backend == "temporal"


def test_temporal_submit_passes_effective_heartbeat_timeout_setting(
    repository, tmp_path, monkeypatch
):
    captured = {}

    class FakeHandle:
        first_execution_run_id = "run-1"

    class FakeClient:
        async def start_workflow(self, workflow, data, id, task_queue):
            captured["data"] = data
            captured["workflow_id"] = id
            captured["task_queue"] = task_queue
            return FakeHandle()

    async def connect(_settings):
        return FakeClient()

    monkeypatch.setattr(
        "feishu_agent_bot.execution.temporal_executor.connect_temporal",
        connect,
    )
    custom_settings = settings(tmp_path)
    custom_settings = custom_settings.__class__(
        **{
            **custom_settings.__dict__,
            "temporal_heartbeat_timeout_seconds": 17,
            "llm_timeout_seconds": 90,
            "fetch_timeout_seconds": 20,
        }
    )
    executor = TemporalExecutor(repository, custom_settings)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )

    result = executor.submit(job)

    assert result.accepted is True
    assert captured["data"].heartbeat_timeout_seconds == 120
    assert captured["workflow_id"] == f"research-{job.job_id}"


def test_effective_heartbeat_timeout_respects_larger_explicit_setting(
    repository, tmp_path
):
    custom_settings = settings(tmp_path)
    custom_settings = custom_settings.__class__(
        **{
            **custom_settings.__dict__,
            "temporal_heartbeat_timeout_seconds": 200,
            "llm_timeout_seconds": 90,
            "fetch_timeout_seconds": 20,
        }
    )

    assert TemporalExecutor(
        repository, custom_settings
    )._effective_heartbeat_timeout() == 200


def test_status_falls_back_to_sqlite_when_temporal_unavailable(
    repository, tmp_path, monkeypatch
):
    async def fail(_settings):
        raise TemporalUnavailable("Temporal 不可用：无法连接 localhost:7233")

    monkeypatch.setattr(
        "feishu_agent_bot.execution.temporal_executor.connect_temporal",
        fail,
    )
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)
    repository.update_progress(job.job_id, "fetching", 30)

    status = TemporalExecutor(repository, settings(tmp_path)).status(job.job_id)

    assert status.realtime_unavailable is True
    assert status.workflow_id == f"research-{job.job_id}"
    assert status.stage == "fetching"
    assert status.progress == 30


def test_status_uses_sqlite_projection_when_workflow_is_closed(
    repository, tmp_path, monkeypatch
):
    async def closed(self, workflow_id):
        raise WorkflowQueryRejectedError(WorkflowExecutionStatus.COMPLETED)

    async def describe(self, workflow_id):
        return WorkflowExecutionStatus.COMPLETED

    monkeypatch.setattr(TemporalExecutor, "_query_workflow", closed)
    monkeypatch.setattr(TemporalExecutor, "_describe_workflow", describe)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)
    repository.complete_job(job.job_id, "done")

    status = TemporalExecutor(repository, settings(tmp_path)).status(job.job_id)

    assert status.realtime_unavailable is False
    assert status.status == "completed"
    assert status.stage == "已完成"
    assert status.progress == 100
    assert status.workflow_id == f"research-{job.job_id}"


def test_status_marks_missing_temporal_workflow_failed(
    repository, tmp_path, monkeypatch
):
    async def missing(self, workflow_id):
        raise RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")

    monkeypatch.setattr(TemporalExecutor, "_query_workflow", missing)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)

    status = TemporalExecutor(repository, settings(tmp_path)).status(job.job_id)

    updated = repository.get_job(job.job_id)
    assert status.status == "failed"
    assert status.realtime_unavailable is False
    assert updated.workflow_status == "not_found"
    assert "不存在" in updated.error_message


def test_recover_reconciles_active_temporal_jobs(repository, tmp_path, monkeypatch):
    running = repository.create_job(
        "u1", "c1", "m1", "running", execution_backend="temporal"
    )
    cancelled = repository.create_job(
        "u1", "c1", "m2", "cancelled", execution_backend="temporal"
    )
    missing = repository.create_job(
        "u1", "c1", "m3", "missing", execution_backend="temporal"
    )
    for job in (running, cancelled, missing):
        repository.bind_temporal_workflow(
            job.job_id, f"research-{job.job_id}", "run-1"
        )
        repository.start_job(job.job_id)

    async def inspect(self, jobs):
        by_topic = {job.topic: job for job in jobs}
        return [
            (
                by_topic["running"],
                "running",
                ResearchWorkflowStatus(
                    job_id=by_topic["running"].job_id,
                    workflow_id=f"research-{by_topic['running'].job_id}",
                    status="running",
                    current_stage="analyzing",
                    progress=78,
                    paused=False,
                ),
            ),
            (
                by_topic["cancelled"],
                "closed",
                WorkflowExecutionStatus.CANCELED,
            ),
            (
                by_topic["missing"],
                "missing",
                RPCError("not found", RPCStatusCode.NOT_FOUND, b""),
            ),
        ]

    monkeypatch.setattr(TemporalExecutor, "_inspect_active_workflows", inspect)
    reconciled = TemporalExecutor(repository, settings(tmp_path)).recover()

    assert reconciled == 3
    refreshed = repository.get_job(running.job_id)
    assert refreshed.status == "running"
    assert refreshed.stage == "analyzing"
    assert refreshed.progress == 78
    assert repository.get_job(cancelled.job_id).status == "cancelled"
    assert repository.get_job(missing.job_id).status == "failed"


def test_recover_preserves_projection_when_temporal_is_unavailable(
    repository, tmp_path, monkeypatch
):
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)
    repository.update_progress(job.job_id, "fetching", 30)

    async def unavailable(self, jobs):
        raise TemporalUnavailable("Temporal unavailable")

    monkeypatch.setattr(
        TemporalExecutor, "_inspect_active_workflows", unavailable
    )
    reconciled = TemporalExecutor(repository, settings(tmp_path)).recover()

    unchanged = repository.get_job(job.job_id)
    assert reconciled == 0
    assert unchanged.status == "running"
    assert unchanged.stage == "fetching"
    assert unchanged.progress == 30


def test_status_projects_failed_workflow_to_sqlite(
    repository, tmp_path, monkeypatch
):
    async def failed(self, workflow_id):
        return ResearchWorkflowStatus(
            job_id=workflow_id.removeprefix("research-"),
            workflow_id=workflow_id,
            status="failed",
            current_stage="extracting_evidence",
            progress=50,
            paused=False,
            last_error_summary="Activity task timed out",
        )

    monkeypatch.setattr(TemporalExecutor, "_query_workflow", failed)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)

    status = TemporalExecutor(repository, settings(tmp_path)).status(job.job_id)

    updated = repository.get_job(job.job_id)
    assert status.status == "failed"
    assert updated.status == "failed"
    assert updated.error_message == "Activity task timed out"


def test_status_projects_cancelled_workflow_to_sqlite(
    repository, tmp_path, monkeypatch
):
    async def cancelled(self, workflow_id):
        return ResearchWorkflowStatus(
            job_id=workflow_id.removeprefix("research-"),
            workflow_id=workflow_id,
            status="cancelled",
            current_stage="cancelled",
            progress=100,
            paused=False,
        )

    monkeypatch.setattr(TemporalExecutor, "_query_workflow", cancelled)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)

    status = TemporalExecutor(repository, settings(tmp_path)).status(job.job_id)

    updated = repository.get_job(job.job_id)
    assert status.status == "cancelled"
    assert updated.status == "cancelled"


def test_temporal_cancel_failure_does_not_mark_sqlite_cancelled(
    repository, tmp_path, monkeypatch
):
    async def fail_cancel(self, workflow_id):
        raise TemporalUnavailable("Temporal 不可用：无法连接 localhost:7233")

    monkeypatch.setattr(TemporalExecutor, "_cancel_workflow", fail_cancel)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)

    result = TemporalExecutor(repository, settings(tmp_path)).cancel(
        job.job_id, "u1"
    )

    updated = repository.get_job(job.job_id)
    assert result == "temporal_unavailable"
    assert updated.status == "running"
    assert updated.cancel_requested is False


def test_temporal_cancel_requested_retries_workflow_cancel(
    repository, tmp_path, monkeypatch
):
    calls = []

    async def record_cancel(self, workflow_id):
        calls.append(workflow_id)

    monkeypatch.setattr(TemporalExecutor, "_cancel_workflow", record_cancel)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)
    repository.cancel_job(job.job_id, "u1")

    result = TemporalExecutor(repository, settings(tmp_path)).cancel(
        job.job_id, "u1"
    )

    assert result == "cancel_requested"
    assert calls == [f"research-{job.job_id}"]


def test_temporal_user_actions_are_audit_logged(
    repository, tmp_path, monkeypatch, caplog
):
    async def record_signal(self, workflow_id, signal):
        return None

    async def record_cancel(self, workflow_id):
        return None

    monkeypatch.setattr(TemporalExecutor, "_signal_workflow", record_signal)
    monkeypatch.setattr(TemporalExecutor, "_cancel_workflow", record_cancel)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    workflow_id = f"research-{job.job_id}"
    repository.bind_temporal_workflow(job.job_id, workflow_id, "run-1")
    repository.start_job(job.job_id)
    executor = TemporalExecutor(repository, settings(tmp_path))

    with caplog.at_level(
        logging.INFO,
        logger="feishu_agent_bot.execution.temporal_executor",
    ):
        assert executor.pause(job.job_id, "u1") == "paused"
        assert executor.cancel(job.job_id, "u1") == "cancel_requested"

    pause_log = next(
        record
        for record in caplog.records
        if record.getMessage() == "用户暂停状态审计"
    )
    cancel_log = next(
        record
        for record in caplog.records
        if record.getMessage() == "用户取消任务审计"
    )
    assert pause_log.job_id == job.job_id
    assert pause_log.requester_id == "u1"
    assert pause_log.workflow_id == workflow_id
    assert pause_log.action == "pause"
    assert pause_log.result == "paused"
    assert cancel_log.job_id == job.job_id
    assert cancel_log.requester_id == "u1"
    assert cancel_log.workflow_id == workflow_id
    assert cancel_log.action == "cancel"
    assert cancel_log.result == "cancel_requested"


def test_temporal_pause_failure_does_not_mark_sqlite_paused(
    repository, tmp_path, monkeypatch
):
    async def fail_signal(self, workflow_id, signal):
        raise TemporalUnavailable("Temporal 不可用：无法连接 localhost:7233")

    monkeypatch.setattr(TemporalExecutor, "_signal_workflow", fail_signal)
    job = repository.create_job(
        "u1", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.bind_temporal_workflow(job.job_id, f"research-{job.job_id}", "run-1")
    repository.start_job(job.job_id)

    result = TemporalExecutor(repository, settings(tmp_path)).pause(
        job.job_id, "u1"
    )

    updated = repository.get_job(job.job_id)
    assert result == "temporal_unavailable"
    assert updated.paused is False
    assert updated.stage == "开始处理"


def test_ping_and_help_do_not_touch_temporal(repository):
    class ExplodingExecutor:
        backend_name = "temporal"

        def submit(self, _job):
            raise AssertionError("should not touch temporal")

    messenger = FakeMessenger()
    handler = EventHandler(repository, ExplodingExecutor(), messenger)
    handler.handle(valid_event(message_id="m1", text="/ping"))
    handler.handle(valid_event(message_id="m2", text="/help"))

    assert messenger.replies[0][1].startswith("pong")
    assert "可用命令" in messenger.replies[1][1]
