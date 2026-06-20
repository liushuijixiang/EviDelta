from __future__ import annotations

import asyncio
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio import activity
from temporalio.client import (
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporalio.worker import Worker

from feishu_agent_bot.config import Settings
from feishu_agent_bot.temporal.client import connect_temporal
from feishu_agent_bot.temporal.models import MonitoringCycleInput
from feishu_agent_bot.temporal.monitoring import (
    MonitoringScheduler,
    ParsedMonitorSchedule,
)
from feishu_agent_bot.temporal.workflows import MonitoringCycleWorkflow


class SmokeActivities:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.starts: list[str] = []
        self.completions: list[str] = []
        self.active = 0
        self.max_active = 0

    def start_count(self) -> int:
        with self._lock:
            return len(self.starts)

    def completion_count(self) -> int:
        with self._lock:
            return len(self.completions)

    def active_count(self) -> int:
        with self._lock:
            return self.active

    def max_active_count(self) -> int:
        with self._lock:
            return self.max_active

    @activity.defn(name="start_monitoring_cycle_activity")
    def start_monitoring_cycle(self, monitor_id: str, workflow_id: str) -> dict:
        with self._lock:
            self.starts.append(workflow_id)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            run_id = f"run-{len(self.starts)}"
        job_id = monitor_id.removeprefix("monitor-")
        return {"run_id": run_id, "job_id": job_id, "monitor_id": monitor_id}

    @activity.defn(name="load_monitor_context_activity")
    def load_monitor_context(self, job_id: str) -> dict:
        return {
            "monitor_id": f"monitor-{job_id}",
            "job_id": job_id,
            "run_id": "run",
            "base_report_version_id": "rv",
            "active_claim_ids": [],
            "competitor_entity_ids": [],
            "watch_target_ids": [],
            "cutoff_from": None,
            "cutoff_to": None,
        }

    @activity.defn(name="create_delta_plan_activity")
    def create_delta_plan(self, job_id: str, context: dict) -> dict:
        return {
            "search_queries": [],
            "watch_target_ids": [],
            "target_event_types": [],
            "cutoff_from": context.get("cutoff_from"),
            "cutoff_to": context.get("cutoff_to"),
            "max_search_requests": 0,
            "max_results_per_query": 0,
            "max_pages": 0,
        }

    @activity.defn(name="recheck_monitored_sources_activity")
    def recheck_monitored_sources(self, job_id: str, delta_plan: dict) -> int:
        time.sleep(2.5)
        return 0

    @activity.defn(name="search_monitoring_sources_activity")
    def search_monitoring_sources(self, job_id: str, delta_plan: dict) -> int:
        return 0

    @activity.defn(name="extract_monitoring_evidence_activity")
    def extract_monitoring_evidence(self, job_id: str) -> int:
        return 0

    @activity.defn(name="profile_datasets_activity")
    def profile_datasets(self, job_id: str) -> int:
        return 0

    @activity.defn(name="detect_monitoring_changes_activity")
    def detect_monitoring_changes(self, job_id: str) -> int:
        return 0

    @activity.defn(name="run_incremental_analysis_activity")
    def run_incremental_analysis(self, job_id: str) -> int:
        return 0

    @activity.defn(name="update_monitoring_report_activity")
    def update_monitoring_report(self, job_id: str) -> str:
        return "no_change"

    @activity.defn(name="validate_monitoring_report_activity")
    def validate_monitoring_report(self, job_id: str, decision: str) -> str:
        return decision

    @activity.defn(name="complete_monitoring_cycle_activity")
    def complete_monitoring_cycle(
        self,
        job_id: str,
        run_id: str,
        decision: str,
        error_message: str | None,
    ) -> str:
        with self._lock:
            self.completions.append(run_id)
            self.active -= 1
        return job_id

    @activity.defn(name="notify_monitoring_cycle_activity")
    def notify_monitoring_cycle(
        self, job_id: str, decision: str, error_message: str | None
    ) -> str:
        return "sent"


async def _wait_for_count(label: str, getter, expected: int) -> None:
    deadline = asyncio.get_running_loop().time() + 180
    while asyncio.get_running_loop().time() < deadline:
        if getter() >= expected:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError(f"{label} did not reach {expected}")


async def _wait_until_idle(activities: SmokeActivities) -> None:
    deadline = asyncio.get_running_loop().time() + 30
    while asyncio.get_running_loop().time() < deadline:
        if activities.active_count() == 0:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError("monitoring cycles did not become idle")


async def main() -> int:
    settings = Settings.from_env(require_credentials=False)
    task_queue = f"monitor-smoke-{uuid.uuid4()}"
    settings = settings.__class__(
        **{
            **settings.__dict__,
            "temporal_task_queue": task_queue,
            "monitor_default_catchup_window_hours": 1,
        }
    )
    client = await connect_temporal(settings)
    scheduler = MonitoringScheduler(settings)
    job_id = f"smoke-{uuid.uuid4().hex[:12]}"
    schedule_id = scheduler.schedule_id(job_id)
    activities = SmokeActivities()
    deleted = False

    with ThreadPoolExecutor(max_workers=4) as activity_executor:
        worker = Worker(
            client,
            task_queue=task_queue,
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
                activities.complete_monitoring_cycle,
                activities.notify_monitoring_cycle,
            ],
            activity_executor=activity_executor,
        )
        async with worker:
            parsed = ParsedMonitorSchedule(
                kind="every",
                value="2s-test-only",
                timezone="UTC",
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(every=timedelta(seconds=2))
                    ]
                ),
                display="every 2s (test only)",
            )
            try:
                await scheduler._create(job_id, parsed)
            except ScheduleAlreadyRunningError:
                await client.get_schedule_handle(schedule_id).delete()
                await scheduler._create(job_id, parsed)
            handle = client.get_schedule_handle(schedule_id)
            try:
                await _wait_for_count(
                    "first monitoring cycle", activities.completion_count, 1
                )
                await _wait_for_count(
                    "second monitoring cycle", activities.completion_count, 2
                )
                if activities.max_active_count() != 1:
                    raise AssertionError("BUFFER_ONE allowed concurrent workflows")

                before_trigger = activities.completion_count()
                await handle.trigger()
                await _wait_for_count(
                    "manually triggered monitoring cycle",
                    activities.completion_count,
                    before_trigger + 1,
                )

                await handle.pause(note="smoke pause")
                await _wait_until_idle(activities)
                await asyncio.sleep(3)
                paused_count = activities.start_count()
                await asyncio.sleep(5)
                if activities.start_count() != paused_count:
                    raise AssertionError("paused schedule started a future workflow")

                await handle.unpause(note="smoke resume")
                resumed_description = await handle.describe()
                if resumed_description.schedule.state.paused:
                    raise AssertionError("schedule remained paused after unpause")

                await handle.delete()
                deleted = True
                await _wait_until_idle(activities)
                deleted_count = activities.start_count()
                await asyncio.sleep(5)
                if activities.start_count() != deleted_count:
                    raise AssertionError("deleted schedule started another workflow")
            finally:
                if not deleted:
                    await handle.delete()
                for workflow_id in list(activities.starts):
                    try:
                        workflow = client.get_workflow_handle(workflow_id)
                        description = await workflow.describe()
                        if description.status.name == "RUNNING":
                            await workflow.cancel()
                    except Exception:
                        pass

    expected_workflow_id_prefix = scheduler.workflow_id(job_id)
    expected_prefix = expected_workflow_id_prefix + "-"
    if len(activities.starts) < 3 or not all(
        workflow_id.startswith(expected_prefix) for workflow_id in activities.starts
    ):
        raise AssertionError(
            "unexpected workflow ids: " + ", ".join(activities.starts)
        )
    print("monitoring_schedule_reuse=ok")
    print(f"schedule_id={schedule_id}")
    print(f"workflow_id_prefix={expected_workflow_id_prefix}")
    print("workflow_ids=" + ",".join(activities.starts))
    print(f"cycles={activities.completion_count()}")
    print(f"max_concurrent={activities.max_active_count()}")
    print("pause_resume_trigger_delete=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
