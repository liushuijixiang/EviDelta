from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow
from temporalio.worker import Worker


@dataclass
class RecoveryInput:
    run_id: str
    data_dir: str


@dataclass
class RecoveryStatus:
    run_id: str
    stage: str
    progress: int
    report_path: str | None = None


@dataclass
class RecoveryResult:
    run_id: str
    report_path: str
    status: str


@workflow.defn
class MockRecoveryWorkflow:
    def __init__(self) -> None:
        self.run_id = ""
        self.stage = "created"
        self.progress = 0
        self.report_path: str | None = None
        self._continue = False

    @workflow.run
    async def run(self, data: RecoveryInput) -> RecoveryResult:
        self.run_id = data.run_id
        self.stage = "preparing"
        self.progress = 10
        await workflow.execute_activity(
            "recovery_prepare_activity",
            args=[data.run_id, data.data_dir],
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.stage = "waiting_for_restart"
        self.progress = 50
        await workflow.wait_condition(lambda: self._continue)
        self.stage = "generating_report"
        self.progress = 80
        self.report_path = await workflow.execute_activity(
            "recovery_generate_report_activity",
            args=[data.run_id, data.data_dir],
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            "recovery_finalize_activity",
            args=[data.run_id, data.data_dir, self.report_path],
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.stage = "completed"
        self.progress = 100
        return RecoveryResult(
            run_id=data.run_id,
            report_path=self.report_path,
            status="completed",
        )

    @workflow.signal
    async def continue_after_restart(self) -> None:
        self._continue = True

    @workflow.query
    def get_status(self) -> RecoveryStatus:
        return RecoveryStatus(
            run_id=self.run_id,
            stage=self.stage,
            progress=self.progress,
            report_path=self.report_path,
        )


@activity.defn(name="recovery_prepare_activity")
def recovery_prepare(run_id: str, data_dir: str) -> str:
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)
    _append_event(path, "prepare")
    return run_id


@activity.defn(name="recovery_generate_report_activity")
def recovery_generate_report(run_id: str, data_dir: str) -> str:
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)
    report = path / "report-v1.md"
    if not report.exists():
        tmp = report.with_suffix(".tmp")
        tmp.write_text(
            f"# Temporal recovery check\n\nrun_id: {run_id}\n",
            encoding="utf-8",
        )
        tmp.replace(report)
        _append_event(path, "report-created")
    else:
        _append_event(path, "report-reused")
    return str(report)


@activity.defn(name="recovery_finalize_activity")
def recovery_finalize(run_id: str, data_dir: str, report_path: str) -> str:
    path = Path(data_dir)
    _append_event(path, f"finalize:{Path(report_path).name}")
    return run_id


def _append_event(path: Path, event: str) -> None:
    with (path / "events.log").open("a", encoding="utf-8") as handle:
        handle.write(event + "\n")


def _workflow_id(run_id: str) -> str:
    return f"recovery-{run_id}"


async def _worker(task_queue: str) -> None:
    from ..config import Settings
    from .client import connect_temporal

    settings = Settings.from_env(require_credentials=False)
    client = await connect_temporal(settings)
    with ThreadPoolExecutor(max_workers=4) as executor:
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[MockRecoveryWorkflow],
            activities=[
                recovery_prepare,
                recovery_generate_report,
                recovery_finalize,
            ],
            activity_executor=executor,
        )
        async with worker:
            await asyncio.Future()


async def _start(run_id: str, data_dir: str, task_queue: str) -> None:
    from ..config import Settings
    from .client import connect_temporal

    settings = Settings.from_env(require_credentials=False)
    client = await connect_temporal(settings)
    handle = await client.start_workflow(
        MockRecoveryWorkflow.run,
        RecoveryInput(run_id=run_id, data_dir=data_dir),
        id=_workflow_id(run_id),
        task_queue=task_queue,
    )
    print(f"workflow_id={handle.id}")


async def _status(run_id: str) -> None:
    from ..config import Settings
    from .client import connect_temporal

    settings = Settings.from_env(require_credentials=False)
    client = await connect_temporal(settings)
    handle = client.get_workflow_handle(_workflow_id(run_id))
    status = await handle.query(MockRecoveryWorkflow.get_status)
    print(f"run_id={status.run_id}")
    print(f"stage={status.stage}")
    print(f"progress={status.progress}")
    print(f"report_path={status.report_path or ''}")


async def _continue(run_id: str) -> None:
    from ..config import Settings
    from .client import connect_temporal

    settings = Settings.from_env(require_credentials=False)
    client = await connect_temporal(settings)
    handle = client.get_workflow_handle(_workflow_id(run_id))
    await handle.signal(MockRecoveryWorkflow.continue_after_restart)
    result = await handle.result()
    status = result.get("status") if isinstance(result, dict) else result.status
    report_path = (
        result.get("report_path") if isinstance(result, dict) else result.report_path
    )
    print(f"status={status}")
    print(f"report_path={report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Temporal recovery check helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    worker_parser = subparsers.add_parser("worker")
    worker_parser.add_argument("task_queue")
    start_parser = subparsers.add_parser("start")
    start_parser.add_argument("run_id")
    start_parser.add_argument("data_dir")
    start_parser.add_argument("task_queue")
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("run_id")
    continue_parser = subparsers.add_parser("continue")
    continue_parser.add_argument("run_id")
    args = parser.parse_args()
    if args.command == "worker":
        asyncio.run(_worker(args.task_queue))
    elif args.command == "start":
        asyncio.run(_start(args.run_id, args.data_dir, args.task_queue))
    elif args.command == "status":
        asyncio.run(_status(args.run_id))
    elif args.command == "continue":
        asyncio.run(_continue(args.run_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
