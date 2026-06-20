from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..models import Job


@dataclass(frozen=True)
class SubmitResult:
    accepted: bool
    message: str
    workflow_id: str | None = None


@dataclass(frozen=True)
class ExecutionStatus:
    job_id: str
    status: str
    stage: str
    progress: int
    paused: bool = False
    workflow_id: str | None = None
    realtime_unavailable: bool = False
    error: str | None = None


class ResearchExecutor(Protocol):
    def submit(self, job: Job) -> SubmitResult:
        ...

    def status(self, job_id: str) -> ExecutionStatus | None:
        ...

    def cancel(self, job_id: str, requester_id: str) -> str:
        ...

    def pause(self, job_id: str, requester_id: str) -> str:
        ...

    def resume(self, job_id: str, requester_id: str) -> str:
        ...

    def recover(self) -> int:
        ...

    def shutdown(self) -> None:
        ...
