from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from ..models import Job


class AgentCancelled(Exception):
    pass


@dataclass(frozen=True)
class AgentResult:
    summary: str
    report_path: str | None = None
    report_json_path: str | None = None
    source_count: int = 0
    evidence_count: int = 0
    claim_count: int = 0
    report_version: int | None = None
    key_claims: tuple[str, ...] = ()


class AgentBackend(Protocol):
    def run(
        self,
        job: Job,
        progress_callback: Callable[[str, int], None],
        cancellation_check: Callable[[], bool],
    ) -> AgentResult:
        ...
