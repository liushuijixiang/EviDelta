from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ResearchWorkflowInput:
    job_id: str
    topic: str
    creator_id: str
    chat_id: str
    source_message_id: str
    heartbeat_timeout_seconds: float = 60
    auto_retry_validation: bool = True


@dataclass
class ResearchWorkflowResult:
    job_id: str
    report_version_id: str
    report_path: str
    status: str


@dataclass
class CompleteJobResult:
    summary: str
    report_version_id: str
    report_path: str


@dataclass
class ResearchWorkflowStatus:
    job_id: str
    workflow_id: str
    status: str
    current_stage: str
    progress: int
    paused: bool
    last_error_summary: str | None = None
    report_version_id: str | None = None


@dataclass(frozen=True)
class MonitoringCycleInput:
    monitor_id: str
    schedule_id: str | None = None
    job_id: str | None = None
    heartbeat_timeout_seconds: float = 300


@dataclass(frozen=True)
class MonitoringCycleResult:
    job_id: str
    status: str
    decision: str
