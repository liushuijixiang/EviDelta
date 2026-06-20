from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

VALID_STATUSES = {
    "queued",
    "running",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
}


@dataclass(frozen=True)
class Job:
    job_id: str
    creator_id: str
    chat_id: str
    source_message_id: str
    topic: str
    status: str
    stage: str
    progress: int
    result_summary: Optional[str]
    error_message: Optional[str]
    cancel_requested: bool
    created_at: str
    updated_at: str
    execution_backend: str = "local"
    temporal_workflow_id: Optional[str] = None
    temporal_run_id: Optional[str] = None
    workflow_status: Optional[str] = None
    paused: bool = False
    last_heartbeat_at: Optional[str] = None
    notification_status: Optional[str] = None
    monitor_registration_status: Optional[str] = None
    monitor_registration_error: Optional[str] = None
    research_options_json: Optional[str] = None


@dataclass(frozen=True)
class MonitoringConfig:
    job_id: str
    creator_id: str
    chat_id: str
    schedule_id: str
    schedule_kind: str
    schedule_value: str
    timezone: str
    mode: str
    notify_level: str
    status: str
    last_success_at: Optional[str]
    last_failure_at: Optional[str]
    last_error: Optional[str]
    last_decision: Optional[str]
    created_at: str
    updated_at: str
    consecutive_failure_count: int = 0
    monitor_id: Optional[str] = None
    owner_id: Optional[str] = None
    temporal_schedule_id: Optional[str] = None
    cadence_type: Optional[str] = None
    interval_seconds: Optional[int] = None
    calendar_json: Optional[str] = None
    update_mode: Optional[str] = None
    catchup_window_seconds: Optional[int] = None
    overlap_policy: Optional[str] = None
    pause_on_failure: int = 1
    last_successful_run_at: Optional[str] = None
    last_failed_run_at: Optional[str] = None
    next_run_at: Optional[str] = None
