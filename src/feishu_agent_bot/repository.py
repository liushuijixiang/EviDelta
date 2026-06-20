from __future__ import annotations

import json
import hashlib
import shutil
import sqlite3
import threading
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .llm.schemas import (
    ClaimItem,
    EvidenceItem,
    ResearchPlan,
    StoredClaim,
    StoredEvidence,
)
from .models import Job, MonitoringConfig, VALID_STATUSES

SCHEMA_VERSION = 28


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dataclass_payload(value):
    if value is None:
        return None
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


class Repository:
    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError("repository is closed")
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        self._backup_before_migration()
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            existing_version = connection.execute("PRAGMA user_version").fetchone()[0]
            if existing_version >= SCHEMA_VERSION:
                integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
                if integrity != "ok":
                    raise RuntimeError(f"SQLite 完整性检查失败: {integrity}")
                return
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS processed_messages (
                    message_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL,
                    received_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    creator_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    source_message_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'queued', 'running', 'completed', 'failed',
                        'cancel_requested', 'cancelled'
                    )),
                    stage TEXT NOT NULL,
                    progress INTEGER NOT NULL CHECK(progress BETWEEN 0 AND 100),
                    result_summary TEXT,
                    error_message TEXT,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    execution_backend TEXT NOT NULL DEFAULT 'local',
                    temporal_workflow_id TEXT,
                    temporal_run_id TEXT,
                    workflow_status TEXT,
                    paused INTEGER NOT NULL DEFAULT 0,
                    last_heartbeat_at TEXT,
                    notification_status TEXT,
                    monitor_registration_status TEXT,
                    monitor_registration_error TEXT,
                    research_options_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_creator ON jobs(creator_id);
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_source_message
                    ON jobs(source_message_id);
                CREATE TABLE IF NOT EXISTS research_plans (
                    job_id TEXT PRIMARY KEY,
                    objective TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    source_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    publisher TEXT,
                    published_at TEXT,
                    retrieved_at TEXT NOT NULL,
                    search_query TEXT NOT NULL,
                    search_rank INTEGER NOT NULL,
                    http_status INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    raw_text TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    error_message TEXT,
                    PRIMARY KEY(job_id, source_id),
                    UNIQUE(job_id, canonical_url),
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    evidence_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    snapshot_id TEXT,
                    entity TEXT NOT NULL,
                    attribute TEXT NOT NULL,
                    value TEXT NOT NULL,
                    exact_quote TEXT NOT NULL,
                    evidence_type TEXT NOT NULL,
                    observed_at TEXT,
                    confidence_band TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, evidence_id),
                    UNIQUE(job_id, source_id, entity, attribute, exact_quote),
                    FOREIGN KEY(job_id, source_id)
                        REFERENCES sources(job_id, source_id)
                );
                CREATE TABLE IF NOT EXISTS claims (
                    claim_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    statement TEXT NOT NULL,
                    claim_type TEXT NOT NULL,
                    confidence_band TEXT NOT NULL,
                    reasoning_summary TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, claim_id),
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS claim_evidence (
                    job_id TEXT NOT NULL,
                    claim_id TEXT NOT NULL,
                    evidence_id TEXT NOT NULL,
                    relation TEXT NOT NULL CHECK(relation IN (
                        'support', 'contradict', 'context'
                    )),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, claim_id, evidence_id, relation),
                    FOREIGN KEY(job_id, claim_id)
                        REFERENCES claims(job_id, claim_id),
                    FOREIGN KEY(job_id, evidence_id)
                        REFERENCES evidence(job_id, evidence_id)
                );
                CREATE TABLE IF NOT EXISTS report_versions (
                    report_version_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    report_path TEXT NOT NULL,
                    report_json_path TEXT NOT NULL,
                    parent_report_version_id TEXT,
                    monitor_run_id TEXT,
                    trigger_type TEXT NOT NULL DEFAULT 'initial_research',
                    change_summary TEXT,
                    status TEXT NOT NULL DEFAULT 'published',
                    publication_status TEXT NOT NULL DEFAULT 'published',
                    published_at TEXT,
                    validation_error TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(job_id, version),
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS monitoring_configs (
                    job_id TEXT PRIMARY KEY,
                    monitor_id TEXT UNIQUE,
                    creator_id TEXT NOT NULL,
                    owner_id TEXT,
                    chat_id TEXT NOT NULL,
                    schedule_id TEXT NOT NULL UNIQUE,
                    temporal_schedule_id TEXT UNIQUE,
                    schedule_kind TEXT NOT NULL,
                    cadence_type TEXT,
                    schedule_value TEXT NOT NULL,
                    interval_seconds INTEGER,
                    calendar_json TEXT,
                    timezone TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    update_mode TEXT,
                    notify_level TEXT NOT NULL,
                    status TEXT NOT NULL,
                    catchup_window_seconds INTEGER,
                    overlap_policy TEXT NOT NULL DEFAULT 'BUFFER_ONE',
                    pause_on_failure INTEGER NOT NULL DEFAULT 1,
                    last_success_at TEXT,
                    last_successful_run_at TEXT,
                    last_failure_at TEXT,
                    last_failed_run_at TEXT,
                    next_run_at TEXT,
                    last_error TEXT,
                    last_decision TEXT,
                    consecutive_failure_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS monitor_registration_requests (
                    job_id TEXT PRIMARY KEY,
                    creator_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    schedule_kind TEXT NOT NULL,
                    schedule_value TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    notify_level TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS research_drafts (
                    draft_id TEXT PRIMARY KEY,
                    creator_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    source_message_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    options_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitoring_runs (
                    run_id TEXT PRIMARY KEY,
                    monitor_run_id TEXT UNIQUE,
                    monitor_id TEXT,
                    job_id TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    temporal_workflow_id TEXT,
                    workflow_run_id TEXT,
                    temporal_run_id TEXT,
                    scheduled_for TEXT,
                    cutoff_from TEXT,
                    cutoff_to TEXT,
                    status TEXT NOT NULL,
                    stage TEXT,
                    new_source_count INTEGER NOT NULL DEFAULT 0,
                    changed_source_count INTEGER NOT NULL DEFAULT 0,
                    new_evidence_count INTEGER NOT NULL DEFAULT 0,
                    search_request_count INTEGER NOT NULL DEFAULT 0,
                    fetched_page_count INTEGER NOT NULL DEFAULT 0,
                    llm_call_count INTEGER NOT NULL DEFAULT 0,
                    change_event_count INTEGER NOT NULL DEFAULT 0,
                    affected_claim_count INTEGER NOT NULL DEFAULT 0,
                    decision TEXT,
                    base_report_version_id TEXT,
                    result_report_version_id TEXT,
                    draft_patch_id TEXT,
                    error_message TEXT,
                    error_summary TEXT,
                    started_at TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT '',
                    completed_at TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS monitoring_source_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    run_id TEXT,
                    monitor_run_id TEXT,
                    url TEXT NOT NULL,
                    http_status INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    raw_text TEXT,
                    raw_object_path TEXT,
                    published_at TEXT,
                    retrieval_method TEXT,
                    observed_at TEXT NOT NULL,
                    retrieved_at TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT,
                    FOREIGN KEY(job_id, source_id)
                        REFERENCES sources(job_id, source_id)
                );
                CREATE TABLE IF NOT EXISTS monitoring_watch_targets (
                    watch_target_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(job_id, source_id, target_type),
                    FOREIGN KEY(job_id, source_id)
                        REFERENCES sources(job_id, source_id)
                );
                CREATE TABLE IF NOT EXISTS change_events (
                    event_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    run_id TEXT,
                    source_id TEXT,
                    entity TEXT,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    novelty_level TEXT,
                    materiality_level TEXT,
                    confidence_band TEXT,
                    summary TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    old_value_json TEXT,
                    new_value_json TEXT,
                    effective_at TEXT,
                    detected_at TEXT,
                    event_fingerprint TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    notification_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    monitor_run_id TEXT,
                    notification_type TEXT NOT NULL,
                    dedup_key TEXT NOT NULL UNIQUE,
                    chat_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    sent_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS claim_impacts (
                    impact_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    claim_id TEXT,
                    section_id TEXT NOT NULL,
                    impact_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    impact_level TEXT,
                    old_confidence_band TEXT,
                    proposed_confidence_band TEXT,
                    affected_section_ids_json TEXT,
                    requires_review INTEGER NOT NULL DEFAULT 0,
                    rationale TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY(event_id) REFERENCES change_events(event_id)
                );
                CREATE TABLE IF NOT EXISTS change_event_evidence (
                    change_event_id TEXT NOT NULL,
                    evidence_id TEXT NOT NULL,
                    relation TEXT NOT NULL CHECK(relation IN (
                        'support', 'contradict', 'context'
                    )),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(change_event_id, evidence_id, relation),
                    FOREIGN KEY(change_event_id) REFERENCES change_events(event_id)
                );
                CREATE TABLE IF NOT EXISTS report_revisions (
                    revision_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    report_version_id TEXT NOT NULL UNIQUE,
                    base_report_version_id TEXT,
                    revision_type TEXT NOT NULL,
                    impacted_section_ids TEXT NOT NULL,
                    impacted_claim_ids TEXT NOT NULL,
                    change_event_ids TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    published_at TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY(report_version_id)
                        REFERENCES report_versions(report_version_id)
                );
                CREATE TABLE IF NOT EXISTS claim_revisions (
                    claim_revision_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    original_claim_id TEXT,
                    supersedes_claim_revision_id TEXT,
                    report_version_id TEXT NOT NULL,
                    statement TEXT NOT NULL,
                    confidence_band TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    supporting_evidence_ids_json TEXT NOT NULL,
                    contradicting_evidence_ids_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY(report_version_id)
                        REFERENCES report_versions(report_version_id)
                );
                CREATE TABLE IF NOT EXISTS report_patches (
                    patch_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    monitor_run_id TEXT,
                    report_version_id TEXT,
                    report_revision_id TEXT UNIQUE,
                    base_report_version_id TEXT,
                    patch_json TEXT NOT NULL,
                    change_summary TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    validation_status TEXT NOT NULL,
                    approval_status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    approved_at TEXT,
                    approved_by TEXT,
                    rejected_at TEXT,
                    rejected_by TEXT,
                    rejection_reason TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY(report_version_id)
                        REFERENCES report_versions(report_version_id)
                );
                CREATE INDEX IF NOT EXISTS idx_sources_job_status
                    ON sources(job_id, status);
                CREATE INDEX IF NOT EXISTS idx_sources_hash
                    ON sources(job_id, content_hash);
                CREATE INDEX IF NOT EXISTS idx_evidence_job_source
                    ON evidence(job_id, source_id);
                CREATE INDEX IF NOT EXISTS idx_claims_job_type
                    ON claims(job_id, claim_type);
                CREATE INDEX IF NOT EXISTS idx_reports_job_version
                    ON report_versions(job_id, version);
                CREATE INDEX IF NOT EXISTS idx_monitoring_creator
                    ON monitoring_configs(creator_id, status);
                CREATE INDEX IF NOT EXISTS idx_monitor_registration_status
                    ON monitor_registration_requests(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_research_drafts_owner
                    ON research_drafts(creator_id, chat_id, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_monitoring_runs_job
                    ON monitoring_runs(job_id, started_at);
                CREATE INDEX IF NOT EXISTS idx_monitoring_snapshots_source
                    ON monitoring_source_snapshots(job_id, source_id, observed_at);
                CREATE INDEX IF NOT EXISTS idx_monitoring_watch_targets_job
                    ON monitoring_watch_targets(job_id, status);
                CREATE INDEX IF NOT EXISTS idx_change_events_job
                    ON change_events(job_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_notification_deliveries_job
                    ON notification_deliveries(job_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_claim_impacts_job_event
                    ON claim_impacts(job_id, event_id);
                CREATE INDEX IF NOT EXISTS idx_change_event_evidence_event
                    ON change_event_evidence(change_event_id);
                CREATE INDEX IF NOT EXISTS idx_report_revisions_job
                    ON report_revisions(job_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_report_patches_job
                    ON report_patches(job_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_report_patches_status
                    ON report_patches(approval_status, created_at);
                CREATE INDEX IF NOT EXISTS idx_claim_revisions_job
                    ON claim_revisions(job_id, report_version_id);
                """
            )
            self._migrate_core_job_schema(connection)
            self._migrate_report_version_status(connection)
            self._migrate_monitoring_tables(connection)
            self._migrate_report_patch_tables(connection)
            self._migrate_monitoring_status_fields(connection)
            self._migrate_monitoring_failure_count(connection)
            self._migrate_monitoring_schedule_fields(connection)
            self._migrate_monitoring_budget_stats(connection)
            self._migrate_change_event_status(connection)
            self._migrate_monitoring_watch_targets(connection)
            self._migrate_evidence_snapshot_links(connection)
            self._migrate_monitoring_plan_fields(connection)
            self._migrate_professional_artifact_tables(connection)
            self._migrate_source_asset_contract(connection)
            self._migrate_dataset_contract(connection)
            self._migrate_lineage_analysis_contract(connection)
            self._migrate_parsed_table_contract(connection)
            self._migrate_analysis_plan_contract(connection)
            self._migrate_research_draft_tables(connection)
            self._migrate_parsed_text_block_contract(connection)
            self._migrate_analysis_idempotency_contract(connection)
            self._migrate_dataset_profile_contract(connection)
            self._ensure_monitoring_views(connection)
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise RuntimeError(f"SQLite 完整性检查失败: {integrity}")

    def _migrate_core_job_schema(self, connection: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
        }
        additions = {
            "execution_backend": "TEXT NOT NULL DEFAULT 'local'",
            "temporal_workflow_id": "TEXT",
            "temporal_run_id": "TEXT",
            "workflow_status": "TEXT",
            "paused": "INTEGER NOT NULL DEFAULT 0",
            "last_heartbeat_at": "TEXT",
            "notification_status": "TEXT",
            "monitor_registration_status": "TEXT",
            "monitor_registration_error": "TEXT",
        }
        for name, definition in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE jobs ADD COLUMN {name} {definition}"
                )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_temporal_workflow
            ON jobs(temporal_workflow_id)
            WHERE temporal_workflow_id IS NOT NULL
            """
        )

    def _migrate_report_version_status(
        self, connection: sqlite3.Connection
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(report_versions)"
            ).fetchall()
        }
        additions = {
            "status": "TEXT NOT NULL DEFAULT 'published'",
            "published_at": "TEXT",
            "validation_error": "TEXT",
        }
        for name, definition in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE report_versions ADD COLUMN {name} {definition}"
                )
        connection.execute(
            """
            UPDATE report_versions
            SET status = COALESCE(status, 'published'),
                published_at = COALESCE(published_at, created_at)
            WHERE status = 'published'
            """
        )

    def _migrate_monitoring_tables(self, connection: sqlite3.Connection) -> None:
        self._add_columns(
            connection,
            "report_versions",
            {
                "parent_report_version_id": "TEXT",
                "monitor_run_id": "TEXT",
                "trigger_type": "TEXT NOT NULL DEFAULT 'initial_research'",
                "change_summary": "TEXT",
                "publication_status": "TEXT NOT NULL DEFAULT 'published'",
            },
        )
        self._add_columns(
            connection,
            "monitoring_runs",
            {
                "workflow_run_id": "TEXT",
                "scheduled_for": "TEXT",
                "cutoff_from": "TEXT",
                "cutoff_to": "TEXT",
                "stage": "TEXT",
                "new_source_count": "INTEGER NOT NULL DEFAULT 0",
                "changed_source_count": "INTEGER NOT NULL DEFAULT 0",
                "new_evidence_count": "INTEGER NOT NULL DEFAULT 0",
                "change_event_count": "INTEGER NOT NULL DEFAULT 0",
                "affected_claim_count": "INTEGER NOT NULL DEFAULT 0",
                "base_report_version_id": "TEXT",
                "result_report_version_id": "TEXT",
                "draft_patch_id": "TEXT",
                "error_summary": "TEXT",
                "created_at": "TEXT NOT NULL DEFAULT ''",
            },
        )
        self._add_columns(
            connection,
            "monitoring_source_snapshots",
            {
                "run_id": "TEXT",
                "http_status": "INTEGER",
                "content_type": "TEXT",
                "raw_text": "TEXT",
                "raw_object_path": "TEXT",
                "published_at": "TEXT",
                "retrieval_method": "TEXT",
            },
        )
        self._add_columns(
            connection,
            "change_events",
            {
                "run_id": "TEXT",
                "entity": "TEXT",
                "novelty_level": "TEXT",
                "materiality_level": "TEXT",
                "confidence_band": "TEXT",
                "old_value_json": "TEXT",
                "new_value_json": "TEXT",
                "effective_at": "TEXT",
                "detected_at": "TEXT",
                "event_fingerprint": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE report_versions
            SET publication_status = COALESCE(publication_status, status, 'published')
            """
        )
        connection.execute(
            """
            UPDATE monitoring_runs
            SET created_at = started_at
            WHERE created_at = ''
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_change_events_fingerprint
            ON change_events(job_id, event_fingerprint)
            WHERE event_fingerprint IS NOT NULL
            """
        )

    def _migrate_report_patch_tables(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS claim_revisions (
                claim_revision_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                original_claim_id TEXT,
                supersedes_claim_revision_id TEXT,
                report_version_id TEXT NOT NULL,
                statement TEXT NOT NULL,
                confidence_band TEXT NOT NULL,
                reason TEXT NOT NULL,
                supporting_evidence_ids_json TEXT NOT NULL,
                contradicting_evidence_ids_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                FOREIGN KEY(report_version_id)
                    REFERENCES report_versions(report_version_id)
            );
            CREATE TABLE IF NOT EXISTS report_patches (
                patch_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                monitor_run_id TEXT,
                report_version_id TEXT,
                report_revision_id TEXT UNIQUE,
                base_report_version_id TEXT,
                patch_json TEXT NOT NULL,
                change_summary TEXT NOT NULL,
                decision TEXT NOT NULL,
                validation_status TEXT NOT NULL,
                approval_status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                approved_at TEXT,
                approved_by TEXT,
                rejected_at TEXT,
                rejected_by TEXT,
                rejection_reason TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                FOREIGN KEY(report_version_id)
                    REFERENCES report_versions(report_version_id)
            );
            CREATE INDEX IF NOT EXISTS idx_report_patches_job
                ON report_patches(job_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_report_patches_status
                ON report_patches(approval_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_claim_revisions_job
                ON claim_revisions(job_id, report_version_id);
            """
        )
        rows = connection.execute(
            """
            SELECT r.*, rv.monitor_run_id
            FROM report_revisions r
            JOIN report_versions rv
                ON rv.report_version_id = r.report_version_id
            WHERE NOT EXISTS (
                SELECT 1 FROM report_patches p
                WHERE p.report_revision_id = r.revision_id
            )
            """
        ).fetchall()
        for row in rows:
            patch_id = str(uuid.uuid4())
            status = row["status"]
            approval_status = {
                "draft": "pending",
                "published": "published",
                "rejected": "rejected",
            }.get(status, status)
            patch_json = json.dumps(
                {
                    "revision_type": row["revision_type"],
                    "impacted_section_ids": json.loads(row["impacted_section_ids"]),
                    "impacted_claim_ids": json.loads(row["impacted_claim_ids"]),
                    "change_event_ids": json.loads(row["change_event_ids"]),
                },
                ensure_ascii=False,
            )
            connection.execute(
                """
                INSERT INTO report_patches (
                    patch_id, job_id, monitor_run_id, report_version_id,
                    report_revision_id, base_report_version_id, patch_json,
                    change_summary, decision, validation_status,
                    approval_status, created_at, approved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    patch_id,
                    row["job_id"],
                    row["monitor_run_id"],
                    row["report_version_id"],
                    row["revision_id"],
                    row["base_report_version_id"],
                    patch_json,
                    row["summary"],
                    "review_required"
                    if approval_status == "pending"
                    else "auto_patch",
                    "passed" if approval_status in {"published", "pending"} else "not_run",
                    approval_status,
                    row["created_at"],
                    row["published_at"],
                ),
            )

    @staticmethod
    def _migrate_monitoring_status_fields(
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(
            """
            UPDATE monitoring_configs
            SET status = 'active'
            WHERE status = 'enabled'
            """
        )

    @staticmethod
    def _migrate_monitoring_failure_count(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "monitoring_configs",
            {"consecutive_failure_count": "INTEGER NOT NULL DEFAULT 0"},
        )

    @staticmethod
    def _migrate_monitoring_schedule_fields(connection: sqlite3.Connection) -> None:
        Repository._add_columns(
            connection,
            "monitoring_configs",
            {
                "monitor_id": "TEXT",
                "owner_id": "TEXT",
                "temporal_schedule_id": "TEXT",
                "cadence_type": "TEXT",
                "interval_seconds": "INTEGER",
                "calendar_json": "TEXT",
                "update_mode": "TEXT",
                "catchup_window_seconds": "INTEGER",
                "overlap_policy": "TEXT NOT NULL DEFAULT 'BUFFER_ONE'",
                "pause_on_failure": "INTEGER NOT NULL DEFAULT 1",
                "last_successful_run_at": "TEXT",
                "last_failed_run_at": "TEXT",
                "next_run_at": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE monitoring_configs
            SET monitor_id = COALESCE(monitor_id, schedule_id, 'monitor-' || job_id),
                owner_id = COALESCE(owner_id, creator_id),
                temporal_schedule_id = COALESCE(temporal_schedule_id, schedule_id),
                cadence_type = COALESCE(cadence_type, schedule_kind),
                update_mode = COALESCE(update_mode, mode),
                last_successful_run_at = COALESCE(last_successful_run_at, last_success_at),
                last_failed_run_at = COALESCE(last_failed_run_at, last_failure_at),
                overlap_policy = COALESCE(overlap_policy, 'BUFFER_ONE'),
                pause_on_failure = COALESCE(pause_on_failure, 1)
            """
        )

    @staticmethod
    def _migrate_monitoring_budget_stats(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "monitoring_runs",
            {
                "search_request_count": "INTEGER NOT NULL DEFAULT 0",
                "fetched_page_count": "INTEGER NOT NULL DEFAULT 0",
                "llm_call_count": "INTEGER NOT NULL DEFAULT 0",
            },
        )

    @staticmethod
    def _migrate_change_event_status(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            UPDATE change_events
            SET status = CASE status
                WHEN 'open' THEN 'detected'
                WHEN 'resolved' THEN 'applied'
                ELSE status
            END
            WHERE status IN ('open', 'resolved')
            """
        )

    @staticmethod
    def _migrate_monitoring_watch_targets(
        connection: sqlite3.Connection,
    ) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS monitoring_watch_targets (
                watch_target_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                target_type TEXT NOT NULL,
                url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(job_id, source_id, target_type),
                FOREIGN KEY(job_id, source_id)
                    REFERENCES sources(job_id, source_id)
            );
            CREATE INDEX IF NOT EXISTS idx_monitoring_watch_targets_job
                ON monitoring_watch_targets(job_id, status);
            """
        )

    @staticmethod
    def _migrate_evidence_snapshot_links(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "evidence",
            {"snapshot_id": "TEXT"},
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_evidence_snapshot
                ON evidence(job_id, snapshot_id)
            """
        )

    @staticmethod
    def _migrate_monitoring_plan_fields(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "monitoring_runs",
            {
                "context_json": "TEXT",
                "delta_plan_json": "TEXT",
            },
        )

    @staticmethod
    def _migrate_professional_artifact_tables(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "jobs",
            {"research_options_json": "TEXT"},
        )
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_assets (
                asset_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                source_id TEXT,
                url TEXT NOT NULL,
                file_name TEXT NOT NULL,
                content_type TEXT,
                file_type TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                local_path TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS parsed_assets (
                parsed_asset_id TEXT PRIMARY KEY,
                asset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                parser_name TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                warning_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(asset_id) REFERENCES source_assets(asset_id),
                FOREIGN KEY(job_id) REFERENCES jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS parsed_text_blocks (
                block_id TEXT PRIMARY KEY,
                parsed_asset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                page_number INTEGER,
                section TEXT,
                section_title TEXT,
                text TEXT NOT NULL,
                bbox_json TEXT,
                source_locator TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(parsed_asset_id) REFERENCES parsed_assets(parsed_asset_id)
            );
            CREATE TABLE IF NOT EXISTS parsed_tables (
                table_id TEXT PRIMARY KEY,
                parsed_asset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                caption TEXT,
                columns_json TEXT NOT NULL,
                rows_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(parsed_asset_id) REFERENCES parsed_assets(parsed_asset_id)
            );
            CREATE TABLE IF NOT EXISTS tabular_datasets (
                dataset_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                name TEXT NOT NULL,
                dataset_hash TEXT,
                columns_json TEXT NOT NULL,
                rows_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS dataset_lineage (
                lineage_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                upstream_asset_id TEXT NOT NULL,
                upstream_table_id TEXT NOT NULL,
                transform_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES tabular_datasets(dataset_id)
            );
            CREATE TABLE IF NOT EXISTS dataset_profiles (
                profile_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                dataset_hash TEXT,
                profiler_version TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES tabular_datasets(dataset_id)
            );
            CREATE TABLE IF NOT EXISTS analysis_runs (
                analysis_run_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                selected_tools_json TEXT NOT NULL,
                selected_skills_json TEXT NOT NULL,
                reason TEXT NOT NULL,
                idempotency_key TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS analysis_results (
                analysis_result_id TEXT PRIMARY KEY,
                analysis_run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                result_json TEXT NOT NULL,
                idempotency_key TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(analysis_run_id) REFERENCES analysis_runs(analysis_run_id)
            );
            CREATE TABLE IF NOT EXISTS report_artifacts (
                artifact_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                report_version_id TEXT,
                artifact_type TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                content_hash TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                FOREIGN KEY(report_version_id) REFERENCES report_versions(report_version_id)
            );
            CREATE TABLE IF NOT EXISTS artifact_deliveries (
                delivery_id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                dedup_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                error_message TEXT,
                delivered_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(artifact_id) REFERENCES report_artifacts(artifact_id)
            );
            CREATE INDEX IF NOT EXISTS idx_report_artifacts_job
                ON report_artifacts(job_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_report_artifacts_version
                ON report_artifacts(report_version_id, artifact_type);
            """
        )

    @staticmethod
    def _migrate_source_asset_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "source_assets",
            {
                "snapshot_id": "TEXT",
                "original_url": "TEXT",
                "canonical_url": "TEXT",
                "generated_filename": "TEXT",
                "original_filename": "TEXT",
                "declared_mime_type": "TEXT",
                "detected_mime_type": "TEXT",
                "file_extension": "TEXT",
                "byte_size": "INTEGER",
                "sha256": "TEXT",
                "retrieved_at": "TEXT",
                "published_at": "TEXT",
                "source_type": "TEXT",
                "raw_object_path": "TEXT",
                "parse_status": "TEXT",
                "parser_name": "TEXT",
                "parser_version": "TEXT",
                "detection_confidence": "REAL",
                "detection_method": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE source_assets
            SET original_url = COALESCE(original_url, url),
                canonical_url = COALESCE(canonical_url, url),
                generated_filename = COALESCE(generated_filename, file_name),
                declared_mime_type = COALESCE(declared_mime_type, content_type),
                file_extension = COALESCE(file_extension, ''),
                byte_size = COALESCE(byte_size, size_bytes),
                sha256 = COALESCE(sha256, content_hash),
                retrieved_at = COALESCE(retrieved_at, created_at),
                source_type = COALESCE(source_type, file_type),
                raw_object_path = COALESCE(raw_object_path, local_path),
                parse_status = COALESCE(parse_status, status)
            """
        )

    @staticmethod
    def _migrate_dataset_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "tabular_datasets",
            {
                "dataset_name": "TEXT",
                "source_locator": "TEXT",
                "normalized_path": "TEXT",
                "schema_json": "TEXT",
                "profile_json": "TEXT",
                "lineage_json": "TEXT",
                "row_count": "INTEGER",
                "column_count": "INTEGER",
            },
        )
        connection.execute(
            """
            UPDATE tabular_datasets
            SET dataset_name = COALESCE(dataset_name, name),
                schema_json = COALESCE(schema_json, columns_json),
                profile_json = COALESCE(profile_json, '{}'),
                lineage_json = COALESCE(lineage_json, '{}'),
                row_count = COALESCE(row_count, json_array_length(rows_json)),
                column_count = COALESCE(column_count, json_array_length(columns_json))
            """
        )
        connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_source_assets_sha256
                ON source_assets(job_id, sha256);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_source_assets_job_url_sha
                ON source_assets(job_id, canonical_url, sha256)
                WHERE canonical_url IS NOT NULL AND sha256 IS NOT NULL;
            """
        )

    @staticmethod
    def _migrate_lineage_analysis_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "dataset_lineage",
            {
                "job_id": "TEXT",
                "asset_id": "TEXT",
                "source_locator": "TEXT",
                "transformation_name": "TEXT",
                "transformation_version": "TEXT",
                "parameters_json": "TEXT",
                "parent_dataset_ids_json": "TEXT",
            },
        )
        Repository._add_columns(
            connection,
            "analysis_runs",
            {
                "report_version_id": "TEXT",
                "status": "TEXT",
                "started_at": "TEXT",
                "completed_at": "TEXT",
            },
        )
        Repository._add_columns(
            connection,
            "analysis_results",
            {
                "skill_name": "TEXT",
                "skill_version": "TEXT",
                "tool_version": "TEXT",
                "input_dataset_ids_json": "TEXT",
                "input_evidence_ids_json": "TEXT",
                "parameters_json": "TEXT",
                "limitations_json": "TEXT",
                "result_hash": "TEXT",
            },
        )
        connection.executescript(
            """
            UPDATE dataset_lineage
            SET asset_id = COALESCE(asset_id, upstream_asset_id),
                transformation_name = COALESCE(transformation_name, 'normalize_dataset'),
                transformation_version = COALESCE(transformation_version, '1.0'),
                parameters_json = COALESCE(parameters_json, transform_json, '{}'),
                parent_dataset_ids_json = COALESCE(parent_dataset_ids_json, '[]')
            WHERE asset_id IS NULL
               OR transformation_name IS NULL
               OR transformation_version IS NULL
               OR parameters_json IS NULL
               OR parent_dataset_ids_json IS NULL;
            UPDATE analysis_runs
            SET status = COALESCE(status, 'completed'),
                started_at = COALESCE(started_at, created_at),
                completed_at = COALESCE(completed_at, created_at);
            CREATE INDEX IF NOT EXISTS idx_dataset_lineage_job
                ON dataset_lineage(job_id, dataset_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_lineage_transform
                ON dataset_lineage(dataset_id, transformation_name, transformation_version);
            CREATE INDEX IF NOT EXISTS idx_analysis_results_job_skill
                ON analysis_results(job_id, skill_name, tool_name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_results_job_hash
                ON analysis_results(job_id, result_hash)
                WHERE result_hash IS NOT NULL;
            """
        )

    @staticmethod
    def _migrate_parsed_table_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "parsed_tables",
            {
                "sheet_name": "TEXT",
                "cell_range": "TEXT",
                "source_locator": "TEXT",
                "extraction_method": "TEXT",
                "metadata_json": "TEXT NOT NULL DEFAULT '{}'",
            },
        )
        connection.execute(
            """
            UPDATE parsed_tables
            SET extraction_method = COALESCE(extraction_method, 'structured'),
                metadata_json = COALESCE(metadata_json, '{}')
            """
        )

    @staticmethod
    def _migrate_analysis_plan_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "analysis_runs",
            {
                "analysis_plan_json": "TEXT",
            },
        )

    @staticmethod
    def _migrate_research_draft_tables(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS research_drafts (
                draft_id TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                source_message_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                options_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_research_drafts_owner
                ON research_drafts(creator_id, chat_id, status, updated_at);
            """
        )

    @staticmethod
    def _migrate_parsed_text_block_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "parsed_text_blocks",
            {
                "section_title": "TEXT",
                "bbox_json": "TEXT",
                "source_locator": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE parsed_text_blocks
            SET section_title = COALESCE(section_title, section)
            """
        )

    @staticmethod
    def _migrate_analysis_idempotency_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "analysis_runs",
            {"idempotency_key": "TEXT"},
        )
        Repository._add_columns(
            connection,
            "analysis_results",
            {"idempotency_key": "TEXT"},
        )
        connection.executescript(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_runs_job_idempotency
                ON analysis_runs(job_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_results_job_idempotency
                ON analysis_results(job_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL;
            """
        )

    @staticmethod
    def _migrate_dataset_profile_contract(
        connection: sqlite3.Connection,
    ) -> None:
        Repository._add_columns(
            connection,
            "tabular_datasets",
            {"dataset_hash": "TEXT"},
        )
        Repository._add_columns(
            connection,
            "dataset_profiles",
            {
                "dataset_hash": "TEXT",
                "profiler_version": "TEXT",
            },
        )
        rows = connection.execute(
            """
            SELECT dataset_id, job_id, columns_json, rows_json, profile_json
            FROM tabular_datasets
            """
        ).fetchall()
        for row in rows:
            dataset_hash = Repository._tabular_dataset_hash(
                json.loads(row["columns_json"] or "[]"),
                json.loads(row["rows_json"] or "[]"),
            )
            connection.execute(
                "UPDATE tabular_datasets SET dataset_hash = ? WHERE dataset_id = ?",
                (dataset_hash, row["dataset_id"]),
            )
            profile_json = row["profile_json"] or "{}"
            if profile_json != "{}":
                profile_id = Repository._dataset_profile_id(
                    row["dataset_id"], dataset_hash, "1.0"
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO dataset_profiles (
                        profile_id, dataset_id, job_id, profile_json,
                        dataset_hash, profiler_version, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile_id,
                        row["dataset_id"],
                        row["job_id"],
                        profile_json,
                        dataset_hash,
                        "1.0",
                        utc_now(),
                    ),
                )
        connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_tabular_datasets_job_profile
                ON tabular_datasets(job_id, dataset_hash);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_profiles_input
                ON dataset_profiles(dataset_id, dataset_hash, profiler_version)
                WHERE dataset_hash IS NOT NULL AND profiler_version IS NOT NULL;
            """
        )

    @staticmethod
    def _ensure_monitoring_views(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            DROP VIEW IF EXISTS source_snapshots;
            CREATE VIEW source_snapshots AS
            SELECT
                snapshot_id,
                source_id,
                COALESCE(monitor_run_id, run_id) AS monitor_run_id,
                COALESCE(retrieved_at, observed_at) AS retrieved_at,
                http_status,
                content_type,
                content_hash,
                raw_text,
                raw_object_path,
                published_at,
                retrieval_method,
                COALESCE(created_at, observed_at) AS created_at
            FROM monitoring_source_snapshots;
            """
        )

    @staticmethod
    def _normalize_change_event_status(status: str) -> str:
        return {
            "open": "detected",
            "resolved": "applied",
        }.get(status, status)
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_monitor_id
            ON monitoring_configs(monitor_id)
            WHERE monitor_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_temporal_schedule
            ON monitoring_configs(temporal_schedule_id)
            WHERE temporal_schedule_id IS NOT NULL
            """
        )
        Repository._add_columns(
            connection,
            "monitoring_runs",
            {
                "monitor_run_id": "TEXT",
                "monitor_id": "TEXT",
                "temporal_workflow_id": "TEXT",
                "temporal_run_id": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE monitoring_runs
            SET monitor_run_id = COALESCE(monitor_run_id, run_id),
                temporal_workflow_id = COALESCE(temporal_workflow_id, workflow_id),
                temporal_run_id = COALESCE(temporal_run_id, workflow_run_id),
                monitor_id = COALESCE(
                    monitor_id,
                    (SELECT monitor_id FROM monitoring_configs c
                     WHERE c.job_id = monitoring_runs.job_id)
                )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_runs_monitor_run_id
            ON monitoring_runs(monitor_run_id)
            WHERE monitor_run_id IS NOT NULL
            """
        )
        Repository._add_columns(
            connection,
            "monitoring_source_snapshots",
            {
                "monitor_run_id": "TEXT",
                "retrieved_at": "TEXT",
                "created_at": "TEXT",
            },
        )
        connection.execute(
            """
            UPDATE monitoring_source_snapshots
            SET monitor_run_id = COALESCE(monitor_run_id, run_id),
                retrieved_at = COALESCE(retrieved_at, observed_at),
                created_at = COALESCE(created_at, observed_at)
            """
        )
        Repository._add_columns(
            connection,
            "claim_impacts",
            {
                "impact_level": "TEXT",
                "old_confidence_band": "TEXT",
                "proposed_confidence_band": "TEXT",
                "affected_section_ids_json": "TEXT",
                "requires_review": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        connection.execute(
            """
            UPDATE claim_impacts
            SET impact_level = COALESCE(impact_level, severity),
                affected_section_ids_json = COALESCE(
                    affected_section_ids_json,
                    json_array(section_id)
                ),
                requires_review = COALESCE(requires_review, CASE
                    WHEN severity IN ('high', 'critical') THEN 1 ELSE 0 END
                )
            """
        )
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS change_event_evidence (
                change_event_id TEXT NOT NULL,
                evidence_id TEXT NOT NULL,
                relation TEXT NOT NULL CHECK(relation IN (
                    'support', 'contradict', 'context'
                )),
                created_at TEXT NOT NULL,
                PRIMARY KEY(change_event_id, evidence_id, relation),
                FOREIGN KEY(change_event_id) REFERENCES change_events(event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_change_event_evidence_event
                ON change_event_evidence(change_event_id);
            DROP VIEW IF EXISTS source_snapshots;
            CREATE VIEW source_snapshots AS
            SELECT
                snapshot_id,
                source_id,
                COALESCE(monitor_run_id, run_id) AS monitor_run_id,
                COALESCE(retrieved_at, observed_at) AS retrieved_at,
                http_status,
                content_type,
                content_hash,
                raw_text,
                raw_object_path,
                published_at,
                retrieval_method,
                COALESCE(created_at, observed_at) AS created_at
            FROM monitoring_source_snapshots;
            """
        )

    @staticmethod
    def _add_columns(
        connection: sqlite3.Connection, table_name: str, additions: dict[str, str]
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for name, definition in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {name} {definition}"
                )

    @staticmethod
    def _schedule_interval_seconds(
        schedule_kind: str, schedule_value: str
    ) -> int | None:
        if schedule_kind != "every":
            return None
        value = schedule_value.strip().lower()
        if len(value) < 2:
            return None
        unit = value[-1]
        try:
            amount = int(value[:-1])
        except ValueError:
            return None
        multipliers = {"m": 60, "h": 3600, "d": 86400}
        multiplier = multipliers.get(unit)
        return amount * multiplier if multiplier else None

    @staticmethod
    def _insert_change_event_evidence_rows(
        connection: sqlite3.Connection,
        *,
        event_id: str,
        evidence_ids: list[str],
        relation: str,
        created_at: str,
    ) -> None:
        if relation not in {"support", "contradict", "context"}:
            raise ValueError(f"invalid change event evidence relation: {relation}")
        for evidence_id in dict.fromkeys(evidence_ids):
            connection.execute(
                """
                INSERT OR IGNORE INTO change_event_evidence (
                    change_event_id, evidence_id, relation, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (event_id, evidence_id, relation, created_at),
            )

    def _backup_before_migration(self) -> None:
        if not self.database_path.exists() or self.database_path.stat().st_size == 0:
            return
        with sqlite3.connect(self.database_path) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version >= SCHEMA_VERSION:
            return
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = self.database_path.with_name(
            f"{self.database_path.name}.schema{version}-backup-{timestamp}"
        )
        shutil.copy2(self.database_path, backup)

    def health(self) -> dict[str, str | int]:
        with self._lock, self._connect() as connection:
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            job_count = connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            message_count = connection.execute(
                "SELECT COUNT(*) FROM processed_messages"
            ).fetchone()[0]
            schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
        return {
            "database_path": str(self.database_path.resolve()),
            "journal_mode": str(journal_mode),
            "job_count": int(job_count),
            "message_count": int(message_count),
            "schema_version": int(schema_version),
        }

    def save_research_plan(self, job_id: str, plan: ResearchPlan) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO research_plans(job_id, objective, plan_json, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    objective=excluded.objective,
                    plan_json=excluded.plan_json,
                    created_at=excluded.created_at
                """,
                (
                    job_id,
                    plan.objective,
                    plan.model_dump_json(),
                    utc_now(),
                ),
            )

    def get_research_plan(self, job_id: str) -> Optional[ResearchPlan]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT plan_json FROM research_plans WHERE job_id = ?", (job_id,)
            ).fetchone()
        return ResearchPlan.model_validate_json(row["plan_json"]) if row else None

    def add_source(
        self,
        *,
        job_id: str,
        url: str,
        canonical_url: str,
        title: str,
        publisher: str | None,
        published_at: str | None,
        search_query: str,
        search_rank: int,
        http_status: int | None,
        content_type: str | None,
        content_hash: str | None,
        raw_text: str,
        status: str,
        error_message: str | None = None,
    ) -> str:
        with self._lock, self._connect() as connection:
            existing = connection.execute(
                """
                SELECT source_id FROM sources
                WHERE job_id = ? AND canonical_url = ?
                """,
                (job_id, canonical_url),
            ).fetchone()
            if existing:
                return str(existing["source_id"])
            count = connection.execute(
                "SELECT COUNT(*) FROM sources WHERE job_id = ?", (job_id,)
            ).fetchone()[0]
            source_id = f"S-{count + 1:03d}"
            connection.execute(
                """
                INSERT INTO sources (
                    source_id, job_id, url, canonical_url, title, publisher,
                    published_at, retrieved_at, search_query, search_rank,
                    http_status, content_type, content_hash, raw_text, status,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    job_id,
                    url,
                    canonical_url,
                    title,
                    publisher,
                    published_at,
                    utc_now(),
                    search_query,
                    search_rank,
                    http_status,
                    content_type,
                    content_hash,
                    raw_text,
                    status,
                    error_message,
                ),
            )
        return source_id

    def content_hash_exists(self, job_id: str, digest: str) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM sources
                WHERE job_id = ? AND content_hash = ? AND status = 'fetched'
                """,
                (job_id, digest),
            ).fetchone()
        return row is not None

    def list_sources(
        self, job_id: str, status: str | None = None
    ) -> list[dict]:
        sql = "SELECT * FROM sources WHERE job_id = ?"
        params: tuple = (job_id,)
        if status:
            sql += " AND status = ?"
            params = (job_id, status)
        sql += " ORDER BY source_id"
        with self._lock, self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def update_source_result(
        self,
        *,
        job_id: str,
        source_id: str,
        canonical_url: str,
        title: str,
        publisher: str | None,
        published_at: str | None,
        http_status: int | None,
        content_type: str | None,
        content_hash: str | None,
        raw_text: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE sources SET canonical_url = ?, title = ?, publisher = ?,
                    published_at = ?, retrieved_at = ?, http_status = ?,
                    content_type = ?, content_hash = ?, raw_text = ?,
                    status = ?, error_message = ?
                WHERE job_id = ? AND source_id = ?
                """,
                (
                    canonical_url,
                    title,
                    publisher,
                    published_at,
                    utc_now(),
                    http_status,
                    content_type,
                    content_hash,
                    raw_text,
                    status,
                    error_message,
                    job_id,
                    source_id,
                ),
            )

    def source_has_evidence(self, job_id: str, source_id: str) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM evidence
                WHERE job_id = ? AND source_id = ? LIMIT 1
                """,
                (job_id, source_id),
            ).fetchone()
        return row is not None

    def snapshot_has_evidence(self, job_id: str, snapshot_id: str) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM evidence
                WHERE job_id = ? AND snapshot_id = ? LIMIT 1
                """,
                (job_id, snapshot_id),
            ).fetchone()
        return row is not None

    def count_monitoring_run_evidence(self, job_id: str, run_id: str) -> int:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM evidence e
                JOIN monitoring_source_snapshots s
                  ON s.snapshot_id = e.snapshot_id
                WHERE e.job_id = ?
                  AND COALESCE(s.monitor_run_id, s.run_id) = ?
                """,
                (job_id, run_id),
            ).fetchone()
        return int(row[0])

    def add_evidence(
        self,
        job_id: str,
        source_id: str,
        item: EvidenceItem,
        *,
        snapshot_id: str | None = None,
    ) -> Optional[StoredEvidence]:
        with self._lock, self._connect() as connection:
            if snapshot_id:
                source = connection.execute(
                    """
                    SELECT COALESCE(s.raw_text, src.raw_text) AS raw_text
                    FROM monitoring_source_snapshots s
                    JOIN sources src
                      ON src.job_id = s.job_id AND src.source_id = s.source_id
                    WHERE s.job_id = ? AND s.source_id = ? AND s.snapshot_id = ?
                      AND s.status = 'fetched'
                    """,
                    (job_id, source_id, snapshot_id),
                ).fetchone()
                if not source:
                    return None
            else:
                source = connection.execute(
                    """
                    SELECT raw_text FROM sources
                    WHERE job_id = ? AND source_id = ? AND status = 'fetched'
                    """,
                    (job_id, source_id),
                ).fetchone()
            if not source or item.exact_quote not in source["raw_text"]:
                return None
            existing = connection.execute(
                """
                SELECT evidence_id, snapshot_id FROM evidence
                WHERE job_id = ? AND source_id = ? AND entity = ?
                  AND attribute = ? AND exact_quote = ?
                """,
                (
                    job_id,
                    source_id,
                    item.entity,
                    item.attribute,
                    item.exact_quote,
                ),
            ).fetchone()
            if existing:
                return None
            count = connection.execute(
                "SELECT COUNT(*) FROM evidence WHERE job_id = ?", (job_id,)
            ).fetchone()[0]
            evidence_id = f"E-{count + 1:03d}"
            connection.execute(
                """
                INSERT INTO evidence (
                    evidence_id, job_id, source_id, snapshot_id, entity,
                    attribute, value, exact_quote, evidence_type, observed_at,
                    confidence_band, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence_id,
                    job_id,
                    source_id,
                    snapshot_id,
                    item.entity,
                    item.attribute,
                    item.value,
                    item.exact_quote,
                    item.evidence_type,
                    item.observed_at,
                    item.confidence_band,
                    utc_now(),
                ),
            )
        return StoredEvidence(
            evidence_id=evidence_id,
            source_id=source_id,
            snapshot_id=snapshot_id,
            **item.model_dump(),
        )

    def list_evidence(self, job_id: str) -> list[StoredEvidence]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM evidence WHERE job_id = ? ORDER BY evidence_id",
                (job_id,),
            ).fetchall()
        return [
            StoredEvidence(
                evidence_id=row["evidence_id"],
                source_id=row["source_id"],
                snapshot_id=row["snapshot_id"] if "snapshot_id" in row.keys() else None,
                entity=row["entity"],
                attribute=row["attribute"],
                value=row["value"],
                exact_quote=row["exact_quote"],
                evidence_type=row["evidence_type"],
                observed_at=row["observed_at"],
                confidence_band=row["confidence_band"],
            )
            for row in rows
        ]

    def add_claim(self, job_id: str, item: ClaimItem) -> StoredClaim:
        valid_evidence = {e.evidence_id for e in self.list_evidence(job_id)}
        referenced = (
            item.supporting_evidence_ids + item.contradicting_evidence_ids
        )
        if any(evidence_id not in valid_evidence for evidence_id in referenced):
            raise ValueError("claim 引用了不存在的 evidence_id")
        if item.claim_type != "uncertainty" and not item.supporting_evidence_ids:
            raise ValueError("事实性 claim 至少需要一条 evidence")
        with self._lock, self._connect() as connection:
            existing = connection.execute(
                """
                SELECT claim_id FROM claims
                WHERE job_id = ? AND statement = ?
                """,
                (job_id, item.statement),
            ).fetchone()
            if existing:
                return StoredClaim(
                    claim_id=existing["claim_id"], **item.model_dump()
                )
            count = connection.execute(
                "SELECT COUNT(*) FROM claims WHERE job_id = ?", (job_id,)
            ).fetchone()[0]
            claim_id = f"C-{count + 1:03d}"
            now = utc_now()
            connection.execute(
                """
                INSERT INTO claims (
                    claim_id, job_id, statement, claim_type, confidence_band,
                    reasoning_summary, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    claim_id,
                    job_id,
                    item.statement,
                    item.claim_type,
                    item.confidence_band,
                    item.reasoning_summary,
                    now,
                ),
            )
            for evidence_id in item.supporting_evidence_ids:
                connection.execute(
                    """
                    INSERT INTO claim_evidence (
                        job_id, claim_id, evidence_id, relation, created_at
                    ) VALUES (?, ?, ?, 'support', ?)
                    """,
                    (job_id, claim_id, evidence_id, now),
                )
            for evidence_id in item.contradicting_evidence_ids:
                connection.execute(
                    """
                    INSERT INTO claim_evidence (
                        job_id, claim_id, evidence_id, relation, created_at
                    ) VALUES (?, ?, ?, 'contradict', ?)
                    """,
                    (job_id, claim_id, evidence_id, now),
                )
        return StoredClaim(claim_id=claim_id, **item.model_dump())

    def list_claims(self, job_id: str) -> list[StoredClaim]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM claims WHERE job_id = ? ORDER BY claim_id",
                (job_id,),
            ).fetchall()
            links = connection.execute(
                """
                SELECT claim_id, evidence_id, relation FROM claim_evidence
                WHERE job_id = ? ORDER BY claim_id, evidence_id
                """,
                (job_id,),
            ).fetchall()
        support: dict[str, list[str]] = {}
        contradict: dict[str, list[str]] = {}
        for link in links:
            target = support if link["relation"] == "support" else contradict
            target.setdefault(link["claim_id"], []).append(link["evidence_id"])
        return [
            StoredClaim(
                claim_id=row["claim_id"],
                statement=row["statement"],
                claim_type=row["claim_type"],
                confidence_band=row["confidence_band"],
                reasoning_summary=row["reasoning_summary"],
                supporting_evidence_ids=support.get(row["claim_id"], []),
                contradicting_evidence_ids=contradict.get(row["claim_id"], []),
            )
            for row in rows
        ]

    def list_active_claims(self, job_id: str) -> list[StoredClaim]:
        claims = self.list_claims(job_id)
        active_revisions = {
            revision["original_claim_id"]: revision
            for revision in self.list_claim_revisions(job_id)
            if revision["status"] == "active"
            and revision.get("original_claim_id")
        }
        return [
            StoredClaim(
                claim_id=claim.claim_id,
                statement=(
                    active_revisions[claim.claim_id]["statement"]
                    if claim.claim_id in active_revisions
                    else claim.statement
                ),
                claim_type=claim.claim_type,
                confidence_band=(
                    active_revisions[claim.claim_id]["confidence_band"]
                    if claim.claim_id in active_revisions
                    else claim.confidence_band
                ),
                reasoning_summary=(
                    active_revisions[claim.claim_id]["reason"]
                    if claim.claim_id in active_revisions
                    else claim.reasoning_summary
                ),
                supporting_evidence_ids=(
                    active_revisions[claim.claim_id][
                        "supporting_evidence_ids"
                    ]
                    if claim.claim_id in active_revisions
                    else claim.supporting_evidence_ids
                ),
                contradicting_evidence_ids=(
                    active_revisions[claim.claim_id][
                        "contradicting_evidence_ids"
                    ]
                    if claim.claim_id in active_revisions
                    else claim.contradicting_evidence_ids
                ),
            )
            for claim in claims
        ]

    def next_report_version(self, job_id: str) -> int:
        with self._lock, self._connect() as connection:
            value = connection.execute(
                """
                SELECT COALESCE(MAX(version), 0) + 1
                FROM report_versions WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()[0]
        return int(value)

    def add_report_version(
        self,
        job_id: str,
        version: int,
        report_path: str,
        report_json_path: str,
        *,
        status: str = "published",
        parent_report_version_id: str | None = None,
        monitor_run_id: str | None = None,
        trigger_type: str = "initial_research",
        change_summary: str | None = None,
    ) -> str:
        if status not in {"draft", "published", "rejected"}:
            raise ValueError(f"unsupported report status: {status}")
        report_version_id = str(uuid.uuid4())
        now = utc_now()
        published_at = now if status == "published" else None
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO report_versions (
                    report_version_id, job_id, version, report_path,
                    report_json_path, parent_report_version_id, monitor_run_id,
                    trigger_type, change_summary, status, publication_status,
                    published_at, validation_error, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    report_version_id,
                    job_id,
                    version,
                    report_path,
                    report_json_path,
                    parent_report_version_id,
                    monitor_run_id,
                    trigger_type,
                    change_summary,
                    status,
                    status,
                    published_at,
                    now,
                ),
            )
        return report_version_id

    def reserve_report_version(
        self,
        job_id: str,
        *,
        status: str = "draft",
        parent_report_version_id: str | None = None,
        monitor_run_id: str | None = None,
        trigger_type: str = "manual",
        change_summary: str | None = None,
    ) -> tuple[str, int]:
        if status not in {"draft", "published", "rejected"}:
            raise ValueError(f"unsupported report status: {status}")
        report_version_id = str(uuid.uuid4())
        now = utc_now()
        published_at = now if status == "published" else None
        with self._lock, self._connect() as connection:
            version = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM report_versions WHERE job_id = ?
                    """,
                    (job_id,),
                ).fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO report_versions (
                    report_version_id, job_id, version, report_path,
                    report_json_path, parent_report_version_id, monitor_run_id,
                    trigger_type, change_summary, status, publication_status,
                    published_at, validation_error, created_at
                ) VALUES (?, ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    report_version_id,
                    job_id,
                    version,
                    parent_report_version_id,
                    monitor_run_id,
                    trigger_type,
                    change_summary,
                    status,
                    status,
                    published_at,
                    now,
                ),
            )
        return report_version_id, version

    def update_report_version_paths(
        self, report_version_id: str, report_path: str, report_json_path: str
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_versions
                SET report_path = ?, report_json_path = ?
                WHERE report_version_id = ?
                """,
                (report_path, report_json_path, report_version_id),
            )

    def publish_report_version(self, report_version_id: str) -> None:
        with self._lock, self._connect() as connection:
            report = connection.execute(
                """
                SELECT report_path, report_json_path
                FROM report_versions
                WHERE report_version_id = ?
                """,
                (report_version_id,),
            ).fetchone()
            if not report:
                raise ValueError("report version not found")
            if not report["report_path"] or not report["report_json_path"]:
                raise ValueError("report version artifact paths are empty")
            connection.execute(
                """
                UPDATE report_versions
                SET status = 'published', published_at = ?,
                    publication_status = 'published',
                    validation_error = NULL
                WHERE report_version_id = ?
                """,
                (utc_now(), report_version_id),
            )

    def mark_report_validation_failed(
        self, report_version_id: str, error: str
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_versions
                SET status = 'draft', publication_status = 'draft',
                    validation_error = ?
                WHERE report_version_id = ?
                """,
                (error[:1000], report_version_id),
            )

    def reject_report_version(
        self, report_version_id: str, reason: str | None = None
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_versions
                SET status = 'rejected', publication_status = 'rejected',
                    validation_error = ?
                WHERE report_version_id = ?
                """,
                ((reason or "")[:1000] or None, report_version_id),
            )

    def delete_draft_report_version(self, report_version_id: str) -> list[str]:
        with self._lock, self._connect() as connection:
            report = connection.execute(
                """
                SELECT report_path, report_json_path, status
                FROM report_versions
                WHERE report_version_id = ?
                """,
                (report_version_id,),
            ).fetchone()
            if not report:
                return []
            if report["status"] != "draft":
                raise ValueError("only draft report versions can be deleted")
            paths = [
                path
                for path in (report["report_path"], report["report_json_path"])
                if path
            ]
            connection.execute(
                "DELETE FROM claim_revisions WHERE report_version_id = ?",
                (report_version_id,),
            )
            connection.execute(
                "DELETE FROM report_patches WHERE report_version_id = ?",
                (report_version_id,),
            )
            connection.execute(
                "DELETE FROM report_revisions WHERE report_version_id = ?",
                (report_version_id,),
            )
            connection.execute(
                "DELETE FROM report_versions WHERE report_version_id = ?",
                (report_version_id,),
            )
        return paths

    def get_latest_report(
        self, job_id: str, status: str | None = "published"
    ) -> Optional[dict]:
        params: list[object] = [job_id]
        status_clause = ""
        if status is not None:
            status_clause = " AND status = ?"
            params.append(status)
        with self._lock, self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT * FROM report_versions
                WHERE job_id = ?{status_clause}
                ORDER BY version DESC LIMIT 1
                """,
                tuple(params),
            ).fetchone()
        return dict(row) if row else None

    def get_report_version(self, job_id: str, version: int) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM report_versions
                WHERE job_id = ? AND version = ?
                """,
                (job_id, version),
            ).fetchone()
        return dict(row) if row else None

    def get_report_version_by_id(self, report_version_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_versions WHERE report_version_id = ?",
                (report_version_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_report_versions(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM report_versions
                WHERE job_id = ? ORDER BY version DESC
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_monitoring_config(
        self,
        *,
        job_id: str,
        creator_id: str,
        chat_id: str,
        schedule_id: str,
        schedule_kind: str,
        schedule_value: str,
        timezone: str,
        mode: str,
        notify_level: str,
        catchup_window_seconds: int = 21_600,
    ) -> MonitoringConfig:
        now = utc_now()
        interval_seconds = self._schedule_interval_seconds(
            schedule_kind, schedule_value
        )
        calendar_json = None
        if schedule_kind in {"daily", "weekly"}:
            calendar_json = json.dumps(
                {
                    "kind": schedule_kind,
                    "value": schedule_value,
                    "timezone": timezone,
                },
                ensure_ascii=False,
            )
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_configs (
                    job_id, monitor_id, creator_id, owner_id, chat_id,
                    schedule_id, temporal_schedule_id, schedule_kind,
                    cadence_type, schedule_value, interval_seconds,
                    calendar_json, timezone, mode, update_mode,
                    notify_level, status, catchup_window_seconds,
                    overlap_policy, pause_on_failure,
                    last_success_at, last_failure_at, last_error, last_decision,
                    consecutive_failure_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'active', ?, 'BUFFER_ONE', 1, NULL, NULL, NULL, NULL,
                    0, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    monitor_id=excluded.monitor_id,
                    creator_id=excluded.creator_id,
                    owner_id=excluded.owner_id,
                    chat_id=excluded.chat_id,
                    schedule_id=excluded.schedule_id,
                    temporal_schedule_id=excluded.temporal_schedule_id,
                    schedule_kind=excluded.schedule_kind,
                    cadence_type=excluded.cadence_type,
                    schedule_value=excluded.schedule_value,
                    interval_seconds=excluded.interval_seconds,
                    calendar_json=excluded.calendar_json,
                    timezone=excluded.timezone,
                    mode=excluded.mode,
                    update_mode=excluded.update_mode,
                    notify_level=excluded.notify_level,
                    status='active',
                    catchup_window_seconds=excluded.catchup_window_seconds,
                    overlap_policy='BUFFER_ONE',
                    pause_on_failure=1,
                    last_error=NULL,
                    consecutive_failure_count=0,
                    updated_at=excluded.updated_at
                """,
                (
                    job_id,
                    schedule_id,
                    creator_id,
                    creator_id,
                    chat_id,
                    schedule_id,
                    schedule_id,
                    schedule_kind,
                    schedule_kind,
                    schedule_value,
                    interval_seconds,
                    calendar_json,
                    timezone,
                    mode,
                    mode,
                    notify_level,
                    catchup_window_seconds,
                    now,
                    now,
                ),
            )
        config = self.get_monitoring_config(job_id)
        assert config is not None
        return config

    def upsert_monitoring_schedule(
        self,
        *,
        job_id: str,
        schedule_kind: str,
        schedule_value: str,
        timezone: str,
        mode: str | None = None,
        notify_level: str | None = None,
        status: str | None = None,
    ) -> None:
        interval_seconds = self._schedule_interval_seconds(
            schedule_kind, schedule_value
        )
        calendar_json = None
        if schedule_kind in {"daily", "weekly"}:
            calendar_json = json.dumps(
                {
                    "kind": schedule_kind,
                    "value": schedule_value,
                    "timezone": timezone,
                },
                ensure_ascii=False,
            )
        assignments = [
            "schedule_kind = ?",
            "cadence_type = ?",
            "schedule_value = ?",
            "interval_seconds = ?",
            "calendar_json = ?",
            "timezone = ?",
            "updated_at = ?",
        ]
        params: list[object] = [
            schedule_kind,
            schedule_kind,
            schedule_value,
            interval_seconds,
            calendar_json,
            timezone,
            utc_now(),
        ]
        if mode is not None:
            assignments.append("mode = ?")
            params.append(mode)
            assignments.append("update_mode = ?")
            params.append(mode)
        if notify_level is not None:
            assignments.append("notify_level = ?")
            params.append(notify_level)
        if status is not None:
            assignments.append("status = ?")
            params.append(status)
        params.append(job_id)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"UPDATE monitoring_configs SET {', '.join(assignments)} "
                "WHERE job_id = ?",
                tuple(params),
            )

    def set_monitoring_status(self, job_id: str, status: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE monitoring_configs
                SET status = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (status, utc_now(), job_id),
            )

    def update_monitoring_next_run(self, job_id: str, next_run_at: str | None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE monitoring_configs
                SET next_run_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (next_run_at, utc_now(), job_id),
            )

    def delete_monitoring_config(self, job_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE monitoring_configs
                SET status = 'deleted', updated_at = ?
                WHERE job_id = ?
                """,
                (utc_now(), job_id),
            )

    def get_monitoring_config(self, job_id: str) -> Optional[MonitoringConfig]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM monitoring_configs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._to_monitoring_config(row) if row else None

    def get_monitoring_config_by_monitor_id(
        self, monitor_id: str
    ) -> Optional[MonitoringConfig]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM monitoring_configs WHERE monitor_id = ?",
                (monitor_id,),
            ).fetchone()
        return self._to_monitoring_config(row) if row else None

    def list_monitoring_configs(
        self, creator_id: str | None = None, include_deleted: bool = False
    ) -> list[MonitoringConfig]:
        query = "SELECT * FROM monitoring_configs"
        clauses = []
        params: list[object] = []
        if creator_id is not None:
            clauses.append("creator_id = ?")
            params.append(creator_id)
        if not include_deleted:
            clauses.append("status != 'deleted'")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC"
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._to_monitoring_config(row) for row in rows]

    def start_monitoring_run(
        self,
        job_id: str,
        workflow_id: str,
        *,
        workflow_run_id: str | None = None,
        scheduled_for: str | None = None,
        cutoff_from: str | None = None,
        cutoff_to: str | None = None,
        base_report_version_id: str | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        now = utc_now()
        config = self.get_monitoring_config(job_id)
        monitor_id = config.monitor_id if config else None
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_runs (
                    run_id, monitor_run_id, monitor_id, job_id, workflow_id,
                    temporal_workflow_id, workflow_run_id, temporal_run_id, scheduled_for,
                    cutoff_from, cutoff_to, status, stage, decision,
                    base_report_version_id, error_message, error_summary,
                    started_at, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 'started', NULL,
                    ?, NULL, NULL, ?, ?, NULL)
                """,
                (
                    run_id,
                    run_id,
                    monitor_id,
                    job_id,
                    workflow_id,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                    scheduled_for,
                    cutoff_from,
                    cutoff_to,
                    base_report_version_id,
                    now,
                    now,
                ),
            )
        return run_id

    def update_monitoring_run_stats(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        new_source_count: int | None = None,
        changed_source_count: int | None = None,
        new_evidence_count: int | None = None,
        search_request_count: int | None = None,
        fetched_page_count: int | None = None,
        llm_call_count: int | None = None,
        change_event_count: int | None = None,
        affected_claim_count: int | None = None,
        result_report_version_id: str | None = None,
        draft_patch_id: str | None = None,
    ) -> None:
        assignments = []
        params: list[object] = []
        values = {
            "stage": stage,
            "new_source_count": new_source_count,
            "changed_source_count": changed_source_count,
            "new_evidence_count": new_evidence_count,
            "search_request_count": search_request_count,
            "fetched_page_count": fetched_page_count,
            "llm_call_count": llm_call_count,
            "change_event_count": change_event_count,
            "affected_claim_count": affected_claim_count,
            "result_report_version_id": result_report_version_id,
            "draft_patch_id": draft_patch_id,
        }
        for name, value in values.items():
            if value is not None:
                assignments.append(f"{name} = ?")
                params.append(value)
        if not assignments:
            return
        params.append(run_id)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"UPDATE monitoring_runs SET {', '.join(assignments)} WHERE run_id = ?",
                tuple(params),
            )

    def update_monitoring_run_plan(
        self,
        run_id: str,
        *,
        context: dict | None = None,
        delta_plan: dict | None = None,
        stage: str | None = None,
        search_request_count: int | None = None,
        fetched_page_count: int | None = None,
    ) -> None:
        assignments = []
        params: list[object] = []
        values: dict[str, object | None] = {
            "stage": stage,
            "context_json": (
                json.dumps(context, ensure_ascii=False, sort_keys=True)
                if context is not None
                else None
            ),
            "delta_plan_json": (
                json.dumps(delta_plan, ensure_ascii=False, sort_keys=True)
                if delta_plan is not None
                else None
            ),
            "search_request_count": search_request_count,
            "fetched_page_count": fetched_page_count,
        }
        for name, value in values.items():
            if value is not None:
                assignments.append(f"{name} = ?")
                params.append(value)
        if not assignments:
            return
        params.append(run_id)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"UPDATE monitoring_runs SET {', '.join(assignments)} WHERE run_id = ?",
                tuple(params),
            )

    def complete_monitoring_run(
        self,
        run_id: str,
        job_id: str,
        *,
        decision: str,
        error_message: str | None = None,
    ) -> None:
        now = utc_now()
        status = (
            "cancelled"
            if decision == "cancelled" and not error_message
            else "failed" if error_message else "completed"
        )
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE monitoring_runs
                SET status = ?, decision = ?, error_message = ?,
                    error_summary = ?, completed_at = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    decision,
                    error_message,
                    error_message[:1000] if error_message else None,
                    now,
                    run_id,
                ),
            )
            if error_message:
                connection.execute(
                    """
                    UPDATE monitoring_configs
                    SET last_failure_at = ?, last_error = ?,
                        last_failed_run_at = ?,
                        last_decision = ?,
                        consecutive_failure_count = consecutive_failure_count + 1,
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (now, error_message, now, decision, now, job_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE monitoring_configs
                    SET last_success_at = ?, last_error = NULL,
                        last_successful_run_at = ?,
                        last_decision = ?, consecutive_failure_count = 0,
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (now, now, decision, now, job_id),
                )

    def get_running_monitoring_run(self, job_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM monitoring_runs
                WHERE job_id = ? AND status = 'running'
                ORDER BY started_at DESC LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_latest_monitoring_run(self, job_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM monitoring_runs
                WHERE job_id = ?
                ORDER BY started_at DESC LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        return dict(row) if row else None

    def try_start_notification_delivery(
        self,
        *,
        job_id: str,
        monitor_run_id: str | None,
        notification_type: str,
        dedup_key: str,
        chat_id: str,
    ) -> tuple[bool, str]:
        notification_id = str(uuid.uuid4())
        try:
            with self._lock, self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO notification_deliveries (
                        notification_id, job_id, monitor_run_id,
                        notification_type, dedup_key, chat_id, status,
                        attempts, last_error, sent_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'sending', 1, NULL, NULL, ?)
                    """,
                    (
                        notification_id,
                        job_id,
                        monitor_run_id,
                        notification_type,
                        dedup_key,
                        chat_id,
                        utc_now(),
                    ),
                )
            return True, notification_id
        except sqlite3.IntegrityError:
            with self._lock, self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT notification_id, status
                    FROM notification_deliveries
                    WHERE dedup_key = ?
                    """,
                    (dedup_key,),
                ).fetchone()
                if not row or row["status"] != "failed":
                    return False, ""
                connection.execute(
                    """
                    UPDATE notification_deliveries
                    SET status = 'sending',
                        attempts = attempts + 1,
                        last_error = NULL
                    WHERE notification_id = ?
                    """,
                    (row["notification_id"],),
                )
                return True, row["notification_id"]

    def mark_notification_delivery_sent(self, notification_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE notification_deliveries
                SET status = 'sent', sent_at = ?, last_error = NULL
                WHERE notification_id = ?
                """,
                (utc_now(), notification_id),
            )

    def mark_notification_delivery_failed(
        self, notification_id: str, error: str
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE notification_deliveries
                SET status = 'failed', last_error = ?
                WHERE notification_id = ?
                """,
                (error[:1000], notification_id),
            )

    def add_monitoring_source_snapshot(
        self,
        *,
        job_id: str,
        source_id: str,
        url: str,
        content_hash: str | None,
        status: str,
        run_id: str | None = None,
        http_status: int | None = None,
        content_type: str | None = None,
        raw_text: str | None = None,
        raw_object_path: str | None = None,
        published_at: str | None = None,
        retrieval_method: str | None = None,
        error_message: str | None = None,
    ) -> str:
        snapshot_id = str(uuid.uuid4())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitoring_source_snapshots (
                    snapshot_id, job_id, source_id, run_id, monitor_run_id,
                    url, http_status,
                    content_type, content_hash, raw_text, raw_object_path,
                    published_at, retrieval_method, observed_at, retrieved_at,
                    status, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    job_id,
                    source_id,
                    run_id,
                    run_id,
                    url,
                    http_status,
                    content_type,
                    content_hash,
                    raw_text,
                    raw_object_path,
                    published_at,
                    retrieval_method,
                    utc_now(),
                    utc_now(),
                    status,
                    error_message,
                    utc_now(),
                ),
            )
        return snapshot_id

    def get_latest_source_snapshot(
        self, job_id: str, source_id: str, *, status: str = "fetched"
    ) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM monitoring_source_snapshots
                WHERE job_id = ? AND source_id = ? AND status = ?
                ORDER BY retrieved_at DESC, created_at DESC
                LIMIT 1
                """,
                (job_id, source_id, status),
            ).fetchone()
        return dict(row) if row else None

    def list_source_snapshots(
        self,
        job_id: str,
        *,
        run_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        clauses = ["job_id = ?"]
        params: list[object] = [job_id]
        if run_id is not None:
            clauses.append("COALESCE(monitor_run_id, run_id) = ?")
            params.append(run_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM monitoring_source_snapshots
                WHERE {' AND '.join(clauses)}
                ORDER BY retrieved_at, snapshot_id
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_monitoring_watch_target(
        self,
        *,
        job_id: str,
        source_id: str,
        target_type: str,
        url: str,
        canonical_url: str,
        status: str = "active",
    ) -> str:
        now = utc_now()
        watch_target_id = str(uuid.uuid4())
        with self._lock, self._connect() as connection:
            existing = connection.execute(
                """
                SELECT watch_target_id FROM monitoring_watch_targets
                WHERE job_id = ? AND source_id = ? AND target_type = ?
                """,
                (job_id, source_id, target_type),
            ).fetchone()
            if existing:
                watch_target_id = str(existing["watch_target_id"])
            connection.execute(
                """
                INSERT INTO monitoring_watch_targets (
                    watch_target_id, job_id, source_id, target_type, url,
                    canonical_url, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, source_id, target_type) DO UPDATE SET
                    url=excluded.url,
                    canonical_url=excluded.canonical_url,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    watch_target_id,
                    job_id,
                    source_id,
                    target_type,
                    url,
                    canonical_url,
                    status,
                    now,
                    now,
                ),
            )
        return watch_target_id

    def list_monitoring_watch_targets(
        self, job_id: str, status: str | None = "active"
    ) -> list[dict]:
        query = """
            SELECT wt.*, s.title, s.content_hash, s.raw_text
            FROM monitoring_watch_targets wt
            JOIN sources s
                ON s.job_id = wt.job_id AND s.source_id = wt.source_id
            WHERE wt.job_id = ?
        """
        params: list[object] = [job_id]
        if status is not None:
            query += " AND wt.status = ?"
            params.append(status)
        query += " ORDER BY wt.created_at, wt.source_id"
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def add_change_event(
        self,
        *,
        job_id: str,
        event_type: str,
        severity: str,
        summary: str,
        run_id: str | None = None,
        source_id: str | None = None,
        entity: str | None = None,
        old_value: str | None = None,
        new_value: str | None = None,
        old_value_json: dict | None = None,
        new_value_json: dict | None = None,
        effective_at: str | None = None,
        novelty_level: str | None = None,
        materiality_level: str | None = None,
        confidence_band: str | None = None,
        event_fingerprint: str | None = None,
        evidence_ids: list[str] | None = None,
        evidence_relation: str = "support",
        status: str = "open",
    ) -> str:
        if event_fingerprint:
            with self._lock, self._connect() as connection:
                existing = connection.execute(
                    """
                    SELECT event_id FROM change_events
                    WHERE job_id = ? AND event_fingerprint = ?
                    """,
                    (job_id, event_fingerprint),
                ).fetchone()
                if existing:
                    self._insert_change_event_evidence_rows(
                        connection,
                        event_id=str(existing["event_id"]),
                        evidence_ids=evidence_ids or [],
                        relation=evidence_relation,
                        created_at=utc_now(),
                    )
                    return str(existing["event_id"])
        event_id = str(uuid.uuid4())
        now = utc_now()
        status = self._normalize_change_event_status(status)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO change_events (
                    event_id, job_id, run_id, source_id, entity, event_type,
                    severity, novelty_level, materiality_level, confidence_band,
                    summary, old_value, new_value, old_value_json, new_value_json,
                    effective_at, detected_at, event_fingerprint, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    job_id,
                    run_id,
                    source_id,
                    entity,
                    event_type,
                    severity,
                    novelty_level or severity,
                    materiality_level or severity,
                    confidence_band or "medium",
                    summary,
                    old_value,
                    new_value,
                    json.dumps(old_value_json, ensure_ascii=False)
                    if old_value_json is not None
                    else None,
                    json.dumps(new_value_json, ensure_ascii=False)
                    if new_value_json is not None
                    else None,
                    effective_at,
                    now,
                    event_fingerprint,
                    status,
                    now,
                ),
            )
            self._insert_change_event_evidence_rows(
                connection,
                event_id=event_id,
                evidence_ids=evidence_ids or [],
                relation=evidence_relation,
                created_at=now,
            )
        return event_id

    def link_change_event_evidence(
        self,
        *,
        change_event_id: str,
        evidence_id: str,
        relation: str = "support",
    ) -> None:
        with self._lock, self._connect() as connection:
            self._insert_change_event_evidence_rows(
                connection,
                event_id=change_event_id,
                evidence_ids=[evidence_id],
                relation=relation,
                created_at=utc_now(),
            )

    def list_change_event_evidence(
        self, change_event_id: str
    ) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM change_event_evidence
                WHERE change_event_id = ?
                ORDER BY created_at, evidence_id
                """,
                (change_event_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_change_events(
        self, job_id: str, status: str | None = None
    ) -> list[dict]:
        sql = "SELECT * FROM change_events WHERE job_id = ?"
        params: tuple[object, ...] = (job_id,)
        if status is not None:
            if status == "open":
                sql += " AND status IN ('detected', 'confirmed')"
                params = (job_id,)
            else:
                sql += " AND status = ?"
                params = (job_id, self._normalize_change_event_status(status))
        sql += " ORDER BY created_at DESC"
        with self._lock, self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def list_evidence_backed_change_events(
        self, job_id: str, status: str | None = None
    ) -> list[dict]:
        events = self.list_change_events(job_id, status)
        backed = []
        for event in events:
            links = self.list_change_event_evidence(event["event_id"])
            if not links:
                continue
            event["evidence_ids"] = [link["evidence_id"] for link in links]
            event["supporting_evidence_ids"] = [
                link["evidence_id"]
                for link in links
                if link["relation"] == "support"
            ]
            event["contradicting_evidence_ids"] = [
                link["evidence_id"]
                for link in links
                if link["relation"] == "contradict"
            ]
            backed.append(event)
        return backed

    def mark_change_events_applied(self, job_id: str, event_ids: list[str]) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" for _ in event_ids)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"""
                UPDATE change_events
                SET status = 'applied'
                WHERE job_id = ? AND event_id IN ({placeholders})
                """,
                (job_id, *event_ids),
            )

    def resolve_change_events(self, job_id: str, event_ids: list[str]) -> None:
        self.mark_change_events_applied(job_id, event_ids)

    def replace_claim_impacts(
        self, job_id: str, event_ids: list[str], impacts: list[dict]
    ) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" for _ in event_ids)
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                f"""
                DELETE FROM claim_impacts
                WHERE job_id = ? AND event_id IN ({placeholders})
                """,
                (job_id, *event_ids),
            )
            for item in impacts:
                connection.execute(
                    """
                    INSERT INTO claim_impacts (
                        impact_id, job_id, event_id, claim_id, section_id,
                        impact_type, severity, impact_level,
                        old_confidence_band, proposed_confidence_band,
                        affected_section_ids_json, requires_review,
                        rationale, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        job_id,
                        item["event_id"],
                        item.get("claim_id"),
                        item["section_id"],
                        item["impact_type"],
                        item["severity"],
                        item.get("impact_level", item["severity"]),
                        item.get("old_confidence_band"),
                        item.get("proposed_confidence_band"),
                        json.dumps(
                            item.get("affected_section_ids")
                            or [item["section_id"]],
                            ensure_ascii=False,
                        ),
                        1
                        if item.get("requires_review")
                        or item["severity"] in {"high", "critical"}
                        else 0,
                        item["rationale"],
                        now,
                    ),
                )

    def list_claim_impacts(
        self, job_id: str, event_ids: list[str] | None = None
    ) -> list[dict]:
        sql = "SELECT * FROM claim_impacts WHERE job_id = ?"
        params: list[object] = [job_id]
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            sql += f" AND event_id IN ({placeholders})"
            params.extend(event_ids)
        sql += " ORDER BY created_at, event_id, claim_id"
        with self._lock, self._connect() as connection:
            rows = connection.execute(sql, tuple(params)).fetchall()
        impacts = []
        for row in rows:
            item = dict(row)
            if item.get("affected_section_ids_json"):
                item["affected_section_ids_json"] = json.loads(
                    item["affected_section_ids_json"]
                )
            impacts.append(item)
        return impacts

    def add_report_revision(
        self,
        *,
        job_id: str,
        report_version_id: str,
        base_report_version_id: str | None,
        revision_type: str,
        impacted_section_ids: list[str],
        impacted_claim_ids: list[str],
        change_event_ids: list[str],
        summary: str,
        status: str,
        patch_json: dict | None = None,
    ) -> str:
        revision_id = str(uuid.uuid4())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO report_revisions (
                    revision_id, job_id, report_version_id,
                    base_report_version_id, revision_type,
                    impacted_section_ids, impacted_claim_ids, change_event_ids,
                    summary, status, created_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(report_version_id) DO UPDATE SET
                    base_report_version_id=excluded.base_report_version_id,
                    revision_type=excluded.revision_type,
                    impacted_section_ids=excluded.impacted_section_ids,
                    impacted_claim_ids=excluded.impacted_claim_ids,
                    change_event_ids=excluded.change_event_ids,
                    summary=excluded.summary,
                    status=excluded.status
                """,
                (
                    revision_id,
                    job_id,
                    report_version_id,
                    base_report_version_id,
                    revision_type,
                    json.dumps(impacted_section_ids, ensure_ascii=False),
                    json.dumps(impacted_claim_ids, ensure_ascii=False),
                    json.dumps(change_event_ids, ensure_ascii=False),
                    summary,
                    status,
                    utc_now(),
                ),
            )
            self._upsert_report_patch(
                connection,
                revision_id=revision_id,
                job_id=job_id,
                report_version_id=report_version_id,
                base_report_version_id=base_report_version_id,
                revision_type=revision_type,
                impacted_section_ids=impacted_section_ids,
                impacted_claim_ids=impacted_claim_ids,
                change_event_ids=change_event_ids,
                summary=summary,
                status=status,
                patch_json=patch_json,
            )
        return revision_id

    def _upsert_report_patch(
        self,
        connection: sqlite3.Connection,
        *,
        revision_id: str,
        job_id: str,
        report_version_id: str,
        base_report_version_id: str | None,
        revision_type: str,
        impacted_section_ids: list[str],
        impacted_claim_ids: list[str],
        change_event_ids: list[str],
        summary: str,
        status: str,
        patch_json: dict | None = None,
    ) -> None:
        report = connection.execute(
            """
            SELECT monitor_run_id FROM report_versions
            WHERE report_version_id = ?
            """,
            (report_version_id,),
        ).fetchone()
        patch = connection.execute(
            """
            SELECT patch_id FROM report_patches
            WHERE report_revision_id = ?
            """,
            (revision_id,),
        ).fetchone()
        patch_id = patch["patch_id"] if patch else str(uuid.uuid4())
        approval_status = {
            "draft": "pending",
            "published": "published",
            "rejected": "rejected",
        }.get(status, status)
        patch_payload = patch_json or {
            "revision_type": revision_type,
            "impacted_section_ids": impacted_section_ids,
            "impacted_claim_ids": impacted_claim_ids,
            "change_event_ids": change_event_ids,
        }
        patch_json_text = json.dumps(patch_payload, ensure_ascii=False)
        connection.execute(
            """
            INSERT INTO report_patches (
                patch_id, job_id, monitor_run_id, report_version_id,
                report_revision_id, base_report_version_id, patch_json,
                change_summary, decision, validation_status,
                approval_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(report_revision_id) DO UPDATE SET
                monitor_run_id=excluded.monitor_run_id,
                report_version_id=excluded.report_version_id,
                base_report_version_id=excluded.base_report_version_id,
                patch_json=excluded.patch_json,
                change_summary=excluded.change_summary,
                decision=excluded.decision,
                validation_status=excluded.validation_status,
                approval_status=excluded.approval_status
            """,
            (
                patch_id,
                job_id,
                report["monitor_run_id"] if report else None,
                report_version_id,
                revision_id,
                base_report_version_id,
                patch_json_text,
                summary,
                "review_required" if approval_status == "pending" else "auto_patch",
                "passed" if approval_status in {"pending", "published"} else "not_run",
                approval_status,
                utc_now(),
            ),
        )

    def add_pending_report_patch(
        self,
        *,
        patch_id: str,
        job_id: str,
        monitor_run_id: str | None,
        base_report_version_id: str | None,
        patch_json: dict,
        change_summary: str,
        decision: str = "review_required",
        approval_status: str = "pending",
    ) -> str:
        if approval_status not in {"pending", "not_required"}:
            raise ValueError("invalid unpublished patch approval status")
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO report_patches (
                    patch_id, job_id, monitor_run_id, report_version_id,
                    report_revision_id, base_report_version_id, patch_json,
                    change_summary, decision, validation_status,
                    approval_status, created_at
                ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_run', ?, ?)
                ON CONFLICT(patch_id) DO UPDATE SET
                    monitor_run_id=excluded.monitor_run_id,
                    base_report_version_id=excluded.base_report_version_id,
                    patch_json=excluded.patch_json,
                    change_summary=excluded.change_summary,
                    decision=excluded.decision,
                    validation_status='not_run',
                    approval_status=excluded.approval_status
                """,
                (
                    patch_id,
                    job_id,
                    monitor_run_id,
                    base_report_version_id,
                    json.dumps(patch_json, ensure_ascii=False),
                    change_summary,
                    decision,
                    approval_status,
                    now,
                ),
            )
        return patch_id

    def publish_pending_report_patch(
        self, patch_id: str, *, approved_by: str | None = None
    ) -> dict:
        now = utc_now()
        with self._lock, self._connect() as connection:
            patch = connection.execute(
                """
                SELECT * FROM report_patches
                WHERE patch_id = ?
                """,
                (patch_id,),
            ).fetchone()
            if not patch:
                raise ValueError("report patch not found")
            if (
                patch["approval_status"] == "published"
                and patch["report_version_id"]
            ):
                report = connection.execute(
                    """
                    SELECT version FROM report_versions
                    WHERE report_version_id = ?
                    """,
                    (patch["report_version_id"],),
                ).fetchone()
                return {
                    "report_version_id": patch["report_version_id"],
                    "revision_id": patch["report_revision_id"],
                    "version": int(report["version"]),
                    "job_id": patch["job_id"],
                    "change_event_ids": json.loads(patch["patch_json"]).get(
                        "change_event_ids", []
                    ),
                    "already_published": True,
                }
            if patch["approval_status"] not in {"pending", "not_required"}:
                raise ValueError(f"report patch is {patch['approval_status']}")
            if patch["report_version_id"]:
                raise ValueError("report patch already has report version")
            patch_payload = json.loads(patch["patch_json"])
            report_path = patch_payload.get("report_path")
            report_json_path = patch_payload.get("report_json_path")
            if not report_path or not report_json_path:
                raise ValueError("pending patch artifact paths are empty")
            current = connection.execute(
                """
                SELECT report_version_id
                FROM report_versions
                WHERE job_id = ? AND status = 'published'
                ORDER BY version DESC
                LIMIT 1
                """,
                (patch["job_id"],),
            ).fetchone()
            current_id = current["report_version_id"] if current else None
            if current_id != patch["base_report_version_id"]:
                raise ValueError("base report version changed")
            version = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM report_versions WHERE job_id = ?
                    """,
                    (patch["job_id"],),
                ).fetchone()[0]
            )
            report_version_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO report_versions (
                    report_version_id, job_id, version, report_path,
                    report_json_path, parent_report_version_id, monitor_run_id,
                    trigger_type, change_summary, status, publication_status,
                    published_at, validation_error, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'monitor_approved_patch',
                    ?, 'published', 'published', ?, NULL, ?)
                """,
                (
                    report_version_id,
                    patch["job_id"],
                    version,
                    report_path,
                    report_json_path,
                    patch["base_report_version_id"],
                    patch["monitor_run_id"],
                    patch["change_summary"],
                    now,
                    now,
                ),
            )
            revision_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO report_revisions (
                    revision_id, job_id, report_version_id,
                    base_report_version_id, revision_type,
                    impacted_section_ids, impacted_claim_ids, change_event_ids,
                    summary, status, created_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?, ?)
                """,
                (
                    revision_id,
                    patch["job_id"],
                    report_version_id,
                    patch["base_report_version_id"],
                    patch_payload.get("revision_type", "partial"),
                    json.dumps(
                        patch_payload.get("impacted_section_ids", []),
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        patch_payload.get("impacted_claim_ids", []),
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        patch_payload.get("change_event_ids", []),
                        ensure_ascii=False,
                    ),
                    patch["change_summary"],
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE report_patches
                SET report_version_id = ?, report_revision_id = ?,
                    approval_status = 'published',
                    validation_status = 'passed',
                    approved_at = COALESCE(approved_at, ?),
                    approved_by = ?
                WHERE patch_id = ?
                """,
                (report_version_id, revision_id, now, approved_by, patch_id),
            )
            for claim in patch_payload.get("claim_revisions", []):
                previous = connection.execute(
                    """
                    SELECT claim_revision_id
                    FROM claim_revisions
                    WHERE job_id = ? AND original_claim_id = ?
                        AND status = 'active'
                    ORDER BY created_at DESC, claim_revision_id DESC
                    LIMIT 1
                    """,
                    (patch["job_id"], claim.get("original_claim_id")),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE claim_revisions
                    SET status = 'superseded'
                    WHERE job_id = ? AND original_claim_id = ?
                        AND status = 'active'
                    """,
                    (patch["job_id"], claim.get("original_claim_id")),
                )
                connection.execute(
                    """
                    INSERT INTO claim_revisions (
                        claim_revision_id, job_id, original_claim_id,
                        supersedes_claim_revision_id, report_version_id,
                        statement, confidence_band, reason,
                        supporting_evidence_ids_json,
                        contradicting_evidence_ids_json, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        patch["job_id"],
                        claim.get("original_claim_id"),
                        (
                            previous["claim_revision_id"]
                            if previous
                            else claim.get("supersedes_claim_revision_id")
                        ),
                        report_version_id,
                        claim["statement"],
                        claim["confidence_band"],
                        claim["reason"],
                        json.dumps(
                            claim.get("supporting_evidence_ids", []),
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            claim.get("contradicting_evidence_ids", []),
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )
        return {
            "report_version_id": report_version_id,
            "revision_id": revision_id,
            "version": version,
            "job_id": patch["job_id"],
            "change_event_ids": patch_payload.get("change_event_ids", []),
            "already_published": False,
        }

    def delete_unpublished_report_patch(self, patch_id: str) -> list[str]:
        with self._lock, self._connect() as connection:
            patch = connection.execute(
                """
                SELECT report_version_id, approval_status, patch_json
                FROM report_patches
                WHERE patch_id = ?
                """,
                (patch_id,),
            ).fetchone()
            if not patch:
                return []
            if patch["report_version_id"] is not None:
                raise ValueError("published report patch cannot be deleted")
            if patch["approval_status"] not in {"pending", "not_required"}:
                raise ValueError("only unpublished report patches can be deleted")
            payload = json.loads(patch["patch_json"])
            connection.execute(
                "DELETE FROM report_patches WHERE patch_id = ?",
                (patch_id,),
            )
        return [
            str(path)
            for path in (
                payload.get("report_path"),
                payload.get("report_json_path"),
            )
            if path
        ]

    def reject_report_patch(
        self,
        patch_id: str,
        *,
        reason: str | None = None,
        rejected_by: str | None = None,
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_patches
                SET approval_status = 'rejected',
                    rejected_at = ?,
                    rejected_by = ?,
                    rejection_reason = ?
                WHERE patch_id = ? AND approval_status = 'pending'
                """,
                (utc_now(), rejected_by, reason, patch_id),
            )

    def mark_report_patch_validation(
        self, patch_id: str, status: str
    ) -> None:
        if status not in {"not_run", "passed", "failed"}:
            raise ValueError("invalid report patch validation status")
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_patches
                SET validation_status = ?
                WHERE patch_id = ?
                """,
                (status, patch_id),
            )

    def mark_report_revision_published(self, report_version_id: str) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_revisions
                SET status = 'published', published_at = ?
                WHERE report_version_id = ?
                """,
                (now, report_version_id),
            )
            connection.execute(
                """
                UPDATE report_patches
                SET approval_status = 'published',
                    validation_status = 'passed',
                    approved_at = COALESCE(approved_at, ?)
                WHERE report_version_id = ?
                """,
                (now, report_version_id),
            )

    def mark_report_revision_rejected(
        self,
        report_version_id: str,
        reason: str | None = None,
        rejected_by: str | None = None,
    ) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_revisions
                SET status = 'rejected', summary = CASE
                    WHEN ? IS NULL OR ? = '' THEN summary
                    ELSE summary || '\n\n拒绝原因：' || ?
                END
                WHERE report_version_id = ?
                """,
                (reason, reason, reason, report_version_id),
            )
            connection.execute(
                """
                UPDATE report_patches
                SET approval_status = 'rejected',
                    rejected_at = ?,
                    rejected_by = ?,
                    rejection_reason = ?
                WHERE report_version_id = ?
                """,
                (now, rejected_by, reason, report_version_id),
            )

    def get_report_revision(self, report_version_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_revisions WHERE report_version_id = ?",
                (report_version_id,),
            ).fetchone()
        return self._decode_report_revision(row)

    def get_report_revision_by_id(self, revision_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT r.*, rv.version, rv.report_path, rv.report_json_path,
                    rv.status AS report_status, rv.validation_error,
                    j.creator_id, j.topic
                FROM report_revisions r
                JOIN report_versions rv
                    ON rv.report_version_id = r.report_version_id
                JOIN jobs j ON j.job_id = r.job_id
                WHERE r.revision_id = ?
                """,
                (revision_id,),
            ).fetchone()
        return self._decode_report_revision(row)

    def get_report_revision_by_patch_id(self, patch_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT r.*, rv.version, rv.report_path, rv.report_json_path,
                    rv.status AS report_status, rv.validation_error,
                    j.creator_id, j.topic
                FROM report_patches p
                JOIN report_revisions r
                    ON r.revision_id = p.report_revision_id
                JOIN report_versions rv
                    ON rv.report_version_id = r.report_version_id
                JOIN jobs j ON j.job_id = r.job_id
                WHERE p.patch_id = ?
                """,
                (patch_id,),
            ).fetchone()
        return self._decode_report_revision(row)

    def get_report_patch(self, patch_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT p.*, rv.version
                FROM report_patches p
                LEFT JOIN report_versions rv
                    ON rv.report_version_id = p.report_version_id
                WHERE p.patch_id = ?
                """,
                (patch_id,),
            ).fetchone()
        return self._decode_report_patch(row)

    def get_report_patch_by_revision_id(self, revision_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_patches WHERE report_revision_id = ?",
                (revision_id,),
            ).fetchone()
        return self._decode_report_patch(row)

    def get_report_patch_by_report_version_id(
        self, report_version_id: str
    ) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_patches WHERE report_version_id = ?",
                (report_version_id,),
            ).fetchone()
        return self._decode_report_patch(row)

    def list_report_patches(
        self,
        *,
        creator_id: str | None = None,
        job_id: str | None = None,
        approval_status: str | None = None,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []
        if creator_id is not None:
            clauses.append("j.creator_id = ?")
            params.append(creator_id)
        if job_id is not None:
            clauses.append("p.job_id = ?")
            params.append(job_id)
        if approval_status is not None:
            clauses.append("p.approval_status = ?")
            params.append(approval_status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT p.*, rv.version, j.creator_id, j.topic
                FROM report_patches p
                LEFT JOIN report_versions rv
                    ON rv.report_version_id = p.report_version_id
                JOIN jobs j ON j.job_id = p.job_id
                {where}
                ORDER BY p.created_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [self._decode_report_patch(row) for row in rows]

    def list_report_revisions(
        self,
        *,
        creator_id: str | None = None,
        job_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []
        if creator_id is not None:
            clauses.append("j.creator_id = ?")
            params.append(creator_id)
        if job_id is not None:
            clauses.append("r.job_id = ?")
            params.append(job_id)
        if status is not None:
            clauses.append("r.status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT r.*, rv.version, rv.report_path, rv.report_json_path,
                    rv.status AS report_status, rv.validation_error,
                    j.creator_id, j.topic
                FROM report_revisions r
                JOIN report_versions rv
                    ON rv.report_version_id = r.report_version_id
                JOIN jobs j ON j.job_id = r.job_id
                {where}
                ORDER BY r.created_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [self._decode_report_revision(row) for row in rows if row]

    def add_claim_revision(
        self,
        *,
        job_id: str,
        original_claim_id: str | None,
        supersedes_claim_revision_id: str | None,
        report_version_id: str,
        statement: str,
        confidence_band: str,
        reason: str,
        supporting_evidence_ids: list[str],
        contradicting_evidence_ids: list[str],
        status: str,
    ) -> str:
        claim_revision_id = str(uuid.uuid4())
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO claim_revisions (
                    claim_revision_id, job_id, original_claim_id,
                    supersedes_claim_revision_id, report_version_id,
                    statement, confidence_band, reason,
                    supporting_evidence_ids_json,
                    contradicting_evidence_ids_json, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    claim_revision_id,
                    job_id,
                    original_claim_id,
                    supersedes_claim_revision_id,
                    report_version_id,
                    statement,
                    confidence_band,
                    reason,
                    json.dumps(supporting_evidence_ids, ensure_ascii=False),
                    json.dumps(contradicting_evidence_ids, ensure_ascii=False),
                    status,
                    utc_now(),
                ),
            )
        return claim_revision_id

    def list_claim_revisions(
        self, job_id: str, report_version_id: str | None = None
    ) -> list[dict]:
        query = "SELECT * FROM claim_revisions WHERE job_id = ?"
        params: list[object] = [job_id]
        if report_version_id is not None:
            query += " AND report_version_id = ?"
            params.append(report_version_id)
        query += " ORDER BY created_at, claim_revision_id"
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._decode_claim_revision(row) for row in rows]

    def activate_claim_revisions(self, report_version_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE claim_revisions
                SET status = 'active'
                WHERE report_version_id = ? AND status = 'draft'
                """,
                (report_version_id,),
            )

    def reject_claim_revisions(self, report_version_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE claim_revisions
                SET status = 'rejected'
                WHERE report_version_id = ? AND status = 'draft'
                """,
                (report_version_id,),
            )

    @staticmethod
    def _decode_report_revision(row) -> Optional[dict]:
        if not row:
            return None
        result = dict(row)
        for key in (
            "impacted_section_ids",
            "impacted_claim_ids",
            "change_event_ids",
        ):
            result[key] = json.loads(result[key])
        return result

    @staticmethod
    def _decode_report_patch(row) -> Optional[dict]:
        if not row:
            return None
        result = dict(row)
        result["patch_json"] = json.loads(result["patch_json"])
        if result.get("version") is None:
            result["version"] = result["patch_json"].get("target_version")
        return result

    @staticmethod
    def _decode_claim_revision(row) -> dict:
        result = dict(row)
        result["supporting_evidence_ids"] = json.loads(
            result.pop("supporting_evidence_ids_json")
        )
        result["contradicting_evidence_ids"] = json.loads(
            result.pop("contradicting_evidence_ids_json")
        )
        return result

    def save_monitor_registration_request(
        self,
        *,
        job_id: str,
        creator_id: str,
        chat_id: str,
        schedule_kind: str,
        schedule_value: str,
        timezone: str,
        mode: str,
        notify_level: str,
    ) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitor_registration_requests (
                    job_id, creator_id, chat_id, schedule_kind,
                    schedule_value, timezone, mode, notify_level, status,
                    error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    schedule_kind=excluded.schedule_kind,
                    schedule_value=excluded.schedule_value,
                    timezone=excluded.timezone,
                    mode=excluded.mode,
                    notify_level=excluded.notify_level,
                    status='pending',
                    error_message=NULL,
                    updated_at=excluded.updated_at
                """,
                (
                    job_id,
                    creator_id,
                    chat_id,
                    schedule_kind,
                    schedule_value,
                    timezone,
                    mode,
                    notify_level,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE jobs
                SET monitor_registration_status = 'pending',
                    monitor_registration_error = NULL,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (now, job_id),
            )

    def get_monitor_registration_request(self, job_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM monitor_registration_requests WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return dict(row) if row else None

    def mark_monitor_registration(
        self, job_id: str, status: str, error_message: str | None = None
    ) -> None:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE monitor_registration_requests
                SET status = ?, error_message = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (status, error_message[:1000] if error_message else None, now, job_id),
            )
            connection.execute(
                """
                UPDATE jobs
                SET monitor_registration_status = ?,
                    monitor_registration_error = ?,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (
                    status,
                    error_message[:1000] if error_message else None,
                    now,
                    job_id,
                ),
            )

    def clear_claims_and_reports(self, job_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "DELETE FROM report_revisions WHERE job_id = ?", (job_id,)
            )
            connection.execute(
                "DELETE FROM claim_evidence WHERE job_id = ?", (job_id,)
            )
            connection.execute("DELETE FROM claims WHERE job_id = ?", (job_id,))
            connection.execute(
                "DELETE FROM report_versions WHERE job_id = ?", (job_id,)
            )

    def claim_message(self, message_id: str, chat_id: str, sender_id: str) -> bool:
        try:
            with self._lock, self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO processed_messages
                        (message_id, chat_id, sender_id, received_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (message_id, chat_id, sender_id, utc_now()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def create_job(
        self,
        creator_id: str,
        chat_id: str,
        source_message_id: str,
        topic: str,
        execution_backend: str = "local",
        research_options: dict | None = None,
    ) -> Job:
        job_id = str(uuid.uuid4())
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    job_id, creator_id, chat_id, source_message_id, topic,
                    status, stage, progress, result_summary, error_message,
                    cancel_requested, execution_backend, workflow_status,
                    paused, notification_status, research_options_json,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, 'queued', '等待处理', 0, NULL, NULL, 0,
                    ?, NULL, 0, NULL, ?, ?, ?
                )
                """,
                (
                    job_id,
                    creator_id,
                    chat_id,
                    source_message_id,
                    topic,
                    execution_backend,
                    json.dumps(research_options or {}, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        return self.get_job(job_id)  # type: ignore[return-value]

    def get_research_options(self, job_id: str) -> dict:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT research_options_json FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if not row or not row["research_options_json"]:
            return {}
        try:
            return json.loads(row["research_options_json"])
        except json.JSONDecodeError:
            return {}

    def upsert_research_draft(
        self,
        *,
        creator_id: str,
        chat_id: str,
        source_message_id: str,
        topic: str,
        options: dict,
    ) -> dict:
        draft_id = str(uuid.uuid4())
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE research_drafts
                SET status = 'superseded', updated_at = ?
                WHERE creator_id = ? AND chat_id = ? AND status = 'active'
                """,
                (now, creator_id, chat_id),
            )
            connection.execute(
                """
                INSERT INTO research_drafts (
                    draft_id, creator_id, chat_id, source_message_id, topic,
                    options_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    draft_id,
                    creator_id,
                    chat_id,
                    source_message_id,
                    topic,
                    json.dumps(options, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        return self.get_research_draft(draft_id)  # type: ignore[return-value]

    def get_research_draft(self, draft_id: str) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM research_drafts WHERE draft_id = ?",
                (draft_id,),
            ).fetchone()
        return self._research_draft_row(row) if row else None

    def get_active_research_draft(
        self, *, creator_id: str, chat_id: str
    ) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM research_drafts
                WHERE creator_id = ? AND chat_id = ? AND status = 'active'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (creator_id, chat_id),
            ).fetchone()
        return self._research_draft_row(row) if row else None

    def update_research_draft_options(
        self, draft_id: str, options: dict
    ) -> Optional[dict]:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE research_drafts
                SET options_json = ?, updated_at = ?
                WHERE draft_id = ? AND status = 'active'
                """,
                (json.dumps(options, ensure_ascii=False), utc_now(), draft_id),
            )
        return self.get_research_draft(draft_id)

    def close_research_draft(self, draft_id: str, status: str) -> None:
        if status not in {"submitted", "cancelled", "superseded"}:
            raise ValueError("invalid research draft status")
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE research_drafts
                SET status = ?, updated_at = ?
                WHERE draft_id = ?
                """,
                (status, utc_now(), draft_id),
            )

    @staticmethod
    def _research_draft_row(row) -> dict:
        data = dict(row)
        try:
            data["options"] = json.loads(data.pop("options_json") or "{}")
        except json.JSONDecodeError:
            data["options"] = {}
        return data

    def add_report_artifact(
        self,
        *,
        job_id: str,
        report_version_id: str | None,
        artifact_type: str,
        artifact_path: str,
        content_hash: str | None,
        status: str,
        error_message: str | None = None,
    ) -> str:
        with self._lock, self._connect() as connection:
            existing = connection.execute(
                """
                SELECT artifact_id
                FROM report_artifacts
                WHERE job_id = ?
                  AND report_version_id IS ?
                  AND artifact_type = ?
                  AND artifact_path = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    job_id,
                    report_version_id,
                    artifact_type,
                    artifact_path,
                ),
            ).fetchone()
            if existing:
                artifact_id = existing["artifact_id"]
                connection.execute(
                    """
                    UPDATE report_artifacts
                    SET content_hash = ?, status = ?, error_message = ?
                    WHERE artifact_id = ?
                    """,
                    (
                        content_hash,
                        status,
                        error_message[-1000:] if error_message else None,
                        artifact_id,
                    ),
                )
                return artifact_id
            artifact_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO report_artifacts (
                    artifact_id, job_id, report_version_id, artifact_type,
                    artifact_path, content_hash, status, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    job_id,
                    report_version_id,
                    artifact_type,
                    artifact_path,
                    content_hash,
                    status,
                    error_message[-1000:] if error_message else None,
                    utc_now(),
                ),
            )
        return artifact_id

    def list_report_artifacts(
        self,
        job_id: str,
        *,
        report_version_id: str | None = None,
        ready_only: bool = False,
    ) -> list[dict]:
        clauses = ["job_id = ?"]
        params: list[object] = [job_id]
        if report_version_id is not None:
            clauses.append("report_version_id = ?")
            params.append(report_version_id)
        if ready_only:
            clauses.append("status = 'ready'")
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM report_artifacts
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def try_start_artifact_delivery(
        self,
        *,
        artifact_id: str,
        job_id: str,
        chat_id: str,
        dedup_key: str,
    ) -> tuple[bool, str]:
        delivery_id = str(uuid.uuid4())
        try:
            with self._lock, self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO artifact_deliveries (
                        delivery_id, artifact_id, job_id, chat_id,
                        dedup_key, status, error_message,
                        delivered_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, 'sending', NULL, NULL, ?)
                    """,
                    (
                        delivery_id,
                        artifact_id,
                        job_id,
                        chat_id,
                        dedup_key,
                        utc_now(),
                    ),
                )
            return True, delivery_id
        except sqlite3.IntegrityError:
            with self._lock, self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT delivery_id, status
                    FROM artifact_deliveries
                    WHERE dedup_key = ?
                    """,
                    (dedup_key,),
                ).fetchone()
                if not row or row["status"] != "failed":
                    return False, ""
                connection.execute(
                    """
                    UPDATE artifact_deliveries
                    SET status = 'sending', error_message = NULL
                    WHERE delivery_id = ?
                    """,
                    (row["delivery_id"],),
                )
                return True, row["delivery_id"]

    def mark_artifact_delivery_sent(self, delivery_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE artifact_deliveries
                SET status = 'sent', delivered_at = ?, error_message = NULL
                WHERE delivery_id = ?
                """,
                (utc_now(), delivery_id),
            )

    def mark_artifact_delivery_failed(
        self,
        delivery_id: str,
        error: str,
        *,
        retryable: bool = True,
    ) -> None:
        status = "failed" if retryable else "rejected"
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE artifact_deliveries
                SET status = ?, error_message = ?
                WHERE delivery_id = ?
                """,
                (status, error[:1000], delivery_id),
            )

    def list_artifact_deliveries(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifact_deliveries
                WHERE job_id = ?
                ORDER BY created_at, delivery_id
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_source_asset(self, asset) -> str:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO source_assets (
                    asset_id, job_id, source_id, snapshot_id,
                    url, original_url, canonical_url,
                    file_name, generated_filename, original_filename,
                    content_type, declared_mime_type, detected_mime_type,
                    file_type, file_extension, size_bytes, byte_size,
                    content_hash, sha256, retrieved_at, published_at,
                    source_type, local_path, raw_object_path,
                    status, parse_status, parser_name, parser_version,
                    detection_confidence, detection_method, error_message,
                    created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(asset_id) DO UPDATE SET
                    source_id=excluded.source_id,
                    snapshot_id=excluded.snapshot_id,
                    canonical_url=excluded.canonical_url,
                    declared_mime_type=excluded.declared_mime_type,
                    detected_mime_type=excluded.detected_mime_type,
                    byte_size=excluded.byte_size,
                    sha256=excluded.sha256,
                    retrieved_at=excluded.retrieved_at,
                    raw_object_path=excluded.raw_object_path,
                    parse_status=excluded.parse_status,
                    parser_name=excluded.parser_name,
                    parser_version=excluded.parser_version,
                    detection_confidence=excluded.detection_confidence,
                    detection_method=excluded.detection_method,
                    error_message=excluded.error_message
                """,
                (
                    asset.asset_id,
                    asset.job_id,
                    asset.source_id,
                    asset.snapshot_id,
                    asset.original_url,
                    asset.original_url,
                    asset.canonical_url,
                    asset.generated_filename,
                    asset.generated_filename,
                    asset.original_filename,
                    asset.declared_mime_type,
                    asset.declared_mime_type,
                    asset.detected_mime_type,
                    asset.file_type,
                    asset.file_extension,
                    asset.byte_size,
                    asset.byte_size,
                    asset.sha256,
                    asset.sha256,
                    asset.retrieved_at,
                    asset.published_at,
                    asset.source_type,
                    str(asset.raw_object_path) if asset.raw_object_path else None,
                    str(asset.raw_object_path) if asset.raw_object_path else None,
                    asset.parse_status,
                    asset.parse_status,
                    asset.parser_name,
                    asset.parser_version,
                    asset.detection_confidence,
                    asset.detection_method,
                    asset.error_message,
                    utc_now(),
                ),
            )
        return asset.asset_id

    def list_source_assets(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM source_assets
                WHERE job_id = ?
                ORDER BY retrieved_at, asset_id
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_source_asset_parse_status(
        self,
        asset_id: str,
        *,
        parse_status: str,
        parser_name: str | None = None,
        parser_version: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE source_assets
                SET status = ?, parse_status = ?, parser_name = ?,
                    parser_version = ?, error_message = ?
                WHERE asset_id = ?
                """,
                (
                    parse_status,
                    parse_status,
                    parser_name,
                    parser_version,
                    error_message[:1000] if error_message else None,
                    asset_id,
                ),
            )

    def save_parsed_asset(self, job_id: str, parsed, *, parser_name: str) -> str:
        parsed_asset_id = f"{parsed.asset_id}:{parser_name}:{parsed.extraction_method}"
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO parsed_assets (
                    parsed_asset_id, asset_id, job_id, parser_name,
                    metadata_json, warning_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(parsed_asset_id) DO UPDATE SET
                    metadata_json=excluded.metadata_json,
                    warning_json=excluded.warning_json
                """,
                (
                    parsed_asset_id,
                    parsed.asset_id,
                    job_id,
                    parser_name,
                    json.dumps(parsed.metadata, ensure_ascii=False),
                    json.dumps(parsed.warnings, ensure_ascii=False),
                    now,
                ),
            )
            for block in parsed.text_blocks:
                connection.execute(
                    """
                    INSERT INTO parsed_text_blocks (
                        block_id, parsed_asset_id, job_id, page_number,
                        section, section_title, text, bbox_json,
                        source_locator, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(block_id) DO UPDATE SET
                        page_number=excluded.page_number,
                        section=excluded.section,
                        section_title=excluded.section_title,
                        text=excluded.text,
                        bbox_json=excluded.bbox_json,
                        source_locator=excluded.source_locator
                    """,
                    (
                        block.block_id,
                        parsed_asset_id,
                        job_id,
                        block.page_number,
                        block.section,
                        block.section,
                        block.text,
                        json.dumps(block.bbox) if block.bbox is not None else None,
                        block.source_locator,
                        now,
                    ),
                )
            for table in parsed.tables:
                connection.execute(
                    """
                    INSERT INTO parsed_tables (
                        table_id, parsed_asset_id, job_id, caption,
                        columns_json, rows_json, sheet_name, cell_range,
                        source_locator, extraction_method, metadata_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(table_id) DO UPDATE SET
                        caption=excluded.caption,
                        columns_json=excluded.columns_json,
                        rows_json=excluded.rows_json,
                        sheet_name=excluded.sheet_name,
                        cell_range=excluded.cell_range,
                        source_locator=excluded.source_locator,
                        extraction_method=excluded.extraction_method,
                        metadata_json=excluded.metadata_json
                    """,
                    (
                        table.table_id,
                        parsed_asset_id,
                        job_id,
                        table.caption,
                        json.dumps(table.columns, ensure_ascii=False),
                        json.dumps(table.rows, ensure_ascii=False),
                        table.sheet_name,
                        table.cell_range,
                        table.source_locator,
                        table.extraction_method,
                        json.dumps(table.metadata, ensure_ascii=False),
                        now,
                    ),
                )
        return parsed_asset_id

    def list_parsed_text_blocks(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM parsed_text_blocks
                WHERE job_id = ?
                ORDER BY block_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            bbox_json = item.pop("bbox_json", None)
            item["bbox"] = (
                tuple(float(value) for value in json.loads(bbox_json))
                if bbox_json
                else None
            )
            result.append(item)
        return result

    def list_parsed_tables(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM parsed_tables
                WHERE job_id = ?
                ORDER BY table_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["columns"] = json.loads(item.pop("columns_json"))
            item["rows"] = json.loads(item.pop("rows_json"))
            item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
            result.append(item)
        return result

    @staticmethod
    def _tabular_dataset_hash(columns: list, rows: list) -> str:
        encoded = json.dumps(
            {"columns": columns, "rows": rows},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _dataset_profile_id(
        dataset_id: str, dataset_hash: str, profiler_version: str
    ) -> str:
        digest = hashlib.sha256(
            f"{dataset_id}:{dataset_hash}:{profiler_version}".encode("utf-8")
        ).hexdigest()[:24]
        return f"dataset-profile-{digest}"

    def save_tabular_dataset(self, dataset, profile=None) -> str:
        now = utc_now()
        dataset_hash = self._tabular_dataset_hash(dataset.columns, dataset.rows)
        profile_payload = _dataclass_payload(profile) if profile is not None else {}
        schema_payload = profile_payload.get("schema") or dataset.columns
        profile_json = json.dumps(profile_payload, ensure_ascii=False)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO tabular_datasets (
                    dataset_id, job_id, asset_id, table_id, name,
                    dataset_name, source_locator, normalized_path, dataset_hash,
                    columns_json, rows_json, schema_json, profile_json,
                    lineage_json, row_count, column_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET
                    name=excluded.name,
                    dataset_name=excluded.dataset_name,
                    source_locator=excluded.source_locator,
                    normalized_path=excluded.normalized_path,
                    columns_json=excluded.columns_json,
                    rows_json=excluded.rows_json,
                    schema_json=excluded.schema_json,
                    profile_json=CASE
                        WHEN tabular_datasets.dataset_hash = excluded.dataset_hash
                         AND excluded.profile_json = '{}'
                        THEN tabular_datasets.profile_json
                        ELSE excluded.profile_json
                    END,
                    dataset_hash=excluded.dataset_hash,
                    lineage_json=excluded.lineage_json,
                    row_count=excluded.row_count,
                    column_count=excluded.column_count
                """,
                (
                    dataset.dataset_id,
                    dataset.job_id,
                    dataset.asset_id,
                    dataset.table_id,
                    dataset.name,
                    dataset.name,
                    dataset.lineage.get("source_locator"),
                    dataset.lineage.get("normalized_path"),
                    dataset_hash,
                    json.dumps(dataset.columns, ensure_ascii=False),
                    json.dumps(dataset.rows, ensure_ascii=False),
                    json.dumps(schema_payload, ensure_ascii=False),
                    profile_json,
                    json.dumps(dataset.lineage, ensure_ascii=False),
                    len(dataset.rows),
                    len(dataset.columns),
                    now,
                ),
            )
            lineage_id = f"{dataset.dataset_id}:normalize_dataset:1.0"
            parameters = {
                "columns": dataset.columns,
                "row_count": len(dataset.rows),
                "source_locator": dataset.lineage.get("source_locator"),
                "normalized_path": dataset.lineage.get("normalized_path"),
                "extraction_method": dataset.lineage.get("extraction_method"),
            }
            connection.execute(
                """
                INSERT INTO dataset_lineage (
                    lineage_id, dataset_id, job_id, upstream_asset_id,
                    upstream_table_id, asset_id, source_locator,
                    transformation_name, transformation_version,
                    parameters_json, parent_dataset_ids_json,
                    transform_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lineage_id) DO UPDATE SET
                    job_id=excluded.job_id,
                    asset_id=excluded.asset_id,
                    source_locator=excluded.source_locator,
                    parameters_json=excluded.parameters_json,
                    parent_dataset_ids_json=excluded.parent_dataset_ids_json,
                    transform_json=excluded.transform_json
                """,
                (
                    lineage_id,
                    dataset.dataset_id,
                    dataset.job_id,
                    dataset.asset_id,
                    dataset.table_id,
                    dataset.asset_id,
                    dataset.lineage.get("source_locator"),
                    "normalize_dataset",
                    "1.0",
                    json.dumps(parameters, ensure_ascii=False),
                    json.dumps(dataset.lineage.get("parent_dataset_ids") or [], ensure_ascii=False),
                    json.dumps(
                        {
                            "name": "normalize_dataset",
                            "version": "1.0",
                            "parameters": parameters,
                        },
                        ensure_ascii=False,
                    ),
                    now,
                ),
            )
        if profile is not None:
            self.save_dataset_profile(
                dataset,
                profile,
                profiler_version=getattr(profile, "profiler_version", "1.0"),
            )
        return dataset.dataset_id

    def save_dataset_profile(
        self,
        dataset,
        profile,
        *,
        profiler_version: str = "1.0",
    ) -> str:
        dataset_hash = self._tabular_dataset_hash(dataset.columns, dataset.rows)
        profile_id = self._dataset_profile_id(
            dataset.dataset_id, dataset_hash, profiler_version
        )
        profile_json = json.dumps(
            _dataclass_payload(profile) or {}, ensure_ascii=False
        )
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO dataset_profiles (
                    profile_id, dataset_id, job_id, profile_json,
                    dataset_hash, profiler_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id) DO UPDATE SET
                    profile_json=excluded.profile_json
                """,
                (
                    profile_id,
                    dataset.dataset_id,
                    dataset.job_id,
                    profile_json,
                    dataset_hash,
                    profiler_version,
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE tabular_datasets
                SET profile_json = ?, schema_json = ?
                WHERE dataset_id = ? AND dataset_hash = ?
                """,
                (
                    profile_json,
                    json.dumps(
                        (_dataclass_payload(profile) or {}).get("schema")
                        or dataset.columns,
                        ensure_ascii=False,
                    ),
                    dataset.dataset_id,
                    dataset_hash,
                ),
            )
        return profile_id

    def get_cached_dataset_profile(
        self,
        dataset_id: str,
        dataset_hash: str,
        profiler_version: str,
    ):
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT profile_json FROM dataset_profiles
                WHERE dataset_id = ? AND dataset_hash = ? AND profiler_version = ?
                """,
                (dataset_id, dataset_hash, profiler_version),
            ).fetchone()
        if not row:
            return None
        from .datasets.models import DatasetProfile

        return DatasetProfile(**json.loads(row["profile_json"] or "{}"))

    def list_dataset_profiles(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM dataset_profiles
                WHERE job_id = ?
                ORDER BY created_at, profile_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["profile"] = json.loads(item.pop("profile_json") or "{}")
            result.append(item)
        return result

    def list_dataset_lineage(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM dataset_lineage
                WHERE job_id = ?
                ORDER BY dataset_id, lineage_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["parameters"] = json.loads(item.get("parameters_json") or "{}")
            item["parent_dataset_ids"] = json.loads(
                item.get("parent_dataset_ids_json") or "[]"
            )
            item["transform"] = json.loads(item.get("transform_json") or "{}")
            result.append(item)
        return result

    def list_tabular_datasets(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM tabular_datasets
                WHERE job_id = ?
                ORDER BY dataset_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["columns"] = json.loads(item.pop("columns_json"))
            item["rows"] = json.loads(item.pop("rows_json"))
            item["schema"] = json.loads(item.pop("schema_json") or "[]")
            item["profile"] = json.loads(item.pop("profile_json") or "{}")
            item["lineage"] = json.loads(item.pop("lineage_json") or "{}")
            result.append(item)
        return result

    def save_analysis_run(self, run, results) -> str:
        self.start_analysis_run(run)
        for result in results:
            self.save_analysis_result(run.job_id, result)
        self.complete_analysis_run(run.run_id)
        return run.run_id

    def start_analysis_run(self, run) -> str:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO analysis_runs (
                    analysis_run_id, job_id, selected_tools_json,
                    selected_skills_json, reason, created_at,
                    status, started_at, completed_at, analysis_plan_json,
                    idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(analysis_run_id) DO UPDATE SET
                    status='running',
                    completed_at=NULL
                WHERE analysis_runs.status != 'completed'
                """,
                (
                    run.run_id,
                    run.job_id,
                    json.dumps(run.selected_tools, ensure_ascii=False),
                    json.dumps(run.selected_skills, ensure_ascii=False),
                    run.reason,
                    now,
                    "running",
                    now,
                    None,
                    json.dumps(
                        _dataclass_payload(getattr(run, "analysis_plan", None)) or {},
                        ensure_ascii=False,
                    ),
                    getattr(run, "idempotency_key", None),
                ),
            )
        return run.run_id

    def save_analysis_result(self, job_id: str, result) -> str:
        now = utc_now()
        result_hash = self._analysis_result_hash(job_id, result)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO analysis_results (
                    analysis_result_id, analysis_run_id, job_id,
                    tool_name, result_json, created_at,
                    skill_name, skill_version, tool_version,
                    input_dataset_ids_json, input_evidence_ids_json,
                    parameters_json, limitations_json, result_hash,
                    idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(analysis_result_id) DO UPDATE SET
                    tool_name=excluded.tool_name,
                    result_json=excluded.result_json,
                    skill_name=excluded.skill_name,
                    skill_version=excluded.skill_version,
                    tool_version=excluded.tool_version,
                    input_dataset_ids_json=excluded.input_dataset_ids_json,
                    input_evidence_ids_json=excluded.input_evidence_ids_json,
                    parameters_json=excluded.parameters_json,
                    limitations_json=excluded.limitations_json,
                    result_hash=excluded.result_hash,
                    idempotency_key=excluded.idempotency_key
                """,
                (
                    result.result_id,
                    result.run_id,
                    job_id,
                    result.tool_name,
                    json.dumps(result.__dict__, ensure_ascii=False),
                    now,
                    result.skill_name,
                    result.skill_version,
                    result.tool_version,
                    json.dumps(result.input_dataset_ids, ensure_ascii=False),
                    json.dumps(result.input_evidence_ids, ensure_ascii=False),
                    json.dumps(result.parameters, ensure_ascii=False),
                    json.dumps(result.limitations, ensure_ascii=False),
                    result_hash,
                    getattr(result, "idempotency_key", None),
                ),
            )
        return result.result_id

    def complete_analysis_run(self, run_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE analysis_runs
                SET status = 'completed', completed_at = ?
                WHERE analysis_run_id = ?
                """,
                (utc_now(), run_id),
            )

    def fail_analysis_run(self, run_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE analysis_runs
                SET status = 'failed', completed_at = ?
                WHERE analysis_run_id = ? AND status != 'completed'
                """,
                (utc_now(), run_id),
            )

    def get_analysis_result_by_idempotency_key(
        self, job_id: str, idempotency_key: str
    ):
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM analysis_results
                WHERE job_id = ? AND idempotency_key = ?
                """,
                (job_id, idempotency_key),
            ).fetchone()
        if not row:
            return None
        from .analysis.schemas import AnalysisResult

        payload = json.loads(row["result_json"] or "{}")
        payload["idempotency_key"] = row["idempotency_key"]
        return AnalysisResult(**payload)

    @staticmethod
    def _analysis_result_hash(job_id: str, result) -> str:
        payload = {
            "job_id": job_id,
            "tool_name": result.tool_name,
            "tool_version": result.tool_version,
            "skill_name": result.skill_name,
            "skill_version": result.skill_version,
            "input_dataset_ids": result.input_dataset_ids,
            "input_evidence_ids": result.input_evidence_ids,
            "parameters": result.parameters,
            "summary": result.summary,
            "tables": result.tables,
            "charts": result.charts,
            "metrics": result.metrics,
            "limitations": result.limitations,
            "confidence_band": result.confidence_band,
            "idempotency_key": getattr(result, "idempotency_key", None),
        }
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def list_analysis_runs(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM analysis_runs
                WHERE job_id = ?
                ORDER BY created_at, analysis_run_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["selected_tools"] = json.loads(
                item.pop("selected_tools_json") or "[]"
            )
            item["selected_skills"] = json.loads(
                item.pop("selected_skills_json") or "[]"
            )
            item["analysis_plan"] = json.loads(
                item.pop("analysis_plan_json", None) or "{}"
            )
            result.append(item)
        return result

    def list_analysis_results(self, job_id: str) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM analysis_results
                WHERE job_id = ?
                ORDER BY created_at, analysis_result_id
                """,
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            payload = json.loads(item.pop("result_json") or "{}")
            item["input_dataset_ids"] = json.loads(
                item.pop("input_dataset_ids_json", None) or "[]"
            )
            item["input_evidence_ids"] = json.loads(
                item.pop("input_evidence_ids_json", None) or "[]"
            )
            item["parameters"] = json.loads(
                item.pop("parameters_json", None) or "{}"
            )
            item["limitations"] = json.loads(
                item.pop("limitations_json", None) or "[]"
            )
            item.update(payload)
            result.append(item)
        return result

    def list_latest_analysis_results(self, job_id: str) -> list[dict]:
        """Return the newest result for each deterministic tool or skill."""
        latest: dict[str, dict] = {}
        for item in reversed(self.list_analysis_results(job_id)):
            identity = item.get("skill_name") or item.get("tool_name")
            if identity and identity not in latest:
                latest[identity] = item
        return list(reversed(list(latest.values())))

    def list_monitoring_run_dataset_ids(
        self, job_id: str, run_id: str
    ) -> list[str]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT dataset.dataset_id
                FROM tabular_datasets AS dataset
                JOIN source_assets AS asset
                  ON asset.asset_id = dataset.asset_id
                JOIN monitoring_source_snapshots AS snapshot
                  ON snapshot.snapshot_id = asset.snapshot_id
                WHERE dataset.job_id = ?
                  AND COALESCE(snapshot.monitor_run_id, snapshot.run_id) = ?
                ORDER BY dataset.dataset_id
                """,
                (job_id, run_id),
            ).fetchall()
        return [str(row["dataset_id"]) for row in rows]

    def bind_temporal_workflow(
        self, job_id: str, workflow_id: str, run_id: str | None = None
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET execution_backend = 'temporal',
                    temporal_workflow_id = ?,
                    temporal_run_id = COALESCE(?, temporal_run_id),
                    workflow_status = COALESCE(workflow_status, 'started'),
                    updated_at = ?
                WHERE job_id = ?
                """,
                (workflow_id, run_id, utc_now(), job_id),
            )

    def update_workflow_projection(
        self,
        job_id: str,
        *,
        workflow_status: str | None = None,
        run_id: str | None = None,
        paused: bool | None = None,
        stage: str | None = None,
        progress: int | None = None,
        heartbeat: bool = False,
        notification_status: str | None = None,
        monitor_registration_status: str | None = None,
        monitor_registration_error: str | None = None,
    ) -> None:
        assignments = ["updated_at = ?"]
        params: list[object] = [utc_now()]
        if workflow_status is not None:
            assignments.append("workflow_status = ?")
            params.append(workflow_status)
        if run_id is not None:
            assignments.append("temporal_run_id = ?")
            params.append(run_id)
        if paused is not None:
            assignments.append("paused = ?")
            params.append(int(paused))
        if stage is not None:
            assignments.append("stage = ?")
            params.append(stage)
        if progress is not None:
            assignments.append("progress = ?")
            params.append(progress)
        if heartbeat:
            assignments.append("last_heartbeat_at = ?")
            params.append(utc_now())
        if notification_status is not None:
            assignments.append("notification_status = ?")
            params.append(notification_status)
        if monitor_registration_status is not None:
            assignments.append("monitor_registration_status = ?")
            params.append(monitor_registration_status)
        if monitor_registration_error is not None:
            assignments.append("monitor_registration_error = ?")
            params.append(monitor_registration_error[:1000])
        params.append(job_id)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"UPDATE jobs SET {', '.join(assignments)} WHERE job_id = ?",
                tuple(params),
            )

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._to_job(row) if row else None

    def list_recoverable_jobs(self) -> list[Job]:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET status = 'queued', stage = '服务重启后重新排队',
                    progress = 0, updated_at = ?
                WHERE status = 'running'
                  AND execution_backend = 'local'
                """,
                (utc_now(),),
            )
            rows = connection.execute(
                """
                SELECT * FROM jobs
                WHERE status = 'queued'
                  AND cancel_requested = 0
                  AND execution_backend = 'local'
                ORDER BY created_at
                """
            ).fetchall()
        return [self._to_job(row) for row in rows]

    def list_active_temporal_jobs(self) -> list[Job]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM jobs
                WHERE execution_backend = 'temporal'
                  AND temporal_workflow_id IS NOT NULL
                  AND status IN ('queued', 'running', 'cancel_requested')
                ORDER BY created_at
                """
            ).fetchall()
        return [self._to_job(row) for row in rows]

    def start_job(self, job_id: str) -> bool:
        with self._lock, self._connect() as connection:
            result = connection.execute(
                """
                UPDATE jobs SET status = 'running', stage = '开始处理',
                    updated_at = ?
                WHERE job_id = ? AND status = 'queued' AND cancel_requested = 0
                """,
                (utc_now(), job_id),
            )
        return result.rowcount == 1

    def update_progress(self, job_id: str, stage: str, progress: int) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET stage = ?, progress = ?, updated_at = ?
                WHERE job_id = ? AND status IN ('running', 'cancel_requested')
                """,
                (stage, progress, utc_now(), job_id),
            )

    def complete_job(self, job_id: str, summary: str) -> None:
        self._set_terminal(job_id, "completed", "已完成", 100, summary, None)

    def fail_job(self, job_id: str, error: str) -> None:
        self._set_terminal(job_id, "failed", "执行失败", 100, None, error[:1000])

    def cancel_job(self, job_id: str, requester_id: str) -> str:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT creator_id, status FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not row:
                return "not_found"
            if row["creator_id"] != requester_id:
                return "forbidden"
            status = row["status"]
            if status == "queued":
                connection.execute(
                    """
                    UPDATE jobs SET status = 'cancelled', stage = '已取消',
                        cancel_requested = 1, updated_at = ? WHERE job_id = ?
                    """,
                    (utc_now(), job_id),
                )
                return "cancelled"
            if status == "running":
                connection.execute(
                    """
                    UPDATE jobs SET status = 'cancel_requested',
                        stage = '等待安全取消', cancel_requested = 1,
                        updated_at = ? WHERE job_id = ?
                    """,
                    (utc_now(), job_id),
                )
                return "cancel_requested"
            if status == "cancel_requested":
                return "cancel_requested"
            return "terminal"

    def pause_job(self, job_id: str, requester_id: str) -> str:
        return self._set_pause(job_id, requester_id, True)

    def resume_job(self, job_id: str, requester_id: str) -> str:
        return self._set_pause(job_id, requester_id, False)

    def _set_pause(self, job_id: str, requester_id: str, paused: bool) -> str:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT creator_id, status, paused FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if not row:
                return "not_found"
            if row["creator_id"] != requester_id:
                return "forbidden"
            if row["status"] in {"completed", "failed", "cancelled"}:
                return "terminal"
            current = bool(row["paused"])
            if current == paused:
                return "paused" if paused else "resumed"
            connection.execute(
                """
                UPDATE jobs SET paused = ?, stage = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    int(paused),
                    "已暂停" if paused else "已恢复，等待继续执行",
                    utc_now(),
                    job_id,
                ),
            )
        return "paused" if paused else "resumed"

    def is_cancellation_requested(self, job_id: str) -> bool:
        job = self.get_job(job_id)
        return bool(job and job.cancel_requested)

    def mark_cancelled(self, job_id: str) -> None:
        self._set_terminal(job_id, "cancelled", "已取消", 100, None, None, True)

    def _set_terminal(
        self,
        job_id: str,
        status: str,
        stage: str,
        progress: int,
        result: Optional[str],
        error: Optional[str],
        cancelled: bool = False,
    ) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status: {status}")
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET status = ?, stage = ?, progress = ?,
                    result_summary = ?, error_message = ?, cancel_requested = ?,
                    updated_at = ?
                WHERE job_id = ?
                  AND (
                    status NOT IN ('completed', 'failed', 'cancelled')
                    OR status = ?
                  )
                """,
                (
                    status,
                    stage,
                    progress,
                    result,
                    error,
                    int(cancelled),
                    utc_now(),
                    job_id,
                    status,
                ),
            )

    def close(self) -> None:
        self._closed = True

    @staticmethod
    def _to_job(row: sqlite3.Row) -> Job:
        values = dict(row)
        values["cancel_requested"] = bool(values["cancel_requested"])
        values["paused"] = bool(values.get("paused", 0))
        return Job(**values)

    @staticmethod
    def _to_monitoring_config(row: sqlite3.Row) -> MonitoringConfig:
        return MonitoringConfig(**dict(row))
