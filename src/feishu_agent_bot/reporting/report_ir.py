from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class ReportSection:
    section_id: str
    title: str
    body: str
    section_type: str = "general"
    claim_ids: list[str] = field(default_factory=list)
    analysis_result_ids: list[str] = field(default_factory=list)
    table_ids: list[str] = field(default_factory=list)
    chart_ids: list[str] = field(default_factory=list)
    evidence_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReportIR:
    job_id: str
    title: str
    sections: list[ReportSection]
    report_id: str = ""
    version: int = 1
    parent_version_id: str | None = None
    subtitle: str | None = None
    purpose: str = "business_research"
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    methodology: dict[str, object] = field(default_factory=dict)
    executive_summary: list[dict[str, object]] = field(default_factory=list)
    tables: list[dict[str, object]] = field(default_factory=list)
    charts: list[dict[str, object]] = field(default_factory=list)
    analysis_results: list[dict[str, object]] = field(default_factory=list)
    change_events: list[dict[str, object]] = field(default_factory=list)
    claims: list[dict[str, object]] = field(default_factory=list)
    evidence_references: list[dict[str, object]] = field(default_factory=list)
    data_quality: list[dict[str, object]] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    sources: list[dict[str, object]] = field(default_factory=list)
    appendices: list[ReportSection] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
