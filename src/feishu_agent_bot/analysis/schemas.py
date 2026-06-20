from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, Field


class SkippedSkill(BaseModel):
    skill_name: str
    reason: str
    missing_inputs: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


class AnalysisPlan(BaseModel):
    selected_tools: list[str] = Field(default_factory=list)
    selected_skills: list[str] = Field(default_factory=list)
    skipped_skills: list[SkippedSkill] = Field(default_factory=list)
    required_dataset_ids: list[str] = Field(default_factory=list)
    required_evidence_ids: list[str] = Field(default_factory=list)
    expected_outputs: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class AnalysisRun:
    run_id: str
    job_id: str
    selected_tools: list[str]
    selected_skills: list[str]
    reason: str
    analysis_plan: AnalysisPlan | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True)
class AnalysisResult:
    result_id: str
    run_id: str
    tool_name: str
    summary: str
    tables: list[dict[str, object]] = field(default_factory=list)
    charts: list[dict[str, object]] = field(default_factory=list)
    metrics: dict[str, object] = field(default_factory=dict)
    skill_name: str | None = None
    skill_version: str | None = None
    tool_version: str | None = None
    input_dataset_ids: list[str] = field(default_factory=list)
    input_evidence_ids: list[str] = field(default_factory=list)
    parameters: dict[str, object] = field(default_factory=dict)
    limitations: list[str] = field(default_factory=list)
    confidence_band: str = "medium"
    idempotency_key: str | None = None
