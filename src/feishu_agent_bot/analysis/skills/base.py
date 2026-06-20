from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ...datasets.models import DataQualityReport, DatasetProfile, TabularDataset
from ..schemas import AnalysisResult


@dataclass(frozen=True)
class SkillApplicability:
    applicable: bool
    reason: str
    missing_inputs: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AnalysisContext:
    job_id: str
    run_id: str
    topic: str
    datasets: list[TabularDataset]
    profiles: list[DatasetProfile]
    quality_reports: list[DataQualityReport] = field(default_factory=list)
    evidence: list[object] = field(default_factory=list)
    claims: list[object] = field(default_factory=list)


class AnalysisSkill(Protocol):
    name: str
    version: str
    required_inputs: list[str]
    optional_inputs: list[str]
    required_tools: list[str]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        ...

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        ...


def result_id(run_id: str, skill_name: str) -> str:
    return f"{run_id}:{skill_name}"


def dataset_ids(context: AnalysisContext) -> list[str]:
    return [dataset.dataset_id for dataset in context.datasets]

