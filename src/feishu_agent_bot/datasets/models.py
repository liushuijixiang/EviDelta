from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class TabularDataset:
    dataset_id: str
    job_id: str
    asset_id: str
    table_id: str
    name: str
    columns: list[str]
    rows: list[dict[str, object]]
    lineage: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetProfile:
    dataset_id: str
    row_count: int
    column_count: int
    missing_counts: dict[str, int]
    duplicate_row_count: int
    numeric_columns: list[str]
    quality_warnings: list[str]
    schema: list[DatasetColumn] = field(default_factory=list)
    missing_rates: dict[str, float] = field(default_factory=dict)
    duplicate_row_rate: float = 0.0
    unique_counts: dict[str, int] = field(default_factory=dict)
    min_values: dict[str, float] = field(default_factory=dict)
    max_values: dict[str, float] = field(default_factory=dict)
    mean_values: dict[str, float] = field(default_factory=dict)
    median_values: dict[str, float] = field(default_factory=dict)
    quantiles: dict[str, dict[str, float]] = field(default_factory=dict)
    time_ranges: dict[str, dict[str, str]] = field(default_factory=dict)
    negative_ratios: dict[str, float] = field(default_factory=dict)
    outlier_candidates: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    currencies: dict[str, str] = field(default_factory=dict)
    units: dict[str, str] = field(default_factory=dict)
    likely_primary_keys: list[str] = field(default_factory=list)
    likely_category_columns: list[str] = field(default_factory=list)
    likely_time_columns: list[str] = field(default_factory=list)
    parsing_warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetColumn:
    name: str
    normalized_name: str
    inferred_type: str
    nullable: bool
    unit: str | None
    currency: str | None
    source_column: str


@dataclass(frozen=True)
class DataQualityIssue:
    issue_type: str
    severity: Literal["low", "medium", "high"]
    message: str
    column: str | None = None


@dataclass(frozen=True)
class DataQualityReport:
    dataset_id: str
    quality_level: Literal["high", "medium", "low", "unusable"]
    issues: list[DataQualityIssue]
    usable_for: list[str]
    not_usable_for: list[str]
