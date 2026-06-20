from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


ImpactType = Literal["supports", "weakens", "contradicts", "supersedes", "context"]
ImpactLevel = Literal["low", "medium", "high", "critical"]
ConfidenceBand = Literal["low", "medium", "high", "conflicting"]
UpdateAction = Literal[
    "no_change", "evidence_only", "auto_patch", "review_required"
]


@dataclass(frozen=True)
class ClaimImpactCandidate:
    claim_id: str | None
    change_event_id: str
    impact_type: ImpactType
    impact_level: ImpactLevel
    rationale: str
    old_confidence_band: str
    proposed_confidence_band: str
    affected_section_ids: list[str]
    requires_review: bool

    def to_repository_rows(self) -> list[dict]:
        return [
            {
                "event_id": self.change_event_id,
                "claim_id": self.claim_id,
                "section_id": section_id,
                "impact_type": self.impact_type,
                "severity": self.impact_level,
                "impact_level": self.impact_level,
                "old_confidence_band": self.old_confidence_band,
                "proposed_confidence_band": self.proposed_confidence_band,
                "affected_section_ids": self.affected_section_ids,
                "requires_review": self.requires_review,
                "rationale": self.rationale,
            }
            for section_id in self.affected_section_ids
        ]


@dataclass(frozen=True)
class UpdateDecision:
    action: UpdateAction
    reason: str
    affected_claim_ids: list[str] = field(default_factory=list)
    affected_section_ids: list[str] = field(default_factory=list)
    change_event_ids: list[str] = field(default_factory=list)
