from __future__ import annotations

from collections import defaultdict

from ..llm.schemas import StoredClaim, StoredEvidence
from .models import ClaimImpactCandidate, ConfidenceBand, ImpactLevel, ImpactType


CORE_CLAIM_TYPES = {"market_position", "business_model", "opportunity", "risk"}
CONTRADICT_EVENT_TYPES = {
    "product_discontinued",
    "feature_removed",
    "market_position_change",
}
SUPERSEDE_EVENT_TYPES = {"target_customer_shift", "business_model_change"}
SUPPORT_EVENT_TYPES = {"new_evidence", "feature_added", "product_launch"}


class ImpactAnalyzer:
    def analyze(
        self,
        events: list[dict],
        claims: list[StoredClaim],
        evidence: list[StoredEvidence],
    ) -> list[ClaimImpactCandidate]:
        evidence_by_id = {item.evidence_id: item for item in evidence}
        claim_ids_by_source: dict[str, set[str]] = defaultdict(set)
        claim_ids_by_fact: dict[tuple[str, str], set[str]] = defaultdict(set)
        claims_by_id = {claim.claim_id: claim for claim in claims}
        for claim in claims:
            referenced = (
                claim.supporting_evidence_ids + claim.contradicting_evidence_ids
            )
            for evidence_id in referenced:
                item = evidence_by_id.get(evidence_id)
                if item:
                    claim_ids_by_source[item.source_id].add(claim.claim_id)
                    claim_ids_by_fact[
                        (
                            item.entity.strip().lower(),
                            item.attribute.strip().lower(),
                        )
                    ].add(claim.claim_id)

        impacts: list[ClaimImpactCandidate] = []
        seen: set[tuple[str, str | None, str]] = set()
        for event in events:
            source_id = event.get("source_id")
            event_evidence_ids = event.get("evidence_ids", [])
            evidence_summary = ", ".join(event_evidence_ids) or "无"
            impacted_claim_ids = set(claim_ids_by_source.get(source_id, set()))
            for evidence_id in event_evidence_ids:
                item = evidence_by_id.get(evidence_id)
                if not item:
                    continue
                impacted_claim_ids.update(
                    claim_ids_by_fact.get(
                        (
                            item.entity.strip().lower(),
                            item.attribute.strip().lower(),
                        ),
                        set(),
                    )
                )
            impacted_claim_ids = sorted(impacted_claim_ids)
            if not impacted_claim_ids:
                section_id = self.fallback_section_for_event(event)
                key = (event["event_id"], None, section_id)
                if key in seen:
                    continue
                seen.add(key)
                impacts.append(
                    ClaimImpactCandidate(
                        claim_id=None,
                        change_event_id=event["event_id"],
                        impact_type=self.impact_type_for_event(event),
                        impact_level=self.impact_level_for_event(event),
                        rationale=(
                            f"Evidence {evidence_summary} 支持的 "
                            f"{event['event_type']} 尚未命中既有 claim，"
                            f"需要复核 {section_id} 章节。"
                        ),
                        old_confidence_band="medium",
                        proposed_confidence_band=self.confidence_for_event(event),
                        affected_section_ids=[section_id],
                        requires_review=self.requires_review(event, None),
                    )
                )
                continue
            for claim_id in impacted_claim_ids:
                claim = claims_by_id[claim_id]
                section_id = claim.claim_type
                key = (event["event_id"], claim_id, section_id)
                if key in seen:
                    continue
                seen.add(key)
                impacts.append(
                    ClaimImpactCandidate(
                        claim_id=claim_id,
                        change_event_id=event["event_id"],
                        impact_type=self.impact_type_for_event(event),
                        impact_level=self.impact_level_for_event(event, claim),
                        rationale=(
                            f"{event['event_type']} 由 Evidence "
                            f"{evidence_summary} 支持，来自来源 {source_id}，"
                            f"该来源支撑或反证 claim {claim_id}。"
                        ),
                        old_confidence_band=claim.confidence_band,
                        proposed_confidence_band=self.confidence_for_event(event),
                        affected_section_ids=[section_id],
                        requires_review=self.requires_review(event, claim),
                    )
                )
        return impacts

    @staticmethod
    def impact_type_for_event(event: dict) -> ImpactType:
        event_type = event["event_type"]
        confidence = event.get("confidence_band")
        if confidence == "conflicting":
            return "weakens"
        if event_type in SUPERSEDE_EVENT_TYPES:
            return "supersedes"
        if event_type in CONTRADICT_EVENT_TYPES:
            return "contradicts"
        if event_type in SUPPORT_EVENT_TYPES:
            return "supports"
        if event_type == "page_changed":
            return "context"
        return "context"

    @staticmethod
    def impact_level_for_event(
        event: dict, claim: StoredClaim | None = None
    ) -> ImpactLevel:
        level = event.get("materiality_level") or event.get("severity") or "medium"
        if level not in {"low", "medium", "high", "critical"}:
            level = "medium"
        if claim and claim.claim_type in CORE_CLAIM_TYPES:
            if ImpactAnalyzer.impact_type_for_event(event) in {
                "contradicts",
                "supersedes",
            }:
                return "high" if level in {"low", "medium"} else level
        return level

    @staticmethod
    def confidence_for_event(event: dict) -> ConfidenceBand:
        confidence = event.get("confidence_band") or "medium"
        if confidence in {"low", "medium", "high", "conflicting"}:
            return confidence
        return "medium"

    @staticmethod
    def requires_review(event: dict, claim: StoredClaim | None) -> bool:
        confidence = ImpactAnalyzer.confidence_for_event(event)
        level = ImpactAnalyzer.impact_level_for_event(event, claim)
        impact_type = ImpactAnalyzer.impact_type_for_event(event)
        if confidence == "conflicting":
            return True
        if level in {"high", "critical"}:
            return True
        if claim and claim.claim_type in CORE_CLAIM_TYPES and impact_type in {
            "contradicts",
            "supersedes",
        }:
            return True
        return False

    @staticmethod
    def fallback_section_for_event(event: dict) -> str:
        if event["event_type"] == "new_source":
            return "competitor_profile"
        if event["event_type"] == "new_evidence":
            return "product_comparison"
        return "uncertainty"
