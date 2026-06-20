from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from ..llm.schemas import StoredEvidence


EventType = Literal[
    "product_launch",
    "price_change",
    "feature_added",
    "feature_removed",
    "product_discontinued",
    "target_customer_shift",
    "market_position_change",
    "business_model_change",
    "partnership",
    "funding",
    "policy_change",
    "company_statement",
    "sentiment_shift",
    "review_topic_emergence",
    "media_volume_spike",
    "new_source",
    "new_evidence",
    "page_changed",
    "other",
]


@dataclass(frozen=True)
class ChangeEventCandidate:
    entity: str
    event_type: EventType
    summary: str
    old_value: dict | None = None
    new_value: dict | None = None
    effective_at: datetime | None = None
    supporting_evidence_ids: list[str] = field(default_factory=list)
    contradicting_evidence_ids: list[str] = field(default_factory=list)
    novelty_level: Literal["low", "medium", "high"] = "medium"
    materiality_level: Literal["low", "medium", "high", "critical"] = "medium"
    confidence_band: Literal["low", "medium", "high", "conflicting"] = "medium"
    fingerprint_components: dict = field(default_factory=dict)


class ChangeDetector:
    EVENT_BY_EVIDENCE_TYPE: dict[str, EventType] = {
        "price": "price_change",
        "product_feature": "feature_added",
        "market_position": "market_position_change",
        "user_opinion": "sentiment_shift",
        "company_statement": "company_statement",
        "metric": "new_evidence",
        "fact": "new_evidence",
    }

    def detect_evidence(
        self,
        evidence: StoredEvidence,
        prior_evidence: list[StoredEvidence],
    ) -> ChangeEventCandidate:
        prior = next(
            (
                item
                for item in reversed(prior_evidence)
                if item.entity == evidence.entity
                and item.attribute == evidence.attribute
                and item.evidence_id != evidence.evidence_id
            ),
            None,
        )
        event_type = self._event_type_for_evidence(evidence)
        old_value = (
            {"attribute": prior.attribute, "value": prior.value}
            if prior and prior.value != evidence.value
            else None
        )
        confidence = evidence.confidence_band
        contradicting_ids: list[str] = []
        if old_value and not self._is_newer_observation(evidence, prior):
            confidence = "conflicting"
            contradicting_ids = [prior.evidence_id]
        materiality = "low" if evidence.evidence_type in {
            "user_opinion",
            "company_statement",
        } else "medium"
        summary = (
            f"{evidence.entity} {evidence.attribute}: {evidence.value}"
        )
        return ChangeEventCandidate(
            entity=evidence.entity,
            event_type=event_type,
            summary=summary,
            old_value=old_value,
            new_value={
                "attribute": evidence.attribute,
                "value": evidence.value,
            },
            supporting_evidence_ids=[evidence.evidence_id],
            contradicting_evidence_ids=contradicting_ids,
            novelty_level="medium",
            materiality_level=materiality,
            confidence_band=confidence,
            fingerprint_components={
                "attribute": evidence.attribute.strip().lower(),
                "value": evidence.value.strip().lower(),
            },
        )

    def _event_type_for_evidence(self, evidence: StoredEvidence) -> EventType:
        text = f"{evidence.attribute} {evidence.value}".lower()
        if any(
            token in text
            for token in (
                "目标客户",
                "客户群",
                "客群",
                "企业客户",
                "consumer to enterprise",
            )
        ):
            return "target_customer_shift"
        if any(
            token in text
            for token in ("商业模式", "订阅制", "收费模式", "business model")
        ):
            return "business_model_change"
        if any(token in text for token in ("合作", "战略伙伴", "partnership")):
            return "partnership"
        if any(token in text for token in ("融资", "募资", "funding", "融资轮")):
            return "funding"
        if any(
            token in text
            for token in ("政策", "监管", "法规", "补贴", "regulation", "policy")
        ):
            return "policy_change"
        if any(token in text for token in ("停产", "下架", "discontinued")):
            return "product_discontinued"
        if any(token in text for token in ("发布", "推出", "launch", "released")):
            return "product_launch"
        if evidence.evidence_type == "product_feature" and any(
            token in text for token in ("移除", "取消", "不再支持", "removed")
        ):
            return "feature_removed"
        return self.EVENT_BY_EVIDENCE_TYPE[evidence.evidence_type]

    @staticmethod
    def _is_newer_observation(
        evidence: StoredEvidence, prior: StoredEvidence
    ) -> bool:
        return bool(
            evidence.observed_at
            and prior.observed_at
            and evidence.observed_at > prior.observed_at
        )

    def validate_candidate(self, candidate: ChangeEventCandidate) -> None:
        evidence_ids = (
            candidate.supporting_evidence_ids
            + candidate.contradicting_evidence_ids
        )
        if not evidence_ids:
            raise ValueError("change event must reference at least one evidence")
        if candidate.event_type == "price_change" and not candidate.new_value:
            raise ValueError("price_change requires a new value")

    def fingerprint(self, candidate: ChangeEventCandidate) -> str:
        self.validate_candidate(candidate)
        components = {
            "entity": candidate.entity.strip().lower(),
            "event_type": candidate.event_type,
            "summary": " ".join(candidate.summary.split()).lower(),
            "new_value": candidate.new_value,
            **candidate.fingerprint_components,
        }
        payload = json.dumps(components, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
