from datetime import datetime, timezone
from dataclasses import replace

import pytest

from feishu_agent_bot.llm.schemas import StoredClaim, StoredEvidence
from feishu_agent_bot.monitoring.change_detector import (
    ChangeDetector,
    ChangeEventCandidate,
)
from feishu_agent_bot.monitoring.impact_analyzer import ImpactAnalyzer
from feishu_agent_bot.monitoring.update_decider import UpdateDecider


def evidence(source_id="S-001"):
    return StoredEvidence(
        evidence_id="E-001",
        source_id=source_id,
        entity="示例公司",
        attribute="产品能力",
        value="支持高功率快充",
        exact_quote="该产品支持高功率快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )


def claim(claim_type="product_comparison"):
    return StoredClaim(
        claim_id="C-001",
        statement="示例公司的产品支持高功率快充。",
        claim_type=claim_type,
        supporting_evidence_ids=["E-001"],
        contradicting_evidence_ids=[],
        confidence_band="high",
        reasoning_summary="证据明确。",
    )


def test_change_detector_requires_evidence_and_stable_fingerprint():
    detector = ChangeDetector()
    candidate = ChangeEventCandidate(
        entity="示例公司",
        event_type="feature_added",
        summary=" 新增 高功率快充 ",
        new_value={"feature": "高功率快充"},
        effective_at=datetime(2026, 6, 18, tzinfo=timezone.utc),
        supporting_evidence_ids=["E-002", "E-001"],
    )
    same = ChangeEventCandidate(
        entity="示例公司",
        event_type="feature_added",
        summary="新增 高功率快充",
        new_value={"feature": "高功率快充"},
        supporting_evidence_ids=["E-001", "E-002"],
    )

    assert detector.fingerprint(candidate) == detector.fingerprint(same)

    with pytest.raises(ValueError, match="at least one evidence"):
        detector.fingerprint(
            ChangeEventCandidate(
                entity="示例公司",
                event_type="feature_added",
                summary="缺少证据",
            )
        )
    with pytest.raises(ValueError, match="price_change"):
        detector.fingerprint(
            ChangeEventCandidate(
                entity="示例公司",
                event_type="price_change",
                summary="价格变化",
                supporting_evidence_ids=["E-001"],
            )
        )


def test_change_detector_maps_evidence_and_marks_unordered_conflicts():
    detector = ChangeDetector()
    prior = evidence()
    changed = StoredEvidence(
        evidence_id="E-002",
        source_id="S-002",
        entity="示例公司",
        attribute="产品能力",
        value="不再支持高功率快充",
        exact_quote="该产品不再支持高功率快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )

    candidate = detector.detect_evidence(changed, [prior])

    assert candidate.event_type == "feature_removed"
    assert candidate.supporting_evidence_ids == ["E-002"]
    assert candidate.contradicting_evidence_ids == ["E-001"]
    assert candidate.confidence_band == "conflicting"
    assert candidate.old_value == {
        "attribute": "产品能力",
        "value": "支持高功率快充",
    }


@pytest.mark.parametrize(
    ("evidence_type", "attribute", "value", "event_type"),
    [
        ("price", "价格", "人民币 99 元", "price_change"),
        ("fact", "新品", "正式发布新产品", "product_launch"),
        ("user_opinion", "评价", "用户认为操作复杂", "sentiment_shift"),
        ("fact", "目标客户", "从学生转向企业客户", "target_customer_shift"),
        ("fact", "商业模式", "改为订阅制收费", "business_model_change"),
        ("fact", "合作", "与示例集团建立战略合作", "partnership"),
        ("fact", "融资", "完成新一轮融资", "funding"),
        ("fact", "政策", "新监管政策开始实施", "policy_change"),
    ],
)
def test_change_detector_maps_business_event_types(
    evidence_type, attribute, value, event_type
):
    item = StoredEvidence(
        evidence_id="E-010",
        source_id="S-010",
        entity="示例公司",
        attribute=attribute,
        value=value,
        exact_quote=value,
        evidence_type=evidence_type,
        confidence_band="medium",
    )

    assert ChangeDetector().detect_evidence(item, []).event_type == event_type


def test_impact_analyzer_maps_source_events_to_claims():
    event = {
        "event_id": "EV-001",
        "source_id": "S-001",
        "event_type": "page_changed",
        "severity": "medium",
        "confidence_band": "medium",
    }

    impacts = ImpactAnalyzer().analyze([event], [claim()], [evidence()])

    assert len(impacts) == 1
    assert impacts[0].claim_id == "C-001"
    assert impacts[0].affected_section_ids == ["product_comparison"]
    assert impacts[0].impact_type == "context"
    assert impacts[0].impact_level == "medium"
    assert impacts[0].requires_review is False
    assert impacts[0].to_repository_rows()[0]["section_id"] == (
        "product_comparison"
    )


def test_impact_analyzer_maps_new_source_by_entity_and_attribute():
    new_evidence = evidence().model_copy(
        update={"evidence_id": "E-002", "source_id": "S-002"}
    )
    event = {
        "event_id": "EV-NEW",
        "source_id": "S-002",
        "event_type": "feature_added",
        "severity": "medium",
        "confidence_band": "high",
        "evidence_ids": ["E-002"],
    }

    impacts = ImpactAnalyzer().analyze(
        [event], [claim()], [evidence(), new_evidence]
    )

    assert impacts[0].claim_id == "C-001"
    assert impacts[0].affected_section_ids == ["product_comparison"]


def test_impact_analyzer_requires_review_for_core_contradictions():
    event = {
        "event_id": "EV-001",
        "source_id": "S-001",
        "event_type": "business_model_change",
        "severity": "medium",
        "confidence_band": "medium",
    }

    impacts = ImpactAnalyzer().analyze(
        [event], [claim("business_model")], [evidence()]
    )

    assert impacts[0].impact_type == "supersedes"
    assert impacts[0].impact_level == "high"
    assert impacts[0].requires_review is True


def test_impact_analyzer_marks_core_business_shift_as_superseding():
    core_claim = claim().model_copy(
        update={"claim_type": "business_model"}
    )
    event = {
        "event_id": "EV-002",
        "source_id": "S-001",
        "event_type": "business_model_change",
        "severity": "medium",
        "materiality_level": "medium",
        "confidence_band": "high",
        "evidence_ids": ["E-001"],
    }

    impacts = ImpactAnalyzer().analyze([event], [core_claim], [evidence()])

    assert impacts[0].impact_type == "supersedes"
    assert impacts[0].impact_level == "high"
    assert impacts[0].requires_review is True


def test_update_decider_covers_observe_auto_patch_and_review():
    event = {"event_id": "EV-001"}
    impact = ImpactAnalyzer().analyze(
        [
            {
                "event_id": "EV-001",
                "source_id": "S-001",
                "event_type": "page_changed",
                "severity": "medium",
                "confidence_band": "medium",
            }
        ],
        [claim()],
        [evidence()],
    )[0]
    decider = UpdateDecider()

    observe = decider.decide(mode="observe", events=[event], impacts=[impact])
    safe = decider.decide(mode="safe", events=[event], impacts=[impact])
    review = decider.decide(
        mode="safe",
        events=[event],
        impacts=[
            ImpactAnalyzer().analyze(
                [
                    {
                        "event_id": "EV-002",
                        "source_id": "S-001",
                        "event_type": "business_model_change",
                        "severity": "medium",
                    }
                ],
                [claim("business_model")],
                [evidence()],
            )[0]
        ],
    )

    assert observe.action == "evidence_only"
    assert safe.action == "auto_patch"
    assert safe.affected_claim_ids == ["C-001"]
    assert safe.affected_section_ids == ["product_comparison"]
    assert review.action == "review_required"


def test_update_decider_uses_evidence_only_for_unchanged_support():
    impact = replace(
        ImpactAnalyzer().analyze(
            [
                {
                    "event_id": "EV-SUPPORT",
                    "source_id": "S-001",
                    "event_type": "new_evidence",
                    "severity": "medium",
                    "confidence_band": "medium",
                }
            ],
            [claim()],
            [evidence()],
        )[0],
        old_confidence_band="medium",
        proposed_confidence_band="medium",
    )

    decision = UpdateDecider().decide(
        mode="safe",
        events=[{"event_id": "EV-SUPPORT"}],
        impacts=[impact],
    )

    assert decision.action == "evidence_only"


def test_update_decider_rejects_low_confidence_auto_patch():
    impact = replace(
        ImpactAnalyzer().analyze(
            [
                {
                    "event_id": "EV-LOW",
                    "source_id": "S-001",
                    "event_type": "page_changed",
                    "severity": "medium",
                    "confidence_band": "low",
                }
            ],
            [claim()],
            [evidence()],
        )[0],
        proposed_confidence_band="low",
    )

    decision = UpdateDecider().decide(
        mode="safe",
        events=[{"event_id": "EV-LOW"}],
        impacts=[impact],
    )

    assert decision.action == "review_required"
