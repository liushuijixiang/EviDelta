from __future__ import annotations

from .models import ClaimImpactCandidate, UpdateDecision


class UpdateDecider:
    def decide(
        self,
        *,
        mode: str,
        events: list[dict],
        impacts: list[ClaimImpactCandidate],
        max_auto_patch_sections: int = 3,
    ) -> UpdateDecision:
        event_ids = [event["event_id"] for event in events]
        affected_claim_ids = sorted(
            {
                impact.claim_id
                for impact in impacts
                if impact.claim_id is not None
            }
        )
        affected_section_ids = sorted(
            {
                section_id
                for impact in impacts
                for section_id in impact.affected_section_ids
            }
        )
        if not events:
            return UpdateDecision(
                action="no_change",
                reason="无有效新事件。",
                affected_claim_ids=[],
                affected_section_ids=[],
                change_event_ids=[],
            )
        if mode == "observe":
            return UpdateDecision(
                action="evidence_only",
                reason="observe 模式只保存证据、事件和影响分析，不生成报告版本。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if mode != "safe":
            return UpdateDecision(
                action="review_required",
                reason=f"未知更新模式 {mode}，需要人工复核。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if any(impact.requires_review for impact in impacts):
            return UpdateDecision(
                action="review_required",
                reason="存在高影响、冲突证据或核心 claim 变化。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if any(
            impact.impact_level in {"high", "critical"} for impact in impacts
        ):
            return UpdateDecision(
                action="review_required",
                reason="影响等级达到 high 或 critical。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if any(
            impact.proposed_confidence_band == "conflicting" for impact in impacts
        ):
            return UpdateDecision(
                action="review_required",
                reason="证据置信等级为 conflicting。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if any(
            impact.proposed_confidence_band == "low" for impact in impacts
        ):
            return UpdateDecision(
                action="review_required",
                reason="新证据置信等级低于自动发布要求。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if len(affected_section_ids) > max_auto_patch_sections:
            return UpdateDecision(
                action="review_required",
                reason="受影响章节超过自动更新上限。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if impacts and all(
            impact.impact_type == "supports"
            and impact.old_confidence_band == impact.proposed_confidence_band
            and not impact.requires_review
            for impact in impacts
        ):
            return UpdateDecision(
                action="evidence_only",
                reason="新证据仅支持原结论且不改变置信等级，更新证据账本即可。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        if not affected_section_ids:
            return UpdateDecision(
                action="evidence_only",
                reason="没有明确受影响章节，只记录证据账本。",
                affected_claim_ids=affected_claim_ids,
                affected_section_ids=affected_section_ids,
                change_event_ids=event_ids,
            )
        return UpdateDecision(
            action="auto_patch",
            reason="safe 模式下仅存在 low/medium 且范围明确的变化。",
            affected_claim_ids=affected_claim_ids,
            affected_section_ids=affected_section_ids,
            change_event_ids=event_ids,
        )
