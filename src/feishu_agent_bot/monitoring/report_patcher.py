from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

from ..agent.report_generator import SECTION_TITLES


PatchOperation = Literal["replace", "append", "remove"]


@dataclass(frozen=True)
class SectionPatch:
    section_id: str
    operation: PatchOperation
    new_content_blocks: list[dict]
    revised_claim_ids: list[str] = field(default_factory=list)
    evidence_ids: list[str] = field(default_factory=list)
    change_reason: str = ""

    def to_dict(self) -> dict:
        return {
            "section_id": self.section_id,
            "operation": self.operation,
            "new_content_blocks": self.new_content_blocks,
            "revised_claim_ids": self.revised_claim_ids,
            "evidence_ids": self.evidence_ids,
            "change_reason": self.change_reason,
        }


class ReportPatcher:
    def apply_patch(
        self,
        *,
        base_markdown: str,
        base_report: dict,
        patch_json: dict,
        monitoring_revision: dict,
        evidence: list[dict],
        claims: list[dict],
        sources: list[dict],
        change_events: list[dict],
        claim_impacts: list[dict],
    ) -> tuple[str, dict]:
        self.validate_patch(
            patch_json,
            allowed_section_ids=patch_json.get("impacted_section_ids", []),
            known_evidence_ids={item["evidence_id"] for item in evidence},
        )
        report = deepcopy(base_report)
        report["generated_at"] = datetime.now(timezone.utc).isoformat()
        report["sources"] = sources
        report["evidence"] = evidence
        report["claims"] = claims
        report["monitoring_changes"] = change_events[:50]
        report["claim_impacts"] = claim_impacts[:100]
        report["monitoring_revision"] = {
            **monitoring_revision,
            "section_patches": patch_json["section_patches"],
        }
        self._apply_section_patches_to_json(report, patch_json["section_patches"])
        markdown = base_markdown
        for section_patch in patch_json["section_patches"]:
            markdown = self._append_section_patch_markdown(markdown, section_patch)
        return markdown.rstrip() + "\n", report

    def build_patch(
        self,
        *,
        base_report_version_id: str | None,
        affected_section_ids: list[str],
        change_events: list[dict],
        claim_impacts: list[dict],
        evidence_ids: list[str],
        summary: str,
        decision: str,
    ) -> dict:
        section_ids = self._validated_sections(affected_section_ids)
        event_ids = [event["event_id"] for event in change_events]
        impacted_claim_ids = sorted(
            {
                impact["claim_id"]
                for impact in claim_impacts
                if impact.get("claim_id")
            }
        )
        events_by_section: dict[str, list[dict]] = {key: [] for key in section_ids}
        for impact in claim_impacts:
            section_id = impact["section_id"]
            if section_id in events_by_section:
                event = next(
                    (
                        item
                        for item in change_events
                        if item["event_id"] == impact["event_id"]
                    ),
                    None,
                )
                if event and event not in events_by_section[section_id]:
                    events_by_section[section_id].append(event)
        section_patches = [
            SectionPatch(
                section_id=section_id,
                operation="append",
                new_content_blocks=self._content_blocks(
                    section_id, events_by_section.get(section_id, [])
                ),
                revised_claim_ids=sorted(
                    {
                        impact["claim_id"]
                        for impact in claim_impacts
                        if impact.get("claim_id")
                        and impact.get("section_id") == section_id
                    }
                ),
                evidence_ids=sorted(set(evidence_ids)),
                change_reason=summary,
            ).to_dict()
            for section_id in section_ids
        ]
        return {
            "revision_type": "partial",
            "base_report_version_id": base_report_version_id,
            "decision": decision,
            "summary": summary,
            "impacted_section_ids": section_ids,
            "impacted_claim_ids": impacted_claim_ids,
            "change_event_ids": event_ids,
            "section_patches": section_patches,
        }

    @staticmethod
    def validate_patch(
        patch_json: dict,
        *,
        allowed_section_ids: list[str],
        known_evidence_ids: set[str],
    ) -> None:
        allowed = set(allowed_section_ids)
        sections = patch_json.get("section_patches", [])
        if not isinstance(sections, list):
            raise ValueError("section_patches must be a list")
        for section in sections:
            section_id = section.get("section_id")
            if section_id not in allowed:
                raise ValueError(f"patch modifies non-affected section: {section_id}")
            if section_id not in SECTION_TITLES:
                raise ValueError(f"unknown section_id: {section_id}")
            operation = section.get("operation")
            if operation not in {"replace", "append", "remove"}:
                raise ValueError(f"invalid patch operation: {operation}")
            evidence_ids = set(section.get("evidence_ids", []))
            missing = evidence_ids - known_evidence_ids
            if missing:
                raise ValueError(
                    "patch references unknown evidence_id: "
                    + ", ".join(sorted(missing))
                )

    @staticmethod
    def _validated_sections(section_ids: list[str]) -> list[str]:
        cleaned = []
        for section_id in section_ids:
            if section_id not in SECTION_TITLES:
                raise ValueError(f"unknown section_id: {section_id}")
            if section_id not in cleaned:
                cleaned.append(section_id)
        return cleaned

    @staticmethod
    def _content_blocks(section_id: str, events: list[dict]) -> list[dict]:
        if not events:
            return [
                {
                    "type": "monitoring_note",
                    "text": (
                        f"本轮监测提示 `{SECTION_TITLES[section_id]}` 章节需要复核。"
                    ),
                    "change_event_ids": [],
                }
            ]
        return [
            {
                "type": "monitoring_change",
                "text": event["summary"],
                "event_type": event["event_type"],
                "materiality_level": (
                    event.get("materiality_level")
                    or event.get("severity")
                    or "medium"
                ),
                "confidence_band": event.get("confidence_band") or "medium",
                "change_event_ids": [event["event_id"]],
                "evidence_ids": event.get("evidence_ids", []),
            }
            for event in events
        ]

    @staticmethod
    def _apply_section_patches_to_json(report: dict, section_patches: list[dict]) -> None:
        sections = report.setdefault("sections", [])
        by_section = {section.get("section_id"): section for section in sections}
        for section_patch in section_patches:
            section_id = section_patch["section_id"]
            section = by_section.setdefault(
                section_id,
                {
                    "section_id": section_id,
                    "title": SECTION_TITLES[section_id],
                    "claim_ids": [],
                    "content_blocks": [],
                },
            )
            if section not in sections:
                sections.append(section)
            blocks = section.setdefault("content_blocks", [])
            operation = section_patch["operation"]
            if operation == "replace":
                section["content_blocks"] = list(section_patch["new_content_blocks"])
            elif operation == "append":
                blocks.extend(section_patch["new_content_blocks"])
            elif operation == "remove":
                section["content_blocks"] = []
            revised_claim_ids = section_patch.get("revised_claim_ids", [])
            claim_ids = section.setdefault("claim_ids", [])
            for claim_id in revised_claim_ids:
                if claim_id not in claim_ids:
                    claim_ids.append(claim_id)

    @staticmethod
    def _append_section_patch_markdown(markdown: str, section_patch: dict) -> str:
        section_id = section_patch["section_id"]
        heading = f"## {SECTION_TITLES[section_id]}"
        insertion = ReportPatcher._render_section_patch_markdown(section_patch)
        lines = markdown.rstrip().splitlines()
        heading_index = next(
            (index for index, line in enumerate(lines) if line.strip() == heading),
            None,
        )
        if heading_index is None:
            return markdown.rstrip() + "\n\n" + heading + insertion + "\n"
        next_heading_index = next(
            (
                index
                for index in range(heading_index + 1, len(lines))
                if lines[index].startswith("## ")
            ),
            len(lines),
        )
        return (
            "\n".join(lines[:next_heading_index]).rstrip()
            + insertion
            + "\n"
            + "\n".join(lines[next_heading_index:]).lstrip("\n")
        ).rstrip() + "\n"

    @staticmethod
    def _render_section_patch_markdown(section_patch: dict) -> str:
        lines = [
            "",
            "",
            "### 本轮监测更新",
            f"- 修订原因：{section_patch.get('change_reason') or '监测发现相关变化'}",
        ]
        evidence_ids = section_patch.get("evidence_ids", [])
        if evidence_ids:
            lines.append(f"- 引用证据：{', '.join(evidence_ids)}")
        for block in section_patch.get("new_content_blocks", []):
            text = block.get("text")
            if not text:
                continue
            event_ids = block.get("change_event_ids", [])
            suffix = f"（事件：{', '.join(event_ids)}）" if event_ids else ""
            lines.append(f"- {text}{suffix}")
        return "\n".join(lines)
