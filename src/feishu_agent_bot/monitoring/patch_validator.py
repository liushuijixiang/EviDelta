from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from ..agent.report_generator import SECTION_TITLES


class MonitoringPatchValidationError(ValueError):
    pass


class MonitoringPatchValidator:
    SECRET_PATTERNS = [
        re.compile(r"(?i)['\"]?\bapi[_-]?key\b['\"]?\s*[:=]\s*['\"]?[A-Za-z0-9_\-]{8,}"),
        re.compile(r"(?i)['\"]?\bauthorization\b['\"]?\s*[:=]\s*['\"]?bearer\s+[A-Za-z0-9._\-]+"),
        re.compile(r"(?i)['\"]?\b(app[_-]?secret|access[_-]?token|tenant[_-]?access[_-]?token)\b['\"]?\s*[:=]\s*['\"]?[A-Za-z0-9._\-]{8,}"),
    ]
    PLACEHOLDERS = {"TODO", "TBD", "PLACEHOLDER", "待补充"}

    def validate(
        self,
        *,
        patch: dict,
        report: dict,
        base_report: dict | None,
        sources: list[dict],
        evidence,
        claim_revisions: list[dict],
        change_events: list[dict],
        markdown_path: Path,
        json_path: Path,
        snapshots: list[dict] | None = None,
    ) -> None:
        patch_json = patch["patch_json"]
        if not base_report:
            raise MonitoringPatchValidationError("base version 不存在")
        if patch.get("base_report_version_id") != base_report["report_version_id"]:
            raise MonitoringPatchValidationError("patch base version 不正确")
        if report.get("parent_report_version_id") != base_report["report_version_id"]:
            raise MonitoringPatchValidationError("report parent version 不正确")
        if not markdown_path.exists() or markdown_path.stat().st_size == 0:
            raise MonitoringPatchValidationError("Markdown 报告为空")
        if not json_path.exists() or json_path.stat().st_size == 0:
            raise MonitoringPatchValidationError("JSON 报告为空")
        self._validate_no_secrets_or_placeholders(markdown_path, json_path)
        self._validate_base_hashes(patch_json, base_report)

        impacted_sections = set(patch_json.get("impacted_section_ids", []))
        if not impacted_sections:
            raise MonitoringPatchValidationError("没有受影响章节，禁止生成空版本")
        section_patches = patch_json.get("section_patches", [])
        if not section_patches:
            raise MonitoringPatchValidationError("patch 缺少 section_patches")
        self._validate_structured_diff(
            base_report=base_report,
            json_path=json_path,
            impacted_sections=impacted_sections,
        )

        evidence_by_id = {item.evidence_id: item for item in evidence}
        raw_by_source = {
            source["source_id"]: source.get("raw_text", "") for source in sources
        }
        snapshot_by_id = {
            snapshot["snapshot_id"]: snapshot for snapshot in (snapshots or [])
        }
        for section in section_patches:
            section_id = section.get("section_id")
            if section_id not in SECTION_TITLES:
                raise MonitoringPatchValidationError("section_id 不存在")
            if section_id not in impacted_sections:
                raise MonitoringPatchValidationError("patch 修改了未允许章节")
            if section.get("operation") not in {"replace", "append", "remove"}:
                raise MonitoringPatchValidationError("patch operation 不合法")
            if section.get("operation") != "remove" and not section.get(
                "new_content_blocks"
            ):
                raise MonitoringPatchValidationError("patch 内容为空")
            for evidence_id in section.get("evidence_ids", []):
                item = evidence_by_id.get(evidence_id)
                if not item:
                    raise MonitoringPatchValidationError(
                        "patch 引用了不存在的 evidence_id"
                    )
                if item.source_id not in raw_by_source:
                    raise MonitoringPatchValidationError(
                        "patch evidence 引用了不存在的 source_id"
                    )
                raw_text = raw_by_source[item.source_id]
                snapshot_id = getattr(item, "snapshot_id", None)
                if snapshot_id:
                    snapshot = snapshot_by_id.get(snapshot_id)
                    if not snapshot:
                        raise MonitoringPatchValidationError(
                            "patch evidence 引用了不存在的 snapshot_id"
                        )
                    raw_text = snapshot.get("raw_text") or raw_text
                if item.exact_quote not in raw_text:
                    raise MonitoringPatchValidationError(
                        "patch evidence exact_quote 不在来源或快照正文中"
                    )

        known_claim_ids = set(patch_json.get("impacted_claim_ids", []))
        for revision in claim_revisions:
            original_claim_id = revision.get("original_claim_id")
            if original_claim_id and original_claim_id not in known_claim_ids:
                raise MonitoringPatchValidationError(
                    "claim revision 引用了未受影响的 claim"
                )
            for evidence_id in (
                revision.get("supporting_evidence_ids", [])
                + revision.get("contradicting_evidence_ids", [])
            ):
                if evidence_id not in evidence_by_id:
                    raise MonitoringPatchValidationError(
                        "claim revision 引用了不存在的 evidence_id"
                    )

        patch_event_ids = set(patch_json.get("change_event_ids", []))
        referenced_evidence_ids = {
            evidence_id
            for section in section_patches
            for evidence_id in section.get("evidence_ids", [])
        }
        self._validate_numeric_changes(
            change_events=change_events,
            patch_event_ids=patch_event_ids,
            referenced_evidence_ids=referenced_evidence_ids,
            evidence_by_id=evidence_by_id,
        )
        if patch_json.get("decision") == "auto_patch":
            for event in change_events:
                if event.get("event_id") not in patch_event_ids:
                    continue
                level = event.get("materiality_level") or event.get("severity")
                confidence = event.get("confidence_band")
                if level in {"high", "critical"} or confidence == "conflicting":
                    raise MonitoringPatchValidationError(
                        "重大或冲突变化不能 auto_patch 发布"
                    )

    @staticmethod
    def _validate_base_hashes(patch_json: dict, base_report: dict) -> None:
        expected_markdown = patch_json.get("base_report_hash")
        expected_json = patch_json.get("base_report_json_hash")
        if expected_markdown:
            actual = hashlib.sha256(
                Path(base_report["report_path"]).read_bytes()
            ).hexdigest()
            if actual != expected_markdown:
                raise MonitoringPatchValidationError("原报告 Markdown 已被修改")
        if expected_json:
            actual = hashlib.sha256(
                Path(base_report["report_json_path"]).read_bytes()
            ).hexdigest()
            if actual != expected_json:
                raise MonitoringPatchValidationError("原报告 JSON 已被修改")

    @staticmethod
    def _validate_structured_diff(
        *,
        base_report: dict,
        json_path: Path,
        impacted_sections: set[str],
    ) -> None:
        base_json_path = Path(base_report["report_json_path"])
        if not base_json_path.exists():
            raise MonitoringPatchValidationError("base report JSON 不存在")
        try:
            base_json = json.loads(base_json_path.read_text(encoding="utf-8"))
            updated_json = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MonitoringPatchValidationError("报告 JSON 无法解析") from exc
        base_sections = base_json.get("sections")
        updated_sections = updated_json.get("sections")
        if not isinstance(base_sections, list) or not isinstance(
            updated_sections, list
        ):
            return
        base_by_id = {
            section.get("section_id"): section
            for section in base_sections
            if section.get("section_id")
        }
        updated_by_id = {
            section.get("section_id"): section
            for section in updated_sections
            if section.get("section_id")
        }
        if set(base_by_id) != set(updated_by_id):
            raise MonitoringPatchValidationError("报告章节集合发生未声明变化")
        changed = {
            section_id
            for section_id in base_by_id
            if base_by_id[section_id] != updated_by_id[section_id]
        }
        if not changed:
            raise MonitoringPatchValidationError("新旧报告没有结构化变化")
        if not changed.issubset(impacted_sections):
            raise MonitoringPatchValidationError("JSON 修改了未允许章节")

    @staticmethod
    def _validate_numeric_changes(
        *,
        change_events: list[dict],
        patch_event_ids: set[str],
        referenced_evidence_ids: set[str],
        evidence_by_id: dict,
    ) -> None:
        for event in change_events:
            if event.get("event_id") not in patch_event_ids:
                continue
            old_value = str(
                event.get("old_value_json") or event.get("old_value") or ""
            )
            new_value = str(
                event.get("new_value_json") or event.get("new_value") or ""
            )
            if old_value == new_value or not (
                re.search(r"\d", old_value) or re.search(r"\d", new_value)
            ):
                continue
            if not any(
                evidence_by_id[evidence_id].evidence_type in {"metric", "price"}
                for evidence_id in referenced_evidence_ids
                if evidence_id in evidence_by_id
            ):
                raise MonitoringPatchValidationError(
                    "数字变化缺少 metric 或 price Evidence"
                )

    def _validate_no_secrets_or_placeholders(
        self, markdown_path: Path, json_path: Path
    ) -> None:
        for path in (markdown_path, json_path):
            text = path.read_text(encoding="utf-8")
            if any(placeholder in text for placeholder in self.PLACEHOLDERS):
                raise MonitoringPatchValidationError("报告包含占位符")
            if any(pattern.search(text) for pattern in self.SECRET_PATTERNS):
                raise MonitoringPatchValidationError("报告包含敏感凭据")
