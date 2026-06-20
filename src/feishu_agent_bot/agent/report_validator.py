from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..llm.schemas import StoredClaim, StoredEvidence
from .report_generator import ReportGenerator


class ReportValidationError(ValueError):
    pass


class ReportValidator:
    def validate(
        self,
        *,
        markdown: str,
        report_path: Path,
        sources: list[dict[str, Any]],
        evidence: list[StoredEvidence],
        claims: list[StoredClaim],
        api_key: str = "",
        snapshots: list[dict[str, Any]] | None = None,
    ) -> None:
        source_ids = {source["source_id"] for source in sources}
        evidence_ids = {item.evidence_id for item in evidence}
        raw_by_source = {
            source["source_id"]: source.get("raw_text", "") for source in sources
        }
        snapshot_by_id = {
            snapshot["snapshot_id"]: snapshot for snapshot in (snapshots or [])
        }
        for item in evidence:
            if item.source_id not in source_ids:
                raise ReportValidationError("evidence 引用了不存在的 source_id")
            raw_text = raw_by_source[item.source_id]
            if item.snapshot_id:
                snapshot = snapshot_by_id.get(item.snapshot_id)
                if not snapshot:
                    raise ReportValidationError(
                        "evidence 引用了不存在的 snapshot_id"
                    )
                raw_text = snapshot.get("raw_text") or raw_text
            if item.exact_quote not in raw_text:
                raise ReportValidationError("evidence exact_quote 不在来源正文中")
        for claim in claims:
            referenced = (
                claim.supporting_evidence_ids + claim.contradicting_evidence_ids
            )
            if any(item not in evidence_ids for item in referenced):
                raise ReportValidationError("claim 引用了不存在的 evidence_id")
            if claim.claim_type != "uncertainty" and not claim.supporting_evidence_ids:
                raise ReportValidationError("事实性 claim 缺少证据")
            if claim.claim_type != "uncertainty":
                matching_lines = [
                    line
                    for line in markdown.splitlines()
                    if claim.statement in line
                ]
                if not matching_lines or any(
                    "[证据 " not in line for line in matching_lines
                ):
                    raise ReportValidationError("核心事实未在同一行附带证据引用")
        cited_evidence = set(re.findall(r"证据 (E-\d+)", markdown))
        cited_sources = set(re.findall(r"来源 (S-\d+)", markdown))
        if not cited_evidence.issubset(evidence_ids):
            raise ReportValidationError("报告包含不存在的 evidence 引用")
        if not cited_sources.issubset(source_ids):
            raise ReportValidationError("报告包含不存在的 source 引用")
        placeholders = {"TODO", "TBD", "PLACEHOLDER", "待补充"}
        if any(item in markdown for item in placeholders):
            raise ReportValidationError("报告包含占位符")
        if api_key and api_key in markdown:
            raise ReportValidationError("报告包含 API Key")
        evidence_by_id = {item.evidence_id: item for item in evidence}
        for line in markdown.splitlines():
            prose = re.sub(r"\[(?:证据|反证) E-\d+，来源 S-\d+\]", "", line)
            numbers = set(re.findall(r"\d+(?:\.\d+)?%?", prose))
            if numbers and "[证据 " in line:
                ids = set(re.findall(r"证据 (E-\d+)", line))
                referenced_text = " ".join(
                    f"{evidence_by_id[item].value} "
                    f"{evidence_by_id[item].exact_quote}"
                    for item in ids
                    if item in evidence_by_id
                )
                if not ReportGenerator._text_numbers_supported(
                    prose, referenced_text
                ):
                    raise ReportValidationError("具体数字未在引用证据中出现")
        if not report_path.exists() or report_path.stat().st_size == 0:
            raise ReportValidationError("报告文件不存在或为空")
