from pathlib import Path

import pytest

from feishu_agent_bot.llm.schemas import EvidenceItem
from feishu_agent_bot.monitoring.patch_validator import (
    MonitoringPatchValidationError,
    MonitoringPatchValidator,
)


def _context(
    repository,
    tmp_path,
    *,
    decision="auto_patch",
    event_severity="medium",
    use_snapshot=False,
):
    job = repository.create_job("u1", "c1", "m1", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/a",
        canonical_url="https://example.com/a",
        title="source",
        publisher="Example",
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="旧正文不包含新证据。" if use_snapshot else "该产品支持高功率快充。",
        status="fetched",
    )
    snapshot_id = None
    if use_snapshot:
        snapshot_id = repository.add_monitoring_source_snapshot(
            job_id=job.job_id,
            source_id=source_id,
            run_id="run-1",
            url="https://example.com/a",
            http_status=200,
            content_type="text/html",
            content_hash="hash-2",
            raw_text="该产品支持高功率快充。",
            retrieval_method="incremental_fetch",
            status="fetched",
        )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
        snapshot_id=snapshot_id,
    )
    base_md = tmp_path / "base.md"
    base_json = tmp_path / "base.json"
    draft_md = tmp_path / "draft.md"
    draft_json = tmp_path / "draft.json"
    base_md.write_text("# base", encoding="utf-8")
    base_json.write_text("{}", encoding="utf-8")
    draft_md.write_text("# draft", encoding="utf-8")
    draft_json.write_text("{}", encoding="utf-8")
    base_id = repository.add_report_version(
        job.job_id, 1, str(base_md), str(base_json)
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="feature_added",
        severity=event_severity,
        summary="新增高功率快充",
    )
    draft_id = repository.add_report_version(
        job.job_id,
        2,
        str(draft_md),
        str(draft_json),
        status="draft",
        parent_report_version_id=base_id,
        trigger_type="monitor_auto_patch",
    )
    patch_json = {
        "revision_type": "partial",
        "base_report_version_id": base_id,
        "decision": decision,
        "summary": "partial update",
        "impacted_section_ids": ["product_comparison"],
        "impacted_claim_ids": ["C-001"],
        "change_event_ids": [event_id],
        "section_patches": [
            {
                "section_id": "product_comparison",
                "operation": "append",
                "new_content_blocks": [{"type": "monitoring_change", "text": "x"}],
                "revised_claim_ids": ["C-001"],
                "evidence_ids": ["E-001"],
                "change_reason": "partial update",
            }
        ],
    }
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=draft_id,
        base_report_version_id=base_id,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="partial update",
        status="draft",
        patch_json=patch_json,
    )
    repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id="C-001",
        supersedes_claim_revision_id=None,
        report_version_id=draft_id,
        statement="updated",
        confidence_band="high",
        reason="partial update",
        supporting_evidence_ids=["E-001"],
        contradicting_evidence_ids=[],
        status="draft",
    )
    return {
        "job_id": job.job_id,
        "base_id": base_id,
        "draft_id": draft_id,
        "revision_id": revision_id,
        "event_id": event_id,
        "snapshot_id": snapshot_id,
    }


def _validate(repository, ctx):
    draft = repository.get_report_version(ctx["job_id"], 2)
    base = repository.get_report_version(ctx["job_id"], 1)
    patch = repository.get_report_patch_by_report_version_id(ctx["draft_id"])
    MonitoringPatchValidator().validate(
        patch=patch,
        report=draft,
        base_report=base,
        sources=repository.list_sources(ctx["job_id"], "fetched"),
        evidence=repository.list_evidence(ctx["job_id"]),
        claim_revisions=repository.list_claim_revisions(
            ctx["job_id"], ctx["draft_id"]
        ),
        change_events=repository.list_change_events(ctx["job_id"]),
        markdown_path=Path(draft["report_path"]),
        json_path=Path(draft["report_json_path"]),
        snapshots=repository.list_source_snapshots(ctx["job_id"]),
    )


def test_monitoring_patch_validator_accepts_valid_patch(repository, tmp_path):
    ctx = _context(repository, tmp_path)

    _validate(repository, ctx)


def test_monitoring_patch_validator_accepts_snapshot_evidence(repository, tmp_path):
    ctx = _context(repository, tmp_path, use_snapshot=True)

    _validate(repository, ctx)


def test_monitoring_patch_validator_rejects_missing_snapshot(repository, tmp_path):
    ctx = _context(repository, tmp_path, use_snapshot=True)
    draft = repository.get_report_version(ctx["job_id"], 2)
    base = repository.get_report_version(ctx["job_id"], 1)
    patch = repository.get_report_patch_by_report_version_id(ctx["draft_id"])

    with pytest.raises(MonitoringPatchValidationError, match="snapshot_id"):
        MonitoringPatchValidator().validate(
            patch=patch,
            report=draft,
            base_report=base,
            sources=repository.list_sources(ctx["job_id"], "fetched"),
            evidence=repository.list_evidence(ctx["job_id"]),
            claim_revisions=repository.list_claim_revisions(
                ctx["job_id"], ctx["draft_id"]
            ),
            change_events=repository.list_change_events(ctx["job_id"]),
            markdown_path=Path(draft["report_path"]),
            json_path=Path(draft["report_json_path"]),
            snapshots=[],
        )


def test_monitoring_patch_validator_rejects_high_auto_patch(repository, tmp_path):
    ctx = _context(repository, tmp_path, decision="auto_patch", event_severity="high")

    with pytest.raises(MonitoringPatchValidationError, match="不能 auto_patch"):
        _validate(repository, ctx)


def test_monitoring_patch_validator_ignores_unrelated_high_events(
    repository, tmp_path
):
    ctx = _context(repository, tmp_path, decision="auto_patch", event_severity="medium")
    repository.add_change_event(
        job_id=ctx["job_id"],
        event_type="business_model_change",
        severity="high",
        summary="unrelated old event",
        status="resolved",
    )

    _validate(repository, ctx)


def test_monitoring_patch_validator_rejects_unknown_evidence(repository, tmp_path):
    ctx = _context(repository, tmp_path)
    patch = repository.get_report_patch_by_report_version_id(ctx["draft_id"])
    patch["patch_json"]["section_patches"][0]["evidence_ids"] = ["E-404"]

    draft = repository.get_report_version(ctx["job_id"], 2)
    base = repository.get_report_version(ctx["job_id"], 1)
    with pytest.raises(MonitoringPatchValidationError, match="evidence_id"):
        MonitoringPatchValidator().validate(
            patch=patch,
            report=draft,
            base_report=base,
            sources=repository.list_sources(ctx["job_id"], "fetched"),
            evidence=repository.list_evidence(ctx["job_id"]),
            claim_revisions=repository.list_claim_revisions(
                ctx["job_id"], ctx["draft_id"]
            ),
            change_events=repository.list_change_events(ctx["job_id"]),
            markdown_path=Path(draft["report_path"]),
            json_path=Path(draft["report_json_path"]),
        )


def test_monitoring_patch_validator_rejects_placeholders(repository, tmp_path):
    ctx = _context(repository, tmp_path)
    draft = repository.get_report_version(ctx["job_id"], 2)
    Path(draft["report_path"]).write_text("# draft\nTODO\n", encoding="utf-8")

    with pytest.raises(MonitoringPatchValidationError, match="占位符"):
        _validate(repository, ctx)


def test_monitoring_patch_validator_rejects_secrets(repository, tmp_path):
    ctx = _context(repository, tmp_path)
    draft = repository.get_report_version(ctx["job_id"], 2)
    Path(draft["report_json_path"]).write_text(
        '{"Authorization": "Bearer example-redacted-token"}',
        encoding="utf-8",
    )

    with pytest.raises(MonitoringPatchValidationError, match="敏感凭据"):
        _validate(repository, ctx)
