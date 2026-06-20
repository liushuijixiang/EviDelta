import pytest

from feishu_agent_bot.monitoring.report_patcher import ReportPatcher


def test_report_patcher_builds_section_patches():
    patch = ReportPatcher().build_patch(
        base_report_version_id="rv-1",
        affected_section_ids=["product_comparison"],
        change_events=[
            {
                "event_id": "EV-001",
                "event_type": "feature_added",
                "summary": "新增高功率快充能力",
                "severity": "medium",
                "confidence_band": "high",
            }
        ],
        claim_impacts=[
            {
                "event_id": "EV-001",
                "claim_id": "C-001",
                "section_id": "product_comparison",
                "impact_type": "supports",
                "severity": "medium",
                "rationale": "新证据支持原结论",
            }
        ],
        evidence_ids=["E-001"],
        summary="监测发现产品能力变化",
        decision="auto_patch",
    )

    assert patch["revision_type"] == "partial"
    assert patch["base_report_version_id"] == "rv-1"
    assert patch["impacted_section_ids"] == ["product_comparison"]
    assert patch["impacted_claim_ids"] == ["C-001"]
    assert patch["change_event_ids"] == ["EV-001"]
    assert patch["section_patches"][0]["operation"] == "append"
    assert patch["section_patches"][0]["evidence_ids"] == ["E-001"]
    assert patch["section_patches"][0]["new_content_blocks"][0]["text"] == (
        "新增高功率快充能力"
    )

    ReportPatcher.validate_patch(
        patch,
        allowed_section_ids=["product_comparison"],
        known_evidence_ids={"E-001"},
    )


def test_report_patcher_rejects_out_of_scope_sections_and_unknown_evidence():
    patch = {
        "section_patches": [
            {
                "section_id": "risk",
                "operation": "append",
                "new_content_blocks": [],
                "evidence_ids": ["E-404"],
            }
        ]
    }

    with pytest.raises(ValueError, match="non-affected section"):
        ReportPatcher.validate_patch(
            patch,
            allowed_section_ids=["product_comparison"],
            known_evidence_ids={"E-001"},
        )

    patch["section_patches"][0]["section_id"] = "product_comparison"
    with pytest.raises(ValueError, match="unknown evidence_id"):
        ReportPatcher.validate_patch(
            patch,
            allowed_section_ids=["product_comparison"],
            known_evidence_ids={"E-001"},
        )


def test_report_patcher_applies_patch_only_to_affected_markdown_section():
    base_markdown = "\n".join(
        [
            "# report",
            "",
            "## 竞品对比",
            "- old product comparison",
            "",
            "## 风险",
            "- risk text must stay identical",
            "",
            "## 来源清单",
            "source list must stay identical",
            "",
        ]
    )
    base_report = {
        "sections": [
            {
                "section_id": "product_comparison",
                "title": "竞品对比",
                "claim_ids": ["C-001"],
                "content_blocks": [{"type": "claim", "text": "old"}],
            },
            {
                "section_id": "risk",
                "title": "风险",
                "claim_ids": ["C-002"],
                "content_blocks": [{"type": "claim", "text": "risk"}],
            },
        ]
    }
    patch = ReportPatcher().build_patch(
        base_report_version_id="rv-1",
        affected_section_ids=["product_comparison"],
        change_events=[
            {
                "event_id": "EV-001",
                "event_type": "feature_added",
                "summary": "新增高功率快充能力",
                "severity": "medium",
                "confidence_band": "high",
            }
        ],
        claim_impacts=[
            {
                "event_id": "EV-001",
                "claim_id": "C-001",
                "section_id": "product_comparison",
                "impact_type": "supports",
                "severity": "medium",
                "rationale": "新证据支持原结论",
            }
        ],
        evidence_ids=["E-001"],
        summary="监测发现产品能力变化",
        decision="auto_patch",
    )

    markdown, report = ReportPatcher().apply_patch(
        base_markdown=base_markdown,
        base_report=base_report,
        patch_json=patch,
        monitoring_revision={"decision": "auto_patch"},
        evidence=[{"evidence_id": "E-001"}],
        claims=[],
        sources=[],
        change_events=[],
        claim_impacts=[],
    )

    assert "## 竞品对比\n- old product comparison\n\n### 本轮监测更新" in markdown
    assert "新增高功率快充能力" in markdown
    assert "## 风险\n- risk text must stay identical" in markdown
    assert "## 来源清单\nsource list must stay identical" in markdown
    risk = next(
        section for section in report["sections"] if section["section_id"] == "risk"
    )
    product = next(
        section
        for section in report["sections"]
        if section["section_id"] == "product_comparison"
    )
    assert risk["content_blocks"] == [{"type": "claim", "text": "risk"}]
    assert product["content_blocks"][-1]["text"] == "新增高功率快充能力"
