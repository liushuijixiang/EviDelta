from pathlib import Path

import pytest

from feishu_agent_bot.agent.report_generator import ReportGenerator
from feishu_agent_bot.agent.report_validator import (
    ReportValidationError,
    ReportValidator,
)
from feishu_agent_bot.artifacts import ArtifactStore
from feishu_agent_bot.llm.schemas import (
    PolishedReport,
    PolishedReportParagraph,
    PolishedReportSection,
    ResearchPlan,
    StoredClaim,
    StoredEvidence,
)
from feishu_agent_bot.llm.mock import MockLLM


def report_data():
    plan = ResearchPlan(
        objective="比较产品",
        research_questions=["差异是什么"],
        search_queries=["公司 官网"],
        comparison_dimensions=["产品"],
        expected_entities=["公司"],
        acceptance_criteria=["有引用"],
    )
    sources = [
        {
            "source_id": "S-001",
            "title": "官方资料",
            "url": "https://example.com",
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "raw_text": "该产品支持快充。",
        }
    ]
    evidence = [
        StoredEvidence(
            evidence_id="E-001",
            source_id="S-001",
            entity="公司",
            attribute="产品",
            value="支持快充",
            exact_quote="该产品支持快充。",
            evidence_type="product_feature",
            confidence_band="high",
        )
    ]
    claims = [
        StoredClaim(
            claim_id="C-001",
            statement="该产品具备快充能力。",
            claim_type="product_comparison",
            supporting_evidence_ids=["E-001"],
            confidence_band="medium",
            reasoning_summary="官方资料支持",
        ),
        StoredClaim(
            claim_id="C-002",
            statement="仍缺少价格和销量资料。",
            claim_type="uncertainty",
            confidence_band="low",
            reasoning_summary="数据缺口",
        ),
    ]
    return plan, sources, evidence, claims


def test_report_references_and_atomic_write(tmp_path):
    plan, sources, evidence, claims = report_data()
    markdown, report = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    markdown_path, json_path = ArtifactStore(tmp_path).write_report(
        "job", 1, markdown, report
    )
    ReportValidator().validate(
        markdown=markdown,
        report_path=markdown_path,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    assert markdown_path.read_text(encoding="utf-8")
    assert json_path.read_text(encoding="utf-8")
    assert not list((tmp_path / "job").glob("*.tmp"))


def test_report_generator_polishes_with_existing_claims_only(tmp_path):
    plan, sources, evidence, claims = report_data()
    llm = MockLLM(
        {
            PolishedReport: PolishedReport(
                sections=[
                    PolishedReportSection(
                        section_id="executive_summary",
                        paragraphs=[
                            PolishedReportParagraph(
                                text="现有资料表明该产品具备快充能力。",
                                claim_ids=["C-001"],
                            )
                        ],
                    ),
                    PolishedReportSection(
                        section_id="product_comparison",
                        paragraphs=[
                            PolishedReportParagraph(
                                text="在产品能力方面，现有资料支持其具备快充能力。",
                                claim_ids=["C-001"],
                            )
                        ],
                    ),
                ],
                final_conclusion=[
                    PolishedReportParagraph(
                        text="综合现有资料，该产品的明确优势是快充能力。",
                        claim_ids=["C-001"],
                    )
                ],
                recommendations=[
                    PolishedReportParagraph(
                        text="建议后续优先补充价格与销量资料，再开展商业比较。",
                        claim_ids=["C-002"],
                    )
                ],
            )
        }
    )

    markdown, report = ReportGenerator(llm).generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown, encoding="utf-8")

    assert report["language"] == "zh"
    assert report["polished_by_ai"] is True
    product = next(
        item for item in report["sections"]
        if item["section_id"] == "product_comparison"
    )
    assert "现有资料支持" in product["body"]
    conclusion = next(
        item for item in report["sections"]
        if item["section_id"] == "final_conclusion"
    )
    recommendations = next(
        item for item in report["sections"]
        if item["section_id"] == "recommendations"
    )
    assert "明确优势" in conclusion["body"]
    assert "建议后续" in recommendations["body"]
    assert "结构化结论审计" in markdown
    assert llm.calls
    assert "不得添加新证据" in llm.calls[0]["system_prompt"]
    ReportValidator().validate(
        markdown=markdown,
        report_path=path,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )


def test_report_generator_rejects_polished_numbers_not_in_evidence():
    plan, sources, evidence, claims = report_data()
    llm = MockLLM(
        {
            PolishedReport: PolishedReport(
                sections=[
                    PolishedReportSection(
                        section_id="product_comparison",
                        paragraphs=[
                            PolishedReportParagraph(
                                text="该产品已部署 700 座快充站。",
                                claim_ids=["C-001"],
                            )
                        ],
                    )
                ]
            )
        }
    )

    _markdown, report = ReportGenerator(llm).generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )

    product = next(
        item for item in report["sections"]
        if item["section_id"] == "product_comparison"
    )
    assert product["body"] == "该产品具备快充能力。"
    assert report["polished_by_ai"] is False


def test_report_generator_accepts_provably_equivalent_unit_conversions():
    claim = StoredClaim(
        claim_id="C-001",
        statement="Revenue reached $9 billion and users reached 650 million.",
        claim_type="market_position",
        supporting_evidence_ids=["E-001"],
        confidence_band="medium",
        reasoning_summary="source",
    )
    evidence = StoredEvidence(
        evidence_id="E-001",
        source_id="S-001",
        entity="Company",
        attribute="metrics",
        value="$9 billion; 650 million users; January 2026",
        exact_quote="$9 billion and 650 million users in January 2026.",
        evidence_type="metric",
        confidence_band="high",
    )

    assert ReportGenerator._numbers_supported(
        "截至2026年1月，收入达到90亿美元，用户达到6.5亿。",
        ["C-001"],
        {"C-001": claim},
        {"E-001": evidence},
    )
    assert ReportGenerator._text_numbers_supported(
        "节省了6000万美元。",
        "Klarna saved $60M.",
    )
    assert not ReportGenerator._numbers_supported(
        "截至2026年1月，收入达到91亿美元，用户达到6.5亿。",
        ["C-001"],
        {"C-001": claim},
        {"E-001": evidence},
    )


def test_report_rejects_missing_evidence_reference(tmp_path):
    plan, sources, evidence, claims = report_data()
    markdown, _ = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown + "\n[证据 E-999，来源 S-001]", encoding="utf-8")
    with pytest.raises(ReportValidationError):
        ReportValidator().validate(
            markdown=path.read_text(),
            report_path=path,
            sources=sources,
            evidence=evidence,
            claims=claims,
        )


def test_report_rejects_core_fact_without_inline_citation(tmp_path):
    plan, sources, evidence, claims = report_data()
    markdown, _ = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    markdown = markdown.replace(
        "该产品具备快充能力。 [证据 E-001，来源 S-001]",
        "该产品具备快充能力。",
        1,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown, encoding="utf-8")
    with pytest.raises(ReportValidationError):
        ReportValidator().validate(
            markdown=markdown,
            report_path=path,
            sources=sources,
            evidence=evidence,
            claims=claims,
        )


def test_report_accepts_number_supported_by_company_statement(tmp_path):
    plan, sources, evidence, claims = report_data()
    sources[0]["raw_text"] = "公司预计2026年建成700座充电站。"
    evidence[0] = evidence[0].model_copy(
        update={
            "value": "2026年建成700座",
            "exact_quote": "公司预计2026年建成700座充电站。",
            "evidence_type": "company_statement",
        }
    )
    claims[0] = claims[0].model_copy(
        update={"statement": "公司预计2026年建成700座充电站。"}
    )
    markdown, _ = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown, encoding="utf-8")
    ReportValidator().validate(
        markdown=markdown,
        report_path=path,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )


def test_report_accepts_thousands_separator_in_referenced_evidence(tmp_path):
    plan, sources, evidence, claims = report_data()
    sources[0]["raw_text"] = "公司服务超过 5,000 家企业客户。"
    evidence[0] = evidence[0].model_copy(
        update={
            "value": "超过5,000家企业客户",
            "exact_quote": "公司服务超过 5,000 家企业客户。",
            "evidence_type": "company_statement",
        }
    )
    claims[0] = claims[0].model_copy(
        update={"statement": "公司服务超过5000家企业客户。"}
    )
    markdown, _ = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown, encoding="utf-8")
    ReportValidator().validate(
        markdown=markdown,
        report_path=path,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )


def test_report_rejects_number_missing_from_referenced_evidence(tmp_path):
    plan, sources, evidence, claims = report_data()
    claims[0] = claims[0].model_copy(
        update={"statement": "该产品已部署700座快充站。"}
    )
    markdown, _ = ReportGenerator().generate(
        topic="测试",
        plan=plan,
        sources=sources,
        evidence=evidence,
        claims=claims,
    )
    path = tmp_path / "report.md"
    path.write_text(markdown, encoding="utf-8")
    with pytest.raises(
        ReportValidationError, match="具体数字未在引用证据中出现"
    ):
        ReportValidator().validate(
            markdown=markdown,
            report_path=path,
            sources=sources,
            evidence=evidence,
            claims=claims,
        )
