import json

from feishu_agent_bot.agent.claim_synthesizer import ClaimSynthesizer
from feishu_agent_bot.agent.evidence_extractor import EvidenceExtractor
from feishu_agent_bot.llm.schemas import (
    ClaimBatch,
    ClaimItem,
    EvidenceBatch,
    EvidenceItem,
    ExtractedPage,
    StoredEvidence,
)


class StaticLLM:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def generate_json(self, **kwargs):
        self.calls.append(kwargs)
        return self.value


class SequenceLLM:
    def __init__(self, *values):
        self.values = list(values)
        self.calls = []

    def generate_json(self, **kwargs):
        self.calls.append(kwargs)
        return self.values.pop(0)


def evidence(evidence_id, source_id):
    return StoredEvidence(
        evidence_id=evidence_id,
        source_id=source_id,
        entity="公司",
        attribute="产品",
        value="支持快充",
        exact_quote="该产品支持快充。",
        evidence_type="product_feature",
        confidence_band="high",
    )


def test_exact_quote_must_exist():
    llm = StaticLLM(
        EvidenceBatch(
            evidence=[
                EvidenceItem(
                    entity="公司",
                    attribute="价格",
                    value="100",
                    exact_quote="正文中不存在",
                    evidence_type="price",
                    confidence_band="high",
                )
            ]
        )
    )
    result = EvidenceExtractor(llm).extract(
        "topic", ExtractedPage(title="x", text="真实正文内容。")
    )
    assert result == []


def test_nonexistent_evidence_id_cannot_generate_claim():
    llm = StaticLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="结论",
                    claim_type="product_comparison",
                    supporting_evidence_ids=["E-999"],
                    confidence_band="high",
                    reasoning_summary="reason",
                )
            ]
        )
    )
    result = ClaimSynthesizer(llm).synthesize([evidence("E-001", "S-001")])
    assert all(item.statement != "结论" for item in result)


def test_single_source_lowers_claim_confidence():
    llm = StaticLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="结论",
                    claim_type="product_comparison",
                    supporting_evidence_ids=["E-001"],
                    confidence_band="high",
                    reasoning_summary="reason",
                )
            ]
        )
    )
    result = ClaimSynthesizer(llm).synthesize([evidence("E-001", "S-001")])
    assert result[0].confidence_band == "medium"


def test_contradicting_evidence_produces_conflicting_claim():
    llm = StaticLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="来源存在冲突",
                    claim_type="market_position",
                    supporting_evidence_ids=["E-001"],
                    contradicting_evidence_ids=["E-002"],
                    confidence_band="high",
                    reasoning_summary="reason",
                )
            ]
        )
    )
    result = ClaimSynthesizer(llm).synthesize(
        [evidence("E-001", "S-001"), evidence("E-002", "S-002")]
    )
    assert result[0].confidence_band == "conflicting"


def test_claim_synthesizer_sends_all_evidence_to_llm():
    llm = StaticLLM(ClaimBatch(claims=[]))
    items = [
        evidence(f"E-{index:03d}", f"S-{(index % 5) + 1:03d}")
        for index in range(100)
    ]

    ClaimSynthesizer(llm).synthesize(items)

    prompt = json.loads(llm.calls[0]["user_prompt"])
    assert len(prompt["evidence"]) == 100
    source_ids = {item["source_id"] for item in prompt["evidence"]}
    assert source_ids == {f"S-{index:03d}" for index in range(1, 6)}


def test_claim_synthesizer_drops_unsupported_numbers():
    llm = StaticLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="英飞源在2024年国内市场销售数量占比超43%。",
                    claim_type="market_position",
                    supporting_evidence_ids=["E-001"],
                    confidence_band="medium",
                    reasoning_summary="reason",
                )
            ]
        )
    )
    item = evidence("E-001", "S-001").model_copy(
        update={
            "value": "超43%",
            "exact_quote": "国内市场销售数量占比超43%",
        }
    )

    result = ClaimSynthesizer(llm).synthesize([item])

    assert all("2024" not in claim.statement for claim in result)


def test_claim_synthesizer_accepts_supported_thousands_separator():
    llm = StaticLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="公司服务超过5000家企业客户。",
                    claim_type="market_position",
                    supporting_evidence_ids=["E-001"],
                    confidence_band="medium",
                    reasoning_summary="reason",
                )
            ]
        )
    )
    item = evidence("E-001", "S-001").model_copy(
        update={
            "value": "超过5,000家企业客户",
            "exact_quote": "公司服务超过 5,000 家企业客户。",
        }
    )

    result = ClaimSynthesizer(llm).synthesize([item])

    assert any("5000" in claim.statement for claim in result)


def test_claim_synthesizer_rewrites_english_claims_to_requested_chinese():
    llm = SequenceLLM(
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="The product supports 4K output.",
                    claim_type="product_comparison",
                    supporting_evidence_ids=["E-001"],
                    confidence_band="medium",
                    reasoning_summary="The source explicitly states this capability.",
                )
            ]
        ),
        ClaimBatch(
            claims=[
                ClaimItem(
                    statement="该产品支持4K输出。",
                    claim_type="product_comparison",
                    supporting_evidence_ids=["E-001"],
                    confidence_band="medium",
                    reasoning_summary="来源明确陈述了该能力。",
                )
            ]
        ),
    )
    item = evidence("E-001", "S-001").model_copy(
        update={
            "value": "Supports 4K output",
            "exact_quote": "The product supports 4K output.",
        }
    )

    result = ClaimSynthesizer(llm).synthesize([item], language="zh")

    assert result[0].statement == "该产品支持4K输出。"
    assert len(llm.calls) == 2
    assert "简体中文" in llm.calls[0]["system_prompt"]
    retry_payload = json.loads(llm.calls[1]["user_prompt"])
    assert retry_payload["evidence"][0]["exact_quote"] == (
        "The product supports 4K output."
    )
