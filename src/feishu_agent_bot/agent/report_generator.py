from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import logging
import re
from typing import Any

from ..llm.base import LLMProvider
from ..llm.schemas import (
    PolishedReport,
    ResearchPlan,
    StoredClaim,
    StoredEvidence,
)

logger = logging.getLogger(__name__)

SECTION_TITLES = {
    "competitor_profile": "竞品或主要参与者",
    "product_comparison": "竞品对比",
    "market_position": "市场定位",
    "business_model": "产品和商业模式差异",
    "opportunity": "机会",
    "risk": "风险",
    "uncertainty": "不确定性和数据边界",
}

SECTION_TITLES_BY_LANGUAGE = {
    "zh": SECTION_TITLES,
    "en": {
        "competitor_profile": "Competitors and Key Participants",
        "product_comparison": "Product Comparison",
        "market_position": "Market Positioning",
        "business_model": "Product and Business Model Differences",
        "opportunity": "Opportunities",
        "risk": "Risks",
        "uncertainty": "Uncertainty and Data Boundaries",
    },
}


class ReportGenerator:
    def __init__(self, llm: LLMProvider | None = None):
        self.llm = llm

    def generate(
        self,
        *,
        topic: str,
        plan: ResearchPlan,
        sources: list[dict[str, Any]],
        evidence: list[StoredEvidence],
        claims: list[StoredClaim],
        language: str = "zh",
    ) -> tuple[str, dict[str, Any]]:
        language = "en" if language == "en" else "zh"
        titles = SECTION_TITLES_BY_LANGUAGE[language]
        evidence_map = {item.evidence_id: item for item in evidence}
        claims_by_id = {item.claim_id: item for item in claims}
        section_claim_ids: dict[str, list[str]] = {
            claim_type: [] for claim_type in titles
        }
        for claim in claims:
            section_claim_ids[claim.claim_type].append(claim.claim_id)

        summary_claims = [
            claim for claim in claims if claim.claim_type != "uncertainty"
        ][:5]
        polish_input = {
            "executive_summary": [claim.claim_id for claim in summary_claims],
            **section_claim_ids,
        }
        conclusion_claim_ids = [
            claim.claim_id
            for claim in claims
            if claim.claim_type != "uncertainty"
        ]
        recommendation_claim_ids = [
            claim.claim_id
            for claim in claims
            if claim.claim_type in {"opportunity", "risk", "uncertainty"}
        ]
        polished = self._polish(
            topic=topic,
            language=language,
            section_claim_ids=polish_input,
            conclusion_claim_ids=conclusion_claim_ids,
            recommendation_claim_ids=recommendation_claim_ids,
            claims_by_id=claims_by_id,
            evidence_map=evidence_map,
        )
        summary_paragraphs = self._section_paragraphs(
            "executive_summary",
            polish_input["executive_summary"],
            polished,
            claims_by_id,
        )

        labels = self._labels(language)
        lines = [
            f"# {topic}{labels['report_suffix']}",
            "",
            f"## {labels['objective']}",
            plan.objective,
            "",
            f"## {labels['executive_summary']}",
            *self._render_paragraphs(
                summary_paragraphs, claims_by_id, evidence_map
            ),
            "",
            f"## {labels['methodology']}",
            labels["methodology_text"].format(
                sources=len(sources), evidence=len(evidence), claims=len(claims)
            ),
        ]
        report_sections = []
        for claim_type, title in titles.items():
            paragraphs = self._section_paragraphs(
                claim_type,
                section_claim_ids[claim_type],
                polished,
                claims_by_id,
            )
            lines.extend(
                [
                    "",
                    f"## {title}",
                    *self._render_paragraphs(
                        paragraphs, claims_by_id, evidence_map
                    ),
                ]
            )
            report_sections.append(
                {
                    "section_id": claim_type,
                    "title": title,
                    "body": "\n".join(
                        paragraph["text"] for paragraph in paragraphs
                    ),
                    "claim_ids": list(
                        dict.fromkeys(
                            claim_id
                            for paragraph in paragraphs
                            for claim_id in paragraph["claim_ids"]
                        )
                    ),
                }
            )

        conclusion_paragraphs = self._section_paragraphs(
            "final_conclusion",
            conclusion_claim_ids[:8],
            polished,
            claims_by_id,
        )
        recommendation_paragraphs = self._section_paragraphs(
            "recommendations",
            recommendation_claim_ids,
            polished,
            claims_by_id,
        )
        report_sections.extend(
            [
                self._report_section(
                    "final_conclusion",
                    labels["conclusion"],
                    conclusion_paragraphs,
                ),
                self._report_section(
                    "recommendations",
                    labels["recommendations"],
                    recommendation_paragraphs,
                ),
            ]
        )
        lines.extend(["", f"## {labels['conclusion']}"])
        lines.extend(
            self._render_paragraphs(
                conclusion_paragraphs, claims_by_id, evidence_map
            )
        )
        lines.extend(["", f"## {labels['recommendations']}"])
        lines.extend(
            self._render_paragraphs(
                recommendation_paragraphs, claims_by_id, evidence_map
            )
        )
        if polished:
            lines.extend(["", f"## {labels['claim_audit']}"])
            lines.extend(
                self._render_paragraphs(
                    [
                        {"text": claim.statement, "claim_ids": [claim.claim_id]}
                        for claim in claims
                    ],
                    claims_by_id,
                    evidence_map,
                )
            )
        lines.extend(["", f"## {labels['sources']}"])
        for source in sources:
            lines.extend(
                [
                    f"### {source['source_id']} {source['title']}",
                    source["url"],
                    f"{labels['retrieved_at']}：{source['retrieved_at']}",
                    "",
                ]
            )
        markdown = "\n".join(lines).strip() + "\n"
        report = {
            "topic": topic,
            "language": language,
            "objective": plan.objective,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "plan": plan.model_dump(),
            "sources": sources,
            "evidence": [item.model_dump() for item in evidence],
            "claims": [item.model_dump() for item in claims],
            "sections": report_sections,
            "summary": [
                {"text": paragraph["text"], "claim_ids": paragraph["claim_ids"]}
                for paragraph in summary_paragraphs
            ],
            "polished_by_ai": bool(polished),
        }
        return markdown, report

    def _polish(
        self,
        *,
        topic: str,
        language: str,
        section_claim_ids: dict[str, list[str]],
        conclusion_claim_ids: list[str],
        recommendation_claim_ids: list[str],
        claims_by_id: dict[str, StoredClaim],
        evidence_map: dict[str, StoredEvidence],
    ) -> dict[str, list[dict[str, object]]]:
        if self.llm is None:
            return {}
        target_language = "简体中文" if language == "zh" else "English"
        payload_sections = []
        for section_id, claim_ids in section_claim_ids.items():
            payload_sections.append(
                {
                    "section_id": section_id,
                    "claims": [
                        {
                            "claim_id": claim_id,
                            "statement": claims_by_id[claim_id].statement,
                            "reasoning_summary": claims_by_id[
                                claim_id
                            ].reasoning_summary,
                            "confidence_band": claims_by_id[
                                claim_id
                            ].confidence_band,
                            "supporting_evidence_ids": claims_by_id[
                                claim_id
                            ].supporting_evidence_ids,
                            "contradicting_evidence_ids": claims_by_id[
                                claim_id
                            ].contradicting_evidence_ids,
                        }
                        for claim_id in claim_ids
                        if claim_id in claims_by_id
                    ],
                }
            )
        try:
            result = self.llm.generate_json(
                system_prompt=(
                    f"你是研究报告终审编辑。使用{target_language}完成全文定稿。"
                    "只能改写和归纳输入中的 claims，不得添加新证据、新事实、"
                    "新实体、新数字、外部知识或推测。每段必须列出实际使用的"
                    " claim_ids；只能使用该 section 提供的 claim_id。"
                    "没有 claim 的 section 必须返回空 paragraphs。"
                    "从全文角度理清论证顺序，各章节开头在必要时加入自然衔接。"
                    "执行摘要概括核心发现；final_conclusion 给出综合结论；"
                    "recommendations 只能由给定机会、风险和不确定性直接推导"
                    "可执行建议，且建议不得伪装成已发生事实。"
                    "保持不确定性、冲突和置信度边界，不得把企业陈述改成客观事实。"
                    "只输出符合 schema 的 JSON。"
                ),
                user_prompt=json.dumps(
                    {
                        "topic": topic,
                        "target_language": target_language,
                        "sections": payload_sections,
                        "final_conclusion_claim_ids": conclusion_claim_ids,
                        "recommendation_claim_ids": recommendation_claim_ids,
                        "schema": PolishedReport.model_json_schema(),
                    },
                    ensure_ascii=False,
                ),
                response_model=PolishedReport,
            )
        except Exception as exc:
            logger.warning("报告 AI 润色失败，使用确定性草稿 error=%s", exc)
            return {}

        accepted = self._accept_polished_result(
            result,
            language=language,
            section_claim_ids=section_claim_ids,
            conclusion_claim_ids=conclusion_claim_ids,
            recommendation_claim_ids=recommendation_claim_ids,
            claims_by_id=claims_by_id,
            evidence_map=evidence_map,
        )
        missing_sections = [
            section_id
            for section_id, claim_ids in section_claim_ids.items()
            if claim_ids and section_id not in accepted
        ]
        missing_special = [
            section_id
            for section_id, claim_ids in (
                ("final_conclusion", conclusion_claim_ids),
                ("recommendations", recommendation_claim_ids),
            )
            if claim_ids and section_id not in accepted
        ]
        if not missing_sections and not missing_special:
            return accepted
        try:
            retry_result = self.llm.generate_json(
                system_prompt=(
                    f"你是研究报告终审编辑。必须使用{target_language}重写指定章节，"
                    "不能保留整段其他语言原文。只能改写输入 claims，不得添加新证据、"
                    "新事实、新实体、新数字、外部知识或推测。每段必须列出实际使用的"
                    " claim_ids；普通章节必须覆盖该章节提供的全部 claim_id。"
                    "最终结论只能综合已有结论，建议只能从机会、风险和不确定性直接推导。"
                    "只输出符合 schema 的 JSON。"
                ),
                user_prompt=json.dumps(
                    {
                        "topic": topic,
                        "target_language": target_language,
                        "retry_sections": missing_sections,
                        "retry_special_sections": missing_special,
                        "sections": [
                            item
                            for item in payload_sections
                            if item["section_id"] in missing_sections
                        ],
                        "final_conclusion_claim_ids": (
                            conclusion_claim_ids
                            if "final_conclusion" in missing_special
                            else []
                        ),
                        "recommendation_claim_ids": (
                            recommendation_claim_ids
                            if "recommendations" in missing_special
                            else []
                        ),
                        "schema": PolishedReport.model_json_schema(),
                    },
                    ensure_ascii=False,
                ),
                response_model=PolishedReport,
            )
        except Exception as exc:
            logger.warning("报告缺失章节补偿终审失败 error=%s", exc)
            retry_result = PolishedReport(sections=[])
        retry_accepted = self._accept_polished_result(
            retry_result,
            language=language,
            section_claim_ids={
                key: value
                for key, value in section_claim_ids.items()
                if key in missing_sections
            },
            conclusion_claim_ids=(
                conclusion_claim_ids
                if "final_conclusion" in missing_special
                else []
            ),
            recommendation_claim_ids=(
                recommendation_claim_ids
                if "recommendations" in missing_special
                else []
            ),
            claims_by_id=claims_by_id,
            evidence_map=evidence_map,
        )
        accepted.update(retry_accepted)
        required_language_sections = [
            section_id
            for section_id, claim_ids in section_claim_ids.items()
            if claim_ids
            and any(
                not self._language_matches(
                    claims_by_id[claim_id].statement, language
                )
                for claim_id in claim_ids
                if claim_id in claims_by_id
            )
            and section_id not in accepted
        ]
        for section_id in required_language_sections:
            section_payload = next(
                item
                for item in payload_sections
                if item["section_id"] == section_id
            )
            single_result = self._retry_single_section(
                topic=topic,
                target_language=target_language,
                section_id=section_id,
                section_payload=section_payload,
            )
            accepted.update(
                self._accept_polished_result(
                    single_result,
                    language=language,
                    section_claim_ids={
                        section_id: section_claim_ids[section_id]
                    },
                    conclusion_claim_ids=[],
                    recommendation_claim_ids=[],
                    claims_by_id=claims_by_id,
                    evidence_map=evidence_map,
                )
            )

        for section_id, claim_ids in (
            ("final_conclusion", conclusion_claim_ids),
            ("recommendations", recommendation_claim_ids),
        ):
            if (
                not claim_ids
                or section_id in accepted
                or all(
                    self._language_matches(
                        claims_by_id[claim_id].statement, language
                    )
                    for claim_id in claim_ids
                    if claim_id in claims_by_id
                )
            ):
                continue
            single_result = self._retry_single_section(
                topic=topic,
                target_language=target_language,
                section_id=section_id,
                section_payload={
                    "section_id": section_id,
                    "claims": [
                        item
                        for section in payload_sections
                        for item in section["claims"]
                        if item["claim_id"] in claim_ids
                    ],
                },
            )
            accepted.update(
                self._accept_polished_result(
                    single_result,
                    language=language,
                    section_claim_ids={},
                    conclusion_claim_ids=(
                        claim_ids if section_id == "final_conclusion" else []
                    ),
                    recommendation_claim_ids=(
                        claim_ids if section_id == "recommendations" else []
                    ),
                    claims_by_id=claims_by_id,
                    evidence_map=evidence_map,
                )
            )

        unresolved = [
            section_id
            for section_id in required_language_sections
            if section_id not in accepted
        ]
        unresolved.extend(
            section_id
            for section_id, claim_ids in (
                ("final_conclusion", conclusion_claim_ids),
                ("recommendations", recommendation_claim_ids),
            )
            if claim_ids
            and section_id not in accepted
            and any(
                not self._language_matches(
                    claims_by_id[claim_id].statement, language
                )
                for claim_id in claim_ids
                if claim_id in claims_by_id
            )
        )
        if unresolved:
            raise ValueError(
                f"报告终审未能生成目标语言章节: {', '.join(unresolved)}"
            )
        return accepted

    def _retry_single_section(
        self,
        *,
        topic: str,
        target_language: str,
        section_id: str,
        section_payload: dict[str, object],
    ) -> PolishedReport:
        return self.llm.generate_json(
            system_prompt=(
                f"你是研究报告终审编辑。只处理 {section_id}，必须使用"
                f"{target_language}完整改写输入中的每条 claim。保留所有数字、实体、"
                "不确定性和限定条件，不得添加任何新事实、证据、数字、实体或推测。"
                "普通章节放入 sections；final_conclusion 放入 final_conclusion；"
                "recommendations 放入 recommendations。每段列出实际使用的 claim_ids，"
                "并覆盖输入提供的全部 claim_id。只输出符合 schema 的 JSON。"
            ),
            user_prompt=json.dumps(
                {
                    "topic": topic,
                    "target_language": target_language,
                    "section": section_payload,
                    "schema": PolishedReport.model_json_schema(),
                },
                ensure_ascii=False,
            ),
            response_model=PolishedReport,
        )

    @classmethod
    def _accept_polished_result(
        cls,
        result: PolishedReport,
        *,
        language: str,
        section_claim_ids: dict[str, list[str]],
        conclusion_claim_ids: list[str],
        recommendation_claim_ids: list[str],
        claims_by_id: dict[str, StoredClaim],
        evidence_map: dict[str, StoredEvidence],
    ) -> dict[str, list[dict[str, object]]]:
        accepted: dict[str, list[dict[str, object]]] = {}
        for section in result.sections:
            allowed = set(section_claim_ids.get(section.section_id, []))
            if not allowed:
                continue
            paragraphs = []
            for paragraph in section.paragraphs:
                claim_ids = list(dict.fromkeys(paragraph.claim_ids))
                if not claim_ids or any(item not in allowed for item in claim_ids):
                    continue
                if not cls._numbers_supported(
                    paragraph.text,
                    claim_ids,
                    claims_by_id,
                    evidence_map,
                ) or not cls._language_matches(paragraph.text, language):
                    continue
                paragraphs.append(
                    {"text": paragraph.text.strip(), "claim_ids": claim_ids}
                )
            covered_claim_ids = {
                str(claim_id)
                for paragraph in paragraphs
                for claim_id in paragraph["claim_ids"]
            }
            if paragraphs and covered_claim_ids == allowed:
                accepted[section.section_id] = paragraphs
        conclusion = cls._accepted_paragraphs(
            result.final_conclusion,
            set(conclusion_claim_ids),
            claims_by_id,
            evidence_map,
            language,
        )
        if conclusion:
            accepted["final_conclusion"] = conclusion
        recommendations = cls._accepted_paragraphs(
            result.recommendations,
            set(recommendation_claim_ids),
            claims_by_id,
            evidence_map,
            language,
        )
        if recommendations:
            accepted["recommendations"] = recommendations
        return accepted

    @classmethod
    def _accepted_paragraphs(
        cls,
        paragraphs,
        allowed: set[str],
        claims_by_id: dict[str, StoredClaim],
        evidence_map: dict[str, StoredEvidence],
        language: str,
    ) -> list[dict[str, object]]:
        accepted = []
        for paragraph in paragraphs:
            claim_ids = list(dict.fromkeys(paragraph.claim_ids))
            if not claim_ids or any(item not in allowed for item in claim_ids):
                continue
            if not cls._numbers_supported(
                paragraph.text,
                claim_ids,
                claims_by_id,
                evidence_map,
            ) or not cls._language_matches(paragraph.text, language):
                continue
            accepted.append(
                {"text": paragraph.text.strip(), "claim_ids": claim_ids}
            )
        return accepted

    @staticmethod
    def _language_matches(text: str, language: str) -> bool:
        cjk_count = len(re.findall(r"[\u3400-\u9fff]", text))
        latin_count = len(re.findall(r"[A-Za-z]", text))
        if language == "zh":
            return cjk_count >= 6 and cjk_count >= latin_count * 0.05
        return latin_count >= 6 and latin_count >= cjk_count

    @staticmethod
    def _report_section(
        section_id: str,
        title: str,
        paragraphs: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "section_id": section_id,
            "title": title,
            "body": "\n".join(str(item["text"]) for item in paragraphs),
            "claim_ids": list(
                dict.fromkeys(
                    str(claim_id)
                    for paragraph in paragraphs
                    for claim_id in paragraph["claim_ids"]
                )
            ),
        }

    @staticmethod
    def _numbers_supported(
        text: str,
        claim_ids: list[str],
        claims_by_id: dict[str, StoredClaim],
        evidence_map: dict[str, StoredEvidence],
    ) -> bool:
        numbers = set(re.findall(r"\d+(?:[.,，]\d+)*%?", text))
        if not numbers:
            return True
        source_parts = []
        for claim_id in claim_ids:
            claim = claims_by_id[claim_id]
            source_parts.append(claim.statement)
            for evidence_id in (
                claim.supporting_evidence_ids + claim.contradicting_evidence_ids
            ):
                evidence = evidence_map.get(evidence_id)
                if evidence:
                    source_parts.extend([evidence.value, evidence.exact_quote])
        source_text = re.sub(r"(?<=\d)[,，](?=\d)", "", " ".join(source_parts))
        return ReportGenerator._text_numbers_supported(text, source_text)

    @staticmethod
    def _text_numbers_supported(text: str, source_text: str) -> bool:
        source_text = re.sub(r"(?<=\d)[,，](?=\d)", "", source_text)
        source_quantities = ReportGenerator._normalized_quantities(source_text)
        month_names = {
            1: "january",
            2: "february",
            3: "march",
            4: "april",
            5: "may",
            6: "june",
            7: "july",
            8: "august",
            9: "september",
            10: "october",
            11: "november",
            12: "december",
        }
        for match in re.finditer(r"\d+(?:[.,，]\d+)*%?", text):
            number = re.sub(r"(?<=\d)[,，](?=\d)", "", match.group(0))
            if number in source_text:
                continue
            quantity = ReportGenerator._normalized_quantity_at(text, match)
            if quantity is not None and quantity in source_quantities:
                continue
            if (
                re.match(r"\s*月", text[match.end() :])
                and number.isdigit()
                and month_names.get(int(number), "") in source_text.lower()
            ):
                continue
            return False
        return True

    @staticmethod
    def _normalized_quantities(text: str) -> set[Decimal]:
        quantities: set[Decimal] = set()
        pattern = re.compile(
            r"(?P<number>\d+(?:\.\d+)?)\s*"
            r"(?P<unit>billion\b|million\b|[BMK]\b|亿|万|千)",
            re.IGNORECASE,
        )
        for match in pattern.finditer(
            re.sub(r"(?<=\d)[,，](?=\d)", "", text)
        ):
            quantity = ReportGenerator._quantity(
                match.group("number"), match.group("unit")
            )
            if quantity is not None:
                quantities.add(quantity)
        return quantities

    @staticmethod
    def _normalized_quantity_at(text: str, match: re.Match[str]) -> Decimal | None:
        suffix = text[match.end() : match.end() + 12]
        unit_match = re.match(
            r"\s*(billion\b|million\b|[BMK]\b|亿|万|千)",
            suffix,
            re.IGNORECASE,
        )
        if not unit_match:
            return None
        return ReportGenerator._quantity(
            re.sub(r"(?<=\d)[,，](?=\d)", "", match.group(0).rstrip("%")),
            unit_match.group(1),
        )

    @staticmethod
    def _quantity(number: str, unit: str) -> Decimal | None:
        multipliers = {
            "billion": Decimal("1000000000"),
            "million": Decimal("1000000"),
            "b": Decimal("1000000000"),
            "m": Decimal("1000000"),
            "k": Decimal("1000"),
            "亿": Decimal("100000000"),
            "万": Decimal("10000"),
            "千": Decimal("1000"),
        }
        try:
            return Decimal(number) * multipliers[unit.lower()]
        except (InvalidOperation, KeyError):
            return None

    @staticmethod
    def _section_paragraphs(
        section_id: str,
        claim_ids: list[str],
        polished: dict[str, list[dict[str, object]]],
        claims_by_id: dict[str, StoredClaim],
    ) -> list[dict[str, object]]:
        if section_id in polished:
            return polished[section_id]
        paragraphs = [
            {"text": claims_by_id[claim_id].statement, "claim_ids": [claim_id]}
            for claim_id in claim_ids
            if claim_id in claims_by_id
        ]
        if paragraphs:
            return paragraphs
        return [{"text": "暂无充分证据。", "claim_ids": []}]

    @staticmethod
    def _render_paragraphs(
        paragraphs: list[dict[str, object]],
        claims_by_id: dict[str, StoredClaim],
        evidence_map: dict[str, StoredEvidence],
    ) -> list[str]:
        rendered = []
        for paragraph in paragraphs:
            references = []
            for claim_id in paragraph["claim_ids"]:
                claim = claims_by_id[str(claim_id)]
                for evidence_id in claim.supporting_evidence_ids:
                    evidence = evidence_map.get(evidence_id)
                    if evidence:
                        references.append(
                            f"[证据 {evidence_id}，来源 {evidence.source_id}]"
                        )
                for evidence_id in claim.contradicting_evidence_ids:
                    evidence = evidence_map.get(evidence_id)
                    if evidence:
                        references.append(
                            f"[反证 {evidence_id}，来源 {evidence.source_id}]"
                        )
            suffix = " " + " ".join(dict.fromkeys(references)) if references else ""
            rendered.append(f"- {paragraph['text']}{suffix}")
        return rendered

    @staticmethod
    def _labels(language: str) -> dict[str, str]:
        if language == "en":
            return {
                "report_suffix": " Competitive Research Report",
                "objective": "Research Objective",
                "executive_summary": "Executive Summary",
                "methodology": "Methodology",
                "methodology_text": (
                    "Based on {sources} fetched public sources, {evidence} traceable "
                    "evidence items were extracted and used to form {claims} "
                    "structured conclusions."
                ),
                "conclusion": "Conclusion",
                "recommendations": "Recommendations",
                "claim_audit": "Structured Claim Audit",
                "sources": "Sources",
                "retrieved_at": "Retrieved at",
            }
        return {
            "report_suffix": "竞品调研报告",
            "objective": "研究目标",
            "executive_summary": "执行摘要",
            "methodology": "研究方法",
            "methodology_text": (
                "基于 {sources} 个已抓取公开来源，提取 {evidence} 条"
                "可回溯原文证据，并据此形成 {claims} 条结构化结论。"
            ),
            "conclusion": "结论",
            "recommendations": "建议",
            "claim_audit": "结构化结论审计",
            "sources": "来源清单",
            "retrieved_at": "抓取时间",
        }
