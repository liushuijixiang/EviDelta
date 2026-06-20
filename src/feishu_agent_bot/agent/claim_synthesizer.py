from __future__ import annotations

import json
import re

from ..llm.base import LLMProvider
from ..llm.schemas import ClaimBatch, ClaimItem, StoredEvidence


class ClaimSynthesizer:
    def __init__(
        self, llm: LLMProvider, max_claims: int = 20
    ):
        self.llm = llm
        self.max_claims = max_claims

    def synthesize(
        self, evidence: list[StoredEvidence], language: str = "zh"
    ) -> list[ClaimItem]:
        language = "en" if language == "en" else "zh"
        target_language = "English" if language == "en" else "简体中文"
        selected_evidence = evidence
        valid_ids = {item.evidence_id for item in selected_evidence}
        evidence_by_id = {item.evidence_id: item for item in selected_evidence}
        source_by_evidence = {
            item.evidence_id: item.source_id for item in selected_evidence
        }
        batch = self.llm.generate_json(
            system_prompt=(
                "只能基于给定 evidence 生成结论。每个事实性结论至少引用一条证据；"
                "不得加入 evidence value 或 exact_quote 中不存在的具体数字、年份、比例；"
                "冲突不得强行合并；必须包含 uncertainty 结论说明数据缺口；"
                f"statement 和 reasoning_summary 必须使用{target_language}；"
                "exact_quote 即使是其他语言也只能作为原文证据，不得改写其内容；"
                f"最多输出 {self.max_claims} 条 claims。"
            ),
            user_prompt=json.dumps(
                {
                    "evidence": [
                        item.model_dump() for item in selected_evidence
                    ],
                    "schema": ClaimBatch.model_json_schema(),
                },
                ensure_ascii=False,
            ),
            response_model=ClaimBatch,
        )
        accepted = self._accept_claims(
            batch,
            valid_ids=valid_ids,
            evidence_by_id=evidence_by_id,
            source_by_evidence=source_by_evidence,
        )
        had_factual_claims = any(
            item.claim_type != "uncertainty" for item in accepted
        )
        if accepted and any(
            not self._language_matches(item.statement, language)
            for item in accepted
        ):
            retry_batch = self.llm.generate_json(
                system_prompt=(
                    f"将给定 claims 严格重写为{target_language}。"
                    "只能翻译或改写原 claim，不得新增、删除或修改事实、数字、实体、"
                    "证据 ID、claim_type、冲突关系和置信度。"
                    "输入 evidence 的 exact_quote 必须保持原文，不需要翻译或输出。"
                    "只输出符合 schema 的 JSON。"
                ),
                user_prompt=json.dumps(
                    {
                        "claims": [item.model_dump() for item in accepted],
                        "evidence": [
                            {
                                "evidence_id": item.evidence_id,
                                "value": item.value,
                                "exact_quote": item.exact_quote,
                            }
                            for item in selected_evidence
                        ],
                        "target_language": target_language,
                        "schema": ClaimBatch.model_json_schema(),
                    },
                    ensure_ascii=False,
                ),
                response_model=ClaimBatch,
            )
            accepted = self._accept_claims(
                retry_batch,
                valid_ids=valid_ids,
                evidence_by_id=evidence_by_id,
                source_by_evidence=source_by_evidence,
            )
        accepted = [
            item
            for item in accepted
            if self._language_matches(item.statement, language)
        ]
        if had_factual_claims and not any(
            item.claim_type != "uncertainty" for item in accepted
        ):
            raise ValueError(
                "claim synthesis could not produce factual claims in target language"
            )
        if not any(claim.claim_type == "uncertainty" for claim in accepted):
            if language == "en":
                uncertainty = ClaimItem(
                    statement=(
                        "Public information coverage is limited; some competitor "
                        "operating data and user feedback require further verification."
                    ),
                    claim_type="uncertainty",
                    confidence_band="low",
                    reasoning_summary="This claim describes the current data boundary.",
                )
            else:
                uncertainty = ClaimItem(
                    statement="公开资料覆盖有限，部分竞品经营数据和用户反馈仍需进一步核验。",
                    claim_type="uncertainty",
                    confidence_band="low",
                    reasoning_summary="该结论描述当前研究的数据边界。",
                )
            accepted.append(uncertainty)
        return accepted

    def _accept_claims(
        self,
        batch: ClaimBatch,
        *,
        valid_ids: set[str],
        evidence_by_id: dict[str, StoredEvidence],
        source_by_evidence: dict[str, str],
    ) -> list[ClaimItem]:
        accepted: list[ClaimItem] = []
        for claim in batch.claims[: self.max_claims]:
            support = [
                evidence_id
                for evidence_id in claim.supporting_evidence_ids
                if evidence_id in valid_ids
            ]
            contradict = [
                evidence_id
                for evidence_id in claim.contradicting_evidence_ids
                if evidence_id in valid_ids
            ]
            if claim.claim_type != "uncertainty" and not support:
                continue
            if support and not self._numbers_supported(
                claim.statement,
                [evidence_by_id[item] for item in support + contradict],
            ):
                continue
            confidence = claim.confidence_band
            independent_sources = {
                source_by_evidence[evidence_id] for evidence_id in support
            }
            if contradict:
                confidence = "conflicting"
            elif len(independent_sources) < 2 and confidence == "high":
                confidence = "medium"
            accepted.append(
                claim.model_copy(
                    update={
                        "supporting_evidence_ids": support,
                        "contradicting_evidence_ids": contradict,
                        "confidence_band": confidence,
                    }
                )
            )
        return accepted

    @staticmethod
    def _language_matches(text: str, language: str) -> bool:
        cjk_count = len(re.findall(r"[\u3400-\u9fff]", text))
        latin_count = len(re.findall(r"[A-Za-z]", text))
        if language == "zh":
            return cjk_count >= 2 and cjk_count >= latin_count * 0.05
        return latin_count >= 4 and latin_count >= cjk_count

    def _numbers_supported(
        self, statement: str, evidence: list[StoredEvidence]
    ) -> bool:
        numbers = set(re.findall(r"\d+(?:\.\d+)?%?", statement))
        if not numbers:
            return True
        referenced_text = " ".join(
            f"{item.value} {item.exact_quote}" for item in evidence
        )
        normalized_referenced_text = re.sub(
            r"(?<=\d)[,，](?=\d)", "", referenced_text
        )
        return all(number in normalized_referenced_text for number in numbers)
