from __future__ import annotations

import json

from ..llm.base import LLMProvider
from ..llm.schemas import EvidenceBatch, EvidenceItem, ExtractedPage


class EvidenceExtractor:
    def __init__(self, llm: LLMProvider):
        self.llm = llm

    def extract(self, topic: str, page: ExtractedPage) -> list[EvidenceItem]:
        batch = self.llm.generate_json(
            system_prompt=(
                "只从给定正文提取证据。exact_quote 必须逐字存在于正文。"
                "区分事实、企业宣传和用户观点；不得把排名、评论数或热度当作销量。"
                "资料不足时返回较少证据，禁止补写。最多提取 12 条证据。"
            ),
            user_prompt=json.dumps(
                {
                    "topic": topic,
                    "source_title": page.title,
                    "source_text": page.text[:12_000],
                    "schema": EvidenceBatch.model_json_schema(),
                },
                ensure_ascii=False,
            ),
            response_model=EvidenceBatch,
        )
        accepted: list[EvidenceItem] = []
        seen: set[tuple[str, str, str]] = set()
        for item in batch.evidence:
            if not item.exact_quote or item.exact_quote not in page.text:
                continue
            key = (item.entity, item.attribute, item.exact_quote)
            if key in seen:
                continue
            seen.add(key)
            accepted.append(item)
        return accepted
