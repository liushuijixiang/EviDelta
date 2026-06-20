from __future__ import annotations

import json
from datetime import datetime, timezone

from ..llm.base import LLMProvider
from ..llm.schemas import ResearchPlan


class ResearchPlanner:
    def __init__(self, llm: LLMProvider, max_search_queries: int):
        self.llm = llm
        self.max_search_queries = max_search_queries

    def create_plan(self, topic: str) -> ResearchPlan:
        plan = self.llm.generate_json(
            system_prompt=(
                "你是严谨的竞品调研规划师。只输出符合给定 schema 的 JSON。"
                "搜索查询必须覆盖官方资料、产品信息、商业模式、用户评价和近期动态。"
                f"最多输出 {self.max_search_queries} 条搜索查询，每条不超过 100 字。"
                "其他列表保持精炼，每项一句话。"
            ),
            user_prompt=json.dumps(
                {
                    "topic": topic,
                    "current_time_utc": datetime.now(timezone.utc).isoformat(),
                    "search_budget": self.max_search_queries,
                    "report_goal": "生成有来源、有原文证据引用的竞品调研报告",
                    "schema": ResearchPlan.model_json_schema(),
                },
                ensure_ascii=False,
            ),
            response_model=ResearchPlan,
        )
        queries = [
            query[:160].strip()
            for query in plan.search_queries[: self.max_search_queries]
            if query.strip()
        ]
        queries = self._augment_data_queries(topic, queries)
        return plan.model_copy(update={"search_queries": queries})

    def _augment_data_queries(self, topic: str, queries: list[str]) -> list[str]:
        augmented = list(dict.fromkeys(queries))
        if len(augmented) >= self.max_search_queries:
            return augmented[: self.max_search_queries]
        base = topic.strip()[:80]
        candidates = [
            f"{base} filetype:pdf 行业报告 白皮书",
            f"{base} filetype:xlsx 价格表 统计数据",
            f"{base} filetype:csv 统计数据",
            f"{base} 年报 财报 市场数据",
        ]
        for candidate in candidates:
            if len(augmented) >= self.max_search_queries:
                break
            if candidate not in augmented:
                augmented.append(candidate)
        return augmented
