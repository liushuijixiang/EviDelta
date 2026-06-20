import pytest
from pydantic import ValidationError

from feishu_agent_bot.llm.schemas import ResearchPlan


def test_research_plan_schema_rejects_empty_queries():
    with pytest.raises(ValidationError):
        ResearchPlan(
            objective="研究目标",
            research_questions=["谁是竞品"],
            search_queries=["", "  "],
            comparison_dimensions=["产品"],
            expected_entities=[],
            acceptance_criteria=["有来源"],
        )


def test_research_plan_schema_deduplicates_queries():
    plan = ResearchPlan(
        objective="研究目标",
        research_questions=["谁是竞品"],
        search_queries=["竞品 官网", "竞品 官网"],
        comparison_dimensions=["产品"],
        expected_entities=[],
        acceptance_criteria=["有来源"],
    )
    assert plan.search_queries == ["竞品 官网"]
