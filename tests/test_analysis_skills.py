import threading
import time

import pytest

from feishu_agent_bot.analysis import AnalysisExecutor
from feishu_agent_bot.analysis.schemas import AnalysisResult
from feishu_agent_bot.analysis.skills import (
    AnalysisContext,
    BusinessModelSkill,
    CompetitorBenchmarkSkill,
    PricingAndPackagingSkill,
    SkillApplicability,
)
from feishu_agent_bot.datasets import DatasetProfiler, TabularDataset


def context_for(dataset: TabularDataset, topic: str = "竞品价格趋势"):
    profile = DatasetProfiler().profile(dataset)
    return AnalysisContext(
        job_id=dataset.job_id,
        run_id="RUN1",
        topic=topic,
        datasets=[dataset],
        profiles=[profile],
    )


def test_competitor_skill_builds_matrix_without_filling_missing_values():
    dataset = TabularDataset(
        dataset_id="D1",
        job_id="J1",
        asset_id="A1",
        table_id="T1",
        name="competitor table",
        columns=["company", "price", "feature"],
        rows=[
            {"company": "A", "price": "99", "feature": "fast"},
            {"company": "B", "price": "", "feature": "slow"},
        ],
    )
    skill = CompetitorBenchmarkSkill()

    assert skill.is_applicable(context_for(dataset)).applicable is True
    result = skill.execute(context_for(dataset))

    assert result.skill_name == "competitor_benchmark"
    assert result.input_dataset_ids == ["D1"]
    assert result.metrics["completeness"] == 0.75
    assert result.tables[0]["missing_fields"]["B"] == ["price"]
    assert result.limitations == ["竞品矩阵字段不完整"]


def test_pricing_skill_preserves_currency_and_generates_chart_points():
    dataset = TabularDataset(
        dataset_id="D2",
        job_id="J1",
        asset_id="A1",
        table_id="T2",
        name="pricing table",
        columns=["company", "price"],
        rows=[
            {"company": "A", "price": "¥99/月"},
            {"company": "B", "price": "$20/mo"},
        ],
    )
    result = PricingAndPackagingSkill().execute(context_for(dataset))

    assert result.skill_name == "pricing_and_packaging"
    assert result.metrics["parsed_price_count"] == 2
    assert result.tables[0]["rows"][0]["currency"] == "CNY"
    assert result.tables[0]["rows"][1]["currency"] == "USD"
    assert result.charts[0]["points"] == [{"x": "A", "y": 99.0}, {"x": "B", "y": 20.0}]
    assert "未提供明确汇率来源" in result.limitations[0]


def test_business_model_skill_reports_insufficient_inputs():
    dataset = TabularDataset(
        dataset_id="D3",
        job_id="J1",
        asset_id="A1",
        table_id="T3",
        name="business model",
        columns=["revenue"],
        rows=[{"revenue": "100"}],
    )

    result = BusinessModelSkill().execute(context_for(dataset, "商业模式"))

    assert result.skill_name == "business_model"
    assert result.metrics["status"] == "insufficient_data"
    assert "customers" in result.metrics["missing_inputs"]
    assert result.confidence_band == "low"


def test_analysis_executor_runs_selected_skills_and_records_limitations():
    dataset = TabularDataset(
        dataset_id="D4",
        job_id="J1",
        asset_id="A1",
        table_id="T4",
        name="competitor pricing market business",
        columns=["company", "price", "revenue"],
        rows=[{"company": "A", "price": "99", "revenue": "100"}],
    )
    profile = DatasetProfiler().profile(dataset)

    run, results = AnalysisExecutor().run(
        job_id="J1",
        topic="竞品价格市场商业模式趋势",
        datasets=[dataset],
        profiles=[profile],
    )

    skill_names = {result.skill_name for result in results if result.skill_name}
    assert {
        "competitor_benchmark",
        "pricing_and_packaging",
        "market_positioning",
        "business_model",
        "trend_and_change",
    }.issubset(skill_names)
    assert "business_model" in run.selected_skills
    assert run.analysis_plan is not None
    assert "竞品矩阵" in run.analysis_plan.expected_outputs
    assert "D4" in run.analysis_plan.required_dataset_ids
    assert any(result.confidence_band == "low" for result in results)


def test_analysis_plan_records_skipped_skills_for_unusable_dataset():
    dataset = TabularDataset(
        dataset_id="D5",
        job_id="J1",
        asset_id="A1",
        table_id="T5",
        name="pricing",
        columns=["company", "price"],
        rows=[],
    )
    profile = DatasetProfiler().profile(dataset)

    run, _ = AnalysisExecutor().run(
        job_id="J1",
        topic="竞品价格趋势",
        datasets=[dataset],
        profiles=[profile],
    )

    assert run.analysis_plan is not None
    assert "pricing_and_packaging" not in run.analysis_plan.selected_skills
    skipped = {
        item.skill_name: item for item in run.analysis_plan.skipped_skills
    }
    assert skipped["pricing_and_packaging"].reason == "所有数据集质量不足，无法执行该分析"
    assert skipped["pricing_and_packaging"].missing_inputs == ["price_column"]


def test_analysis_executor_honors_comma_separated_include_and_exclude():
    dataset = TabularDataset(
        dataset_id="D6",
        job_id="J1",
        asset_id="A1",
        table_id="T6",
        name="competitor pricing market",
        columns=["company", "price", "market_share"],
        rows=[{"company": "A", "price": "99", "market_share": "10"}],
    )
    profile = DatasetProfiler().profile(dataset)

    run, _ = AnalysisExecutor().run(
        job_id="J1",
        topic="竞品价格市场",
        datasets=[dataset],
        profiles=[profile],
        include=["pricing,market_position"],
        exclude=["competitor"],
    )

    assert "pricing_and_packaging" in run.selected_skills
    assert "market_positioning" in run.selected_skills
    assert "competitor_benchmark" not in run.selected_skills
    assert "competitor_matrix" not in run.selected_tools
    assert run.analysis_plan is not None
    skipped = {item.skill_name: item for item in run.analysis_plan.skipped_skills}
    assert skipped["competitor_benchmark"].reason == "用户显式排除该分析"


def test_analysis_executor_runs_independent_skills_with_bounded_concurrency():
    lock = threading.Lock()
    current = 0
    peak = 0

    class SlowSkill:
        version = "1.0"
        required_inputs = []
        optional_inputs = []
        required_tools = []

        def __init__(self, name):
            self.name = name

        def is_applicable(self, context):
            return SkillApplicability(True, "test")

        def execute(self, context):
            nonlocal current, peak
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1
            return AnalysisResult(
                f"{context.run_id}:{self.name}",
                context.run_id,
                self.name,
                self.name,
                skill_name=self.name,
                skill_version=self.version,
            )

    names = ["skill_a", "skill_b", "skill_c", "skill_d"]
    executor = AnalysisExecutor(
        skills=[SlowSkill(name) for name in names], max_concurrency=2
    )

    _run, results = executor.run(
        job_id="J1",
        topic="parallel",
        datasets=[],
        profiles=[],
        selected_tools=[],
        selected_skills=names,
    )

    assert peak == 2
    assert [result.skill_name for result in results] == names


def test_analysis_retry_reuses_completed_task_checkpoints(repository):
    job = repository.create_job("u1", "c1", "analysis-checkpoint", "checkpoint")
    calls = {"skill_a": 0, "skill_b": 0}

    class CheckpointSkill:
        version = "1.0"
        required_inputs = []
        optional_inputs = []
        required_tools = []

        def __init__(self, name, *, fail_once=False):
            self.name = name
            self.fail_once = fail_once

        def is_applicable(self, context):
            return SkillApplicability(True, "test")

        def execute(self, context):
            calls[self.name] += 1
            if self.fail_once and calls[self.name] == 1:
                raise RuntimeError("interrupted")
            return AnalysisResult(
                f"temporary:{self.name}",
                context.run_id,
                self.name,
                self.name,
                skill_name=self.name,
                skill_version=self.version,
            )

    executor = AnalysisExecutor(
        skills=[
            CheckpointSkill("skill_a"),
            CheckpointSkill("skill_b", fail_once=True),
        ],
        max_concurrency=1,
    )
    kwargs = {
        "job_id": job.job_id,
        "topic": job.topic,
        "datasets": [],
        "profiles": [],
        "selected_tools": [],
        "selected_skills": ["skill_a", "skill_b"],
        "load_cached_result": lambda key: (
            repository.get_analysis_result_by_idempotency_key(job.job_id, key)
        ),
        "on_run_started": repository.start_analysis_run,
        "on_result": lambda result: repository.save_analysis_result(
            job.job_id, result
        ),
    }

    with pytest.raises(RuntimeError, match="interrupted"):
        executor.run(**kwargs)

    run, results = executor.run(**kwargs)
    repository.complete_analysis_run(run.run_id)

    assert calls == {"skill_a": 1, "skill_b": 2}
    assert [result.skill_name for result in results] == ["skill_a", "skill_b"]
    assert len(repository.list_analysis_runs(job.job_id)) == 1
    assert len(repository.list_analysis_results(job.job_id)) == 2
