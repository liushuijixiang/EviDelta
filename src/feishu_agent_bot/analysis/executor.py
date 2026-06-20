from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import hashlib
import json
from typing import Callable
import uuid

from ..datasets.profiler import DatasetProfiler
from ..datasets.models import DatasetProfile, TabularDataset
from .schemas import AnalysisResult, AnalysisRun
from .selector import AnalysisSelector
from .skills import AnalysisContext, default_skills
from .tools import AnalysisToolRegistry, default_tool_registry


class AnalysisExecutor:
    def __init__(
        self,
        selector: AnalysisSelector | None = None,
        skills=None,
        tool_registry: AnalysisToolRegistry | None = None,
        max_concurrency: int = 4,
    ):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self.selector = selector or AnalysisSelector()
        self.skills = {skill.name: skill for skill in (skills or default_skills())}
        self.tool_registry = tool_registry or default_tool_registry()
        self.max_concurrency = max_concurrency

    def run(
        self,
        *,
        job_id: str,
        topic: str,
        datasets: list[TabularDataset],
        profiles: list[DatasetProfile],
        selected_tools: list[str] | None = None,
        selected_skills: list[str] | None = None,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        reason: str = "deterministic keyword and dataset rules",
        load_cached_result: Callable[[str], AnalysisResult | None] | None = None,
        on_run_started: Callable[[AnalysisRun], None] | None = None,
        on_result: Callable[[AnalysisResult], None] | None = None,
    ) -> tuple[AnalysisRun, list[AnalysisResult]]:
        quality_reports = [
            DatasetProfiler().quality_report(profile) for profile in profiles
        ]
        plan = self.selector.build_plan(
            topic=topic,
            datasets=datasets,
            profiles=profiles,
            quality_reports=quality_reports,
            include=include,
            exclude=exclude,
        )
        tools = list(
            dict.fromkeys(
                plan.selected_tools if selected_tools is None else selected_tools
            )
        )
        skills = list(
            dict.fromkeys(
                plan.selected_skills if selected_skills is None else selected_skills
            )
        )
        if selected_tools is not None or selected_skills is not None:
            plan = plan.model_copy(
                update={"selected_tools": tools, "selected_skills": skills}
            )
        tasks = [("tool", name) for name in tools]
        tasks.extend(("skill", name) for name in skills if name in self.skills)
        task_keys = {
            task: self._task_idempotency_key(
                job_id=job_id,
                topic=topic,
                task=task,
                datasets=datasets,
            )
            for task in tasks
        }
        run_key = self._stable_hash(
            {
                "job_id": job_id,
                "task_keys": [task_keys[task] for task in tasks],
                "analysis_plan": plan.model_dump(mode="json"),
            }
        )
        run = AnalysisRun(
            f"analysis-{run_key[:24]}",
            job_id,
            tools,
            skills,
            reason,
            plan,
            run_key,
        )
        if on_run_started is not None:
            on_run_started(run)
        profile_by_dataset = {profile.dataset_id: profile for profile in profiles}
        context = AnalysisContext(
            job_id=job_id,
            run_id=run.run_id,
            topic=topic,
            datasets=datasets,
            profiles=profiles,
            quality_reports=quality_reports,
        )
        def execute(task: tuple[str, str]) -> AnalysisResult:
            kind, name = task
            if kind == "tool":
                result = self._execute_tool(
                    name, run, datasets, profiles, profile_by_dataset
                )
            else:
                result = self._execute_skill(self.skills[name], context, datasets)
            task_key = task_keys[task]
            return replace(
                result,
                result_id=f"analysis-result-{task_key[:24]}",
                idempotency_key=task_key,
            )

        results: list[AnalysisResult | None] = [None] * len(tasks)
        pending: list[tuple[int, tuple[str, str]]] = []
        for index, task in enumerate(tasks):
            cached = (
                load_cached_result(task_keys[task])
                if load_cached_result is not None
                else None
            )
            if cached is not None:
                results[index] = cached
            else:
                pending.append((index, task))

        if self.max_concurrency == 1 or len(pending) <= 1:
            for index, task in pending:
                result = execute(task)
                results[index] = result
                if on_result is not None:
                    on_result(result)
        else:
            with ThreadPoolExecutor(
                max_workers=min(self.max_concurrency, len(pending)),
                thread_name_prefix="analysis",
            ) as executor:
                futures = {
                    executor.submit(execute, task): index
                    for index, task in pending
                }
                for future in as_completed(futures):
                    result = future.result()
                    results[futures[future]] = result
                    if on_result is not None:
                        on_result(result)
        return run, [result for result in results if result is not None]

    def _task_idempotency_key(
        self,
        *,
        job_id: str,
        topic: str,
        task: tuple[str, str],
        datasets: list[TabularDataset],
    ) -> str:
        kind, name = task
        version = (
            self.tool_registry.get(name).version
            if kind == "tool"
            else self.skills[name].version
        )
        return self._stable_hash(
            {
                "job_id": job_id,
                "topic": topic,
                "kind": kind,
                "name": name,
                "version": version,
                "parameters": {},
                "datasets": [
                    {
                        "dataset_id": dataset.dataset_id,
                        "columns": dataset.columns,
                        "rows": dataset.rows,
                    }
                    for dataset in datasets
                ],
            }
        )

    @staticmethod
    def _stable_hash(payload: object) -> str:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _execute_tool(
        self,
        tool: str,
        run: AnalysisRun,
        datasets: list[TabularDataset],
        profiles: list[DatasetProfile],
        profile_by_dataset: dict[str, DatasetProfile],
    ) -> AnalysisResult:
        registered_tool = self.tool_registry.get(tool)
        if tool == "data_quality_summarizer":
            warnings = [
                warning
                for profile in profiles
                for warning in profile.quality_warnings
            ]
            return AnalysisResult(
                str(uuid.uuid4()),
                run.run_id,
                tool,
                f"数据集数量 {len(datasets)}，质量提示 {len(warnings)} 条。",
                metrics={
                    "dataset_count": len(datasets),
                    "quality_warning_count": len(warnings),
                },
                tool_version=registered_tool.version,
                input_dataset_ids=[dataset.dataset_id for dataset in datasets],
            )
        tables = [
            {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "rows": (
                    profile_by_dataset[dataset.dataset_id].row_count
                    if dataset.dataset_id in profile_by_dataset
                    else len(dataset.rows)
                ),
                "columns": len(dataset.columns),
            }
            for dataset in datasets
        ]
        return AnalysisResult(
            str(uuid.uuid4()),
            run.run_id,
            tool,
            f"{tool} 已基于 {len(datasets)} 个表格数据集生成结构化摘要。",
            tables=tables,
            tool_version=registered_tool.version,
            input_dataset_ids=[dataset.dataset_id for dataset in datasets],
        )

    @staticmethod
    def _execute_skill(skill, context, datasets) -> AnalysisResult:
        applicability = skill.is_applicable(context)
        if applicability.applicable:
            return skill.execute(context)
        return AnalysisResult(
            f"{context.run_id}:{skill.name}",
            context.run_id,
            "skill_applicability",
            "当前资料不足以执行该分析。",
            metrics={
                "status": "insufficient_data",
                "missing_inputs": applicability.missing_inputs,
            },
            skill_name=skill.name,
            skill_version=skill.version,
            input_dataset_ids=[dataset.dataset_id for dataset in datasets],
            limitations=applicability.limitations or [applicability.reason],
            confidence_band="low",
        )
