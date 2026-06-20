from __future__ import annotations

from ..datasets.models import DataQualityReport, DatasetProfile, TabularDataset
from .schemas import AnalysisPlan, SkippedSkill


SKILL_RULES = {
    "competitor_benchmark": {
        "tools": ["competitor_matrix"],
        "keywords": ["competitor", "竞品", "竞争"],
        "columns": ["company", "公司", "competitor", "竞品"],
        "missing": ["competitor_entity"],
        "output": "竞品矩阵",
    },
    "pricing_and_packaging": {
        "tools": ["pricing_normalizer", "chart_data_builder"],
        "keywords": ["price", "pricing", "价格", "定价"],
        "columns": ["price", "pricing", "价格", "定价", "fee", "费用"],
        "missing": ["price_column"],
        "output": "定价与套餐分析",
    },
    "market_positioning": {
        "tools": ["market_share_calculator"],
        "keywords": ["market", "share", "市场", "份额", "定位"],
        "columns": ["market", "市场", "share", "份额", "segment", "定位"],
        "missing": ["market_positioning_inputs"],
        "output": "市场定位分析",
    },
    "business_model": {
        "tools": ["unit_economics_calculator"],
        "keywords": ["business", "model", "商业模式", "收入", "客户", "revenue"],
        "columns": ["revenue", "收入", "营收", "customers", "客户", "用户"],
        "missing": ["business_model_inputs"],
        "output": "商业模式与单位经济分析",
    },
    "trend_and_change": {
        "tools": ["growth_metrics_calculator", "trend_detector", "chart_data_builder"],
        "keywords": ["growth", "trend", "增长", "趋势", "变化"],
        "columns": ["date", "time", "year", "month", "日期", "时间", "年份"],
        "missing": ["numeric_time_series"],
        "output": "趋势和变化分析",
    },
}

INCLUDE_ALIASES = {
    "competitor": "competitor_benchmark",
    "competitors": "competitor_benchmark",
    "pricing": "pricing_and_packaging",
    "price": "pricing_and_packaging",
    "market_position": "market_positioning",
    "market": "market_positioning",
    "business_model": "business_model",
    "business": "business_model",
    "trends": "trend_and_change",
    "trend": "trend_and_change",
}


class AnalysisSelector:
    def select(self, topic: str, dataset_names: list[str]) -> tuple[list[str], list[str]]:
        text = " ".join([topic, *dataset_names]).lower()
        tools = ["data_quality_summarizer"]
        skills: list[str] = []
        for skill_name, rule in SKILL_RULES.items():
            if any(key in text for key in rule["keywords"]):
                tools.extend(rule["tools"])
                skills.append(skill_name)
        return list(dict.fromkeys(tools)), list(dict.fromkeys(skills))

    def build_plan(
        self,
        *,
        topic: str,
        datasets: list[TabularDataset],
        profiles: list[DatasetProfile] | None = None,
        quality_reports: list[DataQualityReport] | None = None,
        evidence: list[object] | None = None,
        claims: list[object] | None = None,
        change_events: list[object] | None = None,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> AnalysisPlan:
        profiles = profiles or []
        quality_reports = quality_reports or []
        evidence = evidence or []
        claims = claims or []
        change_events = change_events or []
        requested = self._normalize_requested(include)
        excluded = self._normalize_requested(exclude)
        text = " ".join([topic, *[dataset.name for dataset in datasets]]).lower()
        usable_dataset_ids = self._usable_dataset_ids(datasets, quality_reports)
        numeric_dataset_ids = {
            profile.dataset_id for profile in profiles if profile.numeric_columns
        }
        temporal_dataset_ids = {
            profile.dataset_id for profile in profiles if profile.likely_time_columns
        }

        selected_tools = ["data_quality_summarizer"]
        selected_skills: list[str] = []
        skipped_skills: list[SkippedSkill] = []
        required_dataset_ids: list[str] = []
        limitations: list[str] = []
        expected_outputs = ["数据质量摘要"]

        if not datasets:
            limitations.append("未发现可用于专业分析的结构化数据集")

        for skill_name, rule in SKILL_RULES.items():
            if skill_name in excluded:
                skipped_skills.append(
                    SkippedSkill(skill_name=skill_name, reason="用户显式排除该分析")
                )
                continue
            dataset_ids = self._matching_dataset_ids(
                datasets,
                rule["columns"],
                usable_dataset_ids,
            )
            keyword_match = any(key in text for key in rule["keywords"])
            requested_match = not requested or skill_name in requested
            applicable = bool(dataset_ids) and requested_match
            if skill_name == "trend_and_change":
                applicable = (
                    requested_match
                    and bool(dataset_ids or change_events or temporal_dataset_ids)
                    and bool(numeric_dataset_ids)
                )
                dataset_ids = dataset_ids or sorted(numeric_dataset_ids)
            if skill_name == "business_model" and not dataset_ids and keyword_match:
                dataset_ids = sorted(usable_dataset_ids)
                applicable = requested_match and bool(dataset_ids)

            if applicable or (keyword_match and requested_match and dataset_ids):
                selected_skills.append(skill_name)
                selected_tools.extend(rule["tools"])
                required_dataset_ids.extend(dataset_ids)
                expected_outputs.append(rule["output"])
                continue

            if requested and skill_name not in requested:
                continue
            reason = "缺少适用数据集或证据"
            missing = list(rule["missing"])
            if not requested_match:
                reason = "用户未请求该分析"
            elif not datasets:
                reason = "没有可分析的数据集"
            elif quality_reports and not usable_dataset_ids:
                reason = "所有数据集质量不足，无法执行该分析"
                limitations.append(reason)
            skipped_skills.append(
                SkippedSkill(
                    skill_name=skill_name,
                    reason=reason,
                    missing_inputs=missing,
                    limitations=["数据不足时不生成或降级该分析"],
                )
            )

        return AnalysisPlan(
            selected_tools=list(dict.fromkeys(selected_tools)),
            selected_skills=list(dict.fromkeys(selected_skills)),
            skipped_skills=skipped_skills,
            required_dataset_ids=list(dict.fromkeys(required_dataset_ids)),
            required_evidence_ids=self._evidence_ids(evidence),
            expected_outputs=list(dict.fromkeys(expected_outputs)),
            limitations=list(dict.fromkeys(limitations)),
        )

    @staticmethod
    def _normalize_requested(values: list[str] | None) -> set[str]:
        result = set()
        for value in values or []:
            for item in value.split(","):
                normalized = item.strip().lower()
                if normalized:
                    result.add(INCLUDE_ALIASES.get(normalized, normalized))
        return result

    @staticmethod
    def _usable_dataset_ids(
        datasets: list[TabularDataset], quality_reports: list[DataQualityReport]
    ) -> set[str]:
        if not quality_reports:
            return {dataset.dataset_id for dataset in datasets}
        usable = {
            report.dataset_id
            for report in quality_reports
            if report.quality_level != "unusable"
        }
        return {dataset.dataset_id for dataset in datasets if dataset.dataset_id in usable}

    @staticmethod
    def _matching_dataset_ids(
        datasets: list[TabularDataset],
        columns: list[str],
        usable_dataset_ids: set[str],
    ) -> list[str]:
        result = []
        for dataset in datasets:
            if dataset.dataset_id not in usable_dataset_ids:
                continue
            text = " ".join([dataset.name, *dataset.columns]).lower()
            if any(column.lower() in text for column in columns):
                result.append(dataset.dataset_id)
        return result

    @staticmethod
    def _evidence_ids(evidence: list[object]) -> list[str]:
        result = []
        for item in evidence:
            if isinstance(item, dict):
                value = item.get("evidence_id")
            else:
                value = getattr(item, "evidence_id", None)
            if value:
                result.append(str(value))
        return list(dict.fromkeys(result))
