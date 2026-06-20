from __future__ import annotations

from ..schemas import AnalysisResult
from ..tools import (
    chart_data_builder,
    competitor_matrix_builder,
    growth_metrics_calculator,
    market_share_calculator,
    pricing_normalizer,
    unit_economics_calculator,
)
from .base import AnalysisContext, SkillApplicability, dataset_ids, result_id


class CompetitorBenchmarkSkill:
    name = "competitor_benchmark"
    version = "1.0"
    required_inputs = ["competitor_dataset_or_evidence"]
    optional_inputs = ["pricing", "features", "market_position"]
    required_tools = ["competitor_matrix_builder"]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        dataset = _dataset_with_column(context, ["company", "公司", "competitor", "竞品"])
        if dataset is None:
            return SkillApplicability(False, "缺少竞品实体字段", ["competitor_entity"])
        return SkillApplicability(True, "存在竞品实体字段")

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        dataset = _dataset_with_column(context, ["company", "公司", "competitor", "竞品"])
        if dataset is None:
            return _insufficient(context, self.name, self.version, ["competitor_entity"])
        entity_column = _first_matching_column(dataset.columns, ["company", "公司", "competitor", "竞品"])
        dimensions = [column for column in dataset.columns if column != entity_column][:8]
        if not dimensions:
            return _insufficient(context, self.name, self.version, ["comparison_dimensions"])
        matrix = competitor_matrix_builder(
            dataset.rows,
            entity_column=entity_column,
            dimensions=dimensions,
        )
        return AnalysisResult(
            result_id(context.run_id, self.name),
            context.run_id,
            "competitor_matrix_builder",
            f"已生成 {len(matrix['matrix'])} 个竞品、{len(dimensions)} 个维度的竞品矩阵。",
            tables=[
                {
                    "dataset_id": dataset.dataset_id,
                    "name": "competitor_matrix",
                    "rows": matrix["matrix"],
                    "missing_fields": matrix["missing_fields"],
                }
            ],
            metrics={"completeness": matrix["completeness"]},
            skill_name=self.name,
            skill_version=self.version,
            tool_version="1.0",
            input_dataset_ids=[dataset.dataset_id],
            limitations=[] if matrix["completeness"] >= 0.8 else ["竞品矩阵字段不完整"],
        )


class PricingAndPackagingSkill:
    name = "pricing_and_packaging"
    version = "1.0"
    required_inputs = ["price_column"]
    optional_inputs = ["currency", "billing_period", "plan_tier"]
    required_tools = ["pricing_normalizer", "chart_data_builder"]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        dataset = _dataset_with_column(context, ["price", "pricing", "价格", "定价", "fee", "费用"])
        if dataset is None:
            return SkillApplicability(False, "缺少价格字段", ["price_column"])
        return SkillApplicability(True, "存在价格字段")

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        dataset = _dataset_with_column(context, ["price", "pricing", "价格", "定价", "fee", "费用"])
        if dataset is None:
            return _insufficient(context, self.name, self.version, ["price_column"])
        price_column = _first_matching_column(dataset.columns, ["price", "pricing", "价格", "定价", "fee", "费用"])
        normalized = pricing_normalizer(dataset.rows, price_column=price_column)
        x_column = _first_matching_column(dataset.columns, ["company", "公司", "competitor", "plan", "套餐"]) or dataset.columns[0]
        chart = chart_data_builder(
            normalized["rows"],
            x_column=x_column,
            y_column="normalized_price",
            chart_type="bar",
        )
        charts = []
        if chart["status"] == "ok":
            charts.append(
                {
                    "chart_id": f"{result_id(context.run_id, self.name)}-price",
                    "chart_type": "bar",
                    "title": "价格对比",
                    "unit": "original_currency",
                    "points": chart["points"],
                    "dataset_ids": [dataset.dataset_id],
                }
            )
        return AnalysisResult(
            result_id(context.run_id, self.name),
            context.run_id,
            "pricing_normalizer",
            f"已解析 {len(normalized['rows'])} 条价格记录；未做无来源汇率换算。",
            tables=[{"dataset_id": dataset.dataset_id, "name": "normalized_pricing", "rows": normalized["rows"]}],
            charts=charts,
            metrics={"parsed_price_count": sum(1 for row in normalized["rows"] if row.get("status") == "parsed")},
            skill_name=self.name,
            skill_version=self.version,
            tool_version="1.0",
            input_dataset_ids=[dataset.dataset_id],
            limitations=["未提供明确汇率来源时保留原币种"],
        )


class MarketPositioningSkill:
    name = "market_positioning"
    version = "1.0"
    required_inputs = ["positioning_or_market_dataset"]
    optional_inputs = ["market_total", "segment", "target_customer"]
    required_tools = ["market_share_calculator"]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        dataset = _dataset_with_column(context, ["market", "市场", "share", "份额", "segment", "定位"])
        if dataset is None:
            return SkillApplicability(False, "缺少市场定位或份额字段", ["market_positioning_inputs"])
        return SkillApplicability(True, "存在市场定位相关字段")

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        dataset = _dataset_with_column(context, ["market", "市场", "share", "份额", "segment", "定位"])
        if dataset is None:
            return _insufficient(context, self.name, self.version, ["market_positioning_inputs"])
        value_column = _first_numeric_or_matching_column(dataset.columns, dataset.rows, ["share", "份额", "sales", "销量", "revenue", "收入"])
        if value_column is None:
            return _insufficient(context, self.name, self.version, ["numeric_market_proxy"])
        result = market_share_calculator(dataset.rows, entity_value_column=value_column, total_value=None)
        return AnalysisResult(
            result_id(context.run_id, self.name),
            context.run_id,
            "market_share_calculator",
            "缺少可靠市场总量，已降级为代理指标/相对指数分析。",
            tables=[{"dataset_id": dataset.dataset_id, "name": "market_position_proxy", "rows": result.get("shares") or dataset.rows[:100]}],
            metrics={"status": result["status"], "proxy_metric": value_column},
            skill_name=self.name,
            skill_version=self.version,
            tool_version="1.0",
            input_dataset_ids=[dataset.dataset_id],
            limitations=["没有可靠市场总量时不得表述为实际市场份额"],
            confidence_band="low",
        )


class BusinessModelSkill:
    name = "business_model"
    version = "1.0"
    required_inputs = ["revenue_or_customer_inputs"]
    optional_inputs = ["gross_profit", "cac", "ltv", "retention"]
    required_tools = ["unit_economics_calculator"]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        text = " ".join([context.topic, *[dataset.name for dataset in context.datasets]]).lower()
        if any(key in text for key in ["business", "model", "商业模式", "收入", "revenue", "客户"]):
            return SkillApplicability(True, "主题或数据集包含商业模式线索")
        return SkillApplicability(False, "缺少商业模式输入", ["business_model_inputs"])

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        inputs = _collect_unit_economics_inputs(context)
        result = unit_economics_calculator(inputs)
        limitations = []
        if result["status"] == "insufficient_data":
            limitations.append("单位经济模型缺少必要输入，不计算 ARPU/毛利率/回本周期")
        return AnalysisResult(
            result_id(context.run_id, self.name),
            context.run_id,
            "unit_economics_calculator",
            "单位经济测算完成。" if result["status"] == "ok" else "当前资料不足以执行完整单位经济测算。",
            tables=[{"name": "unit_economics", "rows": [result]}],
            metrics=result,
            skill_name=self.name,
            skill_version=self.version,
            tool_version="1.0",
            input_dataset_ids=dataset_ids(context),
            limitations=limitations,
            confidence_band="medium" if result["status"] == "ok" else "low",
        )


class TrendAndChangeSkill:
    name = "trend_and_change"
    version = "1.0"
    required_inputs = ["time_series_or_ordered_numeric_values"]
    optional_inputs = ["monitoring_cycles", "events"]
    required_tools = ["growth_metrics_calculator", "chart_data_builder"]

    def is_applicable(self, context: AnalysisContext) -> SkillApplicability:
        dataset, value_column = _dataset_with_numeric_column(context)
        if dataset is None or value_column is None:
            return SkillApplicability(False, "缺少趋势分析所需数值列", ["numeric_time_series"])
        return SkillApplicability(True, "存在可排序数值列")

    def execute(self, context: AnalysisContext) -> AnalysisResult:
        dataset, value_column = _dataset_with_numeric_column(context)
        if dataset is None or value_column is None:
            return _insufficient(context, self.name, self.version, ["numeric_time_series"])
        growth = growth_metrics_calculator(dataset.rows, value_column=value_column)
        x_column = _first_matching_column(dataset.columns, ["date", "time", "year", "month", "日期", "时间", "年份"]) or dataset.columns[0]
        chart = chart_data_builder(dataset.rows, x_column=x_column, y_column=value_column, chart_type="line")
        charts = []
        if chart["status"] == "ok":
            charts.append(
                {
                    "chart_id": f"{result_id(context.run_id, self.name)}-trend",
                    "chart_type": "line",
                    "title": f"{value_column} 趋势",
                    "points": chart["points"],
                    "dataset_ids": [dataset.dataset_id],
                }
            )
        return AnalysisResult(
            result_id(context.run_id, self.name),
            context.run_id,
            "growth_metrics_calculator",
            "趋势计算完成。" if growth["status"] == "ok" else "样本不足，无法判断趋势。",
            tables=[{"dataset_id": dataset.dataset_id, "name": "growth_metrics", "rows": [growth]}],
            charts=charts,
            metrics=growth,
            skill_name=self.name,
            skill_version=self.version,
            tool_version="1.0",
            input_dataset_ids=[dataset.dataset_id],
            limitations=[] if growth["status"] == "ok" else ["趋势分析至少需要两个有效数值"],
            confidence_band="medium" if growth["status"] == "ok" else "low",
        )


def default_skills():
    return [
        CompetitorBenchmarkSkill(),
        PricingAndPackagingSkill(),
        MarketPositioningSkill(),
        BusinessModelSkill(),
        TrendAndChangeSkill(),
    ]


def _insufficient(
    context: AnalysisContext, skill_name: str, version: str, missing_inputs: list[str]
) -> AnalysisResult:
    return AnalysisResult(
        result_id(context.run_id, skill_name),
        context.run_id,
        "skill_applicability",
        "当前资料不足以执行该分析。",
        metrics={"status": "insufficient_data", "missing_inputs": missing_inputs},
        skill_name=skill_name,
        skill_version=version,
        limitations=[f"缺少：{', '.join(missing_inputs)}"],
        confidence_band="low",
    )


def _dataset_with_column(context: AnalysisContext, names: list[str]):
    for dataset in context.datasets:
        if _first_matching_column(dataset.columns, names):
            return dataset
    return None


def _dataset_with_numeric_column(context: AnalysisContext):
    for profile in context.profiles:
        if profile.numeric_columns:
            dataset = next(
                (item for item in context.datasets if item.dataset_id == profile.dataset_id),
                None,
            )
            if dataset is not None:
                return dataset, profile.numeric_columns[0]
    return None, None


def _first_matching_column(columns: list[str], names: list[str]) -> str | None:
    lowered = [(column, column.lower()) for column in columns]
    for needle in names:
        needle_lower = needle.lower()
        for original, lower in lowered:
            if needle_lower == lower or needle_lower in lower:
                return original
    return None


def _first_numeric_or_matching_column(
    columns: list[str], rows: list[dict[str, object]], names: list[str]
) -> str | None:
    matching = _first_matching_column(columns, names)
    if matching:
        return matching
    for column in columns:
        hits = 0
        for row in rows:
            try:
                float(str(row.get(column)).replace(",", ""))
            except (TypeError, ValueError):
                continue
            hits += 1
        if rows and hits / len(rows) >= 0.8:
            return column
    return None


def _collect_unit_economics_inputs(context: AnalysisContext) -> dict[str, float | None]:
    mapping = {
        "revenue": ["revenue", "收入", "营收"],
        "customers": ["customers", "客户", "用户"],
        "gross_profit": ["gross_profit", "毛利"],
        "cac": ["cac", "获客成本"],
    }
    result: dict[str, float | None] = {key: None for key in mapping}
    for dataset in context.datasets:
        for key, names in mapping.items():
            column = _first_matching_column(dataset.columns, names)
            if not column:
                continue
            values = []
            for row in dataset.rows:
                try:
                    values.append(float(str(row.get(column)).replace(",", "")))
                except (TypeError, ValueError):
                    continue
            if values:
                result[key] = sum(values)
    return result

