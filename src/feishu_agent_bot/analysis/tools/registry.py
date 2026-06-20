from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from pydantic import BaseModel, ConfigDict

from .core import (
    chart_data_builder,
    comparison_ranker,
    competitor_matrix_builder,
    data_quality_summarizer,
    growth_metrics_calculator,
    market_share_calculator,
    pivot_table_builder,
    pricing_normalizer,
    trend_detector,
    unit_economics_calculator,
)


class AnalysisTool(Protocol):
    name: str
    version: str
    input_schema: type[BaseModel]
    output_schema: type[BaseModel]

    def execute(self, input_data: BaseModel) -> BaseModel:
        ...


class ToolInput(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class ToolOutput(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)


class RowsInput(ToolInput):
    rows: list[dict[str, object]]


class CompetitorMatrixInput(RowsInput):
    entity_column: str
    dimensions: list[str]


class PricingInput(RowsInput):
    price_column: str


class GrowthInput(RowsInput):
    value_column: str


class MarketShareInput(RowsInput):
    entity_value_column: str
    total_value: float | None = None


class UnitEconomicsInput(ToolInput):
    inputs: dict[str, float | None]


class PivotInput(RowsInput):
    index_column: str
    value_column: str


class DataQualityInput(ToolInput):
    profile: object


class TrendInput(ToolInput):
    values: list[float]


class RankingInput(RowsInput):
    value_column: str
    descending: bool = True


class ChartDataInput(RowsInput):
    x_column: str
    y_column: str
    chart_type: str


class DictOutput(ToolOutput):
    result: dict[str, object]


class ListOutput(ToolOutput):
    rows: list[dict[str, object]]


@dataclass(frozen=True)
class FunctionAnalysisTool:
    name: str
    version: str
    input_schema: type[BaseModel]
    output_schema: type[BaseModel]
    handler: Callable[[BaseModel], object]

    def execute(self, input_data: BaseModel) -> BaseModel:
        validated = (
            input_data
            if isinstance(input_data, self.input_schema)
            else self.input_schema.model_validate(input_data)
        )
        raw = self.handler(validated)
        if isinstance(raw, self.output_schema):
            return raw
        if isinstance(raw, list):
            return self.output_schema.model_validate({"rows": raw})
        return self.output_schema.model_validate({"result": raw})


class AnalysisToolRegistry:
    def __init__(self, tools: list[AnalysisTool] | None = None):
        self._tools: dict[str, AnalysisTool] = {}
        for tool in tools or []:
            self.register(tool)

    def register(self, tool: AnalysisTool, *aliases: str) -> None:
        self._tools[tool.name] = tool
        for alias in aliases:
            self._tools[alias] = tool

    def get(self, name: str) -> AnalysisTool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"unknown analysis tool: {name}") from exc

    def list_tools(self) -> list[AnalysisTool]:
        seen: dict[str, AnalysisTool] = {}
        for tool in self._tools.values():
            seen[tool.name] = tool
        return [seen[name] for name in sorted(seen)]

    def execute(self, name: str, payload: dict[str, object] | BaseModel) -> BaseModel:
        tool = self.get(name)
        input_data = (
            payload
            if isinstance(payload, tool.input_schema)
            else tool.input_schema.model_validate(payload)
        )
        return tool.execute(input_data)


def default_tool_registry() -> AnalysisToolRegistry:
    registry = AnalysisToolRegistry()
    registry.register(
        FunctionAnalysisTool(
            "competitor_matrix_builder",
            "1.0",
            CompetitorMatrixInput,
            DictOutput,
            lambda data: competitor_matrix_builder(
                data.rows,
                entity_column=data.entity_column,
                dimensions=data.dimensions,
            ),
        ),
        "competitor_matrix",
    )
    registry.register(
        FunctionAnalysisTool(
            "pricing_normalizer",
            "1.0",
            PricingInput,
            DictOutput,
            lambda data: pricing_normalizer(data.rows, price_column=data.price_column),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "growth_metrics_calculator",
            "1.0",
            GrowthInput,
            DictOutput,
            lambda data: growth_metrics_calculator(data.rows, value_column=data.value_column),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "market_share_calculator",
            "1.0",
            MarketShareInput,
            DictOutput,
            lambda data: market_share_calculator(
                data.rows,
                entity_value_column=data.entity_value_column,
                total_value=data.total_value,
            ),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "unit_economics_calculator",
            "1.0",
            UnitEconomicsInput,
            DictOutput,
            lambda data: unit_economics_calculator(data.inputs),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "pivot_table_builder",
            "1.0",
            PivotInput,
            ListOutput,
            lambda data: pivot_table_builder(
                data.rows,
                index_column=data.index_column,
                value_column=data.value_column,
            ),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "data_quality_summarizer",
            "1.0",
            DataQualityInput,
            DictOutput,
            lambda data: data_quality_summarizer(data.profile),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "trend_detector",
            "1.0",
            TrendInput,
            DictOutput,
            lambda data: trend_detector(data.values),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "comparison_ranker",
            "1.0",
            RankingInput,
            ListOutput,
            lambda data: comparison_ranker(
                data.rows,
                value_column=data.value_column,
                descending=data.descending,
            ),
        )
    )
    registry.register(
        FunctionAnalysisTool(
            "chart_data_builder",
            "1.0",
            ChartDataInput,
            DictOutput,
            lambda data: chart_data_builder(
                data.rows,
                x_column=data.x_column,
                y_column=data.y_column,
                chart_type=data.chart_type,
            ),
        )
    )
    return registry
