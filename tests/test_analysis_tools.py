import inspect

from feishu_agent_bot.analysis.tools import (
    chart_data_builder,
    competitor_matrix_builder,
    default_tool_registry,
    growth_metrics_calculator,
    market_share_calculator,
    pricing_normalizer,
    unit_economics_calculator,
)
from feishu_agent_bot.analysis.tools import core as tool_core


def test_pricing_normalizer_preserves_currency_without_exchange_rate():
    result = pricing_normalizer(
        [{"company": "A", "price": "¥99/月"}],
        price_column="price",
    )

    row = result["rows"][0]
    assert row["normalized_price"] == 99
    assert row["currency"] == "CNY"
    assert row["billing_period"] == "monthly"
    assert result["exchange_rate_status"] == "not_converted"


def test_growth_metrics_handles_zero_denominator_and_insufficient_data():
    zero = growth_metrics_calculator(
        [{"value": 0}, {"value": 10}],
        value_column="value",
    )
    insufficient = growth_metrics_calculator(
        [{"value": 1}],
        value_column="value",
    )

    assert zero["relative_change"] is None
    assert zero["trend_direction"] == "up"
    assert insufficient["status"] == "insufficient_data"


def test_market_share_requires_total_value():
    proxy = market_share_calculator(
        [{"company": "A", "sales": 10}],
        entity_value_column="sales",
        total_value=None,
    )
    actual = market_share_calculator(
        [{"company": "A", "sales": 10}],
        entity_value_column="sales",
        total_value=100,
    )

    assert proxy["status"] == "proxy_only"
    assert actual["shares"][0]["market_share"] == 0.1


def test_unit_economics_reports_missing_inputs():
    missing = unit_economics_calculator({"revenue": 100, "customers": 10})
    ok = unit_economics_calculator(
        {"revenue": 100, "customers": 10, "gross_profit": 40, "cac": 20}
    )

    assert missing["status"] == "insufficient_data"
    assert "gross_profit" in missing["missing_inputs"]
    assert ok["arpu"] == 10
    assert ok["gross_margin"] == 0.4


def test_competitor_matrix_and_chart_data_builder_keep_missing_empty():
    matrix = competitor_matrix_builder(
        [
            {"company": "A", "price": "99", "feature": "fast"},
            {"company": "B", "price": "", "feature": "slow"},
        ],
        entity_column="company",
        dimensions=["price", "feature"],
    )
    chart = chart_data_builder(
        [{"company": "A", "score": "1"}, {"company": "B", "score": ""}],
        x_column="company",
        y_column="score",
        chart_type="bar",
    )

    assert matrix["missing_fields"]["B"] == ["price"]
    assert matrix["completeness"] == 0.75
    assert chart["points"] == [{"x": "A", "y": 1.0}]


def test_analysis_tool_registry_exposes_alpha_tool_contracts():
    registry = default_tool_registry()
    tools = {tool.name: tool for tool in registry.list_tools()}

    assert set(tools) == {
        "competitor_matrix_builder",
        "pricing_normalizer",
        "growth_metrics_calculator",
        "market_share_calculator",
        "unit_economics_calculator",
        "pivot_table_builder",
        "data_quality_summarizer",
        "trend_detector",
        "comparison_ranker",
        "chart_data_builder",
    }
    for tool in tools.values():
        assert tool.version == "1.0"
        assert tool.input_schema is not None
        assert tool.output_schema is not None
        assert callable(tool.execute)

    assert registry.get("competitor_matrix") is tools["competitor_matrix_builder"]
    output = registry.execute(
        "pricing_normalizer",
        {"rows": [{"price": "¥99/月"}], "price_column": "price"},
    )
    assert output.result["rows"][0]["normalized_price"] == 99


def test_analysis_tools_do_not_use_dynamic_execution_or_shells():
    source = inspect.getsource(tool_core)
    forbidden = ["eval(", "exec(", "subprocess", "os.system", "__import__"]

    for token in forbidden:
        assert token not in source
