from __future__ import annotations

from collections import defaultdict
import re
from statistics import mean


def competitor_matrix_builder(
    rows: list[dict[str, object]],
    *,
    entity_column: str,
    dimensions: list[str],
) -> dict[str, object]:
    matrix = []
    missing: dict[str, list[str]] = {}
    for row in rows:
        entity = str(row.get(entity_column) or "")
        item = {"entity": entity}
        missing[entity] = []
        for dimension in dimensions:
            value = row.get(dimension)
            item[dimension] = value
            if value in {None, ""}:
                missing[entity].append(dimension)
        matrix.append(item)
    completeness = (
        1
        - sum(len(values) for values in missing.values())
        / max(1, len(rows) * len(dimensions))
    )
    return {"matrix": matrix, "missing_fields": missing, "completeness": completeness}


def pricing_normalizer(rows: list[dict[str, object]], *, price_column: str) -> dict:
    normalized = []
    for row in rows:
        raw = row.get(price_column)
        parsed = _parse_number(raw)
        normalized.append(
            {
                **row,
                "normalized_price": parsed,
                "currency": _detect_currency(str(raw or "")),
                "billing_period": _detect_period(str(raw or "")),
                "status": "parsed" if parsed is not None else "missing_price",
            }
        )
    return {"rows": normalized, "exchange_rate_status": "not_converted"}


def growth_metrics_calculator(
    rows: list[dict[str, object]], *, value_column: str
) -> dict[str, object]:
    values = [
        _parse_number(row.get(value_column))
        for row in rows
        if _parse_number(row.get(value_column)) is not None
    ]
    if len(values) < 2:
        return {"status": "insufficient_data", "missing_inputs": ["at_least_two_values"]}
    absolute_change = values[-1] - values[0]
    pct_change = None if values[0] == 0 else absolute_change / values[0]
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    return {
        "status": "ok",
        "absolute_change": absolute_change,
        "relative_change": pct_change,
        "average_period_delta": mean(deltas),
        "trend_direction": "up" if absolute_change > 0 else "down" if absolute_change < 0 else "flat",
    }


def market_share_calculator(
    rows: list[dict[str, object]], *, entity_value_column: str, total_value: float | None
) -> dict[str, object]:
    if not total_value or total_value <= 0:
        return {
            "status": "proxy_only",
            "shares": [],
            "proxy_metric": entity_value_column,
        }
    shares = []
    for row in rows:
        value = _parse_number(row.get(entity_value_column))
        shares.append({**row, "market_share": None if value is None else value / total_value})
    return {"status": "actual_share", "shares": shares}


def unit_economics_calculator(inputs: dict[str, float | None]) -> dict[str, object]:
    required = ["revenue", "customers", "gross_profit", "cac"]
    missing = [key for key in required if not inputs.get(key)]
    if missing:
        return {"status": "insufficient_data", "missing_inputs": missing}
    revenue = float(inputs["revenue"])
    customers = float(inputs["customers"])
    gross_profit = float(inputs["gross_profit"])
    cac = float(inputs["cac"])
    return {
        "status": "ok",
        "arpu": revenue / customers,
        "gross_margin": gross_profit / revenue if revenue else None,
        "payback_period": cac / (gross_profit / customers) if gross_profit and customers else None,
    }


def pivot_table_builder(
    rows: list[dict[str, object]], *, index_column: str, value_column: str
) -> list[dict[str, object]]:
    buckets: dict[str, float] = defaultdict(float)
    for row in rows:
        value = _parse_number(row.get(value_column))
        if value is not None:
            buckets[str(row.get(index_column) or "")] += value
    return [{index_column: key, value_column: value} for key, value in buckets.items()]


def data_quality_summarizer(profile) -> dict[str, object]:
    return {
        "dataset_id": profile.dataset_id,
        "row_count": profile.row_count,
        "column_count": profile.column_count,
        "warnings": profile.quality_warnings,
        "numeric_columns": profile.numeric_columns,
    }


def trend_detector(values: list[float]) -> dict[str, object]:
    if len(values) < 2:
        return {"status": "insufficient_data"}
    delta = values[-1] - values[0]
    return {
        "status": "ok",
        "direction": "up" if delta > 0 else "down" if delta < 0 else "flat",
        "delta": delta,
    }


def comparison_ranker(
    rows: list[dict[str, object]], *, value_column: str, descending: bool = True
) -> list[dict[str, object]]:
    return sorted(
        rows,
        key=lambda row: (_parse_number(row.get(value_column)) is None, _parse_number(row.get(value_column)) or 0),
        reverse=descending,
    )


def chart_data_builder(
    rows: list[dict[str, object]], *, x_column: str, y_column: str, chart_type: str
) -> dict[str, object]:
    points = [
        {"x": row.get(x_column), "y": _parse_number(row.get(y_column))}
        for row in rows
        if row.get(x_column) is not None and _parse_number(row.get(y_column)) is not None
    ]
    return {
        "status": "ok" if points else "insufficient_data",
        "chart_type": chart_type,
        "points": points,
    }


def _parse_number(value: object) -> float | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    for token in ("¥", "$", "€", "元", "人民币", "USD", "CNY", ","):
        text = text.replace(token, "")
    text = text.replace("%", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if match:
        text = match.group(0)
    try:
        return float(text)
    except ValueError:
        return None


def _detect_currency(text: str) -> str | None:
    if "¥" in text or "元" in text or "CNY" in text.upper() or "人民币" in text:
        return "CNY"
    if "$" in text or "USD" in text.upper():
        return "USD"
    if "€" in text or "EUR" in text.upper():
        return "EUR"
    return None


def _detect_period(text: str) -> str | None:
    lowered = text.lower()
    if "月" in lowered or "/mo" in lowered or "month" in lowered:
        return "monthly"
    if "年" in lowered or "/yr" in lowered or "year" in lowered:
        return "yearly"
    return None
