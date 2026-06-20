from __future__ import annotations

from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re

from .report_ir import ReportIR, ReportSection


class ReportIRBuilder:
    def __init__(self, *, max_charts: int = 12, max_tables: int = 30):
        if max_charts < 1 or max_tables < 1:
            raise ValueError("Report IR chart/table limits must be positive")
        self.max_charts = max_charts
        self.max_tables = max_tables

    def from_report_json(
        self,
        *,
        job_id: str,
        topic: str,
        report_json_path: str | Path,
        analysis_results: list[dict] | None = None,
        datasets: list[dict] | None = None,
        sources: list[dict] | None = None,
        claims: list[dict] | None = None,
        evidence: list[object] | None = None,
        change_events: list[dict] | None = None,
        analysis_runs: list[dict] | None = None,
        report_id: str = "",
        version: int = 1,
        parent_version_id: str | None = None,
    ) -> ReportIR:
        data = json.loads(Path(report_json_path).read_text(encoding="utf-8"))
        analysis_results = analysis_results or []
        datasets = datasets or []
        sources = sources or []
        claims = claims or []
        evidence = evidence or []
        change_events = change_events or []
        analysis_runs = analysis_runs or []
        analysis_plan = self._latest_analysis_plan(analysis_runs)
        claims_by_id = {
            str(item.get("claim_id")): item
            for item in (self._claim_to_dict(claim) for claim in claims)
            if item.get("claim_id")
        }
        sections = []
        for index, section in enumerate(data.get("sections", []), start=1):
            section_claim_ids = [
                str(item) for item in section.get("claim_ids", []) if item
            ]
            body = self._section_body(section, section_claim_ids, claims_by_id)
            sections.append(
                ReportSection(
                    section_id=str(section.get("section_id") or f"S{index:03d}"),
                    title=str(section.get("title") or f"Section {index}"),
                    body=body,
                    claim_ids=section_claim_ids,
                    evidence_ids=self._section_evidence_ids(
                        section, section_claim_ids, claims_by_id
                    ),
                )
            )
        if not sections:
            sections.append(
                ReportSection(
                    "summary",
                    "摘要",
                    json.dumps(data, ensure_ascii=False)[:2000],
                )
            )
        language = "en" if data.get("language") == "en" else "zh"
        analysis_tables = self._tables_from_datasets(datasets, analysis_results)
        analysis_charts = self._charts_from_analysis(analysis_results)
        evidence_charts, evidence_tables = self._evidence_visuals(
            evidence,
            claims,
            sources=sources,
            language=language,
        )
        all_tables = [*analysis_tables, *evidence_tables]
        all_charts = [*analysis_charts, *evidence_charts]
        tables = all_tables[: self.max_tables]
        charts = all_charts[: self.max_charts]
        if analysis_results:
            analysis_result_ids = [
                str(item.get("analysis_result_id") or item.get("result_id"))
                for item in analysis_results
                if item.get("analysis_result_id") or item.get("result_id")
            ]
            sections.append(
                ReportSection(
                    section_id="analysis",
                    title="Professional Analysis" if language == "en" else "专业分析",
                    body="\n".join(
                        f"- {item.get('tool_name')}: {item.get('summary', '')}"
                        for item in analysis_results
                    ),
                    section_type="analysis",
                    analysis_result_ids=analysis_result_ids,
                    table_ids=[
                        str(item.get("table_id"))
                        for item in tables
                        if item.get("table_id")
                        and item in analysis_tables
                    ],
                    chart_ids=[
                        str(item.get("chart_id"))
                        for item in charts
                        if item.get("chart_id")
                        and item in analysis_charts
                    ],
                )
            )
        visible_evidence_charts = [
            item for item in charts if item in evidence_charts
        ]
        visible_evidence_tables = [
            item for item in tables if item in evidence_tables
        ]
        if visible_evidence_charts or visible_evidence_tables:
            visualization_evidence_ids = list(
                dict.fromkeys(
                    str(evidence_id)
                    for item in [
                        *visible_evidence_charts,
                        *visible_evidence_tables,
                    ]
                    for evidence_id in item.get("evidence_ids", [])
                    if evidence_id
                )
            )
            sections.append(
                ReportSection(
                    section_id="research_data_visualization",
                    title=(
                        "Research Data Visualization"
                        if language == "en"
                        else "研究数据可视化"
                    ),
                    body=(
                        "The following charts and tables are derived only from "
                        "the evidence used in this report. They visualize "
                        "comparable values and evidence coverage without "
                        "filling missing data."
                        if language == "en"
                        else "以下图表仅基于本报告已采用的证据生成，用于展示"
                        "同口径数值对比与证据覆盖情况；缺失数据保持为空，不做补齐。"
                    ),
                    section_type="methodology",
                    chart_ids=[
                        str(item["chart_id"])
                        for item in visible_evidence_charts
                        if item.get("chart_id")
                    ],
                    table_ids=[
                        str(item["table_id"])
                        for item in visible_evidence_tables
                        if item.get("table_id")
                    ],
                    evidence_ids=visualization_evidence_ids,
                )
            )
        data_quality = [
            {
                "dataset_id": item.get("dataset_id"),
                "name": item.get("dataset_name") or item.get("name"),
                "row_count": item.get("row_count"),
                "column_count": item.get("column_count"),
                "profile": item.get("profile") or {},
            }
            for item in datasets
        ]
        limitations = self._limitations_from_plan(analysis_plan)
        if len(all_tables) > self.max_tables:
            limitations.append(
                f"表格数量 {len(all_tables)} 超过上限 {self.max_tables}，"
                f"仅保留前 {self.max_tables} 项"
            )
        if len(all_charts) > self.max_charts:
            limitations.append(
                f"图表数量 {len(all_charts)} 超过上限 {self.max_charts}，"
                f"仅保留前 {self.max_charts} 项"
            )
        return ReportIR(
            job_id=job_id,
            report_id=report_id,
            version=version,
            parent_version_id=parent_version_id,
            title=topic,
            generated_at=str(
                data.get("generated_at") or "1970-01-01T00:00:00+00:00"
            ),
            sections=sections,
            executive_summary=self._executive_summary(data),
            tables=tables,
            charts=charts,
            analysis_results=analysis_results,
            change_events=change_events,
            claims=[self._claim_to_dict(item) for item in claims],
            evidence_references=[self._evidence_to_dict(item) for item in evidence],
            data_quality=data_quality,
            limitations=limitations,
            sources=[
                {
                    "source_id": item.get("source_id"),
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "canonical_url": item.get("canonical_url"),
                    "content_type": item.get("content_type"),
                }
                for item in sources
            ],
            metadata={
                "source": "report_json",
                "analysis_result_count": len(analysis_results),
                "dataset_count": len(datasets),
                "analysis_plan": analysis_plan,
                "language": language,
                "evidence_chart_count": len(evidence_charts),
                "evidence_table_count": len(evidence_tables),
            },
        )

    @staticmethod
    def _tables_from_datasets(
        datasets: list[dict], analysis_results: list[dict]
    ) -> list[dict[str, object]]:
        tables: list[dict[str, object]] = []
        for item in datasets:
            rows = list(item.get("rows") or [])
            tables.append(
                {
                    "table_id": item.get("table_id") or item.get("dataset_id"),
                    "dataset_id": item.get("dataset_id"),
                    "title": item.get("dataset_name") or item.get("name"),
                    "columns": item.get("columns") or [],
                    "rows": rows[:100],
                    "row_count": item.get("row_count") or len(rows),
                    "lineage": item.get("lineage") or {},
                }
            )
        for result in analysis_results:
            for index, table in enumerate(result.get("tables") or [], start=1):
                tables.append(
                    {
                        "table_id": f"{result.get('analysis_result_id') or result.get('result_id')}-T{index:03d}",
                        "title": f"{result.get('tool_name')} 结果",
                        "analysis_result_id": result.get("analysis_result_id")
                        or result.get("result_id"),
                        "rows": table if isinstance(table, list) else [table],
                    }
                )
        return tables

    @staticmethod
    def _charts_from_analysis(
        analysis_results: list[dict],
    ) -> list[dict[str, object]]:
        charts: list[dict[str, object]] = []
        for result in analysis_results:
            result_id = result.get("analysis_result_id") or result.get("result_id")
            for chart in result.get("charts") or []:
                if isinstance(chart, dict):
                    item = dict(chart)
                    item.setdefault("analysis_result_ids", [result_id])
                    charts.append(item)
        if charts:
            return charts
        for result in analysis_results:
            metrics = result.get("metrics") or {}
            if not metrics:
                continue
            result_id = result.get("analysis_result_id") or result.get("result_id")
            points = [
                {"x": key, "y": value}
                for key, value in metrics.items()
                if isinstance(value, (int, float))
            ]
            if points:
                charts.append(
                    {
                        "chart_id": f"{result_id}-metrics",
                        "chart_type": "bar",
                        "title": f"{result.get('tool_name')} 指标",
                        "unit": "metric_value",
                        "points": points,
                        "analysis_result_ids": [result_id],
                    }
                )
        return charts

    @classmethod
    def _evidence_visuals(
        cls,
        evidence: list[object],
        claims: list[dict],
        *,
        sources: list[dict],
        language: str,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        items = [cls._evidence_to_dict(item) for item in evidence]
        items = [item for item in items if item.get("evidence_id")]
        if not items:
            return [], []
        source_titles = {
            str(item.get("source_id")): str(item.get("title") or item.get("source_id"))
            for item in sources
            if item.get("source_id")
        }
        all_source_ids = list(
            dict.fromkeys(
                str(item["source_id"])
                for item in items
                if item.get("source_id")
            )
        )
        all_evidence_ids = [str(item["evidence_id"]) for item in items]
        charts: list[dict[str, object]] = []
        tables: list[dict[str, object]] = []

        evidence_counts = Counter(
            str(item.get("evidence_type") or "unknown") for item in items
        )
        charts.append(
            {
                "chart_id": "research-evidence-distribution",
                "chart_type": "pie",
                "title": (
                    "Evidence Type Distribution"
                    if language == "en"
                    else "证据类型分布"
                ),
                "unit": "items" if language == "en" else "条",
                "points": [
                    {"x": key, "y": value}
                    for key, value in evidence_counts.most_common()
                ],
                "dataset_ids": [],
                "analysis_result_ids": [],
                "source_ids": all_source_ids,
                "evidence_ids": all_evidence_ids,
            }
        )

        source_counts = Counter(
            str(item["source_id"]) for item in items if item.get("source_id")
        )
        source_points = [
            {
                "x": cls._short_label(source_titles.get(source_id, source_id)),
                "y": count,
            }
            for source_id, count in source_counts.most_common(10)
        ]
        if len(source_points) >= 2:
            charts.append(
                {
                    "chart_id": "research-source-evidence-coverage",
                    "chart_type": "bar",
                    "title": (
                        "Top Source Evidence Coverage"
                        if language == "en"
                        else "主要来源证据覆盖量"
                    ),
                    "unit": "items" if language == "en" else "条",
                    "points": source_points,
                    "dataset_ids": [],
                    "analysis_result_ids": [],
                    "source_ids": [
                        source_id
                        for source_id, _count in source_counts.most_common(10)
                    ],
                    "evidence_ids": all_evidence_ids,
                }
            )

        claim_counts = Counter(
            str(cls._claim_to_dict(item).get("claim_type") or "unknown")
            for item in claims
        )
        if len(claim_counts) >= 2:
            charts.append(
                {
                    "chart_id": "research-claim-distribution",
                    "chart_type": "bar",
                    "title": (
                        "Conclusion Type Distribution"
                        if language == "en"
                        else "结构化结论类型分布"
                    ),
                    "unit": "items" if language == "en" else "条",
                    "points": [
                        {"x": key, "y": value}
                        for key, value in claim_counts.most_common()
                    ],
                    "dataset_ids": [],
                    "analysis_result_ids": [],
                    "source_ids": all_source_ids,
                    "evidence_ids": all_evidence_ids,
                }
            )

        numeric_groups: dict[
            tuple[str, str], list[dict[str, object]]
        ] = defaultdict(list)
        for item in items:
            parsed = cls._parse_comparable_number(str(item.get("value") or ""))
            entity = str(item.get("entity") or "").strip()
            attribute = str(item.get("attribute") or "").strip()
            if not parsed or not entity or not attribute:
                continue
            value, unit = parsed
            numeric_groups[(attribute.casefold(), unit)].append(
                {
                    "entity": entity,
                    "attribute": attribute,
                    "value": value,
                    "unit": unit,
                    "raw_value": item.get("value"),
                    "evidence_id": str(item["evidence_id"]),
                    "source_id": str(item.get("source_id") or ""),
                    "source_locator": (
                        f"snapshot:{item['snapshot_id']}"
                        if item.get("snapshot_id")
                        else f"source:{item.get('source_id', '')}"
                    ),
                }
            )

        comparison_groups: list[list[dict[str, object]]] = []
        for group in numeric_groups.values():
            by_entity: dict[str, dict[str, object]] = {}
            for row in group:
                by_entity.setdefault(str(row["entity"]), row)
            rows = list(by_entity.values())
            if len(rows) >= 2:
                comparison_groups.append(rows)
        comparison_groups.sort(
            key=lambda rows: (-len(rows), str(rows[0]["attribute"]))
        )
        for rows in comparison_groups[:6]:
            attribute = str(rows[0]["attribute"])
            unit = str(rows[0]["unit"])
            chart_id = f"evidence-comparison-{cls._safe_id(attribute)}-{cls._safe_id(unit)}"
            total = sum(float(row["value"]) for row in rows)
            chart_type = (
                "pie"
                if unit == "%" and 95 <= total <= 105 and len(rows) <= 8
                else "bar"
            )
            evidence_ids = [str(row["evidence_id"]) for row in rows]
            source_ids = list(
                dict.fromkeys(
                    str(row["source_id"])
                    for row in rows
                    if row.get("source_id")
                )
            )
            charts.append(
                {
                    "chart_id": chart_id,
                    "chart_type": chart_type,
                    "title": attribute,
                    "unit": unit,
                    "points": [
                        {"x": cls._short_label(row["entity"]), "y": row["value"]}
                        for row in rows
                    ],
                    "dataset_ids": [],
                    "analysis_result_ids": [],
                    "source_ids": source_ids,
                    "evidence_ids": evidence_ids,
                }
            )
            tables.append(
                {
                    "table_id": f"{chart_id}-table",
                    "title": attribute,
                    "columns": [
                        "entity",
                        "value",
                        "unit",
                        "raw_value",
                        "evidence_id",
                        "source_id",
                        "source_locator",
                    ],
                    "rows": rows,
                    "row_count": len(rows),
                    "evidence_ids": evidence_ids,
                    "lineage": {
                        "transformation": "deterministic_evidence_numeric_extraction",
                        "source_locator": "evidence:value",
                    },
                }
            )

        scatter_added = False
        for left_index, left_rows in enumerate(comparison_groups):
            if scatter_added:
                break
            left_by_entity = {
                str(row["entity"]): row for row in left_rows
            }
            for right_rows in comparison_groups[left_index + 1 :]:
                right_by_entity = {
                    str(row["entity"]): row for row in right_rows
                }
                common_entities = sorted(
                    set(left_by_entity) & set(right_by_entity)
                )
                if len(common_entities) < 3:
                    continue
                left_attribute = str(left_rows[0]["attribute"])
                right_attribute = str(right_rows[0]["attribute"])
                left_unit = str(left_rows[0]["unit"])
                right_unit = str(right_rows[0]["unit"])
                selected_rows = [
                    (left_by_entity[entity], right_by_entity[entity])
                    for entity in common_entities[:12]
                ]
                charts.append(
                    {
                        "chart_id": (
                            "evidence-positioning-"
                            f"{cls._safe_id(left_attribute)}-"
                            f"{cls._safe_id(right_attribute)}"
                        ),
                        "chart_type": "scatter",
                        "title": f"{left_attribute} / {right_attribute}",
                        "unit": right_unit,
                        "x_label": f"{left_attribute} ({left_unit})",
                        "y_label": f"{right_attribute} ({right_unit})",
                        "points": [
                            {
                                "x": left["value"],
                                "y": right["value"],
                                "series": cls._short_label(left["entity"]),
                            }
                            for left, right in selected_rows
                        ],
                        "dataset_ids": [],
                        "analysis_result_ids": [],
                        "source_ids": list(
                            dict.fromkeys(
                                str(row["source_id"])
                                for pair in selected_rows
                                for row in pair
                                if row.get("source_id")
                            )
                        ),
                        "evidence_ids": list(
                            dict.fromkeys(
                                str(row["evidence_id"])
                                for pair in selected_rows
                                for row in pair
                            )
                        ),
                    }
                )
                scatter_added = True
                break

        heatmap_entities = Counter(
            str(item.get("entity"))
            for item in items
            if item.get("entity")
        ).most_common(8)
        heatmap_types = evidence_counts.most_common(6)
        if len(heatmap_entities) >= 3 and len(heatmap_types) >= 2:
            entity_names = [name for name, _count in heatmap_entities]
            type_names = [name for name, _count in heatmap_types]
            coverage = Counter(
                (
                    str(item.get("entity")),
                    str(item.get("evidence_type") or "unknown"),
                )
                for item in items
            )
            charts.append(
                {
                    "chart_id": "research-entity-evidence-heatmap",
                    "chart_type": "heatmap",
                    "title": (
                        "Entity Evidence Coverage Matrix"
                        if language == "en"
                        else "主要对象证据覆盖矩阵"
                    ),
                    "unit": "items" if language == "en" else "条",
                    "points": [
                        {
                            "x": evidence_type,
                            "y": coverage[(entity, evidence_type)],
                            "series": cls._short_label(entity),
                        }
                        for entity in entity_names
                        for evidence_type in type_names
                    ],
                    "dataset_ids": [],
                    "analysis_result_ids": [],
                    "source_ids": all_source_ids,
                    "evidence_ids": all_evidence_ids,
                }
            )
        return charts, tables

    @staticmethod
    def _parse_comparable_number(value: str) -> tuple[float, str] | None:
        percent = re.search(r"(-?\d[\d,]*(?:\.\d+)?)\s*%", value)
        if percent:
            return float(percent.group(1).replace(",", "")), "%"
        currency = re.search(
            r"(?i)(USD|EUR|CNY|RMB|\$|€|£|¥)\s*"
            r"(-?\d[\d,]*(?:\.\d+)?)\s*"
            r"(billion|million|thousand|bn|[bmk])?",
            value,
        )
        if currency:
            currency_name = {
                "$": "USD",
                "USD": "USD",
                "€": "EUR",
                "EUR": "EUR",
                "£": "GBP",
                "¥": "CNY",
                "CNY": "CNY",
                "RMB": "CNY",
            }[currency.group(1).upper() if currency.group(1).isalpha() else currency.group(1)]
            number = float(currency.group(2).replace(",", ""))
            scale = (currency.group(3) or "").lower()
            basis = ReportIRBuilder._currency_basis(value)
            if basis:
                factor = {
                    "billion": 1_000_000_000,
                    "bn": 1_000_000_000,
                    "b": 1_000_000_000,
                    "million": 1_000_000,
                    "m": 1_000_000,
                    "thousand": 1_000,
                    "k": 1_000,
                    "": 1,
                }[scale]
                return number * factor, f"{currency_name}{basis}"
            factor = {
                "billion": 1000,
                "bn": 1000,
                "b": 1000,
                "million": 1,
                "m": 1,
                "thousand": 0.001,
                "k": 0.001,
                "": 0.000001,
            }[scale]
            return number * factor, f"{currency_name} million"
        count = re.fullmatch(
            r"(?i)\s*(?:over|more than|roughly|about|around|close to|~|超过|约)?\s*"
            r"(-?\d[\d,]*(?:\.\d+)?)\s*(billion|million|thousand|bn|[bmk])?\+?\s*",
            value,
        )
        if count:
            number = float(count.group(1).replace(",", ""))
            scale = (count.group(2) or "").lower()
            factor = {
                "billion": 1_000_000_000,
                "bn": 1_000_000_000,
                "b": 1_000_000_000,
                "million": 1_000_000,
                "m": 1_000_000,
                "thousand": 1_000,
                "k": 1_000,
                "": 1,
            }[scale]
            return number * factor, "count"
        return None

    @staticmethod
    def _currency_basis(value: str) -> str:
        normalized = value.casefold().replace(" ", "")
        patterns = (
            (("permilliontokens", "/milliontokens"), "/million tokens"),
            (("per1mtokens", "/1mtokens"), "/million tokens"),
            (("perusermonthly", "peruser/month", "/user/month"), "/user/month"),
            (("percredit", "/credit"), "/credit"),
            (("perrequest", "/request"), "/request"),
            (("perseat", "/seat"), "/seat"),
            (("/month", "monthly", "每月"), "/month"),
            (("/year", "annual", "yearly", "每年"), "/year"),
        )
        for needles, label in patterns:
            if any(needle in normalized for needle in needles):
                return label
        return ""

    @staticmethod
    def _safe_id(value: object) -> str:
        text = str(value)
        cleaned = re.sub(r"[^A-Za-z0-9]+", "-", text.lower()).strip("-")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
        return f"{(cleaned[:30] or 'value')}-{digest}"

    @staticmethod
    def _short_label(value: object, limit: int = 28) -> str:
        text = str(value).strip()
        return text if len(text) <= limit else text[: limit - 1] + "…"

    @staticmethod
    def _claim_to_dict(item) -> dict[str, object]:
        if isinstance(item, dict):
            return item
        return item.__dict__

    @staticmethod
    def _section_body(
        section: dict, claim_ids: list[str], claims_by_id: dict[str, dict]
    ) -> str:
        body = section.get("body") or section.get("content")
        if body:
            return str(body)
        blocks = section.get("content_blocks")
        if isinstance(blocks, list) and blocks:
            lines = [
                str(block.get("text") or block.get("content") or "").strip()
                for block in blocks
                if isinstance(block, dict)
                and (block.get("text") or block.get("content"))
            ]
            if lines:
                return "\n".join(lines)
        claim_lines = []
        for claim_id in claim_ids:
            claim = claims_by_id.get(claim_id)
            if not claim:
                continue
            statement = str(claim.get("statement") or "").strip()
            if not statement:
                continue
            evidence_ids = claim.get("supporting_evidence_ids") or []
            if evidence_ids:
                statement += " [" + ", ".join(str(item) for item in evidence_ids) + "]"
            claim_lines.append(f"- {statement}")
        return "\n".join(claim_lines) if claim_lines else "- 暂无充分证据。"

    @staticmethod
    def _section_evidence_ids(
        section: dict, claim_ids: list[str], claims_by_id: dict[str, dict]
    ) -> list[str]:
        evidence_ids = [
            str(item) for item in section.get("evidence_ids", []) if item
        ]
        for claim_id in claim_ids:
            claim = claims_by_id.get(claim_id)
            if not claim:
                continue
            for key in ("supporting_evidence_ids", "contradicting_evidence_ids"):
                for evidence_id in claim.get(key, []) or []:
                    evidence_id = str(evidence_id)
                    if evidence_id not in evidence_ids:
                        evidence_ids.append(evidence_id)
        return evidence_ids

    @staticmethod
    def _executive_summary(data: dict) -> list[dict[str, object]]:
        summary = data.get("summary") or []
        if not isinstance(summary, list):
            return []
        return [
            {"text": str(item.get("text") or "")}
            if isinstance(item, dict)
            else {"text": str(item)}
            for item in summary[:8]
            if item and (not isinstance(item, dict) or item.get("text"))
        ]

    @staticmethod
    def _evidence_to_dict(item) -> dict[str, object]:
        if isinstance(item, dict):
            return item
        return item.__dict__

    @staticmethod
    def _latest_analysis_plan(analysis_runs: list[dict]) -> dict[str, object]:
        for run in reversed(analysis_runs):
            plan = run.get("analysis_plan")
            if isinstance(plan, dict) and plan:
                return plan
        return {}

    @staticmethod
    def _limitations_from_plan(plan: dict[str, object]) -> list[str]:
        limitations = [
            str(item)
            for item in plan.get("limitations", [])
            if item
        ]
        for skipped in plan.get("skipped_skills", []) or []:
            if not isinstance(skipped, dict):
                continue
            reason = skipped.get("reason")
            skill_name = skipped.get("skill_name")
            missing = skipped.get("missing_inputs") or []
            if reason and skill_name:
                detail = f"{skill_name}: {reason}"
                if missing:
                    detail += "；缺少：" + ", ".join(str(item) for item in missing)
                limitations.append(detail)
        return list(dict.fromkeys(limitations))
