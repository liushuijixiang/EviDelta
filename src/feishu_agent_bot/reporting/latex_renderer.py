from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from markupsafe import Markup

from .report_ir import ReportIR


TEMPLATE_VERSION = "business_report:2.0"
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class LaTeXRenderer:
    def __init__(self, template_name: str = "business_report.tex.j2") -> None:
        template_dir = Path(__file__).with_name("templates")
        self.environment = Environment(
            loader=FileSystemLoader(template_dir),
            undefined=StrictUndefined,
            autoescape=False,
            keep_trailing_newline=True,
        )
        self.environment.filters.update(
            latex=escape_latex,
            mapping_text=mapping_text,
            render_table=render_longtable,
        )
        self.template = self.environment.get_template(template_name)

    def render(self, ir: ReportIR) -> str:
        evidence_sources = {
            str(item.get("evidence_id")): str(item.get("source_id"))
            for item in ir.evidence_references
            if item.get("evidence_id") and item.get("source_id")
        }
        citation_keys = {
            str(source.get("source_id")): bibliography_key(source)
            for source in ir.sources
            if source.get("source_id")
        }
        tables = {str(item.get("table_id")): item for item in ir.tables}
        charts = {
            str(item.get("chart_id")): self._chart_context(item)
            for item in ir.charts
            if self._safe_artifact_id(item.get("chart_id"))
        }
        sections = []
        language = str(ir.metadata.get("language") or "zh")
        for section in ir.sections:
            section_citations = []
            for evidence_id in section.evidence_ids:
                source_id = evidence_sources.get(str(evidence_id))
                key = citation_keys.get(str(source_id))
                if key and key not in section_citations:
                    section_citations.append(key)
            sections.append(
                {
                    "title": section.title,
                    "body": section.body,
                    "citation_keys": section_citations,
                    "tables": [
                        tables[table_id]
                        for table_id in section.table_ids
                        if table_id in tables
                    ],
                    "charts": [
                        charts[chart_id]
                        for chart_id in section.chart_ids
                        if chart_id in charts
                    ],
                    "language": language,
                }
            )
        return self.template.render(
            title=ir.title,
            subtitle=ir.subtitle,
            version=str(ir.version),
            generated_at=ir.generated_at,
            purpose=ir.purpose,
            template_version=TEMPLATE_VERSION,
            executive_summary=ir.executive_summary,
            sections=sections,
            data_quality=ir.data_quality,
            limitations=ir.limitations,
            appendices=ir.appendices,
            has_bibliography=bool(ir.sources),
        )

    def render_bibliography(self, ir: ReportIR) -> str:
        entries = []
        for source in ir.sources:
            if not source.get("source_id"):
                continue
            key = bibliography_key(source)
            title = escape_bibtex(source.get("title") or source.get("source_id"))
            publisher = escape_bibtex(
                source.get("publisher") or source.get("author") or "Unknown"
            )
            url = safe_bibliography_url(source.get("canonical_url") or source.get("url"))
            published = str(source.get("published_at") or "")
            year_match = re.search(r"\b(19|20)\d{2}\b", published)
            year = year_match.group(0) if year_match else ""
            fields = [f"  title = {{{title}}}", f"  author = {{{publisher}}}"]
            if year:
                fields.append(f"  year = {{{year}}}")
            if url:
                fields.append(f"  howpublished = {{\\url{{{url}}}}}")
            entries.append(f"@misc{{{key},\n" + ",\n".join(fields) + "\n}")
        return "\n\n".join(entries) + ("\n" if entries else "")

    @staticmethod
    def _safe_artifact_id(value: object) -> bool:
        return bool(value and _SAFE_ID.fullmatch(str(value)))

    @staticmethod
    def _chart_context(chart: dict[str, object]) -> dict[str, object]:
        chart_id = str(chart["chart_id"])
        return {
            **chart,
            "chart_id": chart_id,
            "path": f"charts/{chart_id}.png",
        }


def escape_latex(value: object) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in str(value))


def escape_bibtex(value: object) -> str:
    text = " ".join(
        str(value).replace("\\", " ").replace("{", "").replace("}", "").split()
    )
    replacements = {
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
        "_": r"\_",
        "$": r"\$",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in text)


def safe_bibliography_url(value: object) -> str:
    text = str(value or "").strip()
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return text.replace("\\", "").replace("{", "%7B").replace("}", "%7D")


def bibliography_key(source: dict[str, object]) -> str:
    source_id = str(source.get("source_id") or "source")
    slug = re.sub(r"[^A-Za-z0-9]", "_", source_id).strip("_")[:24] or "source"
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:8]
    return f"src_{slug}_{digest}"


def mapping_text(value: object) -> str:
    if isinstance(value, dict):
        return "; ".join(
            f"{key}: {json.dumps(item, ensure_ascii=False, default=str) if isinstance(item, (dict, list)) else item}"
            for key, item in value.items()
        )
    return str(value)


def render_longtable(value: object) -> Markup:
    if not isinstance(value, dict):
        return Markup("")
    rows = value.get("rows")
    if not isinstance(rows, list) or not rows:
        return Markup("")
    columns = value.get("columns")
    if not isinstance(columns, list) or not columns:
        columns = list(rows[0]) if isinstance(rows[0], dict) else []
    if not columns:
        return Markup("")
    alignment = "p{" + str(round(0.92 / len(columns), 3)) + r"\textwidth}"
    lines = [
        r"\begin{longtable}{" + "".join([alignment] * len(columns)) + "}",
        r"\toprule",
        " & ".join(escape_latex(column) for column in columns) + r" \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        " & ".join(escape_latex(column) for column in columns) + r" \\",
        r"\midrule",
        r"\endhead",
    ]
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            " & ".join(escape_latex(row.get(str(column), "")) for column in columns)
            + r" \\"
        )
    lines.extend([r"\bottomrule", r"\end{longtable}"])
    return Markup("\n".join(lines))
