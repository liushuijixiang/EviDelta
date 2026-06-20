from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import json
from pathlib import Path
from typing import Callable

from .artifact_validator import ArtifactValidator
from .chart_renderer import ChartRenderer
from .excel_renderer import ExcelRenderer
from .latex_renderer import LaTeXRenderer, TEMPLATE_VERSION
from .models import BuiltArtifact
from .pdf_compiler import PDFCompiler
from .report_ir import ReportIR


class ProfessionalArtifactBuilder:
    def __init__(
        self,
        root: str | Path,
        *,
        pdf_enabled: bool = True,
        latex_engine: str = "xelatex",
        latexmk_path: str = "latexmk",
        pdf_timeout_seconds: int = 180,
        pdf_max_output_bytes: int = 100_000_000,
        pdf_template: str = "business_report",
        max_concurrency: int = 2,
        max_chart_concurrency: int = 2,
        max_charts: int = 12,
        max_tables: int = 30,
    ):
        if (
            max_concurrency < 1
            or max_chart_concurrency < 1
            or max_charts < 1
            or max_tables < 1
        ):
            raise ValueError("artifact concurrency and limits must be positive")
        self.root = Path(root)
        self.max_concurrency = max_concurrency
        self.max_charts = max_charts
        self.max_tables = max_tables
        self.validator = ArtifactValidator()
        self.chart_renderer = ChartRenderer(
            self.validator, max_concurrency=max_chart_concurrency
        )
        self.excel_renderer = ExcelRenderer(self.validator)
        if pdf_template != "business_report":
            raise ValueError(f"unsupported PDF template: {pdf_template}")
        self.latex_renderer = LaTeXRenderer(f"{pdf_template}.tex.j2")
        self.pdf_compiler = PDFCompiler(
            timeout_seconds=pdf_timeout_seconds,
            max_output_bytes=pdf_max_output_bytes,
            enabled=pdf_enabled,
            engine=latex_engine,
            latexmk_path=latexmk_path,
            validator=self.validator,
        )

    def build(
        self,
        ir: ReportIR,
        deliverables: list[str],
        *,
        heartbeat: Callable[[str], None] | None = None,
    ) -> list[BuiltArtifact]:
        def beat(phase: str) -> None:
            if heartbeat is not None:
                heartbeat(phase)

        beat("validate_ir")
        self.validate_ir(ir)
        json_artifact, same_report_ir = self.write_report_ir(ir)
        artifacts: list[BuiltArtifact] = [json_artifact]
        beat("render_charts")
        artifacts.extend(
            self.render_charts(
                ir,
                on_chart_completed=lambda chart_id: beat(
                    f"render_chart:{chart_id}"
                ),
            )
        )
        beat("export_datasets")
        artifacts.extend(self.export_datasets(ir))
        render_tasks = []
        if "xlsx" in deliverables:
            render_tasks.append(
                lambda: [
                    self.render_xlsx(ir, reuse_existing=same_report_ir)
                ]
            )
        if "pdf" in deliverables:
            render_tasks.append(
                lambda: [
                    *self.render_latex(ir),
                    self.compile_pdf(ir, reuse_existing=same_report_ir),
                ]
            )
        if render_tasks:
            beat("render_deliverables")
            if self.max_concurrency == 1 or len(render_tasks) == 1:
                rendered = [task() for task in render_tasks]
            else:
                with ThreadPoolExecutor(
                    max_workers=min(self.max_concurrency, len(render_tasks)),
                    thread_name_prefix="artifact",
                ) as executor:
                    rendered = list(executor.map(lambda task: task(), render_tasks))
            for items in rendered:
                artifacts.extend(items)
        beat("write_manifest")
        artifacts.extend(self.write_manifest(ir, artifacts))
        beat("completed")
        return artifacts

    def validate_ir(self, ir: ReportIR) -> None:
        if len(ir.charts) > self.max_charts:
            raise ValueError(f"report has more than {self.max_charts} charts")
        if len(ir.tables) > self.max_tables:
            raise ValueError(f"report has more than {self.max_tables} tables")
        self.validator.validate_report_ir(ir)

    def job_dir(self, ir: ReportIR) -> Path:
        path = self.root / ir.job_id / "professional" / f"v{ir.version}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_report_ir(self, ir: ReportIR) -> tuple[BuiltArtifact, bool]:
        self.validate_ir(ir)
        path = self.job_dir(ir) / "report_ir.json"
        report_ir_text = json.dumps(
            _ir_to_dict(ir), ensure_ascii=False, indent=2
        )
        report_ir_hash = hashlib.sha256(report_ir_text.encode("utf-8")).hexdigest()
        same_report_ir = (
            path.is_file() and self.validator.sha256(path) == report_ir_hash
        )
        if not same_report_ir:
            temporary = path.with_name(f".{path.name}.tmp")
            temporary.write_text(report_ir_text, encoding="utf-8")
            temporary.replace(path)
        return (
            BuiltArtifact(
                "json", path, self.validator.validate_report_ir_json(path)
            ),
            same_report_ir,
        )

    def render_charts(
        self,
        ir: ReportIR,
        *,
        on_chart_completed: Callable[[str], None] | None = None,
    ) -> list[BuiltArtifact]:
        job_dir = self.job_dir(ir)
        reuse_dirs = [
            path / "charts"
            for path in sorted((self.root / ir.job_id / "professional").glob("v*"))
            if path != job_dir and (path / "charts").is_dir()
        ]
        return self.chart_renderer.render_all(
            ir.charts,
            job_dir / "charts",
            ir=ir,
            reuse_dirs=reuse_dirs,
            on_chart_completed=on_chart_completed,
        )

    def export_datasets(self, ir: ReportIR) -> list[BuiltArtifact]:
        return self._write_dataset_exports(ir, self.job_dir(ir) / "datasets")

    def write_manifest(
        self, ir: ReportIR, artifacts: list[BuiltArtifact]
    ) -> list[BuiltArtifact]:
        job_dir = self.job_dir(ir)
        manifest = {
            "job_id": ir.job_id,
            "report_id": ir.report_id,
            "report_version": ir.version,
            "template_version": TEMPLATE_VERSION,
            "generator_version": "professional_artifact_builder:1.0",
            "created_at": ir.generated_at,
            "artifacts": [
                {
                    "artifact_id": hashlib.sha256(
                        f"{ir.report_id}:{item.artifact_type}:{item.path.name}".encode(
                            "utf-8"
                        )
                    ).hexdigest()[:24],
                    "type": item.artifact_type,
                    "artifact_type": item.artifact_type,
                    "path": str(item.path.relative_to(job_dir)),
                    "sha256": item.content_hash,
                    "byte_size": (
                        item.path.stat().st_size
                        if item.status == "ready" and item.path.exists()
                        else 0
                    ),
                    "status": item.status,
                    "validation_status": item.status,
                    "created_at": ir.generated_at,
                    "error": item.error_message,
                }
                for item in artifacts
            ],
        }
        manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2)
        manifest_path = job_dir / "manifest.json"
        legacy_manifest_path = job_dir / "artifact_manifest.json"
        manifest_path.write_text(manifest_text, encoding="utf-8")
        legacy_manifest_path.write_text(manifest_text, encoding="utf-8")
        return [
            BuiltArtifact(
                "manifest",
                manifest_path,
                self.validator.validate_json(manifest_path),
            ),
            BuiltArtifact(
                "artifact_manifest",
                legacy_manifest_path,
                self.validator.validate_json(legacy_manifest_path),
            ),
        ]

    def render_xlsx(
        self, ir: ReportIR, *, reuse_existing: bool = True
    ) -> BuiltArtifact:
        path = self.job_dir(ir) / "report.xlsx"
        if reuse_existing and path.is_file():
            try:
                return BuiltArtifact(
                    "xlsx", path, self.validator.validate_xlsx(path, ir=ir)
                )
            except Exception:
                pass
        return self.excel_renderer.render(ir, path)

    def render_latex(self, ir: ReportIR) -> list[BuiltArtifact]:
        job_dir = self.job_dir(ir)
        latex = self.latex_renderer.render(ir)
        bibliography = self.latex_renderer.render_bibliography(ir)
        latex_path = job_dir / "report.tex"
        bibliography_path = job_dir / "reference.bib"
        latex_path.write_text(latex, encoding="utf-8")
        bibliography_path.write_text(bibliography, encoding="utf-8")
        return [
            BuiltArtifact(
                "latex_source",
                latex_path,
                self.validator.validate_text_artifact(latex_path),
            ),
            BuiltArtifact(
                "bibliography",
                bibliography_path,
                self.validator.validate_text_artifact(bibliography_path),
            ),
        ]

    def compile_pdf(
        self, ir: ReportIR, *, reuse_existing: bool = True
    ) -> BuiltArtifact:
        job_dir = self.job_dir(ir)
        latex = self.latex_renderer.render(ir)
        bibliography = self.latex_renderer.render_bibliography(ir)
        pdf_path = job_dir / "report.pdf"
        input_hash = self._pdf_input_hash(
            latex, bibliography, job_dir / "charts"
        )
        input_hash_path = job_dir / "report.pdf.input.sha256"
        previous_input_hash = (
            input_hash_path.read_text(encoding="ascii").strip()
            if input_hash_path.is_file()
            else ""
        )
        if (
            reuse_existing
            and pdf_path.is_file()
            and previous_input_hash == input_hash
        ):
            try:
                pdf_hash = self.validator.validate_pdf(
                    pdf_path,
                    log_path=(job_dir / "report.log") if (job_dir / "report.log").is_file() else None,
                    template_version=TEMPLATE_VERSION,
                    max_output_bytes=self.pdf_compiler.max_output_bytes,
                )
                pdf_artifact = BuiltArtifact("pdf", pdf_path, pdf_hash)
            except Exception:
                pdf_artifact = self.pdf_compiler.compile(
                    latex,
                    pdf_path,
                    bibliography=bibliography,
                    assets_dir=job_dir / "charts",
                )
        else:
            pdf_artifact = self.pdf_compiler.compile(
                latex,
                pdf_path,
                bibliography=bibliography,
                assets_dir=job_dir / "charts",
            )
        if pdf_artifact.status == "ready":
            input_hash_path.write_text(input_hash, encoding="ascii")
        return pdf_artifact

    @staticmethod
    def _pdf_input_hash(
        latex: str, bibliography: str, charts_dir: Path
    ) -> str:
        digest = hashlib.sha256()
        digest.update(latex.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bibliography.encode("utf-8"))
        if charts_dir.is_dir():
            for path in sorted(charts_dir.iterdir(), key=lambda item: item.name):
                if path.is_file() and path.suffix.lower() in {".png", ".svg"}:
                    digest.update(b"\0")
                    digest.update(path.name.encode("utf-8"))
                    digest.update(b"\0")
                    digest.update(path.read_bytes())
        return digest.hexdigest()

    def _write_dataset_exports(
        self, ir: ReportIR, output_dir: Path
    ) -> list[BuiltArtifact]:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts: list[BuiltArtifact] = []
        exported: set[str] = set()
        for index, table in enumerate(ir.tables, start=1):
            rows = table.get("rows")
            if not isinstance(rows, list):
                continue
            dataset_id = str(
                table.get("dataset_id")
                or table.get("table_id")
                or f"table_{index:03d}"
            )
            safe_id = "".join(
                char if char.isalnum() or char in {"-", "_", "."} else "_"
                for char in dataset_id
            )
            if safe_id in exported:
                continue
            exported.add(safe_id)
            path = output_dir / f"{safe_id}.csv"
            columns: list[str] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                for key in row:
                    if key not in columns:
                        columns.append(str(key))
            with path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=columns or ["value"])
                writer.writeheader()
                for row in rows:
                    if isinstance(row, dict):
                        writer.writerow({column: row.get(column) for column in columns})
            artifacts.append(
                BuiltArtifact(
                    "dataset_csv",
                    path,
                    self.validator.sha256(path),
                )
            )
        return artifacts

def _ir_to_dict(ir: ReportIR) -> dict[str, object]:
    return {
        "job_id": ir.job_id,
        "report_id": ir.report_id,
        "version": ir.version,
        "parent_version_id": ir.parent_version_id,
        "title": ir.title,
        "subtitle": ir.subtitle,
        "purpose": ir.purpose,
        "generated_at": ir.generated_at,
        "methodology": ir.methodology,
        "executive_summary": ir.executive_summary,
        "sections": [section.__dict__ for section in ir.sections],
        "tables": ir.tables,
        "charts": ir.charts,
        "analysis_results": ir.analysis_results,
        "change_events": ir.change_events,
        "claims": ir.claims,
        "evidence_references": ir.evidence_references,
        "data_quality": ir.data_quality,
        "limitations": ir.limitations,
        "sources": ir.sources,
        "appendices": [section.__dict__ for section in ir.appendices],
        "metadata": ir.metadata,
    }
