from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import matplotlib
import numpy as np

matplotlib.use("Agg")
from matplotlib import font_manager, rcParams  # noqa: E402
from matplotlib.figure import Figure  # noqa: E402

from .artifact_validator import ArtifactValidator
from .models import BuiltArtifact
from .report_ir import ReportIR


class ChartRenderer:
    SUPPORTED_TYPES = {
        "bar",
        "pie",
        "line",
        "stacked_bar",
        "scatter",
        "heatmap",
        "timeline",
        "waterfall",
    }

    GENERATOR_VERSION = "1.2"

    def __init__(
        self,
        validator: ArtifactValidator | None = None,
        *,
        max_concurrency: int = 2,
    ):
        if max_concurrency < 1:
            raise ValueError("chart concurrency must be positive")
        self.validator = validator or ArtifactValidator()
        self.max_concurrency = max_concurrency
        self._configure_fonts()

    def render_all(
        self,
        charts: list[dict[str, object]],
        output_dir: str | Path,
        *,
        ir: ReportIR | None = None,
        reuse_dirs: list[str | Path] | None = None,
        on_chart_completed: Callable[[str], None] | None = None,
    ) -> list[BuiltArtifact]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        known = self._known_reference_ids(ir)
        tasks: list[tuple[int, str, dict[str, object], str, list[object]]] = []
        for index, chart in enumerate(charts, start=1):
            chart_id = str(chart.get("chart_id") or f"chart_{index:03d}")
            chart_type = str(chart.get("chart_type") or chart.get("type") or "bar")
            points = list(chart.get("points") or chart.get("data") or [])
            unit = str(chart.get("unit") or "").strip()
            if (
                chart_type not in self.SUPPORTED_TYPES
                or not unit
                or not self._normalize_points(points)
            ):
                continue
            tasks.append((index, chart_id, chart, chart_type, points))
        chart_ids = [task[1] for task in tasks]
        if len(chart_ids) != len(set(chart_ids)):
            raise ValueError("chart IDs must be unique")

        def render_one(
            task: tuple[int, str, dict[str, object], str, list[object]],
        ) -> list[BuiltArtifact]:
            _, chart_id, chart, chart_type, points = task
            return self.render(
                chart,
                output_dir,
                chart_id=chart_id,
                chart_type=chart_type,
                points=points,
                dataset_ids=known["dataset_ids"],
                analysis_result_ids=known["analysis_result_ids"],
                source_ids=known["source_ids"],
                generated_at=ir.generated_at if ir is not None else None,
                reuse_dirs=reuse_dirs,
            )

        completed: dict[int, list[BuiltArtifact]] = {}
        errors: list[BaseException] = []
        if self.max_concurrency == 1 or len(tasks) <= 1:
            for task in tasks:
                try:
                    completed[task[0]] = render_one(task)
                    if on_chart_completed is not None:
                        on_chart_completed(task[1])
                except BaseException as exc:
                    errors.append(exc)
        else:
            with ThreadPoolExecutor(
                max_workers=min(self.max_concurrency, len(tasks)),
                thread_name_prefix="chart",
            ) as executor:
                futures = {executor.submit(render_one, task): task for task in tasks}
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        completed[task[0]] = future.result()
                        if on_chart_completed is not None:
                            on_chart_completed(task[1])
                    except BaseException as exc:
                        errors.append(exc)
        if errors:
            raise errors[0]
        return [artifact for index in sorted(completed) for artifact in completed[index]]

    def render(
        self,
        chart: dict[str, object],
        output_dir: Path,
        *,
        chart_id: str,
        chart_type: str,
        points: list[object],
        dataset_ids: set[str] | None = None,
        analysis_result_ids: set[str] | None = None,
        source_ids: set[str] | None = None,
        generated_at: str | None = None,
        reuse_dirs: list[str | Path] | None = None,
    ) -> list[BuiltArtifact]:
        title = str(chart.get("title") or chart_id)
        unit = str(chart.get("unit") or "")
        png_path = output_dir / f"{chart_id}.png"
        svg_path = output_dir / f"{chart_id}.svg"
        csv_path = output_dir / f"{chart_id}_data.csv"
        metadata_path = output_dir / f"{chart_id}_metadata.json"

        rows = self._normalize_points(points)
        input_hash = self._input_hash(chart, chart_id, chart_type, title, unit, rows)
        validation_args = {
            "dataset_ids": dataset_ids,
            "analysis_result_ids": analysis_result_ids,
            "source_ids": source_ids,
        }
        existing = self._validated_bundle(
            png_path, svg_path, csv_path, metadata_path, input_hash, **validation_args
        )
        if existing is not None:
            return existing
        for reuse_dir in reuse_dirs or []:
            reused = self._reuse_bundle(
                Path(reuse_dir),
                output_dir,
                chart_id,
                input_hash,
                **validation_args,
            )
            if reused is not None:
                return reused

        temporary = {
            "png": self._temporary_path(png_path),
            "svg": self._temporary_path(svg_path),
            "csv": self._temporary_path(csv_path),
            "metadata": self._temporary_path(metadata_path),
        }
        try:
            self._write_data_csv(temporary["csv"], rows)
            self._write_metadata(
                temporary["metadata"], chart, chart_id, chart_type, title, unit,
                input_hash=input_hash, generated_at=generated_at,
            )
            self._plot(
                chart,
                chart_type,
                title,
                unit,
                rows,
                temporary["png"],
                temporary["svg"],
            )
            self.validator.validate_chart_bundle(
                png_path=temporary["png"],
                svg_path=temporary["svg"],
                csv_path=temporary["csv"],
                metadata_path=temporary["metadata"],
                **validation_args,
            )
            for key, destination in (
                ("png", png_path), ("svg", svg_path), ("csv", csv_path),
                ("metadata", metadata_path),
            ):
                os.replace(temporary[key], destination)
        finally:
            for path in temporary.values():
                path.unlink(missing_ok=True)
        hashes = self.validator.validate_chart_bundle(
            png_path=png_path,
            svg_path=svg_path,
            csv_path=csv_path,
            metadata_path=metadata_path,
            dataset_ids=dataset_ids,
            analysis_result_ids=analysis_result_ids,
            source_ids=source_ids,
        )

        return [
            BuiltArtifact("chart_png", png_path, hashes["png"]),
            BuiltArtifact("chart_svg", svg_path, hashes["svg"]),
            BuiltArtifact("chart_data_csv", csv_path, hashes["csv"]),
            BuiltArtifact("chart_metadata_json", metadata_path, hashes["metadata"]),
        ]

    @staticmethod
    def _normalize_points(points: list[object]) -> list[dict[str, object]]:
        rows = []
        for item in points:
            if isinstance(item, dict):
                rows.append({"x": item.get("x"), "y": item.get("y"), "series": item.get("series")})
        return [row for row in rows if row["x"] is not None and row["y"] is not None]

    @staticmethod
    def _write_data_csv(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["x", "y", "series"])
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _write_metadata(
        path: Path,
        chart: dict[str, object],
        chart_id: str,
        chart_type: str,
        title: str,
        unit: str,
        *,
        input_hash: str,
        generated_at: str | None,
    ) -> None:
        metadata = {
            "chart_id": chart_id,
            "chart_type": chart_type,
            "title": title,
            "unit": unit,
            "dataset_ids": chart.get("dataset_ids") or [],
            "analysis_result_ids": chart.get("analysis_result_ids") or [],
            "source_ids": chart.get("source_ids") or [],
            "generated_at": generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "generator_version": ChartRenderer.GENERATOR_VERSION,
            "input_hash": input_hash,
        }
        path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    def _plot(
        self,
        chart: dict[str, object],
        chart_type: str,
        title: str,
        unit: str,
        rows: list[dict[str, object]],
        png_path: Path,
        svg_path: Path,
    ) -> None:
        fig = Figure(figsize=(8, 4.5))
        ax = fig.subplots()
        x_values = [str(row["x"]) for row in rows]
        y_values = [float(row["y"]) for row in rows]
        if chart_type in {"line", "timeline"}:
            ax.plot(x_values, y_values, marker="o")
        elif chart_type == "pie":
            positive = [
                (label, value)
                for label, value in zip(x_values, y_values)
                if value > 0
            ]
            labels = [item[0] for item in positive]
            values = [item[1] for item in positive]
            ax.pie(
                values,
                labels=labels,
                autopct="%1.1f%%",
                startangle=90,
                counterclock=False,
            )
            ax.axis("equal")
        elif chart_type == "scatter":
            try:
                numeric_x = [float(row["x"]) for row in rows]
            except (TypeError, ValueError):
                numeric_x = list(range(len(rows)))
                ax.set_xticks(numeric_x, labels=x_values)
            ax.scatter(numeric_x, y_values)
            for x_value, y_value, row in zip(numeric_x, y_values, rows):
                label = str(row.get("series") or "")
                if label:
                    ax.annotate(label, (x_value, y_value), xytext=(4, 4), textcoords="offset points")
        elif chart_type == "stacked_bar":
            series_names = list(dict.fromkeys(str(row.get("series") or "value") for row in rows))
            labels = list(dict.fromkeys(x_values))
            bottoms = np.zeros(len(labels))
            for series in series_names:
                values_by_x = {
                    str(row["x"]): float(row["y"])
                    for row in rows
                    if str(row.get("series") or "value") == series
                }
                values = np.array([values_by_x.get(label, 0.0) for label in labels])
                ax.bar(labels, values, bottom=bottoms, label=series)
                bottoms += values
            if len(series_names) > 1:
                ax.legend()
        elif chart_type == "heatmap":
            row_labels = list(
                dict.fromkeys(str(row.get("series") or "value") for row in rows)
            )
            column_labels = list(dict.fromkeys(x_values))
            values = {
                (str(row.get("series") or "value"), str(row["x"])): float(row["y"])
                for row in rows
            }
            matrix = np.array(
                [
                    [values.get((row_label, column_label), np.nan) for column_label in column_labels]
                    for row_label in row_labels
                ]
            )
            image = ax.imshow(matrix, aspect="auto")
            ax.set_xticks(range(len(column_labels)), labels=column_labels)
            ax.set_yticks(range(len(row_labels)), labels=row_labels)
            fig.colorbar(image, ax=ax, label=unit or None)
        elif chart_type == "waterfall":
            cumulative = []
            total = 0.0
            for value in y_values:
                total += value
                cumulative.append(total)
            ax.bar(x_values, cumulative)
        else:
            ax.bar(x_values, y_values)
        if chart_type not in {"heatmap", "pie"}:
            if all(value >= 0 for value in y_values):
                ax.set_ylim(bottom=0)
            elif all(value <= 0 for value in y_values):
                ax.set_ylim(top=0)
        ax.set_title(title)
        if unit and chart_type != "pie":
            ax.set_ylabel(unit)
        if chart.get("x_label") and chart_type != "pie":
            ax.set_xlabel(str(chart["x_label"]))
        if chart.get("y_label") and chart_type != "pie":
            ax.set_ylabel(str(chart["y_label"]))
        if chart_type != "pie":
            ax.tick_params(axis="x", rotation=30)
        fig.tight_layout()
        fig.savefig(png_path, dpi=160, format="png")
        fig.savefig(svg_path, format="svg")

    @classmethod
    def _input_hash(
        cls,
        chart: dict[str, object],
        chart_id: str,
        chart_type: str,
        title: str,
        unit: str,
        rows: list[dict[str, object]],
    ) -> str:
        payload = {
            "chart_id": chart_id,
            "chart_type": chart_type,
            "title": title,
            "unit": unit,
            "rows": rows,
            "dataset_ids": chart.get("dataset_ids") or [],
            "analysis_result_ids": chart.get("analysis_result_ids") or [],
            "source_ids": chart.get("source_ids") or [],
            "evidence_ids": chart.get("evidence_ids") or [],
            "x_label": chart.get("x_label"),
            "y_label": chart.get("y_label"),
            "generator_version": cls.GENERATOR_VERSION,
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def _validated_bundle(
        self,
        png_path: Path,
        svg_path: Path,
        csv_path: Path,
        metadata_path: Path,
        input_hash: str,
        **validation_args: object,
    ) -> list[BuiltArtifact] | None:
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("input_hash") != input_hash:
                return None
            hashes = self.validator.validate_chart_bundle(
                png_path=png_path,
                svg_path=svg_path,
                csv_path=csv_path,
                metadata_path=metadata_path,
                **validation_args,
            )
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        return self._artifacts(png_path, svg_path, csv_path, metadata_path, hashes)

    def _reuse_bundle(
        self,
        source_dir: Path,
        output_dir: Path,
        chart_id: str,
        input_hash: str,
        **validation_args: object,
    ) -> list[BuiltArtifact] | None:
        source_paths = {
            "png": source_dir / f"{chart_id}.png",
            "svg": source_dir / f"{chart_id}.svg",
            "csv": source_dir / f"{chart_id}_data.csv",
            "metadata": source_dir / f"{chart_id}_metadata.json",
        }
        validated = self._validated_bundle(
            source_paths["png"], source_paths["svg"], source_paths["csv"],
            source_paths["metadata"], input_hash, **validation_args,
        )
        if validated is None:
            return None
        destinations = {
            "png": output_dir / f"{chart_id}.png",
            "svg": output_dir / f"{chart_id}.svg",
            "csv": output_dir / f"{chart_id}_data.csv",
            "metadata": output_dir / f"{chart_id}_metadata.json",
        }
        for key, source in source_paths.items():
            temporary = self._temporary_path(destinations[key])
            try:
                shutil.copy2(source, temporary)
                os.replace(temporary, destinations[key])
            finally:
                temporary.unlink(missing_ok=True)
        return self._validated_bundle(
            destinations["png"], destinations["svg"], destinations["csv"],
            destinations["metadata"], input_hash, **validation_args,
        )

    @staticmethod
    def _temporary_path(path: Path) -> Path:
        return path.with_name(
            f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )

    @staticmethod
    def _artifacts(
        png_path: Path,
        svg_path: Path,
        csv_path: Path,
        metadata_path: Path,
        hashes: dict[str, str],
    ) -> list[BuiltArtifact]:
        return [
            BuiltArtifact("chart_png", png_path, hashes["png"]),
            BuiltArtifact("chart_svg", svg_path, hashes["svg"]),
            BuiltArtifact("chart_data_csv", csv_path, hashes["csv"]),
            BuiltArtifact("chart_metadata_json", metadata_path, hashes["metadata"]),
        ]

    @staticmethod
    def _configure_fonts() -> None:
        candidates = [
            "Noto Sans CJK SC",
            "Noto Sans CJK JP",
            "WenQuanYi Zen Hei",
            "SimHei",
            "Microsoft YaHei",
            "DejaVu Sans",
        ]
        available = {font.name for font in font_manager.fontManager.ttflist}
        for candidate in candidates:
            if candidate in available:
                rcParams["font.sans-serif"] = [candidate, "DejaVu Sans"]
                rcParams["axes.unicode_minus"] = False
                return

    @staticmethod
    def _known_reference_ids(ir: ReportIR | None) -> dict[str, set[str] | None]:
        if ir is None:
            return {
                "dataset_ids": None,
                "analysis_result_ids": None,
                "source_ids": None,
            }
        dataset_ids = {
            str(table.get("dataset_id"))
            for table in ir.tables
            if table.get("dataset_id")
        }
        for item in ir.data_quality:
            if item.get("dataset_id"):
                dataset_ids.add(str(item["dataset_id"]))
        for result in ir.analysis_results:
            for dataset_id in result.get("input_dataset_ids") or []:
                dataset_ids.add(str(dataset_id))
            for table in result.get("tables") or []:
                if isinstance(table, dict) and table.get("dataset_id"):
                    dataset_ids.add(str(table["dataset_id"]))
        analysis_result_ids = {
            str(result.get("analysis_result_id") or result.get("result_id"))
            for result in ir.analysis_results
            if result.get("analysis_result_id") or result.get("result_id")
        }
        source_ids = {
            str(source.get("source_id"))
            for source in ir.sources
            if source.get("source_id")
        }
        for evidence in ir.evidence_references:
            if evidence.get("source_id"):
                source_ids.add(str(evidence["source_id"]))
        return {
            "dataset_ids": dataset_ids,
            "analysis_result_ids": analysis_result_ids,
            "source_ids": source_ids,
        }
