from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from .artifact_validator import ArtifactValidationError, ArtifactValidator
from .latex_renderer import TEMPLATE_VERSION
from .models import BuiltArtifact


class PDFCompiler:
    def __init__(
        self,
        timeout_seconds: int = 180,
        max_output_bytes: int = 100_000_000,
        enabled: bool = True,
        engine: str = "xelatex",
        latexmk_path: str = "latexmk",
        validator: ArtifactValidator | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self.enabled = enabled
        if Path(engine).name != "xelatex":
            raise ValueError("LATEX_ENGINE must resolve to xelatex")
        if Path(latexmk_path).name != "latexmk":
            raise ValueError("LATEXMK_PATH must resolve to latexmk")
        self.engine = engine
        self.latexmk_path = latexmk_path
        self.validator = validator or ArtifactValidator()

    def compile(
        self,
        latex: str,
        path: str | Path,
        *,
        bibliography: str = "",
        assets_dir: str | Path | None = None,
    ) -> BuiltArtifact:
        path = Path(path)
        if not self.enabled:
            return BuiltArtifact(
                "pdf", path, "", status="unavailable", error_message="PDF disabled"
            )
        missing = [
            command
            for command in (self.engine, self.latexmk_path, "bibtex")
            if not shutil.which(command)
        ]
        if missing:
            return BuiltArtifact(
                "pdf",
                path,
                "",
                status="unavailable",
                error_message="missing " + ", ".join(missing),
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        log_path = path.with_suffix(".log")
        with tempfile.TemporaryDirectory(prefix="report-pdf-") as directory:
            workdir = Path(directory)
            (workdir / "report.tex").write_text(latex, encoding="utf-8")
            (workdir / "reference.bib").write_text(
                bibliography, encoding="utf-8"
            )
            self._copy_assets(assets_dir, workdir / "charts")
            commands = self._commands(
                has_bibliography=bool(bibliography.strip()),
                engine=self.engine,
                latexmk_path=self.latexmk_path,
            )
            output_parts: list[str] = []
            try:
                for command in commands:
                    completed = subprocess.run(
                        command,
                        cwd=workdir,
                        check=True,
                        timeout=self.timeout_seconds,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        env=self._safe_environment(workdir),
                    )
                    output_parts.append(
                        completed.stdout.decode("utf-8", errors="replace")
                    )
            except subprocess.TimeoutExpired as exc:
                output_parts.append(
                    (exc.stdout or b"").decode("utf-8", errors="replace")
                    if isinstance(exc.stdout, bytes)
                    else str(exc.stdout or "")
                )
                self._persist_log(workdir, log_path, output_parts)
                return BuiltArtifact(
                    "pdf",
                    path,
                    "",
                    status="failed",
                    error_message="PDF compilation timed out",
                )
            except subprocess.CalledProcessError as exc:
                output_parts.append(
                    (exc.stdout or b"").decode("utf-8", errors="replace")
                )
                self._persist_log(workdir, log_path, output_parts)
                return BuiltArtifact(
                    "pdf",
                    path,
                    "",
                    status="failed",
                    error_message=(
                        "PDF compilation failed: " + output_parts[-1]
                    )[-2000:],
                )

            generated = workdir / "report.pdf"
            self._persist_log(workdir, log_path, output_parts)
            try:
                if not generated.is_file():
                    raise ArtifactValidationError("PDF compiler produced no file")
                if generated.stat().st_size > self.max_output_bytes:
                    raise ArtifactValidationError(
                        "PDF artifact exceeds configured output size"
                    )
                content_hash = self.validator.validate_pdf(
                    generated,
                    log_path=log_path,
                    template_version=TEMPLATE_VERSION,
                    max_output_bytes=self.max_output_bytes,
                )
            except ArtifactValidationError as exc:
                return BuiltArtifact(
                    "pdf", path, "", status="failed", error_message=str(exc)
                )
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
            )
            os.close(fd)
            temp_target = Path(temp_name)
            try:
                shutil.copyfile(generated, temp_target)
                os.replace(temp_target, path)
            finally:
                temp_target.unlink(missing_ok=True)
        return BuiltArtifact("pdf", path, content_hash)

    @staticmethod
    def _commands(
        *,
        has_bibliography: bool,
        engine: str = "xelatex",
        latexmk_path: str = "latexmk",
    ) -> list[list[str]]:
        xelatex = [
            engine,
            "-interaction=nonstopmode",
            "-halt-on-error",
            "-no-shell-escape",
            "report.tex",
        ]
        commands = [xelatex.copy(), xelatex.copy()]
        if has_bibliography:
            commands.append(["bibtex", "report"])
        commands.append(xelatex.copy())
        # latexmk performs a final dependency check and only reruns commands when
        # the explicit sequence left references out of date.
        commands.append(
            [
                latexmk_path,
                "-xelatex",
                "-interaction=nonstopmode",
                "-halt-on-error",
                "-no-shell-escape",
                "report.tex",
            ]
        )
        return commands

    @staticmethod
    def _safe_environment(workdir: Path) -> dict[str, str]:
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(workdir),
            "TMPDIR": str(workdir),
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
            "SOURCE_DATE_EPOCH": "0",
        }
        return environment

    @staticmethod
    def _copy_assets(source: str | Path | None, target: Path) -> None:
        if source is None:
            return
        source_path = Path(source)
        if not source_path.is_dir():
            return
        target.mkdir(parents=True, exist_ok=True)
        for item in source_path.iterdir():
            if (
                item.is_file()
                and not item.is_symlink()
                and item.suffix.lower() in {".png", ".svg"}
            ):
                shutil.copyfile(item, target / item.name)

    @staticmethod
    def _persist_log(
        workdir: Path, destination: Path, output_parts: list[str]
    ) -> None:
        compiler_log = workdir / "report.log"
        text = compiler_log.read_text(encoding="utf-8", errors="replace") if compiler_log.exists() else ""
        destination.write_text(
            "\n\n".join(output_parts + ([text] if text else [])),
            encoding="utf-8",
        )
