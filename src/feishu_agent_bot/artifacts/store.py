from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class ArtifactStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write_report(
        self, job_id: str, version: int, markdown: str, report: dict[str, Any]
    ) -> tuple[Path, Path]:
        job_dir = self.root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        markdown_path = job_dir / f"report_v{version}.md"
        json_path = job_dir / f"report_v{version}.json"
        self._atomic_write(markdown_path, markdown)
        self._atomic_write(
            json_path, json.dumps(report, ensure_ascii=False, indent=2)
        )
        return markdown_path, json_path

    def write_pending_report(
        self, job_id: str, patch_id: str, markdown: str, report: dict[str, Any]
    ) -> tuple[Path, Path]:
        job_dir = self.root / job_id / "pending"
        job_dir.mkdir(parents=True, exist_ok=True)
        markdown_path = job_dir / f"{patch_id}.md"
        json_path = job_dir / f"{patch_id}.json"
        self._atomic_write(markdown_path, markdown)
        self._atomic_write(
            json_path, json.dumps(report, ensure_ascii=False, indent=2)
        )
        return markdown_path, json_path

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        fd, temporary = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, path)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
