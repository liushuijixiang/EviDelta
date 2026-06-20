from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BuiltArtifact:
    artifact_type: str
    path: Path
    content_hash: str
    status: str = "ready"
    error_message: str | None = None
