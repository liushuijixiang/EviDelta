from __future__ import annotations

import os
import tempfile
from pathlib import Path


class AssetStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def asset_dir(self, job_id: str) -> Path:
        path = self.root / job_id / "assets"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def temporary_path(self, job_id: str) -> Path:
        directory = self.asset_dir(job_id)
        fd, name = tempfile.mkstemp(prefix=".download-", suffix=".tmp", dir=directory)
        os.close(fd)
        return Path(name)

    def final_path(self, job_id: str, asset_id: str, extension: str | None) -> Path:
        safe_extension = extension if extension and extension.startswith(".") else ".bin"
        return self.asset_dir(job_id) / f"{asset_id}{safe_extension.lower()}"

    def existing_hash_path(self, job_id: str, sha256: str) -> Path | None:
        directory = self.asset_dir(job_id)
        for path in directory.iterdir():
            if path.is_file() and path.name.startswith(sha256[:16]):
                return path
        return None
