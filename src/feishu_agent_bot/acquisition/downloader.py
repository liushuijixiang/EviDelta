from __future__ import annotations

import hashlib
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import httpx

from ..errors import AssetTooLargeError as _AssetTooLargeError
from ..errors import UnsupportedFileTypeError as _UnsupportedFileTypeError
from ..research.url_safety import validate_public_url
from .asset_store import AssetStore
from .file_type import FileTypeDetector
from .models import DownloadedAsset, SourceAsset
from .safety import validate_office_archive


class AssetTooLargeError(_AssetTooLargeError, ValueError):
    pass


class UnsupportedAssetTypeError(_UnsupportedFileTypeError, ValueError):
    pass


class AssetDownloader:
    DEFAULT_LIMITS = {
        "html": 3_000_000,
        "pdf": 50_000_000,
        "csv": 50_000_000,
        "excel": 50_000_000,
        "json": 20_000_000,
        "docx": 30_000_000,
        "text": 10_000_000,
        "unknown": 5_000_000,
    }

    def __init__(
        self,
        store: AssetStore,
        *,
        timeout_seconds: float = 60,
        max_redirects: int = 5,
        max_bytes_by_type: dict[str, int] | None = None,
        archive_max_entries: int = 10_000,
        archive_max_uncompressed_bytes: int = 500_000_000,
        archive_max_compression_ratio: float = 500.0,
        client: httpx.Client | None = None,
        url_validator=validate_public_url,
    ):
        self.store = store
        self.max_redirects = max_redirects
        self.max_bytes_by_type = {**self.DEFAULT_LIMITS, **(max_bytes_by_type or {})}
        self.archive_max_entries = archive_max_entries
        self.archive_max_uncompressed_bytes = archive_max_uncompressed_bytes
        self.archive_max_compression_ratio = archive_max_compression_ratio
        self.client = client or httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=False,
            headers={"User-Agent": "evidelta/0.1 asset-downloader"},
        )
        self.url_validator = url_validator

    def download(
        self,
        *,
        job_id: str,
        url: str,
        source_id: str | None = None,
        snapshot_id: str | None = None,
        published_at: str | None = None,
    ) -> DownloadedAsset:
        requested = self.url_validator(url)
        current = requested
        temporary = self.store.temporary_path(job_id)
        digest = hashlib.sha256()
        total = 0
        headers: dict[str, str] = {}
        status_code = 0
        try:
            for _ in range(self.max_redirects + 1):
                with self.client.stream("GET", current) as response:
                    status_code = response.status_code
                    headers = dict(response.headers)
                    if response.is_redirect:
                        location = response.headers.get("location")
                        if not location:
                            raise httpx.HTTPError("redirect missing Location")
                        current = self.url_validator(urljoin(current, location))
                        continue
                    response.raise_for_status()
                    declared = response.headers.get("content-type")
                    sample = bytearray()
                    with temporary.open("wb") as file:
                        for chunk in response.iter_bytes():
                            if not chunk:
                                continue
                            if len(sample) < 4096:
                                sample.extend(chunk[: 4096 - len(sample)])
                            total += len(chunk)
                            provisional = FileTypeDetector.detect(
                                current,
                                content_type=declared,
                                sample=bytes(sample),
                            )
                            if total > self.max_bytes_by_type.get(provisional, self.max_bytes_by_type["unknown"]):
                                raise AssetTooLargeError(f"{provisional} asset exceeds size limit")
                            digest.update(chunk)
                            file.write(chunk)
                    break
            else:
                raise httpx.TooManyRedirects("too many redirects")
            sha256 = digest.hexdigest()
            detection = FileTypeDetector.inspect(
                current,
                content_type=headers.get("content-type"),
                sample=temporary.read_bytes()[:4096],
            )
            if detection.file_type == "unknown":
                raise UnsupportedAssetTypeError("unsupported or unknown asset type")
            if detection.file_type in {"docx", "excel"}:
                validate_office_archive(
                    temporary,
                    detection.file_type,
                    max_entries=self.archive_max_entries,
                    max_uncompressed_bytes=self.archive_max_uncompressed_bytes,
                    max_compression_ratio=self.archive_max_compression_ratio,
                )
            asset_id = f"{job_id}-{sha256[:16]}"
            final_path = self.store.final_path(job_id, asset_id, detection.extension)
            if final_path.exists():
                temporary.unlink(missing_ok=True)
            else:
                os.replace(temporary, final_path)
            asset = SourceAsset(
                asset_id=asset_id,
                job_id=job_id,
                source_id=source_id,
                snapshot_id=snapshot_id,
                original_url=requested,
                canonical_url=current,
                generated_filename=final_path.name,
                original_filename=_filename_from_url(current),
                declared_mime_type=detection.declared_mime_type,
                detected_mime_type=detection.detected_mime_type,
                file_extension=detection.extension,
                byte_size=total,
                sha256=sha256,
                retrieved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                published_at=published_at,
                raw_object_path=final_path,
                source_type=detection.file_type,
                detection_confidence=detection.confidence,
                detection_method=detection.method,
                error_message=detection.warning,
            )
            return DownloadedAsset(asset=asset, headers=headers, http_status=status_code)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise


def _filename_from_url(url: str) -> str | None:
    name = Path(unquote(urlsplit(url).path)).name
    return name or None
