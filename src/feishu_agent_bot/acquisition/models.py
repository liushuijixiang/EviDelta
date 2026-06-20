from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SourceAsset:
    asset_id: str
    job_id: str
    source_id: str | None
    original_url: str
    canonical_url: str
    generated_filename: str
    original_filename: str | None
    declared_mime_type: str | None
    detected_mime_type: str | None
    file_extension: str | None
    byte_size: int
    sha256: str
    retrieved_at: str
    raw_object_path: Path | None = None
    source_type: str = "download"
    snapshot_id: str | None = None
    published_at: str | None = None
    parse_status: str = "downloaded"
    parser_name: str | None = None
    parser_version: str | None = None
    detection_confidence: float | None = None
    detection_method: str | None = None
    error_message: str | None = None

    @property
    def url(self) -> str:
        return self.original_url

    @property
    def file_name(self) -> str:
        return self.generated_filename

    @property
    def content_type(self) -> str | None:
        return self.declared_mime_type

    @property
    def file_type(self) -> str:
        return FileType.from_mime_or_extension(
            self.detected_mime_type or self.declared_mime_type,
            self.file_extension,
        )

    @property
    def content_hash(self) -> str:
        return self.sha256

    @property
    def size_bytes(self) -> int:
        return self.byte_size

    @property
    def local_path(self) -> Path | None:
        return self.raw_object_path

    @property
    def status(self) -> str:
        return self.parse_status


class FileType:
    @staticmethod
    def from_mime_or_extension(mime_type: str | None, extension: str | None) -> str:
        normalized = (mime_type or "").split(";", 1)[0].strip().lower()
        if normalized == "application/pdf":
            return "pdf"
        if normalized in {"text/csv", "application/csv", "text/tab-separated-values"}:
            return "csv"
        if normalized in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
            "application/vnd.ms-excel.sheet.binary.macroenabled.12",
        }:
            return "excel"
        if normalized in {"application/json", "application/ld+json"}:
            return "json"
        if normalized == (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ):
            return "docx"
        if normalized in {"text/html", "application/xhtml+xml"}:
            return "html"
        if normalized.startswith("text/"):
            return "text"
        return {
            ".html": "html",
            ".htm": "html",
            ".pdf": "pdf",
            ".csv": "csv",
            ".tsv": "csv",
            ".xlsx": "excel",
            ".xls": "excel",
            ".xlsb": "excel",
            ".xlsm": "excel",
            ".json": "json",
            ".docx": "docx",
            ".txt": "text",
            ".md": "text",
        }.get((extension or "").lower(), "unknown")


@dataclass(frozen=True)
class DownloadedAsset:
    asset: SourceAsset
    headers: dict[str, str]
    http_status: int


@dataclass(frozen=True)
class DiscoveryResult:
    job_id: str
    assets: list[SourceAsset] = field(default_factory=list)
    skipped_urls: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
