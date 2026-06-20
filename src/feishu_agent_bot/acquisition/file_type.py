from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileTypeDetection:
    file_type: str
    declared_mime_type: str | None
    detected_mime_type: str | None
    extension: str | None
    confidence: float
    method: str
    warning: str | None = None


class FileTypeDetector:
    _EXTENSIONS = {
        ".html": "html",
        ".htm": "html",
        ".pdf": "pdf",
        ".csv": "csv",
        ".tsv": "csv",
        ".xlsx": "excel",
        ".xlsm": "excel",
        ".xls": "excel",
        ".xlsb": "excel",
        ".json": "json",
        ".docx": "docx",
        ".txt": "text",
        ".md": "text",
    }

    _MIME_TYPES = {
        "text/html": "html",
        "application/xhtml+xml": "html",
        "application/pdf": "pdf",
        "text/csv": "csv",
        "application/csv": "csv",
        "text/tab-separated-values": "csv",
        "application/json": "json",
        "application/ld+json": "json",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "excel",
        "application/vnd.ms-excel": "excel",
        "application/vnd.ms-excel.sheet.binary.macroenabled.12": "excel",
        "application/vnd.ms-excel.sheet.macroenabled.12": "excel",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "text/plain": "text",
    }

    @classmethod
    def inspect(
        cls,
        path: str | Path,
        *,
        content_type: str | None = None,
        sample: bytes | None = None,
    ) -> FileTypeDetection:
        suffix = Path(path).suffix.lower()
        normalized = (content_type or "").split(";", 1)[0].strip().lower()
        if normalized in cls._MIME_TYPES:
            mime_type = normalized
            declared_type = cls._MIME_TYPES[normalized]
            if sample:
                magic_type = cls._detect_magic(sample)
                weak_magic = magic_type == "text" and declared_type in {
                    "csv",
                    "html",
                    "json",
                }
                if magic_type and magic_type != declared_type and not weak_magic:
                    return FileTypeDetection(
                        file_type=magic_type,
                        declared_mime_type=normalized,
                        detected_mime_type=cls._mime_for_type(magic_type),
                        extension=suffix or None,
                        confidence=0.85,
                        method="magic_conflict",
                        warning=f"declared MIME {normalized} conflicts with magic {magic_type}",
                    )
            return FileTypeDetection(
                file_type=declared_type,
                declared_mime_type=normalized,
                detected_mime_type=mime_type,
                extension=suffix or None,
                confidence=0.9,
                method="mime",
            )
        if sample:
            magic = cls._detect_magic(sample)
            if magic:
                return FileTypeDetection(
                    file_type=magic,
                    declared_mime_type=normalized or None,
                    detected_mime_type=cls._mime_for_type(magic),
                    extension=suffix or None,
                    confidence=0.8,
                    method="magic",
                )
        if suffix in cls._EXTENSIONS:
            return FileTypeDetection(
                file_type=cls._EXTENSIONS[suffix],
                declared_mime_type=normalized or None,
                detected_mime_type=None,
                extension=suffix,
                confidence=0.55,
                method="extension",
            )
        return FileTypeDetection(
            file_type="unknown",
            declared_mime_type=normalized or None,
            detected_mime_type=None,
            extension=suffix or None,
            confidence=0.0,
            method="unknown",
        )

    @classmethod
    def detect(
        cls,
        path: str | Path,
        *,
        content_type: str | None = None,
        sample: bytes | None = None,
    ) -> str:
        return cls.inspect(path, content_type=content_type, sample=sample).file_type

    @staticmethod
    def _detect_magic(sample: bytes) -> str | None:
        head = sample[:16]
        stripped = sample.lstrip()
        lower = sample[:1024].lower()
        if head.startswith(b"%PDF"):
            return "pdf"
        if head.startswith(b"PK\x03\x04"):
            if b"word/" in sample[:4096]:
                return "docx"
            if b"xl/" in sample[:4096]:
                return "excel"
        if stripped.startswith((b"{", b"[")):
            return "json"
        if b"<html" in lower or b"<table" in lower:
            return "html"
        if b"\x00" not in sample[:1024]:
            return "text"
        return None

    @staticmethod
    def _mime_for_type(file_type: str) -> str | None:
        return {
            "html": "text/html",
            "pdf": "application/pdf",
            "csv": "text/csv",
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "json": "application/json",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text": "text/plain",
        }.get(file_type)
