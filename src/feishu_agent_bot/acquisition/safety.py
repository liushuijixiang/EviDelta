from __future__ import annotations

from pathlib import Path, PurePosixPath
import stat
import zipfile

from ..errors import UnsafeAssetError as _UnsafeAssetError


class UnsafeArchiveError(_UnsafeAssetError, ValueError):
    """An Office ZIP container exceeds safe structural limits."""


def validate_office_archive(
    path: str | Path,
    file_type: str,
    *,
    max_entries: int = 10_000,
    max_uncompressed_bytes: int = 500_000_000,
    max_compression_ratio: float = 500.0,
) -> None:
    path = Path(path)
    if not zipfile.is_zipfile(path):
        if path.suffix.lower() in {".docx", ".xlsx", ".xlsm", ".xlsb"}:
            raise UnsafeArchiveError("Office asset is not a valid ZIP container")
        return
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if len(entries) > max_entries:
                raise UnsafeArchiveError(
                    f"Office archive contains more than {max_entries} entries"
                )
            total_uncompressed = 0
            total_compressed = 0
            names: set[str] = set()
            for entry in entries:
                name = entry.filename.replace("\\", "/")
                names.add(name.lower())
                parts = PurePosixPath(name).parts
                if (
                    not name
                    or "\x00" in name
                    or name.startswith("/")
                    or ".." in parts
                ):
                    raise UnsafeArchiveError("Office archive contains an unsafe path")
                mode = entry.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise UnsafeArchiveError("Office archive contains a symbolic link")
                if entry.flag_bits & 0x1:
                    raise UnsafeArchiveError("Office archive contains encrypted content")
                total_uncompressed += entry.file_size
                total_compressed += entry.compress_size
                if total_uncompressed > max_uncompressed_bytes:
                    raise UnsafeArchiveError(
                        "Office archive exceeds the uncompressed size limit"
                    )
            if total_uncompressed > 10_000_000:
                ratio = total_uncompressed / max(1, total_compressed)
                if ratio > max_compression_ratio:
                    raise UnsafeArchiveError(
                        "Office archive exceeds the compression ratio limit"
                    )
            required_prefix = "word/" if file_type == "docx" else "xl/"
            if file_type in {"docx", "excel"} and not any(
                name.startswith(required_prefix) for name in names
            ):
                raise UnsafeArchiveError(
                    f"Office archive does not contain the required {required_prefix} tree"
                )
    except zipfile.BadZipFile as exc:
        raise UnsafeArchiveError("Office asset is not a valid ZIP container") from exc
