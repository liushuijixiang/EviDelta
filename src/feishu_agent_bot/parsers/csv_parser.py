from __future__ import annotations

import codecs
import csv
import os
from pathlib import Path
import tempfile

from charset_normalizer import from_bytes

from .base import ParsedAsset, ParsedTable
from .exceptions import CsvParseError
from .tabular_utils import normalize_scalar, unique_headers


class CsvParser:
    name = "csv"
    version = "2.0"
    supported_file_types = {"csv"}
    supported_mime_types = {
        "text/csv",
        "application/csv",
        "text/tab-separated-values",
    }

    def __init__(
        self,
        *,
        max_rows: int = 1_000_000,
        preview_rows: int = 100,
        chunk_size: int = 50_000,
    ) -> None:
        self.max_rows = max_rows
        self.preview_rows = preview_rows
        self.chunk_size = chunk_size

    def can_parse(self, asset) -> bool:
        return (
            asset.file_type in self.supported_file_types
            or asset.detected_mime_type in self.supported_mime_types
        )

    def parse(self, path: Path, *, asset_id: str) -> ParsedAsset:
        import pandas as pd

        try:
            sample = path.read_bytes()[:131_072]
            encoding = self._detect_encoding(sample)
            decoded = sample.decode(encoding, errors="strict")
            delimiter = self._detect_delimiter(decoded, path)
            header_line, raw_headers = self._find_header(decoded, delimiter)
            headers, warnings = unique_headers(raw_headers)
        except (OSError, UnicodeError, csv.Error, ValueError) as exc:
            raise CsvParseError(f"CSV 头部解析失败: {exc}") from exc

        retained: list[dict[str, object]] = []
        total_rows = 0
        normalized_path = path.with_name(path.name + ".normalized.csv")
        temp_path: Path | None = None
        try:
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".normalized.tmp", dir=path.parent
            )
            os.close(fd)
            temp_path = Path(temp_name)
            first_chunk = True
            reader = pd.read_csv(
                path,
                sep=delimiter,
                encoding=encoding,
                header=None,
                names=headers,
                skiprows=header_line + 1,
                comment="#",
                dtype=object,
                keep_default_na=False,
                na_filter=False,
                chunksize=self.chunk_size,
                on_bad_lines="warn",
            )
            for frame in reader:
                records = [
                    {column: normalize_scalar(row.get(column)) for column in headers}
                    for row in frame.to_dict(orient="records")
                ]
                total_rows += len(records)
                if len(retained) < self.max_rows:
                    retained.extend(records[: self.max_rows - len(retained)])
                pd.DataFrame.from_records(records, columns=headers).to_csv(
                    temp_path,
                    mode="w" if first_chunk else "a",
                    header=first_chunk,
                    index=False,
                    encoding="utf-8",
                )
                first_chunk = False
            if first_chunk:
                pd.DataFrame(columns=headers).to_csv(
                    temp_path, index=False, encoding="utf-8"
                )
            temp_path.replace(normalized_path)
            temp_path = None
        except Exception as exc:
            raise CsvParseError(f"CSV 分块解析失败: {exc}") from exc
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

        truncated = total_rows > self.max_rows
        if truncated:
            warnings.append(
                f"analysis_rows_truncated={self.max_rows}; total_rows={total_rows}"
            )
        metadata = {
            "encoding": encoding,
            "delimiter": delimiter,
            "row_count": total_rows,
            "analysis_row_count": len(retained),
            "preview_row_count": min(total_rows, self.preview_rows),
            "chunk_size": self.chunk_size,
            "truncated_for_analysis": truncated,
            "normalized_path": str(normalized_path),
            "comment_prefix": "#",
            "header_line": header_line + 1,
        }
        table = ParsedTable(
            f"{asset_id}-T001",
            headers,
            retained,
            source_locator=(
                f"{path.name}#rows={header_line + 2}:"
                f"{header_line + total_rows + 1}"
            ),
            extraction_method="csv_chunked",
            metadata=metadata,
        )
        return ParsedAsset(
            asset_id,
            "csv",
            None,
            tables=[table],
            metadata=metadata,
            warnings=warnings,
            extraction_method="csv",
        )

    @staticmethod
    def _detect_encoding(sample: bytes) -> str:
        if sample.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"
        try:
            sample.decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass
        # GB18030 is a superset of common simplified-Chinese encodings.
        try:
            sample.decode("gb18030")
            return "gb18030"
        except UnicodeDecodeError:
            match = from_bytes(sample).best()
            if match is None or not match.encoding:
                raise CsvParseError("无法识别 CSV 编码")
            return match.encoding

    @staticmethod
    def _detect_delimiter(sample: str, path: Path) -> str:
        content = "\n".join(
            line for line in sample.splitlines()[:80] if line.strip() and not line.lstrip().startswith("#")
        )
        if not content:
            raise CsvParseError("CSV 没有可解析内容")
        try:
            return csv.Sniffer().sniff(content, delimiters=",\t;|").delimiter
        except csv.Error:
            return "\t" if path.suffix.lower() == ".tsv" else ","

    @staticmethod
    def _find_header(sample: str, delimiter: str) -> tuple[int, list[str]]:
        for line_number, line in enumerate(sample.splitlines()):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            values = next(csv.reader([line], delimiter=delimiter))
            if values:
                return line_number, values
        raise CsvParseError("CSV 缺少表头")
