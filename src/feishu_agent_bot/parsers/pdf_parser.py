from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path
from time import monotonic

from .base import ParsedAsset, ParsedTable, TextBlock
from .exceptions import PdfParseError
from .tabular_utils import normalize_scalar, unique_headers


class PdfParser:
    name = "pdf"
    version = "1.1"
    supported_file_types = {"pdf"}
    supported_mime_types = {"application/pdf"}

    def __init__(
        self,
        *,
        ocr_enabled: bool = True,
        ocr_languages: str = "chi_sim+eng",
        min_text_chars_per_page: int = 50,
        max_pages: int = 500,
        timeout_seconds: int = 300,
    ):
        self.ocr_enabled = ocr_enabled
        self.ocr_languages = ocr_languages
        self.min_text_chars_per_page = min_text_chars_per_page
        self.max_pages = max_pages
        self.timeout_seconds = timeout_seconds

    def can_parse(self, asset) -> bool:
        return (
            asset.file_type in self.supported_file_types
            or asset.detected_mime_type in self.supported_mime_types
        )

    def parse(self, path: Path, *, asset_id: str) -> ParsedAsset:
        deadline = monotonic() + self.timeout_seconds
        parsed = self._parse_with_pymupdf(
            path, asset_id=asset_id, method="pymupdf", deadline=deadline
        )
        page_count = int(parsed.metadata.get("page_count") or 0)
        char_count = sum(len(block.text) for block in parsed.text_blocks)
        avg_chars = char_count / page_count if page_count else 0
        image_count = int(parsed.metadata.get("image_count") or 0)
        needs_ocr = (
            self.ocr_enabled
            and page_count > 0
            and avg_chars < self.min_text_chars_per_page
            and image_count > 0
        )
        if not needs_ocr:
            return parsed
        if not shutil.which("ocrmypdf") or not shutil.which("tesseract"):
            return self._with_warning(
                parsed,
                "OCR fallback required but ocrmypdf or tesseract is not installed.",
            )
        try:
            with tempfile.TemporaryDirectory(prefix="pdf-ocr-") as directory:
                ocr_path = Path(directory) / "ocr.pdf"
                self._run_ocr(path, ocr_path, deadline=deadline)
                ocr_parsed = self._parse_with_pymupdf(
                    ocr_path,
                    asset_id=asset_id,
                    method="ocr",
                    deadline=deadline,
                )
                ocr_sha256 = hashlib.sha256(ocr_path.read_bytes()).hexdigest()
        except Exception as exc:
            return self._with_warning(parsed, f"OCR fallback failed: {exc}")
        return ParsedAsset(
            asset_id=ocr_parsed.asset_id,
            file_type=ocr_parsed.file_type,
            title=ocr_parsed.title,
            text_blocks=ocr_parsed.text_blocks,
            tables=ocr_parsed.tables,
            metadata={
                **ocr_parsed.metadata,
                "original_image_count": parsed.metadata.get("image_count"),
                "original_page_char_counts": parsed.metadata.get("page_char_counts"),
                "ocr_languages": self.ocr_languages,
                "ocr_derived_sha256": ocr_sha256,
            },
            warnings=[
                *parsed.warnings,
                *ocr_parsed.warnings,
                "OCR text defaults to medium or lower confidence.",
            ],
            extraction_method="ocr",
        )

    def _parse_with_pymupdf(
        self, path: Path, *, asset_id: str, method: str, deadline: float
    ) -> ParsedAsset:
        import fitz

        self._ensure_before_deadline(deadline)
        document = fitz.open(path)
        warnings: list[str] = []
        try:
            page_count = document.page_count
            if page_count > self.max_pages:
                warnings.append(
                    f"PDF page count {page_count} exceeds PDF_MAX_PAGES={self.max_pages}; parsed first {self.max_pages} pages."
                )
            text_blocks: list[TextBlock] = []
            tables: list[ParsedTable] = []
            page_char_counts: list[int] = []
            image_count = 0
            image_metadata: list[dict[str, object]] = []
            for page_index in range(min(page_count, self.max_pages)):
                self._ensure_before_deadline(deadline)
                page = document.load_page(page_index)
                page_number = page_index + 1
                page_images = page.get_images(full=True)
                image_count += len(page_images)
                for image_index, image in enumerate(page_images, start=1):
                    xref = int(image[0])
                    try:
                        placements = [
                            [float(value) for value in rect]
                            for rect in page.get_image_rects(xref)
                        ]
                    except Exception:
                        placements = []
                    image_metadata.append(
                        {
                            "page_number": page_number,
                            "image_index": image_index,
                            "xref": xref,
                            "width": int(image[2]),
                            "height": int(image[3]),
                            "bits_per_component": int(image[4]),
                            "colorspace": str(image[5]),
                            "name": str(image[7]) if len(image) > 7 else "",
                            "filter": str(image[8]) if len(image) > 8 else "",
                            "placements": placements,
                            "source_locator": (
                                f"{path.name}#page={page_number}&image={image_index}"
                            ),
                        }
                    )
                page_chars = 0
                for block_index, block in enumerate(page.get_text("blocks"), start=1):
                    if len(block) < 5:
                        continue
                    x0, y0, x1, y1, text = block[:5]
                    text = str(text).strip()
                    if not text:
                        continue
                    page_chars += len(text)
                    text_blocks.append(
                        TextBlock(
                            block_id=f"{asset_id}-P{page_number:03d}-B{block_index:03d}",
                            text=text,
                            page_number=page_number,
                            bbox=(float(x0), float(y0), float(x1), float(y1)),
                            source_locator=(
                                f"{path.name}#page={page_number}&block={block_index}"
                            ),
                        )
                    )
                page_char_counts.append(page_chars)
                self._ensure_before_deadline(deadline)
                try:
                    detected_tables = page.find_tables().tables
                except Exception as exc:
                    warnings.append(
                        f"PDF table extraction failed on page {page_number}: {exc}"
                    )
                    detected_tables = []
                self._ensure_before_deadline(deadline)
                for table_index, table in enumerate(detected_tables, start=1):
                    matrix = table.extract()
                    if not matrix or len(matrix) < 2:
                        continue
                    headers, header_warnings = unique_headers(list(matrix[0]))
                    warnings.extend(
                        f"page={page_number}; table={table_index}; {warning}"
                        for warning in header_warnings
                    )
                    rows = []
                    for values in matrix[1:]:
                        padded = list(values) + [None] * (
                            len(headers) - len(values)
                        )
                        rows.append(
                            {
                                header: normalize_scalar(padded[index])
                                for index, header in enumerate(headers)
                            }
                        )
                    bbox = [float(value) for value in table.bbox]
                    tables.append(
                        ParsedTable(
                            table_id=(
                                f"{asset_id}-P{page_number:03d}-T{table_index:03d}"
                            ),
                            columns=headers,
                            rows=rows,
                            caption=f"PDF page {page_number} table {table_index}",
                            page_number=page_number,
                            source_locator=(
                                f"{path.name}#page={page_number}&table={table_index}"
                            ),
                            extraction_method="pymupdf_find_tables",
                            metadata={
                                "bbox": bbox,
                                "row_count": len(rows),
                                "column_count": len(headers),
                            },
                        )
                    )
            metadata = {
                "page_count": page_count,
                "parsed_page_count": min(page_count, self.max_pages),
                "metadata": dict(document.metadata or {}),
                "image_count": image_count,
                "images": image_metadata,
                "table_count": len(tables),
                "page_char_counts": page_char_counts,
                "average_chars_per_page": (
                    sum(page_char_counts) / len(page_char_counts)
                    if page_char_counts
                    else 0
                ),
                "parser": "PyMuPDF",
                "parser_version": fitz.VersionBind,
            }
            title = document.metadata.get("title") if document.metadata else None
            return ParsedAsset(
                asset_id=asset_id,
                file_type="pdf",
                title=title or None,
                text_blocks=text_blocks,
                tables=tables,
                metadata=metadata,
                warnings=warnings,
                extraction_method=method,
            )
        finally:
            document.close()

    def _run_ocr(
        self, path: Path, output_path: Path, *, deadline: float
    ) -> None:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise PdfParseError("PDF parsing timed out")
        subprocess.run(
            [
                "ocrmypdf",
                "--skip-text",
                "--language",
                self.ocr_languages,
                str(path),
                str(output_path),
            ],
            check=True,
            timeout=remaining,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    @staticmethod
    def _ensure_before_deadline(deadline: float) -> None:
        if monotonic() > deadline:
            raise PdfParseError("PDF parsing timed out")

    @staticmethod
    def _with_warning(parsed: ParsedAsset, warning: str) -> ParsedAsset:
        return ParsedAsset(
            asset_id=parsed.asset_id,
            file_type=parsed.file_type,
            title=parsed.title,
            text_blocks=parsed.text_blocks,
            tables=parsed.tables,
            metadata=parsed.metadata,
            warnings=[*parsed.warnings, warning],
            extraction_method=parsed.extraction_method,
        )
