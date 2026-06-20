from __future__ import annotations

from pathlib import Path

from .base import AssetParser, ParsedAsset
from .pdf_parser import PdfParser
from .structured import CsvParser, DocxParser, ExcelParser, HtmlParser, JsonParser, TextParser


class ParserRegistry:
    def __init__(self, parsers: list[AssetParser] | None = None):
        self._parsers: dict[str, AssetParser] = {}
        self._ordered: list[AssetParser] = []
        for parser in parsers or []:
            self.register(parser)

    def register(self, parser: AssetParser) -> None:
        self._ordered.append(parser)
        for file_type in parser.supported_file_types:
            self._parsers[file_type] = parser

    def parse(self, file_type: str, path: str | Path, *, asset_id: str) -> ParsedAsset:
        parser = self._parsers.get(file_type)
        if not parser:
            raise ValueError(f"unsupported file type: {file_type}")
        return parser.parse(Path(path), asset_id=asset_id)

    def parser_for_asset(self, asset) -> AssetParser | None:
        for parser in self._ordered:
            if parser.can_parse(asset):
                return parser
        return None

    def parse_asset(self, asset) -> ParsedAsset:
        parser = self.parser_for_asset(asset)
        if not parser:
            raise ValueError(f"no parser registered for asset: {asset.asset_id}")
        if not asset.raw_object_path:
            raise ValueError(f"asset has no raw object path: {asset.asset_id}")
        return parser.parse(Path(asset.raw_object_path), asset_id=asset.asset_id)


def default_parser_registry(
    *,
    csv_max_rows: int = 1_000_000,
    csv_preview_rows: int = 100,
    csv_chunk_size: int = 50_000,
    pdf_ocr_enabled: bool = True,
    pdf_ocr_languages: str = "chi_sim+eng",
    pdf_min_text_chars_per_page: int = 50,
    pdf_max_pages: int = 500,
    pdf_parse_timeout_seconds: int = 300,
) -> ParserRegistry:
    return ParserRegistry(
        [
            HtmlParser(),
            CsvParser(
                max_rows=csv_max_rows,
                preview_rows=csv_preview_rows,
                chunk_size=csv_chunk_size,
            ),
            JsonParser(),
            ExcelParser(),
            DocxParser(),
            PdfParser(
                ocr_enabled=pdf_ocr_enabled,
                ocr_languages=pdf_ocr_languages,
                min_text_chars_per_page=pdf_min_text_chars_per_page,
                max_pages=pdf_max_pages,
                timeout_seconds=pdf_parse_timeout_seconds,
            ),
            TextParser(),
        ]
    )
