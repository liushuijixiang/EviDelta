from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class TextBlock:
    block_id: str
    text: str
    page_number: int | None = None
    section: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    source_locator: str | None = None


@dataclass(frozen=True)
class ParsedTable:
    table_id: str
    columns: list[str]
    rows: list[dict[str, object]]
    caption: str | None = None
    page_number: int | None = None
    sheet_name: str | None = None
    cell_range: str | None = None
    source_locator: str | None = None
    extraction_method: str = "structured"
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ParsedAsset:
    asset_id: str
    file_type: str
    title: str | None = None
    text_blocks: list[TextBlock] = field(default_factory=list)
    tables: list[ParsedTable] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    extraction_method: str = "parser"


class AssetParser(Protocol):
    name: str
    version: str
    supported_file_types: set[str]
    supported_mime_types: set[str]

    def can_parse(self, asset) -> bool:
        ...

    def parse(self, path: Path, *, asset_id: str) -> ParsedAsset:
        ...
