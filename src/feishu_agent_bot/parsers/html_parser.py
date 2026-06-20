from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup, Tag

from .base import ParsedAsset, ParsedTable, TextBlock
from .tabular_utils import normalize_scalar, unique_headers


class HtmlParser:
    name = "html"
    version = "2.0"
    supported_file_types = {"html"}
    supported_mime_types = {"text/html", "application/xhtml+xml"}

    def can_parse(self, asset) -> bool:
        return (
            asset.file_type in self.supported_file_types
            or asset.detected_mime_type in self.supported_mime_types
        )

    def parse(self, path: Path, *, asset_id: str) -> ParsedAsset:
        soup = BeautifulSoup(
            path.read_text(encoding="utf-8", errors="replace"), "html.parser"
        )
        text = soup.get_text("\n", strip=True)
        tables: list[ParsedTable] = []
        warnings: list[str] = []
        for index, table in enumerate(soup.find_all("table"), start=1):
            matrix, header_flags = self._expand_table(table)
            if not matrix:
                continue
            header_rows = self._header_row_count(header_flags)
            raw_headers = self._flatten_headers(matrix[:header_rows])
            headers, header_warnings = unique_headers(raw_headers)
            warnings.extend(header_warnings)
            records = []
            for row in matrix[header_rows:]:
                padded = row + [""] * (len(headers) - len(row))
                records.append(
                    {
                        column: normalize_scalar(padded[column_index])
                        for column_index, column in enumerate(headers)
                    }
                )
            caption_node = table.find("caption")
            caption = caption_node.get_text(" ", strip=True) if caption_node else None
            context = self._context_for(table)
            locator = f"{path.name}#table-{index}"
            tables.append(
                ParsedTable(
                    f"{asset_id}-T{index:03d}",
                    headers,
                    records,
                    caption=caption or context.get("heading"),
                    source_locator=locator,
                    extraction_method="html_table",
                    metadata={
                        "table_index": index,
                        "header_row_count": header_rows,
                        "caption": caption,
                        "context": context,
                        "dom_path": self._dom_path(table),
                        "rowspan_colspan_expanded": True,
                    },
                )
            )
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        return ParsedAsset(
            asset_id,
            "html",
            title,
            [TextBlock(f"{asset_id}-B001", text, source_locator=f"{path.name}#text")],
            tables,
            metadata={"table_count": len(tables)},
            warnings=warnings,
            extraction_method="html",
        )

    @staticmethod
    def _expand_table(table: Tag) -> tuple[list[list[str]], list[bool]]:
        matrix: list[list[str]] = []
        header_flags: list[bool] = []
        spans: dict[tuple[int, int], str] = {}
        direct_rows = [
            row for row in table.find_all("tr") if row.find_parent("table") is table
        ]
        for row_index, tr in enumerate(direct_rows):
            row: list[str] = []
            column = 0

            def consume_spans() -> None:
                nonlocal column
                while (row_index, column) in spans:
                    row.append(spans[(row_index, column)])
                    column += 1

            cells = tr.find_all(["th", "td"], recursive=False)
            row_has_header = bool(cells) and all(cell.name == "th" for cell in cells)
            for cell in cells:
                consume_spans()
                value = cell.get_text(" ", strip=True)
                rowspan = max(1, int(cell.get("rowspan", 1) or 1))
                colspan = max(1, int(cell.get("colspan", 1) or 1))
                for offset in range(colspan):
                    row.append(value)
                    for future_row in range(row_index + 1, row_index + rowspan):
                        spans[(future_row, column + offset)] = value
                column += colspan
            consume_spans()
            if row:
                matrix.append(row)
                header_flags.append(row_has_header)
        width = max((len(row) for row in matrix), default=0)
        return [row + [""] * (width - len(row)) for row in matrix], header_flags

    @staticmethod
    def _header_row_count(flags: list[bool]) -> int:
        count = 0
        for flag in flags:
            if not flag:
                break
            count += 1
        return max(1, count)

    @staticmethod
    def _flatten_headers(rows: list[list[str]]) -> list[str]:
        if not rows:
            return []
        columns: list[str] = []
        for column_index in range(max(len(row) for row in rows)):
            parts: list[str] = []
            for row in rows:
                value = row[column_index].strip() if column_index < len(row) else ""
                if value and (not parts or value != parts[-1]):
                    parts.append(value)
            columns.append(" / ".join(parts))
        return columns

    @staticmethod
    def _context_for(table: Tag) -> dict[str, str]:
        context: dict[str, str] = {}
        for node in table.find_all_previous(["h1", "h2", "h3", "h4", "h5", "h6", "p"], limit=12):
            value = node.get_text(" ", strip=True)
            if not value:
                continue
            if node.name.startswith("h") and "heading" not in context:
                context["heading"] = value
            elif node.name == "p" and "preceding_text" not in context:
                context["preceding_text"] = value
            if "heading" in context and "preceding_text" in context:
                break
        return context

    @staticmethod
    def _dom_path(node: Tag) -> str:
        parts: list[str] = []
        current: Tag | None = node
        while current is not None and current.name != "[document]":
            siblings = current.find_previous_siblings(current.name)
            parts.append(f"{current.name}[{len(siblings) + 1}]")
            current = current.parent if isinstance(current.parent, Tag) else None
        return "/" + "/".join(reversed(parts))
