from __future__ import annotations

from datetime import date, datetime
import math
import re


def unique_headers(values: list[object]) -> tuple[list[str], list[str]]:
    headers: list[str] = []
    warnings: list[str] = []
    counts: dict[str, int] = {}
    for index, value in enumerate(values, start=1):
        base = str(value).strip() if value is not None else ""
        base = base or f"column_{index}"
        count = counts.get(base, 0) + 1
        counts[base] = count
        header = base if count == 1 else f"{base}_{count}"
        if count > 1:
            warnings.append(f"duplicate_column={base}; renamed={header}")
        headers.append(header)
    return headers, warnings


_INTEGER = re.compile(r"^[+-]?(?:0|[1-9]\d{0,2}(?:,\d{3})*|[1-9]\d*)$")
_DECIMAL = re.compile(
    r"^[+-]?(?:(?:0|[1-9]\d{0,2}(?:,\d{3})*|[1-9]\d*)?\.\d+)$"
)
_ISO_DATE = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T].*)?$")


def normalize_scalar(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"na", "n/a", "null", "none", "nan", "-"}:
        return None
    if _ISO_DATE.match(text):
        return text.replace("/", "-")
    # Preserve identifier-like values with leading zeroes.
    unsigned = text.lstrip("+-")
    if len(unsigned) > 1 and unsigned.startswith("0") and "," not in text:
        return text
    if _INTEGER.match(text):
        try:
            return int(text.replace(",", ""))
        except ValueError:
            pass
    if _DECIMAL.match(text):
        try:
            return float(text.replace(",", ""))
        except ValueError:
            pass
    return text
