from ..errors import CsvParseError as _CsvParseError
from ..errors import ExcelParseError as _ExcelParseError
from ..errors import PdfParseError as _PdfParseError


class CsvParseError(_CsvParseError, ValueError):
    """The CSV/TSV asset could not be parsed safely."""


class ExcelParseError(_ExcelParseError, ValueError):
    """The Excel asset could not be parsed without executing workbook code."""


class PdfParseError(_PdfParseError, ValueError):
    """The PDF asset could not be parsed within the configured safety bounds."""
