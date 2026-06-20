from __future__ import annotations

import feishu_agent_bot.errors as errors
from feishu_agent_bot.acquisition.downloader import (
    AssetTooLargeError,
    UnsupportedAssetTypeError,
)
from feishu_agent_bot.acquisition.safety import UnsafeArchiveError
from feishu_agent_bot.parsers.exceptions import (
    CsvParseError,
    ExcelParseError,
    PdfParseError,
)
from feishu_agent_bot.reporting.artifact_validator import ArtifactValidationError


def test_alpha_error_taxonomy_is_defined_with_retry_metadata():
    expected = [
        "UnsupportedFileTypeError",
        "UnsafeAssetError",
        "AssetTooLargeError",
        "AssetDownloadError",
        "MimeMismatchError",
        "PdfParseError",
        "OcrUnavailableError",
        "CsvParseError",
        "ExcelParseError",
        "DatasetQualityError",
        "InsufficientDataError",
        "AnalysisValidationError",
        "LatexRenderError",
        "PdfCompileError",
        "ExcelRenderError",
        "ArtifactValidationError",
        "ArtifactDeliveryError",
    ]

    for name in expected:
        item = getattr(errors, name)
        assert issubclass(item, errors.FeishuAgentError)
        assert isinstance(item.retryable, bool)

    assert errors.AssetDownloadError.retryable is True
    assert errors.ArtifactDeliveryError.retryable is True
    assert errors.UnsupportedFileTypeError.retryable is False


def test_existing_parser_and_artifact_errors_inherit_taxonomy_and_value_error():
    assert issubclass(CsvParseError, errors.CsvParseError)
    assert issubclass(CsvParseError, ValueError)
    assert issubclass(ExcelParseError, errors.ExcelParseError)
    assert issubclass(ExcelParseError, ValueError)
    assert issubclass(PdfParseError, errors.PdfParseError)
    assert issubclass(PdfParseError, ValueError)
    assert issubclass(AssetTooLargeError, errors.AssetTooLargeError)
    assert issubclass(AssetTooLargeError, ValueError)
    assert issubclass(UnsupportedAssetTypeError, errors.UnsupportedFileTypeError)
    assert issubclass(UnsupportedAssetTypeError, ValueError)
    assert issubclass(UnsafeArchiveError, errors.UnsafeAssetError)
    assert issubclass(UnsafeArchiveError, ValueError)
    assert issubclass(ArtifactValidationError, errors.ArtifactValidationError)
    assert issubclass(ArtifactValidationError, ValueError)
