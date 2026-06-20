class FeishuAgentError(Exception):
    """Base class for bot-domain failures with retry classification metadata."""

    retryable = False


class UnsupportedFileTypeError(FeishuAgentError):
    pass


class UnsafeAssetError(FeishuAgentError):
    pass


class AssetTooLargeError(FeishuAgentError):
    pass


class AssetDownloadError(FeishuAgentError):
    retryable = True


class MimeMismatchError(FeishuAgentError):
    pass


class PdfParseError(FeishuAgentError):
    pass


class OcrUnavailableError(FeishuAgentError):
    pass


class CsvParseError(FeishuAgentError):
    pass


class ExcelParseError(FeishuAgentError):
    pass


class DatasetQualityError(FeishuAgentError):
    pass


class InsufficientDataError(FeishuAgentError):
    pass


class AnalysisValidationError(FeishuAgentError):
    pass


class LatexRenderError(FeishuAgentError):
    pass


class PdfCompileError(FeishuAgentError):
    pass


class ExcelRenderError(FeishuAgentError):
    pass


class ArtifactValidationError(FeishuAgentError):
    pass


class ArtifactDeliveryError(FeishuAgentError):
    retryable = True
