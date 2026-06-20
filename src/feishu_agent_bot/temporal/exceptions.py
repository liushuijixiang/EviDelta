from __future__ import annotations


class TemporalExecutionError(RuntimeError):
    pass


class TemporalUnavailable(TemporalExecutionError):
    pass


class InvalidConfigurationError(TemporalExecutionError):
    pass


class InvalidResearchInputError(TemporalExecutionError):
    pass


class AuthenticationError(TemporalExecutionError):
    pass


class AuthorizationError(TemporalExecutionError):
    pass


class RateLimitError(TemporalExecutionError):
    pass


class TransientNetworkError(TemporalExecutionError):
    pass


class ProviderServerError(TemporalExecutionError):
    pass


class InvalidModelOutputError(TemporalExecutionError):
    pass


class ContentUnavailableError(TemporalExecutionError):
    pass


class ReportValidationError(TemporalExecutionError):
    pass
