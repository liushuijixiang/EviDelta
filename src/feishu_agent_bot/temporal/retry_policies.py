from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy

NON_RETRYABLE_ERRORS = [
    "AuthenticationError",
    "AuthorizationError",
    "InvalidConfigurationError",
    "InvalidResearchInputError",
    "ReportValidationError",
]


def activity_retry(max_attempts: int) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=2),
        maximum_interval=timedelta(seconds=30),
        maximum_attempts=max_attempts,
        non_retryable_error_types=NON_RETRYABLE_ERRORS,
    )
