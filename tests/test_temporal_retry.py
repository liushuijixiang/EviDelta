from __future__ import annotations

from feishu_agent_bot.temporal.retry_policies import (
    NON_RETRYABLE_ERRORS,
    activity_retry,
)


def test_retry_policy_marks_configuration_and_auth_errors_non_retryable():
    policy = activity_retry(3)

    assert policy.maximum_attempts == 3
    assert "AuthenticationError" in NON_RETRYABLE_ERRORS
    assert "AuthorizationError" in NON_RETRYABLE_ERRORS
    assert "InvalidConfigurationError" in NON_RETRYABLE_ERRORS
    assert "InvalidResearchInputError" in NON_RETRYABLE_ERRORS
    assert "ReportValidationError" in NON_RETRYABLE_ERRORS
    assert "RateLimitError" not in NON_RETRYABLE_ERRORS
    assert policy.non_retryable_error_types == NON_RETRYABLE_ERRORS
