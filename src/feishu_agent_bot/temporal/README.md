# Temporal Execution

Temporal is the preferred backend for production-like deployments.

Responsibilities:

- Workflows define durable research and monitoring orchestration.
- Activities perform search, fetch, parsing, evidence extraction, synthesis,
  validation, rendering, and notification.
- Retry policies distinguish transient external failures from non-retryable
  configuration, validation, and authorization errors.
- Heartbeats keep long-running activities observable without advancing
  checkpoints prematurely.

Local execution remains available for tests and small deployments, but Temporal
is the expected backend for long-running research jobs.
