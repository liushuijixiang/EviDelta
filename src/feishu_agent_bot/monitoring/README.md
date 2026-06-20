# Monitoring

The monitoring package decides whether a scheduled research topic has meaningful
new evidence and whether an existing report should be patched.

The update path is:

1. Re-run bounded search for the monitored topic.
2. Extract candidate events and evidence.
3. Estimate impact against the existing report.
4. Build a proposed patch version.
5. Validate citations and artifact references.
6. Notify Feishu and deliver the new version only after validation succeeds.

Validation failure should produce a user-visible notification and preserve the
previous report version.
