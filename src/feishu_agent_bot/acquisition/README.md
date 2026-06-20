# Acquisition

The acquisition package handles downloadable assets such as PDF, CSV, Excel,
JSON, HTML tables, and Word documents.

Important behavior:

- Downloads use bounded concurrency and size limits.
- Office-like ZIP files are checked for archive bombs, path traversal,
  encrypted entries, symlinks, and required directory structure.
- Parsers return structured metadata and source locators that downstream
  evidence and report builders can cite.
- Runtime assets belong under `data/` and must not be committed.

When adding a new file type, add a parser, safety limits, fixtures, and focused
tests under `tests/test_*parser*` or `tests/test_acquisition_pipeline.py`.
