# Research Pipeline

The research package owns web search, URL safety, fetch, text extraction, and
source deduplication.

Key contracts:

- Search providers return candidate URLs and snippets only.
- `url_safety.py` blocks SSRF-prone targets before fetch.
- Fetchers enforce byte limits, redirect limits, and timeouts.
- Content extraction preserves enough source text for evidence validation.
- Deduplication happens before evidence extraction so repeated pages do not
  inflate confidence.

Search providers currently include DuckDuckGo through `ddgs` and Serper.dev.
Provider credentials must come from runtime configuration, never source files.
