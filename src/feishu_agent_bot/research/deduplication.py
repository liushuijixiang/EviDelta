from __future__ import annotations

import hashlib
import re

from ..llm.schemas import SearchResult
from .url_safety import canonicalize_url


def content_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalized_text_hash(text: str) -> str:
    normalized = re.sub(r"[\W_]+", "", text.lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def deduplicate_search_results(results: list[SearchResult]) -> list[SearchResult]:
    unique: list[SearchResult] = []
    seen: set[str] = set()
    for result in results:
        try:
            canonical = canonicalize_url(result.url)
        except ValueError:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        unique.append(result.model_copy(update={"url": canonical}))
    return unique
