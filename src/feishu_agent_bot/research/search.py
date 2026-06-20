from __future__ import annotations

import re
from typing import Protocol
from urllib.parse import unquote

import httpx

from ..llm.schemas import SearchResult
from ..acquisition.file_type import FileTypeDetector

SUSPICIOUS_PATTERNS = re.compile(
    r"(约炮|外围|楼凤|小姐上门|特殊服务|全套|选妹|yp\d+|薇芯|同城服务)",
    re.IGNORECASE,
)


class SearchProvider(Protocol):
    def search(self, query: str, limit: int) -> list[SearchResult]:
        ...


def _likely_asset_type(url: str, content_type: str | None = None) -> str:
    return FileTypeDetector.detect(url, content_type=content_type)


class DDGSSearchProvider:
    def __init__(
        self,
        *,
        backend: str = "bing",
        region: str = "cn-zh",
        safesearch: str = "moderate",
    ):
        self.backend = backend
        self.region = region
        self.safesearch = safesearch

    def search(self, query: str, limit: int) -> list[SearchResult]:
        from ddgs import DDGS

        rows = DDGS().text(
            query,
            region=self.region,
            safesearch=self.safesearch,
            max_results=max(limit * 2, limit),
            backend=self.backend,
        )
        results = []
        for row in rows:
            url = row.get("href")
            if not url:
                continue
            title = row.get("title") or url
            snippet = row.get("body")
            candidate = f"{title} {unquote(url)} {snippet or ''}"
            if SUSPICIOUS_PATTERNS.search(candidate):
                continue
            results.append(
                SearchResult(
                    title=title,
                    url=url,
                    snippet=snippet,
                    query=query,
                    rank=len(results) + 1,
                    provider="ddgs",
                    detected_extension=_extension_hint(url),
                    likely_asset_type=_likely_asset_type(url),
                )
            )
            if len(results) >= limit:
                break
        return results


class SerperSearchProvider:
    def __init__(
        self,
        *,
        api_key: str,
        url: str = "https://google.serper.dev/search",
        country: str = "cn",
        locale: str = "zh-cn",
        client: httpx.Client | None = None,
    ):
        self.api_key = api_key
        self.url = url
        self.country = country
        self.locale = locale
        self.client = client or httpx.Client(timeout=20)

    def search(self, query: str, limit: int) -> list[SearchResult]:
        response = self.client.post(
            self.url,
            headers={
                "X-API-KEY": self.api_key,
                "Content-Type": "application/json",
            },
            json={
                "q": query,
                "num": limit,
                "gl": self.country,
                "hl": self.locale,
            },
        )
        response.raise_for_status()
        rows = response.json().get("organic") or []
        results = []
        for row in rows:
            url = row.get("link")
            if not url:
                continue
            title = row.get("title") or url
            snippet = row.get("snippet")
            candidate = f"{title} {unquote(url)} {snippet or ''}"
            if SUSPICIOUS_PATTERNS.search(candidate):
                continue
            results.append(
                SearchResult(
                    title=title,
                    url=url,
                    snippet=snippet,
                    query=query,
                    rank=len(results) + 1,
                    provider="serper",
                    detected_extension=_extension_hint(url),
                    likely_asset_type=_likely_asset_type(url),
                )
            )
            if len(results) >= limit:
                break
        return results


class MockSearchProvider:
    def __init__(self, results: dict[str, list[SearchResult]] | None = None):
        self.results = results or {}

    def search(self, query: str, limit: int) -> list[SearchResult]:
        return self.results.get(query, [])[:limit]


def _extension_hint(url: str) -> str | None:
    decoded = unquote(url).lower().split("?", 1)[0]
    for extension in (
        ".pdf",
        ".csv",
        ".tsv",
        ".xlsx",
        ".xls",
        ".xlsb",
        ".json",
        ".docx",
        ".txt",
    ):
        if decoded.endswith(extension):
            return extension
    return None
