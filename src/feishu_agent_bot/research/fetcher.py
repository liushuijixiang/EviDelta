from __future__ import annotations

from urllib.parse import urljoin

import httpx

from ..llm.schemas import FetchResult
from .url_safety import validate_public_url


class PageTooLargeError(ValueError):
    pass


class WebFetcher:
    def __init__(
        self,
        timeout_seconds: float = 20,
        max_page_bytes: int = 3_000_000,
        max_redirects: int = 5,
        client: httpx.Client | None = None,
        url_validator=validate_public_url,
    ):
        self.max_page_bytes = max_page_bytes
        self.max_redirects = max_redirects
        self.client = client or httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=False,
            headers={"User-Agent": "evidelta/0.1 research-fetcher"},
        )
        self.url_validator = url_validator

    def fetch(self, url: str) -> FetchResult:
        requested = self.url_validator(url)
        current = requested
        for _ in range(self.max_redirects + 1):
            with self.client.stream("GET", current) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        raise httpx.HTTPError("重定向缺少 Location")
                    current = self.url_validator(urljoin(current, location))
                    continue
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                if not (
                    content_type.startswith("text/html")
                    or content_type.startswith("text/plain")
                    or "application/xhtml+xml" in content_type
                ):
                    raise ValueError(f"不支持的内容类型: {content_type}")
                body = bytearray()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > self.max_page_bytes:
                        raise PageTooLargeError("页面超过大小限制")
                return FetchResult(
                    requested_url=requested,
                    final_url=current,
                    status_code=response.status_code,
                    content_type=content_type,
                    content=bytes(body),
                )
        raise httpx.TooManyRedirects("重定向次数过多")
