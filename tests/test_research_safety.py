import socket

import httpx
import pytest

from feishu_agent_bot.research.fetcher import PageTooLargeError, WebFetcher
from feishu_agent_bot.research.url_safety import (
    UnsafeURLError,
    canonicalize_url,
    validate_public_url,
)


def test_url_canonicalization():
    assert (
        canonicalize_url("HTTPS://Example.COM:443/path?q=1#fragment")
        == "https://example.com/path?q=1"
    )


@pytest.mark.parametrize(
    "address", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "::1"]
)
def test_ssrf_private_addresses_are_blocked(address):
    def resolver(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0))]

    with pytest.raises(UnsafeURLError):
        validate_public_url("http://example.com/", resolver=resolver)


def test_redirect_to_private_address_is_blocked():
    def handler(request):
        return httpx.Response(
            302, headers={"location": "http://127.0.0.1/admin"}
        )

    def validator(url):
        if "127.0.0.1" in url:
            raise UnsafeURLError("private")
        return url

    fetcher = WebFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=validator,
    )
    with pytest.raises(UnsafeURLError):
        fetcher.fetch("https://example.com")


def test_page_size_limit():
    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"x" * 11,
        )

    fetcher = WebFetcher(
        max_page_bytes=10,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        url_validator=lambda url: url,
    )
    with pytest.raises(PageTooLargeError):
        fetcher.fetch("https://example.com")
