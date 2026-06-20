from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit


class UnsafeURLError(ValueError):
    pass


def canonicalize_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise UnsafeURLError("只允许 http/https URL")
    hostname = (parsed.hostname or "").lower().rstrip(".")
    if not hostname:
        raise UnsafeURLError("URL 缺少主机名")
    port = parsed.port
    netloc = hostname
    if port and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{hostname}:{port}"
    path = parsed.path or "/"
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


def _is_forbidden_ip(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return any(
        (
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        )
    )


def validate_public_url(url: str, resolver=socket.getaddrinfo) -> str:
    canonical = canonicalize_url(url)
    host = urlsplit(canonical).hostname
    assert host is not None
    try:
        addresses = {
            item[4][0] for item in resolver(host, None, type=socket.SOCK_STREAM)
        }
    except socket.gaierror as exc:
        raise UnsafeURLError("URL 主机名无法解析") from exc
    if not addresses or any(_is_forbidden_ip(address) for address in addresses):
        raise UnsafeURLError("URL 指向私有或保留地址")
    return canonical
