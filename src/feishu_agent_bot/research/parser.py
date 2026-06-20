from __future__ import annotations

import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from charset_normalizer import from_bytes

from ..llm.schemas import ExtractedPage


class ContentExtractor:
    def extract(
        self, content: bytes, url: str, content_type: str | None = None
    ) -> ExtractedPage:
        decoded = self._decode(content, content_type)
        soup = BeautifulSoup(decoded, "html.parser")
        for tag in soup(
            [
                "script",
                "style",
                "nav",
                "footer",
                "header",
                "aside",
                "noscript",
                "form",
                "svg",
            ]
        ):
            tag.decompose()
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        publisher_meta = soup.find(
            "meta", attrs={"property": "og:site_name"}
        ) or soup.find("meta", attrs={"name": "application-name"})
        published_meta = soup.find(
            "meta", attrs={"property": "article:published_time"}
        ) or soup.find("meta", attrs={"name": "date"})
        root = soup.find("article") or soup.find("main") or soup.body or soup
        blocks = []
        for node in root.find_all(["h1", "h2", "h3", "p", "li"]):
            text = re.sub(r"\s+", " ", node.get_text(" ", strip=True))
            if len(text) >= 8:
                blocks.append(text)
        text = "\n\n".join(dict.fromkeys(blocks))
        if not text:
            text = re.sub(r"\s+", " ", root.get_text(" ", strip=True))
        publisher = (
            publisher_meta.get("content", "").strip()
            if publisher_meta
            else urlsplit(url).hostname
        )
        published_at = (
            published_meta.get("content", "").strip()
            if published_meta
            else None
        )
        return ExtractedPage(
            title=title or publisher or "Untitled",
            text=text.strip(),
            publisher=publisher,
            published_at=published_at,
        )

    @classmethod
    def _decode(cls, content: bytes, content_type: str | None) -> str:
        candidates: list[str] = []
        header_match = re.search(
            r"charset\s*=\s*[\"']?([^\s;\"']+)",
            content_type or "",
            re.IGNORECASE,
        )
        if header_match:
            candidates.append(header_match.group(1))
        if content.startswith(b"\xef\xbb\xbf"):
            candidates.append("utf-8-sig")
        meta_match = re.search(
            br"<meta[^>]+charset\s*=\s*[\"']?([^\s/\"'>;]+)",
            content[:16_384],
            re.IGNORECASE,
        )
        if meta_match:
            candidates.append(meta_match.group(1).decode("ascii", errors="ignore"))
        candidates.append("utf-8")

        detected = from_bytes(content).best()
        if detected and detected.encoding and detected.percent_coherence >= 20:
            candidates.append(detected.encoding)
        candidates.extend(["gb18030", "big5"])

        for encoding in dict.fromkeys(
            candidate.strip().lower() for candidate in candidates if candidate.strip()
        ):
            try:
                return content.decode(encoding, errors="strict")
            except (LookupError, UnicodeDecodeError):
                continue
        return content.decode("utf-8", errors="replace")
