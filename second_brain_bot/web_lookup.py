from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup

TIME_SENSITIVE_RE = re.compile(
    r"\b(latest|current|today|now|price|availability|available|verify|check|fresh|recent|актуальн|ціна|наявн)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class WebLookupResult:
    url: str
    retrieved_at: str
    title: str
    excerpt: str


def should_allow_public_lookup(text: str, *, explicit: bool = False) -> bool:
    if explicit:
        return True
    return bool(TIME_SENSITIVE_RE.search(text or ""))


def is_public_http_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    lowered = parsed.path.lower()
    risky_fragments = ("/api", "/account", "/login", "/saved", "/private", "/admin")
    return not any(fragment in lowered for fragment in risky_fragments)


async def fetch_public_page_summary(url: str) -> WebLookupResult | None:
    if not is_public_http_url(url):
        return None
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers={"User-Agent": "SecondBrainBot/1.0"}) as response:
            if response.status >= 400:
                return None
            html = await response.text()
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(" ", strip=True) if soup.title else url
    text = soup.get_text(" ", strip=True)
    return WebLookupResult(
        url=url,
        retrieved_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        title=title[:200],
        excerpt=text[:1200],
    )
