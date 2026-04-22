from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import aiohttp


def _base_page_url(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


@dataclass(slots=True)
class HttpFetchResult:
    status_code: int
    text: str
    final_url: str


class AsyncLystHttpClient:
    def __init__(
        self,
        *,
        timeout_sec: int,
        user_agent: str,
        default_headers: dict[str, str] | None = None,
        request_jitter_sec: float = 0.0,
        session_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._timeout_sec = timeout_sec
        self._user_agent = user_agent
        self._default_headers = default_headers or {}
        self._request_jitter_sec = max(0.0, request_jitter_sec)
        self._session_factory = session_factory
        self._session = None

    async def fetch_text(self, url: str, country: str, warm_home: bool = False) -> HttpFetchResult:
        if self._request_jitter_sec > 0:
            await asyncio.sleep(self._request_jitter_sec)

        session = self._get_or_create_session()
        cookies = {"country": country}
        if warm_home:
            # The warmed-home request mirrors the previously working sync path, but
            # keeps it inside the async transport so the event loop is never blocked.
            async with session.get(
                "https://www.lyst.com/",
                cookies=cookies,
                allow_redirects=True,
            ) as response:
                await response.text()

        headers = {}
        if warm_home:
            headers["Referer"] = url

        async with session.get(
            url,
            headers=headers or None,
            cookies=cookies,
            allow_redirects=True,
        ) as response:
            text = await response.text()
            return HttpFetchResult(
                status_code=getattr(response, "status", 0),
                text=text,
                final_url=str(getattr(response, "url", url)),
            )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _get_or_create_session(self):
        if self._session is not None:
            return self._session
        headers = {
            "User-Agent": self._user_agent,
            **self._default_headers,
        }
        timeout = aiohttp.ClientTimeout(total=self._timeout_sec)
        if self._session_factory is not None:
            self._session = self._session_factory(headers=headers, timeout=timeout)
        else:
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return self._session
