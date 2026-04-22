from __future__ import annotations

import asyncio
import random
import re
import urllib.parse

from bs4 import BeautifulSoup

from helpers.lyst.http_client import AsyncLystHttpClient
from helpers.lyst.models import FetchResult, FetchStatus


class LystHttpTerminalPage(Exception):
    # HTTP 410 pages are a real pagination terminal signal. Keeping them as a
    # dedicated exception preserves that meaning across fetch/cycle boundaries.
    def __init__(self, content):
        self.content = content
        super().__init__("lyst_http_terminal_page")


async def handle_route(route, *, blocked_resource_types, blocked_url_parts):
    url = route.request.url
    resource_type = route.request.resource_type
    if resource_type in blocked_resource_types or any(part in url for part in blocked_url_parts):
        await route.abort()
    else:
        await route.continue_()


async def normalize_lazy_images(page):
    # This is kept in the fetch layer because it is part of making the acquired
    # page content parseable, not part of higher-level business logic.
    await page.evaluate(
        """
        () => {
            const attrs = [
                {from: 'data-src', to: 'src'},
                {from: 'data-lazy-src', to: 'src'},
                {from: 'data-srcset', to: 'srcset'},
                {from: 'data-lazy-srcset', to: 'srcset'},
            ];
            document.querySelectorAll('img, source').forEach((el) => {
                for (const {from, to} of attrs) {
                    const value = el.getAttribute(from);
                    if (value && !el.getAttribute(to)) {
                        el.setAttribute(to, value);
                    }
                }
            });
        }
        """
    )


def is_cloudflare_challenge(content):
    if not content:
        return False
    lowered = content.lower()
    markers = (
        "cf-browser-verification",
        "cloudflare",
        "just a moment",
        "attention required",
        "/cdn-cgi/challenge-platform/",
    )
    return any(marker in lowered for marker in markers)


def http_base_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def http_content_has_product_cards(content: str) -> bool:
    return bool(content and re.search(r'class=["\'][^"\']*_693owt3[^"\']*["\']', content))


def fetch_lyst_http_content(
    url: str,
    *,
    timeout_sec: int,
    request_jitter_sec: float,
    cloudflare_retry_count: int,
    cloudflare_retry_delay_sec: float,
    user_agent: str,
    headers: dict,
    logger,
):
    # The old sync helper is intentionally retired so callers cannot silently fall
    # back to requests-based blocking I/O after the async transport refactor.
    raise RuntimeError("fetch_lyst_http_content retired; use fetch_http_page")


async def get_page_content_http(
    url: str,
    country: str = "US",
    *,
    timeout_sec: int,
    request_jitter_sec: float,
    cloudflare_retry_count: int,
    cloudflare_retry_delay_sec: float,
    user_agent: str,
    headers: dict,
    logger,
):
    result = await fetch_http_page(
        url,
        country=country,
        timeout_sec=timeout_sec,
        request_jitter_sec=request_jitter_sec,
        cloudflare_retry_count=cloudflare_retry_count,
        cloudflare_retry_delay_sec=cloudflare_retry_delay_sec,
        user_agent=user_agent,
        headers=headers,
        logger=logger,
    )
    if result.status == FetchStatus.TERMINAL:
        raise LystHttpTerminalPage(result.content or "")
    if not result.is_ok:
        raise RuntimeError(result.extra.get("error") or "http_only_unusable_response")
    return result.content


def is_target_closed_error(exc: Exception) -> bool:
    try:
        if exc.__class__.__name__ == "TargetClosedError":
            return True
    except Exception:
        pass
    msg = str(exc)
    return "Target page, context or browser has been closed" in msg or "TargetClosedError" in msg


def is_pipe_closed_error(exc: Exception) -> bool:
    msg = str(exc)
    if not msg:
        return False
    lowered = msg.lower()
    return "pipe closed" in lowered or "os.write(pipe, data)" in lowered or "epipe" in lowered


async def fetch_http_page(
    url: str,
    *,
    country: str,
    timeout_sec: int,
    request_jitter_sec: float,
    cloudflare_retry_count: int,
    cloudflare_retry_delay_sec: float,
    user_agent: str,
    headers: dict,
    logger,
    http_client: AsyncLystHttpClient | None = None,
) -> FetchResult:
    # All HTTP fetching is routed through the async client so the Lyst runtime no
    # longer blocks the event loop waiting on requests.Session or requests.get.
    owns_client = http_client is None
    client = http_client or AsyncLystHttpClient(
        timeout_sec=timeout_sec,
        user_agent=user_agent,
        default_headers=headers,
        request_jitter_sec=0.0,
    )
    last_error = ""
    variants = (False, True)
    try:
        for warm_home in variants:
            for _ in range(2):
                if request_jitter_sec > 0:
                    await asyncio.sleep(random.uniform(0, request_jitter_sec))
                try:
                    response = await client.fetch_text(url, country, warm_home=warm_home)
                except Exception as exc:
                    last_error = str(exc)
                    continue
                status_code = response.status_code
                content = response.text
                if status_code == 410:
                    return FetchResult(status=FetchStatus.TERMINAL, content=content, final_url=response.final_url)
                if is_cloudflare_challenge(content) or status_code in (403, 429):
                    for _ in range(cloudflare_retry_count):
                        await asyncio.sleep(cloudflare_retry_delay_sec)
                        response = await client.fetch_text(url, country, warm_home=warm_home)
                        status_code = response.status_code
                        content = response.text
                        if not (is_cloudflare_challenge(content) or status_code in (403, 429)):
                            break
                    if is_cloudflare_challenge(content) or status_code in (403, 429):
                        return FetchResult(
                            status=FetchStatus.CLOUDFLARE,
                            content=content,
                            final_url=response.final_url,
                            extra={"error": f"cloudflare_{status_code}"},
                        )
                if status_code >= 400:
                    last_error = f"http_status_{status_code}"
                    continue
                if not content.strip():
                    last_error = "empty_response"
                    continue
                if not http_content_has_product_cards(content):
                    last_error = "http_only_missing_product_cards"
                    continue
                return FetchResult(
                    status=FetchStatus.OK,
                    content=content,
                    final_url=response.final_url,
                    extra={"source": "http"},
                )
        logger.warning("LYST HTTP returned no usable content for %s", url)
        return FetchResult(status=FetchStatus.FAILED, extra={"error": last_error or "http_only_unusable_response"})
    finally:
        if owns_client:
            await client.close()


async def fetch_browser_page(*args, **kwargs) -> FetchResult:
    # Browser fetching stays owned by the orchestrator until the Playwright path is
    # fully extracted, but the typed return contract is established here already.
    raise NotImplementedError("browser fetch remains orchestrator-owned")


async def get_soup_and_content(
    url: str,
    *,
    country: str,
    timeout_sec: int,
    request_jitter_sec: float,
    cloudflare_retry_count: int,
    cloudflare_retry_delay_sec: float,
    user_agent: str,
    headers: dict,
    logger,
    http_client: AsyncLystHttpClient | None = None,
):
    result = await fetch_http_page(
        url,
        country=country,
        timeout_sec=timeout_sec,
        request_jitter_sec=request_jitter_sec,
        cloudflare_retry_count=cloudflare_retry_count,
        cloudflare_retry_delay_sec=cloudflare_retry_delay_sec,
        user_agent=user_agent,
        headers=headers,
        logger=logger,
        http_client=http_client,
    )
    if not result.content:
        return None, None, result
    return BeautifulSoup(result.content, "lxml"), result.content, result
