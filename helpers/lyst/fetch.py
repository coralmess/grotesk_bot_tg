from __future__ import annotations

import asyncio
import random
import re
import urllib.parse

import requests


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
    # All remaining sync requests usage is isolated here so the async Lyst flow
    # can call it through explicit thread offload instead of blocking the loop.
    if request_jitter_sec > 0:
        time_to_sleep = random.uniform(0, request_jitter_sec)
        if time_to_sleep > 0:
            import time

            time.sleep(time_to_sleep)
    base_url = http_base_url(url)
    with requests.Session() as session:
        session.headers.update({"User-Agent": user_agent, **headers})
        for attempt in range(cloudflare_retry_count + 1):
            response = session.get(base_url, timeout=timeout_sec, allow_redirects=True)
            if response.status_code == 410:
                raise LystHttpTerminalPage(response.text)
            response.raise_for_status()
            content = response.text
            if is_cloudflare_challenge(content):
                if attempt >= cloudflare_retry_count:
                    raise RuntimeError("lyst_cloudflare_http")
                if cloudflare_retry_delay_sec > 0:
                    import time

                    time.sleep(cloudflare_retry_delay_sec)
                continue
            return content
    logger.warning(f"LYST HTTP returned no content for {url}")
    return None


async def get_page_content_http(
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
    return await asyncio.to_thread(
        fetch_lyst_http_content,
        url,
        timeout_sec=timeout_sec,
        request_jitter_sec=request_jitter_sec,
        cloudflare_retry_count=cloudflare_retry_count,
        cloudflare_retry_delay_sec=cloudflare_retry_delay_sec,
        user_agent=user_agent,
        headers=headers,
        logger=logger,
    )


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
