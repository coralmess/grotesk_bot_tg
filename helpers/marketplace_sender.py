from __future__ import annotations

import asyncio
import io
import random
from functools import wraps
from typing import Any, Awaitable, Callable, Iterable, Optional

import aiohttp
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut
from telegram.request import HTTPXRequest

from helpers.analytics_events import AnalyticsSink
from helpers.marketplace_core import DeliveryResult
from helpers.image_pipeline import send_remote_photo_with_fallback
from config import (
    MARKET_TELEGRAM_CONNECT_TIMEOUT,
    MARKET_TELEGRAM_MEDIA_WRITE_TIMEOUT,
    MARKET_TELEGRAM_POOL_SIZE,
    MARKET_TELEGRAM_POOL_TIMEOUT,
    MARKET_TELEGRAM_READ_TIMEOUT,
    MARKET_TELEGRAM_SEND_GAP_MAX_SEC,
    MARKET_TELEGRAM_SEND_GAP_MIN_SEC,
    MARKET_TELEGRAM_WRITE_TIMEOUT,
)


class RetryableHttpStatus(Exception):
    def __init__(self, status: int, wait_s: float = 0.0, context: str = ""):
        self.status = status
        self.wait_s = max(0.0, float(wait_s or 0.0))
        self.context = context or "http"
        super().__init__(f"{self.context} status={self.status}")


def async_retry(max_retries: int = 3, backoff_base: float = 1.0, *, assume_timeout_success: bool = False):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as exc:
                    await asyncio.sleep(exc.retry_after)
                except TimedOut:
                    # Marketplace notifications must not be marked as sent unless Telegram
                    # returns a Message; otherwise bursty photo uploads can silently lose
                    # items while poisoning the duplicate ledger.
                    if assume_timeout_success:
                        return True
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1))
                except RetryableHttpStatus as exc:
                    if attempt < max_retries - 1:
                        wait_s = exc.wait_s if exc.wait_s > 0 else (backoff_base * (attempt + 1))
                        await asyncio.sleep(wait_s)
                    else:
                        raise
                except Exception:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        raise
            return None

        return wrapper

    return decorator


def build_marketplace_bot(token: str):
    from telegram import Bot

    # Marketplace sends can upload many photos in a short burst. The default
    # python-telegram-bot HTTP pool is one connection with very small timeouts,
    # which made concurrent sends look successful after ambiguous timeouts.
    request = HTTPXRequest(
        connection_pool_size=MARKET_TELEGRAM_POOL_SIZE,
        connect_timeout=MARKET_TELEGRAM_CONNECT_TIMEOUT,
        read_timeout=MARKET_TELEGRAM_READ_TIMEOUT,
        write_timeout=MARKET_TELEGRAM_WRITE_TIMEOUT,
        pool_timeout=MARKET_TELEGRAM_POOL_TIMEOUT,
        media_write_timeout=MARKET_TELEGRAM_MEDIA_WRITE_TIMEOUT,
    )
    return Bot(token=token, request=request)


async def _sleep_after_marketplace_send() -> None:
    low = max(0.0, MARKET_TELEGRAM_SEND_GAP_MIN_SEC)
    high = max(low, MARKET_TELEGRAM_SEND_GAP_MAX_SEC)
    if high <= 0:
        return
    await asyncio.sleep(random.uniform(low, high))


def build_message_sender(*, send_semaphore: asyncio.Semaphore):
    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=False)
    async def send_message(bot, chat_id: str, text: str) -> DeliveryResult:
        async with send_semaphore:
            try:
                message = await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
            finally:
                # Keep the lock during the pause so the next marketplace send cannot
                # start immediately after this one. This deliberately favors delivery
                # confirmation accuracy over fast bursts.
                await _sleep_after_marketplace_send()
        return DeliveryResult(delivered=True, telegram_message_id=getattr(message, "message_id", None), channel="text")

    return send_message


def build_photo_sender(*, send_semaphore: asyncio.Semaphore):
    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=False)
    async def send_photo_by_bytes(bot, chat_id: str, photo_bytes: bytes, caption: str) -> DeliveryResult:
        async with send_semaphore:
            try:
                message = await bot.send_photo(
                    chat_id=chat_id,
                    photo=io.BytesIO(photo_bytes),
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
            finally:
                # Same throttle as text sends: photo uploads are the most likely path
                # to ambiguous Telegram timeouts, so keep them serialized and spaced.
                await _sleep_after_marketplace_send()
        return DeliveryResult(delivered=True, telegram_message_id=getattr(message, "message_id", None), channel="photo")

    return send_photo_by_bytes


def build_image_downloader(
    *,
    http_semaphore: asyncio.Semaphore,
    get_http_session: Callable[[], aiohttp.ClientSession],
    user_agent: str,
    accept_language: str,
    logger,
):
    @async_retry(max_retries=3, backoff_base=1.0)
    async def download_bytes(url: str, timeout_s: int = 30) -> Optional[bytes]:
        headers = {
            "User-Agent": user_agent,
            "Accept-Language": accept_language,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        async with http_semaphore:
            session = get_http_session()
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as response:
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_s = int(retry_after) if retry_after and retry_after.isdigit() else 15
                    raise RetryableHttpStatus(429, wait_s=wait_s, context="image download")
                if response.status in (404, 410):
                    raise RetryableHttpStatus(response.status, wait_s=2, context="image download")
                if response.status == 403:
                    logger.warning("Image forbidden (403). Falling back to text-only send.")
                    return None
                response.raise_for_status()
                return await response.read()

    return download_bytes


def build_media_sender(
    *,
    is_valid_image_url: Callable[[Optional[str]], bool],
    download_bytes: Callable[[str], Awaitable[Optional[bytes]]],
    send_message: Callable[[object, str, str], Awaitable[DeliveryResult]],
    send_photo_by_bytes: Callable[[object, str, bytes, str], Awaitable[DeliveryResult]],
    run_cpu_bound_fn: Callable[..., Awaitable[Optional[bytes]]],
    logger,
    min_upscale_dim: int = 1500,
    max_dim: int = 5000,
    upscale_factors: Iterable[float] = (2.0,),
    source_kind: str = "",
    analytics_sink: Optional[AnalyticsSink] = None,
):
    async def send_photo_with_upscale(
        bot,
        chat_id: str,
        caption: str,
        image_url: Optional[str],
        source_name: str = "",
    ) -> DeliveryResult:
        # The image pipeline stays centralized so marketplace send behavior stays identical
        # across OLX and SHAFA even when transport or image fallback rules evolve.
        return await send_remote_photo_with_fallback(
            bot=bot,
            chat_id=chat_id,
            caption=caption,
            image_url=image_url,
            is_valid_image_url=is_valid_image_url,
            download_bytes=download_bytes,
            send_message=send_message,
            send_photo_by_bytes=send_photo_by_bytes,
            run_cpu_bound_fn=run_cpu_bound_fn,
            logger=logger,
            min_upscale_dim=min_upscale_dim,
            max_dim=max_dim,
            upscale_factors=upscale_factors,
            analytics_sink=analytics_sink,
            source_kind=source_kind,
            source_name=source_name,
        )

    return send_photo_with_upscale
