from __future__ import annotations

import asyncio
import io
import random
from functools import wraps
from typing import Any, Awaitable, Callable, Iterable, Optional

import aiohttp
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut

from helpers.image_pipeline import send_remote_photo_with_fallback


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
                    # Telegram sometimes delivers the message but times out before the bot
                    # gets the response. Treating those cases as success avoids duplicates.
                    if assume_timeout_success:
                        return True
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1))
                except RetryableHttpStatus as exc:
                    if attempt < max_retries - 1:
                        wait_s = exc.wait_s if exc.wait_s > 0 else (backoff_base * (attempt + 1))
                        await asyncio.sleep(wait_s)
                except Exception:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        raise
            return None

        return wrapper

    return decorator


def build_message_sender(*, send_semaphore: asyncio.Semaphore):
    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=True)
    async def send_message(bot, chat_id: str, text: str) -> bool:
        async with send_semaphore:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
        return True

    return send_message


def build_photo_sender(*, send_semaphore: asyncio.Semaphore):
    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=True)
    async def send_photo_by_bytes(bot, chat_id: str, photo_bytes: bytes, caption: str) -> bool:
        async with send_semaphore:
            await bot.send_photo(
                chat_id=chat_id,
                photo=io.BytesIO(photo_bytes),
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
        return True

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
    send_message: Callable[[object, str, str], Awaitable[bool]],
    send_photo_by_bytes: Callable[[object, str, bytes, str], Awaitable[bool]],
    run_cpu_bound_fn: Callable[..., Awaitable[Optional[bytes]]],
    logger,
    min_upscale_dim: int = 1500,
    max_dim: int = 5000,
    upscale_factors: Iterable[float] = (2.0,),
):
    async def send_photo_with_upscale(bot, chat_id: str, caption: str, image_url: Optional[str]) -> bool:
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
        )

    return send_photo_with_upscale
