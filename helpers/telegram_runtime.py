from __future__ import annotations

import asyncio
import hashlib
import io
import re
import time
import uuid
from collections import deque
from typing import Callable, Optional

from telegram import Bot
from telegram.error import RetryAfter, TimedOut


class TelegramMessageQueue:
    def __init__(
        self,
        bot_token,
        *,
        send_func: Callable[..., "asyncio.Future[bool]"],
        pending_ttl_sec: int = 6 * 3600,
        pending_max_entries: int = 5000,
        dedupe_window_sec: int = 1800,
    ):
        self.queue = asyncio.Queue()
        self.bot_token = bot_token
        self.pending_messages = {}
        self.recent_sent = {}
        self._pending_ttl_sec = pending_ttl_sec
        self._pending_max_entries = pending_max_entries
        self._dedupe_window_sec = dedupe_window_sec
        self._send_func = send_func

    def _fingerprint(self, chat_id, message, image_url, uah_price, sale_percentage):
        payload = f"{chat_id}|{message}|{image_url or ''}|{uah_price or ''}|{sale_percentage or ''}"
        return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()

    def _prune_recent(self, now_ts):
        expired = [k for k, ts in self.recent_sent.items() if now_ts - ts > self._dedupe_window_sec]
        for k in expired:
            self.recent_sent.pop(k, None)

    def _set_pending_status(self, message_id, sent, now_ts=None):
        self.pending_messages[message_id] = {
            "sent": bool(sent),
            "updated_at": now_ts if now_ts is not None else time.time(),
        }

    def _prune_pending_messages(self, now_ts):
        expired = [
            message_id
            for message_id, meta in self.pending_messages.items()
            if meta.get("sent") and (now_ts - float(meta.get("updated_at", now_ts))) > self._pending_ttl_sec
        ]
        for message_id in expired:
            self.pending_messages.pop(message_id, None)
        overflow = len(self.pending_messages) - self._pending_max_entries
        if overflow <= 0:
            return
        ordered = sorted(
            self.pending_messages.items(),
            key=lambda item: (0 if item[1].get("sent") else 1, float(item[1].get("updated_at", 0.0))),
        )
        for message_id, _ in ordered[:overflow]:
            self.pending_messages.pop(message_id, None)

    async def add_message(self, chat_id, message, image_url=None, uah_price=None, sale_percentage=None):
        message_id = str(uuid.uuid4())
        self._set_pending_status(message_id, False)
        await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
        return message_id

    async def process_queue(self):
        while True:
            message_id, chat_id, message, image_url, uah_price, sale_percentage = await self.queue.get()
            now_ts = time.time()
            fingerprint = self._fingerprint(chat_id, message, image_url, uah_price, sale_percentage)
            self._prune_recent(now_ts)
            self._prune_pending_messages(now_ts)
            if fingerprint in self.recent_sent:
                self._set_pending_status(message_id, True, now_ts)
                await asyncio.sleep(1)
                continue
            success = await self._send_func(
                self.bot_token,
                chat_id,
                message,
                image_url,
                uah_price,
                sale_percentage,
            )
            self._set_pending_status(message_id, success, now_ts)
            if success:
                self.recent_sent[fingerprint] = time.time()
            else:
                await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
            await asyncio.sleep(1)

    def is_message_sent(self, message_id):
        meta = self.pending_messages.get(message_id)
        if not meta:
            return False
        return bool(meta.get("sent"))

    def stats(self):
        pending = sum(1 for meta in self.pending_messages.values() if not meta.get("sent"))
        return {"queue_size": self.queue.qsize(), "pending": pending}


async def send_telegram_message(
    bot_token,
    chat_id,
    message,
    image_url=None,
    uah_price=None,
    sale_percentage=None,
    max_retries=3,
    *,
    process_image_func: Optional[Callable] = None,
    upgrade_image_url_func: Optional[Callable[[str | None], str | None]] = None,
    logger,
):
    bot = Bot(token=bot_token)
    for attempt in range(max_retries):
        try:
            if image_url and image_url.startswith(("http://", "https://")):
                if uah_price is not None and sale_percentage is not None and process_image_func is not None:
                    img_byte_arr = await asyncio.to_thread(process_image_func, image_url, uah_price, sale_percentage)
                    await bot.send_photo(chat_id=chat_id, photo=img_byte_arr, caption=message, parse_mode="HTML")
                else:
                    best_url = (
                        upgrade_image_url_func(image_url) if upgrade_image_url_func is not None else image_url
                    ) or image_url
                    await bot.send_photo(chat_id=chat_id, photo=best_url, caption=message, parse_mode="HTML")
            else:
                await bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited. Sleeping for {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            logger.warning(f"Request timed out on attempt {attempt + 1}")
            logger.warning("Assuming Telegram delivered message despite timeout to avoid duplicates")
            return True
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send Telegram message after {max_retries} attempts")
                return False
            await asyncio.sleep(2 * (attempt + 1))
    return False


def get_allowed_chat_ids(default_chat_id, telegram_chat_id):
    allowed = set()
    for raw in (default_chat_id, telegram_chat_id):
        if raw is None:
            continue
        try:
            allowed.add(int(raw))
        except (TypeError, ValueError):
            continue
    return allowed


def tail_log_lines(path, *, line_count, logger):
    if not path.exists():
        return []
    lines = deque(maxlen=line_count)
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                lines.append(line.rstrip("\n"))
    except Exception as exc:
        logger.error(f"Failed to read log file: {exc}")
        return []
    return list(lines)


async def send_log_tail(bot, chat_id, log_path, *, line_count, logger):
    lines = tail_log_lines(log_path, line_count=line_count, logger=logger)
    if not lines:
        await bot.send_message(chat_id=chat_id, text="Log file is empty or missing.")
        return
    payload = "\n".join(lines) + "\n"
    log_bytes = payload.encode("utf-8", errors="replace")
    bio = io.BytesIO(log_bytes)
    bio.name = f"python_last_{line_count}.log"
    await bot.send_document(
        chat_id=chat_id,
        document=bio,
        caption=f"Last {line_count} lines from {log_path.name}",
    )


async def command_listener(
    bot_token,
    allowed_chat_ids,
    log_path,
    *,
    line_count,
    add_dynamic_url_func: Callable[[str], tuple[bool, Optional[str], Optional[str]]],
    logger,
):
    if not bot_token:
        logger.warning("Command listener disabled: TELEGRAM_BOT_TOKEN is not set.")
        return
    if not allowed_chat_ids:
        logger.warning("Command listener disabled: no allowed chat IDs configured.")
        return

    bot = Bot(token=bot_token)
    offset = None
    logger.info("Command listener started.")

    while True:
        try:
            updates = await bot.get_updates(offset=offset, timeout=20, allowed_updates=["message"])
            for update in updates:
                offset = update.update_id + 1
                message = update.message
                if not message or not message.text:
                    continue
                chat_id = message.chat_id
                if chat_id not in allowed_chat_ids:
                    continue
                raw_text = message.text.strip()
                if not raw_text:
                    continue
                command = raw_text.split()[0].split("@")[0].lower()
                if command in ("/log", "/logs", "/log500"):
                    await send_log_tail(bot, chat_id, log_path, line_count=line_count, logger=logger)
                elif command in ("/add", "/addlink", "/addurl"):
                    url_match = re.search(r"https?://\S+", raw_text)
                    if not url_match:
                        await bot.send_message(chat_id=chat_id, text="Send a valid URL after the command.")
                        continue
                    url = url_match.group(0).strip()
                    ok, source, url_name = add_dynamic_url_func(url)
                    if not source:
                        await bot.send_message(chat_id=chat_id, text="Unsupported URL. Send an OLX or Shafa link.")
                        continue
                    source_upper = source.upper()
                    if ok:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"👔 Added {source_upper} link\n\n"
                                f"🌐 Url: {url}\n"
                                f"📥 Name: {url_name}"
                            ),
                        )
                    else:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⚠️ {source_upper} link already exists\n\n"
                                f"🌐 Url: {url}\n"
                                f"📥 Name: {url_name}"
                            ),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Command listener error: {exc}")
            await asyncio.sleep(5)

