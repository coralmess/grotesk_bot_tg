import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set

from dotenv import load_dotenv
from telegram import Update
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes

HOST = "grotesk.tplinkdns.com"
PORT = 45678
CHECK_INTERVAL_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 8

SUBSCRIBERS_FILE = Path("subscribers.json")
STATE_FILE = Path("svitlo_state.json")


@dataclass
class PowerState:
    value: str
    updated_at: str


class SvitloBot:
    def __init__(self) -> None:
        self._subscribers: Set[int] = self._load_subscribers()
        self._state: Optional[PowerState] = self._load_state()
        self._lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.message:
            return
        chat_id = update.effective_chat.id
        added = False
        async with self._lock:
            if chat_id not in self._subscribers:
                self._subscribers.add(chat_id)
                self._save_subscribers()
                added = True

        current_state = self._state.value if self._state else "unknown"
        status_emoji = "⚡" if current_state == "ON" else "🔌"
        action_text = "✅ Підписано." if added else "ℹ️ Вже підписано."
        await update.message.reply_text(
            f"🏠 Моніторим 🏠\n"
            f"{action_text}\n"
            f"{status_emoji} Поточний стан електроенергії: {current_state}\n"
            f"🌐 Перевірка хоста: {HOST}:{PORT} кожні {CHECK_INTERVAL_SECONDS}s"
        )

    async def on_startup(self, application: Application) -> None:
        logging.info("Starting monitor loop for %s:%s", HOST, PORT)
        self._monitor_task = asyncio.create_task(self._monitor_loop(application), name="svitlo-monitor-loop")

    async def on_shutdown(self, application: Application) -> None:
        if not self._monitor_task:
            return
        self._monitor_task.cancel()
        try:
            await self._monitor_task
        except asyncio.CancelledError:
            pass

    async def _monitor_loop(self, application: Application) -> None:
        while True:
            try:
                new_value = await self._check_power_status()
                now_iso = datetime.now(timezone.utc).isoformat()

                if self._state is None:
                    self._state = PowerState(value=new_value, updated_at=now_iso)
                    self._save_state()
                    logging.info("Initial power state: %s", new_value)
                elif new_value != self._state.value:
                    previous = self._state.value
                    self._state = PowerState(value=new_value, updated_at=now_iso)
                    self._save_state()
                    logging.info("Power state changed: %s -> %s", previous, new_value)
                    await self._broadcast_state_change(application, previous, new_value)
                else:
                    self._state.updated_at = now_iso
                    self._save_state()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Unexpected monitor loop error")

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

    async def _check_power_status(self) -> str:
        try:
            connect_future = asyncio.open_connection(HOST, PORT)
            _, writer = await asyncio.wait_for(connect_future, timeout=CONNECT_TIMEOUT_SECONDS)
            writer.close()
            await writer.wait_closed()
            return "ON"
        except (asyncio.TimeoutError, OSError) as error:
            logging.warning("Перевірка електроенергії не вдалася (%r). Вважаємо вимкненою.", error)
            return "OFF"
        except Exception:
            logging.exception("Непередбачена помилка під час перевірки електроенергії. Вважаємо вимкненою.")
            return "OFF"

    async def _broadcast_state_change(self, application: Application, old_state: str, new_state: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state_line = "⚡ Світло Є" if new_state == "ON" else "🚫 Світло пропало"
        text = (
            f"🏠 Попередження про стан електроенергії\n"
            f"{state_line}\n"
            f"🔄 Попередній стан: {old_state}\n"
            f"🕒 Виявлено о: {timestamp}"
        )

        async with self._lock:
            chat_ids = list(self._subscribers)

        for chat_id in chat_ids:
            should_remove = False
            try:
                await application.bot.send_message(chat_id=chat_id, text=text)
            except RetryAfter as error:
                await asyncio.sleep(error.retry_after + 1)
                try:
                    await application.bot.send_message(chat_id=chat_id, text=text)
                except Exception:
                    logging.exception("Failed to notify chat_id=%s after retry", chat_id)
            except (Forbidden, BadRequest):
                should_remove = True
                logging.warning("Removing unreachable chat_id=%s", chat_id)
            except (TimedOut, NetworkError):
                logging.warning("Temporary Telegram error while notifying chat_id=%s", chat_id)
            except Exception:
                logging.exception("Unexpected error while notifying chat_id=%s", chat_id)

            if should_remove:
                await self._remove_subscriber(chat_id)

    async def _remove_subscriber(self, chat_id: int) -> None:
        async with self._lock:
            if chat_id in self._subscribers:
                self._subscribers.remove(chat_id)
                self._save_subscribers()

    @staticmethod
    def _load_subscribers() -> Set[int]:
        if not SUBSCRIBERS_FILE.exists():
            return set()
        try:
            payload = json.loads(SUBSCRIBERS_FILE.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return {int(chat_id) for chat_id in payload}
        except Exception:
            logging.exception("Could not load subscribers from %s", SUBSCRIBERS_FILE)
        return set()

    def _save_subscribers(self) -> None:
        temp_file = SUBSCRIBERS_FILE.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps(sorted(self._subscribers), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(SUBSCRIBERS_FILE)

    @staticmethod
    def _load_state() -> Optional[PowerState]:
        if not STATE_FILE.exists():
            return None
        try:
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            value = payload.get("value")
            updated_at = payload.get("updated_at")
            if value in {"ON", "OFF"} and isinstance(updated_at, str):
                return PowerState(value=value, updated_at=updated_at)
        except Exception:
            logging.exception("Could not load power state from %s", STATE_FILE)
        return None

    def _save_state(self) -> None:
        if self._state is None:
            return
        temp_file = STATE_FILE.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps({"value": self._state.value, "updated_at": self._state.updated_at}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(STATE_FILE)


def build_application() -> Application:
    load_dotenv()
    token = os.getenv("SVITLO_YANUSHA_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing SVITLO_YANUSHA_BOT_TOKEN in .env")

    bot = SvitloBot()
    application = (
        Application.builder()
        .token(token)
        .post_init(bot.on_startup)
        .post_shutdown(bot.on_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("start", bot.start_command))
    return application


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    app = build_application()
    app.run_polling(close_loop=False, drop_pending_updates=True)


if __name__ == "__main__":
    main()
