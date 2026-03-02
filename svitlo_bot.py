import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import Update
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes
from helpers.runtime_paths import SVITLO_SUBSCRIBERS_JSON_FILE, SVITLO_STATE_JSON_FILE

HOST = "grotesk.tplinkdns.com"
PORT = 45678
CHECK_INTERVAL_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 8
QUIET_HOURS_START = 23
QUIET_HOURS_END = 9

SUBSCRIBERS_FILE = SVITLO_SUBSCRIBERS_JSON_FILE
STATE_FILE = SVITLO_STATE_JSON_FILE
KYIV_TZ = ZoneInfo("Europe/Kyiv")


@dataclass
class PowerState:
    value: str
    updated_at: str
    changed_at: str


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

        current_state = self._state.value if self._state else "UNKNOWN"
        current_state_ua = self._state_to_ua(current_state)
        state_emoji = "⚡" if current_state == "ON" else "🔌"
        action_text = "✅ Підписку активовано." if added else "ℹ️ Ви вже підписані."

        silent = self._is_quiet_hours(datetime.now(KYIV_TZ))

        await update.message.reply_text(
            "🏠 Монітор електропостачання\n"
            f"{action_text}\n"
            f"{state_emoji} Поточний стан: {current_state_ua}",
            disable_notification=silent,
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
                    self._state = PowerState(value=new_value, updated_at=now_iso, changed_at=now_iso)
                    self._save_state()
                    logging.info("Initial power state: %s", new_value)
                elif new_value != self._state.value:
                    previous = self._state.value
                    previous_duration = self._format_state_duration(self._state.changed_at, now_iso)
                    self._state = PowerState(value=new_value, updated_at=now_iso, changed_at=now_iso)
                    self._save_state()
                    logging.info("Power state changed: %s -> %s", previous, new_value)
                    await self._broadcast_state_change(application, previous, new_value, previous_duration)
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
            logging.warning("Power check failed (%r). Treating as OFF.", error)
            return "OFF"
        except Exception:
            logging.exception("Unhandled power-check error. Treating as OFF.")
            return "OFF"

    async def _broadcast_state_change(
        self,
        application: Application,
        old_state: str,
        new_state: str,
        previous_duration: str,
    ) -> None:
        now_kyiv = datetime.now(KYIV_TZ)
        timestamp = now_kyiv.strftime("%d.%m %H:%M")
        silent = self._is_quiet_hours(now_kyiv)
        state_line = "⚡ Світло з'явилося ⚡" if new_state == "ON" else "🕯 Світло зникло 🕯"
        duration_label = "Без світла" if old_state == "OFF" else "Зі світлом"
        previous_state_ua = self._state_to_ua(old_state)
        text = (
            f"{state_line}\n\n"
            f"⏱️ {duration_label}: {previous_duration}\n"
            f"⌚️ Час апдейту: {timestamp}"
        )

        async with self._lock:
            chat_ids = list(self._subscribers)

        for chat_id in chat_ids:
            should_remove = False
            try:
                await application.bot.send_message(chat_id=chat_id, text=text, disable_notification=silent)
            except RetryAfter as error:
                await asyncio.sleep(error.retry_after + 1)
                try:
                    await application.bot.send_message(chat_id=chat_id, text=text, disable_notification=silent)
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
            changed_at = payload.get("changed_at", updated_at)
            if value in {"ON", "OFF"} and isinstance(updated_at, str) and isinstance(changed_at, str):
                return PowerState(value=value, updated_at=updated_at, changed_at=changed_at)
        except Exception:
            logging.exception("Could not load power state from %s", STATE_FILE)
        return None

    def _save_state(self) -> None:
        if self._state is None:
            return
        temp_file = STATE_FILE.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps(
                {
                    "value": self._state.value,
                    "updated_at": self._state.updated_at,
                    "changed_at": self._state.changed_at,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        temp_file.replace(STATE_FILE)

    @staticmethod
    def _format_state_duration(start_iso: str, end_iso: str) -> str:
        try:
            start_dt = datetime.fromisoformat(start_iso)
            end_dt = datetime.fromisoformat(end_iso)
        except ValueError:
            return "невідомо"

        total_seconds = max(0, int((end_dt - start_dt).total_seconds()))
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days > 0:
            day_word = SvitloBot._plural_uk(days, "день", "дні", "днів")
            hour_word = SvitloBot._plural_uk(hours, "година", "години", "годин")
            return f"{days} {day_word} {hours} {hour_word}"
        if hours > 0:
            hour_word = SvitloBot._plural_uk(hours, "година", "години", "годин")
            return f"{hours} {hour_word} {minutes}хв"
        return f"{max(1, minutes)}хв"

    @staticmethod
    def _plural_uk(n: int, one: str, few: str, many: str) -> str:
        n_abs = abs(n) % 100
        n1 = n_abs % 10
        if 11 <= n_abs <= 14:
            return many
        if n1 == 1:
            return one
        if 2 <= n1 <= 4:
            return few
        return many

    @staticmethod
    def _state_to_ua(state: str) -> str:
        if state == "ON":
            return "Світло є"
        if state == "OFF":
            return "Світла немає"
        return "Невідомо"

    @staticmethod
    def _is_quiet_hours(dt_kyiv: datetime) -> bool:
        return dt_kyiv.hour >= QUIET_HOURS_START or dt_kyiv.hour < QUIET_HOURS_END


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
