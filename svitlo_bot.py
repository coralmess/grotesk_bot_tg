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

EMOJI_HOME = "\U0001F3E0"
EMOJI_OK = "\u2705"
EMOJI_INFO = "\u2139\ufe0f"
EMOJI_LIGHT_ON = "\u26a1"
EMOJI_LIGHT_OFF = "\U0001F6AB"
EMOJI_PLUG = "\U0001F50C"
EMOJI_GLOBE = "\U0001F310"
EMOJI_REFRESH = "\U0001F504"
EMOJI_CLOCK = "\U0001F552"
EMOJI_TIMER = "\u23f1\ufe0f"

UA_DAY_ONE = "\u0434\u0435\u043d\u044c"
UA_DAY_FEW = "\u0434\u043d\u0456"
UA_DAY_MANY = "\u0434\u043d\u0456\u0432"
UA_HOUR_ONE = "\u0433\u043e\u0434\u0438\u043d\u0430"
UA_HOUR_FEW = "\u0433\u043e\u0434\u0438\u043d\u0438"
UA_HOUR_MANY = "\u0433\u043e\u0434\u0438\u043d"
UA_MINUTES_SHORT = "\u0445\u0432"
UA_WITHOUT_LIGHT = "\u0411\u0435\u0437 \u0441\u0432\u0456\u0442\u043b\u0430"
UA_WITH_LIGHT = "\u0417\u0456 \u0441\u0432\u0456\u0442\u043b\u043e\u043c"
UA_UNKNOWN = "\u043d\u0435\u0432\u0456\u0434\u043e\u043c\u043e"
UA_HEADER = "\u041c\u043e\u043d\u0456\u0442\u043e\u0440 \u0435\u043b\u0435\u043a\u0442\u0440\u043e\u043f\u043e\u0441\u0442\u0430\u0447\u0430\u043d\u043d\u044f"
UA_ACTION_SUBSCRIBED = "\u041f\u0456\u0434\u043f\u0438\u0441\u043a\u0443 \u0430\u043a\u0442\u0438\u0432\u043e\u0432\u0430\u043d\u043e."
UA_ACTION_ALREADY = "\u0412\u0438 \u0432\u0436\u0435 \u043f\u0456\u0434\u043f\u0438\u0441\u0430\u043d\u0456."
UA_STATUS_LINE = "\u041f\u043e\u0442\u043e\u0447\u043d\u0438\u0439 \u0441\u0442\u0430\u043d: {state}"
UA_HOST_CHECK = "\u041f\u0435\u0440\u0435\u0432\u0456\u0440\u043a\u0430 \u0445\u043e\u0441\u0442\u0430: {host}:{port} \u043a\u043e\u0436\u043d\u0456 {seconds}\u0441"
UA_STATE_ON = "\u0441\u0432\u0456\u0442\u043b\u043e \u0454"
UA_STATE_OFF = "\u0441\u0432\u0456\u0442\u043b\u0430 \u043d\u0435\u043c\u0430\u0454"
UA_ALERT_HEADER = "\u0417\u043c\u0456\u043d\u0430 \u0441\u0442\u0430\u043d\u0443 \u0435\u043b\u0435\u043a\u0442\u0440\u043e\u043f\u043e\u0441\u0442\u0430\u0447\u0430\u043d\u043d\u044f"
UA_ALERT_ON = "\u0421\u0432\u0456\u0442\u043b\u043e \u0437\u0027\u044f\u0432\u0438\u043b\u043e\u0441\u044f"
UA_ALERT_OFF = "\u0421\u0432\u0456\u0442\u043b\u043e \u0437\u043d\u0438\u043a\u043b\u043e"
UA_PREVIOUS = "\u041f\u043e\u043f\u0435\u0440\u0435\u0434\u043d\u0456\u0439 \u0441\u0442\u0430\u043d: {state}"
UA_DURATION = "{label}: {duration}"
UA_DETECTED_AT = "\u0417\u0430\u0444\u0456\u043a\u0441\u043e\u0432\u0430\u043d\u043e \u043e: {timestamp}"


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
        state_emoji = EMOJI_LIGHT_ON if current_state == "ON" else EMOJI_PLUG
        action_text = f"{EMOJI_OK} {UA_ACTION_SUBSCRIBED}" if added else f"{EMOJI_INFO} {UA_ACTION_ALREADY}"

        await update.message.reply_text(
            f"{EMOJI_HOME} {UA_HEADER}\n"
            f"{action_text}\n"
            f"{state_emoji} {UA_STATUS_LINE.format(state=current_state_ua)}\n"
            f"{EMOJI_GLOBE} {UA_HOST_CHECK.format(host=HOST, port=PORT, seconds=CHECK_INTERVAL_SECONDS)}"
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state_line = f"{EMOJI_LIGHT_ON} {UA_ALERT_ON}" if new_state == "ON" else f"{EMOJI_LIGHT_OFF} {UA_ALERT_OFF}"
        duration_label = UA_WITHOUT_LIGHT if old_state == "OFF" else UA_WITH_LIGHT
        previous_state_ua = self._state_to_ua(old_state)
        text = (
            f"{EMOJI_HOME} {UA_ALERT_HEADER}\n"
            f"{state_line}\n"
            f"{EMOJI_REFRESH} {UA_PREVIOUS.format(state=previous_state_ua)}\n"
            f"{EMOJI_TIMER} {UA_DURATION.format(label=duration_label, duration=previous_duration)}\n"
            f"{EMOJI_CLOCK} {UA_DETECTED_AT.format(timestamp=timestamp)}"
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
            return UA_UNKNOWN

        total_seconds = max(0, int((end_dt - start_dt).total_seconds()))
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days > 0:
            day_word = SvitloBot._plural_uk(days, UA_DAY_ONE, UA_DAY_FEW, UA_DAY_MANY)
            hour_word = SvitloBot._plural_uk(hours, UA_HOUR_ONE, UA_HOUR_FEW, UA_HOUR_MANY)
            return f"{days} {day_word} {hours} {hour_word}"
        if hours > 0:
            hour_word = SvitloBot._plural_uk(hours, UA_HOUR_ONE, UA_HOUR_FEW, UA_HOUR_MANY)
            return f"{hours} {hour_word} {minutes}{UA_MINUTES_SHORT}"
        return f"{max(1, minutes)}{UA_MINUTES_SHORT}"

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
            return UA_STATE_ON
        if state == "OFF":
            return UA_STATE_OFF
        return UA_UNKNOWN


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
