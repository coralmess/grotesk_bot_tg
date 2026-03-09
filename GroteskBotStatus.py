import json
import time
import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram import Bot
from telegram.constants import ParseMode
from helpers.runtime_paths import STATUS_MESSAGE_ID_FILE, LAST_RUNS_JSON_FILE

KYIV_TZ = ZoneInfo("Europe/Kyiv")
STATUS_MSG_FILE = STATUS_MESSAGE_ID_FILE
LAST_RUNS_FILE = LAST_RUNS_JSON_FILE

LAST_OLX_RUN_UTC = None
LAST_SHAFA_RUN_UTC = None
LAST_OLX_RUN_NOTE = ""
LAST_SHAFA_RUN_NOTE = ""
LAST_LYST_RUN_START_UTC = None
LAST_LYST_RUN_END_UTC = None
LAST_LYST_RUN_OK = None
LAST_LYST_RUN_NOTE = ""
LYST_RUN_HAD_ERRORS = False
LYST_RUN_NOTES = []
_LYST_RUN_STARTED_THIS_CYCLE = False
LOGGER = logging.getLogger(__name__)


def _parse_utc_datetime(raw_value):
    if not raw_value:
        return None
    parsed = datetime.fromisoformat(raw_value)
    # Backward compatibility: old state files stored naive timestamps.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _write_text_atomic(path: Path, text: str) -> None:
    # Atomic replace prevents partial state files after abrupt process termination.
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def _write_json_atomic(path: Path, payload: dict) -> None:
    _write_text_atomic(path, json.dumps(payload, ensure_ascii=False))


def load_last_runs_from_file():
    global LAST_OLX_RUN_UTC, LAST_SHAFA_RUN_UTC, LAST_OLX_RUN_NOTE, LAST_SHAFA_RUN_NOTE
    global LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_END_UTC, LAST_LYST_RUN_OK, LAST_LYST_RUN_NOTE
    if not LAST_RUNS_FILE.exists():
        return
    try:
        data = json.loads(LAST_RUNS_FILE.read_text(encoding="utf-8"))
        olx_raw = data.get("last_olx_run_utc")
        shafa_raw = data.get("last_shafa_run_utc")
        olx_note = data.get("last_olx_run_note")
        shafa_note = data.get("last_shafa_run_note")
        lyst_raw = data.get("last_lyst_run_start_utc")
        lyst_end_raw = data.get("last_lyst_run_end_utc")
        lyst_ok = data.get("last_lyst_run_ok")
        lyst_note = data.get("last_lyst_run_note")
        if olx_raw:
            LAST_OLX_RUN_UTC = _parse_utc_datetime(olx_raw)
        if shafa_raw:
            LAST_SHAFA_RUN_UTC = _parse_utc_datetime(shafa_raw)
        if isinstance(olx_note, str):
            LAST_OLX_RUN_NOTE = olx_note
        if isinstance(shafa_note, str):
            LAST_SHAFA_RUN_NOTE = shafa_note
        if lyst_raw:
            LAST_LYST_RUN_START_UTC = _parse_utc_datetime(lyst_raw)
        if lyst_end_raw:
            LAST_LYST_RUN_END_UTC = _parse_utc_datetime(lyst_end_raw)
        if isinstance(lyst_ok, bool):
            LAST_LYST_RUN_OK = lyst_ok
        if isinstance(lyst_note, str):
            LAST_LYST_RUN_NOTE = lyst_note
    except Exception as exc:
        LOGGER.warning(f"Failed to load last-runs state from {LAST_RUNS_FILE}: {exc}")


def save_last_runs_to_file():
    try:
        payload = {
            "last_olx_run_utc": LAST_OLX_RUN_UTC.isoformat() if LAST_OLX_RUN_UTC else None,
            "last_shafa_run_utc": LAST_SHAFA_RUN_UTC.isoformat() if LAST_SHAFA_RUN_UTC else None,
            "last_olx_run_note": LAST_OLX_RUN_NOTE,
            "last_shafa_run_note": LAST_SHAFA_RUN_NOTE,
            "last_lyst_run_start_utc": LAST_LYST_RUN_START_UTC.isoformat() if LAST_LYST_RUN_START_UTC else None,
            "last_lyst_run_end_utc": LAST_LYST_RUN_END_UTC.isoformat() if LAST_LYST_RUN_END_UTC else None,
            "last_lyst_run_ok": LAST_LYST_RUN_OK,
            "last_lyst_run_note": LAST_LYST_RUN_NOTE,
        }
        _write_json_atomic(LAST_RUNS_FILE, payload)
    except Exception as exc:
        LOGGER.error(f"Failed to save last-runs state to {LAST_RUNS_FILE}: {exc}")


def mark_olx_run(note: str | None = None):
    global LAST_OLX_RUN_UTC, LAST_OLX_RUN_NOTE
    LAST_OLX_RUN_UTC = datetime.now(timezone.utc)
    LAST_OLX_RUN_NOTE = note or ""
    save_last_runs_to_file()


def mark_shafa_run(note: str | None = None):
    global LAST_SHAFA_RUN_UTC, LAST_SHAFA_RUN_NOTE
    LAST_SHAFA_RUN_UTC = datetime.now(timezone.utc)
    LAST_SHAFA_RUN_NOTE = note or ""
    save_last_runs_to_file()


def mark_olx_issue(note: str):
    global LAST_OLX_RUN_NOTE
    if note:
        LAST_OLX_RUN_NOTE = note
    save_last_runs_to_file()


def mark_shafa_issue(note: str):
    global LAST_SHAFA_RUN_NOTE
    if note:
        LAST_SHAFA_RUN_NOTE = note
    save_last_runs_to_file()


def reset_lyst_run_status():
    global LYST_RUN_HAD_ERRORS, LYST_RUN_NOTES, LAST_LYST_RUN_NOTE, _LYST_RUN_STARTED_THIS_CYCLE
    LYST_RUN_HAD_ERRORS = False
    LYST_RUN_NOTES = []
    LAST_LYST_RUN_NOTE = ""
    _LYST_RUN_STARTED_THIS_CYCLE = False


def begin_lyst_cycle():
    """Reset per-cycle status before launching Lyst scraping tasks."""
    reset_lyst_run_status()


def mark_lyst_start():
    global LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_END_UTC, LAST_LYST_RUN_OK, _LYST_RUN_STARTED_THIS_CYCLE
    if _LYST_RUN_STARTED_THIS_CYCLE:
        return
    LAST_LYST_RUN_START_UTC = datetime.now(timezone.utc)
    LAST_LYST_RUN_END_UTC = None
    LAST_LYST_RUN_OK = None
    _LYST_RUN_STARTED_THIS_CYCLE = True
    save_last_runs_to_file()


def mark_lyst_issue(note: str):
    global LYST_RUN_HAD_ERRORS, LYST_RUN_NOTES, LAST_LYST_RUN_NOTE
    if note:
        LYST_RUN_NOTES.append(note)
        LAST_LYST_RUN_NOTE = note
    LYST_RUN_HAD_ERRORS = True


def finalize_lyst_run():
    global LAST_LYST_RUN_OK, LAST_LYST_RUN_END_UTC, LAST_LYST_RUN_NOTE, _LYST_RUN_STARTED_THIS_CYCLE
    LAST_LYST_RUN_OK = not LYST_RUN_HAD_ERRORS
    LAST_LYST_RUN_END_UTC = datetime.now(timezone.utc)
    if not LAST_LYST_RUN_OK and LYST_RUN_NOTES:
        LAST_LYST_RUN_NOTE = "; ".join(sorted(set(LYST_RUN_NOTES)))
    _LYST_RUN_STARTED_THIS_CYCLE = False
    save_last_runs_to_file()


async def _ensure_status_message(bot: Bot, chat_id: int) -> int:
    """Get or create the status message and persist its message_id."""
    if STATUS_MSG_FILE.exists():
        try:
            stored = STATUS_MSG_FILE.read_text(encoding="utf-8").strip()
            if stored.isdigit():
                return int(stored)
        except Exception as exc:
            LOGGER.warning(f"Failed to read status message id from {STATUS_MSG_FILE}: {exc}")
    msg = await bot.send_message(chat_id=chat_id, text="🟢 Bot status: starting...", parse_mode=ParseMode.HTML)
    try:
        _write_text_atomic(STATUS_MSG_FILE, str(msg.message_id))
    except Exception as exc:
        LOGGER.error(f"Failed to save status message id to {STATUS_MSG_FILE}: {exc}")
    try:
        chat = await bot.get_chat(chat_id=chat_id)
        pinned_message = getattr(chat, "pinned_message", None)
        if pinned_message and pinned_message.message_id != msg.message_id:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=pinned_message.message_id)
    except Exception:
        pass
    try:
        await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        pass
    return msg.message_id


def _format_status_text(start_ts: float, *, lyst_stale_after_sec: int | None = None) -> str:
    now = datetime.now(KYIV_TZ)
    uptime_sec = int(time.time() - start_ts)
    if uptime_sec < 3600:
        minutes = uptime_sec // 60
        uptime_str = f"{minutes}m"
    elif uptime_sec < 86400:
        hours = uptime_sec // 3600
        minutes = (uptime_sec % 3600) // 60
        uptime_str = f"{hours}h {minutes}m"
    else:
        days = uptime_sec // 86400
        hours = (uptime_sec % 86400) // 3600
        uptime_str = f"{days}d {hours}h"
    olx_str = LAST_OLX_RUN_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S') if LAST_OLX_RUN_UTC else "never"
    shafa_str = LAST_SHAFA_RUN_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S') if LAST_SHAFA_RUN_UTC else "never"
    olx_note = f" ({LAST_OLX_RUN_NOTE})" if LAST_OLX_RUN_NOTE else ""
    shafa_note = f" ({LAST_SHAFA_RUN_NOTE})" if LAST_SHAFA_RUN_NOTE else ""

    lyst_time = "never"
    if LAST_LYST_RUN_OK is None and LAST_LYST_RUN_START_UTC:
        lyst_time = LAST_LYST_RUN_START_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
    elif LAST_LYST_RUN_END_UTC:
        lyst_time = LAST_LYST_RUN_END_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
    elif LAST_LYST_RUN_START_UTC:
        lyst_time = LAST_LYST_RUN_START_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')

    lyst_stale = False
    if lyst_stale_after_sec and LAST_LYST_RUN_OK is True and LAST_LYST_RUN_END_UTC:
        age_sec = (datetime.now(timezone.utc) - LAST_LYST_RUN_END_UTC).total_seconds()
        if age_sec > lyst_stale_after_sec:
            lyst_stale = True

    if LAST_LYST_RUN_OK is True and not lyst_stale:
        lyst_icon = "🟢"
        lyst_note = f" ({LAST_LYST_RUN_NOTE})" if LAST_LYST_RUN_NOTE else ""
    elif LAST_LYST_RUN_OK is True and lyst_stale:
        lyst_icon = "🟡"
        lyst_note = " (stale)"
    elif LAST_LYST_RUN_OK is False:
        lyst_icon = "🔴"
        lyst_note = f" ({LAST_LYST_RUN_NOTE})" if LAST_LYST_RUN_NOTE else " (Unknown error)"
    else:
        lyst_icon = "🟡"
        lyst_note = " (running)"
    return (
        "✅ <b>Grotesk Bot OK</b>\n"
        f"⏱ Uptime: {uptime_str}\n"
        f"🕒 Last update (Kyiv): {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🧾 Last OLX run: {olx_str}{olx_note}\n"
        f"🧾 Last SHAFA run: {shafa_str}{shafa_note}\n"
        f"{lyst_icon} Last LYST run: {lyst_time}{lyst_note}"
    )


async def status_heartbeat(bot_token: str, chat_id: int, interval_s: int = 600, *, lyst_stale_after_sec: int | None = None):
    if not bot_token or not chat_id:
        return
    bot = Bot(token=bot_token)
    start_ts = time.time()
    message_id = await _ensure_status_message(bot, chat_id)
    while True:
        try:
            text = _format_status_text(start_ts, lyst_stale_after_sec=lyst_stale_after_sec)
        except Exception:
            LOGGER.exception("Failed to format status heartbeat text")
            await asyncio.sleep(interval_s)
            continue
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            # If edit fails (message deleted or not found), send a new one and persist its id
            try:
                old_message_id = message_id
                msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                message_id = msg.message_id
                try:
                    _write_text_atomic(STATUS_MSG_FILE, str(message_id))
                except Exception as exc:
                    LOGGER.error(f"Failed to update status message id in {STATUS_MSG_FILE}: {exc}")
                try:
                    if old_message_id and old_message_id != message_id:
                        await bot.unpin_chat_message(chat_id=chat_id, message_id=old_message_id)
                except Exception:
                    pass
                try:
                    chat = await bot.get_chat(chat_id=chat_id)
                    pinned_message = getattr(chat, "pinned_message", None)
                    if pinned_message and pinned_message.message_id != message_id:
                        await bot.unpin_chat_message(chat_id=chat_id, message_id=pinned_message.message_id)
                except Exception:
                    pass
                try:
                    await bot.pin_chat_message(chat_id=chat_id, message_id=message_id, disable_notification=True)
                except Exception:
                    pass
            except Exception:
                LOGGER.exception("Status heartbeat failed to update Telegram message")
        await asyncio.sleep(interval_s)
