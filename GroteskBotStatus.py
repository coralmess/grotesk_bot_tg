import json
import time
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram import Bot
from telegram.constants import ParseMode

KYIV_TZ = ZoneInfo("Europe/Kyiv")
STATUS_MSG_FILE = Path(__file__).with_name("status_message_id.txt")
LAST_RUNS_FILE = Path(__file__).with_name("last_runs.json")

LAST_OLX_RUN_UTC = None
LAST_SHAFA_RUN_UTC = None
LAST_LYST_RUN_START_UTC = None
LAST_LYST_RUN_OK = None
LAST_LYST_RUN_NOTE = ""
LYST_RUN_HAD_ERRORS = False
LYST_RUN_NOTES = []
_LYST_RUN_STARTED_THIS_CYCLE = False


def load_last_runs_from_file():
    global LAST_OLX_RUN_UTC, LAST_SHAFA_RUN_UTC, LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_OK, LAST_LYST_RUN_NOTE
    if not LAST_RUNS_FILE.exists():
        return
    try:
        data = json.loads(LAST_RUNS_FILE.read_text(encoding="utf-8"))
        olx_raw = data.get("last_olx_run_utc")
        shafa_raw = data.get("last_shafa_run_utc")
        lyst_raw = data.get("last_lyst_run_start_utc")
        lyst_ok = data.get("last_lyst_run_ok")
        lyst_note = data.get("last_lyst_run_note")
        if olx_raw:
            LAST_OLX_RUN_UTC = datetime.fromisoformat(olx_raw)
        if shafa_raw:
            LAST_SHAFA_RUN_UTC = datetime.fromisoformat(shafa_raw)
        if lyst_raw:
            LAST_LYST_RUN_START_UTC = datetime.fromisoformat(lyst_raw)
        if isinstance(lyst_ok, bool):
            LAST_LYST_RUN_OK = lyst_ok
        if isinstance(lyst_note, str):
            LAST_LYST_RUN_NOTE = lyst_note
    except Exception:
        pass


def save_last_runs_to_file():
    try:
        payload = {
            "last_olx_run_utc": LAST_OLX_RUN_UTC.isoformat() if LAST_OLX_RUN_UTC else None,
            "last_shafa_run_utc": LAST_SHAFA_RUN_UTC.isoformat() if LAST_SHAFA_RUN_UTC else None,
            "last_lyst_run_start_utc": LAST_LYST_RUN_START_UTC.isoformat() if LAST_LYST_RUN_START_UTC else None,
            "last_lyst_run_ok": LAST_LYST_RUN_OK,
            "last_lyst_run_note": LAST_LYST_RUN_NOTE,
        }
        LAST_RUNS_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def mark_olx_run():
    global LAST_OLX_RUN_UTC
    LAST_OLX_RUN_UTC = datetime.now(timezone.utc)
    save_last_runs_to_file()


def mark_shafa_run():
    global LAST_SHAFA_RUN_UTC
    LAST_SHAFA_RUN_UTC = datetime.now(timezone.utc)
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
    global LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_OK, _LYST_RUN_STARTED_THIS_CYCLE
    if _LYST_RUN_STARTED_THIS_CYCLE:
        return
    LAST_LYST_RUN_START_UTC = datetime.now(timezone.utc)
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
    global LAST_LYST_RUN_OK, LAST_LYST_RUN_NOTE, _LYST_RUN_STARTED_THIS_CYCLE
    LAST_LYST_RUN_OK = not LYST_RUN_HAD_ERRORS
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
        except Exception:
            pass
    msg = await bot.send_message(chat_id=chat_id, text="🟢 Bot status: starting...", parse_mode=ParseMode.HTML)
    try:
        STATUS_MSG_FILE.write_text(str(msg.message_id), encoding="utf-8")
    except Exception:
        pass
    return msg.message_id


def _format_status_text(start_ts: float) -> str:
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
    lyst_time = LAST_LYST_RUN_START_UTC.astimezone(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S') if LAST_LYST_RUN_START_UTC else "never"
    if LAST_LYST_RUN_OK is True:
        lyst_icon = "🟢"
        lyst_note = f" ({LAST_LYST_RUN_NOTE})" if LAST_LYST_RUN_NOTE else ""
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
        f"🧾 Last OLX run: {olx_str}\n"
        f"🧾 Last SHAFA run: {shafa_str}\n"
        f"{lyst_icon} Last LYST run: {lyst_time}{lyst_note}"
    )


async def status_heartbeat(bot_token: str, chat_id: int, interval_s: int = 600):
    if not bot_token or not chat_id:
        return
    bot = Bot(token=bot_token)
    start_ts = time.time()
    message_id = await _ensure_status_message(bot, chat_id)
    while True:
        text = _format_status_text(start_ts)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            # If edit fails (message deleted or not found), send a new one and persist its id
            try:
                msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                message_id = msg.message_id
                try:
                    STATUS_MSG_FILE.write_text(str(message_id), encoding="utf-8")
                except Exception:
                    pass
            except Exception:
                pass
        await asyncio.sleep(interval_s)
