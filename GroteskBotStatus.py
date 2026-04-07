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
from helpers.runtime_paths import (
    STATUS_MESSAGE_ID_FILE,
    LAST_RUNS_JSON_FILE,
    MARKET_OLX_RUN_STATUS_FILE,
    MARKET_SHAFA_RUN_STATUS_FILE,
    LYST_RUN_STATUS_FILE,
)

KYIV_TZ = ZoneInfo("Europe/Kyiv")
STATUS_MSG_FILE = STATUS_MESSAGE_ID_FILE
LAST_RUNS_FILE = LAST_RUNS_JSON_FILE

STATUS_FILES = {
    "olx": MARKET_OLX_RUN_STATUS_FILE,
    "shafa": MARKET_SHAFA_RUN_STATUS_FILE,
    "lyst": LYST_RUN_STATUS_FILE,
}

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
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _serialize_datetime(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _write_text_atomic(path: Path, text: str) -> None:
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


def _default_service_status() -> dict:
    return {
        "last_run_start_utc": None,
        "last_run_end_utc": None,
        "last_run_ok": None,
        "last_run_note": "",
    }


def _normalize_service_status(data: dict | None) -> dict:
    status = _default_service_status()
    if not isinstance(data, dict):
        return status
    status["last_run_start_utc"] = _parse_utc_datetime(data.get("last_run_start_utc"))
    status["last_run_end_utc"] = _parse_utc_datetime(data.get("last_run_end_utc"))
    last_run_ok = data.get("last_run_ok")
    status["last_run_ok"] = last_run_ok if isinstance(last_run_ok, bool) else None
    last_run_note = data.get("last_run_note")
    status["last_run_note"] = last_run_note if isinstance(last_run_note, str) else ""
    return status


def _status_payload(status: dict) -> dict:
    return {
        "last_run_start_utc": _serialize_datetime(status.get("last_run_start_utc")),
        "last_run_end_utc": _serialize_datetime(status.get("last_run_end_utc")),
        "last_run_ok": status.get("last_run_ok"),
        "last_run_note": status.get("last_run_note") or "",
    }


def _read_service_status(path: Path) -> dict:
    if not path.exists():
        return _default_service_status()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning(f"Failed to read service status from {path}: {exc}")
        return _default_service_status()
    return _normalize_service_status(data)


def _write_service_status(path: Path, status: dict) -> None:
    try:
        _write_json_atomic(path, _status_payload(status))
    except Exception as exc:
        LOGGER.error(f"Failed to save service status to {path}: {exc}")


def _update_service_status(path: Path, **fields) -> dict:
    status = _read_service_status(path)
    status.update(fields)
    _write_service_status(path, status)
    return status


def _seed_status_from_legacy_if_needed() -> None:
    if any(path.exists() for path in STATUS_FILES.values()):
        return
    if not LAST_RUNS_FILE.exists():
        return
    try:
        data = json.loads(LAST_RUNS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning(f"Failed to load legacy last-runs state from {LAST_RUNS_FILE}: {exc}")
        return

    olx_status = _default_service_status()
    olx_status["last_run_end_utc"] = _parse_utc_datetime(data.get("last_olx_run_utc"))
    olx_status["last_run_ok"] = not bool(data.get("last_olx_run_note")) if olx_status["last_run_end_utc"] else None
    olx_status["last_run_note"] = data.get("last_olx_run_note") or ""

    shafa_status = _default_service_status()
    shafa_status["last_run_end_utc"] = _parse_utc_datetime(data.get("last_shafa_run_utc"))
    shafa_status["last_run_ok"] = not bool(data.get("last_shafa_run_note")) if shafa_status["last_run_end_utc"] else None
    shafa_status["last_run_note"] = data.get("last_shafa_run_note") or ""

    lyst_status = _default_service_status()
    lyst_status["last_run_start_utc"] = _parse_utc_datetime(data.get("last_lyst_run_start_utc"))
    lyst_status["last_run_end_utc"] = _parse_utc_datetime(data.get("last_lyst_run_end_utc"))
    lyst_ok = data.get("last_lyst_run_ok")
    lyst_status["last_run_ok"] = lyst_ok if isinstance(lyst_ok, bool) else None
    lyst_status["last_run_note"] = data.get("last_lyst_run_note") or ""

    _write_service_status(STATUS_FILES["olx"], olx_status)
    _write_service_status(STATUS_FILES["shafa"], shafa_status)
    _write_service_status(STATUS_FILES["lyst"], lyst_status)


def read_all_service_statuses() -> dict[str, dict]:
    _seed_status_from_legacy_if_needed()
    return {name: _read_service_status(path) for name, path in STATUS_FILES.items()}


def save_last_runs_to_file():
    statuses = read_all_service_statuses()
    try:
        payload = {
            "last_olx_run_utc": _serialize_datetime(statuses["olx"]["last_run_end_utc"]),
            "last_shafa_run_utc": _serialize_datetime(statuses["shafa"]["last_run_end_utc"]),
            "last_olx_run_note": statuses["olx"]["last_run_note"],
            "last_shafa_run_note": statuses["shafa"]["last_run_note"],
            "last_lyst_run_start_utc": _serialize_datetime(statuses["lyst"]["last_run_start_utc"]),
            "last_lyst_run_end_utc": _serialize_datetime(statuses["lyst"]["last_run_end_utc"]),
            "last_lyst_run_ok": statuses["lyst"]["last_run_ok"],
            "last_lyst_run_note": statuses["lyst"]["last_run_note"],
        }
        _write_json_atomic(LAST_RUNS_FILE, payload)
    except Exception as exc:
        LOGGER.error(f"Failed to save compatibility last-runs state to {LAST_RUNS_FILE}: {exc}")


def _refresh_cached_status() -> None:
    global LAST_OLX_RUN_UTC, LAST_SHAFA_RUN_UTC, LAST_OLX_RUN_NOTE, LAST_SHAFA_RUN_NOTE
    global LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_END_UTC, LAST_LYST_RUN_OK, LAST_LYST_RUN_NOTE
    statuses = read_all_service_statuses()
    LAST_OLX_RUN_UTC = statuses["olx"]["last_run_end_utc"]
    LAST_SHAFA_RUN_UTC = statuses["shafa"]["last_run_end_utc"]
    LAST_OLX_RUN_NOTE = statuses["olx"]["last_run_note"]
    LAST_SHAFA_RUN_NOTE = statuses["shafa"]["last_run_note"]
    LAST_LYST_RUN_START_UTC = statuses["lyst"]["last_run_start_utc"]
    LAST_LYST_RUN_END_UTC = statuses["lyst"]["last_run_end_utc"]
    LAST_LYST_RUN_OK = statuses["lyst"]["last_run_ok"]
    LAST_LYST_RUN_NOTE = statuses["lyst"]["last_run_note"]


def load_last_runs_from_file():
    _refresh_cached_status()


def write_olx_status(*, start_utc=None, end_utc=None, ok=None, note=None) -> None:
    updates = {}
    if start_utc is not None:
        updates["last_run_start_utc"] = start_utc
    if end_utc is not None:
        updates["last_run_end_utc"] = end_utc
    if ok is not None:
        updates["last_run_ok"] = ok
    if note is not None:
        updates["last_run_note"] = note
    _update_service_status(STATUS_FILES["olx"], **updates)
    save_last_runs_to_file()
    _refresh_cached_status()


def write_shafa_status(*, start_utc=None, end_utc=None, ok=None, note=None) -> None:
    updates = {}
    if start_utc is not None:
        updates["last_run_start_utc"] = start_utc
    if end_utc is not None:
        updates["last_run_end_utc"] = end_utc
    if ok is not None:
        updates["last_run_ok"] = ok
    if note is not None:
        updates["last_run_note"] = note
    _update_service_status(STATUS_FILES["shafa"], **updates)
    save_last_runs_to_file()
    _refresh_cached_status()


def write_lyst_status(*, start_utc=None, end_utc=None, ok=None, note=None) -> None:
    updates = {}
    if start_utc is not None:
        updates["last_run_start_utc"] = start_utc
    if end_utc is not None:
        updates["last_run_end_utc"] = end_utc
    if ok is not None:
        updates["last_run_ok"] = ok
    if note is not None:
        updates["last_run_note"] = note
    _update_service_status(STATUS_FILES["lyst"], **updates)
    save_last_runs_to_file()
    _refresh_cached_status()


def mark_olx_run(note: str | None = None):
    write_olx_status(end_utc=datetime.now(timezone.utc), ok=not bool(note), note=note or "")


def mark_shafa_run(note: str | None = None):
    write_shafa_status(end_utc=datetime.now(timezone.utc), ok=not bool(note), note=note or "")


def mark_olx_issue(note: str):
    if note:
        write_olx_status(note=note, ok=False)


def mark_shafa_issue(note: str):
    if note:
        write_shafa_status(note=note, ok=False)


def reset_lyst_run_status():
    global LYST_RUN_HAD_ERRORS, LYST_RUN_NOTES, LAST_LYST_RUN_NOTE, _LYST_RUN_STARTED_THIS_CYCLE
    LYST_RUN_HAD_ERRORS = False
    LYST_RUN_NOTES = []
    LAST_LYST_RUN_NOTE = ""
    _LYST_RUN_STARTED_THIS_CYCLE = False


def begin_lyst_cycle():
    reset_lyst_run_status()


def mark_lyst_start():
    global LAST_LYST_RUN_START_UTC, LAST_LYST_RUN_END_UTC, LAST_LYST_RUN_OK, _LYST_RUN_STARTED_THIS_CYCLE
    if _LYST_RUN_STARTED_THIS_CYCLE:
        return
    LAST_LYST_RUN_START_UTC = datetime.now(timezone.utc)
    LAST_LYST_RUN_END_UTC = None
    LAST_LYST_RUN_OK = None
    _LYST_RUN_STARTED_THIS_CYCLE = True
    write_lyst_status(start_utc=LAST_LYST_RUN_START_UTC, end_utc=None, ok=None, note="")


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
    write_lyst_status(
        start_utc=LAST_LYST_RUN_START_UTC,
        end_utc=LAST_LYST_RUN_END_UTC,
        ok=LAST_LYST_RUN_OK,
        note=LAST_LYST_RUN_NOTE,
    )


async def _ensure_status_message(bot: Bot, chat_id: int) -> int:
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
    statuses = read_all_service_statuses()
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

    olx = statuses["olx"]
    shafa = statuses["shafa"]
    lyst = statuses["lyst"]
    olx_str = olx["last_run_end_utc"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S") if olx["last_run_end_utc"] else "never"
    shafa_str = shafa["last_run_end_utc"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S") if shafa["last_run_end_utc"] else "never"
    olx_note = f" ({olx['last_run_note']})" if olx["last_run_note"] else ""
    shafa_note = f" ({shafa['last_run_note']})" if shafa["last_run_note"] else ""

    lyst_time = "never"
    if lyst["last_run_ok"] is None and lyst["last_run_start_utc"]:
        lyst_time = lyst["last_run_start_utc"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S")
    elif lyst["last_run_end_utc"]:
        lyst_time = lyst["last_run_end_utc"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S")
    elif lyst["last_run_start_utc"]:
        lyst_time = lyst["last_run_start_utc"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M:%S")

    lyst_stale = False
    if lyst_stale_after_sec and lyst["last_run_ok"] is True and lyst["last_run_end_utc"]:
        age_sec = (datetime.now(timezone.utc) - lyst["last_run_end_utc"]).total_seconds()
        if age_sec > lyst_stale_after_sec:
            lyst_stale = True

    if lyst["last_run_ok"] is True and not lyst_stale:
        lyst_icon = "🟢"
        lyst_note = f" ({lyst['last_run_note']})" if lyst["last_run_note"] else ""
    elif lyst["last_run_ok"] is True and lyst_stale:
        lyst_icon = "🟡"
        lyst_note = " (stale)"
    elif lyst["last_run_ok"] is False:
        lyst_icon = "🔴"
        lyst_note = f" ({lyst['last_run_note']})" if lyst["last_run_note"] else " (Unknown error)"
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
