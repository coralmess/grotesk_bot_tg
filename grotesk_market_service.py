import asyncio
import logging
import sqlite3
import time

from GroteskBotStatus import (
    load_last_runs_from_file,
    mark_olx_issue,
    mark_olx_run,
    mark_shafa_issue,
    mark_shafa_run,
    read_all_service_statuses,
)
from config import (
    TELEGRAM_OLX_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    DANYLO_DEFAULT_CHAT_ID,
    MAINTENANCE_INTERVAL_SEC,
    DB_VACUUM,
    OLX_RETENTION_DAYS,
    SHAFA_RETENTION_DAYS,
)
from helpers.dynamic_sources import add_dynamic_url
from helpers import scraper_unsubscribes as scraper_unsubscribes_helpers
from helpers.service_health import build_service_health
from helpers import telegram_runtime as telegram_runtime_helpers
from helpers.scheduler import run_market_scheduler
from helpers.runtime_paths import OLX_ITEMS_DB_FILE, SHAFA_ITEMS_DB_FILE
from olx_scraper import run_olx_scraper
from shafa_scraper import run_shafa_scraper


logger = logging.getLogger("grotesk_market_service")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

SERVICE_HEALTH = build_service_health("grotesk-market")

DB_MAINTENANCE_PRAGMAS = (
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA busy_timeout=5000;",
    "PRAGMA wal_checkpoint(TRUNCATE);",
    "PRAGMA optimize;",
    "ANALYZE;",
)


def get_allowed_chat_ids():
    return telegram_runtime_helpers.get_allowed_chat_ids(DANYLO_DEFAULT_CHAT_ID, TELEGRAM_CHAT_ID)


def _run_db_retention_cleanup(conn, db_path):
    if db_path == OLX_ITEMS_DB_FILE and OLX_RETENTION_DAYS > 0:
        conn.execute(
            "DELETE FROM olx_items WHERE updated_at < datetime('now', ?)",
            (f"-{OLX_RETENTION_DAYS} days",),
        )
        conn.commit()
    if db_path == SHAFA_ITEMS_DB_FILE and SHAFA_RETENTION_DAYS > 0:
        conn.execute(
            "DELETE FROM shafa_items WHERE updated_at < datetime('now', ?)",
            (f"-{SHAFA_RETENTION_DAYS} days",),
        )
        conn.commit()


def _db_maintenance_sync(db_files=None):
    if db_files is None:
        db_files = [OLX_ITEMS_DB_FILE, SHAFA_ITEMS_DB_FILE]
    for db_path in db_files:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(db_path)
            for pragma in DB_MAINTENANCE_PRAGMAS:
                conn.execute(pragma)
            _run_db_retention_cleanup(conn, db_path)
            if DB_VACUUM:
                conn.execute("VACUUM;")
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning(f"DB maintenance failed for {db_path.name}: {exc}")


async def maintenance_loop(interval_s: int):
    if interval_s <= 0:
        return
    while True:
        await asyncio.to_thread(_db_maintenance_sync, [OLX_ITEMS_DB_FILE, SHAFA_ITEMS_DB_FILE])
        await asyncio.sleep(interval_s)


async def command_listener(bot_token, allowed_chat_ids):
    await telegram_runtime_helpers.command_listener(
        bot_token,
        allowed_chat_ids,
        log_path=OLX_ITEMS_DB_FILE,
        line_count=0,
        add_dynamic_url_func=add_dynamic_url,
        unsubscribe_item_func=scraper_unsubscribes_helpers.unsubscribe_from_reply_message,
        allow_log_commands=False,
        allow_add_commands=True,
        allow_unsubscribe_commands=True,
        logger=logger,
    )


async def _run_olx_and_mark():
    started = time.perf_counter()
    err = await run_olx_scraper()
    if err:
        SERVICE_HEALTH.record_failure("olx_run", err, duration_seconds=time.perf_counter() - started)
        mark_olx_issue(err)
    else:
        SERVICE_HEALTH.record_success("olx_run", duration_seconds=time.perf_counter() - started)
    mark_olx_run(err if err else None)


async def _run_shafa_and_mark():
    started = time.perf_counter()
    err = await run_shafa_scraper()
    if err:
        SERVICE_HEALTH.record_failure("shafa_run", err, duration_seconds=time.perf_counter() - started)
        mark_shafa_issue(err)
    else:
        SERVICE_HEALTH.record_success("shafa_run", duration_seconds=time.perf_counter() - started)
    mark_shafa_run(err if err else None)


async def _shutdown_background_tasks(tasks):
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    load_last_runs_from_file()
    background_tasks = []
    SERVICE_HEALTH.start()
    SERVICE_HEALTH.mark_ready("market service starting")

    def _start_background_task(coro, task_name):
        task = asyncio.create_task(coro, name=task_name)
        background_tasks.append(task)
        return task

    _start_background_task(SERVICE_HEALTH.heartbeat_loop(note="market service running"), "market_health_heartbeat")
    _start_background_task(
        command_listener(TELEGRAM_OLX_BOT_TOKEN, get_allowed_chat_ids()),
        "market_command_listener",
    )
    if MAINTENANCE_INTERVAL_SEC > 0:
        _start_background_task(maintenance_loop(MAINTENANCE_INTERVAL_SEC), "market_db_maintenance")

    try:
        statuses = read_all_service_statuses()
        await run_market_scheduler(
            run_olx=_run_olx_and_mark,
            run_shafa=_run_shafa_and_mark,
            logger=logger,
            last_olx_run_exists=statuses["olx"]["last_run_end_utc"] is not None,
            last_shafa_run_exists=statuses["shafa"]["last_run_end_utc"] is not None,
        )
    finally:
        SERVICE_HEALTH.mark_stopping("market service stopping")
        await _shutdown_background_tasks(background_tasks)


if __name__ == "__main__":
    asyncio.run(main())
