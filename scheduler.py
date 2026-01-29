import asyncio
import random
import time


def _sleep_interval_with_jitter(base_sec: int, jitter_sec: int) -> int:
    if base_sec <= 0:
        return 60
    if jitter_sec <= 0:
        return base_sec
    low = max(1, base_sec - jitter_sec)
    high = base_sec + jitter_sec
    return random.randint(low, high)


def _schedule_next_run(min_sec: int, max_sec: int) -> float:
    if min_sec < 1 or max_sec < min_sec:
        min_sec, max_sec = 900, 3600
    return time.time() + random.randint(min_sec, max_sec)


async def run_scheduler(
    *,
    run_olx,
    run_shafa,
    run_lyst,
    is_running_lyst,
    get_lyst_progress_ts,
    check_interval_sec,
    check_jitter_sec,
    logger,
    last_olx_run_exists,
    last_shafa_run_exists,
    olx_timeout_sec=1800,
    shafa_timeout_sec=1800,
    lyst_stall_timeout_sec=1800,
    olx_min_sec=900,
    olx_max_sec=3600,
    shafa_min_sec=900,
    shafa_max_sec=3600,
):
    next_olx_ts = _schedule_next_run(olx_min_sec, olx_max_sec)
    next_shafa_ts = _schedule_next_run(shafa_min_sec, shafa_max_sec)
    next_lyst_ts = time.time()
    if not last_olx_run_exists:
        next_olx_ts = time.time()
    if not last_shafa_run_exists:
        next_shafa_ts = time.time()

    olx_task = None
    shafa_task = None
    lyst_task = None

    while True:
        try:
            now_ts = time.time()
            if now_ts >= next_olx_ts:
                if olx_task is None or olx_task.done():
                    next_olx_ts = _schedule_next_run(olx_min_sec, olx_max_sec)
                    olx_task = asyncio.create_task(asyncio.wait_for(run_olx(), timeout=olx_timeout_sec))
            if now_ts >= next_shafa_ts:
                if shafa_task is None or shafa_task.done():
                    next_shafa_ts = _schedule_next_run(shafa_min_sec, shafa_max_sec)
                    shafa_task = asyncio.create_task(asyncio.wait_for(run_shafa(), timeout=shafa_timeout_sec))

            if is_running_lyst() and now_ts >= next_lyst_ts:
                if lyst_task is None or lyst_task.done():
                    lyst_task = asyncio.create_task(run_lyst())
                    next_lyst_ts = time.time() + _sleep_interval_with_jitter(check_interval_sec, check_jitter_sec)
            elif not is_running_lyst():
                logger.info("Lyst scraping disabled (IsRunningLyst=false)")

            if lyst_task is not None and not lyst_task.done():
                progress_ts = get_lyst_progress_ts()
                if progress_ts and (time.time() - progress_ts) > lyst_stall_timeout_sec:
                    logger.error("Lyst task stalled; cancelling")
                    lyst_task.cancel()
                    next_lyst_ts = time.time() + _sleep_interval_with_jitter(check_interval_sec, check_jitter_sec)

            if lyst_task is not None and lyst_task.done():
                if lyst_task.cancelled():
                    logger.warning("Lyst task cancelled")
                else:
                    try:
                        exc = lyst_task.exception()
                    except asyncio.CancelledError:
                        exc = None
                    if exc:
                        logger.error(f"Lyst task crashed: {exc}")
                lyst_task = None
            if olx_task is not None and olx_task.done():
                if olx_task.cancelled():
                    logger.warning("OLX task cancelled")
                else:
                    try:
                        exc = olx_task.exception()
                    except asyncio.CancelledError:
                        exc = None
                    if exc:
                        logger.error(f"OLX task crashed: {exc}")
                olx_task = None
            if shafa_task is not None and shafa_task.done():
                if shafa_task.cancelled():
                    logger.warning("SHAFA task cancelled")
                else:
                    try:
                        exc = shafa_task.exception()
                    except asyncio.CancelledError:
                        exc = None
                    if exc:
                        logger.error(f"SHAFA task crashed: {exc}")
                shafa_task = None

            next_wake = min(next_olx_ts, next_shafa_ts, next_lyst_ts or now_ts)
            progress_ts = get_lyst_progress_ts()
            if lyst_task is not None and not lyst_task.done() and progress_ts:
                next_wake = min(next_wake, progress_ts + lyst_stall_timeout_sec)
            sleep_for = max(5, int(next_wake - time.time()))
            logger.info(f"Sleeping for {sleep_for} seconds before next check")
            await asyncio.sleep(sleep_for)
        except KeyboardInterrupt:
            logger.info("Script terminated by user")
            break
        except Exception as exc:
            logger.error(f"An unexpected error occurred in main loop: {exc}")
            sleep_for = _sleep_interval_with_jitter(check_interval_sec, check_jitter_sec)
            logger.info(f"Waiting for {sleep_for} seconds before retrying")
            await asyncio.sleep(sleep_for)
