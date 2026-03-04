from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


def build_context_lines(
    *,
    attempt=None,
    max_retries=None,
    max_scroll_attempts=None,
    use_pagination=None,
    block_resources=False,
    page_scrape=False,
    image_strategy="adaptive",
    image_ready_target=0.0,
    image_extra_scrolls=0,
    image_settle_passes=0,
    lyst_http_only=False,
    lyst_http_only_enabled=False,
    lyst_http_only_disabled_reason="",
    lyst_http_timeout_sec=0,
    lyst_page_timeout_sec=0,
    lyst_url_timeout_sec=0,
    lyst_stall_timeout_sec=0,
    lyst_max_browsers=0,
    lyst_shoe_concurrency=0,
    lyst_country_concurrency=0,
    live_mode=False,
):
    return [
        f"attempt: {attempt}/{max_retries}" if attempt is not None and max_retries is not None else "attempt: ",
        f"block_resources: {block_resources}",
        f"page_scrape: {page_scrape}",
        f"use_pagination: {use_pagination if use_pagination is not None else ''}",
        f"max_scroll_attempts: {max_scroll_attempts if max_scroll_attempts is not None else ''}",
        f"image_strategy: {image_strategy}",
        f"image_ready_target: {image_ready_target}",
        f"image_extra_scrolls: {image_extra_scrolls}",
        f"image_settle_passes: {image_settle_passes}",
        f"lyst_http_only: {lyst_http_only}",
        f"lyst_http_only_enabled: {lyst_http_only_enabled}",
        f"lyst_http_only_disabled_reason: {lyst_http_only_disabled_reason}",
        f"lyst_http_timeout_sec: {lyst_http_timeout_sec}",
        f"lyst_page_timeout_sec: {lyst_page_timeout_sec}",
        f"lyst_url_timeout_sec: {lyst_url_timeout_sec}",
        f"lyst_stall_timeout_sec: {lyst_stall_timeout_sec}",
        f"lyst_max_browsers: {lyst_max_browsers}",
        f"lyst_shoe_concurrency: {lyst_shoe_concurrency}",
        f"lyst_country_concurrency: {lyst_country_concurrency}",
        f"live_mode: {live_mode}",
    ]


def reset_http_only_state(*, lyst_http_only_default: bool):
    return lyst_http_only_default, ""


def disable_http_only(*, currently_enabled: bool, reason: str, logger):
    if not currently_enabled:
        return currently_enabled, ""
    disable_reason = reason or "disabled"
    logger.warning(f"LYST HTTP-only disabled for this run: {disable_reason}")
    return False, disable_reason


def _write_json_atomic(path: Path, payload):
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp")
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def load_resume_state(*, resume_file: Path, logger):
    try:
        data = json.loads(resume_file.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "entries" in data:
            data.setdefault("resume_active", False)
            if not isinstance(data.get("entries"), dict):
                data["entries"] = {}
            return data
    except Exception as exc:
        logger.warning(f"Failed to load Lyst resume state from {resume_file}: {exc}")
    return {"resume_active": False, "entries": {}}


def save_resume_state(*, resume_file: Path, state: dict, logger):
    try:
        _write_json_atomic(resume_file, state)
    except Exception as exc:
        logger.error(f"Failed to persist Lyst resume state to {resume_file}: {exc}")


async def update_resume_entry(
    *,
    resume_lock: asyncio.Lock,
    resume_state: dict,
    key: str,
    fields: dict,
    now_kyiv_str_fn: Callable[[], str],
    save_state_fn: Callable[[dict], None],
):
    async with resume_lock:
        entries = resume_state.setdefault("entries", {})
        entry = entries.get(key, {})
        entry.update(fields)
        entry["updated_at"] = now_kyiv_str_fn()
        entries[key] = entry
        save_state_fn(resume_state)


def init_resume_state(*, loaded_state: dict, abort_event: asyncio.Event):
    state = loaded_state
    if not state.get("resume_active", False):
        state["entries"] = {}
    if abort_event.is_set():
        abort_event.clear()
    return state, False, {}


async def mark_run_failed(
    *,
    reason: str,
    resume_lock: asyncio.Lock,
    resume_state: dict,
    run_progress: dict,
    now_kyiv_str_fn: Callable[[], str],
    save_state_fn: Callable[[dict], None],
    abort_event: asyncio.Event,
):
    abort_event.set()
    async with resume_lock:
        resume_state["resume_active"] = True
        resume_state["last_failure_reason"] = reason
        resume_state["last_failure_at"] = now_kyiv_str_fn()
        resume_state["last_run_progress"] = dict(run_progress)
        save_state_fn(resume_state)
    return True


def log_run_progress_summary(*, run_progress: dict, logger):
    if not run_progress:
        return
    logger.error("Lyst run progress before abort:")
    for key, page in sorted(run_progress.items()):
        logger.error(f"LYST progress {key}: last_scraped_page={page}")


async def finalize_resume_after_processing(
    *,
    resume_lock: asyncio.Lock,
    resume_state: dict,
    run_failed: bool,
    save_state_fn: Callable[[dict], None],
):
    async with resume_lock:
        entries = resume_state.get("entries", {})
        for key, entry in entries.items():
            last_scraped = entry.get("last_scraped_page")
            if last_scraped is None:
                continue
            entry["last_success_page"] = last_scraped
            if entry.get("scrape_complete") and not run_failed:
                entry["completed"] = True
                entry["next_page"] = 1
            else:
                entry["completed"] = False
                entry["next_page"] = (last_scraped + 1) if last_scraped else entry.get("next_page", 1)
        if run_failed:
            resume_state["resume_active"] = True
        save_state_fn(resume_state)


def touch_progress(
    *,
    step: str | None,
    details: dict[str, Any],
    kyiv_tz,
):
    last_progress_ts = time.time()
    if step is None:
        return last_progress_ts, {}
    info = {
        "step": step,
        "ts": last_progress_ts,
        "ts_kyiv": datetime.now(kyiv_tz).strftime("%Y-%m-%d %H:%M:%S"),
    }
    info.update({k: v for k, v in details.items() if v is not None})
    return last_progress_ts, info


def step_snapshot(last_step_info: dict):
    return dict(last_step_info) if last_step_info else {}


def format_task_stack(task):
    if task is None:
        return []
    try:
        frames = task.get_stack()
    except Exception:
        return []
    if not frames:
        return []
    lines = []
    for frame in frames[-6:]:
        try:
            lines.append(f"{frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}")
        except Exception:
            continue
    return lines


def format_tasks_snapshot(*, file_hint: str, limit=10):
    try:
        tasks = asyncio.all_tasks()
    except Exception:
        return []
    lines = []
    seen = 0
    for task in tasks:
        if task.done() or task is asyncio.current_task():
            continue
        try:
            frames = task.get_stack()
        except Exception:
            continue
        if not frames:
            continue
        if not any(file_hint in frame.f_code.co_filename for frame in frames):
            continue
        task_name = task.get_name() if hasattr(task, "get_name") else str(task)
        lines.append(f"Task {task_name} id={id(task)}")
        for frame in frames[-4:]:
            try:
                lines.append(f"  {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}")
            except Exception:
                continue
        seen += 1
        if seen >= limit:
            break
    return lines


def describe_task_wait_chain(task, max_depth=6):
    if task is None:
        return []
    lines = []
    try:
        coro = task.get_coro()
    except Exception:
        return []
    awaitable = getattr(coro, "cr_await", None)
    depth = 0
    seen = set()
    while awaitable is not None and depth < max_depth:
        if id(awaitable) in seen:
            lines.append("  ↳ await chain loop detected")
            break
        seen.add(id(awaitable))
        lines.append(f"  ↳ awaiting {type(awaitable).__name__}: {awaitable!r}")
        try:
            if isinstance(awaitable, asyncio.Task):
                lines.append(f"    task id={id(awaitable)} done={awaitable.done()} cancelled={awaitable.cancelled()}")
                frames = awaitable.get_stack()
                for frame in frames[-3:]:
                    try:
                        lines.append(f"    {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}")
                    except Exception:
                        continue
                awaitable = getattr(awaitable.get_coro(), "cr_await", None)
            elif isinstance(awaitable, asyncio.Future):
                lines.append(f"    future done={awaitable.done()} cancelled={awaitable.cancelled()}")
                awaitable = getattr(awaitable, "_fut_waiter", None) or getattr(awaitable, "cr_await", None)
            else:
                awaitable = getattr(awaitable, "cr_await", None)
        except Exception:
            break
        depth += 1
    return lines

