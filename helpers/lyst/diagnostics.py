from __future__ import annotations

from datetime import datetime

from helpers import lyst_debug as lyst_debug_helpers
from helpers import lyst_state as lyst_state_helpers


def url_suffix(url_name=None, page_num=None):
    if url_name or page_num is not None:
        return f" | url_name={url_name or ''} page={page_num if page_num is not None else ''}"
    return ""


def page_progress_data(url, country, url_name=None, page_num=None, attempt=None):
    data = {
        "url": url,
        "country": country,
        "url_name": url_name,
        "page_num": page_num,
    }
    if attempt is not None:
        data["attempt"] = attempt
    return data


def safe_page_final_url(page):
    try:
        return page.url
    except Exception:
        return None


def step_snapshot(last_step_info: dict):
    return lyst_state_helpers.step_snapshot(last_step_info)


def format_task_stack(task):
    return lyst_state_helpers.format_task_stack(task)


def format_tasks_snapshot(*, file_hint: str, limit=10):
    return lyst_state_helpers.format_tasks_snapshot(file_hint=file_hint, limit=limit)


def describe_task_wait_chain(task, max_depth=6):
    return lyst_state_helpers.describe_task_wait_chain(task, max_depth=max_depth)


def debug_snapshot(*, page, kyiv_tz, log_lines_func):
    # Fetch and cycle code need a small, stable snapshot helper without knowing
    # how log tails and page URL probing are implemented.
    return (
        datetime.now(kyiv_tz).strftime("%Y-%m-%d %H:%M:%S"),
        log_lines_func(),
        safe_page_final_url(page),
    )


async def dump_debug_event_safe(
    prefix,
    *,
    reason,
    url,
    country,
    url_name,
    page_num,
    step,
    page,
    debug_events,
    context_lines,
    kyiv_tz,
    log_lines_func,
    content=None,
    shield=False,
):
    now_kyiv, log_lines, final_url = debug_snapshot(
        page=page,
        kyiv_tz=kyiv_tz,
        log_lines_func=log_lines_func,
    )
    payload = dict(
        reason=reason,
        url=url,
        country=country,
        url_name=url_name,
        page_num=page_num,
        step=step,
        page=page,
        content=content,
        now_kyiv=now_kyiv,
        log_lines=log_lines,
        extra_lines=debug_events,
        final_url=final_url,
        context_lines=context_lines,
    )
    coroutine = lyst_debug_helpers.dump_lyst_debug_event(prefix, **payload)
    if shield:
        await coroutine
        return
    await coroutine
