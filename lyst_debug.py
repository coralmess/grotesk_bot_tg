from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional


def attach_lyst_debug_listeners(page, collector: list[str]) -> None:
    def _safe_append(line: str) -> None:
        if line:
            collector.append(line)
            if len(collector) > 200:
                del collector[: len(collector) - 200]
    def _get_value(val) -> str:
        try:
            return val() if callable(val) else val
        except Exception:
            return ""

    page.on("console", lambda msg: _safe_append(f"console.{_get_value(msg.type)}: {_get_value(msg.text)}"))
    page.on("pageerror", lambda exc: _safe_append(f"pageerror: {exc}"))
    page.on(
        "requestfailed",
        lambda req: _safe_append(f"requestfailed: {req.method} {req.url} {req.failure}"),
    )
    page.on(
        "request",
        lambda req: _safe_append(
            f"request: {req.method} {req.url} type={req.resource_type}"
        )
        if req.resource_type == "document"
        else None,
    )
    page.on(
        "response",
        lambda resp: _safe_append(
            f"response: {resp.status} {resp.url} type={resp.request.resource_type}"
        )
        if resp.status >= 400 or resp.request.resource_type == "document"
        else None,
    )


def build_lyst_debug_meta(
    *,
    reason: str,
    url: str,
    country: str,
    url_name: Optional[str] = None,
    page_num: Optional[int] = None,
    step: Optional[str] = None,
    final_url: Optional[str] = None,
    now_kyiv: Optional[str] = None,
    log_lines: Optional[Iterable[str]] = None,
    extra_lines: Optional[Iterable[str]] = None,
    context_lines: Optional[Iterable[str]] = None,
) -> list[str]:
    now_val = now_kyiv or datetime.now().isoformat(sep=" ", timespec="seconds")
    lines = [
        f"timestamp_kyiv: {now_val}",
        f"reason: {reason}",
        f"url_name: {url_name or ''}",
        f"country: {country}",
        f"page: {page_num if page_num is not None else ''}",
        f"step: {step or ''}",
        f"url: {url}",
        f"final_url: {final_url or ''}",
    ]
    if context_lines:
        lines.extend(["", "context:", *context_lines])
    if extra_lines:
        lines.extend(["", "page_debug_events:", *extra_lines])
    if log_lines:
        lines.extend(["", "last_200_log_lines:", *log_lines])
    return lines


def write_lyst_debug_dump(
    prefix: str,
    *,
    content: Optional[str],
    meta_lines: Optional[Iterable[str]],
    screenshot_bytes: Optional[bytes] = None,
    base_dir: Optional[Path] = None,
) -> None:
    base = base_dir or Path(__file__).parent
    html_path = base / f"{prefix}.html"
    meta_path = base / f"{prefix}_meta.txt"
    screenshot_path = base / f"{prefix}.png"
    try:
        html_path.write_text(content or "", encoding="utf-8", errors="replace")
    except Exception:
        pass
    if meta_lines is not None:
        try:
            meta_path.write_text("\n".join(meta_lines), encoding="utf-8", errors="replace")
        except Exception:
            pass
    if screenshot_bytes:
        try:
            screenshot_path.write_bytes(screenshot_bytes)
        except Exception:
            pass


async def dump_lyst_debug(
    prefix: str,
    *,
    page=None,
    content: Optional[str] = None,
    meta_lines: Optional[Iterable[str]] = None,
    base_dir: Optional[Path] = None,
    take_screenshot: bool = True,
) -> None:
    screenshot_bytes = None
    if page and take_screenshot:
        try:
            screenshot_bytes = await page.screenshot(full_page=True)
        except Exception:
            screenshot_bytes = None
    if content is None and page:
        try:
            content = await page.content()
        except Exception:
            content = None
    write_lyst_debug_dump(
        prefix,
        content=content,
        meta_lines=meta_lines,
        screenshot_bytes=screenshot_bytes,
        base_dir=base_dir,
    )


async def dump_lyst_debug_event(
    prefix: str,
    *,
    reason: str,
    url: str,
    country: str,
    url_name: Optional[str] = None,
    page_num: Optional[int] = None,
    step: Optional[str] = None,
    page=None,
    content: Optional[str] = None,
    now_kyiv: Optional[str] = None,
    log_lines: Optional[Iterable[str]] = None,
    extra_lines: Optional[Iterable[str]] = None,
    final_url: Optional[str] = None,
    context_lines: Optional[Iterable[str]] = None,
    base_dir: Optional[Path] = None,
    take_screenshot: bool = True,
) -> None:
    meta_lines = build_lyst_debug_meta(
        reason=reason,
        url=url,
        country=country,
        url_name=url_name,
        page_num=page_num,
        step=step,
        final_url=final_url,
        now_kyiv=now_kyiv,
        log_lines=log_lines,
        extra_lines=extra_lines,
        context_lines=context_lines,
    )
    await dump_lyst_debug(
        prefix,
        page=page,
        content=content,
        meta_lines=meta_lines,
        base_dir=base_dir,
        take_screenshot=take_screenshot,
    )


def write_stop_too_early_dump(
    *,
    reason: str,
    url: str,
    country: str,
    url_name: str,
    page_num: Optional[int] = None,
    step: Optional[str] = None,
    content: Optional[str],
    now_kyiv: str,
    log_lines: Iterable[str],
    context_lines: Optional[Iterable[str]] = None,
    base_dir: Optional[Path] = None,
) -> None:
    meta_lines = build_lyst_debug_meta(
        reason=reason,
        url=url,
        country=country,
        url_name=url_name,
        page_num=page_num,
        step=step,
        now_kyiv=now_kyiv,
        log_lines=log_lines,
        context_lines=context_lines,
    )
    write_lyst_debug_dump(
        "lyst_stop_too_early",
        content=content,
        meta_lines=meta_lines,
        base_dir=base_dir,
    )
