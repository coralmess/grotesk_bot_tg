from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass(slots=True)
class PageRunConfig:
    page_scrape: bool
    max_scroll_attempts: int | None
    log_tail_lines: int = 200


@dataclass(slots=True)
class PageRunHooks:
    logger: Any
    scrape_page: Callable[..., Awaitable[tuple[list[dict], str | None, str]]]
    resume_key: Callable[[dict, str], str]
    should_skip_source_for_backoff: Callable[[str, str], bool]
    touch_progress: Callable[..., None]
    scrape_target_url: Callable[[dict, int, bool], str]
    log_scrape_target: Callable[[str, str, int, bool], None]
    update_resume_with_url: Callable[..., Awaitable[None]]
    record_cloudflare_failure: Callable[[str, str, int | None], Any]
    mark_issue: Callable[[str], None]
    mark_run_failed: Callable[[str], Awaitable[None]]
    log_run_progress_summary: Callable[[], None]
    build_context_lines: Callable[..., list[str]]
    tail_log_lines: Callable[[int], list[str]]
    write_stop_too_early_dump: Callable[..., None]
    now_kyiv: Callable[[], str]
    record_source_success: Callable[[str, str], None]
    sleep: Callable[[float], Awaitable[None]]
    record_page_event: Callable[..., None] = lambda **fields: None


async def scrape_all_pages(
    base_url: dict,
    country: str,
    *,
    config: PageRunConfig,
    hooks: PageRunHooks,
    resume_state: dict,
    resume_entry_outcomes: dict,
    run_progress: dict,
    abort_event,
    use_pagination: bool | None = None,
) -> list[dict]:
    if use_pagination is None:
        use_pagination = config.page_scrape

    max_scroll_attempts = config.max_scroll_attempts
    all_shoes: list[dict] = []
    url_name = base_url["url_name"]
    key = hooks.resume_key(base_url, country)
    resume_active = resume_state.get("resume_active", False)
    entry = resume_state.get("entries", {}).get(key, {})
    if resume_active and entry.get("completed"):
        hooks.logger.info(f"Skipping {url_name} for {country} (completed in previous run)")
        return all_shoes
    if hooks.should_skip_source_for_backoff(url_name, country):
        hooks.logger.warning(f"Skipping {url_name} for {country} due to active Cloudflare cooldown")
        resume_entry_outcomes[key] = "cloudflare_cooldown"
        hooks.record_page_event(
            source_name=url_name,
            country=country,
            page=None,
            status="cloudflare_cooldown",
            items_scraped=0,
            use_pagination=use_pagination,
        )
        return all_shoes

    page = entry.get("next_page", 1) if resume_active else 1
    started_from_resume_page = bool(resume_active and use_pagination and page > 1)
    last_scraped_page = entry.get("last_scraped_page", entry.get("last_success_page", 0))
    if use_pagination and page > 1:
        hooks.logger.info(f"Resuming {url_name} for {country} from page {page}")
    completed_without_fetch_failure = False

    while True:
        if abort_event.is_set():
            break
        hooks.touch_progress("scrape_page_start", url_name=url_name, country=country, page_num=page)
        url = hooks.scrape_target_url(base_url, page, use_pagination)
        hooks.log_scrape_target(url_name, country, page, use_pagination)

        shoes, content, status = await hooks.scrape_page(
            url,
            country,
            max_scroll_attempts=max_scroll_attempts,
            url_name=url_name,
            page_num=page if use_pagination else None,
            use_pagination=use_pagination,
        )
        hooks.touch_progress(
            "scrape_page_end",
            url=url,
            url_name=url_name,
            country=country,
            page_num=page,
            status=status,
        )
        # Page-level telemetry is intentionally recorded before each branch mutates
        # resume state, so Cloudflare/terminal/empty pages can be reconstructed later.
        hooks.record_page_event(
            source_name=url_name,
            country=country,
            page=page,
            status=status,
            items_scraped=len(shoes or []),
            use_pagination=use_pagination,
            terminal_final_page=last_scraped_page if status == "terminal" else None,
        )
        if status == "cloudflare":
            resume_entry_outcomes[key] = "cloudflare"
            hooks.logger.error(f"Cloudflare challenge for {url_name} {country} page {page}")
            decision = hooks.record_cloudflare_failure(url_name, country, page)
            hooks.logger.warning(
                "Cloudflare cooldown for %s %s: failures=%s cooldown=%ss",
                url_name,
                country,
                decision.failure_count,
                decision.cooldown_sec,
            )
            # Keep Cloudflare local to this source/country. The cooldown and
            # resume entry preserve retry safety without aborting independent work.
            hooks.mark_issue("Cloudflare challenge")
            await hooks.update_resume_with_url(
                key,
                url,
                next_page=page,
                last_scraped_page=last_scraped_page,
                completed=False,
                failure_reason="Cloudflare challenge",
            )
            hooks.log_run_progress_summary()
            break
        if status == "aborted":
            resume_entry_outcomes[key] = "aborted"
            hooks.logger.info(f"Aborting {url_name} for {country} after Lyst run abort signal")
            break
        if status == "failed":
            resume_entry_outcomes[key] = "failed"
            hooks.logger.error(f"Failed to fetch page for {url_name} {country} page {page}")
            await hooks.update_resume_with_url(
                key,
                url,
                next_page=page,
                last_scraped_page=last_scraped_page,
                completed=False,
                failure_reason="Failed to get soup",
            )
            await hooks.mark_run_failed("Failed to get soup")
            hooks.log_run_progress_summary()
            break
        if status == "terminal":
            hooks.logger.info(f"{url_name} for {country} reached terminal page {page} (HTTP 410)")
            completed_without_fetch_failure = True
            if started_from_resume_page and not all_shoes:
                resume_entry_outcomes[key] = "terminal_only_resume"
            else:
                resume_entry_outcomes[key] = "terminal"
            await hooks.update_resume_with_url(
                key,
                url,
                scrape_complete=True,
                final_page=last_scraped_page,
                completed=False,
            )
            break
        if not shoes:
            resume_entry_outcomes[key] = "empty"
            stopped_too_early = use_pagination and page < 3
            if stopped_too_early:
                hooks.logger.error(f"{url_name} for {country} Stopped too early. Please check for errors")
                hooks.mark_issue("Stopped too early")
                hooks.write_stop_too_early_dump(
                    reason="Stopped too early",
                    url=url,
                    country=country,
                    url_name=url_name,
                    page_num=page,
                    content=content,
                    now_kyiv=hooks.now_kyiv(),
                    log_lines=hooks.tail_log_lines(config.log_tail_lines),
                    context_lines=hooks.build_context_lines(
                        max_scroll_attempts=max_scroll_attempts,
                        use_pagination=use_pagination,
                    ),
                )
                if use_pagination == config.page_scrape:
                    hooks.logger.info(f"Retrying {url_name} for {country} with PAGE_SCRAPE={not use_pagination}")
                    return await scrape_all_pages(
                        base_url,
                        country,
                        config=config,
                        hooks=hooks,
                        resume_state=resume_state,
                        resume_entry_outcomes=resume_entry_outcomes,
                        run_progress=run_progress,
                        abort_event=abort_event,
                        use_pagination=not use_pagination,
                    )

            hooks.logger.info(f"Total for {country} {url_name}: {len(all_shoes)}. Stopped on page {page}")
            completed_without_fetch_failure = not stopped_too_early
            await hooks.update_resume_with_url(
                key,
                url,
                scrape_complete=True,
                final_page=last_scraped_page,
                completed=False,
            )
            break

        all_shoes.extend(shoes)
        resume_entry_outcomes[key] = "scraped"
        run_progress[key] = page
        last_scraped_page = page
        await hooks.update_resume_with_url(
            key,
            url,
            last_scraped_page=page,
            completed=False if use_pagination else True,
        )

        if not use_pagination:
            await hooks.update_resume_with_url(
                key,
                url,
                last_scraped_page=page,
                scrape_complete=True,
                final_page=page,
                completed=False,
            )
            break

        page += 1
        await hooks.sleep(1)

    if completed_without_fetch_failure:
        hooks.record_source_success(url_name, country)
    return all_shoes
