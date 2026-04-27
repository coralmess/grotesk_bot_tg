from __future__ import annotations

import asyncio


def build_scrape_target_url(base_url, page, use_pagination):
    return base_url["url"] if not use_pagination or page == 1 else f"{base_url['url']}&page={page}"


def log_scrape_target(*, logger, url_name, country, page, use_pagination):
    if use_pagination:
        logger.info(f"Scraping page {page} for country {country} - {url_name}")
    else:
        logger.info(f"Scraping single page for country {country} - {url_name}")


async def update_resume_with_url(*, update_entry, key, url, **fields):
    await update_entry(key, last_url=url, **fields)


async def process_url(
    base_url,
    countries,
    *,
    scrape_all_pages,
    merge_base_url_into_shoes,
    touch_progress,
    mark_start,
    special_logger,
):
    # URL orchestration moves here so GroteskBotTg.py can coordinate the service
    # instead of manually fanning out country batches itself.
    touch_progress()
    mark_start()
    all_shoes = []
    country_results = await asyncio.gather(*(scrape_all_pages(base_url, c) for c in countries))
    for country, result in zip(countries, country_results):
        all_shoes.extend(merge_base_url_into_shoes(result, base_url, country))
        special_logger.info(f"Found {len(result)} items for {country} - {base_url['url_name']}")
    return all_shoes


def collect_successful_results(url_results, *, logger):
    all_shoes = []
    for result in url_results:
        if isinstance(result, Exception):
            logger.error(f"Lyst task failed: {result}")
            continue
        all_shoes.extend(result)
    return all_shoes


def should_restart_after_terminal_resume(*, all_shoes, cycle_started_in_resume, entry_outcomes):
    if all_shoes or not cycle_started_in_resume:
        return False
    if not entry_outcomes:
        return False
    return all(outcome == "terminal_only_resume" for outcome in entry_outcomes.values())


async def clear_resume_state(*, resume_controller):
    await resume_controller.clear()


async def run_resume_step(*, progress_name, operation, touch_progress, mark_issue, logger, timeout_issue, timeout_log):
    try:
        touch_progress(f"{progress_name}_start")
        await asyncio.wait_for(operation(), timeout=60)
        touch_progress(f"{progress_name}_done")
    except asyncio.TimeoutError:
        mark_issue(timeout_issue)
        logger.error(timeout_log)


async def finalize_resume_state(
    *,
    finalize_resume_after_processing,
    clear_resume_state,
    run_failed,
    preserve_resume=False,
    touch_progress,
    mark_issue,
    logger,
):
    await run_resume_step(
        progress_name="finalize_resume",
        operation=lambda: finalize_resume_after_processing(preserve_resume=preserve_resume),
        touch_progress=touch_progress,
        mark_issue=mark_issue,
        logger=logger,
        timeout_issue="resume finalize timeout",
        timeout_log="LYST finalize resume timed out; continuing without resume update",
    )
    if run_failed or preserve_resume:
        return
    await run_resume_step(
        progress_name="finalize_clear",
        operation=clear_resume_state,
        touch_progress=touch_progress,
        mark_issue=mark_issue,
        logger=logger,
        timeout_issue="resume clear timeout",
        timeout_log="LYST resume clear timed out; continuing",
    )


def log_collection_stats(
    all_shoes,
    exchange_rates,
    *,
    run_failed,
    mark_issue,
    special_logger,
    skipped_items,
    filter_duplicates,
):
    if not all_shoes and not run_failed:
        mark_issue("0 items scraped")

    collected_ids = {shoe["unique_id"] for shoe in all_shoes}
    recovered_count = sum(1 for uid in skipped_items if uid in collected_ids)
    special_logger.stat(f"Items skipped due to image but present in final list: {recovered_count}/{len(skipped_items)}")

    unfiltered_len = len(all_shoes)
    all_shoes = filter_duplicates(all_shoes, exchange_rates)
    special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")
    return all_shoes
