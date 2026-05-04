import unittest
from dataclasses import dataclass
from types import SimpleNamespace

from helpers.lyst import page_runner


class FakeLogger:
    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []

    def info(self, message, *args):
        self.infos.append(message % args if args else message)

    def warning(self, message, *args):
        self.warnings.append(message % args if args else message)

    def error(self, message, *args):
        self.errors.append(message % args if args else message)


class FakeAbortEvent:
    def __init__(self, abort=False):
        self.abort = abort

    def is_set(self):
        return self.abort


@dataclass
class Harness:
    state: dict
    outcomes: dict
    progress: dict
    logger: FakeLogger
    scrape_calls: list
    resume_updates: list
    touched: list
    issues: list
    failed_runs: list
    dumps: list
    sleeps: list
    cloudflare_failures: list
    successes: list
    page_events: list


def make_harness(*, resume_state=None, page_results=None, cooldown=False):
    results = list(page_results or [])
    harness = Harness(
        state=resume_state or {"resume_active": False, "entries": {}},
        outcomes={},
        progress={},
        logger=FakeLogger(),
        scrape_calls=[],
        resume_updates=[],
        touched=[],
        issues=[],
        failed_runs=[],
        dumps=[],
        sleeps=[],
        cloudflare_failures=[],
        successes=[],
        page_events=[],
    )

    async def scrape_page(url, country, **kwargs):
        harness.scrape_calls.append((url, country, kwargs))
        if not results:
            return [], "", "terminal"
        return results.pop(0)

    async def update_resume(key, url, **fields):
        harness.resume_updates.append((key, url, fields))

    async def mark_run_failed(reason):
        harness.failed_runs.append(reason)

    async def sleep(seconds):
        harness.sleeps.append(seconds)

    def record_cloudflare_failure(source_name, country, page):
        harness.cloudflare_failures.append((source_name, country, page))
        return SimpleNamespace(failure_count=2, cooldown_sec=900)

    hooks = page_runner.PageRunHooks(
        logger=harness.logger,
        scrape_page=scrape_page,
        resume_key=lambda base_url, country: f"{base_url['url_name']}:{country}",
        should_skip_source_for_backoff=lambda source_name, country: cooldown,
        touch_progress=lambda step=None, **details: harness.touched.append((step, details)),
        scrape_target_url=lambda base_url, page, use_pagination: base_url["url"] if not use_pagination or page == 1 else f"{base_url['url']}&page={page}",
        log_scrape_target=lambda url_name, country, page, use_pagination: harness.logger.info(f"target {url_name} {country} {page} {use_pagination}"),
        update_resume_with_url=update_resume,
        record_cloudflare_failure=record_cloudflare_failure,
        mark_issue=lambda reason: harness.issues.append(reason),
        mark_run_failed=mark_run_failed,
        log_run_progress_summary=lambda: harness.touched.append(("summary", {})),
        build_context_lines=lambda **kwargs: [f"context:{kwargs['use_pagination']}"],
        tail_log_lines=lambda line_count: ["log-line"],
        write_stop_too_early_dump=lambda **kwargs: harness.dumps.append(kwargs),
        now_kyiv=lambda: "2026-04-26 13:00:00",
        record_source_success=lambda source_name, country: harness.successes.append((source_name, country)),
        sleep=sleep,
        record_page_event=lambda **fields: harness.page_events.append(fields),
    )
    config = page_runner.PageRunConfig(page_scrape=True, max_scroll_attempts=7, log_tail_lines=200)
    return harness, config, hooks


class LystPageRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_resume_entry_skips_without_scraping(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        state = {"resume_active": True, "entries": {"Main:US": {"completed": True}}}
        harness, config, hooks = make_harness(resume_state=state)

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [])
        self.assertEqual(harness.scrape_calls, [])
        self.assertIn("Skipping Main for US", harness.logger.infos[0])

    async def test_resume_uses_next_page_and_updates_progress(self):
        base_url = {"url": "https://www.lyst.com/shop?x=1", "url_name": "Main"}
        state = {"resume_active": True, "entries": {"Main:US": {"next_page": 3, "last_scraped_page": 2}}}
        shoe = {"name": "shoe"}
        harness, config, hooks = make_harness(
            resume_state=state,
            page_results=[([shoe], "page3", "ok"), ([], "gone", "terminal")],
        )

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [shoe])
        self.assertEqual(harness.scrape_calls[0][0], "https://www.lyst.com/shop?x=1&page=3")
        self.assertEqual(harness.progress["Main:US"], 3)
        self.assertEqual(harness.resume_updates[0][2]["last_scraped_page"], 3)

    async def test_cloudflare_cooldown_skips_source(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        harness, config, hooks = make_harness(cooldown=True)

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [])
        self.assertEqual(harness.outcomes["Main:US"], "cloudflare_cooldown")
        self.assertEqual(harness.scrape_calls, [])

    async def test_cloudflare_status_records_local_cooldown_without_global_abort(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        harness, config, hooks = make_harness(page_results=[([], "challenge", "cloudflare")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [])
        self.assertEqual(harness.outcomes["Main:US"], "cloudflare")
        self.assertEqual(harness.cloudflare_failures, [("Main", "US", 1)])
        self.assertEqual(harness.issues, ["Cloudflare challenge"])
        self.assertEqual(harness.failed_runs, [])
        self.assertEqual(harness.resume_updates[0][2]["next_page"], 1)
        self.assertEqual(harness.resume_updates[0][2]["failure_reason"], "Cloudflare challenge")

    async def test_aborted_status_records_outcome_without_resume_write(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        harness, config, hooks = make_harness(page_results=[([], None, "aborted")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [])
        self.assertEqual(harness.outcomes["Main:US"], "aborted")
        self.assertEqual(harness.resume_updates, [])

    async def test_terminal_from_resume_without_items_marks_terminal_only_resume(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        state = {"resume_active": True, "entries": {"Main:US": {"next_page": 3, "last_scraped_page": 2}}}
        harness, config, hooks = make_harness(resume_state=state, page_results=[([], "gone", "terminal")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [])
        self.assertEqual(harness.outcomes["Main:US"], "terminal_only_resume")
        self.assertEqual(harness.resume_updates[0][2]["final_page"], 2)

    async def test_empty_first_page_writes_dump_and_retries_without_pagination(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        shoe = {"name": "shoe"}
        harness, config, hooks = make_harness(page_results=[([], "empty", "ok"), ([shoe], "single", "ok")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [shoe])
        self.assertEqual(len(harness.dumps), 1)
        self.assertEqual(harness.dumps[0]["reason"], "Stopped too early")
        self.assertFalse(harness.scrape_calls[1][2]["use_pagination"])
        self.assertEqual(harness.resume_updates[-1][2]["scrape_complete"], True)


    async def test_records_page_analytics_events_for_page_statuses(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        shoe = {"name": "shoe"}
        harness, config, hooks = make_harness(page_results=[([shoe], "page1", "ok"), ([], "gone", "terminal")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [shoe])
        self.assertEqual([event["status"] for event in harness.page_events], ["ok", "terminal"])
        self.assertEqual(harness.page_events[0]["items_scraped"], 1)
        self.assertEqual(harness.page_events[0]["page"], 1)
        self.assertEqual(harness.page_events[0]["country"], "US")
        self.assertEqual(harness.page_events[1]["terminal_final_page"], 1)

    async def test_clean_terminal_after_success_records_source_success(self):
        base_url = {"url": "https://www.lyst.com/shop", "url_name": "Main"}
        shoe = {"name": "shoe"}
        harness, config, hooks = make_harness(page_results=[([shoe], "page1", "ok"), ([], "gone", "terminal")])

        result = await page_runner.scrape_all_pages(
            base_url,
            "US",
            config=config,
            hooks=hooks,
            resume_state=harness.state,
            resume_entry_outcomes=harness.outcomes,
            run_progress=harness.progress,
            abort_event=FakeAbortEvent(),
        )

        self.assertEqual(result, [shoe])
        self.assertEqual(harness.outcomes["Main:US"], "terminal")
        self.assertEqual(harness.successes, [("Main", "US")])
        self.assertEqual(harness.sleeps, [1])


if __name__ == "__main__":
    unittest.main()
