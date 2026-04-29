from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any

from .outcome import LystRunOutcome


@dataclass
class LystRuntimeState:
    """Mutable state for one Lyst service process.

    The production module still exposes compatibility globals, but this object is
    the migration target: state that changes per run should live together instead
    of being scattered across module-level variables.
    """

    abort_event: asyncio.Event = field(default_factory=asyncio.Event)
    run_failed: bool = False
    run_progress: dict[str, int] = field(default_factory=dict)
    cycle_started_in_resume: bool = False
    resume_entry_outcomes: dict[str, str] = field(default_factory=dict)
    last_cloudflare_event: dict[str, Any] | None = None

    def begin_cycle(self, *, resume_active: bool) -> None:
        self.abort_event.clear()
        self.run_failed = False
        self.run_progress.clear()
        self.cycle_started_in_resume = bool(resume_active)
        self.resume_entry_outcomes.clear()
        self.last_cloudflare_event = None

    def mark_failed(self) -> None:
        self.run_failed = True
        self.abort_event.set()

    def record_cloudflare_event(self, *, source_name: str, country: str, page: int | None) -> None:
        self.last_cloudflare_event = {
            "source_name": source_name,
            "country": country,
            "page": page,
        }

    def should_restart_after_terminal_resume(self, all_shoes: list[dict]) -> bool:
        if all_shoes or not self.cycle_started_in_resume:
            return False
        if not self.resume_entry_outcomes:
            return False
        return all(outcome == "terminal_only_resume" for outcome in self.resume_entry_outcomes.values())


@dataclass(slots=True)
class LystCycleHooks:
    """Dependencies needed to run one Lyst cycle outside the legacy main module.

    GroteskBotTg.py still owns Telegram wiring, storage paths, and compatibility
    globals. Passing those pieces as hooks lets the actual cycle flow live here
    without forcing a risky all-at-once rewrite of the scraper internals.
    """

    set_active_task: Callable[[asyncio.Task | None], None]
    init_cycle: Callable[[], None]
    sync_cycle_state: Callable[[], None]
    clear_skipped_items: Callable[[], None]
    reset_http_only_state: Callable[[], None]
    create_run_stats: Callable[[], Any]
    cloudflare_backoff_snapshot: Callable[[], Any]
    touch_progress: Callable[[str], None]
    load_old_data: Callable[[], Awaitable[Any]]
    load_exchange_rates: Callable[[], Awaitable[Any]]
    run_url_batch: Callable[[Any], Awaitable[list[dict]]]
    should_restart_after_terminal_resume: Callable[[list[dict]], bool]
    restart_after_terminal_resume: Callable[[], Awaitable[None]]
    log_collection_stats: Callable[[list[dict], Any], list[dict]]
    process_all_shoes: Callable[[list[dict], Any, Any, Any], Awaitable[Any]]
    finalize_resume_state: Callable[[], Awaitable[None]]
    get_run_failed: Callable[[], bool]
    get_resume_outcomes: Callable[[], dict[str, str]]
    get_cloudflare_event: Callable[[], dict[str, Any] | None]
    build_outcome: Callable[..., LystRunOutcome]
    scraper_runs_file: Any
    finalize_cycle: Callable[..., None]
    format_completion_message: Callable[[LystRunOutcome], str]
    print_statistics: Callable[[], None]
    print_link_statistics: Callable[[], None]
    logger: Any


class LystCycleRunner:
    """Own the high-level Lyst run lifecycle.

    This is intentionally orchestration-only: page fetching, parsing, DB updates,
    image rendering, and Telegram sending stay in their existing focused helpers.
    Keeping the cycle boundary here makes failure handling easier to reason about
    and prevents GroteskBotTg.py from becoming the permanent home of every concern.
    """

    def __init__(self, hooks: LystCycleHooks):
        self.hooks = hooks

    async def run(self, message_queue, *, status_manager=None) -> None:
        hooks = self.hooks
        hooks.set_active_task(asyncio.current_task())
        hooks.init_cycle()
        hooks.sync_cycle_state()
        hooks.clear_skipped_items()
        hooks.reset_http_only_state()
        started = time.perf_counter()
        run_stats = hooks.create_run_stats()
        if status_manager is not None:
            status_manager.begin_cycle()
            status_manager.set_state_fields(
                lyst_cloudflare_backoff=hooks.cloudflare_backoff_snapshot()
            )
        hooks.touch_progress("run_start")
        try:
            old_data = await hooks.load_old_data()
            exchange_rates = await hooks.load_exchange_rates()

            all_shoes = await hooks.run_url_batch(exchange_rates)
            if hooks.should_restart_after_terminal_resume(all_shoes):
                hooks.logger.warning(
                    "LYST resume pass reached only terminal pages with 0 items; "
                    "clearing resume state and restarting once from page 1"
                )
                await hooks.restart_after_terminal_resume()
                hooks.sync_cycle_state()
                all_shoes = await hooks.run_url_batch(exchange_rates)

            all_shoes = hooks.log_collection_stats(all_shoes, exchange_rates)

            # Partial Lyst runs are still useful. If Cloudflare aborts a later page,
            # process the pages already scraped, but downstream code must avoid full
            # catalog cleanup when the run was not complete.
            processing_stats = await hooks.process_all_shoes(
                all_shoes, old_data, message_queue, exchange_rates
            )
            await hooks.finalize_resume_state()
            hooks.touch_progress("finalize_run")
            fallback_note = (
                "; ".join(sorted(set(hooks.get_resume_outcomes().values())))
                if hooks.get_run_failed()
                else ""
            )
            outcome = hooks.build_outcome(
                run_failed=hooks.get_run_failed(),
                items_seen=len(all_shoes),
                new_items=processing_stats.new_total,
                cloudflare_event=hooks.get_cloudflare_event(),
                fallback_note=fallback_note,
                resume_outcomes=hooks.get_resume_outcomes(),
            )
            # JSONL run summaries are the durable audit trail for diagnosing
            # Cloudflare, resume, and terminal-page behavior after the fact.
            run_stats.set_field("items_seen", len(all_shoes))
            run_stats.set_field("new_items", processing_stats.new_total)
            run_stats.set_field("removed_items", processing_stats.removed_total)
            run_stats.set_field(
                "resume_outcomes",
                dict(Counter(hooks.get_resume_outcomes().values())),
            )
            cloudflare_event = hooks.get_cloudflare_event()
            if cloudflare_event:
                run_stats.set_field("cloudflare_event", dict(cloudflare_event))
            run_stats.write_jsonl(
                hooks.scraper_runs_file,
                run_stats.finish(outcome=outcome.state.value),
            )
            hooks.finalize_cycle(issue=outcome.note if not outcome.ok else None)
            if status_manager is not None:
                status_manager.finish_outcome(
                    outcome, duration_seconds=time.perf_counter() - started
                )
            hooks.logger.info(hooks.format_completion_message(outcome))

            hooks.print_statistics()
            hooks.print_link_statistics()
        except asyncio.CancelledError:
            hooks.finalize_cycle(issue="stalled")
            run_stats.set_field("error", "stalled")
            run_stats.write_jsonl(
                hooks.scraper_runs_file,
                run_stats.finish(outcome="failed_stalled"),
            )
            if status_manager is not None:
                status_manager.finish_outcome(
                    LystRunOutcome.failed("stalled"),
                    duration_seconds=time.perf_counter() - started,
                )
            raise
        except Exception as exc:
            hooks.finalize_cycle(issue="failed", error=exc)
            run_stats.set_field("error", str(exc)[:200])
            run_stats.write_jsonl(
                hooks.scraper_runs_file,
                run_stats.finish(outcome="failed"),
            )
            if status_manager is not None:
                status_manager.finish_outcome(
                    LystRunOutcome.failed(str(exc)),
                    duration_seconds=time.perf_counter() - started,
                )
            raise
        finally:
            hooks.set_active_task(None)
