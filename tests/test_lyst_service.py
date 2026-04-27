import unittest
from dataclasses import dataclass

from helpers.lyst.outcome import LystRunOutcome
from helpers.lyst.service import LystCycleHooks, LystCycleRunner, LystRuntimeState


class FakeRunStats:
    def __init__(self):
        self.fields = {}
        self.writes = []

    def set_field(self, key, value):
        self.fields[key] = value

    def finish(self, *, outcome):
        return {"outcome": outcome, "fields": dict(self.fields)}

    def write_jsonl(self, path, payload):
        self.writes.append((path, payload))


@dataclass
class FakeProcessingStats:
    new_total: int
    removed_total: int


class FakeStatusManager:
    def __init__(self):
        self.began = False
        self.state_fields = []
        self.finished = []

    def begin_cycle(self):
        self.began = True

    def set_state_fields(self, **fields):
        self.state_fields.append(fields)

    def finish_outcome(self, outcome, *, duration_seconds):
        self.finished.append((outcome, duration_seconds))


class FakeLogger:
    def __init__(self):
        self.info_messages = []
        self.warning_messages = []
        self.error_messages = []

    def info(self, message):
        self.info_messages.append(message)

    def warning(self, message):
        self.warning_messages.append(message)

    def error(self, message):
        self.error_messages.append(message)


class LystRuntimeStateTests(unittest.TestCase):
    def test_begin_cycle_resets_per_run_state_without_sharing_between_instances(self) -> None:
        first = LystRuntimeState()
        second = LystRuntimeState()

        first.run_failed = True
        first.run_progress["Main:US"] = 3
        first.resume_entry_outcomes["Main:US"] = "cloudflare"
        first.record_cloudflare_event(source_name="Main", country="US", page=3)
        first.begin_cycle(resume_active=True)

        self.assertFalse(first.run_failed)
        self.assertEqual(first.run_progress, {})
        self.assertEqual(first.resume_entry_outcomes, {})
        self.assertIsNone(first.last_cloudflare_event)
        self.assertTrue(first.cycle_started_in_resume)
        self.assertEqual(second.run_progress, {})
        self.assertFalse(second.cycle_started_in_resume)

    def test_terminal_resume_restart_only_applies_to_resume_terminal_only_pass(self) -> None:
        state = LystRuntimeState()
        state.begin_cycle(resume_active=True)
        state.resume_entry_outcomes.update({"Main:US": "terminal_only_resume", "Shoes:GB": "terminal_only_resume"})

        self.assertTrue(state.should_restart_after_terminal_resume([]))
        self.assertFalse(state.should_restart_after_terminal_resume([{"name": "shoe"}]))

        state.resume_entry_outcomes["Main:US"] = "terminal"
        self.assertFalse(state.should_restart_after_terminal_resume([]))

    def test_mark_failed_sets_abort_signal(self) -> None:
        state = LystRuntimeState()
        state.mark_failed()

        self.assertTrue(state.run_failed)
        self.assertTrue(state.abort_event.is_set())


class LystCycleRunnerTests(unittest.IsolatedAsyncioTestCase):
    def _hooks(self, *, run_failed=False, terminal_restart=False):
        events = []
        run_stats = FakeRunStats()
        logger = FakeLogger()
        url_batch_calls = {"count": 0}

        async def run_url_batch(exchange_rates):
            url_batch_calls["count"] += 1
            if terminal_restart and url_batch_calls["count"] == 1:
                return []
            return [{"unique_id": "shoe-1"}]

        async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
            events.append(("process", len(all_shoes)))
            return FakeProcessingStats(new_total=1, removed_total=0)

        hooks = LystCycleHooks(
            set_active_task=lambda task: events.append(("active", bool(task))),
            init_cycle=lambda: events.append(("init", None)),
            sync_cycle_state=lambda: events.append(("sync", None)),
            clear_skipped_items=lambda: events.append(("clear_skipped", None)),
            reset_http_only_state=lambda: events.append(("reset_http", None)),
            create_run_stats=lambda: run_stats,
            cloudflare_backoff_snapshot=lambda: {"active": False},
            touch_progress=lambda name: events.append(("progress", name)),
            load_old_data=lambda: _async_value({"old": "data"}),
            load_exchange_rates=lambda: _async_value({"USD": 40}),
            run_url_batch=run_url_batch,
            should_restart_after_terminal_resume=lambda shoes: terminal_restart and url_batch_calls["count"] == 1 and not shoes,
            restart_after_terminal_resume=lambda: _async_event(events, ("restart", None)),
            log_collection_stats=lambda shoes, exchange_rates: shoes,
            process_all_shoes=process_all_shoes,
            finalize_resume_state=lambda: _async_event(events, ("finalize_resume", None)),
            get_run_failed=lambda: run_failed,
            get_resume_outcomes=lambda: {"Main:US": "cloudflare"} if run_failed else {},
            get_cloudflare_event=lambda: None,
            build_outcome=lambda **kwargs: LystRunOutcome.full_success(
                items_seen=kwargs["items_seen"],
                new_items=kwargs["new_items"],
            ),
            scraper_runs_file="runs.jsonl",
            finalize_cycle=lambda **kwargs: events.append(("finalize_cycle", kwargs.get("issue"))),
            format_completion_message=lambda outcome: f"done {outcome.items_seen}",
            print_statistics=lambda: events.append(("print_stats", None)),
            print_link_statistics=lambda: events.append(("print_links", None)),
            logger=logger,
        )
        return hooks, events, run_stats, logger, url_batch_calls

    async def test_runner_processes_successful_cycle_and_writes_summary(self) -> None:
        hooks, events, run_stats, logger, url_batch_calls = self._hooks()
        status_manager = FakeStatusManager()

        await LystCycleRunner(hooks).run(object(), status_manager=status_manager)

        self.assertEqual(url_batch_calls["count"], 1)
        self.assertIn(("process", 1), events)
        self.assertIn(("finalize_cycle", None), events)
        self.assertEqual(run_stats.fields["items_seen"], 1)
        self.assertEqual(run_stats.fields["new_items"], 1)
        self.assertEqual(run_stats.fields["removed_items"], 0)
        self.assertEqual(run_stats.writes[0][0], "runs.jsonl")
        self.assertTrue(status_manager.began)
        self.assertTrue(status_manager.finished)
        self.assertEqual(logger.info_messages[-1], "done 1")
        self.assertEqual(events[-1], ("active", False))

    async def test_runner_restarts_once_when_resume_only_hits_terminal_pages(self) -> None:
        hooks, events, _run_stats, _logger, url_batch_calls = self._hooks(terminal_restart=True)

        await LystCycleRunner(hooks).run(object())

        self.assertEqual(url_batch_calls["count"], 2)
        self.assertIn(("restart", None), events)
        self.assertIn(("process", 1), events)


async def _async_value(value):
    return value


async def _async_event(events, event):
    events.append(event)


if __name__ == "__main__":
    unittest.main()
