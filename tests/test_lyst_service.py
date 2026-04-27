import unittest

from helpers.lyst.service import LystRuntimeState


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


if __name__ == "__main__":
    unittest.main()
