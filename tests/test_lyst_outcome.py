import unittest

from helpers.lyst.outcome import LystRunOutcome, LystRunState, build_lyst_run_outcome


class LystOutcomeTests(unittest.TestCase):
    def test_full_success_has_ok_status(self):
        outcome = LystRunOutcome.full_success(items_seen=42, new_items=3)

        self.assertEqual(outcome.state, LystRunState.SUCCESS_FULL)
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.note, "")
        self.assertEqual(outcome.phase, "succeeded")

    def test_cloudflare_partial_success_is_not_ok(self):
        outcome = LystRunOutcome.cloudflare_partial(
            source_name="Main brands",
            country="US",
            page=3,
            items_seen=120,
            new_items=0,
        )

        self.assertEqual(outcome.state, LystRunState.FAILED_CLOUDFLARE)
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed_cloudflare")
        self.assertIn("Cloudflare challenge", outcome.note)
        self.assertIn("Main brands", outcome.note)
        self.assertEqual(outcome.service_state_fields()["lyst_cycle_phase"], "failed_cloudflare")

    def test_cloudflare_partial_success_keeps_run_ok_but_records_blocked_location(self):
        outcome = LystRunOutcome.cloudflare_partial_success(
            source_name="Main brands",
            country="US",
            page=3,
            items_seen=120,
            new_items=2,
        )

        self.assertEqual(outcome.state, LystRunState.SUCCESS_PARTIAL)
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertIn("Cloudflare challenge", outcome.note)
        self.assertEqual(outcome.source_name, "Main brands")
        self.assertEqual(outcome.service_state_fields()["lyst_failure_page"], 3)

    def test_stalled_outcome_is_not_ok(self):
        outcome = LystRunOutcome.failed("stalled")

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed_stalled")
        self.assertEqual(outcome.note, "stalled")

    def test_build_outcome_marks_useful_cloudflare_run_as_partial_success_even_when_failed(self):
        outcome = build_lyst_run_outcome(
            run_failed=True,
            items_seen=112,
            new_items=0,
            cloudflare_event={"source_name": "Main brands [3]", "country": "GB", "page": 3},
            fallback_note="failed",
            resume_outcomes={"Main brands [3]::GB": "cloudflare", "Grotesk Shoes::US": "scraped"},
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.state, LystRunState.SUCCESS_PARTIAL)
        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertEqual(outcome.service_state_fields()["lyst_blocked_reason"], "cloudflare")
        self.assertIn("Main brands [3]", outcome.note)

    def test_build_outcome_marks_useful_fetch_failure_as_partial_success(self):
        outcome = build_lyst_run_outcome(
            run_failed=True,
            items_seen=24,
            new_items=1,
            cloudflare_event=None,
            fallback_note="failed",
            resume_outcomes={"Main brands::US": "failed", "Grotesk Shoes::PL": "scraped"},
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.state, LystRunState.SUCCESS_PARTIAL)
        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertEqual(outcome.note, "Partial coverage: failed")
        self.assertEqual(outcome.service_state_fields()["lyst_blocked_reason"], "failed")

    def test_build_outcome_keeps_zero_useful_cloudflare_as_failed(self):
        outcome = build_lyst_run_outcome(
            run_failed=True,
            items_seen=0,
            new_items=0,
            cloudflare_event={"source_name": "Main brands", "country": "US", "page": 4},
            fallback_note="failed",
            resume_outcomes={"Main brands::US": "cloudflare"},
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.state, LystRunState.FAILED_CLOUDFLARE)
        self.assertEqual(outcome.phase, "failed_cloudflare")


if __name__ == "__main__":
    unittest.main()
