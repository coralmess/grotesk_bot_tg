import unittest

from helpers.lyst.outcome import LystRunOutcome, LystRunState


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

    def test_stalled_outcome_is_not_ok(self):
        outcome = LystRunOutcome.failed("stalled")

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed_stalled")
        self.assertEqual(outcome.note, "stalled")


if __name__ == "__main__":
    unittest.main()
