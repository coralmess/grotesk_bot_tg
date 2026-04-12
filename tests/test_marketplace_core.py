import unittest

from helpers.marketplace_core import (
    SourceStats,
    duplicate_key,
    finished_source_decision,
    make_source_decision,
    notification_storage_key,
)


class MarketplaceCoreTests(unittest.TestCase):
    def test_duplicate_key_normalizes_whitespace_and_case(self) -> None:
        self.assertEqual(duplicate_key("  Nike   Air  ", 5000), ("nike air", 5000))

    def test_notification_storage_key_is_stable(self) -> None:
        self.assertEqual(notification_storage_key(("nike air", 5000)), "nike air\x1f5000")

    def test_make_source_decision_processes_first_cycle(self) -> None:
        decision = make_source_decision(SourceStats(streak=0, cycle_count=0))
        self.assertTrue(decision.should_process)
        self.assertEqual(decision.next_cycle_count, 1)
        self.assertEqual(decision.divisor, 1)

    def test_make_source_decision_skips_when_streak_backoff_applies(self) -> None:
        decision = make_source_decision(SourceStats(streak=365, cycle_count=0))
        self.assertFalse(decision.should_process)
        self.assertEqual(decision.divisor, 2)
        self.assertEqual(decision.next_cycle_count, 1)

    def test_finished_source_decision_resets_on_items(self) -> None:
        self.assertEqual(finished_source_decision(10, 3), (0, 0))

    def test_finished_source_decision_increments_streak_on_empty(self) -> None:
        self.assertEqual(finished_source_decision(10, 0), (11, 0))
