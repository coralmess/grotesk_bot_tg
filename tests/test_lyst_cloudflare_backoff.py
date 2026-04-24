import tempfile
import unittest
from pathlib import Path

from helpers.lyst.cloudflare_backoff import CloudflareBackoff


class CloudflareBackoffTests(unittest.TestCase):
    def test_record_failure_escalates_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)

            first = backoff.record_failure("Main brands", "US", now_ts=1000)
            second = backoff.record_failure("Main brands", "US", now_ts=1010)

        self.assertEqual(first.cooldown_sec, 60)
        self.assertEqual(second.cooldown_sec, 120)
        self.assertTrue(second.blocked_until_ts > first.blocked_until_ts)

    def test_allows_after_cooldown_expires(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)
            backoff.record_failure("Main brands", "US", now_ts=1000)

            self.assertFalse(backoff.should_allow("Main brands", "US", now_ts=1059))
            self.assertTrue(backoff.should_allow("Main brands", "US", now_ts=1061))

    def test_success_resets_source_country_penalty(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)
            backoff.record_failure("Main brands", "US", now_ts=1000)
            backoff.record_success("Main brands", "US")

            self.assertTrue(backoff.should_allow("Main brands", "US", now_ts=1001))


if __name__ == "__main__":
    unittest.main()
