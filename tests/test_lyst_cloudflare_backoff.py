import tempfile
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
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

    def test_records_cooldown_set_and_skip_analytics(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = AnalyticsSink(Path(tmp) / "analytics", now_func=lambda: "2026-05-04T14:00:00Z")
            backoff = CloudflareBackoff(
                Path(tmp) / "cf.json",
                base_cooldown_sec=60,
                max_cooldown_sec=600,
                analytics_sink=sink,
            )

            decision = backoff.record_failure("Main brands", "US", now_ts=1000)
            allowed = backoff.should_allow("Main brands", "US", now_ts=1001)

            self.assertFalse(allowed)
            self.assertEqual(decision.cooldown_sec, 60)
            events = [
                json.loads(line)
                for line in (Path(tmp) / "analytics" / "events" / "2026-05-04.lyst_cloudflare_cooldown.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual([event["event"] for event in events], ["cooldown_set", "cooldown_skip"])
            self.assertEqual(events[0]["source_name"], "Main brands")
            self.assertEqual(events[0]["country"], "US")
            self.assertEqual(events[0]["failure_count"], 1)
            self.assertEqual(events[0]["cooldown_seconds"], 60)
            self.assertEqual(events[1]["cooldown_seconds"], 60)

    def test_config_uses_balanced_cloudflare_cooldown_defaults(self):
        env = os.environ.copy()
        env.pop("LYST_CLOUDFLARE_BASE_COOLDOWN_SEC", None)
        env.pop("LYST_CLOUDFLARE_MAX_COOLDOWN_SEC", None)
        env["IS_INSTANCE"] = "false"

        raw = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "import json, config; print(json.dumps([config.LYST_CLOUDFLARE_BASE_COOLDOWN_SEC, config.LYST_CLOUDFLARE_MAX_COOLDOWN_SEC]))",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
        )

        self.assertEqual(json.loads(raw), [3600, 21600])

    def test_config_preserves_cloudflare_cooldown_env_overrides(self):
        env = os.environ.copy()
        env["LYST_CLOUDFLARE_BASE_COOLDOWN_SEC"] = "123"
        env["LYST_CLOUDFLARE_MAX_COOLDOWN_SEC"] = "456"
        env["IS_INSTANCE"] = "true"

        raw = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "import json, config; print(json.dumps([config.LYST_CLOUDFLARE_BASE_COOLDOWN_SEC, config.LYST_CLOUDFLARE_MAX_COOLDOWN_SEC]))",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
        )

        self.assertEqual(json.loads(raw), [123, 456])


if __name__ == "__main__":
    unittest.main()
