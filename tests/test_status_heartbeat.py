import json
import tempfile
import time
import unittest
from pathlib import Path

import GroteskBotStatus as bot_status


class StatusHeartbeatTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self._orig_last_runs = bot_status.LAST_RUNS_FILE
        self._orig_status_files = dict(bot_status.STATUS_FILES)
        self._orig_health_file = bot_status.service_health_file
        bot_status.LAST_RUNS_FILE = self.root / "last_runs.json"
        bot_status.STATUS_FILES = {
            "olx": self.root / "market_olx_run.json",
            "shafa": self.root / "market_shafa_run.json",
            "lyst": self.root / "lyst_run.json",
        }
        bot_status.service_health_file = lambda service_name: self.root / f"{service_name}.json"

    def tearDown(self) -> None:
        bot_status.LAST_RUNS_FILE = self._orig_last_runs
        bot_status.STATUS_FILES = self._orig_status_files
        bot_status.service_health_file = self._orig_health_file
        self._tmpdir.cleanup()

    def test_format_status_text_prefers_health_snapshots(self) -> None:
        market_snapshot = {
            "service_name": "grotesk-market",
            "status": "ready",
            "operation_stats": {
                "olx_run": {
                    "last_success_utc": "2026-04-22T10:00:00+00:00",
                    "last_note": "",
                    "success_count": 1,
                    "failure_count": 0,
                },
                "shafa_run": {
                    "last_failure_utc": "2026-04-22T11:00:00+00:00",
                    "last_error": "rate limit",
                    "success_count": 0,
                    "failure_count": 1,
                },
            },
        }
        lyst_snapshot = {
            "service_name": "grotesk-lyst",
            "status": "degraded",
            "note": "Cloudflare challenge",
            "service_state": {
                "lyst_last_run_start_utc": "2026-04-22T09:00:00+00:00",
                "lyst_last_run_end_utc": "2026-04-22T09:15:00+00:00",
                "lyst_last_run_ok": False,
                "lyst_last_run_note": "Cloudflare challenge",
                "lyst_cycle_phase": "failed",
            },
            "operation_stats": {},
        }
        (self.root / "grotesk-market.json").write_text(json.dumps(market_snapshot), encoding="utf-8")
        (self.root / "grotesk-lyst.json").write_text(json.dumps(lyst_snapshot), encoding="utf-8")

        text = bot_status._format_status_text(time.time() - 120, lyst_stale_after_sec=3600)

        self.assertIn("rate limit", text)
        self.assertIn("Cloudflare challenge", text)
        self.assertIn("Last OLX run:", text)
        self.assertIn("Last LYST run:", text)


if __name__ == "__main__":
    unittest.main()
