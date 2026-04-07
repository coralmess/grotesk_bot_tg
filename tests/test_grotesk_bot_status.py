import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import GroteskBotStatus as bot_status


class GroteskBotStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_last_runs = bot_status.LAST_RUNS_FILE
        self._orig_status_files = dict(bot_status.STATUS_FILES)
        self._orig_last_olx = bot_status.LAST_OLX_RUN_UTC
        self._orig_last_shafa = bot_status.LAST_SHAFA_RUN_UTC
        self._orig_last_olx_note = bot_status.LAST_OLX_RUN_NOTE
        self._orig_last_shafa_note = bot_status.LAST_SHAFA_RUN_NOTE
        self._orig_last_lyst_start = bot_status.LAST_LYST_RUN_START_UTC
        self._orig_last_lyst_end = bot_status.LAST_LYST_RUN_END_UTC
        self._orig_last_lyst_ok = bot_status.LAST_LYST_RUN_OK
        self._orig_last_lyst_note = bot_status.LAST_LYST_RUN_NOTE
        self._orig_lyst_run_had_errors = bot_status.LYST_RUN_HAD_ERRORS
        self._orig_lyst_run_notes = list(bot_status.LYST_RUN_NOTES)
        self._orig_lyst_started_cycle = bot_status._LYST_RUN_STARTED_THIS_CYCLE

    def tearDown(self) -> None:
        bot_status.LAST_RUNS_FILE = self._orig_last_runs
        bot_status.STATUS_FILES = self._orig_status_files
        bot_status.LAST_OLX_RUN_UTC = self._orig_last_olx
        bot_status.LAST_SHAFA_RUN_UTC = self._orig_last_shafa
        bot_status.LAST_OLX_RUN_NOTE = self._orig_last_olx_note
        bot_status.LAST_SHAFA_RUN_NOTE = self._orig_last_shafa_note
        bot_status.LAST_LYST_RUN_START_UTC = self._orig_last_lyst_start
        bot_status.LAST_LYST_RUN_END_UTC = self._orig_last_lyst_end
        bot_status.LAST_LYST_RUN_OK = self._orig_last_lyst_ok
        bot_status.LAST_LYST_RUN_NOTE = self._orig_last_lyst_note
        bot_status.LYST_RUN_HAD_ERRORS = self._orig_lyst_run_had_errors
        bot_status.LYST_RUN_NOTES = self._orig_lyst_run_notes
        bot_status._LYST_RUN_STARTED_THIS_CYCLE = self._orig_lyst_started_cycle

    def _configure_temp_status_paths(self, tmp_dir: str) -> None:
        root = Path(tmp_dir)
        bot_status.LAST_RUNS_FILE = root / "last_runs.json"
        bot_status.STATUS_FILES = {
            "olx": root / "market_olx_run.json",
            "shafa": root / "market_shafa_run.json",
            "lyst": root / "lyst_run.json",
        }

    def test_write_and_read_service_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._configure_temp_status_paths(tmp_dir)
            now = datetime(2026, 4, 7, 12, 0, 0, tzinfo=timezone.utc)
            bot_status.write_olx_status(end_utc=now, ok=True, note="")
            bot_status.write_shafa_status(end_utc=now, ok=False, note="timeout")
            bot_status.write_lyst_status(start_utc=now, end_utc=now, ok=True, note="ok")

            statuses = bot_status.read_all_service_statuses()
            self.assertEqual(statuses["olx"]["last_run_end_utc"], now)
            self.assertTrue(statuses["olx"]["last_run_ok"])
            self.assertEqual(statuses["shafa"]["last_run_note"], "timeout")
            self.assertFalse(statuses["shafa"]["last_run_ok"])
            self.assertEqual(statuses["lyst"]["last_run_start_utc"], now)
            self.assertEqual(statuses["lyst"]["last_run_end_utc"], now)

    def test_load_migrates_legacy_last_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._configure_temp_status_paths(tmp_dir)
            payload = {
                "last_olx_run_utc": "2026-04-07T10:00:00+00:00",
                "last_shafa_run_utc": "2026-04-07T11:00:00+00:00",
                "last_olx_run_note": "",
                "last_shafa_run_note": "rate limit",
                "last_lyst_run_start_utc": "2026-04-07T09:00:00+00:00",
                "last_lyst_run_end_utc": "2026-04-07T09:15:00+00:00",
                "last_lyst_run_ok": False,
                "last_lyst_run_note": "Cloudflare challenge",
            }
            bot_status.LAST_RUNS_FILE.write_text(json.dumps(payload), encoding="utf-8")

            bot_status.load_last_runs_from_file()
            statuses = bot_status.read_all_service_statuses()
            self.assertEqual(
                statuses["olx"]["last_run_end_utc"],
                datetime(2026, 4, 7, 10, 0, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(statuses["shafa"]["last_run_note"], "rate limit")
            self.assertFalse(statuses["lyst"]["last_run_ok"])
            self.assertEqual(statuses["lyst"]["last_run_note"], "Cloudflare challenge")

    def test_format_status_text_uses_service_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._configure_temp_status_paths(tmp_dir)
            now = datetime.now(timezone.utc)
            bot_status.write_olx_status(end_utc=now, ok=True, note="")
            bot_status.write_shafa_status(end_utc=now, ok=True, note="quiet")
            bot_status.write_lyst_status(start_utc=now, end_utc=now, ok=True, note="")

            text = bot_status._format_status_text(time.time() - 120, lyst_stale_after_sec=3600)
            self.assertIn("Last OLX run:", text)
            self.assertIn("Last SHAFA run:", text)
            self.assertIn("Last LYST run:", text)
            self.assertIn("(quiet)", text)


if __name__ == "__main__":
    unittest.main()
