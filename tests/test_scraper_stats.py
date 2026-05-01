import json
import tempfile
import unittest
from pathlib import Path

from helpers.scraper_stats import RunStatsCollector


class RunStatsCollectorTests(unittest.TestCase):
    def test_finish_returns_machine_readable_summary_and_jsonl(self):
        times = iter(["2026-04-26T10:00:00Z", "2026-04-26T10:02:30Z"])
        collector = RunStatsCollector("olx", now_func=lambda: next(times))

        collector.inc("items_scraped", 2)
        collector.inc("items_scraped", 3)
        collector.set_field("sources_total", 4)
        collector.record_source("Nike", status="ok", items_scraped=5)

        summary = collector.finish(outcome="success")

        self.assertEqual(summary["scraper"], "olx")
        self.assertEqual(summary["schema_version"], 2)
        self.assertTrue(summary["run_id"])
        self.assertEqual(summary["outcome"], "success")
        self.assertEqual(summary["started_at_utc"], "2026-04-26T10:00:00Z")
        self.assertEqual(summary["finished_at_utc"], "2026-04-26T10:02:30Z")
        self.assertEqual(summary["counters"]["items_scraped"], 5)
        self.assertEqual(summary["fields"]["sources_total"], 4)
        self.assertEqual(summary["sources"][0]["name"], "Nike")
        self.assertEqual(summary["sources"][0]["status"], "ok")
        self.assertEqual(summary["sources"][0]["items_scraped"], 5)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "runs.jsonl"
            collector.write_jsonl(output_path, summary)

            lines = output_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), summary)

    def test_records_high_value_analytics_blocks(self):
        collector = RunStatsCollector("shafa", run_id="run-1", now_func=lambda: "2026-04-26T10:00:00Z")

        collector.record_source("Source", status="ok", items_scraped=100, new_items=2, sent_items=1, skipped_items=7)
        collector.record_error("HTTP 429", source="Source", message="rate limited")
        collector.set_coverage(expected=10, attempted=8, completed=7, blocked=1, skipped=2)
        collector.set_notification_funnel(
            seen=100,
            candidates=3,
            new=2,
            persisted_without_send=1,
            sent=1,
            failed=1,
            skipped=7,
        )

        summary = collector.finish(outcome="error")

        self.assertEqual(summary["run_id"], "run-1")
        self.assertEqual(summary["error_counts"], {"http_429": 1})
        self.assertEqual(summary["fields"]["coverage"]["completed_percent"], 70.0)
        self.assertEqual(summary["fields"]["notification_funnel"]["sent_per_1000_seen"], 10.0)
        self.assertEqual(summary["sources"][0]["sent_per_1000_items"], 10.0)
