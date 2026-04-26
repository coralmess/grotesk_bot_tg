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
        self.assertEqual(summary["outcome"], "success")
        self.assertEqual(summary["started_at_utc"], "2026-04-26T10:00:00Z")
        self.assertEqual(summary["finished_at_utc"], "2026-04-26T10:02:30Z")
        self.assertEqual(summary["counters"]["items_scraped"], 5)
        self.assertEqual(summary["fields"]["sources_total"], 4)
        self.assertEqual(summary["sources"], [{"name": "Nike", "status": "ok", "items_scraped": 5}])

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "runs.jsonl"
            collector.write_jsonl(output_path, summary)

            lines = output_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), summary)
