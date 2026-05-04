import json
import tempfile
import unittest
from pathlib import Path

from helpers.analytics_summary import build_analytics_daily_summary, write_analytics_summary


class AnalyticsSummaryTests(unittest.TestCase):
    def test_build_daily_summary_compacts_rollup_files_for_latest_date(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            daily_dir = root / "daily"
            daily_dir.mkdir(parents=True)
            (daily_dir / "2026-05-03.scraper_runs.json").write_text(json.dumps({"domain": "scraper_runs", "groups": {"old": {"counters": {"runs": 99}}}}), encoding="utf-8")
            (daily_dir / "2026-05-04.scraper_runs.json").write_text(json.dumps({"domain": "scraper_runs", "groups": {"scraper=olx|outcome=success": {"dimensions": {"scraper": "olx"}, "counters": {"runs": 2, "items_sent": 1}}}}), encoding="utf-8")
            (daily_dir / "2026-05-04.service_operations.json").write_text(json.dumps({"domain": "service_operations", "groups": {"service=usefulbot|operation=exchange|outcome=success": {"dimensions": {"service": "usefulbot"}, "counters": {"runs": 3}}}}), encoding="utf-8")

            summary = build_analytics_daily_summary(root)

            self.assertEqual(summary["date"], "2026-05-04")
            self.assertEqual(summary["domains"]["scraper_runs"]["groups"]["scraper=olx|outcome=success"]["counters"]["runs"], 2)
            self.assertEqual(summary["domains"]["service_operations"]["groups"]["service=usefulbot|operation=exchange|outcome=success"]["counters"]["runs"], 3)
            self.assertNotIn("old", json.dumps(summary))

    def test_write_analytics_summary_writes_stable_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "daily").mkdir(parents=True)
            (root / "daily" / "2026-05-04.image_pipeline.json").write_text(json.dumps({"domain": "image_pipeline", "groups": {}}), encoding="utf-8")

            path = write_analytics_summary(root)

            self.assertEqual(path, root / "summary" / "latest.json")
            self.assertTrue(path.exists())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["date"], "2026-05-04")


if __name__ == "__main__":
    unittest.main()
