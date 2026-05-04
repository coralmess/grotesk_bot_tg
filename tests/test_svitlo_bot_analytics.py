import json
import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from svitlo_bot import SvitloBot


class SvitloBotAnalyticsTests(unittest.TestCase):
    def test_record_power_analytics_writes_event_and_daily_counter(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bot = SvitloBot()
            bot._analytics_sink = AnalyticsSink(Path(tmp_dir), now_func=lambda: "2026-05-04T15:00:00Z")

            bot._record_power_analytics(
                "transition",
                state="OFF",
                previous_state="ON",
                latency_seconds=1.25,
                suppressed_short_outage=False,
            )

            event_path = Path(tmp_dir) / "events" / "2026-05-04.svitlo_power.jsonl"
            event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["event"], "transition")
            self.assertEqual(event["state"], "OFF")
            self.assertEqual(event["previous_state"], "ON")
            self.assertEqual(event["latency_seconds"], 1.25)

            daily = json.loads((Path(tmp_dir) / "daily" / "2026-05-04.svitlo_power.json").read_text(encoding="utf-8"))
            self.assertEqual(daily["groups"]["event=transition|state=OFF"]["counters"]["checks"], 1)


if __name__ == "__main__":
    unittest.main()
