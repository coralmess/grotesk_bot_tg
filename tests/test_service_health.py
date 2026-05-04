import json
import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from helpers.service_health import ServiceHealthReporter, ServiceMetricsConfig


class ServiceHealthReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.health_file = Path(self._tmpdir.name) / "service.json"
        self.reporter = ServiceHealthReporter(
            ServiceMetricsConfig(
                service_name="test-service",
                health_file=self.health_file,
                metrics_port=None,
                heartbeat_interval_sec=1,
            )
        )

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _read_snapshot(self) -> dict:
        return json.loads(self.health_file.read_text(encoding="utf-8"))

    def test_start_and_ready_write_health_snapshot(self) -> None:
        self.reporter.start()
        self.reporter.mark_ready("booted")

        snapshot = self._read_snapshot()
        self.assertEqual(snapshot["service_name"], "test-service")
        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["note"], "booted")
        self.assertEqual(snapshot["metrics_port"], None)
        self.assertIn("started_at_utc", snapshot)
        self.assertIn("last_heartbeat_utc", snapshot)

    def test_success_and_failure_update_operation_stats(self) -> None:
        self.reporter.start()
        self.reporter.record_success("job", duration_seconds=1.25, note="first-pass")
        self.reporter.record_failure("job", "boom", duration_seconds=2.5)

        snapshot = self._read_snapshot()
        stats = snapshot["operation_stats"]["job"]
        self.assertEqual(stats["success_count"], 1)
        self.assertEqual(stats["failure_count"], 1)
        self.assertEqual(stats["last_note"], "first-pass")
        self.assertEqual(stats["last_error"], "boom")
        self.assertEqual(snapshot["status"], "degraded")
        self.assertEqual(snapshot["last_error"], "boom")

    def test_success_and_failure_record_operation_analytics(self) -> None:
        analytics_dir = Path(self._tmpdir.name) / "analytics"
        reporter = ServiceHealthReporter(
            ServiceMetricsConfig(
                service_name="test-service",
                health_file=self.health_file,
                metrics_port=None,
                heartbeat_interval_sec=1,
                analytics_sink=AnalyticsSink(analytics_dir, now_func=lambda: "2026-05-04T11:00:00Z"),
            )
        )
        reporter.start()

        reporter.record_success("exchange_rate_check", duration_seconds=1.25, note="scheduled:no_change")
        reporter.record_failure("exchange_rate_check", "fetch_failed", duration_seconds=2.5)

        event_path = analytics_dir / "events" / "2026-05-04.service_operation.jsonl"
        events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([event["outcome"] for event in events], ["success", "failure"])
        self.assertEqual(events[0]["operation"], "exchange_rate_check")
        self.assertEqual(events[0]["note"], "scheduled:no_change")
        self.assertEqual(events[1]["error"], "fetch_failed")

        daily_path = analytics_dir / "daily" / "2026-05-04.service_operations.json"
        daily = json.loads(daily_path.read_text(encoding="utf-8"))
        self.assertEqual(daily["groups"]["operation=exchange_rate_check|outcome=success|service=test-service"]["counters"]["runs"], 1)
        self.assertEqual(daily["groups"]["operation=exchange_rate_check|outcome=failure|service=test-service"]["counters"]["failures"], 1)


if __name__ == "__main__":
    unittest.main()
