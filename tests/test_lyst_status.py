import json
import tempfile
import unittest
from pathlib import Path

from helpers.lyst.status import LystStatusManager
from helpers.service_health import ServiceHealthReporter, ServiceMetricsConfig


class LystStatusManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.health_file = Path(self._tmpdir.name) / "lyst_health.json"
        self.legacy_calls = []
        self.reporter = ServiceHealthReporter(
            ServiceMetricsConfig(
                service_name="grotesk-lyst",
                health_file=self.health_file,
                metrics_port=None,
                heartbeat_interval_sec=1,
            )
        )
        self.reporter.start()
        self.manager = LystStatusManager(
            reporter=self.reporter,
            legacy_write_status=lambda **kwargs: self.legacy_calls.append(kwargs),
        )

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _snapshot(self) -> dict:
        return json.loads(self.health_file.read_text(encoding="utf-8"))

    def test_begin_cycle_sets_running_service_state(self) -> None:
        self.manager.begin_cycle()

        snapshot = self._snapshot()
        self.assertEqual(snapshot["service_state"]["lyst_last_run_ok"], None)
        self.assertEqual(snapshot["service_state"]["lyst_cycle_phase"], "running")
        self.assertEqual(len(self.legacy_calls), 1)
        self.assertIsNone(self.legacy_calls[0]["ok"])

    def test_finish_success_with_issue_records_failure_semantics(self) -> None:
        self.manager.begin_cycle()
        self.manager.mark_issue("Cloudflare challenge")
        self.manager.finish_success(duration_seconds=1.5)

        snapshot = self._snapshot()
        self.assertEqual(snapshot["service_state"]["lyst_last_run_ok"], False)
        self.assertEqual(snapshot["service_state"]["lyst_cycle_phase"], "failed")
        self.assertEqual(snapshot["operation_stats"]["lyst_run"]["failure_count"], 1)
        self.assertIn("Cloudflare challenge", self.legacy_calls[-1]["note"])

    def test_finish_failure_records_canonical_failure(self) -> None:
        self.manager.begin_cycle()
        self.manager.finish_failure("stalled", duration_seconds=2.0)

        snapshot = self._snapshot()
        self.assertEqual(snapshot["service_state"]["lyst_last_run_ok"], False)
        self.assertEqual(snapshot["service_state"]["lyst_last_run_note"], "stalled")
        self.assertEqual(snapshot["operation_stats"]["lyst_run"]["failure_count"], 1)


if __name__ == "__main__":
    unittest.main()
