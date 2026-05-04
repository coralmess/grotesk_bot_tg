import json
import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import (
    AnalyticsSink,
    fingerprint_url,
    sanitize_payload,
)


class AnalyticsEventsTests(unittest.TestCase):
    def test_append_event_writes_daily_stream_with_safe_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = AnalyticsSink(Path(tmp), now_func=lambda: "2026-05-04T10:11:12Z")

            path = sink.append_event(
                "telegram.send",
                {
                    "bot_token": "123:secret",
                    "chat_id": 123456,
                    "status": "sent",
                },
            )

            self.assertEqual(path.name, "2026-05-04.telegram_send.jsonl")
            line = path.read_text(encoding="utf-8").strip()
            event = json.loads(line)
            self.assertEqual(event["schema_version"], 1)
            self.assertEqual(event["ts_utc"], "2026-05-04T10:11:12Z")
            self.assertEqual(event["stream"], "telegram_send")
            self.assertEqual(event["status"], "sent")
            self.assertEqual(event["bot_token"], "[redacted]")
            self.assertTrue(event["chat_id_hash"].startswith("sha256:"))
            self.assertNotIn("chat_id", event)

    def test_add_daily_counters_merges_dimensions_and_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = AnalyticsSink(Path(tmp), now_func=lambda: "2026-05-04T10:11:12Z")

            sink.add_daily_counters(
                "marketplace.sources",
                dimensions={"source_kind": "olx", "source_name": "Riri"},
                counters={"runs": 1, "items": 10},
            )
            path = sink.add_daily_counters(
                "marketplace.sources",
                dimensions={"source_kind": "olx", "source_name": "Riri"},
                counters={"runs": 1, "sent": 2},
            )

            payload = json.loads(path.read_text(encoding="utf-8"))
            bucket = payload["groups"]["source_kind=olx|source_name=Riri"]
            self.assertEqual(bucket["dimensions"], {"source_kind": "olx", "source_name": "Riri"})
            self.assertEqual(bucket["counters"], {"items": 10, "runs": 2, "sent": 2})

    def test_fingerprint_url_removes_query_and_keeps_host(self):
        data = fingerprint_url("https://example.com/path/item?token=secret")

        self.assertEqual(data["url_host"], "example.com")
        self.assertEqual(data["url_scheme"], "https")
        self.assertTrue(data["url_path_hash"].startswith("sha256:"))
        self.assertNotIn("token", json.dumps(data))

    def test_sanitize_payload_redacts_nested_secrets(self):
        payload = sanitize_payload(
            {
                "ok": True,
                "headers": {"Authorization": "Bearer secret"},
                "items": [{"chat_id": "42"}],
            }
        )

        self.assertEqual(payload["headers"]["Authorization"], "[redacted]")
        self.assertTrue(payload["items"][0]["chat_id_hash"].startswith("sha256:"))
        self.assertNotIn("chat_id", payload["items"][0])


if __name__ == "__main__":
    unittest.main()
