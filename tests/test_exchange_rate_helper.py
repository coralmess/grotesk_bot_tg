import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from helpers.analytics_events import AnalyticsSink
from useful_bot.exchange_rate_helper import ExchangeRateHelper, RateSnapshot


KYIV_TZ = ZoneInfo("Europe/Kyiv")


class _FakeBot:
    def __init__(self) -> None:
        self.sent_photos = []

    async def send_photo(self, *, chat_id, photo):
        self.sent_photos.append((chat_id, photo))
        return SimpleNamespace(message_id=321)


class _FakeApplication:
    def __init__(self) -> None:
        self.bot = _FakeBot()
        self.bot_data = {}


class ExchangeRateHelperTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_check_persists_presenter_history_and_hour_bucket(self) -> None:
        history_plus_today = [
            {
                "fetched_at": "2026-04-14T10:15:00+03:00",
                "source_date": "14.04.2026",
                "usd_buy": 41.1,
                "usd_sell": 41.8,
                "eur_buy": 44.5,
                "eur_sell": 45.3,
                "usd_spread": 0.7,
                "eur_sell_minus_usd_buy": 4.2,
            }
        ]
        snapshot = RateSnapshot(
            fetched_at="2026-04-14T10:15:00+03:00",
            source_date="14.04.2026",
            usd_buy=41.1,
            usd_sell=41.8,
            eur_buy=44.5,
            eur_sell=45.3,
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "useful_bot.exchange_rate_helper.STATE_FILE",
            Path(temp_dir) / "exchange_state.json",
        ):
            helper = ExchangeRateHelper(chat_id=1)
            helper._now_kyiv = lambda: datetime(2026, 4, 14, 10, 15, tzinfo=KYIV_TZ)
            helper._fetch_snapshot = AsyncMock(return_value=snapshot)
            app = _FakeApplication()

            with patch(
                "useful_bot.exchange_rate_helper.build_exchange_rate_render_kwargs",
                return_value=({"usd_buy": 41.1}, history_plus_today),
            ), patch(
                "useful_bot.exchange_rate_helper.run_cpu_bound",
                new=AsyncMock(return_value=io.BytesIO(b"image")),
            ):
                sent = await helper._run_check(app, reason="startup")

        self.assertTrue(sent)
        self.assertEqual(helper._state["history"], history_plus_today)
        self.assertEqual(helper._state["last_auto_sent_bucket"], "2026-04-14T10")
        self.assertEqual(helper._state["recent_sent_messages"][0]["message_id"], 321)

    async def test_run_check_skips_duplicate_auto_send_in_same_hour(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "useful_bot.exchange_rate_helper.STATE_FILE",
            Path(temp_dir) / "exchange_state.json",
        ):
            helper = ExchangeRateHelper(chat_id=1)
            helper._now_kyiv = lambda: datetime(2026, 4, 14, 10, 25, tzinfo=KYIV_TZ)
            helper._state["last_auto_sent_bucket"] = "2026-04-14T10"
            helper._fetch_snapshot = AsyncMock()
            app = _FakeApplication()

            sent = await helper._run_check(app, reason="startup")

        self.assertFalse(sent)
        helper._fetch_snapshot.assert_not_called()

    async def test_manual_check_bypasses_hour_bucket_guard(self) -> None:
        snapshot = RateSnapshot(
            fetched_at="2026-04-14T10:30:00+03:00",
            source_date="14.04.2026",
            usd_buy=41.2,
            usd_sell=41.9,
            eur_buy=44.6,
            eur_sell=45.4,
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "useful_bot.exchange_rate_helper.STATE_FILE",
            Path(temp_dir) / "exchange_state.json",
        ):
            helper = ExchangeRateHelper(chat_id=1)
            helper._now_kyiv = lambda: datetime(2026, 4, 14, 10, 30, tzinfo=KYIV_TZ)
            helper._state["last_auto_sent_bucket"] = "2026-04-14T10"
            helper._fetch_snapshot = AsyncMock(return_value=snapshot)
            app = _FakeApplication()

            with patch(
                "useful_bot.exchange_rate_helper.build_exchange_rate_render_kwargs",
                return_value=({"usd_buy": 41.2}, []),
            ), patch(
                "useful_bot.exchange_rate_helper.run_cpu_bound",
                new=AsyncMock(return_value=io.BytesIO(b"image")),
            ):
                sent = await helper._run_check(app, reason="manual")

        self.assertTrue(sent)
        helper._fetch_snapshot.assert_awaited_once()

    async def test_run_check_records_exchange_analytics_for_sent_update(self) -> None:
        snapshot = RateSnapshot(
            fetched_at="2026-04-14T10:15:00+03:00",
            source_date="14.04.2026",
            usd_buy=41.1,
            usd_sell=41.8,
            eur_buy=44.5,
            eur_sell=45.3,
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "useful_bot.exchange_rate_helper.STATE_FILE",
            Path(temp_dir) / "exchange_state.json",
        ):
            sink = AnalyticsSink(Path(temp_dir) / "analytics", now_func=lambda: "2026-05-04T14:00:00Z")
            helper = ExchangeRateHelper(chat_id=1, analytics_sink=sink)
            helper._now_kyiv = lambda: datetime(2026, 4, 14, 10, 15, tzinfo=KYIV_TZ)
            helper._fetch_snapshot = AsyncMock(return_value=snapshot)
            app = _FakeApplication()

            with patch(
                "useful_bot.exchange_rate_helper.build_exchange_rate_render_kwargs",
                return_value=({"usd_buy": 41.1}, []),
            ), patch(
                "useful_bot.exchange_rate_helper.run_cpu_bound",
                new=AsyncMock(return_value=io.BytesIO(b"image")),
            ):
                sent = await helper._run_check(app, reason="manual")

            self.assertTrue(sent)
            event_path = Path(temp_dir) / "analytics" / "events" / "2026-05-04.exchange_rate_check.jsonl"
            event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["event"], "sent")
            self.assertEqual(event["reason"], "manual")
            self.assertEqual(event["usd_buy"], 41.1)
            self.assertEqual(event["eur_sell"], 45.3)



if __name__ == "__main__":
    unittest.main()
