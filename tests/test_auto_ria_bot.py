import asyncio
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from helpers.auto_ria.runtime import AutoRiaBotRuntime
from helpers.auto_ria.parsing import (
    build_auto_ria_caption,
    build_auto_ria_sold_caption,
    extract_vin_from_detail_html,
    is_auto_ria_sold_detail_html,
    normalize_auto_ria_image_url,
    normalize_auto_ria_search_url,
    parse_auto_ria_search_html,
    parse_nhtsa_vpic_payload,
)
from helpers.auto_ria.storage import AutoRiaSentItem, AutoRiaStorage


SEARCH_HTML = """
<a class="link product-card horizontal" href="/uk/auto_audi_a3_39769491.html" id="39769491" data-car-id="39769491">
  <div class="product-card-template">
    <div class="product-card-gallery">
      <picture>
        <img src="https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fx.jpg" title="Седан Audi A3 2015 в Львові">
      </picture>
    </div>
    <div class="product-card-content">
      <div class="common-text size-16-20 titleS fw-bold mb-4"> Audi A3 2015</div>
      <div class="common-text size-14-16 ellipsis-1 mb-8">8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis</div>
      <div><span class="common-text titleM c-green">10 490 $ </span><span class="common-text body"> · 544 236 грн </span></div>
      <div class="grid-wrapper">
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">109 тис. км</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Робот</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Бензин, 1.98 л</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Львів (Львівська)</span></div>
      </div>
    </div>
  </div>
</a>
"""

DETAIL_HTML = """
<div id="badgesVin">
  <span>WAUB8GFF5G1020011</span>
</div>
"""

NHTSA_VPIC_PAYLOAD = {
    "Results": [
        {
            "Trim": "quattro Premium",
            "Series": "",
            "Series2": "",
            "TransmissionStyle": "Automatic",
            "TransmissionSpeeds": "",
        }
    ]
}

SOLD_DETAIL_HTML = """
<script type="application/ld+json">
{"@type":"Offer","availability":"https://schema.org/SoldOut"}
</script>
"""


class AutoRiaParsingTests(unittest.TestCase):
    def test_parse_search_html_extracts_listing_fields(self) -> None:
        listings = parse_auto_ria_search_html(SEARCH_HTML)

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.id, "39769491")
        self.assertEqual(listing.title, "Audi A3 2015")
        self.assertEqual(listing.subtitle, "8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis")
        self.assertEqual(listing.price_usd, 10490)
        self.assertEqual(listing.price_text, "10 490 $")
        self.assertEqual(listing.mileage_text, "109 тис. км")
        self.assertEqual(listing.fuel_engine_text, "Бензин, 1.98 л")
        self.assertEqual(
            listing.image_url,
            "https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fhd.jpg",
        )
        self.assertEqual(listing.url, "https://auto.ria.com/uk/auto_audi_a3_39769491.html")

    def test_extract_vin_from_detail_html(self) -> None:
        self.assertEqual(extract_vin_from_detail_html(DETAIL_HTML), "WAUB8GFF5G1020011")

    def test_parse_nhtsa_vpic_payload(self) -> None:
        vin_details = parse_nhtsa_vpic_payload(NHTSA_VPIC_PAYLOAD)
        self.assertEqual(vin_details.trim, "quattro Premium")
        self.assertEqual(vin_details.transmission, "Automatic")

    def test_build_caption_includes_optional_vin_fields(self) -> None:
        listing = parse_auto_ria_search_html(SEARCH_HTML)[0]
        caption = build_auto_ria_caption(
            listing,
            transmission="Automatic",
            trim="quattro Premium",
        )
        self.assertEqual(
            caption,
            '<a href="https://auto.ria.com/uk/auto_audi_a3_39769491.html"><b>Audi A3 2015</b></a>\n'
            "<i>8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis</i>\n\n"
            "<b>Ціна:</b> <b>10 490 $</b>\n"
            "<b>Пробіг:</b> 109 тис. км\n"
            "<b>Двигун:</b> Бензин, 1.98 л\n"
            "<b>Коробка:</b> Automatic\n"
            "<b>Комплектація:</b> quattro Premium",
        )

    def test_build_caption_omits_vin_fields_when_unavailable(self) -> None:
        listing = parse_auto_ria_search_html(SEARCH_HTML)[0]
        caption = build_auto_ria_caption(listing, transmission=None, trim=None)
        self.assertNotIn("<b>Коробка:</b>", caption)
        self.assertNotIn("<b>Комплектація:</b>", caption)
        self.assertIn("<b>Ціна:</b> <b>10 490 $</b>", caption)

    def test_normalize_auto_ria_image_url_switches_to_hd_variant(self) -> None:
        self.assertEqual(
            normalize_auto_ria_image_url("https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fx.jpg"),
            "https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fhd.jpg",
        )

    def test_normalize_search_url_requests_one_large_first_page(self) -> None:
        url = "https://auto.ria.com/uk/search/?search_type=1&page=2&limit=20&price[2]=13000"
        self.assertEqual(
            normalize_auto_ria_search_url(url),
            "https://auto.ria.com/uk/search/?search_type=1&page=0&limit=100&price%5B2%5D=13000&order=7",
        )

    def test_sold_detail_detection_uses_schema_availability(self) -> None:
        self.assertTrue(is_auto_ria_sold_detail_html(SOLD_DETAIL_HTML))
        self.assertFalse(is_auto_ria_sold_detail_html(DETAIL_HTML))

    def test_sold_caption_preserves_link_and_marks_listing_sold(self) -> None:
        listing = parse_auto_ria_search_html(SEARCH_HTML)[0]
        caption = build_auto_ria_sold_caption(
            listing,
            original_caption=build_auto_ria_caption(listing, transmission=None, trim=None),
        )
        self.assertIn("<b>Продано</b>", caption)
        self.assertIn("https://auto.ria.com/uk/auto_audi_a3_39769491.html", caption)


class FakeAutoRiaBot:
    def __init__(self) -> None:
        self.edited_captions: list[dict] = []

    async def edit_message_caption(self, **kwargs):
        self.edited_captions.append(kwargs)
        return True


class FakeAutoRiaSoldStorage:
    def __init__(self) -> None:
        self.sold_ids: list[str] = []

    def fetch_active_sent_items(self) -> list[AutoRiaSentItem]:
        return [
            AutoRiaSentItem(
                car_id="39769491",
                title="Audi A3 2015",
                url="https://auto.ria.com/uk/auto_audi_a3_39769491.html",
                price_usd=10490,
                message_id=123,
                message_kind="photo",
                caption="original caption",
            )
        ]

    def mark_sold(self, *, car_id: str) -> None:
        self.sold_ids.append(car_id)


class AutoRiaRuntimeTests(unittest.TestCase):
    def test_runtime_normalizes_configured_search_urls_to_large_first_page(self) -> None:
        runtime = AutoRiaBotRuntime(
            bot_token="123:abc",
            chat_id=1,
            sources=[{"url": "https://auto.ria.com/uk/search/?page=2&limit=20", "url_name": "cars"}],
        )

        self.assertEqual(runtime._sources[0].url, "https://auto.ria.com/uk/search/?page=0&limit=100&order=7")


    def test_runtime_records_send_analytics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = AutoRiaBotRuntime(
                bot_token="123:abc",
                chat_id=1,
                sources=[],
                analytics_sink=AnalyticsSink(Path(tmp_dir), now_func=lambda: "2026-05-04T16:00:00Z"),
            )

            runtime._record_send_analytics(
                event="sent",
                message_kind="photo",
                image_url="https://cdn.example.com/car.jpg?token=hidden",
                raw_bytes=100,
                output_bytes=200,
            )

            event_path = Path(tmp_dir) / "events" / "2026-05-04.auto_ria_send.jsonl"
            event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["event"], "sent")
            self.assertEqual(event["message_kind"], "photo")
            self.assertEqual(event["raw_bytes"], 100)
            self.assertEqual(event["url_host"], "cdn.example.com")
            self.assertNotIn("hidden", json.dumps(event))

    def test_refresh_sold_status_stops_after_detail_rate_limit(self) -> None:
        class MultiStorage(FakeAutoRiaSoldStorage):
            def fetch_active_sent_items(self) -> list[AutoRiaSentItem]:
                return [
                    AutoRiaSentItem(
                        car_id="one",
                        title="One",
                        url="https://auto.ria.com/uk/auto_one.html",
                        price_usd=10000,
                        message_id=1,
                        message_kind="photo",
                        caption="one",
                    ),
                    AutoRiaSentItem(
                        car_id="two",
                        title="Two",
                        url="https://auto.ria.com/uk/auto_two.html",
                        price_usd=11000,
                        message_id=2,
                        message_kind="photo",
                        caption="two",
                    ),
                ]

        async def run_test():
            runtime = AutoRiaBotRuntime(
                bot_token="123:abc",
                chat_id=1,
                sources=[],
                detail_request_delay_sec=0,
                detail_rate_limit_cooldown_sec=30,
                monotonic_func=lambda: 100.0,
            )
            runtime._storage = MultiStorage()
            calls = []

            async def fake_fetch_detail_text(url: str):
                calls.append(url)
                return None

            runtime._fetch_detail_text = fake_fetch_detail_text
            runtime._note_detail_rate_limit(retry_after_seconds=12, url="https://auto.ria.com/uk/auto_one.html")
            await runtime._refresh_sold_statuses()
            return runtime, calls

        runtime, calls = asyncio.run(run_test())

        self.assertEqual(calls, [])
        self.assertEqual(runtime._detail_rate_limited_until, 112.0)

    def test_detail_fetch_sets_cooldown_on_429(self) -> None:
        class FakeResponse:
            status_code = 429
            text = ""
            headers = {"Retry-After": "17"}

            def raise_for_status(self):
                raise AssertionError("429 should be handled before raise_for_status")

        times = iter([10.0, 10.0, 20.0])
        runtime = AutoRiaBotRuntime(
            bot_token="123:abc",
            chat_id=1,
            sources=[],
            detail_request_delay_sec=0,
            detail_rate_limit_cooldown_sec=30,
            monotonic_func=lambda: next(times),
        )

        with unittest.mock.patch("helpers.auto_ria.runtime.requests.get", return_value=FakeResponse()):
            first = runtime._fetch_detail_text_sync("https://auto.ria.com/uk/auto_one.html")
            skipped = runtime._fetch_detail_text_sync("https://auto.ria.com/uk/auto_two.html")

        self.assertIsNone(first)
        self.assertIsNone(skipped)
        self.assertAlmostEqual(runtime._detail_rate_limited_until, 27.0)

    def test_refresh_sold_status_edits_stored_photo_message(self) -> None:
        async def run_test() -> tuple[FakeAutoRiaBot, FakeAutoRiaSoldStorage]:
            runtime = AutoRiaBotRuntime(
                bot_token="123:abc",
                chat_id=1,
                sources=[],
            )
            fake_bot = FakeAutoRiaBot()
            fake_storage = FakeAutoRiaSoldStorage()
            runtime._bot = fake_bot
            runtime._storage = fake_storage

            async def fake_fetch_text(url: str):
                return SOLD_DETAIL_HTML

            runtime._fetch_detail_text = fake_fetch_text
            await runtime._refresh_sold_statuses()
            return fake_bot, fake_storage

        fake_bot, fake_storage = asyncio.run(run_test())

        self.assertEqual(fake_storage.sold_ids, ["39769491"])
        self.assertEqual(fake_bot.edited_captions[0]["message_id"], 123)
        self.assertIn("<b>Продано</b>", fake_bot.edited_captions[0]["caption"])


class AutoRiaStorageTests(unittest.TestCase):
    def test_storage_tracks_seen_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "auto_ria_items.db"
            storage = AutoRiaStorage(db_path)
            storage.create_tables()

            self.assertEqual(storage.fetch_seen_ids(["39769491"]), set())

            storage.mark_sent(
                car_id="39769491",
                title="Audi A3 2015",
                url="https://auto.ria.com/uk/auto_audi_a3_39769491.html",
                price_usd=10490,
                message_id=123,
                message_kind="photo",
                caption="caption",
            )

            self.assertEqual(storage.fetch_seen_ids(["39769491", "other"]), {"39769491"})

            records = storage.fetch_active_sent_items()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].message_id, 123)
            self.assertEqual(records[0].message_kind, "photo")
            self.assertEqual(records[0].caption, "caption")
            storage.mark_sold(car_id="39769491")
            self.assertEqual(storage.fetch_active_sent_items(), [])


if __name__ == "__main__":
    unittest.main()
