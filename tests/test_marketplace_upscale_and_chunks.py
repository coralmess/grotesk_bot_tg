import io
import json
import tempfile
from pathlib import Path
import asyncio
import unittest
from unittest.mock import patch

from PIL import Image
from bs4 import BeautifulSoup

from helpers.analytics_events import AnalyticsSink
from helpers.image_pipeline import encode_jpeg_for_telegram, send_remote_photo_with_fallback, upscale_image_bytes_for_telegram_sync
from olx_scraper import _extract_first_image_from_card, _next_chunk_pause, _source_chunks, fetch_first_image_best


class MarketplaceChunkingTests(unittest.TestCase):
    def test_source_chunks_process_every_source_once(self):
        sources = [{"url": f"https://example.com/{index}"} for index in range(5)]

        chunks = _source_chunks(sources, 2)

        self.assertEqual([len(chunk) for chunk in chunks], [2, 2, 1])
        self.assertEqual([entry["url"] for chunk in chunks for entry in chunk], [entry["url"] for entry in sources])

    def test_chunk_pause_stays_inside_configured_range(self):
        pause = _next_chunk_pause(20, 45)

        self.assertGreaterEqual(pause, 20)
        self.assertLessEqual(pause, 45)


class MarketplaceImageUpscaleTests(unittest.TestCase):
    def _image_bytes(self, width: int, height: int) -> bytes:
        out = io.BytesIO()
        Image.new("RGB", (width, height), (240, 240, 240)).save(out, format="JPEG")
        return out.getvalue()

    def test_large_images_are_not_upscaled(self):
        data = self._image_bytes(1500, 1500)

        result = upscale_image_bytes_for_telegram_sync(data, min_upscale_dim=1500, upscale_factors=(2.0,))

        self.assertIsNone(result)

    def test_small_images_are_upscaled_with_lanczos_x2_enhancement(self):
        data = self._image_bytes(100, 80)

        result = upscale_image_bytes_for_telegram_sync(data, min_upscale_dim=1500, upscale_factors=(2.0,))

        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as im:
            self.assertEqual(im.size, (200, 160))

    def test_medium_marketplace_images_under_1500_short_side_are_upscaled(self):
        data = self._image_bytes(770, 577)

        result = upscale_image_bytes_for_telegram_sync(data, min_upscale_dim=1500, upscale_factors=(2.0,))

        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as im:
            self.assertEqual(im.size, (1540, 1154))

    def test_upscale_records_image_analytics(self):
        data = self._image_bytes(100, 80)

        with tempfile.TemporaryDirectory() as tmp_dir:
            sink = AnalyticsSink(Path(tmp_dir), now_func=lambda: "2026-05-04T13:00:00Z")
            result = upscale_image_bytes_for_telegram_sync(
                data,
                min_upscale_dim=1500,
                upscale_factors=(2.0,),
                analytics_sink=sink,
                source_kind="olx",
                source_name="OLX Source",
                image_url="https://example.com/photo.jpg?token=hidden",
            )

            self.assertIsNotNone(result)
            event_path = Path(tmp_dir) / "events" / "2026-05-04.image_pipeline.jsonl"
            event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["source_kind"], "olx")
            self.assertEqual(event["source_name"], "OLX Source")
            self.assertEqual(event["input_width"], 100)
            self.assertEqual(event["output_width"], 200)
            self.assertEqual(event["upscale_factor"], 2.0)
            self.assertEqual(event["method"], "lanczos_enhanced")
            self.assertEqual(event["fallback_mode"], "photo_prepared")
            self.assertEqual(event["url_host"], "example.com")
            self.assertNotIn("hidden", json.dumps(event))

    def test_send_remote_photo_records_text_fallback_when_no_image(self):
        async def _run():
            sent_messages = []

            async def send_message(_bot, _chat_id, text):
                sent_messages.append(text)
                return True

            async def send_photo_by_bytes(_bot, _chat_id, _photo_bytes, _caption):
                return True

            async def run_cpu_bound_fn(*args, **kwargs):
                return None

            with tempfile.TemporaryDirectory() as tmp_dir:
                sink = AnalyticsSink(Path(tmp_dir), now_func=lambda: "2026-05-04T13:01:00Z")
                result = await send_remote_photo_with_fallback(
                    bot=object(),
                    chat_id="1",
                    caption="caption",
                    image_url=None,
                    is_valid_image_url=lambda url: False,
                    download_bytes=lambda url: None,
                    send_message=send_message,
                    send_photo_by_bytes=send_photo_by_bytes,
                    run_cpu_bound_fn=run_cpu_bound_fn,
                    logger=None,
                    analytics_sink=sink,
                    source_kind="shafa",
                    source_name="SHAFA Source",
                )

                self.assertTrue(result)
                event = json.loads((Path(tmp_dir) / "events" / "2026-05-04.image_pipeline.jsonl").read_text(encoding="utf-8").splitlines()[0])
                self.assertEqual(event["event"], "text_fallback")
                self.assertEqual(event["fallback_reason"], "invalid_or_missing_url")
                self.assertEqual(event["fallback_mode"], "invalid_image")
                self.assertEqual(sent_messages, ["caption"])

        asyncio.run(_run())

    def test_skip_upscale_records_dimensions_method_and_fallback_mode(self):
        data = self._image_bytes(1600, 1500)

        with tempfile.TemporaryDirectory() as tmp_dir:
            sink = AnalyticsSink(Path(tmp_dir), now_func=lambda: "2026-05-04T13:02:00Z")
            result = upscale_image_bytes_for_telegram_sync(
                data,
                min_upscale_dim=1500,
                analytics_sink=sink,
                source_kind="olx",
                source_name="Large Source",
            )

            self.assertIsNone(result)
            event = json.loads((Path(tmp_dir) / "events" / "2026-05-04.image_pipeline.jsonl").read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["input_width"], 1600)
            self.assertEqual(event["input_height"], 1500)
            self.assertEqual(event["output_width"], 1600)
            self.assertEqual(event["output_height"], 1500)
            self.assertEqual(event["method"], "none")
            self.assertEqual(event["fallback_mode"], "photo_prepared")

    def test_marketplace_jpeg_encoder_uses_fixed_quality_98(self):
        qualities = []

        def fake_save(_image, fp, **kwargs):
            qualities.append(kwargs.get("quality"))
            # Simulate an oversized high-quality output. Marketplace callers should
            # resize and retry instead of lowering JPEG quality, because lower quality
            # visibly hurts OLX/SHAFA photos after Telegram recompresses them again.
            fp.write(b"x" * (11 * 1024 * 1024))

        with patch.object(Image.Image, "save", autospec=True, side_effect=fake_save):
            result = encode_jpeg_for_telegram(Image.new("RGB", (100, 80)))

        self.assertIsNone(result)
        self.assertEqual(qualities, [98])


class OlxImageSelectionTests(unittest.TestCase):
    DETAIL_HTML = """
        <div class="swiper-wrapper">
          <div>
            <img
              src="https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1024x768"
              srcset="
                https://ireland.apollo.olxcdn.com/v1/files/item/image;s=389x272 420w,
                https://ireland.apollo.olxcdn.com/v1/files/item/image;s=516x361 780w,
                https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1000x700 992w"
            />
          </div>
        </div>
    """

    def test_olx_image_selection_prefers_largest_declared_area(self):
        card = BeautifulSoup(
            """
            <div data-testid="l-card">
              <img
                src="https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1024x768"
                srcset="
                  https://ireland.apollo.olxcdn.com/v1/files/item/image;s=389x272 420w,
                  https://ireland.apollo.olxcdn.com/v1/files/item/image;s=516x361 780w,
                  https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1000x700 992w"
                data-src="https://ireland.apollo.olxcdn.com/v1/files/item/image;s=800x600"
              />
            </div>
            """,
            "html.parser",
        )

        image_url = _extract_first_image_from_card(card)

        self.assertEqual(image_url, "https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1024x768")

    def test_olx_detail_image_selection_prefers_src_when_it_is_larger_than_srcset(self):
        async def fake_fetch_html(_url):
            return self.DETAIL_HTML

        with patch("olx_scraper.fetch_html", fake_fetch_html):
            image_url = asyncio.run(fetch_first_image_best("https://www.olx.ua/item"))

        self.assertEqual(image_url, "https://ireland.apollo.olxcdn.com/v1/files/item/image;s=1024x768")


if __name__ == "__main__":
    unittest.main()
