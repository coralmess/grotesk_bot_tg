import io
import unittest

from PIL import Image

from helpers.image_pipeline import upscale_image_bytes_for_telegram_sync
from olx_scraper import _next_chunk_pause, _source_chunks


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


if __name__ == "__main__":
    unittest.main()
