import io
import unittest

from PIL import Image

from useful_bot.exchange_rate_image import CANVAS_H, CANVAS_W, render_exchange_rate_card


class ExchangeRateImageTests(unittest.TestCase):
    def test_exchange_card_renders_at_scaled_resolution(self) -> None:
        image_bytes = render_exchange_rate_card(
            usd_buy=41.1,
            usd_sell=41.8,
            eur_buy=44.5,
            eur_sell=45.3,
            prev_usd_buy=41.0,
            prev_usd_sell=41.7,
            prev_eur_buy=44.4,
            prev_eur_sell=45.2,
            usd_spread=0.7,
            eur_sell_minus_usd_buy=4.2,
        )

        rendered = Image.open(io.BytesIO(image_bytes.getvalue()))
        self.assertEqual(rendered.size, (CANVAS_W, CANVAS_H))
        self.assertGreaterEqual(CANVAS_W, 2000)
        self.assertGreaterEqual(CANVAS_H, 1800)


if __name__ == "__main__":
    unittest.main()
