import unittest

from helpers.lyst_runtime import build_shoe_message, get_sale_emoji


class LystRuntimeTests(unittest.TestCase):
    def test_get_sale_emoji_restores_expected_visual_tiers(self) -> None:
        self.assertEqual(get_sale_emoji(95, 5000), "🚀🚀🚀")
        self.assertEqual(get_sale_emoji(70, 3500), "✨✨✨")
        self.assertEqual(get_sale_emoji(70, 5000), "🍄🍄🍄")

    def test_build_shoe_message_matches_operator_preferred_labels(self) -> None:
        shoe = {
            "name": "Test Shoe",
            "original_price": "€300",
            "sale_price": "€180",
            "lowest_price": "€170",
            "lowest_price_uah": 7650,
            "store": "Lyst",
            "country": "IT",
            "shoe_link": "https://www.lyst.com/shoe",
        }

        message = build_shoe_message(
            shoe,
            sale_percentage=40,
            uah_sale=8100,
            kurs=45.0,
            kurs_symbol="€",
        )

        self.assertIn("🍄🍄🍄", message)
        self.assertIn("💀 Prices", message)
        self.assertIn("🤑 Grivniki", message)
        self.assertIn("🧊 Kurs", message)
        self.assertIn("🔗 Store", message)
        self.assertIn("🌍 Country", message)
        self.assertNotIn("????", message)
        self.assertNotIn("?? ", message)


if __name__ == "__main__":
    unittest.main()
