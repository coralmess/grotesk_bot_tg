import unittest

from helpers.lyst import processing


class LystProcessingTests(unittest.TestCase):
    def test_filter_duplicates_prefers_cheaper_country_when_gap_is_material(self):
        shoes = [
            {"name": "Item", "unique_id": "1", "country": "PL", "sale_price": "€200", "image_url": "https://a", "uah_price": 0},
            {"name": "Item", "unique_id": "1", "country": "US", "sale_price": "$120", "image_url": "https://b", "uah_price": 0},
        ]

        def convert_to_uah(price, country, exchange_rates, name):
            return type("R", (), {"uah_amount": 8000 if country == "PL" else 5000})()

        filtered = processing.filter_duplicates(
            shoes,
            {},
            country_priority=["PL", "US"],
            convert_to_uah=convert_to_uah,
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["country"], "US")

