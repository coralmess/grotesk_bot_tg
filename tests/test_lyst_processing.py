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


class LystProcessAllShoesTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_all_shoes_returns_new_and_removed_counts(self):
        messages = []
        saved_batches = []
        progress = []
        processed_keys = set()

        class Queue:
            async def add_message(self, chat_id, message, image_url, uah_sale, sale_percentage):
                messages.append((chat_id, message, image_url, uah_sale, sale_percentage))

        class Logger:
            def info(self, *args, **kwargs):
                pass

            def error(self, *args, **kwargs):
                raise AssertionError(args)

        def convert_to_uah(price, country, exchange_rates, name):
            return type("Rate", (), {"exchange_rate": 40, "uah_amount": 2000, "currency_symbol": "$"})()

        async def is_shoe_processed(key):
            return False

        async def mark_shoe_processed(key):
            processed_keys.add(key)

        async def save_shoe_data_bulk(batch):
            saved_batches.append(batch)

        old_data = {
            "Old_9": {
                "name": "Old",
                "unique_id": "9",
                "country": "US",
                "active": True,
                "sale_price": "$100",
                "lowest_price": "$100",
                "lowest_price_uah": 4000,
                "uah_price": 4000,
            }
        }
        all_shoes = [
            {
                "name": "New",
                "unique_id": "1",
                "country": "US",
                "original_price": "$100",
                "sale_price": "$50",
                "image_url": "https://example.com/new.jpg",
                "shoe_link": "https://example.com/new",
                "base_url": {"telegram_chat_id": "chat", "min_sale": 10},
            }
        ]

        stats = await processing.process_all_shoes(
            all_shoes,
            old_data,
            Queue(),
            {},
            shoe_concurrency=1,
            resolve_redirects=False,
            run_failed=False,
            logger=Logger(),
            touch_progress=lambda event, **fields: progress.append((event, fields)),
            calculate_sale_percentage=lambda original, sale, country: 50,
            convert_to_uah=convert_to_uah,
            build_shoe_message=lambda shoe, sale_percentage, uah_sale, kurs, kurs_symbol: "message",
            is_shoe_processed=is_shoe_processed,
            mark_shoe_processed=mark_shoe_processed,
            save_shoe_data_bulk=save_shoe_data_bulk,
            get_final_clear_link=lambda *args: "unused",
        )

        self.assertEqual(stats.new_total, 1)
        self.assertEqual(stats.removed_total, 1)
        self.assertEqual(len(messages), 1)
        self.assertIn("New_1", old_data)
        self.assertFalse(old_data["Old_9"]["active"])
