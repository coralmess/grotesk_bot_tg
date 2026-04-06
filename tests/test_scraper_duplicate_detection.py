import tempfile
import unittest
from pathlib import Path

import olx_scraper
import shafa_scraper


class OlxDuplicateDetectionTests(unittest.TestCase):
    def test_duplicate_key_normalizes_whitespace_and_case(self) -> None:
        self.assertEqual(
            olx_scraper._duplicate_key("  Nike   Air  ", 5000),
            olx_scraper._duplicate_key("nike air", 5000),
        )

    def test_db_duplicate_lookup_uses_name_and_price_not_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = olx_scraper.DB_FILE
            try:
                olx_scraper.DB_FILE = Path(tmp_dir) / "olx_items.db"
                olx_scraper._db_init_sync()
                existing = olx_scraper.OlxItem(
                    id="existing-1",
                    name="Nike Air Max",
                    link="https://example.com/1",
                    price_text="5000 грн",
                    price_int=5000,
                )
                olx_scraper._db_upsert_item_sync(existing, "OLX", False)

                duplicate = olx_scraper.OlxItem(
                    id="new-2",
                    name="  nike   air max ",
                    link="https://example.com/2",
                    price_text="5000 грн",
                    price_int=5000,
                )
                distinct = olx_scraper.OlxItem(
                    id="new-3",
                    name="Nike Air Max",
                    link="https://example.com/3",
                    price_text="5100 грн",
                    price_int=5100,
                )

                duplicate_keys = olx_scraper._db_fetch_duplicate_keys_sync([duplicate, distinct])
                self.assertIn(olx_scraper._duplicate_key(duplicate.name, duplicate.price_int), duplicate_keys)
                self.assertNotIn(olx_scraper._duplicate_key(distinct.name, distinct.price_int), duplicate_keys)
            finally:
                olx_scraper.DB_FILE = original_db


class ShafaDuplicateDetectionTests(unittest.TestCase):
    def test_duplicate_key_normalizes_whitespace_and_case(self) -> None:
        self.assertEqual(
            shafa_scraper._duplicate_key("  Nike   Air  ", 5000),
            shafa_scraper._duplicate_key("nike air", 5000),
        )

    def test_db_duplicate_lookup_uses_name_and_price_not_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = shafa_scraper.DB_FILE
            try:
                shafa_scraper.DB_FILE = Path(tmp_dir) / "shafa_items.db"
                shafa_scraper._db_init_sync()
                existing = shafa_scraper.ShafaItem(
                    id="existing-1",
                    name="Nike Air Max",
                    link="https://example.com/1",
                    price_text="5000 грн",
                    price_int=5000,
                )
                shafa_scraper._db_upsert_items_sync([(existing, False)], "SHAFA")

                duplicate = shafa_scraper.ShafaItem(
                    id="new-2",
                    name="  nike   air max ",
                    link="https://example.com/2",
                    price_text="5000 грн",
                    price_int=5000,
                )
                distinct = shafa_scraper.ShafaItem(
                    id="new-3",
                    name="Nike Air Max",
                    link="https://example.com/3",
                    price_text="5100 грн",
                    price_int=5100,
                )

                duplicate_keys = shafa_scraper._db_fetch_duplicate_keys_sync([duplicate, distinct])
                self.assertIn(shafa_scraper._duplicate_key(duplicate.name, duplicate.price_int), duplicate_keys)
                self.assertNotIn(shafa_scraper._duplicate_key(distinct.name, distinct.price_int), duplicate_keys)
            finally:
                shafa_scraper.DB_FILE = original_db
