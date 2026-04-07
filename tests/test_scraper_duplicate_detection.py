import tempfile
import unittest
from pathlib import Path

import olx_scraper
import shafa_scraper
from telegram.error import TimedOut


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

    def test_notification_ledger_blocks_duplicate_without_item_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = olx_scraper.DB_FILE
            try:
                olx_scraper.DB_FILE = Path(tmp_dir) / "olx_items.db"
                olx_scraper._db_init_sync()
                sent = olx_scraper.OlxItem(
                    id="sent-1",
                    name="THOM KROM avangard hoodie худі",
                    link="https://example.com/1",
                    price_text="1090 грн",
                    price_int=1090,
                )
                olx_scraper._db_mark_notification_sent_sync(sent, "Thom Krom")

                candidate = olx_scraper.OlxItem(
                    id="candidate-2",
                    name="  thom   krom avangard hoodie   худі ",
                    link="https://example.com/2",
                    price_text="1090 грн",
                    price_int=1090,
                )

                duplicate_keys = olx_scraper._db_fetch_duplicate_keys_sync([candidate])
                self.assertIn(olx_scraper._duplicate_key(candidate.name, candidate.price_int), duplicate_keys)
            finally:
                olx_scraper.DB_FILE = original_db

    def test_timeout_assumed_delivered_for_send_retry(self) -> None:
        calls = {"count": 0}

        @olx_scraper.async_retry(max_retries=3, assume_timeout_success=True)
        async def _send() -> bool:
            calls["count"] += 1
            raise TimedOut("timeout")

        result = __import__("asyncio").run(_send())
        self.assertTrue(result)
        self.assertEqual(calls["count"], 1)


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

    def test_notification_ledger_blocks_duplicate_without_item_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = shafa_scraper.DB_FILE
            try:
                shafa_scraper.DB_FILE = Path(tmp_dir) / "shafa_items.db"
                shafa_scraper._db_init_sync()
                sent = shafa_scraper.ShafaItem(
                    id="sent-1",
                    name="THOM KROM avangard hoodie худі",
                    link="https://example.com/1",
                    price_text="1090 грн",
                    price_int=1090,
                )
                shafa_scraper._db_mark_notification_sent_sync(sent, "Thom Krom")

                candidate = shafa_scraper.ShafaItem(
                    id="candidate-2",
                    name="  thom   krom avangard hoodie   худі ",
                    link="https://example.com/2",
                    price_text="1090 грн",
                    price_int=1090,
                )

                duplicate_keys = shafa_scraper._db_fetch_duplicate_keys_sync([candidate])
                self.assertIn(shafa_scraper._duplicate_key(candidate.name, candidate.price_int), duplicate_keys)
            finally:
                shafa_scraper.DB_FILE = original_db

    def test_timeout_assumed_delivered_for_send_retry(self) -> None:
        calls = {"count": 0}

        @shafa_scraper.async_retry(max_retries=3, assume_timeout_success=True)
        async def _send() -> bool:
            calls["count"] += 1
            raise TimedOut("timeout")

        result = __import__("asyncio").run(_send())
        self.assertTrue(result)
        self.assertEqual(calls["count"], 1)
