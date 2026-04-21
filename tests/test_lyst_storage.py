import asyncio
import logging
import tempfile
import unittest
from pathlib import Path

from helpers.lyst.storage import LystStorage


class LystStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_storage_round_trip_and_processed_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "shoes.db")
            json_path = Path(tmp) / "shoes.json"
            storage = LystStorage(db_name=db_path, shoe_data_file=json_path, logger=logging.getLogger("test_lyst_storage"))

            storage.create_tables()
            await storage.save_shoe_data_bulk(
                [
                    {
                        "key": "shoe-1",
                        "name": "Shoe",
                        "unique_id": "u1",
                        "original_price": "$200",
                        "sale_price": "$100",
                        "image_url": "https://example.com/image.jpg",
                        "store": "Store",
                        "country": "US",
                        "shoe_link": "https://example.com/shoe",
                        "lowest_price": "$100",
                        "lowest_price_uah": 4000.0,
                        "uah_price": 4000.0,
                        "active": True,
                    }
                ]
            )

            data = await storage.load_shoe_data()
            self.assertIn("shoe-1", data)
            self.assertFalse(await storage.is_shoe_processed("shoe-1"))
            await storage.mark_shoe_processed("shoe-1")
            self.assertTrue(await storage.is_shoe_processed("shoe-1"))

