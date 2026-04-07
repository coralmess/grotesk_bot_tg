import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from helpers import scraper_unsubscribes


def _make_reply_message(text: str | None = None, caption: str | None = None, url: str | None = None):
    entity = None
    if url:
        entity = SimpleNamespace(type="text_link", url=url, offset=2, length=8)
    return SimpleNamespace(
        text=text,
        caption=caption,
        entities=[entity] if entity and text else [],
        caption_entities=[entity] if entity and caption else [],
    )


class ScraperUnsubscribesTests(unittest.TestCase):
    def test_parse_reply_item_identity_for_olx_caption(self) -> None:
        reply = _make_reply_message(
            caption="✨THOM KROM avangard hoodie худі✨\n\n💰 Ціна: 1090 грн\n🔗 Відкрити",
            url="https://www.olx.ua/d/uk/obyavlenie/thom-krom-avangard-hoodie-hud-ID10fUj4.html?search_reason=search%7Corganic",
        )
        identity = scraper_unsubscribes.parse_reply_item_identity(reply)
        self.assertIsNotNone(identity)
        self.assertEqual(identity.source, "olx")
        self.assertEqual(identity.item_id, "thom-krom-avangard-hoodie-hud-ID10fUj4")
        self.assertEqual(identity.name, "THOM KROM avangard hoodie худі")

    def test_parse_reply_item_identity_for_shafa_text(self) -> None:
        reply = _make_reply_message(
            text="✨Sunspel polo✨\n\n💰 Ціна: 1001 грн\n🔗 Відкрити",
            url="https://shafa.ua/uk/men/futbolki-i-maiki/polo/123456-sunspel-polo",
        )
        identity = scraper_unsubscribes.parse_reply_item_identity(reply)
        self.assertIsNotNone(identity)
        self.assertEqual(identity.source, "shafa")
        self.assertEqual(identity.item_id, "123456")
        self.assertEqual(identity.name, "Sunspel polo")

    def test_unsubscribe_registry_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = scraper_unsubscribes.UNSUBSCRIBE_DB_FILE
            try:
                scraper_unsubscribes.UNSUBSCRIBE_DB_FILE = Path(tmp_dir) / "scraper_unsubscribes.db"
                asyncio.run(scraper_unsubscribes.init_unsubscribe_db())
                reply = _make_reply_message(
                    caption="✨THOM KROM avangard hoodie худі✨\n\n💰 Ціна: 1090 грн\n🔗 Відкрити",
                    url="https://www.olx.ua/d/uk/obyavlenie/thom-krom-avangard-hoodie-hud-ID10fUj4.html?search_reason=search%7Corganic",
                )
                message = SimpleNamespace(reply_to_message=reply)
                ok, response = asyncio.run(scraper_unsubscribes.unsubscribe_from_reply_message(message))
                self.assertTrue(ok)
                self.assertIn("THOM KROM avangard hoodie худі", response)

                muted = asyncio.run(
                    scraper_unsubscribes.fetch_unsubscribed_ids(
                        "olx",
                        ["thom-krom-avangard-hoodie-hud-ID10fUj4", "another-id"],
                    )
                )
                self.assertEqual(muted, {"thom-krom-avangard-hoodie-hud-ID10fUj4"})
            finally:
                scraper_unsubscribes.UNSUBSCRIBE_DB_FILE = original_db

    def test_unsubscribe_requires_reply_with_supported_link(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_db = scraper_unsubscribes.UNSUBSCRIBE_DB_FILE
            try:
                scraper_unsubscribes.UNSUBSCRIBE_DB_FILE = Path(tmp_dir) / "scraper_unsubscribes.db"
                message = SimpleNamespace(reply_to_message=None)
                ok, response = asyncio.run(scraper_unsubscribes.unsubscribe_from_reply_message(message))
                self.assertFalse(ok)
                self.assertIn("Reply to an OLX or Shafa item message", response)
            finally:
                scraper_unsubscribes.UNSUBSCRIBE_DB_FILE = original_db
