import unittest
from dataclasses import dataclass

from helpers.marketplace_core import MarketplaceItem
from helpers.marketplace_pipeline import (
    ItemDecision,
    ItemUpdate,
    RunDuplicateTracker,
    process_marketplace_items,
)
from helpers import marketplace_pipeline


@dataclass
class DummyItem(MarketplaceItem):
    pass


class DummyRepository:
    def __init__(self) -> None:
        self.existing = {}
        self.duplicate_keys = set()
        self.claim_results = {}
        self.marked_sent = []
        self.released = []
        self.persisted = []

    async def fetch_existing(self, item_ids):
        return [self.existing.get(item_id) for item_id in item_ids]

    async def fetch_duplicate_keys(self, items):
        return set(self.duplicate_keys)

    async def claim_notification_key(self, item, source_name):
        return self.claim_results.get(item.id, True)

    async def mark_notification_sent(self, item, source_name):
        self.marked_sent.append((item.id, source_name))

    async def release_notification_claim(self, item, source_name):
        self.released.append((item.id, source_name))

    async def persist_items(self, updates, source_name):
        self.persisted.extend((update.item.id, update.touch_last_sent, source_name) for update in updates)

    async def get_source_stats(self, url):
        raise NotImplementedError

    async def update_source_stats(self, url, streak, cycle_count):
        raise NotImplementedError


class MarketplacePipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.original_fetch_unsubscribed_ids = marketplace_pipeline.fetch_unsubscribed_ids

        async def _fake_fetch_unsubscribed_ids(source_kind, item_ids):
            return set()

        marketplace_pipeline.fetch_unsubscribed_ids = _fake_fetch_unsubscribed_ids

    async def asyncTearDown(self) -> None:
        marketplace_pipeline.fetch_unsubscribed_ids = self.original_fetch_unsubscribed_ids

    async def test_process_marketplace_items_sends_and_persists(self) -> None:
        repository = DummyRepository()
        duplicate_tracker = RunDuplicateTracker[DummyItem]()
        sent_messages = []

        async def _send_item(item, text, source_name):
            sent_messages.append((item.id, text, source_name))
            return True

        item = DummyItem(id="1", name="Item", link="https://example.com", price_text="100 грн", price_int=100)
        stats = await process_marketplace_items(
            source_kind="olx",
            source_name="OLX",
            items=[item],
            repository=repository,
            duplicate_tracker=duplicate_tracker,
            decide_item=lambda current, previous: ItemDecision(send_notification=True, is_new_item=True),
            build_message=lambda current, previous, source_name: f"{source_name}:{current.name}",
            send_item=_send_item,
        )

        self.assertEqual(stats.total_new, 1)
        self.assertEqual(stats.total_sent, 1)
        self.assertEqual(repository.marked_sent, [("1", "OLX")])
        self.assertEqual(repository.released, [])
        self.assertIn(("1", True, "OLX"), repository.persisted)
        self.assertEqual(sent_messages, [("1", "OLX:Item", "OLX")])

    async def test_process_marketplace_items_persists_without_send(self) -> None:
        repository = DummyRepository()
        duplicate_tracker = RunDuplicateTracker[DummyItem]()
        item = DummyItem(id="2", name="Item", link="https://example.com", price_text="100 грн", price_int=100)

        async def _send_item(item, text, source_name):
            return True

        stats = await process_marketplace_items(
            source_kind="shafa",
            source_name="SHAFA",
            items=[item],
            repository=repository,
            duplicate_tracker=duplicate_tracker,
            decide_item=lambda current, previous: ItemDecision(persist_without_send=True),
            build_message=lambda current, previous, source_name: "unused",
            send_item=_send_item,
        )

        self.assertEqual(stats.total_new, 0)
        self.assertEqual(stats.total_sent, 0)
        self.assertEqual(repository.marked_sent, [])
        self.assertEqual(repository.persisted, [("2", False, "SHAFA")])

    async def test_process_marketplace_items_releases_claim_on_failed_send(self) -> None:
        repository = DummyRepository()
        duplicate_tracker = RunDuplicateTracker[DummyItem]()
        item = DummyItem(id="3", name="Item", link="https://example.com", price_text="100 грн", price_int=100)

        async def _send_item(item, text, source_name):
            return False

        stats = await process_marketplace_items(
            source_kind="olx",
            source_name="OLX",
            items=[item],
            repository=repository,
            duplicate_tracker=duplicate_tracker,
            decide_item=lambda current, previous: ItemDecision(send_notification=True),
            build_message=lambda current, previous, source_name: "message",
            send_item=_send_item,
        )

        self.assertEqual(stats.total_sent, 0)
        self.assertEqual(repository.marked_sent, [])
        self.assertEqual(repository.released, [("3", "OLX")])
        self.assertIn(("3", False, "OLX"), repository.persisted)

    async def test_process_marketplace_items_skips_unsubscribed(self) -> None:
        repository = DummyRepository()
        duplicate_tracker = RunDuplicateTracker[DummyItem]()

        async def _fake_fetch_unsubscribed_ids(source_kind, item_ids):
            return {"4"}

        marketplace_pipeline.fetch_unsubscribed_ids = _fake_fetch_unsubscribed_ids
        item = DummyItem(id="4", name="Item", link="https://example.com", price_text="100 грн", price_int=100)

        async def _send_item(item, text, source_name):
            return True

        stats = await process_marketplace_items(
            source_kind="olx",
            source_name="OLX",
            items=[item],
            repository=repository,
            duplicate_tracker=duplicate_tracker,
            decide_item=lambda current, previous: ItemDecision(send_notification=True),
            build_message=lambda current, previous, source_name: "message",
            send_item=_send_item,
        )

        self.assertEqual(stats.total_sent, 0)
        self.assertEqual(repository.persisted, [])
