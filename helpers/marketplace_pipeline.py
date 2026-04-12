from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, Protocol, Sequence, TypeVar

from helpers.marketplace_core import MarketplaceItem, SourceStats, duplicate_key
from helpers.scraper_unsubscribes import fetch_unsubscribed_ids

ItemT = TypeVar("ItemT", bound=MarketplaceItem)


@dataclass(frozen=True)
class ItemUpdate(Generic[ItemT]):
    item: ItemT
    touch_last_sent: bool


@dataclass(frozen=True)
class ItemDecision:
    send_notification: bool = False
    persist_without_send: bool = False
    is_new_item: bool = False


@dataclass
class PipelineStats:
    total_new: int = 0
    total_sent: int = 0


class MarketplaceRepository(Protocol[ItemT]):
    async def fetch_existing(self, item_ids: list[str]) -> list[Optional[Dict[str, Any]]]:
        ...

    async def fetch_duplicate_keys(self, items: list[ItemT]) -> set[tuple[str, int]]:
        ...

    async def claim_notification_key(self, item: ItemT, source_name: str) -> bool:
        ...

    async def mark_notification_sent(self, item: ItemT, source_name: str) -> None:
        ...

    async def release_notification_claim(self, item: ItemT, source_name: str) -> None:
        ...

    async def persist_items(self, updates: list[ItemUpdate[ItemT]], source_name: str) -> None:
        ...

    async def get_source_stats(self, url: str) -> SourceStats:
        ...

    async def update_source_stats(self, url: str, streak: int, cycle_count: int) -> None:
        ...


class RunDuplicateTracker(Generic[ItemT]):
    def __init__(self) -> None:
        self._seen: set[tuple[str, int]] = set()
        self._lock = asyncio.Lock()

    async def claim(self, item: ItemT) -> bool:
        # In-run duplicate suppression must happen before any send attempt so overlapping
        # source pages inside the same cycle cannot produce double notifications.
        key = duplicate_key(item.name, item.price_int)
        if key is None:
            return True
        async with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            return True


async def process_marketplace_items(
    *,
    source_kind: str,
    source_name: str,
    items: Sequence[ItemT],
    repository: MarketplaceRepository[ItemT],
    duplicate_tracker: RunDuplicateTracker[ItemT],
    decide_item: Callable[[ItemT, Optional[Dict[str, Any]]], ItemDecision],
    build_message: Callable[[ItemT, Optional[Dict[str, Any]], str], str],
    send_item: Callable[[ItemT, str, str], Awaitable[bool]],
    hydrate_from_previous: Optional[Callable[[ItemT, Optional[Dict[str, Any]]], None]] = None,
    logger=None,
) -> PipelineStats:
    # This shared pipeline was introduced to make OLX and SHAFA follow the exact same
    # unsubscribe, duplicate, claim, send, and persistence order after item parsing.
    stats = PipelineStats()
    if not items:
        return stats

    previous_items = await repository.fetch_existing([item.id for item in items])
    duplicate_keys_in_db = await repository.fetch_duplicate_keys(list(items))
    unsubscribed_item_ids = await fetch_unsubscribed_ids(source_kind, [item.id for item in items])

    persist_only_updates: list[ItemUpdate[ItemT]] = []
    send_candidates: list[tuple[ItemT, Optional[Dict[str, Any]]]] = []

    for index, item in enumerate(items):
        previous = previous_items[index]
        if item.id in unsubscribed_item_ids:
            if logger is not None:
                logger.debug("Skipping unsubscribed %s item: %s", source_kind.upper(), item.id)
            continue

        item_duplicate_key = duplicate_key(item.name, item.price_int)
        if item_duplicate_key is not None and item_duplicate_key in duplicate_keys_in_db:
            if logger is not None:
                logger.debug(
                    "Skipping %s duplicate already in DB: %s | %s",
                    source_kind.upper(),
                    item.name,
                    item.price_int,
                )
            continue

        if not await duplicate_tracker.claim(item):
            if logger is not None:
                logger.debug(
                    "Skipping %s duplicate in current run: %s | %s",
                    source_kind.upper(),
                    item.name,
                    item.price_int,
                )
            continue

        if hydrate_from_previous is not None:
            hydrate_from_previous(item, previous)

        decision = decide_item(item, previous)
        if decision.persist_without_send:
            persist_only_updates.append(ItemUpdate(item=item, touch_last_sent=False))
            continue
        if not decision.send_notification:
            continue

        if not await repository.claim_notification_key(item, source_name):
            if logger is not None:
                logger.debug(
                    "Skipping %s duplicate already claimed/sent: %s | %s",
                    source_kind.upper(),
                    item.name,
                    item.price_int,
                )
            continue

        if decision.is_new_item:
            stats.total_new += 1
        send_candidates.append((item, previous))

    if persist_only_updates:
        await repository.persist_items(persist_only_updates, source_name)

    if not send_candidates:
        return stats

    async def _deliver(item: ItemT, previous: Optional[Dict[str, Any]]) -> bool:
        sent = False
        try:
            sent = bool(await send_item(item, build_message(item, previous, source_name), source_name))
            if sent:
                await repository.mark_notification_sent(item, source_name)
            else:
                await repository.release_notification_claim(item, source_name)
        except Exception:
            await repository.release_notification_claim(item, source_name)
            raise
        finally:
            # Persisting in finally keeps item state aligned even when Telegram delivery is
            # ambiguous or fails after the notification key was already claimed.
            await repository.persist_items([ItemUpdate(item=item, touch_last_sent=sent)], source_name)
        return sent

    results = await asyncio.gather(
        *(_deliver(item, previous) for item, previous in send_candidates),
        return_exceptions=True,
    )
    for result in results:
        if result is True:
            stats.total_sent += 1
    return stats
