from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, Protocol, Sequence, TypeVar

from helpers.analytics_events import AnalyticsSink, fingerprint_url, stable_hash
from helpers.marketplace_core import DeliveryResult, MarketplaceItem, SourceStats, duplicate_key
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
    total_seen: int = 0
    total_new: int = 0
    total_sent: int = 0
    total_persisted_without_send: int = 0
    total_unsubscribed: int = 0
    total_duplicate_db: int = 0
    total_duplicate_run: int = 0
    total_notification_claim_skipped: int = 0
    total_send_candidates: int = 0
    total_send_failed: int = 0

    def add(self, other: "PipelineStats") -> None:
        self.total_seen += other.total_seen
        self.total_new += other.total_new
        self.total_sent += other.total_sent
        self.total_persisted_without_send += other.total_persisted_without_send
        self.total_unsubscribed += other.total_unsubscribed
        self.total_duplicate_db += other.total_duplicate_db
        self.total_duplicate_run += other.total_duplicate_run
        self.total_notification_claim_skipped += other.total_notification_claim_skipped
        self.total_send_candidates += other.total_send_candidates
        self.total_send_failed += other.total_send_failed


class MarketplaceRepository(Protocol[ItemT]):
    async def fetch_existing(self, item_ids: list[str]) -> list[Optional[Dict[str, Any]]]:
        ...

    async def fetch_duplicate_keys(self, items: list[ItemT]) -> set[tuple[str, int]]:
        ...

    async def claim_notification_key(self, item: ItemT, source_name: str) -> bool:
        ...

    async def mark_notification_sent(self, item: ItemT, source_name: str, telegram_message_id: Optional[int] = None) -> None:
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
    send_item: Callable[[ItemT, str, str], Awaitable[bool | DeliveryResult]],
    hydrate_from_previous: Optional[Callable[[ItemT, Optional[Dict[str, Any]]], None]] = None,
    analytics_sink: Optional[AnalyticsSink] = None,
    logger=None,
) -> PipelineStats:
    # This shared pipeline was introduced to make OLX and SHAFA follow the exact same
    # unsubscribe, duplicate, claim, send, and persistence order after item parsing.
    stats = PipelineStats(total_seen=len(items))
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
            stats.total_unsubscribed += 1
            if logger is not None:
                logger.debug("Skipping unsubscribed %s item: %s", source_kind.upper(), item.id)
            continue

        item_duplicate_key = duplicate_key(item.name, item.price_int)
        if item_duplicate_key is not None and item_duplicate_key in duplicate_keys_in_db:
            stats.total_duplicate_db += 1
            if logger is not None:
                logger.debug(
                    "Skipping %s duplicate already in DB: %s | %s",
                    source_kind.upper(),
                    item.name,
                    item.price_int,
                )
            continue

        if not await duplicate_tracker.claim(item):
            stats.total_duplicate_run += 1
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
            stats.total_persisted_without_send += 1
            _record_item_analytics(
                analytics_sink,
                event="persist_without_send",
                source_kind=source_kind,
                source_name=source_name,
                item=item,
                previous=previous,
            )
            persist_only_updates.append(ItemUpdate(item=item, touch_last_sent=False))
            continue
        if not decision.send_notification:
            continue

        if not await repository.claim_notification_key(item, source_name):
            stats.total_notification_claim_skipped += 1
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
        stats.total_send_candidates += 1
        _record_item_analytics(
            analytics_sink,
            event="send_candidate",
            source_kind=source_kind,
            source_name=source_name,
            item=item,
            previous=previous,
        )
        send_candidates.append((item, previous))

    if persist_only_updates:
        await repository.persist_items(persist_only_updates, source_name)

    if not send_candidates:
        return stats

    def _coerce_delivery_result(value: bool | DeliveryResult) -> DeliveryResult:
        if isinstance(value, DeliveryResult):
            return value
        return DeliveryResult(delivered=bool(value))

    async def _deliver(item: ItemT, previous: Optional[Dict[str, Any]]) -> DeliveryResult:
        delivery = DeliveryResult(delivered=False)
        try:
            delivery = _coerce_delivery_result(await send_item(item, build_message(item, previous, source_name), source_name))
            if delivery.delivered:
                await repository.mark_notification_sent(item, source_name, delivery.telegram_message_id)
                _record_item_analytics(
                    analytics_sink,
                    event="sent",
                    source_kind=source_kind,
                    source_name=source_name,
                    item=item,
                    previous=previous,
                    delivery=delivery,
                )
            else:
                await repository.release_notification_claim(item, source_name)
                _record_item_analytics(
                    analytics_sink,
                    event="send_failed",
                    source_kind=source_kind,
                    source_name=source_name,
                    item=item,
                    previous=previous,
                    delivery=delivery,
                )
        except Exception:
            await repository.release_notification_claim(item, source_name)
            _record_item_analytics(
                analytics_sink,
                event="send_exception",
                source_kind=source_kind,
                source_name=source_name,
                item=item,
                previous=previous,
            )
            raise
        finally:
            # Failed sends are intentionally not persisted as normal seen items. Otherwise a
            # transient Telegram/image problem can make a first-seen item look already known
            # on the next run and silently suppress the retry.
            if delivery.delivered:
                await repository.persist_items([ItemUpdate(item=item, touch_last_sent=True)], source_name)
        return delivery

    results = await asyncio.gather(
        *(_deliver(item, previous) for item, previous in send_candidates),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, DeliveryResult) and result.delivered:
            stats.total_sent += 1
        else:
            stats.total_send_failed += 1
    return stats


def _record_item_analytics(
    analytics_sink: Optional[AnalyticsSink],
    *,
    event: str,
    source_kind: str,
    source_name: str,
    item: MarketplaceItem,
    previous: Optional[Dict[str, Any]],
    delivery: Optional[DeliveryResult] = None,
) -> None:
    if analytics_sink is None:
        return
    try:
        payload: dict[str, Any] = {
            "event": event,
            "source_kind": source_kind,
            "source_name": source_name,
            "item_id_hash": stable_hash(item.id),
            "name_hash": stable_hash(item.name),
            "price_int": item.price_int,
            "had_previous": previous is not None,
        }
        if delivery is not None:
            payload.update(
                {
                    "telegram_message_id": delivery.telegram_message_id,
                    "delivery_channel": delivery.channel,
                    "failure_reason": delivery.failure_reason,
                    "retry_later": delivery.retry_later,
                }
            )
        payload.update(fingerprint_url(item.link))
        analytics_sink.append_event("marketplace_item", payload)
        analytics_sink.add_daily_counters(
            "marketplace_items",
            dimensions={"source_kind": source_kind, "source_name": source_name, "event": event},
            counters={"items": 1},
        )
    except Exception:
        # Marketplace notification flow must never fail because the optional analytics
        # ledger cannot be written, especially during DB locks or disk-pressure debugging.
        return
